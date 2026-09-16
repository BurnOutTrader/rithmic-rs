use std::sync::Arc;

use tokio::time::Instant;

use super::{Resume, RithmicRequestHandler};
use crate::{
    ReplayEnd, RithmicError, RithmicResponse,
    replay::{ReplayControl, ReplayRequest},
    rti::messages::RithmicMessage,
};

/// Whether this frame carries replay data — the evidence, on either path,
/// that new data has arrived since a truncation notice, re-arming the keys
/// that notice already used. Time-bar and volume-profile parts carry a
/// `marker`; tick-bar parts carry bar ssboe stamps.
pub(super) fn carries_replay_data(response: &RithmicResponse) -> bool {
    match &response.message {
        RithmicMessage::ResponseTimeBarReplay(m) => m.marker.is_some(),
        RithmicMessage::ResponseTickBarReplay(m) => !m.data_bar_ssboe.is_empty(),
        RithmicMessage::ResponseVolumeProfileMinuteBars(m) => m.marker.is_some(),
        _ => false,
    }
}

impl RithmicRequestHandler {
    /// Admission is checked before sending, including a handle dropped while
    /// its start future was waiting for room in the command queue.
    pub(crate) fn register_replay(&mut self, id: String, request: ReplayRequest) -> bool {
        if request.cancelled() {
            request.finish(ReplayEnd::Cancelled);
            return false;
        }
        self.replay_map.insert(id, request);
        true
    }

    pub(crate) fn replay_send_allowed(&mut self, id: &str) -> bool {
        let cancelled = self
            .replay_map
            .get(id)
            .is_some_and(ReplayRequest::cancelled);
        if cancelled {
            self.release_cancelled_replays();
        }
        !cancelled
    }

    pub(crate) fn forget_resume(&mut self, id: &str) {
        self.resumes.remove(id);
    }

    pub(crate) fn mark_sent(&mut self, id: &str) {
        if let Some(request) = self.replay_map.get_mut(id) {
            request.progress.send_if_modified(|progress| {
                if progress.sent_at.is_some() {
                    return false;
                }
                let now = Instant::now();
                progress.sent_at = Some(now);
                progress.last_progress_at = Some(now);
                true
            });
        }
    }

    pub(crate) fn cancel_replay(&mut self, control: &Arc<ReplayControl>) {
        control.cancel();
        self.release_cancelled_replays();
    }

    pub(crate) fn release_cancelled_replays(&mut self) {
        // Each removed value is moved straight to its one result receiver.
        // Only the id and late-frame count remain while the remote reply drains.
        let cancelled: Vec<_> = self
            .replay_map
            .iter()
            .filter(|(_, request)| request.cancelled())
            .map(|(id, _)| id.clone())
            .collect();
        for id in cancelled {
            if let Some(request) = self.replay_map.remove(&id) {
                if request.progress.borrow().sent_at.is_some() {
                    self.late_continuations.insert(id, 0);
                }
                request.finish(ReplayEnd::Cancelled);
            }
        }
    }

    pub(super) fn handle_replay_response(&mut self, response: RithmicResponse) -> Option<Resume> {
        let id = response.request_id.clone();
        let request = self.replay_map.get_mut(&id)?;
        if response.is_truncated() {
            let key = response.resume_key()?.to_owned();
            // Duplicate notices without intervening data are not progress.
            // The venue reuses a key when a later chunk reaches its budget.
            if !request.continuation_keys.insert(key.clone()) {
                return None;
            }
            request.progress.send_modify(|progress| {
                progress.continuations = progress.continuations.saturating_add(1);
                progress.last_progress_at = Some(Instant::now());
            });
            return Some(Resume {
                request_id: id,
                key,
            });
        }
        let data = carries_replay_data(&response);
        if data && response.error.is_none() {
            request.continuation_keys.clear();
            request.progress.send_modify(|progress| {
                progress.data_frames = progress.data_frames.saturating_add(1);
                progress.last_progress_at = Some(Instant::now());
            });
        }
        if response.has_more && response.error.is_none() {
            request.responses.push(response);
            return None;
        }
        let end = if response.rp_code_num() == Some("12") {
            ReplayEnd::Truncated
        } else if let Some(error) = response.error.clone() {
            match error {
                RithmicError::RequestRejected(_) => ReplayEnd::Refused(error),
                _ => ReplayEnd::Failed(error),
            }
        } else if response.rp_code_num() == Some("0") {
            ReplayEnd::Complete
        } else {
            ReplayEnd::Failed(RithmicError::ProtocolError(
                "replay ended without a successful terminal marker".into(),
            ))
        };
        let terminal =
            !response.has_more && response.rp_code().is_some_and(|code| !code.is_empty());
        if let Some(mut request) = self.replay_map.remove(&id) {
            request.responses.push(response);
            request.finish(end);
            if !terminal {
                self.late_continuations.insert(id, 0);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ReplayHandle,
        rti::{ResponseResumeBars, ResponseTimeBarReplay},
    };
    use std::time::Duration;
    use tokio::sync::mpsc;

    fn registered(handler: &mut RithmicRequestHandler, id: &str) -> ReplayHandle {
        let (sender, _receiver) = mpsc::channel(4);
        let (handle, request) = ReplayRequest::new(sender);
        assert!(handler.register_replay(id.into(), request));
        handle
    }

    fn frame(id: &str, marker: Option<i32>, code: &[&str], key: Option<&str>) -> RithmicResponse {
        RithmicResponse {
            request_id: id.into(),
            message: RithmicMessage::ResponseTimeBarReplay(ResponseTimeBarReplay {
                marker,
                rp_code: code.iter().map(|s| (*s).into()).collect(),
                request_key: key.map(Into::into),
                ..Default::default()
            }),
            is_update: false,
            has_more: marker.is_some(),
            multi_response: true,
            error: None,
            source: "test".into(),
        }
    }

    fn ack(id: &str, error: Option<RithmicError>) -> RithmicResponse {
        let mut response = frame(id, None, &[], None);
        response.message = RithmicMessage::ResponseResumeBars(ResponseResumeBars {
            rp_code: vec![if error.is_some() { "7" } else { "0" }.into()],
            ..Default::default()
        });
        response.error = error;
        response
    }

    #[tokio::test(start_paused = true)]
    async fn a_reused_continuation_key_resumes_again_after_new_replay_data() {
        let mut handler = RithmicRequestHandler::new();
        let mut replay = registered(&mut handler, "original");
        let _other = registered(&mut handler, "other");
        let progress = replay.subscribe_progress();
        handler.mark_sent("original");
        handler.handle_response(frame("original", Some(1), &[], None));
        let first = handler
            .handle_response(frame("original", None, &[], Some("0")))
            .expect("first cut asks to resume");
        handler.register_resume("resume-one".into(), first.request_id);
        handler.handle_response(ack("resume-one", None));
        let acknowledged = *progress.borrow();
        handler.handle_response(frame("other", Some(1), &[], None));
        assert!(
            handler
                .handle_response(frame("original", None, &[], Some("0")))
                .is_none(),
            "an acknowledgement or another replay cannot rearm the key"
        );
        assert_eq!(*progress.borrow(), acknowledged);

        tokio::time::advance(Duration::from_secs(1)).await;
        handler.handle_response(frame("original", Some(2), &[], None));
        let continued = handler
            .handle_response(frame("original", None, &[], Some("0")))
            .expect("the venue reuses the same key after another chunk of data");
        assert_eq!(continued.request_id, "original");
        assert_eq!(continued.key, "0");
        let second = *progress.borrow();
        assert_eq!(second.continuations, 2);
        assert!(second.last_progress_at > acknowledged.last_progress_at);
        assert!(
            handler
                .handle_response(frame("original", None, &[], Some("0")))
                .is_none(),
            "the repeated notice remains inert until data advances again"
        );
        assert_eq!(*progress.borrow(), second);
        handler.handle_response(frame("original", None, &["0"], None));
        let outcome = replay.result().await.unwrap();
        assert_eq!(outcome.end, ReplayEnd::Complete);
        assert_eq!(outcome.responses.len(), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn progress_is_request_scoped_and_only_meaningful_continuations_advance_it() {
        let mut handler = RithmicRequestHandler::new();
        let mut replay = registered(&mut handler, "original");
        let other = registered(&mut handler, "other");
        let progress = replay.subscribe_progress();
        assert_eq!(progress.borrow().sent_at, None);
        handler.mark_sent("original");
        let sent = *progress.borrow();
        assert!(sent.sent_at.is_some());
        tokio::time::advance(Duration::from_secs(70)).await;
        handler.handle_response(frame("other", Some(1), &[], None));
        assert_eq!(*progress.borrow(), sent);
        let mut empty = frame("original", None, &[], None);
        empty.has_more = true;
        handler.handle_response(empty);
        assert_eq!(*progress.borrow(), sent);
        handler.handle_response(frame("original", Some(1), &[], None));
        let data = *progress.borrow();
        assert_eq!(data.data_frames, 1);
        assert!(data.last_progress_at > sent.last_progress_at);
        tokio::time::advance(Duration::from_secs(70)).await;
        let resume = handler
            .handle_response(frame("original", None, &[], Some("key")))
            .unwrap();
        assert_eq!(resume.request_id, "original");
        let notice = *progress.borrow();
        assert_eq!(notice.continuations, 1);
        tokio::time::advance(Duration::from_secs(70)).await;
        assert!(
            handler
                .handle_response(frame("original", None, &[], Some("key")))
                .is_none()
        );
        assert_eq!(*progress.borrow(), notice);
        handler.register_resume("resume".into(), resume.request_id);
        handler.mark_sent("original");
        assert_eq!(
            *progress.borrow(),
            notice,
            "resume send is not original send"
        );
        handler.handle_response(ack("resume", None));
        let acknowledged = *progress.borrow();
        assert!(acknowledged.last_progress_at > notice.last_progress_at);
        handler.handle_response(ack("resume", None));
        assert_eq!(
            *progress.borrow(),
            acknowledged,
            "duplicate acknowledgement is inert"
        );
        handler.handle_response(frame("original", None, &["0"], None));
        let outcome = replay.result().await.unwrap();
        assert_eq!(outcome.end, ReplayEnd::Complete);
        assert_eq!(
            outcome.responses.len(),
            3,
            "empty intermediate, data, and real end only"
        );
        drop(other);
    }

    #[tokio::test]
    async fn cancellation_releases_payload_but_keeps_original_and_resume_correlations_until_their_ends()
     {
        let mut handler = RithmicRequestHandler::new();
        let mut replay = registered(&mut handler, "original");
        let other = registered(&mut handler, "other");
        handler.mark_sent("original");
        handler.handle_response(frame("original", Some(1), &[], None));
        handler.handle_response(frame("original", None, &[], Some("key")));
        handler.register_resume("resume".into(), "original".into());
        handler.replay_map["original"].control.cancel();
        handler.release_cancelled_replays();
        assert!(!handler.replay_map.contains_key("original"));
        assert!(handler.replay_map.contains_key("other"));
        assert_eq!(replay.result().await.unwrap().end, ReplayEnd::Cancelled);
        assert!(handler.late_continuations.contains_key("original"));
        assert!(
            handler
                .handle_response(frame("original", None, &[], Some("late")))
                .is_none()
        );
        assert!(
            handler.late_continuations.contains_key("original"),
            "a cut is not remote end"
        );
        handler.handle_response(frame("original", None, &[], None));
        assert!(
            handler.late_continuations.contains_key("original"),
            "a malformed dataless frame is not remote end"
        );
        handler.handle_response(frame("original", Some(2), &[], None));
        assert_eq!(handler.late_continuations.get("original"), Some(&1));
        handler.handle_response(ack("resume", None));
        assert!(handler.resumes.is_empty());
        handler.handle_response(frame("original", None, &["12"], None));
        assert!(handler.late_continuations.is_empty());
        assert!(handler.response_vec_map.is_empty());
        drop(other);
    }

    #[tokio::test]
    async fn refused_resume_is_explicit_and_retains_only_one_owned_prefix() {
        let mut handler = RithmicRequestHandler::new();
        let mut replay = registered(&mut handler, "original");
        handler.handle_response(frame("original", Some(1), &[], None));
        handler.handle_response(frame("original", None, &[], Some("key")));
        handler.register_resume("resume".into(), "original".into());
        let error = RithmicError::ProtocolError("refused".into());
        handler.handle_response(ack("resume", Some(error.clone())));
        let outcome = replay.result().await.unwrap();
        assert_eq!(outcome.end, ReplayEnd::Refused(error));
        assert_eq!(
            outcome.responses.len(),
            2,
            "prefix plus refusal acknowledgement"
        );
        assert!(handler.replay_map.is_empty());
        assert!(handler.response_vec_map.is_empty());
        assert!(handler.late_continuations.contains_key("original"));
    }

    #[tokio::test]
    async fn truncated_and_malformed_terminal_frames_never_complete() {
        for (code, expected) in [
            (&["12"][..], ReplayEnd::Truncated),
            (
                &[][..],
                ReplayEnd::Failed(RithmicError::ProtocolError(
                    "replay ended without a successful terminal marker".into(),
                )),
            ),
        ] {
            let mut handler = RithmicRequestHandler::new();
            let mut replay = registered(&mut handler, "original");
            handler.handle_response(frame("original", Some(1), &[], None));
            handler.handle_response(frame("original", None, code, None));
            let outcome = replay.result().await.unwrap();
            assert_eq!(outcome.end, expected);
            assert_eq!(outcome.responses.len(), 2);
        }
    }

    #[tokio::test]
    async fn a_cancelled_request_is_never_admitted_and_transport_failure_never_claims_sent() {
        let mut handler = RithmicRequestHandler::new();
        let (sender, _receiver) = mpsc::channel(4);
        let (mut replay, request) = ReplayRequest::new(sender);
        request.control.cancel();
        assert!(!handler.register_replay("unsent".into(), request));
        assert_eq!(replay.result().await.unwrap().end, ReplayEnd::Cancelled);
        assert!(handler.late_continuations.is_empty(), "nothing was sent");
        let mut failed = registered(&mut handler, "failed");
        let progress = failed.subscribe_progress();
        handler.fail_request("failed", RithmicError::SendFailed);
        assert_eq!(
            failed.result().await.unwrap().end,
            ReplayEnd::Failed(RithmicError::SendFailed)
        );
        assert_eq!(progress.borrow().sent_at, None);
        assert!(
            !handler.late_continuations.contains_key("failed"),
            "the venue never saw a request that failed to send"
        );

        let mut sent = registered(&mut handler, "sent");
        handler.mark_sent("sent");
        handler.fail_request("sent", RithmicError::SendFailed);
        assert_eq!(
            sent.result().await.unwrap().end,
            ReplayEnd::Failed(RithmicError::SendFailed)
        );
        assert_eq!(
            handler.late_continuations.get("sent"),
            Some(&0),
            "a request the venue saw can still be streaming for its id"
        );
    }

    #[tokio::test]
    async fn local_connection_shutdown_releases_all_replays_and_inert_correlations() {
        let mut handler = RithmicRequestHandler::new();
        let mut replay = registered(&mut handler, "original");
        handler.handle_response(frame("original", Some(1), &[], None));
        handler.register_resume("resume".into(), "original".into());
        handler.drain_and_drop();
        assert_eq!(
            replay.result().await.unwrap().end,
            ReplayEnd::Failed(RithmicError::ConnectionClosed)
        );
        assert!(handler.replay_map.is_empty());
        assert!(handler.resumes.is_empty());
        assert!(handler.late_continuations.is_empty());
    }
}
