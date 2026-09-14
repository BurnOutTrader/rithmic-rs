//! Request-scoped historical replay progress and cancellation.

use std::collections::HashSet;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::Instant;

use crate::{RithmicError, RithmicResponse, plants::history_plant::HistoryPlantCommand};

/// Coalesced progress for one replay. Observing progress never copies historical data.
///
/// The SDK sets no deadline. A caller may measure inactivity from `last_progress_at`
/// after `sent_at` becomes available. Heartbeats, other requests, empty intermediate
/// frames, and repeated continuation keys do not advance this snapshot.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct ReplayProgress {
    /// When the original request finished writing to the socket; not queue admission.
    pub sent_at: Option<Instant>,
    /// Last data frame, distinct continuation notice, matched successful resume
    /// acknowledgement, or original send.
    pub last_progress_at: Option<Instant>,
    /// Data-bearing frames received for this request, including its continuation.
    pub data_frames: u64,
    /// Distinct continuation keys received for this request.
    pub continuations: u64,
}

/// How the replay ended. Only `Complete` proves the server finished its reply.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReplayEnd {
    /// The server supplied its successful terminal marker.
    Complete,
    /// The server ended an incomplete replay with output inhibited (code 12).
    Truncated,
    /// The server refused the original replay or a continuation.
    Refused(RithmicError),
    /// A local transport or protocol failure prevented completion.
    Failed(RithmicError),
    /// The caller cancelled locally. The healthy connection remains available.
    Cancelled,
}

/// The sole owned payload of a finished replay, including its final wire marker
/// when one was received. A non-complete outcome's responses are only a prefix,
/// never evidence that the requested time window is covered.
#[derive(Debug)]
pub struct ReplayOutcome {
    /// Received frames, moved from the accumulator without copying their data.
    pub responses: Vec<RithmicResponse>,
    /// Completion, refusal, incomplete termination, failure, or cancellation.
    pub end: ReplayEnd,
}

/// A replay admitted to the history plant's bounded command queue.
///
/// Clone the progress receiver with [`Self::subscribe_progress`] and select its
/// changes against [`Self::result`] and your own cancellation/deadline signal.
/// Dropping the result future is safe: it does not cancel or consume the reply.
/// Dropping this handle cancels locally; use [`Self::cancel`] when you need an
/// acknowledgement before reusing your caller-side resources.
#[derive(Debug)]
#[must_use = "dropping the handle cancels the replay locally"]
pub struct ReplayHandle {
    result: Option<oneshot::Receiver<ReplayOutcome>>,
    progress: watch::Receiver<ReplayProgress>,
    sender: mpsc::Sender<HistoryPlantCommand>,
    control: Arc<ReplayControl>,
}

impl ReplayHandle {
    /// Observe coalesced progress for this request only.
    pub fn subscribe_progress(&self) -> watch::Receiver<ReplayProgress> {
        self.progress.clone()
    }

    /// Wait for the terminal outcome. Cancel-safe until it returns; call once.
    pub async fn result(&mut self) -> Result<ReplayOutcome, RithmicError> {
        let Some(receiver) = self.result.as_mut() else {
            return Err(RithmicError::InvalidArgument(
                "replay result already consumed".into(),
            ));
        };
        let result = receiver.await.map_err(|_| RithmicError::ConnectionClosed);
        self.result = None;
        result
    }

    /// Cancel only this replay and wait until the actor has released its active
    /// responder and accumulator. If completion won the race, its outcome stands.
    ///
    /// This is local cancellation: no logout, socket close, or invented wire
    /// cancellation is sent. Late frames remain correlated and are discarded
    /// until the server's real terminal marker. Call [`Self::result`] to obtain
    /// the outcome and any received prefix after this acknowledgement.
    pub async fn cancel(&self) -> Result<(), RithmicError> {
        self.control.cancel();
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(HistoryPlantCommand::CancelReplay {
                control: self.control.clone(),
                acknowledged: Some(tx),
            })
            .await
            .map_err(|_| RithmicError::ConnectionClosed)?;
        rx.await.map_err(|_| RithmicError::ConnectionClosed)
    }
}

impl Drop for ReplayHandle {
    fn drop(&mut self) {
        if self.result.is_some() {
            self.control.cancel();
            // The actor also sweeps cancellation flags on every turn, so a full
            // command queue cannot strand a dropped handle's accumulator.
            let _ = self.sender.try_send(HistoryPlantCommand::CancelReplay {
                control: self.control.clone(),
                acknowledged: None,
            });
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct ReplayControl(AtomicBool);

impl ReplayControl {
    pub(crate) fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }
    pub(crate) fn cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

#[derive(Debug)]
pub(crate) struct ReplayRequest {
    pub(crate) control: Arc<ReplayControl>,
    pub(crate) responder: oneshot::Sender<ReplayOutcome>,
    pub(crate) progress: watch::Sender<ReplayProgress>,
    pub(crate) responses: Vec<RithmicResponse>,
    pub(crate) continuation_keys: HashSet<String>,
}

impl ReplayRequest {
    pub(crate) fn new(sender: mpsc::Sender<HistoryPlantCommand>) -> (ReplayHandle, Self) {
        let (tx, rx) = oneshot::channel();
        let (progress_tx, progress_rx) = watch::channel(ReplayProgress::default());
        let control = Arc::new(ReplayControl::default());
        (
            ReplayHandle {
                result: Some(rx),
                progress: progress_rx,
                sender,
                control: control.clone(),
            },
            Self {
                control,
                responder: tx,
                progress: progress_tx,
                responses: Vec::new(),
                continuation_keys: HashSet::new(),
            },
        )
    }

    pub(crate) fn cancelled(&self) -> bool {
        self.control.cancelled() || self.responder.is_closed()
    }

    pub(crate) fn finish(self, end: ReplayEnd) {
        let _ = self.responder.send(ReplayOutcome {
            responses: self.responses,
            end,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::FutureExt as _;

    #[tokio::test]
    async fn dropping_a_result_wait_does_not_cancel_or_lose_the_single_owned_reply() {
        let (sender, _receiver) = mpsc::channel(4);
        let (mut handle, request) = ReplayRequest::new(sender);
        assert!(handle.result().now_or_never().is_none());
        assert!(!request.cancelled());
        request.finish(ReplayEnd::Complete);
        assert_eq!(handle.result().await.unwrap().end, ReplayEnd::Complete);
        assert!(matches!(
            handle.result().await,
            Err(RithmicError::InvalidArgument(_))
        ));
    }
}
