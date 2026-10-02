use super::*;
use futures_util::{SinkExt, StreamExt, poll};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
};

const READY: u8 = 1;
const CLAIMED: u8 = 2;
const REVOKED: u8 = 3;

#[derive(Default)]
pub(crate) struct Controls {
    pub(crate) ready: AtomicBool,
    pub(crate) flush_ready: AtomicBool,
    ready_error: AtomicBool,
    pub(crate) messages: Mutex<Vec<Message>>,
    pub(crate) ready_observed: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    pub(crate) waker: Mutex<Option<std::task::Waker>>,
}

pub(crate) struct Peer(pub(crate) Arc<Controls>);

impl Stream for Peer {
    type Item = Result<Message, Error>;
    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Pending
    }
}

impl Sink<Message> for Peer {
    type Error = Error;
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        if let Some(observed) = self.0.ready_observed.lock().unwrap().take() {
            let _ = observed.send(());
        }
        *self.0.waker.lock().unwrap() = Some(cx.waker().clone());
        if self.0.ready_error.load(Ordering::SeqCst) {
            Poll::Ready(Err(Error::ConnectionClosed))
        } else if self.0.ready.load(Ordering::SeqCst) {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
    fn start_send(self: Pin<&mut Self>, item: Message) -> Result<(), Error> {
        self.0.messages.lock().unwrap().push(item);
        Ok(())
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Error>> {
        if self.0.flush_ready.load(Ordering::SeqCst) {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        self.poll_flush(cx)
    }
}

fn mutation(state: Arc<AtomicU8>, calls: Arc<AtomicUsize>) -> NativeMessage {
    NativeMessage {
        message: Message::Binary(vec![1, 2, 3].into()),
        handoff: Some(MutationHandoff::new(move || {
            calls.fetch_add(1, Ordering::SeqCst);
            state
                .compare_exchange(READY, CLAIMED, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        })),
    }
}

#[tokio::test]
async fn handoff_revoke_wins_below_split_and_refused_slot_is_consumed() {
    let controls = Arc::new(Controls::default());
    controls.flush_ready.store(true, Ordering::SeqCst);
    let state = Arc::new(AtomicU8::new(READY));
    let calls = Arc::new(AtomicUsize::new(0));
    let (mut sink, _reader) = GuardedSocket::new(Peer(controls.clone())).split();
    // SplitSink accepts the envelope before its inner readiness is polled.
    sink.feed(mutation(state.clone(), calls.clone()))
        .await
        .unwrap();
    let mut flush = Box::pin(sink.flush());
    assert!(poll!(&mut flush).is_pending());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(controls.messages.lock().unwrap().is_empty());
    assert_eq!(
        state.compare_exchange(READY, REVOKED, Ordering::SeqCst, Ordering::SeqCst),
        Ok(READY)
    );
    controls.ready.store(true, Ordering::SeqCst);
    assert!(matches!(
        flush.await,
        Err(NativeSendError::MutationHandoffRefused)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(controls.messages.lock().unwrap().is_empty());
    sink.send(Message::Ping(vec![9].into()).into())
        .await
        .unwrap();
    assert_eq!(
        *controls.messages.lock().unwrap(),
        vec![Message::Ping(vec![9].into())]
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn handoff_claim_wins_before_revocation_and_delayed_flush_writes_once() {
    let controls = Arc::new(Controls::default());
    controls.ready.store(true, Ordering::SeqCst);
    let state = Arc::new(AtomicU8::new(READY));
    let calls = Arc::new(AtomicUsize::new(0));
    let (mut sink, _reader) = GuardedSocket::new(Peer(controls.clone())).split();
    sink.feed(mutation(state.clone(), calls.clone()))
        .await
        .unwrap();
    let mut flush = Box::pin(sink.flush());
    assert!(poll!(&mut flush).is_pending());
    assert_eq!(state.load(Ordering::SeqCst), CLAIMED);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(controls.messages.lock().unwrap().len(), 1);
    assert_eq!(
        state.compare_exchange(READY, REVOKED, Ordering::SeqCst, Ordering::SeqCst),
        Err(CLAIMED)
    );
    controls.flush_ready.store(true, Ordering::SeqCst);
    flush.await.unwrap();
    assert_eq!(controls.messages.lock().unwrap().len(), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn handoff_readiness_error_does_not_consume_claim() {
    let controls = Arc::new(Controls::default());
    controls.ready_error.store(true, Ordering::SeqCst);
    let state = Arc::new(AtomicU8::new(READY));
    let calls = Arc::new(AtomicUsize::new(0));
    let (mut sink, _reader) = GuardedSocket::new(Peer(controls.clone())).split();
    let error = sink
        .send(mutation(state.clone(), calls.clone()))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        NativeSendError::Transport(Error::ConnectionClosed)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(state.load(Ordering::SeqCst), READY);
    assert!(controls.messages.lock().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn handoff_post_claim_timeout_is_not_replayed() {
    let controls = Arc::new(Controls::default());
    controls.ready.store(true, Ordering::SeqCst);
    let state = Arc::new(AtomicU8::new(READY));
    let calls = Arc::new(AtomicUsize::new(0));
    let (mut sink, _reader) = GuardedSocket::new(Peer(controls.clone())).split();
    let result = crate::ws::send_with_timeout(
        &mut sink,
        mutation(state.clone(), calls.clone()),
        std::time::Duration::from_secs(10),
    )
    .await;
    assert!(matches!(
        result,
        Err(crate::ws::WebSocketSendError::Timeout)
    ));
    assert_eq!(state.load(Ordering::SeqCst), CLAIMED);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(controls.messages.lock().unwrap().len(), 1);
}
