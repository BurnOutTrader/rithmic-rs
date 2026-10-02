//! Carries a one-use mutation claim below the split sink's pending slot.

use crate::MutationHandoff;
use futures_util::{Sink, Stream};
use std::{
    fmt,
    pin::Pin,
    task::{Context, Poll},
};
use tokio_tungstenite::tungstenite::{Error, Message};

#[derive(Debug)]
pub(crate) struct NativeMessage {
    pub(crate) message: Message,
    pub(crate) handoff: Option<MutationHandoff>,
}

impl From<Message> for NativeMessage {
    fn from(message: Message) -> Self {
        Self {
            message,
            handoff: None,
        }
    }
}

#[derive(Debug)]
pub(crate) enum NativeSendError {
    Transport(Error),
    MutationHandoffRefused,
}

impl fmt::Display for NativeSendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(error) => error.fmt(f),
            Self::MutationHandoffRefused => f.write_str("mutation handoff refused"),
        }
    }
}

impl From<Error> for NativeSendError {
    fn from(error: Error) -> Self {
        Self::Transport(error)
    }
}

#[derive(Debug)]
pub(crate) struct GuardedSocket<S> {
    inner: S,
}

impl<S> GuardedSocket<S> {
    pub(crate) fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S: Stream + Unpin> Stream for GuardedSocket<S> {
    type Item = S::Item;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.get_mut().inner).poll_next(cx)
    }
}

impl<S: Sink<Message, Error = Error> + Unpin> Sink<NativeMessage> for GuardedSocket<S> {
    type Error = NativeSendError;
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.get_mut().inner)
            .poll_ready(cx)
            .map_err(NativeSendError::Transport)
    }
    fn start_send(self: Pin<&mut Self>, item: NativeMessage) -> Result<(), Self::Error> {
        // SplitSink has acquired its inner lock and polled this socket ready.
        // Claim once, then enter the same native sink without an intervening await.
        if let Some(handoff) = item.handoff {
            if !handoff.claim() {
                return Err(NativeSendError::MutationHandoffRefused);
            }
        }
        Pin::new(&mut self.get_mut().inner)
            .start_send(item.message)
            .map_err(NativeSendError::Transport)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.get_mut().inner)
            .poll_flush(cx)
            .map_err(NativeSendError::Transport)
    }
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.get_mut().inner)
            .poll_close(cx)
            .map_err(NativeSendError::Transport)
    }
}

#[cfg(test)]
pub(crate) mod tests;
