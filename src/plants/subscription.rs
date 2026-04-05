use tokio::sync::broadcast;

use crate::{api::RithmicResponse, config::RithmicAccount, rti::messages::RithmicMessage};

/// Account-scoped wrapper around a plant subscription receiver.
///
/// Order and PnL plants share one upstream connection per login session while
/// updates remain account-specific. This receiver filters the shared broadcast
/// stream down to a single account identity while preserving the familiar
/// `.recv().await` API used by callers.
pub struct AccountSubscriptionReceiver {
    account: RithmicAccount,
    receiver: broadcast::Receiver<RithmicResponse>,
}

impl AccountSubscriptionReceiver {
    pub(crate) fn new(
        account: RithmicAccount,
        receiver: broadcast::Receiver<RithmicResponse>,
    ) -> Self {
        Self { account, receiver }
    }

    /// Wait for the next subscription update for this account.
    pub async fn recv(&mut self) -> Result<RithmicResponse, broadcast::error::RecvError> {
        loop {
            let response = self.receiver.recv().await?;
            if self.should_forward(&response) {
                return Ok(response);
            }
        }
    }

    /// Create a second receiver starting at the current stream position.
    #[must_use]
    pub fn resubscribe(&self) -> Self {
        Self {
            account: self.account.clone(),
            receiver: self.receiver.resubscribe(),
        }
    }

    fn should_forward(&self, response: &RithmicResponse) -> bool {
        if is_connection_level(response) {
            return true;
        }

        response_account_id(response)
            .is_some_and(|account_id| account_id == self.account.account_id)
    }
}

impl std::fmt::Debug for AccountSubscriptionReceiver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AccountSubscriptionReceiver")
            .field("account_id", &self.account.account_id)
            .finish_non_exhaustive()
    }
}

fn is_connection_level(response: &RithmicResponse) -> bool {
    matches!(
        response.message,
        RithmicMessage::ResponseHeartbeat(_)
            | RithmicMessage::ForcedLogout(_)
            | RithmicMessage::ConnectionError
            | RithmicMessage::HeartbeatTimeout
    )
}

fn response_account_id(response: &RithmicResponse) -> Option<&str> {
    match &response.message {
        RithmicMessage::UserAccountUpdate(update) => update.account_id.as_deref(),
        RithmicMessage::AccountListUpdates(update) => update.account_id.as_deref(),
        RithmicMessage::AccountRmsUpdates(update) => update.account_id.as_deref(),
        RithmicMessage::BracketUpdates(update) => update.account_id.as_deref(),
        RithmicMessage::RithmicOrderNotification(update) => update.account_id.as_deref(),
        RithmicMessage::ExchangeOrderNotification(update) => update.account_id.as_deref(),
        RithmicMessage::AccountPnLPositionUpdate(update) => update.account_id.as_deref(),
        RithmicMessage::InstrumentPnLPositionUpdate(update) => update.account_id.as_deref(),
        _ => None,
    }
}
