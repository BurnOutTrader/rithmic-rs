use async_trait::async_trait;
use tracing::{error, info, warn};

use crate::{
    ConnectStrategy,
    api::{
        receiver_api::{RithmicReceiverApi, RithmicResponse},
        sender_api::RithmicSenderApi,
    },
    config::RithmicConfig,
    ping_manager::PingManager,
    request_handler::{RithmicRequest, RithmicRequestHandler},
    rti::{messages::RithmicMessage, request_login::SysInfraType, request_pn_l_position_updates},
    ws::{
        HEARTBEAT_SECS, PING_TIMEOUT_SECS, PlantActor, connect_with_strategy,
        get_heartbeat_interval, get_ping_interval,
    },
};

use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};

use tokio::{
    net::TcpStream,
    sync::{broadcast, mpsc, oneshot},
    time::{Interval, sleep_until},
};

use tokio_tungstenite::{
    MaybeTlsStream,
    tungstenite::{Error, Message, error::ProtocolError},
};

pub(crate) enum PnlPlantCommand {
    Close,
    ListSystemInfo {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    Login {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    SetLogin,
    Logout {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    PnlPositionSnapshots {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    SendHeartbeat,
    UpdateHeartbeat {
        seconds: u64,
    },
    SubscribePnlUpdates {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    UnsubscribePnlUpdates {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
}

/// The RithmicPnlPlant provides access to profit and loss (PnL) information through the Rithmic API.
///
/// It allows applications to:
/// - Retrieve current PnL information for positions
/// - Subscribe to real-time PnL updates
/// - Track position changes and risk metrics
///
/// # Example
///
/// ```no_run
/// use rithmic_rs::{
///     RithmicConfig, RithmicEnv, ConnectStrategy, RithmicPnlPlant,
///     rti::messages::RithmicMessage,
/// };
/// use tokio::time::{sleep, Duration};
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     // Step 1: Create connection configuration
///     let config = RithmicConfig::from_env(RithmicEnv::Demo)?;
///
///     // Step 2: Connect to the PnL plant
///     let pnl_plant = RithmicPnlPlant::connect(&config, ConnectStrategy::Simple).await?;
///
///     // Step 3: Get a handle to interact with the plant
///     let mut handle = pnl_plant.get_handle();
///
///     // Step 4: Login to the PnL plant
///     handle.login().await?;
///
///     // Step 5: Get a current snapshot of all PnL positions
///     let snapshots = handle.pnl_position_snapshots().await?;
///     println!("PnL position snapshot: {:?}", snapshots);
///
///     // Step 6: Subscribe to ongoing PnL updates
///     handle.subscribe_pnl_updates().await?;
///
///     // Step 7: Process real-time PnL updates
///     for _ in 0..5 {
///         match handle.subscription_receiver.recv().await {
///             Ok(update) => {
///                 match update.message {
///                     RithmicMessage::AccountPnLPositionUpdate(_) => {}
///                     RithmicMessage::InstrumentPnLPositionUpdate(_) => {}
///                     _ => {}
///                 }
///             },
///             Err(e) => println!("Error receiving update: {}", e),
///         }
///     }
///
///     // Step 8: Disconnect when done
///     handle.disconnect().await?;
///
///     Ok(())
/// }
/// ```
pub struct RithmicPnlPlant {
    pub connection_handle: tokio::task::JoinHandle<()>,
    sender: mpsc::Sender<PnlPlantCommand>,
    subscription_sender: broadcast::Sender<RithmicResponse>,
}

impl RithmicPnlPlant {
    /// Create a new PnL Plant connection to access profit and loss information.
    ///
    /// # Arguments
    /// * `config` - Rithmic configuration
    /// * `strategy` - Connection strategy (Simple, Retry, or AlternateWithRetry)
    ///
    /// # Returns
    /// A `Result` containing the connected `RithmicPnlPlant` instance, or an error if the connection fails.
    ///
    /// # Errors
    /// Returns an error if unable to establish WebSocket connection to the server.
    pub async fn connect(
        config: &RithmicConfig,
        strategy: ConnectStrategy,
    ) -> Result<RithmicPnlPlant, Box<dyn std::error::Error>> {
        let (req_tx, req_rx) = mpsc::channel::<PnlPlantCommand>(64);
        let (sub_tx, _sub_rx) = broadcast::channel(10_000);

        let mut pnl_plant = PnlPlant::new(req_rx, sub_tx.clone(), config, strategy).await?;

        let connection_handle = tokio::spawn(async move {
            pnl_plant.run().await;
        });

        Ok(RithmicPnlPlant {
            connection_handle,
            sender: req_tx,
            subscription_sender: sub_tx,
        })
    }
}

impl RithmicPnlPlant {
    /// Get a handle to interact with the PnL plant.
    ///
    /// The handle provides methods to subscribe to PnL updates and retrieve position snapshots.
    /// Multiple handles can be created from the same plant.
    pub fn get_handle(&self) -> RithmicPnlPlantHandle {
        RithmicPnlPlantHandle {
            sender: self.sender.clone(),
            subscription_receiver: self.subscription_sender.subscribe(),
        }
    }
}

#[derive(Debug)]
struct PnlPlant {
    config: RithmicConfig,
    interval: Interval,
    logged_in: bool,
    ping_interval: Interval,
    ping_manager: PingManager,
    request_handler: RithmicRequestHandler,
    request_receiver: mpsc::Receiver<PnlPlantCommand>,
    rithmic_reader: SplitStream<tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>>,
    rithmic_receiver_api: RithmicReceiverApi,
    rithmic_sender: SplitSink<
        tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>,
        tokio_tungstenite::tungstenite::Message,
    >,
    rithmic_sender_api: RithmicSenderApi,
    subscription_sender: broadcast::Sender<RithmicResponse>,
}

impl PnlPlant {
    async fn new(
        request_receiver: mpsc::Receiver<PnlPlantCommand>,
        subscription_sender: broadcast::Sender<RithmicResponse>,
        config: &RithmicConfig,
        strategy: ConnectStrategy,
    ) -> Result<PnlPlant, Box<dyn std::error::Error>> {
        let ws_stream = connect_with_strategy(&config.url, &config.beta_url, strategy).await?;

        let (rithmic_sender, rithmic_reader) = ws_stream.split();

        let rithmic_sender_api = RithmicSenderApi::new(config);
        let rithmic_receiver_api = RithmicReceiverApi {
            source: "pnl_plant".to_string(),
        };

        let interval = get_heartbeat_interval(None);
        let ping_interval = get_ping_interval(None);
        let ping_manager = PingManager::new(PING_TIMEOUT_SECS);

        Ok(PnlPlant {
            config: config.clone(),
            interval,
            ping_interval,
            logged_in: false,
            ping_manager,
            request_handler: RithmicRequestHandler::new(),
            request_receiver,
            rithmic_reader,
            rithmic_receiver_api,
            rithmic_sender_api,
            rithmic_sender,
            subscription_sender,
        })
    }
}

#[async_trait]
impl PlantActor for PnlPlant {
    type Command = PnlPlantCommand;

    async fn run(&mut self) {
        loop {
            tokio::select! {
                _ = self.interval.tick() => {
                    if self.logged_in {
                        self.handle_command(PnlPlantCommand::SendHeartbeat).await;
                    }
                }
                _ = self.ping_interval.tick() => {
                    self.ping_manager.sent();
                    let _ = self.rithmic_sender.send(Message::Ping(vec![].into())).await;
                }
                _ = async {
                    if let Some(timeout_at) = self.ping_manager.next_timeout_at() {
                        sleep_until(timeout_at).await
                    } else {
                        std::future::pending::<()>().await
                    }
                } => {
                    if self.ping_manager.check_timeout() {
                        error!("WebSocket ping timed out - connection appears dead");

                        let error_response = RithmicResponse {
                            request_id: "websocket_ping_timeout".to_string(),
                            message: RithmicMessage::HeartbeatTimeout,
                            is_update: true,
                            has_more: false,
                            multi_response: false,
                            error: Some("WebSocket ping timeout - connection dead".to_string()),
                            source: self.rithmic_receiver_api.source.clone(),
                        };
                        let _ = self.subscription_sender.send(error_response);
                        break;
                    }
                }
                Some(message) = self.request_receiver.recv() => {
                    self.handle_command(message).await;
                }
                Some(message) = self.rithmic_reader.next() => {
                    let stop = self.handle_rithmic_message(message).await.unwrap();

                    if stop {
                        break;
                    }
                }
                else => { break; }
            }
        }
    }

    async fn handle_rithmic_message(
        &mut self,
        message: Result<Message, Error>,
    ) -> Result<bool, ()> {
        let mut stop = false;

        match message {
            Ok(Message::Close(frame)) => {
                info!("pnl_plant: Received close frame: {:?}", frame);
                stop = true;
            }
            Ok(Message::Pong(_)) => {
                self.ping_manager.received();
            }
            Ok(Message::Binary(data)) => match self.rithmic_receiver_api.buf_to_message(data) {
                Ok(response) => {
                    // Handle heartbeat responses: only forward if they contain an error
                    if matches!(response.message, RithmicMessage::ResponseHeartbeat(_)) {
                        if let Some(error) = response.error {
                            let error_response = RithmicResponse {
                                request_id: response.request_id,
                                message: RithmicMessage::HeartbeatTimeout,
                                is_update: true,
                                has_more: false,
                                multi_response: false,
                                error: Some(error),
                                source: self.rithmic_receiver_api.source.clone(),
                            };

                            let _ = self.subscription_sender.send(error_response);
                        }

                        // Always drop heartbeat responses (successful or error)
                        return Ok(false);
                    }

                    if response.is_update {
                        match self.subscription_sender.send(response) {
                            Ok(_) => {}
                            Err(e) => {
                                warn!("pnl_plant: no active subscribers: {:?}", e);
                            }
                        }
                    } else {
                        self.request_handler.handle_response(response);
                    }
                }
                Err(err_response) => {
                    error!("pnl_plant: error response from server: {:?}", err_response);

                    if err_response.is_update {
                        let _ = self.subscription_sender.send(err_response);
                    } else {
                        self.request_handler.handle_response(err_response);
                    }
                }
            },
            Err(Error::ConnectionClosed) => {
                error!("pnl_plant: connection closed");

                let error_response = RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::ConnectionError,
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: Some("WebSocket connection closed".to_string()),
                    source: self.rithmic_receiver_api.source.clone(),
                };
                let _ = self.subscription_sender.send(error_response);

                stop = true;
            }
            Err(Error::AlreadyClosed) => {
                error!("pnl_plant: connection already closed");

                let error_response = RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::ConnectionError,
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: Some("WebSocket connection already closed".to_string()),
                    source: self.rithmic_receiver_api.source.clone(),
                };
                let _ = self.subscription_sender.send(error_response);

                stop = true;
            }
            Err(Error::Io(ref io_err)) => {
                error!("pnl_plant: I/O error: {}", io_err);

                let error_response = RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::ConnectionError,
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: Some(format!("WebSocket I/O error: {}", io_err)),
                    source: self.rithmic_receiver_api.source.clone(),
                };
                let _ = self.subscription_sender.send(error_response);

                stop = true;
            }
            Err(Error::Protocol(ProtocolError::ResetWithoutClosingHandshake)) => {
                error!("pnl_plant: connection reset without closing handshake");

                let error_response = RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::ConnectionError,
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: Some("WebSocket connection reset without closing handshake".to_string()),
                    source: self.rithmic_receiver_api.source.clone(),
                };
                let _ = self.subscription_sender.send(error_response);

                stop = true;
            }
            Err(Error::Protocol(ProtocolError::SendAfterClosing)) => {
                error!("pnl_plant: attempted to send after closing");

                let error_response = RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::ConnectionError,
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: Some("WebSocket attempted to send after closing".to_string()),
                    source: self.rithmic_receiver_api.source.clone(),
                };
                let _ = self.subscription_sender.send(error_response);

                stop = true;
            }
            Err(Error::Protocol(ProtocolError::ReceivedAfterClosing)) => {
                error!("pnl_plant: received data after closing");

                let error_response = RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::ConnectionError,
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: Some("WebSocket received data after closing".to_string()),
                    source: self.rithmic_receiver_api.source.clone(),
                };
                let _ = self.subscription_sender.send(error_response);

                stop = true;
            }
            _ => {
                warn!("pnl_plant: Unhandled message: {:?}", message);
            }
        }

        Ok(stop)
    }

    async fn handle_command(&mut self, command: PnlPlantCommand) {
        match command {
            PnlPlantCommand::Close => {
                self.rithmic_sender
                    .send(Message::Close(None))
                    .await
                    .unwrap();
            }
            PnlPlantCommand::ListSystemInfo { response_sender } => {
                let (list_system_info_buf, id) =
                    self.rithmic_sender_api.request_rithmic_system_info();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(list_system_info_buf.into()))
                    .await
                    .unwrap();
            }
            PnlPlantCommand::Login { response_sender } => {
                let (login_buf, id) = self.rithmic_sender_api.request_login(
                    &self.config.system_name,
                    SysInfraType::PnlPlant,
                    &self.config.user,
                    &self.config.password,
                );

                info!("pnl_plant: sending login request {}", id);

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(login_buf.into()))
                    .await
                    .unwrap();
            }
            PnlPlantCommand::SetLogin => {
                self.logged_in = true;
            }
            PnlPlantCommand::Logout { response_sender } => {
                let (logout_buf, id) = self.rithmic_sender_api.request_logout();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(logout_buf.into()))
                    .await
                    .unwrap();
            }
            PnlPlantCommand::SendHeartbeat => {
                let (heartbeat_buf, _id) = self.rithmic_sender_api.request_heartbeat();

                let _ = self
                    .rithmic_sender
                    .send(Message::Binary(heartbeat_buf.into()))
                    .await;
            }
            PnlPlantCommand::UpdateHeartbeat { seconds } => {
                self.interval = get_heartbeat_interval(Some(seconds));
            }
            PnlPlantCommand::SubscribePnlUpdates { response_sender } => {
                let (subscribe_buf, id) = self.rithmic_sender_api.request_pnl_position_updates(
                    request_pn_l_position_updates::Request::Subscribe,
                );

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(subscribe_buf.into()))
                    .await
                    .unwrap();
            }
            PnlPlantCommand::PnlPositionSnapshots { response_sender } => {
                let (snapshot_buf, id) = self.rithmic_sender_api.request_pnl_position_snapshot();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(snapshot_buf.into()))
                    .await
                    .unwrap();
            }
            PnlPlantCommand::UnsubscribePnlUpdates { response_sender } => {
                let (unsubscribe_buf, id) = self.rithmic_sender_api.request_pnl_position_updates(
                    request_pn_l_position_updates::Request::Unsubscribe,
                );

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(unsubscribe_buf.into()))
                    .await
                    .unwrap();
            }
        }
    }
}

pub struct RithmicPnlPlantHandle {
    sender: mpsc::Sender<PnlPlantCommand>,
    pub subscription_receiver: broadcast::Receiver<RithmicResponse>,
}

impl RithmicPnlPlantHandle {
    /// List available Rithmic system infrastructure information.
    ///
    /// Returns information about the connected Rithmic system, including
    /// system name, gateway info, and available services.
    pub async fn list_system_info(&self) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = PnlPlantCommand::ListSystemInfo {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx
            .await
            .map_err(|_| "Connection closed".to_string())??
            .remove(0))
    }

    /// Log in to the Rithmic PnL plant
    ///
    /// This must be called before subscribing to any PnL data
    ///
    /// # Returns
    /// The login response or an error message
    pub async fn login(&self) -> Result<RithmicResponse, String> {
        info!("pnl_plant: logging in");

        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = PnlPlantCommand::Login {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;
        let response = rx
            .await
            .map_err(|_| "Connection closed".to_string())??
            .remove(0);

        if let Some(err) = response.error {
            error!("pnl_plant: login failed {:?}", err);
            Err(err)
        } else {
            let _ = self.sender.send(PnlPlantCommand::SetLogin).await;

            if let RithmicMessage::ResponseLogin(resp) = &response.message {
                if let Some(hb) = resp.heartbeat_interval {
                    let secs = hb.max(HEARTBEAT_SECS as f64) as u64;
                    self.update_heartbeat(secs).await;
                }

                if let Some(session_id) = &resp.unique_user_id {
                    info!("pnl_plant: session id: {}", session_id);
                }
            }

            info!("pnl_plant: logged in");

            Ok(response)
        }
    }

    async fn update_heartbeat(&self, seconds: u64) {
        let command = PnlPlantCommand::UpdateHeartbeat { seconds };

        let _ = self.sender.send(command).await;
    }

    /// Disconnect from the Rithmic PnL plant
    ///
    /// # Returns
    /// The logout response or an error message
    pub async fn disconnect(&self) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = PnlPlantCommand::Logout {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;
        let mut r = rx.await.map_err(|_| "Connection closed".to_string())??;
        let _ = self.sender.send(PnlPlantCommand::Close).await;

        Ok(r.remove(0))
    }

    /// Subscribe to PnL updates for all positions
    ///
    /// # Returns
    /// The subscription response or an error message
    pub async fn subscribe_pnl_updates(&self) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = PnlPlantCommand::SubscribePnlUpdates {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx
            .await
            .map_err(|_| "Connection closed".to_string())??
            .remove(0))
    }

    /// Request a snapshot of all current position PnL data
    ///
    /// # Returns
    /// The position snapshot response or an error message
    pub async fn pnl_position_snapshots(&self) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = PnlPlantCommand::PnlPositionSnapshots {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        let response = rx.await;

        if let Ok(Ok(mut v)) = response {
            Ok(v.remove(0))
        } else {
            Err("error with pnl position snapshot payload".to_string())
        }
    }

    /// Unsubscribe from PnL updates
    ///
    /// # Returns
    /// The unsubscription response or an error message
    pub async fn unsubscribe_pnl_updates(&self) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = PnlPlantCommand::UnsubscribePnlUpdates {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx
            .await
            .map_err(|_| "Connection closed".to_string())??
            .remove(0))
    }
}
