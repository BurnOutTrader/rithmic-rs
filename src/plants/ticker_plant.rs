use async_trait::async_trait;
use tracing::{error, info, warn};

use crate::{
    ConnectStrategy,
    api::{
        receiver_api::{RithmicReceiverApi, RithmicResponse},
        sender_api::RithmicSenderApi,
    },
    config::RithmicConfig,
    heartbeat_manager::HeartbeatManager,
    request_handler::{RithmicRequest, RithmicRequestHandler},
    rti::{
        messages::RithmicMessage,
        request_depth_by_order_updates,
        request_login::SysInfraType,
        request_market_data_update::{Request, UpdateBits},
    },
    ws::{
        HEARTBEAT_SECS, HEARTBEAT_TIMEOUT_SECS, PlantActor, RithmicStream, connect_with_strategy,
        get_heartbeat_interval,
    },
};

use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};

use tokio_tungstenite::{
    MaybeTlsStream,
    tungstenite::{Error, Message, error::ProtocolError},
};

use tokio::{
    net::TcpStream,
    sync::{broadcast, mpsc, oneshot},
    time::{Interval, sleep_until},
};

pub enum TickerPlantCommand {
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
    SendHeartbeat {
        ignore_response: bool,
    },
    UpdateHeartbeat {
        seconds: u64,
    },
    SetHeartbeatResponseMode {
        expect_response: bool,
    },
    Subscribe {
        symbol: String,
        exchange: String,
        fields: Vec<UpdateBits>,
        request_type: Request,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    SubscribeOrderBook {
        symbol: String,
        exchange: String,
        request_type: request_depth_by_order_updates::Request,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    RequestDepthByOrderSnapshot {
        symbol: String,
        exchange: String,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
}

/// The RithmicTickerPlant provides access to real-time market data.
///
/// Currently the following market data updates are supported:
/// - Last trades
/// - Best bid and offer (BBO)
/// - Order book depth-by-order updates
///
/// # Connection Health Monitoring
///
/// The subscription receiver provides ALL connection health events, including:
/// - **Heartbeat responses**: Monitor connection health by checking for errors
/// - **Forced logout events**: Server-initiated disconnections requiring reconnection
/// - **Market data updates**: Real-time trade and quote data
///
/// All messages are delivered through the `subscription_receiver` channel. Always check
/// the `error` field on responses - particularly heartbeats - to detect connection issues.
///
/// # Example: Basic Usage
///
/// ```no_run
/// use rithmic_rs::{
///     connection_info::AccountInfo,
///     plants::ticker_plant::RithmicTickerPlant,
///     rti::messages::RithmicMessage,
/// };
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     // Load configuration from environment
///     let config = RithmicConfig::from_dotenv(RithmicEnv::Demo)?;
///
///     // Connect to the ticker plant
///     let ticker_plant = RithmicTickerPlant::connect(&config, ConnectStrategy::Simple).await?;
///     let handle = ticker_plant.get_handle();
///
///     // Login to the ticker plant
///     handle.login().await?;
///
///     // Subscribe to market data for a symbol
///     handle.subscribe("ESM1", "CME").await?;
///
///     // Process incoming updates
///     loop {
///         match handle.subscription_receiver.recv().await {
///             Ok(update) => {
///                 // Always check for errors (especially on heartbeats)
///                 if let Some(error) = &update.error {
///                     eprintln!("Error from {}: {}", update.source, error);
///
///                     // Connection health issue - decide whether to reconnect
///                     if matches!(update.message, RithmicMessage::ResponseHeartbeat(_)) {
///                         eprintln!("Heartbeat error - connection may be degraded");
///                         // Implement reconnection logic here
///                         break;
///                     }
///                     continue;
///                 }
///
///                 match update.message {
///                     RithmicMessage::LastTrade(trade) => {
///                         println!("Trade: {} @ {}", trade.size, trade.price);
///                     }
///                     RithmicMessage::BestBidOffer(bbo) => {
///                         println!("BBO: {} @ {} / {} @ {}",
///                             bbo.bid_size, bbo.bid_price,
///                             bbo.ask_price, bbo.ask_size);
///                     }
///                     RithmicMessage::ResponseHeartbeat(_) => {
///                         // Heartbeat OK - connection healthy
///                     }
///                     RithmicMessage::ForcedLogout(logout) => {
///                         eprintln!("Forced logout: {:?}", logout);
///                         break; // Must reconnect
///                     }
///                     _ => {}
///                 }
///             }
///             Err(e) => {
///                 eprintln!("Channel error: {}", e);
///                 break;
///             }
///         }
///     }
///
///     // Cleanup
///     handle.disconnect().await?;
///     Ok(())
/// }
/// ```
///
/// # Example: Robust Connection Health Monitoring
///
/// ```no_run
/// use rithmic_rs::{
///     connection_info::AccountInfo,
///     plants::ticker_plant::RithmicTickerPlant,
///     rti::messages::RithmicMessage,
/// };
/// use tokio::time::{Duration, Instant};
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let config = RithmicConfig::from_dotenv(RithmicEnv::Demo)?;
///
///     let ticker_plant = RithmicTickerPlant::connect(&config, ConnectStrategy::Simple).await?;
///     let handle = ticker_plant.get_handle();
///     handle.login().await?;
///     handle.subscribe("ESM1", "CME").await?;
///
///     let mut last_heartbeat = Instant::now();
///     let heartbeat_timeout = Duration::from_secs(60);
///
///     loop {
///         match handle.subscription_receiver.recv().await {
///             Ok(update) => {
///                 match update.message {
///                     RithmicMessage::ResponseHeartbeat(_) => {
///                         if let Some(error) = &update.error {
///                             eprintln!("Heartbeat error: {} - reconnection may be needed", error);
///                             // Implement your reconnection strategy here
///                         } else {
///                             last_heartbeat = Instant::now();
///                         }
///                     }
///                     RithmicMessage::ForcedLogout(_) => {
///                         eprintln!("Server forced logout - must reconnect");
///                         break;
///                     }
///                     RithmicMessage::LastTrade(_) | RithmicMessage::BestBidOffer(_) => {
///                         // Process market data...
///                     }
///                     _ => {}
///                 }
///
///                 // Check for heartbeat timeout
///                 if last_heartbeat.elapsed() > heartbeat_timeout {
///                     eprintln!("No heartbeat received in {}s - connection may be dead",
///                         heartbeat_timeout.as_secs());
///                     break;
///                 }
///             }
///             Err(e) => {
///                 eprintln!("Subscription channel error: {}", e);
///                 break;
///             }
///         }
///     }
///
///     handle.disconnect().await?;
///     Ok(())
/// }
/// ```
pub struct RithmicTickerPlant {
    pub connection_handle: tokio::task::JoinHandle<()>,
    sender: mpsc::Sender<TickerPlantCommand>,
    subscription_sender: broadcast::Sender<RithmicResponse>,
}

impl RithmicTickerPlant {
    /// Connect to the Rithmic Ticker Plant to access real-time market data.
    ///
    /// # Arguments
    /// * `config` - Rithmic configuration with credentials and server URLs
    /// * `strategy` - Connection strategy (Simple or AlternateWithRetry)
    ///
    /// # Returns
    /// A `Result` containing the connected `RithmicTickerPlant` instance, or an error if the connection fails.
    ///
    /// # Errors
    /// Returns an error if:
    /// - Unable to establish WebSocket connection to the server
    /// - Network timeout occurs
    /// - Server rejects the connection
    ///
    /// # Example
    /// ```no_run
    /// use rithmic_rs::{RithmicConfig, RithmicEnv, RithmicTickerPlant, ConnectStrategy};
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let config = RithmicConfig::from_dotenv(RithmicEnv::Demo)?;
    ///     let ticker_plant = RithmicTickerPlant::connect(&config, ConnectStrategy::Simple).await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn connect(
        config: &RithmicConfig,
        strategy: ConnectStrategy,
    ) -> Result<RithmicTickerPlant, Box<dyn std::error::Error>> {
        let (req_tx, req_rx) = mpsc::channel::<TickerPlantCommand>(64);
        let (sub_tx, _sub_rx) = broadcast::channel(10_000);

        let mut ticker_plant = TickerPlant::new(req_rx, sub_tx.clone(), config, strategy).await?;

        let connection_handle = tokio::spawn(async move {
            ticker_plant.run().await;
        });

        Ok(RithmicTickerPlant {
            connection_handle,
            sender: req_tx,
            subscription_sender: sub_tx,
        })
    }
}

impl RithmicStream for RithmicTickerPlant {
    type Handle = RithmicTickerPlantHandle;

    fn get_handle(&self) -> RithmicTickerPlantHandle {
        RithmicTickerPlantHandle {
            sender: self.sender.clone(),
            subscription_sender: self.subscription_sender.clone(),
            subscription_receiver: self.subscription_sender.subscribe(),
        }
    }
}

#[derive(Debug)]
pub struct TickerPlant {
    config: RithmicConfig,
    interval: Interval,
    logged_in: bool,
    ignore_heartbeat_response: bool,
    heartbeat_manager: HeartbeatManager,
    request_handler: RithmicRequestHandler,
    request_receiver: mpsc::Receiver<TickerPlantCommand>,
    rithmic_reader: SplitStream<tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>>,
    rithmic_receiver_api: RithmicReceiverApi,
    rithmic_sender: SplitSink<
        tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>,
        tokio_tungstenite::tungstenite::Message,
    >,

    rithmic_sender_api: RithmicSenderApi,
    subscription_sender: broadcast::Sender<RithmicResponse>,
}

impl TickerPlant {
    async fn new(
        request_receiver: mpsc::Receiver<TickerPlantCommand>,
        subscription_sender: broadcast::Sender<RithmicResponse>,
        config: &RithmicConfig,
        strategy: ConnectStrategy,
    ) -> Result<TickerPlant, Box<dyn std::error::Error>> {
        let ws_stream = connect_with_strategy(&config.url, &config.beta_url, strategy).await?;

        let (rithmic_sender, rithmic_reader) = ws_stream.split();

        let rithmic_sender_api = RithmicSenderApi::new(config);
        let rithmic_receiver_api = RithmicReceiverApi {
            source: "ticker_plant".to_string(),
        };

        let interval = get_heartbeat_interval(None);
        let heartbeat_manager = HeartbeatManager::new(HEARTBEAT_TIMEOUT_SECS);

        Ok(TickerPlant {
            config: config.clone(),
            interval,
            logged_in: false,
            ignore_heartbeat_response: true,
            heartbeat_manager,
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
impl PlantActor for TickerPlant {
    type Command = TickerPlantCommand;

    /// Execute the ticker plant in its own thread
    /// We will listen for messages from request_receiver and forward them to Rithmic
    /// while also listening for messages from Rithmic and forwarding them to subscription_sender
    /// or request handler
    async fn run(&mut self) {
        loop {
            tokio::select! {
                _ = self.interval.tick() => {
                    if self.logged_in {
                        self.handle_command(TickerPlantCommand::SendHeartbeat { ignore_response: self.ignore_heartbeat_response }).await;
                    }
                }
                _ = async {
                    if let Some(timeout_at) = self.heartbeat_manager.next_timeout_at() {
                        sleep_until(timeout_at).await
                    } else {
                        std::future::pending::<()>().await
                    }
                } => {
                    if let Some(request_id) = self.heartbeat_manager.check_timeout() {
                        error!("Heartbeat {} timed out", request_id);

                        let error_response = RithmicResponse {
                            request_id,
                            message: RithmicMessage::HeartbeatTimeout,
                            is_update: true,
                            has_more: false,
                            multi_response: false,
                            error: Some("Heartbeat response timeout".to_string()),
                            source: self.rithmic_receiver_api.source.clone(),
                        };
                        let _ = self.subscription_sender.send(error_response);
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
                else => { break }
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
                info!("ticker_plant received close frame: {:?}", frame);

                stop = true;
            }
            Ok(Message::Binary(data)) => match self.rithmic_receiver_api.buf_to_message(data) {
                Ok(response) => {
                    if matches!(response.message, RithmicMessage::ResponseHeartbeat(_)) {
                        self.heartbeat_manager.received(&response.request_id);
                    }

                    if response.is_update {
                        match self.subscription_sender.send(response) {
                            Ok(_) => {}
                            Err(e) => {
                                warn!("ticker_plant: no active subscribers: {:?}", e);
                            }
                        }
                    } else {
                        self.request_handler.handle_response(response);
                    }
                }
                Err(err_response) => {
                    error!(
                        "ticker_plant: error response from server: {:?}",
                        err_response
                    );

                    if err_response.is_update {
                        let _ = self.subscription_sender.send(err_response);
                    } else {
                        self.request_handler.handle_response(err_response);
                    }
                }
            },
            Err(Error::ConnectionClosed) => {
                error!("ticker_plant: connection closed");

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
                error!("ticker_plant: connection already closed");

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
                error!("ticker_plant: I/O error: {}", io_err);

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
                error!("ticker_plant: connection reset without closing handshake");

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
                error!("ticker_plant: attempted to send after closing");

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
                error!("ticker_plant: received data after closing");

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
                warn!("ticker_plant received unknown message {:?}", message);
            }
        }

        Ok(stop)
    }

    async fn handle_command(&mut self, command: TickerPlantCommand) {
        match command {
            TickerPlantCommand::Close => {
                self.rithmic_sender
                    .send(Message::Close(None))
                    .await
                    .unwrap();
            }
            TickerPlantCommand::ListSystemInfo { response_sender } => {
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
            TickerPlantCommand::Login { response_sender } => {
                let (login_buf, id) = self.rithmic_sender_api.request_login(
                    &self.config.system_name,
                    SysInfraType::TickerPlant,
                    &self.config.user,
                    &self.config.password,
                );

                info!("ticker_plant: sending login request {}", id);

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(login_buf.into()))
                    .await
                    .unwrap();
            }
            TickerPlantCommand::SetLogin => {
                self.logged_in = true;
            }
            TickerPlantCommand::Logout { response_sender } => {
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
            TickerPlantCommand::SendHeartbeat { ignore_response } => {
                let (heartbeat_buf, id) = self.rithmic_sender_api.request_heartbeat();

                if !ignore_response {
                    self.heartbeat_manager.sent(id.clone());
                }

                let _ = self
                    .rithmic_sender
                    .send(Message::Binary(heartbeat_buf.into()))
                    .await;
            }
            TickerPlantCommand::UpdateHeartbeat { seconds } => {
                self.interval = get_heartbeat_interval(Some(seconds));
            }
            TickerPlantCommand::SetHeartbeatResponseMode { expect_response } => {
                self.ignore_heartbeat_response = !expect_response;
            }
            TickerPlantCommand::Subscribe {
                symbol,
                exchange,
                fields,
                request_type,
                response_sender,
            } => {
                let (sub_buf, id) = self.rithmic_sender_api.request_market_data_update(
                    &symbol,
                    &exchange,
                    fields,
                    request_type,
                );

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(sub_buf.into()))
                    .await
                    .unwrap();
            }
            TickerPlantCommand::SubscribeOrderBook {
                symbol,
                exchange,
                request_type,
                response_sender,
            } => {
                let (sub_buf, id) = self.rithmic_sender_api.request_depth_by_order_update(
                    &symbol,
                    &exchange,
                    request_type,
                );

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(sub_buf.into()))
                    .await
                    .unwrap();
            }
            TickerPlantCommand::RequestDepthByOrderSnapshot {
                symbol,
                exchange,
                response_sender,
            } => {
                let (snapshot_buf, id) = self
                    .rithmic_sender_api
                    .request_depth_by_order_snapshot(&symbol, &exchange);

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(snapshot_buf.into()))
                    .await
                    .unwrap();
            }
        }
    }
}

pub struct RithmicTickerPlantHandle {
    sender: mpsc::Sender<TickerPlantCommand>,
    subscription_sender: broadcast::Sender<RithmicResponse>,

    /// Receiver for subscription updates
    pub subscription_receiver: broadcast::Receiver<RithmicResponse>,
}

impl RithmicTickerPlantHandle {
    pub async fn list_system_info(&self) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = TickerPlantCommand::ListSystemInfo {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;
        let response = rx.await.unwrap().unwrap().remove(0);

        Ok(response)
    }

    /// Log in to the Rithmic ticker plant
    ///
    /// This must be called before subscribing to any market data
    ///
    /// # Returns
    /// The login response or an error message
    pub async fn login(&self) -> Result<RithmicResponse, String> {
        info!("ticker_plant: logging in");

        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = TickerPlantCommand::Login {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;
        let response = rx.await.unwrap().unwrap().remove(0);

        if response.error.is_none() {
            let _ = self.sender.send(TickerPlantCommand::SetLogin).await;

            if let RithmicMessage::ResponseLogin(resp) = &response.message {
                if let Some(hb) = resp.heartbeat_interval {
                    let secs = hb.max(HEARTBEAT_SECS as f64) as u64;
                    self.update_heartbeat(secs).await;
                }

                if let Some(session_id) = &resp.unique_user_id {
                    info!("ticker_plant: session id: {}", session_id);
                }
            }

            info!("ticker_plant: logged in");

            Ok(response)
        } else {
            error!("ticker_plant: login failed {:?}", response.error);

            Err(response.error.unwrap())
        }
    }

    /// Disconnect from the Rithmic ticker plant
    ///
    /// # Returns
    /// The logout response or an error message
    pub async fn disconnect(&self) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = TickerPlantCommand::Logout {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;
        let mut r = rx.await.unwrap().unwrap();
        let _ = self.sender.send(TickerPlantCommand::Close).await;
        let response = r.remove(0);

        self.subscription_sender.send(response.clone()).unwrap();

        Ok(response)
    }

    /// Subscribe to market data for a specific symbol
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESM1")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The subscription response or an error message
    pub async fn subscribe(&self, symbol: &str, exchange: &str) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = TickerPlantCommand::Subscribe {
            symbol: symbol.to_string(),
            exchange: exchange.to_string(),
            fields: vec![UpdateBits::LastTrade, UpdateBits::Bbo],
            request_type: Request::Subscribe,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx.await.unwrap().unwrap().remove(0))
    }

    /// Subscribe to order book depth-by-order updates for a specific symbol
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESM1")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// The subscription response or an error message
    pub async fn subscribe_order_book(
        &self,
        symbol: &str,
        exchange: &str,
    ) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = TickerPlantCommand::SubscribeOrderBook {
            symbol: symbol.to_string(),
            exchange: exchange.to_string(),
            request_type: request_depth_by_order_updates::Request::Subscribe,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx.await.unwrap().unwrap().remove(0))
    }

    /// Request a snapshot of the order book depth-by-order for a specific symbol
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESM1")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// A vector of responses containing the order book snapshot data or an error message
    pub async fn request_depth_by_order_snapshot(
        &self,
        symbol: &str,
        exchange: &str,
    ) -> Result<Vec<RithmicResponse>, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = TickerPlantCommand::RequestDepthByOrderSnapshot {
            symbol: symbol.to_string(),
            exchange: exchange.to_string(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.unwrap()
    }

    async fn update_heartbeat(&self, seconds: u64) {
        let command = TickerPlantCommand::UpdateHeartbeat { seconds };

        let _ = self.sender.send(command).await;
    }

    /// Set whether heartbeat responses should be returned
    ///
    /// # Arguments
    /// * `expect_response` - If true, heartbeat responses will be handled. If false, they will be ignored.
    ///
    /// # Example
    /// ```no_run
    /// // During trading hours, expect heartbeat responses
    /// handle.return_heartbeat_response(true).await;
    ///
    /// // Outside trading hours, don't expect responses
    /// handle.return_heartbeat_response(false).await;
    /// ```
    pub async fn return_heartbeat_response(&self, expect_response: bool) {
        let command = TickerPlantCommand::SetHeartbeatResponseMode { expect_response };

        let _ = self.sender.send(command).await;
    }
}

impl Clone for RithmicTickerPlantHandle {
    fn clone(&self) -> Self {
        RithmicTickerPlantHandle {
            sender: self.sender.clone(),
            subscription_receiver: self.subscription_sender.subscribe(),
            subscription_sender: self.subscription_sender.clone(),
        }
    }
}
