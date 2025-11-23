use async_trait::async_trait;
use tracing::{error, info, warn};

use tokio_tungstenite::{
    MaybeTlsStream,
    tungstenite::{Error, Message, error::ProtocolError},
};

use crate::{
    ConnectStrategy,
    api::{
        receiver_api::{RithmicReceiverApi, RithmicResponse},
        rithmic_command_types::{RithmicBracketOrder, RithmicCancelOrder, RithmicModifyOrder},
        sender_api::RithmicSenderApi,
    },
    config::RithmicConfig,
    ping_manager::PingManager,
    request_handler::{RithmicRequest, RithmicRequestHandler},
    rti::{messages::RithmicMessage, request_login::SysInfraType},
    ws::{
        HEARTBEAT_SECS, PING_TIMEOUT_SECS, PlantActor, RithmicStream, connect_with_strategy,
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
    task::JoinHandle,
    time::{Interval, sleep_until},
};

pub enum OrderPlantCommand {
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
    SendHeartbeat,
    UpdateHeartbeat {
        seconds: u64,
    },
    AccountList {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    SubscribeOrderUpdates {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    SubscribeBracketUpdates {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    SubscribePnlUpdates {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    PlaceBracketOrder {
        bracket_order: RithmicBracketOrder,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    ModifyOrder {
        order: RithmicModifyOrder,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    ModifyStop {
        order_id: String,
        ticks: i32,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    ModifyProfit {
        order_id: String,
        ticks: i32,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    CancelOrder {
        order_id: String,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    ShowOrders {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    CancelAllOrders {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    GetAccountRmsInfo {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    GetProductRmsInfo {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    GetTradeRoutes {
        subscribe_for_updates: bool,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    ShowOrderHistoryDates {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    ShowOrderHistorySummary {
        date: String,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    ShowOrderHistoryDetail {
        basket_id: String,
        date: String,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    ShowOrderHistory {
        basket_id: Option<String>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
}

/// The RithmicOrderPlant provides functionality to manage trading orders through the Rithmic API.
///
/// It allows applications to:
/// - Place, modify and cancel orders
/// - Work with bracket orders (entry orders with profit targets and stop losses)
/// - Receive real-time order status updates
/// - Track positions and execution reports
///
/// # Connection Health Monitoring
///
/// The subscription receiver provides connection health events:
/// - **WebSocket ping/pong timeouts**: Primary indicator of dead connections (auto-detected)
/// - **Heartbeat errors**: Only forwarded when Rithmic server returns an error (rare)
/// - **Forced logout events**: Server-initiated disconnections requiring reconnection
/// - **Order notifications**: Real-time order fills, cancellations, and status changes
///
/// **Note:** Heartbeat requests are sent automatically for protocol compliance,
/// but successful responses are silently dropped. Only heartbeat errors from the server
/// are forwarded as `HeartbeatTimeout` messages.
///
/// # Example: Basic Usage
///
/// ```no_run
/// use rithmic_rs::{
///     RithmicConfig, RithmicEnv, ConnectStrategy,
///     plants::order_plant::RithmicOrderPlant,
///     api::rithmic_command_types::RithmicBracketOrder,
///     rti::messages::RithmicMessage,
///     ws::RithmicStream,
/// };
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let config = RithmicConfig::from_env(RithmicEnv::Demo)?;
///
///     let order_plant = RithmicOrderPlant::connect(&config, ConnectStrategy::Simple).await?;
///     let mut handle = order_plant.get_handle();
///
///     handle.login().await?;
///     handle.subscribe_order_updates().await?;
///     handle.subscribe_bracket_updates().await?;
///
///     // Place a bracket order
///     let bracket_order = RithmicBracketOrder {
///         action: 1, // Buy
///         duration: 2, // Day
///         exchange: "CME".to_string(),
///         localid: "order1".to_string(),
///         ordertype: 1, // Limit
///         price: Some(4500.00),
///         profit_ticks: 8,
///         qty: 1,
///         stop_ticks: 4,
///         symbol: "ESM1".to_string(),
///     };
///
///     handle.place_bracket_order(bracket_order).await?;
///
///     // Monitor order updates with error handling
///     loop {
///         match handle.subscription_receiver.recv().await {
///             Ok(update) => {
///                 // Check for errors on all messages
///                 if let Some(error) = &update.error {
///                     eprintln!("Error from {}: {}", update.source, error);
///                 }
///
///                 // Handle connection health issues
///                 match update.message {
///                     RithmicMessage::HeartbeatTimeout => {
///                         eprintln!("Connection timeout - reconnection needed");
///                         break;
///                     }
///                     RithmicMessage::ForcedLogout(_) => {
///                         eprintln!("Forced logout - reconnection needed");
///                         break;
///                     }
///                     RithmicMessage::ConnectionError => {
///                         eprintln!("Connection error - reconnection needed");
///                         break;
///                     }
///                     RithmicMessage::RithmicOrderNotification(order) => {
///                         println!("Order notification: {:?}", order);
///                     }
///                     RithmicMessage::ExchangeOrderNotification(order) => {
///                         println!("Exchange notification: {:?}", order);
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
///     handle.disconnect().await?;
///     Ok(())
/// }
/// ```
pub struct RithmicOrderPlant {
    pub connection_handle: JoinHandle<()>,
    sender: mpsc::Sender<OrderPlantCommand>,
    subscription_sender: broadcast::Sender<RithmicResponse>,
}

impl RithmicOrderPlant {
    /// Create a new Order Plant connection
    ///
    /// Create a new Order Plant connection to manage trading orders.
    ///
    /// # Arguments
    /// * `config` - Rithmic configuration
    /// * `strategy` - Connection strategy (Simple, Retry, or AlternateWithRetry)
    ///
    /// # Returns
    /// A `Result` containing the connected `RithmicOrderPlant` instance, or an error if the connection fails.
    ///
    /// # Errors
    /// Returns an error if unable to establish WebSocket connection to the server.
    pub async fn connect(
        config: &RithmicConfig,
        strategy: ConnectStrategy,
    ) -> Result<RithmicOrderPlant, Box<dyn std::error::Error>> {
        let (req_tx, req_rx) = mpsc::channel::<OrderPlantCommand>(64);
        let (sub_tx, _sub_rx) = broadcast::channel(10_000);

        let mut order_plant = OrderPlant::new(req_rx, sub_tx.clone(), config, strategy).await?;

        let connection_handle = tokio::spawn(async move {
            order_plant.run().await;
        });

        Ok(RithmicOrderPlant {
            connection_handle,
            sender: req_tx,
            subscription_sender: sub_tx,
        })
    }
}

impl RithmicStream for RithmicOrderPlant {
    type Handle = RithmicOrderPlantHandle;

    fn get_handle(&self) -> RithmicOrderPlantHandle {
        RithmicOrderPlantHandle {
            sender: self.sender.clone(),
            subscription_receiver: self.subscription_sender.subscribe(),
        }
    }
}

pub struct OrderPlant {
    config: RithmicConfig,
    interval: Interval,
    logged_in: bool,
    ping_interval: Interval,
    ping_manager: PingManager,
    request_handler: RithmicRequestHandler,
    request_receiver: mpsc::Receiver<OrderPlantCommand>,
    rithmic_reader: SplitStream<tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>>,
    rithmic_receiver_api: RithmicReceiverApi,
    rithmic_sender: SplitSink<
        tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>,
        tokio_tungstenite::tungstenite::Message,
    >,
    rithmic_sender_api: RithmicSenderApi,
    subscription_sender: broadcast::Sender<RithmicResponse>,
}

impl OrderPlant {
    pub async fn new(
        request_receiver: mpsc::Receiver<OrderPlantCommand>,
        subscription_sender: broadcast::Sender<RithmicResponse>,
        config: &RithmicConfig,
        strategy: ConnectStrategy,
    ) -> Result<OrderPlant, Box<dyn std::error::Error>> {
        let ws_stream = connect_with_strategy(&config.url, &config.beta_url, strategy).await?;

        let (rithmic_sender, rithmic_reader) = ws_stream.split();

        let rithmic_sender_api = RithmicSenderApi::new(config);
        let rithmic_receiver_api = RithmicReceiverApi {
            source: "order_plant".to_string(),
        };

        let interval = get_heartbeat_interval(None);
        let ping_interval = get_ping_interval(None);
        let ping_manager = PingManager::new(PING_TIMEOUT_SECS);

        Ok(OrderPlant {
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
impl PlantActor for OrderPlant {
    type Command = OrderPlantCommand;

    async fn run(&mut self) {
        loop {
            tokio::select! {
                _ = self.interval.tick() => {
                    if self.logged_in {
                        self.handle_command(OrderPlantCommand::SendHeartbeat).await;
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
        let mut stop: bool = false;

        match message {
            Ok(Message::Close(frame)) => {
                info!("order_plant: Received close frame: {:?}", frame);

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
                                warn!("order_plant: no active subscribers: {:?}", e);
                            }
                        }
                    } else {
                        self.request_handler.handle_response(response);
                    }
                }
                Err(err_response) => {
                    error!(
                        "order_plant: error response from server: {:?}",
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
                error!("order_plant: connection closed");

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
                error!("order_plant: connection already closed");

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
                error!("order_plant: I/O error: {}", io_err);

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
                error!("order_plant: connection reset without closing handshake");

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
                error!("order_plant: attempted to send after closing");

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
                error!("order_plant: received data after closing");

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
                warn!("order_plant: Unhandled message: {:?}", message);
            }
        }

        Ok(stop)
    }

    async fn handle_command(&mut self, command: OrderPlantCommand) {
        match command {
            OrderPlantCommand::Close => {
                self.rithmic_sender
                    .send(Message::Close(None))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::ListSystemInfo { response_sender } => {
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
            OrderPlantCommand::Login { response_sender } => {
                let (login_buf, id) = self.rithmic_sender_api.request_login(
                    &self.config.system_name,
                    SysInfraType::OrderPlant,
                    &self.config.user,
                    &self.config.password,
                );

                info!("order_plant: sending login request {}", id);

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(login_buf.into()))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::SetLogin => {
                self.logged_in = true;
            }
            OrderPlantCommand::Logout { response_sender } => {
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
            OrderPlantCommand::SendHeartbeat => {
                let (heartbeat_buf, _id) = self.rithmic_sender_api.request_heartbeat();

                let _ = self
                    .rithmic_sender
                    .send(Message::Binary(heartbeat_buf.into()))
                    .await;
            }
            OrderPlantCommand::UpdateHeartbeat { seconds } => {
                self.interval = get_heartbeat_interval(Some(seconds));
            }
            OrderPlantCommand::AccountList { response_sender } => {
                let (req_buf, id) = self.rithmic_sender_api.request_account_list();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(req_buf.into()))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::SubscribeOrderUpdates { response_sender } => {
                let (req_buf, id) = self
                    .rithmic_sender_api
                    .request_subscribe_for_order_updates();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(req_buf.into()))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::SubscribeBracketUpdates { response_sender } => {
                let (req_buf, id) = self
                    .rithmic_sender_api
                    .request_subscribe_to_bracket_updates();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(req_buf.into()))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::PlaceBracketOrder {
                bracket_order,
                response_sender,
            } => {
                let (req_buf, id) = self.rithmic_sender_api.request_bracket_order(bracket_order);

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(req_buf.into()))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::ModifyOrder {
                order,
                response_sender,
            } => {
                let (req_buf, id) = self.rithmic_sender_api.request_modify_order(
                    &order.id,
                    &order.exchange,
                    &order.symbol,
                    order.qty,
                    order.price,
                    order.ordertype,
                );

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(req_buf.into()))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::CancelOrder {
                order_id,
                response_sender,
            } => {
                let (req_buf, id) = self.rithmic_sender_api.request_cancel_order(&order_id);

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(req_buf.into()))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::ModifyStop {
                order_id,
                ticks,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .rithmic_sender_api
                    .request_update_stop_bracket_level(&order_id, ticks);

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(req_buf.into()))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::ModifyProfit {
                order_id,
                ticks,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .rithmic_sender_api
                    .request_update_target_bracket_level(&order_id, ticks);

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(req_buf.into()))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::ShowOrders { response_sender } => {
                let (req_buf, id) = self.rithmic_sender_api.request_show_orders();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(req_buf.into()))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::CancelAllOrders { response_sender } => {
                let (req_buf, id) = self.rithmic_sender_api.request_cancel_all_orders();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(req_buf.into()))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::GetAccountRmsInfo { response_sender } => {
                let (req_buf, id) = self.rithmic_sender_api.request_account_rms_info();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(req_buf.into()))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::GetProductRmsInfo { response_sender } => {
                let (req_buf, id) = self.rithmic_sender_api.request_product_rms_info();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(req_buf.into()))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::GetTradeRoutes {
                subscribe_for_updates,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .rithmic_sender_api
                    .request_trade_routes(subscribe_for_updates);

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(req_buf.into()))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::ShowOrderHistoryDates { response_sender } => {
                let (req_buf, id) = self.rithmic_sender_api.request_show_order_history_dates();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(req_buf.into()))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::ShowOrderHistorySummary {
                date,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .rithmic_sender_api
                    .request_show_order_history_summary(&date);

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(req_buf.into()))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::ShowOrderHistoryDetail {
                basket_id,
                date,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .rithmic_sender_api
                    .request_show_order_history_detail(&basket_id, &date);

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(req_buf.into()))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::ShowOrderHistory {
                basket_id,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .rithmic_sender_api
                    .request_show_order_history(basket_id.as_deref());

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(req_buf.into()))
                    .await
                    .unwrap();
            }
            _ => {}
        };
    }
}

pub struct RithmicOrderPlantHandle {
    sender: mpsc::Sender<OrderPlantCommand>,
    pub subscription_receiver: broadcast::Receiver<RithmicResponse>,
}

impl RithmicOrderPlantHandle {
    /// Get the list of available systems
    ///
    /// # Returns
    /// The list of systems response or an error message
    pub async fn list_system_info(&self) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::ListSystemInfo {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx.await.unwrap().unwrap().remove(0))
    }

    /// Log in to the Rithmic Order plant
    ///
    /// This must be called before sending orders or subscriptions
    ///
    /// # Returns
    /// The login response or an error message
    pub async fn login(&self) -> Result<RithmicResponse, String> {
        info!("order_plant: logging in");

        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::Login {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;
        let response = rx.await.unwrap().unwrap().remove(0);

        if response.error.is_none() {
            let _ = self.sender.send(OrderPlantCommand::SetLogin).await;

            if let RithmicMessage::ResponseLogin(resp) = &response.message {
                if let Some(hb) = resp.heartbeat_interval {
                    let secs = hb.max(HEARTBEAT_SECS as f64) as u64;
                    self.update_heartbeat(secs).await;
                }

                if let Some(session_id) = &resp.unique_user_id {
                    info!("order_plant: session id: {}", session_id);
                }
            }

            info!("order_plant: logged in");

            Ok(response)
        } else {
            error!("order_plant: login failed {:?}", response.error);

            Err(response.error.unwrap())
        }
    }

    /// Disconnect from the Rithmic Order plant
    ///
    /// # Returns
    /// The logout response or an error message
    pub async fn disconnect(&self) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::Logout {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;
        let mut r = rx.await.unwrap().unwrap();
        let _ = self.sender.send(OrderPlantCommand::Close).await;

        Ok(r.remove(0))
    }

    /// Get a list of available trading accounts
    ///
    /// # Returns
    /// The account list response or an error message
    pub async fn get_account_list(&self) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::AccountList {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx.await.unwrap().unwrap().remove(0))
    }

    /// Subscribe to order status updates
    ///
    /// # Returns
    /// The subscription response or an error message
    pub async fn subscribe_order_updates(&self) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::SubscribeOrderUpdates {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx.await.unwrap().unwrap().remove(0))
    }

    /// Subscribe to bracket order status updates
    ///
    /// # Returns
    /// The subscription response or an error message
    pub async fn subscribe_bracket_updates(&self) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::SubscribeBracketUpdates {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx.await.unwrap().unwrap().remove(0))
    }

    /// Place a bracket order (entry order with profit target and stop loss)
    ///
    /// # Arguments
    /// * `bracket_order` - The bracket order parameters
    ///
    /// # Returns
    /// The order placement responses or an error message
    pub async fn place_bracket_order(
        &self,
        bracket_order: RithmicBracketOrder,
    ) -> Result<Vec<RithmicResponse>, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::PlaceBracketOrder {
            bracket_order,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.unwrap()
    }

    /// Modify an existing order
    ///
    /// # Arguments
    /// * `order` - The order parameters to modify
    ///
    /// # Returns
    /// The order modification response or an error message
    pub async fn modify_order(&self, order: RithmicModifyOrder) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::ModifyOrder {
            order,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx.await.unwrap().unwrap().remove(0))
    }

    /// Cancel an order
    ///
    /// # Arguments
    /// * `order` - The cancel order parameters
    ///
    /// # Returns
    /// The cancellation response or an error message
    pub async fn cancel_order(&self, order: RithmicCancelOrder) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::CancelOrder {
            order_id: order.id,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx.await.unwrap().unwrap().remove(0))
    }

    /// Adjust the profit target level of a bracket order
    ///
    /// # Arguments
    /// * `id` - The order ID
    /// * `ticks` - Number of ticks to adjust the profit target
    ///
    /// # Returns
    /// The adjustment response or an error message
    pub async fn adjust_profit(&self, id: &str, ticks: i32) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::ModifyProfit {
            order_id: id.to_string(),
            ticks,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx.await.unwrap().unwrap().remove(0))
    }

    /// Adjust the stop loss level of a bracket order
    ///
    /// # Arguments
    /// * `id` - The order ID
    /// * `ticks` - Number of ticks to adjust the stop loss
    ///
    /// # Returns
    /// The adjustment response or an error message
    pub async fn adjust_stop(&self, id: &str, ticks: i32) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::ModifyStop {
            order_id: id.to_string(),
            ticks,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx.await.unwrap().unwrap().remove(0))
    }

    /// Request a list of all open orders
    ///
    /// # Returns
    /// The order list response or an error message
    pub async fn show_orders(&self) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::ShowOrders {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx.await.unwrap().unwrap().remove(0))
    }

    async fn update_heartbeat(&self, seconds: u64) {
        let command = OrderPlantCommand::UpdateHeartbeat { seconds };

        let _ = self.sender.send(command).await;
    }

    /// Cancel all open orders
    ///
    /// # Returns
    /// The cancellation response or an error message
    pub async fn cancel_all_orders(&self) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::CancelAllOrders {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx.await.unwrap().unwrap().remove(0))
    }

    /// Get account RMS (Risk Management System) information
    ///
    /// # Returns
    /// The RMS info response or an error message
    pub async fn get_account_rms_info(&self) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::GetAccountRmsInfo {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx.await.unwrap().unwrap().remove(0))
    }

    /// Get product RMS (Risk Management System) information
    ///
    /// # Returns
    /// The product RMS info response or an error message
    pub async fn get_product_rms_info(&self) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::GetProductRmsInfo {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx.await.unwrap().unwrap().remove(0))
    }

    /// Get available trade routes
    ///
    /// # Arguments
    /// * `subscribe_for_updates` - Whether to receive updates when routes change
    ///
    /// # Returns
    /// The list of trade routes or an error message
    pub async fn get_trade_routes(
        &self,
        subscribe_for_updates: bool,
    ) -> Result<Vec<RithmicResponse>, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::GetTradeRoutes {
            subscribe_for_updates,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.unwrap()
    }

    /// Get dates for which order history is available
    ///
    /// # Returns
    /// The list of available dates or an error message
    pub async fn show_order_history_dates(&self) -> Result<Vec<RithmicResponse>, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::ShowOrderHistoryDates {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.unwrap()
    }

    /// Get order history summary for a specific date
    ///
    /// # Arguments
    /// * `date` - Date in YYYYMMDD format (e.g., "20250122")
    ///
    /// # Returns
    /// The list of order summaries or an error message
    pub async fn show_order_history_summary(
        &self,
        date: &str,
    ) -> Result<Vec<RithmicResponse>, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::ShowOrderHistorySummary {
            date: date.to_string(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.unwrap()
    }

    /// Get detailed order history for a specific order
    ///
    /// # Arguments
    /// * `basket_id` - Order/basket identifier
    /// * `date` - Date in YYYYMMDD format
    ///
    /// # Returns
    /// The detailed order history response or an error message
    pub async fn show_order_history_detail(
        &self,
        basket_id: &str,
        date: &str,
    ) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::ShowOrderHistoryDetail {
            basket_id: basket_id.to_string(),
            date: date.to_string(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx.await.unwrap().unwrap().remove(0))
    }

    /// Get general order history
    ///
    /// # Arguments
    /// * `basket_id` - Optional order/basket identifier filter
    ///
    /// # Returns
    /// The list of order history entries or an error message
    pub async fn show_order_history(
        &self,
        basket_id: Option<&str>,
    ) -> Result<Vec<RithmicResponse>, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::ShowOrderHistory {
            basket_id: basket_id.map(|s| s.to_string()),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.unwrap()
    }
}
