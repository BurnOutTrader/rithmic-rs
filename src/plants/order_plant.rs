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
        rithmic_command_types::{
            RithmicBracketOrder, RithmicCancelOrder, RithmicModifyOrder, RithmicOcoOrderLeg,
        },
        sender_api::RithmicSenderApi,
    },
    config::RithmicConfig,
    ping_manager::PingManager,
    request_handler::{RithmicRequest, RithmicRequestHandler},
    rti::{
        messages::RithmicMessage, request_easy_to_borrow_list, request_login::SysInfraType,
        request_new_order,
    },
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
    task::JoinHandle,
    time::{Interval, sleep_until},
};

pub(crate) enum OrderPlantCommand {
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
    // New commands for additional order functionality
    PlaceNewOrder {
        exchange: String,
        symbol: String,
        qty: i32,
        price: f64,
        action: request_new_order::TransactionType,
        ordertype: request_new_order::PriceType,
        localid: String,
        duration: Option<request_new_order::Duration>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    PlaceOcoOrder {
        order1: RithmicOcoOrderLeg,
        order2: RithmicOcoOrderLeg,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    ShowBrackets {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    ShowBracketStops {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    ExitPosition {
        symbol: String,
        exchange: String,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    LinkOrders {
        basket_ids: Vec<String>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    GetEasyToBorrowList {
        request_type: request_easy_to_borrow_list::Request,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    ModifyOrderReferenceData {
        basket_id: String,
        user_tag: String,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    GetOrderSessionConfig {
        should_defer_request: Option<bool>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    ReplayExecutions {
        start_index_sec: i32,
        finish_index_sec: i32,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    SubscribeAccountRmsUpdates {
        subscribe: bool,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    GetLoginInfo {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    // Agreement-related commands
    ListUnacceptedAgreements {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    ListAcceptedAgreements {
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    AcceptAgreement {
        agreement_id: String,
        market_data_usage_capacity: Option<String>,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    ShowAgreement {
        agreement_id: String,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    SetRithmicMrktDataSelfCertStatus {
        agreement_id: String,
        market_data_usage_capacity: String,
        response_sender: oneshot::Sender<Result<Vec<RithmicResponse>, String>>,
    },
    ListExchangePermissions {
        user: String,
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
///     RithmicConfig, RithmicEnv, ConnectStrategy, RithmicOrderPlant, RithmicBracketOrder,
///     BracketTransactionType, BracketDuration, BracketPriceType,
///     rti::messages::RithmicMessage,
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
///         action: BracketTransactionType::Buy,
///         duration: BracketDuration::Day,
///         exchange: "CME".to_string(),
///         localid: "order1".to_string(),
///         price_type: BracketPriceType::Limit,
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

impl RithmicOrderPlant {
    /// Get a handle to interact with the order plant.
    ///
    /// The handle provides methods to place orders, subscribe to updates, and manage positions.
    /// Multiple handles can be created from the same plant.
    pub fn get_handle(&self) -> RithmicOrderPlantHandle {
        RithmicOrderPlantHandle {
            sender: self.sender.clone(),
            subscription_receiver: self.subscription_sender.subscribe(),
        }
    }
}

struct OrderPlant {
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
                    order.price_type,
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
            OrderPlantCommand::PlaceNewOrder {
                exchange,
                symbol,
                qty,
                price,
                action,
                ordertype,
                localid,
                duration,
                response_sender,
            } => {
                let (req_buf, id) = self.rithmic_sender_api.request_new_order(
                    &exchange, &symbol, qty, price, action, ordertype, &localid, duration,
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
            OrderPlantCommand::PlaceOcoOrder {
                order1,
                order2,
                response_sender,
            } => {
                let (req_buf, id) = self.rithmic_sender_api.request_oco_order(order1, order2);

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(req_buf.into()))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::ShowBrackets { response_sender } => {
                let (req_buf, id) = self.rithmic_sender_api.request_show_brackets();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(req_buf.into()))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::ShowBracketStops { response_sender } => {
                let (req_buf, id) = self.rithmic_sender_api.request_show_bracket_stops();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(req_buf.into()))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::ExitPosition {
                symbol,
                exchange,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .rithmic_sender_api
                    .request_exit_position(&symbol, &exchange);

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(req_buf.into()))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::LinkOrders {
                basket_ids,
                response_sender,
            } => {
                let (req_buf, id) = self.rithmic_sender_api.request_link_orders(basket_ids);

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(req_buf.into()))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::GetEasyToBorrowList {
                request_type,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .rithmic_sender_api
                    .request_easy_to_borrow_list(request_type);

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(req_buf.into()))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::ModifyOrderReferenceData {
                basket_id,
                user_tag,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .rithmic_sender_api
                    .request_modify_order_reference_data(&basket_id, &user_tag);

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(req_buf.into()))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::GetOrderSessionConfig {
                should_defer_request,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .rithmic_sender_api
                    .request_order_session_config(should_defer_request);

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(req_buf.into()))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::ReplayExecutions {
                start_index_sec,
                finish_index_sec,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .rithmic_sender_api
                    .request_replay_executions(start_index_sec, finish_index_sec);

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(req_buf.into()))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::SubscribeAccountRmsUpdates {
                subscribe,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .rithmic_sender_api
                    .request_account_rms_updates(subscribe);

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(req_buf.into()))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::GetLoginInfo { response_sender } => {
                let (req_buf, id) = self.rithmic_sender_api.request_login_info();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(req_buf.into()))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::ListUnacceptedAgreements { response_sender } => {
                let (req_buf, id) = self.rithmic_sender_api.request_list_unaccepted_agreements();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(req_buf.into()))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::ListAcceptedAgreements { response_sender } => {
                let (req_buf, id) = self.rithmic_sender_api.request_list_accepted_agreements();

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(req_buf.into()))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::AcceptAgreement {
                agreement_id,
                market_data_usage_capacity,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .rithmic_sender_api
                    .request_accept_agreement(&agreement_id, market_data_usage_capacity.as_deref());

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(req_buf.into()))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::ShowAgreement {
                agreement_id,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .rithmic_sender_api
                    .request_show_agreement(&agreement_id);

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(req_buf.into()))
                    .await
                    .unwrap();
            }
            OrderPlantCommand::SetRithmicMrktDataSelfCertStatus {
                agreement_id,
                market_data_usage_capacity,
                response_sender,
            } => {
                let (req_buf, id) = self
                    .rithmic_sender_api
                    .request_set_rithmic_mrkt_data_self_cert_status(
                        &agreement_id,
                        &market_data_usage_capacity,
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
            OrderPlantCommand::ListExchangePermissions {
                user,
                response_sender,
            } => {
                let (req_buf, id) = self.rithmic_sender_api.request_list_exchanges(&user);

                self.request_handler.register_request(RithmicRequest {
                    request_id: id,
                    responder: response_sender,
                });

                self.rithmic_sender
                    .send(Message::Binary(req_buf.into()))
                    .await
                    .unwrap();
            }
        };
    }
}

pub struct RithmicOrderPlantHandle {
    sender: mpsc::Sender<OrderPlantCommand>,
    pub subscription_receiver: broadcast::Receiver<RithmicResponse>,
}

impl RithmicOrderPlantHandle {
    /// List available Rithmic system infrastructure information.
    ///
    /// Returns information about the connected Rithmic system, including
    /// system name, gateway info, and available services.
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
    /// A vector of account list responses or an error message
    pub async fn get_account_list(&self) -> Result<Vec<RithmicResponse>, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::AccountList {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.unwrap()
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
    /// A vector of order modification responses or an error message
    pub async fn modify_order(
        &self,
        order: RithmicModifyOrder,
    ) -> Result<Vec<RithmicResponse>, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::ModifyOrder {
            order,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.unwrap()
    }

    /// Cancel an order
    ///
    /// # Arguments
    /// * `order` - The cancel order parameters
    ///
    /// # Returns
    /// A vector of cancellation responses or an error message
    pub async fn cancel_order(
        &self,
        order: RithmicCancelOrder,
    ) -> Result<Vec<RithmicResponse>, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::CancelOrder {
            order_id: order.id,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.unwrap()
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
    /// A vector of RMS info responses or an error message
    pub async fn get_account_rms_info(&self) -> Result<Vec<RithmicResponse>, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::GetAccountRmsInfo {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.unwrap()
    }

    /// Get product RMS (Risk Management System) information
    ///
    /// # Returns
    /// A vector of product RMS info responses or an error message
    pub async fn get_product_rms_info(&self) -> Result<Vec<RithmicResponse>, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::GetProductRmsInfo {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.unwrap()
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

    /// Place a new single order (without brackets)
    ///
    /// # Arguments
    /// * `exchange` - The exchange code (e.g., "CME")
    /// * `symbol` - The trading symbol (e.g., "ESM1")
    /// * `qty` - Order quantity
    /// * `price` - Order price (ignored for market orders)
    /// * `action` - Buy or Sell
    /// * `ordertype` - Order type (Limit, Market, Stop, etc.)
    /// * `localid` - User-defined identifier for this order
    /// * `duration` - Optional time in force (Day, GTC, etc.)
    ///
    /// # Returns
    /// A vector of order placement responses or an error message
    pub async fn place_new_order(
        &self,
        exchange: &str,
        symbol: &str,
        qty: i32,
        price: f64,
        action: request_new_order::TransactionType,
        ordertype: request_new_order::PriceType,
        localid: &str,
        duration: Option<request_new_order::Duration>,
    ) -> Result<Vec<RithmicResponse>, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::PlaceNewOrder {
            exchange: exchange.to_string(),
            symbol: symbol.to_string(),
            qty,
            price,
            action,
            ordertype,
            localid: localid.to_string(),
            duration,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.unwrap()
    }

    /// Place an OCO (One Cancels Other) order pair
    ///
    /// When one order is filled, the other is automatically cancelled.
    ///
    /// # Arguments
    /// * `order1` - First order leg
    /// * `order2` - Second order leg
    ///
    /// # Returns
    /// A vector of order placement responses or an error message
    pub async fn place_oco_order(
        &self,
        order1: RithmicOcoOrderLeg,
        order2: RithmicOcoOrderLeg,
    ) -> Result<Vec<RithmicResponse>, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::PlaceOcoOrder {
            order1,
            order2,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.unwrap()
    }

    /// Show all active bracket orders
    ///
    /// # Returns
    /// A vector of responses containing bracket order information or an error message
    pub async fn show_brackets(&self) -> Result<Vec<RithmicResponse>, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::ShowBrackets {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.unwrap()
    }

    /// Show all active bracket stop orders
    ///
    /// # Returns
    /// A vector of responses containing bracket stop information or an error message
    pub async fn show_bracket_stops(&self) -> Result<Vec<RithmicResponse>, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::ShowBracketStops {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.unwrap()
    }

    /// Exit an entire position for a given symbol
    ///
    /// This closes all open positions for the specified symbol/exchange.
    ///
    /// # Arguments
    /// * `symbol` - The trading symbol (e.g., "ESM1")
    /// * `exchange` - The exchange code (e.g., "CME")
    ///
    /// # Returns
    /// A vector of exit position responses or an error message
    pub async fn exit_position(
        &self,
        symbol: &str,
        exchange: &str,
    ) -> Result<Vec<RithmicResponse>, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::ExitPosition {
            symbol: symbol.to_string(),
            exchange: exchange.to_string(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.unwrap()
    }

    /// Link multiple orders together
    ///
    /// When one linked order is cancelled, all linked orders are cancelled.
    ///
    /// # Arguments
    /// * `basket_ids` - Vector of basket IDs to link together
    ///
    /// # Returns
    /// The link orders response or an error message
    pub async fn link_orders(&self, basket_ids: Vec<String>) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::LinkOrders {
            basket_ids,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx.await.unwrap().unwrap().remove(0))
    }

    /// Get the easy-to-borrow list for short selling
    ///
    /// # Arguments
    /// * `request_type` - Subscribe or Unsubscribe from updates
    ///
    /// # Returns
    /// A vector of responses containing easy-to-borrow securities or an error message
    pub async fn get_easy_to_borrow_list(
        &self,
        request_type: request_easy_to_borrow_list::Request,
    ) -> Result<Vec<RithmicResponse>, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::GetEasyToBorrowList {
            request_type,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.unwrap()
    }

    /// Modify order reference data (user tag)
    ///
    /// # Arguments
    /// * `basket_id` - The order/basket identifier
    /// * `user_tag` - New user tag to set on the order
    ///
    /// # Returns
    /// The modification response or an error message
    pub async fn modify_order_reference_data(
        &self,
        basket_id: &str,
        user_tag: &str,
    ) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::ModifyOrderReferenceData {
            basket_id: basket_id.to_string(),
            user_tag: user_tag.to_string(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx.await.unwrap().unwrap().remove(0))
    }

    /// Get or set order session configuration
    ///
    /// # Arguments
    /// * `should_defer_request` - If true, defers requests until server loads reference data
    ///
    /// # Returns
    /// The session config response or an error message
    pub async fn get_order_session_config(
        &self,
        should_defer_request: Option<bool>,
    ) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::GetOrderSessionConfig {
            should_defer_request,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx.await.unwrap().unwrap().remove(0))
    }

    /// Replay historical executions
    ///
    /// # Arguments
    /// * `start_index_sec` - Start time in unix seconds
    /// * `finish_index_sec` - End time in unix seconds
    ///
    /// # Returns
    /// A vector of execution responses or an error message
    pub async fn replay_executions(
        &self,
        start_index_sec: i32,
        finish_index_sec: i32,
    ) -> Result<Vec<RithmicResponse>, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::ReplayExecutions {
            start_index_sec,
            finish_index_sec,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.unwrap()
    }

    /// Subscribe to account RMS updates
    ///
    /// # Arguments
    /// * `subscribe` - true to subscribe, false to unsubscribe
    ///
    /// # Returns
    /// The subscription response or an error message
    pub async fn subscribe_account_rms_updates(
        &self,
        subscribe: bool,
    ) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::SubscribeAccountRmsUpdates {
            subscribe,
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx.await.unwrap().unwrap().remove(0))
    }

    /// Get login information for the current session
    ///
    /// # Returns
    /// The login info response or an error message
    pub async fn get_login_info(&self) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::GetLoginInfo {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx.await.unwrap().unwrap().remove(0))
    }

    /// List unaccepted agreements
    ///
    /// Returns a list of market data agreements that have not yet been accepted.
    ///
    /// # Returns
    /// A vector of unaccepted agreement responses or an error message
    pub async fn list_unaccepted_agreements(&self) -> Result<Vec<RithmicResponse>, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::ListUnacceptedAgreements {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.unwrap()
    }

    /// List accepted agreements
    ///
    /// Returns a list of market data agreements that have been accepted.
    ///
    /// # Returns
    /// A vector of accepted agreement responses or an error message
    pub async fn list_accepted_agreements(&self) -> Result<Vec<RithmicResponse>, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::ListAcceptedAgreements {
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.unwrap()
    }

    /// Accept a market data agreement
    ///
    /// # Arguments
    /// * `agreement_id` - The ID of the agreement to accept
    /// * `market_data_usage_capacity` - Optional capacity indicator (e.g., "Professional", "Non-Professional")
    ///
    /// # Returns
    /// The acceptance response or an error message
    pub async fn accept_agreement(
        &self,
        agreement_id: &str,
        market_data_usage_capacity: Option<&str>,
    ) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::AcceptAgreement {
            agreement_id: agreement_id.to_string(),
            market_data_usage_capacity: market_data_usage_capacity.map(|s| s.to_string()),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx.await.unwrap().unwrap().remove(0))
    }

    /// Show details of an agreement
    ///
    /// # Arguments
    /// * `agreement_id` - The ID of the agreement to display
    ///
    /// # Returns
    /// A vector of agreement details responses or an error message
    pub async fn show_agreement(&self, agreement_id: &str) -> Result<Vec<RithmicResponse>, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::ShowAgreement {
            agreement_id: agreement_id.to_string(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.unwrap()
    }

    /// Set Rithmic market data self-certification status
    ///
    /// # Arguments
    /// * `agreement_id` - The ID of the agreement
    /// * `market_data_usage_capacity` - The usage capacity (e.g., "Professional", "Non-Professional")
    ///
    /// # Returns
    /// The response or an error message
    pub async fn set_rithmic_mrkt_data_self_cert_status(
        &self,
        agreement_id: &str,
        market_data_usage_capacity: &str,
    ) -> Result<RithmicResponse, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::SetRithmicMrktDataSelfCertStatus {
            agreement_id: agreement_id.to_string(),
            market_data_usage_capacity: market_data_usage_capacity.to_string(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        Ok(rx.await.unwrap().unwrap().remove(0))
    }

    /// List exchange permissions for a user
    ///
    /// Returns the exchanges the user has permission to trade on, along with
    /// their entitlement status for each exchange.
    ///
    /// # Arguments
    /// * `user` - The username to query exchange permissions for
    ///
    /// # Returns
    /// A vector of responses containing exchange permission information or an error message
    pub async fn list_exchange_permissions(
        &self,
        user: &str,
    ) -> Result<Vec<RithmicResponse>, String> {
        let (tx, rx) = oneshot::channel::<Result<Vec<RithmicResponse>, String>>();

        let command = OrderPlantCommand::ListExchangePermissions {
            user: user.to_string(),
            response_sender: tx,
        };

        let _ = self.sender.send(command).await;

        rx.await.unwrap()
    }
}
