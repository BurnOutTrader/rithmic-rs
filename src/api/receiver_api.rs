use prost::{Message, bytes::Bytes};
use tracing::error;

use crate::rti::{
    AccountListUpdates, AccountPnLPositionUpdate, AccountRmsUpdates, BestBidOffer, BracketUpdates,
    DepthByOrder, DepthByOrderEndEvent, EndOfDayPrices, ExchangeOrderNotification, ForcedLogout,
    FrontMonthContractUpdate, IndicatorPrices, InstrumentPnLPositionUpdate, LastTrade, MarketMode,
    MessageType, OpenInterest, OrderBook, OrderPriceLimits, QuoteStatistics, Reject,
    ResponseAcceptAgreement, ResponseAccountList, ResponseAccountRmsInfo,
    ResponseAccountRmsUpdates, ResponseAuxilliaryReferenceData, ResponseBracketOrder,
    ResponseCancelAllOrders, ResponseCancelOrder, ResponseDepthByOrderSnapshot,
    ResponseDepthByOrderUpdates, ResponseEasyToBorrowList, ResponseExitPosition,
    ResponseFrontMonthContract, ResponseGetInstrumentByUnderlying,
    ResponseGetInstrumentByUnderlyingKeys, ResponseGetVolumeAtPrice, ResponseGiveTickSizeTypeTable,
    ResponseHeartbeat, ResponseLinkOrders, ResponseListAcceptedAgreements,
    ResponseListExchangePermissions, ResponseListUnacceptedAgreements, ResponseLogin,
    ResponseLoginInfo, ResponseLogout, ResponseMarketDataUpdate,
    ResponseMarketDataUpdateByUnderlying, ResponseModifyOrder, ResponseModifyOrderReferenceData,
    ResponseNewOrder, ResponseOcoOrder, ResponseOrderSessionConfig, ResponsePnLPositionSnapshot,
    ResponsePnLPositionUpdates, ResponseProductCodes, ResponseProductRmsInfo,
    ResponseReferenceData, ResponseReplayExecutions, ResponseResumeBars,
    ResponseRithmicSystemGatewayInfo, ResponseRithmicSystemInfo, ResponseSearchSymbols,
    ResponseSetRithmicMrktDataSelfCertStatus, ResponseShowAgreement, ResponseShowBracketStops,
    ResponseShowBrackets, ResponseShowOrderHistory, ResponseShowOrderHistoryDates,
    ResponseShowOrderHistoryDetail, ResponseShowOrderHistorySummary, ResponseShowOrders,
    ResponseSubscribeForOrderUpdates, ResponseSubscribeToBracketUpdates, ResponseTickBarReplay,
    ResponseTickBarUpdate, ResponseTimeBarReplay, ResponseTimeBarUpdate, ResponseTradeRoutes,
    ResponseUpdateStopBracketLevel, ResponseUpdateTargetBracketLevel,
    ResponseVolumeProfileMinuteBars, RithmicOrderNotification, SymbolMarginRate, TickBar, TimeBar,
    TradeRoute, TradeStatistics, UpdateEasyToBorrowList, UserAccountUpdate,
    messages::RithmicMessage,
};
use crate::{RithmicError, RithmicRequestError};

/// Response from a Rithmic plant, either from a request or a subscription update.
///
/// This structure wraps all messages received from Rithmic plants, including both
/// request-response messages and subscription updates (like market data, order updates, etc.).
///
/// ## Fields
///
/// - `request_id`: Unique identifier for matching responses to requests. Empty for updates.
/// - `message`: The actual Rithmic message data (see [`RithmicMessage`])
/// - `is_update`: `true` if this is a subscription update, `false` if it's a request response
/// - `has_more`: `true` if more responses are coming for this request
/// - `multi_response`: `true` if this request type can return multiple responses
/// - `error`: Error message if the operation failed or a connection error occurred
/// - `source`: Name of the plant that sent this response (e.g., "ticker_plant", "order_plant")
///
/// ## Error Handling
///
/// The `error` field is populated in two scenarios:
///
/// ### 1. Rithmic Protocol Errors
/// When Rithmic rejects a request or encounters an error, the response will have:
/// - `error: Some("error description from Rithmic")`
/// - `message`: Usually [`RithmicMessage::Reject`]
///
/// These are request/response outcomes, not transport failures. A populated
/// `error` field by itself does not mean the plant disconnected or needs to
/// reconnect. Use [`RithmicResponse::request_rejection`] together with
/// [`RithmicResponse::rp_code`] / [`RithmicResponse::rp_code_text`] to inspect
/// the raw upstream outcome without treating it as a transport failure.
///
/// ### 2. Connection Errors
/// When a plant's WebSocket connection fails, you'll receive:
/// - `message: RithmicMessage::ConnectionError`
/// - `error: Some("WebSocket error description")`
/// - `is_update: true` (routed to subscription channel)
/// - The plant has stopped and the channel will close
///
/// See [`RithmicMessage::ConnectionError`] for detailed error handling guidance.
///
/// ## Example: Handling Errors
///
/// ```no_run
/// # use rithmic_rs::RithmicResponse;
/// # use rithmic_rs::rti::messages::RithmicMessage;
/// # fn handle_response(response: RithmicResponse) {
/// match response.message {
///     RithmicMessage::ConnectionError => {
///         // WebSocket connection failed
///         eprintln!(
///             "Connection error from {}: {}",
///             response.source,
///             response.error.as_ref().unwrap()
///         );
///         // Implement reconnection logic
///     }
///     RithmicMessage::Reject(reject) => {
///         // Rithmic rejected a request
///         eprintln!(
///             "Request rejected: {}",
///             response.error.as_ref().unwrap_or(&"Unknown".to_string())
///         );
///     }
///     _ => {
///         // Check error field even for successful-looking messages
///         if let Some(err) = response.error {
///             eprintln!("Error in {}: {}", response.source, err);
///         }
///     }
/// }
/// # }
/// ```
#[derive(Debug, Clone)]
#[non_exhaustive]
#[allow(missing_docs)]
pub struct RithmicResponse {
    pub request_id: String,
    pub message: RithmicMessage,
    pub is_update: bool,
    pub has_more: bool,
    pub multi_response: bool,
    pub error: Option<String>,
    pub source: String,
}

impl RithmicResponse {
    /// Returns true if this response represents an error condition.
    ///
    /// This checks both:
    /// - The `error` field being set (Rithmic protocol or business-level errors)
    /// - Connection issues (WebSocket errors, heartbeat timeouts, forced logout)
    ///
    /// # Example
    /// ```ignore
    /// if response.is_error() {
    ///     eprintln!("Error: {:?}", response.error);
    /// }
    /// ```
    pub fn is_error(&self) -> bool {
        self.error.is_some() || self.is_connection_issue()
    }

    /// Returns the raw `rp_code` payload for responses that carry it.
    ///
    /// Most request/response templates expose `rp_code`; update-only messages do not.
    pub fn rp_code_raw(&self) -> Option<&[String]> {
        response_rp_code_info(&self.message).map(|(_, rp_code)| rp_code)
    }

    /// Returns the first `rp_code` element, if present.
    pub fn rp_code(&self) -> Option<&str> {
        self.rp_code_raw()
            .and_then(|rp_code| rp_code.first().map(String::as_str))
    }

    /// Returns the second `rp_code` element, if present.
    pub fn rp_code_text(&self) -> Option<&str> {
        self.rp_code_raw()
            .and_then(|rp_code| rp_code.get(1).map(String::as_str))
    }

    /// Returns the typed non-transport `rp_code` rejection attached to this response.
    ///
    /// This only reflects rejections carried explicitly by `rp_code` and
    /// preserves the full raw payload for downstream handling. It does not treat
    /// transport-health events or generic non-`rp_code` errors as request
    /// rejections.
    pub fn request_rejection(&self) -> Option<RithmicRequestError> {
        if self.is_connection_issue() {
            return None;
        }

        match self.rp_code_raw().map(classify_rp_code) {
            Some(RpCodeClassification::Success | RpCodeClassification::KnownBenignEmpty) => None,
            Some(RpCodeClassification::RequestRejected(err)) => Some(err),
            None => None,
        }
    }

    /// Maps non-transport response failures into typed [`RithmicError`] values.
    ///
    /// `rp_code` rejections surface as [`RithmicError::RequestRejected`].
    /// Generic non-transport response failures without `rp_code` surface as
    /// [`RithmicError::ProtocolError`]. This does not treat transport-health
    /// events as request errors.
    pub fn request_error(&self) -> Option<RithmicError> {
        if let Some(err) = self.request_rejection() {
            return Some(RithmicError::RequestRejected(err));
        }

        if self.is_connection_issue() {
            return None;
        }

        self.error.clone().map(RithmicError::ProtocolError)
    }

    /// Returns true if this response indicates a connection health issue.
    ///
    /// Connection issues include:
    /// - `ConnectionError`: WebSocket connection failed
    /// - `HeartbeatTimeout`: Connection appears dead
    /// - `ForcedLogout`: Server forcibly logged out the client
    ///
    /// These conditions typically require reconnection logic. Generic request
    /// failures encoded in `rp_code` do not.
    ///
    /// # Example
    /// ```ignore
    /// if response.is_connection_issue() {
    ///     // Trigger reconnection
    /// }
    /// ```
    pub fn is_connection_issue(&self) -> bool {
        matches!(
            self.message,
            RithmicMessage::ConnectionError
                | RithmicMessage::HeartbeatTimeout
                | RithmicMessage::ForcedLogout(_)
        )
    }

    /// Returns true if this response contains market data.
    ///
    /// Market data messages include:
    /// - `BestBidOffer`: Top-of-book quotes
    /// - `LastTrade`: Trade executions
    /// - `DepthByOrder`: Order book depth updates
    /// - `DepthByOrderEndEvent`: End of depth snapshot marker
    /// - `OrderBook`: Aggregated order book
    ///
    /// # Example
    /// ```ignore
    /// if response.is_market_data() {
    ///     // Process market data update
    /// }
    /// ```
    pub fn is_market_data(&self) -> bool {
        matches!(
            self.message,
            RithmicMessage::BestBidOffer(_)
                | RithmicMessage::LastTrade(_)
                | RithmicMessage::DepthByOrder(_)
                | RithmicMessage::DepthByOrderEndEvent(_)
                | RithmicMessage::OrderBook(_)
        )
    }

    /// Returns true if this response is an order update notification.
    ///
    /// Order update messages include:
    /// - `RithmicOrderNotification`: Order status updates from Rithmic
    /// - `ExchangeOrderNotification`: Order status updates from exchange
    /// - `BracketUpdates`: Bracket order updates
    ///
    /// # Example
    /// ```ignore
    /// if response.is_order_update() {
    ///     // Process order status change
    /// }
    /// ```
    pub fn is_order_update(&self) -> bool {
        matches!(
            self.message,
            RithmicMessage::RithmicOrderNotification(_)
                | RithmicMessage::ExchangeOrderNotification(_)
                | RithmicMessage::BracketUpdates(_)
        )
    }

    /// Returns true if this response is a P&L or position update.
    ///
    /// P&L update messages include:
    /// - `AccountPnLPositionUpdate`: Account-level P&L updates
    /// - `InstrumentPnLPositionUpdate`: Per-instrument P&L updates
    ///
    /// # Example
    /// ```ignore
    /// if response.is_pnl_update() {
    ///     // Update position tracking
    /// }
    /// ```
    pub fn is_pnl_update(&self) -> bool {
        matches!(
            self.message,
            RithmicMessage::AccountPnLPositionUpdate(_)
                | RithmicMessage::InstrumentPnLPositionUpdate(_)
        )
    }
}

#[derive(Debug)]
pub(crate) struct RithmicReceiverApi {
    pub(crate) source: String,
}

impl RithmicReceiverApi {
    // Large Result size (~1296 bytes) due to RithmicMessage enum, but acceptable since
    // the Result is immediately matched and not passed through deep call stacks.
    #[allow(clippy::result_large_err)]
    pub(crate) fn buf_to_message(&self, data: Bytes) -> Result<RithmicResponse, RithmicResponse> {
        if data.len() < 4 {
            error!("Received message too short: {} bytes", data.len());

            return Err(RithmicResponse {
                request_id: "".to_string(),
                message: RithmicMessage::Unknown,
                is_update: false,
                has_more: false,
                multi_response: false,
                error: Some(format!("Message too short: {} bytes", data.len())),
                source: self.source.clone(),
            });
        }

        let payload = &data[4..];

        let parsed_message = match MessageType::decode(payload) {
            Ok(msg) => msg,
            Err(e) => {
                error!(
                    "Failed to decode MessageType: {} - data_size: {} bytes",
                    e,
                    data.len()
                );
                return Err(RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::Unknown,
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error: Some(format!("Failed to decode message: {}", e)),
                    source: self.source.clone(),
                });
            }
        };

        let response = match parsed_message.template_id {
            11 => {
                let resp = ResponseLogin::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseLogin(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            13 => {
                let resp = ResponseLogout::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseLogout(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            15 => {
                let resp = ResponseReferenceData::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseReferenceData(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            17 => {
                let resp = ResponseRithmicSystemInfo::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseRithmicSystemInfo(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            19 => {
                let resp = ResponseHeartbeat::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, true))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseHeartbeat(resp),
                    is_update: true, // Heartbeats are connection health events - route to subscription channel
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            21 => {
                let resp = ResponseRithmicSystemGatewayInfo::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseRithmicSystemGatewayInfo(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            75 => {
                let resp =
                    Reject::decode(payload).map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::Reject(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            76 => {
                let resp = UserAccountUpdate::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::UserAccountUpdate(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            77 => {
                let resp = ForcedLogout::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::ForcedLogout(resp),
                    is_update: true, // Forced logout is a connection health event - route to subscription channel
                    has_more: false,
                    multi_response: false,
                    error: Some("forced logout from server".to_string()),
                    source: self.source.clone(),
                }
            }
            101 => {
                let resp = ResponseMarketDataUpdate::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseMarketDataUpdate(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            103 => {
                let resp = ResponseGetInstrumentByUnderlying::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseGetInstrumentByUnderlying(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            104 => {
                let resp = ResponseGetInstrumentByUnderlyingKeys::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseGetInstrumentByUnderlyingKeys(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            106 => {
                let resp = ResponseMarketDataUpdateByUnderlying::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseMarketDataUpdateByUnderlying(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            108 => {
                let resp = ResponseGiveTickSizeTypeTable::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseGiveTickSizeTypeTable(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            110 => {
                let resp = ResponseSearchSymbols::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseSearchSymbols(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            112 => {
                let resp = ResponseProductCodes::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseProductCodes(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            114 => {
                let resp = ResponseFrontMonthContract::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseFrontMonthContract(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            116 => {
                let resp = ResponseDepthByOrderSnapshot::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseDepthByOrderSnapshot(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            118 => {
                let resp = ResponseDepthByOrderUpdates::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseDepthByOrderUpdates(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            120 => {
                let resp = ResponseGetVolumeAtPrice::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseGetVolumeAtPrice(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            122 => {
                let resp = ResponseAuxilliaryReferenceData::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseAuxilliaryReferenceData(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            150 => {
                let resp =
                    LastTrade::decode(payload).map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::LastTrade(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            151 => {
                let resp = BestBidOffer::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::BestBidOffer(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            152 => {
                let resp = TradeStatistics::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::TradeStatistics(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            153 => {
                let resp = QuoteStatistics::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::QuoteStatistics(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            154 => {
                let resp = IndicatorPrices::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::IndicatorPrices(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            155 => {
                let resp = EndOfDayPrices::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::EndOfDayPrices(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            156 => {
                let resp =
                    OrderBook::decode(payload).map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::OrderBook(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            157 => {
                let resp =
                    MarketMode::decode(payload).map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::MarketMode(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            158 => {
                let resp = OpenInterest::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::OpenInterest(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            159 => {
                let resp = FrontMonthContractUpdate::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::FrontMonthContractUpdate(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            160 => {
                let resp = DepthByOrder::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::DepthByOrder(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            161 => {
                let resp = DepthByOrderEndEvent::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::DepthByOrderEndEvent(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            162 => {
                let resp = SymbolMarginRate::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::SymbolMarginRate(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            163 => {
                let resp = OrderPriceLimits::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::OrderPriceLimits(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            201 => {
                let resp = ResponseTimeBarUpdate::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseTimeBarUpdate(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            203 => {
                let resp = ResponseTimeBarReplay::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseTimeBarReplay(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            205 => {
                let resp = ResponseTickBarUpdate::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseTickBarUpdate(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            207 => {
                let resp = ResponseTickBarReplay::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseTickBarReplay(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            209 => {
                let resp = ResponseVolumeProfileMinuteBars::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseVolumeProfileMinuteBars(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            211 => {
                let resp = ResponseResumeBars::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseResumeBars(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            250 => {
                let resp =
                    TimeBar::decode(payload).map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::TimeBar(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            251 => {
                let resp =
                    TickBar::decode(payload).map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::TickBar(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            301 => {
                let resp = ResponseLoginInfo::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseLoginInfo(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            303 => {
                let resp = ResponseAccountList::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseAccountList(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            305 => {
                let resp = ResponseAccountRmsInfo::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseAccountRmsInfo(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            307 => {
                let resp = ResponseProductRmsInfo::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseProductRmsInfo(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            309 => {
                let resp = ResponseSubscribeForOrderUpdates::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseSubscribeForOrderUpdates(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            311 => {
                let resp = ResponseTradeRoutes::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseTradeRoutes(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            313 => {
                let resp = ResponseNewOrder::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseNewOrder(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            315 => {
                let resp = ResponseModifyOrder::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseModifyOrder(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            317 => {
                let resp = ResponseCancelOrder::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseCancelOrder(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            319 => {
                let resp = ResponseShowOrderHistoryDates::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseShowOrderHistoryDates(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            321 => {
                let resp = ResponseShowOrders::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseShowOrders(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            323 => {
                let resp = ResponseShowOrderHistory::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseShowOrderHistory(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            325 => {
                let resp = ResponseShowOrderHistorySummary::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseShowOrderHistorySummary(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            327 => {
                let resp = ResponseShowOrderHistoryDetail::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseShowOrderHistoryDetail(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            329 => {
                let resp = ResponseOcoOrder::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseOcoOrder(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            331 => {
                let resp = ResponseBracketOrder::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseBracketOrder(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            333 => {
                let resp = ResponseUpdateTargetBracketLevel::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseUpdateTargetBracketLevel(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            335 => {
                let resp = ResponseUpdateStopBracketLevel::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseUpdateStopBracketLevel(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            337 => {
                let resp = ResponseSubscribeToBracketUpdates::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseSubscribeToBracketUpdates(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            339 => {
                let resp = ResponseShowBrackets::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseShowBrackets(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            341 => {
                let resp = ResponseShowBracketStops::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseShowBracketStops(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            343 => {
                let resp = ResponseListExchangePermissions::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseListExchangePermissions(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            345 => {
                let resp = ResponseLinkOrders::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseLinkOrders(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            347 => {
                let resp = ResponseCancelAllOrders::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseCancelAllOrders(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            349 => {
                let resp = ResponseEasyToBorrowList::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseEasyToBorrowList(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            350 => {
                let resp =
                    TradeRoute::decode(payload).map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::TradeRoute(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            351 => {
                let resp = RithmicOrderNotification::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::RithmicOrderNotification(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            352 => {
                let resp = ExchangeOrderNotification::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::ExchangeOrderNotification(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            353 => {
                let resp = BracketUpdates::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::BracketUpdates(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            354 => {
                let resp = AccountListUpdates::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::AccountListUpdates(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            355 => {
                let resp = UpdateEasyToBorrowList::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::UpdateEasyToBorrowList(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            356 => {
                let resp = AccountRmsUpdates::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::AccountRmsUpdates(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            401 => {
                let resp = ResponsePnLPositionUpdates::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponsePnLPositionUpdates(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            403 => {
                let resp = ResponsePnLPositionSnapshot::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponsePnLPositionSnapshot(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            450 => {
                let resp = InstrumentPnLPositionUpdate::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::InstrumentPnLPositionUpdate(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            451 => {
                let resp = AccountPnLPositionUpdate::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, true))?;

                RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::AccountPnLPositionUpdate(resp),
                    is_update: true,
                    has_more: false,
                    multi_response: false,
                    error: None,
                    source: self.source.clone(),
                }
            }
            501 => {
                let resp = ResponseListUnacceptedAgreements::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseListUnacceptedAgreements(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            503 => {
                let resp = ResponseListAcceptedAgreements::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseListAcceptedAgreements(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            505 => {
                let resp = ResponseAcceptAgreement::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseAcceptAgreement(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            507 => {
                let resp = ResponseShowAgreement::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseShowAgreement(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            509 => {
                let resp = ResponseSetRithmicMrktDataSelfCertStatus::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseSetRithmicMrktDataSelfCertStatus(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            3501 => {
                let resp = ResponseModifyOrderReferenceData::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseModifyOrderReferenceData(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            3503 => {
                let resp = ResponseOrderSessionConfig::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseOrderSessionConfig(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            3505 => {
                let resp = ResponseExitPosition::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let has_more = has_multiple(&resp.rq_handler_rp_code);
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseExitPosition(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error,
                    source: self.source.clone(),
                }
            }
            3507 => {
                let resp = ResponseReplayExecutions::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseReplayExecutions(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            3509 => {
                let resp = ResponseAccountRmsUpdates::decode(payload)
                    .map_err(|e| decode_error(&self.source, e, false))?;
                let error = get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseAccountRmsUpdates(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error,
                    source: self.source.clone(),
                }
            }
            _ => {
                error!(
                    "Unknown message type received - template_id: {}, data_size: {} bytes",
                    parsed_message.template_id,
                    data.len()
                );

                return Err(RithmicResponse {
                    request_id: "".to_string(),
                    message: RithmicMessage::Unknown,
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error: Some(format!(
                        "Unknown message type: template_id={}",
                        parsed_message.template_id
                    )),
                    source: self.source.clone(),
                });
            }
        };

        Ok(response)
    }
}

fn has_multiple(rq_handler_rp_code: &[String]) -> bool {
    // Per the Rithmic guide, the presence of `rq_handler_rp_code` means more
    // response messages follow. `rp_code` marks the terminal frame.
    !rq_handler_rp_code.is_empty()
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RpCodeClassification {
    Success,
    KnownBenignEmpty,
    RequestRejected(RithmicRequestError),
}

impl RpCodeClassification {
    fn error_message(&self) -> Option<String> {
        match self {
            Self::Success | Self::KnownBenignEmpty => None,
            Self::RequestRejected(err) => Some(err.message.clone()),
        }
    }
}

macro_rules! rp_code_response_variants {
    ($macro:ident) => {
        $macro! {
            Reject,
            ResponseAcceptAgreement,
            ResponseAccountList,
            ResponseAccountRmsInfo,
            ResponseAccountRmsUpdates,
            ResponseAuxilliaryReferenceData,
            ResponseBracketOrder,
            ResponseCancelAllOrders,
            ResponseCancelOrder,
            ResponseDepthByOrderSnapshot,
            ResponseDepthByOrderUpdates,
            ResponseEasyToBorrowList,
            ResponseExitPosition,
            ResponseFrontMonthContract,
            ResponseGetInstrumentByUnderlying,
            ResponseGetInstrumentByUnderlyingKeys,
            ResponseGetVolumeAtPrice,
            ResponseGiveTickSizeTypeTable,
            ResponseHeartbeat,
            ResponseLinkOrders,
            ResponseListAcceptedAgreements,
            ResponseListExchangePermissions,
            ResponseListUnacceptedAgreements,
            ResponseLogin,
            ResponseLoginInfo,
            ResponseLogout,
            ResponseMarketDataUpdate,
            ResponseMarketDataUpdateByUnderlying,
            ResponseModifyOrder,
            ResponseModifyOrderReferenceData,
            ResponseNewOrder,
            ResponseOcoOrder,
            ResponseOrderSessionConfig,
            ResponsePnLPositionSnapshot,
            ResponsePnLPositionUpdates,
            ResponseProductCodes,
            ResponseProductRmsInfo,
            ResponseReferenceData,
            ResponseReplayExecutions,
            ResponseResumeBars,
            ResponseRithmicSystemGatewayInfo,
            ResponseRithmicSystemInfo,
            ResponseSearchSymbols,
            ResponseSetRithmicMrktDataSelfCertStatus,
            ResponseShowAgreement,
            ResponseShowBracketStops,
            ResponseShowBrackets,
            ResponseShowOrderHistory,
            ResponseShowOrderHistoryDates,
            ResponseShowOrderHistoryDetail,
            ResponseShowOrderHistorySummary,
            ResponseShowOrders,
            ResponseSubscribeForOrderUpdates,
            ResponseSubscribeToBracketUpdates,
            ResponseTickBarReplay,
            ResponseTickBarUpdate,
            ResponseTimeBarReplay,
            ResponseTimeBarUpdate,
            ResponseTradeRoutes,
            ResponseUpdateStopBracketLevel,
            ResponseUpdateTargetBracketLevel,
            ResponseVolumeProfileMinuteBars,
        }
    };
}

macro_rules! define_response_rp_code_info {
    ($($variant:ident),* $(,)?) => {
        fn response_rp_code_info(message: &RithmicMessage) -> Option<(&'static str, &[String])> {
            match message {
                $(RithmicMessage::$variant(resp) => Some((stringify!($variant), resp.rp_code.as_slice())),)*
                _ => None,
            }
        }
    };
}

rp_code_response_variants!(define_response_rp_code_info);

fn classify_rp_code(rp_code: &[String]) -> RpCodeClassification {
    if (rp_code.len() == 1 && rp_code[0] == "0") || rp_code.is_empty() {
        return RpCodeClassification::Success;
    }

    if let Some(code) = rp_code.first() {
        if let Some(message) = rp_code.get(1) {
            // Rithmic uses `["7", "no data"]` across query/list/search-style
            // responses to mean "successful request, zero rows returned".
            if code == "7" && message.eq_ignore_ascii_case("no data") {
                return RpCodeClassification::KnownBenignEmpty;
            }
        }
    }

    let code = rp_code.first().cloned();
    let message = rp_code
        .get(1)
        .cloned()
        .or_else(|| code.clone())
        .unwrap_or_default();

    RpCodeClassification::RequestRejected(RithmicRequestError {
        rp_code: rp_code.to_vec(),
        code,
        message,
    })
}

fn rp_code_error(rp_code: &[String]) -> Option<String> {
    classify_rp_code(rp_code).error_message()
}

fn get_error(rp_code: &[String]) -> Option<String> {
    rp_code_error(rp_code)
}

fn decode_error(source: &str, e: prost::DecodeError, is_update: bool) -> RithmicResponse {
    error!("Failed to decode protobuf message: {}", e);

    RithmicResponse {
        request_id: "".to_string(),
        message: RithmicMessage::Unknown,
        is_update,
        has_more: false,
        multi_response: false,
        error: Some(format!("Failed to decode message: {}", e)),
        source: source.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to create a test response with a specific message type
    fn make_response(message: RithmicMessage) -> RithmicResponse {
        RithmicResponse {
            request_id: String::new(),
            message,
            is_update: false,
            has_more: false,
            multi_response: false,
            error: None,
            source: "test".to_string(),
        }
    }

    fn make_response_with_error(message: RithmicMessage, error: &str) -> RithmicResponse {
        RithmicResponse {
            error: Some(error.to_string()),
            ..make_response(message)
        }
    }

    fn encode_with_header<T: Message>(message: &T) -> Bytes {
        let mut payload = Vec::new();
        message.encode(&mut payload).unwrap();

        let mut framed = (payload.len() as u32).to_be_bytes().to_vec();
        framed.extend(payload);

        Bytes::from(framed)
    }

    fn decode_with_api<T: Message>(message: &T) -> RithmicResponse {
        let api = RithmicReceiverApi {
            source: "test".to_string(),
        };

        api.buf_to_message(encode_with_header(message)).unwrap()
    }

    // =========================================================================
    // is_error() tests
    // =========================================================================

    #[test]
    fn is_error_true_when_error_field_set() {
        // Even with a normal message, if error field is set, is_error should be true
        let response = make_response_with_error(
            RithmicMessage::ResponseHeartbeat(ResponseHeartbeat::default()),
            "some error",
        );
        assert!(response.is_error());
    }

    #[test]
    fn is_error_true_for_connection_issues_without_error_field() {
        // Connection issues should be errors even without error field set
        let response = make_response(RithmicMessage::ConnectionError);
        assert!(response.is_error());
        assert!(response.error.is_none()); // Verify error field is not set
    }

    #[test]
    fn is_error_false_for_normal_response() {
        let response = make_response(RithmicMessage::ResponseHeartbeat(
            ResponseHeartbeat::default(),
        ));
        assert!(!response.is_error());
    }

    // =========================================================================
    // is_connection_issue() tests
    // =========================================================================

    #[test]
    fn is_connection_issue_detects_all_connection_error_types() {
        // Test all three connection issue types
        let connection_error = make_response(RithmicMessage::ConnectionError);
        let heartbeat_timeout = make_response(RithmicMessage::HeartbeatTimeout);
        let forced_logout = make_response(RithmicMessage::ForcedLogout(ForcedLogout::default()));

        assert!(connection_error.is_connection_issue());
        assert!(heartbeat_timeout.is_connection_issue());
        assert!(forced_logout.is_connection_issue());
    }

    #[test]
    fn is_connection_issue_false_for_reject() {
        // Reject is an error but NOT a connection issue
        let response = make_response(RithmicMessage::Reject(Reject::default()));
        assert!(!response.is_connection_issue());
    }

    #[test]
    fn reject_decodes_as_non_update() {
        let response = decode_with_api(&Reject {
            template_id: 75,
            ..Reject::default()
        });

        assert!(matches!(response.message, RithmicMessage::Reject(_)));
        assert!(!response.is_update);
    }

    #[test]
    fn trade_route_decodes_as_update() {
        let response = decode_with_api(&TradeRoute {
            template_id: 350,
            ..TradeRoute::default()
        });

        assert!(matches!(response.message, RithmicMessage::TradeRoute(_)));
        assert!(response.is_update);
    }

    #[test]
    fn update_easy_to_borrow_list_decodes_as_update() {
        let response = decode_with_api(&UpdateEasyToBorrowList {
            template_id: 355,
            ..UpdateEasyToBorrowList::default()
        });

        assert!(matches!(
            response.message,
            RithmicMessage::UpdateEasyToBorrowList(_)
        ));
        assert!(response.is_update);
    }

    // =========================================================================
    // is_market_data() tests
    // =========================================================================

    #[test]
    fn is_market_data_true_for_market_data_types() {
        let bbo = make_response(RithmicMessage::BestBidOffer(BestBidOffer::default()));
        let trade = make_response(RithmicMessage::LastTrade(LastTrade::default()));
        let depth = make_response(RithmicMessage::DepthByOrder(DepthByOrder::default()));
        let depth_end = make_response(RithmicMessage::DepthByOrderEndEvent(
            DepthByOrderEndEvent::default(),
        ));
        let orderbook = make_response(RithmicMessage::OrderBook(OrderBook::default()));

        assert!(bbo.is_market_data());
        assert!(trade.is_market_data());
        assert!(depth.is_market_data());
        assert!(depth_end.is_market_data());
        assert!(orderbook.is_market_data());
    }

    #[test]
    fn is_market_data_false_for_order_notifications() {
        // Order notifications are NOT market data
        let response = make_response(RithmicMessage::RithmicOrderNotification(
            RithmicOrderNotification::default(),
        ));
        assert!(!response.is_market_data());
    }

    // =========================================================================
    // is_order_update() tests
    // =========================================================================

    #[test]
    fn is_order_update_true_for_order_notification_types() {
        let rithmic_notif = make_response(RithmicMessage::RithmicOrderNotification(
            RithmicOrderNotification::default(),
        ));
        let exchange_notif = make_response(RithmicMessage::ExchangeOrderNotification(
            ExchangeOrderNotification::default(),
        ));
        let bracket = make_response(RithmicMessage::BracketUpdates(BracketUpdates::default()));

        assert!(rithmic_notif.is_order_update());
        assert!(exchange_notif.is_order_update());
        assert!(bracket.is_order_update());
    }

    #[test]
    fn is_order_update_false_for_market_data() {
        // Market data is NOT an order update
        let response = make_response(RithmicMessage::BestBidOffer(BestBidOffer::default()));
        assert!(!response.is_order_update());
    }

    // =========================================================================
    // is_pnl_update() tests
    // =========================================================================

    #[test]
    fn is_pnl_update_true_for_pnl_types() {
        let account_pnl = make_response(RithmicMessage::AccountPnLPositionUpdate(
            AccountPnLPositionUpdate::default(),
        ));
        let instrument_pnl = make_response(RithmicMessage::InstrumentPnLPositionUpdate(
            InstrumentPnLPositionUpdate::default(),
        ));

        assert!(account_pnl.is_pnl_update());
        assert!(instrument_pnl.is_pnl_update());
    }

    #[test]
    fn is_pnl_update_false_for_order_updates() {
        // Order updates are NOT P&L updates
        let response = make_response(RithmicMessage::RithmicOrderNotification(
            RithmicOrderNotification::default(),
        ));
        assert!(!response.is_pnl_update());
    }

    // =========================================================================
    // rp_code classification / has_multiple() unit tests
    // =========================================================================

    #[test]
    fn response_rp_code_info_returns_variant_name_and_payload() {
        let message = RithmicMessage::ResponseSearchSymbols(ResponseSearchSymbols {
            rp_code: vec!["5".to_string(), "permission denied".to_string()],
            ..ResponseSearchSymbols::default()
        });

        let (template_name, rp_code) =
            super::response_rp_code_info(&message).expect("response should expose rp_code");

        assert_eq!(template_name, "ResponseSearchSymbols");
        assert_eq!(rp_code, &["5".to_string(), "permission denied".to_string()]);
    }

    #[test]
    fn get_error_returns_none_for_empty_rp_code() {
        assert_eq!(super::classify_rp_code(&[]), RpCodeClassification::Success);
    }

    #[test]
    fn classify_rp_code_returns_success_for_zero_rp_code() {
        assert_eq!(
            super::classify_rp_code(&["0".to_string()]),
            RpCodeClassification::Success
        );
    }

    #[test]
    fn classify_rp_code_returns_benign_empty_for_code_7() {
        let rp_code = vec!["7".to_string(), "no data".to_string()];
        assert_eq!(
            classify_rp_code(&rp_code),
            RpCodeClassification::KnownBenignEmpty
        );
    }

    #[test]
    fn classify_rp_code_returns_benign_empty_for_code_7_case_insensitive_text() {
        let rp_code = vec!["7".to_string(), "No Data".to_string()];
        assert_eq!(
            classify_rp_code(&rp_code),
            RpCodeClassification::KnownBenignEmpty
        );
    }

    #[test]
    fn classify_rp_code_returns_request_rejected_for_other_code_7_text() {
        let rp_code = vec!["7".to_string(), "permission denied".to_string()];
        assert_eq!(
            classify_rp_code(&rp_code),
            RpCodeClassification::RequestRejected(RithmicRequestError {
                rp_code: rp_code.clone(),
                code: Some("7".to_string()),
                message: "permission denied".to_string(),
            })
        );
    }

    #[test]
    fn classify_rp_code_returns_request_rejected_for_code_6() {
        let rp_code = vec!["6".to_string(), "missing symbol".to_string()];
        assert_eq!(
            classify_rp_code(&rp_code),
            RpCodeClassification::RequestRejected(RithmicRequestError {
                rp_code: rp_code.clone(),
                code: Some("6".to_string()),
                message: "missing symbol".to_string(),
            })
        );
    }

    #[test]
    fn classify_rp_code_returns_request_rejected_for_missing_field_text() {
        let rp_code = vec![
            "1039".to_string(),
            "FCM Id field is not received.".to_string(),
        ];
        assert_eq!(
            classify_rp_code(&rp_code),
            RpCodeClassification::RequestRejected(RithmicRequestError {
                rp_code: rp_code.clone(),
                code: Some("1039".to_string()),
                message: "FCM Id field is not received.".to_string(),
            })
        );
    }

    #[test]
    fn classify_rp_code_returns_request_rejected_for_search_pattern_text() {
        let rp_code = vec![
            "1062".to_string(),
            "Search pattern field is not received.".to_string(),
        ];
        assert_eq!(
            classify_rp_code(&rp_code),
            RpCodeClassification::RequestRejected(RithmicRequestError {
                rp_code: rp_code.clone(),
                code: Some("1062".to_string()),
                message: "Search pattern field is not received.".to_string(),
            })
        );
    }

    #[test]
    fn classify_rp_code_keeps_parse_failure_as_request_rejected() {
        let rp_code = vec![
            "13".to_string(),
            "an error occurred while parsing data.".to_string(),
        ];
        assert_eq!(
            classify_rp_code(&rp_code),
            RpCodeClassification::RequestRejected(RithmicRequestError {
                rp_code: rp_code.clone(),
                code: Some("13".to_string()),
                message: "an error occurred while parsing data.".to_string(),
            })
        );
    }

    #[test]
    fn rp_code_error_returns_none_for_code_7_no_data() {
        assert_eq!(
            super::rp_code_error(&["7".to_string(), "no data".to_string()]),
            None
        );
    }

    #[test]
    fn rp_code_error_returns_some_for_other_code_7_text() {
        assert_eq!(
            super::rp_code_error(&["7".to_string(), "parse failure".to_string()]),
            Some("parse failure".to_string())
        );
    }

    #[test]
    fn rp_code_error_returns_second_element_for_non_zero_code() {
        assert_eq!(
            super::rp_code_error(&["3".to_string(), "bad request".to_string()]),
            Some("bad request".to_string())
        );
    }

    #[test]
    fn rp_code_error_returns_first_element_when_no_second() {
        assert_eq!(
            super::rp_code_error(&["5".to_string()]),
            Some("5".to_string())
        );
    }

    #[test]
    fn has_multiple_returns_true_for_zero_code() {
        assert!(super::has_multiple(&["0".to_string()]));
    }

    #[test]
    fn has_multiple_returns_false_for_empty() {
        assert!(!super::has_multiple(&[]));
    }

    #[test]
    fn has_multiple_returns_true_for_non_zero_code_when_field_present() {
        assert!(super::has_multiple(&["7".to_string()]));
    }

    #[test]
    fn has_multiple_returns_true_for_any_present_payload() {
        assert!(super::has_multiple(&["1".to_string(), "0".to_string()]));
    }

    #[test]
    fn replay_no_data_decodes_as_ok() {
        // rp_code = ["7", "no data"] on a ResponseReplayExecutions should produce Ok,
        // confirming the fix flows end-to-end through buf_to_message.
        use crate::rti::ResponseReplayExecutions;
        let api = RithmicReceiverApi {
            source: "test".to_string(),
        };
        let result = api.buf_to_message(encode_with_header(&ResponseReplayExecutions {
            template_id: 3507,
            user_msg: vec!["req-1".to_string()],
            rp_code: vec!["7".to_string(), "no data".to_string()],
        }));
        assert!(
            result.is_ok(),
            "expected Ok but got Err: {:?}",
            result.err()
        );
        assert_eq!(result.unwrap().error, None);
    }

    #[test]
    fn replay_no_data_decodes_as_ok_case_insensitive() {
        use crate::rti::ResponseReplayExecutions;
        let api = RithmicReceiverApi {
            source: "test".to_string(),
        };
        let result = api.buf_to_message(encode_with_header(&ResponseReplayExecutions {
            template_id: 3507,
            user_msg: vec!["req-1b".to_string()],
            rp_code: vec!["7".to_string(), "NO DATA".to_string()],
        }));
        let response = result.expect("case-insensitive no-data should decode as success");
        assert_eq!(response.error, None);
        assert!(response.request_error().is_none());
    }

    #[test]
    fn search_symbols_decode_sets_multi_response_flags_from_field_presence() {
        let api = RithmicReceiverApi {
            source: "test".to_string(),
        };

        let partial = api
            .buf_to_message(encode_with_header(&ResponseSearchSymbols {
                template_id: 110,
                user_msg: vec!["req-multi".to_string()],
                rq_handler_rp_code: vec!["7".to_string()],
                ..ResponseSearchSymbols::default()
            }))
            .expect("intermediate multi-response frame should decode");

        assert!(matches!(
            partial.message,
            RithmicMessage::ResponseSearchSymbols(_)
        ));
        assert!(partial.multi_response);
        assert!(partial.has_more);
        assert!(partial.error.is_none());

        let terminal = api
            .buf_to_message(encode_with_header(&ResponseSearchSymbols {
                template_id: 110,
                user_msg: vec!["req-multi".to_string()],
                rp_code: vec!["0".to_string()],
                ..ResponseSearchSymbols::default()
            }))
            .expect("terminal multi-response frame should decode");

        assert!(terminal.multi_response);
        assert!(!terminal.has_more);
        assert!(terminal.error.is_none());
    }

    #[test]
    fn reject_with_non_zero_rp_code_decodes_as_ok_with_error() {
        let api = RithmicReceiverApi {
            source: "test".to_string(),
        };
        let result = api.buf_to_message(encode_with_header(&Reject {
            template_id: 75,
            user_msg: vec!["req-2".to_string()],
            rp_code: vec!["5".to_string(), "permission denied".to_string()],
        }));

        let response = result.expect("reject with rp_code error should still decode");
        assert!(matches!(response.message, RithmicMessage::Reject(_)));
        assert_eq!(response.error.as_deref(), Some("permission denied"));
        assert!(!response.is_connection_issue());
    }

    #[test]
    fn accepted_agreements_bad_input_decodes_as_ok_with_error() {
        let api = RithmicReceiverApi {
            source: "test".to_string(),
        };
        let result = api.buf_to_message(encode_with_header(&ResponseListAcceptedAgreements {
            template_id: 503,
            user_msg: vec!["req-3".to_string()],
            rp_code: vec!["6".to_string(), "bad input".to_string()],
            ..ResponseListAcceptedAgreements::default()
        }));

        let response = result.expect("known request/business errors should stay on response path");
        assert!(matches!(
            response.message,
            RithmicMessage::ResponseListAcceptedAgreements(_)
        ));
        assert_eq!(response.error.as_deref(), Some("bad input"));
        assert!(!response.is_connection_issue());
    }

    #[test]
    fn response_request_error_maps_code_6_to_request_rejected_with_full_text() {
        let response = decode_with_api(&ResponseListAcceptedAgreements {
            template_id: 503,
            user_msg: vec!["req-4".to_string()],
            rp_code: vec!["6".to_string(), "agreement already signed".to_string()],
            ..ResponseListAcceptedAgreements::default()
        });

        assert_eq!(response.rp_code(), Some("6"));
        assert_eq!(response.rp_code_text(), Some("agreement already signed"));
        assert!(matches!(
            response.request_error(),
            Some(RithmicError::RequestRejected(RithmicRequestError { rp_code, code, message }))
                if rp_code == vec!["6".to_string(), "agreement already signed".to_string()]
                    && code.as_deref() == Some("6")
                    && message == "agreement already signed"
        ));
    }

    #[test]
    fn response_request_error_maps_missing_field_server_text_to_request_rejected() {
        let response = decode_with_api(&ResponseShowOrders {
            template_id: 321,
            user_msg: vec!["req-4b".to_string()],
            rp_code: vec![
                "1039".to_string(),
                "FCM Id field is not received.".to_string(),
            ],
        });

        assert_eq!(response.rp_code(), Some("1039"));
        assert_eq!(
            response.rp_code_text(),
            Some("FCM Id field is not received.")
        );
        assert!(matches!(
            response.request_error(),
            Some(RithmicError::RequestRejected(RithmicRequestError { rp_code, code, message }))
                if rp_code
                    == vec![
                        "1039".to_string(),
                        "FCM Id field is not received.".to_string()
                    ]
                    && code.as_deref() == Some("1039")
                    && message == "FCM Id field is not received."
        ));
    }

    #[test]
    fn response_request_error_maps_search_pattern_server_text_to_request_rejected() {
        let response = decode_with_api(&ResponseSearchSymbols {
            template_id: 110,
            user_msg: vec!["req-4c".to_string()],
            rp_code: vec![
                "1062".to_string(),
                "Search pattern field is not received.".to_string(),
            ],
            ..ResponseSearchSymbols::default()
        });

        assert_eq!(response.rp_code(), Some("1062"));
        assert_eq!(
            response.rp_code_text(),
            Some("Search pattern field is not received.")
        );
        assert!(matches!(
            response.request_error(),
            Some(RithmicError::RequestRejected(RithmicRequestError { rp_code, code, message }))
                if rp_code
                    == vec![
                        "1062".to_string(),
                        "Search pattern field is not received.".to_string()
                    ]
                    && code.as_deref() == Some("1062")
                    && message == "Search pattern field is not received."
        ));
    }

    #[test]
    fn response_request_error_preserves_raw_text_for_code_7_no_data() {
        let response = decode_with_api(&ResponseReplayExecutions {
            template_id: 3507,
            user_msg: vec!["req-5".to_string()],
            rp_code: vec!["7".to_string(), "no data".to_string()],
        });

        assert_eq!(response.rp_code(), Some("7"));
        assert_eq!(response.rp_code_text(), Some("no data"));
        assert!(response.request_error().is_none());
        assert!(response.request_rejection().is_none());
    }

    #[test]
    fn response_request_error_keeps_non_no_data_code_7_as_request_rejected() {
        use crate::rti::ResponseOrderSessionConfig;

        let response = decode_with_api(&ResponseOrderSessionConfig {
            template_id: 3503,
            user_msg: vec!["req-6".to_string()],
            rp_code: vec![
                "7".to_string(),
                "an error occurred while parsing data.".to_string(),
            ],
        });

        assert_eq!(
            response.rp_code_text(),
            Some("an error occurred while parsing data.")
        );
        assert!(matches!(
            response.request_error(),
            Some(RithmicError::RequestRejected(RithmicRequestError { rp_code, code, message }))
                if rp_code
                    == vec![
                        "7".to_string(),
                        "an error occurred while parsing data.".to_string()
                    ]
                    && code.as_deref() == Some("7")
                    && message == "an error occurred while parsing data."
        ));

        assert_eq!(
            make_response_with_error(RithmicMessage::Unknown, "decode failed").request_rejection(),
            None
        );
    }

    #[test]
    fn response_request_error_maps_non_rp_error_to_protocol_error() {
        let response = make_response_with_error(RithmicMessage::Unknown, "decode failed");

        assert!(matches!(
            response.request_error(),
            Some(RithmicError::ProtocolError(message)) if message == "decode failed"
        ));
    }

    // =========================================================================
    // Mutual exclusivity tests - verify categories don't overlap unexpectedly
    // =========================================================================

    #[test]
    fn categories_are_mutually_exclusive() {
        // Market data should not be flagged as order update or pnl
        let market_data = make_response(RithmicMessage::BestBidOffer(BestBidOffer::default()));
        assert!(market_data.is_market_data());
        assert!(!market_data.is_order_update());
        assert!(!market_data.is_pnl_update());
        assert!(!market_data.is_connection_issue());

        // Order update should not be flagged as market data or pnl
        let order = make_response(RithmicMessage::RithmicOrderNotification(
            RithmicOrderNotification::default(),
        ));
        assert!(order.is_order_update());
        assert!(!order.is_market_data());
        assert!(!order.is_pnl_update());
        assert!(!order.is_connection_issue());

        // PnL should not be flagged as market data or order update
        let pnl = make_response(RithmicMessage::AccountPnLPositionUpdate(
            AccountPnLPositionUpdate::default(),
        ));
        assert!(pnl.is_pnl_update());
        assert!(!pnl.is_market_data());
        assert!(!pnl.is_order_update());
        assert!(!pnl.is_connection_issue());

        // Connection issue should not be in any other category
        let conn_err = make_response(RithmicMessage::ConnectionError);
        assert!(conn_err.is_connection_issue());
        assert!(!conn_err.is_market_data());
        assert!(!conn_err.is_order_update());
        assert!(!conn_err.is_pnl_update());
    }
}
