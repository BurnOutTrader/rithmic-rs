use prost::{Message, bytes::Bytes};
use std::io::Cursor;
use tracing::error;

use crate::rti::{
    AccountListUpdates, AccountPnLPositionUpdate, AccountRmsUpdates, BestBidOffer, BracketUpdates,
    DepthByOrder, DepthByOrderEndEvent, EndOfDayPrices, ExchangeOrderNotification, ForcedLogout,
    FrontMonthContractUpdate, IndicatorPrices, InstrumentPnLPositionUpdate, LastTrade, MarketMode,
    MessageType, OpenInterest, OrderBook, OrderPriceLimits, QuoteStatistics, Reject,
    ResponseAcceptAgreement, ResponseAccountList, ResponseAccountRmsInfo, ResponseAccountRmsUpdates,
    ResponseAuxilliaryReferenceData, ResponseBracketOrder, ResponseCancelAllOrders,
    ResponseCancelOrder, ResponseDepthByOrderSnapshot, ResponseDepthByOrderUpdates,
    ResponseEasyToBorrowList, ResponseExitPosition, ResponseFrontMonthContract,
    ResponseGetInstrumentByUnderlying, ResponseGetInstrumentByUnderlyingKeys,
    ResponseGetVolumeAtPrice, ResponseGiveTickSizeTypeTable, ResponseHeartbeat, ResponseLinkOrders,
    ResponseListAcceptedAgreements, ResponseListExchangePermissions,
    ResponseListUnacceptedAgreements, ResponseLogin, ResponseLoginInfo, ResponseLogout,
    ResponseMarketDataUpdate, ResponseMarketDataUpdateByUnderlying, ResponseModifyOrder,
    ResponseModifyOrderReferenceData, ResponseNewOrder, ResponseOcoOrder, ResponseOrderSessionConfig,
    ResponsePnLPositionSnapshot, ResponsePnLPositionUpdates, ResponseProductCodes,
    ResponseProductRmsInfo, ResponseReferenceData, ResponseReplayExecutions, ResponseResumeBars,
    ResponseRithmicSystemGatewayInfo, ResponseRithmicSystemInfo, ResponseSearchSymbols,
    ResponseSetRithmicMrktDataSelfCertStatus, ResponseShowAgreement, ResponseShowBracketStops,
    ResponseShowBrackets, ResponseShowOrderHistory, ResponseShowOrderHistoryDates,
    ResponseShowOrderHistoryDetail, ResponseShowOrderHistorySummary, ResponseShowOrders,
    ResponseSubscribeForOrderUpdates, ResponseSubscribeToBracketUpdates, ResponseTickBarReplay,
    ResponseTickBarUpdate, ResponseTimeBarReplay, ResponseTimeBarUpdate, ResponseTradeRoutes,
    ResponseUpdateStopBracketLevel, ResponseUpdateTargetBracketLevel, ResponseVolumeProfileMinuteBars,
    RithmicOrderNotification, SymbolMarginRate, TickBar, TimeBar, TradeRoute, TradeStatistics,
    UpdateEasyToBorrowList, UserAccountUpdate, messages::RithmicMessage,
};

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
/// # use rithmic_rs::api::receiver_api::RithmicResponse;
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
pub struct RithmicResponse {
    pub request_id: String,
    pub message: RithmicMessage,
    pub is_update: bool,
    pub has_more: bool,
    pub multi_response: bool,
    pub error: Option<String>,
    pub source: String,
}

#[derive(Debug)]
pub struct RithmicReceiverApi {
    pub source: String,
}

impl RithmicReceiverApi {
    pub fn buf_to_message(&self, data: Bytes) -> Result<RithmicResponse, RithmicResponse> {
        let parsed_message = match MessageType::decode(&mut Cursor::new(&data[4..])) {
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
                let resp = ResponseLogin::decode(&mut Cursor::new(&data[4..])).unwrap();
                let error = self.get_error(&resp.rp_code);

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
                let resp = ResponseLogout::decode(&mut Cursor::new(&data[4..])).unwrap();
                let error = self.get_error(&resp.rp_code);

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
                let resp = ResponseReferenceData::decode(&mut Cursor::new(&data[4..])).unwrap();
                let error = self.get_error(&resp.rp_code);

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
                let resp = ResponseRithmicSystemInfo::decode(&mut Cursor::new(&data[4..])).unwrap();
                let error = self.get_error(&resp.rp_code);

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
                let resp = ResponseHeartbeat::decode(&mut Cursor::new(&data[4..])).unwrap();
                let error = self.get_error(&resp.rp_code);

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
                let resp =
                    ResponseRithmicSystemGatewayInfo::decode(&mut Cursor::new(&data[4..])).unwrap();
                let error = self.get_error(&resp.rp_code);

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
                let resp = Reject::decode(&mut Cursor::new(&data[4..])).unwrap();
                let error = self.get_error(&resp.rp_code);

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
                let resp = UserAccountUpdate::decode(&mut Cursor::new(&data[4..])).unwrap();

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
                let resp = ForcedLogout::decode(&mut Cursor::new(&data[4..])).unwrap();

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
                let resp = ResponseMarketDataUpdate::decode(&mut Cursor::new(&data[4..])).unwrap();
                let error = self.get_error(&resp.rp_code);

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
                let resp =
                    ResponseGetInstrumentByUnderlying::decode(&mut Cursor::new(&data[4..])).unwrap();
                let has_more = self.has_multiple(&resp.rq_handler_rp_code);
                let error = self.get_error(&resp.rp_code);

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
                let resp = ResponseGetInstrumentByUnderlyingKeys::decode(&mut Cursor::new(&data[4..]))
                    .unwrap();
                let error = self.get_error(&resp.rp_code);

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
                let resp = ResponseMarketDataUpdateByUnderlying::decode(&mut Cursor::new(&data[4..]))
                    .unwrap();
                let error = self.get_error(&resp.rp_code);

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
                let resp =
                    ResponseGiveTickSizeTypeTable::decode(&mut Cursor::new(&data[4..])).unwrap();
                let has_more = self.has_multiple(&resp.rq_handler_rp_code);
                let error = self.get_error(&resp.rp_code);

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
                let resp = ResponseSearchSymbols::decode(&mut Cursor::new(&data[4..])).unwrap();
                let has_more = self.has_multiple(&resp.rq_handler_rp_code);
                let error = self.get_error(&resp.rp_code);

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
                let resp = ResponseProductCodes::decode(&mut Cursor::new(&data[4..])).unwrap();
                let has_more = self.has_multiple(&resp.rq_handler_rp_code);
                let error = self.get_error(&resp.rp_code);

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
                let resp = ResponseFrontMonthContract::decode(&mut Cursor::new(&data[4..])).unwrap();
                let error = self.get_error(&resp.rp_code);

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
                let resp =
                    ResponseDepthByOrderSnapshot::decode(&mut Cursor::new(&data[4..])).unwrap();
                let has_more = self.has_multiple(&resp.rq_handler_rp_code);
                let error = self.get_error(&resp.rp_code);

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
                let resp =
                    ResponseDepthByOrderUpdates::decode(&mut Cursor::new(&data[4..])).unwrap();
                let error = self.get_error(&resp.rp_code);

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
                let resp = ResponseGetVolumeAtPrice::decode(&mut Cursor::new(&data[4..])).unwrap();
                let has_more = self.has_multiple(&resp.rq_handler_rp_code);
                let error = self.get_error(&resp.rp_code);

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
                let resp =
                    ResponseAuxilliaryReferenceData::decode(&mut Cursor::new(&data[4..])).unwrap();
                let error = self.get_error(&resp.rp_code);

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
                let resp = LastTrade::decode(&mut Cursor::new(&data[4..])).unwrap();

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
                let resp = BestBidOffer::decode(&mut Cursor::new(&data[4..])).unwrap();

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
                let resp = TradeStatistics::decode(&mut Cursor::new(&data[4..])).unwrap();

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
                let resp = QuoteStatistics::decode(&mut Cursor::new(&data[4..])).unwrap();

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
                let resp = IndicatorPrices::decode(&mut Cursor::new(&data[4..])).unwrap();

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
                let resp = EndOfDayPrices::decode(&mut Cursor::new(&data[4..])).unwrap();

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
                let resp = OrderBook::decode(&mut Cursor::new(&data[4..])).unwrap();

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
                let resp = MarketMode::decode(&mut Cursor::new(&data[4..])).unwrap();

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
                let resp = OpenInterest::decode(&mut Cursor::new(&data[4..])).unwrap();

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
                let resp = FrontMonthContractUpdate::decode(&mut Cursor::new(&data[4..])).unwrap();

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
                let resp = DepthByOrder::decode(&mut Cursor::new(&data[4..])).unwrap();

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
                let resp = DepthByOrderEndEvent::decode(&mut Cursor::new(&data[4..])).unwrap();

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
                let resp = SymbolMarginRate::decode(&mut Cursor::new(&data[4..])).unwrap();

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
                let resp = OrderPriceLimits::decode(&mut Cursor::new(&data[4..])).unwrap();

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
                let resp = ResponseTimeBarUpdate::decode(&mut Cursor::new(&data[4..])).unwrap();
                let error = self.get_error(&resp.rp_code);

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
                let resp = ResponseTimeBarReplay::decode(&mut Cursor::new(&data[4..])).unwrap();
                let has_more = self.has_multiple(&resp.rq_handler_rp_code);
                let error = self.get_error(&resp.rp_code);

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
                let resp = ResponseTickBarUpdate::decode(&mut Cursor::new(&data[4..])).unwrap();
                let error = self.get_error(&resp.rp_code);

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
                let resp = ResponseTickBarReplay::decode(&mut Cursor::new(&data[4..])).unwrap();
                let has_more = self.has_multiple(&resp.rq_handler_rp_code);
                let error = self.get_error(&resp.rp_code);

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
                let resp =
                    ResponseVolumeProfileMinuteBars::decode(&mut Cursor::new(&data[4..])).unwrap();
                let has_more = self.has_multiple(&resp.rq_handler_rp_code);
                let error = self.get_error(&resp.rp_code);

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
                let resp = ResponseResumeBars::decode(&mut Cursor::new(&data[4..])).unwrap();
                let error = self.get_error(&resp.rp_code);

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
                let resp = TimeBar::decode(&mut Cursor::new(&data[4..])).unwrap();

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
                let resp = TickBar::decode(&mut Cursor::new(&data[4..])).unwrap();

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
                let resp = ResponseLoginInfo::decode(&mut Cursor::new(&data[4..])).unwrap();
                let error = self.get_error(&resp.rp_code);

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
                let resp = ResponseAccountList::decode(&mut Cursor::new(&data[4..])).unwrap();
                let has_more = self.has_multiple(&resp.rq_handler_rp_code);
                let error = self.get_error(&resp.rp_code);

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
                let resp = ResponseAccountRmsInfo::decode(&mut Cursor::new(&data[4..])).unwrap();
                let has_more = self.has_multiple(&resp.rq_handler_rp_code);
                let error = self.get_error(&resp.rp_code);

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
                let resp = ResponseProductRmsInfo::decode(&mut Cursor::new(&data[4..])).unwrap();
                let has_more = self.has_multiple(&resp.rq_handler_rp_code);
                let error = self.get_error(&resp.rp_code);

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
                let resp =
                    ResponseSubscribeForOrderUpdates::decode(&mut Cursor::new(&data[4..])).unwrap();
                let error = self.get_error(&resp.rp_code);

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
                let resp = ResponseTradeRoutes::decode(&mut Cursor::new(&data[4..])).unwrap();
                let has_more = self.has_multiple(&resp.rq_handler_rp_code);
                let error = self.get_error(&resp.rp_code);

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
                let resp = ResponseNewOrder::decode(&mut Cursor::new(&data[4..])).unwrap();
                let has_more = self.has_multiple(&resp.rq_handler_rp_code);
                let error = self.get_error(&resp.rp_code);

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
                let resp = ResponseModifyOrder::decode(&mut Cursor::new(&data[4..])).unwrap();
                let has_more = self.has_multiple(&resp.rq_handler_rp_code);
                let error = self.get_error(&resp.rp_code);

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
                let resp = ResponseCancelOrder::decode(&mut Cursor::new(&data[4..])).unwrap();
                let has_more = self.has_multiple(&resp.rq_handler_rp_code);
                let error = self.get_error(&resp.rp_code);

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
                let resp =
                    ResponseShowOrderHistoryDates::decode(&mut Cursor::new(&data[4..])).unwrap();
                let has_more = self.has_multiple(&resp.rq_handler_rp_code);
                let error = self.get_error(&resp.rp_code);

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
                let resp = ResponseShowOrders::decode(&mut Cursor::new(&data[4..])).unwrap();
                let error = self.get_error(&resp.rp_code);

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
                let resp = ResponseShowOrderHistory::decode(&mut Cursor::new(&data[4..])).unwrap();
                let error = self.get_error(&resp.rp_code);

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
                let resp =
                    ResponseShowOrderHistorySummary::decode(&mut Cursor::new(&data[4..])).unwrap();
                let error = self.get_error(&resp.rp_code);

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
                let resp =
                    ResponseShowOrderHistoryDetail::decode(&mut Cursor::new(&data[4..])).unwrap();
                let error = self.get_error(&resp.rp_code);

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
                let resp = ResponseOcoOrder::decode(&mut Cursor::new(&data[4..])).unwrap();
                let has_more = self.has_multiple(&resp.rq_handler_rp_code);
                let error = self.get_error(&resp.rp_code);

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
                let resp = ResponseBracketOrder::decode(&mut Cursor::new(&data[4..])).unwrap();
                let has_more = self.has_multiple(&resp.rq_handler_rp_code);
                let error = self.get_error(&resp.rp_code);

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
                let resp =
                    ResponseUpdateTargetBracketLevel::decode(&mut Cursor::new(&data[4..])).unwrap();
                let error = self.get_error(&resp.rp_code);

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
                let resp =
                    ResponseUpdateStopBracketLevel::decode(&mut Cursor::new(&data[4..])).unwrap();
                let error = self.get_error(&resp.rp_code);

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
                let resp = ResponseSubscribeToBracketUpdates::decode(&mut Cursor::new(&data[4..]))
                    .unwrap();
                let error = self.get_error(&resp.rp_code);

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
                let resp = ResponseShowBrackets::decode(&mut Cursor::new(&data[4..])).unwrap();
                let has_more = self.has_multiple(&resp.rq_handler_rp_code);
                let err = self.get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseShowBrackets(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error: err,
                    source: self.source.clone(),
                }
            }
            341 => {
                let resp = ResponseShowBracketStops::decode(&mut Cursor::new(&data[4..])).unwrap();
                let has_more = self.has_multiple(&resp.rq_handler_rp_code);
                let err = self.get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseShowBracketStops(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error: err,
                    source: self.source.clone(),
                }
            }
            343 => {
                let resp =
                    ResponseListExchangePermissions::decode(&mut Cursor::new(&data[4..])).unwrap();
                let has_more = self.has_multiple(&resp.rq_handler_rp_code);
                let error = self.get_error(&resp.rp_code);

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
                let resp = ResponseLinkOrders::decode(&mut Cursor::new(&data[4..])).unwrap();
                let error = self.get_error(&resp.rp_code);

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
                let resp = ResponseCancelAllOrders::decode(&mut Cursor::new(&data[4..])).unwrap();
                let err = self.get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseCancelAllOrders(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error: err,
                    source: self.source.clone(),
                }
            }
            349 => {
                let resp = ResponseEasyToBorrowList::decode(&mut Cursor::new(&data[4..])).unwrap();
                let has_more = self.has_multiple(&resp.rq_handler_rp_code);
                let error = self.get_error(&resp.rp_code);

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
                let resp = TradeRoute::decode(&mut Cursor::new(&data[4..])).unwrap();

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
                let resp = RithmicOrderNotification::decode(&mut Cursor::new(&data[4..])).unwrap();

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
                let resp = ExchangeOrderNotification::decode(&mut Cursor::new(&data[4..])).unwrap();

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
                let resp = BracketUpdates::decode(&mut Cursor::new(&data[4..])).unwrap();

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
                let resp = AccountListUpdates::decode(&mut Cursor::new(&data[4..])).unwrap();

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
                let resp = UpdateEasyToBorrowList::decode(&mut Cursor::new(&data[4..])).unwrap();

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
                let resp = AccountRmsUpdates::decode(&mut Cursor::new(&data[4..])).unwrap();

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
                let resp =
                    ResponsePnLPositionUpdates::decode(&mut Cursor::new(&data[4..])).unwrap();
                let err = self.get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponsePnLPositionUpdates(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error: err,
                    source: self.source.clone(),
                }
            }
            403 => {
                let resp =
                    ResponsePnLPositionSnapshot::decode(&mut Cursor::new(&data[4..])).unwrap();
                let err = self.get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponsePnLPositionSnapshot(resp),
                    is_update: false,
                    has_more: false,
                    multi_response: false,
                    error: err,
                    source: self.source.clone(),
                }
            }
            450 => {
                let resp =
                    InstrumentPnLPositionUpdate::decode(&mut Cursor::new(&data[4..])).unwrap();

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
                let resp = AccountPnLPositionUpdate::decode(&mut Cursor::new(&data[4..])).unwrap();

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
                let resp =
                    ResponseListUnacceptedAgreements::decode(&mut Cursor::new(&data[4..])).unwrap();
                let has_more = self.has_multiple(&resp.rq_handler_rp_code);
                let error = self.get_error(&resp.rp_code);

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
                let resp =
                    ResponseListAcceptedAgreements::decode(&mut Cursor::new(&data[4..])).unwrap();
                let has_more = self.has_multiple(&resp.rq_handler_rp_code);
                let error = self.get_error(&resp.rp_code);

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
                let resp = ResponseAcceptAgreement::decode(&mut Cursor::new(&data[4..])).unwrap();
                let error = self.get_error(&resp.rp_code);

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
                let resp = ResponseShowAgreement::decode(&mut Cursor::new(&data[4..])).unwrap();
                let has_more = self.has_multiple(&resp.rq_handler_rp_code);
                let error = self.get_error(&resp.rp_code);

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
                let resp = ResponseSetRithmicMrktDataSelfCertStatus::decode(
                    &mut Cursor::new(&data[4..]),
                )
                .unwrap();
                let error = self.get_error(&resp.rp_code);

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
                let resp =
                    ResponseModifyOrderReferenceData::decode(&mut Cursor::new(&data[4..])).unwrap();
                let error = self.get_error(&resp.rp_code);

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
                let resp =
                    ResponseOrderSessionConfig::decode(&mut Cursor::new(&data[4..])).unwrap();
                let error = self.get_error(&resp.rp_code);

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
                let resp = ResponseExitPosition::decode(&mut Cursor::new(&data[4..])).unwrap();
                let has_more = self.has_multiple(&resp.rq_handler_rp_code);
                let err = self.get_error(&resp.rp_code);

                RithmicResponse {
                    request_id: resp.user_msg.first().cloned().unwrap_or_default(),
                    message: RithmicMessage::ResponseExitPosition(resp),
                    is_update: false,
                    has_more,
                    multi_response: true,
                    error: err,
                    source: self.source.clone(),
                }
            }
            3507 => {
                let resp = ResponseReplayExecutions::decode(&mut Cursor::new(&data[4..])).unwrap();
                let error = self.get_error(&resp.rp_code);

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
                let resp = ResponseAccountRmsUpdates::decode(&mut Cursor::new(&data[4..])).unwrap();
                let error = self.get_error(&resp.rp_code);

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

        // Handle errors
        let err = self.check_message_error(&response);

        if let Some(error) = err {
            error!("receiver_api: error {:#?} {:?}", response, error);

            return Err(response);
        }

        Ok(response)
    }

    fn has_multiple(&self, rq_handler_rp_code: &[String]) -> bool {
        rq_handler_rp_code.len() == 1 && rq_handler_rp_code[0] == "0"
    }

    fn get_error(&self, rp_code: &Vec<String>) -> Option<String> {
        if (rp_code.len() == 1 && rp_code[0] == "0") || (rp_code.is_empty()) {
            None
        } else {
            error!("receiver_api: error {:#?}", rp_code);

            Some(rp_code[1].clone())
        }
    }

    fn check_message_error(&self, message: &RithmicResponse) -> Option<String> {
        message.error.as_ref().map(|e| e.to_string())
    }
}
