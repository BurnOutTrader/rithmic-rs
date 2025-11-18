use super::{
    AccountPnLPositionUpdate, BestBidOffer, BracketUpdates, DepthByOrder,
    ExchangeOrderNotification, ForcedLogout, InstrumentPnLPositionUpdate, LastTrade, OrderBook,
    Reject, ResponseAccountList, ResponseAccountRmsInfo, ResponseBracketOrder,
    ResponseCancelAllOrders, ResponseCancelOrder, ResponseDepthByOrderSnapshot,
    ResponseDepthByOrderUpdates, ResponseExitPosition, ResponseHeartbeat, ResponseLogin,
    ResponseLogout, ResponseMarketDataUpdate, ResponseModifyOrder, ResponseNewOrder,
    ResponsePnLPositionSnapshot, ResponsePnLPositionUpdates, ResponseProductRmsInfo,
    ResponseRithmicSystemInfo, ResponseSearchSymbols, ResponseShowBracketStops,
    ResponseShowBrackets, ResponseShowOrderHistory, ResponseShowOrderHistoryDates,
    ResponseShowOrderHistoryDetail, ResponseShowOrderHistorySummary, ResponseShowOrders,
    ResponseSubscribeForOrderUpdates, ResponseSubscribeToBracketUpdates, ResponseTickBarReplay,
    ResponseTimeBarReplay, ResponseTradeRoutes, ResponseUpdateStopBracketLevel,
    ResponseUpdateTargetBracketLevel, RithmicOrderNotification, TickBar, TimeBar,
};

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum RithmicMessage {
    AccountPnLPositionUpdate(AccountPnLPositionUpdate),
    BestBidOffer(BestBidOffer),
    BracketUpdates(BracketUpdates),
    DepthByOrder(DepthByOrder),
    ExchangeOrderNotification(ExchangeOrderNotification),
    ForcedLogout(ForcedLogout),
    InstrumentPnLPositionUpdate(InstrumentPnLPositionUpdate),
    LastTrade(LastTrade),
    OrderBook(OrderBook),
    Reject(Reject),
    ResponseAccountList(ResponseAccountList),
    ResponseAccountRmsInfo(ResponseAccountRmsInfo),
    ResponseBracketOrder(ResponseBracketOrder),
    ResponseCancelAllOrders(ResponseCancelAllOrders),
    ResponseCancelOrder(ResponseCancelOrder),
    ResponseDepthByOrderSnapshot(ResponseDepthByOrderSnapshot),
    ResponseDepthByOrderUpdates(ResponseDepthByOrderUpdates),
    ResponseExitPosition(ResponseExitPosition),
    ResponseHeartbeat(ResponseHeartbeat),
    ResponseLogin(ResponseLogin),
    ResponseLogout(ResponseLogout),
    ResponseMarketDataUpdate(ResponseMarketDataUpdate),
    ResponseModifyOrder(ResponseModifyOrder),
    ResponseNewOrder(ResponseNewOrder),
    ResponsePnLPositionSnapshot(ResponsePnLPositionSnapshot),
    ResponsePnLPositionUpdates(ResponsePnLPositionUpdates),
    ResponseProductRmsInfo(ResponseProductRmsInfo),
    ResponseRithmicSystemInfo(ResponseRithmicSystemInfo),
    ResponseSearchSymbols(ResponseSearchSymbols),
    ResponseShowBrackets(ResponseShowBrackets),
    ResponseShowBracketStops(ResponseShowBracketStops),
    ResponseShowOrderHistory(ResponseShowOrderHistory),
    ResponseShowOrderHistoryDates(ResponseShowOrderHistoryDates),
    ResponseShowOrderHistoryDetail(ResponseShowOrderHistoryDetail),
    ResponseShowOrderHistorySummary(ResponseShowOrderHistorySummary),
    ResponseShowOrders(ResponseShowOrders),
    ResponseSubscribeForOrderUpdates(ResponseSubscribeForOrderUpdates),
    ResponseSubscribeToBracketUpdates(ResponseSubscribeToBracketUpdates),
    ResponseTickBarReplay(ResponseTickBarReplay),
    ResponseTimeBarReplay(ResponseTimeBarReplay),
    ResponseTradeRoutes(ResponseTradeRoutes),
    ResponseUpdateStopBracketLevel(ResponseUpdateStopBracketLevel),
    ResponseUpdateTargetBracketLevel(ResponseUpdateTargetBracketLevel),
    RithmicOrderNotification(RithmicOrderNotification),
    TickBar(TickBar),
    TimeBar(TimeBar),

    /// WebSocket connection error.
    ///
    /// This variant is sent when a plant's WebSocket connection experiences a fatal error
    /// and the plant is shutting down. The specific error details are in the `error` field
    /// of the [`RithmicResponse`](crate::api::receiver_api::RithmicResponse).
    ///
    /// ## Error Types Handled
    ///
    /// - **ConnectionClosed**: Normal WebSocket closure
    /// - **AlreadyClosed**: Attempted to use an already-closed connection
    /// - **Io errors**: Network/socket I/O failures (e.g., connection lost, timeout)
    /// - **ResetWithoutClosingHandshake**: Connection reset without proper WebSocket close
    /// - **SendAfterClosing**: Attempted to send data after closing frame sent
    /// - **ReceivedAfterClosing**: Received data after closing frame sent
    ///
    /// ## Handling Connection Errors
    ///
    /// When you receive a `ConnectionError`, the plant has already stopped and its channels
    /// will close. Your application should:
    ///
    /// 1. Check the `error` field for specific error details
    /// 2. Check the `source` field to identify which plant failed
    /// 3. Implement reconnection logic if appropriate
    /// 4. Clean up any state associated with that plant
    ///
    /// ## Example
    ///
    /// ```no_run
    /// use rithmic_rs::rti::messages::RithmicMessage;
    /// # use rithmic_rs::api::receiver_api::RithmicResponse;
    /// # fn example(response: RithmicResponse) {
    /// match response.message {
    ///     RithmicMessage::ConnectionError => {
    ///         eprintln!(
    ///             "Plant {} connection error: {}",
    ///             response.source,
    ///             response.error.unwrap_or_else(|| "Unknown error".to_string())
    ///         );
    ///         // Implement reconnection logic here
    ///         // The plant has stopped and channels will close
    ///     }
    ///     RithmicMessage::ForcedLogout(_) => {
    ///         // Server-initiated logout - different from connection errors
    ///     }
    ///     _ => {}
    /// }
    /// # }
    /// ```
    ///
    /// ## Notes
    ///
    /// - This message always has `is_update: true` and routes to the subscription channel
    /// - The plant has already terminated when you receive this message
    /// - The subscription channel will close shortly after this message
    /// - Unlike [`ForcedLogout`], which is a server-initiated action, `ConnectionError`
    ///   indicates a transport-level failure
    ConnectionError,
    Unknown,
}
