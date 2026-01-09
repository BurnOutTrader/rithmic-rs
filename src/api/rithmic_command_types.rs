use crate::rti::{request_bracket_order, request_modify_order, request_oco_order};

/// One leg of an OCO (One-Cancels-Other) order pair.
///
/// When one leg fills, the other is automatically canceled.
///
/// # Example
///
/// ```ignore
/// use rithmic_rs::{RithmicOcoOrderLeg, OcoTransactionType, OcoDuration, OcoPriceType};
///
/// let take_profit = RithmicOcoOrderLeg {
///     symbol: "ESM5".to_string(),
///     exchange: "CME".to_string(),
///     quantity: 1,
///     price: 5020.0,
///     trigger_price: None,
///     transaction_type: OcoTransactionType::Sell,
///     duration: OcoDuration::Day,
///     price_type: OcoPriceType::Limit,
///     user_tag: "take-profit".to_string(),
/// };
///
/// let stop_loss = RithmicOcoOrderLeg {
///     symbol: "ESM5".to_string(),
///     exchange: "CME".to_string(),
///     quantity: 1,
///     price: 4980.0,
///     trigger_price: Some(4980.0),
///     transaction_type: OcoTransactionType::Sell,
///     duration: OcoDuration::Day,
///     price_type: OcoPriceType::StopMarket,
///     user_tag: "stop-loss".to_string(),
/// };
///
/// handle.place_oco_order(take_profit, stop_loss).await?;
/// ```
#[derive(Debug, Clone)]
pub struct RithmicOcoOrderLeg {
    /// Trading symbol (e.g., "ESM5")
    pub symbol: String,
    /// Exchange code (e.g., "CME")
    pub exchange: String,
    /// Number of contracts
    pub quantity: i32,
    /// Order price
    pub price: f64,
    /// Trigger price for stop orders (None for limit/market)
    pub trigger_price: Option<f64>,
    /// Buy or Sell
    pub transaction_type: request_oco_order::TransactionType,
    /// Order duration
    pub duration: request_oco_order::Duration,
    /// Order type
    pub price_type: request_oco_order::PriceType,
    /// Your identifier for this order
    pub user_tag: String,
}

/// Entry order with linked profit target and stop loss orders.
///
/// When the entry fills, the system creates the profit target and stop loss
/// orders automatically.
///
/// # Example
///
/// ```ignore
/// use rithmic_rs::{RithmicBracketOrder, BracketTransactionType, BracketDuration, BracketPriceType};
///
/// let order = RithmicBracketOrder {
///     symbol: "ESM5".to_string(),
///     exchange: "CME".to_string(),
///     action: BracketTransactionType::Buy,
///     qty: 1,
///     price_type: BracketPriceType::Limit,
///     price: Some(5000.0),
///     duration: BracketDuration::Day,
///     profit_ticks: 20,  // 20 ticks above entry
///     stop_ticks: 10,    // 10 ticks below entry
///     localid: "my-order-1".to_string(),
/// };
/// ```
#[derive(Debug, Clone)]
pub struct RithmicBracketOrder {
    /// Buy or Sell
    pub action: request_bracket_order::TransactionType,
    /// Order duration
    pub duration: request_bracket_order::Duration,
    /// Exchange code (e.g., "CME")
    pub exchange: String,
    /// Your identifier for tracking this order
    pub localid: String,
    /// Order type
    pub price_type: request_bracket_order::PriceType,
    /// Limit price (required for Limit orders)
    pub price: Option<f64>,
    /// Profit target distance in ticks from entry
    pub profit_ticks: i32,
    /// Number of contracts
    pub qty: i32,
    /// Stop loss distance in ticks from entry
    pub stop_ticks: i32,
    /// Trading symbol (e.g., "ESM5")
    pub symbol: String,
}

/// Modify an existing order's price, quantity, or type.
///
/// # Example
///
/// ```ignore
/// use rithmic_rs::{RithmicModifyOrder, ModifyPriceType};
///
/// let modification = RithmicModifyOrder {
///     id: "123456".to_string(),  // basket_id from order notification
///     symbol: "ESM5".to_string(),
///     exchange: "CME".to_string(),
///     qty: 2,
///     price: 5005.0,
///     price_type: ModifyPriceType::Limit,
/// };
/// handle.modify_order(modification).await?;
/// ```
#[derive(Debug, Clone)]
pub struct RithmicModifyOrder {
    /// The `basket_id` from the order notification
    pub id: String,
    /// Exchange code
    pub exchange: String,
    /// Trading symbol
    pub symbol: String,
    /// New quantity
    pub qty: i32,
    /// New price
    pub price: f64,
    /// Order type
    pub price_type: request_modify_order::PriceType,
}

/// Cancel an existing order.
///
/// # Example
///
/// ```ignore
/// let cancel = RithmicCancelOrder {
///     id: "123456".to_string(),  // basket_id from order notification
/// };
/// handle.cancel_order(cancel).await?;
/// ```
#[derive(Debug, Clone)]
pub struct RithmicCancelOrder {
    /// The `basket_id` from the order notification
    pub id: String,
}
