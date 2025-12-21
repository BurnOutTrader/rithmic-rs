use crate::rti::request_oco_order;

/// Represents a single leg of an OCO (One Cancels Other) order pair
#[derive(Debug, Clone)]
pub struct RithmicOcoOrderLeg {
    pub symbol: String,
    pub exchange: String,
    pub quantity: i32,
    pub price: f64,
    /// Trigger price for stop orders (None for Limit/Market orders)
    pub trigger_price: Option<f64>,
    pub transaction_type: request_oco_order::TransactionType,
    pub duration: request_oco_order::Duration,
    pub price_type: request_oco_order::PriceType,
    pub user_tag: String,
}

#[derive(Debug, Clone)]
pub struct RithmicBracketOrder {
    pub action: i32,
    pub duration: i32,
    pub exchange: String,
    pub localid: String,
    pub ordertype: i32,
    pub price: Option<f64>,
    pub profit_ticks: i32,
    pub qty: i32,
    pub stop_ticks: i32,
    pub symbol: String,
}

#[derive(Debug, Clone)]
pub struct RithmicModifyOrder {
    pub id: String,
    pub exchange: String,
    pub symbol: String,
    pub qty: i32,
    pub price: f64,
    pub ordertype: i32,
}

#[derive(Debug, Clone)]
pub struct RithmicCancelOrder {
    pub id: String,
}
