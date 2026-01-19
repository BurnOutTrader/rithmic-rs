//! Instrument information types.
//!
//! This module provides a parsed representation of instrument reference data
//! returned by Rithmic, with helper methods for price and size precision.

use crate::rti::ResponseReferenceData;

/// Parsed instrument information from Rithmic reference data.
///
/// This struct provides a cleaner interface to the raw `ResponseReferenceData`
/// message, with typed fields and helper methods for common operations.
///
/// # Example
/// ```ignore
/// use rithmic_rs::InstrumentInfo;
/// use rithmic_rs::rti::ResponseReferenceData;
///
/// // Convert from reference data response
/// let info = InstrumentInfo::try_from(&response)?;
/// println!("Symbol: {} on {}", info.symbol, info.exchange);
/// println!("Tick size: {:?}, precision: {}", info.tick_size, info.price_precision());
/// ```
#[derive(Debug, Clone, Default)]
pub struct InstrumentInfo {
    /// The symbol identifier (e.g., "ESH4").
    pub symbol: String,
    /// The exchange code (e.g., "CME").
    pub exchange: String,
    /// Exchange-specific symbol if different from symbol.
    pub exchange_symbol: Option<String>,
    /// Human-readable name of the instrument.
    pub name: Option<String>,
    /// Product code for the instrument family (e.g., "ES").
    pub product_code: Option<String>,
    /// Type of instrument (e.g., "Future", "Option").
    pub instrument_type: Option<String>,
    /// Underlying symbol for derivatives.
    pub underlying: Option<String>,
    /// Currency code (e.g., "USD").
    pub currency: Option<String>,
    /// Expiration date string (format varies by exchange).
    pub expiration_date: Option<String>,
    /// Minimum price increment (tick size).
    pub tick_size: Option<f64>,
    /// Dollar value of one point move.
    pub point_value: Option<f64>,
    /// Whether the instrument can be traded.
    pub is_tradable: bool,
}

impl InstrumentInfo {
    /// Calculate the number of decimal places for price display.
    ///
    /// This is derived from the tick size. For example:
    /// - tick_size = 0.25 -> 2 decimal places
    /// - tick_size = 0.01 -> 2 decimal places
    /// - tick_size = 1.0 -> 0 decimal places
    /// - tick_size = 0.0001 -> 4 decimal places
    ///
    /// Returns 2 as a default if tick_size is not available.
    ///
    /// # Example
    /// ```
    /// use rithmic_rs::InstrumentInfo;
    ///
    /// let mut info = InstrumentInfo::default();
    /// info.tick_size = Some(0.25);
    /// assert_eq!(info.price_precision(), 2);
    ///
    /// info.tick_size = Some(1.0);
    /// assert_eq!(info.price_precision(), 0);
    /// ```
    pub fn price_precision(&self) -> u8 {
        match self.tick_size {
            Some(tick) if tick > 0.0 => {
                // Count decimal places needed to represent the tick size
                let mut precision = 0u8;
                let mut value = tick;

                while value < 1.0 && precision < 10 {
                    value *= 10.0;
                    precision += 1;
                }

                // Check if there are more decimal places after the leading digit
                let fractional = value - value.floor();

                if fractional > 0.0001 && precision < 10 {
                    // There are more decimal places
                    let mut frac = fractional;

                    while frac > 0.0001 && precision < 10 {
                        frac *= 10.0;
                        frac -= frac.floor();
                        precision += 1;
                    }
                }
                precision
            }
            _ => 2, // Default to 2 decimal places
        }
    }

    /// Returns the size precision for this instrument.
    ///
    /// For futures contracts, this is always 0 since futures trade
    /// in whole contract units.
    ///
    /// # Example
    /// ```
    /// use rithmic_rs::InstrumentInfo;
    ///
    /// let info = InstrumentInfo::default();
    /// assert_eq!(info.size_precision(), 0);
    /// ```
    pub fn size_precision(&self) -> u8 {
        0 // Futures are always whole contracts
    }
}

/// Error type for InstrumentInfo conversion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstrumentInfoError {
    /// Description of the error.
    pub message: String,
}

impl std::fmt::Display for InstrumentInfoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for InstrumentInfoError {}

impl TryFrom<&ResponseReferenceData> for InstrumentInfo {
    type Error = InstrumentInfoError;

    /// Convert from a `ResponseReferenceData` message to `InstrumentInfo`.
    ///
    /// # Errors
    /// Returns an error if the symbol or exchange fields are missing.
    fn try_from(data: &ResponseReferenceData) -> Result<Self, Self::Error> {
        let symbol = data.symbol.clone().ok_or_else(|| InstrumentInfoError {
            message: "missing symbol".to_string(),
        })?;

        let exchange = data.exchange.clone().ok_or_else(|| InstrumentInfoError {
            message: "missing exchange".to_string(),
        })?;

        let is_tradable = data
            .is_tradable
            .as_ref()
            .map(|s| s.eq_ignore_ascii_case("true") || s == "1")
            .unwrap_or(false);

        Ok(InstrumentInfo {
            symbol,
            exchange,
            exchange_symbol: data.exchange_symbol.clone(),
            name: data.symbol_name.clone(),
            product_code: data.product_code.clone(),
            instrument_type: data.instrument_type.clone(),
            underlying: data.underlying_symbol.clone(),
            currency: data.currency.clone(),
            expiration_date: data.expiration_date.clone(),
            tick_size: data.min_qprice_change,
            point_value: data.single_point_value,
            is_tradable,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_price_precision() {
        let mut info = InstrumentInfo::default();

        // ES tick size is 0.25
        info.tick_size = Some(0.25);
        assert_eq!(info.price_precision(), 2);

        // CL tick size is 0.01
        info.tick_size = Some(0.01);
        assert_eq!(info.price_precision(), 2);

        // ZB tick size is 0.03125 (1/32)
        info.tick_size = Some(0.03125);
        assert_eq!(info.price_precision(), 5);

        // Whole number tick size
        info.tick_size = Some(1.0);
        assert_eq!(info.price_precision(), 0);

        // Default when no tick size
        info.tick_size = None;
        assert_eq!(info.price_precision(), 2);
    }

    #[test]
    fn test_try_from_missing_symbol() {
        let data = ResponseReferenceData {
            template_id: 15,
            symbol: None,
            exchange: Some("CME".to_string()),
            ..Default::default()
        };

        let result = InstrumentInfo::try_from(&data);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().message, "missing symbol");
    }

    #[test]
    fn test_try_from_missing_exchange() {
        let data = ResponseReferenceData {
            template_id: 15,
            symbol: Some("ESH4".to_string()),
            exchange: None,
            ..Default::default()
        };

        let result = InstrumentInfo::try_from(&data);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().message, "missing exchange");
    }

    #[test]
    fn test_try_from_success() {
        let data = ResponseReferenceData {
            template_id: 15,
            symbol: Some("ESH4".to_string()),
            exchange: Some("CME".to_string()),
            symbol_name: Some("E-mini S&P 500".to_string()),
            product_code: Some("ES".to_string()),
            instrument_type: Some("Future".to_string()),
            currency: Some("USD".to_string()),
            min_qprice_change: Some(0.25),
            single_point_value: Some(50.0),
            is_tradable: Some("true".to_string()),
            ..Default::default()
        };

        let info = InstrumentInfo::try_from(&data).unwrap();
        assert_eq!(info.symbol, "ESH4");
        assert_eq!(info.exchange, "CME");
        assert_eq!(info.name, Some("E-mini S&P 500".to_string()));
        assert_eq!(info.product_code, Some("ES".to_string()));
        assert_eq!(info.tick_size, Some(0.25));
        assert_eq!(info.point_value, Some(50.0));
        assert!(info.is_tradable);
    }
}
