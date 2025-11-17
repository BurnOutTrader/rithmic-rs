//! Example: Connect to the RithmicTickerPlant
use tracing::info;

use rithmic_rs::{
    ConnectStrategy, RithmicConfig, RithmicEnv, RithmicTickerPlant, ws::RithmicStream,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Before running this example, copy .env.blank to .env
    // and fill in RITHMIC_ACCOUNT_ID, FCM_ID, and IB_ID

    // Simple one-line configuration from environment variables (.env file)
    let config = RithmicConfig::from_dotenv(RithmicEnv::Demo)?;

    tracing_subscriber::fmt().init();

    let ticker_plant = RithmicTickerPlant::connect(&config, ConnectStrategy::Simple).await?;
    let ticker_plant_handle = ticker_plant.get_handle();

    let resp = ticker_plant_handle.login().await;

    info!("Login response: {:#?}", resp);

    ticker_plant_handle.disconnect().await?;

    info!("Disconnected from Rithmic");

    Ok(())
}
