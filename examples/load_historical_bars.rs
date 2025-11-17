use tracing::{Level, event};

use rithmic_rs::{
    ConnectStrategy, RithmicConfig, RithmicEnv, RithmicHistoryPlant,
    rti::{messages::RithmicMessage, request_time_bar_replay::BarType},
    ws::RithmicStream,
};

fn parse_args() -> Result<(String, String, i32), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();

    let mut symbol = None;
    let mut exchange = None;
    let mut start_time_sec = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--symbol" => {
                i += 1;
                if i < args.len() {
                    symbol = Some(args[i].clone());
                }
            }
            "--exchange" => {
                i += 1;
                if i < args.len() {
                    exchange = Some(args[i].clone());
                }
            }
            "--start-time-sec" => {
                i += 1;
                if i < args.len() {
                    start_time_sec = Some(args[i].parse()?);
                }
            }
            _ => {}
        }
        i += 1;
    }

    let symbol = symbol.ok_or("Missing required argument: --symbol")?;
    let exchange = exchange.ok_or("Missing required argument: --exchange")?;
    let start_time_sec = start_time_sec.ok_or("Missing required argument: --start-time-sec")?;

    Ok((symbol, exchange, start_time_sec))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Before running this example, copy .env.blank to .env
    // and fill in RITHMIC_ACCOUNT_ID, FCM_ID, and IB_ID

    let (symbol, exchange, start_time_sec) = parse_args()?;

    // Simple one-line configuration from environment variables (.env file)
    let config = RithmicConfig::from_dotenv(RithmicEnv::Demo)?;

    tracing_subscriber::fmt().init();

    let history_plant = RithmicHistoryPlant::connect(&config, ConnectStrategy::Simple).await?;
    let handle = history_plant.get_handle();

    handle.login().await?;

    let start_time = start_time_sec;
    let end_time = start_time + (23 * 60 * 60); // Add 23 hours in seconds

    event!(
        Level::INFO,
        "Loading 5-minute bars for {} from {} to {}",
        symbol,
        start_time,
        end_time
    );

    // Load 5-minute time bars
    let five_min_bars = handle
        .load_time_bars(
            symbol.clone(),
            exchange.clone(),
            BarType::MinuteBar,
            5, // 5-minute bars
            start_time,
            end_time,
        )
        .await?;

    event!(
        Level::INFO,
        "Received {} 5-minute bar responses",
        five_min_bars.len()
    );

    // Process the 5-minute bar responses
    for r in five_min_bars.iter() {
        match &r.message {
            RithmicMessage::ResponseTimeBarReplay(bar_message) => {
                event!(Level::INFO, "5-minute bar: {:#?}", bar_message);
            }
            _ => {
                event!(Level::WARN, "Received unexpected message type");
            }
        }
    }

    let _ = handle.disconnect().await;

    event!(Level::INFO, "Disconnected from Rithmic History Plant");

    Ok(())
}
