use tracing::{Level, event};

use rithmic_rs::{
    ConnectStrategy, RithmicConfig, RithmicEnv, RithmicHistoryPlant, rti::messages::RithmicMessage,
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
    let (symbol, exchange, start_time_sec) = parse_args()?;

    // Load environment variables from .env file
    dotenvy::dotenv().ok();

    // Create configuration from environment variables
    let config = RithmicConfig::from_env(RithmicEnv::Demo)?;

    tracing_subscriber::fmt().init();

    let history_plant = RithmicHistoryPlant::connect(&config, ConnectStrategy::Simple).await?;
    let handle = history_plant.get_handle();

    handle.login().await?;

    let start_time = start_time_sec;
    let end_time = start_time + (23 * 60 * 60); // Add 23 hours in seconds

    event!(
        Level::INFO,
        "Loading ticks for {} from {} to {}",
        symbol,
        start_time,
        end_time
    );

    // Rithmic only returns 10_000 ticks at a time, and note that there can be several ticks sharing the same timestamp
    let tick_responses = handle
        .load_ticks(symbol.clone(), exchange, start_time, end_time)
        .await?;

    event!(
        Level::INFO,
        "Received {} tick responses",
        tick_responses.len()
    );

    // Process the tick responses
    for r in tick_responses.iter() {
        match &r.message {
            RithmicMessage::ResponseTickBarReplay(tick_message) => {
                event!(Level::INFO, "Tick: {:#?}", tick_message);
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
