//! Paged history replays against a live Rithmic history plant, timed page by
//! page: the pattern Sharur's adapter uses — a fresh request from the newest
//! row after every truncation notice (`MODE=pages`) — or the venue's own
//! continuation, `RequestResumeBars` on the one request (`MODE=resume`).
//!
//! Read-only: login, replays, logout. Credentials come from the environment
//! or a `DOTENV` file and are never printed.
//!
//! Options: `KIND` (`vp` | `minute` | `daily` | `weekly`), `PERIOD`, `SYMBOL`,
//! `EXCHANGE`, `START`/`END` or `DAYS_BACK`, `MODE`, `PAGES` (most pages),
//! `MAX_SECS` (whole run), `CONNECT_ATTEMPTS`, `RESUME_BARS`.

use std::env;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use prost::Message as _;
use rithmic_rs::rti::{
    RequestHeartbeat, RequestLogin, RequestLogout, RequestResumeBars, RequestTickBarReplay,
    RequestTimeBarReplay, RequestVolumeProfileMinuteBars, ResponseTickBarReplay,
    ResponseTimeBarReplay, ResponseVolumeProfileMinuteBars, request_login::SysInfraType,
    request_tick_bar_replay, request_time_bar_replay,
};
use tokio_tungstenite::{connect_async, tungstenite::Message};

/// The fields every Rithmic response shares, plus a replay frame's stamp.
#[derive(Clone, PartialEq, prost::Message)]
struct Envelope {
    #[prost(int32, optional, tag = "154467")]
    template_id: Option<i32>,
    #[prost(string, optional, tag = "132758")]
    request_key: Option<String>,
    #[prost(string, repeated, tag = "132760")]
    user_msg: Vec<String>,
    #[prost(string, repeated, tag = "132764")]
    rq_handler_rp_code: Vec<String>,
    #[prost(string, repeated, tag = "132766")]
    rp_code: Vec<String>,
    #[prost(int32, optional, tag = "119100")]
    marker: Option<i32>,
    /// Tick bar replays: [open, close] of the bar.
    #[prost(int32, repeated, packed = "false", tag = "119202")]
    data_bar_ssboe: Vec<i32>,
    #[prost(double, optional, tag = "153633")]
    heartbeat_interval: Option<f64>,
    #[prost(string, optional, tag = "153428")]
    unique_user_id: Option<String>,
}

impl Envelope {
    fn stamp(&self) -> Option<i32> {
        self.marker.or_else(|| self.data_bar_ssboe.last().copied())
    }
    fn is_terminal(&self) -> bool {
        self.rq_handler_rp_code.is_empty()
    }
    fn id(&self) -> &str {
        self.user_msg.first().map_or("", String::as_str)
    }
}

fn frame(req: &impl prost::Message) -> Vec<u8> {
    let len = req.encoded_len() as u32;
    let mut buf = Vec::with_capacity(len as usize + 4);
    buf.extend_from_slice(&len.to_be_bytes());
    req.encode(&mut buf)
        .expect("encoding into a Vec is infallible");
    buf
}

fn build<T: Default>(fill: impl FnOnce(&mut T)) -> T {
    let mut value = T::default();
    fill(&mut value);
    value
}

fn wall_now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

fn utc(secs: i64) -> String {
    let days = secs.div_euclid(86_400);
    let s = secs.rem_euclid(86_400);
    // Civil date from days since 1970-01-01 (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        s / 3_600,
        (s % 3_600) / 60,
        s % 60
    )
}

fn var(key: &str) -> Option<String> {
    env::var(key).ok().filter(|v| !v.trim().is_empty())
}

fn require(keys: &[&str]) -> Result<String, String> {
    keys.iter()
        .find_map(|k| var(k))
        .ok_or_else(|| format!("set one of {}", keys.join(" / ")))
}

fn gateway(server: &str) -> String {
    match server.to_ascii_lowercase().trim() {
        "chicago" => "wss://rprotocol.rithmic.com:443".to_owned(),
        "sydney" => "wss://rprotocol-au.rithmic.com:443".to_owned(),
        "frankfurt" => "wss://rprotocol-de.rithmic.com:443".to_owned(),
        raw => raw.to_owned(),
    }
}

/// One replay request for `[start, end]` under `id`.
fn replay_request(
    kind: &str,
    id: &str,
    symbol: &str,
    exchange: &str,
    period: i32,
    start: i32,
    end: i32,
    resume_bars: bool,
) -> Result<Vec<u8>, String> {
    Ok(match kind {
        "vp" => frame(&build(|r: &mut RequestVolumeProfileMinuteBars| {
            r.template_id = 208;
            r.user_msg = vec![id.to_owned()];
            r.symbol = Some(symbol.to_owned());
            r.exchange = Some(exchange.to_owned());
            r.bar_type_period = Some(period);
            r.start_index = Some(start);
            r.finish_index = Some(end);
            r.resume_bars = Some(resume_bars);
        })),
        "minute" | "daily" | "weekly" => frame(&build(|r: &mut RequestTimeBarReplay| {
            r.template_id = 202;
            r.user_msg = vec![id.to_owned()];
            r.symbol = Some(symbol.to_owned());
            r.exchange = Some(exchange.to_owned());
            r.bar_type = Some(match kind {
                "minute" => request_time_bar_replay::BarType::MinuteBar as i32,
                "daily" => request_time_bar_replay::BarType::DailyBar as i32,
                _ => request_time_bar_replay::BarType::WeeklyBar as i32,
            });
            r.bar_type_period = Some(period);
            r.start_index = Some(start);
            r.finish_index = Some(end);
            r.direction = Some(request_time_bar_replay::Direction::First as i32);
            r.time_order = Some(request_time_bar_replay::TimeOrder::Forwards as i32);
            r.resume_bars = Some(resume_bars);
        })),
        "tick" => frame(&build(|r: &mut RequestTickBarReplay| {
            r.template_id = 206;
            r.user_msg = vec![id.to_owned()];
            r.symbol = Some(symbol.to_owned());
            r.exchange = Some(exchange.to_owned());
            r.bar_type = Some(request_tick_bar_replay::BarType::TickBar as i32);
            r.bar_sub_type = Some(request_tick_bar_replay::BarSubType::Regular as i32);
            r.bar_type_specifier = Some(period.to_string());
            r.start_index = Some(start);
            r.finish_index = Some(end);
            r.direction = Some(request_tick_bar_replay::Direction::First as i32);
            r.time_order = Some(request_tick_bar_replay::TimeOrder::Forwards as i32);
            r.resume_bars = Some(resume_bars);
        })),
        other => {
            return Err(format!(
                "KIND {other}: expected vp, minute, daily, weekly or tick"
            ));
        }
    })
}

/// One data frame, decoded as its own template, for a small window one
/// wants to see whole: a time bar or per-price minute with its stamp and
/// volume, a tick row with its second and size.
fn dump_frame(kind: &str, payload: &[u8]) {
    match kind {
        "vp" => {
            if let Ok(f) = ResponseVolumeProfileMinuteBars::decode(payload) {
                println!(
                    "  vp marker={} ({}) levels={} volume={:?} trades={:?}",
                    f.marker.unwrap_or(-1),
                    f.marker.map_or("-".to_owned(), |m| utc(i64::from(m))),
                    f.profile_price.len(),
                    f.volume,
                    f.num_trades
                );
            }
        }
        "tick" => {
            if let Ok(f) = ResponseTickBarReplay::decode(payload) {
                println!(
                    "  tick ssboe={:?} volume={:?} close={:?}",
                    f.data_bar_ssboe, f.volume, f.close_price
                );
            }
        }
        _ => {
            if let Ok(f) = ResponseTimeBarReplay::decode(payload) {
                println!(
                    "  bar marker={} ({}) open={:?} close={:?} volume={:?} trades={:?}",
                    f.marker.unwrap_or(-1),
                    f.marker.map_or("-".to_owned(), |m| utc(i64::from(m))),
                    f.open_price,
                    f.close_price,
                    f.volume,
                    f.num_trades
                );
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if let Some(path) = var("DOTENV") {
        dotenvy::from_path(&path).map_err(|e| format!("DOTENV {path}: {e}"))?;
    } else {
        dotenvy::dotenv().ok();
    }
    let url = var("RITHMIC_URL")
        .or_else(|| var("RITHMIC_SERVER").map(|s| gateway(&s)))
        .ok_or("set RITHMIC_URL or RITHMIC_SERVER")?;
    let user = require(&["RITHMIC_USER", "RITHMIC_USERNAME"])?;
    let password = require(&["RITHMIC_PW", "RITHMIC_PASSWORD"])?;
    let system_name = require(&["RITHMIC_SYSTEM_NAME"])?;
    let app_name = require(&["RITHMIC_APP_NAME"])?;
    let app_version = require(&["RITHMIC_APP_VERSION"])?;

    let kind = var("KIND").unwrap_or_else(|| "vp".to_owned());
    let symbol = var("SYMBOL").unwrap_or_else(|| "MNQU6".to_owned());
    let exchange = var("EXCHANGE").unwrap_or_else(|| "CME".to_owned());
    let period: i32 = var("PERIOD").map(|v| v.parse()).transpose()?.unwrap_or(1);
    let resume_bars: bool = var("RESUME_BARS")
        .map(|v| v.parse())
        .transpose()?
        .unwrap_or(true);
    let mode = var("MODE").unwrap_or_else(|| "pages".to_owned());
    let resume_mode = match mode.as_str() {
        "pages" => false,
        "resume" => true,
        other => return Err(format!("MODE {other}: expected pages or resume").into()),
    };
    let pages_max: u32 = var("PAGES").map(|v| v.parse()).transpose()?.unwrap_or(200);
    let dump = var("DUMP").as_deref() == Some("1");
    let max_secs: u64 = var("MAX_SECS")
        .map(|v| v.parse())
        .transpose()?
        .unwrap_or(900);
    let now = wall_now() as i64;
    let end: i32 = var("END")
        .map(|v| v.parse())
        .transpose()?
        .unwrap_or(i32::try_from(now)?);
    let start: i32 = match var("START") {
        Some(v) => v.parse()?,
        None => {
            let days: f64 = var("DAYS_BACK")
                .map(|v| v.parse())
                .transpose()?
                .unwrap_or(7.0);
            i32::try_from(i64::from(end) - (days * 86_400.0) as i64)?
        }
    };
    println!(
        "paged_replays: {url} kind={kind} symbol={symbol} exchange={exchange} period={period} \
         mode={mode} window=[{start}, {end}] = [{}, {}] ({:.1} minutes) resume_bars={resume_bars} \
         pages_max={pages_max} max_secs={max_secs}",
        utc(i64::from(start)),
        utc(i64::from(end)),
        f64::from(end - start) / 60.0
    );

    let attempts: u32 = var("CONNECT_ATTEMPTS")
        .map(|v| v.parse())
        .transpose()?
        .unwrap_or(3);
    let mut ws = None;
    for attempt in 1..=attempts {
        match tokio::time::timeout(Duration::from_secs(10), connect_async(&url)).await {
            Ok(Ok((stream, _))) => {
                ws = Some(stream);
                break;
            }
            Ok(Err(e)) => println!("connect attempt {attempt}/{attempts} failed: {e}"),
            Err(_) => println!("connect attempt {attempt}/{attempts}: no handshake within 10s"),
        }
    }
    let ws = ws.ok_or_else(|| format!("{url}: could not connect in {attempts} attempts"))?;
    let (mut sink, mut stream) = ws.split();

    let login = build(|r: &mut RequestLogin| {
        r.template_id = 10;
        r.template_version = Some("5.42".to_owned());
        r.user_msg = vec!["login".to_owned()];
        r.user = Some(user);
        r.password = Some(password);
        r.app_name = Some(app_name);
        r.app_version = Some(app_version);
        r.system_name = Some(system_name);
        r.infra_type = Some(SysInfraType::HistoryPlant as i32);
    });
    sink.send(Message::Binary(frame(&login).into())).await?;
    let mut heartbeat_secs = 60u64;
    loop {
        let Some(msg) = stream.next().await else {
            return Err("socket closed before the login reply".into());
        };
        let Message::Binary(data) = msg? else {
            continue;
        };
        let envelope = Envelope::decode(&data[4..])?;
        if envelope.template_id == Some(11) {
            println!(
                "login: rp_code={:?} session={:?}",
                envelope.rp_code, envelope.unique_user_id
            );
            if envelope.rp_code != vec!["0".to_owned()] {
                return Err(format!("login refused: {:?}", envelope.rp_code).into());
            }
            if let Some(interval) = envelope.heartbeat_interval {
                heartbeat_secs = (interval.max(5.0) as u64) / 2;
            }
            break;
        }
    }
    let mut heartbeat = tokio::time::interval(Duration::from_secs(heartbeat_secs.max(1)));
    heartbeat.tick().await;

    let started = Instant::now();
    let mut cursor = start;
    let mut page: u32 = 0;
    let mut total_rows: u64 = 0;
    let mut total_bytes: u64 = 0;
    let mut newest: Option<i32> = None;
    let mut last_key: Option<String> = None;
    let mut current_id = String::new();
    let mut outcome = String::from("stopped: page limit");
    'pages: loop {
        if page >= pages_max {
            break;
        }
        page += 1;
        let request = if resume_mode && page > 1 {
            let key = last_key.clone().ok_or("a notice without a request_key")?;
            frame(&build(|r: &mut RequestResumeBars| {
                r.template_id = 210;
                r.user_msg = vec![current_id.clone()];
                r.request_key = Some(key);
            }))
        } else {
            current_id = format!("page-{page}");
            replay_request(
                &kind,
                &current_id,
                &symbol,
                &exchange,
                period,
                cursor,
                end,
                resume_bars,
            )?
        };
        let sent = Instant::now();
        sink.send(Message::Binary(request.into())).await?;
        let mut rows: u64 = 0;
        let mut bytes: u64 = 0;
        let mut first_at: Option<Duration> = None;
        let mut page_min: Option<i32> = None;
        let mut page_max: Option<i32> = None;
        let terminal: Envelope = loop {
            if started.elapsed() > Duration::from_secs(max_secs) {
                outcome = format!("stopped: MAX_SECS with page {page} open after {rows} rows");
                break 'pages;
            }
            let msg = tokio::select! {
                _ = heartbeat.tick() => {
                    let hb = build(|r: &mut RequestHeartbeat| {
                        r.template_id = 18;
                        r.user_msg = vec!["hb".to_owned()];
                    });
                    sink.send(Message::Binary(frame(&hb).into())).await?;
                    continue;
                }
                msg = stream.next() => msg,
                _ = tokio::time::sleep(Duration::from_secs(1)) => continue,
            };
            let Some(msg) = msg else {
                outcome = format!("socket closed by the server during page {page}");
                break 'pages;
            };
            let data = match msg? {
                Message::Binary(data) => data,
                Message::Close(f) => {
                    outcome = format!("close frame during page {page}: {f:?}");
                    break 'pages;
                }
                _ => continue,
            };
            let envelope = match Envelope::decode(&data[4..]) {
                Ok(e) => e,
                Err(e) => {
                    println!("undecodable frame ({} bytes): {e}", data.len());
                    continue;
                }
            };
            if envelope.template_id == Some(211) {
                println!(
                    "  t={:.3}s resume ack: rp_code={:?} id={:?}",
                    started.elapsed().as_secs_f64(),
                    envelope.rp_code,
                    envelope.id()
                );
                continue;
            }
            if envelope.template_id == Some(19) {
                continue;
            }
            if envelope.id() != current_id {
                if envelope.is_terminal() {
                    println!(
                        "  t={:.3}s final frame of an earlier id {:?}: rp_code={:?} request_key={:?}",
                        started.elapsed().as_secs_f64(),
                        envelope.id(),
                        envelope.rp_code,
                        envelope.request_key
                    );
                }
                continue;
            }
            if !envelope.is_terminal() {
                rows += 1;
                bytes += data.len() as u64;
                first_at.get_or_insert(sent.elapsed());
                if dump {
                    dump_frame(&kind, &data[4..]);
                }
                if let Some(s) = envelope.stamp() {
                    page_min = Some(page_min.map_or(s, |m| m.min(s)));
                    page_max = Some(page_max.map_or(s, |m| m.max(s)));
                }
                continue;
            }
            // A dataless terminal is the page's end; a stamped one is data
            // and the end at once (never seen; counted as data too).
            if let Some(s) = envelope.stamp() {
                rows += 1;
                bytes += data.len() as u64;
                page_min = Some(page_min.map_or(s, |m| m.min(s)));
                page_max = Some(page_max.map_or(s, |m| m.max(s)));
            }
            break envelope;
        };
        let elapsed = sent.elapsed();
        total_rows += rows;
        total_bytes += bytes;
        if let Some(m) = page_max {
            newest = Some(newest.map_or(m, |n| n.max(m)));
        }
        let end_kind = if terminal.rp_code == vec!["0".to_owned()] {
            "complete".to_owned()
        } else if terminal.rp_code.is_empty() && terminal.request_key.is_some() {
            format!("notice key={:?}", terminal.request_key)
        } else {
            format!("rp_code={:?}", terminal.rp_code)
        };
        println!(
            "page {page} id={current_id} start={} rows={rows} bytes={bytes} first_frame_ms={} dur_s={:.2} rows_per_s={:.0} kb_per_s={:.0} stamps=[{} .. {}] end={end_kind} t={:.1}s",
            utc(i64::from(cursor)),
            first_at.map_or(-1_i64, |d| d.as_millis() as i64),
            elapsed.as_secs_f64(),
            rows as f64 / elapsed.as_secs_f64().max(0.001),
            bytes as f64 / 1024.0 / elapsed.as_secs_f64().max(0.001),
            page_min.map_or("-".to_owned(), |s| utc(i64::from(s))),
            page_max.map_or("-".to_owned(), |s| utc(i64::from(s))),
            started.elapsed().as_secs_f64()
        );
        if terminal.rp_code == vec!["0".to_owned()] {
            outcome = "complete".to_owned();
            break;
        }
        if terminal.rp_code.is_empty() && terminal.request_key.is_some() {
            last_key = terminal.request_key.clone();
            if !resume_mode {
                match page_max {
                    Some(m) if m > cursor => cursor = m,
                    _ => {
                        outcome = "stopped: a notice with nothing newer".to_owned();
                        break;
                    }
                }
            }
            continue;
        }
        outcome = format!("refused: {:?}", terminal.rp_code);
        break;
    }
    let elapsed = started.elapsed();
    println!(
        "summary: outcome=\"{outcome}\" pages={page} rows={total_rows} bytes={total_bytes} elapsed_s={:.1} rows_per_s={:.0} kb_per_s={:.0} newest={} end={} short_by_min={:.1}",
        elapsed.as_secs_f64(),
        total_rows as f64 / elapsed.as_secs_f64().max(0.001),
        total_bytes as f64 / 1024.0 / elapsed.as_secs_f64().max(0.001),
        newest.map_or("-".to_owned(), |s| utc(i64::from(s))),
        utc(i64::from(end)),
        newest.map_or(f64::NAN, |s| f64::from(end - s) / 60.0)
    );
    let logout = build(|r: &mut RequestLogout| {
        r.template_id = 12;
        r.user_msg = vec!["logout".to_owned()];
    });
    sink.send(Message::Binary(frame(&logout).into())).await?;
    let _ = tokio::time::timeout(Duration::from_secs(3), stream.next()).await;
    Ok(())
}
