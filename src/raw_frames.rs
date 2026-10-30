//! Raw inbound frame recording, before any decoding.
//!
//! Lets a caller keep the exact bytes Rithmic delivered, so what a client later
//! builds from them can be replayed and checked. Nothing is observed unless a
//! caller installs an observer. Outgoing requests never pass through here, so
//! login credentials cannot be recorded; which inbound templates are written is
//! the caller's choice.

use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::sync::{Arc, RwLock};
use std::thread::JoinHandle;
use std::time::{SystemTime, UNIX_EPOCH};

use prost::Message;

use crate::rti::MessageType;

/// Template reported for a frame whose envelope does not decode.
pub const UNDECODABLE_TEMPLATE: i32 = -1;

/// Template reported for a frame too short to hold its length prefix.
pub const TRUNCATED_TEMPLATE: i32 = -2;

const FILE_MAGIC: &[u8; 8] = b"SHRRAW1\n";

/// Receives the plant name, template and complete frame, length prefix included.
pub type RawFrameObserver = Arc<dyn Fn(&str, i32, &[u8]) + Send + Sync>;

static OBSERVER: RwLock<Option<RawFrameObserver>> = RwLock::new(None);

/// Installs the process-wide observer, or removes it with `None`.
pub fn set_raw_frame_observer(observer: Option<RawFrameObserver>) {
    if let Ok(mut slot) = OBSERVER.write() {
        *slot = observer;
    }
}

pub(crate) fn observe(source: &str, data: &[u8]) {
    let Ok(slot) = OBSERVER.read() else {
        return;
    };
    let Some(observer) = slot.as_ref() else {
        return;
    };
    let template = if data.len() < 4 {
        TRUNCATED_TEMPLATE
    } else {
        MessageType::decode(&data[4..]).map_or(UNDECODABLE_TEMPLATE, |message| message.template_id)
    };
    observer(source, template, data);
}

/// One recorded frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawFrame {
    /// Local receipt time, nanoseconds since the Unix epoch.
    pub received_nanos: u64,
    /// The plant that received it, such as `ticker_plant`.
    pub source: String,
    /// The frame's template, or one of the two sentinel values above.
    pub template: i32,
    /// The complete frame, length prefix included.
    pub data: Vec<u8>,
}

/// What a finished recording holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RawFrameSummary {
    /// Frames written to the file.
    pub written: u64,
    /// Frames lost because the queue was full. Any value above zero means the
    /// recording is incomplete and must not be treated as evidence of the stream.
    pub overflowed: u64,
}

/// Writes frames to a new file from a dedicated thread, so recording never
/// blocks the plant's socket reader.
pub struct RawFrameRecorder {
    sender: SyncSender<Vec<u8>>,
    writer: JoinHandle<io::Result<u64>>,
    overflowed: AtomicU64,
}

impl RawFrameRecorder {
    /// Starts recording to `path` with room for `capacity` queued frames.
    ///
    /// # Errors
    ///
    /// Returns the failure to create the file, including when it already exists.
    pub fn create(path: impl AsRef<Path>, capacity: usize) -> io::Result<Self> {
        let file = OpenOptions::new().write(true).create_new(true).open(path)?;
        let (sender, receiver) = mpsc::sync_channel::<Vec<u8>>(capacity);
        let writer = std::thread::spawn(move || -> io::Result<u64> {
            let mut output = BufWriter::new(file);
            output.write_all(FILE_MAGIC)?;
            let mut written = 0_u64;
            for record in receiver {
                output.write_all(&record)?;
                written += 1;
            }
            output.flush()?;
            output.get_ref().sync_all()?;
            Ok(written)
        });
        Ok(Self {
            sender,
            writer,
            overflowed: AtomicU64::new(0),
        })
    }

    /// Queues one frame, stamped now. A full queue counts as overflow rather
    /// than blocking the plant.
    pub fn record(&self, source: &str, template: i32, data: &[u8]) {
        let received_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| {
                u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX)
            });
        self.record_at(received_nanos, source, template, data);
    }

    fn record_at(&self, received_nanos: u64, source: &str, template: i32, data: &[u8]) {
        let source = source.as_bytes();
        let source = &source[..source.len().min(usize::from(u8::MAX))];
        let mut record = Vec::with_capacity(17 + source.len() + data.len());
        record.extend_from_slice(&received_nanos.to_le_bytes());
        record.extend_from_slice(&template.to_le_bytes());
        record.push(u8::try_from(source.len()).unwrap_or(u8::MAX));
        record.extend_from_slice(source);
        record.extend_from_slice(&u32::try_from(data.len()).unwrap_or(u32::MAX).to_le_bytes());
        record.extend_from_slice(data);
        if let Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) =
            self.sender.try_send(record)
        {
            self.overflowed.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Frames lost so far because the queue was full.
    #[must_use]
    pub fn overflowed(&self) -> u64 {
        self.overflowed.load(Ordering::Relaxed)
    }

    /// Stops recording, syncs the file and reports what it holds.
    ///
    /// # Errors
    ///
    /// Returns the writer's failure, or an error if its thread panicked.
    pub fn finish(self) -> io::Result<RawFrameSummary> {
        let Self {
            sender,
            writer,
            overflowed,
        } = self;
        drop(sender);
        let written = writer
            .join()
            .map_err(|_| io::Error::other("raw frame writer panicked"))??;
        Ok(RawFrameSummary {
            written,
            overflowed: overflowed.into_inner(),
        })
    }
}

/// Reads a whole recording back in order.
///
/// # Errors
///
/// Returns a read failure, or `InvalidData` for a file that is not a recording
/// or that ends partway through a frame.
pub fn read_raw_frames(path: impl AsRef<Path>) -> io::Result<Vec<RawFrame>> {
    let mut input = BufReader::new(File::open(path)?);
    let mut magic = [0_u8; 8];
    input.read_exact(&mut magic)?;
    if &magic != FILE_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "not a raw frame recording",
        ));
    }
    let mut frames = Vec::new();
    loop {
        let mut stamp = [0_u8; 8];
        match input.read_exact(&mut stamp) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(frames),
            Err(error) => return Err(error),
        }
        frames.push(
            read_frame_body(&mut input, u64::from_le_bytes(stamp)).map_err(|error| {
                if error.kind() == io::ErrorKind::UnexpectedEof {
                    io::Error::new(io::ErrorKind::InvalidData, "recording ends inside a frame")
                } else {
                    error
                }
            })?,
        );
    }
}

fn read_frame_body(input: &mut impl Read, received_nanos: u64) -> io::Result<RawFrame> {
    let mut template = [0_u8; 4];
    input.read_exact(&mut template)?;
    let mut source_length = [0_u8; 1];
    input.read_exact(&mut source_length)?;
    let mut source = vec![0_u8; usize::from(source_length[0])];
    input.read_exact(&mut source)?;
    let mut data_length = [0_u8; 4];
    input.read_exact(&mut data_length)?;
    let data_length = usize::try_from(u32::from_le_bytes(data_length))
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "frame too long"))?;
    let mut data = vec![0_u8; data_length];
    input.read_exact(&mut data)?;
    Ok(RawFrame {
        received_nanos,
        source: String::from_utf8(source)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "plant name is not UTF-8"))?,
        template: i32::from_le_bytes(template),
        data,
    })
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Mutex;

    use super::*;

    static GLOBAL_OBSERVER: Mutex<()> = Mutex::new(());

    fn temp_path(name: &str) -> PathBuf {
        let path =
            std::env::temp_dir().join(format!("rithmic-raw-frames-{}-{name}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        path
    }

    #[test]
    fn frames_round_trip_in_order_with_their_exact_bytes() {
        let path = temp_path("round-trip");
        let recorder = RawFrameRecorder::create(&path, 8).unwrap();
        recorder.record_at(11, "ticker_plant", 160, &[0, 0, 0, 2, 9, 8]);
        recorder.record_at(12, "history_plant", -1, &[]);
        let summary = recorder.finish().unwrap();

        assert_eq!(summary.written, 2);
        assert_eq!(summary.overflowed, 0);
        assert_eq!(
            read_raw_frames(&path).unwrap(),
            vec![
                RawFrame {
                    received_nanos: 11,
                    source: "ticker_plant".to_owned(),
                    template: 160,
                    data: vec![0, 0, 0, 2, 9, 8],
                },
                RawFrame {
                    received_nanos: 12,
                    source: "history_plant".to_owned(),
                    template: -1,
                    data: vec![],
                },
            ]
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn an_existing_file_is_refused_rather_than_overwritten() {
        let path = temp_path("existing");
        std::fs::write(&path, b"keep").unwrap();

        assert!(RawFrameRecorder::create(&path, 1).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), b"keep");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn a_file_cut_off_inside_a_frame_is_refused() {
        let path = temp_path("truncated");
        let recorder = RawFrameRecorder::create(&path, 8).unwrap();
        recorder.record_at(1, "ticker_plant", 160, &[1, 2, 3, 4]);
        recorder.finish().unwrap();
        let bytes = std::fs::read(&path).unwrap();
        std::fs::write(&path, &bytes[..bytes.len() - 1]).unwrap();

        let error = read_raw_frames(&path).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn the_receiver_shows_the_observer_each_frame_before_decoding_it() {
        let _guard = GLOBAL_OBSERVER.lock().unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&seen);
        set_raw_frame_observer(Some(Arc::new(move |_source, template, data| {
            sink.lock().unwrap().push((template, data.to_vec()));
        })));
        let api = crate::api::receiver_api::RithmicReceiverApi {
            source: "ticker_plant".to_owned(),
        };

        let _ = api.buf_to_message(prost::bytes::Bytes::from_static(&[0, 0, 0, 1, 0xff]));
        set_raw_frame_observer(None);

        assert_eq!(
            *seen.lock().unwrap(),
            vec![(UNDECODABLE_TEMPLATE, vec![0, 0, 0, 1, 0xff])]
        );
    }

    #[test]
    fn the_observer_reports_decoded_undecodable_and_truncated_frames() {
        let _guard = GLOBAL_OBSERVER.lock().unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&seen);
        set_raw_frame_observer(Some(Arc::new(move |source, template, data| {
            sink.lock()
                .unwrap()
                .push((source.to_owned(), template, data.to_vec()));
        })));

        let body = MessageType { template_id: 160 }.encode_to_vec();
        let mut decoded = u32::try_from(body.len()).unwrap().to_be_bytes().to_vec();
        decoded.extend_from_slice(&body);
        observe("ticker_plant", &decoded);
        observe("ticker_plant", &[0, 0, 0, 1, 0xff]);
        observe("ticker_plant", &[0, 0]);
        set_raw_frame_observer(None);
        observe("ticker_plant", &decoded);

        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 3);
        assert_eq!(seen[0].1, 160);
        assert_eq!(seen[0].2, decoded);
        assert_eq!(seen[1].1, UNDECODABLE_TEMPLATE);
        assert_eq!(seen[2].1, TRUNCATED_TEMPLATE);
    }
}
