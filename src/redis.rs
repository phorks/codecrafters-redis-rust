use std::{
    collections::HashMap,
    time::{Duration, SystemTime},
};

pub const EMPTY_RDB: [u8; 88] = [
    0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31, 0xfa, 0x09, 0x72, 0x65, 0x64, 0x69, 0x73,
    0x2d, 0x76, 0x65, 0x72, 0x05, 0x37, 0x2e, 0x32, 0x2e, 0x30, 0xfa, 0x0a, 0x72, 0x65, 0x64, 0x69,
    0x73, 0x2d, 0x62, 0x69, 0x74, 0x73, 0xc0, 0x40, 0xfa, 0x05, 0x63, 0x74, 0x69, 0x6d, 0x65, 0xc2,
    0x6d, 0x08, 0xbc, 0x65, 0xfa, 0x08, 0x75, 0x73, 0x65, 0x64, 0x2d, 0x6d, 0x65, 0x6d, 0xc2, 0xb0,
    0xc4, 0x10, 0x00, 0xfa, 0x08, 0x61, 0x6f, 0x66, 0x2d, 0x62, 0x61, 0x73, 0x65, 0xc0, 0x00, 0xff,
    0xf0, 0x6e, 0x3b, 0xfe, 0xc0, 0xff, 0x5a, 0xa2,
];

use tokio::{
    io::AsyncReadExt,
    sync::{mpsc, RwLock},
    time::timeout,
};

use crate::{
    commands::{SetCommandOptions, XreadBlocking},
    io_helper::skip_sequence,
    resp::RespMessage,
    streams::{
        StreamEntryId, StreamQueryResponse, StreamRecord, StreamValue, XaddStreamEntryId,
        XreadStreamEntryId,
    },
};

#[derive(Debug)]
enum LengthValue {
    Length(usize),
    IntegerAsString8,
    IntegerAsString16,
    IntegerAsString32,
    CompressedString,
}

async fn read_length<T: AsyncReadExt + Unpin>(buf: &mut T) -> anyhow::Result<LengthValue> {
    let first_byte = buf.read_u8().await?;
    match first_byte >> 6 {
        0b00 => Ok(LengthValue::Length(first_byte as usize)),
        0b01 => {
            let second_byte = buf.read_u8().await?;
            Ok(LengthValue::Length(
                (((first_byte & 0b111111) as usize) << 8) | (second_byte as usize),
            ))
        }
        0b10 => Ok(LengthValue::Length(buf.read_u32().await? as usize)),
        0b11 => match first_byte & 0b111111 {
            0 => Ok(LengthValue::IntegerAsString8),
            1 => Ok(LengthValue::IntegerAsString16),
            2 => Ok(LengthValue::IntegerAsString32),
            3 => Ok(LengthValue::CompressedString),
            _ => {
                anyhow::bail!(
                    "Special length {} is not implemented",
                    first_byte & 0b111111
                )
            }
        },
        _ => {
            // the binary number is shifted right 6 digits, no other case is possible
            unreachable!()
        }
    }
}

async fn read_numeric_length<T: AsyncReadExt + Unpin>(buf: &mut T) -> anyhow::Result<usize> {
    if let LengthValue::Length(length) = read_length(buf).await? {
        Ok(length)
    } else {
        anyhow::bail!("Expected a numeric length, received special length")
    }
}

#[derive(PartialEq, Eq, Hash, Clone)]
pub enum StringValue {
    Str(Vec<u8>),
    Int8(u8),
    Int16(u16),
    Int32(u32),
}

impl StringValue {
    pub fn new_increased(&self) -> anyhow::Result<StringValue> {
        let new = match self {
            StringValue::Str(_) => anyhow::bail!(TypeError::NotIntegerOrOutOfRange),
            StringValue::Int8(i) => StringValue::Int8(i + 1),
            StringValue::Int16(i) => StringValue::Int16(i + 1),
            StringValue::Int32(i) => StringValue::Int32(i + 1),
        };

        Ok(new)
    }

    pub fn to_i64(&self) -> anyhow::Result<i64> {
        let i = match self {
            StringValue::Str(_) => anyhow::bail!(TypeError::NotIntegerOrOutOfRange),
            StringValue::Int8(i) => *i as i64,
            StringValue::Int16(i) => *i as i64,
            StringValue::Int32(i) => *i as i64,
        };

        Ok(i)
    }
}

impl<T: AsRef<str>> From<T> for StringValue {
    fn from(value: T) -> Self {
        StringValue::Str(value.as_ref().chars().map(|x| x as u8).collect())
    }
}

impl ToString for StringValue {
    fn to_string(&self) -> String {
        match self {
            StringValue::Str(vec) => vec.iter().map(|i| *i as char).collect::<String>(),
            StringValue::Int8(i) => i.to_string(),
            StringValue::Int16(i) => i.to_string(),
            StringValue::Int32(i) => i.to_string(),
        }
    }
}

async fn read_string<T: AsyncReadExt + Unpin>(buf: &mut T) -> anyhow::Result<StringValue> {
    match read_length(buf).await? {
        LengthValue::Length(length) => {
            let mut bytes = vec![0u8; length as usize];
            buf.read_exact(&mut bytes).await?;
            Ok(StringValue::Str(bytes))
        }
        LengthValue::IntegerAsString8 => Ok(StringValue::Int8(buf.read_u8().await?)),
        LengthValue::IntegerAsString16 => Ok(StringValue::Int16(buf.read_u16_le().await?)),
        LengthValue::IntegerAsString32 => Ok(StringValue::Int32(buf.read_u32_le().await?)),
        LengthValue::CompressedString => {
            let clen = read_numeric_length(buf).await?;
            let _ulen = read_numeric_length(buf).await?;

            let mut bytes = vec![0u8; clen as usize];
            buf.read_exact(&mut bytes).await?;
            // TODO: perform LZF decompression
            Ok(StringValue::Str(bytes))
        }
    }
}

enum EntryValueType {
    Unknown,
}

impl EntryValueType {
    fn from_u8(_flag: u8) -> anyhow::Result<Self> {
        Ok(Self::Unknown)
    }

    async fn from_buffer<T: AsyncReadExt + Unpin>(_buf: &mut T) -> anyhow::Result<Self> {
        _buf.read_u8().await?;
        Ok(Self::Unknown)
    }
}

enum EntryFlag {
    ValueType(EntryValueType),
    ExpiresInSecs,
    ExpiresInMillis,
}

impl EntryFlag {
    async fn from_buffer<T: AsyncReadExt + Unpin>(buf: &mut T) -> anyhow::Result<Self> {
        let flag = buf.read_u8().await?;

        Ok(match flag {
            0xFCu8 => Self::ExpiresInMillis,
            0xFDu8 => Self::ExpiresInSecs,
            _ => Self::ValueType(EntryValueType::from_u8(flag)?),
        })
    }
}

enum SectionId {
    Metadata,
    Database,
    EndOfFile,
}

async fn read_section_id<T: AsyncReadExt + Unpin>(buf: &mut T) -> anyhow::Result<SectionId> {
    let id = buf.read_u8().await?;

    println!("Read section id: {:#x}.", id);

    match id {
        0xFAu8 => Ok(SectionId::Metadata),
        0xFEu8 => Ok(SectionId::Database),
        0xFFu8 => Ok(SectionId::EndOfFile),
        _ => anyhow::bail!("Unexpected section identifier: {:#x}.", id),
    }
}

pub enum Expiry {
    InSecs(u32),
    InMillis(u64),
}

#[derive(Clone)]
pub enum RecordValue {
    String(StringValue),
    Stream(StreamValue),
}

enum EntryRecord {
    String(StringValue),
    Stream(StreamRecord),
}

impl EntryRecord {
    fn value(&self) -> RecordValue {
        match self {
            EntryRecord::String(value) => RecordValue::String(value.clone()),
            EntryRecord::Stream(stream) => RecordValue::Stream(stream.value().clone()),
        }
    }
}

impl From<Option<RecordValue>> for RespMessage {
    fn from(value: Option<RecordValue>) -> Self {
        match value {
            Some(RecordValue::String(s)) => RespMessage::BulkString(s.to_string()),
            Some(RecordValue::Stream(_)) => todo!(),
            None => RespMessage::Null,
        }
    }
}

pub struct DatabaseEntry {
    record: EntryRecord,
    pub expires_on: Option<Expiry>,
}

impl DatabaseEntry {
    fn new(value: EntryRecord, expires_on: Option<Expiry>) -> Self {
        DatabaseEntry {
            record: value,
            expires_on,
        }
    }

    pub fn new_stream(key: String) -> Self {
        DatabaseEntry {
            record: EntryRecord::Stream(StreamRecord::new(key)),
            expires_on: None,
        }
    }

    pub fn get_expiry_time(&self) -> Option<SystemTime> {
        let expires_on = self.expires_on.as_ref()?;

        let duration = match expires_on {
            Expiry::InSecs(secs) => Duration::from_secs(*secs as u64),
            Expiry::InMillis(millis) => Duration::from_millis(*millis),
        };

        Some(SystemTime::UNIX_EPOCH + duration)
    }

    fn is_expired(&self) -> bool {
        if let Some(expires_on) = self.get_expiry_time() {
            expires_on < SystemTime::now()
        } else {
            false
        }
    }
}

#[derive(Debug)]
pub enum TypeError {
    NotIntegerOrOutOfRange,
}

impl TypeError {
    pub fn to_resp(&self) -> RespMessage {
        let msg = match self {
            TypeError::NotIntegerOrOutOfRange => "ERR value is not an integer or out of range",
        };

        RespMessage::SimpleError(String::from(msg))
    }
}

impl std::fmt::Display for TypeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Value is not an integer or is out of range.");
        Ok(())
    }
}

pub struct Database {
    entries: RwLock<HashMap<StringValue, DatabaseEntry>>,
}

impl Database {
    pub fn new() -> Self {
        Database {
            entries: RwLock::new(HashMap::new()),
        }
    }

    pub async fn set(
        &self,
        key: String,
        value: String,
        options: SetCommandOptions,
    ) -> anyhow::Result<RespMessage> {
        let mut expires_on = None;
        if let Some(px) = options.px {
            if let Some(t) = SystemTime::now().checked_add(Duration::from_millis(px)) {
                let millis = t.duration_since(SystemTime::UNIX_EPOCH)?.as_millis();
                expires_on = Some(Expiry::InMillis(millis as u64))
            } else {
                anyhow::bail!("Invalid px");
            }
        }

        let value = match value.parse::<u32>() {
            Ok(i) => StringValue::Int32(i),
            Err(_) => StringValue::Str((value).into()),
        };

        self.entries.write().await.insert(
            key.clone().into(),
            DatabaseEntry::new(EntryRecord::String(value), expires_on),
        );

        Ok(RespMessage::SimpleString(String::from("OK")))
    }

    pub async fn get(&self, key: &str) -> anyhow::Result<Option<RecordValue>> {
        let r_store = self.entries.read().await;
        let key = &key.into();

        if let Some(entry) = r_store.get(key) {
            if entry.is_expired() {
                drop(r_store);
                let mut w_store = self.entries.write().await;
                w_store.remove(key);
                drop(w_store);
                return Ok(None);
            }

            Ok(Some(entry.record.value()))
        } else {
            Ok(None)
        }
    }

    pub async fn add_stream_entry(
        &self,
        key: &str,
        entry_id: XaddStreamEntryId,
        values: HashMap<String, String>,
    ) -> anyhow::Result<StreamEntryId> {
        let mut store = self.entries.write().await;
        let stream = store
            .entry(key.into())
            .and_modify(|x| {
                if x.is_expired() || matches!(x.record, EntryRecord::String(_)) {
                    *x = DatabaseEntry::new_stream(String::from(key))
                }
            })
            .or_insert_with(|| DatabaseEntry::new_stream(String::from(key)));

        let EntryRecord::Stream(ref mut stream) = &mut stream.record else {
            panic!("The entry is inserted in a way that its value must be stream");
        };

        Ok(stream.xadd(entry_id, values).await?)
    }

    pub async fn get_stream_entries_in_range(
        &self,
        key: &str,
        start: StreamEntryId,
        end: StreamEntryId,
    ) -> anyhow::Result<StreamQueryResponse> {
        let store = self.entries.read().await;

        let Some(stream) = store.get(&key.into()) else {
            return Ok(StreamQueryResponse::empty());
        };

        let EntryRecord::Stream(ref stream) = &stream.record else {
            anyhow::bail!("The key refers to a non-stream entry. Key: {}", &key);
        };

        Ok(stream.get_entries_in_range(&start, &end, false))
    }

    pub async fn get_bulk_stream_entries(
        &self,
        stream_afters: Vec<(String, XreadStreamEntryId)>,
    ) -> anyhow::Result<Vec<(String, StreamQueryResponse)>> {
        let mut res = vec![];
        let store = self.entries.read().await;
        for (key, start) in stream_afters.into_iter() {
            let Some(stream) = store.get(&key.clone().into()) else {
                continue;
            };
            let EntryRecord::Stream(ref stream) = &stream.record else {
                anyhow::bail!("The key refers to a non-stream entry. Key: {}", &key);
            };

            let XreadStreamEntryId::Id(start) = &start else {
                continue;
            };

            res.push((
                key,
                stream.get_entries_in_range(start, &StreamEntryId::MAX, true),
            ));
        }

        Ok(res)
    }

    pub async fn get_bulk_stream_entries_blocking(
        &self,
        stream_afters: Vec<(String, XreadStreamEntryId)>,
        block: XreadBlocking,
    ) -> anyhow::Result<Option<Vec<(String, StreamQueryResponse)>>> {
        let mut responses = HashMap::new();
        let mut store = self.entries.write().await;
        let (tx, mut rx) = mpsc::channel(stream_afters.len());
        let mut n_ready = 0;
        for (key, start) in stream_afters.iter() {
            let Some(stream) = store.get_mut(&key.clone().into()) else {
                continue;
            };
            let EntryRecord::Stream(ref mut stream) = &mut stream.record else {
                anyhow::bail!("The key refers to a non-stream entry. Key: {}", &key);
            };

            let entries = match &start {
                XreadStreamEntryId::Id(start) => {
                    let entries = stream.get_entries_in_range(start, &StreamEntryId::MAX, true);
                    if entries.is_empty() {
                        None
                    } else {
                        Some(entries)
                    }
                }
                XreadStreamEntryId::Future => None,
            };

            match entries {
                Some(entries) => {
                    responses.insert(key.clone(), Some(entries));
                    n_ready += 1;
                }
                None => {
                    responses.insert(key.clone(), None);
                    stream.subscribe(start.clone(), tx.clone()).unwrap();
                }
            }
        }
        drop(store);

        if n_ready != stream_afters.len() {
            let n = stream_afters.len();
            let fut = async move {
                loop {
                    let Some((key, entries)) = rx.recv().await else {
                        break;
                    };

                    n_ready += 1;
                    responses.insert(key, Some(entries));

                    if n_ready == n {
                        break;
                    }
                }

                (n_ready, responses)
            };

            (n_ready, responses) = match block {
                XreadBlocking::Block(millis) => {
                    match timeout(Duration::from_millis(millis), fut).await {
                        Ok(x) => x,
                        Err(_) => return Ok(None),
                    }
                }
                XreadBlocking::BlockIndefinitely => fut.await,
            };
        }

        if n_ready != stream_afters.len() {
            // FIXME: Probably unregister all channels registered to streams
            Ok(None)
        } else {
            let mut res = vec![];
            for (key, _) in &stream_afters {
                let entries = responses.remove(key).unwrap().unwrap();
                res.push((key.clone(), entries));
            }

            Ok(Some(res))
        }
    }

    pub async fn increase_integer(&self, key: &str) -> anyhow::Result<i64> {
        let mut store = self.entries.write().await;

        let entry = store.entry(key.into()).or_insert_with(|| {
            DatabaseEntry::new(EntryRecord::String(StringValue::Int32(0)), None)
        });

        let new = match &entry.record {
            EntryRecord::String(s) => s.new_increased(),
            EntryRecord::Stream(_) => Err(anyhow::anyhow!(TypeError::NotIntegerOrOutOfRange)),
        }?;

        let increased = new.to_i64()?;

        entry.record = EntryRecord::String(new);
        Ok(increased)
    }

    pub async fn get_keys(&self) -> anyhow::Result<Vec<StringValue>> {
        let r_store = self.entries.read().await;

        let mut keys = vec![];
        let mut expired_keys = vec![];
        for (key, value) in r_store.iter() {
            if value.is_expired() {
                expired_keys.push(key.clone());
            } else {
                keys.push(key.clone());
            }
        }

        drop(r_store);

        if expired_keys.len() > 0 {
            let mut w_store = self.entries.write().await;

            for key in expired_keys {
                w_store.remove(&key);
            }
        }

        Ok(keys)
    }
}

pub struct Instance {
    pub _version: [u8; 4],
    pub _metadata: HashMap<StringValue, StringValue>,
    pub dbs: HashMap<usize, Database>,
    pub _checksum: [u8; 8],
}

impl Instance {
    pub async fn new<T: AsyncReadExt + Unpin>(mut buf: T) -> anyhow::Result<Self> {
        skip_sequence(&mut buf, &[0x52u8, 0x45u8, 0x44u8, 0x49u8, 0x53u8]).await?;
        let mut version = [0u8; 4];
        buf.read_exact(&mut version).await?;

        let mut metadata: HashMap<StringValue, StringValue> = HashMap::new();

        let mut dbs = HashMap::new();

        loop {
            match read_section_id(&mut buf).await? {
                SectionId::Metadata => {
                    // metadata section
                    let name = read_string(&mut buf).await?;
                    let value = read_string(&mut buf).await?;
                    metadata.insert(name, value);
                }
                SectionId::Database => {
                    let index = read_numeric_length(&mut buf).await?;
                    _ = skip_sequence(&mut buf, &[0xFBu8]).await?;
                    let db_table_size = read_numeric_length(&mut buf).await?;
                    let _ex_table_size = read_numeric_length(&mut buf).await?;
                    let mut entries = HashMap::new();

                    for _ in 0..db_table_size {
                        let mut expires_on = None;
                        let _value_type = match EntryFlag::from_buffer(&mut buf).await? {
                            EntryFlag::ValueType(entry_value_type) => entry_value_type,
                            EntryFlag::ExpiresInSecs => {
                                expires_on = Some(Expiry::InSecs(buf.read_u32_le().await?));
                                EntryValueType::from_buffer(&mut buf).await?
                            }
                            EntryFlag::ExpiresInMillis => {
                                expires_on = Some(Expiry::InMillis(buf.read_u64_le().await?));
                                EntryValueType::from_buffer(&mut buf).await?
                            }
                        };

                        let key = read_string(&mut buf).await?;
                        let value = read_string(&mut buf).await?;

                        entries.insert(
                            key,
                            DatabaseEntry {
                                expires_on,
                                record: EntryRecord::String(value),
                            },
                        );
                    }

                    dbs.insert(
                        index,
                        Database {
                            entries: RwLock::new(entries),
                        },
                    );
                }
                SectionId::EndOfFile => {
                    let mut checksum = [0u8; 8];
                    buf.read_exact(&mut checksum).await?;
                    return Ok(Instance {
                        _version: version,
                        _metadata: metadata,
                        dbs,
                        _checksum: checksum,
                    });
                }
            }
        }
    }
}
