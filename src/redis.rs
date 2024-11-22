use std::{
    collections::{btree_map::Entry, hash_map, BTreeMap, HashMap},
    str::FromStr,
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
    sync::{RwLock, RwLockWriteGuard},
};

use crate::{
    commands::SetCommandOptions,
    io_helper::skip_sequence,
    resp::RespMessage,
    streams::{StreamEntryId, StreamQueryResponse, StreamValue, XaddStreamEntryId},
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
pub enum EntryValue {
    String(StringValue),
    Stream(StreamValue),
}

impl From<Option<EntryValue>> for RespMessage {
    fn from(value: Option<EntryValue>) -> Self {
        match value {
            Some(EntryValue::String(s)) => RespMessage::BulkString(s.to_string()),
            Some(EntryValue::Stream(_)) => todo!(),
            None => RespMessage::Null,
        }
    }
}

pub struct DatabaseEntry {
    pub value: EntryValue,
    pub expires_on: Option<Expiry>,
}

impl DatabaseEntry {
    pub fn new(value: EntryValue, expires_on: Option<Expiry>) -> Self {
        DatabaseEntry { value, expires_on }
    }

    pub fn new_stream() -> Self {
        DatabaseEntry {
            value: EntryValue::Stream(StreamValue::new()),
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

        self.entries.write().await.insert(
            key.clone().into(),
            DatabaseEntry::new(EntryValue::String((&value).into()), expires_on),
        );

        Ok(RespMessage::SimpleString(String::from("OK")))
    }

    pub async fn get(&self, key: &str) -> anyhow::Result<Option<EntryValue>> {
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

            Ok(Some(entry.value.clone()))
        } else {
            Ok(None)
        }
    }

    async fn with_stream<T, F: FnOnce(&mut StreamValue) -> anyhow::Result<T>>(
        &self,
        key: &str,
        f: F,
    ) -> anyhow::Result<T> {
        let mut store = self.entries.write().await;
        let stream = store
            .entry(key.into())
            .and_modify(|x| {
                if x.is_expired() || matches!(x.value, EntryValue::String(_)) {
                    *x = DatabaseEntry::new_stream()
                }
            })
            .or_insert_with(|| DatabaseEntry::new_stream());

        let EntryValue::Stream(ref mut stream) = &mut stream.value else {
            panic!("The entry is inserted in a way that its value must be stream");
        };

        f(stream)
    }

    pub async fn add_stream_entry(
        &self,
        key: &str,
        entry_id: XaddStreamEntryId,
        values: HashMap<String, String>,
    ) -> anyhow::Result<StreamEntryId> {
        self.with_stream(key, move |stream| stream.xadd(entry_id, values))
            .await
    }

    pub async fn get_stream_entries_in_range(
        &self,
        key: &str,
        start: StreamEntryId,
        end: StreamEntryId,
    ) -> anyhow::Result<StreamQueryResponse> {
        // FIXME: with_stream will unnecessarily create the stream if it doesn't exists or
        // replace it if the key is a string value. It makes sense for add but not for queries.
        // We also don't need to acquire WRITE lock to respond to the query
        self.with_stream(key, move |stream| {
            Ok(stream.get_entries_in_range(&start, &end))
        })
        .await
    }

    pub async fn get_bulk_stream_entries(
        &self,
        stream_starts: Vec<(String, StreamEntryId)>,
    ) -> anyhow::Result<Vec<(String, StreamQueryResponse)>> {
        let mut res = vec![];
        let store = self.entries.read().await;
        for (key, start) in stream_starts.into_iter() {
            let Some(stream) = store.get(&key.clone().into()) else {
                continue;
            };
            let EntryValue::Stream(ref stream) = &stream.value else {
                anyhow::bail!("The key refers to a non-stream entry. Key: {}", &key);
            };

            res.push((
                key,
                stream.get_entries_in_range(&start, &StreamEntryId::MAX),
            ));
        }

        Ok(res)
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
    pub version: [u8; 4],
    pub metadata: HashMap<StringValue, StringValue>,
    pub dbs: HashMap<usize, Database>,
    pub checksum: [u8; 8],
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
                    let ex_table_size = read_numeric_length(&mut buf).await?;
                    let mut entries = HashMap::new();

                    for i in 0..db_table_size {
                        let mut expires_on = None;
                        let value_type = match EntryFlag::from_buffer(&mut buf).await? {
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
                                value: EntryValue::String(value),
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
                        version,
                        metadata,
                        dbs,
                        checksum,
                    });
                }
            }
        }
    }
}
