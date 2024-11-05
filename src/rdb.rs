use std::{collections::HashMap, fmt::Display};

use tokio::io::AsyncReadExt;

async fn skip_sequence<T: AsyncReadExt + Unpin>(buf: &mut T, str: &[u8]) -> anyhow::Result<()> {
    for i in 0..str.len() {
        let ch = buf.read_u8().await?;
        if ch != str[i] {
            anyhow::bail!(
                "Expected character: {}, received: {}",
                str[i] as char,
                ch as char
            );
        }
    }

    Ok(())
}

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
            unreachable!()
        } // the binary number is shifted right 6 digits, no other case is possible
    }
}

async fn read_numeric_length<T: AsyncReadExt + Unpin>(buf: &mut T) -> anyhow::Result<usize> {
    if let LengthValue::Length(length) = read_length(buf).await? {
        Ok(length)
    } else {
        anyhow::bail!("Expected a numeric length, received special length")
    }
}

#[derive(PartialEq, Eq, Hash)]
pub enum StringValue {
    Str(Vec<u8>),
    Int8(u8),
    Int16(u16),
    Int32(u32),
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
    let length = read_length(buf).await?;
    println!("Read length {:?}", length);
    match length {
        LengthValue::Length(length) => {
            let mut bytes = Vec::with_capacity(length as usize);
            buf.read_exact(&mut bytes).await?;
            Ok(StringValue::Str(bytes))
        }
        LengthValue::IntegerAsString8 => Ok(StringValue::Int8(buf.read_u8().await?)),
        LengthValue::IntegerAsString16 => Ok(StringValue::Int16(buf.read_u16_le().await?)),
        LengthValue::IntegerAsString32 => Ok(StringValue::Int32(buf.read_u32_le().await?)),
        LengthValue::CompressedString => {
            let clen = read_numeric_length(buf).await?;
            let _ulen = read_numeric_length(buf).await?;

            let mut bytes = Vec::with_capacity(clen as usize);
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
        Ok(Self::Unknown)
    }
}

enum EntryFlag {
    ValueType(EntryValueType),
    ExpiresInSeconds,
    ExpiresInMillis,
}

impl EntryFlag {
    async fn from_buffer<T: AsyncReadExt + Unpin>(buf: &mut T) -> anyhow::Result<Self> {
        let flag = buf.read_u8().await?;

        Ok(match flag {
            0xFCu8 => Self::ExpiresInMillis,
            0xFDu8 => Self::ExpiresInSeconds,
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
    InSeconds(u32),
    InMillis(u64),
}

pub struct RdbEntry {
    pub value: StringValue,
    pub expires_on: Option<Expiry>,
}

pub struct RdbDatabase {
    pub entries: HashMap<StringValue, RdbEntry>,
}

pub struct Rdb {
    pub version: [u8; 4],
    pub metadata: HashMap<StringValue, StringValue>,
    pub dbs: HashMap<usize, RdbDatabase>,
    pub checksum: [u8; 8],
}

impl Rdb {
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
                            EntryFlag::ExpiresInSeconds => {
                                expires_on = Some(Expiry::InSeconds(buf.read_u32_le().await?));
                                EntryValueType::from_buffer(&mut buf).await?
                            }
                            EntryFlag::ExpiresInMillis => {
                                expires_on = Some(Expiry::InMillis(buf.read_u64_le().await?));
                                EntryValueType::from_buffer(&mut buf).await?
                            }
                        };

                        let key = read_string(&mut buf).await?;
                        let value = read_string(&mut buf).await?;

                        entries.insert(key, RdbEntry { expires_on, value });
                    }

                    dbs.insert(index, RdbDatabase { entries });
                }
                SectionId::EndOfFile => {
                    let mut checksum = [0u8; 8];
                    buf.read_exact(&mut checksum).await?;
                    return Ok(Rdb {
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
