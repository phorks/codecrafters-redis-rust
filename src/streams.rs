use std::{
    collections::{hash_map, HashMap},
    fmt,
    str::FromStr,
};

use crate::resp::RespMessage;

#[derive(PartialEq, Eq, Hash, PartialOrd, Ord, Clone, Debug)]
pub struct StreamEntryId {
    millis: u64,
    seq_no: usize,
}

impl StreamEntryId {
    pub fn new(millis: u64, seq_no: usize) -> Self {
        Self { millis, seq_no }
    }

    pub fn is_min(&self) -> bool {
        self.millis == 0 && self.seq_no == 0
    }
}

impl ToString for StreamEntryId {
    fn to_string(&self) -> String {
        format!("{}-{}", self.millis, self.seq_no)
    }
}

impl Default for StreamEntryId {
    fn default() -> Self {
        Self {
            millis: 0,
            seq_no: 0,
        }
    }
}

#[derive(Debug)]
pub enum InvalidStreamEntryId {
    EqualOrSmallerThanTop(StreamEntryId),
    EqualToMin,
}

impl InvalidStreamEntryId {
    pub fn to_resp(&self) -> RespMessage {
        let msg = match self {
            Self::EqualOrSmallerThanTop(stream_entry_id) => {
                "ERR The ID specified in XADD is equal or smaller than the target stream top item"
            }
            Self::EqualToMin => "ERR The ID specified in XADD must be greater than 0-0",
        };

        RespMessage::SimpleError(String::from(msg))
    }
}

impl fmt::Display for InvalidStreamEntryId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InvalidStreamEntryId::EqualOrSmallerThanTop(top_entry_id) => write!(
                f,
                "The specified ID is equal or smaller than the target stream top ID. Top ID: {}",
                &top_entry_id.to_string()
            ),
            InvalidStreamEntryId::EqualToMin => write!(f, "The ID cannot be 0-0"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum XaddStreamEntryId {
    Explicit(StreamEntryId),
    GenerateSeqNo(u64),
    GenerateBoth,
}

impl FromStr for XaddStreamEntryId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "*" {
            return Ok(Self::GenerateBoth);
        }

        let (millis_str, seq_no_str) = s.split_once('-').ok_or_else(|| {
            anyhow::anyhow!(
                "Invalid entry ID format (it must consist of two parts separated by a '-'"
            )
        })?;

        let millis = millis_str.parse()?;

        Ok(match seq_no_str {
            "*" => Self::GenerateSeqNo(millis),
            _ => Self::Explicit(StreamEntryId {
                millis,
                seq_no: seq_no_str.parse()?,
            }),
        })
    }
}

#[derive(Clone)]
pub struct StreamValue {
    entries: HashMap<StreamEntryId, HashMap<String, String>>,
    top_entry_id: StreamEntryId,
}

impl StreamValue {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
            top_entry_id: StreamEntryId::default(),
        }
    }

    pub async fn get_entry_value(&self, entry_id: &StreamEntryId, key: &str) -> Option<&str> {
        let entry = self.entries.get(entry_id)?;
        entry.get(key).map(|x| x.as_str())
    }

    pub fn add_entry(
        &mut self,
        entry_id: StreamEntryId,
        values: HashMap<String, String>,
    ) -> anyhow::Result<()> {
        if let Some(validation_error) = self.validate_entry_id(&entry_id) {
            anyhow::bail!(validation_error);
        };

        match self.entries.entry(entry_id.clone()) {
            hash_map::Entry::Occupied(_) => anyhow::bail!("An record with the same key exists."),
            hash_map::Entry::Vacant(v) => {
                v.insert(values);
            }
        }

        self.top_entry_id = entry_id;

        Ok(())
    }

    pub fn validate_entry_id(&self, entry_id: &StreamEntryId) -> Option<InvalidStreamEntryId> {
        if entry_id.is_min() {
            Some(InvalidStreamEntryId::EqualToMin)
        } else if entry_id <= &self.top_entry_id {
            Some(InvalidStreamEntryId::EqualOrSmallerThanTop(
                self.top_entry_id.clone(),
            ))
        } else {
            None
        }
    }

    pub fn generate_entry_id(&self, xadd_entry_id: XaddStreamEntryId) -> StreamEntryId {
        match xadd_entry_id {
            XaddStreamEntryId::Explicit(explicit) => explicit,
            XaddStreamEntryId::GenerateSeqNo(millis) => todo!(),
            XaddStreamEntryId::GenerateBoth => todo!(),
        }
    }
}
