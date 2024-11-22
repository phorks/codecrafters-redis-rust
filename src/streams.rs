use std::{
    collections::{hash_map, BTreeMap, BinaryHeap, HashMap},
    fmt,
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::resp::RespMessage;

#[derive(PartialEq, Eq, Hash, PartialOrd, Ord, Clone, Debug)]
pub struct StreamEntryId {
    millis: u64,
    seq_no: u64,
}

impl StreamEntryId {
    fn new(millis: u64, seq_no: u64) -> Self {
        Self { millis, seq_no }
    }

    fn parse_as_range_bound(s: &str, default_seq_no: u64) -> anyhow::Result<Self> {
        match s.split_once('-') {
            Some((millis_str, seq_no_str)) => Ok(Self {
                millis: millis_str.parse()?,
                seq_no: seq_no_str.parse()?,
            }),
            None => Ok(Self {
                millis: s.parse()?,
                seq_no: default_seq_no,
            }),
        }
    }

    pub fn parse_as_range_start(s: &str) -> anyhow::Result<Self> {
        match s {
            "-" => Ok(Self::new(0, 0)),
            _ => Self::parse_as_range_bound(s, 0),
        }
    }

    pub fn parse_as_range_end(s: &str) -> anyhow::Result<Self> {
        match s {
            "+" => Ok(Self::new(0, 0)),
            _ => Self::parse_as_range_bound(s, u64::MAX),
        }
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
            Self::EqualOrSmallerThanTop(_) => {
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
        self.add_entry_unchecked(entry_id, values)
    }

    pub fn xadd(
        &mut self,
        entry_id: XaddStreamEntryId,
        values: HashMap<String, String>,
    ) -> anyhow::Result<StreamEntryId> {
        let entry_id = self
            .generate_entry_id(entry_id)
            .map_err(|e| anyhow::anyhow!(e))?;

        self.add_entry_unchecked(entry_id.clone(), values)?;
        Ok(entry_id)
    }

    pub fn get_entries_in_range(
        &self,
        start: &StreamEntryId,
        end: &StreamEntryId,
    ) -> BTreeMap<StreamEntryId, HashMap<String, String>> {
        // FIXME this is approach is inefficient. It is of O(n + mlogm) where n is
        // the total number of entries in the stream and m is the number of entries in the given range.
        // Redis does it in O(m), while keeping entry insertion in O(1) by leveraging Radix tries
        let mut map = BTreeMap::new();

        for (key, value) in &self.entries {
            if key >= start && key <= end {
                map.insert(key.clone(), value.clone());
            }
        }

        map
    }

    fn add_entry_unchecked(
        &mut self,
        entry_id: StreamEntryId,
        values: HashMap<String, String>,
    ) -> anyhow::Result<()> {
        match self.entries.entry(entry_id.clone()) {
            hash_map::Entry::Occupied(_) => anyhow::bail!("A record with the same key exists."),
            hash_map::Entry::Vacant(v) => {
                v.insert(values);
            }
        }

        self.top_entry_id = entry_id;

        Ok(())
    }

    fn validate_entry_id(&self, entry_id: &StreamEntryId) -> Result<(), InvalidStreamEntryId> {
        if entry_id <= &self.top_entry_id {
            Err(if entry_id.is_min() {
                InvalidStreamEntryId::EqualToMin
            } else {
                InvalidStreamEntryId::EqualOrSmallerThanTop(self.top_entry_id.clone())
            })
        } else {
            Ok(())
        }
    }

    fn generate_entry_id(&self, xadd_entry_id: XaddStreamEntryId) -> anyhow::Result<StreamEntryId> {
        match xadd_entry_id {
            XaddStreamEntryId::Explicit(explicit) => self
                .validate_entry_id(&explicit)
                .map(|_| explicit)
                .map_err(|e| anyhow::anyhow!(e)),
            XaddStreamEntryId::GenerateSeqNo(millis) => {
                if millis < self.top_entry_id.millis {
                    anyhow::bail!(InvalidStreamEntryId::EqualOrSmallerThanTop(
                        self.top_entry_id.clone(),
                    ))
                } else if millis == self.top_entry_id.millis {
                    Ok(StreamEntryId {
                        millis,
                        seq_no: self.top_entry_id.seq_no + 1,
                    })
                } else {
                    Ok(StreamEntryId { millis, seq_no: 0 })
                }
            }
            XaddStreamEntryId::GenerateBoth => {
                let now = Self::now() as u64;

                let seq_no = match now.cmp(&self.top_entry_id.millis) {
                    std::cmp::Ordering::Less => {
                        anyhow::bail!("Time travel is not supported (for now)")
                    }
                    std::cmp::Ordering::Equal => self.top_entry_id.seq_no + 1,
                    std::cmp::Ordering::Greater => 0,
                };

                Ok(StreamEntryId {
                    millis: now,
                    seq_no,
                })
            }
        }
    }

    #[inline]
    fn now() -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
    }
}
