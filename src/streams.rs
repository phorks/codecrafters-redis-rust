use std::{
    collections::{hash_map, BTreeMap, BinaryHeap, HashMap},
    fmt,
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};

use tokio::sync::mpsc;

use crate::{
    resp::RespMessage,
    resp_ext::{ToMapRespArray, ToStringResp},
};

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
            None => Ok(Self::new(s.parse()?, default_seq_no)),
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
            "+" => Ok(Self::new(u64::MAX, u64::MAX)),
            _ => Self::parse_as_range_bound(s, u64::MAX),
        }
    }

    pub const MAX: Self = Self {
        millis: u64::MAX,
        seq_no: u64::MAX,
    };

    pub fn is_min(&self) -> bool {
        self.millis == 0 && self.seq_no == 0
    }
}

impl ToString for StreamEntryId {
    fn to_string(&self) -> String {
        format!("{}-{}", self.millis, self.seq_no)
    }
}

impl FromStr for StreamEntryId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (millis_str, seq_no_str) = s.split_once('-').ok_or_else(|| {
            anyhow::anyhow!(
                "Invalid entry ID format (it must consist of two parts separated by a '-'"
            )
        })?;

        let millis = millis_str.parse()?;
        let seq_no = seq_no_str.parse()?;

        Ok(Self::new(millis, seq_no))
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

#[derive(Debug, Clone)]
pub enum XreadStreamEntryId {
    Id(StreamEntryId),
    Future,
}

impl FromStr for XreadStreamEntryId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "$" {
            Ok(Self::Future)
        } else {
            Ok(Self::Id(s.parse()?))
        }
    }
}

#[derive(Debug)]
pub struct StreamQueryResponse(BTreeMap<StreamEntryId, HashMap<String, String>>);

impl StreamQueryResponse {
    pub fn empty() -> Self {
        Self(BTreeMap::new())
    }

    pub fn to_resp(&self) -> RespMessage {
        self.0
            .iter()
            .map(|x| {
                (
                    x.0.to_bulk_string(),
                    x.1.iter()
                        .map(|y| (y.0.to_bulk_string(), y.1.to_bulk_string()))
                        .to_flattened_map_resp_array(),
                )
            })
            .to_map_resp_array()
    }

    pub fn is_empty(&self) -> bool {
        self.0.len() == 0
    }
}

#[derive(Clone)]
pub struct StreamValue(HashMap<StreamEntryId, HashMap<String, String>>);

impl StreamValue {
    pub async fn get_entry_value(&self, entry_id: &StreamEntryId, key: &str) -> Option<&str> {
        let entry = self.0.get(entry_id)?;
        entry.get(key).map(|x| x.as_str())
    }
}

pub struct StreamRecord {
    id: String,
    value: StreamValue,
    top_entry_id: StreamEntryId,
    listeners: Vec<(StreamEntryId, mpsc::Sender<(String, StreamQueryResponse)>)>,
}

impl StreamRecord {
    pub fn new(id: String) -> Self {
        Self {
            id,
            value: StreamValue(HashMap::new()),
            top_entry_id: StreamEntryId::default(),
            listeners: vec![],
        }
    }

    pub fn value(&self) -> &StreamValue {
        &self.value
    }

    pub async fn add_entry(
        &mut self,
        entry_id: StreamEntryId,
        values: HashMap<String, String>,
    ) -> anyhow::Result<()> {
        self.add_entry_unchecked(entry_id, values).await
    }

    pub async fn xadd(
        &mut self,
        entry_id: XaddStreamEntryId,
        values: HashMap<String, String>,
    ) -> anyhow::Result<StreamEntryId> {
        let entry_id = self
            .generate_entry_id(entry_id)
            .map_err(|e| anyhow::anyhow!(e))?;

        self.add_entry_unchecked(entry_id.clone(), values).await?;
        Ok(entry_id)
    }

    pub fn get_entries_in_range(
        &self,
        start: &StreamEntryId,
        end: &StreamEntryId,
        start_exclusive: bool,
    ) -> StreamQueryResponse {
        // FIXME this is approach is inefficient. It is of O(n + mlogm) where n is
        // the total number of entries in the stream and m is the number of entries in the given range.
        // Redis does it in O(m), while keeping entry insertion in O(1) by leveraging Radix tries
        let mut map = BTreeMap::new();

        for (key, value) in &self.value.0 {
            if (key >= start && (!start_exclusive || key != start)) && key <= end {
                map.insert(key.clone(), value.clone());
            }
        }

        StreamQueryResponse(map)
    }

    pub fn subscribe(
        &mut self,
        entry_id: XreadStreamEntryId,
        tx: mpsc::Sender<(String, StreamQueryResponse)>,
    ) -> anyhow::Result<()> {
        let entry_id = match entry_id {
            XreadStreamEntryId::Id(entry_id) => entry_id,
            XreadStreamEntryId::Future => self.top_entry_id.clone(),
        };

        if entry_id < self.top_entry_id {
            anyhow::bail!("The data is available. There is no need for subscriptions.")
        }

        self.listeners.push((entry_id, tx));
        Ok(())
    }

    async fn add_entry_unchecked(
        &mut self,
        entry_id: StreamEntryId,
        values: HashMap<String, String>,
    ) -> anyhow::Result<()> {
        match self.value.0.entry(entry_id.clone()) {
            hash_map::Entry::Occupied(_) => anyhow::bail!("A record with the same key exists."),
            hash_map::Entry::Vacant(v) => {
                v.insert(values.clone());
            }
        }

        self.top_entry_id = entry_id.clone();

        let old_listeners = std::mem::replace(&mut self.listeners, vec![]);
        let mut new_listeners = Vec::with_capacity(self.listeners.len());

        for (waiting_for, tx) in old_listeners.into_iter() {
            if entry_id >= waiting_for {
                println!("Sending {}", entry_id.clone().to_string());
                let _ = tx
                    .send((
                        self.id.clone(),
                        StreamQueryResponse([(entry_id.clone(), values.clone())].into()),
                    ))
                    .await;
            } else {
                new_listeners.push((waiting_for, tx));
            }
        }

        self.listeners = new_listeners;

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
