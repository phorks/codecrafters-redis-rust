use core::str;
use std::{collections::HashMap, str::FromStr};

use tokio::io::{AsyncBufReadExt, Lines};

use crate::{
    info::{InfoSection, InfoSectionKind},
    redis::StringValue,
    resp::RespMessage,
    server::ServerConfig,
    streams::{StreamEntryId, XaddStreamEntryId, XreadStreamEntryId},
};

#[derive(Debug, Clone)]
pub struct SetCommandOptions {
    pub ex: Option<u64>, // seconds -- Set the specified expire time, in seconds (a positive integer).
    pub px: Option<u64>, // milliseconds -- Set the specified expire time, in milliseconds (a positive integer).
    pub exat: Option<u64>, // timestamp-seconds -- Set the specified Unix time at which the key will expire, in seconds (a positive integer).
    pub pxat: Option<u64>, // timestamp-milliseconds -- Set the specified Unix time at which the key will expire, in milliseconds (a positive integer).
    pub nx: bool,          // -- Only set the key if it does not already exist.
    pub _xx: bool,         // -- Only set the key if it already exists.
    pub keepttl: bool,     // -- Retain the time to live associated with the key.
    pub get: bool, // -- Return the old string stored at key, or nil if key did not exist. An error is returned and SET aborted if the value stored at key is not a string.
}

impl SetCommandOptions {
    pub fn from_rest_params(params: Vec<String>) -> anyhow::Result<SetCommandOptions> {
        let mut ex = None;
        let mut px = None;
        let mut exat = None;
        let mut pxat = None;
        let mut nx = false;
        let mut xx = false;
        let mut keepttl = false;
        let mut get = false;

        let mut it = params.iter();
        while let Some(param) = it.next() {
            match param.to_ascii_lowercase().as_ref() {
                "ex" => {
                    if let Some(val) = it.next().and_then(|next| next.parse::<u64>().ok()) {
                        ex = Some(val);
                    } else {
                        anyhow::bail!("Invalid ex parameter");
                    }
                }
                "px" => {
                    if let Some(val) = it.next().and_then(|next| next.parse::<u64>().ok()) {
                        px = Some(val);
                    } else {
                        anyhow::bail!("Invalid px parameter");
                    }
                }
                "exat" => {
                    if let Some(val) = it.next().and_then(|next| next.parse::<u64>().ok()) {
                        exat = Some(val);
                    } else {
                        anyhow::bail!("Invalid exat parameter");
                    }
                }
                "pxat" => {
                    if let Some(val) = it.next().and_then(|next| next.parse::<u64>().ok()) {
                        pxat = Some(val);
                    } else {
                        anyhow::bail!("Invalid pxat parameter");
                    }
                }
                "nx" => {
                    nx = true;
                }
                "xx" => {
                    xx = true;
                }
                "keepttl" => {
                    keepttl = true;
                }
                "get" => {
                    get = true;
                }
                _ => anyhow::bail!("Unknown SET option: {}", param),
            }
        }

        Ok(SetCommandOptions {
            ex,
            px,
            exat,
            pxat,
            nx,
            _xx: xx,
            keepttl,
            get,
        })
    }

    pub fn append_to_vec(&self, mut lines: &mut Vec<RespMessage>) {
        Self::append_option("ex", &self.ex, &mut lines);
        Self::append_option("px", &self.px, &mut lines);
        Self::append_option("exat", &self.exat, &mut lines);
        Self::append_option("pxat", &self.pxat, &mut lines);

        Self::append_bool("nx", self.nx, &mut lines);
        Self::append_bool("keepttl", self.keepttl, &mut lines);
        Self::append_bool("get", self.get, &mut lines);
        Self::append_bool("get", self.nx, &mut lines);
    }

    fn append_option(key: &'static str, value: &Option<u64>, options: &mut Vec<RespMessage>) {
        if let Some(value) = value {
            options.push(RespMessage::bulk_from_str(key));
            options.push(RespMessage::BulkString(value.to_string()));
        }
    }

    fn append_bool(key: &'static str, value: bool, options: &mut Vec<RespMessage>) {
        if value {
            options.push(RespMessage::bulk_from_str(key));
        }
    }
}

#[derive(Debug, Clone)]
pub enum InfoCommandParameter {
    Single(InfoSectionKind),
    All,
}

impl FromStr for InfoCommandParameter {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "replication" => Ok(InfoCommandParameter::Single(InfoSectionKind::Replication)),
            "all" => Ok(InfoCommandParameter::All),
            _ => anyhow::bail!("Unknown INFO section name"),
        }
    }
}

impl InfoCommandParameter {
    pub async fn get_sections(&self, config: &ServerConfig) -> Vec<InfoSection> {
        match self {
            InfoCommandParameter::Single(kind) => vec![kind.get_info(config).await],
            InfoCommandParameter::All => {
                let mut sections = vec![];
                for kind in InfoSectionKind::iter() {
                    sections.push(kind.get_info(config).await);
                }
                sections
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum ReplCapability {
    Psync2,
    Eof,
}

impl ToString for ReplCapability {
    fn to_string(&self) -> String {
        match self {
            ReplCapability::Psync2 => String::from("psync2"),
            ReplCapability::Eof => String::from("eof"),
        }
    }
}

impl FromStr for ReplCapability {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_ref() {
            "psync2" => Ok(ReplCapability::Psync2),
            "eof" => Ok(ReplCapability::Eof),
            _ => anyhow::bail!("Unknown cap {}", value),
        }
    }
}

#[derive(Debug, Clone)]
pub enum ReplConfData {
    ListeningPort(u16),
    Capability(ReplCapability),
    GetAck,
    Ack(usize),
}

impl<T: AsRef<str>> TryFrom<(&T, &T)> for ReplConfData {
    type Error = anyhow::Error;

    fn try_from(value: (&T, &T)) -> Result<Self, Self::Error> {
        match value.0.as_ref().to_ascii_lowercase().as_str() {
            "listening-port" => Ok(ReplConfData::ListeningPort(value.1.as_ref().parse()?)),
            "capa" => Ok(ReplConfData::Capability(value.1.as_ref().parse()?)),
            "getack" if value.1.as_ref() == "*" => Ok(ReplConfData::GetAck),
            "ack" => Ok(ReplConfData::Ack(value.1.as_ref().parse()?)),
            _ => anyhow::bail!(
                "Invalid replconf data. Key: {}, Value: {}",
                value.0.as_ref(),
                value.1.as_ref()
            ),
        }
    }
}

#[derive(Debug, Clone)]
pub enum XreadBlocking {
    Block(u64),
    BlockIndefinitely,
}

#[derive(Debug, Clone)]
pub enum Command {
    Ping,
    Echo(String),
    Set(String, StringValue, SetCommandOptions),
    Get(String),
    Config(String, Vec<String>),
    Keys(String),
    Info(InfoCommandParameter),
    ReplConf(Vec<ReplConfData>),
    Psync(String, i32),
    Wait(u32, u32),
    Type(String),
    Xadd(String, XaddStreamEntryId, HashMap<String, String>),
    Xrange(String, StreamEntryId, StreamEntryId),
    Xread(Vec<(String, XreadStreamEntryId)>, Option<XreadBlocking>),
    Incr(String),
    Multi,
}

impl Command {
    pub async fn from_buffer<T: AsyncBufReadExt + Unpin>(read: &mut T) -> anyhow::Result<Command> {
        // TODO: use RespMessage.read since the input is already a RespMessage::Array
        async fn read_param<T: AsyncBufReadExt + Unpin>(
            lines: &mut Lines<T>,
        ) -> anyhow::Result<String> {
            if let Some(_) = lines.next_line().await? {
                // println!("Received line: {}", bytes_line);
            }

            if let Some(value_line) = lines.next_line().await? {
                // println!("Received line: {}", value_line);
                Ok(value_line)
            } else {
                Ok(String::new())
            }
        }

        async fn read_rest_params<T: AsyncBufReadExt + Unpin>(
            lines: &mut Lines<T>,
            n: usize,
        ) -> anyhow::Result<Vec<String>> {
            let mut params = Vec::with_capacity(n);
            for _ in 0..n {
                params.push(read_param(lines).await?);
            }

            Ok(params)
        }

        let mut lines = read.lines();
        let Some(params_line) = lines.next_line().await? else {
            anyhow::bail!("Empty params line")
        };

        // println!("Received line: {}", params_line);
        let n_params = params_line.as_str()[1..].parse::<usize>()? - 1;

        let name = read_param(&mut lines).await?;

        match name.to_ascii_lowercase().as_ref() {
            "ping" => {
                if n_params != 0 {
                    anyhow::bail!("Incorrect number of parameters for ping command");
                }

                return Ok(Command::Ping);
            }
            "echo" => {
                if n_params != 1 {
                    anyhow::bail!(
                        "Incorrect number of parameters, expected: 2, received {}",
                        n_params
                    );
                }

                return Ok(Command::Echo(read_param(&mut lines).await?));
            }
            "set" => {
                if n_params < 2 {
                    anyhow::bail!(
                        "Incorrect number of parameters, expected: at least 2, received {}",
                        n_params
                    );
                }

                let key = read_param(&mut lines).await?;
                let value = read_param(&mut lines).await?;

                let value = match value.parse::<u32>() {
                    Ok(i) => StringValue::Integer(i),
                    Err(_) => StringValue::String((value).into()),
                };

                let rest_params = read_rest_params(&mut lines, n_params - 2).await?;
                let options = SetCommandOptions::from_rest_params(rest_params)?;

                return Ok(Command::Set(key, value, options));
            }
            "get" => {
                if n_params == 0 {
                    anyhow::bail!(
                        "Incorrect number of parameters, expected: at least 1, received 0"
                    );
                }

                let key = read_param(&mut lines).await?;

                return Ok(Command::Get(key));
            }
            "config" => {
                if n_params == 0 {
                    anyhow::bail!("Missing CONFIG command action parameter");
                }

                let action = read_param(&mut lines).await?;
                let params = read_rest_params(&mut lines, n_params - 1).await?;

                return Ok(Command::Config(action, params));
            }
            "keys" => {
                if n_params == 0 {
                    anyhow::bail!("Missing the PATTERN arg")
                }

                let pattern = read_param(&mut lines).await?;

                return Ok(Command::Keys(pattern));
            }
            "info" => {
                let section_name = if n_params > 0 {
                    let next = read_param(&mut lines).await?;
                    InfoCommandParameter::from_str(&next)?
                } else {
                    InfoCommandParameter::All
                };

                return Ok(Command::Info(section_name));
            }
            "replconf" => {
                if n_params == 0 {
                    anyhow::bail!("Missing replconf parameters");
                }

                if n_params % 2 == 1 {
                    anyhow::bail!("Invalid number of replconf parameters.");
                }

                let rest = read_rest_params(&mut lines, n_params).await?;

                let mut capas: Vec<ReplConfData> = vec![];
                for kv in rest.chunks(2) {
                    capas.push((&kv[0], &kv[1]).try_into()?);
                }

                Ok(Command::ReplConf(capas))
            }
            "psync" => {
                if n_params != 2 {
                    anyhow::bail!(
                        "Incorrect number of parameters for PSYNC (required 2, received {})",
                        n_params
                    )
                }

                let replid = read_param(&mut lines).await?;
                let repl_offset = read_param(&mut lines).await?.parse()?;

                Ok(Command::Psync(replid, repl_offset))
            }
            "wait" => {
                if n_params != 2 {
                    anyhow::bail!(
                        "Incorrect number of arguments for WAIT (required 2, received {})",
                        n_params
                    )
                }

                let num_replicas = read_param(&mut lines).await?.parse()?;
                let timeout = read_param(&mut lines).await?.parse()?;

                Ok(Command::Wait(num_replicas, timeout))
            }
            "type" => {
                if n_params != 1 {
                    anyhow::bail!(
                        "Incorrect number of arguments for TYPE (required 1, received {})",
                        n_params
                    )
                }

                let key = read_param(&mut lines).await?;

                Ok(Command::Type(key))
            }
            "xadd" => {
                if n_params < 2 {
                    anyhow::bail!(
                        "Incorrect number of arguments for XADD (required at least 2, received {})",
                        n_params
                    )
                }

                let key = read_param(&mut lines).await?;
                let entry_id = read_param(&mut lines).await?.parse()?;

                let mut n_values = n_params - 2;

                if n_values % 2 != 0 {
                    anyhow::bail!("Incorrect key-value pairs format.")
                }

                n_values /= 2;

                let mut values = HashMap::new();
                for _ in 0..n_values {
                    let key = read_param(&mut lines).await?;
                    let value = read_param(&mut lines).await?;
                    values.insert(key, value);
                }

                Ok(Command::Xadd(key, entry_id, values))
            }
            "xrange" => {
                if n_params != 3 {
                    anyhow::bail!(
                        "Incorrect number of arguments for XRANGE (required 3, received {})",
                        n_params
                    )
                }

                let key = read_param(&mut lines).await?;
                let start = StreamEntryId::parse_as_range_start(&read_param(&mut lines).await?)?;
                let end = StreamEntryId::parse_as_range_end(&read_param(&mut lines).await?)?;

                Ok(Command::Xrange(key, start, end))
            }
            "xread" => {
                let mut n_read = 0;
                let mut blocking = None;
                loop {
                    let param = read_param(&mut lines).await?;

                    match param.to_ascii_lowercase().as_str() {
                        "block" => {
                            let millis = read_param(&mut lines).await?.parse()?;
                            blocking = Some(match millis {
                                0 => XreadBlocking::BlockIndefinitely,
                                _ => XreadBlocking::Block(millis),
                            });

                            n_read += 1;
                        }
                        _ => {}
                    }

                    n_read += 1;
                    if param == "streams" {
                        break;
                    }
                }

                let rest = read_rest_params(&mut lines, n_params - n_read).await?;

                if rest.len() % 2 != 0 {
                    anyhow::bail!(
                        "Incorrect number of stream_key+ids for XREAD (required even amount, received {})",
                        rest.len()
                    )
                }

                let mut stream_afters = vec![];

                let mid = rest.len() / 2;
                for i in 0..mid {
                    stream_afters.push((rest[i].clone(), rest[mid + i].parse()?))
                }

                Ok(Command::Xread(stream_afters, blocking))
            }
            "incr" => {
                if n_params != 1 {
                    anyhow::bail!(
                        "Incorrect number of arguments for INCR (required 1, received {})",
                        n_params
                    )
                }

                Ok(Command::Incr(read_param(&mut lines).await?))
            }
            "multi" => {
                if n_params != 0 {
                    anyhow::bail!(
                        "Incorrect number of arguments for MULTI (required 0, received {})",
                        n_params
                    )
                }

                Ok(Command::Multi)
            }
            _ => anyhow::bail!("Unknown command"),
        }
    }

    pub fn to_resp(&self) -> anyhow::Result<RespMessage> {
        let lines = match self {
            Command::Ping => {
                vec![RespMessage::bulk_from_str("PING")]
            }
            Command::ReplConf(confs) => {
                let mut lines = Vec::with_capacity(3);
                lines.push(RespMessage::bulk_from_str("REPLCONF"));
                for data in confs {
                    match data {
                        ReplConfData::ListeningPort(port) => {
                            lines.push(RespMessage::bulk_from_str("listening-port"));
                            lines.push(RespMessage::BulkString(port.to_string()));
                        }
                        ReplConfData::Capability(capa) => {
                            lines.push(RespMessage::bulk_from_str("capa"));
                            lines.push(RespMessage::BulkString(capa.to_string()));
                        }
                        ReplConfData::GetAck => {
                            lines.push(RespMessage::bulk_from_str("GETACK"));
                            lines.push(RespMessage::bulk_from_str("*"));
                        }
                        ReplConfData::Ack(offset) => {
                            lines.push(RespMessage::bulk_from_str("ACK"));
                            lines.push(RespMessage::BulkString(offset.to_string()));
                        }
                    }
                }
                lines
            }
            Command::Psync(replid, repl_offset) => {
                vec![
                    RespMessage::bulk_from_str("PSYNC"),
                    RespMessage::BulkString(replid.clone()),
                    RespMessage::BulkString(repl_offset.to_string()),
                ]
            }
            Command::Set(key, value, options) => {
                let mut lines = vec![
                    RespMessage::bulk_from_str("SET"),
                    RespMessage::BulkString(key.clone()),
                    RespMessage::BulkString(value.to_string()),
                ];

                options.append_to_vec(&mut lines);

                lines
            }
            _ => anyhow::bail!("Other commands are not supported"),
        };

        Ok(RespMessage::Array(lines))
    }
}
