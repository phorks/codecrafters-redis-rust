use core::str;
use std::collections::{BTreeMap, HashMap};

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};

use crate::{
    io_helper::{read_until_crlf, skip_crlf},
    streams::StreamEntryId,
};

#[derive(Debug)]
pub enum RespMessage {
    Array(Vec<RespMessage>),
    BulkString(String),
    Null,
    SimpleString(String),
    Integer(i64),
    SimpleError(String),
}

pub trait ToResp {
    fn to_resp(&self) -> RespMessage;
}

impl ToResp for &str {
    fn to_resp(&self) -> RespMessage {
        RespMessage::bulk_from_str(&self.as_ref())
    }
}

impl ToResp for String {
    fn to_resp(&self) -> RespMessage {
        RespMessage::BulkString(self.clone())
    }
}

impl ToResp for StreamEntryId {
    fn to_resp(&self) -> RespMessage {
        self.to_string().to_resp()
    }
}

impl<K: ToResp, V: ToResp> ToResp for HashMap<K, V> {
    fn to_resp(&self) -> RespMessage {
        let items = self
            .into_iter()
            .flat_map(|x| [x.0.to_resp(), x.1.to_resp()])
            .collect();

        RespMessage::Array(items)
    }
}

impl<K: ToResp, V: ToResp> ToResp for BTreeMap<K, V> {
    fn to_resp(&self) -> RespMessage {
        let items = self
            .into_iter()
            .flat_map(|x| [x.0.to_resp(), x.1.to_resp()])
            .collect();

        RespMessage::Array(items)
    }
}

impl<V: ToResp> ToResp for Vec<V> {
    fn to_resp(&self) -> RespMessage {
        let items = self.into_iter().map(|x| x.to_resp()).collect();
        RespMessage::Array(items)
    }
}

impl RespMessage {
    pub fn bulk_from_str(value: &str) -> Self {
        RespMessage::BulkString(String::from(value))
    }

    pub fn simple_from_str(value: &str) -> Self {
        RespMessage::SimpleString(String::from(value))
    }

    pub fn count_in_bytes(&self) -> usize {
        match self {
            RespMessage::Array(items) => {
                let mut n = 1 + items.len().to_string().len() + 2;
                for item in items {
                    n += item.count_in_bytes();
                }
                n
            }
            RespMessage::BulkString(s) => {
                let bytes = s.as_bytes();
                1 + bytes.len().to_string().len() + 2 + bytes.len() + 2
            }
            RespMessage::Null => 5,
            RespMessage::SimpleString(s) => 1 + s.as_bytes().len() + 2,
            RespMessage::Integer(i) => 1 + i.to_string().len() + 2,
            RespMessage::SimpleError(s) => 1 + s.as_bytes().len() + 2,
        }
    }

    pub async fn write<T: AsyncWriteExt + Unpin>(&self, write: &mut T) -> anyhow::Result<()> {
        match self {
            RespMessage::Array(items) => {
                write.write_all(b"*").await?;
                write.write_all(items.len().to_string().as_bytes()).await?;
                write.write_all(b"\r\n").await?;
                for item in items {
                    Box::pin(item.write(write)).await?;
                }
            }
            RespMessage::BulkString(s) => {
                let bytes = s.as_bytes();

                write.write_all(b"$").await?;
                write.write_all(bytes.len().to_string().as_bytes()).await?;
                write.write_all(b"\r\n").await?;
                write.write_all(bytes).await?;
                write.write_all(b"\r\n").await?;
            }
            RespMessage::Null => {
                write.write_all(b"$-1\r\n").await?;
            }
            RespMessage::SimpleString(s) => {
                write.write_all(b"+").await?;
                write.write_all(s.as_bytes()).await?;
                write.write_all(b"\r\n").await?;
            }
            RespMessage::Integer(i) => {
                write.write_all(b":").await?;
                write.write_all(i.to_string().as_bytes()).await?;
                write.write_all(b"\r\n").await?;
            }
            RespMessage::SimpleError(s) => {
                write.write_all(format!("-{}\r\n", s).as_bytes()).await?;
            }
        }

        write.flush().await?;
        Ok(())
    }

    pub async fn read<R: AsyncBufReadExt + Unpin>(read: &mut R) -> anyhow::Result<RespMessage> {
        let disc = read.read_u8().await?;
        match disc {
            b'*' => {
                // Array
                let length = str::from_utf8(&read_until_crlf(read).await?)?.parse::<usize>()?;
                let mut items = Vec::with_capacity(length);
                for _ in 0..length {
                    let item = Box::pin(Self::read(read)).await?;
                    items.push(item);
                }

                Ok(RespMessage::Array(items))
            }
            b'$' => {
                let line_bytes = read_until_crlf(read).await?;
                let line = str::from_utf8(&line_bytes)?;

                if line == "-1" {
                    // Null
                    Ok(RespMessage::Null)
                } else {
                    // BulkString
                    let length = line.parse::<usize>()?;
                    let mut buf = vec![0u8; length];
                    read.read_exact(&mut buf).await?;
                    skip_crlf(read).await?;
                    Ok(RespMessage::BulkString(
                        String::from_utf8_lossy(&buf).into_owned(),
                    ))
                }
            }
            b'+' => {
                // SimpleString
                let line = String::from_utf8(read_until_crlf(read).await?)?;
                Ok(RespMessage::SimpleString(line))
            }
            _ => anyhow::bail!("Invalid RESP first character"),
        }
    }
}
