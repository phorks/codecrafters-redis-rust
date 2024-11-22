use core::str;
use std::{
    net::{SocketAddr, SocketAddrV4},
    sync::Arc,
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    net::{tcp::OwnedWriteHalf, TcpStream},
};

use crate::{
    commands::{Command, ReplCapability, ReplConfData},
    io_helper::{read_until_crlf, skip_sequence, CountingBufReader, Reader},
    redis::Database,
    resp::RespMessage,
    server::{ServerConfig, ServerRole},
};

pub struct MasterConnection {
    pub read: Reader,
    pub write: OwnedWriteHalf,
    pub addr: SocketAddrV4,
    pub _rdb: Vec<u8>,
}

pub struct ReplicationChannel<Write: AsyncWriteExt + Unpin> {
    read: Reader,
    write: Write,
    _addr: SocketAddr,
    store: Arc<Database>,
    _config: Arc<ServerConfig>,
}

impl<Write: AsyncWriteExt + Unpin> ReplicationChannel<Write> {
    pub async fn run(mut self) -> anyhow::Result<()> {
        loop {
            let mut counting = self.read.new_counting_scope();
            let Ok(command) = Command::from_buffer(&mut counting.reader()).await else {
                break;
            };

            let bytes_read = counting.count_so_far();
            drop(counting);

            match command {
                Command::Ping => {
                    // master is alive :)
                }
                Command::Set(key, value, options) => {
                    _ = self.store.set(key, value, options).await?;
                }
                Command::ReplConf(confs) => {
                    for conf in confs {
                        match conf {
                            ReplConfData::GetAck => {
                                let bytes_before_this = self.read.total_bytes() - bytes_read;
                                Command::ReplConf(vec![ReplConfData::Ack(bytes_before_this)])
                                    .to_resp()?
                                    .write(&mut self.write)
                                    .await?;
                            }
                            _ => anyhow::bail!("Only GETACK REPLCONF is allowed"),
                        }
                    }
                }
                _ => anyhow::bail!("Only SET and GETACK REPLCONF are supported"),
            }
        }

        Ok(())
    }
}

impl ReplicationChannel<OwnedWriteHalf> {
    pub fn new(
        read: Reader,
        write: OwnedWriteHalf,
        addr: SocketAddr,
        store: Arc<Database>,
        config: Arc<ServerConfig>,
    ) -> Self {
        Self {
            read,
            write,
            _addr: addr,
            store,
            _config: config,
        }
    }
}

pub async fn connect_to_master(config: &ServerConfig) -> anyhow::Result<Option<MasterConnection>> {
    let ServerRole::Slave(master_addr) = config.role else {
        return Ok(None);
    };

    let stream = TcpStream::connect(master_addr).await?;
    let (read, mut write) = stream.into_split();
    let mut read = CountingBufReader::new(BufReader::new(read));

    Command::Ping.to_resp()?.write(&mut write).await?;

    skip_sequence(&mut read, "+PONG\r\n".as_bytes()).await?;

    Command::ReplConf(vec![ReplConfData::ListeningPort(config.port())])
        .to_resp()?
        .write(&mut write)
        .await?;

    skip_sequence(&mut read, "+OK\r\n".as_bytes()).await?;

    Command::ReplConf(vec![ReplConfData::Capability(ReplCapability::Psync2)])
        .to_resp()?
        .write(&mut write)
        .await?;

    skip_sequence(&mut read, b"+OK\r\n").await?;

    Command::Psync(String::from("?"), -1)
        .to_resp()?
        .write(&mut write)
        .await?;

    let sync_resp = RespMessage::read(&mut read).await?;

    // FULLRESYNC <MASTER replid> <MASTER repl_offset>
    match sync_resp {
        RespMessage::SimpleString(_) => (),
        _ => anyhow::bail!("Expected a SimpleString"),
    };

    // RDB file in a format similar to BulkString without the trailing \r\n
    skip_sequence(&mut read, b"$").await?;
    let length = read_until_crlf(&mut read).await?;
    let length = str::from_utf8(&length)?.parse()?;
    let mut buf = vec![0u8; length];
    read.read_exact(&mut buf).await?;

    read.reset_counting().unwrap();

    Ok(Some(MasterConnection {
        read,
        write,
        addr: master_addr,
        _rdb: buf,
    }))
}
