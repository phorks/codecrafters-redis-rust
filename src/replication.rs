use core::str;
use std::{
    io::BufRead,
    net::{SocketAddr, SocketAddrV4},
    sync::Arc,
};

use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
    sync::RwLock,
};

use crate::{
    commands::{Command, ReplCapability, ReplConfData},
    io_helper::{read_until_crlf, skip_sequence},
    redis::Database,
    resp::RespMessage,
    server::{ServerConfig, ServerRole},
};

pub struct MasterConnection {
    pub read: BufReader<OwnedReadHalf>,
    pub write: OwnedWriteHalf,
    pub addr: SocketAddrV4,
    pub rdb: Vec<u8>,
}

pub struct ReplicationChannel<Read: AsyncBufReadExt + Unpin, Write: AsyncWriteExt + Unpin> {
    read: Read,
    write: Write,
    addr: SocketAddr,
    store: Arc<Database>,
    config: Arc<ServerConfig>,
}

impl<Read: AsyncBufReadExt + Unpin, Write: AsyncWriteExt + Unpin> ReplicationChannel<Read, Write> {
    pub async fn run(mut self) -> anyhow::Result<()> {
        while let Ok(command) = Command::from_buffer(&mut self.read).await {
            match command {
                Command::Set(key, value, options) => {
                    let resp = self.store.set(key, value, options).await?;
                    resp.write(&mut self.write).await?;
                }
                Command::ReplConf(confs) => {
                    for conf in confs {
                        match conf {
                            ReplConfData::GetAck => {
                                Command::ReplConf(vec![ReplConfData::Ack(0)])
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

impl ReplicationChannel<BufReader<OwnedReadHalf>, OwnedWriteHalf> {
    pub fn new(
        read: BufReader<OwnedReadHalf>,
        write: OwnedWriteHalf,
        addr: SocketAddr,
        store: Arc<Database>,
        config: Arc<ServerConfig>,
    ) -> Self {
        Self {
            read,
            write,
            addr,
            store,
            config,
        }
    }
}

pub async fn connect_to_master(config: &ServerConfig) -> anyhow::Result<Option<MasterConnection>> {
    let ServerRole::Slave(master_addr) = config.role else {
        return Ok(None);
    };

    let stream = TcpStream::connect(master_addr).await?;
    let (read, mut write) = stream.into_split();
    let mut read = BufReader::new(read);

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

    Ok(Some(MasterConnection {
        read,
        write,
        addr: master_addr,
        rdb: buf,
    }))
}
