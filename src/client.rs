use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime},
};

use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, BufReader},
    net::TcpStream,
    sync::RwLock,
};

use crate::{
    commands::{Command, RespMessage},
    server::ServerConfig,
    store::StoreValue,
};

pub struct Client<Read, Write>
where
    Read: AsyncBufReadExt + Unpin,
    Write: AsyncWrite + Unpin,
{
    read: Read,
    write: Write,
    store: Arc<RwLock<HashMap<String, StoreValue>>>,
    config: Arc<ServerConfig>,
}

impl<Read, Write> Client<Read, Write>
where
    Read: AsyncBufReadExt + Unpin,
    Write: AsyncWrite + Unpin,
{
    pub async fn run(mut self) -> anyhow::Result<()> {
        while let Ok(command) = Command::from_buffer(&mut self.read).await {
            println!("Received command: {:?}", command);
            match command {
                Command::Ping => {
                    RespMessage::SimpleString("PONG".into());
                }
                Command::Echo(message) => {
                    RespMessage::BulkString(message.clone());
                }
                Command::Set(key, value, options) => {
                    let mut expires_on = None;
                    if let Some(px) = options.px {
                        if let Some(t) = SystemTime::now().checked_add(Duration::from_millis(px)) {
                            expires_on = Some(t);
                        } else {
                            anyhow::bail!("Invalid px");
                        }
                    }
                    self.store
                        .write()
                        .await
                        .insert(key, StoreValue::new(value, expires_on));

                    self.write(RespMessage::SimpleString("OK".into())).await?;
                }
                Command::Get(key) => {
                    let r_store = self.store.read().await;
                    if let Some(entry) = r_store.get(&key) {
                        if let Some(expires_on) = entry.expires_on {
                            if expires_on < SystemTime::now() {
                                drop(r_store);
                                let mut w_store = self.store.write().await;
                                w_store.remove(&key);
                                drop(w_store);
                                self.write(RespMessage::Null).await?;
                                continue;
                            }
                        }
                        let value = entry.value.clone();
                        drop(r_store);
                        self.write(RespMessage::BulkString(value)).await?;
                    } else {
                        drop(r_store);
                        self.write(RespMessage::Null).await?;
                    }
                }
                Command::Config(action, params) => {
                    if action.eq_ignore_ascii_case("get") {
                        for key in &params {
                            if key.eq_ignore_ascii_case("dir") {
                                let dir = self
                                    .config
                                    .dir
                                    .as_ref()
                                    .map_or_else(|| String::new(), |s| s.clone());
                                self.write(RespMessage::Array(vec![
                                    RespMessage::BulkString("dir".into()),
                                    RespMessage::BulkString(dir),
                                ]))
                                .await;
                            } else if key.eq_ignore_ascii_case("dbfilename") {
                                let dbfilename = self
                                    .config
                                    .dbfilename
                                    .as_ref()
                                    .map_or_else(|| String::new(), |s| s.clone());
                                self.write(RespMessage::Array(vec![
                                    RespMessage::BulkString("dbfilename".into()),
                                    RespMessage::BulkString(dbfilename),
                                ]))
                                .await;
                            }
                        }
                    }
                }
            };
        }

        return Ok(());
    }

    async fn write(&mut self, message: RespMessage) -> anyhow::Result<()> {
        message.write(&mut self.write).await
    }
}

pub fn new_client(
    stream: TcpStream,
    store: Arc<RwLock<HashMap<String, StoreValue>>>,
    config: Arc<ServerConfig>,
) -> Client<tokio::io::BufReader<tokio::net::tcp::OwnedReadHalf>, tokio::net::tcp::OwnedWriteHalf> {
    let (read, write) = stream.into_split();
    let read = BufReader::new(read);
    Client {
        read,
        write,
        store,
        config,
    }
}
