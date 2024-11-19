use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime},
};

use tokio::{
    fs,
    io::{AsyncBufReadExt, AsyncWrite, BufReader},
    net::TcpStream,
    sync::RwLock,
};

use crate::{
    commands::{Command, InfoCommandParameter},
    redis::{Database, DatabaseEntry, Expiry, Instance, StringValue},
    resp::RespMessage,
    server::ServerConfig,
};

pub struct Client<Read: AsyncBufReadExt + Unpin, Write: AsyncWrite + Unpin> {
    read: Read,
    write: Write,
    store: Arc<RwLock<Database>>,
    config: Arc<ServerConfig>,
}

impl<Read: AsyncBufReadExt + Unpin, Write: AsyncWrite + Unpin> Client<Read, Write> {
    pub async fn run(mut self) -> anyhow::Result<()> {
        while let Ok(command) = Command::from_buffer(&mut self.read).await {
            println!("Received command: {:?}", command);
            match command {
                Command::Ping => {
                    self.write(RespMessage::SimpleString("PONG".into())).await?;
                }
                Command::Echo(message) => {
                    self.write(RespMessage::BulkString(message.clone())).await?;
                }
                Command::Set(key, value, options) => {
                    let mut expires_on = None;
                    if let Some(px) = options.px {
                        if let Some(t) = SystemTime::now().checked_add(Duration::from_millis(px)) {
                            let millis = t.duration_since(SystemTime::UNIX_EPOCH)?.as_millis();
                            expires_on = Some(Expiry::InMillis(millis as u64))
                        } else {
                            anyhow::bail!("Invalid px");
                        }
                    }
                    self.store
                        .write()
                        .await
                        .entries
                        .insert(key.into(), DatabaseEntry::new((&value).into(), expires_on));

                    self.write(RespMessage::SimpleString("OK".into())).await?;
                }
                Command::Get(key) => {
                    let r_store = self.store.read().await;
                    let key = &key.into();
                    if let Some(entry) = r_store.entries.get(key) {
                        if let Some(expires_on) = entry.get_expiry_time() {
                            if expires_on < SystemTime::now() {
                                drop(r_store);
                                let mut w_store = self.store.write().await;
                                w_store.entries.remove(key);
                                drop(w_store);
                                self.write(RespMessage::Null).await?;
                                continue;
                            }
                        }
                        let value = entry.value.to_string();
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
                                .await?;
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
                                .await?;
                            }
                        }
                    }
                }
                Command::Keys(pattern) => {
                    if pattern == "*" || pattern == "\"*\"" {
                        let r_store = self.store.read().await;

                        let keys = r_store
                            .entries
                            .keys()
                            .map(|x| RespMessage::BulkString(x.to_string()))
                            .collect();

                        drop(r_store);

                        self.write(RespMessage::Array(keys)).await?;
                    }
                    // if pattern == "*" || pattern == "\"*\"" {
                    //     let Some(db_path) = self.config.db_path() else {
                    //         anyhow::bail!("Database file is not specified.")
                    //     };
                    //
                    //     let mut file = fs::File::open(db_path).await?;
                    //     let rdb = Instance::new(&mut file).await?;
                    //     let keys = rdb
                    //         .dbs
                    //         .values()
                    //         .flat_map(|x| x.entries.keys())
                    //         .map(|x| RespMessage::BulkString(x.to_string()))
                    //         .collect();
                    //
                    //     self.write(RespMessage::Array(keys)).await?;
                    // }
                }
                Command::Info(param) => {
                    let mut resp = String::new();
                    let sections = param.get_sections(&self.config);
                    for section in sections {
                        section.write(&mut resp)?;
                    }

                    self.write(RespMessage::BulkString(resp)).await?;
                }
                Command::ReplConf(_) => {
                    self.write(RespMessage::simple_from_str("OK")).await?;
                }
                Command::Psync(_, _) => todo!(),
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
    store: Arc<RwLock<Database>>,
    config: Arc<ServerConfig>,
) -> Client<tokio::io::BufReader<tokio::net::tcp::OwnedReadHalf>, tokio::net::tcp::OwnedWriteHalf> {
    // TODO: move this to Client impl maybe?
    let (read, write) = stream.into_split();
    let read = BufReader::new(read);
    Client {
        read,
        write,
        store,
        config,
    }
}
