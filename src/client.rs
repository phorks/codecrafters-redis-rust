use std::{
    borrow::BorrowMut,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, SystemTime},
};

use tokio::{
    io::{AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
    sync::{mpsc, RwLock},
};

use crate::{
    commands::{Command, ReplCapability, ReplConfData},
    redis::{Database, DatabaseEntry, Expiry, EMPTY_RDB},
    resp::RespMessage,
    server::{ServerConfig, ServerRole, SlaveClientInfo},
};

enum SlaveHandshakeState {
    PingReceived,
    PortReceived(u16),
    CapaReceived(u16, Vec<ReplConfData>),
    Ready(u16, Vec<ReplConfData>),
}

pub struct Client<Read: AsyncBufReadExt + Unpin, Write: AsyncWriteExt + Unpin> {
    read: Read,
    write: Write,
    addr: SocketAddr,
    store: Arc<Database>,
    config: Arc<ServerConfig>,
}

impl<Read: AsyncBufReadExt + Unpin, Write: AsyncWrite + Unpin> Client<Read, Write> {
    pub async fn run(mut self) -> anyhow::Result<()> {
        let mut n_commands = 0;
        let mut slave_state: Option<(u64, SlaveHandshakeState)> = None;

        while let Ok(command) = Command::from_buffer(&mut self.read).await {
            println!("Received command: {:?}", command);
            match command {
                Command::Ping => {
                    self.write(RespMessage::SimpleString("PONG".into())).await?;
                    slave_state = Some((n_commands, SlaveHandshakeState::PingReceived));
                }
                Command::Echo(message) => {
                    self.write(RespMessage::BulkString(message.clone())).await?;
                }
                Command::Set(key, value, options) => {
                    let resp = self
                        .store
                        .set(key.clone(), value.clone(), options.clone())
                        .await?;

                    if let ServerRole::Master(master_info) = &self.config.role {
                        let slaves = master_info.slaves.read().await;
                        for slave in slaves.iter() {
                            slave.tx.send(Command::Set(
                                key.clone(),
                                value.clone(),
                                options.clone(),
                            ))?;
                        }
                    }

                    self.write(resp).await?;
                }
                Command::Get(key) => {
                    let resp = self.store.get(key).await?;
                    self.write(resp).await?;
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
                        let keys = self
                            .store
                            .get_keys()
                            .await?
                            .iter()
                            .map(|x| RespMessage::BulkString(x.to_string()))
                            .collect();

                        self.write(RespMessage::Array(keys)).await?;
                    }
                }
                Command::Info(param) => {
                    let mut resp = String::new();
                    let sections = param.get_sections(&self.config);
                    for section in sections {
                        section.write(&mut resp)?;
                    }

                    self.write(RespMessage::BulkString(resp)).await?;
                }
                Command::ReplConf(data) => {
                    let Some((n, prev_state)) = &slave_state else {
                        anyhow::bail!("Unexpected replconf command")
                    };

                    if *n != n_commands - 1 {
                        anyhow::bail!("Illegal handshake pattern");
                    }

                    slave_state = match (prev_state, &data[..]) {
                        (
                            SlaveHandshakeState::PingReceived,
                            [ReplConfData::ListeningPort(port)],
                        ) => Some((n_commands, SlaveHandshakeState::PortReceived(port.clone()))),
                        (SlaveHandshakeState::PortReceived(port), _) => {
                            if data.iter().any(|x| match x {
                                ReplConfData::ListeningPort(_) => true,
                                _ => false,
                            }) {
                                anyhow::bail!(
                                    "Listening port is already set. Illegal handshake pattern"
                                );
                            }

                            Some((
                                (n_commands),
                                SlaveHandshakeState::CapaReceived(port.clone(), data),
                            ))
                        }
                        _ => anyhow::bail!("Illegal handshake pattern"),
                    };

                    self.write(RespMessage::simple_from_str("OK")).await?;
                }
                Command::Psync(replid, repl_offset) => {
                    let ServerRole::Master(my_info) = &self.config.role else {
                        anyhow::bail!("I am not the master :)")
                    };

                    let Some((n, SlaveHandshakeState::CapaReceived(port, repl_confs))) =
                        slave_state
                    else {
                        anyhow::bail!("Unexpected replconf command")
                    };

                    if n != n_commands - 1 {
                        anyhow::bail!("Illegal handshake pattern");
                    }

                    if replid == "?" && repl_offset == -1 {
                        self.write(RespMessage::SimpleString(format!(
                            "FULLRESYNC {} {}",
                            my_info.replid, my_info.repl_offset
                        )))
                        .await?;

                        self.write
                            .write_all(format!("${}\r\n", EMPTY_RDB.len()).as_bytes())
                            .await?;

                        self.write.write_all(&EMPTY_RDB).await?;
                    } else {
                        anyhow::bail!("Not implemented")
                    }

                    slave_state = Some((0, SlaveHandshakeState::Ready(port, repl_confs)));
                    break;
                }
                Command::Wait(num_replicas, timeout) => {
                    if let ServerRole::Master(master_info) = &self.config.role {
                        let slaves = master_info.slaves.read().await;
                        let len = slaves.len();
                        drop(slaves);
                        self.write(RespMessage::Integer(len as i64)).await?;
                    } else {
                        self.write(RespMessage::Integer(0)).await?;
                    }
                }
            };
            n_commands += 1;
        }

        if let Some((_, SlaveHandshakeState::Ready(port, repl_confs))) = slave_state {
            self.run_as_slave(port, repl_confs).await
        } else {
            Ok(())
        }
    }

    async fn run_as_slave(
        mut self,
        port: u16,
        repl_confs: Vec<ReplConfData>,
    ) -> anyhow::Result<()> {
        let ServerRole::Master(master_info) = &self.config.role else {
            anyhow::bail!("Slave connected to non-master")
        };

        let (tx, mut rx) = mpsc::unbounded_channel::<Command>();

        let capas = repl_confs
            .into_iter()
            .filter_map(|x| match x {
                ReplConfData::Capability(repl_capability) => Some(repl_capability),
                _ => None,
            })
            .collect();

        let mut slaves = master_info.slaves.write().await;
        slaves.push(SlaveClientInfo::new(self.addr.clone(), capas, tx));
        drop(slaves);

        while let Some(command) = &rx.recv().await {
            match command {
                Command::Set(key, value, options) => {
                    let mut lines = vec![
                        RespMessage::bulk_from_str("SET"),
                        RespMessage::BulkString(key.clone()),
                        RespMessage::BulkString(value.clone()),
                    ];

                    options.append_to_vec(&mut lines);

                    self.write(RespMessage::Array(lines)).await?;
                }
                _ => anyhow::bail!("Unexpected command forwarded to slave client {:?}", command),
            }
        }

        Ok(())
    }

    async fn write(&mut self, message: RespMessage) -> anyhow::Result<()> {
        message.write(&mut self.write).await
    }
}

impl Client<BufReader<OwnedReadHalf>, OwnedWriteHalf> {
    pub fn new(
        read: BufReader<OwnedReadHalf>,
        write: OwnedWriteHalf,
        addr: SocketAddr,
        store: Arc<Database>,
        config: Arc<ServerConfig>,
    ) -> Self {
        Client {
            read,
            write,
            addr,
            store,
            config,
        }
    }

    pub fn from_stream(
        stream: TcpStream,
        addr: SocketAddr,
        store: Arc<Database>,
        config: Arc<ServerConfig>,
    ) -> Self {
        let (read, write) = stream.into_split();
        let read = BufReader::new(read);
        Client {
            read,
            write,
            addr,
            store,
            config,
        }
    }
}
