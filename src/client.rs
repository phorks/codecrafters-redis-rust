use std::{net::SocketAddr, sync::Arc};

use tokio::{
    io::{AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
};

use crate::{
    commands::{Command, ReplConfData},
    redis::{Database, RecordValue, TypeError, EMPTY_RDB},
    resp::RespMessage,
    resp_ext::{ToMapRespArray, ToStringResp},
    server::{ServerConfig, ServerRole},
    slave_proxy::{propagate_commands, receive_acks},
    streams::InvalidStreamEntryId,
};

enum SlaveHandshakeState {
    PingReceived,
    PortReceived(u16),
    CapaReceived(u16, Vec<ReplConfData>),
    Ready(u16, Vec<ReplConfData>),
}

pub struct Client<Read: AsyncBufReadExt + Unpin + Send + 'static, Write: AsyncWriteExt + Unpin> {
    read: Read,
    write: Write,
    addr: SocketAddr,
    store: Arc<Database>,
    config: Arc<ServerConfig>,
    n_commands: u64,
    slave_state: Option<(u64, SlaveHandshakeState)>,
    queued_commands: Option<Vec<Command>>,
    queued_responses: Option<Vec<RespMessage>>,
}

impl<Read: AsyncBufReadExt + Unpin + Send + 'static, Write: AsyncWrite + Unpin>
    Client<Read, Write>
{
    pub async fn run(mut self) -> anyhow::Result<()> {
        while let Ok(command) = Command::from_buffer(&mut self.read).await {
            println!("Received command: {:?}", command);
            self.exec_command(command).await?;

            if self.slave_state.as_ref().map_or(false, |x| {
                matches!(x, (_, SlaveHandshakeState::Ready(_, _)))
            }) {
                break;
            }
        }

        let slave_state = std::mem::replace(&mut self.slave_state, None);
        if let Some((_, SlaveHandshakeState::Ready(port, repl_confs))) = slave_state {
            self.run_as_slave_proxy(port, repl_confs).await
        } else {
            Ok(())
        }
    }

    async fn exec_command(&mut self, command: Command) -> anyhow::Result<()> {
        if let Some(queue) = self.queued_commands.as_mut() {
            match command {
                Command::Exec => {
                    println!("Correct EXEC");
                    let queue = self.queued_commands.take().unwrap();
                    self.queued_responses = Some(vec![]);
                    for command in queue {
                        Box::pin(self.exec_command(command)).await?;
                    }

                    // HACK: What if another exec happens while performing this exec? Do we need to
                    // use a stack for responses?
                    let responses = self.queued_responses.take().unwrap();
                    self.write(RespMessage::Array(responses)).await?;
                }
                Command::Discard => {
                    self.queued_commands = None;
                    self.write("OK".to_simple_string()).await?;
                }
                _ => {
                    queue.push(command);
                    "QUEUED".to_simple_string().write(&mut self.write).await?;
                }
            }

            return Ok(());
        }

        match command {
            Command::Ping => {
                self.write(RespMessage::SimpleString("PONG".into())).await?;
                self.slave_state = Some((self.n_commands, SlaveHandshakeState::PingReceived));
            }
            Command::Echo(message) => {
                self.write(RespMessage::BulkString(message.clone())).await?;
            }
            Command::Set(key, value, options) => {
                let resp = self
                    .store
                    .set(key.clone(), value.clone(), options.clone())
                    .await?;

                if let ServerRole::Master(master) = &self.config.role {
                    master.propagate_set(&key, &value, options).await?;
                }

                self.write(resp).await?;
            }
            Command::Get(key) => {
                let resp = self.store.get(&key).await?;
                self.write(resp.into()).await?;
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
                let sections = param.get_sections(&self.config).await;
                for section in sections {
                    section.write(&mut resp)?;
                }

                self.write(RespMessage::BulkString(resp)).await?;
            }
            Command::ReplConf(data) => {
                let Some((n, prev_state)) = &self.slave_state else {
                    anyhow::bail!("Unexpected replconf command")
                };

                if *n != self.n_commands - 1 {
                    anyhow::bail!("Illegal handshake pattern");
                }

                self.slave_state = match (prev_state, &data[..]) {
                    (SlaveHandshakeState::PingReceived, [ReplConfData::ListeningPort(port)]) => {
                        Some((
                            self.n_commands,
                            SlaveHandshakeState::PortReceived(port.clone()),
                        ))
                    }
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
                            (self.n_commands),
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

                let slave_state = std::mem::replace(&mut self.slave_state, None);

                let Some((n, SlaveHandshakeState::CapaReceived(port, repl_confs))) = slave_state
                else {
                    anyhow::bail!("Unexpected replconf command")
                };

                if n != self.n_commands - 1 {
                    anyhow::bail!("Illegal handshake pattern");
                }

                if replid == "?" && repl_offset == -1 {
                    self.write(RespMessage::SimpleString(format!(
                        "FULLRESYNC {} {}",
                        my_info.replid(),
                        my_info.repl_offset().await
                    )))
                    .await?;

                    self.write
                        .write_all(format!("${}\r\n", EMPTY_RDB.len()).as_bytes())
                        .await?;

                    self.write.write_all(&EMPTY_RDB).await?;
                } else {
                    anyhow::bail!("Not implemented")
                }

                self.slave_state = Some((0, SlaveHandshakeState::Ready(port, repl_confs)));
            }
            Command::Wait(num_replicas, timeout) => {
                if let ServerRole::Master(master) = &self.config.role {
                    let n = master.wait(num_replicas, timeout).await?;
                    println!("Wait response: {}", (n as i64).to_string());
                    self.write(RespMessage::Integer(n as i64)).await?;
                } else {
                    self.write(RespMessage::Integer(0)).await?;
                }
            }
            Command::Type(key) => {
                let typ = match self.store.get(&key).await? {
                    Some(RecordValue::String(_)) => "string",
                    Some(RecordValue::Stream(_)) => "stream",
                    None => "none",
                };

                self.write(RespMessage::simple_from_str(typ)).await?;
            }
            Command::Xadd(key, entry_id, values) => {
                let result = self.store.add_stream_entry(&key, entry_id, values).await;

                match result {
                    Ok(entry_id) => {
                        self.write(entry_id.to_bulk_string()).await?;
                    }
                    Err(e) => match e.downcast::<InvalidStreamEntryId>() {
                        Ok(e) => {
                            self.write(e.to_resp()).await?;
                        }
                        Err(e) => return Err(e),
                    },
                }
            }
            Command::Xrange(key, start, end) => {
                let entries = self
                    .store
                    .get_stream_entries_in_range(&key, start, end)
                    .await?;

                self.write(entries.to_resp()).await?;
            }
            Command::Xread(stream_afters, blocking) => match blocking {
                Some(block) => {
                    let res = self
                        .store
                        .get_bulk_stream_entries_blocking(stream_afters, block)
                        .await?;

                    match res {
                        Some(res) => {
                            let resp = res
                                .iter()
                                .map(|x| (x.0.to_bulk_string(), x.1.to_resp()))
                                .to_map_resp_array();
                            self.write(resp).await?;
                        }
                        None => {
                            self.write(RespMessage::Null).await?;
                        }
                    }
                }
                None => {
                    let res = self.store.get_bulk_stream_entries(stream_afters).await?;
                    let resp = res
                        .iter()
                        .map(|x| (x.0.to_bulk_string(), x.1.to_resp()))
                        .to_map_resp_array();
                    self.write(resp).await?;
                }
            },
            Command::Incr(key) => match self.store.increase_integer(&key).await {
                Ok(i) => {
                    self.write(RespMessage::Integer(i)).await?;
                }
                Err(e) => match e.downcast::<TypeError>() {
                    Ok(e) => {
                        self.write(e.to_resp()).await?;
                    }
                    Err(e) => return Err(e),
                },
            },
            Command::Multi => {
                self.queued_commands = Some(vec![]);
                self.write("OK".to_simple_string()).await?;
            }
            Command::Exec => {
                // { match &self.queued_commands {
                // Some(queue) => {
                //     if queue.len() == 0 {
                //         self.write(RespMessage::Array(vec![])).await?;
                //     }
                //     self.queued_commands = None
                // }
                // None => {
                println!("Bad EXEC");
                self.write("ERR EXEC without MULTI".to_simple_error())
                    .await?;
                // }
                // }
            }
            Command::Discard => {
                self.write("ERR DISCARD without MULTI".to_simple_error())
                    .await?;
            }
        };
        self.n_commands += 1;
        Ok(())
    }

    async fn run_as_slave_proxy(
        self,
        _port: u16,
        repl_confs: Vec<ReplConfData>,
    ) -> anyhow::Result<()> {
        let ServerRole::Master(master) = &self.config.role else {
            anyhow::bail!("Slave connected to non-master")
        };

        let capas = repl_confs
            .into_iter()
            .filter_map(|x| match x {
                ReplConfData::Capability(repl_capability) => Some(repl_capability),
                _ => None,
            })
            .collect();

        let (id, rx) = master.register_slave(self.addr.clone(), capas).await;

        let write = self.write;
        let read = self.read;
        let config = Arc::clone(&self.config);

        let incoming = tokio::spawn(receive_acks(id, read, config));

        propagate_commands(id, self.addr, write, rx).await?;

        incoming.await??;

        Ok(())
    }

    async fn write(&mut self, message: RespMessage) -> anyhow::Result<()> {
        match &mut self.queued_responses {
            Some(queue) => queue.push(message),
            None => message.write(&mut self.write).await?,
        };

        Ok(())
    }
}

impl Client<BufReader<OwnedReadHalf>, OwnedWriteHalf> {
    pub fn _new(
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
            n_commands: 0,
            slave_state: None,
            queued_commands: None,
            queued_responses: None,
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
            n_commands: 0,
            slave_state: None,
            queued_commands: None,
            queued_responses: None,
        }
    }
}
