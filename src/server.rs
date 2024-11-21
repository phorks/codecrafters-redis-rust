use std::{
    collections::HashMap,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    path::{Path, PathBuf},
    str::FromStr,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::Duration,
};

use tokio::sync::{mpsc, oneshot, Mutex, RwLock};

use crate::commands::{Command, ReplCapability, ReplConfData, SetCommandOptions};

const DEFAULT_PORT: u16 = 6379;

pub struct SlaveClientInfo {
    // should've used uuid
    id: u32,
    pub addr: SocketAddr,
    pub capabilities: Vec<ReplCapability>,
    pub client_tx: Arc<Mutex<mpsc::UnboundedSender<Command>>>,
    offset: usize,
}

impl SlaveClientInfo {
    pub fn new(
        id: u32,
        addr: SocketAddr,
        capabilities: Vec<ReplCapability>,
        client_tx: mpsc::UnboundedSender<Command>,
    ) -> Self {
        Self {
            id,
            addr,
            capabilities,
            client_tx: Arc::new(Mutex::new(client_tx)),
            offset: 0,
        }
    }
}

pub struct MasterServerInfo {
    replid: String,
    repl_offset: RwLock<usize>,
    slaves: RwLock<HashMap<u32, SlaveClientInfo>>,
    max_id: AtomicU32,
}

impl MasterServerInfo {
    pub fn new() -> Self {
        MasterServerInfo {
            replid: Self::new_master_replid(),
            repl_offset: RwLock::new(0),
            slaves: RwLock::new(HashMap::new()),
            max_id: AtomicU32::new(0),
        }
    }

    fn new_master_replid() -> String {
        String::from("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb")
    }

    pub fn replid(&self) -> &str {
        &self.replid
    }

    pub async fn repl_offset(&self) -> usize {
        self.repl_offset.read().await.clone()
    }

    pub async fn register_slave(
        &self,
        addr: SocketAddr,
        capas: Vec<ReplCapability>,
    ) -> (u32, mpsc::UnboundedReceiver<Command>) {
        let (tx, rx) = mpsc::unbounded_channel::<Command>();
        let id = self.max_id.fetch_add(1, Ordering::SeqCst);

        self.slaves
            .write()
            .await
            .insert(id, SlaveClientInfo::new(id, addr, capas, tx));
        println!("Slave registered! {}", self.slaves.read().await.len());

        (id, rx)
    }

    pub async fn register_ack(&self, slave_id: u32, offset: usize) -> anyhow::Result<usize> {
        let mut slaves = self.slaves.write().await;
        let slave = slaves
            .get_mut(&slave_id)
            .ok_or(anyhow::anyhow!("No slave with the given id {}", slave_id))?;

        if slave.offset < offset {
            println!("Slave {} acked with {}", slave_id, offset);
            slave.offset = offset
        }

        Ok(slave.offset)
    }

    pub async fn propagate_set(
        &self,
        key: &str,
        value: &str,
        options: SetCommandOptions,
    ) -> anyhow::Result<usize> {
        let command = Command::Set(String::from(key), String::from(value), options.clone());

        {
            *self.repl_offset.write().await += command.to_resp()?.count_in_bytes();
        }

        let slaves = self.slaves.read().await;
        let mut n_received = 0;
        for slave in slaves.iter() {
            if let Ok(_) = slave.1.client_tx.lock().await.send(command.clone()) {
                n_received += 1;
            } else {
                eprintln!("Failed to send SET command to replica {:?}", slave.1.addr);
            }
        }

        Ok(n_received)
    }

    pub async fn wait(&self, num_replicas: u32, timeout: u32) -> anyhow::Result<usize> {
        let command = Command::ReplConf(vec![ReplConfData::GetAck]);
        let offset = {
            let mut repl_offset = self.repl_offset.write().await;
            let n = *repl_offset;
            *repl_offset += command.to_resp()?.count_in_bytes();
            n
        };

        println!("Offset for wait {}", offset);
        if offset == 0 {
            let n = self.slaves.read().await.len();
            return Ok(n);
        }

        {
            let slaves = self.slaves.read().await;
            for slave in slaves.values() {
                let tx = Arc::clone(&slave.client_tx);
                let sent = tx
                    .lock()
                    .await
                    .send(Command::ReplConf(vec![ReplConfData::GetAck]));

                if sent.is_err() {
                    continue;
                }
            }
        }

        tokio::time::sleep(Duration::from_millis(timeout as u64)).await;
        let n = self
            .slaves
            .read()
            .await
            .values()
            .filter(|x| x.offset == offset)
            .count();
        Ok(n)
    }
}

pub enum ServerRole {
    Master(MasterServerInfo),
    Slave(SocketAddrV4),
}

pub struct ServerConfig {
    pub dir: Option<String>,
    pub dbfilename: Option<String>,
    port: Option<u16>,
    pub role: ServerRole,
}

impl ServerConfig {
    pub fn new(args: Vec<String>) -> ServerConfig {
        let mut it = args.iter();
        let mut dir = None;
        let mut dbfilename = None;
        let mut port = None;
        let mut replica_of = None;
        while let Some(arg) = it.next() {
            if arg == "--dir" {
                if let Some(next) = it.next() {
                    dir = Some(next.clone());
                } else {
                    panic!("'dir' value expected");
                }
            } else if arg == "--dbfilename" {
                if let Some(next) = it.next() {
                    dbfilename = Some(next.clone());
                } else {
                    panic!("'dbfilename' value expected");
                }
            } else if arg == "--port" {
                if let Some(next) = it.next().and_then(|x| x.parse::<u16>().ok()) {
                    port = Some(next);
                } else {
                    panic!("'port' expected");
                }
            } else if arg == "--replicaof" {
                let next = it.next();
                if let Some(addr) = next.and_then(|x| x.split_once(' ')) {
                    let Some(ip_addr) = Ipv4Addr::from_str(addr.0).ok().or_else(|| {
                        if addr.0.eq_ignore_ascii_case("localhost") {
                            Some(Ipv4Addr::LOCALHOST)
                        } else {
                            None
                        }
                    }) else {
                        panic!("Invalid ipv4 address in --replicaof")
                    };
                    let Ok(port_number) = addr.1.parse::<u16>() else {
                        panic!("Invalid port number in --replicaof")
                    };
                    replica_of = Some(SocketAddrV4::new(ip_addr, port_number));
                }

                if replica_of.is_none() {
                    panic!(
                        "Invalid --replicaof value {}",
                        next.unwrap_or(&String::from("<empty>"))
                    );
                }
            }
        }

        ServerConfig {
            dir,
            dbfilename,
            port,
            role: replica_of.map_or_else(
                || ServerRole::Master(MasterServerInfo::new()),
                |x| ServerRole::Slave(x),
            ),
        }
    }

    pub fn db_path(&self) -> Option<PathBuf> {
        Some(Path::new(self.dir.as_ref()?).join(self.dbfilename.as_ref()?))
    }

    pub fn port(&self) -> u16 {
        self.port.unwrap_or(DEFAULT_PORT)
    }

    pub fn is_master(&self) -> bool {
        match self.role {
            ServerRole::Master(_) => true,
            ServerRole::Slave(_) => false,
        }
    }
}
