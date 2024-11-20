use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use tokio::{
    select,
    sync::{broadcast, mpsc, oneshot, Mutex, RwLock},
};

use crate::commands::{Command, ReplCapability, ReplConfData, SetCommandOptions};

const DEFAULT_PORT: u16 = 6379;

pub struct SlaveClientInfo {
    // should've used uuid
    pub addr: SocketAddr,
    pub capabilities: Vec<ReplCapability>,
    pub client_tx: Arc<Mutex<mpsc::UnboundedSender<(Command, Option<oneshot::Sender<Command>>)>>>,
}

impl SlaveClientInfo {
    pub fn new(
        addr: SocketAddr,
        capabilities: Vec<ReplCapability>,
        client_tx: mpsc::UnboundedSender<(Command, Option<oneshot::Sender<Command>>)>,
    ) -> Self {
        Self {
            addr,
            capabilities,
            client_tx: Arc::new(Mutex::new(client_tx)),
        }
    }
}

pub struct MasterServerInfo {
    replid: String,
    repl_offset: RwLock<usize>,
    slaves: RwLock<Vec<SlaveClientInfo>>,
}

impl MasterServerInfo {
    pub fn new() -> Self {
        MasterServerInfo {
            replid: Self::new_master_replid(),
            repl_offset: RwLock::new(0),
            slaves: RwLock::new(vec![]),
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
    ) -> mpsc::UnboundedReceiver<(Command, Option<oneshot::Sender<Command>>)> {
        let (tx, rx) = mpsc::unbounded_channel::<(Command, Option<oneshot::Sender<Command>>)>();
        self.slaves
            .write()
            .await
            .push(SlaveClientInfo::new(addr, capas, tx));

        rx
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
            if let Ok(_) = slave.client_tx.lock().await.send((command.clone(), None)) {
                n_received += 1;
            }
        }

        Ok(n_received)
    }

    pub async fn wait(&self, num_replicas: u32, timeout: u32) -> anyhow::Result<usize> {
        let offset = { self.repl_offset().await };
        let n = Arc::new(Mutex::new(0));
        if offset == 0 {
            return Ok(num_replicas as usize);
        }
        let (cancel_tx, mut cancel_rx) = broadcast::channel::<()>(1);

        let slaves = self.slaves.read().await;
        for slave in slaves.iter() {
            let tx = Arc::clone(&slave.client_tx);
            let n = Arc::clone(&n);
            let addr = slave.addr.clone();
            tokio::spawn(async move {
                let (response_tx, response_rx) = oneshot::channel();
                let sent = tx.lock().await.send((
                    Command::ReplConf(vec![ReplConfData::GetAck]),
                    Some(response_tx),
                ));

                if sent.is_err() {
                    return;
                }

                select! {
                    res = response_rx => {
                        println!("Master server received {:?} from slave proxy", res);
                        let Ok(Command::ReplConf(confs)) = res else { return };
                        let [ReplConfData::Ack(_ack)] = confs[..] else { return };
                        *n.lock().await += 1;
                    }
                    _ = cancel_rx.recv() => {
                        println!("Wait request canceled for slave due to timeout (slave: {:?})", addr);
                        return;
                    }
                }
            });
            cancel_rx = cancel_tx.subscribe();
        }

        tokio::time::sleep(Duration::from_millis(timeout as u64)).await;
        cancel_tx.send(())?;
        let n = *n.lock().await;
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
