use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    path::{Path, PathBuf},
    str::FromStr,
};

use tokio::sync::{mpsc, RwLock};

use crate::commands::{Command, ReplCapability};

const DEFAULT_PORT: u16 = 6379;

pub struct SlaveClientInfo {
    // should've used uuid
    pub addr: SocketAddr,
    pub capabilities: Vec<ReplCapability>,
    pub tx: mpsc::UnboundedSender<Command>,
}

impl SlaveClientInfo {
    pub fn new(
        addr: SocketAddr,
        capabilities: Vec<ReplCapability>,
        tx: mpsc::UnboundedSender<Command>,
    ) -> Self {
        Self {
            addr,
            capabilities,
            tx,
        }
    }
}

pub struct MasterServerInfo {
    pub replid: String,
    pub repl_offset: u32,
    pub slaves: RwLock<Vec<SlaveClientInfo>>,
}

impl MasterServerInfo {
    pub fn new() -> Self {
        MasterServerInfo {
            replid: Self::new_master_replid(),
            repl_offset: 0,
            slaves: RwLock::new(vec![]),
        }
    }

    fn new_master_replid() -> String {
        String::from("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb")
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
