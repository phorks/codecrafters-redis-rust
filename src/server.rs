use std::{
    net::{Ipv4Addr, SocketAddrV4},
    path::{Path, PathBuf},
    str::FromStr,
};

const DEFAULT_PORT: u16 = 6379;

pub struct ServerConfig {
    pub dir: Option<String>,
    pub dbfilename: Option<String>,
    port: Option<u16>,
    pub replica_of: Option<SocketAddrV4>,
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
                    let Ok(ip_addr) = Ipv4Addr::from_str(addr.0) else {
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
            replica_of,
        }
    }

    pub fn db_path(&self) -> Option<PathBuf> {
        Some(Path::new(self.dir.as_ref()?).join(self.dbfilename.as_ref()?))
    }

    pub fn port(&self) -> u16 {
        self.port.unwrap_or(DEFAULT_PORT)
    }
}
