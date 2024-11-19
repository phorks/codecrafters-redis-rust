use std::path::{Path, PathBuf};

const DEFAULT_PORT: u16 = 6379;

pub struct ServerConfig {
    pub dir: Option<String>,
    pub dbfilename: Option<String>,
    port: Option<u16>,
}

impl ServerConfig {
    pub fn new(args: Vec<String>) -> ServerConfig {
        let mut it = args.iter();
        let mut dir = None;
        let mut dbfilename = None;
        let mut port = None;
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
            }
        }

        ServerConfig {
            dir,
            dbfilename,
            port,
        }
    }

    pub fn db_path(&self) -> Option<PathBuf> {
        Some(Path::new(self.dir.as_ref()?).join(self.dbfilename.as_ref()?))
    }

    pub fn port(&self) -> u16 {
        self.port.unwrap_or(DEFAULT_PORT)
    }
}
