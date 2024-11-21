use std::{collections::HashMap, fmt::Write};

use crate::server::{ServerConfig, ServerRole};

#[derive(Debug, Clone)]
pub enum InfoSectionKind {
    Replication,
}

// would've used the enum-iterator crate if Cargo.toml could change :)
pub struct InfoSectionKindIterator {
    curr: Option<InfoSectionKind>,
}

impl InfoSectionKindIterator {
    pub fn new() -> Self {
        Self {
            curr: Some(InfoSectionKind::Replication),
        }
    }
}

impl Iterator for InfoSectionKindIterator {
    type Item = InfoSectionKind;

    fn next(&mut self) -> Option<Self::Item> {
        self.curr = match self.curr {
            Some(InfoSectionKind::Replication) => None,
            None => None,
        };

        return self.curr.clone();
    }
}

impl InfoSectionKind {
    pub fn as_header(&self) -> &'static str {
        match self {
            InfoSectionKind::Replication => "Replication",
        }
    }

    pub fn iter() -> InfoSectionKindIterator {
        InfoSectionKindIterator::new()
    }
}

pub struct InfoSection {
    kind: InfoSectionKind,
    data: HashMap<String, String>,
}

impl InfoSection {
    pub fn write<W: Write>(&self, write: &mut W) -> anyhow::Result<()> {
        write!(write, "# {}\r\n", self.kind.as_header())?;
        for (key, value) in &self.data {
            write!(write, "{}:{}\r\n", key, value)?;
        }

        Ok(())
    }
}

impl InfoSectionKind {
    pub async fn get_info(&self, config: &ServerConfig) -> InfoSection {
        match self {
            InfoSectionKind::Replication => {
                let mut data = HashMap::new();
                match &config.role {
                    ServerRole::Master(info) => {
                        data.insert(String::from("role"), String::from("master"));
                        data.insert(String::from("master_replid"), String::from(info.replid()));
                        data.insert(
                            String::from("master_repl_offset"),
                            info.repl_offset().await.to_string(),
                        );
                    }
                    ServerRole::Slave(_) => {
                        data.insert(String::from("role"), String::from("slave"));
                    }
                }

                InfoSection {
                    kind: self.clone(),
                    data,
                }
            }
        }
    }
}
