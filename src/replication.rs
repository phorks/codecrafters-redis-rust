use tokio::net::TcpStream;

use crate::{
    commands::{Command, RespMessage},
    server::{ServerConfig, ServerRole},
};

enum ReplCapability {
    Psync2,
}

impl ToString for ReplCapability {
    fn to_string(&self) -> String {
        match self {
            ReplCapability::Psync2 => String::from("psync2"),
        }
    }
}

enum ReplConfData {
    ListeningPort(u16),
    Capabilities(ReplCapability),
}

enum HandshakeMessage {
    Ping,
    ReplConf(ReplConfData),
}

impl HandshakeMessage {
    fn to_resp(&self) -> RespMessage {
        let lines = match self {
            HandshakeMessage::Ping => {
                vec![RespMessage::bulk_from_str("PING")]
            }
            HandshakeMessage::ReplConf(data) => {
                let mut lines = Vec::with_capacity(3);
                lines.push(RespMessage::bulk_from_str("REPLCONF"));
                match data {
                    ReplConfData::ListeningPort(port) => {
                        lines.push(RespMessage::bulk_from_str("listening-port"));
                        lines.push(RespMessage::BulkString(port.to_string()));
                    }
                    ReplConfData::Capabilities(capa) => {
                        lines.push(RespMessage::bulk_from_str("capa"));
                        lines.push(RespMessage::BulkString(capa.to_string()));
                    }
                }
                lines
            }
        };

        RespMessage::Array(lines)
    }
}

pub async fn connect_to_master(config: &ServerConfig) -> anyhow::Result<()> {
    let ServerRole::Slave(master_addr) = config.role else {
        return Ok(());
    };

    let mut stream = TcpStream::connect(master_addr).await?;

    HandshakeMessage::Ping.to_resp().write(&mut stream).await?;

    HandshakeMessage::ReplConf(ReplConfData::ListeningPort(config.port()))
        .to_resp()
        .write(&mut stream)
        .await?;

    HandshakeMessage::ReplConf(ReplConfData::Capabilities(ReplCapability::Psync2))
        .to_resp()
        .write(&mut stream)
        .await?;

    Ok(())
}
