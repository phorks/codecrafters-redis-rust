use tokio::net::TcpStream;

use crate::{
    commands::{Command, RespMessage},
    io_helper::skip_sequence,
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
    Psync(String, i32),
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
            HandshakeMessage::Psync(replid, repl_offset) => {
                vec![
                    RespMessage::bulk_from_str("PSYNC"),
                    RespMessage::BulkString(replid.clone()),
                    RespMessage::BulkString(repl_offset.to_string()),
                ]
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

    skip_sequence(&mut stream, "+PONG\r\n".as_bytes()).await?;

    HandshakeMessage::ReplConf(ReplConfData::ListeningPort(config.port()))
        .to_resp()
        .write(&mut stream)
        .await?;

    skip_sequence(&mut stream, "+OK\r\n".as_bytes()).await?;

    HandshakeMessage::ReplConf(ReplConfData::Capabilities(ReplCapability::Psync2))
        .to_resp()
        .write(&mut stream)
        .await?;

    skip_sequence(&mut stream, "+OK\r\n".as_bytes()).await?;

    HandshakeMessage::Psync(String::from("?"), -1)
        .to_resp()
        .write(&mut stream)
        .await?;

    Ok(())
}
