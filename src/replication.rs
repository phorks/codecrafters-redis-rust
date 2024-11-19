use tokio::net::TcpStream;

use crate::{
    commands::{Command, RespMessage},
    server::{ServerConfig, ServerRole},
};

enum HandshakeMessage {
    Echo,
}

impl ToString for HandshakeMessage {
    fn to_string(&self) -> String {
        match self {
            HandshakeMessage::Echo => String::from("ECHO"),
        }
    }
}

pub async fn connect_to_master(config: &ServerConfig) -> anyhow::Result<()> {
    let ServerRole::Slave(master_addr) = config.role else {
        return Ok(());
    };

    let mut stream = TcpStream::connect(master_addr).await?;

    RespMessage::Array(vec![RespMessage::BulkString(
        HandshakeMessage::Echo.to_string(),
    )])
    .write(&mut stream)
    .await?;

    Ok(())
}
