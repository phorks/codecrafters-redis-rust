use tokio::net::TcpStream;

use crate::{
    commands::{Command, ReplCapability, ReplConfData},
    io_helper::skip_sequence,
    server::{ServerConfig, ServerRole},
};

pub async fn connect_to_master(config: &ServerConfig) -> anyhow::Result<()> {
    let ServerRole::Slave(master_addr) = config.role else {
        return Ok(());
    };

    let mut stream = TcpStream::connect(master_addr).await?;

    Command::Ping.to_resp()?.write(&mut stream).await?;

    skip_sequence(&mut stream, "+PONG\r\n".as_bytes()).await?;

    Command::ReplConf(vec![ReplConfData::ListeningPort(config.port())])
        .to_resp()?
        .write(&mut stream)
        .await?;

    skip_sequence(&mut stream, "+OK\r\n".as_bytes()).await?;

    Command::ReplConf(vec![ReplConfData::Capability(ReplCapability::Psync2)])
        .to_resp()?
        .write(&mut stream)
        .await?;

    skip_sequence(&mut stream, "+OK\r\n".as_bytes()).await?;

    Command::Psync(String::from("?"), -1)
        .to_resp()?
        .write(&mut stream)
        .await?;

    Ok(())
}
