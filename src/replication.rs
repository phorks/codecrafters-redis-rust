use core::str;

use tokio::{
    io::{AsyncReadExt, BufReader},
    net::TcpStream,
};

use crate::{
    commands::{Command, ReplCapability, ReplConfData},
    io_helper::{read_until_crlf, skip_sequence},
    resp::RespMessage,
    server::{ServerConfig, ServerRole},
};

pub async fn connect_to_master(config: &ServerConfig) -> anyhow::Result<Option<Vec<u8>>> {
    let ServerRole::Slave(master_addr) = config.role else {
        return Ok(None);
    };

    let stream = TcpStream::connect(master_addr).await?;
    let (read, mut write) = stream.into_split();
    let mut read = BufReader::new(read);

    Command::Ping.to_resp()?.write(&mut write).await?;

    skip_sequence(&mut read, "+PONG\r\n".as_bytes()).await?;

    Command::ReplConf(vec![ReplConfData::ListeningPort(config.port())])
        .to_resp()?
        .write(&mut write)
        .await?;

    skip_sequence(&mut read, "+OK\r\n".as_bytes()).await?;

    Command::ReplConf(vec![ReplConfData::Capability(ReplCapability::Psync2)])
        .to_resp()?
        .write(&mut write)
        .await?;

    skip_sequence(&mut read, b"+OK\r\n").await?;

    Command::Psync(String::from("?"), -1)
        .to_resp()?
        .write(&mut write)
        .await?;

    let sync_resp = RespMessage::read(&mut read).await?;

    // FULLRESYNC <MASTER replid> <MASTER repl_offset>
    match sync_resp {
        RespMessage::SimpleString(_) => (),
        _ => anyhow::bail!("Expected a SimpleString"),
    };

    // RDB file in a format similar to BulkString without the trailing \r\n
    skip_sequence(&mut read, b"$").await?;
    let length = read_until_crlf(&mut read).await?;
    let length = str::from_utf8(&length)?.parse()?;
    let mut buf = vec![0u8; length];
    read.read_exact(&mut buf).await?;

    Ok(Some(buf))
}
