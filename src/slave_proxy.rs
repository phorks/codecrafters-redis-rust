use std::{net::SocketAddr, sync::Arc};

use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt},
    sync::mpsc::UnboundedReceiver,
};

use crate::{
    commands::{Command, ReplConfData},
    server::{ServerConfig, ServerRole},
};

pub async fn receive_acks<R: AsyncBufReadExt + Unpin + Send>(
    slave_id: u32,
    mut read: R,
    config: Arc<ServerConfig>,
) -> anyhow::Result<()> {
    let ServerRole::Master(master) = &config.role else {
        panic!("config.role must be Master")
    };

    loop {
        match Command::from_buffer(&mut read).await {
            Ok(command) => match &command {
                Command::ReplConf(confs) => {
                    if let [ReplConfData::Ack(offset)] = confs[..] {
                        master.register_ack(slave_id, offset).await?;
                    } else {
                        eprintln!(
                            "Received invalid REPLCONF from slave while expecting an ACK. {:?}",
                            confs
                        )
                    }
                }
                _ => {
                    eprintln!("Received {:?} from slave while expecting an ACK", command);
                }
            },
            Err(err) => {
                anyhow::bail!("Invalid ack received from slave. {}", err);
            }
        }
    }
}

pub async fn propagate_commands<W: AsyncWriteExt + Unpin>(
    _slave_id: u32,
    addr: SocketAddr,
    mut write: W,
    mut rx: UnboundedReceiver<Command>,
) -> anyhow::Result<()> {
    while let Some(command) = rx.recv().await {
        match &command {
            Command::Set(_, _, _) => {
                println!("Propagating {:?} to save {:?}", command, addr);
                command.to_resp()?.write(&mut write).await?;
            }
            Command::ReplConf(confs) => {
                if let [ReplConfData::GetAck] = confs[..] {
                    Command::ReplConf(vec![ReplConfData::GetAck])
                        .to_resp()?
                        .write(&mut write)
                        .await?;
                } else {
                    anyhow::bail!(
                        "Unexpected REPLCONF command forwarded to slave client {:?}",
                        confs
                    );
                }
            }
            _ => anyhow::bail!("Unexpected command forwarded to slave client {:?}", command),
        };
    }
    Ok(())
}
