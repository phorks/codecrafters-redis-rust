use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader, Lines};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

enum RespMessage {
    SimpleString(String),
    BulkString(String),
    Null,
}

impl RespMessage {
    async fn write<T>(self, write: &mut T) -> anyhow::Result<()>
    where
        T: AsyncWrite + Unpin,
    {
        match self {
            RespMessage::SimpleString(s) => {
                let bytes = s.as_bytes();

                write.write_all(b"$").await?;
                write.write_all(bytes.len().to_string().as_bytes()).await?;
                write.write_all(b"\r\n").await?;
                write.write_all(bytes).await?;
                write.write_all(b"\r\n").await?;
            }
            RespMessage::BulkString(s) => {
                write.write_all(b"+").await?;
                write.write_all(s.as_bytes()).await?;
                write.write_all(b"\r\n").await?;
            }
            RespMessage::Null => {
                write.write_all(b"$-1\r\n").await?;
            }
        }

        Ok(())
    }
}

#[derive(Debug)]
enum Command {
    Ping,
    Echo(String),
    Set(String, String),
    Get(String),
}

impl Command {
    async fn from_buffer<T>(read: &mut T) -> anyhow::Result<Command>
    where
        T: AsyncBufReadExt + Unpin,
    {
        async fn read_param<T>(lines: &mut Lines<T>) -> anyhow::Result<String>
        where
            T: AsyncBufReadExt + Unpin,
        {
            if let Some(bytes_line) = lines.next_line().await? {
                println!("Received line: {}", bytes_line);
            }

            if let Some(value_line) = lines.next_line().await? {
                println!("Received line: {}", value_line);
                Ok(value_line)
            } else {
                Ok(String::new())
            }
        }

        let mut lines = read.lines();
        let Some(params_line) = lines.next_line().await? else {
            anyhow::bail!("Empty params line")
        };
        println!("Received line: {}", params_line);
        let n_params = params_line.as_str()[1..].parse::<usize>()? - 1;

        let name = read_param(&mut lines).await?;

        if name.eq_ignore_ascii_case("ping") {
            if n_params != 0 {
                anyhow::bail!("Incorrect number of parameters for ping command");
            }

            return Ok(Command::Ping);
        } else if name.eq_ignore_ascii_case("echo") {
            if n_params != 1 {
                anyhow::bail!(
                    "Incorrect number of parameters, expected: 2, received {}",
                    n_params
                );
            }

            return Ok(Command::Echo(read_param(&mut lines).await?));
        } else if name.eq_ignore_ascii_case("set") {
            if n_params < 2 {
                anyhow::bail!(
                    "Incorrect number of parameters, expected: at least 2, received {}",
                    n_params
                );
            }

            let key = read_param(&mut lines).await?;
            let value = read_param(&mut lines).await?;

            return Ok(Command::Set(key, value));
        } else if name.eq_ignore_ascii_case("get") {
            if n_params == 0 {
                anyhow::bail!("Incorrect number of parameters, expected: at least 1, received 0");
            }

            let key = read_param(&mut lines).await?;

            return Ok(Command::Get(key));
        }

        anyhow::bail!("Unknown command");
    }
}

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let store = Arc::new(RwLock::new(HashMap::new()));
    loop {
        let (stream, addr) = listener.accept().await.unwrap();
        let store = store.clone();
        println!("Accepted connection from {addr}");
        tokio::spawn(async move {
            match handle_connection(stream, store).await {
                Ok(_) => println!("Success"),
                Err(e) => println!("Failed: {:?}", e),
            }
            println!("Disconnected from {addr}");
        });
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    store: Arc<RwLock<HashMap<String, String>>>,
) -> anyhow::Result<()> {
    let (read, mut write) = stream.split();
    let mut reader = BufReader::new(read);
    while let Ok(command) = Command::from_buffer(&mut reader).await {
        println!("Received command: {:?}", command);
        let response = match command {
            Command::Ping => RespMessage::SimpleString("PONG".into()),
            Command::Echo(message) => RespMessage::BulkString(message.clone()),
            Command::Set(key, value) => {
                store.write().await.insert(key, value);
                RespMessage::SimpleString("OK".into())
            }
            Command::Get(key) => {
                let store = store.read().await;
                let value = store.get(&key);
                value.map_or(RespMessage::Null, |m| RespMessage::BulkString(m.clone()))
            }
        };

        response.write(&mut write).await?;
    }

    return Ok(());
}
