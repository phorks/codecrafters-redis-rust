use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

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
struct SetCommandOptions {
    ex: Option<u64>, // seconds -- Set the specified expire time, in seconds (a positive integer).
    px: Option<u64>, // milliseconds -- Set the specified expire time, in milliseconds (a positive integer).
    exat: Option<u64>, // timestamp-seconds -- Set the specified Unix time at which the key will expire, in seconds (a positive integer).
    pxat: Option<u64>, // timestamp-milliseconds -- Set the specified Unix time at which the key will expire, in milliseconds (a positive integer).
    nx: bool,          // -- Only set the key if it does not already exist.
    xx: bool,          // -- Only set the key if it already exists.
    keepttl: bool,     // -- Retain the time to live associated with the key.
    get: bool, // -- Return the old string stored at key, or nil if key did not exist. An error is returned and SET aborted if the value stored at key is not a string.
}

impl SetCommandOptions {
    fn from_rest_params(params: Vec<String>) -> anyhow::Result<SetCommandOptions> {
        let mut ex = None;
        let mut px = None;
        let mut exat = None;
        let mut pxat = None;
        let mut nx = false;
        let mut xx = false;
        let mut keepttl = false;
        let mut get = false;

        let mut it = params.iter();
        while let Some(param) = it.next() {
            if param.eq_ignore_ascii_case("ex") {
                if let Some(val) = it.next().and_then(|next| next.parse::<u64>().ok()) {
                    ex = Some(val);
                } else {
                    anyhow::bail!("Invalid ex parameter");
                }
            } else if param.eq_ignore_ascii_case("px") {
                if let Some(val) = it.next().and_then(|next| next.parse::<u64>().ok()) {
                    px = Some(val);
                } else {
                    anyhow::bail!("Invalid px parameter");
                }
            } else if param.eq_ignore_ascii_case("exat") {
                if let Some(val) = it.next().and_then(|next| next.parse::<u64>().ok()) {
                    exat = Some(val);
                } else {
                    anyhow::bail!("Invalid exat parameter");
                }
            } else if param.eq_ignore_ascii_case("pxat") {
                if let Some(val) = it.next().and_then(|next| next.parse::<u64>().ok()) {
                    pxat = Some(val);
                } else {
                    anyhow::bail!("Invalid pxat parameter");
                }
            } else if param.eq_ignore_ascii_case("nx") {
                nx = true;
            } else if param.eq_ignore_ascii_case("xx") {
                xx = true;
            } else if param.eq_ignore_ascii_case("keepttl") {
                keepttl = true;
            } else if param.eq_ignore_ascii_case("get") {
                get = true;
            }
        }

        Ok(SetCommandOptions {
            ex,
            px,
            exat,
            pxat,
            nx,
            xx,
            keepttl,
            get,
        })
    }
}

#[derive(Debug)]
enum Command {
    Ping,
    Echo(String),
    Set(String, String, SetCommandOptions),
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

        async fn read_rest_params<T>(lines: &mut Lines<T>, n: usize) -> anyhow::Result<Vec<String>>
        where
            T: AsyncBufReadExt + Unpin,
        {
            let mut params = Vec::with_capacity(n);
            for _ in 0..n {
                params.push(read_param(lines).await?);
            }

            Ok(params)
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

            let rest_params = read_rest_params(&mut lines, n_params - 2).await?;
            let options = SetCommandOptions::from_rest_params(rest_params)?;

            return Ok(Command::Set(key, value, options));
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

struct StoreValue {
    value: String,
    expires_on: Option<SystemTime>,
}

impl StoreValue {
    fn new(value: String, expires_on: Option<SystemTime>) -> Self {
        StoreValue { value, expires_on }
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
    store: Arc<RwLock<HashMap<String, StoreValue>>>,
) -> anyhow::Result<()> {
    let (read, mut write) = stream.split();
    let mut reader = BufReader::new(read);
    while let Ok(command) = Command::from_buffer(&mut reader).await {
        println!("Received command: {:?}", command);
        let response = match command {
            Command::Ping => RespMessage::SimpleString("PONG".into()),
            Command::Echo(message) => RespMessage::BulkString(message.clone()),
            Command::Set(key, value, options) => {
                let mut expires_on = None;
                if let Some(px) = options.px {
                    if let Some(t) = SystemTime::now().checked_add(Duration::from_millis(px)) {
                        expires_on = Some(t);
                    } else {
                        anyhow::bail!("Invalid px");
                    }
                }
                store
                    .write()
                    .await
                    .insert(key, StoreValue::new(value, expires_on));

                RespMessage::SimpleString("OK".into())
            }
            Command::Get(key) => {
                let r_store = store.read().await;
                if let Some(entry) = r_store.get(&key) {
                    if let Some(expires_on) = entry.expires_on {
                        if expires_on < SystemTime::now() {
                            drop(r_store);
                            let mut w_store = store.write().await;
                            w_store.remove(&key);
                            RespMessage::Null
                        } else {
                            RespMessage::BulkString(entry.value.clone())
                        }
                    } else {
                        RespMessage::BulkString(entry.value.clone())
                    }
                } else {
                    RespMessage::Null
                }
            }
        };

        response.write(&mut write).await?;
    }

    return Ok(());
}
