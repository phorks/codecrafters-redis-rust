use tokio::io::{AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader, Lines};

pub enum RespMessage {
    Array(Vec<RespMessage>),
    BulkString(String),
    Null,
    SimpleString(String),
}

impl RespMessage {
    pub async fn write<T: AsyncWrite + Unpin>(self, write: &mut T) -> anyhow::Result<()> {
        match self {
            RespMessage::Array(items) => {
                write.write_all(b"*").await?;
                write.write_all(items.len().to_string().as_bytes()).await?;
                write.write_all(b"\r\n").await?;
                for item in items {
                    Box::pin(item.write(write)).await?;
                }
            }
            RespMessage::BulkString(s) => {
                write.write_all(b"+").await?;
                write.write_all(s.as_bytes()).await?;
                write.write_all(b"\r\n").await?;
            }
            RespMessage::Null => {
                write.write_all(b"$-1\r\n").await?;
            }
            RespMessage::SimpleString(s) => {
                let bytes = s.as_bytes();

                write.write_all(b"$").await?;
                write.write_all(bytes.len().to_string().as_bytes()).await?;
                write.write_all(b"\r\n").await?;
                write.write_all(bytes).await?;
                write.write_all(b"\r\n").await?;
            }
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct SetCommandOptions {
    pub ex: Option<u64>, // seconds -- Set the specified expire time, in seconds (a positive integer).
    pub px: Option<u64>, // milliseconds -- Set the specified expire time, in milliseconds (a positive integer).
    pub exat: Option<u64>, // timestamp-seconds -- Set the specified Unix time at which the key will expire, in seconds (a positive integer).
    pub pxat: Option<u64>, // timestamp-milliseconds -- Set the specified Unix time at which the key will expire, in milliseconds (a positive integer).
    pub nx: bool,          // -- Only set the key if it does not already exist.
    pub xx: bool,          // -- Only set the key if it already exists.
    pub keepttl: bool,     // -- Retain the time to live associated with the key.
    pub get: bool, // -- Return the old string stored at key, or nil if key did not exist. An error is returned and SET aborted if the value stored at key is not a string.
}

impl SetCommandOptions {
    pub fn from_rest_params(params: Vec<String>) -> anyhow::Result<SetCommandOptions> {
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
pub enum Command {
    Ping,
    Echo(String),
    Set(String, String, SetCommandOptions),
    Get(String),
    Config(String, Vec<String>),
    Keys(String),
}

impl Command {
    pub async fn from_buffer<T: AsyncBufReadExt + Unpin>(read: &mut T) -> anyhow::Result<Command> {
        async fn read_param<T: AsyncBufReadExt + Unpin>(
            lines: &mut Lines<T>,
        ) -> anyhow::Result<String> {
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

        async fn read_rest_params<T: AsyncBufReadExt + Unpin>(
            lines: &mut Lines<T>,
            n: usize,
        ) -> anyhow::Result<Vec<String>> {
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
        } else if name.eq_ignore_ascii_case("config") {
            if n_params == 0 {
                anyhow::bail!("Missing CONFIG command action parameter");
            }

            let action = read_param(&mut lines).await?;
            let params = read_rest_params(&mut lines, n_params - 1).await?;

            return Ok(Command::Config(action, params));
        } else if name.eq_ignore_ascii_case("keys") {
            if n_params == 0 {
                anyhow::bail!("Missing the PATTERN arg")
            }

            let pattern = read_param(&mut lines).await?;

            return Ok(Command::Keys(pattern));
        }

        anyhow::bail!("Unknown command");
    }
}
