use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

enum Command {
    Ping,
    Echo(String),
}

impl Command {
    async fn from_buffer<T>(reader: &mut T) -> anyhow::Result<Command>
    where
        T: AsyncBufReadExt + Unpin,
    {
        let mut lines = reader.lines();
        let Some(params_line) = lines.next_line().await? else {
            anyhow::bail!("Empty params line")
        };
        let n_params = params_line.parse::<usize>()?;

        _ = lines.next_line().await?;
        let Some(name) = lines.next_line().await? else {
            anyhow::bail!("No command name")
        };

        if name.eq_ignore_ascii_case("ping") {
            if n_params != 0 {
                anyhow::bail!("Incorrect number of parameters for ping command");
            }

            return Ok(Command::Ping);
        } else if name.eq_ignore_ascii_case("hello") {
            if n_params != 1 {
                anyhow::bail!(
                    "Incorrect number of parameters, expected: 2, received {}",
                    n_params
                );
            }

            _ = lines.next_line().await?;
            return Ok(Command::Echo(
                lines.next_line().await?.unwrap_or(String::new()),
            ));
        }

        anyhow::bail!("Unknown command");
    }
}

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    loop {
        let (stream, addr) = listener.accept().await.unwrap();
        println!("Accepted connection from {addr}");
        tokio::spawn(async move {
            match handle_connection(stream).await {
                Ok(_) => println!("Success"),
                Err(e) => println!("{e}"),
            }
            println!("Disconnected from {addr}");
        });
    }
}

async fn handle_connection(mut stream: TcpStream) -> Result<(), std::io::Error> {
    let (read, mut write) = stream.split();
    let mut reader = BufReader::new(read);
    while let Ok(command) = Command::from_buffer(&mut reader).await {
        match command {
            Command::Ping => write.write_all(b"+PONG\r\n").await?,
            Command::Echo(message) => write.write_all(message.as_bytes()).await?,
        }
    }

    return Ok(());
}
