use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::net::{TcpListener, TcpStream};

async fn write_resp_string<T>(write: &mut T, s: &str) -> anyhow::Result<()>
where
    T: AsyncWriteExt + Unpin,
{
    let bytes = s.as_bytes();

    write.write_all(b"$").await?;
    write.write_all(bytes.len().to_string().as_bytes()).await?;
    write.write_all(b"\r\n").await?;
    write.write_all(bytes).await?;
    write.write_all(b"\r\n").await?;

    Ok(())
}

#[derive(Debug)]
enum Command {
    Ping,
    Echo(String),
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
                Err(e) => println!("Failed: {:?}", e),
            }
            println!("Disconnected from {addr}");
        });
    }
}

async fn handle_connection(mut stream: TcpStream) -> anyhow::Result<()> {
    let (read, mut write) = stream.split();
    let mut reader = BufReader::new(read);
    while let Ok(command) = Command::from_buffer(&mut reader).await {
        println!("Received command: {:?}", command);
        match command {
            Command::Ping => {
                write.write_all(b"+PONG\r\n").await?;
            }
            Command::Echo(message) => {
                write_resp_string(&mut write, &message).await?;
            }
        }
    }

    return Ok(());
}
