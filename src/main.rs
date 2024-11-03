use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

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
    let reader = BufReader::new(read);
    let mut lines = reader.lines();
    while let Some(line) = lines.next_line().await? {
        println!("Received {:?}", line);
        match line.as_str() {
            "ping" => {
                write.write_all(b"+PONG\r\n").await?;
            }
            _ => continue,
        }
    }
    return Ok(());
}
