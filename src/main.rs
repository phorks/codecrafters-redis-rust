use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::env;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use client::new_client;
use commands::{Command, RespMessage};
use server::ServerConfig;
use store::StoreValue;
use tokio::io::{AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader, Lines};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

mod client;
mod commands;
mod server;
mod store;

#[tokio::main]
async fn main() {
    let config = Arc::new(ServerConfig::new(env::args().collect()));
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let store = Arc::new(RwLock::new(HashMap::new()));
    loop {
        let (stream, addr) = listener.accept().await.unwrap();
        let store = store.clone();
        let config = config.clone();
        println!("Accepted connection from {addr}");
        tokio::spawn(async move {
            let client = new_client(stream, store, config);
            match client.run().await {
                Ok(_) => println!("Successfully disconnected from {addr}"),
                Err(err) => println!("Disconnected because of a failure: {:?}", err),
            }
        });
    }
}
