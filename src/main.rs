use std::env;
use std::sync::Arc;

use client::new_client;
use redis::{Database, Instance};
use server::ServerConfig;
use tokio::fs;
use tokio::net::TcpListener;
use tokio::sync::RwLock;

mod client;
mod commands;
mod redis;
mod server;

async fn create_database_from_file(config: &ServerConfig) -> Option<Database> {
    let db_path = config.db_path()?;
    let mut file = fs::File::open(db_path).await.ok()?;
    let rdb = Instance::new(&mut file).await.ok()?;
    rdb.dbs.into_iter().next().map(|x| x.1)
}

#[tokio::main]
async fn main() {
    let config = Arc::new(ServerConfig::new(env::args().collect()));
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let store = create_database_from_file(&config)
        .await
        .unwrap_or_else(&Database::new);
    let store = Arc::new(RwLock::new(store));

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
