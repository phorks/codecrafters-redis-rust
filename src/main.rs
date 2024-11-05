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

async fn create_database_from_file(config: &ServerConfig) -> anyhow::Result<Database> {
    let db_path = config.db_path().unwrap();
    let mut file = fs::File::open(db_path).await?;
    let rdb = Instance::new(&mut file).await?;

    println!("Initial db:");
    let mut i = 0;
    for db in &rdb.dbs {
        for entry in &db.1.entries {
            println!(
                "DB{}: {} = {}",
                i,
                entry.0.to_string(),
                entry.1.value.to_string()
            );
        }
        i += 1;
    }

    rdb.dbs
        .into_iter()
        .next()
        .map(|x| x.1)
        .ok_or(anyhow::Error::msg("No database specified"))
}

#[tokio::main]
async fn main() {
    let config = Arc::new(ServerConfig::new(env::args().collect()));
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    let store = match create_database_from_file(&config).await {
        Ok(store) => store,
        Err(err) => {
            eprintln!("Failed to read from the rdb file: {}", err);
            Database::new()
        }
    };

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
