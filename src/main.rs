#![allow(unused_imports)]
use std::{
    io::{BufRead, BufReader, BufWriter, Read, Write},
    net::TcpListener,
};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                let reader = BufReader::new(&stream);
                let mut writer = BufWriter::new(&stream);
                for line in reader.lines() {
                    if let Ok(_line) = line {
                        writer.write(b"+PONG\r\n").unwrap();
                    } else {
                        break;
                    }
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
