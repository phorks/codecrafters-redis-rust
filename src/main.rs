#![allow(unused_imports)]
use std::{fmt::Write, io::Read, net::TcpListener};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                println!("accepted new connection");
                let mut input = String::new();
                _stream.read_to_string(&mut input).unwrap();
                match input.as_str() {
                    "*1\r\n$4\r\nping\r\n" => {
                        input.write_str("+PONG\r\n").unwrap();
                    }
                    _ => {}
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
