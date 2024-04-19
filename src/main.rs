use std::net::TcpListener;
use std::io::{Read, Write};

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    let pong_resp = simple_resp("PONG");
    
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let mut buffer = [0; 1024];
                stream.read(&mut buffer).unwrap();
                if stream.write_all(pong_resp.as_bytes()).is_err() {
                    println!("Error writing to stream");
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn simple_resp(message: &str) -> String {
    format!("+{}\r\n", message)
}
