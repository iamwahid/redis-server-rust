use std::net::TcpListener;
use std::io::{Read, Write};
use std::thread;

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    for stream in listener.incoming() {
        let worker = thread::spawn(
            move || {
                match stream {
                    Ok(mut stream) => {
                        let mut buffer = [0; 1024];
                        stream.read(&mut buffer).unwrap();
                        if stream.write_all(simple_resp("PONG").as_bytes()).is_err() {
                            println!("Error writing to stream");
                        }
                    }
                    Err(e) => {
                        println!("error: {}", e);
                    }
                }
            }
        );
        let _ = worker.join();
    }
}

fn simple_resp(message: &str) -> String {
    format!("+{}\r\n", message)
}
