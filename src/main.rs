use std::net::TcpListener;
use std::io::{BufRead, Read, Write};
use std::thread;


fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    for stream in listener.incoming() {
        let _worker = thread::spawn(
            move || {
                match stream {
                    Ok(mut stream) => loop {
                        let mut buffer = [0; 1024];
                        let read_count = stream.read(&mut buffer).unwrap_or_default();
                        if read_count == 0 {
                            break;
                        }

                        let response = parse_req(&buffer);

                        if stream.write_all(simple_resp(&response).as_bytes()).is_err() {
                            println!("Error writing to stream");
                        }
                    }
                    Err(e) => {
                        println!("error: {}", e);
                    }
                }
            }
        );
    }
}

fn simple_resp(message: &str) -> String {
    format!("+{}\r\n", message)
}

enum Command {
    Ping,
    Echo,
}

// parse buffer as vector
fn parse_req(&buffer: &[u8; 1024]) -> String {
    let mut response = String::from("");
    let command: Vec<_> = buffer
        .lines()
        .map(|r| r.unwrap().replace("\x00", ""))
        .take_while(|line| !line.is_empty())
        .collect();

    // PARSING
    let command_length = match command.get(0) {
        Some(fi) => {
            if fi.starts_with("*") {
                let length =  fi.replace("*", "").parse::<i8>().unwrap();
                length
            } else {
                -1
            }
        },
        None => {
            -1
        },
    };

    let mut command_items = Vec::with_capacity(command_length as usize);
    let mut offset = 1;
    while offset <= command_length+2 {
        match (command.get(offset as usize), command.get(offset as usize + 1)) {
            (Some(com1), Some(com2)) => {
                command_items.push((com1, com2));
            },
            _ => {
                break;
            }
        };
        offset = offset+2;
    }

    // parse command
    let command : Option<Command> = match command_items.get(0) {
        Some((_pre, command)) => {
            let command = *command;
            match command.clone().as_str() {
                "ping" => Some(Command::Ping),
                "echo" => Some(Command::Echo),
                _ => None,
            }
        },
        None => {
            None
        }
    };

    if let Some(command) = command {
        match command {
            Command::Echo => {
                match command_items.get(1) {
                    Some((_pre, message)) => {
                        let message = *message;
                        println!("{}", message);
                        response = message.clone();
                    },
                    None => {
                        println!("no message");
                    }
                };
            },
            Command::Ping => {
                response = String::from("PONG");
            },
        }
    }

    response
}
