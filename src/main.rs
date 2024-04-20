use std::collections::HashMap;
use std::net::TcpListener;
use std::io::{BufRead, Read, Write};
use std::sync::{Arc, Mutex};
use std::thread;


fn main() {
    let data_store: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    for stream in listener.incoming() {
        let data_store = data_store.clone();
        let _worker = thread::spawn(
            move || {
                match stream {
                    Ok(mut stream) => loop {
                        let mut buffer = [0; 1024];
                        let read_count = stream.read(&mut buffer).unwrap_or_default();
                        if read_count == 0 {
                            break;
                        }

                        let response = process_req(&buffer, data_store.clone());

                        if stream.write_all(response.as_bytes()).is_err() {
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

fn null_resp() -> String {
    // null bulk string response
    format!("$-1\r\n")
}

enum Command {
    Ping,
    Echo,
    Set,
    Get,
}

// parse buffer as vector
fn process_req(&buffer: &[u8; 1024], data_store: Arc<Mutex<HashMap<String, String>>>) -> String {
    let mut response = simple_resp("");
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
            match command.clone().to_ascii_lowercase().as_str() {
                "ping" => Some(Command::Ping),
                "echo" => Some(Command::Echo),
                "set" => Some(Command::Set),
                "get" => Some(Command::Get),
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
                        response = simple_resp(message.as_str());
                    },
                    None => {
                        println!("no message");
                    }
                };
            },
            Command::Ping => {
                response = simple_resp("PONG");
            },
            Command::Set => {
                response = simple_resp("OK");
                let key = match command_items.get(1) {
                    Some((_pre, message)) => {
                        let message = *message;
                        println!("key {}", message);
                        Some(message.clone())
                    },
                    None => None
                };
                let value = match command_items.get(2) {
                    Some((_pre, message)) => {
                        let message = *message;
                        println!("value {}", message);
                        Some(message.clone())
                    },
                    None => None
                };
                match (key, value) {
                    (Some(key), Some(value)) => {
                        data_store.lock().unwrap().insert(key, value);
                    },
                    _ => println!("no set"),
                }
            },
            Command::Get => {
                match command_items.get(1) {
                    Some((_pre, key)) => {
                        let key = *key;
                        if let Some(value) = data_store.lock().unwrap().get(key) {
                            response = simple_resp(value.as_str());
                        } else {
                            response = null_resp();
                        }
                    },
                    None => {
                        response = null_resp();
                    }
                };
            }
        }
    }

    response
}
