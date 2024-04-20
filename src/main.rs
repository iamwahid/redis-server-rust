use std::collections::HashMap;
use std::net::TcpListener;
use std::io::{BufRead, Read, Write};
use std::sync::{Arc, Mutex};
use std::thread;
use std::env;
use tokio::time::{Duration, Instant};


fn main() {
    let data_store: Arc<Mutex<HashMap<String, DataStoreValue>>> = Arc::new(Mutex::new(HashMap::new()));
    // parse cli args
    let args: Vec<_> = env::args().collect();
    let port = if let Some(port) = args.get(2) {
        port.parse::<u16>().unwrap()
    } else {
        6379
    };
    let server_repl_config: Arc<ServerReplicationConfig> = Arc::new(ServerReplicationConfig{role: String::from("master")});

    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).unwrap();
    for stream in listener.incoming() {
        let data_store = data_store.clone();
        let server_repl_config = server_repl_config.clone();
        let _worker = thread::spawn(
            move || {
                match stream {
                    Ok(mut stream) => loop {
                        let mut buffer = [0; 1024];
                        let read_count = stream.read(&mut buffer).unwrap_or_default();
                        if read_count == 0 {
                            break;
                        }

                        let response = process_req(&buffer, data_store.clone(), server_repl_config.clone());

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

fn bulk_string_resp(message: &str) -> String {
    format!("${}\r\n{}\r\n", message.len(), message)
}

enum Command {
    Ping,
    Echo,
    Set,
    Get,
    Info,
}

#[derive(Debug)]
enum SetExpiry {
    Ex,
    Px,
}

struct DataStoreValue {
    value: String,
    created_at: Instant,
    expired_in: Option<Duration>,
}

struct ServerReplicationConfig {
    role: String,
}

// parse buffer as vector
fn process_req(&buffer: &[u8; 1024], data_store: Arc<Mutex<HashMap<String, DataStoreValue>>>, server_repl_config: Arc<ServerReplicationConfig>) -> String {
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
    while command_items.len() < command_length as usize {
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
                "info" => Some(Command::Info),
                _ => None,
            }
        },
        None => {
            None
        }
    };

    if let Some(command) = command {
        match command {
            Command::Ping => {
                response = simple_resp("PONG");
            },
            Command::Echo => {
                match command_items.get(1) {
                    Some((_pre, message)) => {
                        let message = *message;
                        response = simple_resp(message.as_str());
                    },
                    None => {
                        println!("no message");
                    }
                };
            },
            Command::Set => {
                response = simple_resp("OK");
                let key = match command_items.get(1) {
                    Some((_pre, message)) => {
                        let message = *message;
                        Some(message.clone())
                    },
                    None => None
                };
                let value = match command_items.get(2) {
                    Some((_pre, message)) => {
                        let message = *message;
                        Some(message.clone())
                    },
                    None => None
                };
                let expiry_type = match command_items.get(3) {
                    Some((_pre, command)) => {
                        let command = *command;
                        match command.to_ascii_lowercase().as_str() {
                            "ex" => {
                                Some(SetExpiry::Ex)
                            },
                            "px" => {
                                Some(SetExpiry::Px)
                            },
                            _ => None
                        }
                    },
                    None => None
                };
                let expiry_number = match command_items.get(4) {
                    Some((_pre, expiry)) => {
                        let expiry = *expiry;
                        Some(expiry.clone().parse::<u64>().unwrap_or(0))
                    },
                    None => None
                };
                match (key, value, expiry_type, expiry_number) {
                    (Some(key), Some(value), Some(expiry_type), Some(expiry_number)) => {
                        // default to milliseconds
                        let expiry_number = match expiry_type {
                            SetExpiry::Ex => expiry_number * 1000,
                            SetExpiry::Px => expiry_number,
                        };
                        data_store.lock().unwrap().insert(key, DataStoreValue{value, created_at: Instant::now(), expired_in: Some(Duration::from_millis(expiry_number))});
                    },
                    (Some(key), Some(value), None, None) => {
                        data_store.lock().unwrap().insert(key, DataStoreValue{value, created_at: Instant::now(), expired_in: None});
                    },
                    _ => println!("no set"),
                }
            },
            Command::Get => {
                match command_items.get(1) {
                    Some((_pre, key)) => {
                        let key = *key;
                        let mut data_store = data_store.lock().unwrap();
                        if let Some(value) = data_store.get(key) {
                            if let (Some(elapsed), Some(expired_in)) = (Instant::now().checked_duration_since(value.created_at), value.expired_in) {
                                if elapsed.as_millis() >= expired_in.as_millis() {
                                    data_store.remove(key);
                                    response = null_resp();
                                } else {
                                    let value = value.value.clone();
                                    response = simple_resp(value.as_str());
                                }
                            } else {
                                let value = value.value.clone();
                                response = simple_resp(value.as_str());
                            }
                        } else {
                            response = null_resp();
                        }
                    },
                    None => {
                        response = null_resp();
                    }
                };
            },
            Command::Info => {
                match command_items.get(1) {
                    Some((_pre, info_type)) => {
                        let info_type = *info_type;
                        if info_type.eq("replication") {
                            response = bulk_string_resp(format!("role:{}", server_repl_config.role).as_str());
                        } else {
                            response = null_resp();
                        }
                    },
                    None => {
                        println!("no message");
                    }
                };
            }
        }
    }

    response
}
