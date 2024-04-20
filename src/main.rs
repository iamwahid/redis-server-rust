use std::collections::{HashMap, HashSet};
use std::io::{stdout, BufRead, Write};
use std::sync::{Arc, Mutex};
use std::{env, env::Args};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::WriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{Duration, Instant};

const DEFAULT_PORT: u16 = 6379;

#[tokio::main]
async fn main() {
    let data_store: Arc<Mutex<HashMap<String, DataStoreValue>>> =
        Arc::new(Mutex::new(HashMap::new()));
    // parse cli args
    let parsed = parse_args(env::args());

    // config values
    let mut server_repl_config = ServerReplicationConfig {
        role: String::from("master"),
        master_replid: String::from("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"),
        master_repl_offset: 0,
        ..ServerReplicationConfig::default()
    };

    let mut bind_address = format!("127.0.0.1:{}", DEFAULT_PORT);
    for arg in parsed.into_iter() {
        match arg {
            ServerArg::Port(port) => {
                bind_address = format!("127.0.0.1:{}", port);
            }
            ServerArg::ReplicaOf(host, port) => {
                server_repl_config.role = String::from("slave");
                server_repl_config.master_host = Some(host);
                server_repl_config.master_port = Some(port);
            }
        }
    }

    let listener = TcpListener::bind(bind_address).await.unwrap();

    let mut replication_stream = if let (server_type, Some(master_host), Some(master_port)) = (
        &server_repl_config.role,
        &server_repl_config.master_host,
        server_repl_config.master_port,
    ) {
        if server_type.as_str() == "slave" {
            let mut client = TcpStream::connect((master_host.clone(), master_port))
                .await
                .expect("Failed to connect Master");
            // connection handshake
            let (input_stream, output_stream) = &mut client.split();
            if output_stream
                .write_all(array_resp(vec!["PING".to_string()]).as_bytes())
                .await
                .is_ok()
            {
                let mut client_buffer = [0u8; 512];
                match input_stream.read(&mut client_buffer).await {
                    Ok(n) => {
                        if n == 0 {
                            println!("REPL: PING Failed!");
                        } else {
                            println!("REPL: Replication connected!");
                            // stdout().write(&client_buffer).unwrap();
                            // stdout().flush().unwrap();
                        }
                    }
                    Err(_error) => (),
                }
            } else {
                println!("REPL: replication connection failed!");
            }

            Some(client)
        } else {
            None
        }
    } else {
        None
    };

    println!("MAIN: Server running ...");

    let server_repl_config: Arc<Mutex<ServerReplicationConfig>> =
        Arc::new(Mutex::new(server_repl_config));

    loop {
        // Replication
        if let Some(ref mut replication_stream) = replication_stream {
            let (input_stream, output_stream) = &mut replication_stream.split();

            let mut client_buffer = [0u8; 1024];
            tokio::select! {
                stream = listener.accept() => {
                    let (stream, _) = stream.unwrap();
                    process_connection(stream, data_store.clone(), server_repl_config.clone()).await;
                },
                repl_buffer = input_stream.read(&mut client_buffer) => {
                    match repl_buffer {
                        Ok(n) => {
                            if n == 0 {
                                println!("REPL: master disconnected!");
                                break;
                            } else {
                                println!("REPL: {:?}", client_buffer);
                                process_repl_connection(client_buffer, output_stream).await;
                            }
                        },
                        Err(_error) => (),
                    }
                }
            }
        } else {
            let (stream, _) = listener.accept().await.unwrap();
            process_connection(stream, data_store.clone(), server_repl_config.clone()).await;
        }
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

fn array_resp(messages: Vec<String>) -> String {
    let mut resp = format!("*{}\r\n", messages.len());
    for message in messages.into_iter() {
        resp = format!("{}{}", resp, bulk_string_resp(message.as_str()));
    }
    resp
}

#[derive(Hash, Eq, PartialEq, Debug)]
enum ServerArg {
    Port(u16),
    ReplicaOf(String, u16),
}

#[derive(Debug)]
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

#[derive(Default, Debug)]
struct ServerReplicationConfig {
    role: String,
    master_replid: String,
    master_repl_offset: u8,
    master_host: Option<String>,
    master_port: Option<u16>,
}

async fn process_connection(
    mut stream: TcpStream,
    data_store: Arc<Mutex<HashMap<String, DataStoreValue>>>,
    server_repl_config: Arc<Mutex<ServerReplicationConfig>>,
) {
    let data_store = data_store.clone();
    let server_repl_config = server_repl_config.clone();
    let _worker = tokio::spawn(async move {
        loop {
            let mut buffer = [0; 1024];
            let read_count = stream.read(&mut buffer).await.unwrap_or_default();
            if read_count == 0 {
                break;
            }

            let response = process_req(&buffer, data_store.clone(), server_repl_config.clone());

            if stream.write_all(response.as_bytes()).await.is_err() {
                println!("Error writing to stream");
            }
        }
    });
}

async fn process_repl_connection<'a>(
    mut _client_buffer: [u8; 1024],
    output_stream: &mut WriteHalf<'a>,
) {
    output_stream.write("user_buffer".as_bytes()).await.unwrap();
    output_stream.flush().await.unwrap();
}

fn parse_args(args: Args) -> HashSet<ServerArg> {
    let args: Vec<_> = args.collect();
    // let parsed
    let mut args_iter = args.into_iter();
    let mut parsed_args: HashSet<ServerArg> = HashSet::new();
    loop {
        if let Some(arg) = args_iter.next() {
            match arg.replace("--", "").as_str() {
                "port" => {
                    let port = if let Some(port) = args_iter.next() {
                        port.parse::<u16>().expect("invalid Port")
                    } else {
                        panic!("Port empty");
                    };
                    parsed_args.insert(ServerArg::Port(port));
                }
                "replicaof" => {
                    let host = args_iter.next().unwrap();
                    let port = if let Some(port) = args_iter.next() {
                        port.parse::<u16>().expect("invalid Port")
                    } else {
                        DEFAULT_PORT
                    };
                    parsed_args.insert(ServerArg::ReplicaOf(host, port));
                }
                _ => (),
            }
        } else {
            break;
        }
    }
    parsed_args
}

// parse buffer as vector
fn process_req(
    &buffer: &[u8; 1024],
    data_store: Arc<Mutex<HashMap<String, DataStoreValue>>>,
    server_repl_config: Arc<Mutex<ServerReplicationConfig>>,
) -> String {
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
                let length = fi.replace("*", "").parse::<i8>().unwrap();
                length
            } else {
                -1
            }
        }
        None => -1,
    };

    let mut command_items = Vec::with_capacity(command_length as usize);
    let mut offset = 1;
    while command_items.len() < command_length as usize {
        match (
            command.get(offset as usize),
            command.get(offset as usize + 1),
        ) {
            (Some(com1), Some(com2)) => {
                command_items.push((com1, com2));
            }
            _ => {
                break;
            }
        };
        offset = offset + 2;
    }

    // parse command
    let command: Option<Command> = match command_items.get(0) {
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
        }
        None => None,
    };

    if let Some(command) = command {
        match command {
            Command::Ping => {
                response = simple_resp("PONG");
            }
            Command::Echo => {
                match command_items.get(1) {
                    Some((_pre, message)) => {
                        let message = *message;
                        response = simple_resp(message.as_str());
                    }
                    None => {
                        println!("no message");
                    }
                };
            }
            Command::Set => {
                response = simple_resp("OK");
                let key = match command_items.get(1) {
                    Some((_pre, message)) => {
                        let message = *message;
                        Some(message.clone())
                    }
                    None => None,
                };
                let value = match command_items.get(2) {
                    Some((_pre, message)) => {
                        let message = *message;
                        Some(message.clone())
                    }
                    None => None,
                };
                let expiry_type = match command_items.get(3) {
                    Some((_pre, command)) => {
                        let command = *command;
                        match command.to_ascii_lowercase().as_str() {
                            "ex" => Some(SetExpiry::Ex),
                            "px" => Some(SetExpiry::Px),
                            _ => None,
                        }
                    }
                    None => None,
                };
                let expiry_number = match command_items.get(4) {
                    Some((_pre, expiry)) => {
                        let expiry = *expiry;
                        Some(expiry.clone().parse::<u64>().unwrap_or(0))
                    }
                    None => None,
                };
                match (key, value, expiry_type, expiry_number) {
                    (Some(key), Some(value), Some(expiry_type), Some(expiry_number)) => {
                        // default to milliseconds
                        let expiry_number = match expiry_type {
                            SetExpiry::Ex => expiry_number * 1000,
                            SetExpiry::Px => expiry_number,
                        };
                        data_store.lock().unwrap().insert(
                            key,
                            DataStoreValue {
                                value,
                                created_at: Instant::now(),
                                expired_in: Some(Duration::from_millis(expiry_number)),
                            },
                        );
                    }
                    (Some(key), Some(value), None, None) => {
                        data_store.lock().unwrap().insert(
                            key,
                            DataStoreValue {
                                value,
                                created_at: Instant::now(),
                                expired_in: None,
                            },
                        );
                    }
                    _ => (),
                }
            }
            Command::Get => {
                match command_items.get(1) {
                    Some((_pre, key)) => {
                        let key = *key;
                        let mut data_store = data_store.lock().unwrap();
                        if let Some(value) = data_store.get(key) {
                            if let (Some(elapsed), Some(expired_in)) = (
                                Instant::now().checked_duration_since(value.created_at),
                                value.expired_in,
                            ) {
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
                    }
                    None => {
                        response = null_resp();
                    }
                };
            }
            Command::Info => {
                match command_items.get(1) {
                    Some((_pre, info_type)) => {
                        let info_type = *info_type;
                        if info_type.eq("replication") {
                            let repl_config = server_repl_config.lock().unwrap();
                            response = bulk_string_resp(
                                format!(
                                    "role:{}\r\nmaster_replid:{}\r\nmaster_repl_offset:{}",
                                    repl_config.role,
                                    repl_config.master_replid,
                                    repl_config.master_repl_offset
                                )
                                .as_str(),
                            );
                        } else {
                            response = null_resp();
                        }
                    }
                    None => {
                        println!("no message");
                    }
                };
            }
        }
    }

    response
}
