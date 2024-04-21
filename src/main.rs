use std::collections::{HashMap, HashSet};
use std::io::BufRead;
use std::net::SocketAddr;
use std::num::ParseIntError;
use std::sync::{Arc, Mutex};
use std::{env, env::Args};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::WriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{self, Duration, Instant};

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

    let mut bind_address = ("127.0.0.1", DEFAULT_PORT);
    for arg in parsed.into_iter() {
        match arg {
            ServerArg::Port(port) => {
                bind_address.1 = port;
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
            // handle handshake
            handle_repl_handshake(&mut client, bind_address.1).await;
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

            let mut client_buffer = [0u8; 512];
            tokio::select! {
                stream = listener.accept() => {
                    let (stream, client) = stream.unwrap();
                    process_connection(client, stream, data_store.clone(), server_repl_config.clone()).await;
                },
                repl_buffer = input_stream.read(&mut client_buffer) => {
                    // receive replication
                    match repl_buffer {
                        Ok(n) => {
                            if n != 0 {
                                process_repl_connection(client_buffer, output_stream).await;
                            }
                        },
                        Err(_error) => (),
                    }
                }
            }
        } else {
            let (stream, client) = listener.accept().await.unwrap();
            println!("from client {:?}", client);
            process_connection(
                client,
                stream,
                data_store.clone(),
                server_repl_config.clone(),
            )
            .await;
        }
    }
}

fn simple_resp(message: &str) -> String {
    format!("+{}\r\n", message)
}

fn simple_error_resp(message: &str) -> String {
    format!("-{}\r\n", message)
}

fn arg_error_resp(command: &str) -> String {
    simple_error_resp(format!("ERR wrong number of arguments for '{}' command", command).as_str())
}

fn null_resp() -> String {
    // null bulk string response
    format!("$-1\r\n")
}

fn bulk_string_resp(message: &str) -> String {
    format!("${}\r\n{}\r\n", message.len(), message)
}

fn array_resp(messages: Vec<&str>) -> String {
    let mut resp = format!("*{}\r\n", messages.len());
    for message in messages.into_iter() {
        resp = format!("{}{}", resp, bulk_string_resp(message));
    }
    resp
}

fn decode_hex(s: &str) -> Result<Vec<u8>, ParseIntError> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16))
        .collect()
}

fn empty_rdb_resp() -> Vec<u8> {
    let empty_rdb = decode_hex("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2").unwrap();
    let len = format!("${}\r\n", empty_rdb.len());
    [len.as_bytes(), &empty_rdb].concat()
}

#[derive(Hash, Eq, PartialEq, Debug)]
enum ServerArg {
    Port(u16),
    ReplicaOf(String, u16),
}

#[derive(Debug)]
enum Command {
    Ping,
    Echo(Vec<(String, String)>),
    Set(Vec<(String, String)>),
    Get(Vec<(String, String)>),
    Info(Vec<(String, String)>),
    Replconf(Vec<(String, String)>),
    Psync(Vec<(String, String)>),
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

#[derive(Default, Debug)]
struct ResponseContext {
    responses: Vec<Vec<u8>>,
    add_repl_client: bool,
}

async fn handle_repl_handshake(client: &mut TcpStream, bind_port: u16) {
    // connection handshake
    let (input_stream, output_stream) = &mut client.split();
    let mut stage = 1;
    let mut client_buffer = [0u8; 512];
    // send PING
    output_stream
        .write_all(array_resp(vec!["PING"]).as_bytes())
        .await
        .unwrap();

    let bind_port = format!("{}", bind_port);
    let handshakes: HashMap<i32, Vec<&str>> = HashMap::from_iter([
        (2, vec!["REPLCONF", "listening-port", bind_port.as_str()]),
        (3, vec!["REPLCONF", "capa", "psync2"]),
        (4, vec!["PSYNC", "?", "-1"]),
    ]);
    loop {
        match input_stream.read(&mut client_buffer).await {
            Ok(read_count) => {
                if read_count != 0 {
                    match stage {
                        1 => stage = 2,
                        2 => stage = 3,
                        3 => stage = 4,
                        _ => break,
                    };
                    output_stream
                        .write_all(array_resp(handshakes.get(&stage).unwrap().clone()).as_bytes())
                        .await
                        .unwrap();
                    if stage == 4 {
                        break;
                    }
                } else {
                    println!("No reply");
                    break;
                }
            }
            Err(_) => {
                break;
            }
        }
    }
    println!("Entering replica output mode...  (press Ctrl-C to quit)");
}

async fn process_connection(
    client: SocketAddr,
    mut stream: TcpStream,
    data_store: Arc<Mutex<HashMap<String, DataStoreValue>>>,
    server_repl_config: Arc<Mutex<ServerReplicationConfig>>,
) {
    let data_store = data_store.clone();
    let server_repl_config = server_repl_config.clone();

    // determine if it replication connection

    let _worker = tokio::spawn(async move {
        loop {
            let mut buffer = [0; 1024];
            let read_count = stream.read(&mut buffer).await.unwrap_or_default();
            if read_count == 0 {
                break;
            }

            let command = parse_req(&buffer);
            if let Some(command) = command {
                let context =
                    process_command(command, data_store.clone(), server_repl_config.clone());
                for response in context.responses.into_iter() {
                    time::sleep(Duration::from_millis(50)).await;
                    if stream.write_all(&response).await.is_err() {
                        println!("Error writing to stream");
                    }
                }

                if context.add_repl_client {
                    println!("replication client {:?}", client);
                    // move process in new thread
                }
            }
        }
    });
}

async fn process_repl_connection<'a>(client_buffer: [u8; 512], _output_stream: &mut WriteHalf<'a>) {
    let command = client_buffer
        .to_vec()
        .into_iter()
        .map(|r| r as char)
        .collect::<String>();
    if command.starts_with("+FULLRESYNC") {
        print!("PSYNC replied {}", command);
    } else if command.starts_with("$") && !command.ends_with("\r\n") {
        let len: Vec<_> = command.split("\r\n").into_iter().collect();
        println!(
            "Full resync with master, discarding {} bytes of bulk transfer...",
            len.first().unwrap().replace("$", "")
        );
        println!("Full resync done. Logging commands from master.");
    }
}

fn parse_args(args: Args) -> HashSet<ServerArg> {
    let args: Vec<_> = args.collect();

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
fn parse_req(&buffer: &[u8; 1024]) -> Option<Command> {
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
                command_items.push((com1.clone(), com2.clone()));
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
            let com_args = command_items[1..].to_vec();
            match command.to_ascii_lowercase().as_str() {
                "ping" => Some(Command::Ping),
                "echo" => Some(Command::Echo(com_args)),
                "set" => Some(Command::Set(com_args)),
                "get" => Some(Command::Get(com_args)),
                "info" => Some(Command::Info(com_args)),
                "replconf" => Some(Command::Replconf(com_args)),
                "psync" => Some(Command::Psync(com_args)),
                _ => None,
            }
        }
        None => None,
    };

    command
}

fn process_command(
    command: Command,
    data_store: Arc<Mutex<HashMap<String, DataStoreValue>>>,
    server_repl_config: Arc<Mutex<ServerReplicationConfig>>,
) -> ResponseContext {
    let mut should_send_repl_init = false;
    let mut add_repl_client = false;
    let response = match command {
        Command::Ping => {
            simple_resp("PONG")
        }
        Command::Echo(com_args) => {
            match com_args.get(0) {
                Some((_pre, message)) => {
                    simple_resp(message.as_str())
                }
                None => {
                    arg_error_resp("echo")
                }
            }
        }
        Command::Set(com_args) => {
            let key = match com_args.get(0) {
                Some((_pre, message)) => Some(message.clone()),
                None => None,
            };
            let value = match com_args.get(1) {
                Some((_pre, message)) => Some(message.clone()),
                None => None,
            };
            let expiry_type = match com_args.get(2) {
                Some((_pre, command)) => match command.to_ascii_lowercase().as_str() {
                    "ex" => Some(SetExpiry::Ex),
                    "px" => Some(SetExpiry::Px),
                    _ => None,
                },
                None => None,
            };
            let expiry_number = match com_args.get(3) {
                Some((_pre, expiry)) => Some(expiry.clone().parse::<u64>().unwrap_or(0)),
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
                    simple_resp("OK")
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
                    simple_resp("OK")
                }
                _ => {
                    arg_error_resp("set")
                }
            }
        }
        Command::Get(com_args) => {
            match com_args.get(0) {
                Some((_pre, key)) => {
                    let mut data_store = data_store.lock().unwrap();
                    if let Some(value) = data_store.get(key) {
                        if let (Some(elapsed), Some(expired_in)) = (
                            Instant::now().checked_duration_since(value.created_at),
                            value.expired_in,
                        ) {
                            if elapsed.as_millis() >= expired_in.as_millis() {
                                data_store.remove(key);
                                null_resp()
                            } else {
                                let value = value.value.clone();
                                simple_resp(value.as_str())
                            }
                        } else {
                            let value = value.value.clone();
                            simple_resp(value.as_str())
                        }
                    } else {
                        null_resp()
                    }
                }
                None => {
                    arg_error_resp("get")
                }
            }
        }
        Command::Info(com_args) => {
            match com_args.get(0) {
                Some((_pre, info_type)) => {
                    if info_type.eq("replication") {
                        let repl_config = server_repl_config.lock().unwrap();
                        bulk_string_resp(
                            format!(
                                "role:{}\r\nmaster_replid:{}\r\nmaster_repl_offset:{}",
                                repl_config.role,
                                repl_config.master_replid,
                                repl_config.master_repl_offset
                            )
                            .as_str(),
                        )
                    } else {
                        null_resp()
                    }
                }
                None => {
                    arg_error_resp("info") // TODO: not supported yet
                }
            }
        }
        Command::Replconf(com_args) => {
            match (com_args.get(0), com_args.get(1)) {
                (Some(conf_key), Some(conf_val)) => {
                    println!("REPLCONF {} {}", conf_key.1, conf_val.1);
                }
                _ => (),
            };
            simple_resp("OK")
        }
        Command::Psync(com_args) => {
            let repl_config = server_repl_config.lock().unwrap();
            match (com_args.get(0), com_args.get(1)) {
                (Some(conf_key), Some(conf_val)) => {
                    should_send_repl_init = true;
                    add_repl_client = true;
                    if conf_key.1.to_ascii_lowercase().as_str() == "?"
                        && conf_val.1.to_ascii_lowercase().as_str() == "-1"
                    {
                        simple_resp(
                            format!(
                                "FULLRESYNC {} {}",
                                repl_config.master_replid, repl_config.master_repl_offset
                            )
                            .as_str(),
                        )
                    } else {
                        simple_resp(format!("FULLRESYNC {} {}", conf_key.1, 0).as_str())
                    }
                }
                _ => {
                    arg_error_resp("psync")
                }
            }
        }
    };

    let mut responses = Vec::with_capacity(2);
    responses.push(response.as_bytes().to_vec());
    if should_send_repl_init {
        responses.push(empty_rdb_resp());
    }
    ResponseContext {
        responses,
        add_repl_client,
    }
}
