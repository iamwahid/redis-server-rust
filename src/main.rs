use std::collections::{HashMap, HashSet};
use std::io::{BufRead, ErrorKind};
use std::net::SocketAddr;
use std::num::ParseIntError;
use std::sync::{Arc, Mutex as StdMutex};
use std::{env, env::Args};
use tokio::sync::Mutex;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{Duration, Instant};

const DEFAULT_PORT: u16 = 6379;
const BUFFER_SIZE: usize = 512;

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

    let replication_stream = if let (server_type, Some(master_host), Some(master_port)) = (
        &server_repl_config.role,
        &server_repl_config.master_host,
        server_repl_config.master_port,
    ) {
        if server_type.as_str() == "slave" {
            let mut client = TcpStream::connect((master_host.clone(), master_port))
                .await
                .expect("Failed to connect Master");
            // handle handshake
            handle_repl_handshake(&mut client, bind_address.1).await.unwrap();
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

    listener_loop(listener, replication_stream, data_store, server_repl_config).await
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

#[derive(Debug, Clone, Default)]
enum Command {
    #[default]
    Ping,
    Echo(Vec<String>),
    Set(Vec<String>),
    Get(Vec<String>),
    Info(Vec<String>),
    Replconf(Vec<String>),
    Psync(Vec<String>),
}

// impl Clone for Command { fn clone(&self) -> Self { *self } }

#[derive(Debug)]
enum SetExpiry {
    Ex(u64),
    Px(u64),
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
    repl_clients: HashMap<SocketAddr, Arc<Mutex<TcpStream>>>,
}

impl ServerReplicationConfig {
    pub async fn send_to_replicas(&mut self, repl_command: String) -> Result<(), String> {
        if &self.role == "master" {
            for (addr, send_stream) in &self.repl_clients {
                let mut send_stream = send_stream.lock().await;
                if repl_command.len() > 0 { 
                    match send_stream.write(&repl_command.as_bytes()).await {
                        Ok(_) => {
                            send_stream.flush().await.unwrap();
                        },
                        Err(e) if e.kind() == ErrorKind::ConnectionAborted || e.kind() == ErrorKind::BrokenPipe => {
                            println!("client disconnected {:?}", addr);
                        }
                        Err(e) => {
                            println!("Error {:?}", e);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn send_to_client(&self, context: &RequestContext, stream: &mut TcpStream) -> Result<(), String> {
        write_response(stream, context.responses.clone()).await?;
        Ok(())
    }
}

#[derive(Default, Debug, Clone)]
struct RequestContext {
    responses: Vec<Vec<u8>>,
    add_repl_client: bool,
}

struct ConnectionManager {
    stream: Arc<Mutex<TcpStream>>,
    addr: SocketAddr,
}

impl ConnectionManager {
    pub fn new(stream: Arc<Mutex<TcpStream>>, addr: SocketAddr) -> Self {
        ConnectionManager{
            stream,
            addr

        }
    }

    async fn process_connection(&self, data_store: Arc<Mutex<HashMap<String, DataStoreValue>>>, server_repl_config: Arc<Mutex<ServerReplicationConfig>>,) -> Result<(), String> {
        loop {
            let mut buffer = [0; BUFFER_SIZE];
            let stream = Arc::clone(&self.stream);
            let mut stream_guard = stream.lock().await;
            stream_guard
                .readable()
                .await
                .map_err(|e| format!("error code: {}", e))?;

            let read_count = stream_guard.read(&mut buffer).await.map_err(|e| format!("error code: {}", e))?;
            if read_count == 0 {
                break;
            }

            let command = parse_req(&buffer);
            let _done = match command {
                Some(command) => {
                    let mut server_repl_config = server_repl_config.lock().await;
                    let mut data_store = data_store.lock().await;
                    let context = process_command(command.clone(), &mut data_store, &mut server_repl_config).await;
                    let sent = match server_repl_config.send_to_client(&context, &mut stream_guard).await {
                        Ok(_) => Ok(()),
                        Err(e) => Err(format!("error code {}", e))
                    };
                    if context.add_repl_client {
                        println!("replication client {:?}", self.addr);
                        server_repl_config.repl_clients.insert(self.addr, self.stream.clone());
                        // moving stream....
                        return Ok(());
                    }
                    sent
                },
                None => Err("Can't parse command".to_string())
            };
        }
        Ok(())
    }

    pub async fn handle(
        &mut self, 
        data_store: Arc<Mutex<HashMap<String, DataStoreValue>>>,
        server_repl_config: Arc<Mutex<ServerReplicationConfig>>,
    ) -> io::Result<()> {
        match self.process_connection(data_store, server_repl_config).await {
            Ok(_) => Ok(()),
            Err(message) => {
                println!("handle error {}", message);
                let mut stream = self.stream.lock().await;
                stream
                    .write_all(simple_error_resp(&message).as_bytes())
                    .await
            }
        }
    }
}

async fn write_response(stream: &mut TcpStream, responses: Vec<Vec<u8>>) -> Result<(), String> {
    for response in responses {
        stream
            .writable()
            .await
            .map_err(|e| format!("error code: {}", e))?;
        stream
            .write_all(&response)
            .await
            .map_err(|e| format!("error code: {}", e))?;
        stream.flush().await.map_err(|e| format!("error code: {}", e))?;
    }
    Ok(())
}

async fn handle_repl_handshake(client: &mut TcpStream, bind_port: u16) -> Result<(), String> {
    let mut stage = 1;
    let mut client_buffer = [0u8; BUFFER_SIZE];

    write_response(client, vec![array_resp(vec!["PING"]).as_bytes().to_vec()]).await?;

    let bind_port = format!("{}", bind_port);
    let handshakes: HashMap<i32, Vec<&str>> = HashMap::from_iter([
        (2, vec!["REPLCONF", "listening-port", bind_port.as_str()]),
        (3, vec!["REPLCONF", "capa", "psync2"]),
        (4, vec!["PSYNC", "?", "-1"]),
    ]);
    loop {
        match client.read(&mut client_buffer).await {
            Ok(read_count) => {
                if read_count != 0 {
                    match stage {
                        1 => stage = 2,
                        2 => stage = 3,
                        3 => stage = 4,
                        _ => break,
                    };
                    write_response(client, vec![array_resp(handshakes.get(&stage).unwrap().clone()).as_bytes().to_vec()]).await?;
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
    Ok(())
}

async fn listener_loop(listener: TcpListener, mut replication_stream: Option<TcpStream>, data_store: Arc<Mutex<HashMap<String, DataStoreValue>>>, server_repl_config: Arc<Mutex<ServerReplicationConfig>>) {
    loop {
        // Replication
        if let Some(ref mut replication_stream) = replication_stream {
            let mut client_buffer = [0u8; BUFFER_SIZE];
            tokio::select! {
                stream = listener.accept() => {
                    let (stream, client) = stream.unwrap();
                    let stream = Arc::new(Mutex::new(stream));
                    let data_store = data_store.clone();
                    let server_repl_config = server_repl_config.clone();

                    // determine if it replication connection
                    let stream = stream.clone();
                    tokio::spawn(async move {
                        ConnectionManager::new(stream, client).handle(data_store, server_repl_config).await
                    });
                    
                },
                repl_buffer = replication_stream.read(&mut client_buffer) => {
                    // receive replication
                    match repl_buffer {
                        Ok(n) => {
                            if n != 0 {
                                let data_store = data_store.clone();
                                let server_repl_config = server_repl_config.clone();
                                process_repl_connection(&client_buffer, data_store, server_repl_config).await
                            }
                        },
                        Err(_error) => println!("Can't read response")
                    }
                }
            }
        } else {
            let (stream, client) = listener.accept().await.unwrap();
            println!("from client {:?}", client);
            let stream = Arc::new(Mutex::new(stream));
            let data_store = data_store.clone();
            let server_repl_config = server_repl_config.clone();

            let stream = stream.clone();
            tokio::spawn(async move {
                ConnectionManager::new(stream, client).handle(data_store, server_repl_config).await
            });
        }
    }
}

async fn process_repl_connection(client_buffer: &[u8; BUFFER_SIZE], data_store: Arc<Mutex<HashMap<String, DataStoreValue>>>, server_repl_config: Arc<Mutex<ServerReplicationConfig>>,) {
    let mut command = client_buffer
        .to_vec()
        .into_iter()
        .map(|r| r as char)
        .collect::<String>();
    if command.starts_with("+FULLRESYNC") {
        let lines: Vec<_> = command.match_indices("\r\n").collect();
        if lines.len() > 1 {
            let first_line = lines.first().unwrap().0;
            let drained: String = command.drain(0..first_line+2).collect();
            print!("PSYNC replied {}", drained.as_str());
            let len: Vec<_> = command.split("\r\n").into_iter().collect();
            println!(
                "Full resync with master, discarding {} bytes of bulk transfer...",
                len.first().unwrap().replace("$", "")
            );
            println!("Full resync done. Logging commands from master.");
        } else {
            print!("PSYNC replied {}", command);
        }
    } else if command.starts_with("$") && !command.ends_with("\r\n") {
        let len: Vec<_> = command.split("\r\n").into_iter().collect();
        println!(
            "Full resync with master, discarding {} bytes of bulk transfer...",
            len.first().unwrap().replace("$", "")
        );
        println!("Full resync done. Logging commands from master.");
    } else {
        let command = parse_req(client_buffer);
        if let Some(command) = command {
            let mut data_store = data_store.lock().await;
            let mut server_repl_config = server_repl_config.lock().await;
            process_command(command, &mut data_store, &mut server_repl_config).await;
        }
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
fn parse_req(&buffer: &[u8; BUFFER_SIZE]) -> Option<Command> {
    let command: Vec<String> = buffer
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

    let mut command_items: Vec<String> = Vec::with_capacity(command_length as usize);
    let mut offset = 1;
    while command_items.len() < command_length as usize {
        // skipping first
        let get_offset = offset as usize + 1;
        match command.get(get_offset) {
            Some(arg) => {
                if offset == 1 {
                    command_items.push(arg.clone().to_lowercase());
                } else {
                    command_items.push(arg.clone());
                }
            }
            _ => {
                break;
            }
        };
        offset = offset + 2;
    }

    let comm_parse: Vec<&str> = command_items.iter().map(|s| s.as_str()).collect();
    let str_to_string = |strs: Vec<&str>| {
        strs.into_iter().map(|s| s.to_string()).collect::<Vec<String>>()
    };
    // parse command
    let command: Option<Command> = match comm_parse.as_slice() {
        ["ping"] => Some(Command::Ping),
        ["echo", args @ ..] => Some(Command::Echo(str_to_string(args.to_vec()))),
        ["set", args @ ..] => Some(Command::Set(str_to_string(args.to_vec()))),
        ["get", args @ ..] => Some(Command::Get(str_to_string(args.to_vec()))),
        ["info", args @ ..] => Some(Command::Info(str_to_string(args.to_vec()))),
        ["replconf", args @ ..] => Some(Command::Replconf(str_to_string(args.to_vec()))),
        ["psync", args @ ..] => Some(Command::Psync(str_to_string(args.to_vec()))),
        _ => None,
    };

    command
}

// should be save only
async fn process_command(
    ref command: Command,
    data_store: &mut HashMap<String, DataStoreValue>,
    server_repl_config: &mut ServerReplicationConfig,
) -> RequestContext {
    let mut should_send_repl_init = false;
    let mut add_repl_client = false;
    let response = match command {
        Command::Ping => {
            simple_resp("PONG")
        }
        Command::Echo(com_args) => {
            match com_args.get(0) {
                Some(message) => {
                    simple_resp(message)
                }
                None => {
                    arg_error_resp("echo")
                }
            }
        }
        Command::Set(com_args) => {
            let com_args = com_args.iter().map(|s| s.as_str()).collect::<Vec<&str>>();
            let parsed_option = match com_args.as_slice() {
                [key, value, ex_type, duration] => {
                    let duration = duration.parse::<u64>().unwrap_or(0);
                    let expiry = match ex_type.to_lowercase().as_str() {
                        "ex" => Some(SetExpiry::Ex(duration)),
                        "px" => Some(SetExpiry::Px(duration)),
                        _ => None
                    };
                    Some((key, value, expiry))
                },
                [key, value] => {
                    Some((key, value, None))
                },
                _ => None
            };

            let (resp, repl_command) = match parsed_option {
                Some((key, value, Some(expiry))) => {
                    let (duration, repl_command) = match expiry {
                        SetExpiry::Ex(duration) => {
                            (Duration::from_secs(duration), array_resp(vec!["SET", key, value, "EX", format!("{}", duration).as_str()]))
                        },
                        SetExpiry::Px(duration) => {
                            (Duration::from_millis(duration), array_resp(vec!["SET", key, value, "PX", format!("{}", duration).as_str()]))
                        },
                    };
                    data_store.insert(
                        key.to_string(),
                        DataStoreValue {
                            value: value.to_string(),
                            created_at: Instant::now(),
                            expired_in: Some(duration),
                        },
                    );
                    (simple_resp("OK"), Some(repl_command))
                }
                Some((key, value, None)) => {
                    data_store.insert(
                        key.to_string(),
                        DataStoreValue {
                            value: value.to_string(),
                            created_at: Instant::now(),
                            expired_in: None,
                        },
                    );

                    let repl_command = array_resp(vec!["SET", &key, &value]);
                    (simple_resp("OK"), Some(repl_command))
                }
                _ => {
                    (arg_error_resp("set"), None)
                }
            };
            if let Some(repl) = repl_command {
                server_repl_config.send_to_replicas(repl).await.expect("not send");
            }
            resp
        }
        Command::Get(com_args) => {
            match com_args.get(0) {
                Some(key) => {
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
                Some(info_type) => {
                    if info_type.to_owned() == "replication" {
                        let repl_config = server_repl_config;
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
            match com_args.iter().map(|s| s.as_str()).collect::<Vec<&str>>().as_slice() {
                ["listening-port", port] => {
                    println!("REPLCONF {} {}", "listening-port", port);
                }
                ["capa", capa] => {
                    println!("REPLCONF {} {}", "capa", capa);
                }
                _ => (),
            };
            simple_resp("OK")
        }
        Command::Psync(com_args) => {
            let repl_config = server_repl_config;
            match com_args.iter().map(|s| s.as_str()).collect::<Vec<&str>>().as_slice() {
                ["?", "-1"] => {
                    should_send_repl_init = true;
                    add_repl_client = true;
                    simple_resp(
                        format!(
                            "FULLRESYNC {} {}",
                            repl_config.master_replid, repl_config.master_repl_offset
                        )
                        .as_str(),
                    )
                }
                [replid, _offset] => {
                    simple_resp(format!("FULLRESYNC {} {}", replid, 0).as_str())
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
    RequestContext {
        responses,
        add_repl_client,
    }
}
