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
            Some(Arc::new(Mutex::new(client)))
        } else {
            None
        }
    } else {
        None
    };

    println!("MAIN: Server running ...");

    let server_repl_config: Arc<Mutex<ServerReplicationConfig>> =
        Arc::new(Mutex::new(server_repl_config)); 

    if let Some(replica_stream) = replication_stream {
        replica_listener_loop(listener, replica_stream, data_store, server_repl_config).await;
    } else {
        master_listener_loop(listener, data_store, server_repl_config).await
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

fn integer_resp(num: i32) -> String {
    format!(":{}\r\n", num)
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

#[allow(dead_code)]
fn empty_rdb_and_propagate() -> Vec<u8> {
    let empty = empty_rdb_resp();
    let propagate = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n123\r\n*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$3\r\n456\r\n*3\r\n$3\r\nSET\r\n$3\r\nbaz\r\n$3\r\n789\r\n".as_bytes().to_vec();
    [empty, propagate].concat()
}

#[allow(dead_code)]
fn empty_rdb_and_getack() -> Vec<u8> {
    let empty = empty_rdb_resp();
    let getack = "*3\r\n$8\r\nreplconf\r\n$6\r\ngetack\r\n$1\r\n*\r\n".as_bytes().to_vec();
    [empty, getack].concat()
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
    Wait(Vec<String>),
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
    master_repl_offset: usize,
    master_host: Option<String>,
    master_port: Option<u16>,
    repl_clients: HashMap<SocketAddr, Arc<Mutex<TcpStream>>>,
    repl_init_done: bool,
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
    should_send_repl_reply: bool,
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

async fn master_listener_loop(listener: TcpListener, data_store: Arc<Mutex<HashMap<String, DataStoreValue>>>, server_repl_config: Arc<Mutex<ServerReplicationConfig>>) {
    loop {
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

async fn replica_listener_loop(listener: TcpListener, replication_stream: Arc<Mutex<TcpStream>>, data_store: Arc<Mutex<HashMap<String, DataStoreValue>>>, server_repl_config: Arc<Mutex<ServerReplicationConfig>>) {
    loop {
        // Replication
        let mut client_buffer = [0u8; BUFFER_SIZE];
        let mut replication_stream = replication_stream.lock().await;
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
                            let mut server_repl_config = server_repl_config.lock().await;
                            process_repl_connection(&mut replication_stream, &client_buffer, data_store, &mut server_repl_config).await
                        }
                    },
                    Err(_error) => println!("Can't read response")
                }
            }
        }
        // if let Some(ref mut replication_stream) = replication_stream 
    }
}

async fn process_repl_connection(replication_stream: &mut TcpStream, client_buffer: &[u8; BUFFER_SIZE], data_store: Arc<Mutex<HashMap<String, DataStoreValue>>>, server_repl_config: &mut ServerReplicationConfig) {
    let mut buffer_iter = client_buffer.into_iter();
    let mut parsed_buffer: Vec<Vec<u8>> = Vec::new();
    let mut remaining_buffer: Vec<Vec<String>> = Vec::new();
    let mut rdb_data: Vec<u8> = Vec::new();
    let mut bytes_processed = server_repl_config.master_repl_offset;
    let mut repl_init_done = server_repl_config.repl_init_done;

    while let Some(buf) = buffer_iter.next() {
        if *buf == '$' as u8 && rdb_data.len() == 0 {
            let mut raw_len = String::new();
            'rdb: while let Some(len_) = buffer_iter.next() {
                if *len_ == '\r' as u8 {
                    let next_next = buffer_iter.next().unwrap();
                    if *next_next == '\n' as u8 {
                        break 'rdb;
                    } else {
                        break 'rdb;
                    }
                }
                raw_len = format!("{}{}", raw_len, *len_ as char);
            }
            let len = raw_len.parse::<usize>().unwrap();
            let mut offset: usize = 0;
            while offset < len {
                if let Some(rdb) = buffer_iter.next() {
                    rdb_data.push(*rdb);
                }
                offset = offset + 1;
            }
            parsed_buffer.push(rdb_data.clone());
        } else if *buf == '+' as u8 {
            // count bytes here
            // bytes_processed = bytes_processed + 1;
            let mut simple_str: Vec<u8> = Vec::new();
            simple_str.push(*buf);
            'simplestr: while let Some(ss) = buffer_iter.next() {
                // bytes_processed = bytes_processed + 1;
                simple_str.push(*ss);
                let next = buffer_iter.next().unwrap();
                // bytes_processed = bytes_processed + 1;
                simple_str.push(*next);
                if *next == '\r' as u8 {
                    let next_next = buffer_iter.next().unwrap();
                    // bytes_processed = bytes_processed + 1;
                    simple_str.push(*next_next);
                    if *next_next == '\n' as u8 {
                        break 'simplestr;
                    } else {
                        break 'simplestr;
                    }
                }
            }
            parsed_buffer.push(simple_str);
        } else if *buf == '*' as u8 {
            // count bytes here
            // bytes_processed = bytes_processed + 1;
            let mut command_buffer: Vec<u8> = Vec::new();
            let mut raw_len = String::new();
            command_buffer.push(*buf);
            'commandlen: while let Some(len_) = buffer_iter.next() {
                // bytes_processed = bytes_processed + 1;
                command_buffer.push(*len_);
                if *len_ == '\r' as u8 {
                    let next_next = buffer_iter.next().unwrap();
                    // bytes_processed = bytes_processed + 1;
                    command_buffer.push(*next_next);
                    if *next_next == '\n' as u8 {
                        break 'commandlen;
                    } else {
                        break 'commandlen;
                    }
                }
                raw_len = format!("{}{}", raw_len, *len_ as char);
            }

            let mut commands: Vec<String> = Vec::new();
            let com_len = raw_len.parse::<usize>().unwrap_or(0);
            let mut offset = 0;
            'comlen: while offset < com_len {
                if let Some(c) = buffer_iter.next() {
                    // bytes_processed = bytes_processed + 1;
                    command_buffer.push(*c);
                    if *c == '\r' as u8 {
                        let c_next = buffer_iter.next().unwrap();
                        command_buffer.push(*c_next);
                        // bytes_processed = bytes_processed + 1;
                        if *c_next == '\n' as u8 {
                            continue 'comlen;
                        } else {
                            continue 'comlen;
                        }
                    }
                    if *c == '$' as u8 {
                        let mut raw_txtlen = String::new();
                        'txtlen: while let Some(len_) = buffer_iter.next() {
                            // bytes_processed = bytes_processed + 1;
                            command_buffer.push(*len_);
                            if *len_ == '\r' as u8 {
                                let next_next = buffer_iter.next().unwrap();
                                // bytes_processed = bytes_processed + 1;
                                command_buffer.push(*next_next);
                                if *next_next == '\n' as u8 {
                                    break 'txtlen;
                                } else {
                                    break 'txtlen;
                                }
                            }
                            raw_txtlen = format!("{}{}", raw_txtlen, *len_ as char);
                        }
                        let txtlen = raw_txtlen.parse::<usize>().unwrap_or(0);
                        let mut parsed_txt = String::new();
                        'txt: while let Some(t) = buffer_iter.next() {
                            // bytes_processed = bytes_processed + 1;
                            command_buffer.push(*t);
                            if *t == '\r' as u8 {
                                let t_next = buffer_iter.next().unwrap();
                                // bytes_processed = bytes_processed + 1;
                                command_buffer.push(*t_next);
                                break 'txt;
                            }
                            if parsed_txt.len() < txtlen {
                                parsed_txt = format!("{}{}", parsed_txt, *t as char);
                            }
                        }
                        commands.push(parsed_txt.clone());
                        offset = offset + 1;
                    }
                }
            }
            parsed_buffer.push(command_buffer);
            remaining_buffer.push(commands);
        }
    }

    let parsed_debug = parsed_buffer.iter().map(|a| {a.iter().map(|f| *f as char).collect::<String>()}).collect::<Vec<String>>();

    println!("parsed_debug {:?}", parsed_debug);
    let mut savelater = false;

    for (i, debg) in parsed_debug.into_iter().enumerate() {
        if debg.starts_with("+FULLRESYNC") {
            println!("PSYNC replied {}", debg);
        } else if !repl_init_done {
            repl_init_done = true;
            println!(
                "Full resync with master, discarding {} bytes of bulk transfer...",
                rdb_data.len()
            );
            println!("Full resync done. Logging commands from master.");
        } else {
            bytes_processed = bytes_processed + parsed_buffer.get(i).unwrap().len();
            if debg.to_lowercase().contains("replconf") && debg.to_ascii_lowercase().contains("getack") {
                savelater = true;
            } else {
                server_repl_config.repl_init_done = repl_init_done;
                server_repl_config.master_repl_offset = bytes_processed;
            }
        }
    }
    println!("bytes_processed {}", bytes_processed);

    for cmd in remaining_buffer {
        let command = parse_command(cmd);
        if let Some(command) = command {
            let mut data_store = data_store.lock().await;
            let context = process_command(command, &mut data_store, server_repl_config).await;
            if context.should_send_repl_reply {
                println!("should_send_repl_reply");
                write_response(replication_stream, context.responses).await.unwrap();
            }
        }
    }
    if savelater {
        server_repl_config.repl_init_done = repl_init_done;
        server_repl_config.master_repl_offset = bytes_processed;
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


fn parse_command(mut command_items: Vec<String>) -> Option<Command> {
    if let Some(first) = command_items.first_mut() {
        *first = first.to_ascii_lowercase();
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
        ["wait", args @ ..] => Some(Command::Wait(str_to_string(args.to_vec()))),
        _ => None,
    };
    command
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

    parse_command(command_items)
}

// should be save only
async fn process_command(
    ref mut command: Command,
    data_store: &mut HashMap<String, DataStoreValue>,
    server_repl_config: &mut ServerReplicationConfig,
) -> RequestContext {
    let mut should_send_repl_init = false;
    let mut should_send_repl_reply = false;
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
        Command::Replconf(ref mut com_args) => {
            if let Some(first) = com_args.first_mut() {
                *first = first.to_ascii_lowercase();
            }
            let mut resp = simple_resp("OK");
            match com_args.iter().map(|s| s.as_str()).collect::<Vec<&str>>().as_slice() {
                ["listening-port", port] => {
                    println!("REPLCONF {} {}", "listening-port", port);
                }
                ["capa", capa] => {
                    println!("REPLCONF {} {}", "capa", capa);
                },
                ["getack", _ack] => {
                    should_send_repl_reply = true;
                    resp = array_resp(vec!["REPLCONF", "ACK", format!("{}", server_repl_config.master_repl_offset).as_str()]);
                },
                _ => (),
            };
            resp
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
        },
        Command::Wait(_com_args) => {
            integer_resp(0)
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
        should_send_repl_reply,
    }
}
