use std::collections::HashMap;
use std::io;
use std::path::Path;
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};

const AUX_FIELD: u8 = 0xFA;
const EOF: u8 = 0xFF;

const MAGIC: [u8; 5] = [0x52, 0x45, 0x44, 0x49, 0x53];

#[derive(Debug,PartialEq)]
pub enum Type {
    String,
    List,
    Set,
    SortedSet,
    Hash
}

const E_STRING : u8 = 0;
const E_LIST : u8 = 1;
const E_SET : u8 = 2;
const E_ZSET : u8 = 3;
const E_HASH : u8 = 4;
const E_HASH_ZIPMAP : u8 = 9;
const E_LIST_ZIPLIST : u8 = 10;
const E_SET_INTSET : u8 = 11;
const E_ZSET_ZIPLIST : u8 = 12;
const E_HASH_ZIPLIST : u8 = 13;
const E_LIST_QUICKLIST : u8 = 14;

impl Type {
    pub fn from_encoding(enc_type: u8) -> Type {
        match enc_type {
            E_STRING => Type::String,
            E_HASH | E_HASH_ZIPMAP | E_HASH_ZIPLIST => Type::Hash,
            E_LIST | E_LIST_ZIPLIST => Type::List,
            E_SET | E_SET_INTSET => Type::Set,
            E_ZSET | E_ZSET_ZIPLIST => Type::SortedSet,
            _ => { panic!("Unknown encoding type: {}", enc_type) }
        }
    }
}

fn byte_to_bits(byte: &u8) -> Vec<i32> {
    let mut bits = [0; 8];
    let mut remaining = 8;
    for i in 0..8 {
        remaining -= 1;
        let mask = 1 << i;
        let one = (mask & byte) > 0;
        bits[remaining] = one as i32;
    }
    bits.to_vec()
}

pub async fn read_length_with_encoding<R: AsyncRead + std::marker::Unpin>(input: &mut R) -> io::Result<(u32, bool)> {
    let length;
    let mut is_encoded = false;

    let enc_type = input.read_u8().await.unwrap();

    match (enc_type & 0xC0) >> 6 {
        3 => {
            is_encoded = true;
            length = (enc_type & 0x3F) as u32;
        },
        0 => {
            length = (enc_type & 0x3F) as u32;
        },
        1 => {
            let next_byte = input.read_u8().await.unwrap();
            length = (((enc_type & 0x3F) as u32) <<8) | next_byte as u32;
        },
        _ => {
            let mut big_endian = [0u8; 4];
            let read_count = input.read(&mut big_endian).await.unwrap();
            println!("read_count {}", read_count);
            length = u32::from_be_bytes(big_endian);
        }
    }

    Ok((length, is_encoded))
}

async fn read_length<R: AsyncRead + std::marker::Unpin>(input: &mut R) -> io::Result<u32> {
    let (length, _) = read_length_with_encoding(input).await.unwrap();
    Ok(length)
}

pub fn int_to_vec(number: i32) -> Vec<u8> {
    let number = number.to_string();
    let mut result = Vec::with_capacity(number.len());
    for &c in number.as_bytes().iter() {
      result.push(c);
    }
    result
}

pub async fn read_blob<R: AsyncRead + std::marker::Unpin>(input: &mut R) -> io::Result<Vec<u8>> {
    let (length, is_encoded) = read_length_with_encoding(input).await.unwrap();

    if is_encoded {
        let result = match length {
            0 => { int_to_vec(input.read_i8().await.unwrap() as i32) },
            1 => { 
                let mut le_bytes = [0u8; 2];
                let _ = input.read(&mut le_bytes).await.unwrap();
                let le_val: i16 = i16::from_le_bytes(le_bytes);
                int_to_vec(le_val as i32) 
            },
            2 => {
                let mut le_bytes = [0u8; 4];
                let _ = input.read(&mut le_bytes).await.unwrap();
                let le_val: i32 = i32::from_le_bytes(le_bytes);
                int_to_vec(le_val)
            },
            _ => { panic!("Unknown encoding: {}", length) }
        };
        Ok(result)
    } else {
        let mut rex = vec![0u8; length as usize]; 
        let _ = input.read(&mut rex).await.unwrap();
        Ok(rex)
    }
}

fn is_empty_types(types: &Vec<Type>) -> bool {
    types.is_empty()
}

fn match_type(types: &mut Vec<Type>, enc_type: u8) -> bool {
    if is_empty_types(types) {
        return true;
    }

    let typ = Type::from_encoding(enc_type);
    types.iter().any(|x| *x == typ)
}

fn match_key(keys: Option<&str>, key: &[u8]) -> bool {
    match keys.clone() {
        None => true,
        Some(re) => {
            let key = unsafe {
                String::from_utf8_unchecked(key.to_vec())
            };
            println!("Key => {} {}", key, re);
            re.eq(key.as_str())
        }
    }
}

async fn read_type<R: AsyncRead + std::marker::Unpin>(key: &[u8], value_type: u8, reader: &mut R, last_expiretime: &Option<u64>, datastore: &mut HashMap<String, String>) -> io::Result<()> {
    match value_type {
        E_STRING => {
            let val = read_blob(reader).await.unwrap();
            let parsed_key = key.iter().map(|s| *s as char).collect::<String>();
            let parsed_val = val.iter().map(|s| *s as char).collect::<String>();
            datastore.insert(parsed_key.clone(), parsed_val.clone());
            println!("key => {:?} ", parsed_key);
            println!("val => {:?} ", parsed_val);
            // println!("exp => {:?} ", last_expiretime);
        },
        // E_LIST => {
        //     try!(self.read_linked_list(key, Type::List))
        // },
        // E_SET => {
        //     try!(self.read_linked_list(key, Type::Set))
        // },
        // E_ZSET => {
        //     try!(self.read_sorted_set(key))
        // },
        // E_HASH => {
        //     try!(self.read_hash(key))
        // },
        // E_HASH_ZIPMAP => {
        //     try!(self.read_hash_zipmap(key))
        // },
        // E_LIST_ZIPLIST => {
        //     try!(self.read_list_ziplist(key))
        // },
        // E_SET_INTSET => {
        //     try!(self.read_set_intset(key))
        // },
        // E_ZSET_ZIPLIST => {
        //     try!(self.read_sortedset_ziplist(key))
        // },
        // E_HASH_ZIPLIST => {
        //     try!(self.read_hash_ziplist(key))
        // },
        // E_LIST_QUICKLIST => {
        //     try!(self.read_quicklist(key))
        // },
        _ => { panic!("Value Type not implemented: {}", value_type) }
    };

    Ok(())
}

async fn skip<R: AsyncRead + std::marker::Unpin>(reader: &mut R, skip_bytes: usize) -> Result<(), String> {
    let mut buf = vec![0; skip_bytes];
    reader.read_exact(&mut buf).await.unwrap();
    Ok(())
}

async fn skip_blob<R: AsyncRead + std::marker::Unpin>(mut reader: &mut R) -> Result<(), String> {
    let (len, is_encoded) = read_length_with_encoding(&mut reader).await.unwrap();
    let skip_bytes;

    if is_encoded {
        skip_bytes = match len {
            0 => 1,
            1 => 2,
            2 => 4,
            3 => {
                let compressed_length = read_length(&mut reader).await.unwrap();
                let _real_length = read_length(&mut reader).await.unwrap();
                compressed_length
            },
            _ => { panic!("Unknown encoding: {}", len) }
        }
    } else {
        skip_bytes = len;
    }

    skip(&mut reader, skip_bytes as usize).await.unwrap();
    Ok(())
}

async fn skip_object<R: AsyncRead + std::marker::Unpin>(mut reader: &mut R, enc_type: u8) -> Result<(), String> {
    let blobs_to_skip = match enc_type {
        E_STRING |
            E_HASH_ZIPMAP |
            E_LIST_ZIPLIST |
            E_SET_INTSET |
            E_ZSET_ZIPLIST |
            E_HASH_ZIPLIST => 1,
        E_LIST |
            E_SET |
            E_LIST_QUICKLIST => read_length(&mut reader).await.unwrap(),
        E_ZSET | E_HASH => read_length(&mut reader).await.unwrap() * 2,
        _ => { panic!("Unknown encoding type: {}", enc_type) }
    };

    for _ in 0..blobs_to_skip {
        skip_blob(&mut reader).await.unwrap();
    }
    Ok(())
}

async fn skip_key_and_object<R: AsyncRead + std::marker::Unpin>(mut reader: &mut R, enc_type: u8) -> Result<(), String> {
    skip_blob(&mut reader).await.unwrap();
    skip_object(&mut reader, enc_type).await.unwrap();
    Ok(())
}

pub async fn rdb_parser<R: AsyncRead + std::marker::Unpin>(mut reader: &mut R, mut datastore: &mut HashMap<String, String>, key: Option<&str>) -> Result<(), String> {
    let mut magic = [0u8; 5];
    reader.read(&mut magic).await.expect("Can't parse RDB");
    if MAGIC != magic {
        return Err("Invalid RDB".to_string());
    }
    let mut rdb_version = [0u8; 4];
    reader.read(&mut rdb_version).await.expect("Can't parse RDB");

    let mut aux_buffer: Vec<(Vec<u8>, Vec<u8>)> = vec![];
    let mut last_db = 0;
    let mut last_expiretime: Option<u64> = None;

    let mut types: Vec<Type> = vec![];
    let keys: Option<&str> = key;

    loop {
        let next_op = reader.read_u8().await.unwrap();

        match next_op {
            EOF => {
                break;
            },
            AUX_FIELD => {
                let key = read_blob(&mut reader).await.unwrap();
                let value = read_blob(&mut reader).await.unwrap();
                print!("{:?} => ", key.iter().map(|s| *s as char).collect::<String>());
                println!("{:?}", value.iter().map(|s| *s as char).collect::<String>());
                aux_buffer.push((key, value));
            },
            0xFE => {
                last_db = read_length(&mut reader).await.unwrap();
                println!("Database: {}", last_db);
            }
            0xFD => {
                let mut be_bytes = [0u8; 4];
                reader.read_exact(&mut be_bytes).await.unwrap();
                let exp_sec = u32::from_be_bytes(be_bytes);
                last_expiretime = Some(exp_sec as u64 * 1000);
                println!("Expire time sec: {exp_sec}");
            }
            0xFC => {
                let mut le_bytes = [0u8; 8];
                reader.read_exact(&mut le_bytes).await.unwrap();
                let exp_ms = u64::from_le_bytes(le_bytes);
                last_expiretime = Some(exp_ms);
                println!("Expire time ms: {exp_ms}");
            }
            0xFB => {
                let db_size = read_length(&mut reader).await.unwrap();
                let expires_size = read_length(&mut reader).await.unwrap();

                println!("Resizedb: {db_size} {expires_size}");
            }
            _unh => {
                let key = read_blob(&mut reader).await.unwrap();

                if match_type(&mut types, next_op) && match_key(keys, &key) {
                    read_type(&key, next_op, &mut reader, &last_expiretime, &mut datastore).await.unwrap();
                } else {
                    skip_object(&mut reader, next_op).await.unwrap();
                }
                last_expiretime = None;
            }
        }
    }
    // println!(" {:02X?}", aux_buffer);
    Ok(())
}

// pub async fn parse_buffer(input: &[u8]) -> Result<(), String> {
//     let mut reader = BufReader::new(input);
//     rdb_parser(&mut reader).await
// }

pub async fn parse_rdb_file(path: String, mut datastore: &mut HashMap<String, String>, key: Option<&str>) -> Result<(), String> {
    println!("parsing rdb file {}", path);
    let file = File::open(&Path::new(&*path)).await.unwrap();
    let mut reader = BufReader::new(file);
    
    rdb_parser(&mut reader, &mut datastore, key).await
}