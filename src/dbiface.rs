use std::net::{TcpStream, SocketAddr};
use std::io::prelude::*;
use db::Db;

pub struct DbIface {
    db: Db
}

impl DbIface {
    pub fn new() -> DbIface {
        DbIface {
            db: Db::new()
        }
    }
    
    pub fn handle_packet(&mut self, stream: &mut TcpStream, data: &[u8]) -> Result<u16, &'static str> {

        let d = String::from_utf8(data.to_vec()).ok().unwrap();
        let s:Vec<&str> = d.trim().split(" ").collect();

        debug!("splited s: {:?}", s);

        if s.len() == 1 && s[0] == "" {
            return Ok(0);
        }

        match &s[0] {
            &"set" => {

                if s.len() != 5 {
                    return Err("`set` parameters must be 5");
                }

                let k = s[1];
                let metadata = s[2];
                let expiration:u32 = s[3].parse().unwrap();
                let length:usize = s[4].parse().unwrap();

                let _ = stream.write(b"read for data >\n");

                let mut buff = vec![0u8; length];
                match stream.read(&mut buff){
                    Ok(count) if count > 0 => {
                        let data_str = String::from_utf8(buff[0..count].to_vec()).unwrap();
                        let data = format!("{}:{}:{}|{}", length, metadata, expiration, data_str);
                        debug!("data to store: k: {}, v: {:?}", k, data);
                        self.db.insert(k.as_bytes(), data.as_bytes());
                        let _ = stream.write(b"STORED\n");
                    },
                    _ => ()
                }

                Ok(0)
            },
            &"get" => {
                let k = s[1];

                match self.db.get(k.as_bytes()){
                    Some(v) => {
                        
                        let s = String::from_utf8(v.to_vec()).unwrap();
                        let s:Vec<&str> = s.split("|").collect();
                        let meta_s:Vec<&str> = s[0].split(":").collect();
                        let length = meta_s[0];
                        let metadata = meta_s[1];
                        //let expiration = meta_s[2];
                        let content = s[1];
                        
                        let data = format!("VALUE {} {} {}\n{}\nEND\n", k, metadata, length, content);
                        let _ = stream.write(data.as_bytes());
                    },
                    _ => {
                        let _ = stream.write(b"END\n");
                    }
                }

                Ok(0)
            },
            _ => Ok(1)
        }
    }
}


