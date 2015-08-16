use std;
use std::thread;
use std::net::TcpListener;
use std::net::{TcpStream, SocketAddr};
use std::io::prelude::*;
use std::str;

#[derive(Debug)]
pub struct Node {
    guid: u32,
    node_address: String,
    api_address: String
}

impl Node {
    
    pub fn new(guid:u32, node_address:&String, api_address:&String) -> Node {
        Node {
            guid: guid,
            node_address: node_address.clone(),
            api_address: api_address.clone()
        }
    }
    
    pub fn add_to_rts(&mut self, node:Node){
        debug!("adding {:?} to {:?} rts", &node, self);
        
        let addr:SocketAddr = self.node_address.parse().unwrap();
        match TcpStream::connect(addr){
            Ok(ref mut stream) => {

                
                self.send_cmd_and_handle(stream, &*format!("v1|add-me|{}|{}|{}", 
                    node.guid, node.node_address, node.api_address));

                trace!("add to rts done.");
            },
            Err(e) => {
                error!("cannot connect to {:?}, {}", &self, e);
            }
        }
    }
    
    fn send_cmd_and_handle(&mut self, stream:&mut TcpStream, data:&str){
        let _ = stream.write(data.as_bytes());
        let _ = stream.flush();
        let mut buff = vec![0u8; 100];
        let resp:&str = match stream.read(&mut buff){
            Ok(count) => {
                str::from_utf8(&buff[0..count]).unwrap()
            },
            Err(_) => ""
        };
        
        debug!("buff: {}", resp);
        
        let s:Vec<&str> = resp.trim().split("|").collect();
        
        if s.len() < 2 {
            return;
        }
        
    }
}
