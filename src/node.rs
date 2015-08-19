//use std;
//use std::thread;
//use std::net::TcpListener;
use std::net::{TcpStream, SocketAddr};
use std::io::prelude::*;
use std::str;

#[derive(Debug)]
pub struct Node {
    guid: u32,
    node_address: String,
    api_address: String
}

pub struct NodeClient;

impl NodeClient {
    pub fn new(node_address:&String) -> Node {
        Node {
            guid: 0,
            node_address: node_address.clone(),
            api_address: "".to_string()
        }
    }
}

pub struct NodeInfo {
    load: usize,
    disk_load: usize
}

impl NodeInfo {
    pub fn load(&self) -> usize {
        self.load
    }

    pub fn disk_load(&self) -> usize {
        self.disk_load
    }
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

        let _:Option<String> = self.dispatch(&mut |ref mut _self, stream| {

            _self.send_cmd_and_handle(stream, &*format!("v1|add-me|{}|{}|{}",
                node.guid, node.node_address, node.api_address));


            trace!("add to rts done.");

            None
        });

    }

    pub fn dispatch<F, R>(&mut self, _op: &mut F) -> Option<R>
                where F: FnMut(&mut Node, &mut TcpStream) -> Option<R> {

        let addr:SocketAddr = self.node_address.parse()
            .unwrap_or_else(|_| panic!("cannot parse address: {}", self.node_address));

        match TcpStream::connect(addr){
            Ok(ref mut stream) => {

                _op(self, stream)

            },
            Err(e) => {
                //panic!("cannot connect to {:?}, {}", &self, e);
                error!("cannot connect to remote node: {}", e);
                None
            }
        }
    }

    pub fn info(&mut self) -> Option<NodeInfo> {
        self.dispatch(&mut |_self, stream| {
            match _self.send_cmd_and_handle(stream, "v1|info|0"){
                Some(s) => {
                    let s:Vec<String> = s[2..].to_vec();

                    let ni = NodeInfo {
                        load: s[0].parse().unwrap(),
                        disk_load: s[1].parse().unwrap()
                    };

                    Some(ni)

                },
                _ => None
            }
        })
    }

    fn send_cmd_and_handle(&mut self, stream:&mut TcpStream, data:&str) -> Option<Vec<String>>{
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

        let s:Vec<String> = resp.trim().split("|").map(|s| s.to_string()).collect();

        if s.len() < 2 {
            error!("invalid packet < 2");
            return None;
        }

        Some(s)

        // match &s[1]{
        //     &"info" => {
        //
        //
        //     },
        //     _ => Ok("".to_string())
        // }

    }
}
