
use std::thread;
use std::net::TcpListener;
use std::io::prelude::*;

pub fn setup_internode_communicator(node_address: &String){
    let node_address = node_address.clone();
    thread::spawn(move || {
        let listener = TcpListener::bind(&*node_address).unwrap();
        println!("internode comm listening at {} ...", node_address);
        for stream in listener.incoming() {
            thread::spawn(move || {
                let mut stream = stream.unwrap();
                stream.write(b"INTERNODE: Hello World\r\n").unwrap();
            });
        }
    });
}


