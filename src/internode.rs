
use std;
use std::thread;
use std::net::TcpListener;
use std::io::prelude::*;

use msgpack;

struct InternodeService;

struct RoutingTable {
    guid: i32,
    ip_address: String,
    port: i32,
}

impl RoutingTable {
    pub fn get_host_n_port(&self) -> String {
        format!("{}:{}", &self.ip_address, self.port).clone()
    }
}

pub fn setup_internode_communicator(node_address: &String){
    // let arr = vec!["str1".to_string(), "str2".to_string()];
    // let str = msgpack::Encoder::to_msgpack(&arr).ok().unwrap();
    // println!("Encoded: {:?}", str);
    //
    // let dec: Vec<String> = msgpack::from_msgpack(&str).ok().unwrap();
    // println!("Decoded: {:?}", dec);

    let node_address = node_address.clone();
    thread::spawn(move || {
        let listener = TcpListener::bind(&*node_address).unwrap();
        println!("internode comm listening at {} ...", node_address);
        for stream in listener.incoming() {
            thread::spawn(move || {
                let mut stream = stream.unwrap();
                stream.write(b"INTERNODE: Hello World\r\n").unwrap();


                'the_loop: loop {
                    let mut buff = vec![0u8; 100];
                    match stream.read(&mut buff){
                        Ok(count) if count == 0 => break 'the_loop,
                        Ok(count) => {
                            let data = String::from_utf8(buff[0..count].to_vec()).unwrap();
                            println!("read {} bytes : {}", count, &data);
                            if data.len() == 0 {
                                break 'the_loop;
                            }

                            let s:Vec<&str> = data.split("|").collect();

                            println!("data splited: {:?}", s);

                        },
                        Err(e) => {
                            err!(10, "Cannot read data from other node. {}", e);
                            break 'the_loop;
                        }
                    };
                }

                println!("INTERNODE: conn closed.");

            });
        }
    });
}
