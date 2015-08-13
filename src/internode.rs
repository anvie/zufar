
use std;
use std::thread;
use std::net::TcpListener;
use std::io::prelude::*;
use std::error;
use std::str;
use std::sync::{Arc, Mutex};


use encd;

use encd::MessageEncoderDecoder;

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

pub struct InternodeService {
    my_guid: i32,
    the_encd: encd::PlainTextEncoderDecoder,
    routing_tables: Vec<RoutingTable>
}

impl InternodeService {

    pub fn new() -> InternodeService {
        InternodeService {
            my_guid: 0,
            the_encd: encd::PlainTextEncoderDecoder::new(),
            routing_tables: Vec::new()
        }
    }

    pub fn setup_internode_communicator(&self, node_address: &String){
        // let arr = vec!["str1".to_string(), "str2".to_string()];
        // let str = msgpack::Encoder::to_msgpack(&arr).ok().unwrap();
        // println!("Encoded: {:?}", str);
        //
        // let dec: Vec<String> = msgpack::from_msgpack(&str).ok().unwrap();
        // println!("Decoded: {:?}", dec);

        let node_address = node_address.clone();

        let xx:&'static InternodeService = unsafe{ std::mem::transmute(self) };
        let x = Arc::new(Mutex::new(xx));


        thread::spawn(move || {
            let listener = TcpListener::bind(&*node_address).unwrap();
            println!("internode comm listening at {} ...", node_address);
            for stream in listener.incoming() {
                let z = x.clone();
                thread::spawn(move || {

                    let mut stream = stream.unwrap();

                    {
                        let z = z.lock().unwrap();

                        //let encd = encd::PlainTextEncoderDecoder::new();
                        let data = z.the_encd.encode(&"INTERNODE: Hello World\r\n".to_string()).ok().unwrap();
                        stream.write(&*data).unwrap();
                    }


                    'the_loop: loop {
                        let mut buff = vec![0u8; 100];
                        match stream.read(&mut buff){
                            Ok(count) if count == 0 => break 'the_loop,
                            Ok(count) => {

                                let z = z.lock().unwrap();

                                // let data = String::from_utf8(buff[0..count].to_vec()).unwrap();
                                let data = z.the_encd.decode(&buff[0..count]).ok().unwrap();

                                println!("read {} bytes : {}", count, &data);
                                if data.len() == 0 {
                                    break 'the_loop;
                                }

                                let s:Vec<&str> = data.split("|").collect();

                                println!("data splited: {:?}", s);

                                let data = z.the_encd.encode(&data).ok().unwrap();
                                stream.write(&*data).unwrap();
                                let _ = stream.write(b"\n");

                            },
                            Err(e) => {
                                err!(10, "Cannot read data from other node. {}", e);
                                //break 'the_loop;
                            }
                        };
                    }

                    println!("INTERNODE: conn closed.");

                });
            }
        });
    }

}
