
use std;
use std::thread;
use std::net::TcpListener;
use std::net::TcpStream;
use std::io::prelude::*;
//use std::error;
//use std::str;
use std::sync::{Arc, Mutex};


use encd;

use encd::MessageEncoderDecoder;

#[derive(Debug)]
struct RoutingTable {
    guid: u32,
    ip_address: String,
    port: u16,
}

impl RoutingTable {
    pub fn new(guid: u32, ip_address: String, port: u16) -> RoutingTable {
        RoutingTable {
            guid: guid,
            ip_address: ip_address,
            port: port
        }
    }

    pub fn get_host_n_port(&self) -> String {
        format!("{}:{}", &self.ip_address, self.port).clone()
    }
}

pub struct InternodeService {
    my_guid: u32,
    the_encd: encd::PlainTextEncoderDecoder,
    routing_tables: Vec<RoutingTable>
}

type ZResult = Result<i32, &'static str>;


impl InternodeService {

    pub fn new() -> InternodeService {
        InternodeService {
            my_guid: 0,
            the_encd: encd::PlainTextEncoderDecoder::new(),
            routing_tables: Vec::new()
        }
    }

    pub fn setup_internode_communicator(&mut self, node_address: &String){
        // let arr = vec!["str1".to_string(), "str2".to_string()];
        // let str = msgpack::Encoder::to_msgpack(&arr).ok().unwrap();
        // println!("Encoded: {:?}", str);
        //
        // let dec: Vec<String> = msgpack::from_msgpack(&str).ok().unwrap();
        // println!("Decoded: {:?}", dec);

        let node_address = node_address.clone();

        let xx:&'static mut InternodeService = unsafe{ std::mem::transmute(self) };
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

                                let mut z = z.lock().unwrap();

                                // let data = String::from_utf8(buff[0..count].to_vec()).unwrap();
                                let data = z.the_encd.decode(&buff[0..count]).ok().unwrap();

                                debug!("read {} bytes : {}", count, &data);
                                if data.len() == 0 {
                                    break 'the_loop;
                                }

                                let s:Vec<&str> = data.split("|").collect();

                                debug!("data splited: {:?}", s);

                                // check for version
                                let version = s[0];

                                if !version.starts_with("v"){
                                    warn!("invalid protocol version: {}", version);
                                    break 'the_loop;
                                }

                                debug!("connected node version: {}", version);

                                let cmd = s[1];

                                debug!("got cmd: {}", &cmd);

                                match z.process_cmd(cmd, &mut stream){
                                    Ok(i) if i != 0 => {
                                        break 'the_loop;
                                    },
                                    Ok(0) => (),
                                    Ok(_) => (),
                                    Err(e) => {
                                        error!("error: {}", e);
                                        break 'the_loop;
                                    }
                                }

                                // let data = z.the_encd.encode(&data).ok().unwrap();
                                // stream.write(&*data).unwrap();
                                // let _ = stream.write(b"\n");

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

    fn write(&self, stream: &mut TcpStream, data: &String){
        let _ = stream.write(&*self.the_encd.encode(data).ok().unwrap());
    }

    fn process_cmd(&mut self, cmd: &str, stream: &mut TcpStream) -> ZResult {
        match cmd {
            "join" => {
                trace!("join cmd recvd.");

                match self.get_rt(stream){
                    Some(rt) => {
                        warn!("ip {} already joined as guid {}", rt.ip_address, rt.guid);

                        // just send existing guid
                        let data = &format!("v1|guid|{}\n", rt.guid);
                        //stream.write(&*self.the_encd.encode(data).ok().unwrap());
                        self.write(stream, data);

                        return Ok(0);
                    },
                    None => ()
                }

                let guid = self.generate_guid();

                debug!("generated guid: {}", guid);

                let data = &format!("v1|guid|{}\n", guid);

                let (ip_addr, port) = match stream.local_addr() {
                    Ok(sock_addr) => {
                        (format!("{}",sock_addr.ip()), sock_addr.port())
                    },
                    Err(e) => {
                        error!("{}", e);
                        return Err("cannot get peer address");
                    }
                };

                debug!("peer addr: {}:{}", ip_addr, port);

                self.routing_tables.push(RoutingTable::new(guid, ip_addr, port));

                //stream.write(&*self.the_encd.encode(data).ok().unwrap());

                self.write(stream, data);

                Ok(0)
            },
            "leave" => {
                trace!("leave cmd recvd.");

                let guid = self.get_guid_from_stream(stream);
                trace!("guid: {}", guid);

                if guid > 0 {
                    let idx = self.node_index(guid) as usize;
                    let x = self.routing_tables.swap_remove(idx);
                    debug!("removing node {:?} from routing tables", x);
                }

                Ok(1)
            },
            "copy-rt" => { // copy routing tables

                if self.get_guid_from_stream(stream) > 0 {
                    trace!("copy routing tables...");
                    println!("routing tables: {:?}", self.routing_tables);
                    Ok(0)
                }else{
                    Err("not registered as node")
                }
            },
            x => {
                debug!("unknwon cmd: {}", x);
                Err("unknown cmd")
            }
        }
    }

    fn get_guid_from_stream(&self, stream: &TcpStream) -> u32 {
        match self.get_rt(stream) {
            Some(rt) => rt.guid,
            None => 0,
        }
    }

    fn get_routing_tables<'a>(&'a mut self) -> &'a mut Vec<RoutingTable> {
        &mut self.routing_tables
    }

    fn get_rt<'a>(&'a self, stream: &TcpStream) -> Option<&'a RoutingTable> {
        let (ip_addr, port) = match stream.local_addr() {
            Ok(sock_addr) => {
                (format!("{}",sock_addr.ip()), sock_addr.port())
            },
            Err(e) => {
                error!("{}", e);
                panic!("cannot get peer address");
            }
        };
        (&self.routing_tables).iter().find(|&r| r.ip_address == ip_addr && r.port == port)
    }

    fn node_index(&self, id:u32) -> i32 {
        match self.routing_tables.binary_search_by(|p| p.guid.cmp(&id)){
            Ok(i) => i as i32,
            Err(_) => -1
        }
    }

    fn is_node_registered(&self, id:u32) -> bool {
        self.node_index(id) > 0
    }

    fn generate_guid(&self) -> u32 {
        let mut gid = self.my_guid + 1;
        //let mut done = false;
        while self.is_node_registered(gid) {
            gid = gid + 1;
        }
        gid
    }

}
