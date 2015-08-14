
use std;
use std::thread;
use std::net::TcpListener;
use std::net::{TcpStream, SocketAddr};
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
//    socket_address: String
}

impl RoutingTable {
    pub fn new(guid: u32, ip_address: String, port: u16) -> RoutingTable {
        RoutingTable {
            guid: guid,
            ip_address: ip_address,
            port: port,
//            socket_address: socket_address
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

    pub fn setup_internode_communicator(&mut self, node_address: &String, seeds:Vec<String>){
        
        let node_address = node_address.clone();
        
        let static_self:&'static mut InternodeService = unsafe{ std::mem::transmute(self) };
        let arc_self = Arc::new(Mutex::new(static_self));
        
        InternodeService::setup_network_keeper(&arc_self);
        
        let my_node_address = node_address.clone();

        thread::spawn(move || {
            let listener = TcpListener::bind(&*node_address).unwrap();
            println!("internode comm listening at {} ...", node_address);
            for stream in listener.incoming() {
                let z = arc_self.clone();
                thread::spawn(move || {

                    let mut stream = stream.unwrap();

                    {
                        //let z = z.lock().unwrap();

                        //let data = z.the_encd.encode(&"INTERNODE: Hello World\r\n".to_string()).ok().unwrap();
                        //stream.write(&*data).unwrap();
                    }


                    'the_loop: loop {
                        let mut buff = vec![0u8; 100];
                        match stream.read(&mut buff){
                            Ok(count) if count == 0 => break 'the_loop,
                            Ok(count) => {

                                let mut z = z.lock().unwrap();

                                // let data = String::from_utf8(buff[0..count].to_vec()).unwrap();
                                let data = z.the_encd.decode(&buff[0..count]).ok().unwrap();

                                debug!("read {} bytes : {:?}", count, &data);
                                if data.len() == 0 {
                                    continue 'the_loop;
                                }
                                
                                if data == "ping" {
                                    info!("got ping from {}", stream.peer_addr().ok().unwrap());
                                    z.write(&mut stream, "pong");
                                    continue 'the_loop;
                                }

                                let s:Vec<&str> = data.split("|").collect();

                                debug!("data splited: {:?}", s);

                                // check for version
                                let version = s[0];

                                if !version.starts_with("v"){
                                    warn!("invalid protocol version: {}", version);
                                    break 'the_loop;
                                }

                                if s.len() == 1 {
                                    warn!("invalid request, no cmd provided.");
                                    break 'the_loop;
                                }

                                debug!("connected node version: {}", version);

                                let cmd = s[1];

                                debug!("got cmd: {}", &cmd);

                                match z.process_cmd(cmd, &s, &mut stream){
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
        
        // join the network
        for seed in &seeds {
            let addr:SocketAddr = seed.parse().unwrap();
            match TcpStream::connect(addr){
                Ok(ref mut stream) => {
                    
                    fn write_ignore(stream:&mut TcpStream, data:&str){
                        let _ = stream.write(data.as_bytes());
                        let _ = stream.flush();
                        let mut buff = vec![0u8; 100];
                        stream.read(&mut buff);
                        debug!("buff: {}", String::from_utf8(buff).unwrap());
                    }
                    
                    //let data = format!("v1|join|{}", my_node_address);
                    write_ignore(stream, &*format!("v1|join|{}", my_node_address));
                    write_ignore(stream, "v1|copy-rt");
                    
                    trace!("join network done.");
                },
                Err(_) => {
                }
            }
        }

    }
    
    fn setup_network_keeper(arc_self: &Arc<Mutex<&'static mut InternodeService>>){
        //let static_self:&'static mut InternodeService = unsafe{ std::mem::transmute(self) };
        //let arc_self = Arc::new(Mutex::new(static_self));
        
        let z = arc_self.clone();
        
        thread::spawn(move || {
            loop {
                
                debug!("checking network health...");
                
                let mut to_remove:Vec<u32> = Vec::new();
                
                
                let mut _self = z.lock().unwrap();
                for rt in &_self.routing_tables {
                    debug!("   -> {}:{}", rt.ip_address, rt.port);

                    let addr:SocketAddr = format!("{}:{}", rt.ip_address, rt.port).parse().unwrap();
                    match TcpStream::connect(addr){
                        Ok(ref mut stream) => {
                            let _ = stream.write(b"ping");
                            let mut buff = vec![0u8; 100];
                            match stream.read(&mut buff) {
                                Ok(count) if count > 0 => {
                                    let data = String::from_utf8(buff).ok().unwrap();
                                    debug!("got {}", data);
                                },
                                _ => to_remove.push(rt.guid)
                            }
                        },
                        Err(e) => {
                            // remove from routing tables
                            debug!("{} not reached: {}, removed from routing tables.", addr, e);
                            to_remove.push(rt.guid);
                        }
                    }

                }
            
            
                //let mut _self = z.lock().unwrap();
                for guid in to_remove {
                    let idx = _self.node_index(guid) as usize;
                    &_self.routing_tables.swap_remove(idx);
                }
            
                
                debug!("health checking done.");
                
                thread::sleep_ms(5000);
            }
        });
    }

    // fn write(&self, stream: &mut TcpStream, data: &String){
    //     let _ = stream.write(&*self.the_encd.encode(data).ok().unwrap());
    // }

    fn write(&self, stream: &mut TcpStream, data: &str){
        //self.write(stream, data.to_string());
        let _ = stream.write(&*self.the_encd.encode(&data.to_string()).ok().unwrap());
    }

    fn process_cmd(&mut self, cmd: &str, params:&Vec<&str>, stream: &mut TcpStream) -> ZResult {
        match cmd {
            "join" => {
                trace!("join cmd recvd.");
                // check parameters
                
                if params.len() != 3 {
                    return Err("Invalid parameter, should be 3");
                }
                
                
                let x:Vec<&str> = params[2].split(":").collect();
                
                if x.len() < 2 {
                    return Err("invalid address");
                }

                let (ip_addr, port) = (x[0].to_string(), x[1].parse().ok().unwrap());
                
                debug!("peer addr: {}:{}", ip_addr, port);


                match self.get_rt(stream){
                    Some(rt) if rt.ip_address == ip_addr && rt.port == port => {
                        warn!("ip {}:{} already joined as guid {}", rt.ip_address, rt.port, rt.guid);

                        // just send existing guid
                        let data = &format!("v1|guid|{}\n", rt.guid);
                        //stream.write(&*self.the_encd.encode(data).ok().unwrap());
                        self.write(stream, data);

                        return Ok(0);
                    },
                    _ => ()
                }

                let guid = self.generate_guid();

                debug!("generated guid: {}", guid);

                let data = &format!("v1|guid|{}\n", guid);

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

                    let rtb:Vec<String> = self.routing_tables.iter()
                        .map(|rt| format!("{},{},{}", rt.guid, rt.ip_address, rt.port))
                        .collect();
                    let data = format!("v1|rt|{}", rtb.join("|"));

                    self.write(stream, &data);

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
        let socket_ip_address = match stream.peer_addr() {
            Ok(sock_addr) => {
                format!("{}",sock_addr.ip())
            },
            Err(e) => {
                error!("{}", e);
                panic!("cannot get peer address");
            }
        };
        (&self.routing_tables).iter().find(|&r| r.ip_address == socket_ip_address)
    }
    
    fn get_rt_by_guid(&self, guid: u32) -> Option<&RoutingTable> {
        self.routing_tables.iter().find(|&r| r.guid == guid)
    }

    fn node_index(&self, id:u32) -> i32 {
        let mut it = self.routing_tables.iter();
        match it.position(|p| p.guid == id){
            Some(i) => i as i32,
            None => -1
        }
    }

    fn is_node_registered(&self, id:u32) -> bool {
        let idx = self.node_index(id);
        debug!("idx: {}", idx);
        idx > -1
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
