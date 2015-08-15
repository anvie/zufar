
use std;
use std::thread;
use std::net::TcpListener;
use std::net::{TcpStream, SocketAddr};
use std::io::prelude::*;
//use std::error;
use std::str;
use std::sync::{Arc, Mutex};


use encd;
use encd::MessageEncoderDecoder;
use node::Node;

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
    my_node_address: String,
    the_encd: Arc<Mutex<encd::PlainTextEncoderDecoder>>,
    routing_tables: Arc<Mutex<Vec<RoutingTable>>>,
}

type ZResult = Result<i32, &'static str>;


impl InternodeService {

    pub fn new() -> InternodeService {
        InternodeService {
            my_guid: 0,
            my_node_address: "unset".to_string(),
            the_encd: Arc::new(Mutex::new(encd::PlainTextEncoderDecoder::new())),
            routing_tables: Arc::new(Mutex::new(Vec::new()))
        }
    }

    pub fn setup_internode_communicator(&mut self, node_address: &String, seeds:Vec<String>){
        
        let node_address = node_address.clone();
        self.my_node_address = node_address.clone();
    
        //let _self:&'static mut InternodeService = unsafe{ std::mem::transmute(self) };

        // join the network
        for seed in &seeds {
            let addr:SocketAddr = seed.parse().unwrap();
            match TcpStream::connect(addr){
                Ok(ref mut stream) => {

                    self.send_cmd_and_handle(stream, &*format!("v1|join|{}", node_address));
                    self.send_cmd_and_handle(stream, "v1|copy-rt");

                    trace!("join network done.");
                },
                Err(_) => {
                }
            }
        }
    
        
        //let my_node_address = node_address.clone();
        //{
            self.setup_network_keeper();
        //}

        //let static_self:&'static mut InternodeService = unsafe{ std::mem::transmute(self) };
        
        let the_encd = self.the_encd.clone();
        let my_guid = self.my_guid;
        let routing_tables = self.routing_tables.clone();
        
        thread::spawn(move || {
            
            //let arc_self = Arc::new(Mutex::new(static_self));
            //InternodeService::setup_network_keeper(&arc_self);
            
            //let tcp_node_address:SocketAddr = (&node_address).parse().unwrap();
            let listener = TcpListener::bind(&*node_address).unwrap();
            println!("internode comm listening at {} ...", node_address);
            for stream in listener.incoming() {
                
                let routing_tables = routing_tables.clone();
                let the_encd = the_encd.clone();
                
                //let z = arc_self.clone();
                thread::spawn(move || {

                    let mut stream = stream.unwrap();

                    'the_loop: loop {
                        
                        let mut routing_tables = routing_tables.lock().unwrap();
                        let the_encd = the_encd.lock().unwrap();
                        
                        let mut buff = vec![0u8; 100];
                        match stream.read(&mut buff){
                            Ok(count) if count == 0 => break 'the_loop,
                            Ok(count) => {

                                //let mut z = z.lock().unwrap();

                                // let data = String::from_utf8(buff[0..count].to_vec()).unwrap();
                                //let data = z.the_encd.decode(&buff[0..count]).ok().unwrap();
                                
                                let data = the_encd.decode(&buff[0..count]).ok().unwrap();

                                debug!("read {} bytes : {:?}", count, &data);
                                if data.len() == 0 {
                                    continue 'the_loop;
                                }
                                
                                let s:Vec<&str> = data.trim().split("|").collect();
                                
                                if s[0] == "ping" {
                                    if s.len() == 2 && s[1].len() > 0 {
                                        info!("got ping from {} (node-{})", stream.peer_addr().ok().unwrap(), s[1]);
                                    }else{
                                        info!("got ping from {} (node-??)", stream.peer_addr().ok().unwrap());
                                    }
                                    InternodeService::write(&mut stream, "pong", &the_encd);
                                    continue 'the_loop;
                                }

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

                                match InternodeService::process_cmd(cmd, &s, &mut stream, 
                                    my_guid, &mut routing_tables, &the_encd){
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
    
    fn send_cmd_and_handle(&mut self, stream:&mut TcpStream, data:&str){
        
        let cl = self.routing_tables.clone();
        let mut routing_tables = cl.lock().unwrap();
        
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
        
        match &s[1] {
            &"guid" => {
                self.my_guid = s[2].parse().unwrap();
                info!("my guid is {}", self.my_guid);
                
                // add first connected seed to routing tables
                let seed_guid:u32 = s[3].parse().unwrap();
                let pa = &stream.peer_addr().ok().unwrap();
                let connected_seed_addr = format!("{}", pa.ip());
                info!("added {}:{} to the rts with guid {}", connected_seed_addr, pa.port(), seed_guid);
                routing_tables.push(RoutingTable::new(seed_guid, connected_seed_addr, pa.port()));
                debug!("rts count now: {}", routing_tables.len());
            },
            &"rt" => {
                
                let ss = &s[2..];
                
                for s in ss {
                    let s:Vec<&str> = s.split(",").collect();
                    let guid:u32 = s[0].parse().unwrap();
                    if InternodeService::is_node_registered(guid, &routing_tables){
                        continue;
                    }
                    let ip_addr = s[1].to_string();
                    let port:u16 = s[2].parse().unwrap();
                    if guid != self.my_guid {
                        info!("added {}:{} to the rts with guid {}", ip_addr, port, guid);
                        
                        routing_tables.push(RoutingTable::new(guid, ip_addr.clone(), port));
                        
                        // request to add me 
                        let mut node = Node::new(guid, &format!("{}:{}", ip_addr, port));
                        node.add_to_rts(Node::new(self.my_guid, &self.my_node_address));
                    }
                }
                debug!("rts count now: {}", routing_tables.len());
                
                
            },
            x => {
                warn!("unknown cmd: {}", x)
            }
        }
        
    }
    
    fn setup_network_keeper(&mut self){ //(arc_self: &Arc<Mutex<&'static mut InternodeService>>){
        //let static_self:&'static mut InternodeService = unsafe{ std::mem::transmute(self) };
        //let arc_self = Arc::new(Mutex::new(static_self));
        
        //let z = arc_self.clone();
        
        //let _self:&'static mut InternodeService = unsafe{ std::mem::transmute(self) }; //Arc::new(unsafe{ std::mem::transmute(self) });
        //let mut _self = _self.clone();
        
        let rts = self.routing_tables.clone();
        let my_guid = self.my_guid;
        
        thread::spawn(move || {
            //let mut cycle = 0;
            loop {
                
                debug!("checking network health...");
                
                {
                    let mut to_remove:Vec<u32> = Vec::new();


                    //let mut _self = z.lock().unwrap();
                    let mut routing_tables = rts.lock().unwrap();
                    
                    for rt in &*routing_tables {
                        debug!("   -> {}", rt.get_host_n_port());
                        //debug!("   -> {:?}", rt);

                        let addr:SocketAddr = format!("{}:{}", rt.ip_address, rt.port).parse().unwrap();
                        match TcpStream::connect(addr){
                            Ok(ref mut stream) => {
                                let _ = stream.write(format!("ping|{}", my_guid).as_bytes());
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
                    let mut count = 0;
                    for guid in to_remove {
                        //let idx = _self.node_index(*guid) as usize;
                        
                        let idx = {
                            let mut it = routing_tables.iter();
                            let idx = match it.position(|p| p.guid == guid){
                                Some(i) => i as i32,
                                None => -1
                            };
                            idx
                        };
                        
                        if idx > 0 {
                            routing_tables.swap_remove(idx as usize);
                            count = count + 1;
                        }
                    }
                    if count > 0 {
                        debug!("rts count now: {}", routing_tables.len());
                    }
                }
            
                
                debug!("health checking done.");
                
                // @TODO(robin): code this
                // {
                //     let mut _self = z.lock().unwrap();
                //     if cycle > 10 {
                //         cycle = 0;
                // 
                //         _self.send_cmd_and_handle(stream, )
                // 
                //     }
                //     cycle = cycle + 1;
                // }
                
                
                thread::sleep_ms(15000);
                
            }
        });
    }

    // fn write(&self, stream: &mut TcpStream, data: &String){
    //     let _ = stream.write(&*self.the_encd.encode(data).ok().unwrap());
    // }

    fn write(stream: &mut TcpStream, data: &str, the_encd:&encd::PlainTextEncoderDecoder){
        //self.write(stream, data.to_string());
        let _ = stream.write(the_encd.encode(&data.to_string()).ok().unwrap().as_ref());
    }

    fn process_cmd(cmd: &str, params:&Vec<&str>, stream: &mut TcpStream, 
            my_guid:u32, routing_tables:&mut Vec<RoutingTable>, the_encd:&encd::PlainTextEncoderDecoder) -> ZResult {
        
        // let my_guid = self.my_guid;
        // let cl = self.routing_tables.clone();
        // let routing_tables = cl.lock().unwrap();
        
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


                match InternodeService::get_rt(stream, &routing_tables){
                    Some(rt) if rt.ip_address == ip_addr && rt.port == port => {
                        warn!("ip {}:{} already joined as guid {}", rt.ip_address, rt.port, rt.guid);

                        // just send existing guid
                        let data = &format!("v1|guid|{}|{}\n", rt.guid, my_guid);
                        //stream.write(&*self.the_encd.encode(data).ok().unwrap());
                        InternodeService::write(stream, data, &the_encd);

                        return Ok(0);
                    },
                    _ => ()
                }

                let guid = InternodeService::generate_guid(my_guid, &routing_tables);

                debug!("generated guid: {}", guid);

                let data = &format!("v1|guid|{}|{}\n", guid, my_guid);

                routing_tables.push(RoutingTable::new(guid, ip_addr, port));

                //stream.write(&*self.the_encd.encode(data).ok().unwrap());

                InternodeService::write(stream, data, the_encd);

                Ok(0)
            },
            "leave" => {
                trace!("leave cmd recvd.");

                let guid = InternodeService::get_guid_from_stream(stream, &routing_tables);
                trace!("guid: {}", guid);

                if guid > 0 {
                    let idx = InternodeService::node_index(guid, &routing_tables) as usize;
                    let x = routing_tables.swap_remove(idx);
                    debug!("removing node {:?} from routing tables", x);
                }

                Ok(1)
            },
            "copy-rt" => { // copy routing tables

                if InternodeService::get_guid_from_stream(stream, &routing_tables) > 0 {
                    trace!("copy routing tables...");
                    println!("routing tables: {:?}", routing_tables);

                    let rtb:Vec<String> = routing_tables.iter()
                        .map(|rt| format!("{},{},{}", rt.guid, rt.ip_address, rt.port))
                        .collect();
                    let data = format!("v1|rt|{}", rtb.join("|"));

                    InternodeService::write(stream, &data, the_encd);

                    Ok(0)
                }else{
                    Err("not registered as node")
                }
            },
            "add-me" => {
                
                //let x:Vec<&str> = params[2].split(":").collect();
                let params = &params[2..];
                
                let guid:u32 = params[0].parse().unwrap();
                
                // check is already exists?
                if !InternodeService::is_node_registered(guid, &routing_tables){
                    
                    let s:Vec<&str> = params[1].split(":").collect();
                    
                    if s.len() == 2 {
                        let ip_addr = s[0];
                        let port:u16 = s[1].parse().unwrap();

                        routing_tables.push(RoutingTable::new(guid, ip_addr.to_string(), port));
                        
                        info!("node {}:{} added into rts by request", ip_addr, port);
                        info!("rts count now: {}", routing_tables.len());
                    }
                    
                }
                
                let data = format!("v1|rv|200");
                InternodeService::write(stream, &data, the_encd);
                
                Ok(1)
                
            },
            x => {
                debug!("unknwon cmd: {}", x);
                Err("unknown cmd")
            }
        }
    }

    fn get_guid_from_stream(stream: &TcpStream, routing_tables: &Vec<RoutingTable>) -> u32 {
        match InternodeService::get_rt(stream, routing_tables) {
            Some(rt) => rt.guid,
            None => 0,
        }
    }

    // fn get_routing_tables<'a>(&'a mut self) -> &'a mut Vec<RoutingTable> {
    //     &mut self.routing_tables
    // }

    fn get_rt<'a>(stream: &TcpStream, routing_tables: &'a Vec<RoutingTable>) -> Option<&'a RoutingTable> {
        let socket_ip_address = match stream.peer_addr() {
            Ok(sock_addr) => {
                format!("{}",sock_addr.ip())
            },
            Err(e) => {
                error!("{}", e);
                panic!("cannot get peer address");
            }
        };
        routing_tables.iter().find(|r| r.ip_address == socket_ip_address)
    }
    
    fn get_rt_by_guid(routing_tables: &Vec<RoutingTable>, guid: u32) -> Option<&RoutingTable> {
        routing_tables.iter().find(|&r| r.guid == guid)
    }

    fn node_index(id:u32, routing_tables: &Vec<RoutingTable>) -> i32 {
        let mut it = routing_tables.iter();
        match it.position(|p| p.guid == id){
            Some(i) => i as i32,
            None => -1
        }
    }

    fn is_node_registered(id:u32, routing_tables: &Vec<RoutingTable>) -> bool {
        let idx = InternodeService::node_index(id, routing_tables);
        debug!("idx: {}", idx);
        idx > -1
    }

    fn generate_guid(init_guid:u32, routing_tables: &Vec<RoutingTable>) -> u32 {
        let mut gid = init_guid + 1;
        //let mut done = false;
        while InternodeService::is_node_registered(gid, routing_tables) {
            gid = gid + 1;
        }
        gid
    }

}
