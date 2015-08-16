
use std;
use std::thread;
use std::net::TcpListener;
use std::net::{TcpStream, SocketAddr};
use std::io::prelude::*;
//use std::error;
use std::str;
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{Receiver, Sender};
use std::cell::RefCell;

use encd;
use encd::MessageEncoderDecoder;
use node::Node;
use cluster;
use cluster::RoutingTable;



pub struct MeState {
    pub my_guid: u32,
    pub rts_count: usize,
//    pub routing_tables: RefCell<Vec<RoutingTable>>
}

impl MeState {
    pub fn new(my_guid:u32, rts_count: usize) -> MeState {
        MeState {
            my_guid: my_guid,
            rts_count: rts_count,
//            routing_tables: RefCell::new(Vec::new())
        }
    }
}


pub struct InternodeService {
    pub my_guid: u32,
    the_encd: encd::PlainTextEncoderDecoder,
    info: Arc<Mutex<cluster::Info>>
    //tx:Sender<MeState>
}

type ZResult = Result<i32, &'static str>;


impl InternodeService {

    pub fn new(info:Arc<Mutex<cluster::Info>>) -> Arc<Mutex<InternodeService>> {
        Arc::new(Mutex::new(InternodeService {
            my_guid: 0,
            // my_node_address: node_address.clone(),
            // my_api_address: api_address.clone(),
            the_encd: encd::PlainTextEncoderDecoder::new(),
            // routing_tables: Arc::new(Mutex::new(Vec::new())),
            //routing_tables: Vec::new(),
            // seeds: seeds,
            //tx: tx,
            info: info
        }))
    }

    pub fn start(_self:Arc<Mutex<InternodeService>>){
        
        
        let (my_guid, my_node_address, my_api_address, seeds) = {
            let _self = _self.clone();
            let _self = _self.lock().unwrap();
            let info = _self.info.clone();
            let info = info.lock().unwrap();
            (info.my_guid, info.my_node_address.clone(), info.my_api_address.clone(), info.seeds.clone())
        };
        
        //let _self = Arc::new(Mutex::new(self));
    
        // join the network
        for seed in seeds {
            let addr:SocketAddr = seed.parse().unwrap();
            match TcpStream::connect(addr){
                Ok(ref mut stream) => {

                    // [VERSION]|join|[NODE-ADDRESS]|[API-ADDRESS]
                    //let mut inode = inode.lock().unwrap();
                    //let info = info.lock().unwrap();
                    
                    let mut _self = _self.lock().unwrap();
                    _self.send_cmd_and_handle(stream, &*format!("v1|join|{}|{}", my_node_address, my_api_address));
                    _self.send_cmd_and_handle(stream, "v1|copy-rt");

                    trace!("join network done.");
                },
                Err(_) => {
                }
            }
        }
    
        
        //let my_node_address = node_address.clone();
        {
            let mut _self = _self.lock().unwrap();
            _self.setup_network_keeper();
        }

        //let static_self:&'static mut InternodeService = unsafe{ std::mem::transmute(self) };
        
        //let the_encd = inode.the_encd.clone();
        //let my_guid = inode.my_guid;
        //let routing_tables = inode.routing_tables;
        
        //let inode = inode.clone();
        
        //let inode = Arc::new(Mutex::new(self));
        
        let _self = _self.clone();
        let info = {
            _self.lock().unwrap().info.clone()
        };
        
        
        thread::spawn(move || {
            
            let listener = TcpListener::bind(&*my_node_address).unwrap();
            println!("internode comm listening at {} ...", my_node_address);
            for stream in listener.incoming() {
                
                let _self = _self.clone();
                let info = info.clone();
                
                thread::spawn(move || {

                    let mut stream = stream.unwrap();

                    'the_loop: loop {
                        
                        //let inode = inode.lock().unwrap();

                        
                        let mut buff = vec![0u8; 100];
                        match stream.read(&mut buff){
                            Ok(count) if count == 0 => break 'the_loop,
                            Ok(count) => {

                                let _self = _self.lock().unwrap();
                                
                                //let mut z = z.lock().unwrap();

                                // let data = String::from_utf8(buff[0..count].to_vec()).unwrap();
                                //let data = z.the_encd.decode(&buff[0..count]).ok().unwrap();
                                
                                let data = _self.the_encd.decode(&buff[0..count]).ok().unwrap();

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
                                    _self.write(&mut stream, "pong");
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

                                match _self.process_cmd(cmd, &s, &mut stream){
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
        
        //let cl = self.routing_tables.clone();
        //let mut routing_tables = cl.lock().unwrap();
        
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
                let connected_seed_addr = format!("{}:{}", pa.ip(), pa.port());
                info!("added {} to the rts with guid {}", connected_seed_addr, seed_guid);
                {
                    let mut info = self.info.lock().unwrap();
                    let my_api_address = info.my_api_address.clone();
                    let mut rts = &mut info.routing_tables;
                    rts.push(RoutingTable::new(seed_guid, connected_seed_addr, my_api_address));
                    debug!("rts count now: {}", rts.len());
                }
                
            },
            &"rt" => { 
                
                //// v1|rt|1,127.0.0.1:7123,127.0.0.1:7122
                //// [VERSION]|rt|[GUID],[NODE-ADDRESS],[API-ADDRESS]
                
                let ss = &s[2..];
                
                for s in ss {
                    let s:Vec<&str> = s.split(",").collect();
                    let guid:u32 = s[0].parse().unwrap();
                    if self.is_node_registered(guid){
                        continue;
                    }
                    let node_address = s[1].to_string();
                    let api_address = s[2].to_string();
                    
                    if guid != self.my_guid {
                        info!("added {} to the rts with guid {}", node_address, guid);
                        
                        let mut info = self.info.lock().unwrap();
                        {
                            let mut rts = &mut info.routing_tables;
                            rts.push(RoutingTable::new(guid, node_address.clone(), api_address.clone()));
                            debug!("rts count now: {}", rts.len());
                        }
                        
                        // request to add me 
                        let (my_node_address, my_api_address) = (&info.my_node_address, &info.my_api_address);
                        let mut node = Node::new(guid, &node_address, &api_address); //&format!("node: {} - api: {}", ip_addr, api_addr));
                        node.add_to_rts(Node::new(info.my_guid, &my_node_address, &my_api_address));
                        
                    
                    }
                }
                
                
                
            },
            x => {
                warn!("unknown cmd: {}", x)
            }
        }
        
    }
    
    fn setup_network_keeper(&self){ //(arc_self: &Arc<Mutex<&'static mut InternodeService>>){
        //let static_self:&'static mut InternodeService = unsafe{ std::mem::transmute(self) };
        //let arc_self = Arc::new(Mutex::new(static_self));
        
        //let z = arc_self.clone();
        
        //let _self:&'static mut InternodeService = unsafe{ std::mem::transmute(self) }; //Arc::new(unsafe{ std::mem::transmute(self) });
        //let mut _self = _self.clone();
        
        let info = self.info.clone();
        
        //let rts = info.routing_tables.clone();
        //let rts2 = info.routing_tables.clone();
        //let my_guid = info.my_guid;
        
        thread::spawn(move || {
            //let mut cycle = 0;
            loop {
                
                debug!("checking network health...");
                
                {
                    let mut to_remove:Vec<u32> = Vec::new();

                    let mut info = info.lock().unwrap();
                    let my_guid = info.my_guid;
                    let mut routing_tables = &mut info.routing_tables;
                    
                    for rt in &*routing_tables {
                        debug!("   -> {}", rt.node_address());
                        //debug!("   -> {:?}", rt);

                        let addr:SocketAddr = rt.node_address().parse().unwrap();
                        match TcpStream::connect(addr){
                            Ok(ref mut stream) => {
                                let _ = stream.write(format!("ping|{}", my_guid).as_bytes());
                                let mut buff = vec![0u8; 100];
                                match stream.read(&mut buff) {
                                    Ok(count) if count > 0 => {
                                        let data = String::from_utf8(buff).ok().unwrap();
                                        debug!("got {}", data);
                                    },
                                    _ => to_remove.push(rt.guid())
                                }
                            },
                            Err(e) => {
                                // remove from routing tables
                                debug!("{} not reached: {}, removed from routing tables.", addr, e);
                                to_remove.push(rt.guid());
                            }
                        }

                    }


                    //let mut _self = z.lock().unwrap();
                    let mut count = 0;
                    for guid in to_remove {
                        //let idx = _self.node_index(*guid) as usize;
                        
                        let idx = {
                            let mut it = routing_tables.iter();
                            match it.position(|p| p.guid() == guid){
                                Some(i) => i as i32,
                                None => -1
                            }
                        };
                        
                        if idx > -1 {
                            routing_tables.swap_remove(idx as usize);
                            count = count + 1;
                        }
                    }
                    if count > 0 {
                        debug!("rts count now: {}", routing_tables.len());
                    }
                    //tx.send(MeState::new(my_guid, routing_tables.len()));
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

    fn write(&self, stream: &mut TcpStream, data: &str){
        //self.write(stream, data.to_string());
        let _ = stream.write(self.the_encd.encode(&data.to_string()).ok().unwrap().as_ref());
    }

    fn process_cmd(&self, cmd: &str, params:&Vec<&str>, stream: &mut TcpStream) -> ZResult {
        
        match cmd {
            "join" => { // [VERSION]|join|[NODE-ADDRESS]|[API-ADDRESS]
                trace!("join cmd recvd.");
                // check parameters
                
                if params.len() != 4 {
                    return Err("Invalid parameter, should be 3");
                }
                
                
                // let x:Vec<&str> = params[2].split(":").collect();
                // 
                // if x.len() < 2 {
                //     return Err("invalid address");
                // }
                // 
                // let (ip_addr, port) = (x[0].to_string(), x[1].parse().ok().unwrap());
                
                let peer_addr = params[2];
                
                debug!("peer addr: {}", peer_addr);


                match self.get_rt(stream){
                    Some(ref rt) if rt.node_address() == peer_addr => {
                        warn!("ip {} already joined as guid {}", rt.node_address(), rt.guid());

                        // just send existing guid
                        let data = &format!("v1|guid|{}|{}\n", rt.guid(), self.my_guid);
                        //stream.write(&*self.the_encd.encode(data).ok().unwrap());
                        self.write(stream, data);

                        return Ok(0);
                    },
                    _ => ()
                }

                let guid = self.generate_guid();

                debug!("generated guid: {}", guid);

                let data = &format!("v1|guid|{}|{}\n", guid, self.my_guid);
                         

                // let api_addr = {
                //     let x:Vec<&str> = params[3].split(":").collect();
                // 
                //     if x.len() < 2 {
                //         return Err("invalid address");
                //     }
                //     format!("{}:{}",x[0].to_string(), x[1].parse().ok().unwrap())
                // };
                
                let api_address = params[3];

                {
                    let mut info = self.info.lock().unwrap();
                    let mut rts = &mut info.routing_tables;
                    rts.push(RoutingTable::new(guid, peer_addr.to_string(), api_address.to_string()));
                }

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
                    {
                        let mut info = self.info.lock().unwrap();
                        let mut rts = &mut info.routing_tables;
                        let x = rts.swap_remove(idx);    
                        debug!("removing node {:?} from routing tables", x);
                    }
                    
                    
                }

                Ok(1)
            },
            "copy-rt" => { // copy routing tables

                //if self.get_guid_from_stream(stream) > 0 {
                    trace!("copy routing tables...");

                    {
                        let mut info = self.info.lock().unwrap();
                        let mut rts = &mut info.routing_tables;
                        
                        println!("routing tables: {:?}", rts);
                        
                        let rtb:Vec<String> = rts.iter()
                            .map(|rt| format!("{},{},{}", rt.guid(), rt.node_address(), rt.api_address()))
                            .collect();
                        let data = format!("v1|rt|{}", rtb.join("|"));

                        self.write(stream, &data);
                    }

                    Ok(0)
                // }else{
                //                     Err("not registered as node")
                //                 }
            },
            "add-me" => {
                
                //let x:Vec<&str> = params[2].split(":").collect();
                let params = &params[2..];
                
                let guid:u32 = params[0].parse().unwrap();
                
                // check is already exists?
                if !self.is_node_registered(guid){
                    

                    {
                        let mut info = self.info.lock().unwrap();
                        let mut rts = &mut info.routing_tables;
                        
                        let node_address = params[2];
                        let api_address = params[3];

                        rts.push(RoutingTable::new(guid, node_address.to_string(), api_address.to_string()));

                        info!("node {} added into rts by request", node_address);
                        info!("rts count now: {}", rts.len());
                    }
                
                    
                }
                
                let data = format!("v1|rv|200");
                self.write(stream, &data);
                
                Ok(1)
                
            },
            x => {
                debug!("unknwon cmd: {}", x);
                Err("unknown cmd")
            }
        }
    }

    fn get_guid_from_stream(&self, stream: &TcpStream) -> u32 {
        match self.get_rt(stream) {
            Some(ref rt) => rt.guid(),
            None => 0,
        }
    }

    // fn get_routing_tables<'a>(&'a mut self) -> &'a mut Vec<RoutingTable> {
    //     &mut self.routing_tables
    // }
    
    // pub fn routing_tables<'a>(&'a self) -> &'a Arc<Mutex<Vec<RoutingTable>>> {
    //     &self.routing_tables
    // }

    fn get_rt<'a>(&'a self, stream: &TcpStream) -> Option<RoutingTable> {
        let socket_ip_address = match stream.peer_addr() {
            Ok(sock_addr) => {
                format!("{}", sock_addr.ip())
            },
            Err(e) => {
                error!("{}", e);
                panic!("cannot get peer address");
            }
        };
        
        let info = self.info.lock().unwrap();
        let rts = &info.routing_tables;
        match rts.iter().find(|r| *r.node_address() == socket_ip_address){
            Some(rt) => Some(rt.clone()),
            None => None
        }
    }
    
    pub fn get_rt_by_guid(&self, guid: u32) -> Option<RoutingTable> {
        let info = self.info.lock().unwrap();
        let rts = &info.routing_tables;
        
        match rts.iter().find(|&r| r.guid() == guid){
            Some(rt) => Some(rt.clone()),
            None => None
        }
    }

    fn node_index(&self, id:u32) -> i32 {
        let info = self.info.lock().unwrap();
        let rts = &info.routing_tables;
        let mut it = rts.iter();
        match it.position(|p| p.guid() == id){
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
