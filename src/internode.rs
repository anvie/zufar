
use std;
use std::thread;
use std::net::TcpListener;
use std::net::{TcpStream, SocketAddr};
use std::io::prelude::*;
//use std::error;
use std::str;
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{Receiver, Sender};
//use std::cell::RefCell;


use encd;
use encd::MessageEncoderDecoder;
use node::{Node, NodeClient};
use cluster;
use cluster::RoutingTable;
use db::Db;


pub struct InternodeService {
    pub my_guid: u32,
    the_encd: encd::BytesEncoderDecoder,
    info: Arc<Mutex<cluster::Info>>,
    db: Arc<Mutex<Db>>,
    // tx:Sender<String>,
    // rx:Receiver<String>
}

type ZResult = Result<i32, &'static str>;


impl InternodeService {

    pub fn new(info:Arc<Mutex<cluster::Info>>, db:Arc<Mutex<Db>>) -> InternodeService {
        
        InternodeService {
            my_guid: 0,
            the_encd: encd::BytesEncoderDecoder::new(),
            info: info,
            db: db
            // tx: tx,
            // rx: rx
        }
    }

    pub fn start(info:Arc<Mutex<cluster::Info>>, db:Arc<Mutex<Db>>){


        let (my_node_address, my_api_address, seeds) = {
            //let info = info.clone();
            let info = info.lock().unwrap();
            (info.my_node_address.clone(), info.my_api_address.clone(), info.seeds.clone())
        };

        // join the network
        InternodeService::join_the_network(info);
        InternodeService::setup_network_keeper(info);
        

        thread::spawn(move || {

            let listener = TcpListener::bind(&*my_node_address).unwrap();
            println!("internode comm listening at {} ...", my_node_address);
            for stream in listener.incoming() {

                let info = info.clone();
                let db = db.clone();

                thread::spawn(move || {

                    let mut stream = stream.unwrap();

                    'the_loop: loop {


                        let mut buff = vec![0u8; 100];
                        match stream.read(&mut buff){
                            Ok(count) if count == 0 => break 'the_loop,
                            Ok(count) => {

                                let _self = InternodeService::new(info, db);

                                // trace!("acquire lock for `self` in main loop");
                                // let _self = _self.lock().expect("cannot lock");
                                // trace!("acquire lock for `self` in main loop --> acquired.");

                                let data = _self.the_encd.decode(&buff[0..count])
                                    .ok().expect("cannot decode data");

                                debug!("read {} bytes : {:?}", count, &data);
                                if data.len() == 0 {
                                    continue 'the_loop;
                                }

                                let data = String::from_utf8(data.to_vec()).unwrap();

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


                            },
                            Err(e) => {
                                err!(10, "Cannot read data from other node. {}", e);
                                //break 'the_loop;
                            }
                        };
                    }

                });
            }
        });
    }

    fn join_the_network(info:Arc<Mutex<cluster::Info>>){
        
        fn _send(stream:&mut TcpStream, data:&str) -> Option<Vec<String>> {
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
                return None;
            }
            
            Some(s.iter().map(|d| d.to_string()).collect())
        }
        
        let (my_node_address, my_api_address, seeds) = {
            //let info = info.clone();
            let info = info.lock().unwrap();
            (info.my_node_address.clone(), info.my_api_address.clone(), info.seeds.clone())
        };
        
        for seed in seeds {
            let addr:SocketAddr = seed.parse().unwrap();
            match TcpStream::connect(addr){
                Ok(ref mut stream) => {

                    // [VERSION]|join|[NODE-ADDRESS]|[API-ADDRESS]


                    // let mut _self = _self.lock().unwrap();
                    // _self.send_cmd_and_handle(stream, &*format!("v1|join|{}|{}", my_node_address, my_api_address));
                    // _self.send_cmd_and_handle(stream, "v1|copy-rt");
                    
                    match _send(stream, &*format!("v1|join|{}|{}", my_node_address, my_api_address)){
                        Some(s) if s[1] == "guid" => {
                            let my_guid = s[2].parse().unwrap();
                            info!("my guid is {}", my_guid);


                            // add first connected seed to routing tables
                            let seed_guid:u32 = s[3].parse().unwrap();
                            let pa = &stream.peer_addr().ok().unwrap();
                            let connected_seed_addr = format!("{}:{}", pa.ip(), pa.port());
                            info!("added {} to the rts with guid {}", connected_seed_addr, seed_guid);

                            let my_api_address = s[4];

                            {
                                let mut info = info.lock().unwrap();
                                info.my_guid = my_guid;

                                let mut rts = &mut info.routing_tables;
                                rts.push(RoutingTable::new(seed_guid, connected_seed_addr, my_api_address.to_string()));
                                debug!("rts count now: {}", rts.len());
                            }
                        },
                        _ => ()
                    }
                    match _send(stream, "v1|copy-rt"){
                        Some(s) if s[1] == "rt" => {
                            
                            //// v1|rt|1,127.0.0.1:7123,127.0.0.1:7122
                            //// [VERSION]|rt|[GUID],[NODE-ADDRESS],[API-ADDRESS]

                            let ss = &s[2..];
                            
                            let my_guid = {
                               let info = info.lock().unwrap();
                               info.my_guid
                            };

                            for s in ss {
                                let s:Vec<&str> = s.split(",").collect();
                                let guid:u32 = s[0].parse().unwrap();

                                {
                                    let info = info.lock().unwrap();
                                    let rts = &info.routing_tables;
                                    let mut it = rts.iter();
                                    match it.position(|p| p.guid() == guid){
                                        Some(i) => {
                                            continue;
                                        },
                                        None => ()
                                    }
                                }
                                
                                // if self.is_node_registered(guid){
                                //     continue;
                                // }
                                let node_address = s[1].to_string();
                                let api_address = s[2].to_string();

                                if guid != my_guid {
                                    info!("added {} to the rts with guid {}", node_address, guid);

                                    let mut info = info.lock().unwrap();
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
                        _ => ()
                    }

                    trace!("join network done.");
                },
                Err(_) => {
                }
            }
        }



        // match &s[1] {
        //     &"guid" => {
        //         
        // 
        //     },
        //     &"rt" => {
        // 
        //         
        // 
        // 
        // 
        //     },
        //     x => {
        //         warn!("unknown cmd: {}", x)
        //     }
        // }

    }

    fn setup_network_keeper(info:Arc<Mutex<cluster::Info>>){

        let info = info.clone();

        thread::spawn(move || {

            loop {

                debug!("checking network health...");

                {
                    let mut to_remove:Vec<u32> = Vec::new();
                    let mut routing_tables:Vec<RoutingTable>;
                    let mut my_guid:u32;

                    {
                        trace!("acquire lock for `info` in check network health");
                        let mut info = info.lock().unwrap();
                        trace!("acquire lock for `info` in check network health ---> acquired.");
                        my_guid = info.my_guid;
                        routing_tables = (&info.routing_tables).clone();
                    }

                    for rt in &routing_tables {
                        debug!("   -> {}", rt.node_address());

                        let addr:SocketAddr = rt.node_address().parse().unwrap();
                        match TcpStream::connect(addr){
                            Ok(ref mut stream) => {
                                let _ = stream.write(format!("ping|{}", my_guid).as_bytes());
                                let mut buff = vec![0u8; 100];
                                match stream.read(&mut buff) {
                                    Ok(count) if count > 0 => {
                                        // let data = String::from_utf8(buff).ok().unwrap();
                                        // debug!("got {}", data);
                                        ()
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


                    let mut count = 0;
                    for guid in to_remove {

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
                }


                debug!("health checking done.");

                thread::sleep_ms(25000);

            }
        });
    }

    fn write(&self, stream: &mut TcpStream, data: &str){
        let _ = stream.write(self.the_encd.encode(&data.as_bytes()).ok().unwrap().as_ref());
    }

    fn process_cmd(&self, cmd: &str, params:&Vec<&str>, stream: &mut TcpStream) -> ZResult {

        match cmd {
            "status" => { // [VERSION]|status|[RESERVED]
                trace!("status cmd recvd.");

                //@TODO(robin): FIXXMEE
                let _ = stream.write(b"Datacenter: DC1\r\n");
                let _ = stream.write(b"===============\r\n");
                let _ = stream.write(b"Status=Up/Down\r\n");
                let _ = stream.write(b"|/ State=Normal/Leaving/Joining/Moving\r\n");
                let _ = stream.write(b"--  Address                Load        GUID                           Rack\r\n");

                trace!("acquire `info` lock for getting status");
                let info = self.info.lock().expect("cannot acquire lock for info");
                trace!("acquire `info` lock for getting status --> acquired.");

                // me first
                // trace!("send tx");
                // self.tx.send("info".to_string()).expect("cannot send tx to info");
                // trace!("recv rx");
                // let data = match self.rx.try_recv(){
                //     Ok(d) => d,
                //     Err(e) => {
                //         error!("cannot recv data from channel, {}", e);
                //         return Err("error");
                //     }
                // };
                // trace!("received: {}", data);
                // 
                // let s:Vec<&str> = data.split("|").collect();
                // 
                // let mem_load:usize = s[0].parse().expect("cannot get memory load");
                // let disk_load:usize = s[1].parse().expect("cannot get disk load");
                
                let stat = {
                    let db = self.db.lock().expect("cannot acquire lock for `db` in get status");
                    &db.stat()
                };

                let data = format!("UN  {}         {}/{}         {}                                1\r\n",
                    info.my_node_address, stat.mem_load(), stat.disk_load(), self.my_guid);
                let _ = stream.write(data.as_bytes());

                let rts = &info.routing_tables;

                for rt in rts {

                    let mut node = NodeClient::new(rt.node_address());

                    let (mem_load, disk_load) = match node.info(){
                        Some(n_info) => {
                            (format!("{}", n_info.mem_load()), format!("{}", n_info.disk_load()))
                        },
                        None => {
                            ("?".to_string(), "?".to_string())
                        }
                    };

                    let data = format!("UN  {}         {}/{}         {}                                1\r\n",
                        rt.node_address(), mem_load, disk_load, rt.guid());

                    let _ = stream.write(data.as_bytes());
                }

                Ok(1)

            },
            "info" => {

                // trace!("send `info` via tx");
                // self.tx.send("info".to_string()).unwrap();
                // trace!("recv `info` via rx");
                // let data = self.rx.recv().unwrap();
                // trace!("got data from `info` via rx");

                // let s:Vec<&str> = data.split("|").collect();
                // 
                // let load:usize = s[0].parse().unwrap();
                // let disk_load:usize = s[1].parse().unwrap();
                
                let stat = {
                    let db = self.db.lock().expect("cannot acquire lock for `db` in get info");
                    &db.stat()
                };
                
                
                let data = format!("v1|info|{}|{}", stat.mem_load(), stat.disk_load());

                let _ = stream.write(data.as_bytes());

                Ok(0)
            },
            "join" => { // [VERSION]|join|[NODE-ADDRESS]|[API-ADDRESS]
                trace!("join cmd recvd.");

                // check parameters
                if params.len() != 4 {
                    return Err("Invalid parameter, should be 3");
                }

                let peer_addr = params[2];

                debug!("peer addr: {}", peer_addr);

                let my_api_address = {
                    let info = self.info.lock().unwrap();
                    info.my_api_address.clone()
                };


                match self.get_rt_by_node_address(peer_addr){
                    Some(ref rt) => {
                        warn!("ip {} already joined as guid {}", rt.node_address(), rt.guid());

                        // just send existing guid
                        let data = &format!("v1|guid|{}|{}|{}\n", rt.guid(), self.my_guid, my_api_address);
                        //stream.write(&*self.the_encd.encode(data).ok().unwrap());
                        self.write(stream, data);

                        return Ok(0);
                    },
                    _ => ()
                }

                let guid = self.generate_guid();

                debug!("generated guid: {}", guid);

                let data = &format!("v1|guid|{}|{}|{}\n", guid, self.my_guid, my_api_address);

                let api_address = params[3];

                {
                    let mut info = self.info.lock().unwrap();
                    let mut rts = &mut info.routing_tables;
                    rts.push(RoutingTable::new(guid, peer_addr.to_string(), api_address.to_string()));
                    info!("rts count now: {}", rts.len());
                }

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

                trace!("copy routing tables...");

                {
                    let mut info = self.info.lock().unwrap();
                    let rts = &mut info.routing_tables;

                    //println!("routing tables: {:?}", rts);

                    let rtb:Vec<String> = rts.iter()
                        .map(|rt| format!("{},{},{}", rt.guid(), rt.node_address(), rt.api_address()))
                        .collect();
                    let data = format!("v1|rt|{}", rtb.join("|"));

                    info!("routing tables: ");
                    for rt in rts {
                        info!("    + node-{} - {} - {}", rt.guid(), rt.node_address(), rt.api_address());
                    }

                    self.write(stream, &data);
                }

                Ok(0)
            },
            "add-me" => {

                // [VERSION]|add-me|[GUID]|[NODE-ADDRESS]|[API-ADDRESS]

                let params = &params[2..];

                let guid:u32 = params[0].parse().unwrap();

                // check is already exists?
                if !self.is_node_registered(guid){


                    let mut info = self.info.lock().unwrap();
                    let mut rts = &mut info.routing_tables;

                    let node_address = params[1];
                    let api_address = params[2];

                    rts.push(RoutingTable::new(guid, node_address.to_string(), api_address.to_string()));

                    info!("node {} added into rts by request", node_address);
                    info!("rts count now: {}", rts.len());

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

        trace!("acquire info lock for get_rt()");
        let info = self.info.lock().unwrap();
        let rts = &info.routing_tables;
        match rts.iter().find(|r| *r.node_address() == socket_ip_address){
            Some(rt) => Some(rt.clone()),
            None => None
        }
    }

    fn get_rt_by_node_address(&self, node_address: &str) -> Option<RoutingTable> {
        trace!("acquire info lock for get_rt_by_node_address()");
        let info = self.info.lock().unwrap();
        let rts = &info.routing_tables;
        match rts.iter().find(|r| *r.node_address() == node_address.to_string()){
            Some(rt) => Some(rt.clone()),
            None => None
        }
    }

    pub fn get_rt_by_guid(&self, guid: u32) -> Option<RoutingTable> {
        trace!("acquire info lock for get_rt_by_guid()");
        let info = self.info.lock().unwrap();
        let rts = &info.routing_tables;

        match rts.iter().find(|&r| r.guid() == guid){
            Some(rt) => Some(rt.clone()),
            None => None
        }
    }

    fn node_index(&self, id:u32) -> i32 {
        trace!("acquire info lock for node_index()");
        let info = self.info.lock().unwrap();
        trace!("acquire info lock for node_index() --> acquired");
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
