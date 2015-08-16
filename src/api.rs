use std::net::{TcpStream, SocketAddr};
use std::io::prelude::*;
use db::Db;
use time;
//use std::sync::mpsc::Receiver;
use crc32::Crc32;
use std::thread;
use std::sync::{Arc, Mutex};
use std::cell::RefCell;

use internode::{MeState, InternodeService};
use dbclient::DbClient;
use std::net::TcpListener;
use cluster;
use cluster::RoutingTable;


static END:&'static [u8] = b"END\n";

pub struct ApiService {
    db: Db,
    //rx: Receiver<u32>,
    //rts_count: usize,
    pub me_state: RefCell<Option<MeState>>,
    crc32: Crc32,
    inode: Arc<Mutex<InternodeService>>,
    info: Arc<Mutex<cluster::Info>>
}

impl ApiService {
    pub fn new(inode:Arc<Mutex<InternodeService>>, info:Arc<Mutex<cluster::Info>>) -> ApiService {        
        ApiService {
            db: Db::new(),
            //rx: rx,
            //rts_count: 0,
            me_state: RefCell::new(None),
            crc32: Crc32::new(),
            inode: inode,
            info: info
        }
    }
    
    pub fn start(self, api_address:&String){
        
        let api_service = Arc::new(Mutex::new(self));

        let listener = TcpListener::bind(&**api_address).unwrap();
        println!("client comm listening at {} ...", api_address);
        for stream in listener.incoming() {

            let api_service = api_service.clone();

            thread::spawn(move || {
                let mut stream = stream.unwrap();
                stream.write(b"Welcome to Zufar\r\n").unwrap();

                'the_loop: loop {
                    let mut buff = vec![0u8; 100];
                    match stream.read(&mut buff){
                        Ok(count) if count > 0 => {
                            let data = &buff[0..count];
                            let mut api_service = api_service.lock().unwrap();
                            match api_service.handle_packet(&mut stream, data){
                                Ok(i) if i > 0 =>
                                    break 'the_loop
                                ,
                                _ => ()
                            }
                        },
                        Err(e) => panic!("error when reading. {}", e),
                        _ => ()
                    }
                }

            });
        }
        
    //         thread::spawn(move || {
    //             loop {
    //                 // let mut dbi = _db_iface.lock().unwrap();
    // 
    //                 match rx.recv(){
    //                     Ok(me_state) => {
    //                         let mut dbi = _db_iface.lock().unwrap();
    // 
    //                         let mut c = dbi.me_state.borrow_mut();
    //                         *c = Some(me_state);
    // 
    //                         //dbi.set_rts_count(me_state.rts_count);
    // 
    //                         //let mut rts_count = _rts_count.lock().unwrap();
    //                         //*rts_count = count;
    //                         debug!("rts_count updated via rx: {}", c.as_ref().unwrap().rts_count);
    //                     },
    //                     _ => ()
    //                 };
    //                 debug!("recv..");
    //                 //thread::sleep_ms(100);
    //             }
    //         });
    }
    
    pub fn handle_packet(&mut self, stream: &mut TcpStream, data: &[u8]) -> Result<u16, &'static str> {

        let d = String::from_utf8(data.to_vec()).ok().unwrap();
        let s:Vec<&str> = d.trim().split(" ").collect();

        debug!("splited s: {:?}", s);

        if s.len() == 1 && s[0] == "" {
            return Ok(0);
        }
        
        // let c = self.me_state.borrow();
        // let c = c.as_ref().unwrap();
        // 
        // let my_guid = c.my_guid;
        // let rts_count = c.rts_count;
        let (my_guid, rts_count) = {
            let info = self.info.lock().unwrap();
            //let inode = inode.lock().unwrap();
            let rts = &info.routing_tables;
            (info.my_guid, rts.len())
        };
        
        
        trace!("rts_count: {}", rts_count);

        match &s[0] {
            &"set" => {

                if s.len() != 5 {
                    return Err("`set` parameters must be 5");
                }

                let k = s[1];
                let metadata = s[2];
                let expiration:u32 = s[3].parse().unwrap();
                let length:usize = s[4].parse().unwrap();

                let _ = stream.write(b">\n");

                let mut buff = vec![0u8; length];
                match stream.read(&mut buff){
                    Ok(count) if count > 0 => {
                        let data_str = String::from_utf8(buff[0..count].to_vec()).unwrap();
                        let now = time::now();
                        let ts = now.to_timespec().sec;
                        
                        
                        // calculate route
                        let target_node_id = self.calculate_route(k, rts_count);
                        
                        debug!("key {} target_node_id: {}", k, target_node_id);
                        
                        if target_node_id == my_guid {
                            
                            trace!("insert to myself");
                            
                            let data = format!("{}:{}:{}:{}|{}", length, metadata, expiration, ts, data_str);
                            debug!("data to store: k: {}, v: {:?}", k, data);
                            
                            self.db.insert(k.as_bytes(), data.as_bytes());
                        }else{
                            // on other node
                            
                            trace!("insert to other node with guid {}", target_node_id);
                            
                            //let inode = self.inode.lock().unwrap();
                            let rt = self.get_rt_by_guid(target_node_id).unwrap();
                            let mut dbc = DbClient::new(&rt.api_address());
                            dbc.connect();
                            dbc.set(k, &*data_str);
                        }
                        
                        
                        let _ = stream.write(b"STORED\n");
                    },
                    _ => ()
                }

                Ok(0)
            },
            &"get" => {
                let k = s[1];
                
                // calculate route
                let source_node_id = self.calculate_route(k, rts_count);
                
                debug!("key {} source_node_id: {}", k, source_node_id);
                
                if source_node_id == my_guid {
                    trace!("get from myself");
                    
                    match self.db.get(k.as_bytes()){
                        Some(v) => {

                            let s = String::from_utf8(v.to_vec()).unwrap();
                            let s:Vec<&str> = s.split("|").collect();
                            let meta_s:Vec<&str> = s[0].split(":").collect();
                            let length = meta_s[0];
                            let metadata = meta_s[1];
                            //let expiration = meta_s[2];
                            let content = s[1];

                            let data = format!("VALUE {} {} {}\n{}\nEND\n", k, metadata, length, content);
                            let _ = stream.write(data.as_bytes());
                        },
                        _ => {
                            let _ = stream.write(END);
                        }
                    }
                    
                }else{
                    trace!("get from other node-{}", source_node_id);
                    
                    match self.get_rt_by_guid(source_node_id){
                        Some(rt) => {
                            let mut dbc = DbClient::new(&rt.api_address());
                            dbc.connect();
                            match dbc.get_raw(k){
                                Some(result) => {
                                    let _ = stream.write(result.as_bytes());
                                    let _ = stream.flush();
                                },
                                None => {
                                    warn!("cannot get data from node-{}", source_node_id);
                                    let _ = stream.write(END);
                                }
                            }
                        },
                        None => {
                            let err_str = format!("cannot contact node-{}", source_node_id);
                            error!("{}", err_str);
                            //let _ = stream.write(END);
                            let _ = stream.write(format!("SERVER_ERROR {}\r\n", err_str).as_bytes());
                        }
                    }
                    
                }

                

                Ok(0)
            },
            _ => Ok(1)
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
    
    ///
    /// Calculate route node id based on modulo of CRC32 hash of the key
    ///
    fn calculate_route(&mut self, key:&str, rts_count:usize) -> u32 {
        if rts_count > 0 {
            ((self.crc32.crc(key.as_bytes()) as usize) % (rts_count + 1)) as u32
        }else{
            0u32
        }
    }
}


