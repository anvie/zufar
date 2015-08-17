use std::net::{TcpStream, SocketAddr};
use std::io::prelude::*;
use db::Db;
use time;
//use std::sync::mpsc::Receiver;
use crc32::Crc32;
use std::thread;
use std::sync::{Arc, Mutex};
use std::cell::RefCell;
use std::net::Shutdown;
use std::collections::HashMap;
use std::net::TcpListener;

//use nix::sys::signal;


use internode::{MeState, InternodeService};
use dbclient::DbClient;
use dbclient::{RetryPolicy, BackoffRetryPolicy};
use cluster;
use cluster::RoutingTable;


static END:&'static [u8] = b"END\r\n";
static ERROR:&'static [u8] = b"ERROR\r\n";



pub struct ApiService {
    db: Db,
    //rx: Receiver<u32>,
    //rts_count: usize,
    pub me_state: RefCell<Option<MeState>>,
    crc32: Crc32,
    inode: Arc<Mutex<InternodeService>>,
    info: Arc<Mutex<cluster::Info>>,
    db_client_cache: HashMap<u32, DbClient<BackoffRetryPolicy>>
}

// static mut global_db:Option<Db> = None;

impl ApiService {
    pub fn new(inode:Arc<Mutex<InternodeService>>, info:Arc<Mutex<cluster::Info>>) -> ApiService {        
        
        
        //let db = Db::new();

        // global_db = Some(db);
        
        let data_dir = {
            let info = info.clone();
            let info = info.lock().unwrap();
            info.data_dir.clone()
        };

        ApiService {
            db: Db::new(&data_dir),
            //rx: rx,
            //rts_count: 0,
            me_state: RefCell::new(None),
            crc32: Crc32::new(),
            inode: inode,
            info: info,
            db_client_cache: HashMap::new()
        }
    }
    
    pub fn start(self, api_address:&String){
        
        let api_service = Arc::new(Mutex::new(self));
        
        {
            let api_service = api_service.clone();
            thread::spawn(move || {
                loop {
                    thread::sleep_ms(10000);
                    {
                        let mut api_service = api_service.lock().unwrap();
                        debug!("flushing...");
                        api_service.db.flush();
                    }
                }
            });
        }
        
        

        let listener = TcpListener::bind(&**api_address).unwrap();
        println!("client comm listening at {} ...", api_address);
        for stream in listener.incoming() {

            let api_service = api_service.clone();

            thread::spawn(move || {
                let mut stream = stream.unwrap();
                //stream.write(b"Welcome to Zufar\r\n").unwrap();

                'the_loop: loop {
                    let mut buff = vec![0u8; 512];
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
        
        
        //@TODO(robin): check this, not working??? Should be working with ^C
        {
            info!("flushing...");
            let api_service = api_service.clone();
            let mut api_service = api_service.lock().unwrap();
            api_service.flush();
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

                let len = s.len();
                
                if len < 5 {
                    return Err("`set` parameters must be greater than 5");
                }
                
                let mut data_str = 
                if len > 5 {
                    s[5..].join(" ").trim().to_string()
                }else{
                    "".to_string()
                };

                let key = s[1];
                let metadata = s[2];
                let expiration:u32 = s[3].parse().expect("invalid expiration format");
                let length:usize = s[4].parse().expect("invalid length format");

                //let _ = stream.write(b">\n");

                if data_str.len() == 0 {
                    let mut buff = vec![0u8; length];
                    match stream.read(&mut buff){
                        Ok(count) if count > 0 => {
                            data_str = String::from_utf8(buff[0..count].to_vec()).unwrap();
                        },
                        _ => ()
                    }
                }
                
                
                if data_str.len() > 0 {
                    let now = time::now();
                    let ts = now.to_timespec().sec;
                    
                    // calculate route
                    let target_node_id = self.calculate_route(key, rts_count);
                    
                    debug!("key {} target_node_id: {}", key, target_node_id);
                    
                    if target_node_id == my_guid {
                        
                        trace!("insert to myself");
                        
                        let data = format!("{}:{}:{}:{}|{}", length, metadata, expiration, ts, data_str);
                        debug!("data to store: k: `{}`, v: `{:?}`", key, data);
                        
                        self.db.insert(key.as_bytes(), data.as_bytes());
                        
                        let _ = stream.write(b"STORED\r\n");
                    }else{
                        // on other node
                        
                        trace!("insert to other node with guid {}", target_node_id);
                        
                        //let inode = self.inode.lock().unwrap();
                        let rt = self.get_rt_by_guid(target_node_id).unwrap();
                        //let mut dbc = DbClient::new(&rt.api_address());
                        match self.get_db_client(target_node_id, rt.api_address()){
                            Some(dbc) => {
                                dbc.set(key, &*data_str);
                                let _ = stream.write(b"STORED\r\n");
                            },
                            None => {
                                error!("cannot get db_client with guid {} - {}", target_node_id, rt.api_address());
                                let _ = stream.write(ERROR);
                            }
                        }
                        //dbc.connect();
                        
                    }
                    
                    
                    
                }

                Ok(0)
            },
            &"get" => {
                
                if s.len() != 2 {
                    return Err("bad parameter length");
                }
                
                let k = s[1];
                
                self.op_get(k, stream, my_guid, rts_count);

                Ok(0)
            },
            &"getd" => {
                
                if s.len() != 2 {
                    return Err("bad parameter length");
                }
                
                let key = s[1];

                let ts = time::now().to_timespec();

                let ms1 = (ts.sec as f32 * 1000.0f32) + (ts.nsec as f32 / 1_000_000f32) as f32;
                
                self.op_get(key, stream, my_guid, rts_count);
                
                let ts2 = time::now().to_timespec();
                let ms2 = (ts2.sec as f32 * 1000.0f32) + (ts2.nsec as f32 / 1_000_000 as f32) as f32;
                
                let ms = (ms2 as f32 - ms1 as f32) as f32;
                
                let target_node_id = self.calculate_route(key, rts_count); 
                
                let _ = stream.write(format!("from node-{}\r\n", target_node_id).as_bytes());
                let _ = stream.write(format!("in {}ms\r\n", ms).as_bytes());
                info!("get record done in {}ms", ms);

                Ok(0)
            },
            &"delete" | &"deleted" | &"del" | &"deld" => {
                
                if s.len() != 2 {
                    return Err("bad parameter length");
                }
                
                let key = s[1];

                let trace = s[0] == "deleted" || s[0] == "deld";
                
                if trace {
                    let ts = time::now().to_timespec().nsec;
                    self.op_del(key, stream, my_guid, rts_count);
                    let ts = (time::now().to_timespec().nsec - ts) as f32 / 1_000_000f32;

                    stream.write(format!("in {}ms\r\n", (ts as f32 * 0.100f32)).as_bytes());
                    info!("delete record done in {}ms", ts);
                }else{
                    self.op_del(key, stream, my_guid, rts_count);
                }
                
                
                Ok(0)
            }
            _ => Ok(1)
        }
    }
    
    fn get_db_client<'a>(&'a mut self, node_id:u32, address:&String) -> Option<&'a mut DbClient<BackoffRetryPolicy>> {
        if self.db_client_cache.contains_key(&node_id) {
            trace!("get db client for {} from cache", address);
            self.db_client_cache.get_mut(&node_id)
        }else{
            trace!("get db client for {} miss from cache, build it.", address);
            {
                let rp = BackoffRetryPolicy::new();
                let dbc = DbClient::new(&address, rp);
                dbc.connect();
                self.db_client_cache.insert(node_id, dbc);
            }
            self.db_client_cache.get_mut(&node_id)
        }
    }
    
    fn op_del(&mut self, key:&str, stream:&mut TcpStream, my_guid:u32, rts_count:usize){
        // calculate route
        let target_node_id = self.calculate_route(key, rts_count); 
        
        debug!("key {} target_node_id: {}", key, target_node_id);
        
        if target_node_id == my_guid {
            trace!("del from myself");
            
            if self.db.del(key.as_bytes()) > 0 {
                let _ = stream.write(b"DELETED\r\n");
            }else{
                let _ = stream.write(b"NOT_FOUND\r\n");
            }
            
        }else{
            trace!("del in other node with guid {}", target_node_id);
            
            match self.get_rt_by_guid(target_node_id){
                Some(rt) => {
                    
                    trace!("trying to delete data from: {:?}", rt);
                    
                    let mut dbc = DbClient::new(&rt.api_address(), BackoffRetryPolicy::new());
                    match dbc.connect(){
                        Err(_) => panic!("cannot contact node-{}", target_node_id),
                        _ => (),
                    }
                    match dbc.del(key){
                        Ok(result) => {
                            let _ = stream.write(result.as_bytes());
                            let _ = stream.flush();
                        },
                        Err(e) => {
                            warn!("cannot delete data from node-{}. {}", target_node_id, e);
                            let _ = stream.write(ERROR);
                        }
                    }
                },
                None => {
                    let err_str = format!("cannot contact node-{}", target_node_id);
                    error!("{}", err_str);
                    //let _ = stream.write(END);
                    let _ = stream.write(format!("SERVER_ERROR {}\r\n", err_str).as_bytes());
                }
            }
        }
        
    }
    
    fn op_get(&mut self, key:&str, stream:&mut TcpStream, my_guid:u32, rts_count:usize){
        // calculate route
        let source_node_id = self.calculate_route(key, rts_count);
        
        debug!("key {} source_node_id: {}", key, source_node_id);
        
        if source_node_id == my_guid {
            trace!("get from myself");
            
            match self.db.get(key.as_bytes()){
                Some(v) => {

                    let s = String::from_utf8(v.to_vec()).unwrap();
                    let s:Vec<&str> = s.split("|").collect();
                    let meta_s:Vec<&str> = s[0].split(":").collect();
                    let length = meta_s[0];
                    let metadata = meta_s[1];
                    //let expiration = meta_s[2];
                    let content = s[1];

                    let data = format!("VALUE {} {} {} \r\n{}\r\nEND\r\n", key, metadata, length, content);
                    
                    trace!("data: {}", data);
                    
                    let _ = stream.write(data.as_bytes());
                },
                _ => {
                    trace!("get -> None");
                    let _ = stream.write(END);
                }
            }
            
        }else{
            trace!("get from other node with guid {}", source_node_id);
            

            match self.get_rt_by_guid(source_node_id){
                Some(rt) => {
                    
                    match self.get_db_client(source_node_id, rt.api_address()){
                        Some(dbc) => {
                            match dbc.get_raw(key){
                                Ok(result) => {
                                    let _ = stream.write(result.as_bytes());
                                    //let _ = stream.flush();
                                },
                                Err(e) => {
                                    warn!("cannot get data from node-{}. {}", source_node_id, e);
                                    let _ = stream.write(END);
                                }
                            }
                        },
                        None => {
                            error!("cannot get db_client with guid {} - {}", source_node_id, rt.api_address());
                            let _ = stream.write(ERROR);
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
    }
    
    pub fn get_rt_by_guid(&self, guid: u32) -> Option<RoutingTable> {
        trace!("get rt");
        let info = self.info.lock().unwrap();
        let rts = &info.routing_tables;
        trace!("after get rt");
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
    
    pub fn flush(&mut self){
        self.db.flush();
    }
}


