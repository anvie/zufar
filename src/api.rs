use std::net::TcpStream;
use std::io::prelude::*;
use db::Db;
use time;
//use std::sync::mpsc::Receiver;
use crc32::Crc32;
use std::thread;
use std::sync::{Arc, Mutex};
//use std::cell::RefCell;
//use std::net::Shutdown;
use std::collections::HashMap;
use std::net::TcpListener;
use std::sync::mpsc::{Receiver, Sender};
//use nix::sys::signal;


use internode::InternodeService;
use dbclient::DbClient;
use dbclient::{BackoffRetryPolicy, RetryPolicyType};
use cluster;
use cluster::RoutingTable;


static END:&'static [u8] = b"END\r\n";
static ERROR:&'static [u8] = b"ERROR\r\n";



pub struct ApiService {
    db: Arc<Mutex<Db>>,
    //rx: Receiver<u32>,
    //rts_count: usize,
    //pub me_state: RefCell<Option<MeState>>,
    crc32: Crc32,
    // inode: Arc<Mutex<InternodeService>>,
    info: Arc<Mutex<cluster::Info>>,
    db_client_cache: HashMap<u32, DbClient>,
    _rps: usize,
    _last_op: i64
}

// static mut global_db:Option<Db> = None;

type ApiResult = Result<u16, &'static str>;

macro_rules! speed_track {
    ($myself:ident) => {
        {
            let ts = time::now().to_timespec();

            if $myself._last_op == ts.sec {
                $myself._rps = $myself._rps + 1;
            }else{
                if $myself._rps > 1 {
                    $myself._rps = $myself._rps - ($myself._rps / 2);
                }
            }

            trace!("  last op: {}, ts.sec: {}, rps: {}", $myself._last_op, ts.sec, $myself._rps);

            $myself._last_op = ts.sec;
        }
    }
}

macro_rules! op_timing {
    ($op_str:expr, $op:expr, $target_node_id:expr, $stream:ident) => {
        {
            let ts = time::now().to_timespec();

            let ms1 = (ts.sec as f32 * 1000.0f32) + (ts.nsec as f32 / 1_000_000f32) as f32;

            let result = $op;

            let ts2 = time::now().to_timespec();
            let ms2 = (ts2.sec as f32 * 1000.0f32) + (ts2.nsec as f32 / 1_000_000 as f32) as f32;

            let ms = (ms2 as f32 - ms1 as f32) as f32;

            let _ = $stream.write(format!("node-{} ", $target_node_id).as_bytes());
            let _ = $stream.write(format!("in {}ms\r\n", ms).as_bytes());
            info!("{} record done in {}ms", $op_str, ms);

            if result.is_err(){
                error!("{}", result.unwrap_err());
            }

            result
        }
    }
}

impl ApiService {

    pub fn new(info:Arc<Mutex<cluster::Info>>,
            db:Arc<Mutex<Db>>) -> ApiService {
        
        let data_dir = {
            let info = info.clone();
            let info = info.lock().unwrap();
            info.data_dir.clone()
        };

        ApiService {
            db: db,
            crc32: Crc32::new(),
            //inode: inode,
            info: info,
            db_client_cache: HashMap::new(),
            _rps: 1,
            _last_op: 0
        }
    }

    pub fn start(self, api_address:&String, tx:Sender<String>, rx:Receiver<String>){

        let api_service = Arc::new(Mutex::new(self));

        {
            let api_service = api_service.clone();
            thread::spawn(move || {
                loop {
                    let mut rps:usize = 1;
                    thread::sleep_ms(10000 * (rps as u32));
                    {
                        trace!("try to acquire lock for `api_service` in flusher");
                        let mut api_service = api_service.lock().unwrap();
                        trace!("try to acquire lock for `api_service` in flusher --> acquired.");
                        rps = api_service._rps;
                        debug!("   rps: {}", rps);
                        debug!("flushing...");
                        api_service.db.flush();
                    }
                }
            });
        }

        {
            let api_service = api_service.clone();

            thread::spawn(move || {
                loop {
                    {
                        //let mut _self = api_service.lock().unwrap();
                        match rx.recv(){
                            Ok(data) => {
                                match &*data {
                                    "info" => {
                                        trace!("try to acquire lock for `api_service` in rx comm");
                                        let mut _self = api_service.lock().unwrap();
                                        trace!("try to acquire lock for `api_service` in rx comm --> acquired");
                                        let stat = _self.db.stat();
                                        tx.send(format!("{}|{}", stat.mem_load(), stat.disk_load())).unwrap();
                                    },
                                    _ => ()
                                }
                            },
                            _ => ()
                        }
                    }

                    //thread::sleep_ms(100);
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
                    let mut buff = vec![0u8; 4096 + 512];
                    match stream.read(&mut buff){
                        Ok(count) if count > 0 => {
                            let data = &buff[0..count];
                            trace!("try to acquire lock for `api_service` in main_loop");
                            let mut api_service = api_service.lock().unwrap();
                            trace!("try to acquire lock for `api_service` in main_loop --> acquired.");
                            match api_service.handle_packet(&mut stream, data){
                                Ok(i) if i > 0 =>
                                    break 'the_loop
                                ,
                                _ => ()
                            }
                        },
                        Err(e) => {
                            warn!("error when reading. {}", e);
                            break 'the_loop;
                        },
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

    }

    pub fn handle_packet(&mut self, stream: &mut TcpStream, data: &[u8]) -> ApiResult {

        let d = String::from_utf8(data.to_vec()).ok()
            .expect("cannot encode data to utf8");
        let s:Vec<&str> = d.trim().split(" ").collect();

        debug!("splited s: {:?}", s);

        if s.len() == 1 && s[0] == "" {
            return Ok(0);
        }

        let (my_guid, rts_count) = {
            trace!("trying to acquire lock for `info` in handle_packet");
            let info = self.info.lock().expect("cannot acquire lock for info");
            trace!("trying to acquire lock for `info` in handle_packet --> acquired.");
            //let inode = inode.lock().unwrap();
            let rts = &info.routing_tables;
            (info.my_guid, rts.len())
        };


        trace!("rts_count: {}", rts_count);

        match &s[0] {
            &"set" | &"setd" => {

                speed_track!(self);

                let trace = s[0] == "setd";

                if trace {

                    let key = s[1];
                    let target_node_id = self.calculate_route(key, rts_count);

                    op_timing!("set", self.op_set(&s, stream, my_guid, rts_count), target_node_id, stream)

                    // let ts = time::now().to_timespec();
                    //
                    // let ms1 = (ts.sec as f32 * 1000.0f32) + (ts.nsec as f32 / 1_000_000f32) as f32;
                    //
                    // let result = self.op_set(&s, stream, my_guid, rts_count);
                    //
                    // let ts2 = time::now().to_timespec();
                    // let ms2 = (ts2.sec as f32 * 1000.0f32) + (ts2.nsec as f32 / 1_000_000 as f32) as f32;
                    //
                    // let ms = (ms2 as f32 - ms1 as f32) as f32;
                    //
                    // let key = s[1];
                    // let target_node_id = self.calculate_route(key, rts_count);
                    //
                    // let _ = stream.write(format!("to node-{} ", target_node_id).as_bytes());
                    // let _ = stream.write(format!("in {}ms\r\n", ms).as_bytes());
                    // info!("set record done in {}ms", ms);

                    // result
                }else{
                    self.op_set(&s, stream, my_guid, rts_count)
                }
            },
            // &"get" => {
            //
            //     if s.len() != 2 {
            //         warn!("bad parameter length");
            //         let _ = stream.write(END);
            //         return Err("bad parameter length");
            //     }
            //
            //     let k = s[1];
            //
            //     self.op_get(k, stream, my_guid, rts_count);
            //
            //     Ok(0)
            // },
            &"get" | &"getd" => {


                speed_track!(self);

                if s.len() != 2 {
                    warn!("bad parameter length");
                    let _ = stream.write(END);
                    return Err("bad parameter length");
                }

                let key = s[1];

                // let ts = time::now().to_timespec();
                //
                // let ms1 = (ts.sec as f32 * 1000.0f32) + (ts.nsec as f32 / 1_000_000f32) as f32;
                //
                // self.op_get(key, stream, my_guid, rts_count);
                //
                // let ts2 = time::now().to_timespec();
                // let ms2 = (ts2.sec as f32 * 1000.0f32) + (ts2.nsec as f32 / 1_000_000 as f32) as f32;
                //
                // let ms = (ms2 as f32 - ms1 as f32) as f32;
                //
                let target_node_id = self.calculate_route(key, rts_count);
                //
                // let _ = stream.write(format!("from node-{}\r\n", target_node_id).as_bytes());
                // let _ = stream.write(format!("in {}ms\r\n", ms).as_bytes());
                // info!("get record done in {}ms", ms);
                //
                // Ok(0)

                let trace = s[0] == "getd";

                if trace {
                    op_timing!("get", self.op_get(key, stream, my_guid, rts_count), target_node_id, stream)
                }else{
                    self.op_get(key, stream, my_guid, rts_count)
                }

            },
            &"delete" | &"deleted" | &"del" | &"deld" => {

                speed_track!(self);

                if s.len() != 2 {
                    return Err("bad parameter length");
                }

                let key = s[1];

                let trace = s[0] == "deleted" || s[0] == "deld";

                if trace {
                    let ts = time::now().to_timespec().nsec;
                    self.op_del(key, stream, my_guid, rts_count);
                    let ts = (time::now().to_timespec().nsec - ts) as f32 / 1_000_000f32;

                    let _ = stream.write(format!("in {}ms\r\n", (ts as f32 * 0.100f32)).as_bytes());
                    info!("delete record done in {}ms", ts);
                }else{
                    self.op_del(key, stream, my_guid, rts_count);
                }


                Ok(0)
            }
            _ => Ok(1)
        }
    }

    fn get_db_client<'a>(&'a mut self, node_id:u32, address:&String) -> Option<&'a mut DbClient> {
        if self.db_client_cache.contains_key(&node_id) {
            trace!("get db client for {} from cache", address);
            self.db_client_cache.get_mut(&node_id)
        }else{
            trace!("get db client for {} miss from cache, build it.", address);
            {
                //let rp = BackoffRetryPolicy::new();
                let dbc = DbClient::new(&address, RetryPolicyType::Backoff);
                match dbc.connect(){
                    Ok(_) => (),
                    Err(e) => error!("cannot connect to remote node {}, {}", address, e)
                }
                self.db_client_cache.insert(node_id, dbc);
            }
            self.db_client_cache.get_mut(&node_id)
        }
    }


    fn op_set(&mut self, s:&Vec<&str>, stream:&mut TcpStream, my_guid:u32, rts_count:usize) -> ApiResult {

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
        let length:usize = {
            if len == 5 {
                let s:Vec<&str> = s[4].split("\r\n").collect();
                if s.len() > 1 {
                    data_str = s[1].to_string();
                    s[0].parse().expect("invalid length format (1)")
                }else{
                    s[0].parse().expect("invalid length format (2)")
                }
            }else{
                s[4].parse().expect("invalid length format (3)")
            }
        };

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

        trace!("data_str: {}", data_str);


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
            }
        }else{
            return Err("data length is zero");
        }

        Ok(0)
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

                    let mut dbc = DbClient::new(&rt.api_address(), RetryPolicyType::Backoff);
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

    fn op_get(&mut self, key:&str, stream:&mut TcpStream, my_guid:u32, rts_count:usize) -> ApiResult {
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
                            match dbc.get_raw(key, &mut BackoffRetryPolicy::new()){
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

        Ok(0)
    }

    pub fn get_rt_by_guid(&self, guid: u32) -> Option<RoutingTable> {
        trace!("trying to acquire lock for `info` in get_rt_by_guid in api.");
        let info = self.info.lock().unwrap();
        trace!("trying to acquire lock for `info` in get_rt_by_guid in api --> acquired.");
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

    pub fn flush(&mut self){
        self.db.flush();
    }
}
