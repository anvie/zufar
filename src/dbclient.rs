

use std::time::Duration;
//use std;
//use std::thread;
//use std::net::TcpListener;
use std::net::{TcpStream, SocketAddr};
use std::io::prelude::*;
//use std::str;
use std::cell::RefCell;
//use std::io::BufReader;
use std::error::Error;
use std::net::Shutdown;

type DbcResult = Result<String,&'static str>;

#[derive(Debug)]
pub struct DbClient {
    address: String,
    stream: RefCell<Option<TcpStream>>
}

impl DbClient {
    
    pub fn new(address:&String) -> DbClient {
        DbClient {
            address: address.clone(),
            stream: RefCell::new(None),
        }
    }
    
    pub fn connect(&self) -> Result<u16, &'static str> {
        let addr:SocketAddr = self.address.parse().unwrap();
        match TcpStream::connect(addr){
            Ok(stream) => {

                let _ = stream.set_read_timeout(Some(Duration::new(5, 0)));
                
                // clean up welcome message
                //let _ = stream.read(&mut [0u8; 128]);
                
                let mut s = self.stream.borrow_mut();
                
                *s = Some(stream);
             
                Ok(0)
            },
            Err(e) => {
                error!("cannot connect to {:?}, {}", &self, e);
                Err("cannot connect")
            }
        }
    }
    
    pub fn set(&mut self, key:&str, v:&str){
        let s = self.stream.borrow_mut();
        
        if s.is_some() {
            let mut stream = s.as_ref().unwrap();
            let data = format!("set {} 0 0 {} \r\n", key, v.len());
            let _ = stream.write(data.as_bytes());
            // let _ = stream.flush();
            
            // let _ = stream.read(&mut [0u8; 512]);
            
            let _ = stream.write(v.as_bytes());
            let _ = stream.flush();
            let _ = stream.read(&mut [0u8; 512]);
        }
    }
    
    pub fn get_raw(&mut self, key:&str) -> Result<String,&str> {
        let s = self.stream.borrow_mut();
        
        if s.is_some() {
            let mut stream = s.as_ref().unwrap();
            let data = format!("get {}", key);
            
            trace!("querying server with: {}", data);
            
            let _ = stream.write(data.as_bytes());
            let _ = stream.flush();
            let mut buff = vec![0u8; 256];
            
            trace!("reading...");
            
            match stream.read(&mut buff) {
                Ok(count) if count > 0 => {
                    
                    trace!("done reading with {} bytes", count);
                    
                    let content = String::from_utf8(buff[0..count].to_vec()).unwrap();
                    
                    trace!("content: {}", content);
                    
                    // if content.trim() != "END" {
                        Ok(content)
                    // }else{
                    //     Ok("".to_string())
                    // }
                },
                Err(e) => {
                    error!("cannot read from stream. {}", e.description());
                    Err("")
                },
                x => { 
                    error!("unexpected return: {:?}", x);
                    Err("cannot read from remote node")
                }
            }
            
        }else{
            Err("cannot get stream")
        }
    }
    
    pub fn get(&mut self, key:&str) -> Option<String> {
        match self.get_raw(key){
            Ok(d) => {
                let s:Vec<&str> = d.split("\n").collect();
                Some(s[1].to_string())
            },
            Err(e) => {
                error!("error: {}", e);
                None
            }
        }
    }
    
    pub fn del(&mut self, key:&str) -> DbcResult {
        let stream = self.stream.borrow_mut();
        let mut stream = stream.as_ref().unwrap();
        let cmd = format!("del {}", key);
        let _ = stream.write(cmd.as_bytes());
        let mut buff = vec![0u8; 512];
        match stream.read(&mut buff){
            Ok(count) if count > 0 => {
                let rv = String::from_utf8(buff[0..count].to_vec()).unwrap();
                //if rv == "DELETED\r\n"
                Ok(rv)
            },
            Ok(_) => Err("count is zero"),
            Err(_) => Err("cannot read stream")
        }
    }
}



impl Drop for DbClient {
    fn drop(&mut self){
        debug!("db client shutdown.");
        self.stream.borrow_mut().as_ref()
            .map(|s| s.shutdown(Shutdown::Both));
    }
}


#[cfg(test)]
mod tests {
    
    use super::DbClient;
    
    fn get_db() -> DbClient {
        DbClient::new(&"127.0.0.1:8122".to_string())
    }
    
    #[test]
    fn test_set_n_get(){
        let mut dbc = get_db();
        let _ = dbc.connect();
        dbc.set("name", "Zufar");
        dbc.set("something", "In the way");
        dbc.set("article", "This is very long-long text we tried so far");
        assert_eq!(dbc.get_raw("name"), Ok("VALUE name 1 5\nZufar\nEND\n".to_string()));
        assert_eq!(dbc.get_raw("no_name"), Err("???"));
        assert_eq!(dbc.get("name"), Some("Zufar".to_string()));
        assert_eq!(dbc.get("none"), None);
        assert_eq!(dbc.get("something"), Some("In the way".to_string()));
        assert_eq!(dbc.get("article"), Some("This is very long-long text we tried so far".to_string()));
    }

}
