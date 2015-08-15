

use std::time::Duration;
use std;
use std::thread;
use std::net::TcpListener;
use std::net::{TcpStream, SocketAddr};
use std::io::prelude::*;
//use std::str;
use std::cell::RefCell;
use std::io::BufReader;

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
            Ok(mut stream) => {

                let _ = stream.set_read_timeout(Some(Duration::new(5, 0)));
                
                // clean up welcome message
                // {
                //     let mut buff = String::new();
                //     //let mut reader = BufReader::new(&stream);
                // 
                //     //reader.read_until(0x00,&mut buff);
                //     stream.read_to_string(&mut buff);
                //     //println!("got: {}", String::from_utf8(buff).unwrap());
                //     
                //     println!("got: {}", buff);
                //     
                // }
                
                {
                    // let mut buff = vec![0u8; 100];
                    // match stream.read(&mut buff){
                    //     Ok(count) => {
                    //         println!("welcome msg: {}", String::from_utf8(buff[0..count].to_vec()).unwrap());
                    //     },
                    //     Err(e) => println!("error while getting welcome msg, err: {}", e)
                    // }
                    let _ = stream.read(&mut [0u8; 128]);
                }
                
                let mut s = self.stream.borrow_mut();
                
                *s = Some(stream);
                

                
                
                //self.send_cmd_and_handle(stream, &*format!("v1|add-me|{}|{}", node.guid, node.address));
                
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
            let data = format!("set {} 1 0 {}\r\n", key, v.len());
            let _ = stream.write(data.as_bytes());
            let _ = stream.flush();
            
            // let mut buff = vec![0u8; 512];
            // match stream.read(&mut buff){
            //     Ok(count) => {
            //         println!("DbClient.set() buff: {:?}", buff);
            //     },
            //     Err(e) => println!("DbClient.set() err: {}", e)
            // }
            
            let _ = stream.read(&mut [0u8; 512]);
            
            let _ = stream.write(v.as_bytes());
            let _ = stream.flush();
            let _ = stream.read(&mut [0u8; 512]);
        }
    }
    
    pub fn get_raw(&mut self, key:&str) -> Option<String> {
        let s = self.stream.borrow_mut();
        
        if s.is_some() {
            let mut stream = s.as_ref().unwrap();
            let data = format!("get {}", key);
            let _ = stream.write(data.as_bytes());
            let _ = stream.flush();
            let mut buff = vec![0u8; 256];
            match stream.read(&mut buff) {
                Ok(count) if count > 0 => {
                    let content = String::from_utf8(buff[0..count].to_vec()).unwrap();
                    if content.trim() != "END" {
                       Some(content)
                    }else{
                        None
                    }
                },
                _ => None
            }
            
        }else{
            None
        }
    }
    
    pub fn get(&mut self, key:&str) -> Option<String> {
        match self.get_raw(key){
            Some(d) => {
                let s:Vec<&str> = d.split("\n").collect();
                Some(s[1].to_string())
            },
            _ => None
        }
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
        assert_eq!(dbc.get_raw("name"), Some("VALUE name 1 5\nZufar\nEND\n".to_string()));
        assert_eq!(dbc.get_raw("no_name"), None);
        assert_eq!(dbc.get("name"), Some("Zufar".to_string()));
        assert_eq!(dbc.get("none"), None);
        assert_eq!(dbc.get("something"), Some("In the way".to_string()));
        assert_eq!(dbc.get("article"), Some("This is very long-long text we tried so far".to_string()));
    }

}