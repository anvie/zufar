extern crate test;


use std::collections::BTreeMap;

use std::io::prelude::*;
use std::io::{BufWriter, BufReader};
use std::fs::File;
use std::path::Path;
use std::fs::OpenOptions;
use time;

use crc32::Crc32;


pub struct Db {
    memtable_eden: BTreeMap<u32, Vec<u8>>,
    memtable: BTreeMap<u32, Vec<u8>>,
    fstore: File,
    crc32: Crc32
}

impl Db {
    pub fn new() -> Db {
        
        let file_name = Box::new("commitlog.txt");
        let path = Path::new(*file_name);
        
        let mut _memtable = BTreeMap::new();
        {

            if path.exists(){
                // load data into memtable
                info!("loading commitlog data into memtable...");

                let ts = time::now().to_timespec().sec;
                
                let mut file = BufReader::new(File::open(&path).unwrap());
                for line in file.lines().filter_map(|result| result.ok()) {
                    let s:Vec<&str> = line.split("|").collect();
                    let version = s[0];
                    let hash_key:u32 = s[1].parse().unwrap();
                    let content = s[2..].join("|");
                    debug!("  (v{}) -> k: {}, content: {}", version, hash_key, content);
                    _memtable.insert(hash_key, content.into_bytes());
                }
                
                let ts = (time::now().to_timespec().sec - ts);
                
                info!("loading commitlog done. {} record(s) added in {}s", _memtable.len(), ts);
            }
        }

        let mut file = match OpenOptions::new()
                    .write(true)
                    .create(true)
                    .append(true)
                    .open(path){
                        Ok(f) => f,
                        Err(e) => panic!("cannot open commitlog.txt. {}", e)
                    };
        
        Db {
            memtable_eden: BTreeMap::new(),
            memtable: _memtable,
            fstore: file,
            crc32: Crc32::new()
        }
    }
    
    pub fn insert(&mut self, k:&[u8], v:&[u8]){
        //let mut crc32 = Crc32::new();
        self.memtable_eden.insert(self.crc32.crc(k), v.to_vec());
        //self.flush();
    }
    
    pub fn get(&mut self, k:&[u8]) -> Option<&[u8]> {
        //let mut crc32 = Crc32::new();
        let rv = self.memtable.get(&self.crc32.crc(k)).map(|d| d.as_ref());
        if rv.is_some(){
            rv
        }else{
            // try search in eden 
            self.memtable_eden.get(&self.crc32.crc(k)).map(|d| d.as_ref())
        }
    }
    
    pub fn del(&mut self, key:&[u8]) -> usize {
        let hash_key = self.crc32.crc(key);
        if self.memtable_eden.remove(&hash_key).is_none(){
           if self.memtable.remove(&hash_key).is_none(){
               return 0;
           }
        }
        return 1;
    }
    
    pub fn flush(&mut self){
        
        let mut to_remove:Vec<u32> = Vec::new();
        
        {
            let iter = self.memtable_eden.iter();

            for (k, v) in iter {

                //println!("flushing k: {:?}, v: {:?}", k, v);

                //let mut writer = BufWriter::new(&self.fstore);

                // format: [VERSION]|[KEY-HASH]|[CONTENT]
                self.fstore.write_all(format!("1|{}|{}\n", k, String::from_utf8(v.clone()).unwrap()).as_bytes());

                to_remove.push(*k);
            }
        }
        
        let _ = self.fstore.flush();
        let _ = self.fstore.sync_all();
        
        // move flushed data
        let mut count = 0;
        
        {
            let iter = self.memtable_eden.iter();
            for (k, v) in iter {
                self.memtable.insert(*k, v.clone());
                count = count + 1;
            }
        }
        
        // let mut count = 0;
        // for key in &to_remove {
        //     // remove from eden
        //     self.memtable_eden.remove(key);
        //     count = count + 1;
        // }
        self.memtable_eden.clear();
        
        info!("flushed {} item(s)", count);
        self.print_stats();
        
    }
    
    fn print_stats(&self){
        info!("memtable records: eden: {}, old: {}", self.memtable_eden.len(), self.memtable.len());
    }
}


#[cfg(test)]
mod tests {
    use super::Db;
    //use crc32::Crc32;
    use super::test::Bencher;
    use rand::random;
    
    #[test]
    fn test_insert(){
        let mut db = Db::new();
        db.insert(b"name", b"robin");
        
        assert_eq!(db.get(b"name"), Some(&b"robin"[..]));
        assert_eq!(db.get(b"boy_name"), None);
        
        db.insert(b"other_name", b"anything");
        
        assert_eq!(db.get(b"other_name"), Some(&b"anything"[..]));
        
        db.flush();
    }
    
    #[test]
    fn test_delete(){
        let mut db = Db::new();
        db.insert(b"name", b"Zufar");
        assert_eq!(db.get(b"name"), Some(&b"Zufar"[..]));
        db.del(b"name");
        assert_eq!(db.get(b"name"), None);
    }
    
    fn rand_string(count:usize) -> String {
        (0..count).map(|_| (0x20u8 + (random::<f32>() * 96.0) as u8) as char).collect()
    }
    
    #[bench]
    fn bench_insert(b: &mut Bencher){
        let mut db = Db::new();
        b.iter(|| {
            let k = format!("k-{}", rand_string(10));
            let v = format!("v-{}", rand_string(20));
            //println!("k: {}, v: {}", k, v);
            db.insert(k.as_bytes(), v.as_bytes());
        })
    }
    
    #[bench]
    fn bench_read(b: &mut Bencher){
        let mut db = Db::new();
        b.iter(|| {
            let k = format!("k-{}", rand_string(10));
            //let v = format!("v-{}", rand_string(20));
            //println!("k: {}, v: {}", k, v);
            db.get(k.as_bytes());
        })
    }
    
    #[bench]
    fn bench_delete(b: &mut Bencher){
        let mut db = Db::new();
        b.iter(|| {
            let k = format!("k-{}", rand_string(10));
            db.del(k.as_bytes());
        })
    }

    
}

