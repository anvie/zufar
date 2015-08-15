extern crate test;


use std::collections::BTreeMap;

use std::io::prelude::*;
use std::io::BufWriter;
use std::fs::File;
use std::path::Path;
use std::fs::OpenOptions;

use crc32::Crc32;


pub struct Db {
    memtable: BTreeMap<u32, Vec<u8>>,
    fstore: File
}

impl Db {
    pub fn new() -> Db {
        // let path = Path::new("commitlog.txt");
        // 
        // let file = if path.exists(){
        //     File::open(path).unwrap()
        // }else{
        //     File::create(path).unwrap()
        // };
        
        let mut file = match OpenOptions::new()
                    .write(true)
                    .create(true)
                    .append(true)
                    .open("commitlog.txt"){
                        Ok(mut f) => f,
                        Err(e) => panic!("cannot open commitlog.txt. {}", e)
                    };
        
        Db {
            memtable: BTreeMap::new(),
            fstore: file
        }
    }
    
    pub fn insert(&mut self, k:&[u8], v:&[u8]){
        let mut crc32 = Crc32::new();
        self.memtable.insert(crc32.crc(k), v.to_vec());
        self.flush();
    }
    
    pub fn get(&mut self, k:&[u8]) -> Option<&[u8]> {
        let mut crc32 = Crc32::new();
        self.memtable.get(&crc32.crc(k)).map(|d| d.as_ref())
    }
    
    pub fn flush(&mut self){
        let iter = self.memtable.iter();
        for (k, v) in iter {
            
            println!("flushing k: {:?}, v: {:?}", k, v);
            
            //let mut writer = BufWriter::new(&self.fstore);
            
            self.fstore.write_all(format!("{}:{}\n", k, String::from_utf8(v.clone()).unwrap()).as_bytes());
        }
        self.fstore.flush();
        self.fstore.sync_all();
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
}

