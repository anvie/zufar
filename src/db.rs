extern crate test;


use std::collections::BTreeMap;

use std::io::prelude::*;
use std::io::{BufWriter, BufReader, SeekFrom};
use std::fs::File;
use std::path::Path;
use std::fs::OpenOptions;
use std::cell::{RefCell, RefMut, UnsafeCell};
use std::rc::Rc;

use time;

use crc32::Crc32;
use rocksdb::{RocksDB, Writable, WriteBatch, RocksDBResult};
use byteorder::ByteOrder;
use byteorder::{LittleEndian, WriteBytesExt};


pub struct Db {
    memtable_eden: BTreeMap<u32, Vec<u8>>,
    memtable: UnsafeCell<BTreeMap<u32, Vec<u8>>>,
    fstore: File,
    crc32: Crc32,
    rocksdb: RocksDB,
    _flush_counter: u16
}

impl Db {
    pub fn new(data_dir:&str) -> Db {
        
        let data_path = format!("{}/commitlog.txt", data_dir);
        //let file_name = Box::new(data_path);
        let path = Path::new(&*data_path);
        
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
        
        // initialize rocksdb
        let rocksdb = {
            let data_path = format!("{}/rocks", data_dir);
            RocksDB::open_default(&*data_path).unwrap()
        };
        
        Db {
            memtable_eden: BTreeMap::new(),
            memtable: UnsafeCell::new(_memtable),
            fstore: file,
            crc32: Crc32::new(),
            rocksdb: rocksdb,
            _flush_counter: 0u16
        }
    }
    
    pub fn insert(&mut self, k:&[u8], v:&[u8]){
        self.memtable_eden.insert(self.crc32.crc(k), v.to_vec());
        //self.flush();
    }
    
    pub fn get(&mut self, k:&[u8]) -> Option<&[u8]> {
        
        // let _self = Rc::new(RefCell::new(self));
        
        // let _self = _self.borrow();
        let key_hashed = self.crc32.crc(k);
        
        let rv = unsafe { (*self.memtable.get()).get(&key_hashed).map(|d| d.as_ref()) };
        if rv.is_some(){
            rv
        }else{
            // try search in eden 
            let rv = self.memtable_eden.get(&key_hashed).map(|d| d.as_ref()).clone();
            
            if rv.is_some(){
                rv
            }else{
                // try search in rocks
                // let mt = &mut self.memtable;
                
                debug!("not found both from old and eden memtable, try to find in rocks");
                
                let mut wtr = Vec::with_capacity(4);
                wtr.write_u32::<LittleEndian>(key_hashed).unwrap();
                match self.rocksdb.get(&*wtr){//.map(|d| d.as_ref()){
                    RocksDBResult::Some(x) => {
                        //self.insert(k, &*x);
                        //self.get(k)
                        
                        trace!("got from rocks, adding into old memtable for recent use.");
                        
                        unsafe {
                            (*self.memtable.get()).insert(key_hashed, (&*x).to_vec());
                            (*self.memtable.get()).get(&key_hashed).map(|d| d.as_ref())
                        }
                    },
                    _ => None,
                }
            }
            
        }
    
    
    }
    
    pub fn del(&mut self, key:&[u8]) -> usize {
        let hash_key = self.crc32.crc(key);
        if self.memtable_eden.remove(&hash_key).is_none(){
           unsafe {
                if (*self.memtable.get()).remove(&hash_key).is_none(){
                    
                    // try remove from rocks
                    let mut wtr = Vec::with_capacity(4);
                    wtr.write_u32::<LittleEndian>(hash_key).unwrap();
                    self.rocksdb.delete(&*wtr).unwrap();
                    
                    return 0;
                }
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
        
        unsafe {
            let iter = self.memtable_eden.iter();
            for (k, v) in iter {
                (*self.memtable.get()).insert(*k, v.clone());
                count = count + 1;
            }
        }
        

        self.memtable_eden.clear();
        
        info!("flushed {} item(s)", count);
        self.print_stats();
        
        self._flush_counter = self._flush_counter + 1;
        
        if self._flush_counter > 5 {
            self._flush_counter = 0;
            unsafe {
                info!("flusing to rocks...");
                let mut batch = WriteBatch::new();
                let iter = (*self.memtable.get()).iter();
                let mut count = 0;
                for (k, v) in iter {
                    //let _k_bytes:&mut [u8] = &mut [0u8; 4];
                    let mut wtr = Vec::with_capacity(4); //vec![];
                    // ByteOrder::write_u32::<LittleEndian>(_k_bytes, *k);
                    wtr.write_u32::<LittleEndian>(*k).unwrap();
                    batch.put(&*wtr, v);
                    count = count + 1;
                }
                self.rocksdb.write(batch);
                info!("{} records flushed into rocks.", count);
            }
            
            // clean up old memtable
            unsafe {
                (*self.memtable.get()).clear();
                self.fstore.seek(SeekFrom::Start(0u64));
            }
        }
        
    }
    
    fn print_stats(&self){
        let old_count = unsafe { (*self.memtable.get()).len() };
        info!("memtable records: eden: {}, old: {}", self.memtable_eden.len(), old_count);
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

