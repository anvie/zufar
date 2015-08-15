

use std::collections::BTreeMap;
use crc32::Crc32;


struct Db {
    memtable: BTreeMap<u32, Vec<u8>>
}

impl Db {
    pub fn new() -> Db {
        Db {
            memtable: BTreeMap::new()
        }
    }
    // 
    // pub fn insert(&mut self, k:u32, v:&[u8]){
    //     self.memtable.insert(k, v.to_vec());
    // }
    
    pub fn insert(&mut self, k:&[u8], v:&[u8]){
        let mut crc32 = Crc32::new();
        self.memtable.insert(crc32.crc(k), v.to_vec());
    }
    
    pub fn get(&mut self, k:&[u8]) -> Option<&[u8]> {
        let mut crc32 = Crc32::new();
        self.memtable.get(&crc32.crc(k)).map(|d| d.as_ref())
    }
    
    pub fn flush(&self){
        let iter = self.memtable.iter();
        for (k, v) in iter {
            println!("flushing k: {:?}, v: {:?}", k, v);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Db;
    use crc32::Crc32;
    
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
}

