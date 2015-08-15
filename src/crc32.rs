



pub struct Crc32 {
    table: [u32; 256],
    value: u32
}

static CRC32_INITIAL:u32 = 0xedb88320;

impl Crc32 {

    pub fn new() -> Crc32 {
        let mut c = Crc32 { table: [0; 256], value: 0xffffffff };

        for i in 0..256 {
            let mut v = i as u32;
            for _ in 0..8 {
                v = if v & 1 != 0 {
                    CRC32_INITIAL ^ (v >> 1)
                } else {
                    v >> 1
                }
            }
            c.table[i] = v;
        }

        c
    }

    fn begin(&mut self) {
        self.value = 0xffffffff;
    }

    fn update(&mut self, buf: &[u8]) {
        for &i in buf {
            self.value = self.table[((self.value ^ (i as u32)) & 0xFF) as usize] ^ (self.value >> 8);
        }
    }

    fn finalize(&mut self) -> u32 {
        self.value ^ 0xffffffffu32
    }

    pub fn crc(&mut self, buf: &[u8]) -> u32 {
        self.begin();
        self.update(buf);
        self.finalize()
    }
    
}
