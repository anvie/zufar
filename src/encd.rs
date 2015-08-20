

use std::io;



pub trait MessageEncoderDecoder {
    fn encode<'a>(&self, d: &'a [u8]) -> io::Result<&'a [u8]>;
    fn decode<'a>(&self, bytes: &'a [u8]) -> io::Result<&'a [u8]>;
}




pub struct BytesEncoderDecoder;

impl BytesEncoderDecoder {
    pub fn new() -> BytesEncoderDecoder {
        BytesEncoderDecoder
    }
}

impl MessageEncoderDecoder for BytesEncoderDecoder {
    fn encode<'a>(&self, d: &'a [u8]) -> io::Result<&'a [u8]> {
        Ok(d)
    }
    fn decode<'a>(&self, bytes: &'a [u8]) -> io::Result<&'a [u8]> {
        Ok(bytes)
    }
}
