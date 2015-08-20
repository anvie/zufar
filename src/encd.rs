
//use std::str;
//use std::error::Error;
use std::io;

//use rustc_serialize::{Encodable, Decodable};

// use msgpack;
// use byteorder;
//
// use msgpack::MsgpackResult;



pub trait MessageEncoderDecoder {
    fn encode<'a>(&self, d: &'a [u8]) -> io::Result<&'a [u8]>;
    fn decode<'a>(&self, bytes: &'a [u8]) -> io::Result<&'a [u8]>;
}


// pub struct MsgPackEncoderDecoder;
//
// impl<A, B> MessageEncoderDecoder<A, B, byteorder::Error> for MsgPackEncoderDecoder
//     where A:Encodable, B: Decodable {
//
//     fn encode(&self, d: &A) -> MsgpackResult<Vec<u8>> {
//         msgpack::Encoder::to_msgpack(d)
//     }
//     fn decode<'a>(&self, bytes: &'a [u8]) -> MsgpackResult<B> {
//         msgpack::from_msgpack(bytes)
//     }
// }



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
