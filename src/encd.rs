
use std::str;
use std::error::Error;

use rustc_serialize::{Encodable, Decodable};

use msgpack;
use byteorder;

use msgpack::MsgpackResult;



pub trait MessageEncoderDecoder<T: ?Sized, D: ?Sized, E> {
    fn encode(&self, d: &T) -> Result<Vec<u8>, E>;
    fn decode<'a>(&self, d: &'a [u8]) -> Result<D, E>;
}


pub struct MsgPackEncoderDecoder;

impl<A, B> MessageEncoderDecoder<A, B, byteorder::Error> for MsgPackEncoderDecoder
    where A:Encodable, B: Decodable {

    fn encode(&self, d: &A) -> MsgpackResult<Vec<u8>> {
        msgpack::Encoder::to_msgpack(d)
    }
    fn decode<'a>(&self, bytes: &'a [u8]) -> MsgpackResult<B> {
        msgpack::from_msgpack(bytes)
    }
}



pub struct PlainTextEncoderDecoder;

impl PlainTextEncoderDecoder {
    pub fn new() -> PlainTextEncoderDecoder {
        PlainTextEncoderDecoder
    }
}

impl MessageEncoderDecoder<String, String, &'static str> for PlainTextEncoderDecoder {
    fn encode(&self, d: &String) -> Result<Vec<u8>, &'static str> {
        Ok(d.bytes().collect())
    }
    fn decode<'a>(&self, bytes: &'a [u8]) -> Result<String, &'static str> {
        match str::from_utf8(bytes){
            Ok(d) => Ok(d.trim().to_string()),
            Err(_) => Err("Cannot decode from utf8")
        }
    }
}
