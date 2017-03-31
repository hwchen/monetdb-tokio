//! Mapi codec for MonetDB server
//!
//! Implementation follows
//! https://github.com/gijzelaerr/pymonetdb/blob/master/pymonetdb/mapi.py
//! which contains an official and native implementation of mapi.
//! and
//! https://github.com/snaga/monetdb/blob/master/java/src/nl/cwi/monetdb/mcl/net/MapiSocket.java
//!
//! Protocol uses this codec for encoding/decoding block of message.

use std::io;
use std::io::Cursor;
use std::str;
use byteorder::{ReadBytesExt, WriteBytesExt, LittleEndian};
use bytes::BytesMut;
use tokio_io::codec::{Encoder, Decoder};

const MAX_PACKAGE_LENGTH: u16 = (1024 * 8) -2;

#[derive(Debug, Clone)]
struct Flag {
    length: u16,
    last: u16,
}

#[derive(Debug, Clone)]
pub struct MapiCodec {
    flag: Option<Flag>,
    block: Vec<u8>,
}

impl MapiCodec {
    pub fn new() -> Self {
        MapiCodec {
            flag: None,
            block: Vec::new(),
        }
    }
}

impl Decoder for MapiCodec {
    type Item = String;
    type Error = io::Error;

    /// A mapi "block" is one entire message, sent in chunks that are
    /// a maximum of MAX_PACKAGE_LENGTH.
    ///
    /// Overall strategy for dealing with async:
    /// - read flag eagerly, to know what's coming up.
    /// - only read the following chunk when it's completely available.
    ///
    /// This way, we never have to track how much of a block is read.
    ///
    /// Also, we only ever try to read one chunk at a time. If we tried to
    /// do multiple chunks at a time, there could be a lot of partial
    /// chunks being read. And, better to let tokio tell us when bytes are
    /// ready, instead of doing it both in tokio and in our logic.
    fn decode(&mut self, buf:&mut BytesMut) -> io::Result<Option<String>> {
        println!("{:?}", self);
        // If no flag, it's the start of a chunk.
        // Therefore, start by reading flag.
        //
        // If there's a flag, use current flag (and we know that the following
        // chunk has not been pulled out of the buffer yet).
        if self.flag.is_none() {
            if buf.len() < 2 { return Ok(None) };
            let flag = buf.split_to(2);
            let flag = Cursor::new(flag).read_u16::<LittleEndian>().unwrap();
            self.flag = Some(Flag {
                length: flag >> 1,
                last: flag & 1,
            });
        }

        // Now that there is a flag, check that there is a full chunk
        // available to read. If not, wait for more bytes.
        let length = self.flag.as_ref().unwrap().length as usize;
        if length > buf.len() { return Ok(None) };

        // We know there is a full chunk available to read, now we can
        // read it!
        let bytes = buf.split_to(length);

        // Chunk is read; now append to block. Done with one chunk!
        self.block.extend_from_slice(&bytes[..]);

        // Check if all chunks are appended into block. If so, parse the whole
        // block. If block is not yet completed, go back for more chunks.
        // (And flag needs to be reset whether it's chunk or block completed.
        // Incompletes should have short-circuited above)
        let last = self.flag.as_ref().unwrap().last;
        self.flag = None;

        if last == 1 {
            match str::from_utf8(&self.block) {
                Ok(s) => Ok(Some(s.to_owned())),
                Err(_) => Err(io::Error::new(io::ErrorKind::Other, "invalid UTF-8")),
            }
        } else {
            Ok(None)
        }
    }
}

impl Encoder for MapiCodec {
    type Item = String;
    type Error = io::Error;

    fn encode(&mut self, msg: String, buf: &mut BytesMut) -> io::Result<()> {
        // Split at MAX_PACKAGE_LENGTH, then insert flag, then join,
        // then append. Basically, preprocess and then send whole
        // block at once. tokio will take care of streaming it out.
        let bytes = msg.as_bytes();
        let chunks = bytes.chunks(MAX_PACKAGE_LENGTH as usize);

        for chunk in chunks {
            let length = chunk.len() as u16;
            let last = if length < MAX_PACKAGE_LENGTH { 1 } else { 0 };
            let flag: u16 = (length << 1) + last;

            let mut flag_bytes = vec![];
            flag_bytes.write_u16::<LittleEndian>(flag).unwrap();

            buf.extend(&flag_bytes);
            buf.extend(chunk);
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use bytes::BytesMut;
    use std::io::Cursor;
    use byteorder::{ReadBytesExt, LittleEndian};

    const MAX_PACKAGE_LENGTH: u16 = (1024 * 8) -2;

    #[test]
    fn codec_decode_simple_input() {
        // basic test case
        let test_input = "this is test input";
        let test_input_bytes = test_input.as_bytes();
        let flag = (test_input_bytes.len() << 1) + 1;
        let mut flag_bytes = Vec::new();
        flag_bytes.write_u16::<LittleEndian>(flag as u16);

        let mut input = BytesMut::with_capacity(MAX_PACKAGE_LENGTH as usize + 2);
        input.extend(flag_bytes);
        input.extend(test_input_bytes);
        let mut codec = MapiCodec::new();
        let output = codec.decode(&mut input);
        assert_eq!(output.map_err(|_|()), Ok(Some(test_input.to_owned())));


        // flag is set for length too long
        let test_input = "this is test input";
        let test_input_bytes = test_input.as_bytes();
        let flag = ((test_input_bytes.len() + 1) << 1) + 1;
        let mut flag_bytes = Vec::new();
        flag_bytes.write_u16::<LittleEndian>(flag as u16);

        let mut input = BytesMut::with_capacity(MAX_PACKAGE_LENGTH as usize + 2);
        input.extend(flag_bytes);
        input.extend(test_input_bytes);
        let mut codec = MapiCodec::new();
        let output = codec.decode(&mut input);
        assert_eq!(output.map_err(|_|()), Ok(None));

        // Two chunks;
        let test_input = "this is test input";
        let test_input_bytes = test_input.as_bytes();
        let flag = ((test_input_bytes.len() + 1) << 1) + 1;
        let mut flag_bytes = Vec::new();
        flag_bytes.write_u16::<LittleEndian>(flag as u16);

        let mut input = BytesMut::with_capacity(MAX_PACKAGE_LENGTH as usize + 2);
        input.extend(flag_bytes);
        input.extend(&test_input_bytes[..5]);
        println!("{:?}", input);
        let mut codec = MapiCodec::new();
        let output = codec.decode(&mut input);
        assert_eq!(output.map_err(|_|()), Ok(None));
        input.extend(&test_input_bytes[5..]);
        println!("{:?}", input);
        let output = codec.decode(&mut input);
        assert_eq!(output.map_err(|_|()), Ok(Some(test_input.to_owned())));
    }

    #[test]
    #[ignore]
    fn codec_real_input() {
        //println!("{:?}", 0xdf05);
        let flag = Cursor::new([223u8, 5]).read_u16::<LittleEndian>().unwrap();
        println!("{:?}", flag);
        println!("{:?}", flag >> 1);
        println!("{:?}", flag & 1);
        //println!("{:?}", 0x5d0e);
        // Packet sent from client is 1840 bytes long, - 2 bytes for flag.
        let flag = Cursor::new([93u8, 14]).read_u16::<LittleEndian>().unwrap();
        println!("{:?}", flag);
        println!("{:?}", flag >> 1);
        println!("{:?}", flag & 1);
        panic!();
    }
}
