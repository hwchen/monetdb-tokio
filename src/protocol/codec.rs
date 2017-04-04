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

const MAX_PACKAGE_LENGTH: u16 = (1024 * 8) - 2;

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
    fn codec_decode_simple_decode() {
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
        let flag = (test_input_bytes.len() << 1) + 1;
        let mut flag_bytes = Vec::new();
        flag_bytes.write_u16::<LittleEndian>(flag as u16);

        let mut input = BytesMut::with_capacity(MAX_PACKAGE_LENGTH as usize + 2);
        input.extend(flag_bytes);
        input.extend(&test_input_bytes[..5]);
        let mut codec = MapiCodec::new();
        let output = codec.decode(&mut input);
        assert_eq!(output.map_err(|_|()), Ok(None));
        input.extend(&test_input_bytes[5..]);
        let output = codec.decode(&mut input);
        assert_eq!(output.map_err(|_|()), Ok(Some(test_input.to_owned())));
    }

    #[test]
    fn codec_test_simple_encode() {
        let mut buf = BytesMut::with_capacity(MAX_PACKAGE_LENGTH as usize + 2);
        let test_output = "this is test output";
        let mut codec = MapiCodec::new();
        codec.encode(test_output.to_owned(), &mut buf);

        println!("{:?}", buf);
        assert_eq!(
            &buf[..],
            &[39, 0, 116, 104, 105, 115, 32, 105,
              115, 32, 116, 101, 115, 116, 32, 111,
              117, 116, 112, 117, 116][..]
        );
    }

    #[test]
    fn codec_test_simple_encode_decode() {
        let mut buf = BytesMut::with_capacity(MAX_PACKAGE_LENGTH as usize + 2);
        let test_input = "this is test output";
        let mut codec = MapiCodec::new();
        codec.encode(test_input.to_owned(), &mut buf);
        let output = codec.decode(&mut buf);
        assert_eq!(output.unwrap().unwrap(), test_input);
    }

    // This is not a strong test, since it just tests a typical input.
    // But it was nice to just see it work on a real message.
    #[test]
    fn codec_real_input() {
        let mut input = BytesMut::with_capacity(MAX_PACKAGE_LENGTH as usize + 2);
        input.extend(REAL_FLAG_BYTES);
        let mut codec = MapiCodec::new();
        let output = codec.decode(&mut input);
        assert_eq!(output.map_err(|_|()), Ok(None));
        input.extend(REAL_INPUT_BYTES);
        let output = codec.decode(&mut input);
        assert_eq!(output.map_err(|_|()), Ok(Some(REAL_INPUT.to_owned())));
    }

    // from tcpdump listening to message sent from monetdb to client,
    // translated the hex dump to bytes.
    const REAL_FLAG_BYTES: &[u8] = &[223, 5];
    const REAL_INPUT_BYTES: &[u8] =
        &[38, 49, 32, 49, 56, 32, 48, 32, 50, 50, 32, 48, 10, 37, 32, 46, 76, 53, 48,
          44, 9, 46, 76, 53, 50, 44, 9, 46, 76, 53, 52, 44, 9, 46, 76, 53, 54, 44, 9,
          46, 76, 54, 49, 44, 9, 46, 76, 54, 51, 44, 9, 46, 76, 54, 53, 44, 9, 46, 76,
          54, 55, 44, 9, 46, 76, 55, 49, 44, 9, 46, 76, 55, 51, 44, 9, 46, 76, 55, 54,
          44, 9, 46, 76, 49, 48, 49, 44, 9, 46, 76, 49, 48, 51, 44, 9, 46, 76, 49, 48,
          53, 44, 9, 46, 76, 49, 48, 55, 44, 9, 46, 76, 49, 49, 49, 44, 9, 46, 76, 49,
          49, 51, 44, 9, 46, 76, 49, 49, 55, 44, 9, 46, 76, 49, 50, 50, 44, 9, 46, 76,
          49, 50, 53, 44, 9, 46, 76, 49, 51, 48, 44, 9, 46, 76, 49, 51, 51, 32, 35, 32,
          116, 97, 98, 108, 101, 95, 110, 97, 109, 101, 10, 37, 32, 84, 65, 66, 76, 69,
          95, 67, 65, 84, 44, 9, 84, 65, 66, 76, 69, 95, 83, 67, 72, 69, 77, 44, 9, 84,
          65, 66, 76, 69, 95, 78, 65, 77, 69, 44, 9, 67, 79, 76, 85, 77, 78, 95, 78, 65,
          77, 69, 44, 9, 68, 65, 84, 65, 95, 84, 89, 80, 69, 44, 9, 84, 89, 80, 69, 95,
          78, 65, 77, 69, 44, 9, 67, 79, 76, 85, 77, 78, 95, 83, 73, 90, 69, 44, 9, 66,
          85, 70, 70, 69, 82, 95, 76, 69, 78, 71, 84, 72, 44, 9, 68, 69, 67, 73, 77, 65,
          76, 95, 68, 73, 71, 73, 84, 83, 44, 9, 78, 85, 77, 95, 80, 82, 69, 67, 95, 82,
          65, 68, 73, 88, 44, 9, 78, 85, 76, 76, 65, 66, 76, 69, 44, 9, 82, 69, 77, 65,
          82, 75, 83, 44, 9, 67, 79, 76, 85, 77, 78, 95, 68, 69, 70, 44, 9, 83, 81, 76,
          95, 68, 65, 84, 65, 95, 84, 89, 80, 69, 44, 9, 83, 81, 76, 95, 68, 65, 84, 69,
          84, 73, 77, 69, 95, 83, 85, 66, 44, 9, 67, 72, 65, 82, 95, 79, 67, 84, 69, 84,
          95, 76, 69, 78, 71, 84, 72, 44, 9, 79, 82, 68, 73, 78, 65, 76, 95, 80, 79, 83,
          73, 84, 73, 79, 78, 44, 9, 73, 83, 95, 78, 85, 76, 76, 65, 66, 76, 69, 44, 9,
          83, 67, 79, 80, 69, 95, 67, 65, 84, 65, 76, 79, 71, 44, 9, 83, 67, 79, 80, 69,
          95, 83, 67, 72, 69, 77, 65, 44, 9, 83, 67, 79, 80, 69, 95, 84, 65, 66, 76, 69,
          44, 9, 83, 79, 85, 82, 67, 69, 95, 68, 65, 84, 65, 95, 84, 89, 80, 69, 32, 35,
          32, 110, 97, 109, 101, 10, 37, 32, 99, 104, 97, 114, 44, 9, 118, 97, 114, 99,
          104, 97, 114, 44, 9, 118, 97, 114, 99, 104, 97, 114, 44, 9, 118, 97, 114, 99,
          104, 97, 114, 44, 9, 115, 109, 97, 108, 108, 105, 110, 116, 44, 9, 118, 97,
          114, 99, 104, 97, 114, 44, 9, 105, 110, 116, 44, 9, 116, 105, 110, 121, 105,
          110, 116, 44, 9, 105, 110, 116, 44, 9, 116, 105, 110, 121, 105, 110, 116, 44,
          9, 105, 110, 116, 44, 9, 118, 97, 114, 99, 104, 97, 114, 44, 9, 118, 97, 114,
          99, 104, 97, 114, 44, 9, 116, 105, 110, 121, 105, 110, 116, 44, 9, 116, 105,
          110, 121, 105, 110, 116, 44, 9, 116, 105, 110, 121, 105, 110, 116, 44, 9, 98,
          105, 103, 105, 110, 116, 44, 9, 118, 97, 114, 99, 104, 97, 114, 44, 9, 118,
          97, 114, 99, 104, 97, 114, 44, 9, 118, 97, 114, 99, 104, 97, 114, 44, 9, 118,
          97, 114, 99, 104, 97, 114, 44, 9, 115, 109, 97, 108, 108, 105, 110, 116, 32,
          35, 32, 116, 121, 112, 101, 10, 37, 32, 51, 44, 9, 48, 44, 9, 48, 44, 9, 48,
          44, 9, 49, 44, 9, 48, 44, 9, 49, 44, 9, 49, 44, 9, 49, 44, 9, 49, 44, 9, 49,
          44, 9, 48, 44, 9, 48, 44, 9, 49, 44, 9, 49, 44, 9, 49, 44, 9, 49, 44, 9, 48,
          44, 9, 48, 44, 9, 48, 44, 9, 48, 44, 9, 49, 32, 35, 32, 108, 101, 110, 103,
          116, 104, 10];

    const REAL_INPUT: &'static str = "&1 18 0 22 0\n% .L50,\t.L52,\t.L54,\t.L56,\t.L61,\t.L63,\t.L65,\t.L67,\t.L71,\t.L73,\t.L76,\t.L101,\t.L103,\t.L105,\t.L107,\t.L111,\t.L113,\t.L117,\t.L122,\t.L125,\t.L130,\t.L133 # table_name\n% TABLE_CAT,\tTABLE_SCHEM,\tTABLE_NAME,\tCOLUMN_NAME,\tDATA_TYPE,\tTYPE_NAME,\tCOLUMN_SIZE,\tBUFFER_LENGTH,\tDECIMAL_DIGITS,\tNUM_PREC_RADIX,\tNULLABLE,\tREMARKS,\tCOLUMN_DEF,\tSQL_DATA_TYPE,\tSQL_DATETIME_SUB,\tCHAR_OCTET_LENGTH,\tORDINAL_POSITION,\tIS_NULLABLE,\tSCOPE_CATALOG,\tSCOPE_SCHEMA,\tSCOPE_TABLE,\tSOURCE_DATA_TYPE # name\n% char,\tvarchar,\tvarchar,\tvarchar,\tsmallint,\tvarchar,\tint,\ttinyint,\tint,\ttinyint,\tint,\tvarchar,\tvarchar,\ttinyint,\ttinyint,\ttinyint,\tbigint,\tvarchar,\tvarchar,\tvarchar,\tvarchar,\tsmallint # type\n% 3,\t0,\t0,\t0,\t1,\t0,\t1,\t1,\t1,\t1,\t1,\t0,\t0,\t1,\t1,\t1,\t1,\t0,\t0,\t0,\t0,\t1 # length\n";
}
