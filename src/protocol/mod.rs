//! Mapi protocol for MonetDB server
//!
//! Implementation follows
//! https://github.com/gijzelaerr/pymonetdb/blob/master/pymonetdb/mapi.py
//! which contains an official and native implementation of mapi.
//! and
//! https://github.com/snaga/monetdb/blob/master/java/src/nl/cwi/monetdb/mcl/net/MapiSocket.java
//!
//! The protocol handles handshake, as well as transport.

mod codec;

use std::io;
use self::codec::MapiCodec;
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_io::codec::Framed;
use tokio_proto::pipeline::ClientProto;

use error;
use error::MonetError;

const MSG_PROMPT: &'static str = "";
const MSG_MORE: &'static str = r"\1\2\n";
const MSG_INFO: &'static str = "#";
const MSG_ERROR: &'static str = "!";
const MSG_Q: &'static str = "&";
const MSG_QTABLE: &'static str = "&1";
const MSG_QUPDATE: &'static str = "&2";
const MSG_QSCHEMA: &'static str = "&3";
const MSG_QTRANS: &'static str = "&4";
const MSG_QPREPARE: &'static str = "&5";
const MSG_QBLOCK: &'static str = "&6";
const MSG_HEADER: &'static str = "%";
const MSG_TUPLE: &'static str = "[";
const MSG_TUPLE_NOSLICE: &'static str = "=";
const MSG_REDIRECT: &'static str = "^";
const MSG_OK: &'static str = "=OK";


pub struct MapiProtocol;

impl<T: AsyncRead + AsyncWrite + 'static> ClientProto<T> for MapiProtocol {
    type Request = String;
    type Response = String;

    type Transport = Framed<T, MapiCodec>;
    type BindTransport = Result<Self::Transport, io::Error>;
    fn bind_transport(&self, io: T) -> Self::BindTransport {
        Ok(io.framed(MapiCodec::new()))
        // handshake will be performed here.
    }
}

