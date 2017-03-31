//! Mapi protocol for MonetDB server
//!
//! Implementation follows
//! https://github.com/gijzelaerr/pymonetdb/blob/master/pymonetdb/mapi.py
//! which contains an official and native implementation of mapi.
//! and
//! https://github.com/snaga/monetdb/blob/master/java/src/nl/cwi/monetdb/mcl/net/MapiSocket.java
//!

use super::codec;

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

enum State {
    Init,
    Ready,
}

fn parse_error_str(s: &str) -> (MonetError, &str) {
    use self::MonetError::*;

    if s.len() > 6 {
        if let Some(err) = MonetError::from_mapi_code(&s[0..6]) {
            return (err, &s[6..]);
        }
    }
    (OperationalError, s)
}

struct Config {
    state: State,
    socket: String,
    hostname: String,
    port: usize,
    username: String,
    password: String,
    database: String,
    language: String,
}

