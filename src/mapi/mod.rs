//! Mapi protocol for MonetDB server
//!
//! Implementation follows
//! https://github.com/gijzelaerr/pymonetdb/blob/master/pymonetdb/mapi.py
//! which contains an official and native implementation of mapi.
//!
//! and
//! https://github.com/snaga/monetdb/blob/master/java/src/nl/cwi/monetdb/mcl/net/MapiSocket.java

pub mod protocol;
pub mod codec;
