extern crate byteorder;
extern crate bytes;
extern crate futures;
extern crate tokio_core;
extern crate tokio_io;
extern crate tokio_proto;
extern crate tokio_service;

pub mod error;
mod protocol;

use error::MonetError;
use protocol::MapiProtocol;

use futures::{future, Future};
use std::io;
use tokio_core::net::TcpStream;
use tokio_core::reactor::Handle;
use tokio_proto::{TcpClient};
use tokio_proto::pipeline::{ClientService};
use tokio_service::Service;

pub struct Params {
    socket: Option<String>,
    hostname: String,
    port: usize,
    username: String,
    password: String,
    database: String,
    language: String,
}

// Fix this later... should there be a builder pattern here?
impl Default for Params {
    fn default() -> Self {
        Params {
            socket: None,
            hostname: "localhost".to_owned(),
            port: 50000,
            username: "monetdb".to_owned(),
            password: "monetdb".to_owned(),
            database: "monetdb".to_owned(),
            language: "english".to_owned(),
        }
    }
}

pub struct Connection {
    inner: ClientService<TcpStream, MapiProtocol>,
}

impl Connection {

    pub fn connect(&mut self, params: &Params, handle: &Handle) -> Box<Future<Item=Connection, Error=io::Error>> {
        // use url concatenate and into_params trait
        let addr = format!("mapi://{}:{}@{}:{}/{})",
            params.username,
            params.password,
            params.hostname,
            params.port,
            params.database
        );

        let res = TcpClient::new(MapiProtocol)
            .connect(&addr.parse().unwrap(), handle)
            .map(|client_service| {
                Connection { inner: client_service }
            }
        );
        Box::new(res)
    }
}

impl Service for Connection {
    type Request = String;
    type Response = String;
    type Error = io::Error;
    type Future = Box<Future<Item=String, Error=io::Error>>;

    fn call(&self, req: String) -> Self::Future {
        self.inner.call(req).boxed()
    }
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

