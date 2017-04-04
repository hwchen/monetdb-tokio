use error::MonetError;
use protocol::MapiProtocol;

use futures::{future, Future};
use std::io;
use tokio_core::net::TcpStream;
use tokio_core::reactor::Handle;
use tokio_proto::{TcpClient};
use tokio_proto::pipeline::{ClientService};

pub struct Config {
    socket: Option<String>,
    hostname: String,
    port: usize,
    username: String,
    password: String,
    database: String,
    language: String,
}

// Fix this later... should there be a builder pattern here?
impl Default for Config {
    fn default() -> Self {
        Config {
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

pub struct Client {
    inner: ClientService<TcpStream, MapiProtocol>,
}


impl Client {

    pub fn connect(&mut self, config: Config, handle: &Handle) -> Box<Future<Item=Client, Error=io::Error>> {
        // use url concatenate
        let addr = format!("mapi://{}:{}@{}:{}/{})",
            config.username,
            config.password,
            config.hostname,
            config.port,
            config.database
        );

        let res = TcpClient::new(MapiProtocol)
            .connect(&addr.parse().unwrap(), handle)
            .map(|client_service| {
                Client { inner: client_service }
            }
        );
        Box::new(res)
    }
}

impl Service for Client {
    type Request = String;
    type Response = String;
    type Error = io::Error;
    type Future = Box<Future<Item=String, Error=io::Error>>;

    fn call(&self, req: String) -> Self::Future {
        self.inner.call(req)
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

