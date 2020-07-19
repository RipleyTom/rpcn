use std::io::{Error, ErrorKind, Read, Result, Write};
use std::net::TcpStream;
use std::sync::Arc;

use rustls::Session;

pub struct TlsStream {
    stream: TcpStream,
    tls_session: rustls::ServerSession,
}

impl TlsStream {
    pub fn new(stream: TcpStream, tls_config: Arc<rustls::ServerConfig>) -> TlsStream {
        stream.set_read_timeout(None).expect("set_read_timeout error!");
        stream.set_write_timeout(None).expect("set_write_timeout error!");

        let tls_session = rustls::ServerSession::new(&tls_config);

        TlsStream { stream, tls_session }
    }

    pub fn do_handshake(&mut self) -> Result<()> {
        while self.tls_session.is_handshaking() {
            if self.tls_session.wants_read() {
                match self.tls_session.read_tls(&mut self.stream) {
                    Ok(0) => return Err(Error::new(ErrorKind::UnexpectedEof, "Client Disconnected")),
                    _ => {}
                }
                self.tls_session.process_new_packets().map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
            }
            if self.tls_session.wants_write() {
                self.tls_session.write_tls(&mut self.stream)?;
            }
        }

        Ok(())
    }

    pub fn read_exact(&mut self, mut buf: &mut [u8]) -> Result<()> {
        while !buf.is_empty() {
            if self.tls_session.wants_write() {
                self.tls_session.write_tls(&mut self.stream)?;
            }
            match self.tls_session.read(&mut buf) {
                Ok(0) => {
                    // Read new packets and process them
                    match self.tls_session.read_tls(&mut self.stream) {
                        Ok(0) => break,
                        Ok(_) => self.tls_session.process_new_packets().map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?,
                        Err(ref e) if e.kind() == ErrorKind::Interrupted => {}
                        Err(e) => return Err(e),
                    }
                }
                Ok(n) => {
                    let tmp = buf;
                    buf = &mut tmp[n..];
                }
                Err(ref e) if e.kind() == ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        if !buf.is_empty() {
            Err(Error::new(ErrorKind::UnexpectedEof, "Failed to fill the whole buffer"))
        } else {
            Ok(())
        }
    }

    pub fn write(&mut self, buf: &[u8]) -> Result<usize> {
        match self.tls_session.write(buf) {
            Ok(n) => {
                assert!(n == buf.len());
            }
            Err(e) => return Err(e),
        }
        match self.tls_session.write_tls(&mut self.stream) {
            Ok(_) => return Ok(buf.len()),
            Err(e) => return Err(e),
        }
    }
}
