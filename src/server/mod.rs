use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufReader};
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;

use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::runtime;
use tokio_rustls::rustls::internal::pemfile::{certs, pkcs8_private_keys};
use tokio_rustls::rustls::{NoClientAuth, ServerConfig, TLSError};
use tokio_rustls::TlsAcceptor;
use thiserror::*;

use parking_lot::{Mutex, RwLock};

mod client;
use client::{Client, ClientSignalingInfo, PacketType, HEADER_SIZE};
mod database;
use database::DatabaseManager;
mod room_manager;
use room_manager::RoomManager;
mod log;
use log::LogManager;
mod udp_server;
use crate::Config;
use udp_server::{UdpServer, UdpServerStartError};

#[allow(non_snake_case, dead_code)]
mod stream_extractor;

// use client::tls_stream::TlsStream;

const PROTOCOL_VERSION: u32 = 5;

pub struct Server {
    host: String,
    port: String,
    db: Arc<Mutex<DatabaseManager>>,
    room_manager: Arc<RwLock<RoomManager>>,
    log_manager: Arc<Mutex<LogManager>>,
    signaling_infos: Arc<RwLock<HashMap<i64, ClientSignalingInfo>>>,
}

#[non_exhaustive]
#[derive(Debug, Error)]
pub enum ServerStartError {
    #[error("{str_addr} is not a valid address")]
    InvalidAddress {
        str_addr: String,
    },
    #[error(transparent)]
    UdpServerError(#[from] UdpServerStartError),
    #[error("Failed to open certificate cert.pem")]
    OpenCertificateError(#[source] std::io::Error),
    #[error("Failed to open certificate key.pem")]
    OpenPrivateKeyError(#[source] std::io::Error),
    #[error("cert.pem is invalid")]
    InvalidCertificate,
    #[error("key.pem is invalid")]
    InvalidPrivateKey,
    #[error("key.pem is invalid")]
    CertificateSetupError(#[from] TLSError),
    #[error(transparent)]
    IoError(#[from] io::Error),
    #[error("Error binding to <{addr}>: {source}")]
    TcpBindError {
        #[source]
        source: io::Error,
        addr: SocketAddr,
    }
}

impl Server {
    pub fn new(s_host: &str, s_port: &str) -> Server {
        let log_manager = Arc::new(Mutex::new(LogManager::new()));
        let db = Arc::new(Mutex::new(DatabaseManager::new(log_manager.clone())));
        let room_manager = Arc::new(RwLock::new(RoomManager::new(log_manager.clone())));
        let signaling_infos = Arc::new(RwLock::new(HashMap::new()));

        Server {
            host: String::from(s_host),
            port: String::from(s_port),
            db,
            room_manager,
            log_manager,
            signaling_infos,
        }
    }

    fn log(&self, s: &str) {
        self.log_manager.lock().write(&format!("Server: {}", s));
    }

    #[allow(dead_code)]
    fn log_verbose(&self, s: &str) {
        if Config::is_verbose() {
            self.log(s);
        }
    }

    pub fn start(&mut self) -> Result<(), ServerStartError> {
        // Starts udp signaling helper on dedicated thread
        let mut udp_serv = UdpServer::new(&self.host, self.log_manager.clone(), self.signaling_infos.clone());
        udp_serv.start()?;

        // Parse host address
        let str_addr = self.host.clone() + ":" + &self.port;
        let mut addr = str_addr.to_socket_addrs().map_err(|_| ServerStartError::InvalidAddress { str_addr: str_addr.clone() })?;
        let addr = addr
            .next()
            .ok_or_else(|| ServerStartError::InvalidAddress { str_addr })?;

        // Setup TLS(TODO:ideally should not require a certificate)
        let f_cert = File::open("cert.pem").map_err(|e| ServerStartError::OpenCertificateError(e))?;
        let f_key = std::fs::File::open("key.pem").map_err(|e| ServerStartError::OpenPrivateKeyError(e))?;
        let certif = certs(&mut BufReader::new(&f_cert)).map_err(|_| ServerStartError::InvalidCertificate)?;
        let mut private_key = pkcs8_private_keys(&mut BufReader::new(&f_key)).map_err(|_| ServerStartError::InvalidPrivateKey)?;
        let mut server_config = ServerConfig::new(NoClientAuth::new());
        server_config.set_single_cert(certif, private_key.remove(0))?;

        // Setup Tokio
        let mut runtime = runtime::Builder::new().threaded_scheduler().enable_io().build()?;
        let handle = runtime.handle().clone();
        let acceptor = TlsAcceptor::from(Arc::new(server_config));

        let mut servinfo_vec = Vec::new();
        servinfo_vec.push(PacketType::ServerInfo as u8);
        servinfo_vec.extend(&0u16.to_le_bytes());
        servinfo_vec.extend(&(4 + HEADER_SIZE as u16).to_le_bytes());
        servinfo_vec.extend(&0u32.to_le_bytes());
        servinfo_vec.extend(&PROTOCOL_VERSION.to_le_bytes());
        let servinfo_vec: Arc<Vec<u8>> = Arc::new(servinfo_vec);

        let fut_server = async {
            let mut listener = TcpListener::bind(&addr).await.map_err(|source| ServerStartError::TcpBindError { addr, source })?;
            self.log(&format!("Now waiting for connections on <{}>", &addr));
            loop {
                let accept_result = listener.accept().await;
                if let Err(e) = accept_result {
                    self.log(&format!("Accept failed with: {}", e));
                    continue;
                }

                let (stream, peer_addr) = accept_result.unwrap();
                self.log(&format!("New client from {}", peer_addr));

                let acceptor = acceptor.clone();

                let db_client = self.db.clone();
                let room_client = self.room_manager.clone();
                let log_client = self.log_manager.clone();
                let signaling_infos = self.signaling_infos.clone();
                let servinfo_vec = servinfo_vec.clone();

                let fut_client = async move {
                    let mut stream = acceptor.accept(stream).await?;
                    stream.write_all(&servinfo_vec).await?;
                    let mut client = Client::new(stream, db_client, room_client, log_client, signaling_infos).await;
                    client.process().await;
                    Ok(()) as io::Result<()>
                };

                handle.spawn(fut_client);
            }
        };
        let res = runtime.block_on(fut_server);
        udp_serv.stop();
        res
    }
}
