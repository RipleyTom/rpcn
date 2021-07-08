use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufReader};
use std::net::ToSocketAddrs;
use std::sync::Arc;

use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::runtime;
use tokio_rustls::rustls::internal::pemfile::{certs, pkcs8_private_keys};
use tokio_rustls::rustls::{NoClientAuth, ServerConfig};
use tokio_rustls::TlsAcceptor;
use tracing::{info, warn};

use parking_lot::{Mutex, RwLock};

pub mod client;
use client::{Client, ClientSignalingInfo, PacketType, HEADER_SIZE};
mod database;
use database::DatabaseManager;
mod room_manager;
use room_manager::RoomManager;
mod udp_server;
use crate::Config;
use udp_server::UdpServer;

#[allow(non_snake_case, dead_code)]
mod stream_extractor;

// use client::tls_stream::TlsStream;

const PROTOCOL_VERSION: u32 = 11;

pub struct Server {
    config: Arc<RwLock<Config>>,
    db: Arc<Mutex<DatabaseManager>>,
    room_manager: Arc<RwLock<RoomManager>>,
    signaling_infos: Arc<RwLock<HashMap<i64, ClientSignalingInfo>>>,
}

impl Server {
    pub fn new(config: Config) -> Server {
        let config = Arc::new(RwLock::new(config));
        let db = Arc::new(Mutex::new(DatabaseManager::new(config.clone())));
        let room_manager = Arc::new(RwLock::new(RoomManager::new()));
        let signaling_infos = Arc::new(RwLock::new(HashMap::new()));

        Server {
            config,
            db,
            room_manager,
            signaling_infos,
        }
    }

    pub fn start(&mut self) -> io::Result<()> {
        // Starts udp signaling helper on dedicated thread
        let mut udp_serv = UdpServer::new(&self.config.read().get_host(), self.signaling_infos.clone());
        if self.config.read().is_run_udp_server() {
            udp_serv.start()?;
        }

        // Parse host address
        let str_addr = self.config.read().get_host().clone() + ":" + &self.config.read().get_port();
        let mut addr = str_addr.to_socket_addrs().map_err(|e| io::Error::new(e.kind(), format!("{} is not a valid address", &str_addr)))?;
        let addr = addr
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::AddrNotAvailable, format!("{} is not a valid address", &str_addr)))?;

        // Setup TLS
        let f_cert = File::open("cert.pem").map_err(|e| io::Error::new(e.kind(), "Failed to open certificate cert.pem"))?;
        let f_key = std::fs::File::open("key.pem").map_err(|e| io::Error::new(e.kind(), "Failed to open private key key.pem"))?;
        let certif = certs(&mut BufReader::new(&f_cert)).map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "cert.pem is invalid"))?;
        let mut private_key = pkcs8_private_keys(&mut BufReader::new(&f_key)).map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "key.pem is invalid"))?;
        if private_key.len() == 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "key.pem doesn't contain a PKCS8 encoded private key!"));
        }
        let mut server_config = ServerConfig::new(NoClientAuth::new());
        server_config
            .set_single_cert(certif, private_key.remove(0))
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "Failed to setup certificate"))?;

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
            let mut listener = TcpListener::bind(&addr).await.map_err(|e| io::Error::new(e.kind(), format!("Error binding to <{}>: {}", &addr, e)))?;
            info!("Now waiting for connections on <{}>", &addr);
            loop {
                let accept_result = listener.accept().await;
                if let Err(e) = accept_result {
                    warn!("Accept failed with: {}", e);
                    continue;
                }

                let (stream, peer_addr) = accept_result.unwrap();
                stream.set_keepalive(Some(std::time::Duration::new(30, 0))).unwrap();
                info!("New client from {}", peer_addr);

                let acceptor = acceptor.clone();

                let config = self.config.clone();
                let db_client = self.db.clone();
                let room_client = self.room_manager.clone();
                let signaling_infos = self.signaling_infos.clone();
                let servinfo_vec = servinfo_vec.clone();

                let fut_client = async move {
                    let mut stream = acceptor.accept(stream).await?;
                    stream.write_all(&servinfo_vec).await?;
                    let mut client = Client::new(config, stream, db_client, room_client, signaling_infos).await;
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
