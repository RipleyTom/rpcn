use std::collections::HashMap;
use std::net::TcpListener;
use std::sync::Arc;
use std::thread;

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
use udp_server::UdpServer;
use crate::Config;

#[allow(non_snake_case, dead_code)]
mod stream_extractor;

use client::tls_stream::TlsStream;

const PROTOCOL_VERSION: u32 = 5;

pub struct Server {
    host: String,
    port: String,
    db: Arc<Mutex<DatabaseManager>>,
    room_manager: Arc<RwLock<RoomManager>>,
    log_manager: Arc<Mutex<LogManager>>,
    signaling_infos: Arc<RwLock<HashMap<i64, ClientSignalingInfo>>>,
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

    fn log_verbose(&self, s: &str) {
        if Config::is_verbose() {
            self.log(s);
        }
    }

    pub fn start(&mut self) {
        // Starts udp signaling helper on dedicated thread
        let udp_serv = UdpServer::new(&self.host, self.log_manager.clone(), self.signaling_infos.clone());
        udp_serv.start();

        // Setup TLS(TODO:ideally should not require a certificate)
        let mut server_config = rustls::ServerConfig::new(rustls::NoClientAuth::new());
        let f = std::fs::File::open("cert.pem");
        if f.is_err() {
            self.log("Failed to open certificate cert.pem");
            return;
        }
        let mut f = std::io::BufReader::new(f.unwrap());
        let certif = rustls::internal::pemfile::certs(&mut f).unwrap();
        let f = std::fs::File::open("key.pem");
        if f.is_err() {
            self.log("Failed to open private key key.pem");
            return;
        }
        let mut f = std::io::BufReader::new(f.unwrap());
        let private_key = rustls::internal::pemfile::pkcs8_private_keys(&mut f).unwrap();

        server_config.set_single_cert(certif, private_key[0].clone()).unwrap();
        // server_config.ciphersuites = server_config.ciphersuites.iter().filter(|s| s.suite == rustls::internal::msgs::enums::CipherSuite::TLS_DH_anon_WITH_AES_256_CBC_SHA256).map(|s| *s).collect();
        let server_config = Arc::new(server_config);

        // Starts actual server
        let bind_addr = self.host.clone() + ":" + &self.port;
        let listener = TcpListener::bind(&bind_addr);
        if let Err(e) = listener {
            self.log(&format!("Error binding to <{}>: {}", &bind_addr, e));
            return;
        }
        let listener = listener.unwrap();

        self.log(&format!("Now waiting on connections on <{}>", bind_addr));

        let mut servinfo_vec = Vec::new();
        servinfo_vec.push(PacketType::ServerInfo as u8);
        servinfo_vec.extend(&0u16.to_le_bytes());
        servinfo_vec.extend(&(4 + HEADER_SIZE as u16).to_le_bytes());
        servinfo_vec.extend(&0u32.to_le_bytes());
        servinfo_vec.extend(&PROTOCOL_VERSION.to_le_bytes());

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    if let Ok(addr) = stream.peer_addr() {
                        self.log(&format!("New client from {}", addr));

                        // Check validity of TLS stream before allocating resources to client
                        let tls_stream = Arc::new(Mutex::new(TlsStream::new(stream, server_config.clone())));
                        if let Err(e) = tls_stream.lock().do_handshake() {
                            self.log(&format!("Error doing the handshake: {}", e.to_string()));
                            continue;
                        }

                        self.log_verbose("Handshake successful!");

                        let db_client = self.db.clone();
                        let room_client = self.room_manager.clone();
                        let log_client = self.log_manager.clone();
                        let signaling_infos = self.signaling_infos.clone();

                        let res = tls_stream.lock().write(&servinfo_vec);
                        if let Err(e) = res {
                            self.log(&format!("Failed to write ServerInfo packet: {}", e.to_string()));
                        } else {
                            thread::spawn(|| {
                                Server::handle_client(tls_stream, db_client, room_client, log_client, signaling_infos);
                            });
                        }
                    }
                }
                Err(_) => self.log("Accept failed!"),
            }
        }

        udp_serv.stop();
    }

    fn handle_client(
        tls_stream: Arc<Mutex<TlsStream>>,
        db: Arc<Mutex<DatabaseManager>>,
        room_manager: Arc<RwLock<RoomManager>>,
        log_manager: Arc<Mutex<LogManager>>,
        signaling_infos: Arc<RwLock<HashMap<i64, ClientSignalingInfo>>>,
    ) {
        let mut client = Client::new(tls_stream, db, room_manager, log_manager, signaling_infos);
        client.process();
    }
}
