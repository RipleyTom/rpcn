use std::collections::HashMap;
use std::io::Write;
use std::net::{TcpListener, TcpStream};
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

#[allow(non_snake_case, dead_code)]
mod stream_extractor;

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

    pub fn start(&mut self) {
        // Starts udp signaling helper
        let udp_serv = UdpServer::new(&self.host, self.log_manager.clone(), self.signaling_infos.clone());
        udp_serv.start();

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
                Ok(mut stream) => {
                    self.log(&format!("New client from {}", stream.peer_addr().unwrap()));
                    let db_client = self.db.clone();
                    let room_client = self.room_manager.clone();
                    let log_client = self.log_manager.clone();
                    let signaling_infos = self.signaling_infos.clone();

                    let _ = stream.write(&servinfo_vec);

                    thread::spawn(|| {
                        Server::handle_client(stream, db_client, room_client, log_client, signaling_infos);
                    });
                }
                Err(_) => self.log("Accept failed!"),
            }
        }

        udp_serv.stop();
    }

    fn handle_client(
        stream: TcpStream,
        db: Arc<Mutex<DatabaseManager>>,
        room_manager: Arc<RwLock<RoomManager>>,
        log_manager: Arc<Mutex<LogManager>>,
        signaling_infos: Arc<RwLock<HashMap<i64, ClientSignalingInfo>>>,
    ) {
        let mut client = Client::new(stream, db, room_manager, log_manager, signaling_infos);
        client.process();
    }
}
