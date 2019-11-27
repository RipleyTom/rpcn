mod client;
use client::{Client, PacketType, HEADER_SIZE};
mod database;
use database::DatabaseManager;
mod room_manager;
use room_manager::RoomManager;
mod log;
use log::LogManager;

#[allow(non_snake_case, dead_code)]
mod stream_extractor;

use std::collections::HashMap;
use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;

const PROTOCOL_VERSION: u32 = 3;

pub struct Server {
    host: String,
    db: Arc<Mutex<DatabaseManager>>,
    room_manager: Arc<RwLock<RoomManager>>,
    log_manager: Arc<Mutex<LogManager>>,
    sockets_list: Arc<Mutex<HashMap<i64, TcpStream>>>,
}

impl Server {
    pub fn new(s_host: &str) -> Server {
        let log_manager = Arc::new(Mutex::new(LogManager::new()));
        let db = Arc::new(Mutex::new(DatabaseManager::new(log_manager.clone())));
        let room_manager = Arc::new(RwLock::new(RoomManager::new(log_manager.clone())));
        let sockets_list = Arc::new(Mutex::new(HashMap::new()));

        Server {
            host: String::from(s_host),
            db,
            room_manager,
            log_manager,
            sockets_list,
        }
    }

    fn log(&self, s: &str) {
        self.log_manager.lock().unwrap().write(&format!("Server: {}", s));
    }

    pub fn start(&mut self) {
        let _ = self.db.lock().unwrap().add_user("GalCiv", "abcdef", "RPCS3's GalCiv", "https://i.imgur.com/AfWIyQP.jpg");
        let _ = self.db.lock().unwrap().add_user("Whatcookie", "abcdef", "RPCS3's Cookie", "https://i.imgur.com/AfWIyQP.jpg");

        let listener = TcpListener::bind(&self.host);
        if let Err(e) = listener {
            self.log(&format!("Error binding to <{}>: {}", &self.host, e));
            return;
        }
        let listener = listener.unwrap();

        self.log(&format!("Now waiting on connections on <{}>", self.host));

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
                    let sockets_list = self.sockets_list.clone();

                    let _ = stream.write(&servinfo_vec);

                    thread::spawn(|| {
                        Server::handle_client(stream, db_client, room_client, log_client, sockets_list);
                    });
                }
                Err(_) => self.log("Accept failed!"),
            }
        }
    }

    fn handle_client(
        stream: TcpStream,
        db: Arc<Mutex<DatabaseManager>>,
        room_manager: Arc<RwLock<RoomManager>>,
        log_manager: Arc<Mutex<LogManager>>,
        sockets_list: Arc<Mutex<HashMap<i64, TcpStream>>>,
    ) {
        let mut client = Client::new(stream, db, room_manager, log_manager, sockets_list);
        client.process();
    }
}
