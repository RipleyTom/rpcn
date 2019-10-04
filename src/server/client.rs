use crate::server::database::DatabaseManager;
use crate::server::log::LogManager;
use crate::server::room_manager::{Room, RoomBinAttr, RoomManager, RoomUser};
use crate::server::stream_extractor::np2_structs_generated::*;
use crate::server::stream_extractor::StreamExtractor;
use crate::Config;

use num_derive::FromPrimitive;
use num_traits::FromPrimitive;

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{IpAddr, TcpStream};
use std::sync::{Arc, Mutex, RwLock};

const HEADER_SIZE: u16 = 5;

pub struct ClientInfo {
    pub user_id: i64,
    pub npid: String,
    pub psn_name: String,
    pub avatar_url: String,
}

pub struct Client {
    stream: TcpStream,
    db: Arc<Mutex<DatabaseManager>>,
    room_manager: Arc<RwLock<RoomManager>>,
    log_manager: Arc<Mutex<LogManager>>,
    sockets_list: Arc<Mutex<HashMap<i64, TcpStream>>>,
    authentified: bool,
    client_info: ClientInfo,
}

#[repr(u8)]
enum PacketType {
    Request,
    Reply,
    Notification,
}

#[repr(u16)]
#[derive(FromPrimitive, Debug)]
enum CommandType {
    Login,
    Terminate,
    Create,
    GetServerList,
    GetWorldList,
    CreateRoom,
    JoinRoom,
    SearchRoom,
    SetRoomDataExternal,
    GetRoomDataInternal,
    SetRoomDataInternal,
}

#[repr(u16)]
enum NotificationType {
    UserJoinedRoom,
    _UserLeftRoom,
    SignalP2PEstablished,
    _SignalP2PDisconnected,
}

#[repr(u8)]
pub enum ErrorType {
    NoError,
    Malformed,
    Invalid,
    ErrorLogin,
    DbFail,
    NotFound,
}

impl Client {
    pub fn new(
        stream: TcpStream,
        db: Arc<Mutex<DatabaseManager>>,
        room_manager: Arc<RwLock<RoomManager>>,
        log_manager: Arc<Mutex<LogManager>>,
        sockets_list: Arc<Mutex<HashMap<i64, TcpStream>>>,
    ) -> Client {
        stream.set_read_timeout(None).expect("set_read_timeout error!");
        stream.set_write_timeout(None).expect("set_write_timeout error!");

        let client_info = ClientInfo {
            user_id: 0,
            npid: String::new(),
            psn_name: String::new(),
            avatar_url: String::new(),
        };

        Client {
            stream,
            db,
            room_manager,
            log_manager,
            sockets_list,
            authentified: false,
            client_info,
        }
    }

    ///// Logging functions

    fn log(&self, s: &str) {
        self.log_manager.lock().unwrap().write(&format!("Client({}): {}", &self.client_info.npid, s));
    }
    fn log_verbose(&self, s: &str) {
        if Config::is_verbose() {
            self.log(s);
        }
    }
    fn dump_packet(&self, packet: &Vec<u8>, source: &str) {
        if !Config::is_verbose() {
            return;
        }
        self.log(&format!("Dumping packet({}):", source));
        let mut line = String::new();

        let mut count = 0;
        for p in packet {
            if (count != 0) && (count % 16) == 0 {
                self.log(&format!("{}", line));
                line.clear();
            }

            line = format!("{} {:02x}", line, p);
            count += 1;
        }
        self.log(&format!("{}", line));
    }

    ///// Command processing

    pub fn process(&mut self) {
        loop {
            let mut peek_data = [0; HEADER_SIZE as usize];

            match self.stream.read_exact(&mut peek_data) {
                Ok(_) => {
                    if peek_data[0] != PacketType::Request as u8 {
                        self.log("Received non request packed, disconnecting client");
                        break;
                    }

                    let command = u16::from_le_bytes([peek_data[1], peek_data[2]]);
                    let packet_size = u16::from_le_bytes([peek_data[3], peek_data[4]]);
                    if !self.interpret_command(command, packet_size) {
                        self.log("Disconnecting client");
                        break;
                    }
                }
                Err(_) => {
                    self.log("Client disconnected");
                    break;
                }
            }
        }

        if self.authentified {
            // self.db
            //     .lock()
            //     .unwrap()
            //     .leave_all_rooms(self.user_id)
            //     .unwrap();
            self.sockets_list.lock().unwrap().remove(&self.client_info.user_id);
        }
    }

    fn interpret_command(&mut self, command: u16, length: u16) -> bool {
        if length < HEADER_SIZE {
            self.log(&format!("Malformed packet(size < {})", HEADER_SIZE));
            return false;
        }

        let to_read = length - HEADER_SIZE;

        let mut data = vec![0; to_read as usize];
        match self.stream.read_exact(&mut data) {
            Ok(_) => {
                self.dump_packet(&data, "input");

                let mut reply = Vec::with_capacity(1000);

                reply.push(PacketType::Reply as u8);
                reply.extend(&command.to_le_bytes());
                reply.extend(&HEADER_SIZE.to_le_bytes());

                let mut se_data = StreamExtractor::new(data);
                let res = self.process_command(command, &mut se_data, &mut reply);

                // update length
                let len = reply.len() as u16;
                reply[3..5].clone_from_slice(&len.to_le_bytes());

                self.dump_packet(&reply, "output");

                match self.stream.write(&reply) {
                    Ok(nb) => {
                        if nb != reply.len() {
                            self.log("Failed to write all bytes!");
                            return false;
                        }
                    }
                    Err(e) => {
                        self.log(&format!("Write error: {}", e));
                        return false;
                    }
                }

                assert!(reply.len() >= 5);
                self.log_verbose(&format!("Returning: {}({}) for command: {}", res, reply[4], command));

                return res;
            }
            Err(e) => {
                self.log(&format!("Read error: {}", e));
                return false;
            }
        }
    }

    fn process_command(&mut self, command: u16, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> bool {
        let command = FromPrimitive::from_u16(command);
        if command.is_none() {
            self.log("Unknown command received");
            return false;
        }

        self.log_verbose(&format!("Parsing command {:?}", command));

        let command = command.unwrap();

        match command {
            CommandType::Terminate => return false,
            _ => {}
        }

        if !self.authentified {
            match command {
                CommandType::Login => return self.login(data, reply),
                CommandType::Create => return false, // TODO
                _ => {
                    reply.push(ErrorType::Invalid as u8);
                    return false;
                }
            }
        }

        match command {
            CommandType::GetServerList => return self.get_server_list(data, reply),
            CommandType::GetWorldList => return self.get_world_list(data, reply),
            CommandType::CreateRoom => return self.create_room(data, reply),
            CommandType::JoinRoom => return self.join_room(data, reply),
            CommandType::SearchRoom => return self.search_room(data, reply),
            CommandType::SetRoomDataExternal => return self.set_roomdata_external(data, reply),
            CommandType::GetRoomDataInternal => return self.get_roomdata_internal(data, reply),
            CommandType::SetRoomDataInternal => return self.set_roomdata_internal(data, reply),
            _ => {
                reply.push(ErrorType::Invalid as u8);
                return false;
            }
        }
    }

    ///// Account management

    fn login(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> bool {
        let login = data.get_string(false);
        let password = data.get_string(false);

        if data.error() {
            self.log("Error while extracting data from Login command");
            reply.push(ErrorType::Malformed as u8);
            return false;
        }

        if let Ok(user_data) = self.db.lock().unwrap().check_user(&login, &password) {
            self.authentified = true;
            self.client_info.npid = login;
            self.client_info.psn_name = user_data.psn_name.clone();
            self.client_info.avatar_url = user_data.avatar_url.clone();
            self.client_info.user_id = user_data.user_id;
            reply.push(ErrorType::NoError as u8);

            reply.extend(user_data.psn_name.as_bytes());
            reply.push(0);
            reply.extend(user_data.avatar_url.as_bytes());
            reply.push(0);

            self.log("Authentified");

            self.sockets_list.lock().unwrap().insert(self.client_info.user_id, self.stream.try_clone().unwrap());

            return true;
        }

        reply.push(ErrorType::ErrorLogin as u8);

        false
    }

    ///// Server/world retrieval

    fn get_server_list(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> bool {
        // Expecting 10(communicationId)
        let com_id = data.get_string(false);

        if data.error() || com_id.len() != 9 {
            self.log("Error while extracting data from GetServerList command");
            reply.push(ErrorType::Malformed as u8);
            return false;
        }

        // TODO: Generalize this (redirects DeS US queries to EU servers)
        // if com_id == "NPWR00881" {
        //     com_id = String::from("NPWR01249");
        // }

        let servs = self.db.lock().unwrap().get_server_list(&com_id);
        if let Err(_) = servs {
            reply.push(ErrorType::DbFail as u8);
            return false;
        }
        let servs = servs.unwrap();

        reply.push(ErrorType::NoError as u8);

        let num_servs = servs.len() as u16;
        reply.extend(&num_servs.to_le_bytes());
        for serv in servs {
            reply.extend(&serv.to_le_bytes());
        }

        self.log_verbose(&format!("Returning {} servers", num_servs));

        true
    }
    fn get_world_list(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> bool {
        // Expecting 2(serverId)
        let server_id = data.get::<u16>();

        if data.error() {
            self.log("Error while extracting data from GetWorldList command");
            reply.push(ErrorType::Malformed as u8);
            return false;
        }

        let worlds = self.db.lock().unwrap().get_world_list(server_id);
        if let Err(_) = worlds {
            reply.push(ErrorType::DbFail as u8);
            return false;
        }
        let worlds = worlds.unwrap();

        reply.push(ErrorType::NoError as u8);

        let num_worlds = worlds.len() as u32;
        reply.extend(&num_worlds.to_le_bytes());
        for world in worlds {
            reply.extend(&world.to_le_bytes());
        }

        self.log_verbose(&format!("Returning {} worlds", num_worlds));

        true
    }

    ///// Room commands

    fn create_room(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> bool {
        if let Ok(create_req) = data.get_flatbuffer::<CreateJoinRoomRequest>() {
            let server_id = self.db.lock().unwrap().get_corresponding_server(create_req.worldId());

            let resp = self.room_manager.write().unwrap().create_room(&create_req, &self.client_info, server_id);
            reply.push(ErrorType::NoError as u8);
            reply.extend(&(resp.len() as u32).to_le_bytes());
            reply.extend(resp);
            true
        } else {
            self.log("Error while extracting data from CreateRoom command");
            reply.push(ErrorType::Malformed as u8);
            false
        }
    }
    fn join_room(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> bool {
        if let Ok(create_req) = data.get_flatbuffer::<JoinRoomRequest>() {
            let resp = self.room_manager.write().unwrap().join_room(&create_req, &self.client_info);
            if let Err(e) = resp {
                reply.push(e);
            } else {
                let resp = resp.unwrap();
                reply.push(ErrorType::NoError as u8);
                reply.extend(&(resp.len() as u32).to_le_bytes());
                reply.extend(resp);
            }
            true
        } else {
            self.log("Error while extracting data from JoinRoom command");
            reply.push(ErrorType::Malformed as u8);
            false
        }

        // if data.error() {
        //     self.log("Error while extracting data from JoinRoom command");
        //     reply.push(ErrorType::Malformed as u8);
        //     return false;
        // }

        // let serv_id: u16;
        // let room_info;
        // let users_list;
        // let user_ids_list;
        // let member_id;
        // {
        //     // Lock the db for a little while as we require atomic listing of users
        //     let mut dadb = self.db.lock().unwrap();

        //     let r_user_ids_list = dadb.get_room_user_ids(room_id as i64);
        //     if r_user_ids_list.is_err() {
        //         reply.push(ErrorType::Empty as u8);
        //         return false;
        //     }
        //     user_ids_list = r_user_ids_list.unwrap();

        //     let r_member_id = dadb.join_room(self.user_id, room_id as i64, false);
        //     if r_member_id.is_err() {
        //         reply.push(ErrorType::DbFail as u8);
        //         return false;
        //     }
        //     member_id = r_member_id.unwrap();

        //     let r_room_info = dadb.get_room_info(room_id as i64);
        //     if r_room_info.is_err() {
        //         reply.push(ErrorType::DbFail as u8);
        //         return false;
        //     }
        //     room_info = r_room_info.unwrap();
        //     serv_id = dadb.get_corresponding_server(room_info.world_id);

        //     let r_users_list = dadb.get_room_users(room_id as i64);
        //     if r_users_list.is_err() {
        //         reply.push(ErrorType::DbFail as u8);
        //         return false;
        //     }
        //     users_list = r_users_list.unwrap();
        // }

        // reply.push(ErrorType::NoError as u8);
        // reply.extend(&serv_id.to_le_bytes());
        // reply.extend(&room_info.world_id.to_le_bytes());
        // reply.extend(&room_info.lobby_id.to_le_bytes());
        // reply.extend(&room_info.max_slot.to_le_bytes());
        // reply.extend(&room_info.owner_id.to_le_bytes());
        // reply.extend(&(users_list.len() as u16).to_le_bytes());
        // for user in users_list {
        //     reply.extend(&user.member_id.to_le_bytes());
        //     reply.extend(user.npid.as_bytes());
        //     reply.push(0);
        //     reply.extend(user.psn_name.as_bytes());
        //     reply.push(0);
        //     reply.extend(user.avatar_url.as_bytes());
        //     reply.push(0);
        // }

        // // Send notifications that a user joined
        // let mut n_msg: Vec<u8> = Vec::new();
        // n_msg.push(PacketType::Notification as u8);
        // n_msg.extend(&(NotificationType::UserJoinedRoom as u16).to_le_bytes());
        // n_msg.extend(&(0 as u16).to_le_bytes());
        // n_msg.extend(&room_id.to_le_bytes());
        // n_msg.extend(&member_id.to_le_bytes());
        // n_msg.extend(self.npid.as_bytes());
        // n_msg.push(0);
        // n_msg.extend(self.psn_name.as_bytes());
        // n_msg.push(0);
        // n_msg.extend(self.avatar_url.as_bytes());
        // n_msg.push(0);
        // let len = n_msg.len() as u16;
        // n_msg[3..5].clone_from_slice(&len.to_le_bytes());

        // self.dump_packet(&n_msg, "Notification User Joined");

        // // Notifies other room member than a new member joined the room
        // {
        //     let mut dasocks = self.sockets_list.lock().unwrap();

        //     for uid in &user_ids_list {
        //         let entry = dasocks.get_mut(&uid.user_id);

        //         if let Some(s) = entry {
        //             let _ = s.write(&n_msg);
        //         }
        //     }
        // }

        // // Notifies other room members than p2p connection was established
        // // Note that this is for the mesh format
        // let cur_user_addr = self.stream.peer_addr().unwrap().ip();

        // let cur_addr_bytes;
        // if let IpAddr::V4(ip4addr) = cur_user_addr {
        //     cur_addr_bytes = ip4addr.octets();
        //     println!("{:?}", cur_addr_bytes);
        // } else {
        //     panic!("An IPV6 got in here!");
        // }

        // let mut s_msg: Vec<u8> = Vec::new();
        // s_msg.push(PacketType::Notification as u8); // 0..1
        // s_msg.extend(&(NotificationType::SignalP2PEstablished as u16).to_le_bytes()); // 1..3
        // s_msg.extend(&(0 as u16).to_le_bytes()); // 3..5
        // s_msg.extend(&room_id.to_le_bytes()); // 5..13
        // s_msg.extend(&member_id.to_le_bytes()); // 13..15
        // s_msg.extend(&cur_addr_bytes); // 15..19
        // let len = s_msg.len() as u16;
        // s_msg[3..5].clone_from_slice(&len.to_le_bytes());

        // self.dump_packet(&s_msg, "Signaling message");

        // {
        //     let mut dasocks = self.sockets_list.lock().unwrap();

        //     for uid in &user_ids_list {
        //         let entry = dasocks.get_mut(&uid.user_id);

        //         if let Some(s) = entry {
        //             let _ = s.write(&s_msg);
        //         }
        //     }
        // }

        // // Notifies user that connection has been established with all other occupants
        // {
        //     let mut dasocks = self.sockets_list.lock().unwrap();

        //     for uid in &user_ids_list {
        //         let entry = dasocks.get_mut(&uid.user_id);
        //         s_msg[13..15].clone_from_slice(&uid.member_id.to_le_bytes());

        //         if let Some(s) = entry {
        //             let user_addr = s.peer_addr().unwrap().ip();

        //             let addr_bytes;
        //             if let IpAddr::V4(ip4addr) = user_addr {
        //                 addr_bytes = ip4addr.octets();
        //             } else {
        //                 panic!("An IPV6 got in here!");
        //             }

        //             s_msg[15..19].clone_from_slice(&addr_bytes);

        //             let _ = self.stream.write(&s_msg);
        //         }
        //     }
        // }

        // true
    }
    fn search_room(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> bool {
        if let Ok(search_req) = data.get_flatbuffer::<SearchRoomRequest>() {
            let resp = self.room_manager.read().unwrap().search_room(&search_req);

            reply.push(ErrorType::NoError as u8);
            reply.extend(&(resp.len() as u32).to_le_bytes());
            reply.extend(resp);
            true
        } else {
            self.log("Error while extracting data from SearchRoom command");
            reply.push(ErrorType::Malformed as u8);
            false
        }
    }
    fn set_roomdata_external(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> bool {
        if let Ok(setdata_req) = data.get_flatbuffer::<SetRoomDataExternalRequest>() {
            if let Err(e) = self.room_manager.write().unwrap().set_roomdata_external(&setdata_req) {
                reply.push(e);
            } else {
                reply.push(ErrorType::NoError as u8);
            }
            true
        } else {
            self.log("Error while extracting data from SetRoomDataExternal command");
            reply.push(ErrorType::Malformed as u8);
            false
        }
    }
    fn get_roomdata_internal(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> bool {
        if let Ok(setdata_req) = data.get_flatbuffer::<GetRoomDataInternalRequest>() {
            let resp = self.room_manager.write().unwrap().get_roomdata_internal(&setdata_req);
            if let Err(e) = resp {
                reply.push(e);
            } else {
                let resp = resp.unwrap();
                reply.push(ErrorType::NoError as u8);
                reply.extend(&(resp.len() as u32).to_le_bytes());
                reply.extend(resp);
            }
            true
        } else {
            self.log("Error while extracting data from GetRoomDataInternal command");
            reply.push(ErrorType::Malformed as u8);
            false
        }
    }
    fn set_roomdata_internal(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> bool {
        if let Ok(setdata_req) = data.get_flatbuffer::<SetRoomDataInternalRequest>() {
            if let Err(e) = self.room_manager.write().unwrap().set_roomdata_internal(&setdata_req) {
                reply.push(e);
            } else {
                reply.push(ErrorType::NoError as u8);
            }
            true
        } else {
            self.log("Error while extracting data from SetRoomDataExternal command");
            reply.push(ErrorType::Malformed as u8);
            false
        }
    }
}
