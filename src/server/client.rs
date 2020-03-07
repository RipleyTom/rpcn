use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;

use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use parking_lot::{Mutex, RwLock};

use crate::server::database::DatabaseManager;
use crate::server::log::LogManager;
use crate::server::room_manager::{RoomManager, SignalParam, SignalingType};
use crate::server::stream_extractor::fb_helpers::*;
use crate::server::stream_extractor::np2_structs_generated::*;
use crate::server::stream_extractor::StreamExtractor;
use crate::Config;

pub const HEADER_SIZE: u16 = 9;

pub struct ClientInfo {
    pub user_id: i64,
    pub npid: String,
    pub online_name: String,
    pub avatar_url: String,
}

pub struct ClientSignalingInfo {
    pub tcp_stream: TcpStream,
    pub addr_p2p: [u8; 4],
    pub port_p2p: u16,
}

impl ClientSignalingInfo {
    pub fn new(tcp_stream: TcpStream) -> ClientSignalingInfo {
        ClientSignalingInfo {
            tcp_stream,
            addr_p2p: [0; 4],
            port_p2p: 0,
        }
    }
}

pub struct Client {
    stream: TcpStream,
    db: Arc<Mutex<DatabaseManager>>,
    room_manager: Arc<RwLock<RoomManager>>,
    log_manager: Arc<Mutex<LogManager>>,
    signaling_infos: Arc<RwLock<HashMap<i64, ClientSignalingInfo>>>,
    authentified: bool,
    client_info: ClientInfo,
}

#[repr(u8)]
pub enum PacketType {
    Request,
    Reply,
    Notification,
    ServerInfo,
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
    LeaveRoom,
    SearchRoom,
    SetRoomDataExternal,
    GetRoomDataInternal,
    SetRoomDataInternal,
    PingRoomOwner,
}

#[repr(u16)]
enum NotificationType {
    UserJoinedRoom,
    UserLeftRoom,
    RoomDestroyed,
    SignalP2PEstablished,
    _SignalP2PDisconnected,
}

#[repr(u8)]
#[derive(Clone)]
#[allow(dead_code)]
pub enum EventCause {
    None,
    LeaveAction,
    KickoutAction,
    GrantOwnerAction,
    ServerOperation,
    MemberDisappeared,
    ServerInternal,
    ConnectionError,
    NpSignedOut,
    SystemError,
    ContextError,
    ContextAction,
}

#[repr(u8)]
pub enum ErrorType {
    NoError,
    Malformed,
    Invalid,
    ErrorLogin,
    ErrorCreate,
    DbFail,
    NotFound,
}

impl Client {
    pub fn new(
        stream: TcpStream,
        db: Arc<Mutex<DatabaseManager>>,
        room_manager: Arc<RwLock<RoomManager>>,
        log_manager: Arc<Mutex<LogManager>>,
        signaling_infos: Arc<RwLock<HashMap<i64, ClientSignalingInfo>>>,
    ) -> Client {
        stream.set_read_timeout(None).expect("set_read_timeout error!");
        stream.set_write_timeout(None).expect("set_write_timeout error!");

        let client_info = ClientInfo {
            user_id: 0,
            npid: String::new(),
            online_name: String::new(),
            avatar_url: String::new(),
        };

        Client {
            stream,
            db,
            room_manager,
            log_manager,
            signaling_infos,
            authentified: false,
            client_info,
        }
    }

    ///// Logging functions

    fn log(&self, s: &str) {
        self.log_manager.lock().write(&format!("Client({}): {}", &self.client_info.npid, s));
    }
    fn log_verbose(&self, s: &str) {
        if Config::is_verbose() {
            self.log(s);
        }
    }

    #[allow(dead_code)]
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
                    let packet_id = u32::from_le_bytes([peek_data[5], peek_data[6], peek_data[7], peek_data[8]]);
                    if !self.interpret_command(command, packet_size, packet_id) {
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
            // leave all rooms user is still in
            let rooms = self.room_manager.read().get_rooms_by_user(self.client_info.user_id);

            if let Some(rooms) = rooms {
                for room in rooms {
                    self.leave_room(&mut self.room_manager.write(), room, None, EventCause::MemberDisappeared);
                }
            }
            
            self.signaling_infos.write().remove(&self.client_info.user_id);
        }
    }

    fn interpret_command(&mut self, command: u16, length: u16, packet_id: u32) -> bool {
        if length < HEADER_SIZE {
            self.log(&format!("Malformed packet(size < {})", HEADER_SIZE));
            return false;
        }

        let to_read = length - HEADER_SIZE;

        let mut data = vec![0; to_read as usize];
        match self.stream.read_exact(&mut data) {
            Ok(_) => {
                //self.dump_packet(&data, "input");

                let mut reply = Vec::with_capacity(1000);

                reply.push(PacketType::Reply as u8);
                reply.extend(&command.to_le_bytes());
                reply.extend(&HEADER_SIZE.to_le_bytes());
                reply.extend(&packet_id.to_le_bytes());

                let mut se_data = StreamExtractor::new(data);
                let res = self.process_command(command, &mut se_data, &mut reply);

                // update length
                let len = reply.len() as u16;
                reply[3..5].clone_from_slice(&len.to_le_bytes());

                //self.dump_packet(&reply, "output");

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

                self.log_verbose(&format!("Returning: {}({})", res, reply[4]));

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
                CommandType::Create => return self.create_account(data, reply),
                _ => {
                    reply.push(ErrorType::Invalid as u8);
                    return false;
                }
            }
        }

        match command {
            CommandType::GetServerList => return self.req_get_server_list(data, reply),
            CommandType::GetWorldList => return self.req_get_world_list(data, reply),
            CommandType::CreateRoom => return self.req_create_room(data, reply),
            CommandType::JoinRoom => return self.req_join_room(data, reply),
            CommandType::LeaveRoom => return self.req_leave_room(data, reply),
            CommandType::SearchRoom => return self.req_search_room(data, reply),
            CommandType::SetRoomDataExternal => return self.req_set_roomdata_external(data, reply),
            CommandType::GetRoomDataInternal => return self.req_get_roomdata_internal(data, reply),
            CommandType::SetRoomDataInternal => return self.req_set_roomdata_internal(data, reply),
            CommandType::PingRoomOwner => return self.req_ping_room_owner(data, reply),
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

        if let Ok(user_data) = self.db.lock().check_user(&login, &password) {
            self.authentified = true;
            self.client_info.npid = login;
            self.client_info.online_name = user_data.online_name.clone();
            self.client_info.avatar_url = user_data.avatar_url.clone();
            self.client_info.user_id = user_data.user_id;
            reply.push(ErrorType::NoError as u8);

            reply.extend(user_data.online_name.as_bytes());
            reply.push(0);
            reply.extend(user_data.avatar_url.as_bytes());
            reply.push(0);

            reply.extend(&self.client_info.user_id.to_le_bytes());

            self.log("Authentified");

            self.signaling_infos
                .write()
                .insert(self.client_info.user_id, ClientSignalingInfo::new(self.stream.try_clone().unwrap()));

            return true;
        }

        reply.push(ErrorType::ErrorLogin as u8);

        false
    }

    fn create_account(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> bool {
        let npid = data.get_string(false);
        let password = data.get_string(false);
        let online_name = data.get_string(false);
        let avatar_url = data.get_string(false);

        if data.error() {
            self.log("Error while extracting data from Create command");
            reply.push(ErrorType::Malformed as u8);
            return false;
        }

        if let Err(_) = self.db.lock().add_user(&npid, &password, &online_name, &avatar_url) {
            self.log(&format!("Account creation failed(npid: {})", &npid));
            reply.push(ErrorType::ErrorCreate as u8);
        } else {
            self.log(&format!("Successfully created account {}", &npid));
            reply.push(ErrorType::NoError as u8);
        }
        false
    }

    ///// Helper functions
    fn create_notification(n_type: NotificationType, data: &Vec<u8>) -> Vec<u8> {
        let final_size = data.len() + HEADER_SIZE as usize;

        let mut final_vec = Vec::with_capacity(final_size);
        final_vec.push(PacketType::Notification as u8);
        final_vec.extend(&(n_type as u16).to_le_bytes());
        final_vec.extend(&(final_size as u16).to_le_bytes());
        final_vec.extend(&0u32.to_le_bytes()); // packet_id doesn't matter for notifications
        final_vec.extend(data);

        final_vec
    }
    fn send_notification(&self, notif: &Vec<u8>, user_list: &HashSet<i64>) {
        let sig_infos = self.signaling_infos.read();

        for user_id in user_list {
            let entry = sig_infos.get(user_id);

            if let Some(c) = entry {
                let mut stream_clone = c.tcp_stream.try_clone().unwrap(); // To avoid needing a write lock on signaling_infos
                let _ = stream_clone.write(&notif);
            }
        }
    }
    fn signal_connections(&self, room_id: u64, from: (u16, i64), to: HashMap<u16, i64>, sig_param: Option<SignalParam>) {
        if let None = sig_param {
            return;
        }
        let sig_param = sig_param.unwrap();
        if !sig_param.should_signal() {
            return;
        }

        match sig_param.get_type() {
            SignalingType::SignalingMesh => {
                // Notifies other room members that p2p connection was established
                let user_ids: HashSet<i64> = to.iter().map(|x| x.1.clone()).collect();

                let mut self_id = HashSet::new();
                self_id.insert(from.1);

                let mut addr_p2p = [0; 4];
                let mut port_p2p = 0;
                {
                    let sig_infos = self.signaling_infos.read();
                    if let Some(entry) = sig_infos.get(&from.1) {
                        addr_p2p = entry.addr_p2p;
                        port_p2p = entry.port_p2p;
                    }
                }

                let mut s_msg: Vec<u8> = Vec::new();
                s_msg.extend(&room_id.to_le_bytes()); // +0..+8 room ID
                s_msg.extend(&from.0.to_le_bytes()); // +8..+10 member ID
                s_msg.extend(&port_p2p.to_be_bytes()); // +10..+12 port
                s_msg.extend(&addr_p2p); // +12..+16 addr
                let mut s_notif = Client::create_notification(NotificationType::SignalP2PEstablished, &s_msg);
                self.send_notification(&s_notif, &user_ids);

                // Notifies user that connection has been established with all other occupants
                {
                    for user in &to {
                        let mut tosend = false; // tosend is there to avoid double locking on signaling_infos
                        {
                            let sig_infos = self.signaling_infos.read();
                            let user_si = sig_infos.get(&user.1);

                            if let Some(user_si) = user_si {
                                s_notif[(HEADER_SIZE as usize + 8)..(HEADER_SIZE as usize + 10)].clone_from_slice(&user.0.to_le_bytes());
                                s_notif[(HEADER_SIZE as usize + 10)..(HEADER_SIZE as usize + 12)].clone_from_slice(&user_si.port_p2p.to_be_bytes());
                                s_notif[(HEADER_SIZE as usize + 12)..(HEADER_SIZE as usize + 16)].clone_from_slice(&user_si.addr_p2p);

                                tosend = true;
                            }
                        }
                        if tosend {
                            self.send_notification(&s_notif, &self_id);
                        }
                    }
                }
            }
            _ => panic!("Unimplemented SignalingType({:?})", sig_param.get_type()),
        }
    }

    ///// Server/world retrieval

    fn req_get_server_list(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> bool {
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

        let servs = self.db.lock().get_server_list(&com_id);
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
    fn req_get_world_list(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> bool {
        // Expecting 2(serverId)
        let server_id = data.get::<u16>();

        if data.error() {
            self.log("Error while extracting data from GetWorldList command");
            reply.push(ErrorType::Malformed as u8);
            return false;
        }

        let worlds = self.db.lock().get_world_list(server_id);
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

    fn req_create_room(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> bool {
        if let Ok(create_req) = data.get_flatbuffer::<CreateJoinRoomRequest>() {
            let server_id = self.db.lock().get_corresponding_server(create_req.worldId());

            let resp = self.room_manager.write().create_room(&create_req, &self.client_info, server_id);
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
    fn req_join_room(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> bool {
        let mut room_manager = self.room_manager.write();
        if let Ok(join_req) = data.get_flatbuffer::<JoinRoomRequest>() {
            let room_id = join_req.roomId();
            if !room_manager.room_exists(room_id) {
                reply.push(ErrorType::NotFound as u8);
                return true;
            }

            let (users, siginfo);
            {
                let room = room_manager.get_room(room_id.clone());
                users = room.get_room_users();
                siginfo = room.get_signaling_info();
            }

            let resp = room_manager.join_room(&join_req, &self.client_info);
            if let Err(e) = resp {
                reply.push(e);
                return true;
            }

            let (member_id, resp) = resp.unwrap();
            reply.push(ErrorType::NoError as u8);
            reply.extend(&(resp.len() as u32).to_le_bytes());
            reply.extend(resp);

            let user_ids: HashSet<i64> = users.iter().map(|x| x.1.clone()).collect();

            // Notif other room users a new user has joined
            let mut n_msg: Vec<u8> = Vec::new();
            n_msg.extend(&room_id.to_le_bytes());
            let up_info = room_manager
                .get_room(room_id)
                .get_room_member_update_info(member_id, EventCause::None, Some(&join_req.optData().unwrap()));
            n_msg.extend(&(up_info.len() as u32).to_le_bytes());
            n_msg.extend(up_info);
            let notif = Client::create_notification(NotificationType::UserJoinedRoom, &n_msg);
            self.send_notification(&notif, &user_ids);

            // Send signaling stuff if any
            self.signal_connections(room_id, (member_id, self.client_info.user_id), users, siginfo);
        } else {
            self.log("Error while extracting data from JoinRoom command");
            reply.push(ErrorType::Malformed as u8);
            return false;
        }
        true
    }
    fn leave_room(&self, room_manager: &mut RoomManager, room_id: u64, opt_data: Option<&PresenceOptionData>, event_cause: EventCause) -> u8 {
        if !room_manager.room_exists(room_id) {
            return ErrorType::NotFound as u8;
        }

        let room = room_manager.get_room(room_id);
        let member_id = room.get_member_id(self.client_info.user_id);
        if let Err(e) = member_id {
            return e;
        }

        // We get this in advance in case the room is not destroyed
        let user_data = room.get_room_member_update_info(member_id.unwrap(), event_cause.clone(), opt_data);

        let res = room_manager.leave_room(room_id, self.client_info.user_id.clone());
        if let Err(e) = res {
            return e;
        }
        let (destroyed, users) = res.unwrap();

        if destroyed {
            // Notify other room users that the room has been destroyed
            let mut builder = flatbuffers::FlatBufferBuilder::new_with_capacity(1024);
            let opt_data = dc_opt_data(&mut builder, opt_data);
            let room_update = RoomUpdateInfo::create(
                &mut builder,
                &RoomUpdateInfoArgs {
                    eventCause: event_cause as u8,
                    errorCode: 0,
                    optData: Some(opt_data),
                },
            );
            builder.finish(room_update, None);
            let room_update_data = builder.finished_data().to_vec();

            let mut n_msg: Vec<u8> = Vec::new();
            n_msg.extend(&room_id.to_le_bytes());
            n_msg.extend(&(room_update_data.len() as u32).to_le_bytes());
            n_msg.extend(&room_update_data);

            let notif = Client::create_notification(NotificationType::RoomDestroyed, &n_msg);
            self.send_notification(&notif, &users);
        } else {
            // Notify other room users that someone left the room
            let mut n_msg: Vec<u8> = Vec::new();
            n_msg.extend(&room_id.to_le_bytes());
            n_msg.extend(&(user_data.len() as u32).to_le_bytes());
            n_msg.extend(&user_data);

            let notif = Client::create_notification(NotificationType::UserLeftRoom, &n_msg);
            self.send_notification(&notif, &users);
        }

        ErrorType::NoError as u8
    }

    fn req_leave_room(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> bool {
        let mut room_manager = self.room_manager.write();
        if let Ok(leave_req) = data.get_flatbuffer::<LeaveRoomRequest>() {
            reply.push(self.leave_room(&mut room_manager, leave_req.roomId(), Some(&leave_req.optData().unwrap()), EventCause::LeaveAction));
            reply.extend(&leave_req.roomId().to_le_bytes());
            true
        } else {
            self.log("Error while extracting data from SearchRoom command");
            reply.push(ErrorType::Malformed as u8);
            false
        }
    }
    fn req_search_room(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> bool {
        if let Ok(search_req) = data.get_flatbuffer::<SearchRoomRequest>() {
            let resp = self.room_manager.read().search_room(&search_req);

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
    fn req_set_roomdata_external(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> bool {
        if let Ok(setdata_req) = data.get_flatbuffer::<SetRoomDataExternalRequest>() {
            if let Err(e) = self.room_manager.write().set_roomdata_external(&setdata_req) {
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
    fn req_get_roomdata_internal(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> bool {
        if let Ok(setdata_req) = data.get_flatbuffer::<GetRoomDataInternalRequest>() {
            let resp = self.room_manager.read().get_roomdata_internal(&setdata_req);
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
    fn req_set_roomdata_internal(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> bool {
        if let Ok(setdata_req) = data.get_flatbuffer::<SetRoomDataInternalRequest>() {
            if let Err(e) = self.room_manager.write().set_roomdata_internal(&setdata_req) {
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
    fn req_ping_room_owner(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> bool {
        let room_id = data.get::<u64>();
        if data.error() {
            self.log("Error while extracting data from PingRoomOwner command");
            reply.push(ErrorType::Malformed as u8);
            return false;
        }

        let world_id = self.room_manager.read().get_corresponding_world(room_id);
        if let Err(e) = world_id {
            reply.push(e);
            return true;
        }
        let world_id = world_id.unwrap();
        let server_id = self.db.lock().get_corresponding_server(world_id);

        let mut builder = flatbuffers::FlatBufferBuilder::new_with_capacity(1024);
        let resp = GetPingInfoResponse::create(
            &mut builder,
            &GetPingInfoResponseArgs {
                serverId: server_id,
                worldId: world_id,
                roomId: room_id,
                rtt: 20000,
            },
        );

        builder.finish(resp, None);
        let finished_data = builder.finished_data().to_vec();

        reply.push(ErrorType::NoError as u8);
        reply.extend(&(finished_data.len() as u32).to_le_bytes());
        reply.extend(finished_data);

        true
    }
}
