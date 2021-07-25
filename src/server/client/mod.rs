mod ticket;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use lettre::smtp::authentication::{Credentials, Mechanism};
use lettre::{EmailAddress, SmtpClient, SmtpTransport, Transport};
use lettre_email::EmailBuilder;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use parking_lot::{Mutex, RwLock};
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_rustls::server::TlsStream;
use tracing::{error, info, info_span, trace, warn, Instrument};

use crate::server::client::ticket::Ticket;
use crate::server::database::DatabaseManager;
use crate::server::room_manager::{RoomManager, SignalParam, SignalingType};
use crate::server::stream_extractor::fb_helpers::*;
use crate::server::stream_extractor::np2_structs_generated::*;
use crate::server::stream_extractor::StreamExtractor;
use crate::Config;

use super::room_manager::Room;

pub const HEADER_SIZE: u16 = 9;

pub type ComId = [u8; 9];

pub struct ClientInfo {
    pub user_id: i64,
    pub npid: String,
    pub online_name: String,
    pub avatar_url: String,
    pub token: String,
    pub flags: u16,
}

pub struct ClientSignalingInfo {
    pub channel: mpsc::Sender<Vec<u8>>,
    pub addr_p2p: [u8; 4],
    pub port_p2p: u16,
}

impl ClientSignalingInfo {
    pub fn new(channel: mpsc::Sender<Vec<u8>>) -> ClientSignalingInfo {
        ClientSignalingInfo {
            channel,
            addr_p2p: [0; 4],
            port_p2p: 0,
        }
    }
}

pub struct Client {
    config: Arc<RwLock<Config>>,
    tls_reader: io::ReadHalf<TlsStream<TcpStream>>,
    channel_sender: mpsc::Sender<Vec<u8>>,
    db: Arc<Mutex<DatabaseManager>>,
    room_manager: Arc<RwLock<RoomManager>>,
    signaling_infos: Arc<RwLock<HashMap<i64, ClientSignalingInfo>>>,
    authentified: bool,
    client_info: ClientInfo,
    post_reply_notifications: Vec<Vec<u8>>,
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
    SendToken,
    GetServerList,
    GetWorldList,
    CreateRoom,
    JoinRoom,
    LeaveRoom,
    SearchRoom,
    SetRoomDataExternal,
    GetRoomDataInternal,
    SetRoomDataInternal,
    SetRoomMemberDataInternal,
    PingRoomOwner,
    SendRoomMessage,
    RequestSignalingInfos,
    RequestTicket,
    UpdateDomainBans = 0x0100,
}

#[repr(u16)]
enum NotificationType {
    UserJoinedRoom,
    UserLeftRoom,
    RoomDestroyed,
    UpdatedRoomDataInternal,
    UpdatedRoomMemberDataInternal,
    SignalP2PConnect,
    _SignalP2PDisconnect,
    RoomMessageReceived,
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
    InvalidInput,
    ErrorLogin,
    ErrorCreate,
    AlreadyLoggedIn,
    AlreadyJoined,
    DbFail,
    NotFound,
    Unsupported,
}

pub fn com_id_to_string(com_id: &ComId) -> String {
    let mut com_id_str = String::new();
    for c in com_id {
        com_id_str.push(*c as char);
    }

    com_id_str
}

impl Client {
    pub async fn new(
        config: Arc<RwLock<Config>>,
        tls_stream: TlsStream<TcpStream>,
        db: Arc<Mutex<DatabaseManager>>,
        room_manager: Arc<RwLock<RoomManager>>,
        signaling_infos: Arc<RwLock<HashMap<i64, ClientSignalingInfo>>>,
    ) -> Client {
        let client_info = ClientInfo {
            user_id: 0,
            npid: String::new(),
            online_name: String::new(),
            avatar_url: String::new(),
            token: String::new(),
            flags: 0,
        };

        let (channel_sender, mut channel_receiver) = mpsc::channel::<Vec<u8>>(32);
        let (tls_reader, mut tls_writer) = io::split(tls_stream);

        let fut_sock_writer = async move {
            while let Some(outgoing_packet) = channel_receiver.recv().await {
                let _ = tls_writer.write_all(&outgoing_packet).await;
            }
            let _ = tls_writer.shutdown().await;
        };

        tokio::spawn(fut_sock_writer);

        Client {
            config,
            tls_reader,
            channel_sender,
            db,
            room_manager,
            signaling_infos,
            authentified: false,
            client_info,
            post_reply_notifications: Vec::new(),
        }
    }

    pub fn get_users_but_self(&self, users: &HashMap<u16, i64>) -> HashSet<i64> {
        return users.iter().filter_map(|x| if *x.1 != self.client_info.user_id { Some(x.1.clone()) } else { None }).collect();
    }

    pub fn get_room_users_but_self(&self, room: &Room) -> HashSet<i64> {
        let users = room.get_room_users();
        return self.get_users_but_self(&users);
    }

    ///// Logging functions
    #[allow(dead_code)]
    fn dump_packet(&self, packet: &Vec<u8>, source: &str) {
        if !self.config.read().is_verbose() {
            return;
        }
        trace!("Dumping packet({}):", source);
        let mut line = String::new();

        let mut count = 0;
        for p in packet {
            if (count != 0) && (count % 16) == 0 {
                trace!("{}", line);
                line.clear();
            }

            line = format!("{} {:02x}", line, p);
            count += 1;
        }
        trace!("{}", line);
    }

    ///// Command processing
    pub async fn process(&mut self) {
        loop {
            let mut header_data = [0; HEADER_SIZE as usize];

            let r = self.tls_reader.read_exact(&mut header_data).await;

            match r {
                Ok(_) => {
                    if header_data[0] != PacketType::Request as u8 {
                        warn!("Received non request packed, disconnecting client");
                        break;
                    }

                    let command = u16::from_le_bytes([header_data[1], header_data[2]]);
                    let packet_size = u16::from_le_bytes([header_data[3], header_data[4]]);
                    let packet_id = u32::from_le_bytes([header_data[5], header_data[6], header_data[7], header_data[8]]);
                    let npid_span = info_span!("", npid = %self.client_info.npid);
                    if self.interpret_command(command, packet_size, packet_id).instrument(npid_span).await.is_err() {
                        info!("Disconnecting client");
                        break;
                    }
                }
                Err(e) => {
                    info!("Client disconnected: {}", &e);
                    break;
                }
            }
        }

        if self.authentified {
            // leave all rooms user is still in
            let rooms = self.room_manager.read().get_rooms_by_user(self.client_info.user_id);

            if let Some(rooms) = rooms {
                for room in rooms {
                    self.leave_room(&self.room_manager, &room.0, room.1, None, EventCause::MemberDisappeared).await;
                }
            }
            self.signaling_infos.write().remove(&self.client_info.user_id);
        }
    }

    async fn interpret_command(&mut self, command: u16, length: u16, packet_id: u32) -> Result<(), ()> {
        if length < HEADER_SIZE {
            warn!("Malformed packet(size < {})", HEADER_SIZE);
            return Err(());
        }

        let to_read = length - HEADER_SIZE;

        let mut data = vec![0; to_read as usize];

        let r = self.tls_reader.read_exact(&mut data).await;

        match r {
            Ok(_) => {
                //self.dump_packet(&data, "input");

                let mut reply = Vec::with_capacity(1000);

                reply.push(PacketType::Reply as u8);
                reply.extend(&command.to_le_bytes());
                reply.extend(&HEADER_SIZE.to_le_bytes());
                reply.extend(&packet_id.to_le_bytes());

                let mut se_data = StreamExtractor::new(data);
                let res = self.process_command(command, &mut se_data, &mut reply).await;

                // update length
                let len = reply.len() as u16;
                reply[3..5].clone_from_slice(&len.to_le_bytes());

                //self.dump_packet(&reply, "output");

                info!("Returning: {}({})", res.is_ok(), reply[9]);
                let _ = self.channel_sender.send(reply).await;

                // Send post command notifications if any
                for notif in &self.post_reply_notifications {
                    let _ = self.channel_sender.send(notif.clone()).await;
                }
                self.post_reply_notifications.clear();

                return res;
            }
            Err(e) => {
                warn!("Read error: {}", e);
                return Err(());
            }
        }
    }

    async fn process_command(&mut self, command: u16, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<(), ()> {
        let command = FromPrimitive::from_u16(command);
        if command.is_none() {
            warn!("Unknown command received");
            return Err(());
        }

        info!("Parsing command {:?}", command);

        let command = command.unwrap();

        match command {
            CommandType::Terminate => return Err(()),
            _ => {}
        }

        if !self.authentified {
            match command {
                CommandType::Login => return self.login(data, reply),
                CommandType::Create => return self.create_account(data, reply),
                CommandType::SendToken => return self.resend_token(data, reply),
                _ => {
                    warn!("User attempted an invalid command at this stage");
                    reply.push(ErrorType::Invalid as u8);
                    return Err(());
                }
            }
        }

        match command {
            CommandType::GetServerList => return self.req_get_server_list(data, reply),
            CommandType::GetWorldList => return self.req_get_world_list(data, reply),
            CommandType::CreateRoom => return self.req_create_room(data, reply),
            CommandType::JoinRoom => return self.req_join_room(data, reply).await,
            CommandType::LeaveRoom => return self.req_leave_room(data, reply).await,
            CommandType::SearchRoom => return self.req_search_room(data, reply),
            CommandType::SetRoomDataExternal => return self.req_set_roomdata_external(data, reply),
            CommandType::GetRoomDataInternal => return self.req_get_roomdata_internal(data, reply),
            CommandType::SetRoomDataInternal => return self.req_set_roomdata_internal(data, reply).await,
            CommandType::SetRoomMemberDataInternal => return self.req_set_roommemberdata_internal(data, reply).await,
            CommandType::PingRoomOwner => return self.req_ping_room_owner(data, reply),
            CommandType::SendRoomMessage => return self.req_send_room_message(data, reply).await,
            CommandType::RequestSignalingInfos => return self.req_signaling_infos(data, reply),
            CommandType::RequestTicket => return self.req_ticket(data, reply),
            CommandType::UpdateDomainBans => return self.req_admin_update_domain_bans(),
            _ => {
                warn!("Unknown command received");
                reply.push(ErrorType::Invalid as u8);
                return Err(());
            }
        }
    }

    ///// Account management

    fn login(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<(), ()> {
        let login = data.get_string(false);
        let password = data.get_string(false);
        let token = data.get_string(true);

        if data.error() {
            warn!("Error while extracting data from Login command");
            reply.push(ErrorType::Malformed as u8);
            return Err(());
        }

        if let Ok(user_data) = self.db.lock().check_user(&login, &password, &token, true) {
            if self.signaling_infos.read().contains_key(&user_data.user_id) {
                reply.push(ErrorType::AlreadyLoggedIn as u8);
                return Err(());
            }

            self.authentified = true;
            self.client_info.npid = login;
            self.client_info.online_name = user_data.online_name.clone();
            self.client_info.avatar_url = user_data.avatar_url.clone();
            self.client_info.user_id = user_data.user_id;
            self.client_info.token = user_data.token.clone();
            self.client_info.flags = user_data.flags;
            reply.push(ErrorType::NoError as u8);

            reply.extend(user_data.online_name.as_bytes());
            reply.push(0);
            reply.extend(user_data.avatar_url.as_bytes());
            reply.push(0);

            reply.extend(&self.client_info.user_id.to_le_bytes());

            info!("Authentified as {}", &self.client_info.npid);

            self.signaling_infos.write().insert(self.client_info.user_id, ClientSignalingInfo::new(self.channel_sender.clone()));

            return Ok(());
        }

        reply.push(ErrorType::ErrorLogin as u8);

        Err(())
    }

    fn create_account(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<(), ()> {
        let npid = data.get_string(false);
        let password = data.get_string(false);
        let online_name = data.get_string(false);
        let avatar_url = data.get_string(false);
        let email = data.get_string(false);

        if data.error() {
            warn!("Error while extracting data from Create command");
            reply.push(ErrorType::Malformed as u8);
            return Err(());
        }

        if npid.len() < 3 || npid.len() > 16 || !npid.chars().all(|x| x.is_ascii_alphanumeric() || x == '-' || x == '_') {
            warn!("Error validating NpId");
            reply.push(ErrorType::InvalidInput as u8);
            return Err(());
        }

        if online_name.len() < 3 || online_name.len() > 16 || !online_name.chars().all(|x| x.is_alphabetic() || x.is_ascii_digit() || x == '-' || x == '_') {
            warn!("Error validating Online Name");
            reply.push(ErrorType::InvalidInput as u8);
            return Err(());
        }

        let email = email.trim().to_string();

        if EmailAddress::new(email.clone()).is_err() {
            warn!("Invalid email provided: {}", email);
            reply.push(ErrorType::InvalidInput as u8);
            return Err(());
        }

        let mut check_email = email.clone();

        if self.config.read().is_email_validated() {
            let tokens: Vec<&str> = email.split('@').collect();
            // This should not happen as email has been validated above
            if tokens.len() != 2 {
                reply.push(ErrorType::InvalidInput as u8);
                return Err(());
            }
            if self.config.read().is_banned_domain(tokens[1]) {
                warn!("Attempted to use banned domain: {}", email);
                reply.push(ErrorType::InvalidInput as u8);
                return Err(());
            }

            let alias_split: Vec<&str> = tokens[0].split('+').collect();
            if alias_split.len() > 1 {
                check_email = format!("{}%@{}", alias_split[0], tokens[1]);
            }
        }

        if let Ok(token) = self.db.lock().add_user(&npid, &password, &online_name, &avatar_url, &email, &check_email) {
            info!("Successfully created account {}", &npid);
            reply.push(ErrorType::NoError as u8);
            if self.config.read().is_email_validated() {
                if let Err(e) = self.send_token_mail(&email, &npid, &token) {
                    error!("Error sending email: {}", e);
                }
            }
        } else {
            warn!("Account creation failed(npid: {})", &npid);
            reply.push(ErrorType::ErrorCreate as u8);
        }

        Err(()) // this is not an error, we disconnect the client after account creation, successful or not
    }

    fn resend_token(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<(), ()> {
        let login = data.get_string(false);
        let password = data.get_string(false);

        if data.error() {
            warn!("Error while extracting data from Login command");
            reply.push(ErrorType::Malformed as u8);
            return Err(());
        }

        if let Ok(user_data) = self.db.lock().check_user(&login, &password, "", false) {
            if self.config.read().is_email_validated() {
                if let Err(e) = self.send_token_mail(&user_data.email, &login, &user_data.token) {
                    error!("Error sending email: {}", e);
                }
            }
            reply.push(ErrorType::NoError as u8);
        } else {
            reply.push(ErrorType::ErrorLogin as u8);
        }

        Err(())
    }

    ///// Admin stuff
    fn req_admin_update_domain_bans(&self) -> Result<(), ()> {
        if (self.client_info.flags & 1) == 0 {
            return Err(());
        }

        self.config.write().load_domains_banlist();

        Ok(())
    }

    ///// Helper functions
    fn get_com_id_with_redir(&self, data: &mut StreamExtractor) -> ComId {
        let com_id = data.get_com_id();
        self.config.read().get_server_redirection(com_id)
    }

    fn send_token_mail(&self, email_addr: &str, npid: &str, token: &str) -> Result<(), lettre::smtp::error::Error> {
        // Send the email
        let email_to_send = EmailBuilder::new()
            .to((email_addr, npid))
            .from("np@rpcs3.net")
            .subject("Your token for RPCN")
            .text(format!("Your token for username {} is:\n{}", npid, token))
            .build()
            .unwrap();
        let (host, login, password) = self.config.read().get_email_auth();

        let mut smtp_client;
        if host.len() == 0 {
            smtp_client = SmtpClient::new_unencrypted_localhost().unwrap();
        } else {
            smtp_client = SmtpClient::new_simple(&host).unwrap();

            if login.len() != 0 {
                smtp_client = smtp_client
                    .credentials(Credentials::new(login, password))
                    .authentication_mechanism(Mechanism::Plain)
                    .hello_name(lettre::smtp::extension::ClientId::new("np.rpcs3.net".to_string()));
            }
        }

        let mut mailer = SmtpTransport::new(smtp_client);

        mailer.send(email_to_send.into())?;
        Ok(())
    }

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
    async fn send_notification(&self, notif: &Vec<u8>, user_list: &HashSet<i64>) {
        for user_id in user_list {
            let mut channel_copy;
            let entry;
            {
                let sig_infos = self.signaling_infos.read();
                entry = sig_infos.get(user_id);
                if let Some(c) = entry {
                    channel_copy = c.channel.clone();
                } else {
                    continue;
                }
            }

            let _ = channel_copy.send(notif.clone()).await;
        }
    }

    fn self_notification(&mut self, notif: &Vec<u8>) {
        self.post_reply_notifications.push(notif.clone());
    }

    async fn signal_connections(&mut self, room_id: u64, from: (u16, i64), to: HashMap<u16, i64>, sig_param: Option<SignalParam>, owner: u16) {
        if let None = sig_param {
            return;
        }
        let sig_param = sig_param.unwrap();
        if !sig_param.should_signal() {
            return;
        }

        // Get user signaling
        let mut addr_p2p = [0; 4];
        let mut port_p2p = 0;
        {
            let sig_infos = self.signaling_infos.read();
            if let Some(entry) = sig_infos.get(&from.1) {
                addr_p2p = entry.addr_p2p;
                port_p2p = entry.port_p2p;
            }
        }

        // Create a notification to send to other user(s)
        let mut s_msg: Vec<u8> = Vec::new();
        s_msg.extend(&room_id.to_le_bytes()); // +0..+8 room ID
        s_msg.extend(&from.0.to_le_bytes()); // +8..+10 member ID
        s_msg.extend(&port_p2p.to_be_bytes()); // +10..+12 port
        s_msg.extend(&addr_p2p); // +12..+16 addr
        let mut s_notif = Client::create_notification(NotificationType::SignalP2PConnect, &s_msg);

        let mut self_id = HashSet::new();
        self_id.insert(from.1);

        match sig_param.get_type() {
            SignalingType::SignalingMesh => {
                // Notifies other room members that p2p connection was established
                let user_ids: HashSet<i64> = to.iter().map(|x| x.1.clone()).collect();

                self.send_notification(&s_notif, &user_ids).await;

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
                            self.self_notification(&s_notif); // Special function that will post the notification after the reply
                        }
                    }
                }
            }
            SignalingType::SignalingStar => {
                let mut hub = sig_param.get_hub();
                if hub == 0 {
                    hub = owner;
                }
                // Sends notification to hub
                let user_ids: HashSet<i64> = to.iter().filter_map(|x| if *x.0 == hub { Some(x.1.clone()) } else { None }).collect();
                self.send_notification(&s_notif, &user_ids).await;
                // Send notification to user to connect to hub
                let mut tosend = false;
                {
                    let sig_infos = self.signaling_infos.read();
                    let user_si = sig_infos.get(to.get(&hub).unwrap());

                    if let Some(user_si) = user_si {
                        s_notif[(HEADER_SIZE as usize + 8)..(HEADER_SIZE as usize + 10)].clone_from_slice(&hub.to_le_bytes());
                        s_notif[(HEADER_SIZE as usize + 10)..(HEADER_SIZE as usize + 12)].clone_from_slice(&user_si.port_p2p.to_be_bytes());
                        s_notif[(HEADER_SIZE as usize + 12)..(HEADER_SIZE as usize + 16)].clone_from_slice(&user_si.addr_p2p);

                        tosend = true;
                    }
                }
                if tosend {
                    self.self_notification(&s_notif); // Special function that will post the notification after the reply
                }
            }
            _ => panic!("Unimplemented SignalingType({:?})", sig_param.get_type()),
        }
    }

    ///// Server/world retrieval

    fn req_get_server_list(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<(), ()> {
        let com_id = self.get_com_id_with_redir(data);

        if data.error() {
            warn!("Error while extracting data from GetServerList command");
            reply.push(ErrorType::Malformed as u8);
            return Err(());
        }

        // TODO: Generalize this (redirects DeS US queries to EU servers)
        // if com_id == "NPWR00881" {
        //     com_id = String::from("NPWR01249");
        // }

        let servs = self.db.lock().get_server_list(&com_id);
        if let Err(_) = servs {
            reply.push(ErrorType::DbFail as u8);
            return Err(());
        }
        let servs = servs.unwrap();

        reply.push(ErrorType::NoError as u8);

        let num_servs = servs.len() as u16;
        reply.extend(&num_servs.to_le_bytes());
        for serv in servs {
            reply.extend(&serv.to_le_bytes());
        }

        info!("Returning {} servers for comId {}", num_servs, com_id_to_string(&com_id));

        Ok(())
    }
    fn req_get_world_list(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<(), ()> {
        let com_id = self.get_com_id_with_redir(data);
        let server_id = data.get::<u16>();

        if data.error() {
            warn!("Error while extracting data from GetWorldList command");
            reply.push(ErrorType::Malformed as u8);
            return Err(());
        }

        let worlds = self.db.lock().get_world_list(&com_id, server_id);
        if let Err(_) = worlds {
            reply.push(ErrorType::DbFail as u8);
            return Err(());
        }
        let worlds = worlds.unwrap();

        reply.push(ErrorType::NoError as u8);

        let num_worlds = worlds.len() as u32;
        reply.extend(&num_worlds.to_le_bytes());
        for world in worlds {
            reply.extend(&world.to_le_bytes());
        }

        info!("Returning {} worlds", num_worlds);

        Ok(())
    }

    ///// Room commands

    fn req_create_room(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<(), ()> {
        let com_id = self.get_com_id_with_redir(data);
        let create_req = data.get_flatbuffer::<CreateJoinRoomRequest>();

        if data.error() || create_req.is_err() {
            warn!("Error while extracting data from CreateRoom command");
            reply.push(ErrorType::Malformed as u8);
            return Err(());
        }
        let create_req = create_req.unwrap();

        let server_id = self.db.lock().get_corresponding_server(&com_id, create_req.worldId(), create_req.lobbyId()).map_err(|_| {
            warn!(
                "Attempted to use invalid worldId/lobbyId for comId {}: {}/{}",
                &com_id_to_string(&com_id),
                create_req.worldId(),
                create_req.lobbyId()
            );
            reply.push(ErrorType::InvalidInput as u8);
            ()
        })?;

        let resp = self.room_manager.write().create_room(&com_id, &create_req, &self.client_info, server_id);
        reply.push(ErrorType::NoError as u8);
        reply.extend(&(resp.len() as u32).to_le_bytes());
        reply.extend(resp);
        Ok(())
    }
    async fn req_join_room(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<(), ()> {
        let com_id = self.get_com_id_with_redir(data);
        let join_req = data.get_flatbuffer::<JoinRoomRequest>();

        if data.error() || join_req.is_err() {
            warn!("Error while extracting data from JoinRoom command");
            reply.push(ErrorType::Malformed as u8);
            return Err(());
        }
        let join_req = join_req.unwrap();

        let room_id = join_req.roomId();
        let user_ids: HashSet<i64>;
        let (notif, member_id, users, siginfo, owner);
        {
            let mut room_manager = self.room_manager.write();
            if !room_manager.room_exists(&com_id, room_id) {
                warn!("User requested to join a room that doesn't exist!");
                reply.push(ErrorType::InvalidInput as u8);
                return Ok(());
            }
            {
                let room = room_manager.get_room(&com_id, room_id);
                users = room.get_room_users();
                siginfo = room.get_signaling_info();
                owner = room.get_owner();
            }

            if users.iter().any(|x| *x.1 == self.client_info.user_id) {
                warn!("User tried to join a room he was already a member of!");
                reply.push(ErrorType::AlreadyJoined as u8);
                return Ok(());
            }

            let resp = room_manager.join_room(&com_id, &join_req, &self.client_info);
            if let Err(e) = resp {
                warn!("User failed to join the room!");
                reply.push(e);
                return Ok(());
            }

            let (member_id_ta, resp) = resp.unwrap();
            member_id = member_id_ta;
            reply.push(ErrorType::NoError as u8);
            reply.extend(&(resp.len() as u32).to_le_bytes());
            reply.extend(resp);

            user_ids = users.iter().map(|x| x.1.clone()).collect();

            // Notif other room users a new user has joined
            let mut n_msg: Vec<u8> = Vec::new();
            n_msg.extend(&room_id.to_le_bytes());
            let up_info = room_manager
                .get_room(&com_id, room_id)
                .get_room_member_update_info(member_id, EventCause::None, Some(&join_req.optData().unwrap()));
            n_msg.extend(&(up_info.len() as u32).to_le_bytes());
            n_msg.extend(up_info);
            notif = Client::create_notification(NotificationType::UserJoinedRoom, &n_msg);
        }
        self.send_notification(&notif, &user_ids).await;

        // Send signaling stuff if any
        self.signal_connections(room_id, (member_id, self.client_info.user_id), users, siginfo, owner).await;

        Ok(())
    }
    async fn leave_room(&self, room_manager: &Arc<RwLock<RoomManager>>, com_id: &ComId, room_id: u64, opt_data: Option<&PresenceOptionData<'_>>, event_cause: EventCause) -> u8 {
        let (destroyed, users, user_data);
        {
            let mut room_manager = room_manager.write();
            if !room_manager.room_exists(com_id, room_id) {
                return ErrorType::NotFound as u8;
            }

            let room = room_manager.get_room(com_id, room_id);
            let member_id = room.get_member_id(self.client_info.user_id);
            if let Err(e) = member_id {
                return e;
            }

            // We get this in advance in case the room is not destroyed
            user_data = room.get_room_member_update_info(member_id.unwrap(), event_cause.clone(), opt_data);

            let res = room_manager.leave_room(com_id, room_id, self.client_info.user_id.clone());
            if let Err(e) = res {
                return e;
            }
            let (destroyed_toa, users_toa) = res.unwrap();
            destroyed = destroyed_toa;
            users = users_toa;
        }

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
            self.send_notification(&notif, &users).await;
        } else {
            // Notify other room users that someone left the room
            let mut n_msg: Vec<u8> = Vec::new();
            n_msg.extend(&room_id.to_le_bytes());
            n_msg.extend(&(user_data.len() as u32).to_le_bytes());
            n_msg.extend(&user_data);

            let notif = Client::create_notification(NotificationType::UserLeftRoom, &n_msg);
            self.send_notification(&notif, &users).await;
        }

        ErrorType::NoError as u8
    }

    async fn req_leave_room(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<(), ()> {
        let com_id = self.get_com_id_with_redir(data);
        let leave_req = data.get_flatbuffer::<LeaveRoomRequest>();

        if data.error() || leave_req.is_err() {
            warn!("Error while extracting data from SearchRoom command");
            reply.push(ErrorType::Malformed as u8);
            return Err(());
        }
        let leave_req = leave_req.unwrap();

        reply.push(
            self.leave_room(&self.room_manager, &com_id, leave_req.roomId(), Some(&leave_req.optData().unwrap()), EventCause::LeaveAction)
                .await,
        );
        reply.extend(&leave_req.roomId().to_le_bytes());
        Ok(())
    }
    fn req_search_room(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<(), ()> {
        let com_id = self.get_com_id_with_redir(data);
        let search_req = data.get_flatbuffer::<SearchRoomRequest>();

        if data.error() || search_req.is_err() {
            warn!("Error while extracting data from SearchRoom command");
            reply.push(ErrorType::Malformed as u8);
            return Err(());
        }
        let search_req = search_req.unwrap();

        let resp = self.room_manager.read().search_room(&com_id, &search_req);
        reply.push(ErrorType::NoError as u8);
        reply.extend(&(resp.len() as u32).to_le_bytes());
        reply.extend(resp);
        Ok(())
    }
    fn req_set_roomdata_external(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<(), ()> {
        let com_id = self.get_com_id_with_redir(data);
        let setdata_req = data.get_flatbuffer::<SetRoomDataExternalRequest>();

        if data.error() || setdata_req.is_err() {
            warn!("Error while extracting data from SetRoomDataExternal command");
            reply.push(ErrorType::Malformed as u8);
            return Err(());
        }
        let setdata_req = setdata_req.unwrap();

        if let Err(e) = self.room_manager.write().set_roomdata_external(&com_id, &setdata_req) {
            reply.push(e);
        } else {
            reply.push(ErrorType::NoError as u8);
        }
        Ok(())
    }
    fn req_get_roomdata_internal(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<(), ()> {
        let com_id = self.get_com_id_with_redir(data);
        let getdata_req = data.get_flatbuffer::<GetRoomDataInternalRequest>();

        if data.error() || getdata_req.is_err() {
            warn!("Error while extracting data from GetRoomDataInternal command");
            reply.push(ErrorType::Malformed as u8);
            return Err(());
        }
        let getdata_req = getdata_req.unwrap();

        let resp = self.room_manager.read().get_roomdata_internal(&com_id, &getdata_req);
        if let Err(e) = resp {
            reply.push(e);
        } else {
            let resp = resp.unwrap();
            reply.push(ErrorType::NoError as u8);
            reply.extend(&(resp.len() as u32).to_le_bytes());
            reply.extend(resp);
        }
        Ok(())
    }
    async fn req_set_roomdata_internal(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<(), ()> {
        let com_id = self.get_com_id_with_redir(data);
        let setdata_req = data.get_flatbuffer::<SetRoomDataInternalRequest>();

        if data.error() || setdata_req.is_err() {
            warn!("Error while extracting data from SetRoomDataInternal command");
            reply.push(ErrorType::Malformed as u8);
            return Err(());
        }
        let setdata_req = setdata_req.unwrap();

        let room_id = setdata_req.roomId();
        {
            let mut room_manager = self.room_manager.write();
            if !room_manager.room_exists(&com_id, room_id) {
                warn!("User requested SetRoomDataInternal for room that doesn't exist!");
                reply.push(ErrorType::InvalidInput as u8);
                return Ok(());
            }
            if let Err(e) = room_manager.set_roomdata_internal(&com_id, &setdata_req) {
                reply.push(e);
            } else {
                reply.push(ErrorType::NoError as u8);
            }
        }

        let mut builder = flatbuffers::FlatBufferBuilder::new_with_capacity(1024);
        let user_ids;
        {
            let room_manager = self.room_manager.read();
            let room = room_manager.get_room(&com_id, room_id.clone());
            user_ids = self.get_room_users_but_self(&room);

            let mut new_room_group = None;
            if room.group_config.len() != 0 {
                let mut group_list = Vec::new();
                for group in &room.group_config {
                    group_list.push(group.to_flatbuffer(&mut builder));
                }
                new_room_group = Some(builder.create_vector(&group_list));
            }

            let mut final_internalbinattr = None;

            // dbg!(room.bin_attr_internal.len());
            if room.bin_attr_internal.len() != 0 {
                let mut bin_list = Vec::new();
                for bin in &room.bin_attr_internal {
                    bin_list.push(bin.to_flatbuffer(&mut builder));
                }
                final_internalbinattr = Some(builder.create_vector(&bin_list));
            }

            let room_data_internal = room.to_RoomDataInternal(&mut builder);
            let resp = RoomDataInternalUpdateInfo::create(
                &mut builder,
                &RoomDataInternalUpdateInfoArgs {
                    newRoomDataInternal: Some(room_data_internal),
                    newFlagAttr: setdata_req.flagAttr(),
                    prevFlagAttr: room.flag_attr,
                    newRoomPasswordSlotMask: setdata_req.passwordSlotMask(),
                    prevRoomPasswordSlotMask: room.password_slot_mask,
                    newRoomGroup: new_room_group,
                    newRoomBinAttrInternal: final_internalbinattr,
                },
            );
            builder.finish(resp, None);
        }

        let finished_data = builder.finished_data().to_vec();

        let mut n_msg: Vec<u8> = Vec::new();
        n_msg.extend(&room_id.to_le_bytes());
        n_msg.extend(&(finished_data.len() as u32).to_le_bytes());
        n_msg.extend(finished_data);
        let notif = Client::create_notification(NotificationType::UpdatedRoomDataInternal, &n_msg);

        self.send_notification(&notif, &user_ids).await;
        self.self_notification(&notif);

        Ok(())
    }
    async fn req_set_roommemberdata_internal(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<(), ()> {
        let com_id = self.get_com_id_with_redir(data);
        let setdata_req = data.get_flatbuffer::<SetRoomMemberDataInternalRequest>();

        if data.error() || setdata_req.is_err() {
            warn!("Error while extracting data from SetRoomMemberDataInternal command");
            reply.push(ErrorType::Malformed as u8);
            return Err(());
        }
        let setdata_req = setdata_req.unwrap();

        let mut member_id = setdata_req.memberId();
        let room_id = setdata_req.roomId();
        {
            let mut room_manager = self.room_manager.write();
            if !room_manager.room_exists(&com_id, room_id) {
                warn!("User requested SetRoomMemberDataInternal for room that doesn't exist!");
                reply.push(ErrorType::InvalidInput as u8);
                return Ok(());
            }

            let room = room_manager.get_room(&com_id, room_id.clone());
            if member_id == 0 {
                let member_id_res = room.get_member_id(self.client_info.user_id);
                if member_id_res.is_err() {
                    warn!("Couldn't find memberId of user that called SetRoomMemberDataInternal");
                    reply.push(ErrorType::InvalidInput as u8);
                    return Ok(());
                }
                member_id = member_id_res.unwrap();
            }

            if let Err(e) = room_manager.set_roommemberdata_internal(&com_id, &setdata_req, member_id) {
                reply.push(e);
            } else {
                reply.push(ErrorType::NoError as u8);
            }
        }

        let mut builder = flatbuffers::FlatBufferBuilder::new_with_capacity(1024);
        let user_ids;
        {
            let room_manager = self.room_manager.read();
            let room = room_manager.get_room(&com_id, room_id.clone());
            let user = room.users.get(&member_id).unwrap();

            let member_internal = user.to_RoomMemberDataInternal(&mut builder);

            user_ids = self.get_room_users_but_self(&room);

            let mut bin_attr = None;
            if user.member_attr.len() != 0 {
                let mut bin_attrs = Vec::new();
                for i in 0..user.member_attr.len() {
                    bin_attrs.push(user.member_attr[i].to_flatbuffer(&mut builder));
                }
                bin_attr = Some(builder.create_vector(&bin_attrs));
            }

            let resp = RoomMemberDataInternalUpdateInfo::create(
                &mut builder,
                &RoomMemberDataInternalUpdateInfoArgs {
                    newRoomMemberDataInternal: Some(member_internal),
                    newFlagAttr: setdata_req.flagAttr(),
                    prevFlagAttr: room.flag_attr,
                    newTeamId: user.team_id,
                    newRoomMemberBinAttrInternal: bin_attr,
                },
            );
            builder.finish(resp, None);
        }

        let finished_data = builder.finished_data().to_vec();

        let mut n_msg: Vec<u8> = Vec::new();
        n_msg.extend(&room_id.to_le_bytes());
        n_msg.extend(&(finished_data.len() as u32).to_le_bytes());
        n_msg.extend(finished_data);
        let notif = Client::create_notification(NotificationType::UpdatedRoomMemberDataInternal, &n_msg);

        self.send_notification(&notif, &user_ids).await;
        self.self_notification(&notif);

        Ok(())
    }
    fn req_ping_room_owner(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<(), ()> {
        let com_id = self.get_com_id_with_redir(data);
        let room_id = data.get::<u64>();
        if data.error() {
            warn!("Error while extracting data from PingRoomOwner command");
            reply.push(ErrorType::Malformed as u8);
            return Err(());
        }

        let infos = self.room_manager.read().get_room_infos(&com_id, room_id);
        if let Err(e) = infos {
            reply.push(e);
            return Ok(());
        }
        let (server_id, world_id, _) = infos.unwrap();

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

        Ok(())
    }
    async fn req_send_room_message(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<(), ()> {
        let com_id = self.get_com_id_with_redir(data);
        let msg_req = data.get_flatbuffer::<SendRoomMessageRequest>();

        if data.error() || msg_req.is_err() {
            warn!("Error while extracting data from SendRoomMessage command");
            reply.push(ErrorType::Malformed as u8);
            return Err(());
        }
        let msg_req = msg_req.unwrap();

        let room_id = msg_req.roomId();
        let (notif, member_id, users);
        let mut dst_vec: Vec<u16> = Vec::new();
        {
            let room_manager = self.room_manager.read();
            if !room_manager.room_exists(&com_id, room_id) {
                warn!("User requested to send a message to a room that doesn't exist!");
                reply.push(ErrorType::InvalidInput as u8);
                return Ok(());
            }
            {
                let room = room_manager.get_room(&com_id, room_id.clone());
                let m_id = room.get_member_id(self.client_info.user_id);
                if m_id.is_err() {
                    warn!("User requested to send a message to a room that he's not a member of!");
                    reply.push(ErrorType::InvalidInput as u8);
                    return Ok(());
                }
                member_id = m_id.unwrap();
                users = room.get_room_users();
            }

            let mut builder = flatbuffers::FlatBufferBuilder::new_with_capacity(1024);

            if let Some(dst) = msg_req.dst() {
                for i in 0..dst.len() {
                    dst_vec.push(dst.get(i));
                }
            }
            let dst = Some(builder.create_vector(&dst_vec));

            let mut npid = None;
            if (msg_req.option() & 0x01) != 0 {
                npid = Some(builder.create_string(&self.client_info.npid));
            }
            let mut online_name = None;
            if (msg_req.option() & 0x02) != 0 {
                online_name = Some(builder.create_string(&self.client_info.online_name));
            }
            let mut avatar_url = None;
            if (msg_req.option() & 0x04) != 0 {
                avatar_url = Some(builder.create_string(&self.client_info.avatar_url));
            }

            let src_user_info = UserInfo2::create(
                &mut builder,
                &UserInfo2Args {
                    npId: npid,
                    onlineName: online_name,
                    avatarUrl: avatar_url,
                },
            );

            let mut msg_vec: Vec<u8> = Vec::new();
            if let Some(msg) = msg_req.msg() {
                for i in 0..msg.len() {
                    msg_vec.push(*msg.get(i).unwrap());
                }
            }
            let msg = Some(builder.create_vector(&msg_vec));

            let resp = RoomMessageInfo::create(
                &mut builder,
                &RoomMessageInfoArgs {
                    filtered: false,
                    castType: msg_req.castType(),
                    dst,
                    srcMember: Some(src_user_info),
                    msg,
                },
            );
            builder.finish(resp, None);
            let finished_data = builder.finished_data().to_vec();

            let mut n_msg: Vec<u8> = Vec::new();
            n_msg.extend(&room_id.to_le_bytes());
            n_msg.extend(&member_id.to_le_bytes());
            n_msg.extend(&(finished_data.len() as u32).to_le_bytes());
            n_msg.extend(finished_data);
            notif = Client::create_notification(NotificationType::RoomMessageReceived, &n_msg);
        }

        match msg_req.castType() {
            1 => {
                // SCE_NP_MATCHING2_CASTTYPE_BROADCAST
                let user_ids = self.get_users_but_self(&users);
                self.send_notification(&notif, &user_ids).await;
                self.self_notification(&notif);
            }
            2 | 3 => {
                // SCE_NP_MATCHING2_CASTTYPE_UNICAST & SCE_NP_MATCHING2_CASTTYPE_MULTICAST
                let mut found_self = false;
                let user_ids: HashSet<i64> = users
                    .iter()
                    .filter_map(|x| {
                        if !dst_vec.iter().any(|dst| *dst == *x.0) {
                            None
                        } else if *x.1 != self.client_info.user_id {
                            Some(x.1.clone())
                        } else {
                            found_self = true;
                            None
                        }
                    })
                    .collect();
                self.send_notification(&notif, &user_ids).await;
                if found_self {
                    self.self_notification(&notif);
                };
            }
            4 => {
                // SCE_NP_MATCHING2_CASTTYPE_MULTICAST_TEAM
                reply.push(ErrorType::Unsupported as u8);
                return Ok(());
            }
            _ => {
                warn!("Invalid broadcast type in send_room_message!");
                reply.push(ErrorType::InvalidInput as u8);
                return Err(()); // This shouldn't happen, closing connection
            }
        }

        reply.push(ErrorType::NoError as u8);
        Ok(())
    }
    fn req_signaling_infos(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<(), ()> {
        let npid = data.get_string(false);
        if data.error() || npid.len() > 16 {
            warn!("Error while extracting data from RequestSignalingInfos command");
            reply.push(ErrorType::Malformed as u8);
            return Err(());
        }

        let user_id = self.db.lock().get_user_id(&npid);
        if user_id.is_err() {
            reply.push(ErrorType::NotFound as u8);
            return Ok(());
        }

        let user_id = user_id.unwrap();
        let sig_infos = self.signaling_infos.read();
        if let Some(entry) = sig_infos.get(&user_id) {
            reply.push(ErrorType::NoError as u8);
            reply.extend(&entry.addr_p2p);
            reply.extend(&((entry.port_p2p).to_le_bytes()));
            info!("Requesting signaling infos for {} => {:?}:{}", &npid, &entry.addr_p2p, entry.port_p2p);
        } else {
            reply.push(ErrorType::NotFound as u8);
        }

        Ok(())
    }
    fn req_ticket(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<(), ()> {
        let service_id = data.get_string(false);
        if data.error() {
            warn!("Error while extracting data from RequestTicket command");
            reply.push(ErrorType::Malformed as u8);
            return Err(());
        }

        info!("Requested a ticket for <{}>", service_id);

        let ticket = Ticket::new(self.client_info.user_id as u64, &self.client_info.npid, &service_id);
        let ticket_blob = ticket.generate_blob();

        reply.push(ErrorType::NoError as u8);
        reply.extend(&(ticket_blob.len() as u32).to_le_bytes());
        reply.extend(ticket_blob);

        Ok(())
    }
}
