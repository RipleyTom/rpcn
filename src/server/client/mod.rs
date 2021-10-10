mod cmd_account;
mod cmd_admin;
mod cmd_friend;
mod cmd_room;
mod cmd_server;

mod ticket;

mod notifications;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::SystemTime;

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

use crate::server::client::notifications::NotificationType;
use crate::server::client::ticket::Ticket;
use crate::server::database::DatabaseManager;
use crate::server::room_manager::{RoomManager, SignalParam, SignalingType};
use crate::server::stream_extractor::fb_helpers::*;
use crate::server::stream_extractor::np2_structs_generated::*;
use crate::server::stream_extractor::StreamExtractor;
use crate::Config;

pub const HEADER_SIZE: u16 = 13;

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
	pub friends: HashSet<i64>,
}

impl ClientSignalingInfo {
	pub fn new(channel: mpsc::Sender<Vec<u8>>, friends: HashSet<i64>) -> ClientSignalingInfo {
		ClientSignalingInfo {
			channel,
			addr_p2p: [0; 4],
			port_p2p: 0,
			friends,
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
	AddFriend,
	RemoveFriend,
	AddBlock,
	RemoveBlock,
	GetServerList,
	GetWorldList,
	CreateRoom,
	JoinRoom,
	LeaveRoom,
	SearchRoom,
	GetRoomDataExternalList,
	SetRoomDataExternal,
	GetRoomDataInternal,
	SetRoomDataInternal,
	PingRoomOwner,
	SendRoomMessage,
	RequestSignalingInfos,
	RequestTicket,
	SendMessage,
	UpdateDomainBans = 0x0100,
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
	NoError,                     // No error
	Malformed,                   // Query was malformed, critical error that should close the connection
	Invalid,                     // The request type is invalid(wrong stage?)
	InvalidInput,                // The Input doesn't fit the constraints of the request
	TooSoon,                     // Time limited operation attempted too soon
	LoginError,                  // An error happened related to login
	LoginAlreadyLoggedIn,        // Can't log in because you're already logged in
	LoginInvalidUsername,        // Invalid username
	LoginInvalidPassword,        // Invalid password
	LoginInvalidToken,           // Invalid token
	CreationError,               // An error happened related to account creation
	CreationExistingUsername,    // Specific
	CreationBannedEmailProvider, // Specific to Account Creation: the email provider is banned
	CreationExistingEmail,       // Specific to Account Creation: that email is already registered to an account
	AlreadyJoined,               // User tried to join a room he's already part of
	DbFail,                      // Generic failure on db side
	EmailFail,                   // Generic failure related to email
	NotFound,                    // Object of the query was not found(room, user, etc)
	Blocked,                     // The operation can't complete because you've been blocked
	AlreadyFriend,               // Can't add friend because already friend
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

	// Logging Functions
	#[allow(dead_code)]
	fn dump_packet(&self, packet: &Vec<u8>, source: &str) {
		if *self.config.read().get_verbosity() != tracing::Level::TRACE {
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

	///// Command Processing
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
					let packet_id = u64::from_le_bytes([header_data[5], header_data[6], header_data[7], header_data[8], header_data[9], header_data[10], header_data[11], header_data[12]]);
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

			// Notify friends that the user went offline and remove him from signaling infos
			let mut to_notif: HashSet<i64> = HashSet::new();
			let timestamp;
			{
				let mut sign_infos = self.signaling_infos.write();
				timestamp = Client::get_timestamp();
				if let Some(infos) = sign_infos.get(&self.client_info.user_id) {
					to_notif = infos.friends.clone();
				}
				sign_infos.remove(&self.client_info.user_id);
			}

			let notif = Client::create_friend_status_notification(&self.client_info.npid, timestamp, false);
			self.send_notification(&notif, &to_notif).await;
		}
	}

	async fn interpret_command(&mut self, command: u16, length: u16, packet_id: u64) -> Result<(), ()> {
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

				info!("Returning: {}({})", res.is_ok(), reply[HEADER_SIZE as usize]);
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
				CommandType::Login => return self.login(data, reply).await,
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
			CommandType::AddFriend => return self.add_friend(data, reply).await,
			CommandType::RemoveFriend => return self.remove_friend(data, reply).await,
			CommandType::AddBlock => return self.add_block(data, reply),
			CommandType::RemoveBlock => return self.remove_block(data, reply),
			CommandType::GetServerList => return self.req_get_server_list(data, reply),
			CommandType::GetWorldList => return self.req_get_world_list(data, reply),
			CommandType::CreateRoom => return self.req_create_room(data, reply),
			CommandType::JoinRoom => return self.req_join_room(data, reply).await,
			CommandType::LeaveRoom => return self.req_leave_room(data, reply).await,
			CommandType::SearchRoom => return self.req_search_room(data, reply),
			CommandType::GetRoomDataExternalList => return self.req_get_roomdata_external_list(data, reply),
			CommandType::SetRoomDataExternal => return self.req_set_roomdata_external(data, reply),
			CommandType::GetRoomDataInternal => return self.req_get_roomdata_internal(data, reply),
			CommandType::SetRoomDataInternal => return self.req_set_roomdata_internal(data, reply),
			CommandType::PingRoomOwner => return self.req_ping_room_owner(data, reply),
			CommandType::SendRoomMessage => return self.req_send_room_message(data, reply).await,
			CommandType::RequestSignalingInfos => return self.req_signaling_infos(data, reply),
			CommandType::RequestTicket => return self.req_ticket(data, reply),
			CommandType::SendMessage => return self.send_message(data, reply).await,
			CommandType::UpdateDomainBans => return self.req_admin_update_domain_bans(),
			_ => {
				warn!("Unknown command received");
				reply.push(ErrorType::Invalid as u8);
				return Err(());
			}
		}
	}

	// Helper Functions

	pub fn get_timestamp() -> u64 {
		SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64
	}
	pub fn get_timestamp_seconds() -> u32 {
		SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs() as u32
	}

	fn get_com_id_with_redir(&self, data: &mut StreamExtractor) -> ComId {
		let com_id = data.get_com_id();
		self.config.read().get_server_redirection(com_id)
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

	// Misc (might get split in another file later)

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
	async fn send_message(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<(), ()> {
		let sendmessage_req = data.get_flatbuffer::<SendMessageRequest>();
		if data.error() || sendmessage_req.is_err() {
			warn!("Error while extracting data from SendMessage command");
			reply.push(ErrorType::Malformed as u8);
			return Err(());
		}
		let sendmessage_req = sendmessage_req.unwrap();

		let message = sendmessage_req.message();
		let npids = sendmessage_req.npids();
		if message.is_none() || npids.is_none() {
			warn!("Error while extracting data from SendMessage command(missing message or npids)");
			reply.push(ErrorType::Malformed as u8);
			return Err(());
		}
		let message = message.unwrap();
		let npids = npids.unwrap();

		// Get all the IDs
		let mut ids = HashSet::new();
		{
			let mut db_lock = self.db.lock();
			for npid in &npids {
				match db_lock.get_user_id(npid) {
					Ok(id) => {
						ids.insert(id);
					}
					Err(_) => {
						warn!("Requested to send a message to invalid npid: {}", npid);
						reply.push(ErrorType::InvalidInput as u8);
						return Ok(());
					}
				}
			}
		}

		// Can't message self
		if ids.is_empty() || ids.contains(&self.client_info.user_id) {
			warn!("Requested to send a message to empty set or self!");
			reply.push(ErrorType::InvalidInput as u8);
			return Ok(());
		}

		// Ensure all recipients are friends(TODO: might not be necessary for all messages?)
		{
			let sig_infos = self.signaling_infos.read();
			let user_si = sig_infos.get(&self.client_info.user_id).unwrap();
			if !user_si.friends.is_superset(&ids) {
				warn!("Requested to send a message to a non-friend!");
				reply.push(ErrorType::InvalidInput as u8);
				return Ok(());
			}
		}

		// Finally send the notifications
		let mut n_msg: Vec<u8> = Vec::new();
		n_msg.extend(self.client_info.npid.as_bytes());
		n_msg.push(0);
		n_msg.extend(&(message.len() as u32).to_le_bytes());
		n_msg.extend(message);
		let notif = Client::create_notification(NotificationType::MessageReceived, &n_msg);
		self.send_notification(&notif, &ids).await;

		reply.push(ErrorType::NoError as u8);

		Ok(())
	}
}
