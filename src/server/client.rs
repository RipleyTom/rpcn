mod cmd_account;
mod cmd_admin;
mod cmd_friend;
mod cmd_misc;
mod cmd_room;
mod cmd_room_gui;
mod cmd_score;
mod cmd_server;
mod cmd_session;
pub mod cmd_tus;

mod ticket;

pub mod notifications;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use lettre::transport::smtp::authentication::{Credentials, Mechanism};
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use parking_lot::{Mutex, RwLock};
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};
use tokio::task;
use tokio::time::timeout;
use tokio_rustls::server::TlsStream;
use tracing::{error, info, info_span, trace, warn, Instrument};

use crate::server::client::cmd_session::ClientSharedSessionInfo;
use crate::server::client::notifications::NotificationType;
use crate::server::client::ticket::Ticket;
use crate::server::database::Database;
use crate::server::game_tracker::GameTracker;
use crate::server::gui_room_manager::GuiRoomManager;
use crate::server::room_manager::{RoomManager, SignalParam, SignalingType};
use crate::server::score_cache::ScoresCache;
use crate::server::stream_extractor::fb_helpers::*;
use crate::server::stream_extractor::np2_structs_generated::*;
use crate::server::stream_extractor::StreamExtractor;
use crate::Config;

pub const HEADER_SIZE: u32 = 15;
pub const MAX_PACKET_SIZE: u32 = 0x800000; // 8MiB

pub const COMMUNICATION_ID_SIZE: usize = 12;
pub type ComId = [u8; COMMUNICATION_ID_SIZE];

#[derive(Clone)]
pub struct ClientInfo {
	pub user_id: i64,
	pub npid: String,
	pub online_name: String,
	pub avatar_url: String,
	pub token: String,
	pub admin: bool,
	pub stat_agent: bool,
	pub banned: bool,
}

pub struct ClientSharedSignalingInfo {
	pub addr_p2p: [u8; 4],
	pub port_p2p: u16,
	pub local_addr_p2p: [u8; 4],
}

impl ClientSharedSignalingInfo {
	fn new() -> ClientSharedSignalingInfo {
		ClientSharedSignalingInfo {
			addr_p2p: [0; 4],
			port_p2p: 0,
			local_addr_p2p: [0; 4],
		}
	}
}

#[derive(Clone)]
pub struct ClientSharedPresence {
	pub communication_id: ComId,
	pub title: String,
	pub status: String,
	pub comment: String,
	pub data: Vec<u8>,
}

impl ClientSharedPresence {
	pub fn new(communication_id: ComId, title: &str, status: Option<&str>, comment: Option<&str>, data: Option<&[u8]>) -> ClientSharedPresence {
		ClientSharedPresence {
			communication_id,
			title: title.to_string(),
			status: status.map(|s| s.to_string()).unwrap_or_default(),
			comment: comment.map(|s| s.to_string()).unwrap_or_default(),
			data: data.map(|d| d.to_vec()).unwrap_or_default(),
		}
	}

	pub fn dump(&self, vec: &mut Vec<u8>) {
		vec.extend(self.communication_id);
		vec.extend(self.title.as_bytes());
		vec.push(0);
		vec.extend(self.status.as_bytes());
		vec.push(0);
		vec.extend(self.comment.as_bytes());
		vec.push(0);
		vec.extend((self.data.len() as u32).to_le_bytes());
		vec.extend(&self.data);
	}

	pub fn dump_empty(vec: &mut Vec<u8>) {
		const VEC_EMPTY_COMID: [u8; COMMUNICATION_ID_SIZE] = [0, 0, 0, 0, 0, 0, 0, 0, 0, b'_', b'0', b'0'];
		vec.extend(VEC_EMPTY_COMID); // com_id
		vec.push(0); // title
		vec.push(0); // status string
		vec.push(0); // comment
		vec.extend(0u32.to_le_bytes()); // data
	}
}

pub struct ClientSharedFriendInfo {
	pub friends: HashMap<i64, String>,
	pub presence: Option<ClientSharedPresence>,
}

impl ClientSharedFriendInfo {
	fn new(friends: HashMap<i64, String>) -> ClientSharedFriendInfo {
		ClientSharedFriendInfo { friends, presence: None }
	}
}

pub struct ClientSharedInfo {
	pub signaling_info: RwLock<ClientSharedSignalingInfo>,
	pub friend_info: RwLock<ClientSharedFriendInfo>,
	pub session_info: RwLock<ClientSharedSessionInfo>,
	pub channel: mpsc::Sender<Vec<u8>>,
}

impl ClientSharedInfo {
	pub fn new(friends: HashMap<i64, String>, channel: mpsc::Sender<Vec<u8>>) -> ClientSharedInfo {
		ClientSharedInfo {
			signaling_info: RwLock::new(ClientSharedSignalingInfo::new()),
			friend_info: RwLock::new(ClientSharedFriendInfo::new(friends)),
			session_info: RwLock::new(ClientSharedSessionInfo::new()),
			channel,
		}
	}
}

#[derive(Clone)]
pub struct TerminateWatch {
	pub recv: watch::Receiver<bool>,
	pub send: Arc<Mutex<watch::Sender<bool>>>,
}

impl TerminateWatch {
	pub fn new(recv: watch::Receiver<bool>, send: watch::Sender<bool>) -> TerminateWatch {
		TerminateWatch {
			recv,
			send: Arc::new(Mutex::new(send)),
		}
	}
}

#[derive(Clone)]
pub struct SharedData {
	gui_room_manager: Arc<RwLock<GuiRoomManager>>,
	room_manager: Arc<RwLock<RoomManager>>,
	client_infos: Arc<RwLock<HashMap<i64, ClientSharedInfo>>>,
	score_cache: Arc<ScoresCache>,
	game_tracker: Arc<GameTracker>,
	cleanup_duty: Arc<RwLock<HashSet<i64>>>,
}

#[derive(Clone)]
pub struct Client {
	config: Arc<RwLock<Config>>,
	channel_sender: mpsc::Sender<Vec<u8>>,
	db_pool: r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
	shared: SharedData,
	authentified: bool,
	client_info: ClientInfo,
	post_reply_notifications: Vec<Vec<u8>>,
	terminate_watch: TerminateWatch,
	current_game: (Option<ComId>, Option<String>),
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
	SendResetToken,
	ResetPassword,
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
	GetRoomMemberDataInternal,
	SetRoomMemberDataInternal,
	SetUserInfo,
	PingRoomOwner,
	SendRoomMessage,
	RequestSignalingInfos,
	RequestTicket,
	SendMessage,
	GetBoardInfos,
	RecordScore,
	RecordScoreData,
	GetScoreData,
	GetScoreRange,
	GetScoreFriends,
	GetScoreNpid,
	GetNetworkTime,
	TusSetMultiSlotVariable,
	TusGetMultiSlotVariable,
	TusGetMultiUserVariable,
	TusGetFriendsVariable,
	TusAddAndGetVariable,
	TusTryAndSetVariable,
	TusDeleteMultiSlotVariable,
	TusSetData,
	TusGetData,
	TusGetMultiSlotDataStatus,
	TusGetMultiUserDataStatus,
	TusGetFriendsDataStatus,
	TusDeleteMultiSlotData,
	ClearPresence,
	SetPresence,
	CreateRoomGUI,
	JoinRoomGUI,
	LeaveRoomGUI,
	GetRoomListGUI,
	SetRoomSearchFlagGUI,
	GetRoomSearchFlagGUI,
	SetRoomInfoGUI,
	GetRoomInfoGUI,
	QuickMatchGUI,
	SearchJoinRoomGUI,
	UpdateDomainBans = 0x0100,
	TerminateServer,
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
#[derive(Clone, Copy, Debug)]
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
	CreationExistingUsername,    // Specific to Account Creation: username exists already
	CreationBannedEmailProvider, // Specific to Account Creation: the email provider is banned
	CreationExistingEmail,       // Specific to Account Creation: that email is already registered to an account
	RoomMissing,                 // User tried to join a non existing room
	RoomAlreadyJoined,           // User tried to join a room he's already part of
	RoomFull,                    // User tried to join a full room
	Unauthorized,                // User attempted an unauthorized operation
	DbFail,                      // Generic failure on db side
	EmailFail,                   // Generic failure related to email
	NotFound,                    // Object of the query was not found(room, user, etc)
	Blocked,                     // The operation can't complete because you've been blocked
	AlreadyFriend,               // Can't add friend because already friend
	ScoreNotBest,                // A better score is already registered for that user/character_id
	ScoreInvalid,                // Score for player was found but wasn't what was expected
	ScoreHasData,                // Score already has data
	CondFail,                    // Condition related to query failed
	Unsupported,
}

pub fn com_id_to_string(com_id: &ComId) -> String {
	com_id.iter().map(|c| *c as char).collect()
}

impl SharedData {
	pub fn new(
		gui_room_manager: Arc<RwLock<GuiRoomManager>>,
		room_manager: Arc<RwLock<RoomManager>>,
		client_infos: Arc<RwLock<HashMap<i64, ClientSharedInfo>>>,
		score_cache: Arc<ScoresCache>,
		game_tracker: Arc<GameTracker>,
		cleanup_duty: Arc<RwLock<HashSet<i64>>>,
	) -> SharedData {
		SharedData {
			gui_room_manager,
			room_manager,
			client_infos,
			score_cache,
			game_tracker,
			cleanup_duty,
		}
	}
}

impl Drop for Client {
	fn drop(&mut self) {
		// This code is here instead of at the end of process in case of thread panic to ensure user is properly 'cleaned out' of the system
		if self.authentified {
			self.shared.cleanup_duty.write().insert(self.client_info.user_id);
			let mut self_clone = Box::new(self.clone()); // We box the clone in order to avoid stack issues when a lot of users drop(shutdown)
			task::spawn(async move {
				// leave all rooms user is still in
				let rooms = self_clone.shared.room_manager.read().get_rooms_by_user(self_clone.client_info.user_id);

				if let Some(rooms) = rooms {
					for (com_id, room_id) in rooms {
						self_clone.leave_room(&com_id, room_id, None, EventCause::MemberDisappeared).await;
					}
				}

				let rooms = self_clone.shared.gui_room_manager.read().get_rooms_by_user(self_clone.client_info.user_id);

				if let Some(rooms) = rooms {
					for room in rooms {
						let _ = self_clone.leave_room_gui(&room).await;
					}
				}

				// Notify friends that the user went offline and remove him from signaling infos
				let mut to_notif: HashSet<i64> = HashSet::new();
				let timestamp;
				{
					let mut client_infos = self_clone.shared.client_infos.write();
					timestamp = Client::get_timestamp_nanos();
					if let Some(client_info) = client_infos.get(&self_clone.client_info.user_id) {
						to_notif = client_info.friend_info.read().friends.keys().copied().collect();
					}
					client_infos.remove(&self_clone.client_info.user_id);
				}

				let notif = Client::create_friend_status_notification(&self_clone.client_info.npid, timestamp, false);
				self_clone.send_notification(&notif, &to_notif).await;

				// Remove user from game stats
				self_clone.shared.game_tracker.decrease_num_users();

				if let Some(ref cur_com_id) = self_clone.current_game.0 {
					self_clone.shared.game_tracker.decrease_count_psn(cur_com_id);
				}

				if let Some(ref cur_service_id) = self_clone.current_game.1 {
					self_clone.shared.game_tracker.decrease_count_ticket(cur_service_id);
				}

				// Needed to make sure this is not called recursively!
				self_clone.authentified = false;
				self_clone.shared.cleanup_duty.write().remove(&self_clone.client_info.user_id);
			});
		}
	}
}

impl Client {
	pub async fn new(
		config: Arc<RwLock<Config>>,
		tls_stream: TlsStream<TcpStream>,
		db_pool: r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
		shared: SharedData,
		terminate_watch: TerminateWatch,
	) -> (Client, io::ReadHalf<TlsStream<TcpStream>>) {
		let client_info = ClientInfo {
			user_id: 0,
			npid: String::new(),
			online_name: String::new(),
			avatar_url: String::new(),
			token: String::new(),
			admin: false,
			stat_agent: false,
			banned: false,
		};

		let (channel_sender, mut channel_receiver) = mpsc::channel::<Vec<u8>>(32);
		let (tls_reader, mut tls_writer) = io::split(tls_stream);

		let fut_sock_writer = async move {
			while let Some(outgoing_packet) = channel_receiver.recv().await {
				let _ = tls_writer.write_all(&outgoing_packet).await;
				let _ = tls_writer.flush().await;
			}
			let _ = tls_writer.shutdown().await;
		};

		tokio::spawn(fut_sock_writer);

		(
			Client {
				config,
				channel_sender,
				db_pool,
				shared,
				authentified: false,
				client_info,
				post_reply_notifications: Vec::new(),
				terminate_watch,
				current_game: (None, None),
			},
			tls_reader,
		)
	}

	// Logging Functions
	#[allow(dead_code)]
	fn dump_packet(&self, packet: &[u8], source: &str) {
		if *self.config.read().get_verbosity() != tracing::Level::TRACE {
			return;
		}
		trace!("Dumping packet({}):", source);
		let mut line = String::new();

		for (count, p) in packet.iter().enumerate() {
			if (count != 0) && (count % 16) == 0 {
				trace!("{}", line);
				line.clear();
			}

			line = format!("{} {:02x}", line, p);
		}
		trace!("{}", line);
	}

	///// Command Processing
	pub async fn process(&mut self, tls_reader: &mut io::ReadHalf<TlsStream<TcpStream>>) {
		if *self.terminate_watch.recv.borrow_and_update() {
			return;
		}

		'main_client_loop: loop {
			let mut header_data = [0; HEADER_SIZE as usize];

			let r;
			if !self.authentified {
				let timeout_r = timeout(Duration::from_secs(10), tls_reader.read_exact(&mut header_data)).await;
				if timeout_r.is_err() {
					trace!("Unauthentified client timeouted after 10 seconds");
					break 'main_client_loop;
				}
				r = timeout_r.unwrap();
			} else {
				tokio::select! {
					_ = self.terminate_watch.recv.changed() => {
						assert!(*self.terminate_watch.recv.borrow());
						break 'main_client_loop;
					}
					result = tls_reader.read_exact(&mut header_data) => {
						r = result;
					}
				}
			}

			match r {
				Ok(_) => {
					if header_data[0] != PacketType::Request as u8 {
						warn!("Received non request packed, disconnecting client");
						break 'main_client_loop;
					}

					let command = u16::from_le_bytes([header_data[1], header_data[2]]);
					let packet_size = u32::from_le_bytes([header_data[3], header_data[4], header_data[5], header_data[6]]);
					let packet_id = u64::from_le_bytes([
						header_data[7],
						header_data[8],
						header_data[9],
						header_data[10],
						header_data[11],
						header_data[12],
						header_data[13],
						header_data[14],
					]);
					let npid_span = info_span!("", npid = %self.client_info.npid);

					// Max packet size of 8MiB
					if packet_size > MAX_PACKET_SIZE {
						warn!("Received oversized packet of {} bytes", packet_size);
						break 'main_client_loop;
					}

					if self.interpret_command(tls_reader, command, packet_size, packet_id).instrument(npid_span).await.is_err() {
						info!("Disconnecting client({})", self.client_info.npid);
						break 'main_client_loop;
					}
				}
				Err(e) => {
					info!("Client({}) disconnected: {}", self.client_info.npid, &e);
					break 'main_client_loop;
				}
			}
		}
	}

	async fn interpret_command(&mut self, tls_reader: &mut io::ReadHalf<TlsStream<TcpStream>>, command: u16, length: u32, packet_id: u64) -> Result<(), ()> {
		if length < HEADER_SIZE {
			warn!("Malformed packet(size < {})", HEADER_SIZE);
			return Err(());
		}

		let to_read = length - HEADER_SIZE;

		let mut data = vec![0; to_read as usize];

		let r = tls_reader.read_exact(&mut data).await;

		match r {
			Ok(_) => {
				// self.dump_packet(&data, "input");

				let mut reply = Vec::with_capacity(1000);

				reply.push(PacketType::Reply as u8);
				reply.extend(&command.to_le_bytes());
				reply.extend(&HEADER_SIZE.to_le_bytes());
				reply.extend(&packet_id.to_le_bytes());
				reply.push(ErrorType::NoError as u8);

				let mut se_data = StreamExtractor::new(data);
				let res = self.process_command(command, &mut se_data, &mut reply).await;

				// update length
				let len = reply.len() as u32;
				reply[3..7].clone_from_slice(&len.to_le_bytes());

				match res {
					Ok(ok_res) => {
						info!("Succeeded with {:?}", ok_res);
						reply[HEADER_SIZE as usize] = ok_res as u8;
					}
					Err(error) => {
						info!("Failed with {:?}", error);
						reply[HEADER_SIZE as usize] = error as u8;
					}
				}

				// self.dump_packet(&reply, "output");
				let _ = self.channel_sender.send(reply).await;

				// Send post command notifications if any
				for notif in &self.post_reply_notifications {
					let _ = self.channel_sender.send(notif.clone()).await;
				}
				self.post_reply_notifications.clear();

				match res {
					Ok(_) => Ok(()),
					Err(_) => Err(()),
				}
			}
			Err(e) => {
				warn!("Read error: {}", e);
				Err(())
			}
		}
	}

	async fn process_command(&mut self, command: u16, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let command = FromPrimitive::from_u16(command);
		if command.is_none() {
			warn!("Unknown command received");
			return Err(ErrorType::Malformed);
		}

		info!("Parsing command {:?}", command);

		let command = command.unwrap();

		if let CommandType::Terminate = command {
			return Err(ErrorType::NoError);
		}

		if !self.authentified {
			match command {
				CommandType::Login => return self.login(data, reply).await,
				CommandType::Create => return self.create_account(data),
				CommandType::SendToken => return self.resend_token(data),
				CommandType::SendResetToken => return self.send_reset_token(data),
				CommandType::ResetPassword => return self.reset_password(data),
				_ => {
					warn!("User attempted an invalid command at this stage");
					return Err(ErrorType::Invalid);
				}
			}
		}

		match command {
			CommandType::AddFriend => self.add_friend(data).await,
			CommandType::RemoveFriend => self.remove_friend(data).await,
			CommandType::AddBlock => self.add_block(data, reply),
			CommandType::RemoveBlock => self.remove_block(data, reply),
			CommandType::GetServerList => self.req_get_server_list(data, reply),
			CommandType::GetWorldList => self.req_get_world_list(data, reply),
			CommandType::CreateRoom => self.req_create_room(data, reply),
			CommandType::JoinRoom => self.req_join_room(data, reply).await,
			CommandType::LeaveRoom => self.req_leave_room(data, reply).await,
			CommandType::SearchRoom => self.req_search_room(data, reply),
			CommandType::GetRoomDataExternalList => self.req_get_roomdata_external_list(data, reply),
			CommandType::SetRoomDataExternal => self.req_set_roomdata_external(data),
			CommandType::GetRoomDataInternal => self.req_get_roomdata_internal(data, reply),
			CommandType::SetRoomDataInternal => self.req_set_roomdata_internal(data).await,
			CommandType::GetRoomMemberDataInternal => self.req_get_roommemberdata_internal(data, reply).await,
			CommandType::SetRoomMemberDataInternal => self.req_set_roommemberdata_internal(data).await,
			CommandType::SetUserInfo => self.req_set_userinfo(data).await,
			CommandType::PingRoomOwner => self.req_ping_room_owner(data, reply),
			CommandType::SendRoomMessage => self.req_send_room_message(data).await,
			CommandType::RequestSignalingInfos => self.req_signaling_infos(data, reply).await,
			CommandType::RequestTicket => self.req_ticket(data, reply),
			CommandType::SendMessage => self.send_message(data).await,
			CommandType::GetBoardInfos => self.get_board_infos(data, reply).await,
			CommandType::RecordScore => self.record_score(data, reply).await,
			CommandType::RecordScoreData => self.record_score_data(data).await,
			CommandType::GetScoreData => self.get_score_data(data, reply).await,
			CommandType::GetScoreRange => self.get_score_range(data, reply).await,
			CommandType::GetScoreFriends => self.get_score_friends(data, reply).await,
			CommandType::GetScoreNpid => self.get_score_npid(data, reply).await,
			CommandType::GetNetworkTime => self.get_network_time(reply),
			CommandType::TusSetMultiSlotVariable => self.tus_set_multislot_variable(data).await,
			CommandType::TusGetMultiSlotVariable => self.tus_get_multislot_variable(data, reply).await,
			CommandType::TusGetMultiUserVariable => self.tus_get_multiuser_variable(data, reply).await,
			CommandType::TusGetFriendsVariable => self.tus_get_friends_variable(data, reply).await,
			CommandType::TusAddAndGetVariable => self.tus_add_and_get_variable(data, reply).await,
			CommandType::TusTryAndSetVariable => self.tus_try_and_set_variable(data, reply).await,
			CommandType::TusDeleteMultiSlotVariable => self.tus_delete_multislot_variable(data).await,
			CommandType::TusSetData => self.tus_set_data(data).await,
			CommandType::TusGetData => self.tus_get_data(data, reply).await,
			CommandType::TusGetMultiSlotDataStatus => self.tus_get_multislot_data_status(data, reply).await,
			CommandType::TusGetMultiUserDataStatus => self.tus_get_multiuser_data_status(data, reply).await,
			CommandType::TusGetFriendsDataStatus => self.tus_get_friends_data_status(data, reply).await,
			CommandType::TusDeleteMultiSlotData => self.tus_delete_multislot_data(data).await,
			CommandType::ClearPresence => self.clear_presence().await,
			CommandType::SetPresence => self.set_presence(data).await,
			CommandType::CreateRoomGUI => self.req_create_room_gui(data, reply),
			CommandType::JoinRoomGUI => self.req_join_room_gui(data, reply).await,
			CommandType::LeaveRoomGUI => self.req_leave_room_gui(data, reply).await,
			CommandType::GetRoomListGUI => self.req_get_room_list_gui(data, reply),
			CommandType::SetRoomSearchFlagGUI => self.req_set_room_search_flag_gui(data),
			CommandType::GetRoomSearchFlagGUI => self.req_get_room_search_flag_gui(data, reply),
			CommandType::SetRoomInfoGUI => self.req_set_room_info_gui(data),
			CommandType::GetRoomInfoGUI => self.req_get_room_info_gui(data, reply),
			CommandType::QuickMatchGUI => self.req_quickmatch_gui(data, reply).await,
			CommandType::SearchJoinRoomGUI => self.req_searchjoin_gui(data, reply).await,
			CommandType::UpdateDomainBans => self.req_admin_update_domain_bans(),
			CommandType::TerminateServer => self.req_admin_terminate_server(),
			_ => {
				warn!("Unknown command received");
				Err(ErrorType::Invalid)
			}
		}
	}

	// Helper Functions

	pub fn get_timestamp_nanos() -> u64 {
		SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64
	}
	pub fn get_timestamp_seconds() -> u32 {
		SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs() as u32
	}
	pub fn get_psn_timestamp() -> u64 {
		(SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros() + (62_135_596_800 * 1000 * 1000)) as u64
	}

	fn get_database_connection(&self) -> Result<r2d2::PooledConnection<r2d2_sqlite::SqliteConnectionManager>, ErrorType> {
		self.db_pool.get().map_err(|e| {
			error!("Failed to get a database connection: {}", e);
			ErrorType::DbFail
		})
	}

	pub fn add_data_packet(reply: &mut Vec<u8>, data: &Vec<u8>) {
		reply.extend(&(data.len() as u32).to_le_bytes());
		reply.extend(data);
	}

	fn get_com_id_with_redir(&mut self, data: &mut StreamExtractor) -> ComId {
		let com_id = data.get_com_id();
		let final_com_id = self.config.read().get_server_redirection(com_id);

		if let Some(ref mut cur_com_id) = self.current_game.0 {
			if *cur_com_id != final_com_id {
				self.shared.game_tracker.decrease_count_psn(cur_com_id);
				*cur_com_id = final_com_id;
				self.shared.game_tracker.increase_count_psn(&final_com_id);
			}
		} else {
			self.current_game.0 = Some(final_com_id);
			self.shared.game_tracker.increase_count_psn(&final_com_id);
		}

		final_com_id
	}

	async fn signal_connections(&mut self, room_id: u64, from: (u16, i64), to: HashMap<u16, i64>, sig_param: Option<SignalParam>, owner: u16) {
		if sig_param.is_none() {
			return;
		}
		let sig_param = sig_param.unwrap();
		if !sig_param.should_signal() {
			return;
		}

		// Get user signaling
		let mut addr_p2p = [0; 4];
		let mut port_p2p = 0;
		let mut local_ip = [0; 4];
		{
			let client_infos = self.shared.client_infos.read();
			if let Some(client_entry) = client_infos.get(&from.1) {
				let client_si = client_entry.signaling_info.read();
				addr_p2p = client_si.addr_p2p;
				port_p2p = client_si.port_p2p;
				local_ip = client_si.local_addr_p2p;
			}
		}

		// Create a notification to send to other user(s)
		let mut s_msg: Vec<u8> = Vec::new();
		s_msg.extend(&room_id.to_le_bytes()); // +0..+8 room ID
		s_msg.extend(&from.0.to_le_bytes()); // +8..+10 member ID
		s_msg.extend(&port_p2p.to_le_bytes()); // +10..+12 port
		s_msg.extend(&addr_p2p); // +12..+16 addr
		let s_notif_extern = Client::create_notification(NotificationType::SignalP2PConnect, &s_msg);

		// Create a local IP version for users behind the same IP
		let mut s_notif_local = s_notif_extern.clone();
		s_notif_local[(HEADER_SIZE as usize + 10)..(HEADER_SIZE as usize + 12)].clone_from_slice(&3658u16.to_le_bytes());
		s_notif_local[(HEADER_SIZE as usize + 12)..(HEADER_SIZE as usize + 16)].clone_from_slice(&local_ip);

		let mut s_self_notif = s_notif_extern.clone();

		let mut user_ids_extern: HashSet<i64> = HashSet::new();
		let mut user_ids_local: HashSet<i64> = HashSet::new();

		match sig_param.get_type() {
			SignalingType::SignalingMesh => {
				// Notifies user that connection needs to be established with all other occupants
				{
					for user in &to {
						let mut tosend = false; // tosend is there to avoid double locking on signaling_infos
						{
							let client_infos = self.shared.client_infos.read();
							let client_entry = client_infos.get(user.1);

							if let Some(client_entry) = client_entry {
								let client_si = client_entry.signaling_info.read();
								s_self_notif[(HEADER_SIZE as usize + 8)..(HEADER_SIZE as usize + 10)].clone_from_slice(&user.0.to_le_bytes());

								if client_si.addr_p2p == addr_p2p {
									user_ids_local.insert(*user.1);
									s_self_notif[(HEADER_SIZE as usize + 12)..(HEADER_SIZE as usize + 16)].clone_from_slice(&client_si.local_addr_p2p);
									s_self_notif[(HEADER_SIZE as usize + 10)..(HEADER_SIZE as usize + 12)].clone_from_slice(&3658u16.to_le_bytes());
								} else {
									user_ids_extern.insert(*user.1);
									s_self_notif[(HEADER_SIZE as usize + 12)..(HEADER_SIZE as usize + 16)].clone_from_slice(&client_si.addr_p2p);
									s_self_notif[(HEADER_SIZE as usize + 10)..(HEADER_SIZE as usize + 12)].clone_from_slice(&client_si.port_p2p.to_le_bytes());
								}

								tosend = true;
							}
						}
						if tosend {
							self.self_notification(&s_self_notif); // Special function that will post the notification after the reply
						}
					}
				}
			}
			SignalingType::SignalingStar => {
				let mut hub = sig_param.get_hub();
				if hub == 0 {
					hub = owner;
				}

				// Send notification to user to connect to hub
				let mut tosend = false;
				{
					let client_infos = self.shared.client_infos.read();
					let hub_user_id = *to.get(&hub).unwrap();
					let client_entry = client_infos.get(&hub_user_id);

					if let Some(client_entry) = client_entry {
						let client_si = client_entry.signaling_info.read();
						s_self_notif[(HEADER_SIZE as usize + 8)..(HEADER_SIZE as usize + 10)].clone_from_slice(&hub.to_le_bytes());

						if client_si.addr_p2p == addr_p2p {
							user_ids_local.insert(hub_user_id);
							s_self_notif[(HEADER_SIZE as usize + 12)..(HEADER_SIZE as usize + 16)].clone_from_slice(&client_si.local_addr_p2p);
							s_self_notif[(HEADER_SIZE as usize + 10)..(HEADER_SIZE as usize + 12)].clone_from_slice(&3658u16.to_le_bytes());
						} else {
							user_ids_extern.insert(hub_user_id);
							s_self_notif[(HEADER_SIZE as usize + 12)..(HEADER_SIZE as usize + 16)].clone_from_slice(&client_si.addr_p2p);
							s_self_notif[(HEADER_SIZE as usize + 10)..(HEADER_SIZE as usize + 12)].clone_from_slice(&client_si.port_p2p.to_le_bytes());
						}

						tosend = true;
					}
				}
				if tosend {
					self.self_notification(&s_self_notif); // Special function that will post the notification after the reply
				}
			}
			_ => panic!("Unimplemented SignalingType({:?})", sig_param.get_type()),
		}

		// Notify other members of connections needing to be established
		self.send_notification(&s_notif_extern, &user_ids_extern).await;
		self.send_notification(&s_notif_local, &user_ids_local).await;
	}

	//Helper for avoiding potentially pointless queries to database
	pub fn get_username_with_helper(&self, user_id: i64, helper_list: &HashMap<i64, String>, db: &Database) -> Result<String, ErrorType> {
		if let Some(npid) = helper_list.get(&user_id) {
			return Ok(npid.clone());
		}

		db.get_username(user_id).map_err(|_| ErrorType::DbFail)
	}

	// Various helpers to help with validation
	pub fn get_fb<'a, T: flatbuffers::Follow<'a> + flatbuffers::Verifiable + 'a>(&mut self, data: &'a mut StreamExtractor) -> Result<T::Inner, ErrorType> {
		let fb_req = data.get_flatbuffer::<T>();

		if data.error() || fb_req.is_err() {
			warn!("Validation error while extracting data into {}", std::any::type_name::<T>());
			return Err(ErrorType::Malformed);
		}
		let fb_req = fb_req.unwrap();

		Ok(fb_req)
	}

	pub fn get_com_and_fb<'a, T: flatbuffers::Follow<'a> + flatbuffers::Verifiable + 'a>(&mut self, data: &'a mut StreamExtractor) -> Result<(ComId, T::Inner), ErrorType> {
		let com_id = self.get_com_id_with_redir(data);
		let fb_req = self.get_fb::<T>(data)?;
		Ok((com_id, fb_req))
	}

	pub fn validate_and_unwrap<T>(opt: Option<T>) -> Result<T, ErrorType> {
		if opt.is_none() {
			warn!("Validation error while validating {}", std::any::type_name::<T>());
			return Err(ErrorType::Malformed);
		}

		Ok(opt.unwrap())
	}
}

macro_rules! validate_all {
	($($x:expr),*) => {
		{
			(
			$(Client::validate_and_unwrap($x)?,)*
			)
		}
	}
}

pub(crate) use validate_all;
