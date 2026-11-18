// Matching2 lobbies
// Lobbies are declared in servers.cfg and are permanent: their state only exists while they have members

use std::collections::{BTreeMap, HashMap, HashSet};

use prost::Message;
use tracing::warn;

use crate::server::client::{Client, ClientInfo, ComId, ErrorType, EventCause};
use crate::server::stream_extractor::np2_structs::*;

const SCE_NP_MATCHING2_LOBBY_MAX_SLOT: u32 = 256;
const SCE_NP_MATCHING2_LOBBY_FLAG_ATTR_PERMANENT: u32 = 0x8000_0000;
const SCE_NP_MATCHING2_LOBBY_FLAG_ATTR_MEMBER_NOTIFICATION: u32 = 0x2000_0000;
const SCE_NP_MATCHING2_LOBBYMEMBER_BIN_ATTR_INTERNAL_1_ID: u32 = 0x0039;
const SCE_NP_MATCHING2_LOBBYMEMBER_BIN_ATTR_INTERNAL_MAX_SIZE: usize = 64;
const SCE_NP_MATCHING2_LOBBY_MEMBER_DATA_INTERNAL_LIST_MAX: usize = 256;
const SCE_NP_MATCHING2_LOBBY_MEMBER_DATA_INTERNAL_EXTENDED_DATA_LIST_MAX: usize = 64;
const SCE_NP_MATCHING2_RANGE_FILTER_MAX: u32 = 20;
const SCE_NP_MATCHING2_PRESENCE_OPTION_DATA_SIZE: usize = 16;
const SCE_NP_MATCHING2_CHAT_MSG_MAX_SIZE: usize = 1024;
const SCE_NP_MATCHING2_INVITATION_OPTION_DATA_MAX_SIZE: usize = 32;
const SCE_NP_MATCHING2_INVITATION_TARGET_SESSION_MAX: usize = 2;

const SCE_NP_MATCHING2_CASTTYPE_BROADCAST: u32 = 1;
const SCE_NP_MATCHING2_CASTTYPE_UNICAST: u32 = 2;
const SCE_NP_MATCHING2_CASTTYPE_MULTICAST: u32 = 3;

// SCE_NP_MATCHING2_EVENT_DATA_MAX_SIZE_LobbyMemberDataInternalUpdateInfo leaves room for 10 joined sessions
const MAX_JOINED_SESSIONS: usize = 10;

const LOBBY_FLAG_ATTR: u32 = SCE_NP_MATCHING2_LOBBY_FLAG_ATTR_PERMANENT | SCE_NP_MATCHING2_LOBBY_FLAG_ATTR_MEMBER_NOTIFICATION;

fn validate_joined_sessions(sessions: &[JoinedSessionInfo], max: usize) -> Result<Vec<JoinedSessionInfo>, ErrorType> {
	if sessions.len() > max {
		warn!("Lobby request with {} joined sessions (max {})", sessions.len(), max);
		return Err(ErrorType::Malformed);
	}

	for session in sessions {
		if session.session_type > u8::MAX as u32 || session.server_id > u16::MAX as u32 {
			warn!("Lobby request with an invalid joined session");
			return Err(ErrorType::Malformed);
		}
	}

	Ok(sessions.to_vec())
}

fn validate_opt_data(opt_data: Option<&PresenceOptionData>) -> Result<Option<PresenceOptionData>, ErrorType> {
	match opt_data {
		Some(opt) if opt.data.len() > SCE_NP_MATCHING2_PRESENCE_OPTION_DATA_SIZE || opt.len as usize > SCE_NP_MATCHING2_PRESENCE_OPTION_DATA_SIZE => {
			warn!("Lobby request with oversized presence option data");
			Err(ErrorType::Malformed)
		}
		_ => Ok(opt_data.cloned()),
	}
}

// Only LOBBYMEMBER_BIN_ATTR_INTERNAL_1 exists, anything else is ignored like for rooms
fn extract_member_bin_attr(attrs: &[BinAttr]) -> Result<Option<Vec<u8>>, ErrorType> {
	let mut result = None;

	for attr in attrs {
		let id = attr.id.as_ref().map(|v| v.value).unwrap_or(0);
		if id != SCE_NP_MATCHING2_LOBBYMEMBER_BIN_ATTR_INTERNAL_1_ID {
			warn!("Lobby request with unexpected member bin attr id: 0x{:x}", id);
			continue;
		}

		if attr.data.len() > SCE_NP_MATCHING2_LOBBYMEMBER_BIN_ATTR_INTERNAL_MAX_SIZE {
			warn!("Lobby request with oversized member bin attr: {}", attr.data.len());
			return Err(ErrorType::Malformed);
		}

		result = Some(attr.data.clone());
	}

	Ok(result)
}

struct LobbyMember {
	user_id: i64,
	npid: String,
	online_name: String,
	avatar_url: String,
	join_date: u64,
	flag_attr: u32,
	joined_sessions: Vec<JoinedSessionInfo>,
	bin_attr: (u64, Vec<u8>), // (update date, data)
}

impl LobbyMember {
	fn bin_attr_to_protobuf(&self) -> RoomMemberBinAttrInternal {
		RoomMemberBinAttrInternal {
			update_date: self.bin_attr.0,
			data: Some(BinAttr {
				id: Some(Uint16 {
					value: SCE_NP_MATCHING2_LOBBYMEMBER_BIN_ATTR_INTERNAL_1_ID,
				}),
				data: self.bin_attr.1.clone(),
			}),
		}
	}

	fn to_protobuf(&self, member_id: u16, extended: bool, with_bin_attr: bool) -> LobbyMemberDataInternal {
		let user_info = if extended {
			UserInfo {
				np_id: self.npid.clone(),
				online_name: self.online_name.clone(),
				avatar_url: self.avatar_url.clone(),
			}
		} else {
			UserInfo {
				np_id: self.npid.clone(),
				online_name: String::new(),
				avatar_url: String::new(),
			}
		};

		LobbyMemberDataInternal {
			user_info: Some(user_info),
			join_date: self.join_date,
			member_id: member_id as u32,
			flag_attr: self.flag_attr,
			joined_session_info: if extended { self.joined_sessions.clone() } else { Vec::new() },
			lobby_member_bin_attr_internal: if extended && with_bin_attr { vec![self.bin_attr_to_protobuf()] } else { Vec::new() },
		}
	}
}

struct Lobby {
	server_id: u16,
	world_id: u32,
	members: BTreeMap<u16, LobbyMember>,
	next_member_id: u16,
}

impl Lobby {
	fn new(server_id: u16, world_id: u32) -> Lobby {
		Lobby {
			server_id,
			world_id,
			members: BTreeMap::new(),
			next_member_id: 1,
		}
	}

	fn get_member_id(&self, user_id: i64) -> Option<u16> {
		self.members.iter().find(|(_, m)| m.user_id == user_id).map(|(id, _)| *id)
	}

	fn get_other_users(&self, user_id: i64) -> HashSet<i64> {
		self.members.values().filter(|m| m.user_id != user_id).map(|m| m.user_id).collect()
	}

	fn allocate_member_id(&mut self) -> u16 {
		// Member ids are recycled once all u16 values have been used, the lobby can't hold more than 256 members
		loop {
			let candidate = self.next_member_id;
			self.next_member_id = self.next_member_id.checked_add(1).unwrap_or(1);
			if !self.members.contains_key(&candidate) {
				return candidate;
			}
		}
	}

	fn to_protobuf(&self, lobby_id: u64, me: u16) -> LobbyDataInternal {
		LobbyDataInternal {
			server_id: self.server_id as u32,
			world_id: self.world_id,
			lobby_id,
			max_slot: SCE_NP_MATCHING2_LOBBY_MAX_SLOT,
			member_ids: self.members.keys().map(|id| *id as u32).collect(),
			me: me as u32,
			flag_attr: LOBBY_FLAG_ATTR,
			lobby_bin_attr_internal: Vec::new(),
		}
	}

	fn member_update_notification(&self, lobby_id: u64, member_id: u16, event_cause: EventCause, opt_data: Option<PresenceOptionData>) -> Vec<u8> {
		let member = &self.members[&member_id];
		NotificationLobbyMemberUpdate {
			lobby_id,
			update_info: Some(LobbyMemberUpdateInfo {
				lobby_member_data_internal: Some(member.to_protobuf(member_id, true, true)),
				event_cause: event_cause as u32,
				opt_data,
			}),
		}
		.encode_to_vec()
	}
}

pub struct LobbyMessageTargets {
	pub src_member_id: u16,
	pub users: HashSet<i64>,
	pub include_self: bool,
}

pub struct LobbyManager {
	lobbies: HashMap<(ComId, u64), Lobby>,
	user_lobbies: HashMap<i64, HashSet<(ComId, u64)>>,
}

impl LobbyManager {
	pub fn new() -> LobbyManager {
		LobbyManager {
			lobbies: HashMap::new(),
			user_lobbies: HashMap::new(),
		}
	}

	pub fn get_lobbies_by_user(&self, user_id: i64) -> Option<Vec<(ComId, u64)>> {
		self.user_lobbies.get(&user_id).map(|lobbies| lobbies.iter().copied().collect())
	}

	// lobbies are the (server_id, lobby_id) declared for that world, sorted by lobby id
	pub fn get_lobby_info_list(&self, com_id: &ComId, world_id: u32, lobbies: &[(u16, u64)], req: &GetLobbyInfoListRequest) -> GetLobbyInfoListResponse {
		let start_index = req.range_filter_start_index.max(1);
		let max = req.range_filter_max.clamp(1, SCE_NP_MATCHING2_RANGE_FILTER_MAX);

		let lobbies_data = lobbies
			.iter()
			.skip((start_index - 1) as usize)
			.take(max as usize)
			.map(|(server_id, lobby_id)| LobbyDataExternal {
				server_id: *server_id as u32,
				world_id,
				lobby_id: *lobby_id,
				max_slot: SCE_NP_MATCHING2_LOBBY_MAX_SLOT,
				cur_member_num: self.lobbies.get(&(*com_id, *lobby_id)).map_or(0, |l| l.members.len() as u32),
				flag_attr: LOBBY_FLAG_ATTR,
				lobby_searchable_int_attr_external: Vec::new(),
				lobby_searchable_bin_attr_external: Vec::new(),
				lobby_bin_attr_external: Vec::new(),
			})
			.collect();

		GetLobbyInfoListResponse {
			start_index,
			total: lobbies.len() as u32,
			lobbies: lobbies_data,
		}
	}

	// Returns the reply, the users to notify and the notification
	pub fn join_lobby(&mut self, com_id: &ComId, server_id: u16, world_id: u32, req: &JoinLobbyRequest, cinfo: &ClientInfo) -> Result<(Vec<u8>, HashSet<i64>, Vec<u8>), ErrorType> {
		let joined_sessions = validate_joined_sessions(&req.joined_session_info, MAX_JOINED_SESSIONS)?;
		let bin_attr = extract_member_bin_attr(&req.lobby_member_bin_attr_internal)?.unwrap_or_default();
		let opt_data = validate_opt_data(req.opt_data.as_ref())?;

		let lobby = self.lobbies.entry((*com_id, req.lobby_id)).or_insert_with(|| Lobby::new(server_id, world_id));

		if lobby.get_member_id(cinfo.user_id).is_some() {
			return Err(ErrorType::RoomAlreadyJoined);
		}

		if lobby.members.len() >= SCE_NP_MATCHING2_LOBBY_MAX_SLOT as usize {
			return Err(ErrorType::RoomFull);
		}

		let timestamp = Client::get_psn_timestamp();
		let member_id = lobby.allocate_member_id();
		lobby.members.insert(
			member_id,
			LobbyMember {
				user_id: cinfo.user_id,
				npid: cinfo.npid.clone(),
				online_name: cinfo.online_name.clone(),
				avatar_url: cinfo.avatar_url.clone(),
				join_date: timestamp,
				flag_attr: 0,
				joined_sessions,
				bin_attr: (timestamp, bin_attr),
			},
		);

		let reply = JoinLobbyResponse {
			lobby_data: Some(lobby.to_protobuf(req.lobby_id, member_id)),
		}
		.encode_to_vec();
		let notif = lobby.member_update_notification(req.lobby_id, member_id, EventCause::None, opt_data);
		let users = lobby.get_other_users(cinfo.user_id);

		self.user_lobbies.entry(cinfo.user_id).or_default().insert((*com_id, req.lobby_id));

		Ok((reply, users, notif))
	}

	// Returns the users to notify and the notification
	pub fn leave_lobby(&mut self, com_id: &ComId, lobby_id: u64, user_id: i64, event_cause: EventCause, opt_data: Option<&PresenceOptionData>) -> Result<(HashSet<i64>, Vec<u8>), ErrorType> {
		let opt_data = validate_opt_data(opt_data)?;
		let lobby = self.lobbies.get_mut(&(*com_id, lobby_id)).ok_or(ErrorType::RoomMissing)?;
		let member_id = lobby.get_member_id(user_id).ok_or(ErrorType::NotFound)?;

		let notif = lobby.member_update_notification(lobby_id, member_id, event_cause, opt_data);
		lobby.members.remove(&member_id);
		let users = lobby.get_other_users(user_id);

		if lobby.members.is_empty() {
			self.lobbies.remove(&(*com_id, lobby_id));
		}

		if let Some(lobbies) = self.user_lobbies.get_mut(&user_id) {
			lobbies.remove(&(*com_id, lobby_id));
			if lobbies.is_empty() {
				self.user_lobbies.remove(&user_id);
			}
		}

		Ok((users, notif))
	}

	// Returns the other users to notify and the notification(also sent to self)
	pub fn set_lobby_member_data_internal(&mut self, com_id: &ComId, req: &SetLobbyMemberDataInternalRequest, user_id: i64) -> Result<(HashSet<i64>, Vec<u8>), ErrorType> {
		let joined_sessions = validate_joined_sessions(&req.joined_session_info, MAX_JOINED_SESSIONS)?;
		let bin_attr = extract_member_bin_attr(&req.lobby_member_bin_attr_internal)?;

		let lobby = self.lobbies.get_mut(&(*com_id, req.lobby_id)).ok_or(ErrorType::RoomMissing)?;
		let member_id = lobby.get_member_id(user_id).ok_or(ErrorType::NotFound)?;

		// Members can only modify their own data
		if req.member_id != 0 && req.member_id != member_id as u32 {
			warn!("User attempted to set lobby member data of another member");
			return Err(ErrorType::Unauthorized);
		}

		let member = lobby.members.get_mut(&member_id).unwrap();
		member.flag_attr = (member.flag_attr & !req.flag_filter) | (req.flag_attr & req.flag_filter);
		if !joined_sessions.is_empty() {
			member.joined_sessions = joined_sessions;
		}
		if let Some(bin_attr) = bin_attr {
			member.bin_attr = (Client::get_psn_timestamp(), bin_attr);
		}

		let notif = NotificationLobbyMemberDataInternalUpdate {
			lobby_id: req.lobby_id,
			member_id: member_id as u32,
			np_id: member.npid.clone(),
			flag_filter: req.flag_filter,
			new_flag_attr: member.flag_attr,
			new_joined_session_info: member.joined_sessions.clone(),
			new_lobby_member_bin_attr_internal: vec![member.bin_attr_to_protobuf()],
		}
		.encode_to_vec();

		Ok((lobby.get_other_users(user_id), notif))
	}

	pub fn get_lobby_member_data_internal_list(&self, com_id: &ComId, req: &GetLobbyMemberDataInternalListRequest, user_id: i64) -> Result<Vec<u8>, ErrorType> {
		let lobby = self.lobbies.get(&(*com_id, req.lobby_id)).ok_or(ErrorType::RoomMissing)?;
		if lobby.get_member_id(user_id).is_none() {
			warn!("User requested the member list of a lobby he's not a member of");
			return Err(ErrorType::Unauthorized);
		}

		let max_members = if req.extended_data {
			SCE_NP_MATCHING2_LOBBY_MEMBER_DATA_INTERNAL_EXTENDED_DATA_LIST_MAX
		} else {
			SCE_NP_MATCHING2_LOBBY_MEMBER_DATA_INTERNAL_LIST_MAX
		};
		let with_bin_attr = req.attr_id.contains(&SCE_NP_MATCHING2_LOBBYMEMBER_BIN_ATTR_INTERNAL_1_ID);

		let members: Vec<LobbyMemberDataInternal> = if req.member_id.is_empty() {
			lobby
				.members
				.iter()
				.take(max_members)
				.map(|(id, member)| member.to_protobuf(*id, req.extended_data, with_bin_attr))
				.collect()
		} else {
			req.member_id
				.iter()
				.filter_map(|id| {
					let id = u16::try_from(*id).ok()?;
					lobby.members.get(&id).map(|member| member.to_protobuf(id, req.extended_data, with_bin_attr))
				})
				.take(max_members)
				.collect()
		};

		Ok(GetLobbyMemberDataInternalListResponse { members }.encode_to_vec())
	}

	pub fn get_message_targets(&self, com_id: &ComId, lobby_id: u64, user_id: i64, cast_type: u32, dst: &[u32]) -> Result<LobbyMessageTargets, ErrorType> {
		let lobby = self.lobbies.get(&(*com_id, lobby_id)).ok_or(ErrorType::RoomMissing)?;
		let src_member_id = lobby.get_member_id(user_id).ok_or(ErrorType::Unauthorized)?;

		let (users, include_self) = match cast_type {
			SCE_NP_MATCHING2_CASTTYPE_BROADCAST => (lobby.get_other_users(user_id), true),
			SCE_NP_MATCHING2_CASTTYPE_UNICAST | SCE_NP_MATCHING2_CASTTYPE_MULTICAST => {
				if dst.is_empty() || (cast_type == SCE_NP_MATCHING2_CASTTYPE_UNICAST && dst.len() != 1) {
					return Err(ErrorType::InvalidInput);
				}
				let mut include_self = false;
				let users = dst
					.iter()
					.filter_map(|id| lobby.members.get(&u16::try_from(*id).ok()?))
					.filter_map(|member| {
						if member.user_id == user_id {
							include_self = true;
							None
						} else {
							Some(member.user_id)
						}
					})
					.collect();
				(users, include_self)
			}
			_ => {
				warn!("Invalid cast type for a lobby message: {}", cast_type);
				return Err(ErrorType::InvalidInput);
			}
		};

		Ok(LobbyMessageTargets { src_member_id, users, include_self })
	}
}

pub fn validate_chat_message(msg: &[u8]) -> Result<(), ErrorType> {
	if msg.len() > SCE_NP_MATCHING2_CHAT_MSG_MAX_SIZE {
		warn!("Lobby chat message is too big: {}", msg.len());
		return Err(ErrorType::Malformed);
	}
	Ok(())
}

pub fn validate_invitation(target_session: &[JoinedSessionInfo], opt_data: &[u8]) -> Result<(), ErrorType> {
	validate_joined_sessions(target_session, SCE_NP_MATCHING2_INVITATION_TARGET_SESSION_MAX)?;
	if opt_data.len() > SCE_NP_MATCHING2_INVITATION_OPTION_DATA_MAX_SIZE {
		warn!("Lobby invitation option data is too big: {}", opt_data.len());
		return Err(ErrorType::Malformed);
	}
	Ok(())
}
