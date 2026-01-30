#![allow(non_snake_case)]
#![allow(dead_code)]
#![allow(non_camel_case_types)]

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use prost::Message;
use rand::Rng;
use tracing::{info, warn};

use crate::server::client::{Client, ClientInfo, ComId, ErrorType, EventCause};
use crate::server::stream_extractor::np2_structs::*;
use crate::server::stream_extractor::protobuf_helpers::{ProtobufMaker, ProtobufVerifier};

use super::client::ClientSharedSignalingInfo;

#[repr(u8)]
#[derive(FromPrimitive)]
#[allow(clippy::enum_variant_names)]
enum SceNpMatching2Operator {
	OperatorEq = 1,
	OperatorNe = 2,
	OperatorLt = 3,
	OperatorLe = 4,
	OperatorGt = 5,
	OperatorGe = 6,
}

const SCE_NP_MATCHING2_ROOM_SEARCHABLE_INT_ATTR_EXTERNAL_1_ID: u16 = 0x4C;
const SCE_NP_MATCHING2_ROOM_SEARCHABLE_INT_ATTR_EXTERNAL_2_ID: u16 = 0x4D;
const SCE_NP_MATCHING2_ROOM_SEARCHABLE_INT_ATTR_EXTERNAL_3_ID: u16 = 0x4E;
const SCE_NP_MATCHING2_ROOM_SEARCHABLE_INT_ATTR_EXTERNAL_4_ID: u16 = 0x4F;
const SCE_NP_MATCHING2_ROOM_SEARCHABLE_INT_ATTR_EXTERNAL_5_ID: u16 = 0x50;
const SCE_NP_MATCHING2_ROOM_SEARCHABLE_INT_ATTR_EXTERNAL_6_ID: u16 = 0x51;
const SCE_NP_MATCHING2_ROOM_SEARCHABLE_INT_ATTR_EXTERNAL_7_ID: u16 = 0x52;
const SCE_NP_MATCHING2_ROOM_SEARCHABLE_INT_ATTR_EXTERNAL_8_ID: u16 = 0x53;

const SCE_NP_MATCHING2_ROOM_SEARCHABLE_BIN_ATTR_EXTERNAL_1_ID: u16 = 0x54;

const SCE_NP_MATCHING2_ROOM_BIN_ATTR_EXTERNAL_1_ID: u16 = 0x55;
const SCE_NP_MATCHING2_ROOM_BIN_ATTR_EXTERNAL_2_ID: u16 = 0x56;

const SCE_NP_MATCHING2_ROOM_BIN_ATTR_INTERNAL_1_ID: u16 = 0x57;
const SCE_NP_MATCHING2_ROOM_BIN_ATTR_INTERNAL_2_ID: u16 = 0x58;

const SCE_NP_MATCHING2_ROOMMEMBER_BIN_ATTR_INTERNAL_1_ID: u16 = 0x59;

const SCE_NP_MATCHING2_SESSION_PASSWORD_SIZE: usize = 8;
const SCE_NP_MATCHING2_GROUP_LABEL_SIZE: usize = 8;

#[repr(u32)]
enum SceNpMatching2FlagAttr {
	SCE_NP_MATCHING2_ROOM_FLAG_ATTR_OWNER_AUTO_GRANT = 0x80000000,
	SCE_NP_MATCHING2_ROOM_FLAG_ATTR_CLOSED = 0x40000000,
	SCE_NP_MATCHING2_ROOM_FLAG_ATTR_FULL = 0x20000000,
	SCE_NP_MATCHING2_ROOM_FLAG_ATTR_HIDDEN = 0x10000000,
	SCE_NP_MATCHING2_ROOM_FLAG_ATTR_NAT_TYPE_RESTRICTION = 0x04000000,
	SCE_NP_MATCHING2_ROOM_FLAG_ATTR_PROHIBITIVE_MODE = 0x02000000,
}

const PROTECTED_ROOM_FLAGS: u32 = SceNpMatching2FlagAttr::SCE_NP_MATCHING2_ROOM_FLAG_ATTR_FULL as u32;

const SCE_NP_MATCHING2_ROOMMEMBER_FLAG_ATTR_OWNER: u32 = 0x80000000;

const SCE_NP_MATCHING2_ROLE_MEMBER: u8 = 1;
const SCE_NP_MATCHING2_ROLE_OWNER: u8 = 2;

const CREATOR_ROOM_MEMBER_ID: u16 = 16;

#[repr(u8)]
#[derive(FromPrimitive, Clone, Copy, Debug)]
#[allow(clippy::enum_variant_names)]
pub enum SignalingType {
	SignalingNone = 0,
	SignalingMesh = 1,
	SignalingStar = 2,
}

pub struct RoomBinAttr<const N: usize> {
	id: u16,
	attr: [u8; N],
	cur_size: usize,
}
impl<const N: usize> RoomBinAttr<N> {
	pub fn with_id(id: u16) -> RoomBinAttr<N> {
		RoomBinAttr { id, attr: [0; N], cur_size: 0 }
	}

	pub fn from_protobuf(pb: &BinAttr) -> Result<RoomBinAttr<N>, ErrorType> {
		let id = pb.id.get_verified()?;
		let mut attr: [u8; N] = [0; N];
		let len = if pb.data.len() > N {
			warn!("Error converting a protobuf BinAttr to a RoomBinAttr, mismatched size: pb:{} vs expected:{}", pb.data.len(), N);
			return Err(ErrorType::Malformed);
		} else {
			pb.data.len()
		};
		attr[0..len].clone_from_slice(&pb.data[0..len]);

		Ok(RoomBinAttr { id, attr, cur_size: len })
	}

	pub fn to_protobuf(&self) -> BinAttr {
		BinAttr {
			id: Uint16::new_from_value(self.id),
			data: self.attr[0..self.cur_size].to_vec(),
		}
	}
}

pub struct RoomMemberBinAttr {
	update_date: u64,
	data: RoomBinAttr<128>,
}
impl RoomMemberBinAttr {
	pub fn new() -> RoomMemberBinAttr {
		RoomMemberBinAttr {
			update_date: Client::get_psn_timestamp(),
			data: RoomBinAttr::with_id(SCE_NP_MATCHING2_ROOMMEMBER_BIN_ATTR_INTERNAL_1_ID),
		}
	}

	pub fn from_protobuf(pb: &BinAttr) -> Result<RoomMemberBinAttr, ErrorType> {
		let data = RoomBinAttr::from_protobuf(pb)?;
		Ok(RoomMemberBinAttr {
			update_date: Client::get_psn_timestamp(),
			data,
		})
	}

	pub fn to_protobuf(&self) -> RoomMemberBinAttrInternal {
		RoomMemberBinAttrInternal {
			update_date: self.update_date,
			data: Some(self.data.to_protobuf()),
		}
	}
}

pub struct RoomBinAttrInternal {
	update_date: u64,
	update_member_id: u16,
	data: RoomBinAttr<256>,
}
impl RoomBinAttrInternal {
	pub fn with_id(id: u16) -> RoomBinAttrInternal {
		RoomBinAttrInternal {
			update_date: Client::get_psn_timestamp(),
			update_member_id: CREATOR_ROOM_MEMBER_ID,
			data: RoomBinAttr::<256>::with_id(id),
		}
	}

	pub fn from_protobuf(pb: &BinAttr, member_id: u16) -> Result<RoomBinAttrInternal, ErrorType> {
		let data = RoomBinAttr::from_protobuf(pb)?;
		Ok(RoomBinAttrInternal {
			update_date: Client::get_psn_timestamp(),
			update_member_id: member_id,
			data,
		})
	}

	pub fn to_protobuf(&self) -> BinAttrInternal {
		BinAttrInternal {
			update_date: self.update_date,
			update_member_id: Uint16::new_from_value(self.update_member_id),
			data: Some(self.data.to_protobuf()),
		}
	}
}

struct RoomIntAttr {
	id: u16,
	attr: u32,
}
impl RoomIntAttr {
	pub fn with_id(id: u16) -> RoomIntAttr {
		RoomIntAttr { id, attr: 0 }
	}
	pub fn from_protobuf(pb: &IntAttr) -> Result<RoomIntAttr, ErrorType> {
		let id = pb.id.get_verified()?;
		let attr = pb.num;
		Ok(RoomIntAttr { id, attr })
	}
	pub fn to_protobuf(&self) -> IntAttr {
		IntAttr {
			id: Uint16::new_from_value(self.id),
			num: self.attr,
		}
	}
}

struct RoomGroupConfig {
	slot_num: u32,
	fixed_label: bool,
	label: Option<[u8; SCE_NP_MATCHING2_GROUP_LABEL_SIZE]>,
	with_password: bool,
	group_id: u8,
	num_members: u32,
}
impl RoomGroupConfig {
	pub fn from_protobuf(pb: &GroupConfig, group_id: u8) -> RoomGroupConfig {
		let slot_num = pb.slot_num;
		let label = if pb.label.len() == SCE_NP_MATCHING2_GROUP_LABEL_SIZE {
			let mut label_array = [0; SCE_NP_MATCHING2_GROUP_LABEL_SIZE];
			label_array.clone_from_slice(&pb.label[0..SCE_NP_MATCHING2_GROUP_LABEL_SIZE]);
			Some(label_array)
		} else {
			None
		};

		let with_password = pb.with_password;
		RoomGroupConfig {
			slot_num,
			fixed_label: label.is_some(),
			label,
			with_password,
			group_id,
			num_members: 0,
		}
	}

	pub fn to_protobuf(&self) -> RoomGroup {
		RoomGroup {
			group_id: Uint8::new_from_value(self.group_id),
			with_password: self.with_password,
			label: self.label.map(|l| l.to_vec()).unwrap_or_default(),
			slot_num: self.slot_num,
			cur_group_member_num: self.num_members,
		}
	}
}

#[derive(Clone)]
struct SignalParam {
	sig_type: SignalingType,
	flag: u8,
	hub_member_id: u16,
}
impl SignalParam {
	pub fn from_protobuf(pb: &OptParam) -> Result<SignalParam, ErrorType> {
		let sig_type = FromPrimitive::from_u8(pb.r#type.get_verified()?).ok_or(ErrorType::Malformed)?;
		let flag = pb.flag.get_verified()?;
		let hub_member_id = pb.hub_member_id.get_verified()?;

		Ok(SignalParam { sig_type, flag, hub_member_id })
	}

	pub fn to_protobuf(&self) -> OptParam {
		OptParam {
			r#type: Uint8::new_from_value(self.sig_type as u8),
			flag: Uint8::new_from_value(self.flag),
			hub_member_id: Uint16::new_from_value(self.hub_member_id),
		}
	}

	pub fn should_signal(&self) -> bool {
		match self.sig_type {
			SignalingType::SignalingNone => false,
			_ => (self.flag & 1) != 1,
		}
	}

	pub fn get_type(&self) -> SignalingType {
		self.sig_type.clone()
	}

	pub fn get_hub(&self) -> u16 {
		self.hub_member_id
	}
}

struct RoomUser {
	user_id: i64,
	npid: String,
	online_name: String,
	avatar_url: String,
	join_date: u64,
	flag_attr: u32,

	group_id: u8,
	member_attr: RoomMemberBinAttr,
	team_id: u8,

	member_id: u16,

	signaling_info: ClientSharedSignalingInfo,
}
impl RoomUser {
	pub fn from_CreateJoinRoomRequest(pb: &CreateJoinRoomRequest, signaling_info: ClientSharedSignalingInfo) -> Result<RoomUser, ErrorType> {
		let group_id = 0;

		let member_attr = {
			if pb.room_member_bin_attr_internal.len() == 1 {
				let bin_attr = &pb.room_member_bin_attr_internal[0];
				let bin_attr_id = bin_attr.id.get_verified()?;
				if bin_attr_id == SCE_NP_MATCHING2_ROOMMEMBER_BIN_ATTR_INTERNAL_1_ID {
					RoomMemberBinAttr::from_protobuf(bin_attr)?
				} else {
					warn!("CreateJoinRoom request with unexpected id in room member bin: {}", bin_attr_id);
					return Err(ErrorType::Malformed);
				}
			} else if !pb.room_member_bin_attr_internal.is_empty() {
				warn!("CreateJoinRoom request with {} member binary attribute(s)!", pb.room_member_bin_attr_internal.len());
				return Err(ErrorType::Malformed);
			} else {
				RoomMemberBinAttr::new()
			}
		};

		let team_id = pb.team_id.get_verified()?;

		Ok(RoomUser {
			user_id: 0,
			npid: String::new(),
			online_name: String::new(),
			avatar_url: String::new(),
			join_date: Client::get_psn_timestamp(),
			flag_attr: 0,

			group_id,
			member_attr,
			team_id,
			member_id: 0,

			signaling_info,
		})
	}

	pub fn from_JoinRoomRequest(pb: &JoinRoomRequest, signaling_info: ClientSharedSignalingInfo) -> Result<RoomUser, ErrorType> {
		let group_id = 0;

		if !pb.join_room_group_label.is_empty() {
			// TODO: Find/Create corresponding group and set id
		}

		let member_attr = {
			if pb.room_member_bin_attr_internal.len() == 1 {
				let bin_attr = &pb.room_member_bin_attr_internal[0];
				let bin_attr_id = bin_attr.id.get_verified()?;
				if bin_attr_id == SCE_NP_MATCHING2_ROOMMEMBER_BIN_ATTR_INTERNAL_1_ID {
					RoomMemberBinAttr::from_protobuf(bin_attr)?
				} else {
					warn!("JoinRoom request with unexpected id in room member bin: {}", bin_attr_id);
					// ensure coherency
					RoomMemberBinAttr::new()
				}
			} else if !pb.room_member_bin_attr_internal.is_empty() {
				warn!("JoinRoom request with {} member binary attribute(s)!", pb.room_member_bin_attr_internal.len());
				RoomMemberBinAttr::new()
			} else {
				RoomMemberBinAttr::new()
			}
		};

		let team_id = pb.team_id.get_verified()?;

		Ok(RoomUser {
			user_id: 0,
			npid: String::new(),
			online_name: String::new(),
			avatar_url: String::new(),
			join_date: Client::get_psn_timestamp(),
			flag_attr: 0,

			group_id,
			member_attr,
			team_id,
			member_id: 0,

			signaling_info,
		})
	}

	pub fn to_RoomMemberDataInternal(&self, room: &Room) -> RoomMemberDataInternal {
		let user_info = Some(UserInfo {
			np_id: self.npid.clone(),
			online_name: self.online_name.clone(),
			avatar_url: self.avatar_url.clone(),
		});

		let bin_attr = vec![self.member_attr.to_protobuf()];

		let room_group = if self.group_id != 0 {
			Some(room.group_config[self.group_id as usize - 1].to_protobuf())
		} else {
			None
		};

		RoomMemberDataInternal {
			user_info,
			join_date: self.join_date,
			member_id: self.member_id as u32,
			team_id: Uint8::new_from_value(self.team_id),
			room_group,
			nat_type: Uint8::new_from_value(2),
			flag_attr: self.flag_attr,
			room_member_bin_attr_internal: bin_attr,
		}
	}

	pub fn to_roomMemberDataExternal(&self) -> RoomMemberDataExternal {
		let user_info = Some(UserInfo {
			np_id: self.npid.clone(),
			online_name: self.online_name.clone(),
			avatar_url: self.avatar_url.clone(),
		});

		let join_date = self.join_date;
		let role = if (self.flag_attr & SCE_NP_MATCHING2_ROOMMEMBER_FLAG_ATTR_OWNER) != 0 {
			Uint8::new_from_value(SCE_NP_MATCHING2_ROLE_OWNER)
		} else {
			Uint8::new_from_value(SCE_NP_MATCHING2_ROLE_MEMBER)
		};

		RoomMemberDataExternal {
			user_info,
			join_date,
			role,
		}
	}
}

pub struct Room {
	// Info given from stream
	world_id: u32,
	lobby_id: u64,
	max_slot: u16,
	flag_attr: u32,
	bin_attr_internal: [RoomBinAttrInternal; 2],
	bin_attr_external: [RoomBinAttr<256>; 2],
	search_bin_attr: RoomBinAttr<64>,
	search_int_attr: [RoomIntAttr; 8],
	room_password: Option<[u8; 8]>,
	group_config: Vec<RoomGroupConfig>,
	password_slot_mask: u64,
	allowed_users: Vec<String>,
	blocked_users: Vec<String>,
	signaling_param: Option<SignalParam>,

	// Data not from stream
	server_id: u16,
	room_id: u64,
	slots: Vec<bool>,
	member_id_counter: u16,
	users: BTreeMap<u16, RoomUser>,
	owner: u16,

	// Set by SetInternal
	owner_succession: VecDeque<u16>,
}
impl Room {
	pub fn from_protobuf(pb: &CreateJoinRoomRequest) -> Result<Room, ErrorType> {
		let world_id = pb.world_id;
		let lobby_id = pb.lobby_id;
		let max_slot = pb.max_slot as u16;
		let flag_attr = pb.flag_attr & !PROTECTED_ROOM_FLAGS;
		let mut bin_attr_internal: [RoomBinAttrInternal; 2] = [
			RoomBinAttrInternal::with_id(SCE_NP_MATCHING2_ROOM_BIN_ATTR_INTERNAL_1_ID),
			RoomBinAttrInternal::with_id(SCE_NP_MATCHING2_ROOM_BIN_ATTR_INTERNAL_2_ID),
		];
		let mut bin_attr_external: [RoomBinAttr<256>; 2] = [
			RoomBinAttr::<256>::with_id(SCE_NP_MATCHING2_ROOM_BIN_ATTR_EXTERNAL_1_ID),
			RoomBinAttr::<256>::with_id(SCE_NP_MATCHING2_ROOM_BIN_ATTR_EXTERNAL_2_ID),
		];
		let mut search_bin_attr: RoomBinAttr<64> = RoomBinAttr::<64>::with_id(SCE_NP_MATCHING2_ROOM_SEARCHABLE_BIN_ATTR_EXTERNAL_1_ID);
		let mut search_int_attr: [RoomIntAttr; 8] = [
			RoomIntAttr::with_id(SCE_NP_MATCHING2_ROOM_SEARCHABLE_INT_ATTR_EXTERNAL_1_ID),
			RoomIntAttr::with_id(SCE_NP_MATCHING2_ROOM_SEARCHABLE_INT_ATTR_EXTERNAL_2_ID),
			RoomIntAttr::with_id(SCE_NP_MATCHING2_ROOM_SEARCHABLE_INT_ATTR_EXTERNAL_3_ID),
			RoomIntAttr::with_id(SCE_NP_MATCHING2_ROOM_SEARCHABLE_INT_ATTR_EXTERNAL_4_ID),
			RoomIntAttr::with_id(SCE_NP_MATCHING2_ROOM_SEARCHABLE_INT_ATTR_EXTERNAL_5_ID),
			RoomIntAttr::with_id(SCE_NP_MATCHING2_ROOM_SEARCHABLE_INT_ATTR_EXTERNAL_6_ID),
			RoomIntAttr::with_id(SCE_NP_MATCHING2_ROOM_SEARCHABLE_INT_ATTR_EXTERNAL_7_ID),
			RoomIntAttr::with_id(SCE_NP_MATCHING2_ROOM_SEARCHABLE_INT_ATTR_EXTERNAL_8_ID),
		];
		let mut room_password = None;
		let mut group_config: Vec<RoomGroupConfig> = Vec::new();
		let mut allowed_users: Vec<String> = Vec::new();
		let mut blocked_users: Vec<String> = Vec::new();
		let mut signaling_param = None;

		for room_binattr_internal_from_pb in &pb.room_bin_attr_internal {
			let room_binattr_internal = RoomBinAttrInternal::from_protobuf(room_binattr_internal_from_pb, CREATOR_ROOM_MEMBER_ID)?;

			if room_binattr_internal.data.id != SCE_NP_MATCHING2_ROOM_BIN_ATTR_INTERNAL_1_ID && room_binattr_internal.data.id != SCE_NP_MATCHING2_ROOM_BIN_ATTR_INTERNAL_2_ID {
				warn!("Invalid Room BinAttr Internal ID in CreateRoom: {}", room_binattr_internal.data.id);
				continue;
			}

			let id = room_binattr_internal.data.id;
			bin_attr_internal[(id - SCE_NP_MATCHING2_ROOM_BIN_ATTR_INTERNAL_1_ID) as usize] = room_binattr_internal;
		}

		for room_binattr_external_from_pb in &pb.room_bin_attr_external {
			let room_binattr_external = RoomBinAttr::from_protobuf(room_binattr_external_from_pb)?;

			if room_binattr_external.id != SCE_NP_MATCHING2_ROOM_BIN_ATTR_EXTERNAL_1_ID && room_binattr_external.id != SCE_NP_MATCHING2_ROOM_BIN_ATTR_EXTERNAL_2_ID {
				warn!("Invalid Room BinAttr External ID in CreateRoom: {}", room_binattr_external.id);
				continue;
			}

			let id = room_binattr_external.id;
			bin_attr_external[(id - SCE_NP_MATCHING2_ROOM_BIN_ATTR_EXTERNAL_1_ID) as usize] = room_binattr_external;
		}

		for room_binattr_search_from_pb in &pb.room_searchable_bin_attr_external {
			let room_binattr_search = RoomBinAttr::from_protobuf(room_binattr_search_from_pb)?;

			if room_binattr_search.id != SCE_NP_MATCHING2_ROOM_SEARCHABLE_BIN_ATTR_EXTERNAL_1_ID {
				warn!("Invalid Room BinAttr Search ID in CreateRoom: {}", room_binattr_search.id);
				continue;
			}
			search_bin_attr = room_binattr_search;
		}

		for room_intattr_from_pb in &pb.room_searchable_int_attr_external {
			let room_intattr = RoomIntAttr::from_protobuf(room_intattr_from_pb)?;
			if room_intattr.id < SCE_NP_MATCHING2_ROOM_SEARCHABLE_INT_ATTR_EXTERNAL_1_ID || room_intattr.id > SCE_NP_MATCHING2_ROOM_SEARCHABLE_INT_ATTR_EXTERNAL_8_ID {
				warn!("Invalid Room IntAttr ID in CreateRoom: {}", room_intattr.id);
				continue;
			}

			let id = room_intattr.id;
			search_int_attr[(id - SCE_NP_MATCHING2_ROOM_SEARCHABLE_INT_ATTR_EXTERNAL_1_ID) as usize] = room_intattr;
		}

		if pb.room_password.len() == 8 {
			let mut room_password_data = [0; 8];
			room_password_data.clone_from_slice(&pb.room_password[0..8]);
			room_password = Some(room_password_data);
		} else if !pb.room_password.is_empty() {
			warn!("Invalid password length in CreateRoom: {}", pb.room_password.len());
		}

		for (i, gc) in pb.group_config.iter().enumerate() {
			let group_id = (i + 1) as u8;
			group_config.push(RoomGroupConfig::from_protobuf(gc, group_id));
		}

		let password_slot_mask = pb.password_slot_mask;
		for user in &pb.allowed_user {
			allowed_users.push(user.clone());
		}
		for user in &pb.blocked_user {
			blocked_users.push(user.clone());
		}

		if let Some(ref sig_opt) = pb.sig_opt_param {
			signaling_param = Some(SignalParam::from_protobuf(sig_opt)?);
		}

		Ok(Room {
			world_id,
			lobby_id,
			room_id: 0,
			max_slot,
			flag_attr,
			bin_attr_internal,
			bin_attr_external,
			search_bin_attr,
			search_int_attr,
			room_password,
			group_config,
			password_slot_mask,
			allowed_users,
			blocked_users,
			signaling_param,
			server_id: 0,
			slots: vec![false; max_slot as usize],
			member_id_counter: 0,
			users: BTreeMap::new(),
			owner: 0,
			owner_succession: VecDeque::new(),
		})
	}

	pub fn to_RoomDataInternal(&self) -> RoomDataInternal {
		let member_list: Vec<RoomMemberDataInternal> = self.users.values().map(|user| user.to_RoomMemberDataInternal(self)).collect();

		let room_group: Vec<RoomGroup> = self.group_config.iter().map(|group| group.to_protobuf()).collect();

		// Atm we include all the bin attr, no matter what, is this correct?
		let room_bin_attr_internal: Vec<BinAttrInternal> = self.bin_attr_internal.iter().map(|bin| bin.to_protobuf()).collect();

		RoomDataInternal {
			server_id: Uint16::new_from_value(self.server_id),
			world_id: self.world_id,
			lobby_id: self.lobby_id,
			room_id: self.room_id,
			password_slot_mask: self.password_slot_mask,
			max_slot: self.max_slot as u32,
			member_list,
			owner_id: Uint16::new_from_value(self.owner),
			room_group,
			flag_attr: self.flag_attr,
			room_bin_attr_internal,
		}
	}

	pub fn to_OptParam(&self) -> Option<OptParam> {
		self.signaling_param.as_ref().map(|s| s.to_protobuf())
	}

	pub fn to_RoomDataExternal(&self, search_option: i32, inc_attrs: &Vec<u16>) -> RoomDataExternal {
		let owner_info = if (search_option & 0x7) != 0 {
			let owner_user = self.users.get(&self.owner).unwrap();
			let mut info = UserInfo {
				np_id: owner_user.npid.clone(),
				online_name: String::new(),
				avatar_url: String::new(),
			};

			if (search_option & 0x2) != 0 {
				info.online_name = owner_user.online_name.clone();
			}
			if (search_option & 0x4) != 0 {
				info.avatar_url = owner_user.avatar_url.clone();
			}

			Some(info)
		} else {
			None
		};

		let room_group: Vec<RoomGroup> = self.group_config.iter().map(|group| group.to_protobuf()).collect();

		let mut vec_searchint = Vec::new();
		let mut vec_searchbin = Vec::new();
		let mut vec_binattrexternal = Vec::new();

		'inc_loop: for inc_attr in inc_attrs {
			match *inc_attr {
				SCE_NP_MATCHING2_ROOM_SEARCHABLE_INT_ATTR_EXTERNAL_1_ID..=SCE_NP_MATCHING2_ROOM_SEARCHABLE_INT_ATTR_EXTERNAL_8_ID => {
					vec_searchint.push(self.search_int_attr[(*inc_attr - SCE_NP_MATCHING2_ROOM_SEARCHABLE_INT_ATTR_EXTERNAL_1_ID) as usize].to_protobuf());
				}
				SCE_NP_MATCHING2_ROOM_SEARCHABLE_BIN_ATTR_EXTERNAL_1_ID => {
					vec_searchbin.push(self.search_bin_attr.to_protobuf());
				}
				SCE_NP_MATCHING2_ROOM_BIN_ATTR_EXTERNAL_1_ID..=SCE_NP_MATCHING2_ROOM_BIN_ATTR_EXTERNAL_2_ID => {
					vec_binattrexternal.push(self.bin_attr_external[(*inc_attr - SCE_NP_MATCHING2_ROOM_BIN_ATTR_EXTERNAL_1_ID) as usize].to_protobuf());
				}
				v => {
					warn!("Invalid ID included in to_inc in to_RoomDataExternal: {}", v);
					continue 'inc_loop;
				}
			}
		}

		let mut max_private_slots = 0u16;
		let mut open_public_slots = 0u16;
		let mut open_private_slots = 0u16;

		if self.room_password.is_some() {
			for i in 0..self.max_slot as usize {
				if (self.password_slot_mask & (1 << (63 - i))) != 0 {
					max_private_slots += 1;
					if !self.slots[i] {
						open_private_slots += 1;
					}
				} else if !self.slots[i] {
					open_public_slots += 1;
				}
			}
		} else {
			open_public_slots = self.max_slot;
		}

		let max_public_slots = self.max_slot - max_private_slots;

		RoomDataExternal {
			server_id: Uint16::new_from_value(self.server_id),
			world_id: self.world_id,
			public_slot_num: Uint16::new_from_value(max_public_slots),
			private_slot_num: Uint16::new_from_value(max_private_slots),
			lobby_id: self.lobby_id,
			room_id: self.room_id,
			open_public_slot_num: Uint16::new_from_value(open_public_slots),
			max_slot: Uint16::new_from_value(self.max_slot),
			open_private_slot_num: Uint16::new_from_value(open_private_slots),
			cur_member_num: Uint16::new_from_value(self.users.len() as u16), // Verification?
			password_slot_mask: self.password_slot_mask,
			owner: owner_info,
			room_group,
			flag_attr: self.flag_attr,
			room_searchable_int_attr_external: vec_searchint,
			room_searchable_bin_attr_external: vec_searchbin,
			room_bin_attr_external: vec_binattrexternal,
		}
	}

	pub fn get_room_member_update_info(&self, member_id: u16, event_cause: EventCause, user_opt_data: Option<&PresenceOptionData>) -> RoomMemberUpdateInfo {
		assert!(self.users.contains_key(&member_id));
		let user = self.users.get(&member_id).unwrap();

		let member_internal = user.to_RoomMemberDataInternal(self);

		let opt_data = user_opt_data.cloned();

		RoomMemberUpdateInfo {
			room_member_data_internal: Some(member_internal),
			event_cause: Uint8::new_from_value(event_cause as u8),
			opt_data,
		}
	}

	pub fn get_room_users(&self) -> HashMap<u16, i64> {
		let mut users_vec = HashMap::new();
		for user in &self.users {
			users_vec.insert(*user.0, user.1.user_id);
		}

		users_vec
	}
	pub fn get_room_user_ids(&self) -> HashSet<i64> {
		let mut users = HashSet::new();
		for user in &self.users {
			users.insert(user.1.user_id);
		}

		users
	}
	pub fn get_member_id(&self, user_id: i64) -> Result<u16, ErrorType> {
		for user in &self.users {
			if user.1.user_id == user_id {
				return Ok(*user.0);
			}
		}

		Err(ErrorType::NotFound)
	}
	pub fn get_owner(&self) -> u16 {
		self.owner
	}

	pub fn is_match(&self, req: &SearchRoomRequest) -> Result<bool, ErrorType> {
		// Hidden rooms never turn up in searches
		if (self.flag_attr & (SceNpMatching2FlagAttr::SCE_NP_MATCHING2_ROOM_FLAG_ATTR_HIDDEN as u32)) != 0 {
			return Ok(false);
		}

		let mut flag_filter = req.flag_filter;
		let mut flag_attr = req.flag_attr;

		// We ignore the SCE_NP_MATCHING2_ROOM_FLAG_ATTR_NAT_TYPE_RESTRICTION attribute as it is pretty irrelevant to rpcs3 and caused issue with top spin search
		flag_filter &= !(SceNpMatching2FlagAttr::SCE_NP_MATCHING2_ROOM_FLAG_ATTR_NAT_TYPE_RESTRICTION as u32);
		flag_attr &= !(SceNpMatching2FlagAttr::SCE_NP_MATCHING2_ROOM_FLAG_ATTR_NAT_TYPE_RESTRICTION as u32);

		if (self.flag_attr & flag_filter) != flag_attr {
			return Ok(false);
		}

		for intfilter in &req.int_filter {
			let op = intfilter.search_operator.get_verified()?;
			let attr = intfilter.attr.as_ref();
			if attr.is_none() {
				warn!("intFilter with no attr!");
				continue;
			}
			let attr = attr.unwrap();
			let id = attr.id.get_verified()?;
			let num = attr.num;

			if id < SCE_NP_MATCHING2_ROOM_SEARCHABLE_INT_ATTR_EXTERNAL_1_ID || id > SCE_NP_MATCHING2_ROOM_SEARCHABLE_INT_ATTR_EXTERNAL_8_ID {
				warn!("Invalid Room IntAttr ID in search parameters: {}", id);
				return Ok(false);
			}

			// Find matching id
			let found_intsearch = &self.search_int_attr[(id - SCE_NP_MATCHING2_ROOM_SEARCHABLE_INT_ATTR_EXTERNAL_1_ID) as usize];
			let op: Option<SceNpMatching2Operator> = FromPrimitive::from_u8(op);
			if op.is_none() {
				warn!("Unsupported op in int search filter");
				return Ok(false);
			}
			let op = op.unwrap();

			match op {
				SceNpMatching2Operator::OperatorEq => {
					if found_intsearch.attr != num {
						return Ok(false);
					}
				}
				SceNpMatching2Operator::OperatorNe => {
					if found_intsearch.attr == num {
						return Ok(false);
					}
				}
				SceNpMatching2Operator::OperatorLt => {
					if found_intsearch.attr >= num {
						return Ok(false);
					}
				}
				SceNpMatching2Operator::OperatorLe => {
					if found_intsearch.attr > num {
						return Ok(false);
					}
				}
				SceNpMatching2Operator::OperatorGt => {
					if found_intsearch.attr <= num {
						return Ok(false);
					}
				}
				SceNpMatching2Operator::OperatorGe => {
					if found_intsearch.attr < num {
						return Ok(false);
					}
				}
			}
		}

		for binfilter in &req.bin_filter {
			let op = binfilter.search_operator.get_verified()?;
			let attr = binfilter.attr.as_ref();
			if attr.is_none() {
				continue;
			}
			let attr = attr.unwrap();
			let id = attr.id.get_verified()?;
			let data = &attr.data;

			if id != SCE_NP_MATCHING2_ROOM_SEARCHABLE_BIN_ATTR_EXTERNAL_1_ID {
				warn!("Invalid Search BinAttr ID in search parameters: {}", id);
				return Ok(false);
			}

			let op: Option<SceNpMatching2Operator> = FromPrimitive::from_u8(op);
			if op.is_none() {
				warn!("Unsupported op in bin search filter");
				return Ok(false);
			}
			let op = op.unwrap();

			// Unsure if cur_size should be compared to data's size
			let len_compare = std::cmp::min(data.len(), self.search_bin_attr.attr.len());
			let equality = self.search_bin_attr.attr[0..len_compare] == data[0..len_compare];

			match op {
				SceNpMatching2Operator::OperatorEq => {
					if !equality {
						return Ok(false);
					}
				}
				SceNpMatching2Operator::OperatorNe => {
					if equality {
						return Ok(false);
					}
				}
				_ => panic!("Non EQ/NE in binfilter!"),
			}
		}
		Ok(true)
	}

	fn is_slot_private(&self, slot: usize) -> bool {
		(self.password_slot_mask & (64 - slot as u64)) != 0
	}

	fn occupy_slot(&mut self, slot: usize) -> u16 {
		assert!(!self.slots[slot - 1]);
		self.slots[slot - 1] = true;
		let member_id = ((slot as u16) << 4) + self.member_id_counter;
		self.member_id_counter = (self.member_id_counter + 1) & 0xF;

		member_id
	}

	fn req_slot(&mut self, private: bool) -> Result<u16, ErrorType> {
		for i in 1..=(self.max_slot as usize) {
			if !self.slots[i - 1] && private == self.is_slot_private(i) {
				return Ok(self.occupy_slot(i));
			}
		}

		Err(ErrorType::RoomFull)
	}

	fn req_specific_slots(&mut self, start_slot: usize, num_slots: usize) -> u16 {
		for i in start_slot..(start_slot + num_slots) {
			if !self.slots[i - 1] {
				return self.occupy_slot(i);
			}
		}
		unreachable!("req_specific_slots didn't find a slot!");
	}

	pub fn free_slot(&mut self, member_id: u16) -> Result<(), ()> {
		let slot = ((member_id >> 4) - 1) as usize;
		if self.slots[slot] {
			self.slots[slot] = false;
			Ok(())
		} else {
			Err(())
		}
	}

	pub fn find_user(&self, user_id: i64) -> u16 {
		for user in &self.users {
			if user.1.user_id == user_id {
				return *user.0;
			}
		}

		0
	}

	fn get_a_slot(&mut self, join_label: Option<[u8; 8]>, password: Option<[u8; 8]>) -> Result<(u8, u16), ErrorType> {
		if !self.group_config.is_empty() {
			if join_label.is_none() {
				warn!("Tried to join a group room without a label!");
				return Err(ErrorType::RoomGroupNoJoinLabel);
			}

			let mut cur_slot_id = 1;
			// First try to see if the group already exist
			for (group_id, group) in self.group_config.iter_mut().enumerate() {
				if group.label == join_label {
					// Found the group
					if group.with_password && password != self.room_password {
						warn!("Tried to join a private group without a password or with a wrong password!");
						return Err(ErrorType::RoomPasswordMismatch);
					}

					if group.num_members >= group.slot_num {
						warn!("Tried to join a full group!");
						return Err(ErrorType::RoomGroupFull);
					}

					group.num_members += 1;
					let slot_num = group.slot_num as usize;

					let member_id = self.req_specific_slots(cur_slot_id, slot_num);
					return Ok(((group_id + 1) as u8, member_id));
				} else {
					cur_slot_id += group.slot_num as usize;
				}
			}
			// If that fails then we search for a group with no label and we set it
			cur_slot_id = 1;
			'labelless_search: for (group_id, group) in self.group_config.iter_mut().enumerate() {
				if group.label.is_none() {
					// Found a possible group
					if group.with_password {
						if password.is_none() {
							cur_slot_id += group.slot_num as usize;
							continue 'labelless_search;
						}

						if password != self.room_password {
							warn!("Tried to join a private group with a wrong password!");
							return Err(ErrorType::RoomPasswordMismatch);
						}
					}

					group.num_members += 1;
					let slot_num = group.slot_num as usize;

					let member_id = self.req_specific_slots(cur_slot_id, slot_num);
					return Ok(((group_id + 1) as u8, member_id));
				} else {
					cur_slot_id += group.slot_num as usize;
				}
			}

			// If both failed then we couldn't find the label
			Err(ErrorType::RoomGroupJoinLabelNotFound)
		} else {
			// Determine lowest member id available
			let member_id = if let Some(password) = password {
				if Some(password) == self.room_password {
					// We try to get a private slot and if it fails we try to get a public slot
					match self.req_slot(true) {
						Ok(member_id) => member_id,
						Err(_) => self.req_slot(false)?,
					}
				} else {
					warn!("User submitted a password but it was invalid!");
					return Err(ErrorType::RoomPasswordMismatch);
				}
			} else {
				// Try to get a public slot
				self.req_slot(false)?
			};

			Ok((0, member_id))
		}
	}
}

pub struct RoomManager {
	rooms: HashMap<(ComId, u64), Room>,               // (com_id, roomid)/roomdata
	room_cnt: HashMap<ComId, u64>,                    // counter per com_id
	world_rooms: HashMap<(ComId, u32), HashSet<u64>>, // (com_id, worldid)/roomids
	lobby_rooms: HashMap<(ComId, u64), HashSet<u64>>, // (com_id, lobbyid)/roomids
	user_rooms: HashMap<i64, HashSet<(ComId, u64)>>,  // List of user / list of rooms
}

impl RoomManager {
	pub fn new() -> RoomManager {
		RoomManager {
			rooms: HashMap::new(),
			room_cnt: HashMap::new(),
			world_rooms: HashMap::new(),
			lobby_rooms: HashMap::new(),
			user_rooms: HashMap::new(),
		}
	}

	pub fn room_exists(&self, com_id: &ComId, room_id: u64) -> bool {
		self.rooms.contains_key(&(*com_id, room_id))
	}
	pub fn get_room(&self, com_id: &ComId, room_id: u64) -> &Room {
		self.rooms.get(&(*com_id, room_id)).unwrap()
	}
	pub fn get_mut_room(&mut self, com_id: &ComId, room_id: u64) -> &mut Room {
		self.rooms.get_mut(&(*com_id, room_id)).unwrap()
	}

	pub fn get_room_infos(&self, com_id: &ComId, room_id: u64) -> Result<(u16, u32, u64), ErrorType> {
		if !self.room_exists(com_id, room_id) {
			return Err(ErrorType::RoomMissing);
		}

		let room = self.get_room(com_id, room_id);
		Ok((room.server_id, room.world_id, room.lobby_id))
	}

	fn unfailing_create_room(&mut self, room_id: u64, com_id: &ComId, room: Room, cinfo: &ClientInfo) -> Vec<u8> {
		if room.lobby_id == 0 {
			let daset = self.world_rooms.entry((*com_id, room.world_id)).or_default();
			daset.insert(room_id);
		} else {
			let daset = self.lobby_rooms.entry((*com_id, room.lobby_id)).or_default();
			daset.insert(room_id);
		}

		self.rooms.insert((*com_id, room_id), room);
		self.user_rooms.entry(cinfo.user_id).or_default().insert((*com_id, room_id));

		// Prepare roomDataInternal
		let room = &self.rooms[&(*com_id, room_id)];
		let internal = room.to_RoomDataInternal();
		let opt_param = room.to_OptParam();

		let room_data = CreateRoomResponse { internal: Some(internal), opt_param };

		room_data.encode_to_vec()
	}

	pub fn create_room(&mut self, com_id: &ComId, req: &CreateJoinRoomRequest, cinfo: &ClientInfo, server_id: u16, signaling_info: ClientSharedSignalingInfo) -> Result<Vec<u8>, ErrorType> {
		if req.max_slot == 0 || req.max_slot > 64 {
			return Err(ErrorType::Malformed);
		}

		// Creates the room and user from input protobuf
		let mut room = Room::from_protobuf(req)?;
		let mut room_user = RoomUser::from_CreateJoinRoomRequest(req, signaling_info)?;

		// Nothing failing beyond this point!

		let room_cnt = self.room_cnt.entry(*com_id).or_insert(0);

		// Initial room owner always get slot 1
		let member_id = room.occupy_slot(1);

		room.owner = member_id;
		room.room_id = (*room_cnt) + 1;
		room.server_id = server_id;
		// Add the user as its owner
		room_user.user_id = cinfo.user_id;
		room_user.npid = cinfo.npid.clone();
		room_user.online_name = cinfo.online_name.clone();
		room_user.avatar_url = cinfo.avatar_url.clone();
		room_user.member_id = member_id;
		room_user.flag_attr = SCE_NP_MATCHING2_ROOMMEMBER_FLAG_ATTR_OWNER;

		if !room.group_config.is_empty() {
			// Check that the groups fill the max slots size
			if room.group_config.iter().map(|g| g.slot_num).sum::<u32>() != room.max_slot as u32 {
				warn!("Tried to create a group room where max_slot != sum(group.slot_num)!");
				return Err(ErrorType::RoomGroupMaxSlotMismatch);
			}
			if room.group_config.iter().any(|g| g.with_password) && room.room_password.is_none() {
				warn!("Tried to create a group room with private slots and no password!");
				return Err(ErrorType::RoomPasswordMissing);
			}
			if req.join_room_group_label.is_empty() {
				warn!("Tried to create a group room without a join label!");
				return Err(ErrorType::RoomGroupNoJoinLabel);
			}
			if req.join_room_group_label.len() != SCE_NP_MATCHING2_GROUP_LABEL_SIZE {
				warn!("Tried to create a group room with a join label with size != 8!");
				return Err(ErrorType::Malformed);
			}

			let mut label = [0u8; SCE_NP_MATCHING2_GROUP_LABEL_SIZE];
			label.clone_from_slice(&req.join_room_group_label[0..SCE_NP_MATCHING2_GROUP_LABEL_SIZE]);

			// Requires Group to be the first group
			if room.group_config[0].label.as_ref().is_some_and(|v| *v == label) {
			} else if room.group_config[0].label.is_none() {
				room.group_config[0].label = Some(label);
			} else {
				warn!("Invalid join label when creating the room!");
				return Err(ErrorType::Malformed);
			}

			room.group_config[0].num_members += 1;

			room_user.group_id = 1;
		} else {
			// If private slots are used the password must be set!
			if room.password_slot_mask != 0 && room.room_password.is_none() {
				warn!("Tried to create a room with private slots and no password!");
				return Err(ErrorType::RoomPasswordMissing);
			}
		}

		room.users.insert(member_id, room_user);

		// No error possible past this point
		*room_cnt += 1;
		let room_id = *room_cnt;

		Ok(self.unfailing_create_room(room_id, com_id, room, cinfo))
	}

	pub fn join_room(
		&mut self,
		com_id: &ComId,
		req: &JoinRoomRequest,
		cinfo: &ClientInfo,
		signaling_info: ClientSharedSignalingInfo,
	) -> Result<(Vec<u8>, [Option<(Vec<u8>, HashSet<i64>)>; 4]), ErrorType> {
		let room_id = req.room_id;
		let room = self.rooms.get_mut(&(*com_id, room_id)).unwrap();

		if (room.flag_attr & SceNpMatching2FlagAttr::SCE_NP_MATCHING2_ROOM_FLAG_ATTR_FULL as u32) != 0 {
			return Err(ErrorType::RoomFull);
		}

		if !req.join_room_group_label.is_empty() && req.join_room_group_label.len() != SCE_NP_MATCHING2_GROUP_LABEL_SIZE {
			warn!("Tried to join a room with a join label with size != SCE_NP_MATCHING2_GROUP_LABEL_SIZE!");
			return Err(ErrorType::Malformed);
		}

		if !req.room_password.is_empty() && req.room_password.len() != SCE_NP_MATCHING2_SESSION_PASSWORD_SIZE {
			warn!("Tried to join a room with a password with size != SCE_NP_MATCHING2_SESSION_PASSWORD_SIZE!");
			return Err(ErrorType::Malformed);
		}

		let join_label = if req.join_room_group_label.len() == SCE_NP_MATCHING2_GROUP_LABEL_SIZE {
			let mut label = [0u8; SCE_NP_MATCHING2_GROUP_LABEL_SIZE];
			label.clone_from_slice(&req.join_room_group_label[0..SCE_NP_MATCHING2_GROUP_LABEL_SIZE]);
			Some(label)
		} else {
			None
		};

		let password = if req.room_password.len() == SCE_NP_MATCHING2_SESSION_PASSWORD_SIZE {
			let mut pass = [0u8; SCE_NP_MATCHING2_SESSION_PASSWORD_SIZE];
			pass.clone_from_slice(&req.room_password[0..SCE_NP_MATCHING2_SESSION_PASSWORD_SIZE]);
			Some(pass)
		} else {
			if !req.room_password.is_empty() {
				warn!("room_password.len() != SCE_NP_MATCHING2_SESSION_PASSWORD_SIZE");
			}

			None
		};

		let (group_id, member_id) = room.get_a_slot(join_label, password)?;

		let mut room_user = RoomUser::from_JoinRoomRequest(req, signaling_info.clone())?;
		room_user.user_id = cinfo.user_id;
		room_user.npid = cinfo.npid.clone();
		room_user.online_name = cinfo.online_name.clone();
		room_user.avatar_url = cinfo.avatar_url.clone();
		room_user.member_id = member_id;
		room_user.group_id = group_id;

		// Get all previous user ids before adding our user
		let all_previous_users: HashSet<i64> = room.users.iter().map(|user| user.1.user_id).collect();

		room.users.insert(member_id, room_user);

		// Set full flag if necessary
		if room.users.len() == room.max_slot as usize {
			room.flag_attr |= SceNpMatching2FlagAttr::SCE_NP_MATCHING2_ROOM_FLAG_ATTR_FULL as u32;
		}

		self.user_rooms.entry(cinfo.user_id).or_default().insert((*com_id, room.room_id));

		// Build notifications
		let need_signaling = room.signaling_param.as_ref().is_some_and(|sig_param| sig_param.should_signal());

		// We have 4 possible types of notification:
		// - No signaling
		// - Local Signaling
		// - Public Signaling IPv4
		// - Public Signaling IPv6

		let build_notification = |room: &Room, signaling_info_arg: Option<(&[u8], u16)>| {
			let update_info = Some(room.get_room_member_update_info(member_id, EventCause::None, req.opt_data.as_ref()));
			let signaling = signaling_info_arg.map(|(ip, port)| SignalingAddr {
				ip: ip.to_vec(),
				port: Uint16::new_from_value(port),
			});

			let notif_data = NotificationUserJoinedRoom { room_id, update_info, signaling };

			notif_data.encode_to_vec()
		};

		let add_to_notif_list = |notif_list: &mut Option<(Vec<u8>, HashSet<i64>)>, room: &Room, signaling_info_notif: (&[u8], u16), to_add: i64| match notif_list {
			None => {
				*notif_list = Some((build_notification(room, Some(signaling_info_notif)), [to_add].into()));
			}
			Some((_notif_data, notif_user_ids)) => {
				notif_user_ids.insert(to_add);
			}
		};

		let mut notif_no_signaling = None;
		let mut notif_local_signaling = None;
		let mut notif_public_signaling_ipv4 = None;
		let mut notif_public_signaling_ipv6 = None;

		let mut signaling_list_reply: Vec<(u16, Vec<u8>, u16)> = Vec::new();

		// Signaling
		if need_signaling {
			let sig_param = room.signaling_param.as_ref().unwrap();

			match sig_param.get_type() {
				SignalingType::SignalingMesh => {
					// User should connect to everyone
					// Everyone should connect to user
					for user in room.users.values() {
						if user.member_id == member_id {
							continue;
						}
						if user.signaling_info.addr_p2p_ipv4.0 == signaling_info.addr_p2p_ipv4.0 {
							add_to_notif_list(&mut notif_local_signaling, room, (&signaling_info.local_addr_p2p, 3658), user.user_id);
							signaling_list_reply.push((user.member_id, user.signaling_info.local_addr_p2p.to_vec(), 3658));
						} else {
							match (user.signaling_info.addr_p2p_ipv6, signaling_info.addr_p2p_ipv6) {
								(Some(user_ipv6), Some(joiner_ipv6)) => {
									add_to_notif_list(&mut notif_public_signaling_ipv6, room, (&joiner_ipv6.0, joiner_ipv6.1), user.user_id);
									signaling_list_reply.push((user.member_id, user_ipv6.0.to_vec(), user_ipv6.1));
								}
								_ => {
									add_to_notif_list(&mut notif_public_signaling_ipv4, room, (&signaling_info.addr_p2p_ipv4.0, signaling_info.addr_p2p_ipv4.1), user.user_id);
									signaling_list_reply.push((user.member_id, user.signaling_info.addr_p2p_ipv4.0.to_vec(), user.signaling_info.addr_p2p_ipv4.1));
								}
							}
						}
					}
				}
				SignalingType::SignalingStar => {
					// User should connect to hub
					// Hub should connect to User
					let hub = if sig_param.get_hub() == 0 { room.owner } else { sig_param.get_hub() };
					let hub_user = room.users.get(&hub).unwrap();

					if hub_user.signaling_info.addr_p2p_ipv4.0 == signaling_info.addr_p2p_ipv4.0 {
						add_to_notif_list(&mut notif_local_signaling, room, (&signaling_info.local_addr_p2p, 3658), hub_user.user_id);
						signaling_list_reply.push((hub_user.member_id, hub_user.signaling_info.local_addr_p2p.to_vec(), 3658));
					} else {
						match (hub_user.signaling_info.addr_p2p_ipv6, signaling_info.addr_p2p_ipv6) {
							(Some(hub_user_ipv6), Some(joiner_ipv6)) => {
								add_to_notif_list(&mut notif_public_signaling_ipv6, room, (&joiner_ipv6.0, joiner_ipv6.1), hub_user.user_id);
								signaling_list_reply.push((hub_user.member_id, hub_user_ipv6.0.to_vec(), hub_user_ipv6.1));
							}
							_ => {
								add_to_notif_list(
									&mut notif_public_signaling_ipv4,
									room,
									(&signaling_info.addr_p2p_ipv4.0, signaling_info.addr_p2p_ipv4.1),
									hub_user.user_id,
								);
								signaling_list_reply.push((hub_user.member_id, hub_user.signaling_info.addr_p2p_ipv4.0.to_vec(), hub_user.signaling_info.addr_p2p_ipv4.1));
							}
						}
					}

					let mut all_users_except_hub_user = all_previous_users;
					all_users_except_hub_user.remove(&hub_user.user_id);
					notif_no_signaling = Some((build_notification(room, None), all_users_except_hub_user));
				}
				_ => unreachable!(),
			}
		} else {
			notif_no_signaling = Some((build_notification(room, None), all_previous_users));
		}

		let room_data = Some(room.to_RoomDataInternal());

		let signaling_data: Vec<Matching2SignalingInfo> = signaling_list_reply
			.iter()
			.map(|(mid, ip, port)| Matching2SignalingInfo {
				member_id: Uint16::new_from_value(*mid),
				addr: Some(SignalingAddr {
					ip: ip.clone(),
					port: Uint16::new_from_value(*port),
				}),
			})
			.collect();

		let opt_param = room.to_OptParam();

		let reply = JoinRoomResponse { room_data, signaling_data, opt_param };

		Ok((
			reply.encode_to_vec(),
			[notif_no_signaling, notif_local_signaling, notif_public_signaling_ipv4, notif_public_signaling_ipv6],
		))
	}

	pub fn leave_room(&mut self, com_id: &ComId, room_id: u64, user_id: i64) -> Result<(bool, HashSet<i64>), ErrorType> {
		if !self.room_exists(com_id, room_id) {
			warn!("Attempted to leave a non existing room");
			return Err(ErrorType::RoomMissing);
		}

		if let Some(user_set) = self.user_rooms.get_mut(&user_id) {
			if user_set.get(&(*com_id, room_id)).is_none() {
				warn!("Couldn't find the room in the user user_rooms set");
				return Err(ErrorType::NotFound);
			}
			user_set.remove(&(*com_id, room_id));
		} else {
			warn!("Couldn't find the user in the user_rooms list");
			return Err(ErrorType::NotFound);
		}

		let room = self.get_mut_room(com_id, room_id);
		let member_id = room.find_user(user_id);
		assert!(member_id != 0); // This should never happen as it would mean user_rooms is incoherent

		let member = room.users.remove(&member_id).unwrap();
		if member.group_id != 0 {
			let group = &mut room.group_config[(member.group_id - 1) as usize];

			group.num_members -= 1;
			if group.num_members == 0 && !group.fixed_label {
				group.label = None;
			}
		}

		room.free_slot(member_id).unwrap();

		// Remove full flag if necessary
		if room.users.len() != room.max_slot as usize {
			room.flag_attr &= !(SceNpMatching2FlagAttr::SCE_NP_MATCHING2_ROOM_FLAG_ATTR_FULL as u32);
		}

		// Generate list of users left
		let user_list: HashSet<i64> = room.users.iter().map(|user| user.1.user_id).collect();

		if member_id == room.owner {
			// Check if the room is getting destroyed
			let mut found_successor = false;

			// Try to find successor in the designated successor list
			while let Some(s) = room.owner_succession.pop_front() {
				if room.users.contains_key(&s) {
					found_successor = true;
					room.owner = s;
					room.users.entry(s).and_modify(|e| e.flag_attr |= SCE_NP_MATCHING2_ROOMMEMBER_FLAG_ATTR_OWNER);
					break;
				}
			}

			// If no successor is found and there are still users, assign ownership randomly
			if !found_successor && !room.users.is_empty() {
				let random_user = rand::thread_rng().gen_range(0..room.users.len());
				room.owner = *room.users.keys().nth(random_user).unwrap();
				room.users.entry(room.owner).and_modify(|e| e.flag_attr |= SCE_NP_MATCHING2_ROOMMEMBER_FLAG_ATTR_OWNER);
				found_successor = true;
			}

			if !found_successor {
				// Remove the room from appropriate list
				let lobby_id = room.lobby_id;
				let world_id = room.world_id;

				// Remove users from user_rooms
				for user_id in &user_list {
					self.user_rooms.get_mut(user_id).unwrap().remove(&(*com_id, room_id));
				}

				if lobby_id == 0 {
					self.world_rooms.get_mut(&(*com_id, world_id)).unwrap().remove(&room_id);
				} else {
					self.lobby_rooms.get_mut(&(*com_id, lobby_id)).unwrap().remove(&room_id);
				}
				// Remove from global room list
				self.rooms.remove(&(*com_id, room_id));
				return Ok((true, user_list));
			}
		}

		// TODO: signaling if room is in star signaling mode!

		Ok((false, user_list))
	}

	pub fn search_room(&self, com_id: &ComId, req: &SearchRoomRequest) -> Result<Vec<u8>, ErrorType> {
		let world_id = req.world_id;
		let lobby_id = req.lobby_id;

		let startindex = if req.range_filter_start_index == 0 {
			// This is a common API mistake done by games, downgraded to info
			info!("SearchRoomRequest.startIndex was 0!");
			1
		} else {
			req.range_filter_start_index
		};

		let max = if req.range_filter_max == 0 || req.range_filter_max > 20 {
			warn!("SearchRoomRequest.max was invalid: {}", req.range_filter_max);
			20
		} else {
			req.range_filter_max
		};

		let mut list = None;
		if world_id != 0 {
			list = self.world_rooms.get(&(*com_id, world_id));
		} else if lobby_id != 0 {
			list = self.lobby_rooms.get(&(*com_id, lobby_id));
		}

		let mut matching_rooms = Vec::new();

		if let Some(room_list) = list {
			for room_id in room_list.iter() {
				let room = self.get_room(com_id, *room_id);
				if room.is_match(req)? {
					matching_rooms.push(room);
				}
			}
		}

		let mut rooms = Vec::new();

		if matching_rooms.len() >= startindex as usize {
			let mut inc_attrs: Vec<u16> = Vec::with_capacity(req.attr_id.len());
			for a in &req.attr_id {
				inc_attrs.push(a.get_verified()?);
			}

			let start = startindex as usize - 1;
			let num_to_get = std::cmp::min(matching_rooms.len() - start, max as usize);
			let end = start + num_to_get;

			for room in &matching_rooms[start..end] {
				rooms.push(room.to_RoomDataExternal(req.option, &inc_attrs));
			}
		}

		let resp = SearchRoomResponse {
			start_index: startindex,
			total: matching_rooms.len() as u32,
			rooms,
		};
		Ok(resp.encode_to_vec())
	}

	pub fn get_roomdata_external_list(&self, com_id: &ComId, req: &GetRoomDataExternalListRequest) -> Result<Vec<u8>, ErrorType> {
		let mut inc_attrs: Vec<u16> = Vec::with_capacity(req.attr_ids.len());
		for a in &req.attr_ids {
			inc_attrs.push(a.get_verified()?);
		}

		let rooms: Vec<RoomDataExternal> = req
			.room_ids
			.iter()
			.filter(|room_id| self.room_exists(com_id, **room_id))
			.map(|room_id| self.get_room(com_id, *room_id).to_RoomDataExternal(7, &inc_attrs))
			.collect();

		let resp = GetRoomDataExternalListResponse { rooms };
		Ok(resp.encode_to_vec())
	}

	pub fn get_room_member_data_external_list(&self, com_id: &ComId, room_id: u64) -> Result<Vec<u8>, ErrorType> {
		if !self.room_exists(com_id, room_id) {
			return Err(ErrorType::RoomMissing);
		}

		let room = self.get_room(com_id, room_id);
		let members: Vec<RoomMemberDataExternal> = room.users.iter().map(|(_, user)| user.to_roomMemberDataExternal()).collect();

		let resp = GetRoomMemberDataExternalListResponse {
			members,
		};

		Ok(resp.encode_to_vec())
	}

	pub fn set_roomdata_external(&mut self, com_id: &ComId, req: &SetRoomDataExternalRequest, user_id: i64) -> Result<(), ErrorType> {
		if !self.room_exists(com_id, req.room_id) {
			return Err(ErrorType::RoomMissing);
		}
		let room = self.get_mut_room(com_id, req.room_id);

		let member_id = room.get_member_id(user_id)?;
		let is_room_owner = room.get_owner() == member_id;

		// Only the room owner can change external information of the room
		if !is_room_owner {
			return Err(ErrorType::Unauthorized);
		}

		for room_binattr_external_from_pb in &req.room_bin_attr_external {
			let room_binattr_external = RoomBinAttr::from_protobuf(room_binattr_external_from_pb)?;

			if room_binattr_external.id != SCE_NP_MATCHING2_ROOM_BIN_ATTR_EXTERNAL_1_ID && room_binattr_external.id != SCE_NP_MATCHING2_ROOM_BIN_ATTR_EXTERNAL_2_ID {
				warn!("Invalid Room BinAttr External ID in SetRoomDataExternal: {}", room_binattr_external.id);
				continue;
			}

			let id = room_binattr_external.id;
			room.bin_attr_external[(id - SCE_NP_MATCHING2_ROOM_BIN_ATTR_EXTERNAL_1_ID) as usize] = room_binattr_external;
		}

		for room_binattr_search_from_pb in &req.room_searchable_bin_attr_external {
			let room_binattr_search = RoomBinAttr::from_protobuf(room_binattr_search_from_pb)?;

			if room_binattr_search.id != SCE_NP_MATCHING2_ROOM_SEARCHABLE_BIN_ATTR_EXTERNAL_1_ID {
				warn!("Invalid Room BinAttr Search ID in SetRoomDataExternal: {}", room_binattr_search.id);
				continue;
			}
			room.search_bin_attr = room_binattr_search;
		}

		for room_intattr_from_pb in &req.room_searchable_int_attr_external {
			let room_intattr = RoomIntAttr::from_protobuf(room_intattr_from_pb)?;
			if room_intattr.id < SCE_NP_MATCHING2_ROOM_SEARCHABLE_INT_ATTR_EXTERNAL_1_ID || room_intattr.id > SCE_NP_MATCHING2_ROOM_SEARCHABLE_INT_ATTR_EXTERNAL_8_ID {
				warn!("Invalid Room IntAttr ID in SetRoomDataExternal: {}", room_intattr.id);
				continue;
			}

			let id = room_intattr.id;
			room.search_int_attr[(id - SCE_NP_MATCHING2_ROOM_SEARCHABLE_INT_ATTR_EXTERNAL_1_ID) as usize] = room_intattr;
		}

		Ok(())
	}

	pub fn get_roomdata_internal(&self, com_id: &ComId, req: &GetRoomDataInternalRequest) -> Result<Vec<u8>, ErrorType> {
		if !self.room_exists(com_id, req.room_id) {
			return Err(ErrorType::RoomMissing);
		}
		let room = self.get_room(com_id, req.room_id);

		let room_data = room.to_RoomDataInternal();
		Ok(room_data.encode_to_vec())
	}

	pub fn set_roomdata_internal(&mut self, com_id: &ComId, req: &SetRoomDataInternalRequest, user_id: i64) -> Result<Option<(HashSet<i64>, Vec<u8>)>, ErrorType> {
		if !self.room_exists(com_id, req.room_id) {
			return Err(ErrorType::RoomMissing);
		}
		let room = self.get_mut_room(com_id, req.room_id);
		let member_id = room.get_member_id(user_id)?;

		let is_room_owner = room.get_owner() == member_id;
		let mut has_changed = false;

		let old_password_slot_mask = room.password_slot_mask;
		let old_flag_attr = room.flag_attr;
		let mut vec_new_groups: Vec<u8> = Vec::new();

		if is_room_owner {
			let flag_filter = req.flag_filter & !PROTECTED_ROOM_FLAGS;
			let flag_attr = req.flag_attr & !PROTECTED_ROOM_FLAGS;
			let new_room_flag_attr = (flag_attr & flag_filter) | (room.flag_attr & !flag_filter);

			if new_room_flag_attr != room.flag_attr {
				room.flag_attr = new_room_flag_attr;
				has_changed = true;
			}

			for group_config in &req.password_config {
				let group_id = group_config.group_id.get_verified()?;

				if group_id as usize > room.group_config.len() {
					warn!("set_roomdata_internal: group_id > roomgroup_config.len() ( {} : {} )", group_id, room.group_config.len());
					continue;
				}

				if room.group_config[(group_id - 1) as usize].with_password != group_config.with_password {
					room.group_config[(group_id - 1) as usize].with_password = group_config.with_password;
					vec_new_groups.push(group_id);
					has_changed = true;
				}
			}

			if req.password_slot_mask.len() == 1 {
				let password_slot_mask = req.password_slot_mask[0];
				if password_slot_mask != old_password_slot_mask {
					room.password_slot_mask = password_slot_mask;
					has_changed = true;
				}
			}

			if !req.owner_privilege_rank.is_empty() {
				let mut succession_list: VecDeque<u16> = VecDeque::new();
				for rank in &req.owner_privilege_rank {
					succession_list.push_back(rank.get_verified()?);
				}

				if succession_list != room.owner_succession {
					room.owner_succession = succession_list;
					has_changed = true;
				}
			}
		}

		let mut new_room_bin_attr_internal = Vec::new();
		for room_binattr_internal_from_pb in &req.room_bin_attr_internal {
			let room_binattr_internal = RoomBinAttrInternal::from_protobuf(room_binattr_internal_from_pb, member_id)?;

			if room_binattr_internal.data.id != SCE_NP_MATCHING2_ROOM_BIN_ATTR_INTERNAL_1_ID && room_binattr_internal.data.id != SCE_NP_MATCHING2_ROOM_BIN_ATTR_INTERNAL_2_ID {
				warn!("Invalid Room BinAttr Internal ID in SetRoomDataInternal: {}", room_binattr_internal.data.id);
				continue;
			}

			let id = room_binattr_internal.data.id;
			room.bin_attr_internal[(id - SCE_NP_MATCHING2_ROOM_BIN_ATTR_INTERNAL_1_ID) as usize] = room_binattr_internal;
			new_room_bin_attr_internal.push(Uint16 { value: id as u32 });
			has_changed = true;
		}

		if has_changed {
			// Build the notification buffer
			let room_data_internal = room.to_RoomDataInternal();

			let resp = RoomDataInternalUpdateInfo {
				new_room_data_internal: Some(room_data_internal),
				prev_flag_attr: old_flag_attr,
				prev_room_password_slot_mask: old_password_slot_mask,
				new_room_group: vec_new_groups,
				new_room_bin_attr_internal,
			};

			let mut to_notif = room.get_room_user_ids();
			to_notif.remove(&user_id);

			return Ok(Some((to_notif, resp.encode_to_vec())));
		}

		Ok(None)
	}

	pub fn get_roommemberdata_internal(&self, com_id: &ComId, req: &GetRoomMemberDataInternalRequest) -> Result<Vec<u8>, ErrorType> {
		if !self.room_exists(com_id, req.room_id) {
			return Err(ErrorType::RoomMissing);
		}

		let room = self.get_room(com_id, req.room_id);
		let member_id = req.member_id.get_verified()?;
		let user = room.users.get(&member_id).ok_or(ErrorType::NotFound)?;
		let resp = user.to_RoomMemberDataInternal(room);
		Ok(resp.encode_to_vec())
	}

	pub fn set_roommemberdata_internal(&mut self, com_id: &ComId, req: &SetRoomMemberDataInternalRequest, user_id: i64) -> Result<Option<(HashSet<i64>, Vec<u8>)>, ErrorType> {
		if !self.room_exists(com_id, req.room_id) {
			return Err(ErrorType::RoomMissing);
		}

		let mut has_changed = false;
		let mut new_room_member_bin_attr_internal = Vec::new();
		let target_member_id;
		let prev_team_id;
		{
			// Update RoomMemberData
			let room = self.get_mut_room(com_id, req.room_id);
			let member_id = room.get_member_id(user_id)?;
			let req_member_id = req.member_id.get_verified()?;
			target_member_id = if req_member_id == 0 { member_id } else { req_member_id };

			// You can only change a member's binattrs if they are your own or you are room owner
			if (member_id != target_member_id) && (member_id != room.owner) {
				return Err(ErrorType::Unauthorized);
			}

			if !room.users.contains_key(&target_member_id) {
				return Err(ErrorType::NotFound);
			}

			let user = room.users.get_mut(&target_member_id).unwrap();

			let team_id = req.team_id.get_verified()?;
			prev_team_id = user.team_id;
			if team_id != 0 && user.team_id != team_id {
				user.team_id = team_id;
				has_changed = true;
			}

			if req.room_member_bin_attr_internal.len() == 1 {
				let bin_attr = RoomMemberBinAttr::from_protobuf(&req.room_member_bin_attr_internal[0])?;
				if bin_attr.data.id == SCE_NP_MATCHING2_ROOMMEMBER_BIN_ATTR_INTERNAL_1_ID {
					user.member_attr = RoomMemberBinAttr::from_protobuf(&req.room_member_bin_attr_internal[0])?;
					new_room_member_bin_attr_internal.push(Uint16 {
						value: user.member_attr.data.id as u32,
					});
					has_changed = true;
				} else {
					warn!("SetRoomMemberDataInternal request with unexpected id in room member data: {}", bin_attr.data.id);
				}
			} else if !req.room_member_bin_attr_internal.is_empty() {
				warn!("SetRoomMemberDataInternal request with {} member binary attribute(s)!", req.room_member_bin_attr_internal.len());
			}
		}

		if has_changed {
			// Build the notification buffer
			let room = self.get_room(com_id, req.room_id);
			let user = room.users.get(&target_member_id).unwrap();

			let member_internal = user.to_RoomMemberDataInternal(room);

			let resp = RoomMemberDataInternalUpdateInfo {
				new_room_member_data_internal: Some(member_internal),
				prev_flag_attr: user.flag_attr,
				prev_team_id: Uint8::new_from_value(prev_team_id),
				new_room_member_bin_attr_internal,
			};

			let mut to_notif = room.get_room_user_ids();
			to_notif.remove(&user_id);

			Ok(Some((to_notif, resp.encode_to_vec())))
		} else {
			Ok(None)
		}
	}

	pub fn get_rooms_by_user(&self, user: i64) -> Option<HashSet<(ComId, u64)>> {
		if !self.user_rooms.contains_key(&user) {
			return None;
		}

		Some(self.user_rooms.get(&user).unwrap().clone())
	}
}
