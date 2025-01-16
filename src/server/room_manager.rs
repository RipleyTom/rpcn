#![allow(non_snake_case)]
#![allow(dead_code)]
#![allow(non_camel_case_types)]

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use rand::Rng;
use tracing::{error, warn};

use crate::server::client::{Client, ClientInfo, ComId, ErrorType, EventCause};
use crate::server::stream_extractor::fb_helpers::*;
use crate::server::stream_extractor::np2_structs_generated::*;

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

const SCE_NP_MATCHING2_ROOMMEMBER_FLAG_ATTR_OWNER: u32 = 0x80000000;

const CREATOR_ROOM_MEMBER_ID: u16 = 16;

#[repr(u8)]
#[derive(FromPrimitive, Clone, Debug)]
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

	pub fn from_flatbuffer(fb: &BinAttr) -> RoomBinAttr<N> {
		let id = fb.id();
		let mut attr: [u8; N] = [0; N];
		let mut cur_size: usize = 0;
		if let Some(fb_attrs) = fb.data() {
			let len = if fb_attrs.len() > N {
				error!("Error converting a fb BinAttr to a RoombinAttr, mismatched size: fb:{} vs expected:{}", fb_attrs.len(), N);
				N
			} else {
				fb_attrs.len()
			};
			attr[0..len].clone_from_slice(&fb_attrs.bytes()[0..len]);
			cur_size = len;
		}

		RoomBinAttr { id, attr, cur_size }
	}

	pub fn to_flatbuffer<'a>(&self, builder: &mut flatbuffers::FlatBufferBuilder<'a>) -> flatbuffers::WIPOffset<BinAttr<'a>> {
		let final_attr = builder.create_vector(&self.attr[0..self.cur_size]);
		BinAttr::create(builder, &BinAttrArgs { id: self.id, data: Some(final_attr) })
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

	pub fn from_flatbuffer(fb: &BinAttr) -> RoomMemberBinAttr {
		let data = RoomBinAttr::from_flatbuffer(fb);
		RoomMemberBinAttr {
			update_date: Client::get_psn_timestamp(),
			data,
		}
	}

	pub fn to_flatbuffer<'a>(&self, builder: &mut flatbuffers::FlatBufferBuilder<'a>) -> flatbuffers::WIPOffset<RoomMemberBinAttrInternal<'a>> {
		let data = Some(self.data.to_flatbuffer(builder));

		RoomMemberBinAttrInternal::create(builder, &RoomMemberBinAttrInternalArgs { updateDate: self.update_date, data })
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

	pub fn from_flatbuffer(fb: &BinAttr, member_id: u16) -> RoomBinAttrInternal {
		let data = RoomBinAttr::from_flatbuffer(fb);
		RoomBinAttrInternal {
			update_date: Client::get_psn_timestamp(),
			update_member_id: member_id,
			data,
		}
	}

	pub fn to_flatbuffer<'a>(&self, builder: &mut flatbuffers::FlatBufferBuilder<'a>) -> flatbuffers::WIPOffset<BinAttrInternal<'a>> {
		let data = Some(self.data.to_flatbuffer(builder));
		BinAttrInternal::create(
			builder,
			&BinAttrInternalArgs {
				updateDate: self.update_date,
				updateMemberId: self.update_member_id,
				data,
			},
		)
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
	pub fn from_flatbuffer(fb: &IntAttr) -> RoomIntAttr {
		let id = fb.id();
		let attr = fb.num();
		RoomIntAttr { id, attr }
	}
	pub fn to_flatbuffer<'a>(&self, builder: &mut flatbuffers::FlatBufferBuilder<'a>) -> flatbuffers::WIPOffset<IntAttr<'a>> {
		IntAttr::create(builder, &IntAttrArgs { id: self.id, num: self.attr })
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
	pub fn from_flatbuffer(fb: &GroupConfig, group_id: u8) -> RoomGroupConfig {
		let slot_num = fb.slotNum();
		let label = if let Some(vec) = fb.label() {
			if vec.len() == SCE_NP_MATCHING2_GROUP_LABEL_SIZE {
				let mut label_array = [0; SCE_NP_MATCHING2_GROUP_LABEL_SIZE];
				label_array.clone_from_slice(&vec.bytes()[0..SCE_NP_MATCHING2_GROUP_LABEL_SIZE]);
				Some(label_array)
			} else {
				None
			}
		} else {
			None
		};

		let with_password = fb.withPassword();
		RoomGroupConfig {
			slot_num,
			fixed_label: label.is_some(),
			label,
			with_password,
			group_id,
			num_members: 0,
		}
	}

	pub fn to_flatbuffer<'a>(&self, builder: &mut flatbuffers::FlatBufferBuilder<'a>) -> flatbuffers::WIPOffset<RoomGroup<'a>> {
		let label = self.label.map(|label_array| builder.create_vector(&label_array));

		RoomGroup::create(
			builder,
			&RoomGroupArgs {
				groupId: self.group_id,
				withPassword: self.with_password,
				label,
				slotNum: self.slot_num,
				curGroupMemberNum: self.num_members,
			},
		)
	}
}

#[derive(Clone)]
struct SignalParam {
	sig_type: SignalingType,
	flag: u8,
	hub_member_id: u16,
}
impl SignalParam {
	pub fn from_flatbuffer(fb: &OptParam) -> SignalParam {
		let sig_type = FromPrimitive::from_u8(fb.type_()).unwrap_or(SignalingType::SignalingNone);
		let flag = fb.flag();
		let hub_member_id = fb.hubMemberId();

		SignalParam { sig_type, flag, hub_member_id }
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
	pub fn from_CreateJoinRoomRequest(fb: &CreateJoinRoomRequest, signaling_info: ClientSharedSignalingInfo) -> RoomUser {
		let group_id = 0;

		let member_attr = {
			if let Some(vec) = fb.roomMemberBinAttrInternal() {
				if vec.len() == 1 {
					let bin_attr = &vec.get(0);
					if bin_attr.id() == SCE_NP_MATCHING2_ROOMMEMBER_BIN_ATTR_INTERNAL_1_ID {
						RoomMemberBinAttr::from_flatbuffer(bin_attr)
					} else {
						error!("CreateJoinRoom request with unexpected id in room member bin: {}", bin_attr.id());
						// ensure coherency
						RoomMemberBinAttr::new()
					}
				} else {
					error!("CreateJoinRoom request with {} member binary attribute(s)!", vec.len());
					RoomMemberBinAttr::new()
				}
			} else {
				RoomMemberBinAttr::new()
			}
		};

		let team_id = fb.teamId();

		RoomUser {
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
		}
	}
	pub fn from_JoinRoomRequest(fb: &JoinRoomRequest, signaling_info: ClientSharedSignalingInfo) -> RoomUser {
		let group_id = 0;

		if let Some(_vec) = fb.joinRoomGroupLabel() {
			// Find/Create corresponding group and set id
		}

		let member_attr = {
			if let Some(vec) = fb.roomMemberBinAttrInternal() {
				if vec.len() == 1 {
					let bin_attr = &vec.get(0);
					if bin_attr.id() == SCE_NP_MATCHING2_ROOMMEMBER_BIN_ATTR_INTERNAL_1_ID {
						RoomMemberBinAttr::from_flatbuffer(bin_attr)
					} else {
						error!("JoinRoom request with unexpected id in room member bin: {}", bin_attr.id());
						// ensure coherency
						RoomMemberBinAttr::new()
					}
				} else {
					error!("JoinRoom request with {} member binary attribute(s)!", vec.len());
					RoomMemberBinAttr::new()
				}
			} else {
				RoomMemberBinAttr::new()
			}
		};

		let team_id = fb.teamId();

		RoomUser {
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
		}
	}
	pub fn to_RoomMemberDataInternal<'a>(&self, builder: &mut flatbuffers::FlatBufferBuilder<'a>, room: &Room) -> flatbuffers::WIPOffset<RoomMemberDataInternal<'a>> {
		let npid = builder.create_string(&self.npid);
		let online_name = builder.create_string(&self.online_name);
		let avatar_url = builder.create_string(&self.avatar_url);

		let user_info = UserInfo::create(
			builder,
			&UserInfoArgs {
				npId: Some(npid),
				onlineName: Some(online_name),
				avatarUrl: Some(avatar_url),
			},
		);

		let bin_attr = {
			let bin_attrs = vec![self.member_attr.to_flatbuffer(builder)];
			Some(builder.create_vector(&bin_attrs))
		};

		let room_group = if self.group_id != 0 {
			Some(room.group_config[self.group_id as usize - 1].to_flatbuffer(builder))
		} else {
			None
		};

		RoomMemberDataInternal::create(
			builder,
			&RoomMemberDataInternalArgs {
				userInfo: Some(user_info),
				joinDate: self.join_date,
				memberId: self.member_id,
				teamId: self.team_id,
				roomGroup: room_group,
				natType: 2,
				flagAttr: self.flag_attr,
				roomMemberBinAttrInternal: bin_attr,
			},
		)
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
	pub fn from_flatbuffer(fb: &CreateJoinRoomRequest) -> Room {
		let world_id = fb.worldId();
		let lobby_id = fb.lobbyId();
		let max_slot = fb.maxSlot() as u16;
		let flag_attr = fb.flagAttr();
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

		if let Some(vec) = fb.roomBinAttrInternal() {
			for i in 0..vec.len() {
				let room_binattr_internal_from_fb = RoomBinAttrInternal::from_flatbuffer(&vec.get(i), CREATOR_ROOM_MEMBER_ID);

				if room_binattr_internal_from_fb.data.id != SCE_NP_MATCHING2_ROOM_BIN_ATTR_INTERNAL_1_ID && room_binattr_internal_from_fb.data.id != SCE_NP_MATCHING2_ROOM_BIN_ATTR_INTERNAL_2_ID {
					error!("Invalid Room BinAttr Internal ID in CreateRoom: {}", room_binattr_internal_from_fb.data.id);
					continue;
				}

				let id = room_binattr_internal_from_fb.data.id;
				bin_attr_internal[(id - SCE_NP_MATCHING2_ROOM_BIN_ATTR_INTERNAL_1_ID) as usize] = room_binattr_internal_from_fb;
			}
		}
		if let Some(vec) = fb.roomBinAttrExternal() {
			for i in 0..vec.len() {
				let room_binattr_external_from_fb = RoomBinAttr::from_flatbuffer(&vec.get(i));

				if room_binattr_external_from_fb.id != SCE_NP_MATCHING2_ROOM_BIN_ATTR_EXTERNAL_1_ID && room_binattr_external_from_fb.id != SCE_NP_MATCHING2_ROOM_BIN_ATTR_EXTERNAL_2_ID {
					error!("Invalid Room BinAttr External ID in CreateRoom: {}", room_binattr_external_from_fb.id);
					continue;
				}

				let id = room_binattr_external_from_fb.id;
				bin_attr_external[(id - SCE_NP_MATCHING2_ROOM_BIN_ATTR_EXTERNAL_1_ID) as usize] = room_binattr_external_from_fb;
			}
		}
		if let Some(vec) = fb.roomSearchableBinAttrExternal() {
			for i in 0..vec.len() {
				let room_binattr_search_from_fb = RoomBinAttr::from_flatbuffer(&vec.get(i));

				if room_binattr_search_from_fb.id != SCE_NP_MATCHING2_ROOM_SEARCHABLE_BIN_ATTR_EXTERNAL_1_ID {
					error!("Invalid Room BinAttr Search ID in CreateRoom: {}", room_binattr_search_from_fb.id);
					continue;
				}
				search_bin_attr = room_binattr_search_from_fb;
			}
		}
		if let Some(vec) = fb.roomSearchableIntAttrExternal() {
			for i in 0..vec.len() {
				let room_intattr_from_fb = RoomIntAttr::from_flatbuffer(&vec.get(i));
				if room_intattr_from_fb.id < SCE_NP_MATCHING2_ROOM_SEARCHABLE_INT_ATTR_EXTERNAL_1_ID || room_intattr_from_fb.id > SCE_NP_MATCHING2_ROOM_SEARCHABLE_INT_ATTR_EXTERNAL_8_ID {
					error!("Invalid Room IntAttr ID in CreateRoom: {}", room_intattr_from_fb.id);
					continue;
				}

				let id = room_intattr_from_fb.id;
				search_int_attr[(id - SCE_NP_MATCHING2_ROOM_SEARCHABLE_INT_ATTR_EXTERNAL_1_ID) as usize] = room_intattr_from_fb;
			}
		}
		if let Some(password) = fb.roomPassword() {
			if password.len() == 8 {
				let mut room_password_data = [0; 8];
				room_password_data.clone_from_slice(&password.bytes()[0..8]);
				room_password = Some(room_password_data);
			} else {
				error!("Invalid password length in CreateRoom: {}", password.len());
			}
		}
		if let Some(vec) = fb.groupConfig() {
			for i in 0..vec.len() {
				let group_id = (i + 1) as u8;
				group_config.push(RoomGroupConfig::from_flatbuffer(&vec.get(i), group_id));
			}
		}
		let password_slot_mask = fb.passwordSlotMask();
		if let Some(vec) = fb.allowedUser() {
			for i in 0..vec.len() {
				allowed_users.push(vec.get(i).to_string());
			}
		}
		if let Some(vec) = fb.blockedUser() {
			for i in 0..vec.len() {
				blocked_users.push(vec.get(i).to_string());
			}
		}
		if let Some(vec) = fb.sigOptParam() {
			signaling_param = Some(SignalParam::from_flatbuffer(&vec));
		}

		Room {
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
		}
	}
	pub fn to_RoomDataInternal<'a>(&self, builder: &mut flatbuffers::FlatBufferBuilder<'a>) -> flatbuffers::WIPOffset<RoomDataInternal<'a>> {
		let mut final_member_list = None;
		if !self.users.is_empty() {
			let mut member_list = Vec::new();
			for user in &self.users {
				member_list.push(user.1.to_RoomMemberDataInternal(builder, self));
			}
			final_member_list = Some(builder.create_vector(&member_list));
		}
		let mut final_group_list = None;
		if !self.group_config.is_empty() {
			let mut group_list = Vec::new();
			for group in &self.group_config {
				group_list.push(group.to_flatbuffer(builder));
			}
			final_group_list = Some(builder.create_vector(&group_list));
		}
		let final_internalbinattr = {
			let mut bin_list = Vec::new();
			for bin in &self.bin_attr_internal {
				bin_list.push(bin.to_flatbuffer(builder));

				// Should we include bin attrs that haven't been set?
				// if bin.data.cur_size != 0 {
				// 	bin_list.push(bin.to_flatbuffer(builder));
				// }
			}

			if bin_list.is_empty() {
				None
			} else {
				Some(builder.create_vector(&bin_list))
			}
		};

		let mut rbuild = RoomDataInternalBuilder::new(builder);
		rbuild.add_serverId(self.server_id);
		rbuild.add_worldId(self.world_id);
		rbuild.add_lobbyId(self.lobby_id);
		rbuild.add_roomId(self.room_id);
		rbuild.add_passwordSlotMask(self.password_slot_mask);
		rbuild.add_maxSlot(self.max_slot as u32);
		if let Some(final_member_list) = final_member_list {
			rbuild.add_memberList(final_member_list);
		}
		rbuild.add_ownerId(self.owner);
		if let Some(final_group_list) = final_group_list {
			rbuild.add_roomGroup(final_group_list);
		}
		rbuild.add_flagAttr(self.flag_attr);
		if let Some(final_internalbinattr) = final_internalbinattr {
			rbuild.add_roomBinAttrInternal(final_internalbinattr);
		}
		rbuild.finish()
	}
	pub fn to_RoomDataExternal<'a>(&self, builder: &mut flatbuffers::FlatBufferBuilder<'a>, search_option: i32, inc_attrs: &Vec<u16>) -> flatbuffers::WIPOffset<RoomDataExternal<'a>> {
		let mut final_owner_info = None;
		if (search_option & 0x7) != 0 {
			let mut online_name = None;
			let mut avatar_url = None;

			// NPID is mandatory in SceNpUserInfo2 so assume that this bit is set if other info is required
			let npid = Some(builder.create_string(&self.users.get(&self.owner).unwrap().npid));

			if (search_option & 0x2) != 0 {
				let s = builder.create_string(&self.users.get(&self.owner).unwrap().online_name);
				online_name = Some(s);
			}
			if (search_option & 0x4) != 0 {
				let s = builder.create_string(&self.users.get(&self.owner).unwrap().avatar_url);
				avatar_url = Some(s);
			}

			final_owner_info = Some(UserInfo::create(
				builder,
				&UserInfoArgs {
					npId: npid,
					onlineName: online_name,
					avatarUrl: avatar_url,
				},
			));
		}
		let mut final_group_list = None;
		if !self.group_config.is_empty() {
			let mut group_list = Vec::new();
			for group in &self.group_config {
				group_list.push(group.to_flatbuffer(builder));
			}
			final_group_list = Some(builder.create_vector(&group_list));
		}

		let mut vec_searchint = Vec::new();
		let mut vec_searchbin = Vec::new();
		let mut vec_binattrexternal = Vec::new();

		'inc_loop: for inc_attr in inc_attrs {
			match *inc_attr {
				SCE_NP_MATCHING2_ROOM_SEARCHABLE_INT_ATTR_EXTERNAL_1_ID..=SCE_NP_MATCHING2_ROOM_SEARCHABLE_INT_ATTR_EXTERNAL_8_ID => {
					vec_searchint.push(self.search_int_attr[(*inc_attr - SCE_NP_MATCHING2_ROOM_SEARCHABLE_INT_ATTR_EXTERNAL_1_ID) as usize].to_flatbuffer(builder));
				}
				SCE_NP_MATCHING2_ROOM_SEARCHABLE_BIN_ATTR_EXTERNAL_1_ID => {
					vec_searchbin.push(self.search_bin_attr.to_flatbuffer(builder));
				}
				SCE_NP_MATCHING2_ROOM_BIN_ATTR_EXTERNAL_1_ID..=SCE_NP_MATCHING2_ROOM_BIN_ATTR_EXTERNAL_2_ID => {
					vec_binattrexternal.push(self.bin_attr_external[(*inc_attr - SCE_NP_MATCHING2_ROOM_BIN_ATTR_EXTERNAL_1_ID) as usize].to_flatbuffer(builder));
				}
				v => {
					error!("Invalid ID included in to_inc in to_RoomDataExternal: {}", v);
					continue 'inc_loop;
				}
			}
		}

		let final_searchint = if vec_searchint.is_empty() { None } else { Some(builder.create_vector(&vec_searchint)) };
		let final_searchbin = if vec_searchbin.is_empty() { None } else { Some(builder.create_vector(&vec_searchbin)) };
		let final_binattrexternal = if vec_binattrexternal.is_empty() {
			None
		} else {
			Some(builder.create_vector(&vec_binattrexternal))
		};

		let mut max_private_slots = 0;
		let mut open_public_slots = 0;
		let mut open_private_slots = 0;

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

		let mut rbuild = RoomDataExternalBuilder::new(builder);
		rbuild.add_serverId(self.server_id);
		rbuild.add_worldId(self.world_id);
		rbuild.add_publicSlotNum(max_public_slots);
		rbuild.add_privateSlotNum(max_private_slots);
		rbuild.add_lobbyId(self.lobby_id);
		rbuild.add_roomId(self.room_id);
		rbuild.add_openPublicSlotNum(open_public_slots);
		rbuild.add_maxSlot(self.max_slot);
		rbuild.add_openPrivateSlotNum(open_private_slots);
		rbuild.add_curMemberNum(self.users.len() as u16);
		rbuild.add_passwordSlotMask(self.password_slot_mask);
		if let Some(owner_info) = final_owner_info {
			rbuild.add_owner(owner_info);
		}
		if !self.group_config.is_empty() {
			rbuild.add_roomGroup(final_group_list.unwrap());
		}
		rbuild.add_flagAttr(self.flag_attr);
		// External stuff
		if let Some(final_searchint) = final_searchint {
			rbuild.add_roomSearchableIntAttrExternal(final_searchint);
		}
		if let Some(final_searchbin) = final_searchbin {
			rbuild.add_roomSearchableBinAttrExternal(final_searchbin);
		}
		if let Some(final_binattrexternal) = final_binattrexternal {
			rbuild.add_roomBinAttrExternal(final_binattrexternal);
		}

		rbuild.finish()
	}

	pub fn get_room_member_update_info<'a>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'a>,
		member_id: u16,
		event_cause: EventCause,
		user_opt_data: Option<&PresenceOptionData>,
	) -> flatbuffers::WIPOffset<RoomMemberUpdateInfo<'a>> {
		assert!(self.users.contains_key(&member_id));
		let user = self.users.get(&member_id).unwrap();

		// Builds flatbuffer
		let member_internal = user.to_RoomMemberDataInternal(builder, self);

		let opt_data = dc_opt_data(builder, user_opt_data);

		RoomMemberUpdateInfo::create(
			builder,
			&RoomMemberUpdateInfoArgs {
				roomMemberDataInternal: Some(member_internal),
				eventCause: event_cause as u8,
				optData: Some(opt_data),
			},
		)
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

	pub fn is_match(&self, req: &SearchRoomRequest) -> bool {
		// Hidden rooms never turn up in searches
		if (self.flag_attr & (SceNpMatching2FlagAttr::SCE_NP_MATCHING2_ROOM_FLAG_ATTR_HIDDEN as u32)) != 0 {
			return false;
		}

		let mut flag_filter = req.flagFilter();
		let mut flag_attr = req.flagAttr();

		// We ignore the SCE_NP_MATCHING2_ROOM_FLAG_ATTR_NAT_TYPE_RESTRICTION attribute as it is pretty irrelevant to rpcs3 and caused issue with top spin search
		flag_filter &= !(SceNpMatching2FlagAttr::SCE_NP_MATCHING2_ROOM_FLAG_ATTR_NAT_TYPE_RESTRICTION as u32);
		flag_attr &= !(SceNpMatching2FlagAttr::SCE_NP_MATCHING2_ROOM_FLAG_ATTR_NAT_TYPE_RESTRICTION as u32);

		if (self.flag_attr & flag_filter) != flag_attr {
			return false;
		}

		let intfilters = req.intFilter();
		if let Some(intfilters) = intfilters {
			for i in 0..intfilters.len() {
				let intfilter = intfilters.get(i);
				let op = intfilter.searchOperator();
				let id = intfilter.attr().unwrap().id();
				let num = intfilter.attr().unwrap().num();

				if id < SCE_NP_MATCHING2_ROOM_SEARCHABLE_INT_ATTR_EXTERNAL_1_ID || id > SCE_NP_MATCHING2_ROOM_SEARCHABLE_INT_ATTR_EXTERNAL_8_ID {
					error!("Invalid Room IntAttr ID in search parameters: {}", id);
					return false;
				}

				// Find matching id
				let found_intsearch = &self.search_int_attr[(id - SCE_NP_MATCHING2_ROOM_SEARCHABLE_INT_ATTR_EXTERNAL_1_ID) as usize];
				let op = FromPrimitive::from_u8(op);
				if op.is_none() {
					error!("Unsupported op in int search filter: {}", intfilter.searchOperator());
					return false;
				}
				let op = op.unwrap();

				match op {
					SceNpMatching2Operator::OperatorEq => {
						if found_intsearch.attr != num {
							return false;
						}
					}
					SceNpMatching2Operator::OperatorNe => {
						if found_intsearch.attr == num {
							return false;
						}
					}
					SceNpMatching2Operator::OperatorLt => {
						if found_intsearch.attr >= num {
							return false;
						}
					}
					SceNpMatching2Operator::OperatorLe => {
						if found_intsearch.attr > num {
							return false;
						}
					}
					SceNpMatching2Operator::OperatorGt => {
						if found_intsearch.attr <= num {
							return false;
						}
					}
					SceNpMatching2Operator::OperatorGe => {
						if found_intsearch.attr < num {
							return false;
						}
					}
				}
			}
		}

		let binfilters = req.binFilter();
		if let Some(binfilters) = binfilters {
			for i in 0..binfilters.len() {
				let binfilter = binfilters.get(i);
				let op = binfilter.searchOperator();
				let id = binfilter.attr().unwrap().id();
				let data = binfilter.attr().unwrap().data().unwrap();

				if id != SCE_NP_MATCHING2_ROOM_SEARCHABLE_BIN_ATTR_EXTERNAL_1_ID {
					error!("Invalid Search BinAttr ID in search parameters: {}", id);
					return false;
				}

				let op = FromPrimitive::from_u8(op);
				if op.is_none() {
					error!("Unsupported op in bin search filter: {}", binfilter.searchOperator());
					return false;
				}
				let op = op.unwrap();

				// Unsure if cur_size should be compared to data's size
				let len_compare = std::cmp::min(data.len(), self.search_bin_attr.attr.len());
				let equality = self.search_bin_attr.attr[0..len_compare] == data.bytes()[0..len_compare];

				match op {
					SceNpMatching2Operator::OperatorEq => {
						if !equality {
							return false;
						}
					}
					SceNpMatching2Operator::OperatorNe => {
						if equality {
							return false;
						}
					}
					_ => panic!("Non EQ/NE in binfilter!"),
				}
			}
		}
		true
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

	pub fn create_room(&mut self, com_id: &ComId, req: &CreateJoinRoomRequest, cinfo: &ClientInfo, server_id: u16, signaling_info: ClientSharedSignalingInfo) -> Result<Vec<u8>, ErrorType> {
		if req.maxSlot() == 0 || req.maxSlot() > 64 {
			return Err(ErrorType::Malformed);
		}

		let room_cnt = self.room_cnt.entry(*com_id).or_insert(0);

		// Creates the room from input fb
		let mut room = Room::from_flatbuffer(req);

		// Initial room owner always get slot 1
		let member_id = room.occupy_slot(1);

		room.owner = member_id;
		room.room_id = (*room_cnt) + 1;
		room.server_id = server_id;
		// Add the user as its owner
		let mut room_user = RoomUser::from_CreateJoinRoomRequest(req, signaling_info);
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
			if req.joinRoomGroupLabel().is_none() {
				warn!("Tried to create a group room without a join label!");
				return Err(ErrorType::RoomGroupNoJoinLabel);
			}
			let join_label = req.joinRoomGroupLabel().unwrap();
			if join_label.len() != SCE_NP_MATCHING2_GROUP_LABEL_SIZE {
				error!("Tried to create a group room with a join label with size != 8!");
				return Err(ErrorType::Malformed);
			}

			let mut label = [0u8; SCE_NP_MATCHING2_GROUP_LABEL_SIZE];
			label.clone_from_slice(&join_label.bytes()[0..SCE_NP_MATCHING2_GROUP_LABEL_SIZE]);

			// Requires Group to be the first group
			if room.group_config[0].label.as_ref().is_some_and(|v| *v == label) {
			} else if room.group_config[0].label.is_none() {
				room.group_config[0].label = Some(label);
			} else {
				error!("Invalid join label when creating the room!");
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

		if room.lobby_id == 0 {
			let daset = self.world_rooms.entry((*com_id, room.world_id)).or_default();
			daset.insert(*room_cnt);
		} else {
			let daset = self.lobby_rooms.entry((*com_id, room.lobby_id)).or_default();
			daset.insert(*room_cnt);
		}

		self.rooms.insert((*com_id, *room_cnt), room);
		self.user_rooms.entry(cinfo.user_id).or_default().insert((*com_id, *room_cnt));

		// Prepare roomDataInternal
		let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);
		let room_data = self.rooms[&(*com_id, *room_cnt)].to_RoomDataInternal(&mut builder);

		builder.finish(room_data, None);
		Ok(builder.finished_data().to_vec())
	}

	pub fn join_room(
		&mut self,
		com_id: &ComId,
		req: &JoinRoomRequest,
		cinfo: &ClientInfo,
		signaling_info: ClientSharedSignalingInfo,
	) -> Result<(Vec<u8>, [Option<(Vec<u8>, HashSet<i64>)>; 4]), ErrorType> {
		let room_id = req.roomId();
		let room = self.rooms.get_mut(&(*com_id, room_id)).unwrap();

		if (room.flag_attr & SceNpMatching2FlagAttr::SCE_NP_MATCHING2_ROOM_FLAG_ATTR_FULL as u32) != 0 {
			return Err(ErrorType::RoomFull);
		}

		if req.joinRoomGroupLabel().as_ref().is_some_and(|v| v.len() != SCE_NP_MATCHING2_GROUP_LABEL_SIZE) {
			error!("Tried to join a room with a join label with size != SCE_NP_MATCHING2_GROUP_LABEL_SIZE!");
			return Err(ErrorType::Malformed);
		}

		if req.roomPassword().as_ref().is_some_and(|v| v.len() != SCE_NP_MATCHING2_SESSION_PASSWORD_SIZE) {
			error!("Tried to join a room with a password with size != SCE_NP_MATCHING2_SESSION_PASSWORD_SIZE!");
			return Err(ErrorType::Malformed);
		}

		let join_label = req.joinRoomGroupLabel().map(|v| {
			let mut label = [0u8; SCE_NP_MATCHING2_GROUP_LABEL_SIZE];
			label.clone_from_slice(&v.bytes()[0..SCE_NP_MATCHING2_GROUP_LABEL_SIZE]);
			label
		});

		let password = req.roomPassword().map(|v| {
			let mut pass = [0u8; SCE_NP_MATCHING2_SESSION_PASSWORD_SIZE];
			pass.clone_from_slice(&v.bytes()[0..SCE_NP_MATCHING2_SESSION_PASSWORD_SIZE]);
			pass
		});

		let (group_id, member_id) = room.get_a_slot(join_label, password)?;

		let mut room_user = RoomUser::from_JoinRoomRequest(req, signaling_info.clone());
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

		let build_notification = |signaling_info: Option<(&[u8], u16)>| {
			let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);

			let update_info = Some(room.get_room_member_update_info(&mut builder, member_id, EventCause::None, Some(&req.optData().unwrap())));
			let signaling = if let Some((ip, port)) = signaling_info {
				let vec_ip = builder.create_vector(ip);
				Some(SignalingAddr::create(&mut builder, &SignalingAddrArgs { ip: Some(vec_ip), port }))
			} else {
				None
			};

			let notif_data = NotificationUserJoinedRoom::create(&mut builder, &NotificationUserJoinedRoomArgs { room_id, update_info, signaling });

			builder.finish(notif_data, None);
			builder.finished_data().to_vec()
		};

		let add_to_notif_list = |notif_list: &mut Option<(Vec<u8>, HashSet<i64>)>, signaling_info: (&[u8], u16), to_add: i64| match notif_list {
			None => {
				*notif_list = Some((build_notification(Some(signaling_info)), [to_add].into()));
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
							add_to_notif_list(&mut notif_local_signaling, (&signaling_info.local_addr_p2p, 3658), user.user_id);
							signaling_list_reply.push((user.member_id, user.signaling_info.local_addr_p2p.to_vec(), 3658));
						} else {
							match (user.signaling_info.addr_p2p_ipv6, signaling_info.addr_p2p_ipv6) {
								(Some(user_ipv6), Some(joiner_ipv6)) => {
									add_to_notif_list(&mut notif_public_signaling_ipv6, (&joiner_ipv6.0, joiner_ipv6.1), user.user_id);
									signaling_list_reply.push((user.member_id, user_ipv6.0.to_vec(), user_ipv6.1));
								}
								_ => {
									add_to_notif_list(&mut notif_public_signaling_ipv4, (&signaling_info.addr_p2p_ipv4.0, signaling_info.addr_p2p_ipv4.1), user.user_id);
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
						add_to_notif_list(&mut notif_local_signaling, (&signaling_info.local_addr_p2p, 3658), hub_user.user_id);
						signaling_list_reply.push((hub_user.member_id, hub_user.signaling_info.local_addr_p2p.to_vec(), 3658));
					} else {
						match (hub_user.signaling_info.addr_p2p_ipv6, signaling_info.addr_p2p_ipv6) {
							(Some(hub_user_ipv6), Some(joiner_ipv6)) => {
								add_to_notif_list(&mut notif_public_signaling_ipv6, (&joiner_ipv6.0, joiner_ipv6.1), hub_user.user_id);
								signaling_list_reply.push((hub_user.member_id, hub_user_ipv6.0.to_vec(), hub_user_ipv6.1));
							}
							_ => {
								add_to_notif_list(&mut notif_public_signaling_ipv4, (&signaling_info.addr_p2p_ipv4.0, signaling_info.addr_p2p_ipv4.1), hub_user.user_id);
								signaling_list_reply.push((hub_user.member_id, hub_user.signaling_info.addr_p2p_ipv4.0.to_vec(), hub_user.signaling_info.addr_p2p_ipv4.1));
							}
						}
					}

					let mut all_users_except_hub_user = all_previous_users;
					all_users_except_hub_user.remove(&hub_user.user_id);
					notif_no_signaling = Some((build_notification(None), all_users_except_hub_user));
				}
				_ => unreachable!(),
			}
		} else {
			notif_no_signaling = Some((build_notification(None), all_previous_users));
		}

		let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);
		let room_data = Some(room.to_RoomDataInternal(&mut builder));

		let signaling_data = if !signaling_list_reply.is_empty() {
			let mut wip_vec = Vec::new();

			for (member_id, ip, port) in signaling_list_reply {
				let vec_ip = builder.create_vector(&ip);
				let sig_addr = Some(SignalingAddr::create(&mut builder, &SignalingAddrArgs { ip: Some(vec_ip), port }));
				wip_vec.push(Matching2SignalingInfo::create(&mut builder, &Matching2SignalingInfoArgs { member_id, addr: sig_addr }));
			}

			Some(builder.create_vector(&wip_vec))
		} else {
			None
		};

		let reply = JoinRoomResponse::create(&mut builder, &JoinRoomResponseArgs { room_data, signaling_data });

		builder.finish(reply, None);
		Ok((
			builder.finished_data().to_vec(),
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
			if group.num_members == 0 && group.fixed_label == false {
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
	pub fn search_room(&self, com_id: &ComId, req: &SearchRoomRequest) -> Vec<u8> {
		let world_id = req.worldId();
		let lobby_id = req.lobbyId();

		let startindex = if req.rangeFilter_startIndex() == 0 {
			error!("SearchRoomRequest.startIndex was 0!");
			1
		} else {
			req.rangeFilter_startIndex()
		};

		let max = if req.rangeFilter_max() == 0 || req.rangeFilter_max() > 20 {
			error!("SearchRoomRequest.max was invalid: {}", req.rangeFilter_max());
			20
		} else {
			req.rangeFilter_max()
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
				if room.is_match(req) {
					matching_rooms.push(room);
				}
			}
		}
		let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);

		let mut list_roomdataexternal = Default::default();

		if matching_rooms.len() >= startindex as usize {
			let inc_attrs = if let Some(attr_ids) = req.attrId() { attr_ids.iter().collect() } else { Vec::new() };

			let start = startindex as usize - 1;
			let num_to_get = std::cmp::min(matching_rooms.len() - start, max as usize);
			let end = start + num_to_get;

			let mut room_list = Vec::new();
			for room in &matching_rooms[start..end] {
				room_list.push(room.to_RoomDataExternal(&mut builder, req.option(), &inc_attrs));
			}
			list_roomdataexternal = Some(builder.create_vector(&room_list));
		}

		let resp = SearchRoomResponse::create(
			&mut builder,
			&SearchRoomResponseArgs {
				startIndex: startindex,
				total: matching_rooms.len() as u32,
				rooms: list_roomdataexternal,
			},
		);
		builder.finish(resp, None);
		builder.finished_data().to_vec()
	}

	pub fn get_roomdata_external_list(&self, com_id: &ComId, req: &GetRoomDataExternalListRequest) -> Vec<u8> {
		let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);

		let mut list_roomdataexternal = Default::default();

		let inc_attrs = if let Some(attr_ids) = req.attrIds() { attr_ids.iter().collect() } else { Vec::new() };

		if let Some(roomids) = req.roomIds() {
			let mut room_list = Vec::new();
			for room_id in &roomids {
				if self.room_exists(com_id, room_id) {
					room_list.push(self.get_room(com_id, room_id));
				}
			}

			let mut vec_roomdataexternal = Vec::new();
			for room in &room_list {
				vec_roomdataexternal.push(room.to_RoomDataExternal(&mut builder, 7, &inc_attrs));
			}
			list_roomdataexternal = Some(builder.create_vector(&vec_roomdataexternal));
		}

		let resp = GetRoomDataExternalListResponse::create(&mut builder, &GetRoomDataExternalListResponseArgs { rooms: list_roomdataexternal });
		builder.finish(resp, None);
		builder.finished_data().to_vec()
	}

	pub fn set_roomdata_external(&mut self, com_id: &ComId, req: &SetRoomDataExternalRequest, user_id: i64) -> Result<(), ErrorType> {
		if !self.room_exists(com_id, req.roomId()) {
			return Err(ErrorType::RoomMissing);
		}
		let room = self.get_mut_room(com_id, req.roomId());

		let member_id = room.get_member_id(user_id)?;
		let is_room_owner = room.get_owner() == member_id;

		// Only the room owner can change external information of the room
		if !is_room_owner {
			return Err(ErrorType::Unauthorized);
		}

		if let Some(vec) = req.roomBinAttrExternal() {
			for i in 0..vec.len() {
				let room_binattr_external_from_fb = RoomBinAttr::from_flatbuffer(&vec.get(i));

				if room_binattr_external_from_fb.id != SCE_NP_MATCHING2_ROOM_BIN_ATTR_EXTERNAL_1_ID && room_binattr_external_from_fb.id != SCE_NP_MATCHING2_ROOM_BIN_ATTR_EXTERNAL_2_ID {
					error!("Invalid Room BinAttr External ID in SetRoomDataExternal: {}", room_binattr_external_from_fb.id);
					continue;
				}

				let id = room_binattr_external_from_fb.id;
				room.bin_attr_external[(id - SCE_NP_MATCHING2_ROOM_BIN_ATTR_EXTERNAL_1_ID) as usize] = room_binattr_external_from_fb;
			}
		}
		if let Some(vec) = req.roomSearchableBinAttrExternal() {
			for i in 0..vec.len() {
				let room_binattr_search_from_fb = RoomBinAttr::from_flatbuffer(&vec.get(i));

				if room_binattr_search_from_fb.id != SCE_NP_MATCHING2_ROOM_SEARCHABLE_BIN_ATTR_EXTERNAL_1_ID {
					error!("Invalid Room BinAttr Search ID in SetRoomDataExternal: {}", room_binattr_search_from_fb.id);
					continue;
				}
				room.search_bin_attr = room_binattr_search_from_fb;
			}
		}
		if let Some(vec) = req.roomSearchableIntAttrExternal() {
			for i in 0..vec.len() {
				let room_intattr_from_fb = RoomIntAttr::from_flatbuffer(&vec.get(i));
				if room_intattr_from_fb.id < SCE_NP_MATCHING2_ROOM_SEARCHABLE_INT_ATTR_EXTERNAL_1_ID || room_intattr_from_fb.id > SCE_NP_MATCHING2_ROOM_SEARCHABLE_INT_ATTR_EXTERNAL_8_ID {
					error!("Invalid Room IntAttr ID in SetRoomDataExternal: {}", room_intattr_from_fb.id);
					continue;
				}

				let id = room_intattr_from_fb.id;
				room.search_int_attr[(id - SCE_NP_MATCHING2_ROOM_SEARCHABLE_INT_ATTR_EXTERNAL_1_ID) as usize] = room_intattr_from_fb;
			}
		}

		Ok(())
	}
	pub fn get_roomdata_internal(&self, com_id: &ComId, req: &GetRoomDataInternalRequest) -> Result<Vec<u8>, ErrorType> {
		if !self.room_exists(com_id, req.roomId()) {
			return Err(ErrorType::RoomMissing);
		}
		let room = self.get_room(com_id, req.roomId());

		let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);
		let room_data = room.to_RoomDataInternal(&mut builder);

		builder.finish(room_data, None);

		Ok(builder.finished_data().to_vec())
	}
	pub fn set_roomdata_internal(&mut self, com_id: &ComId, req: &SetRoomDataInternalRequest, user_id: i64) -> Result<Option<(HashSet<i64>, Vec<u8>)>, ErrorType> {
		if !self.room_exists(com_id, req.roomId()) {
			return Err(ErrorType::RoomMissing);
		}
		let room = self.get_mut_room(com_id, req.roomId());
		let member_id = room.get_member_id(user_id)?;

		let is_room_owner = room.get_owner() == member_id;
		let mut has_changed = false;

		let old_password_slot_mask = room.password_slot_mask;
		let old_flag_attr = room.flag_attr;
		let mut vec_new_groups: Vec<u8> = Vec::new();

		if is_room_owner {
			let flag_filter = req.flagFilter();
			let flag_attr = req.flagAttr();
			let new_room_flag_attr = (flag_attr & flag_filter) | (room.flag_attr & !flag_filter);

			if new_room_flag_attr != room.flag_attr {
				room.flag_attr = new_room_flag_attr;
				has_changed = true;
			}

			if let Some(room_group_config) = req.passwordConfig() {
				for i in 0..room_group_config.len() {
					let group_config = room_group_config.get(i);

					if group_config.groupId() as usize > room.group_config.len() {
						error!("set_roomdata_internal: group_id > roomgroup_config.len() ( {} : {} )", group_config.groupId(), room.group_config.len());
						continue;
					}

					if room.group_config[(group_config.groupId() - 1) as usize].with_password != group_config.withPassword() {
						room.group_config[(group_config.groupId() - 1) as usize].with_password = group_config.withPassword();
						vec_new_groups.push(group_config.groupId());
						has_changed = true;
					}
				}
			}

			if old_password_slot_mask != req.passwordSlotMask() {
				room.password_slot_mask = req.passwordSlotMask();
				has_changed = true;
			}

			if let Some(vec) = req.ownerPrivilegeRank() {
				let mut succession_list: VecDeque<u16> = VecDeque::new();
				for i in 0..vec.len() {
					succession_list.push_back(vec.get(i));
				}

				if succession_list != room.owner_succession {
					room.owner_succession = succession_list;
					has_changed = true;
				}
			}
		}

		let new_binattr;
		if let Some(vec) = req.roomBinAttrInternal() {
			let mut vec_new_binattr = Vec::new();
			for i in 0..vec.len() {
				let room_binattr_internal_from_fb = RoomBinAttrInternal::from_flatbuffer(&vec.get(i), member_id);

				if room_binattr_internal_from_fb.data.id != SCE_NP_MATCHING2_ROOM_BIN_ATTR_INTERNAL_1_ID && room_binattr_internal_from_fb.data.id != SCE_NP_MATCHING2_ROOM_BIN_ATTR_INTERNAL_2_ID {
					error!("Invalid Room BinAttr Internal ID in SetRoomDataInternal: {}", room_binattr_internal_from_fb.data.id);
					continue;
				}
				let id = room_binattr_internal_from_fb.data.id;
				room.bin_attr_internal[(id - SCE_NP_MATCHING2_ROOM_BIN_ATTR_INTERNAL_1_ID) as usize] = room_binattr_internal_from_fb;
				vec_new_binattr.push(id);
			}
			new_binattr = Some(vec_new_binattr);
			has_changed = true;
		} else {
			new_binattr = None;
		}

		if has_changed {
			// Build the notification buffer
			let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);
			let room_data_internal = room.to_RoomDataInternal(&mut builder);
			let fb_new_binattr = new_binattr.map(|vec_new_binattr| builder.create_vector(&vec_new_binattr));
			let fb_new_groups = if vec_new_groups.is_empty() { None } else { Some(builder.create_vector(&vec_new_groups)) };

			let resp = RoomDataInternalUpdateInfo::create(
				&mut builder,
				&RoomDataInternalUpdateInfoArgs {
					newRoomDataInternal: Some(room_data_internal),
					prevFlagAttr: old_flag_attr,
					prevRoomPasswordSlotMask: old_password_slot_mask,
					newRoomGroup: fb_new_groups,
					newRoomBinAttrInternal: fb_new_binattr,
				},
			);
			builder.finish(resp, None);

			let mut to_notif = room.get_room_user_ids();
			to_notif.remove(&user_id);

			return Ok(Some((to_notif, builder.finished_data().to_vec())));
		}

		Ok(None)
	}

	pub fn get_roommemberdata_internal(&self, com_id: &ComId, req: &GetRoomMemberDataInternalRequest) -> Result<Vec<u8>, ErrorType> {
		if !self.room_exists(com_id, req.roomId()) {
			return Err(ErrorType::RoomMissing);
		}

		let room = self.get_room(com_id, req.roomId());
		let user = room.users.get(&req.memberId()).ok_or(ErrorType::NotFound)?;
		let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);
		let resp = user.to_RoomMemberDataInternal(&mut builder, room);
		builder.finish(resp, None);
		Ok(builder.finished_data().to_vec())
	}

	pub fn set_roommemberdata_internal(&mut self, com_id: &ComId, req: &SetRoomMemberDataInternalRequest, user_id: i64) -> Result<(HashSet<i64>, Vec<u8>), ErrorType> {
		if !self.room_exists(com_id, req.roomId()) {
			return Err(ErrorType::RoomMissing);
		}

		let mut new_binattr = None;
		let target_member_id;
		let prev_team_id;
		{
			// Update RoomMemberData
			let room = self.get_mut_room(com_id, req.roomId());
			let member_id = room.get_member_id(user_id)?;
			target_member_id = if req.memberId() == 0 { member_id } else { req.memberId() };

			// You can only change a member's binattrs if they are your own or you are room owner
			if (member_id != target_member_id) && (member_id != room.owner) {
				return Err(ErrorType::Unauthorized);
			}

			if !room.users.contains_key(&target_member_id) {
				return Err(ErrorType::NotFound);
			}

			let user = room.users.get_mut(&target_member_id).unwrap();

			let team_id = req.teamId();
			prev_team_id = user.team_id;
			if team_id != 0 {
				user.team_id = team_id;
			}

			if let Some(fb_member_binattr) = req.roomMemberBinAttrInternal() {
				let mut vec_new_binattr = Vec::new();
				if fb_member_binattr.len() == 1 {
					let bin_attr = RoomMemberBinAttr::from_flatbuffer(&fb_member_binattr.get(0));
					if bin_attr.data.id == SCE_NP_MATCHING2_ROOMMEMBER_BIN_ATTR_INTERNAL_1_ID {
						user.member_attr = RoomMemberBinAttr::from_flatbuffer(&fb_member_binattr.get(0));
						vec_new_binattr.push(user.member_attr.data.id);
						new_binattr = Some(vec_new_binattr);
					} else {
						error!("SetRoomMemberDataInternal request with unexpected id in room member data: {}", bin_attr.data.id);
					}
				} else {
					error!("SetRoomMemberDataInternal request with {} member binary attribute(s)!", fb_member_binattr.len());
				}
			}
		}

		// Build the notification buffer
		let room = self.get_room(com_id, req.roomId());
		let user = room.users.get(&target_member_id).unwrap();

		let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);
		let member_internal = user.to_RoomMemberDataInternal(&mut builder, room);
		let fb_new_binattr = new_binattr.map(|vec_new_binattr| builder.create_vector(&vec_new_binattr));

		let resp = RoomMemberDataInternalUpdateInfo::create(
			&mut builder,
			&RoomMemberDataInternalUpdateInfoArgs {
				newRoomMemberDataInternal: Some(member_internal),
				prevFlagAttr: user.flag_attr,
				prevTeamId: prev_team_id,
				newRoomMemberBinAttrInternal: fb_new_binattr,
			},
		);
		builder.finish(resp, None);

		let mut to_notif = room.get_room_user_ids();
		to_notif.remove(&user_id);

		Ok((to_notif, builder.finished_data().to_vec()))
	}

	pub fn get_rooms_by_user(&self, user: i64) -> Option<HashSet<(ComId, u64)>> {
		if !self.user_rooms.contains_key(&user) {
			return None;
		}

		Some(self.user_rooms.get(&user).unwrap().clone())
	}
}
