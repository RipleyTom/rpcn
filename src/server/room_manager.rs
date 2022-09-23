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
const SCE_NP_MATCHING2_USER_BIN_ATTR_1_ID: u16 = 0x5F;

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
}
impl<const N: usize> RoomBinAttr<N> {
	pub fn with_id(id: u16) -> RoomBinAttr<N> {
		RoomBinAttr { id, attr: [0; N] }
	}

	pub fn from_flatbuffer(fb: &BinAttr) -> RoomBinAttr<N> {
		let id = fb.id();
		let mut attr: [u8; N] = [0; N];
		if let Some(fb_attrs) = fb.data() {
			let len = if fb_attrs.len() > N {
				error!("Error converting a fb BinAttr to a RoombinAttr, mismatched size: fb:{} vs expected:{}", fb_attrs.len(), N);
				N
			} else {
				fb_attrs.len()
			};
			attr[0..len].clone_from_slice(&fb_attrs[0..len]);
		}

		RoomBinAttr { id, attr }
	}

	pub fn to_flatbuffer<'a>(&self, builder: &mut flatbuffers::FlatBufferBuilder<'a>) -> flatbuffers::WIPOffset<BinAttr<'a>> {
		let final_attr = builder.create_vector(&self.attr);
		BinAttr::create(builder, &BinAttrArgs { id: self.id, data: Some(final_attr) })
	}
}
pub struct RoomMemberBinAttr {
	update_date: u64,
	data: RoomBinAttr<64>,
}
impl RoomMemberBinAttr {
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
			update_member_id: 1,
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
	with_label: bool,
	label: [u8; 8],
	with_password: bool,
	group_id: u8,
	num_members: u32,
}
impl RoomGroupConfig {
	pub fn from_flatbuffer(fb: &GroupConfig, group_id: u8) -> RoomGroupConfig {
		let slot_num = fb.slotNum();
		let with_label = fb.withLabel();
		let mut label = [0; 8];
		if let Some(vec) = fb.label() {
			if vec.len() == 8 {
				label.clone_from_slice(&vec[0..8]);
			}
		}
		let with_password = fb.withPassword();
		RoomGroupConfig {
			slot_num,
			with_label,
			label,
			with_password,
			group_id,
			num_members: 0,
		}
	}
	pub fn to_flatbuffer<'a>(&self, builder: &mut flatbuffers::FlatBufferBuilder<'a>) -> flatbuffers::WIPOffset<RoomGroup<'a>> {
		let label = Some(builder.create_vector(&self.label));

		RoomGroup::create(
			builder,
			&RoomGroupArgs {
				groupId: self.group_id,
				withPassword: self.with_password,
				withLabel: self.with_label,
				label,
				slotNum: self.slot_num,
				curGroupMemberNum: self.num_members,
			},
		)
	}
}

#[derive(Clone)]
pub struct SignalParam {
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
	member_attr: BTreeMap<u16, RoomMemberBinAttr>,
	team_id: u8,

	member_id: u16,
}
impl RoomUser {
	pub fn from_CreateJoinRoomRequest(fb: &CreateJoinRoomRequest) -> RoomUser {
		let group_id = 0;
		let mut member_attr: BTreeMap<u16, RoomMemberBinAttr> = BTreeMap::new();

		if let Some(_vec) = fb.joinRoomGroupLabel() {
			// Add group to room and set id TODO
		}
		if let Some(vec) = fb.roomMemberBinAttrInternal() {
			for i in 0..vec.len() {
				let member_binattr = RoomMemberBinAttr::from_flatbuffer(&vec.get(i));
				member_attr.insert(member_binattr.data.id, RoomMemberBinAttr::from_flatbuffer(&vec.get(i)));
			}
		}
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
		}
	}
	pub fn from_JoinRoomRequest(fb: &JoinRoomRequest) -> RoomUser {
		let group_id = 0;
		let mut member_attr: BTreeMap<u16, RoomMemberBinAttr> = BTreeMap::new();

		if let Some(_vec) = fb.joinRoomGroupLabel() {
			// Find/Create corresponding group and set id
		}
		if let Some(vec) = fb.roomMemberBinAttrInternal() {
			for i in 0..vec.len() {
				let member_binattr = RoomMemberBinAttr::from_flatbuffer(&vec.get(i));
				member_attr.insert(member_binattr.data.id, RoomMemberBinAttr::from_flatbuffer(&vec.get(i)));
			}
		}
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
		}
	}
	pub fn to_RoomMemberDataInternal<'a>(&self, builder: &mut flatbuffers::FlatBufferBuilder<'a>, room: &Room) -> flatbuffers::WIPOffset<RoomMemberDataInternal<'a>> {
		let npid = builder.create_string(&self.npid);
		let online_name = builder.create_string(&self.online_name);
		let avatar_url = builder.create_string(&self.avatar_url);

		let user_info = UserInfo2::create(
			builder,
			&UserInfo2Args {
				npId: Some(npid),
				onlineName: Some(online_name),
				avatarUrl: Some(avatar_url),
			},
		);

		let bin_attr = if !self.member_attr.is_empty() {
			let mut bin_attrs = Vec::new();
			for binattr in self.member_attr.values() {
				bin_attrs.push(binattr.to_flatbuffer(builder));
			}
			Some(builder.create_vector(&bin_attrs))
		} else {
			None
		};

		let room_group = if self.group_id != 0 {
			Some(room.group_config.get(&self.group_id).unwrap().to_flatbuffer(builder))
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
				natType: 2, // todo
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
	group_config: BTreeMap<u8, RoomGroupConfig>,
	password_slot_mask: u64,
	allowed_users: Vec<String>,
	blocked_users: Vec<String>,
	signaling_param: Option<SignalParam>,

	// Data not from stream
	server_id: u16,
	room_id: u64,
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
		let mut group_config: BTreeMap<u8, RoomGroupConfig> = BTreeMap::new();
		let mut allowed_users: Vec<String> = Vec::new();
		let mut blocked_users: Vec<String> = Vec::new();
		let mut signaling_param = None;

		if let Some(vec) = fb.roomBinAttrInternal() {
			for i in 0..vec.len() {
				// Since we're creating the room member id is always 1
				let room_binattr_internal_from_fb = RoomBinAttrInternal::from_flatbuffer(&vec.get(i), 1);

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
				room_password_data.clone_from_slice(&password[0..8]);
				room_password = Some(room_password_data);
			}
		}
		if let Some(vec) = fb.groupConfig() {
			for i in 0..vec.len() {
				let group_id = (i + 1) as u8;
				group_config.insert(group_id, RoomGroupConfig::from_flatbuffer(&vec.get(i), group_id));
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
			for group in self.group_config.values() {
				group_list.push(group.to_flatbuffer(builder));
			}
			final_group_list = Some(builder.create_vector(&group_list));
		}
		let mut final_internalbinattr = None;
		if !self.bin_attr_internal.is_empty() {
			let mut bin_list = Vec::new();
			for bin in &self.bin_attr_internal {
				bin_list.push(bin.to_flatbuffer(builder));
			}
			final_internalbinattr = Some(builder.create_vector(&bin_list));
		}

		let mut rbuild = RoomDataInternalBuilder::new(builder);
		rbuild.add_serverId(self.server_id);
		rbuild.add_worldId(self.world_id);
		rbuild.add_lobbyId(self.lobby_id);
		rbuild.add_roomId(self.room_id);
		rbuild.add_passwordSlotMask(self.password_slot_mask);
		rbuild.add_maxSlot(self.max_slot as u32);
		if !self.users.is_empty() {
			rbuild.add_memberList(final_member_list.unwrap());
		}
		rbuild.add_ownerId(self.owner);
		if !self.group_config.is_empty() {
			rbuild.add_roomGroup(final_group_list.unwrap());
		}
		rbuild.add_flagAttr(self.flag_attr);
		if !self.bin_attr_internal.is_empty() {
			rbuild.add_roomBinAttrInternal(final_internalbinattr.unwrap());
		}
		rbuild.finish()
	}
	pub fn to_RoomDataExternal<'a>(&self, builder: &mut flatbuffers::FlatBufferBuilder<'a>, search_option: i32, inc_attrs: &Vec<u16>) -> flatbuffers::WIPOffset<RoomDataExternal<'a>> {
		let mut final_owner_info = None;
		if (search_option & 0x7) != 0 {
			let mut npid = None;
			let mut online_name = None;
			let mut avatar_url = None;

			if (search_option & 0x1) != 0 {
				let s = builder.create_string(&self.users.get(&self.owner).unwrap().npid);
				npid = Some(s);
			}
			if (search_option & 0x2) != 0 {
				let s = builder.create_string(&self.users.get(&self.owner).unwrap().online_name);
				online_name = Some(s);
			}
			if (search_option & 0x4) != 0 {
				let s = builder.create_string(&self.users.get(&self.owner).unwrap().avatar_url);
				avatar_url = Some(s);
			}

			final_owner_info = Some(UserInfo2::create(
				builder,
				&UserInfo2Args {
					npId: npid,
					onlineName: online_name,
					avatarUrl: avatar_url,
				},
			));
		}
		let mut final_group_list = None;
		if !self.group_config.is_empty() {
			let mut group_list = Vec::new();
			for group in self.group_config.values() {
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

		let mut rbuild = RoomDataExternalBuilder::new(builder);
		rbuild.add_serverId(self.server_id);
		rbuild.add_worldId(self.world_id);
		rbuild.add_publicSlotNum(self.max_slot);
		rbuild.add_privateSlotNum(0); // Mystery: TODO?
		rbuild.add_lobbyId(self.lobby_id);
		rbuild.add_roomId(self.room_id);
		rbuild.add_openPublicSlotNum(self.max_slot - (self.users.len() as u16));
		rbuild.add_maxSlot(self.max_slot);
		rbuild.add_openPrivateSlotNum(0); // Mystery: TODO?
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
		if final_searchint.is_some() {
			rbuild.add_roomSearchableIntAttrExternal(final_searchint.unwrap());
		}
		if final_searchbin.is_some() {
			rbuild.add_roomSearchableBinAttrExternal(final_searchbin.unwrap());
		}
		if final_binattrexternal.is_some() {
			rbuild.add_roomBinAttrExternal(final_binattrexternal.unwrap());
		}

		rbuild.finish()
	}

	pub fn get_signaling_info(&self) -> Option<SignalParam> {
		self.signaling_param.clone()
	}
	pub fn get_room_member_update_info(&self, member_id: u16, event_cause: EventCause, user_opt_data: Option<&PresenceOptionData>) -> Vec<u8> {
		assert!(self.users.contains_key(&member_id));
		let user = self.users.get(&member_id).unwrap();

		// Builds flatbuffer
		let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);

		let member_internal = user.to_RoomMemberDataInternal(&mut builder, self);

		let opt_data = dc_opt_data(&mut builder, user_opt_data);

		let up_info = RoomMemberUpdateInfo::create(
			&mut builder,
			&RoomMemberUpdateInfoArgs {
				roomMemberDataInternal: Some(member_internal),
				eventCause: event_cause as u8,
				optData: Some(opt_data),
			},
		);
		builder.finish(up_info, None);
		builder.finished_data().to_vec()
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
	pub fn get_member_id(&self, user_id: i64) -> Result<u16, u8> {
		for user in &self.users {
			if user.1.user_id == user_id {
				return Ok(*user.0);
			}
		}

		Err(ErrorType::NotFound as u8)
	}
	pub fn get_owner(&self) -> u16 {
		self.owner
	}

	pub fn is_match(&self, req: &SearchRoomRequest) -> bool {
		// Hidden rooms never turn up in searches
		if (self.flag_attr & (SceNpMatching2FlagAttr::SCE_NP_MATCHING2_ROOM_FLAG_ATTR_HIDDEN as u32)) != 0 {
			return false;
		}

		let flag_filter = req.flagFilter();
		let flag_attr = req.flagAttr();
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

				match op {
					SceNpMatching2Operator::OperatorEq => {
						if self.search_bin_attr.attr != data {
							return false;
						}
					}
					SceNpMatching2Operator::OperatorNe => {
						if self.search_bin_attr.attr == data {
							return false;
						}
					}
					_ => panic!("Non EQ/NE in binfilter!"),
				}
			}
		}
		true
	}
	pub fn find_user(&self, user_id: i64) -> u16 {
		for user in &self.users {
			if user.1.user_id == user_id {
				return *user.0;
			}
		}

		0
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

	pub fn get_room_infos(&self, com_id: &ComId, room_id: u64) -> Result<(u16, u32, u64), u8> {
		if !self.room_exists(com_id, room_id) {
			return Err(ErrorType::NotFound as u8);
		}

		let room = self.get_room(com_id, room_id);
		Ok((room.server_id, room.world_id, room.lobby_id))
	}

	pub fn create_room(&mut self, com_id: &ComId, req: &CreateJoinRoomRequest, cinfo: &ClientInfo, server_id: u16) -> Vec<u8> {
		let room_cnt = self.room_cnt.entry(*com_id).or_insert(0);
		*room_cnt += 1;

		// Creates the room from input fb
		let mut room = Room::from_flatbuffer(req);
		let member_id: u16 = 1; // initial creator always gets member id 1
		room.owner = member_id;
		room.room_id = *room_cnt;
		room.server_id = server_id;
		// Add the user as its owner
		let mut room_user = RoomUser::from_CreateJoinRoomRequest(req);
		room_user.user_id = cinfo.user_id;
		room_user.npid = cinfo.npid.clone();
		room_user.online_name = cinfo.online_name.clone();
		room_user.avatar_url = cinfo.avatar_url.clone();
		room_user.member_id = member_id;
		room_user.flag_attr = SCE_NP_MATCHING2_ROOMMEMBER_FLAG_ATTR_OWNER;
		// TODO: Group Label, joindate
		room.users.insert(member_id, room_user);

		if room.lobby_id == 0 {
			let daset = self.world_rooms.entry((*com_id, room.world_id)).or_insert_with(HashSet::new);
			daset.insert(*room_cnt);
		} else {
			let daset = self.lobby_rooms.entry((*com_id, room.lobby_id)).or_insert_with(HashSet::new);
			daset.insert(*room_cnt);
		}

		self.rooms.insert((*com_id, *room_cnt), room);
		let user_set = self.user_rooms.entry(cinfo.user_id).or_insert_with(HashSet::new);
		user_set.insert((*com_id, *room_cnt));

		// Prepare roomDataInternal
		let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);
		let room_data = self.rooms[&(*com_id, *room_cnt)].to_RoomDataInternal(&mut builder);

		builder.finish(room_data, None);
		builder.finished_data().to_vec()
	}

	pub fn join_room(&mut self, com_id: &ComId, req: &JoinRoomRequest, cinfo: &ClientInfo) -> Result<(u16, Vec<u8>), u8> {
		let room = self.rooms.get_mut(&(*com_id, req.roomId())).unwrap();

		// Determine lowest member id available
		// TODO: check if password was submitted and use id associated with password slotmask
		let mut member_id: u16 = 1;
		for i in 1..=u16::MAX {
			if !room.users.contains_key(&i) {
				member_id = i;
				break;
			}
		}

		let mut room_user = RoomUser::from_JoinRoomRequest(req);
		room_user.user_id = cinfo.user_id;
		room_user.npid = cinfo.npid.clone();
		room_user.online_name = cinfo.online_name.clone();
		room_user.avatar_url = cinfo.avatar_url.clone();
		room_user.member_id = member_id;
		// TODO: Group Label
		room.users.insert(member_id, room_user);

		// Set full flag if necessary
		if room.users.len() == room.max_slot as usize {
			room.flag_attr |= SceNpMatching2FlagAttr::SCE_NP_MATCHING2_ROOM_FLAG_ATTR_FULL as u32;
		}

		let user_set = self.user_rooms.entry(cinfo.user_id).or_insert_with(HashSet::new);
		user_set.insert((*com_id, room.room_id));

		let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);
		let room_data = room.to_RoomDataInternal(&mut builder);

		builder.finish(room_data, None);
		Ok((member_id, builder.finished_data().to_vec()))
	}

	pub fn leave_room(&mut self, com_id: &ComId, room_id: u64, user_id: i64) -> Result<(bool, HashSet<i64>), u8> {
		if !self.room_exists(com_id, room_id) {
			warn!("Attempted to leave a non existing room");
			return Err(ErrorType::NotFound as u8);
		}

		if let Some(user_set) = self.user_rooms.get_mut(&user_id) {
			if user_set.get(&(*com_id, room_id)).is_none() {
				warn!("Couldn't find the room in the user user_rooms set");
				return Err(ErrorType::NotFound as u8);
			}
			user_set.remove(&(*com_id, room_id));
		} else {
			warn!("Couldn't find the user in the user_rooms list");
			return Err(ErrorType::NotFound as u8);
		}

		let room = self.get_mut_room(com_id, room_id);
		let member_id = room.find_user(user_id);
		assert!(member_id != 0); // This should never happen as it would mean user_rooms is incoherent

		room.users.remove(&member_id);

		// Remove full flag if necessary
		if room.users.len() != room.max_slot as usize {
			room.flag_attr &= !(SceNpMatching2FlagAttr::SCE_NP_MATCHING2_ROOM_FLAG_ATTR_FULL as u32);
		}

		// Generate list of users left
		let mut user_list = HashSet::new();
		for user in &room.users {
			user_list.insert(user.1.user_id);
		}

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
				found_successor = true;
			}

			if !found_successor {
				// Remove the room from appropriate list
				let lobby_id = room.lobby_id;
				let world_id = room.world_id;
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

		Ok((false, user_list))
	}
	pub fn search_room(&self, com_id: &ComId, req: &SearchRoomRequest) -> Vec<u8> {
		let world_id = req.worldId();
		let lobby_id = req.lobbyId();

		// Unclear what the given startIndex means
		let startindex = 0; // req.rangeFilter_startIndex();
		let max = req.rangeFilter_max();

		let mut list = None;
		if world_id != 0 {
			list = self.world_rooms.get(&(*com_id, world_id));
		} else if lobby_id != 0 {
			list = self.lobby_rooms.get(&(*com_id, lobby_id));
		}

		let mut matching_rooms = Vec::new();

		let mut num_found = 0;

		if let Some(room_list) = list {
			for room_id in room_list.iter() {
				let room = self.get_room(com_id, *room_id);
				if room.is_match(req) {
					matching_rooms.push(room);
					num_found += 1;
				}
				if num_found >= max {
					break;
				}
			}
		}
		let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);

		let mut list_roomdataexternal = Default::default();
		if !matching_rooms.is_empty() {
			let inc_attrs = if let Some(attr_ids) = req.attrId() { attr_ids.iter().collect() } else { Vec::new() };

			let mut room_list = Vec::new();
			for room in &matching_rooms {
				room_list.push(room.to_RoomDataExternal(&mut builder, req.option(), &inc_attrs));
			}
			list_roomdataexternal = Some(builder.create_vector(&room_list));
		}

		let resp = SearchRoomResponse::create(
			&mut builder,
			&SearchRoomResponseArgs {
				startIndex: startindex,
				total: matching_rooms.len() as u32,
				size_: matching_rooms.len() as u32,
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

	pub fn set_roomdata_external(&mut self, com_id: &ComId, req: &SetRoomDataExternalRequest) -> Result<(), u8> {
		if !self.room_exists(com_id, req.roomId()) {
			return Err(ErrorType::NotFound as u8);
		}
		let room = self.get_mut_room(com_id, req.roomId());

		if let Some(vec) = req.roomBinAttrExternal() {
			for i in 0..vec.len() {
				let room_binattr_external_from_fb = RoomBinAttr::from_flatbuffer(&vec.get(i));

				if room_binattr_external_from_fb.id != SCE_NP_MATCHING2_ROOM_BIN_ATTR_EXTERNAL_1_ID && room_binattr_external_from_fb.id != SCE_NP_MATCHING2_ROOM_BIN_ATTR_EXTERNAL_2_ID {
					error!("Invalid Room BinAttr External ID in CreateRoom: {}", room_binattr_external_from_fb.id);
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
					error!("Invalid Room BinAttr Search ID in CreateRoom: {}", room_binattr_search_from_fb.id);
					continue;
				}
				room.search_bin_attr = room_binattr_search_from_fb;
			}
		}
		if let Some(vec) = req.roomSearchableIntAttrExternal() {
			for i in 0..vec.len() {
				let room_intattr_from_fb = RoomIntAttr::from_flatbuffer(&vec.get(i));
				if room_intattr_from_fb.id < SCE_NP_MATCHING2_ROOM_SEARCHABLE_INT_ATTR_EXTERNAL_1_ID || room_intattr_from_fb.id > SCE_NP_MATCHING2_ROOM_SEARCHABLE_INT_ATTR_EXTERNAL_8_ID {
					error!("Invalid Room IntAttr ID in CreateRoom: {}", room_intattr_from_fb.id);
					continue;
				}

				let id = room_intattr_from_fb.id;
				room.search_int_attr[(id - SCE_NP_MATCHING2_ROOM_SEARCHABLE_INT_ATTR_EXTERNAL_1_ID) as usize] = room_intattr_from_fb;
			}
		}

		Ok(())
	}
	pub fn get_roomdata_internal(&self, com_id: &ComId, req: &GetRoomDataInternalRequest) -> Result<Vec<u8>, u8> {
		if !self.room_exists(com_id, req.roomId()) {
			return Err(ErrorType::NotFound as u8);
		}
		let room = self.get_room(com_id, req.roomId());

		// TODO: only retrieve specified values

		let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);
		let room_data = room.to_RoomDataInternal(&mut builder);

		builder.finish(room_data, None);

		Ok(builder.finished_data().to_vec())
	}
	pub fn set_roomdata_internal(&mut self, com_id: &ComId, req: &SetRoomDataInternalRequest, user_id: i64) -> Result<(HashSet<i64>, Vec<u8>), u8> {
		if !self.room_exists(com_id, req.roomId()) {
			return Err(ErrorType::NotFound as u8);
		}
		let room = self.get_mut_room(com_id, req.roomId());
		let member_id = room.get_member_id(user_id)?;

		let old_flag_attr = room.flag_attr;
		let flag_filter = req.flagFilter();
		let flag_attr = req.flagAttr();
		room.flag_attr = (flag_attr & flag_filter) | (room.flag_attr & !flag_filter);

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
		} else {
			new_binattr = None;
		}

		// Group stuff TODO

		let old_password_slot_mask = room.password_slot_mask;
		room.password_slot_mask = req.passwordSlotMask();

		if let Some(vec) = req.ownerPrivilegeRank() {
			let mut succession_list: VecDeque<u16> = VecDeque::new();
			for i in 0..vec.len() {
				succession_list.push_back(vec.get(i));
			}
			room.owner_succession = succession_list;
		}

		// Build the notification buffer
		let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);
		let room_data_internal = room.to_RoomDataInternal(&mut builder);
		let fb_new_binattr = new_binattr.map(|vec_new_binattr| builder.create_vector(&vec_new_binattr));

		let resp = RoomDataInternalUpdateInfo::create(
			&mut builder,
			&RoomDataInternalUpdateInfoArgs {
				newRoomDataInternal: Some(room_data_internal),
				prevFlagAttr: old_flag_attr,
				prevRoomPasswordSlotMask: old_password_slot_mask,
				newRoomGroup: None, // TODO
				newRoomBinAttrInternal: fb_new_binattr,
			},
		);
		builder.finish(resp, None);

		let mut to_notif = room.get_room_user_ids();
		to_notif.remove(&user_id);

		Ok((to_notif, builder.finished_data().to_vec()))
	}

	pub fn set_roommemberdata_internal(&mut self, com_id: &ComId, req: &SetRoomMemberDataInternalRequest, user_id: i64) -> Result<(HashSet<i64>, Vec<u8>), u8> {
		if !self.room_exists(com_id, req.roomId()) {
			return Err(ErrorType::NotFound as u8);
		}

		let new_binattr;
		let target_member_id;
		let prev_team_id;
		{
			// Update RoomMemberData
			let room = self.get_mut_room(com_id, req.roomId());
			let member_id = room.get_member_id(user_id)?;
			target_member_id = if req.memberId() == 0 { member_id } else { req.memberId() };

			// You can only change a member's binattrs if they are your own or you are room owner
			if (member_id != target_member_id) && (member_id != room.owner) {
				return Err(ErrorType::Unauthorized as u8);
			}

			if !room.users.contains_key(&target_member_id) {
				return Err(ErrorType::NotFound as u8);
			}

			let user = room.users.get_mut(&target_member_id).unwrap();

			let team_id = req.teamId();
			prev_team_id = user.team_id;
			if team_id != 0 {
				user.team_id = team_id;
			}

			if let Some(fb_member_binattr) = req.roomMemberBinAttrInternal() {
				let mut vec_new_binattr = Vec::new();
				for i in 0..fb_member_binattr.len() {
					let member_binattr = RoomMemberBinAttr::from_flatbuffer(&fb_member_binattr.get(i));
					let member_binattr_id = member_binattr.data.id;
					user.member_attr.insert(member_binattr_id, member_binattr);
					vec_new_binattr.push(member_binattr_id);
				}
				new_binattr = Some(vec_new_binattr);
			} else {
				new_binattr = None;
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
