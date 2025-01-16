#![allow(non_snake_case)]
#![allow(dead_code)]
#![allow(non_camel_case_types)]

use std::collections::{HashMap, HashSet};

use rand::Rng;
use tracing::{error, warn};

use crate::server::client::notifications::NotificationType;
use crate::server::client::{com_id_to_string, ClientInfo, ComId, ErrorType};
use crate::server::stream_extractor::np2_structs_generated::*;

// We store room id as communication_id + room number as hexadecimal (12 + 16)
pub const GUI_ROOM_ID_SIZE: usize = 28;
pub type GuiRoomId = [u8; GUI_ROOM_ID_SIZE];
pub type GuiRoomNumAttr = u32;

const _SCE_NP_MATCHING_ATTR_TYPE_BASIC_BIN: u32 = 1;
const SCE_NP_MATCHING_ATTR_TYPE_BASIC_NUM: u32 = 2;
const SCE_NP_MATCHING_ATTR_TYPE_GAME_BIN: u32 = 3;
const SCE_NP_MATCHING_ATTR_TYPE_GAME_NUM: u32 = 4;

const SCE_NP_MATCHING_ROOM_ATTR_ID_TOTAL_SLOT: u32 = 1;
const SCE_NP_MATCHING_ROOM_ATTR_ID_PRIVATE_SLOT: u32 = 2;
const SCE_NP_MATCHING_ROOM_ATTR_ID_CUR_TOTAL_NUM: u32 = 3;
const SCE_NP_MATCHING_ROOM_ATTR_ID_CUR_PUBLIC_NUM: u32 = 4;
const SCE_NP_MATCHING_ROOM_ATTR_ID_CUR_PRIVATE_NUM: u32 = 5;
const SCE_NP_MATCHING_ROOM_ATTR_ID_PRIVILEGE_TYPE: u32 = 6;
const SCE_NP_MATCHING_ROOM_ATTR_ID_ROOM_SEARCH_FLAG: u32 = 7;

const SCE_NP_MATCHING_CONDITION_SEARCH_EQ: u32 = 0;
const SCE_NP_MATCHING_CONDITION_SEARCH_NE: u32 = 1;
const SCE_NP_MATCHING_CONDITION_SEARCH_LT: u32 = 2;
const SCE_NP_MATCHING_CONDITION_SEARCH_LE: u32 = 3;
const SCE_NP_MATCHING_CONDITION_SEARCH_GT: u32 = 4;
const SCE_NP_MATCHING_CONDITION_SEARCH_GE: u32 = 5;

#[derive(Clone, Copy)]
pub struct GuiRoomBinAttr<const N: usize> {
	data: [u8; N],
	cur_size: usize,
}

impl<const N: usize> GuiRoomBinAttr<N> {
	fn default() -> GuiRoomBinAttr<N> {
		GuiRoomBinAttr { data: [0; N], cur_size: 0 }
	}

	fn from_flatbuffer(fb: &MatchingAttr) -> GuiRoomBinAttr<N> {
		let mut cur_size = 0;
		let mut data = [0; N];

		if let Some(bin_data) = fb.data() {
			if bin_data.len() > N {
				error!("BinAttr size > capacity: {} vs {}", bin_data.len(), N);
			}

			cur_size = std::cmp::min(bin_data.len(), N);
			for i in 0..cur_size {
				data[i] = bin_data.get(i);
			}
		}

		GuiRoomBinAttr { data, cur_size }
	}

	fn gen_MatchingAttr<'a>(&self, builder: &mut flatbuffers::FlatBufferBuilder<'a>, attr_id: u32) -> flatbuffers::WIPOffset<MatchingAttr<'a>> {
		let data = Some(builder.create_vector(&self.data[0..self.cur_size]));

		MatchingAttr::create(
			builder,
			&MatchingAttrArgs {
				attr_type: SCE_NP_MATCHING_ATTR_TYPE_GAME_BIN,
				attr_id,
				num: 0,
				data,
			},
		)
	}
}

pub struct GuiRoomMember {
	npid: String,
	online_name: String,
	avatar_url: String,
	owner: bool,
}

impl GuiRoomMember {
	fn from_clientinfo(cinfo: &ClientInfo, owner: bool) -> GuiRoomMember {
		GuiRoomMember {
			npid: cinfo.npid.clone(),
			online_name: cinfo.online_name.clone(),
			avatar_url: cinfo.avatar_url.clone(),
			owner,
		}
	}

	fn to_flatbuffer<'a>(&self, builder: &mut flatbuffers::FlatBufferBuilder<'a>) -> flatbuffers::WIPOffset<GUIUserInfo<'a>> {
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

		GUIUserInfo::create(
			builder,
			&GUIUserInfoArgs {
				info: Some(user_info),
				owner: self.owner,
			},
		)
	}
}

pub struct GuiRoom {
	total_slots: u32,
	private_slots: u32,
	privilege_grant: bool,
	stealth: bool,
	members: HashMap<i64, GuiRoomMember>,
	num_attrs: [GuiRoomNumAttr; 16],
	big_bin_attrs: [GuiRoomBinAttr<256>; 2],
	small_bin_attrs: [GuiRoomBinAttr<64>; 14],
	communication_id: ComId,
	quickmatch_room: bool,
}

impl GuiRoom {
	fn from_flatbuffer(fb: &CreateRoomGUIRequest, com_id: &ComId) -> GuiRoom {
		let total_slots = std::cmp::min(fb.total_slots(), 64);
		let private_slots = std::cmp::min(std::cmp::min(fb.private_slots(), 64), total_slots);

		let mut room = GuiRoom {
			total_slots,
			private_slots,
			privilege_grant: fb.privilege_grant(),
			stealth: fb.stealth(),
			members: HashMap::new(),
			num_attrs: [0; 16],
			big_bin_attrs: [GuiRoomBinAttr::<256>::default(); 2],
			small_bin_attrs: [GuiRoomBinAttr::<64>::default(); 14],
			communication_id: *com_id,
			quickmatch_room: false,
		};

		if let Some(game_attrs) = fb.game_attrs() {
			for attr in game_attrs {
				room.set_attr(&attr);
			}
		}

		room
	}

	fn from_quickmatch_flatbuffer(fb: &QuickMatchGUIRequest, com_id: &ComId) -> GuiRoom {
		let total_slots = std::cmp::min(fb.available_num(), 64);

		let mut room = GuiRoom {
			total_slots,
			private_slots: 0,
			privilege_grant: true,
			stealth: false,
			members: HashMap::new(),
			num_attrs: [0; 16],
			big_bin_attrs: [GuiRoomBinAttr::<256>::default(); 2],
			small_bin_attrs: [GuiRoomBinAttr::<64>::default(); 14],
			communication_id: *com_id,
			quickmatch_room: true,
		};

		if let Some(conds) = fb.conds() {
			for cond in conds {
				match cond.attr_type() {
					SCE_NP_MATCHING_ATTR_TYPE_GAME_NUM => match cond.attr_id() {
						1..=16 => {
							if cond.comp_op() == SCE_NP_MATCHING_CONDITION_SEARCH_EQ {
								room.num_attrs[(cond.attr_id() - 1) as usize] = cond.comp_value()
							} else {
								warn!("Encountered a != condition in QM, id: {}, op: {}, val: {}", cond.attr_id(), cond.comp_op(), cond.comp_value());
							}
						}
						id => error!("Unexpected QM game num id: {}", id),
					},
					v => error!("Unexpected QM Cond type: {}", v),
				}
			}
		}

		room
	}

	fn set_attr(&mut self, attr: &MatchingAttr) {
		match attr.attr_type() {
			SCE_NP_MATCHING_ATTR_TYPE_GAME_BIN => match attr.attr_id() {
				1 | 2 => self.big_bin_attrs[(attr.attr_id() - 1) as usize] = GuiRoomBinAttr::<256>::from_flatbuffer(attr),
				3..=16 => self.small_bin_attrs[(attr.attr_id() - 3) as usize] = GuiRoomBinAttr::<64>::from_flatbuffer(attr),
				id => error!("Unexpected game bin id: {}", id),
			},
			SCE_NP_MATCHING_ATTR_TYPE_GAME_NUM => match attr.attr_id() {
				1..=16 => self.num_attrs[(attr.attr_id() - 1) as usize] = attr.num(),
				id => error!("Unexpected game num id: {}", id),
			},
			v => error!("Unexpected MatchingAttr type: {}", v),
		}
	}

	fn add_member(&mut self, cinfo: &ClientInfo, owner: bool) {
		self.members.insert(cinfo.user_id, GuiRoomMember::from_clientinfo(cinfo, owner));
	}

	fn gen_MatchingRoomStatus<'a>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'a>,
		id: &GuiRoomId,
		member_filter: Option<i64>,
		extra_member: Option<&GuiRoomMember>,
	) -> flatbuffers::WIPOffset<MatchingRoomStatus<'a>> {
		let vec_id = builder.create_vector(id);

		let mut tmp_vec_members = Vec::new();

		if let Some(user_id) = member_filter {
			if user_id != 0 {
				if let Some(member) = self.members.get(&user_id) {
					tmp_vec_members.push(member.to_flatbuffer(builder));
				}
			}
		} else {
			for member in self.members.values() {
				tmp_vec_members.push(member.to_flatbuffer(builder));
			}
		}

		if let Some(extra_member) = extra_member {
			tmp_vec_members.push(extra_member.to_flatbuffer(builder));
		}

		let vec_members = builder.create_vector(&tmp_vec_members);

		let mut rbuild = MatchingRoomStatusBuilder::new(builder);
		rbuild.add_id(vec_id);
		rbuild.add_members(vec_members);
		rbuild.finish()
	}

	fn gen_MatchingAttr<'a>(&self, builder: &mut flatbuffers::FlatBufferBuilder<'a>, attr_type: u32, attr_id: u32) -> Option<flatbuffers::WIPOffset<MatchingAttr<'a>>> {
		match attr_type {
			SCE_NP_MATCHING_ATTR_TYPE_BASIC_NUM => {
				let num = match attr_id {
					SCE_NP_MATCHING_ROOM_ATTR_ID_TOTAL_SLOT => self.total_slots,
					SCE_NP_MATCHING_ROOM_ATTR_ID_PRIVATE_SLOT => self.private_slots,
					SCE_NP_MATCHING_ROOM_ATTR_ID_CUR_TOTAL_NUM => self.members.len() as u32,
					SCE_NP_MATCHING_ROOM_ATTR_ID_CUR_PUBLIC_NUM => self.members.len() as u32,
					SCE_NP_MATCHING_ROOM_ATTR_ID_CUR_PRIVATE_NUM => 0, // TODO?
					SCE_NP_MATCHING_ROOM_ATTR_ID_PRIVILEGE_TYPE => {
						if self.privilege_grant {
							1
						} else {
							0
						}
					}
					SCE_NP_MATCHING_ROOM_ATTR_ID_ROOM_SEARCH_FLAG => {
						if self.stealth {
							1
						} else {
							0
						}
					}
					_ => return None,
				};

				Some(MatchingAttr::create(builder, &MatchingAttrArgs { attr_type, attr_id, num, data: None }))
			}
			SCE_NP_MATCHING_ATTR_TYPE_GAME_BIN => match attr_id {
				1 | 2 => Some(self.big_bin_attrs[(attr_id - 1) as usize].gen_MatchingAttr(builder, attr_id)),
				3..=16 => Some(self.small_bin_attrs[(attr_id - 3) as usize].gen_MatchingAttr(builder, attr_id)),
				_ => None,
			},
			SCE_NP_MATCHING_ATTR_TYPE_GAME_NUM => {
				if attr_id == 0 || attr_id > 16 {
					return None;
				}

				let num = self.num_attrs[(attr_id - 1) as usize];
				Some(MatchingAttr::create(builder, &MatchingAttrArgs { attr_type, attr_id, num, data: None }))
			}
			_ => None,
		}
	}

	fn gen_MatchingSearchJoinRoomInfo<'a>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'a>,
		id: &GuiRoomId,
		member_filter: Option<i64>,
		extra_member: Option<&GuiRoomMember>,
		attrs: Option<flatbuffers::Vector<flatbuffers::ForwardsUOffset<MatchingAttr>>>,
	) -> flatbuffers::WIPOffset<MatchingSearchJoinRoomInfo<'a>> {
		let room_status = self.gen_MatchingRoomStatus(builder, id, member_filter, extra_member);

		let inc_attrs = if let Some(attr_ids) = attrs { attr_ids.iter().collect() } else { Vec::new() };
		let parsed_attrs: Vec<(u32, u32)> = inc_attrs.iter().map(|attr| (attr.attr_type(), attr.attr_id())).collect();

		let mut tmp_vec_attrs = Vec::new();
		for attr in parsed_attrs {
			if let Some(res_attr) = self.gen_MatchingAttr(builder, attr.0, attr.1) {
				tmp_vec_attrs.push(res_attr);
			}
		}

		let vec_attrs = builder.create_vector(&tmp_vec_attrs);

		let mut rbuild = MatchingSearchJoinRoomInfoBuilder::new(builder);
		rbuild.add_room(room_status);
		rbuild.add_attr(vec_attrs);
		rbuild.finish()
	}

	fn gen_MatchingRoom<'a>(&self, builder: &mut flatbuffers::FlatBufferBuilder<'a>, id: &GuiRoomId, attrs: &Vec<(u32, u32)>) -> flatbuffers::WIPOffset<MatchingRoom<'a>> {
		let vec_id = builder.create_vector(id);

		let mut tmp_vec_attrs = Vec::new();
		for attr in attrs {
			if let Some(res_attr) = self.gen_MatchingAttr(builder, attr.0, attr.1) {
				tmp_vec_attrs.push(res_attr);
			}
		}

		let vec_attrs = builder.create_vector(&tmp_vec_attrs);

		let mut rbuild = MatchingRoomBuilder::new(builder);
		rbuild.add_id(vec_id);
		rbuild.add_attr(vec_attrs);
		rbuild.finish()
	}

	fn match_value(value: u32, cmp_value: u32, cmp_op: u32) -> bool {
		match cmp_op {
			SCE_NP_MATCHING_CONDITION_SEARCH_EQ => value == cmp_value,
			SCE_NP_MATCHING_CONDITION_SEARCH_NE => value != cmp_value,
			SCE_NP_MATCHING_CONDITION_SEARCH_LT => value < cmp_value,
			SCE_NP_MATCHING_CONDITION_SEARCH_LE => value <= cmp_value,
			SCE_NP_MATCHING_CONDITION_SEARCH_GT => value > cmp_value,
			SCE_NP_MATCHING_CONDITION_SEARCH_GE => value >= cmp_value,
			_ => false,
		}
	}

	fn is_match(&self, conds: &Option<flatbuffers::Vector<'_, flatbuffers::ForwardsUOffset<MatchingSearchCondition<'_>>>>, check_full: bool) -> bool {
		if self.quickmatch_room || self.stealth {
			return false;
		}

		if check_full && self.members.len() == self.total_slots as usize {
			return false;
		}

		if let Some(conds) = conds {
			'cond_loop: for cond in conds {
				match cond.attr_type() {
					SCE_NP_MATCHING_ATTR_TYPE_BASIC_NUM => {
						if cond.attr_id() != 1 {
							error!("Invalid id for SCE_NP_MATCHING_ATTR_TYPE_BASIC_NUM for GetRoomListGUI request: {}", cond.attr_id());
							continue 'cond_loop;
						}

						if !GuiRoom::match_value(self.total_slots, cond.comp_value(), cond.comp_op()) {
							return false;
						}
					}
					SCE_NP_MATCHING_ATTR_TYPE_GAME_NUM => {
						if cond.attr_id() == 0 || cond.attr_id() > 8 {
							error!("Invalid id for SCE_NP_MATCHING_ATTR_TYPE_GAME_NUM for GetRoomListGUI request: {}", cond.attr_id());
							continue 'cond_loop;
						}

						if !GuiRoom::match_value(self.num_attrs[(cond.attr_id() - 1) as usize], cond.comp_value(), cond.comp_op()) {
							return false;
						}
					}
					v => {
						error!("Invalid type in the cond for GetRoomListGUI request: {}", v);
						continue 'cond_loop;
					}
				}
			}
		}

		true
	}

	fn is_quickmatch(&self, req: &QuickMatchGUIRequest) -> bool {
		if !self.quickmatch_room || self.stealth || self.total_slots != req.available_num() {
			return false;
		}

		if let Some(conds) = req.conds() {
			'cond_loop: for cond in &conds {
				if cond.attr_type() != SCE_NP_MATCHING_ATTR_TYPE_GAME_NUM
					|| cond.attr_id() == 0
					|| cond.attr_id() > 8
					|| (cond.comp_op() != SCE_NP_MATCHING_CONDITION_SEARCH_EQ && cond.comp_op() != SCE_NP_MATCHING_CONDITION_SEARCH_NE)
				{
					error!("Invalid cond in quickmatch: {}:{}", cond.attr_type(), cond.attr_id());
					continue 'cond_loop;
				}

				if !GuiRoom::match_value(self.num_attrs[(cond.attr_id() - 1) as usize], cond.comp_value(), cond.comp_op()) {
					return false;
				}
			}
		}

		true
	}
}

pub struct GuiRoomManager {
	cur_room_id: u64,
	rooms: HashMap<GuiRoomId, GuiRoom>,
	comid_rooms: HashMap<ComId, HashSet<GuiRoomId>>,
	user_rooms: HashMap<i64, HashSet<GuiRoomId>>,
}

impl<'a> GuiRoomManager {
	pub fn new() -> GuiRoomManager {
		GuiRoomManager {
			cur_room_id: 1,
			rooms: HashMap::new(),
			comid_rooms: HashMap::new(),
			user_rooms: HashMap::new(),
		}
	}

	pub fn fb_vec_to_room_id(fb_vec: Option<flatbuffers::Vector<u8>>) -> Result<GuiRoomId, ErrorType> {
		if let Some(fb_vec) = fb_vec {
			if fb_vec.len() == GUI_ROOM_ID_SIZE {
				return Ok(fb_vec.iter().collect::<Vec<u8>>().try_into().unwrap());
			}
		}

		Err(ErrorType::Malformed)
	}

	fn generate_room_id(cur_room_id: &mut u64, com_id: &ComId) -> GuiRoomId {
		let room_id: GuiRoomId = format!("{}{:016X}", com_id_to_string(com_id), cur_room_id).bytes().collect::<Vec<u8>>().try_into().unwrap();
		*cur_room_id += 1;
		room_id
	}

	fn insert_room(rooms: &'a mut HashMap<GuiRoomId, GuiRoom>, comid_rooms: &mut HashMap<ComId, HashSet<GuiRoomId>>, com_id: &ComId, room_id: &GuiRoomId, room: GuiRoom) -> &'a mut GuiRoom {
		rooms.insert(*room_id, room);
		comid_rooms.entry(*com_id).or_default().insert(*room_id);
		rooms.get_mut(room_id).unwrap()
	}

	fn add_member_to_room(user_rooms: &mut HashMap<i64, HashSet<GuiRoomId>>, room_id: &GuiRoomId, cinfo: &ClientInfo, owner: bool, room: &mut GuiRoom) {
		room.add_member(cinfo, owner);
		assert!(user_rooms.entry(cinfo.user_id).or_default().insert(*room_id));
	}

	pub fn quickmatch_gui(&mut self, com_id: &ComId, req: &QuickMatchGUIRequest, cinfo: &ClientInfo) -> (Vec<u8>, Option<(HashSet<i64>, Vec<u8>)>) {
		let list = self.comid_rooms.get(com_id);

		let found_room = list.and_then(|list| {
			list.iter().find(|room_id| {
				let room = self.rooms.get(*room_id).unwrap();
				room.is_quickmatch(req)
			})
		});

		let (room_id, room) = {
			if let Some(found_room) = found_room {
				let room = self.rooms.get_mut(found_room).unwrap();
				GuiRoomManager::add_member_to_room(&mut self.user_rooms, found_room, cinfo, false, room);
				(*found_room, room)
			} else {
				let room_id = GuiRoomManager::generate_room_id(&mut self.cur_room_id, com_id);
				let room = GuiRoomManager::insert_room(&mut self.rooms, &mut self.comid_rooms, com_id, &room_id, GuiRoom::from_quickmatch_flatbuffer(req, com_id));
				GuiRoomManager::add_member_to_room(&mut self.user_rooms, &room_id, cinfo, true, room);
				(room_id, room)
			}
		};

		let notification_infos = if room.members.len() == room.total_slots as usize {
			// room is complete, notify members that it is
			room.stealth = true;
			// room.privilege_grant = false;
			let member_ids: HashSet<i64> = room.members.keys().filter_map(|user_id| if *user_id != cinfo.user_id { Some(*user_id) } else { None }).collect();
			let mut notif_builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);
			let status = room.gen_MatchingRoomStatus(&mut notif_builder, &room_id, None, None);
			notif_builder.finish(status, None);
			Some((member_ids, notif_builder.finished_data().to_vec()))
		} else {
			None
		};

		let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);
		let vec_id = builder.create_vector(&room_id);
		let mut rbuild = MatchingGuiRoomIdBuilder::new(&mut builder);
		rbuild.add_id(vec_id);
		let fb_room_id = rbuild.finish();
		builder.finish(fb_room_id, None);
		(builder.finished_data().to_vec(), notification_infos)
	}

	pub fn create_room_gui(&mut self, com_id: &ComId, req: &CreateRoomGUIRequest, cinfo: &ClientInfo) -> Vec<u8> {
		let room_id = GuiRoomManager::generate_room_id(&mut self.cur_room_id, com_id);

		let room = GuiRoomManager::insert_room(&mut self.rooms, &mut self.comid_rooms, com_id, &room_id, GuiRoom::from_flatbuffer(req, com_id));
		GuiRoomManager::add_member_to_room(&mut self.user_rooms, &room_id, cinfo, true, room);

		let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);
		let room_data = room.gen_MatchingRoomStatus(&mut builder, &room_id, None, None);
		builder.finish(room_data, None);
		builder.finished_data().to_vec()
	}

	pub fn join_room_gui(&mut self, room_id: &GuiRoomId, cinfo: &ClientInfo) -> Result<(Vec<u8>, HashSet<i64>, Vec<u8>), ErrorType> {
		let room = self.rooms.get_mut(room_id).ok_or(ErrorType::RoomMissing)?;

		if room.members.len() == room.total_slots as usize {
			return Err(ErrorType::RoomFull);
		}

		if room.members.contains_key(&cinfo.user_id) {
			return Err(ErrorType::RoomAlreadyJoined);
		}

		let member_ids: HashSet<i64> = room.members.keys().copied().collect();
		GuiRoomManager::add_member_to_room(&mut self.user_rooms, room_id, cinfo, false, room);

		let reply = {
			let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);
			let room_data = room.gen_MatchingRoomStatus(&mut builder, room_id, None, None);
			builder.finish(room_data, None);
			builder.finished_data().to_vec()
		};

		let notif = {
			let mut notif_builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);
			let notif_status = room.gen_MatchingRoomStatus(&mut notif_builder, room_id, Some(cinfo.user_id), None);
			notif_builder.finish(notif_status, None);
			notif_builder.finished_data().to_vec()
		};

		Ok((reply, member_ids, notif))
	}

	pub fn leave_room_gui(&mut self, room_id: &GuiRoomId, cinfo: &ClientInfo) -> Result<(Vec<u8>, HashSet<i64>, Vec<(NotificationType, Vec<u8>)>), ErrorType> {
		let room = self.rooms.get_mut(room_id).ok_or(ErrorType::RoomMissing)?;

		let member_left = room.members.remove(&cinfo.user_id);
		if member_left.is_none() {
			return Err(ErrorType::NotFound);
		}
		let mut member_left = member_left.unwrap();

		self.user_rooms.get_mut(&cinfo.user_id).unwrap().remove(room_id);

		let set_user_ids = room.members.keys().copied().collect();
		let mut notifications = Vec::new();

		let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);
		let reply_status = room.gen_MatchingRoomStatus(&mut builder, room_id, Some(0), None);
		builder.finish(reply_status, None);
		let reply = builder.finished_data().to_vec();

		if room.members.is_empty() {
			assert!(self.comid_rooms.get_mut(&room.communication_id).unwrap().remove(room_id));
			assert!(self.rooms.remove(room_id).is_some());
			return Ok((reply, HashSet::new(), Vec::new()));
		}

		if member_left.owner {
			member_left.owner = false;

			if !room.privilege_grant {
				for user_id in &set_user_ids {
					self.user_rooms.get_mut(user_id).unwrap().remove(room_id);
				}

				// Room is destroyed(should a notification that user has left be sent before?)
				let mut notif_builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);
				let notif_status = room.gen_MatchingRoomStatus(&mut notif_builder, room_id, Some(0), None);
				notif_builder.finish(notif_status, None);
				let notif = notif_builder.finished_data().to_vec();

				notifications.push((NotificationType::RoomDisappearedGUI, notif));

				assert!(self.comid_rooms.get_mut(&room.communication_id).unwrap().remove(room_id));
				assert!(self.rooms.remove(room_id).is_some());
				return Ok((reply, set_user_ids, notifications));
			}

			// Grant owner to a random member
			// Prepare an extra notification that owner has changed
			let random_user = rand::thread_rng().gen_range(0..room.members.len());
			let new_owner_user_id = *room.members.iter_mut().nth(random_user).unwrap().0;
			room.members.get_mut(&new_owner_user_id).unwrap().owner = true;

			let mut notif_builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);
			let notif_status = room.gen_MatchingRoomStatus(&mut notif_builder, room_id, Some(new_owner_user_id), Some(&member_left));
			notif_builder.finish(notif_status, None);
			let notif = notif_builder.finished_data().to_vec();

			notifications.push((NotificationType::RoomOwnerChangedGUI, notif));
		}

		// Prepare the notification that user has left
		let mut notif_builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);
		let notif_status = room.gen_MatchingRoomStatus(&mut notif_builder, room_id, Some(0), Some(&member_left));
		notif_builder.finish(notif_status, None);
		let notif = notif_builder.finished_data().to_vec();
		notifications.push((NotificationType::MemberLeftRoomGUI, notif));

		Ok((reply, set_user_ids, notifications))
	}

	pub fn get_room_list_gui(&self, com_id: &ComId, req: &GetRoomListGUIRequest) -> Vec<u8> {
		let list = self.comid_rooms.get(com_id);

		let range_start = if req.range_start() == 0 {
			error!("GetRoomListGUIRequest.range_start was 0!");
			1
		} else {
			req.range_start()
		};

		let range_max = if req.range_max() == 0 || req.range_max() > 20 {
			error!("GetRoomListGUIRequest.range_max was invalid: {}", req.range_max());
			20
		} else {
			req.range_max()
		};

		let mut matching_rooms = Vec::new();

		if let Some(rooms) = list {
			for room_id in rooms {
				let room = self.rooms.get(room_id).unwrap();
				if room.is_match(&req.conds(), false) {
					matching_rooms.push((room_id, room));
				}
			}
		}

		let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);
		let mut list_rooms = Default::default();

		if matching_rooms.len() >= range_start as usize {
			let inc_attrs = if let Some(attr_ids) = req.attrs() { attr_ids.iter().collect() } else { Vec::new() };

			let start = range_start as usize - 1;
			let num_to_get = std::cmp::min(matching_rooms.len() - start, range_max as usize);
			let end = start + num_to_get;

			let parsed_attrs: Vec<(u32, u32)> = inc_attrs.iter().map(|attr| (attr.attr_type(), attr.attr_id())).collect();

			let mut room_list = Vec::new();
			for (room_id, room) in &matching_rooms[start..end] {
				room_list.push(room.gen_MatchingRoom(&mut builder, room_id, &parsed_attrs));
			}
			list_rooms = Some(builder.create_vector(&room_list));
		}

		let resp = MatchingRoomList::create(
			&mut builder,
			&MatchingRoomListArgs {
				start: range_start,
				total: matching_rooms.len() as u32,
				rooms: list_rooms,
			},
		);
		builder.finish(resp, None);
		builder.finished_data().to_vec()
	}

	pub fn search_join_gui(&mut self, com_id: &ComId, req: &SearchJoinRoomGUIRequest, cinfo: &ClientInfo) -> Result<(Vec<u8>, HashSet<i64>, Vec<u8>), ErrorType> {
		let list = self.comid_rooms.get(com_id);

		if let Some(rooms) = list {
			for room_id in rooms {
				let room = self.rooms.get_mut(room_id).unwrap();
				if room.is_match(&req.conds(), true) {
					let member_ids: HashSet<i64> = room.members.keys().copied().collect();
					GuiRoomManager::add_member_to_room(&mut self.user_rooms, room_id, cinfo, false, room);

					let reply = {
						let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);
						let room_data = room.gen_MatchingSearchJoinRoomInfo(&mut builder, room_id, None, None, req.attrs());
						builder.finish(room_data, None);
						builder.finished_data().to_vec()
					};

					let notif = {
						let mut notif_builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);
						let notif_status = room.gen_MatchingRoomStatus(&mut notif_builder, room_id, Some(cinfo.user_id), None);
						notif_builder.finish(notif_status, None);
						notif_builder.finished_data().to_vec()
					};

					return Ok((reply, member_ids, notif));
				}
			}
		}

		Err(ErrorType::NotFound)
	}

	pub fn get_rooms_by_user(&self, user: i64) -> Option<HashSet<GuiRoomId>> {
		if !self.user_rooms.contains_key(&user) {
			return None;
		}

		Some(self.user_rooms.get(&user).unwrap().clone())
	}

	pub fn get_room_and_check_ownership(&mut self, room_id: &GuiRoomId, req_user_id: i64) -> Result<&mut GuiRoom, ErrorType> {
		let room = self.rooms.get_mut(room_id).ok_or(ErrorType::RoomMissing)?;

		if !room.members.get(&req_user_id).ok_or(ErrorType::NotFound)?.owner {
			return Err(ErrorType::Unauthorized);
		}

		Ok(room)
	}

	pub fn set_search_flag(&mut self, room_id: &GuiRoomId, stealth: bool, req_user_id: i64) -> Result<(), ErrorType> {
		let room = self.get_room_and_check_ownership(room_id, req_user_id)?;

		if !room.quickmatch_room {
			// We are assuming this is impossible for quickmatching
			// SF2 HD Remix attempts to do it, not sure if an error should be returned
			room.stealth = stealth;
		}

		Ok(())
	}

	pub fn get_search_flag(&self, room_id: &GuiRoomId) -> Result<Vec<u8>, ErrorType> {
		let room = self.rooms.get(room_id).ok_or(ErrorType::NotFound)?;

		let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);
		let fb_matchingroom = room.gen_MatchingRoom(&mut builder, room_id, &vec![(SCE_NP_MATCHING_ATTR_TYPE_BASIC_NUM, SCE_NP_MATCHING_ROOM_ATTR_ID_ROOM_SEARCH_FLAG)]);
		builder.finish(fb_matchingroom, None);
		Ok(builder.finished_data().to_vec())
	}

	pub fn set_room_info_gui(&mut self, room_id: &GuiRoomId, attrs: flatbuffers::Vector<flatbuffers::ForwardsUOffset<MatchingAttr>>, req_user_id: i64) -> Result<(), ErrorType> {
		let room = self.get_room_and_check_ownership(room_id, req_user_id)?;

		for attr in attrs {
			room.set_attr(&attr);
		}

		Ok(())
	}

	pub fn get_room_info_gui(&self, room_id: &GuiRoomId, attrs: flatbuffers::Vector<flatbuffers::ForwardsUOffset<MatchingAttr>>) -> Result<Vec<u8>, ErrorType> {
		let room = self.rooms.get(room_id).ok_or(ErrorType::RoomMissing)?;

		let attrs_vec = &attrs.iter().map(|attr| (attr.attr_type(), attr.attr_id())).collect();

		let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);
		let fb_matchingroom = room.gen_MatchingRoom(&mut builder, room_id, attrs_vec);
		builder.finish(fb_matchingroom, None);
		Ok(builder.finished_data().to_vec())
	}
}
