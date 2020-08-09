#![allow(non_snake_case)]
#![allow(dead_code)]
#![allow(non_camel_case_types)]

use std::collections::{HashMap, HashSet, VecDeque};
use std::convert::TryInto;
use std::sync::Arc;

use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use parking_lot::Mutex;
use rand::Rng;

use crate::server::client::{ClientInfo, ErrorType, EventCause};
use crate::server::log::LogManager;
use crate::server::stream_extractor::fb_helpers::*;
use crate::server::stream_extractor::np2_structs_generated::*;

#[repr(u8)]
#[derive(FromPrimitive)]
enum SceNpMatching2Operator {
    OperatorEq = 1,
    OperatorNe = 2,
    OperatorLt = 3,
    OperatorLe = 4,
    OperatorGt = 5,
    OperatorGe = 6,
}

#[repr(u32)]
enum SceNpMatching2FlagAttr {
    SCE_NP_MATCHING2_ROOM_FLAG_ATTR_OWNER_AUTO_GRANT = 0x80000000,
    SCE_NP_MATCHING2_ROOM_FLAG_ATTR_CLOSED = 0x40000000,
    SCE_NP_MATCHING2_ROOM_FLAG_ATTR_FULL = 0x20000000,
    SCE_NP_MATCHING2_ROOM_FLAG_ATTR_HIDDEN = 0x10000000,
    SCE_NP_MATCHING2_ROOM_FLAG_ATTR_NAT_TYPE_RESTRICTION = 0x04000000,
    SCE_NP_MATCHING2_ROOM_FLAG_ATTR_PROHIBITIVE_MODE = 0x02000000,
}

#[repr(u8)]
#[derive(FromPrimitive, Clone, Debug)]
pub enum SignalingType {
    SignalingNone = 0,
    SignalingMesh = 1,
    SignalingStar = 2,
}

pub struct RoomBinAttr {
    id: u16,
    attr: Vec<u8>,
}
impl RoomBinAttr {
    pub fn from_flatbuffer(fb: &BinAttr) -> RoomBinAttr {
        let id = fb.id();
        let attr = fb.data().unwrap_or_default().to_vec();

        RoomBinAttr { id, attr }
    }

    pub fn to_flatbuffer<'a>(&self, builder: &mut flatbuffers::FlatBufferBuilder<'a>) -> flatbuffers::WIPOffset<BinAttr<'a>> {
        let final_attr = builder.create_vector(&self.attr);
        BinAttr::create(builder, &BinAttrArgs { id: self.id, data: Some(final_attr) })
    }
}
pub struct RoomMemberBinAttr {
    update_date: u64,
    data: RoomBinAttr,
}
impl RoomMemberBinAttr {
    pub fn from_flatbuffer(fb: &BinAttr) -> RoomMemberBinAttr {
        let data = RoomBinAttr::from_flatbuffer(fb);
        RoomMemberBinAttr { update_date: 0, data }
    }

    pub fn to_flatbuffer<'a>(&self, builder: &mut flatbuffers::FlatBufferBuilder<'a>) -> flatbuffers::WIPOffset<MemberBinAttrInternal<'a>> {
        let data = Some(self.data.to_flatbuffer(builder));

        MemberBinAttrInternal::create(builder, &MemberBinAttrInternalArgs { updateDate: self.update_date, data })
    }
}
pub struct RoomBinAttrInternal {
    update_date: u64,
    update_member_id: u16,
    data: RoomBinAttr,
}
impl RoomBinAttrInternal {
    pub fn from_flatbuffer(fb: &BinAttr) -> RoomBinAttrInternal {
        let data = RoomBinAttr::from_flatbuffer(fb);
        RoomBinAttrInternal {
            update_date: 0,
            update_member_id: 1,
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
    label: Option<[u8; 8]>,
    with_password: bool,

    group_id: u8,
    num_members: u32,
}
impl RoomGroupConfig {
    pub fn from_flatbuffer(fb: &GroupConfig, group_id: u8) -> RoomGroupConfig {
        let slot_num = fb.slotNum();
        let label = fb.label().map(|x| x.try_into()).transpose().unwrap_or_default();
        let with_password = fb.withPassword();
        RoomGroupConfig {
            slot_num,
            label,
            with_password,
            group_id,
            num_members: 0,
        }
    }
    pub fn to_flatbuffer<'a>(&self, builder: &mut flatbuffers::FlatBufferBuilder<'a>) -> flatbuffers::WIPOffset<RoomGroup<'a>> {
        let label = Some(builder.create_vector(&self.label.unwrap_or_default()));

        RoomGroup::create(
            builder,
            &RoomGroupArgs {
                groupId: self.group_id,
                withPassword: self.with_password,
                withLabel: self.label.is_some(),
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
            SignalingType::SignalingNone => return false,
            _ => return (self.flag & 1) != 1,
        }
    }
    pub fn get_type(&self) -> SignalingType {
        return self.sig_type.clone();
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
    member_attr: Vec<RoomMemberBinAttr>,
    team_id: u8,

    member_id: u16,
}
impl RoomUser {
    pub fn from_CreateJoinRoomRequest(fb: &CreateJoinRoomRequest) -> RoomUser {
        let group_id = 0;

        if let Some(_vec) = fb.joinRoomGroupLabel() {
            // Add group to room and set id TODO
        }
        let member_attr = if let Some(vec) = fb.roomMemberBinAttrInternal() {
            vec.iter().map(|x| RoomMemberBinAttr::from_flatbuffer(&x)).collect()
        } else { 
            Vec::new() 
        };
        let team_id = fb.teamId();

        RoomUser {
            user_id: 0,
            npid: String::new(),
            online_name: String::new(),
            avatar_url: String::new(),
            join_date: 0, // TODO
            flag_attr: 0,

            group_id,
            member_attr,
            team_id,
            member_id: 0,
        }
    }
    pub fn from_JoinRoomRequest(fb: &JoinRoomRequest) -> RoomUser {
        let group_id = 0;

        if let Some(_vec) = fb.joinRoomGroupLabel() {
            // Find/Create corresponding group and set id
        }
        let member_attr = if let Some(vec) = fb.roomMemberBinAttrInternal() {
           vec.iter()
               .map(|x| RoomMemberBinAttr::from_flatbuffer(&x))
               .collect()
        } else {
            Vec::new()
        };
        let team_id = fb.teamId();

        RoomUser {
            user_id: 0,
            npid: String::new(),
            online_name: String::new(),
            avatar_url: String::new(),
            join_date: 0, // TODO
            flag_attr: 0,

            group_id,
            member_attr,
            team_id,
            member_id: 0,
        }
    }
    pub fn to_RoomMemberDataInternal<'a>(&self, builder: &mut flatbuffers::FlatBufferBuilder<'a>) -> flatbuffers::WIPOffset<RoomMemberDataInternal<'a>> {
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

        let bin_attr = if self.member_attr.len() != 0 {
            let bin_attrs: Vec<_> = self.member_attr.iter().map(|x| x.to_flatbuffer(builder)).collect();
            Some(builder.create_vector(&bin_attrs))
        } else { None };

        RoomMemberDataInternal::create(
            builder,
            &RoomMemberDataInternalArgs {
                userInfo: Some(user_info),
                joinDate: self.join_date,
                memberId: self.member_id,
                teamId: self.team_id,
                roomGroup: self.group_id,
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
    bin_attr_internal: Vec<RoomBinAttrInternal>,
    bin_attr_external: Vec<RoomBinAttr>,
    search_bin_attr: Vec<RoomBinAttr>,
    search_int_attr: Vec<RoomIntAttr>,
    room_password: Option<[u8; 8]>,
    group_config: Vec<RoomGroupConfig>,
    password_slot_mask: u64,
    allowed_users: Vec<String>,
    blocked_users: Vec<String>,
    signaling_param: Option<SignalParam>,

    // Data not from stream
    server_id: u16,
    room_id: u64,
    users: HashMap<u16, RoomUser>,
    user_cnt: u16,
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
        let password_slot_mask;

        let bin_attr_internal = if let Some(vec) = fb.roomBinAttrInternal() {
            vec.iter().map(|x| RoomBinAttrInternal::from_flatbuffer(&x)).collect()
        } else {
            Vec::new()
        };
        let bin_attr_external = if let Some(vec) = fb.roomBinAttrExternal() {
            vec.iter().map(|x| RoomBinAttr::from_flatbuffer(&x)).collect()
        } else {
            Vec::new()
        };
        let search_bin_attr = if let Some(vec) = fb.roomSearchableBinAttrExternal() {
            vec.iter().map(|x| RoomBinAttr::from_flatbuffer(&x)).collect()
        } else {
            Vec::new()
        };
        let search_int_attr = if let Some(vec) = fb.roomSearchableIntAttrExternal() {
            vec.iter().map(|x| RoomIntAttr::from_flatbuffer(&x)).collect()
        } else {
            Vec::new()
        };
        let room_password = fb.roomPassword().map(|x| x.try_into()).transpose().unwrap_or_default();
        let group_config = if let Some(vec) = fb.groupConfig() {
            vec.iter().enumerate().map(|(i, x)| RoomGroupConfig::from_flatbuffer(&x, i as u8+1)).collect()
        } else {
            Vec::new()
        };
        password_slot_mask = fb.passwordSlotMask();
        let allowed_users = if let Some(vec) = fb.allowedUser() {
            vec.iter().map(ToString::to_string).collect()
        } else { 
            Vec::new()
        };
        let blocked_users = if let Some(vec) = fb.blockedUser() { 
            vec.iter().map(ToString::to_string).collect()
        } else {
            Vec::new()
        };
        let signaling_param = fb.sigOptParam().map(|x| SignalParam::from_flatbuffer(&x));

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
            users: HashMap::new(),
            user_cnt: 0,
            owner: 0,
            owner_succession: VecDeque::new(),
        }
    }
    pub fn to_RoomDataInternal<'a>(&self, builder: &mut flatbuffers::FlatBufferBuilder<'a>) -> flatbuffers::WIPOffset<RoomDataInternal<'a>> {
        let member_list: Vec<_> = self.users.iter().map(|user| user.1.to_RoomMemberDataInternal(builder)).collect();
        let final_member_list = builder.create_vector(&member_list);
        let group_list: Vec<_> = self.group_config.iter().map(|group| group.to_flatbuffer(builder)).collect();
        let final_group_list = builder.create_vector(&group_list);
        let bin_list: Vec<_> = self.bin_attr_internal.iter().map(|bin| bin.to_flatbuffer(builder)).collect();
        let final_internalbinattr = builder.create_vector(&bin_list);

        let mut rbuild = RoomDataInternalBuilder::new(builder);
        rbuild.add_serverId(self.server_id);
        rbuild.add_worldId(self.world_id);
        rbuild.add_lobbyId(self.lobby_id);
        rbuild.add_roomId(self.room_id);
        rbuild.add_passwordSlotMask(self.password_slot_mask);
        rbuild.add_maxSlot(self.max_slot as u32);
        if self.users.len() != 0 {
            rbuild.add_memberList(final_member_list);
        }
        rbuild.add_ownerId(self.owner);
        if self.group_config.len() != 0 {
            rbuild.add_roomGroup(final_group_list);
        }
        rbuild.add_flagAttr(self.flag_attr);
        if self.bin_attr_internal.len() != 0 {
            rbuild.add_roomBinAttrInternal(final_internalbinattr);
        }
        rbuild.finish()
    }
    pub fn to_RoomDataExternal<'a>(&self, builder: &mut flatbuffers::FlatBufferBuilder<'a>, search_option: i32) -> flatbuffers::WIPOffset<RoomDataExternal<'a>> {
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
        let items: Vec<_> = self.group_config.iter().map(|x| x.to_flatbuffer(builder)).collect();
        let final_group_list = builder.create_vector(&items);
        let items: Vec<_> = self.search_int_attr.iter().map(|x| x.to_flatbuffer(builder)).collect();
        let final_searchint = builder.create_vector(&items);
        let items: Vec<_> = self.search_bin_attr.iter().map(|x| x.to_flatbuffer(builder)).collect();
        let final_searchbin = builder.create_vector(&items);
        let items: Vec<_> = self.bin_attr_external.iter().map(|x| x.to_flatbuffer(builder)).collect();
        let final_binattrexternal = builder.create_vector(&items);

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
        if self.group_config.len() != 0 {
            rbuild.add_roomGroup(final_group_list);
        }
        rbuild.add_flagAttr(self.flag_attr);
        // External stuff
        if self.search_int_attr.len() != 0 {
            rbuild.add_roomSearchableIntAttrExternal(final_searchint);
        }
        if self.search_bin_attr.len() != 0 {
            rbuild.add_roomSearchableBinAttrExternal(final_searchbin);
        }
        if self.bin_attr_external.len() != 0 {
            rbuild.add_roomBinAttrExternal(final_binattrexternal);
        }

        rbuild.finish()
    }

    pub fn get_signaling_info(&self) -> Option<SignalParam> {
        self.signaling_param.clone()
    }
    pub fn get_room_member_update_info(&self, member_id: u16, event_cause: EventCause, user_opt_data: Option<&PresenceOptionData>) -> Option<Vec<u8>> {
        let user = self.users.get(&member_id)?;

        // Builds flatbuffer
        let mut builder = flatbuffers::FlatBufferBuilder::new_with_capacity(1024);

        let member_internal = user.to_RoomMemberDataInternal(&mut builder);

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
        Some(builder.finished_data().to_vec())
    }
    pub fn get_room_users(&self) -> HashMap<u16, i64> {
        self.users.iter().map(|(i, u)| (*i, u.user_id)).collect()
    }
    pub fn get_room_user_ids(&self) -> HashSet<i64> {
        self.users.iter().map(|u| u.1.user_id).collect()
    }
    pub fn get_member_id(&self, user_id: i64) -> Option<u16> {
        self.users.iter().find(|u| u.1.user_id == user_id).map(|u| *u.0)
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

                // Find matching id
                let mut found_intsearch = None;
                for searchint in &self.search_int_attr {
                    if searchint.id == id {
                        found_intsearch = Some(searchint);
                        break;
                    }
                }
                if let None = found_intsearch {
                    return false;
                }
                let found_intsearch = found_intsearch.unwrap();
                let op = FromPrimitive::from_u8(op);
                if let None = op {
                    panic!("Unsupported op in int search filter!");
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

                // Find matching id
                let mut found_binsearch = None;
                for searchbin in &self.search_bin_attr {
                    if searchbin.id == id {
                        found_binsearch = Some(searchbin);
                        break;
                    }
                }
                if let None = found_binsearch {
                    return false;
                }
                let found_binsearch = found_binsearch.unwrap();
                let op = FromPrimitive::from_u8(op);
                if let None = op {
                    panic!("Unsupported op in int search filter!");
                }
                let op = op.unwrap();

                match op {
                    SceNpMatching2Operator::OperatorEq => {
                        if found_binsearch.attr.len() != data.len() {
                            return false;
                        }
                        for index in 0..found_binsearch.attr.len() {
                            if found_binsearch.attr[index] != data[index] {
                                return false;
                            }
                        }
                    }
                    _ => panic!("Non EQ in binfilter!"),
                }
            }
        }
        true
    }
    pub fn find_user(&self, user_id: i64) -> Option<u16> {
        self.users.iter().find(|x| x.1.user_id == user_id).map(|x| *x.0)
    }
}

pub struct RoomManager {
    rooms: HashMap<u64, Room>, // roomid/roomdata
    room_cnt: u64,
    world_rooms: HashMap<u32, HashSet<u64>>, // worldid/roomids
    lobby_rooms: HashMap<u64, HashSet<u64>>, // lobbyid/roomids
    user_rooms: HashMap<i64, HashSet<u64>>,  // List of user / list of rooms
    log_manager: Arc<Mutex<LogManager>>,
}

use thiserror::*;
//All not founds get turned into `ErrorType::NotFound`
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum RoomError {
    #[error("Attempted to leave a non existing room")]
    RoomNotFound,
    #[error("Couldn't find the room in the user user_rooms set")]
    UserRoomNotFound,
    #[error("Couldn't find the user in the user_rooms list")]
    UserSetNotFound,
    #[error("Couldn't find the user in the room's users list")]
    UserNotFound,
}

impl From<RoomError> for ErrorType {
    fn from(err: RoomError) -> ErrorType {
        match err {
            _ => ErrorType::NotFound
        }
    }
}

impl From<RoomError> for u8 {
    fn from(err: RoomError) -> u8 {
        ErrorType::from(err) as u8
    }
}

impl RoomManager {
    pub fn new(log_manager: Arc<Mutex<LogManager>>) -> RoomManager {
        RoomManager {
            rooms: HashMap::new(),
            room_cnt: 0,
            world_rooms: HashMap::new(),
            lobby_rooms: HashMap::new(),
            user_rooms: HashMap::new(),
            log_manager,
        }
    }

    fn log(&self, s: &str) {
        self.log_manager.lock().write(&format!("RoomManager: {}", s));
    }

    pub fn room_exists(&self, room_id: u64) -> bool {
        self.rooms.contains_key(&room_id)
    }
    pub fn get_room(&self, room_id: u64) -> Option<&Room> {
        self.rooms.get(&room_id)
    }
    pub fn get_mut_room(&mut self, room_id: u64) -> Option<&mut Room> {
        self.rooms.get_mut(&room_id)
    }

    pub fn get_corresponding_world(&self, room_id: u64) -> Option<u32> {
        self.get_room(room_id).map(|r| r.world_id)
    }

    pub fn create_room(&mut self, req: &CreateJoinRoomRequest, cinfo: &ClientInfo, server_id: u16) -> Vec<u8> {
        self.room_cnt += 1;

        // Creates the room from input fb
        let mut room = Room::from_flatbuffer(req);
        room.user_cnt += 1;
        room.owner = room.user_cnt;
        room.room_id = self.room_cnt;
        room.server_id = server_id;
        // Add the user as its owner
        let mut room_user = RoomUser::from_CreateJoinRoomRequest(req);
        room_user.user_id = cinfo.user_id;
        room_user.npid = cinfo.npid.clone();
        room_user.online_name = cinfo.online_name.clone();
        room_user.avatar_url = cinfo.avatar_url.clone();
        room_user.member_id = room.user_cnt;
        // TODO: Group Label, joindate
        room.users.insert(room.user_cnt, room_user);

        if room.lobby_id == 0 {
            let daset = self.world_rooms.entry(room.world_id).or_insert(HashSet::new());
            daset.insert(self.room_cnt);
        } else {
            let daset = self.lobby_rooms.entry(room.lobby_id).or_insert(HashSet::new());
            daset.insert(self.room_cnt);
        }

        self.rooms.insert(self.room_cnt, room);
        let user_set = self.user_rooms.entry(cinfo.user_id).or_insert(HashSet::new());
        user_set.insert(self.room_cnt);

        // Prepare roomDataInternal
        let mut builder = flatbuffers::FlatBufferBuilder::new_with_capacity(1024);
        let room_data = self.rooms[&self.room_cnt].to_RoomDataInternal(&mut builder);

        builder.finish(room_data, None);
        builder.finished_data().to_vec()
    }

    ///First value in tuple is the user count
    pub fn join_room(&mut self, req: &JoinRoomRequest, cinfo: &ClientInfo) -> Option<(u16, Vec<u8>)> {
        // TODO: Check password, presence & group label, set join date
        let room = self.rooms.get_mut(&req.roomId())?;
        room.user_cnt += 1;
        let mut room_user = RoomUser::from_JoinRoomRequest(req);
        room_user.user_id = cinfo.user_id;
        room_user.npid = cinfo.npid.clone();
        room_user.online_name = cinfo.online_name.clone();
        room_user.avatar_url = cinfo.avatar_url.clone();
        room_user.member_id = room.user_cnt;
        // TODO: Group Label, joindate
        room.users.insert(room.user_cnt, room_user);

        // Set full flag if necessary
        if room.users.len() == room.max_slot as usize {
            room.flag_attr |= SceNpMatching2FlagAttr::SCE_NP_MATCHING2_ROOM_FLAG_ATTR_FULL as u32;
        }

        let user_set = self.user_rooms.entry(cinfo.user_id).or_insert(HashSet::new());
        user_set.insert(room.room_id);

        let mut builder = flatbuffers::FlatBufferBuilder::new_with_capacity(1024);
        let room_data = room.to_RoomDataInternal(&mut builder);

        builder.finish(room_data, None);
        Some((room.user_cnt, builder.finished_data().to_vec()))
    }

    ///The bool represnets if the room was removed
    pub fn leave_room(&mut self, room_id: u64, user_id: i64) -> Result<(bool, HashSet<i64>), RoomError> {
        let room = self.rooms.get_mut(&room_id).ok_or(RoomError::RoomNotFound)?;

        let user_set = self.user_rooms.get_mut(&user_id).ok_or(RoomError::UserSetNotFound)?;
        if !user_set.remove(&room_id) {
            return Err(RoomError::UserRoomNotFound)
        }

        // This should never panic as it would mean user_rooms is incoherent
        let member_id = room.find_user(user_id).unwrap();

        room.users.remove(&member_id);

        // Remove full flag if necessary
        if room.users.len() != room.max_slot as usize {
            room.flag_attr &= !(SceNpMatching2FlagAttr::SCE_NP_MATCHING2_ROOM_FLAG_ATTR_FULL as u32);
        }

        // Generate list of users left
        let user_list = room.users.values().map(|x| x.user_id).collect();

        if member_id == room.owner {
            // Try to find successor in the designated successor list
            if let Some(&s) = room.owner_succession.iter().find(|x| room.users.contains_key(x)) {
                room.owner = s;
                return Ok((false, user_list));
            }

            use rand::seq::IteratorRandom;
            let mut rng = rand::thread_rng();

            // If no successor is found and there are still users, assign ownership randomly
            if let Some(&s) = room.users.keys().choose(&mut rng) {
                room.owner = s;
                return Ok((false, user_list));
            } else {
                // Remove the room from appropriate list
                let lobby_id = room.lobby_id;
                let world_id = room.world_id;
                if lobby_id == 0 {
                    self.world_rooms.get_mut(&world_id).unwrap().remove(&room_id);
                } else {
                    self.lobby_rooms.get_mut(&lobby_id).unwrap().remove(&room_id);
                }
                // Remove from global room list
                self.rooms.remove(&room_id);
                return Ok((true, user_list));
            }
        }

        Ok((false, user_list))
    }
    pub fn search_room(&self, req: &SearchRoomRequest) -> Vec<u8> {
        let world_id = req.worldId();
        let lobby_id = req.lobbyId();
        let startindex = req.rangeFilter_startIndex();
        let max = req.rangeFilter_max();

        let list = if world_id != 0 {
            self.world_rooms.get(&world_id)
        } else if lobby_id != 0 {
            self.lobby_rooms.get(&lobby_id)
        } else { None };

        let mut builder = flatbuffers::FlatBufferBuilder::new_with_capacity(1024);
        let mut total = 0;
        let list_roomdataexternal  = if let Some(list) = list {
            let room_list: Vec<_> = list
                .iter()
                .filter_map(|&x| self.get_room(x))
                .take(max as usize)
                .map(|x| x.to_RoomDataExternal(&mut builder, req.option()))
                .collect();
            total = room_list.len() as u32;
            Some(builder.create_vector(&room_list))
        } else {
            None
        };

        let resp = SearchRoomResponse::create(
            &mut builder,
            &SearchRoomResponseArgs {
                startIndex: startindex,
                total,
                size_: total,
                rooms: list_roomdataexternal,
            },
        );
        builder.finish(resp, None);
        builder.finished_data().to_vec()
    }

    pub fn set_roomdata_external(&mut self, req: &SetRoomDataExternalRequest) -> Option<()> {
        let room = self.get_mut_room(req.roomId())?;

        let bin_attr_external = req
            .roomBinAttrExternal()
            .map(|x| x.iter().map(|y| RoomBinAttr::from_flatbuffer(&y)).collect())
            .unwrap_or_default();
        let search_bin_attr = req
            .roomSearchableBinAttrExternal()
            .map(|x| x.iter().map(|y| RoomBinAttr::from_flatbuffer(&y)).collect())
            .unwrap_or_default();
        let search_int_attr = req
            .roomSearchableIntAttrExternal()
            .map(|x| x.iter().map(|y| RoomIntAttr::from_flatbuffer(&y)).collect())
            .unwrap_or_default();
        room.bin_attr_external = bin_attr_external;
        room.search_bin_attr = search_bin_attr;
        room.search_int_attr = search_int_attr;

        Some(())
    }
    pub fn get_roomdata_internal(&self, req: &GetRoomDataInternalRequest) -> Option<Vec<u8>> {
        let room = self.get_room(req.roomId())?;

        // TODO: only retrieve specified values

        let mut builder = flatbuffers::FlatBufferBuilder::new_with_capacity(1024);
        let room_data = room.to_RoomDataInternal(&mut builder);

        builder.finish(room_data, None);

        Some(builder.finished_data().to_vec())
    }
    pub fn set_roomdata_internal(&mut self, req: &SetRoomDataInternalRequest) -> Option<()> {
        let room = self.get_mut_room(req.roomId())?;

        let flag_filter = req.flagFilter();
        let flag_attr = req.flagAttr();
        room.flag_attr = (flag_attr & flag_filter) | (room.flag_attr & !flag_filter);
        let bin_attr_internal = req
            .roomBinAttrInternal()
            .map(|x| x.iter().map(|y| RoomBinAttrInternal::from_flatbuffer(&y)).collect())
            .unwrap_or_default();
        room.bin_attr_internal = bin_attr_internal;
        // Group stuff TODO
        room.password_slot_mask = req.passwordSlotMask();
        let mut succession_list: VecDeque<u16> = VecDeque::new();
        if let Some(vec) = req.ownerPrivilegeRank() {
            for i in 0..vec.len() {
                succession_list.push_back(vec.get(i));
            }
        }
        let succession_list = req.ownerPrivilegeRank().map(|x| x.iter().collect()).unwrap_or_default();
        room.owner_succession = succession_list;

        Some(())
    }

    pub fn get_rooms_by_user(&self, user: i64) -> Option<HashSet<u64>> {
        self.user_rooms.get(&user).cloned()
    }
}
