use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use crate::server::client::ClientInfo;
use crate::server::client::ErrorType;
use crate::server::log::LogManager;
use crate::server::stream_extractor::np2_structs_generated::*;

use num_derive::FromPrimitive;
use num_traits::FromPrimitive;

#[allow(non_camel_case_types)]
#[repr(u8)]
#[derive(FromPrimitive)]
enum SceNpMatching2Operator {
    SCE_NP_MATCHING2_OPERATOR_EQ = 1,
    SCE_NP_MATCHING2_OPERATOR_NE = 2,
    SCE_NP_MATCHING2_OPERATOR_LT = 3,
    SCE_NP_MATCHING2_OPERATOR_LE = 4,
    SCE_NP_MATCHING2_OPERATOR_GT = 5,
    SCE_NP_MATCHING2_OPERATOR_GE = 6,
}

pub struct RoomBinAttr {
    id: u16,
    attr: Vec<u8>,
}
impl RoomBinAttr {
    pub fn from_flatbuffer(fb: &BinAttr) -> RoomBinAttr {
        let id = fb.id();
        let mut attr: Vec<u8> = Vec::new();
        if let Some(fb_attr) = fb.data() {
            for i in 0..fb_attr.len() {
                attr.push(fb_attr[i]);
            }
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
            for i in 0..8 {
                label[i] = vec[i];
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

struct SignalParam {
    sig_type: u8,
    flag: u8,
    hub_member_id: u16,
}
impl SignalParam {
    pub fn from_flatbuffer(fb: &OptParam) -> SignalParam {
        let sig_type = fb.type_();
        let flag = fb.flag();
        let hub_member_id = fb.hubMemberId();

        SignalParam { sig_type, flag, hub_member_id }
    }
}

pub struct RoomUser {
    pub user_id: i64,
    pub npid: String,
    pub psn_name: String,
    pub avatar_url: String,
    join_date: u64,
    flag_attr: u32,

    group_id: u8,
    member_attr: Vec<RoomMemberBinAttr>,
    team_id: u8,

    member_id: u16,
}
impl RoomUser {
    pub fn from_CreateJoinRoomRequest(fb: &CreateJoinRoomRequest) -> RoomUser {
        let mut group_id = 0;
        let mut member_attr: Vec<RoomMemberBinAttr> = Vec::new();

        if let Some(vec) = fb.joinRoomGroupLabel() {
            // Add group to room and set id TODO
        }
        if let Some(vec) = fb.roomMemberBinAttrInternal() {
            for i in 0..vec.len() {
                member_attr.push(RoomMemberBinAttr::from_flatbuffer(&vec.get(i)));
            }
        }
        let team_id = fb.teamId();

        RoomUser {
            user_id: 0,
            npid: String::new(),
            psn_name: String::new(),
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
        let mut group_id = 0;
        let mut member_attr: Vec<RoomMemberBinAttr> = Vec::new();

        if let Some(vec) = fb.joinRoomGroupLabel() {
            // Find/Create corresponding group and set id
        }
        if let Some(vec) = fb.roomMemberBinAttrInternal() {
            for i in 0..vec.len() {
                member_attr.push(RoomMemberBinAttr::from_flatbuffer(&vec.get(i)));
            }
        }
        let team_id = fb.teamId();

        RoomUser {
            user_id: 0,
            npid: String::new(),
            psn_name: String::new(),
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
        let psn_name = builder.create_string(&self.psn_name);
        let avatar_url = builder.create_string(&self.avatar_url);

        let user_info = UserInfo2::create(
            builder,
            &UserInfo2Args {
                npId: Some(npid),
                onlineName: Some(psn_name),
                avatarUrl: Some(avatar_url),
            },
        );

        let mut bin_attr = None;
        if self.member_attr.len() != 0 {
            let mut bin_attrs = Vec::new();
            for i in 0..self.member_attr.len() {
                bin_attrs.push(self.member_attr[i].to_flatbuffer(builder));
            }
            bin_attr = Some(builder.create_vector(&bin_attrs));
        }

        RoomMemberDataInternal::create(
            builder,
            &RoomMemberDataInternalArgs {
                userInfo: Some(user_info),
                joinDate: self.join_date,
                memberId: self.member_id,
                teamId: self.team_id,
                roomGroup: self.group_id,
                natType: 0, // todo
                flagAttr: self.flag_attr,
                roomMemberBinAttrInternal: bin_attr,
            },
        )
    }
}

pub struct Room {
    // Info given from stream
    pub world_id: u32,
    pub lobby_id: u64,
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
    pub server_id: u16,
    pub room_id: u64,
    users: HashMap<u16, RoomUser>,
    user_cnt: u16,
    owner: u16,

    // Set by SetInternal
    owner_succession: Vec<u16>,
}
impl Room {
    pub fn from_flatbuffer(fb: &CreateJoinRoomRequest) -> Room {
        let world_id = fb.worldId();
        let lobby_id = fb.lobbyId();
        let max_slot = fb.maxSlot() as u16;
        let flag_attr = fb.flagAttr();
        let mut bin_attr_internal: Vec<RoomBinAttrInternal> = Vec::new();
        let mut bin_attr_external: Vec<RoomBinAttr> = Vec::new();
        let mut search_bin_attr: Vec<RoomBinAttr> = Vec::new();
        let mut search_int_attr: Vec<RoomIntAttr> = Vec::new();
        let mut room_password = None;
        let mut group_config: Vec<RoomGroupConfig> = Vec::new();
        let password_slot_mask;
        let mut allowed_users: Vec<String> = Vec::new();
        let mut blocked_users: Vec<String> = Vec::new();
        let mut signaling_param = None;

        if let Some(vec) = fb.roomBinAttrInternal() {
            for i in 0..vec.len() {
                bin_attr_internal.push(RoomBinAttrInternal::from_flatbuffer(&vec.get(i)));
            }
        }
        if let Some(vec) = fb.roomBinAttrExternal() {
            for i in 0..vec.len() {
                bin_attr_external.push(RoomBinAttr::from_flatbuffer(&vec.get(i)));
            }
        }
        if let Some(vec) = fb.roomSearchableBinAttrExternal() {
            for i in 0..vec.len() {
                search_bin_attr.push(RoomBinAttr::from_flatbuffer(&vec.get(i)));
            }
        }
        if let Some(vec) = fb.roomSearchableIntAttrExternal() {
            for i in 0..vec.len() {
                search_int_attr.push(RoomIntAttr::from_flatbuffer(&vec.get(i)));
            }
        }
        if let Some(password) = fb.roomPassword() {
            let mut room_password_data = [0; 8];
            for i in 0..8 {
                room_password_data[i] = password[i];
            }
            room_password = Some(room_password_data);
        }
        if let Some(vec) = fb.groupConfig() {
            for i in 0..vec.len() {
                group_config.push(RoomGroupConfig::from_flatbuffer(&vec.get(i), (i + 1) as u8));
            }
        }
        password_slot_mask = fb.passwordSlotMask();
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
            users: HashMap::new(),
            user_cnt: 0,
            owner: 0,
            owner_succession: Vec::new(),
        }
    }
    pub fn to_RoomDataInternal<'a>(&self, builder: &mut flatbuffers::FlatBufferBuilder<'a>) -> flatbuffers::WIPOffset<RoomDataInternal<'a>> {
        let mut final_member_list = None;
        if self.users.len() != 0 {
            let mut member_list = Vec::new();
            for user in &self.users {
                member_list.push(user.1.to_RoomMemberDataInternal(builder));
            }
            final_member_list = Some(builder.create_vector(&member_list));
        }
        let mut final_group_list = None;
        if self.group_config.len() != 0 {
            let mut group_list = Vec::new();
            for group in &self.group_config {
                group_list.push(group.to_flatbuffer(builder));
            }
            final_group_list = Some(builder.create_vector(&group_list));
        }
        let mut final_internalbinattr = None;
        if self.bin_attr_internal.len() != 0 {
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
        if self.users.len() != 0 {
            rbuild.add_memberList(final_member_list.unwrap());
        }
        rbuild.add_ownerId(self.owner);
        if self.group_config.len() != 0 {
            rbuild.add_roomGroup(final_group_list.unwrap());
        }
        rbuild.add_flagAttr(self.flag_attr);
        if self.bin_attr_internal.len() != 0 {
            rbuild.add_roomBinAttrInternal(final_internalbinattr.unwrap());
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
                let s = builder.create_string(&self.users.get(&self.owner).unwrap().psn_name);
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
        if self.group_config.len() != 0 {
            let mut group_list = Vec::new();
            for group in &self.group_config {
                group_list.push(group.to_flatbuffer(builder));
            }
            final_group_list = Some(builder.create_vector(&group_list));
        }
        let mut final_searchint = None;
        if self.search_int_attr.len() != 0 {
            let mut int_list = Vec::new();
            for int in &self.search_int_attr {
                int_list.push(int.to_flatbuffer(builder));
            }
            final_searchint = Some(builder.create_vector(&int_list));
        }
        let mut final_searchbin = None;
        if self.search_bin_attr.len() != 0 {
            let mut bin_list = Vec::new();
            for bin in &self.search_bin_attr {
                bin_list.push(bin.to_flatbuffer(builder));
            }
            final_searchbin = Some(builder.create_vector(&bin_list));
        }
        let mut final_binattrexternal = None;
        if self.bin_attr_external.len() != 0 {
            let mut bin_list = Vec::new();
            for bin in &self.bin_attr_external {
                bin_list.push(bin.to_flatbuffer(builder));
            }
            final_binattrexternal = Some(builder.create_vector(&bin_list));
        }

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
            rbuild.add_roomGroup(final_group_list.unwrap());
        }
        rbuild.add_flagAttr(self.flag_attr);
        // External stuff
        if self.search_int_attr.len() != 0 {
            rbuild.add_roomSearchableIntAttrExternal(final_searchint.unwrap());
        }
        if self.search_bin_attr.len() != 0 {
            rbuild.add_roomSearchableBinAttrExternal(final_searchbin.unwrap());
        }
        if self.bin_attr_external.len() != 0 {
            rbuild.add_roomBinAttrExternal(final_binattrexternal.unwrap());
        }

        rbuild.finish()
    }

    pub fn is_match(&self, req: &SearchRoomRequest) -> bool {
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
                    SceNpMatching2Operator::SCE_NP_MATCHING2_OPERATOR_EQ => {
                        if found_intsearch.attr != num {
                            return false;
                        }
                    }
                    SceNpMatching2Operator::SCE_NP_MATCHING2_OPERATOR_NE => {
                        if found_intsearch.attr == num {
                            return false;
                        }
                    }
                    SceNpMatching2Operator::SCE_NP_MATCHING2_OPERATOR_LT => {
                        if found_intsearch.attr >= num {
                            return false;
                        }
                    }
                    SceNpMatching2Operator::SCE_NP_MATCHING2_OPERATOR_LE => {
                        if found_intsearch.attr > num {
                            return false;
                        }
                    }
                    SceNpMatching2Operator::SCE_NP_MATCHING2_OPERATOR_GT => {
                        if found_intsearch.attr <= num {
                            return false;
                        }
                    }
                    SceNpMatching2Operator::SCE_NP_MATCHING2_OPERATOR_GE => {
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
                    SceNpMatching2Operator::SCE_NP_MATCHING2_OPERATOR_EQ => {
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
}

pub struct RoomManager {
    rooms: HashMap<u64, Room>, // roomid/roomdata
    room_cnt: u64,
    world_rooms: HashMap<u32, Vec<u64>>, // worldid/roomids
    lobby_rooms: HashMap<u64, Vec<u64>>, // lobbyid/roomids

    log_manager: Arc<Mutex<LogManager>>,
}

impl RoomManager {
    pub fn new(log_manager: Arc<Mutex<LogManager>>) -> RoomManager {
        RoomManager {
            rooms: HashMap::new(),
            room_cnt: 0,
            world_rooms: HashMap::new(),
            lobby_rooms: HashMap::new(),
            log_manager,
        }
    }

    fn log(&self, s: &str) {
        self.log_manager.lock().unwrap().write(&format!("RoomManager: {}", s));
    }

    pub fn room_exists(&self, room_id: u64) -> bool {
        return self.rooms.contains_key(&room_id);
    }
    pub fn get_room(&self, room_id: u64) -> &Room {
        return self.rooms.get(&room_id).unwrap();
    }
    pub fn get_mut_room(&mut self, room_id: u64) -> &mut Room {
        return self.rooms.get_mut(&room_id).unwrap();
    }

    pub fn get_corresponding_world(&self, room_id: u64) -> Result<u32, u8> {
        if !self.room_exists(room_id) {
            return Err(ErrorType::NotFound as u8);
        }
        Ok(self.get_room(room_id).world_id)
    }

    pub fn create_room(&mut self, req: &CreateJoinRoomRequest, cinfo: &ClientInfo, server_id: u16) -> Vec<u8> {
        self.room_cnt += 1;

        let mut room = Room::from_flatbuffer(req);
        room.user_cnt += 1;
        room.owner = room.user_cnt;
        room.room_id = self.room_cnt;
        room.server_id = server_id;
        let mut room_user = RoomUser::from_CreateJoinRoomRequest(req);
        room_user.user_id = cinfo.user_id;
        room_user.npid = cinfo.npid.clone();
        room_user.psn_name = cinfo.psn_name.clone();
        room_user.avatar_url = cinfo.avatar_url.clone();
        room_user.member_id = room.user_cnt;
        // TODO: Group Label, joindate
        room.users.insert(room.user_cnt, room_user);

        if room.lobby_id == 0 {
            let davec = self.world_rooms.entry(room.world_id).or_insert(Vec::new());
            davec.push(self.room_cnt);
        } else {
            let davec = self.lobby_rooms.entry(room.lobby_id).or_insert(Vec::new());
            davec.push(self.room_cnt);
        }

        self.rooms.insert(self.room_cnt, room);

        // Prepare roomDataInternal
        let mut builder = flatbuffers::FlatBufferBuilder::new_with_capacity(1024);
        let room_data = self.rooms[&self.room_cnt].to_RoomDataInternal(&mut builder);

        builder.finish(room_data, None);
        builder.finished_data().to_vec()
    }

    pub fn join_room(&mut self, req: &JoinRoomRequest, cinfo: &ClientInfo) -> Result<(u16, Vec<u8>), u8> {
        if !self.room_exists(req.roomId()) {
            return Err(ErrorType::NotFound as u8);
        }

        // TODO: Check password, presence & group label, set join date

        let room = self.rooms.get_mut(&req.roomId()).unwrap();
        room.user_cnt += 1;
        let mut room_user = RoomUser::from_JoinRoomRequest(req);
        room_user.user_id = cinfo.user_id;
        room_user.npid = cinfo.npid.clone();
        room_user.psn_name = cinfo.psn_name.clone();
        room_user.avatar_url = cinfo.avatar_url.clone();
        room_user.member_id = room.user_cnt;
        // TODO: Group Label, joindate
        room.users.insert(room.user_cnt, room_user);

        let mut builder = flatbuffers::FlatBufferBuilder::new_with_capacity(1024);
        let room_data = room.to_RoomDataInternal(&mut builder);

        builder.finish(room_data, None);
        Ok((room.user_cnt, builder.finished_data().to_vec()))
    }

    pub fn search_room(&self, req: &SearchRoomRequest) -> Vec<u8> {
        let world_id = req.worldId();
        let lobby_id = req.lobbyId();
        let startindex = req.rangeFilter_startIndex();
        let max = req.rangeFilter_max();

        let mut list = None;
        if world_id != 0 {
            list = self.world_rooms.get(&world_id);
        } else if lobby_id != 0 {
            list = self.lobby_rooms.get(&lobby_id);
        }

        let mut matching_rooms = Vec::new();

        let mut num_found = 0;

        if let Some(room_list) = list {
            for room_id in room_list.iter() {
                let room = self.get_room(*room_id);
                if room.is_match(req) {
                    matching_rooms.push(room);
                    num_found += 1;
                }
                if num_found >= max {
                    break;
                }
            }
        }
        let mut builder = flatbuffers::FlatBufferBuilder::new_with_capacity(1024);

        let mut list_roomdataexternal = Default::default();
        if matching_rooms.len() != 0 {
            let mut room_list = Vec::new();
            for room in &matching_rooms {
                room_list.push(room.to_RoomDataExternal(&mut builder, req.option()));
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
    pub fn set_roomdata_external(&mut self, req: &SetRoomDataExternalRequest) -> Result<(), u8> {
        if !self.room_exists(req.roomId()) {
            return Err(ErrorType::NotFound as u8);
        }
        let room = self.get_mut_room(req.roomId());

        let mut bin_attr_external: Vec<RoomBinAttr> = Vec::new();
        let mut search_bin_attr: Vec<RoomBinAttr> = Vec::new();
        let mut search_int_attr: Vec<RoomIntAttr> = Vec::new();

        if let Some(vec) = req.roomBinAttrExternal() {
            for i in 0..vec.len() {
                bin_attr_external.push(RoomBinAttr::from_flatbuffer(&vec.get(i)));
            }
        }
        if let Some(vec) = req.roomSearchableBinAttrExternal() {
            for i in 0..vec.len() {
                search_bin_attr.push(RoomBinAttr::from_flatbuffer(&vec.get(i)));
            }
        }
        if let Some(vec) = req.roomSearchableIntAttrExternal() {
            for i in 0..vec.len() {
                search_int_attr.push(RoomIntAttr::from_flatbuffer(&vec.get(i)));
            }
        }

        room.bin_attr_external = bin_attr_external;
        room.search_bin_attr = search_bin_attr;
        room.search_int_attr = search_int_attr;

        Ok(())
    }
    pub fn get_roomdata_internal(&self, req: &GetRoomDataInternalRequest) -> Result<Vec<u8>, u8> {
        if !self.room_exists(req.roomId()) {
            return Err(ErrorType::NotFound as u8);
        }
        let room = self.get_room(req.roomId());

        // TODO: only retrieve specified values

        let mut builder = flatbuffers::FlatBufferBuilder::new_with_capacity(1024);
        let room_data = room.to_RoomDataInternal(&mut builder);

        builder.finish(room_data, None);

        Ok(builder.finished_data().to_vec())
    }
    pub fn set_roomdata_internal(&mut self, req: &SetRoomDataInternalRequest) -> Result<(), u8> {
        if !self.room_exists(req.roomId()) {
            return Err(ErrorType::NotFound as u8);
        }
        let room = self.get_mut_room(req.roomId());

        let flag_filter = req.flagFilter();
        let flag_attr = req.flagAttr();
        room.flag_attr = (flag_attr & flag_filter) | (room.flag_attr & !flag_filter);
        let mut bin_attr_internal: Vec<RoomBinAttrInternal> = Vec::new();
        if let Some(vec) = req.roomBinAttrInternal() {
            for i in 0..vec.len() {
                bin_attr_internal.push(RoomBinAttrInternal::from_flatbuffer(&vec.get(i)));
            }
        }
        room.bin_attr_internal = bin_attr_internal;
        // Group stuff TODO
        room.password_slot_mask = req.passwordSlotMask();
        let mut succession_list: Vec<u16> = Vec::new();
        if let Some(vec) = req.ownerPrivilegeRank() {
            for i in 0..vec.len() {
                succession_list.push(vec.get(i));
            }
        }
        room.owner_succession = succession_list;

        Ok(())
    }
    pub fn get_room_member_update_info(&self, room_id: u64, member_id: u16, event_cause: u8, user_opt_data: &PresenceOptionData) -> Vec<u8> {
        assert!(self.room_exists(room_id));
        let room = self.get_room(room_id);

        assert!(room.users.contains_key(&member_id));
        let user = room.users.get(&member_id).unwrap();

        // Builds flatbuffer
        let mut builder = flatbuffers::FlatBufferBuilder::new_with_capacity(1024);

        let member_internal = user.to_RoomMemberDataInternal(&mut builder);

        let mut opt_data_vec: Vec<u8> = Vec::new();
        for i in 0..16 {
            opt_data_vec.push(*user_opt_data.data().unwrap().get(i).unwrap());
        }
        let opt_data_vec = builder.create_vector(&opt_data_vec);

        let opt_data = PresenceOptionData::create(&mut builder, &PresenceOptionDataArgs {
            len: user_opt_data.len(),
            data: Some(opt_data_vec),
        });

        let up_info = RoomMemberUpdateInfo::create(&mut builder, &RoomMemberUpdateInfoArgs {
            roomMemberDataInternal: Some(member_internal),
            eventCause: event_cause,
            optData: Some(opt_data),
        });
        
        builder.finish(up_info, None);
        builder.finished_data().to_vec()
    }
    pub fn get_room_users(&self, room_id: u64) -> Vec<(u16, i64)> {
        assert!(self.room_exists(room_id));
        let room = self.get_room(room_id);

        let mut users_vec = Vec::new();
        for user in &room.users {
            users_vec.push((user.0.clone(), user.1.user_id));
        }

        users_vec
    }
}
