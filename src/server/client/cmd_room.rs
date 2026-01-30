// Room Commands

use prost::Message;

use crate::server::client::*;
use crate::server::stream_extractor::protobuf_helpers::{ProtobufMaker, ProtobufVerifier};

impl Client {
	pub fn req_create_room(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let (com_id, create_req) = self.get_com_and_pb::<CreateJoinRoomRequest>(data)?;

		let server_id = Database::new(self.get_database_connection()?)
			.get_corresponding_server(&com_id, create_req.world_id, create_req.lobby_id)
			.map_err(|_| {
				warn!(
					"Attempted to use invalid worldId/lobbyId for comId {}: {}/{}",
					&com_id_to_string(&com_id),
					create_req.world_id,
					create_req.lobby_id
				);
				ErrorType::InvalidInput
			})?;

		let signaling_info = if let Some(sig_info) = self.shared.client_infos.read().get(&self.client_info.user_id) {
			sig_info.signaling_info.read().clone()
		} else {
			warn!("User created a room with no signaling info!");
			ClientSharedSignalingInfo::new()
		};

		let resp = self.shared.room_manager.write().create_room(&com_id, &create_req, &self.client_info, server_id, signaling_info);

		match resp {
			Err(ErrorType::Malformed) => Err(ErrorType::Malformed),
			Err(e) => Ok(e),
			Ok(resp) => {
				Client::add_data_packet(reply, &resp);
				Ok(ErrorType::NoError)
			}
		}
	}

	pub async fn req_join_room(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let (com_id, join_req) = self.get_com_and_pb::<JoinRoomRequest>(data)?;
		let room_id = join_req.room_id;

		let (response, notifications);
		{
			let mut room_manager = self.shared.room_manager.write();
			if !room_manager.room_exists(&com_id, room_id) {
				warn!("User requested to join a room that doesn't exist!");
				return Ok(ErrorType::RoomMissing);
			}

			let users = {
				let room = room_manager.get_room(&com_id, room_id);
				room.get_room_users()
			};

			if users.iter().any(|x| *x.1 == self.client_info.user_id) {
				warn!("User tried to join a room he was already a member of!");
				return Ok(ErrorType::RoomAlreadyJoined);
			}

			let signaling_info = if let Some(sig_info) = self.shared.client_infos.read().get(&self.client_info.user_id) {
				sig_info.signaling_info.read().clone()
			} else {
				warn!("User joined a room with no signaling info!");
				ClientSharedSignalingInfo::new()
			};

			let resp = room_manager.join_room(&com_id, &join_req, &self.client_info, signaling_info);
			match resp {
				Err(ErrorType::Malformed) => return Err(ErrorType::Malformed),
				Err(e) => return Ok(e),
				Ok((resp, noti)) => {
					(response, notifications) = (resp, noti);
				}
			}
		}

		Client::add_data_packet(reply, &response);

		for (notification_data, user_ids) in notifications.into_iter().flatten() {
			let mut n_msg = Vec::with_capacity(notification_data.len() + size_of::<u32>());
			Client::add_data_packet(&mut n_msg, &notification_data);
			let notif = Client::create_notification(NotificationType::UserJoinedRoom, &n_msg);
			self.send_notification(&notif, &user_ids).await;
		}

		Ok(ErrorType::NoError)
	}

	pub async fn leave_room(&self, com_id: &ComId, room_id: u64, opt_data: Option<&PresenceOptionData>, event_cause: EventCause) -> ErrorType {
		let (destroyed, users, user_data);
		{
			let mut room_manager = self.shared.room_manager.write();
			if !room_manager.room_exists(com_id, room_id) {
				return ErrorType::RoomMissing;
			}

			let room = room_manager.get_room(com_id, room_id);
			let member_id = room.get_member_id(self.client_info.user_id);
			if let Err(e) = member_id {
				return e;
			}

			// We get this in advance in case the room is not destroyed
			let wip_user_data = room.get_room_member_update_info(member_id.unwrap(), event_cause.clone(), opt_data);
			user_data = wip_user_data.encode_to_vec();

			let res = room_manager.leave_room(com_id, room_id, self.client_info.user_id);
			if let Err(e) = res {
				return e;
			}
			let (destroyed_toa, users_toa) = res.unwrap();
			destroyed = destroyed_toa;
			users = users_toa;
		}

		if destroyed {
			// Notify other room users that the room has been destroyed
			let room_update = RoomUpdateInfo {
				event_cause: Uint8::new_from_value(event_cause as u8),
				error_code: 0,
				opt_data: opt_data.cloned(),
			};
			let room_update_data = room_update.encode_to_vec();

			let mut n_msg: Vec<u8> = Vec::new();
			n_msg.extend(&room_id.to_le_bytes());
			Client::add_data_packet(&mut n_msg, &room_update_data);

			let notif = Client::create_notification(NotificationType::RoomDestroyed, &n_msg);
			self.send_notification(&notif, &users).await;
		} else {
			// Notify other room users that someone left the room
			let mut n_msg: Vec<u8> = Vec::new();
			n_msg.extend(&room_id.to_le_bytes());
			Client::add_data_packet(&mut n_msg, &user_data);

			let notif = Client::create_notification(NotificationType::UserLeftRoom, &n_msg);
			self.send_notification(&notif, &users).await;
		}

		ErrorType::NoError
	}

	pub async fn req_leave_room(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let (com_id, leave_req) = self.get_com_and_pb::<LeaveRoomRequest>(data)?;

		let res = self.leave_room(&com_id, leave_req.room_id, leave_req.opt_data.as_ref(), EventCause::LeaveAction).await;
		reply.extend(&leave_req.room_id.to_le_bytes());
		Ok(res)
	}

	pub fn req_search_room(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let (com_id, search_req) = self.get_com_and_pb::<SearchRoomRequest>(data)?;

		let resp = self.shared.room_manager.read().search_room(&com_id, &search_req)?;
		Client::add_data_packet(reply, &resp);
		Ok(ErrorType::NoError)
	}

	pub fn req_get_roomdata_external_list(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let (com_id, getdata_req) = self.get_com_and_pb::<GetRoomDataExternalListRequest>(data)?;

		let resp = self.shared.room_manager.read().get_roomdata_external_list(&com_id, &getdata_req)?;

		Client::add_data_packet(reply, &resp);

		Ok(ErrorType::NoError)
	}

	pub fn req_get_room_member_data_external_list(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let com_id = self.get_com_id_with_redir(data);
		let room_id = data.get::<u64>();

		if data.error() {
			warn!("Error while extracting data from GetRoomMemberDataExternalList command");
			return Err(ErrorType::Malformed);
		}

		let resp = self.shared.room_manager.read().get_room_member_data_external_list(&com_id, room_id);

		match resp {
			Err(e) => Ok(e),
			Ok(resp) => {
				Client::add_data_packet(reply, &resp);
				Ok(ErrorType::NoError)
			}
		}
	}

	pub fn req_set_roomdata_external(&mut self, data: &mut StreamExtractor) -> Result<ErrorType, ErrorType> {
		let (com_id, setdata_req) = self.get_com_and_pb::<SetRoomDataExternalRequest>(data)?;

		if let Err(e) = self.shared.room_manager.write().set_roomdata_external(&com_id, &setdata_req, self.client_info.user_id) {
			Ok(e)
		} else {
			Ok(ErrorType::NoError)
		}
	}

	pub fn req_get_roomdata_internal(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let (com_id, getdata_req) = self.get_com_and_pb::<GetRoomDataInternalRequest>(data)?;

		let resp = self.shared.room_manager.read().get_roomdata_internal(&com_id, &getdata_req);
		if let Err(e) = resp {
			return Ok(e);
		}

		let resp = resp.unwrap();
		Client::add_data_packet(reply, &resp);
		Ok(ErrorType::NoError)
	}

	pub async fn req_set_roomdata_internal(&mut self, data: &mut StreamExtractor) -> Result<ErrorType, ErrorType> {
		let (com_id, setdata_req) = self.get_com_and_pb::<SetRoomDataInternalRequest>(data)?;

		let room_id = setdata_req.room_id;
		let res = self.shared.room_manager.write().set_roomdata_internal(&com_id, &setdata_req, self.client_info.user_id);

		match res {
			Ok(Some((users, notif_data))) => {
				let mut n_msg: Vec<u8> = Vec::new();
				n_msg.extend(&room_id.to_le_bytes());
				Client::add_data_packet(&mut n_msg, &notif_data);
				let notif = Client::create_notification(NotificationType::UpdatedRoomDataInternal, &n_msg);
				self.send_notification(&notif, &users).await;
				self.self_notification(&notif);
				Ok(ErrorType::NoError)
			}
			Ok(None) => Ok(ErrorType::NoError),
			Err(e) => Ok(e),
		}
	}

	pub async fn req_get_roommemberdata_internal(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let (com_id, getdata_req) = self.get_com_and_pb::<GetRoomMemberDataInternalRequest>(data)?;

		let resp = self.shared.room_manager.read().get_roommemberdata_internal(&com_id, &getdata_req);

		if let Err(e) = resp {
			return Ok(e);
		}
		let resp = resp.unwrap();

		Client::add_data_packet(reply, &resp);
		Ok(ErrorType::NoError)
	}

	pub async fn req_set_roommemberdata_internal(&mut self, data: &mut StreamExtractor) -> Result<ErrorType, ErrorType> {
		let (com_id, setdata_req) = self.get_com_and_pb::<SetRoomMemberDataInternalRequest>(data)?;

		let room_id = setdata_req.room_id;
		let res = self.shared.room_manager.write().set_roommemberdata_internal(&com_id, &setdata_req, self.client_info.user_id);

		match res {
			Ok(Some((users, notif_data))) => {
				let mut n_msg: Vec<u8> = Vec::new();
				n_msg.extend(&room_id.to_le_bytes());
				Client::add_data_packet(&mut n_msg, &notif_data);
				let notif = Client::create_notification(NotificationType::UpdatedRoomMemberDataInternal, &n_msg);
				self.send_notification(&notif, &users).await;
				self.self_notification(&notif);
				Ok(ErrorType::NoError)
			}
			Ok(None) => Ok(ErrorType::NoError),
			Err(e) => Ok(e),
		}
	}

	pub fn req_ping_room_owner(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let com_id = self.get_com_id_with_redir(data);
		let room_id = data.get::<u64>();
		if data.error() {
			warn!("Error while extracting data from PingRoomOwner command");
			return Err(ErrorType::Malformed);
		}

		let infos = self.shared.room_manager.read().get_room_infos(&com_id, room_id);
		if let Err(e) = infos {
			return Ok(e);
		}
		let (server_id, world_id, _) = infos.unwrap();

		let resp = GetPingInfoResponse {
			server_id: Uint16::new_from_value(server_id),
			world_id,
			room_id,
			rtt: 20000,
		};

		let finished_data = resp.encode_to_vec();
		Client::add_data_packet(reply, &finished_data);

		Ok(ErrorType::NoError)
	}

	pub async fn req_send_room_message(&mut self, data: &mut StreamExtractor) -> Result<ErrorType, ErrorType> {
		let (com_id, msg_req) = self.get_com_and_pb::<SendRoomMessageRequest>(data)?;

		let room_id = msg_req.room_id;
		let (notif, member_id, users);

		let mut dst_vec: Vec<u16> = Vec::with_capacity(msg_req.dst.len());
		for a in &msg_req.dst {
			dst_vec.push(a.get_verified()?);
		}

		{
			let room_manager = self.shared.room_manager.read();
			if !room_manager.room_exists(&com_id, room_id) {
				warn!("User requested to send a message to a room that doesn't exist!");
				return Ok(ErrorType::RoomMissing);
			}
			{
				let room = room_manager.get_room(&com_id, room_id);
				let m_id = room.get_member_id(self.client_info.user_id);
				if m_id.is_err() {
					warn!("User requested to send a message to a room that he's not a member of!");
					return Ok(ErrorType::Unauthorized);
				}
				member_id = m_id.unwrap();
				users = room.get_room_users();
			}

			let cast_type = msg_req.cast_type.get_verified()?;
			let option = msg_req.option.get_verified()?;

			let mut npid = None;
			if (option & 0x01) != 0 {
				npid = Some(self.client_info.npid.clone());
			}
			let mut online_name = None;
			if (option & 0x02) != 0 {
				online_name = Some(self.client_info.online_name.clone());
			}
			let mut avatar_url = None;
			if (option & 0x04) != 0 {
				avatar_url = Some(self.client_info.avatar_url.clone());
			}

			let src_user_info = UserInfo {
				np_id: npid.unwrap_or_default(),
				online_name: online_name.unwrap_or_default(),
				avatar_url: avatar_url.unwrap_or_default(),
			};

			let resp = RoomMessageInfo {
				filtered: false,
				cast_type: Uint8::new_from_value(cast_type),
				dst: msg_req.dst.clone(),
				src_member: Some(src_user_info),
				msg: msg_req.msg.clone(),
			};

			let finished_data = resp.encode_to_vec();

			let mut n_msg: Vec<u8> = Vec::new();
			n_msg.extend(&room_id.to_le_bytes());
			n_msg.extend(&member_id.to_le_bytes());
			Client::add_data_packet(&mut n_msg, &finished_data);
			notif = Client::create_notification(NotificationType::RoomMessageReceived, &n_msg);
		}

		let cast_type = msg_req.cast_type.get_verified()?;
		match cast_type {
			1 => {
				// SCE_NP_MATCHING2_CASTTYPE_BROADCAST
				let user_ids: HashSet<i64> = users.iter().filter_map(|x| if *x.1 != self.client_info.user_id { Some(*x.1) } else { None }).collect();
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
							Some(*x.1)
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
				return Ok(ErrorType::Unsupported);
			}
			_ => {
				warn!("Invalid broadcast type in send_room_message!");
				return Err(ErrorType::InvalidInput); // This shouldn't happen, closing connection
			}
		}

		Ok(ErrorType::NoError)
	}
}
