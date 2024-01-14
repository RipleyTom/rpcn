// Room Commands

use crate::server::client::*;

impl Client {
	pub fn req_create_room(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let (com_id, create_req) = self.get_com_and_fb::<CreateJoinRoomRequest>(data)?;

		let server_id = Database::new(self.get_database_connection()?)
			.get_corresponding_server(&com_id, create_req.worldId(), create_req.lobbyId())
			.map_err(|_| {
				warn!(
					"Attempted to use invalid worldId/lobbyId for comId {}: {}/{}",
					&com_id_to_string(&com_id),
					create_req.worldId(),
					create_req.lobbyId()
				);
				ErrorType::InvalidInput
			})?;

		let resp = self.shared.room_manager.write().create_room(&com_id, &create_req, &self.client_info, server_id);
		Client::add_data_packet(reply, &resp);
		Ok(ErrorType::NoError)
	}

	pub async fn req_join_room(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let (com_id, join_req) = self.get_com_and_fb::<JoinRoomRequest>(data)?;

		let room_id = join_req.roomId();
		let user_ids: HashSet<i64>;
		let (notif, member_id, users, siginfo, owner);
		{
			let mut room_manager = self.shared.room_manager.write();
			if !room_manager.room_exists(&com_id, room_id) {
				warn!("User requested to join a room that doesn't exist!");
				return Ok(ErrorType::RoomMissing);
			}
			{
				let room = room_manager.get_room(&com_id, room_id);
				users = room.get_room_users();
				siginfo = room.get_signaling_info();
				owner = room.get_owner();
			}

			if users.iter().any(|x| *x.1 == self.client_info.user_id) {
				warn!("User tried to join a room he was already a member of!");
				return Ok(ErrorType::RoomAlreadyJoined);
			}

			let resp = room_manager.join_room(&com_id, &join_req, &self.client_info);
			if let Err(e) = resp {
				warn!("User failed to join the room!");
				return Ok(e);
			}

			let (member_id_ta, resp) = resp.unwrap();
			member_id = member_id_ta;
			Client::add_data_packet(reply, &resp);

			user_ids = users.iter().map(|x| *x.1).collect();

			// Notif other room users a new user has joined
			let mut n_msg: Vec<u8> = Vec::new();
			n_msg.extend(&room_id.to_le_bytes());
			let up_info = room_manager
				.get_room(&com_id, room_id)
				.get_room_member_update_info(member_id, EventCause::None, Some(&join_req.optData().unwrap()));
			Client::add_data_packet(&mut n_msg, &up_info);
			notif = Client::create_notification(NotificationType::UserJoinedRoom, &n_msg);
		}
		self.send_notification(&notif, &user_ids).await;

		// Send signaling stuff if any
		self.signal_connections(room_id, (member_id, self.client_info.user_id), users, siginfo, owner).await;

		Ok(ErrorType::NoError)
	}

	pub async fn leave_room(&self, room_manager: &Arc<RwLock<RoomManager>>, com_id: &ComId, room_id: u64, opt_data: Option<&PresenceOptionData<'_>>, event_cause: EventCause) -> ErrorType {
		let (destroyed, users, user_data);
		{
			let mut room_manager = room_manager.write();
			if !room_manager.room_exists(com_id, room_id) {
				return ErrorType::NotFound;
			}

			let room = room_manager.get_room(com_id, room_id);
			let member_id = room.get_member_id(self.client_info.user_id);
			if let Err(e) = member_id {
				return e;
			}

			// We get this in advance in case the room is not destroyed
			user_data = room.get_room_member_update_info(member_id.unwrap(), event_cause.clone(), opt_data);

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
			let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);
			let opt_data = dc_opt_data(&mut builder, opt_data);
			let room_update = RoomUpdateInfo::create(
				&mut builder,
				&RoomUpdateInfoArgs {
					eventCause: event_cause as u8,
					errorCode: 0,
					optData: Some(opt_data),
				},
			);
			builder.finish(room_update, None);
			let room_update_data = builder.finished_data().to_vec();

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
		let (com_id, leave_req) = self.get_com_and_fb::<LeaveRoomRequest>(data)?;

		let res = self
			.leave_room(&self.shared.room_manager, &com_id, leave_req.roomId(), Some(&leave_req.optData().unwrap()), EventCause::LeaveAction)
			.await;
		reply.extend(&leave_req.roomId().to_le_bytes());
		Ok(res)
	}
	pub fn req_search_room(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let (com_id, search_req) = self.get_com_and_fb::<SearchRoomRequest>(data)?;

		let resp = self.shared.room_manager.read().search_room(&com_id, &search_req);
		Client::add_data_packet(reply, &resp);
		Ok(ErrorType::NoError)
	}
	pub fn req_get_roomdata_external_list(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let (com_id, getdata_req) = self.get_com_and_fb::<GetRoomDataExternalListRequest>(data)?;

		let resp = self.shared.room_manager.read().get_roomdata_external_list(&com_id, &getdata_req);

		Client::add_data_packet(reply, &resp);

		Ok(ErrorType::NoError)
	}

	pub fn req_set_roomdata_external(&mut self, data: &mut StreamExtractor) -> Result<ErrorType, ErrorType> {
		let (com_id, setdata_req) = self.get_com_and_fb::<SetRoomDataExternalRequest>(data)?;

		if let Err(e) = self.shared.room_manager.write().set_roomdata_external(&com_id, &setdata_req) {
			Ok(e)
		} else {
			Ok(ErrorType::NoError)
		}
	}
	pub fn req_get_roomdata_internal(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let (com_id, getdata_req) = self.get_com_and_fb::<GetRoomDataInternalRequest>(data)?;

		let resp = self.shared.room_manager.read().get_roomdata_internal(&com_id, &getdata_req);
		if let Err(e) = resp {
			reply.push(e);
		} else {
			let resp = resp.unwrap();
			Client::add_data_packet(reply, &resp);
		}
		Ok(ErrorType::NoError)
	}
	pub async fn req_set_roomdata_internal(&mut self, data: &mut StreamExtractor) -> Result<ErrorType, ErrorType> {
		let (com_id, setdata_req) = self.get_com_and_fb::<SetRoomDataInternalRequest>(data)?;

		let room_id = setdata_req.roomId();
		let res = self.shared.room_manager.write().set_roomdata_internal(&com_id, &setdata_req, self.client_info.user_id);

		match res {
			Ok((users, notif_data)) => {
				let mut n_msg: Vec<u8> = Vec::new();
				n_msg.extend(&room_id.to_le_bytes());
				Client::add_data_packet(&mut n_msg, &notif_data);
				let notif = Client::create_notification(NotificationType::UpdatedRoomDataInternal, &n_msg);
				self.send_notification(&notif, &users).await;
				self.self_notification(&notif);
				Ok(ErrorType::NoError)
			}
			Err(e) => Ok(e),
		}
	}

	pub async fn req_get_roommemberdata_internal(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let (com_id, getdata_req) = self.get_com_and_fb::<GetRoomMemberDataInternalRequest>(data)?;

		let resp = self.shared.room_manager.read().get_roommemberdata_internal(&com_id, &getdata_req);

		if let Err(e) = resp {
			return Ok(e);
		}
		let resp = resp.unwrap();

		Client::add_data_packet(reply, &resp);
		Ok(ErrorType::NoError)
	}

	pub async fn req_set_roommemberdata_internal(&mut self, data: &mut StreamExtractor) -> Result<ErrorType, ErrorType> {
		let (com_id, setdata_req) = self.get_com_and_fb::<SetRoomMemberDataInternalRequest>(data)?;

		let room_id = setdata_req.roomId();
		let res = self.shared.room_manager.write().set_roommemberdata_internal(&com_id, &setdata_req, self.client_info.user_id);

		match res {
			Ok((users, notif_data)) => {
				let mut n_msg: Vec<u8> = Vec::new();
				n_msg.extend(&room_id.to_le_bytes());
				Client::add_data_packet(&mut n_msg, &notif_data);
				let notif = Client::create_notification(NotificationType::UpdatedRoomMemberDataInternal, &n_msg);
				self.send_notification(&notif, &users).await;
				self.self_notification(&notif);
				Ok(ErrorType::NoError)
			}
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

		let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);
		let resp = GetPingInfoResponse::create(
			&mut builder,
			&GetPingInfoResponseArgs {
				serverId: server_id,
				worldId: world_id,
				roomId: room_id,
				rtt: 20000,
			},
		);

		builder.finish(resp, None);
		let finished_data = builder.finished_data().to_vec();
		Client::add_data_packet(reply, &finished_data);

		Ok(ErrorType::NoError)
	}

	pub async fn req_send_room_message(&mut self, data: &mut StreamExtractor) -> Result<ErrorType, ErrorType> {
		let (com_id, msg_req) = self.get_com_and_fb::<SendRoomMessageRequest>(data)?;

		let room_id = msg_req.roomId();
		let (notif, member_id, users);
		let mut dst_vec: Vec<u16> = Vec::new();
		{
			let room_manager = self.shared.room_manager.read();
			if !room_manager.room_exists(&com_id, room_id) {
				warn!("User requested to send a message to a room that doesn't exist!");
				return Ok(ErrorType::InvalidInput);
			}
			{
				let room = room_manager.get_room(&com_id, room_id);
				let m_id = room.get_member_id(self.client_info.user_id);
				if m_id.is_err() {
					warn!("User requested to send a message to a room that he's not a member of!");
					return Ok(ErrorType::InvalidInput);
				}
				member_id = m_id.unwrap();
				users = room.get_room_users();
			}

			let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);

			if let Some(dst) = msg_req.dst() {
				for i in 0..dst.len() {
					dst_vec.push(dst.get(i));
				}
			}
			let dst = Some(builder.create_vector(&dst_vec));

			let mut npid = None;
			if (msg_req.option() & 0x01) != 0 {
				npid = Some(builder.create_string(&self.client_info.npid));
			}
			let mut online_name = None;
			if (msg_req.option() & 0x02) != 0 {
				online_name = Some(builder.create_string(&self.client_info.online_name));
			}
			let mut avatar_url = None;
			if (msg_req.option() & 0x04) != 0 {
				avatar_url = Some(builder.create_string(&self.client_info.avatar_url));
			}

			let src_user_info = UserInfo2::create(
				&mut builder,
				&UserInfo2Args {
					npId: npid,
					onlineName: online_name,
					avatarUrl: avatar_url,
				},
			);

			let msg_vec: Vec<u8>;
			if let Some(msg) = msg_req.msg() {
				msg_vec = msg.iter().collect();
			} else {
				msg_vec = Vec::new();
			}
			let msg = Some(builder.create_vector(&msg_vec));

			let resp = RoomMessageInfo::create(
				&mut builder,
				&RoomMessageInfoArgs {
					filtered: false,
					castType: msg_req.castType(),
					dst,
					srcMember: Some(src_user_info),
					msg,
				},
			);
			builder.finish(resp, None);
			let finished_data = builder.finished_data().to_vec();

			let mut n_msg: Vec<u8> = Vec::new();
			n_msg.extend(&room_id.to_le_bytes());
			n_msg.extend(&member_id.to_le_bytes());
			Client::add_data_packet(&mut n_msg, &finished_data);
			notif = Client::create_notification(NotificationType::RoomMessageReceived, &n_msg);
		}

		match msg_req.castType() {
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
