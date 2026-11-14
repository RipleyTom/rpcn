// Lobby Commands

use parking_lot::RwLockUpgradableReadGuard;
use prost::Message;

use crate::server::client::*;
use crate::server::lobby_manager::{validate_chat_message, validate_invitation};

impl Client {
	fn create_lobby_notification(n_type: NotificationType, notif_data: &Vec<u8>) -> Vec<u8> {
		let mut n_msg: Vec<u8> = Vec::with_capacity(notif_data.len() + size_of::<u32>());
		Client::add_data_packet(&mut n_msg, notif_data);
		Client::create_notification(n_type, &n_msg)
	}

	// Same option flags as room messages: SCE_NP_MATCHING2_SEND_MSG_OPTION_WITH_NPID/ONLINENAME/AVATARURL
	fn get_message_src_user_info(&self, option: u32) -> UserInfo {
		UserInfo {
			np_id: if (option & 0x01) != 0 { self.client_info.npid.clone() } else { String::new() },
			online_name: if (option & 0x02) != 0 { self.client_info.online_name.clone() } else { String::new() },
			avatar_url: if (option & 0x04) != 0 { self.client_info.avatar_url.clone() } else { String::new() },
		}
	}

	pub fn req_get_lobby_info_list(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let (com_id, req) = self.get_com_and_pb::<GetLobbyInfoListRequest>(data)?;

		// Like servers and worlds, titles not in servers.cfg get default lobbies
		let (create_missing, default_lobbies) = {
			let config = self.config.read();
			(config.is_create_missing(), config.get_default_lobbies())
		};

		// The game_infos lock is released before lobby_manager is locked so the two are never nested
		let lobbies = {
			let game_infos = self.shared.game_infos.upgradable_read();
			match game_infos.get_lobby_list(&com_id, req.world_id) {
				Some(lobbies) if lobbies.is_empty() && create_missing && default_lobbies != 0 => RwLockUpgradableReadGuard::upgrade(game_infos)
					.create_default_lobbies(&com_id, req.world_id, default_lobbies)
					.unwrap_or_default(),
				Some(lobbies) => lobbies,
				None => {
					warn!("Attempted to query lobby list on a non-existing world({}:{})", com_id_to_string(&com_id), req.world_id);
					Vec::new()
				}
			}
		};

		if lobbies.is_empty() {
			warn!("No lobbies exist for comId {} world {}", com_id_to_string(&com_id), req.world_id);
		}

		let resp = self.shared.lobby_manager.read().get_lobby_info_list(&com_id, req.world_id, &lobbies, &req);
		Client::add_data_packet(reply, &resp.encode_to_vec());
		Ok(ErrorType::NoError)
	}

	pub async fn req_join_lobby(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let (com_id, req) = self.get_com_and_pb::<JoinLobbyRequest>(data)?;

		let location = self.shared.game_infos.read().get_lobby_location(&com_id, req.lobby_id);
		let Some((server_id, world_id)) = location else {
			warn!("User attempted to join an unknown lobby {} for comId {}", req.lobby_id, com_id_to_string(&com_id));
			return Ok(ErrorType::RoomMissing);
		};

		let res = self.shared.lobby_manager.write().join_lobby(&com_id, server_id, world_id, &req, &self.client_info);
		let (resp, users, notif_data) = match res {
			Err(ErrorType::Malformed) => return Err(ErrorType::Malformed),
			Err(e) => return Ok(e),
			Ok(res) => res,
		};

		Client::add_data_packet(reply, &resp);

		let notif = Client::create_lobby_notification(NotificationType::MemberJoinedLobby, &notif_data);
		self.send_notification(&notif, &users).await;

		Ok(ErrorType::NoError)
	}

	pub async fn leave_lobby(&self, com_id: &ComId, lobby_id: u64, opt_data: Option<&PresenceOptionData>, event_cause: EventCause) -> ErrorType {
		let res = self.shared.lobby_manager.write().leave_lobby(com_id, lobby_id, self.client_info.user_id, event_cause, opt_data);

		match res {
			Ok((users, notif_data)) => {
				let notif = Client::create_lobby_notification(NotificationType::MemberLeftLobby, &notif_data);
				self.send_notification(&notif, &users).await;
				ErrorType::NoError
			}
			Err(e) => e,
		}
	}

	pub async fn req_leave_lobby(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let (com_id, req) = self.get_com_and_pb::<LeaveLobbyRequest>(data)?;

		let res = self.leave_lobby(&com_id, req.lobby_id, req.opt_data.as_ref(), EventCause::LeaveAction).await;
		if let ErrorType::Malformed = res {
			return Err(ErrorType::Malformed);
		}

		reply.extend(&req.lobby_id.to_le_bytes());
		Ok(res)
	}

	pub async fn req_set_lobby_member_data_internal(&mut self, data: &mut StreamExtractor) -> Result<ErrorType, ErrorType> {
		let (com_id, req) = self.get_com_and_pb::<SetLobbyMemberDataInternalRequest>(data)?;

		let res = self.shared.lobby_manager.write().set_lobby_member_data_internal(&com_id, &req, self.client_info.user_id);
		let (users, notif_data) = match res {
			Err(ErrorType::Malformed) => return Err(ErrorType::Malformed),
			Err(e) => return Ok(e),
			Ok(res) => res,
		};

		let notif = Client::create_lobby_notification(NotificationType::UpdatedLobbyMemberDataInternal, &notif_data);
		self.send_notification(&notif, &users).await;
		self.self_notification(&notif);

		Ok(ErrorType::NoError)
	}

	pub fn req_get_lobby_member_data_internal_list(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let (com_id, req) = self.get_com_and_pb::<GetLobbyMemberDataInternalListRequest>(data)?;

		let res = self.shared.lobby_manager.read().get_lobby_member_data_internal_list(&com_id, &req, self.client_info.user_id);
		match res {
			Err(e) => Ok(e),
			Ok(resp) => {
				Client::add_data_packet(reply, &resp);
				Ok(ErrorType::NoError)
			}
		}
	}

	pub async fn req_send_lobby_chat_message(&mut self, data: &mut StreamExtractor) -> Result<ErrorType, ErrorType> {
		let (com_id, req) = self.get_com_and_pb::<SendLobbyMessageRequest>(data)?;
		validate_chat_message(&req.msg)?;

		let res = self
			.shared
			.lobby_manager
			.read()
			.get_message_targets(&com_id, req.lobby_id, self.client_info.user_id, req.cast_type, &req.dst);
		let targets = match res {
			Err(e) => return Ok(e),
			Ok(targets) => targets,
		};

		let notif_data = NotificationLobbyMessage {
			lobby_id: req.lobby_id,
			src_member_id: targets.src_member_id as u32,
			filtered: false,
			cast_type: req.cast_type,
			dst: req.dst.clone(),
			src_member: Some(self.get_message_src_user_info(req.option)),
			msg: req.msg.clone(),
		}
		.encode_to_vec();

		let notif = Client::create_lobby_notification(NotificationType::LobbyMessageReceived, &notif_data);
		self.send_notification(&notif, &targets.users).await;
		if targets.include_self {
			self.self_notification(&notif);
		}

		Ok(ErrorType::NoError)
	}

	pub async fn req_send_lobby_invitation(&mut self, data: &mut StreamExtractor) -> Result<ErrorType, ErrorType> {
		let (com_id, req) = self.get_com_and_pb::<SendLobbyInvitationRequest>(data)?;
		validate_invitation(&req.target_session, &req.opt_data)?;

		let res = self
			.shared
			.lobby_manager
			.read()
			.get_message_targets(&com_id, req.lobby_id, self.client_info.user_id, req.cast_type, &req.dst);
		let targets = match res {
			Err(e) => return Ok(e),
			Ok(targets) => targets,
		};

		let notif_data = NotificationLobbyInvitation {
			lobby_id: req.lobby_id,
			src_member_id: targets.src_member_id as u32,
			cast_type: req.cast_type,
			dst: req.dst.clone(),
			src_member: Some(self.get_message_src_user_info(req.option)),
			target_session: req.target_session.clone(),
			opt_data: req.opt_data.clone(),
		}
		.encode_to_vec();

		let notif = Client::create_lobby_notification(NotificationType::LobbyInvitationReceived, &notif_data);
		self.send_notification(&notif, &targets.users).await;
		if targets.include_self {
			self.self_notification(&notif);
		}

		Ok(ErrorType::NoError)
	}
}
