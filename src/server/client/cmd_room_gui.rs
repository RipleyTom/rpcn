// Room Commands for old GUI api

use crate::server::{client::*, gui_room_manager::GuiRoomId};

impl Client {
	pub fn req_create_room_gui(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let (com_id, create_req) = self.get_com_and_fb::<CreateRoomGUIRequest>(data)?;

		let resp = self.shared.gui_room_manager.write().create_room_gui(&com_id, &create_req, &self.client_info);
		Client::add_data_packet(reply, &resp);
		Ok(ErrorType::NoError)
	}

	pub async fn req_join_room_gui(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let join_req = self.get_fb::<MatchingGuiRoomId>(data)?;

		let room_id = GuiRoomManager::fb_vec_to_room_id(join_req.id())?;
		let res = self.shared.gui_room_manager.write().join_room_gui(&room_id, &self.client_info);
		match res {
			Ok((resp, notif_users, notif_data)) => {
				let mut n_msg: Vec<u8> = Vec::with_capacity(notif_data.len() + size_of::<u32>());
				Client::add_data_packet(&mut n_msg, &notif_data);
				let notif = Client::create_notification(NotificationType::MemberJoinedRoomGUI, &n_msg);
				self.send_notification(&notif, &notif_users).await;

				Client::add_data_packet(reply, &resp);
				Ok(ErrorType::NoError)
			}
			Err(e) => Ok(e),
		}
	}

	pub async fn leave_room_gui(&mut self, room_id: &GuiRoomId) -> Result<Vec<u8>, ErrorType> {
		let (resp, notif_ids, notifs) = self.shared.gui_room_manager.write().leave_room_gui(room_id, &self.client_info)?;

		for (notif_type, notif_data) in &notifs {
			let mut n_msg: Vec<u8> = Vec::with_capacity(notif_data.len() + size_of::<u32>());
			Client::add_data_packet(&mut n_msg, &notif_data);
			let notif_vec = Client::create_notification(*notif_type, &n_msg);
			self.send_notification(&notif_vec, &notif_ids).await;
		}

		Ok(resp)
	}

	pub async fn req_leave_room_gui(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let leave_req = self.get_fb::<MatchingGuiRoomId>(data)?;
		let room_id = GuiRoomManager::fb_vec_to_room_id(leave_req.id())?;

		match self.leave_room_gui(&room_id).await {
			Ok(data) => Client::add_data_packet(reply, &data),
			Err(e) => return Ok(e),
		}

		Ok(ErrorType::NoError)
	}

	pub fn req_get_room_list_gui(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let (com_id, search_req) = self.get_com_and_fb::<GetRoomListGUIRequest>(data)?;

		let resp = self.shared.gui_room_manager.read().get_room_list_gui(&com_id, &search_req);
		Client::add_data_packet(reply, &resp);
		Ok(ErrorType::NoError)
	}

	pub fn req_set_room_search_flag_gui(&mut self, data: &mut StreamExtractor) -> Result<ErrorType, ErrorType> {
		let set_req = self.get_fb::<SetRoomSearchFlagGUI>(data)?;
		let room_id = GuiRoomManager::fb_vec_to_room_id(set_req.roomid())?;
		let stealth = set_req.stealth();

		match self.shared.gui_room_manager.write().set_search_flag(&room_id, stealth, self.client_info.user_id) {
			Ok(()) => Ok(ErrorType::NoError),
			Err(e) => Ok(e),
		}
	}

	pub fn req_get_room_search_flag_gui(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let get_req = self.get_fb::<MatchingGuiRoomId>(data)?;
		let room_id = GuiRoomManager::fb_vec_to_room_id(get_req.id())?;

		match self.shared.gui_room_manager.read().get_search_flag(&room_id) {
			Ok(resp) => {
				Client::add_data_packet(reply, &resp);
				Ok(ErrorType::NoError)
			}
			Err(e) => Ok(e),
		}
	}

	pub fn req_set_room_info_gui(&mut self, data: &mut StreamExtractor) -> Result<ErrorType, ErrorType> {
		let set_req = self.get_fb::<MatchingRoom>(data)?;
		let room_id = GuiRoomManager::fb_vec_to_room_id(set_req.id())?;
		let attrs = Client::validate_and_unwrap(set_req.attr())?;

		match self.shared.gui_room_manager.write().set_room_info_gui(&room_id, attrs, self.client_info.user_id) {
			Ok(()) => Ok(ErrorType::NoError),
			Err(e) => Ok(e),
		}
	}

	pub fn req_get_room_info_gui(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let get_req = self.get_fb::<MatchingRoom>(data)?;
		let room_id = GuiRoomManager::fb_vec_to_room_id(get_req.id())?;
		let attrs = Client::validate_and_unwrap(get_req.attr())?;

		match self.shared.gui_room_manager.read().get_room_info_gui(&room_id, attrs) {
			Ok(resp) => {
				Client::add_data_packet(reply, &resp);
				Ok(ErrorType::NoError)
			}
			Err(e) => Ok(e),
		}
	}

	pub async fn req_quickmatch_gui(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let (com_id, qm_req) = self.get_com_and_fb::<QuickMatchGUIRequest>(data)?;

		let (resp, notif) = self.shared.gui_room_manager.write().quickmatch_gui(&com_id, &qm_req, &self.client_info);

		if let Some((client_ids, notification)) = notif {
			let mut n_msg: Vec<u8> = Vec::with_capacity(notification.len() + size_of::<u32>());
			Client::add_data_packet(&mut n_msg, &notification);
			let notif_vec = Client::create_notification(NotificationType::QuickMatchCompleteGUI, &n_msg);
			self.send_notification(&notif_vec, &client_ids).await;
			self.self_notification(&notif_vec);
		}

		Client::add_data_packet(reply, &resp);
		Ok(ErrorType::NoError)
	}

	pub async fn req_searchjoin_gui(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let (com_id, sj_req) = self.get_com_and_fb::<SearchJoinRoomGUIRequest>(data)?;

		let res = self.shared.gui_room_manager.write().search_join_gui(&com_id, &sj_req, &self.client_info);
		match res {
			Ok((resp, notif_users, notif_data)) => {
				let mut n_msg: Vec<u8> = Vec::with_capacity(notif_data.len() + size_of::<u32>());
				Client::add_data_packet(&mut n_msg, &notif_data);
				let notif = Client::create_notification(NotificationType::MemberJoinedRoomGUI, &n_msg);
				self.send_notification(&notif, &notif_users).await;

				Client::add_data_packet(reply, &resp);
				Ok(ErrorType::NoError)
			}
			Err(e) => Ok(e),
		}
	}
}
