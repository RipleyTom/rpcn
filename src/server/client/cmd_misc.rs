use crate::server::client::*;

const SCE_NP_BASIC_PRESENCE_TITLE_SIZE_MAX: usize = 128;
const SCE_NP_BASIC_PRESENCE_EXTENDED_STATUS_SIZE_MAX: usize = 192;
const SCE_NP_BASIC_PRESENCE_COMMENT_SIZE_MAX: usize = 64;
const SCE_NP_BASIC_MAX_PRESENCE_SIZE: usize = 128;

#[allow(dead_code)]
#[allow(non_camel_case_types)]
#[repr(u16)]
enum MessageMainType {
	SCE_NP_BASIC_MESSAGE_MAIN_TYPE_DATA_ATTACHMENT = 0,
	SCE_NP_BASIC_MESSAGE_MAIN_TYPE_GENERAL = 1,
	SCE_NP_BASIC_MESSAGE_MAIN_TYPE_ADD_FRIEND = 2,
	SCE_NP_BASIC_MESSAGE_MAIN_TYPE_INVITE = 3,
}

impl Client {
	pub async fn req_signaling_infos(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let npid = data.get_string(false);
		if data.error() || npid.len() > 16 {
			warn!("Error while extracting data from RequestSignalingInfos command");
			return Err(ErrorType::Malformed);
		}

		let user_id = Database::new(self.get_database_connection()?).get_user_id(&npid);
		if user_id.is_err() {
			return Ok(ErrorType::NotFound);
		}
		let user_id = user_id.unwrap();

		let caller_ip;
		let caller_port;
		{
			let client_infos = self.shared.client_infos.read();
			{
				let si = client_infos.get(&self.client_info.user_id).unwrap().signaling_info.read();
				caller_ip = si.addr_p2p;
				caller_port = si.port_p2p;
			}

			if let Some(client_info) = client_infos.get(&user_id) {
				let client_si = client_info.signaling_info.read();
				if caller_ip == client_si.addr_p2p {
					reply.extend(&client_si.local_addr_p2p);
					reply.extend(&3658u16.to_le_bytes());
					info!("Requesting signaling infos for {} => (local) {:?}:{}", &npid, &client_si.local_addr_p2p, 3658);
					// Don't send notification for local addresses as connectivity shouldn't be an issue
					return Ok(ErrorType::NoError);
				} else {
					reply.extend(&client_si.addr_p2p);
					reply.extend(&((client_si.port_p2p).to_le_bytes()));
					info!("Requesting signaling infos for {} => (extern) {:?}:{}", &npid, &client_si.addr_p2p, client_si.port_p2p);
				}
			} else {
				return Ok(ErrorType::NotFound);
			}
		}

		// Send a notification to the target to help with connectivity
		let mut n_msg: Vec<u8> = Vec::with_capacity(std::mem::size_of::<u16>() + std::mem::size_of::<u32>());
		n_msg.extend(&caller_ip);
		n_msg.extend(&caller_port.to_le_bytes());
		n_msg.extend(self.client_info.npid.as_bytes());
		n_msg.push(0);
		let friend_n = Client::create_notification(NotificationType::SignalingInfo, &n_msg);
		self.send_single_notification(&friend_n, user_id).await;
		Ok(ErrorType::NoError)
	}

	pub fn req_ticket(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let service_id = data.get_string(false);
		let cookie = data.get_rawdata();

		if data.error() {
			warn!("Error while extracting data from RequestTicket command");
			return Err(ErrorType::Malformed);
		}

		info!("Requested a ticket for <{}>", service_id);

		if let Some(ref mut cur_service_id) = self.current_game.1 {
			if *cur_service_id != service_id {
				self.shared.game_tracker.decrease_count_ticket(cur_service_id);
				*cur_service_id = service_id.clone();
				self.shared.game_tracker.increase_count_ticket(&service_id);
			}
		} else {
			self.current_game.1 = Some(service_id.clone());
			self.shared.game_tracker.increase_count_ticket(&service_id);
		}

		let ticket;
		{
			let config = self.config.read();
			let sign_info = config.get_ticket_signing_info();
			ticket = Ticket::new(self.client_info.user_id as u64, &self.client_info.npid, &service_id, cookie, sign_info);
		}
		let ticket_blob = ticket.generate_blob();

		Client::add_data_packet(reply, &ticket_blob);

		Ok(ErrorType::NoError)
	}

	pub async fn send_message(&mut self, data: &mut StreamExtractor) -> Result<ErrorType, ErrorType> {
		let sendmessage_req = data.get_flatbuffer::<SendMessageRequest>();
		if data.error() || sendmessage_req.is_err() {
			warn!("Error while extracting data from SendMessage command");
			return Err(ErrorType::Malformed);
		}
		let sendmessage_req = sendmessage_req.unwrap();

		let message = Client::validate_and_unwrap(sendmessage_req.message())?;
		let npids = Client::validate_and_unwrap(sendmessage_req.npids())?;

		// Get all the IDs
		let mut ids = HashSet::new();
		{
			let db = Database::new(self.get_database_connection()?);
			for npid in &npids {
				match db.get_user_id(npid) {
					Ok(id) => {
						ids.insert(id);
					}
					Err(_) => {
						warn!("Requested to send a message to invalid npid: {}", npid);
						return Ok(ErrorType::InvalidInput);
					}
				}
			}
		}

		// Can't message self
		if ids.is_empty() || ids.contains(&self.client_info.user_id) {
			warn!("Requested to send a message to empty set or self!");
			return Ok(ErrorType::InvalidInput);
		}

		// Ensure all recipients are friends for invite messages(is this necessary?)
		let msg = flatbuffers::root::<MessageDetails>(message.bytes()).map_err(|e| {
			warn!("MessageDetails was malformed: {}", e);
			ErrorType::Malformed
		})?;
		if msg.mainType() == MessageMainType::SCE_NP_BASIC_MESSAGE_MAIN_TYPE_INVITE as u16 {
			let client_infos = self.shared.client_infos.read();
			let client_fi = client_infos.get(&self.client_info.user_id).unwrap().friend_info.read();
			if !client_fi.friends.keys().copied().collect::<HashSet<i64>>().is_superset(&ids) {
				warn!("Requested to send a message to a non-friend!");
				return Ok(ErrorType::InvalidInput);
			}
		}

		// Finally send the notifications
		let mut n_msg: Vec<u8> = Vec::new();
		n_msg.extend(self.client_info.npid.as_bytes());
		n_msg.push(0);
		n_msg.extend(&(message.len() as u32).to_le_bytes());
		n_msg.extend(message);
		let notif = Client::create_notification(NotificationType::MessageReceived, &n_msg);
		self.send_notification(&notif, &ids).await;

		Ok(ErrorType::NoError)
	}

	pub fn get_network_time(&self, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		reply.extend(Client::get_psn_timestamp().to_le_bytes());
		Ok(ErrorType::NoError)
	}

	pub async fn clear_presence(&self) -> Result<ErrorType, ErrorType> {
		let notify: Option<HashSet<i64>>;
		{
			let client_infos = self.shared.client_infos.read();
			let mut friend_info = client_infos.get(&self.client_info.user_id).unwrap().friend_info.write();
			if friend_info.presence.is_some() {
				notify = Some(friend_info.friends.keys().copied().collect());
				friend_info.presence = None;
			} else {
				notify = None;
			}
		}

		if let Some(user_ids) = notify {
			let mut n_msg: Vec<u8> = Vec::new();
			n_msg.extend(self.client_info.npid.as_bytes());
			n_msg.push(0);
			ClientSharedPresence::dump_empty(&mut n_msg);
			let notif = Client::create_notification(NotificationType::FriendPresenceChanged, &n_msg);
			self.send_notification(&notif, &user_ids).await;
		}

		Ok(ErrorType::NoError)
	}

	pub async fn set_presence(&mut self, data: &mut StreamExtractor) -> Result<ErrorType, ErrorType> {
		let (com_id, pr_req) = self.get_com_and_fb::<SetPresenceRequest>(data)?;
		let title = Client::validate_and_unwrap(pr_req.title())?;
		let title_id = Client::validate_and_unwrap(pr_req.title_id())?;

		let db = Database::new(self.get_database_connection()?);

		if db.update_game_list(&com_id, title_id, title).is_err() {
			error!("Unexpected error updating game list");
		}

		let notify: Option<(ClientSharedPresence, HashSet<i64>)>;

		{
			let client_infos = self.shared.client_infos.read();
			let mut friend_info = client_infos.get(&self.client_info.user_id).unwrap().friend_info.write();

			if let Some(ref mut presence_info) = friend_info.presence {
				if presence_info.communication_id != com_id
					|| presence_info.title != title
					|| pr_req.status().is_some_and(|status| status != presence_info.status)
					|| pr_req.comment().is_some_and(|comment| comment != presence_info.comment)
					|| pr_req.data().is_some_and(|data| data.bytes() != presence_info.data)
				{
					presence_info.communication_id = com_id;
					presence_info.title = title.to_string();
					presence_info.title.truncate(SCE_NP_BASIC_PRESENCE_TITLE_SIZE_MAX - 1);

					if let Some(status) = pr_req.status() {
						presence_info.status = status.to_string();
						presence_info.status.truncate(SCE_NP_BASIC_PRESENCE_EXTENDED_STATUS_SIZE_MAX - 1);
					}

					if let Some(comment) = pr_req.comment() {
						presence_info.comment = comment.to_string();
						presence_info.status.truncate(SCE_NP_BASIC_PRESENCE_COMMENT_SIZE_MAX - 1);
					}

					if let Some(data) = pr_req.data() {
						presence_info.data = data.bytes().to_vec();
						presence_info.data.truncate(SCE_NP_BASIC_MAX_PRESENCE_SIZE);
					}

					notify = Some((presence_info.clone(), friend_info.friends.keys().copied().collect()));
				} else {
					notify = None;
				}
			} else {
				friend_info.presence = Some(ClientSharedPresence::new(com_id, title, pr_req.status(), pr_req.comment(), pr_req.data().map(|v| v.bytes())));
				notify = None;
			}
		}

		if let Some((presence, user_ids)) = notify {
			let mut n_msg: Vec<u8> = Vec::new();
			n_msg.extend(self.client_info.npid.as_bytes());
			n_msg.push(0);
			presence.dump(&mut n_msg);
			let notif = Client::create_notification(NotificationType::FriendPresenceChanged, &n_msg);
			self.send_notification(&notif, &user_ids).await;
		}

		Ok(ErrorType::NoError)
	}
}
