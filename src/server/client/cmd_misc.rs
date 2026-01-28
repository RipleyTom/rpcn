use std::io::Cursor;

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

		let (caller_sig_infos, op_sig_infos);
		{
			let client_infos = self.shared.client_infos.read();

			caller_sig_infos = client_infos.get(&self.client_info.user_id).unwrap().signaling_info.read().clone();

			let client_info = client_infos.get(&user_id);
			if client_info.is_none() {
				return Ok(ErrorType::NotFound);
			}
			let client_info = client_info.unwrap();

			op_sig_infos = client_info.signaling_info.read().clone();

			if caller_sig_infos.addr_p2p_ipv4.0 == op_sig_infos.addr_p2p_ipv4.0 {
				Client::add_data_packet(reply, &Client::build_signaling_addr(&op_sig_infos.local_addr_p2p, 3658));
				info!("Requesting signaling infos for {} => (local) {:?}:{}", &npid, &op_sig_infos.local_addr_p2p, 3658);
				// Don't send notification for local addresses as connectivity shouldn't be an issue
				return Ok(ErrorType::NoError);
			} else {
				match (caller_sig_infos.addr_p2p_ipv6, op_sig_infos.addr_p2p_ipv6) {
					(Some(_), Some((op_ipv6, op_port_ipv6))) => {
						Client::add_data_packet(reply, &Client::build_signaling_addr(&op_ipv6, op_port_ipv6));
						info!("Requesting signaling infos for {} => (extern ipv6) {:?}:{}", &npid, &op_ipv6, op_port_ipv6);
					}
					_ => {
						Client::add_data_packet(reply, &Client::build_signaling_addr(&op_sig_infos.addr_p2p_ipv4.0, op_sig_infos.addr_p2p_ipv4.1));
						info!(
							"Requesting signaling infos for {} => (extern ipv4) {:?}:{}",
							&npid, &op_sig_infos.addr_p2p_ipv4.0, op_sig_infos.addr_p2p_ipv4.1
						);
					}
				}
			}
		}

		// Send a notification to the target to help with connectivity
		let npid_str = self.client_info.npid.clone();
		let addr = match (caller_sig_infos.addr_p2p_ipv6, op_sig_infos.addr_p2p_ipv6) {
			(Some((caller_ipv6, caller_port_ipv6)), Some(_)) => Client::make_signaling_addr(&caller_ipv6, caller_port_ipv6),
			_ => Client::make_signaling_addr(&caller_sig_infos.addr_p2p_ipv4.0, caller_sig_infos.addr_p2p_ipv4.1),
		};

		let matching_info = MatchingSignalingInfo { npid: npid_str, addr: Some(addr) };

		let vec_notif = matching_info.encode_to_vec();

		let mut n_msg: Vec<u8> = Vec::with_capacity(size_of::<u32>() + vec_notif.len());
		Client::add_data_packet(&mut n_msg, &vec_notif);
		let friend_n = Client::create_notification(NotificationType::SignalingHelper, &n_msg);
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
		let sendmessage_req = data.get_protobuf::<SendMessageRequest>();
		if data.error() || sendmessage_req.is_err() {
			warn!("Error while extracting data from SendMessage command");
			return Err(ErrorType::Malformed);
		}
		let sendmessage_req = sendmessage_req.unwrap();

		let message = &sendmessage_req.message;
		let npids = &sendmessage_req.npids;

		// Get all the IDs
		let mut ids = HashSet::new();
		{
			let db = Database::new(self.get_database_connection()?);
			for npid in npids {
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
		let msg: MessageDetails = prost::Message::decode(&mut Cursor::new(&message)).map_err(|e| {
			warn!("MessageDetails was malformed: {}", e);
			ErrorType::Malformed
		})?;
		if msg.main_type.map(|v| v.value).unwrap_or(0) == MessageMainType::SCE_NP_BASIC_MESSAGE_MAIN_TYPE_INVITE as u32 {
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

	pub async fn set_presence(&mut self, data: &mut StreamExtractor) -> Result<ErrorType, ErrorType> {
		let (com_id, pr_req) = self.get_com_and_pb::<SetPresenceRequest>(data)?;
		let title = pr_req.title.clone();

		let notify: Option<(ClientSharedPresence, HashSet<i64>)>;

		{
			let client_infos = self.shared.client_infos.read();
			let mut friend_info = client_infos.get(&self.client_info.user_id).unwrap().friend_info.write();

			if let Some(ref mut presence_info) = friend_info.presence {
				if presence_info.communication_id != com_id
					|| presence_info.title != title
					|| pr_req.status != presence_info.status
					|| pr_req.comment != presence_info.comment
					|| pr_req.data != presence_info.data
				{
					presence_info.communication_id = com_id;
					self.shared.game_tracker.add_gamename_hint(&com_id, &title);
					presence_info.title = title;
					presence_info.title.truncate(SCE_NP_BASIC_PRESENCE_TITLE_SIZE_MAX - 1);

					presence_info.status = pr_req.status;
					presence_info.status.truncate(SCE_NP_BASIC_PRESENCE_EXTENDED_STATUS_SIZE_MAX - 1);
					presence_info.comment = pr_req.comment;
					presence_info.comment.truncate(SCE_NP_BASIC_PRESENCE_COMMENT_SIZE_MAX - 1);
					presence_info.data = pr_req.data;
					presence_info.data.truncate(SCE_NP_BASIC_MAX_PRESENCE_SIZE);

					notify = Some((presence_info.clone(), friend_info.friends.keys().copied().collect()));
				} else {
					notify = None;
				}
			} else {
				let new_presence = ClientSharedPresence::new(com_id, &title, &pr_req.status, &pr_req.comment, &pr_req.data);
				friend_info.presence = Some(new_presence.clone());
				notify = Some((new_presence, friend_info.friends.keys().copied().collect()));
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

	pub async fn reset_state(&mut self) -> Result<ErrorType, ErrorType> {
		Client::clean_user_state(self).await;
		Ok(ErrorType::NoError)
	}
}
