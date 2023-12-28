use crate::server::client::*;

const SCE_NP_BASIC_PRESENCE_TITLE_SIZE_MAX: usize = 128;
const SCE_NP_BASIC_PRESENCE_EXTENDED_STATUS_SIZE_MAX: usize = 192;
const SCE_NP_BASIC_PRESENCE_COMMENT_SIZE_MAX: usize = 64;
const SCE_NP_BASIC_MAX_PRESENCE_SIZE: usize = 128;

impl Client {
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
