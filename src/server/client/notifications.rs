use crate::server::client::*;

#[repr(u16)]
pub enum NotificationType {
	UserJoinedRoom,
	UserLeftRoom,
	RoomDestroyed,
	UpdatedRoomDataInternal,
	UpdatedRoomMemberDataInternal,
	SignalP2PConnect,
	_SignalP2PDisconnect,
	FriendQuery,  // Other user sent a friend request
	FriendNew,    // Add a friend to the friendlist(either accepted a friend request or friend accepted it)
	FriendLost,   // Remove friend from the friendlist(user removed friend or friend removed friend)
	FriendStatus, // Set status of friend to Offline or Online
	RoomMessageReceived,
	MessageReceived,
	FriendPresenceChanged,
}

impl Client {
	pub fn create_notification(n_type: NotificationType, data: &[u8]) -> Vec<u8> {
		let final_size = data.len() + HEADER_SIZE as usize;

		let mut final_vec = Vec::with_capacity(final_size);
		final_vec.push(PacketType::Notification as u8);
		final_vec.extend(&(n_type as u16).to_le_bytes());
		final_vec.extend(&(final_size as u32).to_le_bytes());
		final_vec.extend(&0u64.to_le_bytes()); // packet_id doesn't matter for notifications
		final_vec.extend(data);

		final_vec
	}

	pub fn create_friend_status_notification(npid: &str, timestamp: u64, online: bool) -> Vec<u8> {
		let mut n_msg: Vec<u8> = Vec::new();
		n_msg.push(if online { 1 } else { 0 });
		n_msg.extend(&timestamp.to_le_bytes());
		n_msg.extend(npid.as_bytes());
		n_msg.push(0);
		Client::create_notification(NotificationType::FriendStatus, &n_msg)
	}

	pub fn create_new_friend_notification(npid: &str, online: bool) -> Vec<u8> {
		let mut n_msg: Vec<u8> = Vec::new();
		n_msg.push(if online { 1 } else { 0 });
		n_msg.extend(npid.as_bytes());
		n_msg.push(0);
		Client::create_notification(NotificationType::FriendNew, &n_msg)
	}

	pub async fn send_single_notification(&self, notif: &[u8], user_id: i64) {
		let channel_copy;
		{
			let client_infos = self.shared.client_infos.read();
			let client_info = client_infos.get(&user_id);
			if let Some(client_info) = client_info {
				channel_copy = client_info.channel.clone();
			} else {
				return;
			}
		}
		let _ = channel_copy.send(notif.to_vec()).await;
	}

	pub async fn send_notification(&self, notif: &[u8], user_list: &HashSet<i64>) {
		for user_id in user_list {
			self.send_single_notification(notif, *user_id).await;
		}
	}

	pub fn self_notification(&mut self, notif: &[u8]) {
		self.post_reply_notifications.push(notif.to_vec());
	}
}
