// Friend/Block Commands

use crate::server::client::notifications::NotificationType;
use crate::server::client::*;
use crate::server::database::{DbError, FriendStatus};

impl Client {
	pub async fn add_friend(&mut self, data: &mut StreamExtractor) -> Result<ErrorType, ErrorType> {
		let friend_npid = data.get_string(false);
		if data.error() {
			warn!("Error while extracting data from AddFriend command");
			return Err(ErrorType::Malformed);
		}

		let (friend_user_id, status_friend);
		{
			let db = Database::new(self.get_database_connection()?);

			let friend_user_id_res = db.get_user_id(&friend_npid);
			if friend_user_id_res.is_err() {
				return Ok(ErrorType::NotFound);
			}

			friend_user_id = friend_user_id_res.unwrap();
			if friend_user_id == self.client_info.user_id {
				return Ok(ErrorType::InvalidInput);
			}

			let rel_status = db.get_rel_status(self.client_info.user_id, friend_user_id);
			match rel_status {
				Ok((mut status_user, s_friend)) => {
					status_friend = s_friend;
					// If there is a block, either from current user or person added as friend, query is invalid
					if (status_user & (FriendStatus::Blocked as u8)) != 0 || (status_friend & (FriendStatus::Blocked as u8)) != 0 {
						return Ok(ErrorType::Blocked);
					}
					// If user has already requested friendship or is a friend
					if (status_user & (FriendStatus::Friend as u8)) != 0 {
						return Ok(ErrorType::AlreadyFriend);
					}
					// Finally just add friendship flag!
					status_user |= FriendStatus::Friend as u8;
					if let Err(e) = db.set_rel_status(self.client_info.user_id, friend_user_id, status_user, status_friend) {
						error!("Unexpected error happened setting relationship status with preexisting status: {:?}", e);
						return Err(ErrorType::DbFail);
					}
				}
				Err(DbError::Empty) => {
					status_friend = 0;
					if let Err(e) = db.set_rel_status(self.client_info.user_id, friend_user_id, FriendStatus::Friend as u8, 0) {
						error!("Unexpected error happened creating relationship status: {:?}", e);
						return Err(ErrorType::DbFail);
					}
				}
				Err(e) => {
					error!("Unexpected result querying relationship status: {:?}", e);
					return Err(ErrorType::DbFail);
				}
			}
		}

		// Send notifications as needed
		if (status_friend & FriendStatus::Friend as u8) != 0 {
			// If user accepted friend request from friend send FriendNew notification
			// Update signaling infos
			let friend_online: bool;
			{
				let client_infos = self.shared.client_infos.read();
				if let Some(user_info) = client_infos.get(&self.client_info.user_id) {
					let mut user_fi = user_info.friend_info.write();
					user_fi.friends.insert(friend_user_id, friend_npid.clone());
				}
				if let Some(other_user_info) = client_infos.get(&friend_user_id) {
					let mut other_user_fi = other_user_info.friend_info.write();
					other_user_fi.friends.insert(self.client_info.user_id, self.client_info.npid.clone());
					friend_online = true;
				} else {
					friend_online = false;
				}
			}

			// Send notification to user that he has a new friend!
			let self_n = Client::create_new_friend_notification(&friend_npid, friend_online);
			self.self_notification(&self_n);
			if friend_online {
				// Send notification to friend that he has a new friend!
				let friend_n = Client::create_new_friend_notification(&self.client_info.npid, true);
				self.send_single_notification(&friend_n, friend_user_id).await;
			}
		} else {
			// Else send friend the FriendQuery notification
			let mut n_msg: Vec<u8> = Vec::new();
			n_msg.extend(self.client_info.npid.as_bytes());
			n_msg.push(0);
			let friend_n = Client::create_notification(NotificationType::FriendQuery, &n_msg);
			self.send_single_notification(&friend_n, friend_user_id).await;
		}

		Ok(ErrorType::NoError)
	}

	pub async fn remove_friend(&mut self, data: &mut StreamExtractor) -> Result<ErrorType, ErrorType> {
		let friend_npid = data.get_string(false);
		if data.error() {
			warn!("Error while extracting data from RemoveFriend command");
			return Err(ErrorType::Malformed);
		}

		let friend_user_id;
		{
			let db = Database::new(self.get_database_connection()?);

			let friend_user_id_res = db.get_user_id(&friend_npid);
			if friend_user_id_res.is_err() {
				return Ok(ErrorType::NotFound);
			}

			friend_user_id = friend_user_id_res.unwrap();
			if friend_user_id == self.client_info.user_id {
				return Ok(ErrorType::InvalidInput);
			}

			let rel_status = db.get_rel_status(self.client_info.user_id, friend_user_id);
			match rel_status {
				Ok((mut status_user, mut status_friend)) => {
					// Check that some friendship relationship exist
					if ((status_user & (FriendStatus::Friend as u8)) | (status_friend & (FriendStatus::Friend as u8))) == 0 {
						return Ok(ErrorType::NotFound);
					}
					// Remove friendship flag from relationship
					status_user &= !(FriendStatus::Friend as u8);
					status_friend &= !(FriendStatus::Friend as u8);
					if let Err(e) = db.set_rel_status(self.client_info.user_id, friend_user_id, status_user, status_friend) {
						error!("Unexpected error happened setting relationship status with preexisting status: {:?}", e);
						return Err(ErrorType::DbFail);
					}
				}
				Err(DbError::Empty) => {
					return Ok(ErrorType::NotFound);
				}
				Err(e) => {
					error!("Unexpected result querying relationship status: {:?}", e);
					return Err(ErrorType::DbFail);
				}
			}
		}

		// Send notifications as needed
		// Send to user
		let mut n_msg: Vec<u8> = Vec::new();
		n_msg.extend(friend_npid.as_bytes());
		n_msg.push(0);
		let self_n = Client::create_notification(NotificationType::FriendLost, &n_msg);
		self.self_notification(&self_n);
		// Send to ex-friend too
		n_msg.clear();
		n_msg.extend(self.client_info.npid.as_bytes());
		n_msg.push(0);
		let friend_n = Client::create_notification(NotificationType::FriendLost, &n_msg);
		self.send_single_notification(&friend_n, friend_user_id).await;
		// Update signaling infos
		let client_infos = self.shared.client_infos.read();
		if let Some(client_info) = client_infos.get(&self.client_info.user_id) {
			let mut user_fi = client_info.friend_info.write();
			user_fi.friends.remove(&friend_user_id);
		}
		if let Some(other_user_info) = client_infos.get(&friend_user_id) {
			let mut other_user_fi = other_user_info.friend_info.write();
			other_user_fi.friends.remove(&self.client_info.user_id);
		}

		Ok(ErrorType::NoError)
	}

	pub fn add_block(&mut self, _data: &mut StreamExtractor, _reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		// TODO
		Ok(ErrorType::NoError)
	}

	pub fn remove_block(&mut self, _data: &mut StreamExtractor, _reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		// TODO
		Ok(ErrorType::NoError)
	}
}
