// Friend/Block Commands

use crate::server::client::*;
use crate::server::client::notifications::NotificationType;
use crate::server::database::{DbError, FriendStatus};

impl Client {
	pub async fn add_friend(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<(), ()> {
		let friend_npid = data.get_string(false);
		if data.error() {
			warn!("Error while extracting data from AddFriend command");
			reply.push(ErrorType::Malformed as u8);
			return Err(());
		}

		let (friend_user_id, status_friend);
		{
			let mut db_lock = self.db.lock();

			let friend_user_id_res = db_lock.get_user_id(&friend_npid);
			if friend_user_id_res.is_err() {
				reply.push(ErrorType::NotFound as u8);
				return Ok(());
			}

			friend_user_id = friend_user_id_res.unwrap();
			if friend_user_id == self.client_info.user_id {
				reply.push(ErrorType::InvalidInput as u8);
				return Ok(());
			}

			let rel_status = db_lock.get_rel_status(self.client_info.user_id, friend_user_id);
			match rel_status {
				Ok((mut status_user, s_friend)) => {
					status_friend = s_friend;
					// If there is a block, either from current user or person added as friend, query is invalid
					if (status_user & (FriendStatus::Blocked as u16)) != 0 || (status_friend & (FriendStatus::Blocked as u16)) != 0 {
						reply.push(ErrorType::Blocked as u8);
						return Ok(());
					}
					// If user has already requested friendship or is a friend
					if (status_user & (FriendStatus::Friend as u16)) != 0 {
						reply.push(ErrorType::AlreadyFriend as u8);
						return Ok(());
					}
					// Finally just add friendship flag!
					status_user |= FriendStatus::Friend as u16;
					if let Err(e) = db_lock.set_rel_status(self.client_info.user_id, friend_user_id, status_user, status_friend) {
						error!("Unexpected error happened setting relationship status with preexisting status: {:?}", e);
						return Err(());
					}
				}
				Err(DbError::Empty) => {
					status_friend = 0;
					if let Err(e) = db_lock.set_rel_status(self.client_info.user_id, friend_user_id, FriendStatus::Friend as u16, 0) {
						error!("Unexpected error happened creating relationship status: {:?}", e);
						return Err(());
					}
				}
				Err(e) => {
					error!("Unexpected result querying relationship status: {:?}", e);
					return Err(());
				}
			}
		}

		// Send notifications as needed
		if (status_friend & FriendStatus::Friend as u16) != 0 {
			// If user accepted friend request from friend send FriendNew notification
			// Update signaling infos
			let friend_online: bool;
			{
				let mut sign_infos = self.signaling_infos.write();
				if let Some(user) = sign_infos.get_mut(&self.client_info.user_id) {
					user.friends.insert(friend_user_id);
				}
				if let Some(other_user) = sign_infos.get_mut(&friend_user_id) {
					other_user.friends.insert(self.client_info.user_id);
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

		reply.push(ErrorType::NoError as u8);
		return Ok(());
	}

	pub async fn remove_friend(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<(), ()> {
		let friend_npid = data.get_string(false);
		if data.error() {
			warn!("Error while extracting data from RemoveFriend command");
			reply.push(ErrorType::Malformed as u8);
			return Err(());
		}

		let friend_user_id;
		{
			let mut db_lock = self.db.lock();

			let friend_user_id_res = db_lock.get_user_id(&friend_npid);
			if friend_user_id_res.is_err() {
				reply.push(ErrorType::NotFound as u8);
				return Ok(());
			}

			friend_user_id = friend_user_id_res.unwrap();
			if friend_user_id == self.client_info.user_id {
				reply.push(ErrorType::InvalidInput as u8);
				return Ok(());
			}

			let rel_status = db_lock.get_rel_status(self.client_info.user_id, friend_user_id);
			match rel_status {
				Ok((mut status_user, mut status_friend)) => {
					// Check that some friendship relationship exist
					if ((status_user & (FriendStatus::Friend as u16)) | (status_friend & (FriendStatus::Friend as u16))) == 0 {
						reply.push(ErrorType::NotFound as u8);
						return Ok(());
					}
					// Remove friendship flag from relationship
					status_user &= !(FriendStatus::Friend as u16);
					status_friend &= !(FriendStatus::Friend as u16);
					if let Err(e) = db_lock.set_rel_status(self.client_info.user_id, friend_user_id, status_user, status_friend) {
						error!("Unexpected error happened setting relationship status with preexisting status: {:?}", e);
						return Err(());
					}
				}
				Err(DbError::Empty) => {
					reply.push(ErrorType::NotFound as u8);
					return Ok(());
				}
				Err(e) => {
					error!("Unexpected result querying relationship status: {:?}", e);
					return Err(());
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
		let mut sign_infos = self.signaling_infos.write();
		if let Some(user) = sign_infos.get_mut(&self.client_info.user_id) {
			user.friends.remove(&friend_user_id);
		}
		if let Some(other_user) = sign_infos.get_mut(&friend_user_id) {
			other_user.friends.remove(&self.client_info.user_id);
		}

		reply.push(ErrorType::NoError as u8);
		return Ok(());
	}

	pub fn add_block(&mut self, _data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<(), ()> {
		// TODO
		reply.push(ErrorType::NoError as u8);
		Ok(())
	}

	pub fn remove_block(&mut self, _data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<(), ()> {
		// TODO
		reply.push(ErrorType::NoError as u8);
		Ok(())
	}
}
