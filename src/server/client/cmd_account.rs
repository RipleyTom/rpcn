// Account Management Commands

use crate::server::client::*;
use crate::server::database::DbError;

impl Client {
	fn send_token_mail(&self, email_addr: &str, npid: &str, token: &str) -> Result<(), lettre::smtp::error::Error> {
		// Send the email
		let email_to_send = EmailBuilder::new()
			.to((email_addr, npid))
			.from("np@rpcs3.net")
			.subject("Your token for RPCN")
			.text(format!("Your token for username {} is:\n{}", npid, token))
			.build()
			.unwrap();
		let (host, login, password) = self.config.read().get_email_auth();

		let mut smtp_client;
		if host.len() == 0 {
			smtp_client = SmtpClient::new_unencrypted_localhost().unwrap();
		} else {
			smtp_client = SmtpClient::new_simple(&host).unwrap();

			if login.len() != 0 {
				smtp_client = smtp_client
					.credentials(Credentials::new(login, password))
					.authentication_mechanism(Mechanism::Plain)
					.hello_name(lettre::smtp::extension::ClientId::new("np.rpcs3.net".to_string()));
			}
		}

		let mut mailer = SmtpTransport::new(smtp_client);

		mailer.send(email_to_send.into())?;
		Ok(())
	}

	pub async fn login(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<(), ()> {
		let login = data.get_string(false);
		let password = data.get_string(false);
		let token = data.get_string(true);
		let friend_userids: HashSet<i64>;

		if data.error() {
			warn!("Error while extracting data from Login command");
			reply.push(ErrorType::Malformed as u8);
			return Err(());
		}

		let timestamp;
		{
			let mut db_lock = self.db.lock();
			match db_lock.check_user(&login, &password, &token, self.config.read().is_email_validated()) {
				Ok(user_data) => {
					if self.signaling_infos.read().contains_key(&user_data.user_id) {
						reply.push(ErrorType::LoginAlreadyLoggedIn as u8);
						return Err(());
					}

					let rels = db_lock.get_relationships(user_data.user_id).map_err(|_| {
						reply.push(ErrorType::DbFail as u8);
						()
					})?;

					// Authentified beyond this point

					// Update last login time
					db_lock.update_login_time(user_data.user_id).map_err(|_| {
						reply.push(ErrorType::DbFail as u8);
						()
					})?;

					// Get friends infos
					self.authentified = true;
					self.client_info.npid = login;
					self.client_info.online_name = user_data.online_name.clone();
					self.client_info.avatar_url = user_data.avatar_url.clone();
					self.client_info.user_id = user_data.user_id;
					self.client_info.token = user_data.token.clone();
					self.client_info.flags = user_data.flags;
					reply.push(ErrorType::NoError as u8);
					reply.extend(user_data.online_name.as_bytes());
					reply.push(0);
					reply.extend(user_data.avatar_url.as_bytes());
					reply.push(0);
					reply.extend(&self.client_info.user_id.to_le_bytes());

					let dump_usernames = |reply: &mut Vec<u8>, v_usernames: &Vec<(i64, String)>| {
						reply.extend(&(v_usernames.len() as u32).to_le_bytes());
						for (_userid, username) in v_usernames {
							reply.extend(username.as_bytes());
							reply.push(0);
						}
					};

					let dump_usernames_and_status =
						|reply: &mut Vec<u8>, v_usernames: &Vec<(i64, String)>, sign_infos: &parking_lot::lock_api::RwLockWriteGuard<parking_lot::RawRwLock, HashMap<i64, ClientSignalingInfo>>| {
							reply.extend(&(v_usernames.len() as u32).to_le_bytes());
							for (userid, username) in v_usernames {
								reply.extend(username.as_bytes());
								reply.push(0);
								if sign_infos.contains_key(userid) {
									reply.push(1);
								} else {
									reply.push(0);
								}
							}
						};

					let mut sign_infos = self.signaling_infos.write();

					timestamp = Client::get_timestamp();

					dump_usernames_and_status(reply, &rels.friends, &sign_infos);
					dump_usernames(reply, &rels.friend_requests);
					dump_usernames(reply, &rels.friend_requests_received);
					dump_usernames(reply, &rels.blocked);

					friend_userids = rels.friends.iter().map(|(userid, _username)| *userid).collect();

					info!("Authentified as {}", &self.client_info.npid);
					sign_infos.insert(self.client_info.user_id, ClientSignalingInfo::new(self.channel_sender.clone(), friend_userids.clone()));
				}
				Err(e) => {
					match e {
						DbError::Empty => reply.push(ErrorType::LoginInvalidUsername as u8),
						DbError::WrongPass => reply.push(ErrorType::LoginInvalidPassword as u8),
						DbError::WrongToken => reply.push(ErrorType::LoginInvalidToken as u8),
						_ => reply.push(ErrorType::LoginError as u8),
					}
					return Err(());
				}
			}
		}

		if self.authentified {
			// Notify friends that user has come Online
			let notif = Client::create_friend_status_notification(&self.client_info.npid, timestamp, true);
			self.send_notification(&notif, &friend_userids).await;
			Ok(())
		} else {
			reply.push(ErrorType::LoginError as u8);
			Err(())
		}
	}

	pub fn create_account(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<(), ()> {
		let npid = data.get_string(false);
		let password = data.get_string(false);
		let online_name = data.get_string(false);
		let avatar_url = data.get_string(false);
		let email = data.get_string(false);

		if data.error() {
			warn!("Error while extracting data from Create command");
			reply.push(ErrorType::Malformed as u8);
			return Err(());
		}

		if npid.len() < 3 || npid.len() > 16 || !npid.chars().all(|x| x.is_ascii_alphanumeric() || x == '-' || x == '_') {
			warn!("Error validating NpId");
			reply.push(ErrorType::InvalidInput as u8);
			return Err(());
		}

		if online_name.len() < 3 || online_name.len() > 16 || !online_name.chars().all(|x| x.is_alphabetic() || x.is_ascii_digit() || x == '-' || x == '_') {
			warn!("Error validating Online Name");
			reply.push(ErrorType::InvalidInput as u8);
			return Err(());
		}

		let email = email.trim().to_string();

		if EmailAddress::new(email.clone()).is_err() {
			warn!("Invalid email provided: {}", email);
			reply.push(ErrorType::InvalidInput as u8);
			return Err(());
		}

		let mut check_email = email.to_ascii_lowercase();

		if self.config.read().is_email_validated() {
			let tokens: Vec<&str> = check_email.split('@').collect();
			if self.config.read().is_banned_domain(tokens[1]) {
				warn!("Attempted to use banned domain: {}", email);
				reply.push(ErrorType::CreationBannedEmailProvider as u8);
				return Err(());
			}

			let alias_split: Vec<&str> = tokens[0].split('+').collect();
			if alias_split.len() > 1 {
				check_email = format!("{}@{}", alias_split[0], tokens[1]);
			}
		}

		match self.db.lock().add_user(&npid, &password, &online_name, &avatar_url, &email, &check_email) {
			Ok(token) => {
				info!("Successfully created account {}", &npid);
				reply.push(ErrorType::NoError as u8);
				if self.config.read().is_email_validated() {
					if let Err(e) = self.send_token_mail(&email, &npid, &token) {
						error!("Error sending email: {}", e);
					}
				}
			}
			Err(e) => {
				warn!("Account creation failed(npid: {})", &npid);
				match e {
					DbError::ExistingUsername => reply.push(ErrorType::CreationExistingUsername as u8),
					DbError::ExistingEmail => reply.push(ErrorType::CreationExistingEmail as u8),
					_ => reply.push(ErrorType::CreationError as u8),
				}
			}
		}

		Err(()) // this is not an error, we disconnect the client after account creation, successful or not
	}

	pub fn resend_token(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<(), ()> {
		if !self.config.read().is_email_validated() {
			reply.push(ErrorType::Invalid as u8);
			return Err(());
		}

		let login = data.get_string(false);
		let password = data.get_string(false);

		if data.error() {
			warn!("Error while extracting data from Login command");
			reply.push(ErrorType::Malformed as u8);
			return Err(());
		}

		// check that a token email hasn't been sent in the last 24 hours
		{
			let mut db_lock = self.db.lock();

			if let Ok(user_data) = db_lock.check_user(&login, &password, "", false) {
				// Let's check that the email hasn't been sent in the last 24 hours
				let last_token_sent_timestamp = db_lock.get_token_sent_time(user_data.user_id).map_err(|_| {
					error!("Unexpected error querying last token sent time");
					reply.push(ErrorType::DbFail as u8);
					()
				})?;
				if (Client::get_timestamp_seconds() - last_token_sent_timestamp) < (24*60*60) {
					warn!("User {} attempted to get token again too soon!", login);
					reply.push(ErrorType::TooSoon as u8);
					return Err(());
				}

				if let Err(e) = self.send_token_mail(&user_data.email, &login, &user_data.token) {
					error!("Error sending email: {}", e);
					reply.push(ErrorType::EmailFail as u8);
				} else {
					// Update last token sent time
					if db_lock.set_token_sent_time(user_data.user_id).is_err() {
						error!("Unexpected error setting token sent time");
					}
					reply.push(ErrorType::NoError as u8);
				}
			} else {
				reply.push(ErrorType::LoginError as u8);
			}
		}

		Err(())
	}
}
