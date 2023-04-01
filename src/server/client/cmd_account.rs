// Account Management Commands

use crate::server::client::*;
use crate::server::database::DbError;

use lettre::{message::Mailbox, Message, SmtpTransport, Transport};

fn strip_email(email: &str) -> String {
	let check_email = email.to_ascii_lowercase();
	let tokens: Vec<&str> = check_email.split('@').collect();
	let alias_split: Vec<&str> = tokens[0].split('+').collect();
	format!("{}@{}", alias_split[0], tokens[1])
}

impl Client {
	pub fn is_admin(&self) -> bool {
		self.client_info.admin
	}

	#[allow(dead_code)]
	pub fn is_stat_agent(&self) -> bool {
		self.client_info.stat_agent
	}

	fn send_email(&self, email_to_send: Message) -> Result<(), lettre::transport::smtp::Error> {
		let (host, login, password) = self.config.read().get_email_auth();

		let smtp_client;
		if host.is_empty() {
			smtp_client = SmtpTransport::unencrypted_localhost();
		} else {
			let mut smtp_client_builder = SmtpTransport::relay(&host)?;

			if !login.is_empty() {
				smtp_client_builder = smtp_client_builder
					.credentials(Credentials::new(login, password))
					.authentication(vec![Mechanism::Plain])
					.hello_name(lettre::transport::smtp::extension::ClientId::Domain("np.rpcs3.net".to_string()));
			}

			smtp_client = smtp_client_builder.build();
		}

		smtp_client.send(&email_to_send)?;
		Ok(())
	}

	fn send_token_mail(&self, email_addr: &str, npid: &str, token: &str) -> Result<(), String> {
		let email_to_send = Message::builder()
			.to(Mailbox::new(
				Some(npid.to_owned()),
				email_addr.parse().map_err(|e| format!("Error parsing email({}): {}", email_addr, e))?,
			))
			.from("RPCN <np@rpcs3.net>".parse().unwrap())
			.subject("Your token for RPCN")
			.header(lettre::message::header::ContentType::TEXT_PLAIN)
			.body(format!("Your token for username {} is:\n{}", npid, token))
			.unwrap();
		self.send_email(email_to_send).map_err(|e| format!("SMTP error: {}", e))
	}

	fn send_reset_token_mail(&self, email_addr: &str, npid: &str, reset_token: &str) -> Result<(), String> {
		let email_to_send = Message::builder()
			.to(Mailbox::new(
				Some(npid.to_owned()),
				email_addr.parse().map_err(|e| format!("Error parsing email({}): {}", email_addr, e))?,
			))
			.from("RPCN <np@rpcs3.net>".parse().unwrap())
			.subject("Your password reset code for RPCN")
			.header(lettre::message::header::ContentType::TEXT_PLAIN)
			.body(format!(
				"Your password reset code for username {} is:\n{}\n\nNote that this code can only be used once!",
				npid, reset_token
			))
			.unwrap();
		self.send_email(email_to_send).map_err(|e| format!("SMTP error: {}", e))
	}

	pub async fn login(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let login = data.get_string(false);
		let password = data.get_string(false);
		let token = data.get_string(true);
		let friend_userids: HashMap<i64, String>;

		if data.error() {
			warn!("Error while extracting data from Login command");
			return Err(ErrorType::Malformed);
		}

		let timestamp;
		{
			let db = Database::new(self.get_database_connection()?);

			match db.check_user(&login, &password, &token, self.config.read().is_email_validated()) {
				Ok(user_data) => {
					let mut sign_infos = self.signaling_infos.write();

					if sign_infos.contains_key(&user_data.user_id) {
						return Err(ErrorType::LoginAlreadyLoggedIn);
					}

					let rels = db.get_relationships(user_data.user_id).map_err(|_| ErrorType::DbFail)?;

					// Authentified beyond this point

					// Update last login time
					db.update_login_time(user_data.user_id).map_err(|_| ErrorType::DbFail)?;

					// Get friends infos
					self.authentified = true;
					self.client_info.npid = login;
					self.client_info.online_name = user_data.online_name.clone();
					self.client_info.avatar_url = user_data.avatar_url.clone();
					self.client_info.user_id = user_data.user_id;
					self.client_info.token = user_data.token.clone();
					self.client_info.admin = user_data.admin;
					self.client_info.stat_agent = user_data.stat_agent;
					self.client_info.banned = user_data.banned;
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

					timestamp = Client::get_timestamp_nanos();

					dump_usernames_and_status(reply, &rels.friends, &sign_infos);
					dump_usernames(reply, &rels.friend_requests);
					dump_usernames(reply, &rels.friend_requests_received);
					dump_usernames(reply, &rels.blocked);

					friend_userids = rels.friends.iter().map(|v| (*v).clone()).collect();

					info!("Authentified as {}", &self.client_info.npid);
					sign_infos.insert(self.client_info.user_id, ClientSignalingInfo::new(self.channel_sender.clone(), friend_userids.clone()));

					self.game_tracker.increase_num_users();
				}
				Err(e) => {
					return Err(match e {
						DbError::Empty => ErrorType::LoginInvalidUsername,
						DbError::WrongPass => ErrorType::LoginInvalidPassword,
						DbError::WrongToken => ErrorType::LoginInvalidToken,
						_ => ErrorType::LoginError,
					});
				}
			}
		}

		if self.authentified {
			// Notify friends that user has come Online
			let notif = Client::create_friend_status_notification(&self.client_info.npid, timestamp, true);
			self.send_notification(&notif, &friend_userids.keys().map(|k| *k).collect()).await;
			Ok(ErrorType::NoError)
		} else {
			Err(ErrorType::LoginError)
		}
	}

	pub fn create_account(&self, data: &mut StreamExtractor) -> Result<ErrorType, ErrorType> {
		let npid = data.get_string(false);
		let password = data.get_string(false);
		let online_name = data.get_string(false);
		let avatar_url = data.get_string(false);
		let email = data.get_string(false);

		if data.error() {
			warn!("Error while extracting data from Create command");
			return Err(ErrorType::Malformed);
		}

		if npid.len() < 3 || npid.len() > 16 || !npid.chars().all(|x| x.is_ascii_alphanumeric() || x == '-' || x == '_') {
			warn!("Error validating NpId");
			return Err(ErrorType::InvalidInput);
		}

		if online_name.len() < 3 || online_name.len() > 16 || !online_name.chars().all(|x| x.is_alphabetic() || x.is_ascii_digit() || x == '-' || x == '_') {
			warn!("Error validating Online Name");
			return Err(ErrorType::InvalidInput);
		}

		let email = email.trim().to_string();

		if email.parse::<lettre::Address>().is_err() || email.contains(' ') {
			warn!("Invalid email provided: {}", email);
			return Err(ErrorType::InvalidInput);
		}

		let check_email = strip_email(&email);

		if self.config.read().is_email_validated() {
			let tokens: Vec<&str> = check_email.split('@').collect();
			if self.config.read().is_banned_domain(tokens[1]) {
				warn!("Attempted to use banned domain: {}", email);
				return Err(ErrorType::CreationBannedEmailProvider);
			}
		}

		match Database::new(self.get_database_connection()?).add_user(&npid, &password, &online_name, &avatar_url, &email, &check_email) {
			Ok(token) => {
				info!("Successfully created account {}", &npid);
				if self.config.read().is_email_validated() {
					if let Err(e) = self.send_token_mail(&email, &npid, &token) {
						error!("Error sending email: {}", e);
					}
				}

				// this is not an error, we disconnect the client after account creation, successful or not
				Err(ErrorType::NoError)
			}
			Err(e) => {
				warn!("Account creation failed(npid: {})", &npid);
				Err(match e {
					DbError::ExistingUsername => ErrorType::CreationExistingUsername,
					DbError::ExistingEmail => ErrorType::CreationExistingEmail,
					_ => ErrorType::CreationError,
				})
			}
		}
	}

	pub fn resend_token(&self, data: &mut StreamExtractor) -> Result<ErrorType, ErrorType> {
		if !self.config.read().is_email_validated() {
			return Err(ErrorType::Invalid);
		}

		let login = data.get_string(false);
		let password = data.get_string(false);

		if data.error() {
			warn!("Error while extracting data from SendToken command");
			return Err(ErrorType::Malformed);
		}

		{
			let db = Database::new(self.get_database_connection()?);

			if let Ok(user_data) = db.check_user(&login, &password, "", false) {
				// Let's check that the email hasn't been sent in the last 24 hours
				let last_token_sent_timestamp = db.get_token_sent_time(user_data.user_id).map_err(|_| {
					error!("Unexpected error querying last token sent time");
					ErrorType::DbFail
				})?;

				// check that a token email hasn't been sent in the last 24 hours
				if let Some(last_token_sent_timestamp) = last_token_sent_timestamp {
					if (Client::get_timestamp_seconds() - last_token_sent_timestamp) < (24 * 60 * 60) {
						warn!("User {} attempted to get token again too soon!", login);
						return Err(ErrorType::TooSoon);
					}
				}

				if let Err(e) = self.send_token_mail(&user_data.email, &login, &user_data.token) {
					error!("Error sending email: {}", e);
					Err(ErrorType::EmailFail)
				} else {
					// Update last token sent time
					if db.set_token_sent_time(user_data.user_id).is_err() {
						error!("Unexpected error setting token sent time");
					}
					Err(ErrorType::NoError)
				}
			} else {
				Err(ErrorType::LoginError)
			}
		}
	}

	pub fn send_reset_token(&self, data: &mut StreamExtractor) -> Result<ErrorType, ErrorType> {
		if !self.config.read().is_email_validated() {
			return Err(ErrorType::Invalid);
		}

		let username = data.get_string(false);
		let email = data.get_string(false);
		if data.error() {
			warn!("Error while extracting data from SendResetToken command");
			return Err(ErrorType::Malformed);
		}

		let db = Database::new(self.get_database_connection()?);
		let email_check = strip_email(&email);

		if let Ok((user_id, email_to_use)) = db.check_email(&username, &email_check) {
			let last_pass_token_sent_timestamp = db.get_reset_password_token_sent_time(user_id).map_err(|_| {
				error!("Unexpected error querying last password token timestamp");
				ErrorType::DbFail
			})?;

			// check that a reset token email hasn't been sent in the last 24 hours
			if let Some(last_pass_token_sent_timestamp) = last_pass_token_sent_timestamp {
				if (Client::get_timestamp_seconds() - last_pass_token_sent_timestamp) < (24 * 60 * 60) {
					warn!("User {} attempted to get password reset token again too soon!", username);
					return Err(ErrorType::TooSoon);
				}
			}

			// Generate a new token and update the user entry
			let token = db.update_password_token(user_id).map_err(|_| {
				error!("Unexpected error updating reset password token");
				ErrorType::DbFail
			})?;

			if let Err(e) = self.send_reset_token_mail(&email_to_use, &username, &token) {
				error!("Error sending email: {}", e);
				Err(ErrorType::EmailFail)
			} else {
				// Update last token sent time
				db.set_reset_password_token_sent_time(user_id).map_err(|_| {
					error!("Unexpected error setting new reset password token timestamp");
					ErrorType::DbFail
				})?;
				Err(ErrorType::NoError)
			}
		} else {
			Err(ErrorType::LoginError)
		}
	}

	pub fn reset_password(&self, data: &mut StreamExtractor) -> Result<ErrorType, ErrorType> {
		if !self.config.read().is_email_validated() {
			return Err(ErrorType::Invalid);
		}

		let username = data.get_string(false);
		let token = data.get_string(false);
		let new_password = data.get_string(false);

		if data.error() {
			warn!("Error while extracting data from ResetPassword command");
			return Err(ErrorType::Malformed);
		}

		{
			let db = Database::new(self.get_database_connection()?);

			if let Ok(user_id) = db.check_reset_token(&username, &token) {
				if db.update_user_password(user_id, &token, &new_password).is_err() {
					error!("Unexpected error updating user password!");
					Err(ErrorType::DbFail)
				} else {
					Err(ErrorType::NoError)
				}
			} else {
				Err(ErrorType::LoginError)
			}
		}
	}
}
