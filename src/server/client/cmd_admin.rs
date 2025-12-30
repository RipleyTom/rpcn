// Admin Commands

use crate::server::client::*;

impl Client {
	fn check_admin(&self, command: &str) -> Result<(), ErrorType> {
		if !self.is_admin() {
			error!("Attempted to use {} command without admin rights!", command);
			return Err(ErrorType::Invalid);
		}

		Ok(())
	}

	pub fn req_admin_update_domain_bans(&self) -> Result<ErrorType, ErrorType> {
		self.check_admin("UpdateDomainBans")?;

		self.config.write().load_domains_banlist();
		Ok(ErrorType::NoError)
	}

	pub fn req_admin_terminate_server(&self) -> Result<ErrorType, ErrorType> {
		self.check_admin("TerminateServer")?;

		// Client always holds both a Receiver and an Arc to sender so it should always succeed
		self.terminate_watch.send.lock().send(true).unwrap();
		Ok(ErrorType::NoError)
	}

	pub fn req_admin_update_servers_cfg(&self) -> Result<ErrorType, ErrorType> {
		self.check_admin("UpdateServersCfg")?;

		if let Err(e) = Database::new(self.get_database_connection()?).update_servers_cfg() {
			error!("Failed to update servers config: {}", e);
			return Ok(ErrorType::DbFail);
		}

		Ok(ErrorType::NoError)
	}

	pub fn req_admin_ban_user(&self, data: &mut StreamExtractor) -> Result<ErrorType, ErrorType> {
		self.check_admin("BanUser")?;

		let username = data.get_string(false);

		if data.error() {
			warn!("Error while extracting data from BanUser command");
			return Err(ErrorType::Malformed);
		}

		let db = Database::new(self.get_database_connection()?);

		match db.get_user_id(&username) {
			Ok(user_id) => match db.ban_user(user_id) {
				Ok(()) => {
					warn!("Successfully banned user {}", username);
					Ok(ErrorType::NoError)
				}
				Err(e) => {
					error!("Failed to ban user {}: {:?}", username, e);
					Ok(ErrorType::DbFail)
				}
			},

			Err(e) => {
				error!("Couldn't find user {}: {}", username, e);
				Ok(ErrorType::NotFound)
			}
		}
	}

	pub fn req_admin_del_user(&self, data: &mut StreamExtractor) -> Result<ErrorType, ErrorType> {
		self.check_admin("DelUser")?;

		let username = data.get_string(false);

		if data.error() {
			warn!("Error while extracting data from DelUser command");
			return Err(ErrorType::Malformed);
		}
		let db = Database::new(self.get_database_connection()?);

		let user_id = match db.get_user_id(&username) {
			Ok(user_id) => user_id,
			Err(e) => {
				error!("Couldn't find user {}: {}", username, e);
				return Ok(ErrorType::NotFound);
			}
		};

		let email_check = match db.get_email_check(user_id) {
			Ok(email_check) => email_check,
			Err(e) => {
				error!("Couldn't get the email_check for user_id {}: {}", user_id, e);
				return Ok(ErrorType::DbFail);
			}
		};

		let client_infos = self.shared.client_infos.write();

		if client_infos.contains_key(&user_id) {
			error!("Couldn't delete {} as user is online!", username);
			return Ok(ErrorType::CondFail);
		}

		match db.delete_user(user_id, &username, &email_check) {
			Ok(()) => {
				info!("Successfully banned user {}", username);
				Ok(ErrorType::NoError)
			}
			Err(e) => {
				error!("Failed to ban user {}: {:?}", username, e);
				Ok(ErrorType::DbFail)
			}
		}
	}
}
