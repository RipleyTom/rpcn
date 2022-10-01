// Admin Commands

use crate::server::client::*;

impl Client {
	pub fn req_admin_update_domain_bans(&self) -> Result<ErrorType, ErrorType> {
		if !self.is_admin() {
			return Err(ErrorType::Invalid);
		}

		self.config.write().load_domains_banlist();
		Ok(ErrorType::NoError)
	}

	pub fn req_admin_terminate_server(&self) -> Result<ErrorType, ErrorType> {
		if !self.is_admin() {
			return Err(ErrorType::Invalid);
		}

		// Client always holds both a Receiver and an Arc to sender so it should always succeed
		self.terminate_watch.send.lock().send(true).unwrap();
		Ok(ErrorType::NoError)
	}
}
