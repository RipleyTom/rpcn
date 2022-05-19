// Admin Commands

use crate::server::client::*;

impl Client {
	pub fn req_admin_update_domain_bans(&self) -> Result<(), ()> {
		if !self.is_admin() {
			return Err(());
		}

		self.config.write().load_domains_banlist();

		Ok(())
	}

	pub fn req_admin_terminate_server(&self) -> Result<(), ()> {
		if !self.is_admin() {
			return Err(());
		}

		// Client always holds both a Receiver and an Arc to sender so it should always succeed
		self.terminate_watch.send.lock().send(true).unwrap();

		Ok(())
	}
}
