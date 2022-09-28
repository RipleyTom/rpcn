// Admin Commands

use crate::server::client::*;

impl Client {
	pub fn req_admin_update_domain_bans(&self, reply: &mut Vec<u8>) -> Result<(), ()> {
		if !self.is_admin() {
			reply.push(ErrorType::Invalid as u8);
			return Err(());
		}

		self.config.write().load_domains_banlist();

		reply.push(ErrorType::NoError as u8);
		Ok(())
	}

	pub fn req_admin_terminate_server(&self, reply: &mut Vec<u8>) -> Result<(), ()> {
		if !self.is_admin() {
			reply.push(ErrorType::Invalid as u8);
			return Err(());
		}

		// Client always holds both a Receiver and an Arc to sender so it should always succeed
		self.terminate_watch.send.lock().send(true).unwrap();

		reply.push(ErrorType::NoError as u8);
		Ok(())
	}
}
