// Admin Commands

use crate::server::client::*;

impl Client {
	pub fn req_admin_update_domain_bans(&self) -> Result<(), ()> {
		if (self.client_info.flags & 1) == 0 {
			return Err(());
		}

		self.config.write().load_domains_banlist();

		Ok(())
	}
}
