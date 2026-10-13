// Server/World Commands

use parking_lot::RwLockUpgradableReadGuard;

use crate::server::client::*;

impl Client {
	pub fn req_get_server_list(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let com_id = self.get_com_id_with_redir(data);

		if data.error() {
			warn!("Error while extracting data from GetServerList command");
			return Err(ErrorType::Malformed);
		}

		let servs = {
			let game_infos = self.shared.game_infos.upgradable_read();
			let servs = game_infos.get_server_list(&com_id);
			if servs.is_empty() && self.config.read().is_create_missing() {
				RwLockUpgradableReadGuard::upgrade(game_infos).create_default_server(&com_id)
			} else {
				servs
			}
		};

		let num_servs = servs.len() as u16;
		reply.extend(&num_servs.to_le_bytes());
		for serv in servs {
			reply.extend(&serv.to_le_bytes());
		}

		info!("Returning {} servers for comId {}", num_servs, com_id_to_string(&com_id));

		Ok(ErrorType::NoError)
	}
	pub fn req_get_world_list(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let com_id = self.get_com_id_with_redir(data);
		let server_id = data.get::<u16>();

		if data.error() {
			warn!("Error while extracting data from GetWorldList command");
			return Err(ErrorType::Malformed);
		}

		let worlds = {
			let game_infos = self.shared.game_infos.upgradable_read();
			match game_infos.get_world_list(&com_id, server_id) {
				None => return Err(ErrorType::DbFail),
				Some(worlds) if worlds.is_empty() && self.config.read().is_create_missing() => {
					RwLockUpgradableReadGuard::upgrade(game_infos).create_default_world(&com_id, server_id).ok_or(ErrorType::DbFail)?
				}
				Some(worlds) => worlds,
			}
		};

		let num_worlds = worlds.len() as u32;
		reply.extend(&num_worlds.to_le_bytes());
		for world in worlds {
			reply.extend(&world.to_le_bytes());
		}

		info!("Returning {} worlds", num_worlds);

		Ok(ErrorType::NoError)
	}
}
