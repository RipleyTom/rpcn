// Server/World Commands

use crate::server::client::*;

impl Client {
	pub fn req_get_server_list(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let com_id = self.get_com_id_with_redir(data);

		if data.error() {
			warn!("Error while extracting data from GetServerList command");
			return Err(ErrorType::Malformed);
		}

		let servs = Database::new(self.get_database_connection()?).get_server_list(&com_id, self.config.read().is_create_missing());
		if servs.is_err() {
			return Err(ErrorType::DbFail);
		}
		let servs = servs.unwrap();

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

		let worlds = Database::new(self.get_database_connection()?).get_world_list(&com_id, server_id, self.config.read().is_create_missing());
		if worlds.is_err() {
			return Err(ErrorType::DbFail);
		}
		let worlds = worlds.unwrap();

		let num_worlds = worlds.len() as u32;
		reply.extend(&num_worlds.to_le_bytes());
		for world in worlds {
			reply.extend(&world.to_le_bytes());
		}

		info!("Returning {} worlds", num_worlds);

		Ok(ErrorType::NoError)
	}
}
