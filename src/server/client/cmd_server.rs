// Server/World Commands

use crate::server::client::*;

impl Client {
	pub fn req_get_server_list(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<(), ()> {
		let com_id = self.get_com_id_with_redir(data);

		if data.error() {
			warn!("Error while extracting data from GetServerList command");
			reply.push(ErrorType::Malformed as u8);
			return Err(());
		}

		// TODO: Generalize this (redirects DeS US queries to EU servers)
		// if com_id == "NPWR00881" {
		//     com_id = String::from("NPWR01249");
		// }

		let servs = self.db.lock().get_server_list(&com_id);
		if let Err(_) = servs {
			reply.push(ErrorType::DbFail as u8);
			return Err(());
		}
		let servs = servs.unwrap();

		reply.push(ErrorType::NoError as u8);

		let num_servs = servs.len() as u16;
		reply.extend(&num_servs.to_le_bytes());
		for serv in servs {
			reply.extend(&serv.to_le_bytes());
		}

		info!("Returning {} servers for comId {}", num_servs, com_id_to_string(&com_id));

		Ok(())
	}
	pub fn req_get_world_list(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<(), ()> {
		let com_id = self.get_com_id_with_redir(data);
		let server_id = data.get::<u16>();

		if data.error() {
			warn!("Error while extracting data from GetWorldList command");
			reply.push(ErrorType::Malformed as u8);
			return Err(());
		}

		let worlds = self.db.lock().get_world_list(&com_id, server_id);
		if let Err(_) = worlds {
			reply.push(ErrorType::DbFail as u8);
			return Err(());
		}
		let worlds = worlds.unwrap();

		reply.push(ErrorType::NoError as u8);

		let num_worlds = worlds.len() as u32;
		reply.extend(&num_worlds.to_le_bytes());
		for world in worlds {
			reply.extend(&world.to_le_bytes());
		}

		info!("Returning {} worlds", num_worlds);

		Ok(())
	}
}
