use tracing::{error, warn};

use crate::server::client::{Client, ErrorType, com_id_to_string};
use crate::server::database::Database;
use crate::server::stream_extractor::StreamExtractor;

impl Client {
	pub fn req_unlock_trophy(&mut self, data: &mut StreamExtractor, _reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let com_id = data.get_com_id();
		let trophy_id = data.get::<i32>();
		let timestamp = data.get::<u64>();

		if data.error() {
			warn!("UnlockTrophy: malformed packet");
			return Err(ErrorType::Malformed);
		}

		if trophy_id < 0 {
			warn!("UnlockTrophy: negative trophy_id {}", trophy_id);
			return Err(ErrorType::InvalidInput);
		}

		let communication_id = com_id_to_string(&com_id);

		let db = Database::new(self.get_database_connection()?);

		db.record_user_trophy(self.client_info.user_id, &communication_id, trophy_id, timestamp as i64)
			.map_err(|e| {
				error!("UnlockTrophy: db error: {:?}", e);
				ErrorType::DbFail
			})?;

		Ok(ErrorType::NoError)
	}

	pub fn req_sync_trophies(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let com_id = data.get_com_id();
		let count = data.get::<u32>();

		if data.error() {
			warn!("SyncTrophies: malformed header");
			return Err(ErrorType::Malformed);
		}

		const MAX_TROPHIES: u32 = 1024;
		if count > MAX_TROPHIES {
			warn!("SyncTrophies: count {} exceeds maximum {}", count, MAX_TROPHIES);
			return Err(ErrorType::InvalidInput);
		}

		let mut local_trophies: Vec<(i32, u64)> = Vec::with_capacity(count as usize);
		for _ in 0..count {
			let tid = data.get::<i32>();
			let ts = data.get::<u64>();
			if data.error() {
				warn!("SyncTrophies: malformed trophy entry");
				return Err(ErrorType::Malformed);
			}
			if tid < 0 {
				warn!("SyncTrophies: negative trophy_id {} in local list", tid);
				return Err(ErrorType::InvalidInput);
			}
			local_trophies.push((tid, ts));
		}

		let communication_id = com_id_to_string(&com_id);

		let db = Database::new(self.get_database_connection()?);

		for (tid, ts) in &local_trophies {
			if let Err(e) = db.record_user_trophy(self.client_info.user_id, &communication_id, *tid, *ts as i64) {
				error!("SyncTrophies: failed to record trophy {}: {:?}", tid, e);
				return Err(ErrorType::DbFail);
			}
		}

		let server_trophies = db.get_user_trophies(self.client_info.user_id, &communication_id).map_err(|e| {
			error!("SyncTrophies: failed to query server trophies: {:?}", e);
			ErrorType::DbFail
		})?;

		reply.extend_from_slice(&(server_trophies.len() as u32).to_le_bytes());
		for (tid, ts) in &server_trophies {
			reply.extend_from_slice(&tid.to_le_bytes());
			reply.extend_from_slice(&ts.to_le_bytes());
		}

		Ok(ErrorType::NoError)
	}
}
