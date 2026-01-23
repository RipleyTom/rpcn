use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI64, Ordering};

use parking_lot::RwLock;
use tracing::error;

use crate::server::client::ComId;

pub struct GameInfo {
	pub num_users: AtomicI64,
	pub name_hints: RwLock<HashSet<String>>,
}

pub struct GameTracker {
	pub num_users: AtomicI64,
	pub psn_games: RwLock<HashMap<ComId, GameInfo>>,
	pub ticket_games: RwLock<HashMap<String, AtomicI64>>,
}

impl GameTracker {
	pub fn new() -> GameTracker {
		GameTracker {
			num_users: AtomicI64::new(0),
			psn_games: RwLock::new(HashMap::new()),
			ticket_games: RwLock::new(HashMap::new()),
		}
	}

	pub fn add_gamename_hint(&self, com_id: &ComId, name_hint: &str) {
		let psn_games = self.psn_games.read();
		let game_info = psn_games.get(com_id);
		if game_info.is_none() {
			error!("Inconsistency in gametracker!");
			return;
		}
		let game_info = game_info.unwrap();

		if game_info.name_hints.read().contains(name_hint) {
			return;
		}

		game_info.name_hints.write().insert(name_hint.to_string());
	}

	pub fn increase_num_users(&self) {
		self.num_users.fetch_add(1, Ordering::SeqCst);
	}

	pub fn decrease_num_users(&self) {
		self.num_users.fetch_sub(1, Ordering::SeqCst);
	}

	fn add_value_psn(&self, com_id: &ComId, to_add: i64) {
		{
			let psn_games = self.psn_games.read();
			if psn_games.contains_key(com_id) {
				psn_games[com_id].num_users.fetch_add(to_add, Ordering::SeqCst);
				return;
			}
		}

		self.psn_games
			.write()
			.entry(*com_id)
			.or_insert_with(|| GameInfo {
				num_users: AtomicI64::new(0),
				name_hints: RwLock::new(HashSet::new()),
			})
			.num_users
			.fetch_add(to_add, Ordering::SeqCst);
	}

	pub fn increase_count_psn(&self, com_id: &ComId) {
		self.add_value_psn(com_id, 1);
	}

	pub fn decrease_count_psn(&self, com_id: &ComId) {
		self.add_value_psn(com_id, -1);
	}

	fn add_value_ticket(&self, service_id: &str, to_add: i64) {
		{
			let ticket_games = self.ticket_games.read();
			if ticket_games.contains_key(service_id) {
				ticket_games[service_id].fetch_add(to_add, Ordering::SeqCst);
				return;
			}
		}

		self.ticket_games
			.write()
			.entry(service_id.to_owned())
			.or_insert_with(|| AtomicI64::new(0))
			.fetch_add(to_add, Ordering::SeqCst);
	}

	pub fn increase_count_ticket(&self, service_id: &str) {
		self.add_value_ticket(service_id, 1);
	}

	pub fn decrease_count_ticket(&self, service_id: &str) {
		self.add_value_ticket(service_id, -1);
	}
}
