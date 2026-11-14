// Tracks servers/worlds/lobbies for games

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;

use tracing::{info, warn};

use crate::server::client::{COMMUNICATION_ID_SIZE, ComId, com_id_to_string};

#[derive(Default)]
struct GameInfo {
	servers: BTreeMap<u16, BTreeSet<u32>>,      // server_id -> world_ids
	worlds: HashMap<u32, u16>,                  // world_id -> server_id
	lobbies: HashMap<u64, u32>,                 // lobby_id -> world_id
	world_lobbies: HashMap<u32, BTreeSet<u64>>, // world_id -> lobby_ids
}

// Maximum number of lobbies a single range in servers.cfg or the default lobby creation can produce
pub const MAX_LOBBIES_PER_RANGE: u64 = 4096;

// Lobbies are separated by commas, a-b describes an inclusive range
fn parse_lobbies(lobbies: &str) -> Result<Vec<u64>, ()> {
	if lobbies.is_empty() {
		return Ok(Vec::new());
	}

	lobbies
		.split(',')
		.map(|s| {
			let s = s.trim();
			if let Some((first, last)) = s.split_once('-') {
				let (first, last) = (first.trim().parse::<u64>().map_err(|_| ())?, last.trim().parse::<u64>().map_err(|_| ())?);
				if first > last || last - first >= MAX_LOBBIES_PER_RANGE {
					return Err(());
				}
				Ok((first..=last).collect())
			} else {
				s.parse::<u64>().map(|v| vec![v]).map_err(|_| ())
			}
		})
		.collect::<Result<Vec<Vec<u64>>, ()>>()
		.map(|v| v.concat())
}

impl GameInfo {
	fn add_server(&mut self, server_id: u16) {
		self.servers.entry(server_id).or_default();
	}

	fn add_world(&mut self, server_id: u16, world_id: u32) -> bool {
		if self.worlds.contains_key(&world_id) || !self.servers.contains_key(&server_id) {
			return false;
		}

		self.worlds.insert(world_id, server_id);
		self.servers.get_mut(&server_id).unwrap().insert(world_id);
		true
	}

	fn add_lobby(&mut self, world_id: u32, lobby_id: u64) -> bool {
		if self.lobbies.contains_key(&lobby_id) || !self.worlds.contains_key(&world_id) {
			return false;
		}

		self.lobbies.insert(lobby_id, world_id);
		self.world_lobbies.entry(world_id).or_default().insert(lobby_id);
		true
	}

	// Returns (server_id, lobby_id) for every lobby of the world, sorted by lobby id
	fn get_lobby_list(&self, world_id: u32) -> Option<Vec<(u16, u64)>> {
		let server_id = *self.worlds.get(&world_id)?;
		Some(
			self.world_lobbies
				.get(&world_id)
				.map(|lobbies| lobbies.iter().map(|lobby_id| (server_id, *lobby_id)).collect())
				.unwrap_or_default(),
		)
	}
}

#[derive(Default)]
pub struct GameInfos {
	games: HashMap<ComId, GameInfo>,
}

impl GameInfos {
	pub fn new() -> GameInfos {
		let buf_file = match fs::read_to_string("servers.cfg") {
			Ok(buf_file) => buf_file,
			Err(e) => {
				println!("Failed to read servers.cfg: {}", e);
				return Default::default();
			}
		};

		let mut game_infos: GameInfos = Default::default();

		for l in buf_file.lines() {
			if l.trim().is_empty() || l.trim().starts_with('#') {
				continue;
			}

			let servers_infos: Vec<&str> = l.trim().split('|').map(|v| v.trim()).collect();
			if servers_infos.len() != 4 || servers_infos[0].len() != COMMUNICATION_ID_SIZE {
				println!("servers.cfg: line({}) was considered invalid and was skipped", l);
				continue;
			}

			let com_id: ComId = servers_infos[0].as_bytes().try_into().unwrap();
			let server = servers_infos[1].parse::<u16>();
			let world = servers_infos[2].parse::<u32>();
			let lobbies = parse_lobbies(servers_infos[3]);

			let (Ok(server), Ok(world), Ok(lobbies)) = (server, world, lobbies) else {
				println!("servers.cfg: line({}) couldn't be parsed and was skipped", l);
				continue;
			};

			let game = game_infos.games.entry(com_id).or_default();
			game.add_server(server);
			if !game.add_world(server, world) {
				println!("servers.cfg: line({}) has a world that is already defined", l);
			}
			for lobby in lobbies {
				if !game.add_lobby(world, lobby) {
					println!("servers.cfg: line({}) has a lobby({}) that couldn't be added", l, lobby);
				}
			}
		}

		game_infos
	}

	pub fn get_server_list(&self, com_id: &ComId) -> Vec<u16> {
		self.games.get(com_id).map(|game| game.servers.keys().copied().collect()).unwrap_or_default()
	}

	// Creates a server so the server list is not empty
	pub fn create_default_server(&mut self, com_id: &ComId) -> Vec<u16> {
		info!("Creating server 1 for {}", com_id_to_string(com_id));
		self.games.entry(*com_id).or_default().add_server(1);
		self.get_server_list(com_id)
	}

	// Returns None if the server doesn't exist
	pub fn get_world_list(&self, com_id: &ComId, server_id: u16) -> Option<Vec<u32>> {
		let worlds = self.games.get(com_id).and_then(|game| game.servers.get(&server_id)).map(|worlds| worlds.iter().copied().collect());

		if worlds.is_none() {
			// This was initially assumed that some games would hardcode those but this doesn't seem to be the case
			// The few cases where it has happened seems to mostly have been client sending an uninitialized value
			warn!("Attempted to query world list on a non-existing server({}:{})", com_id_to_string(com_id), server_id);
		}

		worlds
	}

	// Creates a world so that the world list is not empty
	pub fn create_default_world(&mut self, com_id: &ComId, server_id: u16) -> Option<Vec<u32>> {
		let game = self.games.get_mut(com_id)?;
		if !game.servers.contains_key(&server_id) {
			return None;
		}

		let new_wid = game.worlds.keys().max().map_or(65537, |max| max + 1);
		info!("Creating world {} for server {}:{}", new_wid, com_id_to_string(com_id), server_id);
		game.add_world(server_id, new_wid);

		self.get_world_list(com_id, server_id)
	}

	pub fn get_corresponding_server(&self, com_id: &ComId, world_id: u32, lobby_id: u64) -> Option<u16> {
		if world_id == 0 && lobby_id == 0 {
			warn!("Attempted to get server for com_id {} from world(0) and lobby(0)", com_id_to_string(com_id));
			return None;
		}

		let game = self.games.get(com_id)?;
		let world_id = if world_id == 0 { *game.lobbies.get(&lobby_id)? } else { world_id };
		game.worlds.get(&world_id).copied()
	}

	// Returns None if the world doesn't exist
	pub fn get_lobby_list(&self, com_id: &ComId, world_id: u32) -> Option<Vec<(u16, u64)>> {
		self.games.get(com_id)?.get_lobby_list(world_id)
	}

	// Creates lobbies so that the lobby list of a world is not empty, some games wait for a minimum number of lobbies
	// Lobby ids are unique per communication id so the new ones follow the highest existing id
	pub fn create_default_lobbies(&mut self, com_id: &ComId, world_id: u32, count: u32) -> Option<Vec<(u16, u64)>> {
		let game = self.games.get_mut(com_id)?;
		if !game.worlds.contains_key(&world_id) {
			warn!("Attempted to create lobbies for a non-existing world({}:{})", com_id_to_string(com_id), world_id);
			return None;
		}

		// Another client may have created them between the read and the write lock
		if game.world_lobbies.get(&world_id).is_some_and(|lobbies| !lobbies.is_empty()) {
			return game.get_lobby_list(world_id);
		}

		let count = (count as u64).min(MAX_LOBBIES_PER_RANGE);
		let first = game.lobbies.keys().max().map_or(1, |max| max + 1);
		info!("Creating {} lobbies for world {}:{}", count, com_id_to_string(com_id), world_id);
		for lobby_id in first..first + count {
			game.add_lobby(world_id, lobby_id);
		}

		game.get_lobby_list(world_id)
	}

	// Returns (server_id, world_id) of the lobby
	pub fn get_lobby_location(&self, com_id: &ComId, lobby_id: u64) -> Option<(u16, u32)> {
		let game = self.games.get(com_id)?;
		let world_id = *game.lobbies.get(&lobby_id)?;
		Some((*game.worlds.get(&world_id)?, world_id))
	}
}
