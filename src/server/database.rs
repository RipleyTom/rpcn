use std::fs;
use std::sync::Arc;

use parking_lot::RwLock;
use rand::prelude::*;
use rusqlite;
use tracing::{error, info, warn};

use crate::server::client::*;
use crate::Config;

pub struct DatabaseManager {
	config: Arc<RwLock<Config>>,
	conn: rusqlite::Connection,
}

#[derive(Debug)]
#[repr(u8)]
#[allow(dead_code)]
pub enum DbError {
	Internal,         // Unknown error
	Invalid,          // parameters given to db function are invalid
	Empty,            // Not found
	WrongPass,        // For login only: wrong pass
	WrongToken,       // For login only: wrong token
	ExistingUsername, // Username already exists
	ExistingEmail,    // Email already exists
}

#[allow(dead_code)]
pub struct UserQueryResult {
	pub user_id: i64,
	hash: Vec<u8>,
	salt: Vec<u8>,
	pub online_name: String,
	pub avatar_url: String,
	pub email: String,
	pub token: String,
	pub flags: u16,
}

#[repr(u16)]
pub enum FriendStatus {
	Friend = (1 << 0),
	Blocked = (1 << 1),
}

pub struct UserRelationship {
	user1: i64,
	user2: i64,
	relationship: i64,
}

pub struct UserRelationships {
	pub friends: Vec<(i64, String)>,
	pub friend_requests: Vec<(i64, String)>,
	pub friend_requests_received: Vec<(i64, String)>,
	pub blocked: Vec<(i64, String)>,
}

impl UserRelationships {
	fn new() -> UserRelationships {
		UserRelationships {
			friends: Vec::new(),
			friend_requests: Vec::new(),
			friend_requests_received: Vec::new(),
			blocked: Vec::new(),
		}
	}
}

impl DatabaseManager {
	pub fn new(config: Arc<RwLock<Config>>) -> DatabaseManager {
		let _ = fs::create_dir("db");

		// Reminder: the various integer types don't actually matter in sqlite3 as all integer types end up being INTEGER(64 bit signed)

		let conn = rusqlite::Connection::open("db/rpcnv4.db").expect("Failed to open \"db/rpcnv4.db\"!");
		conn.execute(
            "CREATE TABLE IF NOT EXISTS users ( userId INTEGER PRIMARY KEY NOT NULL, username TEXT NOT NULL COLLATE NOCASE, hash BLOB NOT NULL, salt BLOB NOT NULL, online_name TEXT NOT NULL, avatar_url TEXT NOT NULL, email TEXT NOT NULL, email_check TEXT NOT NULL, token TEXT NOT NULL, rst_token TEXT, flags UNSIGNED SMALLINT NOT NULL)",
            [],
		)
		.expect("Failed to create users table!");
		conn.execute(
			"CREATE TABLE IF NOT EXISTS friends ( user1 INTEGER NOT NULL, user2 INTEGER NOT NULL, status INTEGER NOT NULL, PRIMARY KEY(user1, user2), FOREIGN KEY(user1) REFERENCES users(userId), FOREIGN KEY(user2) REFERENCES users(userId))",
			[],
		)
		.expect("Failed to create friends table!");
		conn.execute(
			"CREATE TABLE IF NOT EXISTS dates ( user INTEGER NOT NULL, creation INTEGER NOT NULL, last_login INTEGER, token_last_sent INTEGER, rst_emit INTEGER, PRIMARY KEY(user), FOREIGN KEY(user) REFERENCES users(userId))",
			[],
		)
		.expect("Failed to create dates table!");
		conn.execute(
			"CREATE TABLE IF NOT EXISTS servers ( communicationId TEXT NOT NULL, serverId INTEGER NOT NULL, PRIMARY KEY(communicationId, serverId))",
			[],
		)
		.expect("Failed to create servers table!");
		conn.execute(
            "CREATE TABLE IF NOT EXISTS worlds ( communicationId TEXT NOT NULL, serverId INTEGER NOT NULL, worldId INTEGER NOT NULL, PRIMARY KEY(communicationId, worldId), FOREIGN KEY(communicationId, serverId) REFERENCES servers(communicationId, serverId))",
            [],
        )
        .expect("Failed to create worlds table!");
		conn.execute(
            "CREATE TABLE IF NOT EXISTS lobbies ( communicationId TEXT NOT NULL, worldId INTEGER NOT NULL, lobbyId INTEGER NOT NULL, PRIMARY KEY(communicationId, lobbyId), FOREIGN KEY(communicationId, worldId) REFERENCES worlds(communicationId, worldId))",
            [],
        )
        .expect("Failed to create lobbies table!");
		DatabaseManager { config, conn }
	}

	pub fn add_user(&mut self, username: &str, password: &str, online_name: &str, avatar_url: &str, email: &str, email_check: &str) -> Result<String, DbError> {
		let count: rusqlite::Result<i64> = self.conn.query_row("SELECT COUNT(*) FROM users WHERE username=?1", rusqlite::params![username], |r| r.get(0));
		if let Err(e) = count {
			error!("Unexpected error querying username count: {}", e);
			return Err(DbError::Internal);
		}
		if count.unwrap() != 0 {
			warn!("Attempted to create an already existing user({})", username);
			return Err(DbError::ExistingUsername);
		}

		let count_email: rusqlite::Result<i64> = self.conn.query_row("SELECT COUNT(*) FROM users WHERE email_check=?1", rusqlite::params![email_check], |r| r.get(0));
		if let Err(e) = count_email {
			error!("Unexpected error querying email count: {}", e);
			return Err(DbError::Internal);
		}
		if count_email.unwrap() != 0 {
			warn!("Attempted to create an account with an already existing email({})", email);
			return Err(DbError::ExistingEmail);
		}

		let mut rng_gen = StdRng::from_entropy();

		let mut salt = [0; 64];
		rng_gen.fill_bytes(&mut salt);

		let config = argon2::Config::default();
		let hash = argon2::hash_raw(password.as_bytes(), &salt, &config).expect("Password hashing failed!");

		let salt_slice = &salt[..];

		let mut token = [0; 8];
		rng_gen.fill_bytes(&mut token);
		let mut token_str = String::new();
		for t in &token {
			token_str += &format!("{:02X}", t);
		}

		let flags: u16 = 0;

		if let Err(e) = self.conn.execute(
			"INSERT INTO users ( username, hash, salt, online_name, avatar_url, email, email_check, token, flags ) VALUES ( ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9 )",
			rusqlite::params![username, hash, salt_slice, online_name, avatar_url, email, email_check, token_str, flags],
		) {
			error!("Unexpected error inserting a new user: {}", e);
			Err(DbError::Internal)
		} else {
			let user_id = self.get_user_id(username).unwrap();
			let timestamp = Client::get_timestamp_seconds();
			if let Err(e) = self.conn.execute("INSERT INTO dates ( user, creation ) VALUES ( ?1, ?2 )", rusqlite::params![user_id, timestamp]) {
				error!("Unexpected error inserting entry into dates: {}", e);
				Err(DbError::Internal)
			} else {
				Ok(token_str)
			}
		}
	}
	pub fn check_user(&mut self, username: &str, password: &str, token: &str, check_token: bool) -> Result<UserQueryResult, DbError> {
		let res: rusqlite::Result<UserQueryResult> = self.conn.query_row(
			"SELECT userId, hash, salt, online_name, avatar_url, email, token, flags FROM users WHERE username=?1",
			rusqlite::params![username],
			|r| {
				Ok(UserQueryResult {
					user_id: r.get(0).unwrap(),
					hash: r.get(1).unwrap(),
					salt: r.get(2).unwrap(),
					online_name: r.get(3).unwrap(),
					avatar_url: r.get(4).unwrap(),
					email: r.get(5).unwrap(),
					token: r.get(6).unwrap(),
					flags: r.get(7).unwrap(),
				})
			},
		);

		if let Err(e) = res {
			if e == rusqlite::Error::QueryReturnedNoRows {
				warn!("No row for username {} found", username);
				return Err(DbError::Empty);
			}

			error!("Unexpected error querying username row: {}", e);
			return Err(DbError::Internal);
		}

		let res = res.unwrap();

		if check_token {
			if res.token != token {
				return Err(DbError::WrongToken);
			}
		}

		let config = argon2::Config::default();
		let hash = argon2::hash_raw(password.as_bytes(), &res.salt, &config).expect("Password hashing failed!");

		if hash != res.hash {
			return Err(DbError::WrongPass);
		}

		Ok(res)
	}
	pub fn get_user_id(&mut self, npid: &str) -> Result<i64, DbError> {
		let res: rusqlite::Result<i64> = self.conn.query_row("SELECT userId FROM users WHERE username=?1", rusqlite::params![npid], |r| Ok(r.get(0).unwrap()));

		if let Err(e) = res {
			if e == rusqlite::Error::QueryReturnedNoRows {
				warn!("Attempted to get the user id of non existent username {}", npid);
				return Err(DbError::Empty);
			}
			error!("Unexpected error querying user id: {}", e);
			return Err(DbError::Internal);
		}

		Ok(res.unwrap())
	}
	pub fn get_username(&mut self, user_id: i64) -> Result<String, DbError> {
		let res: rusqlite::Result<String> = self.conn.query_row("SELECT username FROM users WHERE userId=?1", rusqlite::params![user_id], |r| Ok(r.get(0).unwrap()));

		if let Err(e) = res {
			if e == rusqlite::Error::QueryReturnedNoRows {
				warn!("Attempted to get the username of non existent userId {}", user_id);
				return Err(DbError::Empty);
			}
			error!("Unexpected error querying username: {}", e);
			return Err(DbError::Internal);
		}

		Ok(res.unwrap())
	}
	pub fn create_server(&mut self, com_id: &ComId, server_id: u16) -> Result<(), DbError> {
		let com_id_str = com_id_to_string(com_id);

		if server_id == 0 {
			warn!("Attempted to create an invalid server(0) for {}", com_id_str);
			return Err(DbError::Internal);
		}

		info!("Creating server {} for {}", server_id, com_id_str);
		if let Err(e) = self
			.conn
			.execute("INSERT INTO servers ( communicationId, serverId ) VALUES (?1, ?2)", rusqlite::params!(com_id_str, server_id))
		{
			error!("Unexpected error creating server({}:{}): {}", &com_id_str, server_id, e);
			return Err(DbError::Internal);
		}

		Ok(())
	}

	pub fn get_server_list(&mut self, com_id: &ComId) -> Result<Vec<u16>, DbError> {
		let com_id_str = com_id_to_string(com_id);
		let mut list_servers = Vec::new();
		{
			let mut stmt = self.conn.prepare("SELECT serverId FROM servers WHERE communicationId=?1").unwrap();
			let server_iter = stmt.query_map(rusqlite::params![com_id_str], |r| r.get(0)).expect("Server query failed!");

			for sid in server_iter {
				list_servers.push(sid.unwrap());
			}
		}

		if list_servers.len() == 0 && self.config.read().is_create_missing() {
			// Creates a server so the server list is not empty
			self.create_server(com_id, 1)?;
			return self.get_server_list(com_id);
		}

		Ok(list_servers)
	}
	pub fn get_world_list(&mut self, com_id: &ComId, server_id: u16) -> Result<Vec<u32>, DbError> {
		let com_id_str = com_id_to_string(com_id);
		// Ensures server exists
		{
			let count: rusqlite::Result<i64> = self
				.conn
				.query_row("SELECT COUNT(1) FROM servers WHERE communicationId=?1 AND serverId=?2", rusqlite::params![com_id_str, server_id], |r| {
					r.get(0)
				});
			if let Err(e) = count {
				error!("Unexpected error querying for server existence: {}", e);
				return Err(DbError::Internal);
			}

			if count.unwrap() == 0 {
				// Some games request a specifically hardcoded server, just create it for them
				if self.config.read().is_create_missing() {
					self.create_server(com_id, server_id)?;
					return self.get_world_list(com_id, server_id);
				} else {
					warn!("Attempted to query world list on an unexisting server({}:{})", &com_id_str, server_id);
					return Err(DbError::Empty);
				}
			}
		}

		let mut list_worlds = Vec::new();
		{
			let mut stmt = self.conn.prepare("SELECT worldId FROM worlds WHERE communicationId=?1 AND serverId=?2").unwrap();
			let world_iter = stmt.query_map(rusqlite::params![com_id_str, server_id], |r| r.get(0)).expect("World query failed!");

			for wid in world_iter {
				list_worlds.push(wid.unwrap());
			}
		}

		if list_worlds.len() == 0 && self.config.read().is_create_missing() {
			// Create a world so that the list is not empty
			let cur_max_res: rusqlite::Result<u32> = self
				.conn
				.query_row("SELECT MAX(worldId) FROM worlds WHERE communicationId=?1", rusqlite::params![com_id_str], |r| r.get(0));

			let mut new_wid = 1;
			if cur_max_res.is_ok() {
				new_wid = cur_max_res.unwrap() + 1;
			}

			info!("Creating world {} for server {}:{}", new_wid, &com_id_str, server_id);
			if let Err(e) = self.conn.execute(
				"INSERT INTO worlds ( communicationId, serverId, worldId ) VALUES (?1, ?2, ?3)",
				rusqlite::params!(com_id_str, server_id, new_wid),
			) {
				error!("Unexpected error inserting a world: {}", e);
				return Err(DbError::Internal);
			}
			return self.get_world_list(com_id, server_id);
		}

		Ok(list_worlds)
	}
	pub fn get_corresponding_server(&mut self, com_id: &ComId, world_id: u32, lobby_id: u64) -> Result<u16, rusqlite::Error> {
		let com_id_str = com_id_to_string(com_id);

		if world_id == 0 && lobby_id == 0 {
			warn!("Attempted to get server for com_id {} from world(0) and lobby(0)", &com_id_str);
			return Err(rusqlite::Error::InvalidQuery);
		}

		let mut world_id = world_id;

		if world_id == 0 {
			world_id = self.conn.query_row(
				"SELECT (worldId) FROM lobbies WHERE communicationId = ?1 AND lobbyId = ?2",
				rusqlite::params![com_id_str, lobby_id as i64],
				|r| r.get(0),
			)?;
		}

		let serv: rusqlite::Result<u16> = self.conn.query_row(
			"SELECT (serverId) FROM worlds WHERE communicationId = ?1 AND worldId = ?2",
			rusqlite::params![com_id_str, world_id],
			|r| r.get(0),
		);

		serv
	}
	fn get_ordered_userids(user1: i64, user2: i64) -> (i64, i64, bool) {
		if user1 < user2 {
			(user1, user2, false)
		} else {
			(user2, user1, true)
		}
	}
	pub fn get_rel_status(&mut self, user_id: i64, friend_user_id: i64) -> Result<(u16, u16), DbError> {
		if user_id == friend_user_id {
			return Err(DbError::Invalid);
		}

		let (first, second, swapped) = DatabaseManager::get_ordered_userids(user_id, friend_user_id);

		let status_res: rusqlite::Result<u16> = self
			.conn
			.query_row("SELECT (status) FROM friends WHERE user1 = ?1 AND user2 = ?2", rusqlite::params![first, second], |r| r.get(0));

		match status_res {
			Err(rusqlite::Error::QueryReturnedNoRows) => return Err(DbError::Empty),
			Err(e) => {
				error!("Unexpected error querying relationship status: {}", e);
				return Err(DbError::Internal);
			}
			Ok(status) => {
				let (status_first, status_second) = (status >> 8, status & 0xFF);
				let (status_user, status_friend) = if swapped { (status_second, status_first) } else { (status_first, status_second) };
				return Ok((status_user, status_friend));
			}
		}
	}
	pub fn set_rel_status(&mut self, user_id: i64, friend_user_id: i64, status_user: u16, status_friend: u16) -> Result<(), DbError> {
		if user_id == friend_user_id {
			return Err(DbError::Invalid);
		}

		let (first, second, swapped) = DatabaseManager::get_ordered_userids(user_id, friend_user_id);

		let (status_first, status_second);
		if swapped {
			status_first = status_friend;
			status_second = status_user;
		} else {
			status_first = status_user;
			status_second = status_friend;
		}

		let status = (status_first << 8) | status_second;

		let update_res = self
			.conn
			.execute("REPLACE INTO friends ( user1, user2, status ) VALUES (?1, ?2, ?3)", rusqlite::params![first, second, status]);
		if let Err(e) = update_res {
			error!("Unexpected error updating relationship status: {}", e);
			return Err(DbError::Internal);
		}

		Ok(())
	}
	pub fn get_relationships(&mut self, user_id: i64) -> Result<UserRelationships, DbError> {
		let mut friends: Vec<i64> = Vec::new();
		let mut friend_requests: Vec<i64> = Vec::new();
		let mut friend_requests_received: Vec<i64> = Vec::new();
		let mut blocked: Vec<i64> = Vec::new();

		{
			let mut stmt = self
				.conn
				.prepare("SELECT user1, user2, status FROM friends WHERE user1 = ?1 OR user2 = ?1")
				.map_err(|_| DbError::Internal)?;
			let rels = stmt
				.query_map(rusqlite::params![user_id], |row| {
					let user1: i64 = row.get(0)?;
					let user2: i64 = row.get(1)?;
					let relationship: i64 = row.get(2)?;
					Ok(UserRelationship { user1, user2, relationship })
				})
				.map_err(|_| DbError::Internal)?;
			for rel in rels {
				let rel = rel.unwrap();

				let swap_status = |status| -> i64 { ((status & 0xFF) << 8) | ((status & 0xFF00) >> 8) };

				let (other_user, status_user_otheruser) = if rel.user1 != user_id {
					(rel.user1, swap_status(rel.relationship))
				} else {
					(rel.user2, rel.relationship)
				};

				const MUTUAL_FRIENDS: i64 = ((FriendStatus::Friend as u16) << 8 | FriendStatus::Friend as u16) as i64;
				const FRIEND_REQUEST_SENT: i64 = ((FriendStatus::Friend as u16) << 8) as i64;
				const FRIEND_REQUEST_RECEIVED: i64 = FriendStatus::Friend as i64;
				const BLOCKED_OTHER_USER: i64 = ((FriendStatus::Blocked as u16) << 8) as i64;

				match status_user_otheruser {
					MUTUAL_FRIENDS => friends.push(other_user),
					FRIEND_REQUEST_SENT => friend_requests.push(other_user),
					FRIEND_REQUEST_RECEIVED => friend_requests_received.push(other_user),
					_ => {}
				}

				if (status_user_otheruser & BLOCKED_OTHER_USER) != 0 {
					blocked.push(other_user);
				}
			}
		}

		let mut relationships = UserRelationships::new();

		let mut userid_to_username = |v_id: &Vec<i64>, v_username: &mut Vec<(i64, String)>| {
			for id in v_id {
				if let Ok(username) = self.get_username(*id) {
					v_username.push((*id, username));
				}
			}
		};

		userid_to_username(&friends, &mut relationships.friends);
		userid_to_username(&friend_requests, &mut relationships.friend_requests);
		userid_to_username(&friend_requests_received, &mut relationships.friend_requests_received);
		userid_to_username(&blocked, &mut relationships.blocked);

		Ok(relationships)
	}

	pub fn update_login_time(&mut self, user_id: i64) -> Result<(), DbError> {
		let timestamp = Client::get_timestamp_seconds();
		self.conn
			.execute("UPDATE dates SET last_login = ?1 WHERE user = ?2", rusqlite::params![timestamp, user_id])
			.map_err(|e| {
				error!("Unexpected error updating last login time: {}", e);
				DbError::Internal
			})?;
		Ok(())
	}

	pub fn get_token_sent_time(&mut self, user_id: i64) -> Result<Option<u32>, DbError> {
		self.conn
			.query_row("SELECT (token_last_sent) FROM dates WHERE user = ?1", rusqlite::params![user_id], |r| r.get(0))
			.map_err(|e| {
				error!("Unexpected error getting token sent time: {}", e);
				DbError::Internal
			})
	}

	pub fn set_token_sent_time(&mut self, user_id: i64) -> Result<(), DbError> {
		let timestamp = Client::get_timestamp_seconds();
		self.conn
			.execute("UPDATE dates SET token_last_sent = ?1 WHERE user = ?2", rusqlite::params![timestamp, user_id])
			.map_err(|e| {
				error!("Unexpected error updating token sent time: {}", e);
				DbError::Internal
			})?;
		Ok(())
	}
}
