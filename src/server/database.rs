use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Write;
use std::fs::File;
use std::io::Read;
use std::{fs, io};

use rand::prelude::*;
use tracing::{error, info, warn};

use crate::server::client::*;
use crate::server::Server;

pub struct Database {
	conn: r2d2::PooledConnection<r2d2_sqlite::SqliteConnectionManager>,
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
	ScoreNotBest,
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
	pub admin: bool,
	pub banned: bool,
	pub stat_agent: bool,
}

#[repr(u8)]
pub enum FriendStatus {
	Friend = (1 << 0),
	Blocked = (1 << 1),
}

pub struct UserRelationship {
	user_id_1: i64,
	user_id_2: i64,
	status_user_1: u8,
	status_user_2: u8,
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

#[derive(Clone)]
pub struct DbBoardInfo {
	pub rank_limit: u32,
	pub update_mode: u32,
	pub sort_mode: u32,
	pub upload_num_limit: u32,
	pub upload_size_limit: u32,
}

#[derive(Clone, Default)]
pub struct DbScoreInfo {
	pub user_id: i64,
	pub character_id: i32,
	pub score: i64,
	pub comment: Option<String>,
	pub game_info: Option<Vec<u8>>,
	pub data_id: Option<u64>,
	pub timestamp: u64,
}

struct MigrationData {
	version: u32,
	text: &'static str,
	function: fn(&r2d2::PooledConnection<r2d2_sqlite::SqliteConnectionManager>) -> Result<(), String>,
}

static DATABASE_PATH: &str = "db/rpcn.db";

static DATABASE_MIGRATIONS: [MigrationData; 1] = [MigrationData {
	version: 1,
	text: "Initial setup",
	function: initial_setup,
}];

fn initial_setup(conn: &r2d2::PooledConnection<r2d2_sqlite::SqliteConnectionManager>) -> Result<(), String> {
	// user_id is actually used internally as u64(UNSIGNED BIGINT) but needs to be INTEGER for AUTOINCREMENT
	conn.execute(
		"CREATE TABLE IF NOT EXISTS account ( user_id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT NOT NULL, hash BLOB NOT NULL, salt BLOB NOT NULL, online_name TEXT NOT NULL, avatar_url TEXT NOT NULL, email TEXT NOT NULL, email_check TEXT NOT NULL UNIQUE, token TEXT NOT NULL, reset_token TEXT, admin BOOL NOT NULL, stat_agent BOOL NOT NULL, banned BOOL NOT NULL, UNIQUE(username COLLATE NOCASE) )",
		[],
	)
	.map_err(|e| format!("Failed to create account table: {}", e))?;
	conn.execute(
		"CREATE TABLE IF NOT EXISTS friendship ( user_id_1 UNSIGNED BIGINT NOT NULL, user_id_2 UNSIGNED BIGINT NOT NULL, status_user_1 UNSIGNED TINYINT NOT NULL, status_user_2 UNSIGNED TINYINT NOT NULL, PRIMARY KEY(user_id_1, user_id_2), FOREIGN KEY(user_id_1) REFERENCES account(user_id), FOREIGN KEY(user_id_2) REFERENCES account(user_id), CHECK(user_id_1 < user_id_2) )",
		[],
	)
	.map_err(|e| format!("Failed to create friendship table: {}", e))?;
	conn.execute(
		"CREATE TABLE IF NOT EXISTS account_timestamp ( user_id UNSIGNED BIGINT NOT NULL, creation UNSIGNED INTEGER NOT NULL, last_login UNSIGNED INTEGER, token_last_sent UNSIGNED INTEGER, reset_emit UNSIGNED INTEGER, PRIMARY KEY(user_id), FOREIGN KEY(user_id) REFERENCES account(user_id) )",
		[],
	)
	.map_err(|e| format!("Failed to create account_timestamp table: {}", e))?;
	conn.execute(
		"CREATE TABLE IF NOT EXISTS server ( communication_id TEXT NOT NULL, server_id UNSIGNED SMALLINT NOT NULL, PRIMARY KEY(communication_id, server_id) )",
		[],
	)
	.map_err(|e| format!("Failed to create server table: {}", e))?;
	conn.execute(
		"CREATE TABLE IF NOT EXISTS world ( communication_id TEXT NOT NULL, server_id UNSIGNED SMALLINT NOT NULL, world_id UNSIGNED INTEGER NOT NULL, PRIMARY KEY(communication_id, world_id), FOREIGN KEY(communication_id, server_id) REFERENCES server(communication_id, server_id) )",
		[],
	)
	.map_err(|e| format!("Failed to create world table: {}", e))?;
	conn.execute(
		"CREATE TABLE IF NOT EXISTS lobby ( communication_id TEXT NOT NULL, world_id UNSIGNED INTEGER NOT NULL, lobby_id UNSIGNED BIGINT NOT NULL, PRIMARY KEY(communication_id, lobby_id), FOREIGN KEY(communication_id, world_id) REFERENCES world(communication_id, world_id) )",
		[],
	)
	.map_err(|e| format!("Failed to create lobby table: {}", e))?;
	conn.execute(
		"CREATE TABLE IF NOT EXISTS score_table ( communication_id TEXT NOT NULL, board_id UNSIGNED INTEGER NOT NULL, rank_limit UNSIGNED INTEGER NOT NULL, update_mode UNSIGNED INTEGER NOT NULL, sort_mode UNSIGNED INTEGER NOT NULL, upload_num_limit UNSIGNED INTEGER NOT NULL, upload_size_limit UNSIGNED INTEGER NOT NULL, PRIMARY KEY (communication_id, board_id) )",
		[],
	)
	.map_err(|e| format!("Failed to create score_table table: {}", e))?;

	Ok(())
}

impl Server {
	pub fn initialize_database() -> Result<r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>, String> {
		match fs::create_dir("db") {
			Ok(_) => {}
			Err(e) => match e.kind() {
				io::ErrorKind::AlreadyExists => {}
				other_error => {
					return Err(format!("Failed to create db directory: {}", other_error));
				}
			},
		}

		let manager = r2d2_sqlite::SqliteConnectionManager::file(DATABASE_PATH);
		let pool = r2d2::Pool::builder()
			.max_size(50)
			.connection_timeout(std::time::Duration::from_secs(60))
			.build(manager)
			.map_err(|e| format!("Error creating database pool: {}", e))?;

		let conn = pool.get().map_err(|e| format!("Error getting connection: {}", e))?;

		// Reminder: the various integer types don't actually matter in sqlite3 as all integer types end up being INTEGER(max 64 bit signed)
		// The types are there only as indications of actual values expected(and for optional db change later)

		conn.execute("CREATE TABLE IF NOT EXISTS migration ( migration_id UNSIGNED INTEGER PRIMARY KEY, description TEXT NOT NULL )", [])
			.map_err(|e| format!("Failed to create migration table: {}", e))?;

		let mut stmt = conn.prepare("SELECT * FROM migration").map_err(|e| format!("Failed to prepare statement to query migrations: {}", e))?;
		let applied_migrations: BTreeMap<u32, String> = stmt
			.query_map([], |row| {
				let migration_id: u32 = row.get(0)?;
				let description: String = row.get(1)?;
				Ok((migration_id, description))
			})
			.map_err(|e| format!("Failed to query migrations: {}", e))?
			.collect::<Result<BTreeMap<u32, String>, _>>()
			.map_err(|e| format!("Some of the migration queries failed: {}", e))?;

		for mig in &DATABASE_MIGRATIONS {
			if applied_migrations.contains_key(&mig.version) {
				continue;
			}

			println!("Applying database migration version {} : {}", mig.version, mig.text);

			let backup_path = format!("db/backup_before_v{}_{}.db", mig.version, Client::get_timestamp_seconds());
			println!("Backing up database to {}", backup_path);
			// Maybe switch to using r2d2 backup function here?
			fs::copy(DATABASE_PATH, backup_path).map_err(|e| format!("Failed to backup database: {}", e))?;
			(mig.function)(&conn)?;

			conn.execute("INSERT INTO migration ( migration_id, description ) VALUES ( ?1, ?2 )", rusqlite::params![mig.version, mig.text])
				.map_err(|e| format!("Failed to update migration table: {}", e))?;

			println!("Successfully applied migration to version {}", mig.version);
		}

		// Load presets for game servers configurations
		let file = File::open("servers.cfg");

		match file {
			Ok(mut file) => {
				let mut buf_file = String::new();
				file.read_to_string(&mut buf_file).map_err(|e| format!("Failed to read servers.cfg: {}", e))?;

				let config_servers: Vec<(String, u16, u32, Vec<u64>)> = buf_file
					.lines()
					.filter_map(|l| {
						if l.trim().is_empty() || l.chars().nth(0).unwrap() == '#' {
							return None;
						}

						let servers_infos: Vec<&str> = l.trim().split('|').collect();
						if servers_infos.len() != 4 || servers_infos[0].len() != 9 {
							println!("servers.cfg: line({}) was considered invalid and was skipped", l);
							return None;
						}

						let com_id = servers_infos[0].to_owned();
						let server = servers_infos[1].parse::<u16>();
						let world = servers_infos[2].parse::<u32>();
						let lobbies: Result<Vec<u64>, _> = if servers_infos[3].trim().is_empty() {
							Ok(Vec::new())
						} else {
							servers_infos[3].trim().split(',').map(|s| s.parse::<u64>()).collect()
						};

						if server.is_err() || world.is_err() || lobbies.is_err() {
							println!("servers.cfg: line({}) couldn't be parsed and was skipped", l);
							return None;
						}

						Some((com_id, server.unwrap(), world.unwrap(), lobbies.unwrap()))
					})
					.collect();

				for (com_id, server, world, lobbies) in &config_servers {
					let _ = conn.execute("INSERT INTO server ( communication_id, server_id ) VALUES (?1, ?2)", rusqlite::params![com_id, server]);
					let _ = conn.execute(
						"INSERT INTO world ( communication_id, server_id, world_id ) VALUES (?1, ?2, ?3)",
						rusqlite::params![com_id, server, world],
					);
					for lobby in lobbies {
						let _ = conn.execute("INSERT INTO lobby (communication_id, world_id, lobby_id) VALUES (?1, ?2, ?3)", rusqlite::params![com_id, world, lobby]);
					}
				}
			}
			Err(e) => match e.kind() {
				std::io::ErrorKind::NotFound => {}
				_ => return Err(format!("Unexpected error opening servers.cfg: {}", e)),
			},
		}

		Ok(pool)
	}
}

impl Default for DbBoardInfo {
	fn default() -> DbBoardInfo {
		DbBoardInfo {
			rank_limit: 100,
			update_mode: 0, // SCE_NP_SCORE_NORMAL_UPDATE
			sort_mode: 0,   // SCE_NP_SCORE_DESCENDING_ORDER
			upload_num_limit: 10,
			upload_size_limit: 6_000_000, // 6MB
		}
	}
}

fn hash_password(rng_gen: &mut StdRng, password: &str) -> (Vec<u8>, [u8; 64]) {
	let mut salt = [0; 64];
	rng_gen.fill_bytes(&mut salt);

	let config = argon2::Config::default();
	let hash = argon2::hash_raw(password.as_bytes(), &salt, &config).expect("Password hashing failed!");

	(hash, salt)
}

fn generate_token(rng_gen: &mut StdRng) -> String {
	let mut token = [0; 8];
	rng_gen.fill_bytes(&mut token);
	let mut token_str = String::new();
	for t in &token {
		let _ = write!(token_str, "{:02X}", t);
	}

	token_str
}

impl Database {
	pub fn new(conn: r2d2::PooledConnection<r2d2_sqlite::SqliteConnectionManager>) -> Database {
		Database { conn }
	}

	pub fn add_user(&self, username: &str, password: &str, online_name: &str, avatar_url: &str, email: &str, email_check: &str) -> Result<String, DbError> {
		let mut rng_gen = StdRng::from_entropy();
		let (hash, salt) = hash_password(&mut rng_gen, password);

		let salt_slice = &salt[..];
		let token_str = generate_token(&mut rng_gen);

		let admin = false;
		let stat_agent = false;
		let banned = false;

		if let Err(e) = self.conn.execute(
			"INSERT INTO account ( username, hash, salt, online_name, avatar_url, email, email_check, token, admin, stat_agent, banned ) VALUES ( ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11 )",
			rusqlite::params![username, hash, salt_slice, online_name, avatar_url, email, email_check, token_str, admin, stat_agent, banned],
		) {
			if let rusqlite::Error::SqliteFailure(error, ref msg) = e {
				if error.code == rusqlite::ErrorCode::ConstraintViolation {
					if let Some(msg) = msg {
						if msg.contains("account.username") {
							warn!("Attempted to create an already existing user({})", username);
							return Err(DbError::ExistingUsername);
						}
						if msg.contains("account.email_check") {
							warn!("Attempted to create an account with an already existing email({})", email);
							return Err(DbError::ExistingEmail);
						}
					}
				}
			}
			error!("Unexpected error inserting a new user: {:?}", e);
			Err(DbError::Internal)
		} else {
			let user_id = self.get_user_id(username).unwrap();
			let timestamp = Client::get_timestamp_seconds();
			if let Err(e) = self
				.conn
				.execute("INSERT INTO account_timestamp ( user_id, creation ) VALUES ( ?1, ?2 )", rusqlite::params![user_id, timestamp])
			{
				error!("Unexpected error inserting entry into account_timestamp: {}", e);
				Err(DbError::Internal)
			} else {
				Ok(token_str)
			}
		}
	}
	pub fn update_user_password(&self, user_id: i64, old_token: &str, new_password: &str) -> Result<(), DbError> {
		let mut rng_gen = StdRng::from_entropy();
		let (hash, salt) = hash_password(&mut rng_gen, new_password);
		let token_str = generate_token(&mut rng_gen);

		let salt_slice = &salt[..];

		self.conn
			.execute(
				"UPDATE account SET hash = ?1, salt = ?2, reset_token = ?3 WHERE user_id = ?4 AND reset_token = ?5",
				rusqlite::params![hash, salt_slice, token_str, user_id, old_token],
			)
			.map_err(|e| {
				error!("Unexpected error updating password: {}", e);
				DbError::Internal
			})?;
		Ok(())
	}
	pub fn update_password_token(&self, user_id: i64) -> Result<String, DbError> {
		let mut rng_gen = StdRng::from_entropy();
		let token_str = generate_token(&mut rng_gen);

		self.conn
			.execute("UPDATE account SET reset_token = ?1 WHERE user_id = ?2", rusqlite::params![token_str, user_id])
			.map_err(|e| {
				error!("Unexpected error updating password reset token: {}", e);
				DbError::Internal
			})?;
		Ok(token_str)
	}
	pub fn check_user(&self, username: &str, password: &str, token: &str, check_token: bool) -> Result<UserQueryResult, DbError> {
		let res: rusqlite::Result<UserQueryResult> = self.conn.query_row(
			"SELECT user_id, hash, salt, online_name, avatar_url, email, token, admin, stat_agent, banned FROM account WHERE username=?1",
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
					admin: r.get(7).unwrap(),
					stat_agent: r.get(8).unwrap(),
					banned: r.get(9).unwrap(),
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

		if check_token && res.token != token {
			return Err(DbError::WrongToken);
		}

		let config = argon2::Config::default();
		let hash = argon2::hash_raw(password.as_bytes(), &res.salt, &config).expect("Password hashing failed!");

		if hash != res.hash {
			return Err(DbError::WrongPass);
		}

		Ok(res)
	}
	pub fn check_email(&self, username: &str, email: &str) -> Result<(i64, String), DbError> {
		let res: rusqlite::Result<(i64, String)> = self
			.conn
			.query_row("SELECT user_id, email FROM account WHERE username=?1 AND email_check=?2", rusqlite::params![username, email], |r| {
				Ok((r.get_unwrap(0), r.get_unwrap(1)))
			});

		if let Err(e) = res {
			if e == rusqlite::Error::QueryReturnedNoRows {
				warn!("No row for username {} and email {} found", username, email);
				return Err(DbError::Empty);
			}
			error!("Unexpected error querying username row: {}", e);
			return Err(DbError::Internal);
		}

		Ok(res.unwrap())
	}
	pub fn check_reset_token(&self, username: &str, reset_token: &str) -> Result<i64, DbError> {
		let res: rusqlite::Result<i64> = self
			.conn
			.query_row("SELECT user_id FROM account WHERE username=?1 AND reset_token=?2", rusqlite::params![username, reset_token], |r| {
				Ok(r.get_unwrap(0))
			});
		if let Err(e) = res {
			if e == rusqlite::Error::QueryReturnedNoRows {
				warn!("Reset token for username {} was invalid", username);
				return Err(DbError::Empty);
			}
			error!("Unexpected error checking reset token: {}", e);
			return Err(DbError::Internal);
		}

		Ok(res.unwrap())
	}
	pub fn get_user_id(&self, npid: &str) -> Result<i64, DbError> {
		let res: rusqlite::Result<i64> = self.conn.query_row("SELECT user_id FROM account WHERE username=?1", rusqlite::params![npid], |r| Ok(r.get(0).unwrap()));

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
	pub fn get_username(&self, user_id: i64) -> Result<String, DbError> {
		let res: rusqlite::Result<String> = self
			.conn
			.query_row("SELECT username FROM account WHERE user_id=?1", rusqlite::params![user_id], |r| Ok(r.get(0).unwrap()));

		if let Err(e) = res {
			if e == rusqlite::Error::QueryReturnedNoRows {
				warn!("Attempted to get the username of non existent user_id {}", user_id);
				return Err(DbError::Empty);
			}
			error!("Unexpected error querying username: {}", e);
			return Err(DbError::Internal);
		}

		Ok(res.unwrap())
	}
	pub fn get_username_and_online_name_from_user_ids(&self, user_list: &HashSet<i64>) -> Result<Vec<(i64, String, String)>, DbError> {
		let mut stmt = self.conn.prepare("SELECT username, online_name FROM account WHERE user_id = ?1").map_err(|_| DbError::Internal)?;
		Ok(user_list
			.iter()
			.map(|u| stmt.query_row(rusqlite::params![u], |r| Ok((*u, r.get_unwrap(0), r.get_unwrap(1)))))
			.filter_map(|r| r.ok())
			.collect())
	}
	pub fn create_server(&self, com_id: &ComId, server_id: u16) -> Result<(), DbError> {
		let com_id_str = com_id_to_string(com_id);

		if server_id == 0 {
			warn!("Attempted to create an invalid server(0) for {}", com_id_str);
			return Err(DbError::Internal);
		}

		info!("Creating server {} for {}", server_id, com_id_str);
		if let Err(e) = self
			.conn
			.execute("INSERT INTO server ( communication_id, server_id ) VALUES (?1, ?2)", rusqlite::params![com_id_str, server_id])
		{
			error!("Unexpected error creating server({}:{}): {}", &com_id_str, server_id, e);
			return Err(DbError::Internal);
		}

		Ok(())
	}

	pub fn get_server_list(&self, com_id: &ComId, create_missing: bool) -> Result<Vec<u16>, DbError> {
		let com_id_str = com_id_to_string(com_id);
		let mut list_servers = Vec::new();
		{
			let mut stmt = self.conn.prepare("SELECT server_id FROM server WHERE communication_id = ?1").unwrap();
			let server_iter = stmt.query_map(rusqlite::params![com_id_str], |r| r.get(0)).expect("Server query failed!");

			for sid in server_iter {
				list_servers.push(sid.unwrap());
			}
		}

		if list_servers.is_empty() && create_missing {
			// Creates a server so the server list is not empty
			self.create_server(com_id, 1)?;
			return self.get_server_list(com_id, false);
		}

		Ok(list_servers)
	}
	pub fn get_world_list(&self, com_id: &ComId, server_id: u16, create_missing: bool) -> Result<Vec<u32>, DbError> {
		let com_id_str = com_id_to_string(com_id);
		// Ensures server exists
		{
			let count: rusqlite::Result<i64> = self.conn.query_row(
				"SELECT COUNT(*) FROM server WHERE communication_id=?1 AND server_id=?2",
				rusqlite::params![com_id_str, server_id],
				|r| r.get(0),
			);
			if let Err(e) = count {
				error!("Unexpected error querying for server existence: {}", e);
				return Err(DbError::Internal);
			}

			if count.unwrap() == 0 {
				// Some games request a specifically hardcoded server, just create it for them
				if create_missing {
					self.create_server(com_id, server_id)?;
					return self.get_world_list(com_id, server_id, false);
				} else {
					warn!("Attempted to query world list on an unexisting server({}:{})", &com_id_str, server_id);
					return Err(DbError::Empty);
				}
			}
		}

		let mut list_worlds = Vec::new();
		{
			let mut stmt = self.conn.prepare("SELECT world_id FROM world WHERE communication_id=?1 AND server_id=?2").unwrap();
			let world_iter = stmt.query_map(rusqlite::params![com_id_str, server_id], |r| r.get(0)).expect("World query failed!");

			for wid in world_iter {
				list_worlds.push(wid.unwrap());
			}
		}

		if list_worlds.is_empty() && create_missing {
			// Create a world so that the list is not empty
			let cur_max_res: rusqlite::Result<u32> = self
				.conn
				.query_row("SELECT MAX(world_id) FROM world WHERE communication_id = ?1", rusqlite::params![com_id_str], |r| r.get(0));

			let mut new_wid = 1;
			if let Ok(cur_max_res) = cur_max_res {
				new_wid = cur_max_res + 1;
			}

			info!("Creating world {} for server {}:{}", new_wid, &com_id_str, server_id);
			if let Err(e) = self.conn.execute(
				"INSERT INTO world ( communication_id, server_id, world_id ) VALUES (?1, ?2, ?3)",
				rusqlite::params![com_id_str, server_id, new_wid],
			) {
				error!("Unexpected error inserting a world: {}", e);
				return Err(DbError::Internal);
			}
			return self.get_world_list(com_id, server_id, false);
		}

		Ok(list_worlds)
	}
	pub fn get_corresponding_server(&self, com_id: &ComId, world_id: u32, lobby_id: u64) -> Result<u16, rusqlite::Error> {
		let com_id_str = com_id_to_string(com_id);

		if world_id == 0 && lobby_id == 0 {
			warn!("Attempted to get server for com_id {} from world(0) and lobby(0)", &com_id_str);
			return Err(rusqlite::Error::InvalidQuery);
		}

		let mut world_id = world_id;

		if world_id == 0 {
			world_id = self.conn.query_row(
				"SELECT world_id FROM lobby WHERE communication_id = ?1 AND lobby_id = ?2",
				rusqlite::params![com_id_str, lobby_id as i64],
				|r| r.get(0),
			)?;
		}

		let serv: rusqlite::Result<u16> = self.conn.query_row(
			"SELECT server_id FROM world WHERE communication_id = ?1 AND world_id = ?2",
			rusqlite::params![com_id_str, world_id],
			|r| r.get(0),
		);

		serv
	}
	fn get_ordered_userids(user_id_1: i64, user_id_2: i64) -> (i64, i64, bool) {
		if user_id_1 < user_id_2 {
			(user_id_1, user_id_2, false)
		} else {
			(user_id_2, user_id_1, true)
		}
	}
	pub fn get_rel_status(&self, user_id: i64, friend_user_id: i64) -> Result<(u8, u8), DbError> {
		if user_id == friend_user_id {
			return Err(DbError::Invalid);
		}

		let (first, second, swapped) = Database::get_ordered_userids(user_id, friend_user_id);

		let status_res: rusqlite::Result<(u8, u8)> = self.conn.query_row(
			"SELECT status_user_1, status_user_2 FROM friendship WHERE user_id_1 = ?1 AND user_id_2 = ?2",
			rusqlite::params![first, second],
			|r| Ok((r.get(0)?, r.get(1)?)),
		);

		match status_res {
			Err(rusqlite::Error::QueryReturnedNoRows) => Err(DbError::Empty),
			Err(e) => {
				error!("Unexpected error querying relationship status: {}", e);
				Err(DbError::Internal)
			}
			Ok((status_first, status_second)) => {
				let (status_user, status_friend) = if swapped { (status_second, status_first) } else { (status_first, status_second) };
				Ok((status_user, status_friend))
			}
		}
	}
	pub fn set_rel_status(&self, user_id: i64, friend_user_id: i64, status_user: u8, status_friend: u8) -> Result<(), DbError> {
		if user_id == friend_user_id {
			return Err(DbError::Invalid);
		}

		let (first, second, swapped) = Database::get_ordered_userids(user_id, friend_user_id);

		let (status_first, status_second);
		if swapped {
			status_first = status_friend;
			status_second = status_user;
		} else {
			status_first = status_user;
			status_second = status_friend;
		}

		let update_res = self.conn.execute(
			"REPLACE INTO friendship ( user_id_1, user_id_2, status_user_1, status_user_2 ) VALUES (?1, ?2, ?3, ?4)",
			rusqlite::params![first, second, status_first, status_second],
		);
		if let Err(e) = update_res {
			error!("Unexpected error updating relationship status: {}", e);
			return Err(DbError::Internal);
		}

		Ok(())
	}
	pub fn get_relationships(&self, user_id: i64) -> Result<UserRelationships, DbError> {
		let mut friends: Vec<i64> = Vec::new();
		let mut friend_requests: Vec<i64> = Vec::new();
		let mut friend_requests_received: Vec<i64> = Vec::new();
		let mut blocked: Vec<i64> = Vec::new();

		{
			let mut stmt = self
				.conn
				.prepare("SELECT user_id_1, user_id_2, status_user_1, status_user_2 FROM friendship WHERE user_id_1 = ?1 OR user_id_2 = ?1")
				.map_err(|_| DbError::Internal)?;
			let rels = stmt
				.query_map(rusqlite::params![user_id], |row| {
					let user_id_1: i64 = row.get(0)?;
					let user_id_2: i64 = row.get(1)?;
					let status_user_1: u8 = row.get(2)?;
					let status_user_2: u8 = row.get(3)?;
					Ok(UserRelationship {
						user_id_1,
						user_id_2,
						status_user_1,
						status_user_2,
					})
				})
				.map_err(|_| DbError::Internal)?;
			for rel in rels {
				let rel = rel.unwrap();

				let (user_other, status_user, status_other) = if rel.user_id_1 != user_id {
					(rel.user_id_1, rel.status_user_2, rel.status_user_1)
				} else {
					(rel.user_id_2, rel.status_user_1, rel.status_user_2)
				};

				const MUTUAL_FRIENDS: (u8, u8) = (FriendStatus::Friend as u8, FriendStatus::Friend as u8);
				const FRIEND_REQUEST_SENT: (u8, u8) = (FriendStatus::Friend as u8, 0);
				const FRIEND_REQUEST_RECEIVED: (u8, u8) = (0, FriendStatus::Friend as u8);

				match (status_user, status_other) {
					MUTUAL_FRIENDS => friends.push(user_other),
					FRIEND_REQUEST_SENT => friend_requests.push(user_other),
					FRIEND_REQUEST_RECEIVED => friend_requests_received.push(user_other),
					_ => {}
				}

				if (status_other & (FriendStatus::Blocked as u8)) != 0 {
					blocked.push(user_other);
				}
			}
		}

		let mut relationships = UserRelationships::new();

		let userid_to_username = |v_id: &Vec<i64>, v_username: &mut Vec<(i64, String)>| {
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

	pub fn update_login_time(&self, user_id: i64) -> Result<(), DbError> {
		let timestamp = Client::get_timestamp_seconds();
		self.conn
			.execute("UPDATE account_timestamp SET last_login = ?1 WHERE user_id = ?2", rusqlite::params![timestamp, user_id])
			.map_err(|e| {
				error!("Unexpected error updating last login time: {}", e);
				DbError::Internal
			})?;
		Ok(())
	}

	pub fn get_reset_password_token_sent_time(&self, user_id: i64) -> Result<Option<u32>, DbError> {
		self.conn
			.query_row("SELECT reset_emit FROM account_timestamp WHERE user_id = ?1", rusqlite::params![user_id], |r| r.get(0))
			.map_err(|e| {
				error!("Unexpected error getting reset emit time: {}", e);
				DbError::Internal
			})
	}

	pub fn set_reset_password_token_sent_time(&self, user_id: i64) -> Result<(), DbError> {
		let timestamp = Client::get_timestamp_seconds();
		self.conn
			.execute("UPDATE account_timestamp SET reset_emit = ?1 WHERE user_id = ?2", rusqlite::params![timestamp, user_id])
			.map_err(|e| {
				error!("Unexpected error updating reset emit time: {}", e);
				DbError::Internal
			})?;
		Ok(())
	}

	pub fn get_token_sent_time(&self, user_id: i64) -> Result<Option<u32>, DbError> {
		self.conn
			.query_row("SELECT token_last_sent FROM account_timestamp WHERE user_id = ?1", rusqlite::params![user_id], |r| r.get(0))
			.map_err(|e| {
				error!("Unexpected error getting token sent time: {}", e);
				DbError::Internal
			})
	}

	pub fn set_token_sent_time(&self, user_id: i64) -> Result<(), DbError> {
		let timestamp = Client::get_timestamp_seconds();
		self.conn
			.execute("UPDATE account_timestamp SET token_last_sent = ?1 WHERE user_id = ?2", rusqlite::params![timestamp, user_id])
			.map_err(|e| {
				error!("Unexpected error updating token sent time: {}", e);
				DbError::Internal
			})?;
		Ok(())
	}

	pub fn get_scoreboard_name(com_id: &ComId, board_id: u32) -> String {
		// Note that this generates a safe sql table name as ComId is checked in input handling to only contain uppercase+digits
		format!("{}_{}", com_id_to_string(com_id), board_id)
	}

	fn create_score_board(&self, com_id: &ComId, board_id: u32) -> Result<(), DbError> {
		let table_name = Database::get_scoreboard_name(com_id, board_id);
		self.conn.execute(&format!("CREATE TABLE IF NOT EXISTS {} ( user_id INTEGER NOT NULL, character_id INTEGER NOT NULL, score BIGINT NOT NULL, comment TEXT, game_info BLOB, data_id UNSIGNED BIGINT, timestamp UNSIGNED BIGINT NOT NULL, PRIMARY KEY(user_id, character_id), FOREIGN KEY(user_id) REFERENCES account(user_id) )", table_name),
			[],).map_err(|e| {
				error!("Error creating table in create_score_board: {}", e);
				DbError::Internal
			})?;

		let com_id_str = com_id_to_string(com_id);

		let default_boardinfo: DbBoardInfo = Default::default();

		if let Err(e) = self.conn.execute(
			"INSERT INTO score_table ( communication_id, board_id, rank_limit, update_mode, sort_mode, upload_num_limit, upload_size_limit ) VALUES ( ?1, ?2, ?3, ?4, ?5, ?6, ?7 )",
			rusqlite::params![
				com_id_str,
				board_id,
				default_boardinfo.rank_limit,
				default_boardinfo.update_mode,
				default_boardinfo.sort_mode,
				default_boardinfo.upload_num_limit,
				default_boardinfo.upload_size_limit,
			],
		) {
			match e {
				rusqlite::Error::StatementChangedRows(_) => {}
				_ => {
					error!("Unexpected error inserting scoreboard({}:{}) in score_table: {}", com_id_str, board_id, e);
					return Err(DbError::Internal);
				}
			}
		}

		Ok(())
	}

	pub fn get_score_tables(&self) -> Result<HashMap<String, DbBoardInfo>, DbError> {
		let mut stmt = self
			.conn
			.prepare("SELECT communication_id, board_id, rank_limit, update_mode, sort_mode, upload_num_limit, upload_size_limit FROM score_table")
			.map_err(|e| {
				println!("Failed to prepare statement: {}", e);
				DbError::Internal
			})?;
		let rows = stmt.query_map([], |r| {
			let com_id: String = r.get_unwrap(0);
			let board_id: u32 = r.get_unwrap(1);
			Ok((
				format!("{}_{}", com_id, board_id),
				DbBoardInfo {
					rank_limit: r.get_unwrap(2),
					update_mode: r.get_unwrap(3),
					sort_mode: r.get_unwrap(4),
					upload_num_limit: r.get_unwrap(5),
					upload_size_limit: r.get_unwrap(6),
				},
			))
		});

		let mut tables_map = HashMap::new();

		match rows {
			Err(rusqlite::Error::QueryReturnedNoRows) => {}
			Err(e) => {
				println!("Err: Failed to query scores: {}", e);
				return Err(DbError::Internal);
			}
			Ok(rows) => {
				for table in rows {
					let table = table.unwrap();
					tables_map.insert(table.0, table.1);
				}
			}
		}

		Ok(tables_map)
	}

	pub fn get_scores_from_table(&self, table_name: &str, table_info: &DbBoardInfo) -> Result<Vec<DbScoreInfo>, DbError> {
		let statement_str = if table_info.sort_mode == 0 {
			format!(
				"SELECT user_id, character_id, score, comment, game_info, data_id, timestamp FROM {} ORDER BY score DESC, timestamp ASC, user_id ASC LIMIT ?1",
				table_name
			)
		} else {
			format!(
				"SELECT user_id, character_id, score, comment, game_info, data_id, timestamp FROM {} ORDER BY score ASC, timestamp ASC, user_id ASC LIMIT ?1",
				table_name
			)
		};

		let mut stmt = self.conn.prepare(&statement_str).map_err(|_| DbError::Internal)?;
		let rows = stmt
			.query_map(rusqlite::params![table_info.rank_limit], |r| {
				Ok(DbScoreInfo {
					user_id: r.get_unwrap(0),
					character_id: r.get_unwrap(1),
					score: r.get_unwrap(2),
					comment: r.get_unwrap(3),
					game_info: r.get_unwrap(4),
					data_id: r.get_unwrap(5),
					timestamp: r.get_unwrap(6),
				})
			})
			.map_err(|_| DbError::Internal)?;

		let mut vec_scores = Vec::new();
		for row in rows {
			vec_scores.push(row.unwrap());
		}

		Ok(vec_scores)
	}

	pub fn get_board_infos(&self, com_id: &ComId, board_id: u32, create_missing: bool) -> Result<DbBoardInfo, DbError> {
		let com_id_str = com_id_to_string(com_id);

		let res = self.conn.query_row(
			"SELECT rank_limit, update_mode, sort_mode, upload_num_limit, upload_size_limit FROM score_table WHERE communication_id = ?1 AND board_id = ?2",
			rusqlite::params![com_id_str, board_id],
			|r| {
				Ok(DbBoardInfo {
					rank_limit: r.get_unwrap(0),
					update_mode: r.get_unwrap(1),
					sort_mode: r.get_unwrap(2),
					upload_num_limit: r.get_unwrap(3),
					upload_size_limit: r.get_unwrap(4),
				})
			},
		);

		if let Err(e) = res {
			if e == rusqlite::Error::QueryReturnedNoRows {
				if create_missing {
					self.create_score_board(com_id, board_id)?;
					return self.get_board_infos(com_id, board_id, false);
				}
				warn!("Attempted to query an unexisting score board({}:{})", &com_id_str, board_id);
				return Err(DbError::Empty);
			}

			error!("Unexpected error querying score board: {}", e);
			return Err(DbError::Internal);
		}

		Ok(res.unwrap())
	}

	pub fn record_score(&self, com_id: &ComId, board_id: u32, score_infos: &DbScoreInfo, create_missing: bool) -> Result<DbBoardInfo, DbError> {
		let board_infos = self.get_board_infos(com_id, board_id, create_missing)?;
		let table_name = Database::get_scoreboard_name(com_id, board_id);

		let query_str: String = if board_infos.update_mode == 0 {
			if board_infos.sort_mode == 0 {
				format!("INSERT INTO {} ( user_id, character_id, score, comment, game_info, timestamp ) VALUES ( ?1, ?2, ?3, ?4, ?5, ?6 ) ON CONFLICT ( user_id, character_id ) DO UPDATE SET score = excluded.score, comment = excluded.comment, game_info = excluded.game_info, timestamp = excluded.timestamp WHERE excluded.score >= score", table_name)
			} else {
				format!("INSERT INTO {} ( user_id, character_id, score, comment, game_info, timestamp ) VALUES ( ?1, ?2, ?3, ?4, ?5, ?6 ) ON CONFLICT ( user_id, character_id ) DO UPDATE SET score = excluded.score, comment = excluded.comment, game_info = excluded.game_info, timestamp = excluded.timestamp WHERE excluded.score <= score", table_name)
			}
		} else {
			format!("INSERT INTO {} ( user_id, character_id, score, comment, game_info, timestamp ) VALUES ( ?1, ?2, ?3, ?4, ?5, ?6 ) ON CONFLICT ( user_id, character_id ) DO UPDATE SET score = excluded.score, comment = excluded.comment, game_info = excluded.game_info, timestamp = excluded.timestamp", table_name)
		};

		let res = self.conn.execute(
			&query_str,
			rusqlite::params![
				score_infos.user_id,
				score_infos.character_id,
				score_infos.score,
				score_infos.comment,
				score_infos.game_info,
				score_infos.timestamp
			],
		);

		match res {
			Ok(n) => {
				if n == 1 {
					Ok(board_infos)
				} else {
					Err(DbError::ScoreNotBest)
				}
			}
			Err(e) => {
				error!("Unexpected error inserting score: {}", e);
				Err(DbError::Internal)
			}
		}
	}
}
