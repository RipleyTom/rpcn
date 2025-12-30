use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Write;
use std::fs::File;
use std::io::Read;
use std::{fs, io};

use rand::prelude::*;
use tracing::{error, info, warn};

use crate::server::Server;
use crate::server::client::*;

pub mod db_score;
pub mod db_tus;

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

impl std::fmt::Display for DbError {
	fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
		let error_str = match self {
			DbError::Internal => "Internal",
			DbError::Invalid => "Invalid",
			DbError::Empty => "Empty",
			DbError::WrongPass => "WrongPass",
			DbError::WrongToken => "WrongToken",
			DbError::ExistingUsername => "ExistingUsername",
			DbError::ExistingEmail => "ExistingEmail",
			DbError::ScoreNotBest => "ScoreNotBest",
		};
		write!(f, "DbError::{}", error_str)
	}
}

#[allow(dead_code)]
pub struct UserQueryResult {
	pub user_id: i64,
	hash: Vec<u8>,
	salt: Vec<u8>,
	pub online_name: String,
	pub avatar_url: String,
	pub email: String,
	pub email_check: String,
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

struct MigrationData {
	version: u32,
	text: &'static str,
	function: fn(&r2d2::PooledConnection<r2d2_sqlite::SqliteConnectionManager>) -> Result<(), String>,
}

static DATABASE_PATH: &str = "db/rpcn.db";

static DATABASE_MIGRATIONS: [MigrationData; 11] = [
	MigrationData {
		version: 1,
		text: "Initial setup",
		function: initial_setup,
	},
	MigrationData {
		version: 2,
		text: "Adding tables for TUS",
		function: add_tus_tables,
	},
	MigrationData {
		version: 3,
		text: "Unifying score tables",
		function: unify_score_tables,
	},
	MigrationData {
		version: 4,
		text: "Adding FOREIGN_KEY constraints to tus tables",
		function: add_foreign_key_to_tus_tables,
	},
	MigrationData {
		version: 5,
		text: "Adding indexes to tables",
		function: add_indexes,
	},
	MigrationData {
		version: 6,
		text: "Updating communication_id entries to show subid",
		function: update_communication_ids,
	},
	MigrationData {
		version: 7,
		text: "Change score rank limit's default",
		function: change_score_rank_limit_default,
	},
	MigrationData {
		version: 8,
		text: "Fixup for RR7 tables",
		function: ridge_racer_7_fixup,
	},
	MigrationData {
		version: 9,
		text: "Fixes WorldIDs",
		function: world_ids_fixup,
	},
	MigrationData {
		version: 10,
		text: "Cleanup server IDs",
		function: cleanup_server_ids,
	},
	MigrationData {
		version: 11,
		text: "Prepare tables for user deletion",
		function: prepare_for_deletion,
	},
];

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

fn add_tus_tables(conn: &r2d2::PooledConnection<r2d2_sqlite::SqliteConnectionManager>) -> Result<(), String> {
	conn.execute(
		"CREATE TABLE IF NOT EXISTS tus_var ( owner_id UNSIGNED BIGINT NOT NULL, communication_id TEXT NOT NULL, slot_id INTEGER NOT NULL, var_value BIGINT NOT NULL, timestamp UNSIGNED BIGINT NOT NULL, author_id UNSIGNED BIGINT NOT NULL, PRIMARY KEY (owner_id, communication_id, slot_id) )",
		[],
	)
	.map_err(|e| format!("Failed to create tus_var table: {}", e))?;
	conn.execute(
		"CREATE TABLE IF NOT EXISTS tus_data ( owner_id UNSIGNED BIGINT NOT NULL, communication_id TEXT NOT NULL, slot_id INTEGER NOT NULL, data_id UNSIGNED BIGINT NOT NULL, data_info BLOB NOT NULL, timestamp UNSIGNED BIGINT NOT NULL, author_id UNSIGNED BIGINT NOT NULL, PRIMARY KEY (owner_id, communication_id, slot_id) )",
		[],
	)
	.map_err(|e| format!("Failed to create tus_data table: {}", e))?;

	conn.execute(
		"CREATE TABLE IF NOT EXISTS tus_var_vuser ( vuser TEXT NOT NULL, communication_id TEXT NOT NULL, slot_id INTEGER NOT NULL, var_value BIGINT NOT NULL, timestamp UNSIGNED BIGINT NOT NULL, author_id UNSIGNED BIGINT NOT NULL, PRIMARY KEY (vuser, communication_id, slot_id) )",
		[],
	)
	.map_err(|e| format!("Failed to create tus_var_vuser table: {}", e))?;
	conn.execute(
		"CREATE TABLE IF NOT EXISTS tus_data_vuser ( vuser TEXT NOT NULL, communication_id TEXT NOT NULL, slot_id INTEGER NOT NULL, data_id UNSIGNED BIGINT NOT NULL, data_info BLOB NOT NULL, timestamp UNSIGNED BIGINT NOT NULL, author_id UNSIGNED BIGINT NOT NULL, PRIMARY KEY (vuser, communication_id, slot_id) )",
		[],
	)
	.map_err(|e| format!("Failed to create tus_data_vuser table: {}", e))?;

	Ok(())
}

fn unify_score_tables(conn: &r2d2::PooledConnection<r2d2_sqlite::SqliteConnectionManager>) -> Result<(), String> {
	conn.execute(
		"CREATE TABLE IF NOT EXISTS score ( communication_id TEXT NOT NULL, board_id UNSIGNED INTEGER NOT NULL, user_id INTEGER NOT NULL, character_id INTEGER NOT NULL, score BIGINT NOT NULL, comment TEXT, game_info BLOB, data_id UNSIGNED BIGINT UNIQUE, timestamp UNSIGNED BIGINT NOT NULL, PRIMARY KEY(communication_id, board_id, user_id, character_id), FOREIGN KEY(user_id) REFERENCES account(user_id) )",
		[],
	)
	.map_err(|e| format!("Failed to create score table: {}", e))?;

	let mut stmt_tables = conn
		.prepare("SELECT communication_id, board_id FROM score_table")
		.map_err(|e| format!("Failed to prepare statement to query existing score tables: {}", e))?;

	let score_tables: Vec<(String, u32)> = stmt_tables
		.query_map([], |row| Ok((row.get_unwrap(0), row.get_unwrap(1))))
		.map_err(|e| format!("Failed to query score tables: {}", e))?
		.collect::<Result<Vec<(String, u32)>, _>>()
		.map_err(|e| format!("Some of the score tables queries failed: {}", e))?;

	for (com_id, board_id) in &score_tables {
		let table_name = format!("{}_{}", com_id, board_id);
		let transfer_query = format!(
			"INSERT INTO score ( communication_id, board_id, user_id, character_id, score, comment, game_info, data_id, timestamp ) SELECT '{}', {}, user_id, character_id, score, comment, game_info, data_id, timestamp FROM {}",
			com_id, board_id, table_name
		);
		conn.execute(&transfer_query, []).map_err(|e| format!("Failed to transfer scores from table: {}", e))?;
		let delete_query = format!("DROP TABLE {}", table_name);
		conn.execute(&delete_query, []).map_err(|e| format!("Failed to delete table: {}", e))?;
	}

	if !score_tables.is_empty() {
		println!("Transferred {} table(s)", score_tables.len());
	}

	Ok(())
}

fn alter_table(conn: &r2d2::PooledConnection<r2d2_sqlite::SqliteConnectionManager>, table_name: &str, creation_query: &str, parameters: &str) -> Result<(), String> {
	let alter_query = format!("ALTER TABLE {} RENAME TO _old_{}", table_name, table_name);
	conn.execute(&alter_query, []).map_err(|e| format!("Failed to rename {}: {}", table_name, e))?;
	conn.execute(creation_query, []).map_err(|e| format!("Failed to create new {} table: {}", table_name, e))?;
	let insert_query = format!("INSERT INTO {} SELECT {} from _old_{}", table_name, parameters, table_name);
	conn.execute(&insert_query, []).map_err(|e| format!("Failed to copy {} data to new table: {}", table_name, e))?;
	let drop_query = format!("DROP TABLE _old_{}", table_name);
	conn.execute(&drop_query, []).map_err(|e| format!("Failed to drop old {} table: {}", table_name, e))?;
	Ok(())
}

fn add_foreign_key_to_tus_tables(conn: &r2d2::PooledConnection<r2d2_sqlite::SqliteConnectionManager>) -> Result<(), String> {
	alter_table(
		conn,
		"tus_var",
		"CREATE TABLE IF NOT EXISTS tus_var ( owner_id UNSIGNED BIGINT NOT NULL, communication_id TEXT NOT NULL, slot_id INTEGER NOT NULL, var_value BIGINT NOT NULL, timestamp UNSIGNED BIGINT NOT NULL, author_id UNSIGNED BIGINT NOT NULL, PRIMARY KEY (owner_id, communication_id, slot_id), FOREIGN KEY(owner_id) REFERENCES account(user_id), FOREIGN KEY(author_id) REFERENCES account(user_id) )",
		"*",
	)?;
	alter_table(
		conn,
		"tus_data",
		"CREATE TABLE IF NOT EXISTS tus_data ( owner_id UNSIGNED BIGINT NOT NULL, communication_id TEXT NOT NULL, slot_id INTEGER NOT NULL, data_id UNSIGNED BIGINT NOT NULL, data_info BLOB NOT NULL, timestamp UNSIGNED BIGINT NOT NULL, author_id UNSIGNED BIGINT NOT NULL, PRIMARY KEY (owner_id, communication_id, slot_id), FOREIGN KEY(owner_id) REFERENCES account(user_id), FOREIGN KEY(author_id) REFERENCES account(user_id) )",
		"*",
	)?;
	alter_table(
		conn,
		"tus_var_vuser",
		"CREATE TABLE IF NOT EXISTS tus_var_vuser ( vuser TEXT NOT NULL, communication_id TEXT NOT NULL, slot_id INTEGER NOT NULL, var_value BIGINT NOT NULL, timestamp UNSIGNED BIGINT NOT NULL, author_id UNSIGNED BIGINT NOT NULL, PRIMARY KEY (vuser, communication_id, slot_id), FOREIGN KEY(author_id) REFERENCES account(user_id) )",
		"*",
	)?;
	alter_table(
		conn,
		"tus_data_vuser",
		"CREATE TABLE IF NOT EXISTS tus_data_vuser ( vuser TEXT NOT NULL, communication_id TEXT NOT NULL, slot_id INTEGER NOT NULL, data_id UNSIGNED BIGINT NOT NULL, data_info BLOB NOT NULL, timestamp UNSIGNED BIGINT NOT NULL, author_id UNSIGNED BIGINT NOT NULL, PRIMARY KEY (vuser, communication_id, slot_id), FOREIGN KEY(author_id) REFERENCES account(user_id) )",
		"*",
	)?;

	Ok(())
}

fn add_indexes(conn: &r2d2::PooledConnection<r2d2_sqlite::SqliteConnectionManager>) -> Result<(), String> {
	conn.execute("CREATE INDEX IF NOT EXISTS friendship_user1 ON friendship(user_id_1)", [])
		.map_err(|e| format!("Error creating friendship_user1 index: {}", e))?;
	conn.execute("CREATE INDEX IF NOT EXISTS friendship_user2 ON friendship(user_id_2)", [])
		.map_err(|e| format!("Error creating friendship_user2 index: {}", e))?;

	conn.execute("CREATE INDEX IF NOT EXISTS score_user_id ON score(user_id)", [])
		.map_err(|e| format!("Error creating score_user_id index: {}", e))?;

	conn.execute("CREATE INDEX IF NOT EXISTS tus_var_owner_id ON tus_var(owner_id)", [])
		.map_err(|e| format!("Error creating tus_var_owner_id index: {}", e))?;
	conn.execute("CREATE INDEX IF NOT EXISTS tus_var_author_id ON tus_var(author_id)", [])
		.map_err(|e| format!("Error creating tus_var_author_id index: {}", e))?;

	conn.execute("CREATE INDEX IF NOT EXISTS tus_data_owner_id ON tus_data(owner_id)", [])
		.map_err(|e| format!("Error creating tus_data_owner_id index: {}", e))?;
	conn.execute("CREATE INDEX IF NOT EXISTS tus_data_author_id ON tus_data(author_id)", [])
		.map_err(|e| format!("Error creating tus_data_author_id index: {}", e))?;

	conn.execute("CREATE INDEX IF NOT EXISTS tus_var_vuser_author_id ON tus_var_vuser(author_id)", [])
		.map_err(|e| format!("Error creating tus_var_vuser_author_id index: {}", e))?;
	conn.execute("CREATE INDEX IF NOT EXISTS tus_data_vuser_author_id ON tus_data_vuser(author_id)", [])
		.map_err(|e| format!("Error creating tus_data_vuser_author_id index: {}", e))?;

	Ok(())
}

fn update_communication_ids(conn: &r2d2::PooledConnection<r2d2_sqlite::SqliteConnectionManager>) -> Result<(), String> {
	conn.pragma_update(None, "foreign_keys", false)
		.map_err(|e| format!("Failed to disable foreign_keys constraint: {}", e))?;

	let table_list = ["server", "world", "lobby", "score_table", "tus_var", "tus_data", "tus_var_vuser", "tus_data_vuser", "score"];

	for table in &table_list {
		let query = format!("UPDATE {} SET communication_id = communication_id || '_00'", table);
		conn.execute(&query, []).map_err(|e| format!("Error updating communication_id for table {}: {}", table, e))?;
	}

	conn.pragma_update(None, "foreign_keys", true).map_err(|e| format!("Failed to enable foreign_keys constraint: {}", e))?;
	Ok(())
}

fn change_score_rank_limit_default(conn: &r2d2::PooledConnection<r2d2_sqlite::SqliteConnectionManager>) -> Result<(), String> {
	conn.execute("UPDATE score_table SET rank_limit = 250", [])
		.map_err(|e| format!("Failed to update score rank_limit default: {}", e))?;
	Ok(())
}

fn ridge_racer_7_fixup(conn: &r2d2::PooledConnection<r2d2_sqlite::SqliteConnectionManager>) -> Result<(), String> {
	for table_id in 0..=1 {
		let res = conn.execute(
				"INSERT INTO score_table ( communication_id, board_id, rank_limit, update_mode, sort_mode, upload_num_limit, upload_size_limit ) VALUES ( ?1, ?2, ?3, ?4, ?5, ?6, ?7 ) ON CONFLICT( communication_id, board_id ) DO UPDATE SET rank_limit = excluded.rank_limit, update_mode = excluded.update_mode, sort_mode = excluded.sort_mode, upload_num_limit = excluded.upload_num_limit, upload_size_limit = excluded.upload_size_limit",
				rusqlite::params![
					"NPWR00001_01",
					table_id,
					250,
					0,
					0,
					10,
					6000000,
				],
			);

		if let Err(e) = res {
			return Err(format!("Unexpected error creating/updating RR7 score table {}: {}", table_id, e));
		}

		let res = conn.execute(
			"UPDATE score SET communication_id = ?1 WHERE communication_id = ?2 AND board_id = ?3 AND score > 60000000000000000",
			rusqlite::params!["NPWR00001_01", "NPWR00001_00", table_id,],
		);

		if let Err(e) = res {
			return Err(format!("Unexpected error updating RR7 scores: {}", e));
		}
	}

	Ok(())
}

fn world_ids_fixup(conn: &r2d2::PooledConnection<r2d2_sqlite::SqliteConnectionManager>) -> Result<(), String> {
	let res = conn.execute("UPDATE OR REPLACE world SET world_id = world_id + 65536 WHERE world_id < 65537", []);

	if let Err(e) = res {
		return Err(format!("Unexpected error updating world table to fixup ids: {}", e));
	}

	Ok(())
}

fn cleanup_server_ids(conn: &r2d2::PooledConnection<r2d2_sqlite::SqliteConnectionManager>) -> Result<(), String> {
	let res = conn.execute("DELETE FROM world WHERE server_id != 1", []);
	if let Err(e) = res {
		return Err(format!("Unexpected error removing servers != 1 from world table: {}", e));
	}

	let res = conn.execute("DELETE FROM server WHERE server_id != 1", []);
	if let Err(e) = res {
		return Err(format!("Unexpected error removing servers != 1 from server table: {}", e));
	}

	Ok(())
}

fn create_deleted_user_account(conn: &r2d2::PooledConnection<r2d2_sqlite::SqliteConnectionManager>) -> Result<(), String> {
	let username = DELETED_USER_USERNAME;
	let hash = [0u8; 32];
	let salt = [0u8; 64];
	let online_name = "Deleted User";
	let avatar_url = "https://rpcs3.net/cdn/netplay/DefaultAvatar.png";
	let email = "deleted@user";
	let email_check = "deleted@user";
	let token = "0000000000000000";
	let admin = false;
	let stat_agent = false;
	let banned = true;

	conn.execute(
		"INSERT INTO account ( username, hash, salt, online_name, avatar_url, email, email_check, token, admin, stat_agent, banned ) VALUES ( ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11 )",
		rusqlite::params![username, hash, salt, online_name, avatar_url, email, email_check, token, admin, stat_agent, banned],
	)
	.map_err(|e| format!("Error adding DeletedUser account: {}", e))?;

	let user_id: i64 = conn
		.query_row("SELECT user_id FROM account WHERE username = ?1", rusqlite::params![username], |r| Ok(r.get(0).unwrap()))
		.map_err(|e| format!("Failed to get user_id for DeletedUser account: {}", e))?;

	let timestamp = Client::get_timestamp_seconds();
	conn.execute(
		"INSERT INTO account_timestamp ( user_id, creation, last_login ) VALUES ( ?1, ?2, ?2 )",
		rusqlite::params![user_id, timestamp],
	)
	.map_err(|e| format!("Error adding DeletedUser information to account_timestamp: {}", e))?;

	Ok(())
}

fn create_gdpr_hash_tables(conn: &r2d2::PooledConnection<r2d2_sqlite::SqliteConnectionManager>) -> Result<(), String> {
	// Add a table for email hashes
	conn.execute("CREATE TABLE IF NOT EXISTS email_hash ( hash_value BLOB PRIMARY KEY, hash_date UNSIGNED BIGINT NOT NULL )", [])
		.map_err(|e| format!("Failed to create email_hash table: {}", e))?;

	// Add a table for username hashes
	conn.execute("CREATE TABLE IF NOT EXISTS username_hash ( hash_value BLOB PRIMARY KEY, hash_date UNSIGNED BIGINT NOT NULL )", [])
		.map_err(|e| format!("Failed to create username_hash table: {}", e))?;

	// Add a table to store unique values
	conn.execute("CREATE TABLE IF NOT EXISTS unique_blob ( name TEXT PRIMARY KEY, blob_value BLOB NOT NULL )", [])
		.map_err(|e| format!("Failed to create unique_blob table: {}", e))?;

	// Create a server-unique salt for all the emails and usernames
	let mut rng_gen = StdRng::from_entropy();
	let mut email_salt = [0u8; 64];
	rng_gen.fill_bytes(&mut email_salt);
	let mut username_salt = [0u8; 64];
	rng_gen.fill_bytes(&mut username_salt);

	conn.execute("INSERT INTO unique_blob ( name, blob_value ) VALUES ( 'email_salt', ?1 )", rusqlite::params![email_salt])
		.map_err(|e| format!("Failed to insert email_salt into unique_blob: {}", e))?;

	conn.execute("INSERT INTO unique_blob ( name, blob_value ) VALUES ( 'username_salt', ?1 )", rusqlite::params![username_salt])
		.map_err(|e| format!("Failed to insert username_salt into unique_blob: {}", e))?;

	Ok(())
}

fn improve_tables_for_deletion(conn: &r2d2::PooledConnection<r2d2_sqlite::SqliteConnectionManager>) -> Result<(), String> {
	// Update tables with cascading to make deletion faster
	alter_table(
		conn,
		"friendship",
		"CREATE TABLE IF NOT EXISTS friendship ( user_id_1 UNSIGNED BIGINT NOT NULL, user_id_2 UNSIGNED BIGINT NOT NULL, status_user_1 UNSIGNED TINYINT NOT NULL, status_user_2 UNSIGNED TINYINT NOT NULL, PRIMARY KEY(user_id_1, user_id_2), FOREIGN KEY(user_id_1) REFERENCES account(user_id) ON DELETE CASCADE, FOREIGN KEY(user_id_2) REFERENCES account(user_id) ON DELETE CASCADE, CHECK(user_id_1 < user_id_2) )",
		"*",
	)?;

	alter_table(
		conn,
		"account_timestamp",
		"CREATE TABLE IF NOT EXISTS account_timestamp ( user_id UNSIGNED BIGINT NOT NULL, creation UNSIGNED INTEGER NOT NULL, last_login UNSIGNED INTEGER, token_last_sent UNSIGNED INTEGER, reset_emit UNSIGNED INTEGER, PRIMARY KEY(user_id), FOREIGN KEY(user_id) REFERENCES account(user_id) ON DELETE CASCADE )",
		"*",
	)?;

	alter_table(
		conn,
		"tus_var",
		"CREATE TABLE IF NOT EXISTS tus_var ( owner_id UNSIGNED BIGINT NOT NULL, communication_id TEXT NOT NULL, slot_id INTEGER NOT NULL, var_value BIGINT NOT NULL, timestamp UNSIGNED BIGINT NOT NULL, author_id UNSIGNED BIGINT NOT NULL, PRIMARY KEY (owner_id, communication_id, slot_id), FOREIGN KEY(owner_id) REFERENCES account(user_id) ON DELETE CASCADE, FOREIGN KEY(author_id) REFERENCES account(user_id) )",
		"*",
	)?;

	alter_table(
		conn,
		"tus_data",
		"CREATE TABLE IF NOT EXISTS tus_data ( owner_id UNSIGNED BIGINT NOT NULL, communication_id TEXT NOT NULL, slot_id INTEGER NOT NULL, data_id UNSIGNED BIGINT NOT NULL, data_info BLOB NOT NULL, timestamp UNSIGNED BIGINT NOT NULL, author_id UNSIGNED BIGINT NOT NULL, PRIMARY KEY (owner_id, communication_id, slot_id), FOREIGN KEY(owner_id) REFERENCES account(user_id) ON DELETE CASCADE, FOREIGN KEY(author_id) REFERENCES account(user_id) )",
		"*",
	)?;

	alter_table(
		conn,
		"score",
		"CREATE TABLE IF NOT EXISTS score ( communication_id TEXT NOT NULL, board_id UNSIGNED INTEGER NOT NULL, user_id INTEGER NOT NULL, character_id INTEGER NOT NULL, score BIGINT NOT NULL, comment TEXT, game_info BLOB, data_id UNSIGNED BIGINT UNIQUE, timestamp UNSIGNED BIGINT NOT NULL, PRIMARY KEY(communication_id, board_id, user_id, character_id), FOREIGN KEY(user_id) REFERENCES account(user_id) ON DELETE CASCADE )",
		"*",
	)?;

	// tus_data_vuser_author_id was incorrectly defined
	conn.execute("DROP INDEX IF EXISTS tus_data_vuser_author_id", [])
		.map_err(|e| format!("Failed to drop tus_data_vuser_author_id table: {}", e))?;

	// Recreate indexes
	add_indexes(conn)?;

	Ok(())
}

fn prepare_for_deletion(conn: &r2d2::PooledConnection<r2d2_sqlite::SqliteConnectionManager>) -> Result<(), String> {
	create_deleted_user_account(conn)?;
	create_gdpr_hash_tables(conn)?;
	improve_tables_for_deletion(conn)?;
	Ok(())
}

impl Server {
	pub fn initialize_database(admin_list: &[String]) -> Result<r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>, String> {
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

		let applied_migrations: BTreeMap<u32, String> = {
			let mut stmt = conn.prepare("SELECT * FROM migration").map_err(|e| format!("Failed to prepare statement to query migrations: {}", e))?;
			stmt.query_map([], |row| {
				let migration_id: u32 = row.get(0)?;
				let description: String = row.get(1)?;
				Ok((migration_id, description))
			})
			.map_err(|e| format!("Failed to query migrations: {}", e))?
			.collect::<Result<BTreeMap<u32, String>, _>>()
			.map_err(|e| format!("Some of the migration queries failed: {}", e))?
		};

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

		// Set admin rights from config
		if !admin_list.is_empty() {
			let set_admin_cmd = format!("UPDATE account SET admin = TRUE WHERE username IN ({})", "'".to_string() + &admin_list.join("','") + "'");
			let _ = conn.execute(&set_admin_cmd, []);
		}

		// Load presets for game servers configurations
		{
			let db = Database::new(conn);
			if let Err(e) = db.update_servers_cfg() {
				println!("Failed to load servers.cfg: {}!", e);
			}
		}

		Ok(pool)
	}
}

fn hash_password(rng_gen: &mut StdRng, password: &str) -> (Vec<u8>, [u8; 64]) {
	let mut salt = [0; 64];
	rng_gen.fill_bytes(&mut salt);

	let config = argon2::Config::original();
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

fn generate_string_from_user_list(user_list: &[i64]) -> String {
	let mut s = String::with_capacity(user_list.len() * 8);
	for user in user_list {
		write!(s, "{},", *user).unwrap();
	}
	s.pop();
	s
}

fn generate_string_from_slot_list(slot_list: &[i32]) -> String {
	let mut s = String::with_capacity(slot_list.len() * 8);
	for slot in slot_list {
		write!(s, "{},", *slot).unwrap();
	}
	s.pop();
	s
}

impl Database {
	pub fn new(conn: r2d2::PooledConnection<r2d2_sqlite::SqliteConnectionManager>) -> Database {
		Database { conn }
	}

	pub fn update_servers_cfg(&self) -> Result<(), DbError> {
		let mut file = File::open("servers.cfg").map_err(|_| DbError::Internal)?;

		let mut buf_file = String::new();
		file.read_to_string(&mut buf_file).map_err(|_| DbError::Internal)?;

		let config_servers: Vec<(String, u16, u32, Vec<u64>)> = buf_file
			.lines()
			.filter_map(|l| {
				if l.trim().is_empty() || l.trim().chars().nth(0).unwrap() == '#' {
					return None;
				}

				let servers_infos: Vec<&str> = l.trim().split('|').map(|v| v.trim()).collect();
				if servers_infos.len() != 4 || servers_infos[0].len() != COMMUNICATION_ID_SIZE {
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
			let _ = self
				.conn
				.execute("INSERT INTO server ( communication_id, server_id ) VALUES (?1, ?2)", rusqlite::params![com_id, server]);
			let _ = self.conn.execute(
				"INSERT INTO world ( communication_id, server_id, world_id ) VALUES (?1, ?2, ?3)",
				rusqlite::params![com_id, server, world],
			);
			for lobby in lobbies {
				let _ = self
					.conn
					.execute("INSERT INTO lobby (communication_id, world_id, lobby_id) VALUES (?1, ?2, ?3)", rusqlite::params![com_id, world, lobby]);
			}
		}
		Ok(())
	}

	pub fn cleanup_expired_hashes(&self) -> Result<(u64, u64), DbError> {
		let timestamp = Client::get_timestamp_days().checked_sub(3 * 30).ok_or(DbError::Internal)?;

		// Delete entries older than 180 days from username_hash table
		self.conn.execute("DELETE FROM username_hash WHERE hash_date < ?1", rusqlite::params![timestamp]).map_err(|e| {
			error!("Failed to cleanup username_hash table: {}", e);
			DbError::Internal
		})?;

		let num_username = self.conn.changes();

		// Delete entries older than 180 days from email_hash table
		self.conn.execute("DELETE FROM email_hash WHERE hash_date < ?1", rusqlite::params![timestamp]).map_err(|e| {
			error!("Failed to cleanup email_hash table: {}", e);
			DbError::Internal
		})?;

		let num_email = self.conn.changes();

		Ok((num_username, num_email))
	}

	pub fn cleanup_never_used_accounts(&self) -> Result<usize, String> {
		// Delete accounts older than 30 days old that have never logged in
		let timestamp = Client::get_timestamp_seconds().checked_sub(60 * 60 * 24 * 30).ok_or("Overflow when calculing timestamp")?;
		let mut stmt = self
			.conn
			.prepare("SELECT user_id FROM account_timestamp WHERE creation < ?1 AND last_login IS NULL")
			.map_err(|e| format!("Failed to prepare statement to query stale accounts: {}", e))?;
		let stale_user_ids: Vec<i64> = stmt
			.query_map(rusqlite::params![timestamp], |row| Ok(row.get_unwrap(0)))
			.map_err(|e| format!("Failed to query stale accounts: {}", e))?
			.collect::<Result<Vec<i64>, _>>()
			.map_err(|e| format!("Some of the stale accounts queries failed: {}", e))?;

		if stale_user_ids.is_empty() {
			return Ok(0);
		}

		let user_id_string = generate_string_from_user_list(&stale_user_ids);
		let friendship_query = format!("DELETE FROM friendship WHERE user_id_1 IN ({}) OR user_id_2 IN ({})", user_id_string, user_id_string);
		let account_timestamp_query = format!("DELETE FROM account_timestamp WHERE user_id IN ({})", user_id_string);
		let account_query = format!("DELETE FROM account WHERE user_id IN ({})", user_id_string);

		self.conn
			.execute(&account_timestamp_query, [])
			.map_err(|e| format!("Failure to delete from account_timestamp during stale accounts cleanup: {}", e))?;
		self.conn
			.execute(&friendship_query, [])
			.map_err(|e| format!("Failure to delete from friendship during stale accounts cleanup: {}", e))?;
		self.conn
			.execute(&account_query, [])
			.map_err(|e| format!("Failure to delete from account during stale accounts cleanup: {}", e))?;

		Ok(stale_user_ids.len())
	}

	fn get_username_salt(&self) -> Result<[u8; 64], DbError> {
		self.conn
			.query_row("SELECT blob_value FROM unique_blob WHERE name = 'username_salt'", [], |r| r.get(0))
			.map_err(|_| DbError::Internal)
	}

	fn get_email_salt(&self) -> Result<[u8; 64], DbError> {
		self.conn
			.query_row("SELECT blob_value FROM unique_blob WHERE name = 'email_salt'", [], |r| r.get(0))
			.map_err(|_| DbError::Internal)
	}

	fn hash_username(&self, username: &str) -> Result<Vec<u8>, DbError> {
		let username_salt = self.get_username_salt()?;
		let config = argon2::Config::original();
		argon2::hash_raw(username.as_bytes(), &username_salt, &config).map_err(|e| {
			error!("Failed to hash username: {}", e);
			DbError::Internal
		})
	}

	fn hash_email(&self, email_check: &str) -> Result<Vec<u8>, DbError> {
		let email_salt = self.get_email_salt()?;
		let config = argon2::Config::original();
		argon2::hash_raw(email_check.as_bytes(), &email_salt, &config).map_err(|e| {
			error!("Failed to hash email_check: {}", e);
			DbError::Internal
		})
	}

	fn check_username_vs_deleted(&self, username: &str) -> Result<(), DbError> {
		let username_hash = self.hash_username(username)?;
		let count: i64 = self
			.conn
			.query_row("SELECT COUNT(*) FROM username_hash WHERE hash_value = ?1", rusqlite::params![username_hash], |r| r.get(0))
			.map_err(|e| {
				error!("Failed to get count from username_hash: {}", e);
				DbError::Internal
			})?;

		if count != 0 {
			return Err(DbError::ExistingUsername);
		}

		Ok(())
	}

	fn check_email_vs_deleted(&self, email_check: &str) -> Result<(), DbError> {
		let email_hash = self.hash_email(email_check)?;
		let count: i64 = self
			.conn
			.query_row("SELECT COUNT(*) FROM email_hash WHERE hash_value = ?1", rusqlite::params![email_hash], |r| r.get(0))
			.map_err(|e| {
				error!("Failed to get count from email_hash: {}", e);
				DbError::Internal
			})?;

		if count != 0 {
			return Err(DbError::ExistingEmail);
		}

		Ok(())
	}

	fn get_deleted_user_id(&self) -> Result<i64, DbError> {
		self.get_user_id(DELETED_USER_USERNAME).map_err(|_| DbError::Internal)
	}

	pub fn add_user(&self, username: &str, password: &str, online_name: &str, avatar_url: &str, email: &str, email_check: &str, is_admin: bool) -> Result<String, DbError> {
		self.check_username_vs_deleted(username)?;
		self.check_email_vs_deleted(email_check)?;

		let mut rng_gen = StdRng::from_entropy();
		let (hash, salt) = hash_password(&mut rng_gen, password);

		let salt_slice = &salt[..];
		let token_str = generate_token(&mut rng_gen);

		let stat_agent = false;
		let banned = false;

		if let Err(e) = self.conn.execute(
			"INSERT INTO account ( username, hash, salt, online_name, avatar_url, email, email_check, token, admin, stat_agent, banned ) VALUES ( ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11 )",
			rusqlite::params![username, hash, salt_slice, online_name, avatar_url, email, email_check, token_str, is_admin, stat_agent, banned],
		) {
			if let rusqlite::Error::SqliteFailure(error, ref msg) = e
				&& error.code == rusqlite::ErrorCode::ConstraintViolation
				&& let Some(msg) = msg
			{
				if msg.contains("account.username") {
					warn!("Attempted to create an already existing user({})", username);
					return Err(DbError::ExistingUsername);
				}
				if msg.contains("account.email_check") {
					warn!("Attempted to create an account with an already existing email({})", email);
					return Err(DbError::ExistingEmail);
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

	pub fn delete_user(&self, user_id: i64, username: &str, email_check: &str) -> Result<(), DbError> {
		// Protection against stupidity
		if username == DELETED_USER_USERNAME {
			return Err(DbError::Internal);
		}

		let username_hash = self.hash_username(username)?;
		let email_hash = self.hash_email(email_check)?;
		let deleted_userid = self.get_deleted_user_id()?;
		let timestamp = Client::get_timestamp_days();

		// Insert hashed username into username_hash table
		if let Err(e) = self
			.conn
			.execute("INSERT INTO username_hash ( hash_value, hash_date ) VALUES ( ?1, ?2 )", rusqlite::params![username_hash, timestamp])
		{
			error!("Unexpected error inserting username hash: {}", e);
			// Continue with deletion even if hash insertion fails
		}

		// Insert hashed email into email_hash table
		if let Err(e) = self
			.conn
			.execute("INSERT INTO email_hash ( hash_value, hash_date ) VALUES ( ?1, ?2 )", rusqlite::params![email_hash, timestamp])
		{
			error!("Unexpected error inserting email hash: {}", e);
			// Continue with deletion even if hash insertion fails
		}

		if let Err(e) = self.conn.execute("UPDATE tus_var SET author_id = ?1 WHERE author_id = ?2", rusqlite::params![deleted_userid, user_id]) {
			error!("Unexpected error updating deleted user in tus_var: {}", e);
			return Err(DbError::Internal);
		}

		if let Err(e) = self.conn.execute("UPDATE tus_data SET author_id = ?1 WHERE author_id = ?2", rusqlite::params![deleted_userid, user_id]) {
			error!("Unexpected error updating deleted user in tus_data: {}", e);
			return Err(DbError::Internal);
		}

		if let Err(e) = self
			.conn
			.execute("UPDATE tus_var_vuser SET author_id = ?1 WHERE author_id = ?2", rusqlite::params![deleted_userid, user_id])
		{
			error!("Unexpected error updating deleted user in tus_var_vuser: {}", e);
			return Err(DbError::Internal);
		}

		if let Err(e) = self
			.conn
			.execute("UPDATE tus_data_vuser SET author_id = ?1 WHERE author_id = ?2", rusqlite::params![deleted_userid, user_id])
		{
			error!("Unexpected error updating deleted user in tus_data_vuser: {}", e);
			return Err(DbError::Internal);
		}

		if let Err(e) = self.conn.execute("DELETE FROM account WHERE user_id = ?1", rusqlite::params![user_id]) {
			error!("Unexpected error deleting user from account: {}", e);
			return Err(DbError::Internal);
		}

		Ok(())
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
			"SELECT user_id, hash, salt, online_name, avatar_url, email, email_check, token, admin, stat_agent, banned FROM account WHERE username = ?1",
			rusqlite::params![username],
			|r| {
				Ok(UserQueryResult {
					user_id: r.get(0).unwrap(),
					hash: r.get(1).unwrap(),
					salt: r.get(2).unwrap(),
					online_name: r.get(3).unwrap(),
					avatar_url: r.get(4).unwrap(),
					email: r.get(5).unwrap(),
					email_check: r.get(6).unwrap(),
					token: r.get(7).unwrap(),
					admin: r.get(8).unwrap(),
					stat_agent: r.get(9).unwrap(),
					banned: r.get(10).unwrap(),
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

		let config = argon2::Config::original();
		let hash = argon2::hash_raw(password.as_bytes(), &res.salt, &config).expect("Password hashing failed!");

		if hash != res.hash {
			return Err(DbError::WrongPass);
		}

		Ok(res)
	}

	pub fn check_email(&self, username: &str, email: &str) -> Result<(i64, String), DbError> {
		let res: rusqlite::Result<(i64, String)> = self
			.conn
			.query_row("SELECT user_id, email FROM account WHERE username = ?1 AND email_check = ?2", rusqlite::params![username, email], |r| {
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
			.query_row("SELECT user_id FROM account WHERE username = ?1 AND reset_token = ?2", rusqlite::params![username, reset_token], |r| {
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
		let res: rusqlite::Result<i64> = self
			.conn
			.query_row("SELECT user_id FROM account WHERE username = ?1", rusqlite::params![npid], |r| Ok(r.get(0).unwrap()));

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
			.query_row("SELECT username FROM account WHERE user_id = ?1", rusqlite::params![user_id], |r| Ok(r.get(0).unwrap()));

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

	pub fn get_email_check(&self, user_id: i64) -> Result<String, DbError> {
		let res: rusqlite::Result<String> = self
			.conn
			.query_row("SELECT email_check FROM account WHERE user_id = ?1", rusqlite::params![user_id], |r| Ok(r.get(0).unwrap()));

		if let Err(e) = res {
			if e == rusqlite::Error::QueryReturnedNoRows {
				warn!("Attempted to get the email_check of non existent user_id {}", user_id);
				return Err(DbError::Empty);
			}
			error!("Unexpected error querying email_check: {}", e);
			return Err(DbError::Internal);
		}

		Ok(res.unwrap())
	}

	pub fn get_username_and_online_name_from_user_ids(&self, user_list: &HashSet<i64>) -> Result<Vec<(i64, String, String)>, DbError> {
		let stmt_string = format!(
			"SELECT user_id, username, online_name FROM account WHERE user_id IN ({})",
			generate_string_from_user_list(&user_list.iter().copied().collect::<Vec<i64>>())
		);
		let mut stmt = self.conn.prepare(&stmt_string).map_err(|_| DbError::Internal)?;

		let rows = stmt.query_map([], |r| Ok((r.get_unwrap(0), r.get_unwrap(1), r.get_unwrap(2)))).map_err(|_| DbError::Internal)?;

		let mut vec_user_infos = Vec::new();
		for row in rows {
			vec_user_infos.push(row.unwrap());
		}

		Ok(vec_user_infos)
	}

	pub fn create_server(&self, com_id: &ComId, server_id: u16) -> Result<(), DbError> {
		let com_id_str = com_id_to_string(com_id);

		if server_id != 1 {
			warn!("Attempted to create an invalid server({}) for {}", server_id, com_id_str);
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
				"SELECT COUNT(*) FROM server WHERE communication_id = ?1 AND server_id = ?2",
				rusqlite::params![com_id_str, server_id],
				|r| r.get(0),
			);
			if let Err(e) = count {
				error!("Unexpected error querying for server existence: {}", e);
				return Err(DbError::Internal);
			}

			if count.unwrap() == 0 {
				warn!("Attempted to query world list on a non-existing server({}:{})", &com_id_str, server_id);
				return Err(DbError::Empty);

				// This was initially assumed that some games would hardcode those but this doesn't seem to be the case
				// The few cases where it has happened seems to mostly have been client sending an uninitialized value

				// // Some games request a specifically hardcoded server, just create it for them
				// if create_missing {
				// 	self.create_server(com_id, server_id)?;
				// 	return self.get_world_list(com_id, server_id, false);
				// } else {
				// 	warn!("Attempted to query world list on an unexisting server({}:{})", &com_id_str, server_id);
				// 	return Err(DbError::Empty);
				// }
			}
		}

		let mut list_worlds = Vec::new();
		{
			let mut stmt = self.conn.prepare("SELECT world_id FROM world WHERE communication_id = ?1 AND server_id = ?2").unwrap();
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

			let new_wid = if let Ok(cur_max_res) = cur_max_res { cur_max_res + 1 } else { 65537 };

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
		if user_id_1 < user_id_2 { (user_id_1, user_id_2, false) } else { (user_id_2, user_id_1, true) }
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

	pub fn ban_user(&self, user_id: i64) -> Result<(), DbError> {
		self.conn.execute("UPDATE account SET banned = TRUE WHERE user_id = ?1", rusqlite::params![user_id]).map_err(|e| {
			error!("Unexpected error banning user: {}", e);
			DbError::Internal
		})?;
		Ok(())
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
}
