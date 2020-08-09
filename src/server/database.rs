use std::fs;
use std::sync::Arc;

use parking_lot::Mutex;
use rand::prelude::*;
use rusqlite;
use rusqlite::NO_PARAMS;

use crate::server::log::LogManager;
use crate::Config;

pub struct DatabaseManager {
    conn: rusqlite::Connection,
    log_manager: Arc<Mutex<LogManager>>,
}

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
#[allow(dead_code)]
pub enum DbError {
    Internal,
    Empty,
    WrongPass,
    Existing,
}

pub struct UserQueryResult {
    pub user_id: i64,
    hash: Vec<u8>,
    salt: Vec<u8>,
    pub online_name: String,
    pub avatar_url: String,
}

impl DatabaseManager {
    pub fn new(log_manager: Arc<Mutex<LogManager>>) -> DatabaseManager {
        let _ = fs::create_dir("db");

        let conn = rusqlite::Connection::open("db/rpcnv2.db").expect("Failed to open \"db/rpcnv2.db\"!");
        conn.execute(
            "CREATE TABLE IF NOT EXISTS users ( userId INTEGER PRIMARY KEY NOT NULL, username TEXT NOT NULL, hash BLOB NOT NULL, salt BLOB NOT NULL, online_name TEXT NOT NULL, avatar_url TEXT NOT NULL)",
            NO_PARAMS,
        )
        .expect("Failed to create users table!");
        conn.execute(
            "CREATE TABLE IF NOT EXISTS servers ( serverId UNSIGNED SMALLINT PRIMARY KEY NOT NULL, communicationId TEXT NOT NULL)",
            NO_PARAMS,
        )
        .expect("Failed to create servers table!");
        conn.execute(
            "CREATE TABLE IF NOT EXISTS worlds ( worldId UNSIGNED INT PRIMARY KEY NOT NULL, serverId UNSIGNED TINY INT NOT NULL, FOREIGN KEY(serverId) REFERENCES servers(serverId))",
            NO_PARAMS,
        )
        .expect("Failed to create worlds table!");
        conn.execute(
            "CREATE TABLE IF NOT EXISTS lobbies ( lobbyId UNSIGNED BIGINT PRIMARY KEY NOT NULL, worldId UNSIGNED INT NOT NULL, FOREIGN KEY(worldId) REFERENCES worlds(worldId))",
            NO_PARAMS,
        )
        .expect("Failed to create lobbies table!");
        DatabaseManager { conn, log_manager }
    }

    fn log(&self, s: &str) {
        self.log_manager.lock().write(&format!("DB: {}", s));
    }

    pub fn add_user(&mut self, username: &str, password: &str, online_name: &str, avatar_url: &str) -> Result<(), DbError> {
        let count: rusqlite::Result<i64> = self.conn.query_row("SELECT COUNT(*) FROM users WHERE username=?1", rusqlite::params![username], |r| r.get(0));

        if let Err(e) = count {
            self.log(&format!("Error querying username count: {}", e));
            return Err(DbError::Internal);
        }

        if count.unwrap() != 0 {
            self.log(&format!("Attempted to create an already existing user({})", username));
            return Err(DbError::Existing);
        }

        let mut rng_gen = StdRng::from_entropy();

        let mut salt = [0; 64];
        rng_gen.fill_bytes(&mut salt);

        let config = argon2::Config::default();
        let hash = argon2::hash_raw(password.as_bytes(), &salt, &config).expect("Password hashing failed!");

        let salt_slice = &salt[..];

        if self
            .conn
            .execute(
                "INSERT INTO users ( username, hash, salt, online_name, avatar_url ) VALUES ( ?1, ?2, ?3, ?4, ?5 )",
                rusqlite::params![username, hash, salt_slice, online_name, avatar_url],
            )
            .is_err()
        {
            Err(DbError::Internal)
        } else {
            Ok(())
        }
    }
    pub fn check_user(&mut self, username: &str, password: &str) -> Result<UserQueryResult, DbError> {
        let res: rusqlite::Result<UserQueryResult> = self
            .conn
            .query_row("SELECT userId, hash, salt, online_name, avatar_url FROM users WHERE username=?1", rusqlite::params![username], |r| {
                Ok(UserQueryResult {
                    user_id: r.get(0).unwrap(),
                    hash: r.get(1).unwrap(),
                    salt: r.get(2).unwrap(),
                    online_name: r.get(3).unwrap(),
                    avatar_url: r.get(4).unwrap(),
                })
            });

        if let Err(e) = res {
            self.log(&format!("Error querying username row: {}", e));
            return Err(DbError::Internal);
        }

        let res = res.unwrap();

        let config = argon2::Config::default();
        let hash = argon2::hash_raw(password.as_bytes(), &res.salt, &config).expect("Password hashing failed!");

        if hash != res.hash {
            return Err(DbError::WrongPass);
        }

        Ok(res)
    }
    pub fn get_server_list(&mut self, communication_id: &str) -> Result<Vec<u16>, DbError> {
        let mut list_servers = Vec::new();
        {
            let mut stmt = self.conn.prepare("SELECT serverId FROM servers WHERE communicationId=?1").unwrap();
            let server_iter = stmt.query_map(rusqlite::params![communication_id], |r| r.get(0)).expect("Server query failed!");

            for sid in server_iter {
                list_servers.push(sid.unwrap());
            }
        }

        if list_servers.len() == 0 && Config::is_create_empty() {
            // Create missing server
            let cur_max_res: rusqlite::Result<u16> = self.conn.query_row("SELECT MAX(serverId) FROM servers", NO_PARAMS, |r| r.get(0));

            let mut new_sid = 1;
            if cur_max_res.is_ok() {
                new_sid = cur_max_res.unwrap() + 1;
            }

            self.log(&format!("Creating a server for {}", communication_id));
            self.conn
                .execute("INSERT INTO servers ( serverId, communicationId ) VALUES (?1, ?2)", rusqlite::params!(new_sid, communication_id))
                .expect("Failed to insert server");
            return self.get_server_list(communication_id);
        }

        Ok(list_servers)
    }
    pub fn get_world_list(&mut self, server_id: u16) -> Result<Vec<u32>, DbError> {
        // Ensures server exists
        {
            let count: rusqlite::Result<i64> = self.conn.query_row("SELECT COUNT(1) FROM servers WHERE serverId=?1", rusqlite::params![server_id], |r| r.get(0));
            if let Err(e) = count {
                self.log(&format!("Error querying for server existence: {}", e));
                return Err(DbError::Internal);
            }

            if count.unwrap() == 0 {
                self.log(&format!("Attempted to query world list on an unexisting server({})", server_id));
                return Err(DbError::Empty);
            }
        }

        let mut list_worlds = Vec::new();
        {
            let mut stmt = self.conn.prepare("SELECT worldId FROM worlds WHERE serverId=?1").unwrap();
            let world_iter = stmt.query_map(rusqlite::params![server_id], |r| r.get(0)).expect("World query failed!");

            for wid in world_iter {
                list_worlds.push(wid.unwrap());
            }
        }

        if list_worlds.len() == 0 && Config::is_create_empty() {
            // Create missing world
            let cur_max_res: rusqlite::Result<u16> = self.conn.query_row("SELECT MAX(worldId) FROM worlds", NO_PARAMS, |r| r.get(0));

            let mut new_wid = 1;
            if cur_max_res.is_ok() {
                new_wid = cur_max_res.unwrap() + 1;
            }

            self.log(&format!("Creating a world for server id {}", server_id));
            self.conn
                .execute("INSERT INTO worlds ( worldId, serverId ) VALUES (?1, ?2)", rusqlite::params!(new_wid, server_id))
                .expect("Failed to insert world");
            return self.get_world_list(server_id);
        }

        Ok(list_worlds)
    }
    pub fn get_corresponding_server(&mut self, world_id: u32) -> Result<u16, rusqlite::Error> {
        let serv: rusqlite::Result<u16> = self.conn.query_row("SELECT (serverId) FROM worlds WHERE worldId = ?1", rusqlite::params![world_id], |r| r.get(0));

        serv
    }
}
