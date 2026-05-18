use std::collections::HashMap;
use tracing::error;
use crate::server::database::*;

pub struct DbTrophySet {
	pub communication_id: String,
	pub title: String,
	pub platform: Option<String>,
	pub trophy_set_version: Option<String>,
	pub has_trophy_groups: bool,
	pub total_item_count: i32,
}

pub struct DbTrophyDefinition {
	pub trophy_id: i32,
	pub trophy_group_id: String,
	pub trophy_name: String,
	pub trophy_detail: String,
	pub trophy_type: i32, // 0=Bronze, 1=Silver, 2=Gold, 3=Platinum
	pub trophy_hidden: bool,
	pub trophy_icon_url: Option<String>,
}

pub struct DbTrophyEarner {
	pub npid: String,
	pub earned_at: i64,
}

impl Database {
	// Returns the trophy set metadata
	pub fn get_trophy_set(&self, communication_id: &str) -> Result<Option<DbTrophySet>, DbError> {
		let res = self.conn.query_row(
			"SELECT communication_id, title, platform, trophy_set_version, has_trophy_groups, total_item_count \
			 FROM trophy_sets WHERE communication_id = ?1",
			rusqlite::params![communication_id],
			|r| {
				Ok(DbTrophySet {
					communication_id: r.get(0)?,
					title: r.get(1)?,
					platform: r.get(2)?,
					trophy_set_version: r.get(3)?,
					has_trophy_groups: r.get::<_, i32>(4)? != 0,
					total_item_count: r.get(5)?,
				})
			},
		);

		match res {
			Ok(ts) => Ok(Some(ts)),
			Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
			Err(e) => {
				error!("Unexpected error querying trophy set for {}: {}", communication_id, e);
				Err(DbError::Internal)
			}
		}
	}

	// Returns all trophy definitions for requested comm_id, ordered by trophy_id.
	pub fn get_trophy_definitions(&self, communication_id: &str) -> Result<Vec<DbTrophyDefinition>, DbError> {
		let mut stmt = self
			.conn
			.prepare(
				"SELECT trophy_id, trophy_group_id, trophy_name, trophy_detail, trophy_type, trophy_hidden, trophy_icon_url \
				 FROM trophy_definitions WHERE communication_id = ?1 ORDER BY trophy_id ASC",
			)
			.map_err(|e| {
				error!("Failed to prepare trophy_definitions statement: {}", e);
				DbError::Internal
			})?;

		let rows = stmt
			.query_map(rusqlite::params![communication_id], |r| {
				Ok(DbTrophyDefinition {
					trophy_id: r.get(0)?,
					trophy_group_id: r.get::<_, Option<String>>(1)?.unwrap_or_else(|| "default".to_string()),
					trophy_name: r.get::<_, Option<String>>(2)?.unwrap_or_default(),
					trophy_detail: r.get::<_, Option<String>>(3)?.unwrap_or_default(),
					trophy_type: r.get::<_, Option<i32>>(4)?.unwrap_or(0),
					trophy_hidden: r.get::<_, i32>(5)? != 0,
					trophy_icon_url: r.get(6)?,
				})
			})
			.map_err(|e| {
				error!("Failed to query trophy definitions for {}: {}", communication_id, e);
				DbError::Internal
			})?;

		let mut defs = Vec::new();
		for row in rows {
			defs.push(row.map_err(|e| {
				error!("Failed to read trophy definition row: {}", e);
				DbError::Internal
			})?);
		}
 
		Ok(defs)
	}

	// Returns all earned trophies for a game, grouped by trophy_id.
	// Joins with the account table to get the npid of each earner.
	pub fn get_trophy_earners_for_game(&self, communication_id: &str) -> Result<HashMap<i32, Vec<DbTrophyEarner>>, DbError> {
		let mut stmt = self
			.conn
			.prepare(
				"SELECT ut.trophy_id, a.username, ut.earned_at \
				 FROM user_trophies ut \
				 JOIN account a ON ut.user_id = a.user_id \
				 WHERE ut.communication_id = ?1 \
				 ORDER BY ut.trophy_id ASC, ut.earned_at ASC",
			)
			.map_err(|e| {
				error!("Failed to prepare user_trophies statement: {}", e);
				DbError::Internal
			})?;

		let rows = stmt
			.query_map(rusqlite::params![communication_id], |r| Ok((r.get::<_, i32>(0)?, r.get::<_, String>(1)?, r.get::<_, i64>(2)?)))
			.map_err(|e| {
				error!("Failed to query user trophies for {}: {}", communication_id, e);
				DbError::Internal
			})?;

		let mut map: HashMap<i32, Vec<DbTrophyEarner>> = HashMap::new();
		for row in rows {
			let (trophy_id, npid, earned_at) = row.map_err(|e| {
				error!("Failed to read user trophy row: {}", e);
				DbError::Internal
			})?;
			map.entry(trophy_id).or_default().push(DbTrophyEarner { npid, earned_at });
		}
 
		Ok(map)
	}

	pub fn upsert_trophy_set(&self, ts: &DbTrophySet) -> Result<(), DbError> {
		self.conn
			.execute(
				"INSERT INTO trophy_sets (communication_id, title, platform, trophy_set_version, has_trophy_groups, total_item_count) \
				 VALUES (?1, ?2, ?3, ?4, ?5, ?6) \
				 ON CONFLICT(communication_id) DO UPDATE SET \
				   title = excluded.title, \
				   platform = excluded.platform, \
				   trophy_set_version = excluded.trophy_set_version, \
				   has_trophy_groups = excluded.has_trophy_groups, \
				   total_item_count = excluded.total_item_count",
				rusqlite::params![ts.communication_id, ts.title, ts.platform, ts.trophy_set_version, ts.has_trophy_groups as i32, ts.total_item_count,],
			)
			.map_err(|e| {
				error!("Unexpected error upserting trophy set: {}", e);
				DbError::Internal
			})?;
		Ok(())
	}

	pub fn upsert_trophy_definition(&self, communication_id: &str, def: &DbTrophyDefinition) -> Result<(), DbError> {
		self.conn
			.execute(
				"INSERT INTO trophy_definitions \
				   (communication_id, trophy_id, trophy_group_id, trophy_name, trophy_detail, trophy_type, trophy_hidden, trophy_icon_url) \
				 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) \
				 ON CONFLICT(communication_id, trophy_id) DO UPDATE SET \
				   trophy_group_id = excluded.trophy_group_id, \
				   trophy_name = excluded.trophy_name, \
				   trophy_detail = excluded.trophy_detail, \
				   trophy_type = excluded.trophy_type, \
				   trophy_hidden = excluded.trophy_hidden, \
				   trophy_icon_url = excluded.trophy_icon_url",
				rusqlite::params![
					communication_id,
					def.trophy_id,
					def.trophy_group_id,
					def.trophy_name,
					def.trophy_detail,
					def.trophy_type,
					def.trophy_hidden as i32,
					def.trophy_icon_url,
				],
			)
			.map_err(|e| {
				error!("Unexpected error upserting trophy definition: {}", e);
				DbError::Internal
			})?;
		Ok(())
	}

	pub fn record_user_trophy(&self, user_id: i64, communication_id: &str, trophy_id: i32, earned_at: i64) -> Result<(), DbError> {
		self.conn
			.execute(
				"INSERT OR IGNORE INTO user_trophies (user_id, communication_id, trophy_id, earned_at) VALUES (?1, ?2, ?3, ?4)",
				rusqlite::params![user_id, communication_id, trophy_id, earned_at],
			)
			.map_err(|e| {
				error!("Unexpected error recording user trophy: {}", e);
				DbError::Internal
			})?;
		Ok(())
	}

	pub fn get_all_trophies_for_user(
		&self,
		npid: &str,
	) -> Result<Vec<(String, String, Option<String>, Vec<(i32, String, String, i32, bool, u64)>)>, DbError> {
		let mut stmt = self
			.conn
			.prepare(
				"SELECT \
				     ut.communication_id, \
				     COALESCE(ts.title, ut.communication_id), \
				     ts.platform, \
				     ut.trophy_id, \
				     COALESCE(td.trophy_name, ''), \
				     COALESCE(td.trophy_detail, ''), \
				     COALESCE(td.trophy_type, 0), \
				     COALESCE(td.trophy_hidden, 0), \
				     ut.earned_at \
				 FROM user_trophies ut \
				 JOIN account a ON ut.user_id = a.user_id \
				 LEFT JOIN trophy_sets ts ON ts.communication_id = ut.communication_id \
				 LEFT JOIN trophy_definitions td \
				     ON td.communication_id = ut.communication_id AND td.trophy_id = ut.trophy_id \
				 WHERE a.username = ?1 \
				 ORDER BY ut.communication_id ASC, ut.trophy_id ASC",
			)
			.map_err(|e| {
				error!("Failed to prepare get_all_trophies_for_user statement: {}", e);
				DbError::Internal
			})?;

		let rows = stmt
			.query_map(rusqlite::params![npid], |r| {
				Ok((
					r.get::<_, String>(0)?,
					r.get::<_, String>(1)?,
					r.get::<_, Option<String>>(2)?,
					r.get::<_, i32>(3)?,
					r.get::<_, String>(4)?,
					r.get::<_, String>(5)?,
					r.get::<_, i32>(6)?,
					r.get::<_, i32>(7)? != 0,
					r.get::<_, i64>(8)? as u64,
				))
			})
			.map_err(|e| {
				error!("Failed to query all trophies for npid={}: {}", npid, e);
				DbError::Internal
			})?;

		// Group rows by communication_id
		let mut games: Vec<(String, String, Option<String>, Vec<(i32, String, String, i32, bool, u64)>)> = Vec::new();
		for row in rows {
			let (comm_id, title, platform, trophy_id, name, detail, ttype, hidden, earned_at) = row.map_err(|e| {
				error!("Failed to read all_trophies row: {}", e);
				DbError::Internal
			})?;

			match games.last_mut() {
				Some(last) if last.0 == comm_id => {
					last.3.push((trophy_id, name, detail, ttype, hidden, earned_at));
				}
				_ => {
					games.push((comm_id, title, platform, vec![(trophy_id, name, detail, ttype, hidden, earned_at)]));
				}
			}
		}

		Ok(games)
	}

	pub fn get_user_trophies_by_npid(&self, npid: &str, communication_id: &str) -> Result<Vec<(i32, u64)>, DbError> {
		let mut stmt = self
			.conn
			.prepare(
				"SELECT ut.trophy_id, ut.earned_at \
				 FROM user_trophies ut \
				 JOIN account a ON ut.user_id = a.user_id \
				 WHERE a.username = ?1 AND ut.communication_id = ?2 \
				 ORDER BY ut.trophy_id ASC",
			)
			.map_err(|e| {
				error!("Failed to prepare get_user_trophies_by_npid statement: {}", e);
				DbError::Internal
			})?;

		let rows = stmt
			.query_map(rusqlite::params![npid, communication_id], |r| Ok((r.get::<_, i32>(0)?, r.get::<_, i64>(1)?)))
			.map_err(|e| {
				error!("Failed to query user trophies for npid={} comm={}: {}", npid, communication_id, e);
				DbError::Internal
			})?;

		let mut result = Vec::new();
		for row in rows {
			let (tid, ts) = row.map_err(|e| {
				error!("Failed to read user trophy row: {}", e);
				DbError::Internal
			})?;
			result.push((tid, ts as u64));
		}

		Ok(result)
	}

	pub fn get_unique_player_count_for_game(&self, communication_id: &str) -> Result<i64, DbError> {
		self.conn
			.query_row(
				"SELECT COUNT(DISTINCT user_id) FROM user_trophies WHERE communication_id = ?1",
				rusqlite::params![communication_id],
				|r| r.get(0),
			)
			.map_err(|e| {
				error!("Failed to count unique players for {}: {}", communication_id, e);
				DbError::Internal
			})
	}

	pub fn get_user_trophies(&self, user_id: i64, communication_id: &str) -> Result<Vec<(i32, u64)>, DbError> {
		let mut stmt = self
			.conn
			.prepare("SELECT trophy_id, earned_at FROM user_trophies WHERE user_id = ?1 AND communication_id = ?2 ORDER BY trophy_id ASC")
			.map_err(|e| {
				error!("Failed to prepare get_user_trophies statement: {}", e);
				DbError::Internal
			})?;

		let rows = stmt
			.query_map(rusqlite::params![user_id, communication_id], |r| Ok((r.get::<_, i32>(0)?, r.get::<_, i64>(1)?)))
			.map_err(|e| {
				error!("Failed to query user trophies for user_id={} comm={}: {}", user_id, communication_id, e);
				DbError::Internal
			})?;

		let mut result = Vec::new();
		for row in rows {
			let (tid, ts) = row.map_err(|e| {
				error!("Failed to read user trophy row: {}", e);
				DbError::Internal
			})?;
			result.push((tid, ts as u64));
		}

		Ok(result)
	}
}