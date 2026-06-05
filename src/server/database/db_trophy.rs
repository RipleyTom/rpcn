use tracing::error;
use crate::server::database::*;

impl Database {
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

	pub fn record_user_trophies_bulk(&self, user_id: i64, communication_id: &str, trophies: &[(i32, i64)]) -> Result<(), DbError> {
		if trophies.is_empty() {
			return Ok(());
		}

		for chunk in trophies.chunks(500) {
			let placeholders: String = chunk
				.iter()
				.enumerate()
				.map(|(i, _)| format!("(?1, ?2, ?{}, ?{})", i * 2 + 3, i * 2 + 4))
				.collect::<Vec<_>>()
				.join(", ");

			let sql = format!(
				"INSERT OR IGNORE INTO user_trophies (user_id, communication_id, trophy_id, earned_at) VALUES {}",
				placeholders
			);

			let mut params: Vec<Box<dyn rusqlite::ToSql>> = vec![
				Box::new(user_id),
				Box::new(communication_id.to_owned()),
			];
			for (tid, ts) in chunk {
				params.push(Box::new(*tid));
				params.push(Box::new(*ts));
			}

			self.conn
				.execute(&sql, rusqlite::params_from_iter(params.iter().map(|p| p.as_ref())))
				.map_err(|e| {
					error!("Unexpected error bulk recording user trophies: {}", e);
					DbError::Internal
				})?;
		}

		Ok(())
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

	/// Returns all games a user has earned at least one trophy in, with the list of (trophy_id, earned_at) per game.
	pub fn get_all_trophies_for_user(&self, npid: &str) -> Result<Vec<(String, Vec<(i32, u64)>)>, DbError> {
		let mut stmt = self
			.conn
			.prepare(
				"SELECT ut.communication_id, ut.trophy_id, ut.earned_at \
				 FROM user_trophies ut \
				 JOIN account a ON ut.user_id = a.user_id \
				 WHERE a.username = ?1 \
				 ORDER BY ut.communication_id ASC, ut.trophy_id ASC",
			)
			.map_err(|e| {
				error!("Failed to prepare get_all_trophies_for_user statement: {}", e);
				DbError::Internal
			})?;

		let rows = stmt
			.query_map(rusqlite::params![npid], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i32>(1)?, r.get::<_, i64>(2)?)))
			.map_err(|e| {
				error!("Failed to query all trophies for npid={}: {}", npid, e);
				DbError::Internal
			})?;

		let mut games: Vec<(String, Vec<(i32, u64)>)> = Vec::new();
		for row in rows {
			let (comm_id, trophy_id, earned_at) = row.map_err(|e| {
				error!("Failed to read all_trophies row: {}", e);
				DbError::Internal
			})?;

			match games.last_mut() {
				Some(last) if last.0 == comm_id => last.1.push((trophy_id, earned_at as u64)),
				_ => games.push((comm_id, vec![(trophy_id, earned_at as u64)])),
			}
		}

		Ok(games)
	}

	/// Returns (trophy_id, earner_count) for every trophy that has been earned at least once for a given game.
	pub fn get_trophy_earner_counts_for_game(&self, communication_id: &str) -> Result<Vec<(i32, i64)>, DbError> {
		let mut stmt = self
			.conn
			.prepare(
				"SELECT trophy_id, COUNT(*) \
				 FROM user_trophies \
				 WHERE communication_id = ?1 \
				 GROUP BY trophy_id \
				 ORDER BY trophy_id ASC",
			)
			.map_err(|e| {
				error!("Failed to prepare get_trophy_earner_counts_for_game statement: {}", e);
				DbError::Internal
			})?;

		let rows = stmt
			.query_map(rusqlite::params![communication_id], |r| Ok((r.get::<_, i32>(0)?, r.get::<_, i64>(1)?)))
			.map_err(|e| {
				error!("Failed to query trophy earner counts for {}: {}", communication_id, e);
				DbError::Internal
			})?;

		let mut result = Vec::new();
		for row in rows {
			result.push(row.map_err(|e| {
				error!("Failed to read trophy earner count row: {}", e);
				DbError::Internal
			})?);
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
}
