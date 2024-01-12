use crate::server::database::*;

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

impl Database {
	pub fn score_get_all_data_ids(&self) -> Result<HashSet<u64>, DbError> {
		let mut stmt = self.conn.prepare("SELECT data_id FROM score WHERE data_id IS NOT NULL").map_err(|_| DbError::Internal)?;
		let data_ids: HashSet<u64> = stmt
			.query_map([], |r| Ok(r.get_unwrap(0)))
			.map_err(|_| DbError::Internal)?
			.collect::<Result<HashSet<u64>, _>>()
			.map_err(|_| DbError::Internal)?;
		Ok(data_ids)
	}

	fn create_score_board(&self, com_id: &ComId, board_id: u32) -> Result<(), DbError> {
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

	pub fn get_score_tables(&self) -> Result<HashMap<(String, u32), DbBoardInfo>, DbError> {
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
				(com_id, board_id),
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
				println!("Err: Failed to query score tables: {}", e);
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

	pub fn get_scores_from_table(&self, com_id: &str, board_id: u32, table_info: &DbBoardInfo) -> Result<Vec<DbScoreInfo>, DbError> {
		let statement_str = if table_info.sort_mode == 0 {
			"SELECT user_id, character_id, score, comment, game_info, data_id, timestamp FROM score WHERE communication_id = ?1 AND board_id = ?2 ORDER BY score DESC, timestamp ASC, user_id ASC LIMIT ?3"
		} else {
			"SELECT user_id, character_id, score, comment, game_info, data_id, timestamp FROM score WHERE communication_id = ?1 AND board_id = ?2 ORDER BY score ASC, timestamp ASC, user_id ASC LIMIT ?3"
		};

		let mut stmt = self.conn.prepare(statement_str).map_err(|_| DbError::Internal)?;
		let rows = stmt
			.query_map(rusqlite::params![com_id, board_id, table_info.rank_limit], |r| {
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
		let com_id_str = com_id_to_string(com_id);

		let query_str = if board_infos.update_mode == 0 {
			if board_infos.sort_mode == 0 {
				"INSERT INTO score ( communication_id, board_id, user_id, character_id, score, comment, game_info, data_id, timestamp ) VALUES ( ?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, ?8 ) ON CONFLICT ( communication_id, board_id, user_id, character_id ) DO UPDATE SET score = excluded.score, comment = excluded.comment, game_info = excluded.game_info, data_id = NULL, timestamp = excluded.timestamp WHERE excluded.score >= score"
			} else {
				"INSERT INTO score ( communication_id, board_id, user_id, character_id, score, comment, game_info, data_id, timestamp ) VALUES ( ?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, ?8 ) ON CONFLICT ( communication_id, board_id, user_id, character_id ) DO UPDATE SET score = excluded.score, comment = excluded.comment, game_info = excluded.game_info, data_id = NULL, timestamp = excluded.timestamp WHERE excluded.score <= score"
			}
		} else {
			"INSERT INTO score ( communication_id, board_id, user_id, character_id, score, comment, game_info, data_id, timestamp ) VALUES ( ?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, ?8 ) ON CONFLICT ( communication_id, board_id, user_id, character_id ) DO UPDATE SET score = excluded.score, comment = excluded.comment, game_info = excluded.game_info, data_id = NULL, timestamp = excluded.timestamp"
		};

		let res = self.conn.execute(
			query_str,
			rusqlite::params![
				com_id_str,
				board_id,
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

	pub fn set_score_data(&self, com_id: &ComId, user_id: i64, character_id: i32, board_id: u32, score: i64, data_id: u64) -> Result<(), DbError> {
		let com_id_str = com_id_to_string(com_id);
		let res = self.conn.execute(
			"UPDATE score SET data_id = ?1 WHERE communication_id = ?2 AND board_id = ?3 AND user_id = ?4 AND character_id = ?5 AND score = ?6",
			rusqlite::params![data_id, com_id_str, board_id, user_id, character_id, score],
		);

		match res {
			Ok(n) => {
				if n == 1 {
					Ok(())
				} else {
					Err(DbError::Invalid)
				}
			}
			Err(e) => {
				error!("Unexpected error setting game data: {}", e);
				Err(DbError::Internal)
			}
		}
	}
}
