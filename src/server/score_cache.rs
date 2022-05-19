use crate::server::client::ComId;
use crate::server::database::{Database, DbBoardInfo, DbScoreInfo};
use crate::server::Server;
use parking_lot::RwLock;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

struct ScoreTableCache {
	sorted_scores: Vec<DbScoreInfo>,
	npid_lookup: HashMap<i64, HashMap<i32, usize>>,
	table_info: DbBoardInfo,
}

pub struct ScoresCache {
	tables: RwLock<HashMap<String, Arc<RwLock<ScoreTableCache>>>>,
}

impl Server {
	pub fn initialize_score(conn: r2d2::PooledConnection<r2d2_sqlite::SqliteConnectionManager>) -> Result<Arc<ScoresCache>, String> {
		Server::initialize_score_data_handler()?;

		let cache = Arc::new(ScoresCache::new());

		// Populate cache from database
		let db = Database::new(conn);
		let tables = db.get_score_tables().map_err(|_| "Failed to read database scores for the cache")?;

		{
			let mut cache_data = cache.tables.write();
			for (table_name, table_info) in tables {
				let sorted_scores = db
					.get_scores_from_table(&table_name, &table_info)
					.map_err(|_| format!("Failed to read scores for table {}", table_name))?;
				let mut npid_lookup = HashMap::new();

				for (index, score) in sorted_scores.iter().enumerate() {
					let user_entry = npid_lookup.entry(score.user_id).or_insert(HashMap::new());
					user_entry.insert(score.character_id, index);
				}

				cache_data.insert(
					table_name.clone(),
					Arc::new(RwLock::new(ScoreTableCache {
						sorted_scores,
						npid_lookup,
						table_info,
					})),
				);
			}
		}

		Ok(cache)
	}
}

impl DbScoreInfo {
	fn cmp(&self, other: &DbScoreInfo, sort_mode: u32) -> Ordering {
		let ret_value = if sort_mode == 0 { Ordering::Greater } else { Ordering::Less };

		if self.score > other.score {
			ret_value
		} else if self.score < other.score {
			ret_value.reverse()
		} else {
			if self.timestamp < other.timestamp {
				Ordering::Greater
			} else if self.timestamp > other.timestamp {
				Ordering::Less
			} else {
				if self.user_id < other.user_id {
					Ordering::Greater
				} else if self.user_id > other.user_id {
					Ordering::Less
				} else {
					Ordering::Equal
				}
			}
		}
	}
}

impl ScoreTableCache {
	fn from_db(table_name: &str, db_info: &DbBoardInfo) -> ScoreTableCache {
		ScoreTableCache {
			sorted_scores: Vec::new(),
			npid_lookup: HashMap::new(),
			table_info: db_info.clone(),
		}
	}
}

impl ScoresCache {
	fn new() -> ScoresCache {
		ScoresCache { tables: RwLock::new(HashMap::new()) }
	}

	fn get_table(&self, table_name: &String, db_info: &DbBoardInfo) -> Arc<RwLock<ScoreTableCache>> {
		{
			let tables = self.tables.read();
			if tables.contains_key(table_name) {
				return tables[table_name].clone();
			}
		}

		self.tables
			.write()
			.entry(table_name.clone())
			.or_insert(Arc::new(RwLock::new(ScoreTableCache::from_db(table_name, db_info))))
			.clone()
	}

	pub fn insert_score(&self, db_info: &DbBoardInfo, com_id: &ComId, board_id: u32, score: &DbScoreInfo) {
		let table_name = Database::get_scoreboard_name(com_id, board_id);
		let table = self.get_table(&table_name, db_info);
		let mut table = table.write();

		// First check if the user_id/char_id is already in the cache
		if table.npid_lookup.contains_key(&score.user_id) && table.npid_lookup[&score.user_id].contains_key(&score.character_id) {
			let pos = table.npid_lookup[&score.user_id][&score.character_id];
			table.sorted_scores.remove(pos);
		}

		if (table.sorted_scores.len() < table.table_info.rank_limit as usize) || score.cmp(table.sorted_scores.last().unwrap(), table.table_info.sort_mode) == Ordering::Greater {
			let insert_pos = table.sorted_scores.binary_search_by(|probe| probe.cmp(&score, table.table_info.sort_mode)).unwrap_err();
			table.sorted_scores.insert(insert_pos, (*score).clone());
			table.npid_lookup.entry(score.user_id).or_insert(HashMap::new()).insert(score.character_id, insert_pos);

			// Set index for everything after insert_pos
			for i in (insert_pos + 1)..table.sorted_scores.len() {
				let cur_user_id = table.sorted_scores[i].user_id;
				let cur_char_id = table.sorted_scores[i].character_id;
				*table.npid_lookup.get_mut(&cur_user_id).unwrap().get_mut(&cur_char_id).unwrap() = i;
			}

			if table.sorted_scores.len() > table.table_info.rank_limit as usize {
				// Remove the last score
				let last = table.sorted_scores.last().unwrap();
				let user_id = last.user_id;
				let char_id = last.character_id;
	
				let last_index = table.sorted_scores.len() - 1;
				table.sorted_scores.remove(last_index);
				table.npid_lookup.get_mut(&user_id).unwrap().remove(&char_id);
			}
		}
	}
}
