use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::server::client::{Client, ComId, ErrorType};
use crate::server::database::db_score::{DbBoardInfo, DbScoreInfo};
use crate::server::database::Database;
use crate::server::stream_extractor::np2_structs_generated::*;
use crate::server::Server;

use parking_lot::RwLock;
use tracing::warn;

use super::client::com_id_to_string;

struct ScoreUserCache {
	npid: String,
	online_name: String,
}

struct ScoreTableCache {
	sorted_scores: Vec<DbScoreInfo>,
	npid_lookup: HashMap<i64, HashMap<i32, usize>>,
	table_info: DbBoardInfo,
	last_insert: u64,
}

pub struct ScoresCache {
	tables: RwLock<HashMap<TableDescription, Arc<RwLock<ScoreTableCache>>>>,
	users: RwLock<HashMap<i64, ScoreUserCache>>,
}

#[derive(Default)]
pub struct ScoreRankDataCache {
	npid: String,
	online_name: String,
	pcid: i32,
	rank: u32,
	score: i64,
	has_gamedata: bool,
	timestamp: u64,
}

pub struct GetScoreResultCache {
	pub scores: Vec<ScoreRankDataCache>,
	pub comments: Option<Vec<String>>,
	pub infos: Option<Vec<Vec<u8>>>,
	pub timestamp: u64,
	pub total_records: u32,
}

#[derive(Hash, PartialEq, Eq)]
pub struct TableDescription {
	com_id: String,
	board_id: u32,
}

impl TableDescription {
	pub fn new(com_id: String, board_id: u32) -> TableDescription {
		TableDescription { com_id, board_id }
	}
}

impl Server {
	pub fn initialize_score(conn: r2d2::PooledConnection<r2d2_sqlite::SqliteConnectionManager>) -> Result<Arc<ScoresCache>, String> {
		let db = Database::new(conn);

		Server::setup_config_scoreboards(&db)?;
		Server::initialize_score_data_handler()?;

		let cache = Arc::new(ScoresCache::new());

		// Populate cache from database
		let tables = db.get_score_tables().map_err(|_| "Failed to read database scores for the cache")?;

		let mut users_list: HashSet<i64> = HashSet::new();

		{
			let mut cache_data = cache.tables.write();
			for ((com_id, board_id), table_info) in tables {
				let sorted_scores = db
					.get_scores_from_table(&com_id, board_id, &table_info)
					.map_err(|_| format!("Failed to read scores for table {}:{}", com_id, board_id))?;
				let mut npid_lookup = HashMap::new();

				for (index, score) in sorted_scores.iter().enumerate() {
					users_list.insert(score.user_id);
					let user_entry: &mut HashMap<i32, usize> = npid_lookup.entry(score.user_id).or_default();
					user_entry.insert(score.character_id, index);
				}

				cache_data.insert(
					TableDescription::new(com_id, board_id),
					Arc::new(RwLock::new(ScoreTableCache {
						sorted_scores,
						npid_lookup,
						table_info,
						last_insert: Client::get_psn_timestamp(),
					})),
				);
			}
		}

		{
			let vec_userinfo = db
				.get_username_and_online_name_from_user_ids(&users_list)
				.map_err(|_| "Failed to acquire all the users informations!")?;
			let mut users_data = cache.users.write();
			vec_userinfo.iter().for_each(|u| {
				users_data.insert(
					u.0,
					ScoreUserCache {
						npid: u.1.clone(),
						online_name: u.2.clone(),
					},
				);
			});
		}

		Ok(cache)
	}
}

impl ScoreRankDataCache {
	pub fn to_flatbuffer<'a>(&self, builder: &mut flatbuffers::FlatBufferBuilder<'a>) -> flatbuffers::WIPOffset<ScoreRankData<'a>> {
		let str_npid = builder.create_string(&self.npid);
		let str_online_name = builder.create_string(&self.online_name);
		ScoreRankData::create(
			builder,
			&ScoreRankDataArgs {
				npId: Some(str_npid),
				onlineName: Some(str_online_name),
				pcId: self.pcid,
				rank: self.rank + 1,
				score: self.score,
				hasGameData: self.has_gamedata,
				recordDate: self.timestamp,
			},
		)
	}
}

impl GetScoreResultCache {
	pub fn serialize(&self) -> Vec<u8> {
		let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);

		let mut vec_ranks = Vec::new();
		for s in &self.scores {
			vec_ranks.push(s.to_flatbuffer(&mut builder));
		}
		let rank_array = Some(builder.create_vector(&vec_ranks));

		let comment_array = if let Some(ref comments) = self.comments {
			let mut vec_comments = Vec::new();
			for c in comments {
				vec_comments.push(builder.create_string(c));
			}
			Some(builder.create_vector(&vec_comments))
		} else {
			None
		};

		let info_array = if let Some(ref game_infos) = self.infos {
			let mut vec_infos = Vec::new();
			for i in game_infos {
				let data = Some(builder.create_vector(i));
				vec_infos.push(ScoreInfo::create(&mut builder, &ScoreInfoArgs { data }));
			}
			Some(builder.create_vector(&vec_infos))
		} else {
			None
		};

		let board_info = GetScoreResponse::create(
			&mut builder,
			&GetScoreResponseArgs {
				rankArray: rank_array,
				commentArray: comment_array,
				infoArray: info_array,
				lastSortDate: self.timestamp,
				totalRecord: self.total_records,
			},
		);
		builder.finish(board_info, None);
		builder.finished_data().to_vec()
	}
}

impl DbScoreInfo {
	fn cmp(&self, other: &DbScoreInfo, sort_mode: u32) -> Ordering {
		let ret_value = if sort_mode == 0 { Ordering::Less } else { Ordering::Greater };

		match self.score.cmp(&other.score) {
			Ordering::Greater => ret_value,
			Ordering::Less => ret_value.reverse(),
			Ordering::Equal => match self.timestamp.cmp(&other.timestamp) {
				Ordering::Less => Ordering::Less,
				Ordering::Greater => Ordering::Greater,
				Ordering::Equal => other.user_id.cmp(&self.user_id),
			},
		}
	}
}

impl ScoreTableCache {
	fn from_db(db_info: &DbBoardInfo) -> ScoreTableCache {
		ScoreTableCache {
			sorted_scores: Vec::new(),
			npid_lookup: HashMap::new(),
			table_info: db_info.clone(),
			last_insert: 0,
		}
	}
}

impl ScoresCache {
	fn new() -> ScoresCache {
		ScoresCache {
			tables: RwLock::new(HashMap::new()),
			users: RwLock::new(HashMap::new()),
		}
	}

	fn get_table(&self, com_id: &ComId, board_id: u32, db_info: &DbBoardInfo) -> Arc<RwLock<ScoreTableCache>> {
		let table_desc = TableDescription::new(com_id_to_string(com_id), board_id);
		{
			let tables = self.tables.read();
			if let Some(table) = tables.get(&table_desc) {
				return table.clone();
			}
		}

		self.tables
			.write()
			.entry(table_desc)
			.or_insert_with(|| Arc::new(RwLock::new(ScoreTableCache::from_db(db_info))))
			.clone()
	}

	fn add_user(&self, user_id: i64, npid: &str, online_name: &str) {
		if self.users.read().contains_key(&user_id) {
			return;
		}

		self.users.write().insert(
			user_id,
			ScoreUserCache {
				npid: npid.to_owned(),
				online_name: online_name.to_owned(),
			},
		);
	}

	pub fn insert_score(&self, db_info: &DbBoardInfo, com_id: &ComId, board_id: u32, score: &DbScoreInfo, npid: &str, online_name: &str) -> u32 {
		let table = self.get_table(com_id, board_id, db_info);
		let mut table = table.write();

		// First check if the user_id/char_id is already in the cache
		// Note that the inserted position may be lower to the previous position if the score inserted is the same but timestamp is higher
		let mut initial_pos = None;

		if table.npid_lookup.contains_key(&score.user_id) && table.npid_lookup[&score.user_id].contains_key(&score.character_id) {
			let pos = table.npid_lookup[&score.user_id][&score.character_id];
			table.sorted_scores.remove(pos);
			initial_pos = Some(pos);
		}

		if (table.sorted_scores.len() < table.table_info.rank_limit as usize) || score.cmp(table.sorted_scores.last().unwrap(), table.table_info.sort_mode) == Ordering::Less {
			let insert_pos = table.sorted_scores.binary_search_by(|probe| probe.cmp(score, table.table_info.sort_mode)).unwrap_err();
			table.sorted_scores.insert(insert_pos, (*score).clone());
			table.npid_lookup.entry(score.user_id).or_default().insert(score.character_id, insert_pos);

			let reorder_start = if let Some(pos) = initial_pos { std::cmp::min(pos, insert_pos + 1) } else { insert_pos + 1 };

			// Set index for everything after insert_pos
			for i in reorder_start..table.sorted_scores.len() {
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

			self.add_user(score.user_id, npid, online_name);

			(insert_pos + 1) as u32
		} else {
			table.table_info.rank_limit + 1
		}
	}

	pub fn get_score_range(&self, com_id: &ComId, board_id: u32, start_rank: u32, num_ranks: u32, with_comment: bool, with_gameinfo: bool) -> GetScoreResultCache {
		let mut vec_scores = Vec::new();
		let mut vec_comments = if with_comment { Some(Vec::new()) } else { None };
		let mut vec_gameinfos = if with_gameinfo { Some(Vec::new()) } else { None };

		let table = self.get_table(com_id, board_id, &Default::default());
		let table = table.read();
		let users = self.users.read();

		let start_index = (start_rank - 1) as usize;
		let end_index = std::cmp::min(start_index + num_ranks as usize, table.sorted_scores.len());
		for index in start_index..end_index {
			let cur_score = &table.sorted_scores[index];
			let cur_user = &users[&cur_score.user_id];
			vec_scores.push(ScoreRankDataCache {
				npid: cur_user.npid.clone(),
				online_name: cur_user.online_name.clone(),
				pcid: cur_score.character_id,
				rank: index as u32,
				score: cur_score.score,
				has_gamedata: cur_score.data_id.is_some(),
				timestamp: cur_score.timestamp,
			});
			if let Some(ref mut comments) = vec_comments {
				comments.push(cur_score.comment.clone().unwrap_or_default());
			}
			if let Some(ref mut gameinfos) = vec_gameinfos {
				gameinfos.push(cur_score.game_info.clone().unwrap_or_default());
			}
		}
		GetScoreResultCache {
			scores: vec_scores,
			comments: vec_comments,
			infos: vec_gameinfos,
			timestamp: table.last_insert,
			total_records: table.sorted_scores.len() as u32,
		}
	}

	pub fn get_score_ids(&self, com_id: &ComId, board_id: u32, npids: &[(i64, i32)], with_comment: bool, with_gameinfo: bool) -> GetScoreResultCache {
		let mut vec_scores = Vec::new();
		let mut vec_comments = if with_comment { Some(Vec::new()) } else { None };
		let mut vec_gameinfos = if with_gameinfo { Some(Vec::new()) } else { None };

		let table = self.get_table(com_id, board_id, &Default::default());
		let table = table.read();
		let users = self.users.read();

		npids.iter().for_each(|(user_id, pcid)| {
			if !table.npid_lookup.contains_key(user_id) || !table.npid_lookup[user_id].contains_key(pcid) {
				vec_scores.push(Default::default());
				if let Some(ref mut comments) = vec_comments {
					comments.push(Default::default());
				}
				if let Some(ref mut gameinfos) = vec_gameinfos {
					gameinfos.push(Default::default());
				}
			} else {
				let index = table.npid_lookup[user_id][pcid];
				let cur_user = &users[user_id];
				let cur_score = &table.sorted_scores[index];
				vec_scores.push(ScoreRankDataCache {
					npid: cur_user.npid.clone(),
					online_name: cur_user.online_name.clone(),
					pcid: cur_score.character_id,
					rank: index as u32,
					score: cur_score.score,
					has_gamedata: cur_score.data_id.is_some(),
					timestamp: cur_score.timestamp,
				});
				if let Some(ref mut comments) = vec_comments {
					comments.push(cur_score.comment.clone().unwrap_or_default());
				}
				if let Some(ref mut gameinfos) = vec_gameinfos {
					gameinfos.push(cur_score.game_info.clone().unwrap_or_default());
				}
			}
		});

		GetScoreResultCache {
			scores: vec_scores,
			comments: vec_comments,
			infos: vec_gameinfos,
			timestamp: table.last_insert,
			total_records: table.sorted_scores.len() as u32,
		}
	}

	pub fn contains_score_with_no_data(&self, com_id: &ComId, user_id: i64, character_id: i32, board_id: u32, score: i64) -> Result<(), ErrorType> {
		let table = self.get_table(com_id, board_id, &DbBoardInfo::default());

		{
			let table = table.read();
			if !table.npid_lookup.contains_key(&user_id) || !table.npid_lookup[&user_id].contains_key(&character_id) {
				warn!("Attempted to set score data for a missing UserId/PcId!");
				return Err(ErrorType::NotFound);
			}

			let rank = table.npid_lookup[&user_id][&character_id];
			let the_score = &table.sorted_scores[rank];

			if the_score.score != score {
				warn!("Attempted to update score data for wrong score!");
				return Err(ErrorType::ScoreInvalid);
			}

			if the_score.data_id.is_some() {
				warn!("Attempted to update score data of score with existing score data!");
				return Err(ErrorType::ScoreHasData);
			}
		}

		Ok(())
	}

	pub fn set_game_data(&self, com_id: &ComId, user_id: i64, character_id: i32, board_id: u32, data_id: u64) -> Result<(), ErrorType> {
		let table = self.get_table(com_id, board_id, &DbBoardInfo::default());

		{
			// Existence needs to be checked again as another score might have pushed this one out
			let mut table = table.write();
			if !table.npid_lookup.contains_key(&user_id) || !table.npid_lookup[&user_id].contains_key(&character_id) {
				return Err(ErrorType::NotFound);
			}

			// Score itself doesn't need to be checked as only current user can change it

			let rank = table.npid_lookup[&user_id][&character_id];
			let the_score = &mut table.sorted_scores[rank];

			the_score.data_id = Some(data_id);
		}

		Ok(())
	}

	pub fn get_game_data_id(&self, com_id: &ComId, user_id: i64, character_id: i32, board_id: u32) -> Result<u64, ErrorType> {
		let table = self.get_table(com_id, board_id, &DbBoardInfo::default());

		{
			let table = table.read();
			if !table.npid_lookup.contains_key(&user_id) || !table.npid_lookup[&user_id].contains_key(&character_id) {
				return Err(ErrorType::NotFound);
			}

			let rank = table.npid_lookup[&user_id][&character_id];
			let the_score = &table.sorted_scores[rank];
			if let Some(data_id) = the_score.data_id {
				Ok(data_id)
			} else {
				Err(ErrorType::NotFound)
			}
		}
	}
}
