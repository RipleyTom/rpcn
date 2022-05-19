// Score Commands

use std::fs;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::server::client::*;
use crate::server::database::{DbError, DbScoreInfo};
use crate::server::Server;

const SCORE_DATA_DIRECTORY: &str = "score_data";
static SCORE_DATA_ID_DISPENSER: AtomicU64 = AtomicU64::new(1);

impl Server {
	pub fn initialize_score_data_handler() -> Result<(), String> {
		match fs::create_dir(SCORE_DATA_DIRECTORY) {
			Ok(_) => {}
			Err(e) => match e.kind() {
				io::ErrorKind::AlreadyExists => {}
				other_error => {
					return Err(format!("Failed to create score_data directory: {}", other_error));
				}
			},
		}

		let mut max = 0u64;

		for file in fs::read_dir(SCORE_DATA_DIRECTORY).map_err(|e| format!("Failed to list score_data directory: {}", e))? {
			if let Err(e) = file {
				println!("Error reading file inside {}: {}", SCORE_DATA_DIRECTORY, e);
				continue;
			}
			let file = file.unwrap();

			let filename = file.file_name().into_string();
			if let Err(_) = filename {
				println!("A file inside score_data contains invalid unicode");
				continue;
			}
			let filename = filename.unwrap();

			let r = filename.parse::<u64>();
			if r.is_err() {
				println!("A file inside score_data doesn't have an integer filename: {}", filename);
				continue;
			}
			let r = r.unwrap();

			if r > max {
				max = r;
			}
		}

		max += 1;
		SCORE_DATA_ID_DISPENSER.store(max, Ordering::SeqCst);

		Ok(())
	}
}

impl Client {
	fn _delete_score_data(data_name: &str) {
		let path = format!("{}/{}", SCORE_DATA_DIRECTORY, data_name);
		if let Err(e) = fs::remove_file(path) {
			error!("Failed to delete score data {}: {}", data_name, e);
			// Todo: wait in case of usage by another user
		}
	}

	pub async fn get_board_infos(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<(), ()> {
		let com_id = self.get_com_id_with_redir(data);
		let board_id = data.get::<u32>();

		if data.error() {
			warn!("Error while extracting data from GetBoardInfos command");
			reply.push(ErrorType::Malformed as u8);
			return Err(());
		}

		let db = Database::new(self.get_database_connection(reply)?);
		let res = db.get_board_infos(&com_id, board_id, self.config.read().is_create_missing());
		if let Err(e) = res {
			match e {
				DbError::Empty => {
					reply.push(ErrorType::NotFound as u8);
					return Ok(());
				}
				_ => {
					reply.push(ErrorType::DbFail as u8);
					return Err(());
				}
			}
		}
		let res = res.unwrap();

		let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);
		let board_info = BoardInfo::create(
			&mut builder,
			&BoardInfoArgs {
				rankLimit: res.rank_limit,
				updateMode: res.update_mode,
				sortMode: res.sort_mode,
				uploadNumLimit: res.upload_num_limit,
				uploadSizeLimit: res.upload_size_limit,
			},
		);
		builder.finish(board_info, None);
		let finished_data = builder.finished_data().to_vec();

		reply.push(ErrorType::NoError as u8);
		reply.extend(&(finished_data.len() as u32).to_le_bytes());
		reply.extend(finished_data);

		Ok(())
	}

	pub async fn record_score(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<(), ()> {
		let com_id = self.get_com_id_with_redir(data);
		let score_req = data.get_flatbuffer::<RecordScoreRequest>();

		if data.error() || score_req.is_err() {
			warn!("Error while extracting data from RecordScore command");
			reply.push(ErrorType::Malformed as u8);
			return Err(());
		}

		let score_req = score_req.unwrap();

		let score_infos = DbScoreInfo {
			user_id: self.client_info.user_id,
			character_id: score_req.pcId(),
			score: score_req.score(),
			comment: score_req.comment().map(|s| s.to_owned()),
			game_info: score_req.data().map(|v| v.to_owned()),
			data_id: None,
			timestamp: Client::get_psn_timestamp(),
		};

		let db = Database::new(self.get_database_connection(reply)?);
		let res = db.record_score(&com_id, score_req.boardId(), &score_infos, self.config.read().is_create_missing());

		match res {
			Ok(board_infos) => {
				reply.push(ErrorType::NoError as u8);
				let pos = self
					.score_cache
					.insert_score(&board_infos, &com_id, score_req.boardId(), &score_infos, &self.client_info.npid, &self.client_info.online_name);
				reply.extend(&pos.to_le_bytes());
			}
			Err(e) => match e {
				DbError::ScoreNotBest => reply.push(ErrorType::ScoreNotBest as u8),
				DbError::Internal => reply.push(ErrorType::DbFail as u8),
				_ => unreachable!(),
			},
		}
		Ok(())
	}

	pub async fn store_score_data(&mut self, _data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<(), ()> {
		reply.push(ErrorType::Unsupported as u8);
		Err(())
	}

	pub async fn get_score_data(&mut self, _data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<(), ()> {
		reply.push(ErrorType::Unsupported as u8);
		Err(())
	}

	pub async fn get_score_range(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<(), ()> {
		let com_id = self.get_com_id_with_redir(data);
		let score_req = data.get_flatbuffer::<GetScoreRangeRequest>();

		if data.error() || score_req.is_err() {
			warn!("Error while extracting data from GetScoreRange command");
			reply.push(ErrorType::Malformed as u8);
			return Err(());
		}

		let score_req = score_req.unwrap();
		let getscore_result = self.score_cache.get_score_range(
			&com_id,
			score_req.boardId(),
			score_req.startRank(),
			score_req.numRanks(),
			score_req.withComment(),
			score_req.withGameInfo(),
		);
		let finished_data = getscore_result.serialize();

		reply.push(ErrorType::NoError as u8);
		reply.extend(&(finished_data.len() as u32).to_le_bytes());
		reply.extend(finished_data);

		Ok(())
	}

	pub async fn get_score_friends(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<(), ()> {
		let com_id = self.get_com_id_with_redir(data);
		let score_req = data.get_flatbuffer::<GetScoreFriendsRequest>();

		if data.error() || score_req.is_err() {
			warn!("Error while extracting data from GetScoreFriends command");
			reply.push(ErrorType::Malformed as u8);
			return Err(());
		}

		let score_req = score_req.unwrap();

		let mut id_vec = Vec::new();

		if score_req.include_self() {
			id_vec.push((self.client_info.user_id, 0));
		}

		let friends = {
			let sig_infos = self.signaling_infos.read();
			let self_ci = sig_infos.get(&self.client_info.user_id).unwrap();
			self_ci.friends.clone()
		};

		friends.iter().for_each(|user_id| id_vec.push((*user_id, 0)));
		id_vec.truncate(score_req.max() as usize);

		let getscore_result = self.score_cache.get_score_ids(&com_id, score_req.boardId(), &id_vec, score_req.withComment(), score_req.withGameInfo());
		let finished_data = getscore_result.serialize();

		reply.push(ErrorType::NoError as u8);
		reply.extend(&(finished_data.len() as u32).to_le_bytes());
		reply.extend(finished_data);

		Ok(())
	}

	pub async fn get_score_npid(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<(), ()> {
		let com_id = self.get_com_id_with_redir(data);
		let score_req = data.get_flatbuffer::<GetScoreNpIdRequest>();

		if data.error() || score_req.is_err() {
			warn!("Error while extracting data from GetScoreNpid command");
			reply.push(ErrorType::Malformed as u8);
			return Err(());
		}

		let score_req = score_req.unwrap();

		let db = Database::new(self.get_database_connection(reply)?);

		let mut id_vec: Vec<(i64, i32)> = Vec::new();
		if let Some(npids) = score_req.npids() {
			for i in 0..npids.len() {
				let npid_and_pcid = npids.get(i);
				let user_id = db.get_user_id(npid_and_pcid.npid().unwrap_or("")).unwrap_or(0);
				id_vec.push((user_id, npid_and_pcid.pcId()));
			}
		}

		let getscore_result = self.score_cache.get_score_ids(&com_id, score_req.boardId(), &id_vec, score_req.withComment(), score_req.withGameInfo());
		let finished_data = getscore_result.serialize();

		reply.push(ErrorType::NoError as u8);
		reply.extend(&(finished_data.len() as u32).to_le_bytes());
		reply.extend(finished_data);

		Ok(())
	}
}
