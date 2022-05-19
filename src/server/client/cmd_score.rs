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
			if let Err(_) = r {
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
	fn delete_score_data(data_name: &str) {
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

		reply.push(ErrorType::NoError as u8);
		reply.extend(&builder.finished_data().to_vec());

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
			timestamp: Client::get_timestamp_micros(),
		};

		let db = Database::new(self.get_database_connection(reply)?);
		let res = db.record_score(&com_id, score_req.boardId(), &score_infos, self.config.read().is_create_missing());

		match res {
			Ok(board_infos) => {
				self.score_cache.insert_score(&board_infos, &com_id, score_req.boardId(), &score_infos);
			}
			Err(e) => match e {
				DbError::ScoreNotBest => reply.push(ErrorType::ScoreNotBest as u8),
				DbError::Internal => reply.push(ErrorType::DbFail as u8),
				_ => unreachable!(),
			},
		}
		Ok(())
	}

	pub async fn retrieve_scores(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<(), ()> {
		Ok(())
	}

	pub async fn store_score_data(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<(), ()> {
		Ok(())
	}

	pub async fn get_score_data(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<(), ()> {
		Ok(())
	}
}
