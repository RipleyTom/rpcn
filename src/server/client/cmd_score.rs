// Score Commands

use std::sync::atomic::{AtomicU64, Ordering};

use tokio::fs;

use crate::server::client::*;
use crate::server::database::db_score::DbScoreInfo;
use crate::server::database::DbError;
use crate::server::Server;

const SCORE_DATA_DIRECTORY: &str = "score_data";
const SCORE_FILE_EXTENSION: &str = "sdt";
static SCORE_DATA_ID_DISPENSER: AtomicU64 = AtomicU64::new(1);

impl Server {
	pub fn initialize_score_data_handler() -> Result<(), String> {
		let max = Server::create_data_directory(SCORE_DATA_DIRECTORY, SCORE_FILE_EXTENSION)?;
		SCORE_DATA_ID_DISPENSER.store(max, Ordering::SeqCst);
		Ok(())
	}

	pub fn clean_score_data(conn: r2d2::PooledConnection<r2d2_sqlite::SqliteConnectionManager>) -> Result<(), String> {
		let db = Database::new(conn);

		let dir_ids_list = Server::get_ids_from_directory(SCORE_DATA_DIRECTORY, SCORE_FILE_EXTENSION)?;
		let db_ids_list = db.score_get_all_data_ids().map_err(|_| String::from("Failure to get score data ids from database"))?;

		let unused_ids = dir_ids_list.difference(&db_ids_list);
		let mut num_deleted = 0;

		for unused_id in unused_ids {
			let filename = Client::score_id_to_path(*unused_id);
			std::fs::remove_file(&filename).map_err(|e| format!("Failed to delete score data file({}): {}", filename, e))?;
			num_deleted += 1;
		}

		if num_deleted != 0 {
			println!("Deleted {} score data files", num_deleted);
		}

		Ok(())
	}
}

impl Client {
	fn score_id_to_path(id: u64) -> String {
		format!("{}/{:020}.{}", SCORE_DATA_DIRECTORY, id, SCORE_FILE_EXTENSION)
	}

	async fn create_score_data_file(data: &[u8]) -> u64 {
		let id = SCORE_DATA_ID_DISPENSER.fetch_add(1, Ordering::SeqCst);
		let path = Client::score_id_to_path(id);

		let file = fs::File::create(&path).await;
		if let Err(e) = file {
			error!("Failed to create score data {}: {}", &path, e);
			return 0;
		}
		let mut file = file.unwrap();

		if let Err(e) = file.write_all(data).await {
			error!("Failed to write score data {}: {}", &path, e);
			return 0;
		}

		id
	}

	async fn get_score_data_file(id: u64) -> Result<Vec<u8>, ErrorType> {
		let path = Client::score_id_to_path(id);

		fs::read(&path).await.map_err(|e| {
			error!("Failed to open/read score data file {}: {}", &path, e);
			ErrorType::NotFound
		})
	}

	async fn delete_score_data(id: u64) {
		let path = Client::score_id_to_path(id);
		if let Err(e) = std::fs::remove_file(&path) {
			error!("Failed to delete score data {}: {}", &path, e);
			// Todo: wait in case of usage by another user
			// or clean it on next server restart?
		}
	}

	pub async fn get_board_infos(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let com_id = self.get_com_id_with_redir(data);
		let board_id = data.get::<u32>();

		if data.error() {
			warn!("Error while extracting data from GetBoardInfos command");
			return Err(ErrorType::Malformed);
		}

		let db = Database::new(self.get_database_connection()?);
		let res = db.get_board_infos(&com_id, board_id, self.config.read().is_create_missing());
		if let Err(e) = res {
			match e {
				DbError::Empty => {
					return Ok(ErrorType::NotFound);
				}
				_ => {
					return Err(ErrorType::DbFail);
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
		Client::add_data_packet(reply, &finished_data);

		Ok(ErrorType::NoError)
	}

	pub async fn record_score(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let (com_id, score_req) = self.get_com_and_fb::<RecordScoreRequest>(data)?;

		let score_infos = DbScoreInfo {
			user_id: self.client_info.user_id,
			character_id: score_req.pcId(),
			score: score_req.score(),
			comment: score_req.comment().map(|s| s.to_owned()),
			game_info: score_req.data().map(|v| v.iter().collect()),
			data_id: None,
			timestamp: Client::get_psn_timestamp(),
		};

		let db = Database::new(self.get_database_connection()?);
		let res = db.record_score(&com_id, score_req.boardId(), &score_infos, self.config.read().is_create_missing());

		match res {
			Ok(board_infos) => {
				let pos = self
					.shared
					.score_cache
					.insert_score(&board_infos, &com_id, score_req.boardId(), &score_infos, &self.client_info.npid, &self.client_info.online_name);
				reply.extend(&pos.to_le_bytes());
				Ok(ErrorType::NoError)
			}
			Err(e) => Ok(match e {
				DbError::ScoreNotBest => ErrorType::ScoreNotBest,
				DbError::Internal => ErrorType::DbFail,
				_ => unreachable!(),
			}),
		}
	}

	pub async fn record_score_data(&mut self, data: &mut StreamExtractor) -> Result<ErrorType, ErrorType> {
		let com_id = self.get_com_id_with_redir(data);
		let score_req = data.get_flatbuffer::<RecordScoreGameDataRequest>();
		let score_data = data.get_rawdata();

		if data.error() || score_req.is_err() {
			warn!("Error while extracting data from RecordScoreData command");
			return Err(ErrorType::Malformed);
		}
		let score_req = score_req.unwrap();

		// Before going further make sure that the score exist
		if let Err(e) = self
			.shared
			.score_cache
			.contains_score_with_no_data(&com_id, self.client_info.user_id, score_req.pcId(), score_req.boardId(), score_req.score())
		{
			return Ok(e);
		}

		let score_data_id = Client::create_score_data_file(&score_data).await;

		// Update cache
		if let Err(e) = self
			.shared
			.score_cache
			.set_game_data(&com_id, self.client_info.user_id, score_req.pcId(), score_req.boardId(), score_data_id)
		{
			Client::delete_score_data(score_data_id).await;
			return Ok(e);
		}

		// Update db
		let db = Database::new(self.get_database_connection()?);
		if db
			.set_score_data(&com_id, self.client_info.user_id, score_req.pcId(), score_req.boardId(), score_req.score(), score_data_id)
			.is_err()
		{
			error!("Unexpected error updating score game data in database!");
		}

		Ok(ErrorType::NoError)
	}

	pub async fn get_score_data(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let (com_id, score_req) = self.get_com_and_fb::<GetScoreGameDataRequest>(data)?;
		let npid = Client::validate_and_unwrap(score_req.npId())?;

		let db = Database::new(self.get_database_connection()?);
		let user_id = db.get_user_id(npid);

		if user_id.is_err() {
			return Ok(ErrorType::NotFound);
		}
		let user_id = user_id.unwrap();

		let data_id = self.shared.score_cache.get_game_data_id(&com_id, user_id, score_req.pcId(), score_req.boardId());
		if let Err(e) = data_id {
			return Ok(e);
		}
		let data_id = data_id.unwrap();

		let data = Client::get_score_data_file(data_id).await;
		if let Err(e) = data {
			return Ok(e);
		}
		let data = data.unwrap();
		Client::add_data_packet(reply, &data);

		Ok(ErrorType::NoError)
	}

	pub async fn get_score_range(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let (com_id, score_req) = self.get_com_and_fb::<GetScoreRangeRequest>(data)?;

		let getscore_result = self.shared.score_cache.get_score_range(
			&com_id,
			score_req.boardId(),
			score_req.startRank(),
			score_req.numRanks(),
			score_req.withComment(),
			score_req.withGameInfo(),
		);
		let finished_data = getscore_result.serialize();
		Client::add_data_packet(reply, &finished_data);

		Ok(ErrorType::NoError)
	}

	pub async fn get_score_friends(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let (com_id, score_req) = self.get_com_and_fb::<GetScoreFriendsRequest>(data)?;

		let mut id_vec = Vec::new();

		if score_req.include_self() {
			id_vec.push((self.client_info.user_id, 0));
		}

		let friends = { self.shared.client_infos.read().get(&self.client_info.user_id).unwrap().friend_info.read().friends.clone() };

		friends.iter().for_each(|(user_id, _)| id_vec.push((*user_id, 0)));
		id_vec.truncate(score_req.max() as usize);

		let getscore_result = self
			.shared
			.score_cache
			.get_score_ids(&com_id, score_req.boardId(), &id_vec, score_req.withComment(), score_req.withGameInfo());
		let finished_data = getscore_result.serialize();

		reply.extend(&(finished_data.len() as u32).to_le_bytes());
		reply.extend(finished_data);

		Ok(ErrorType::NoError)
	}

	pub async fn get_score_npid(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let (com_id, score_req) = self.get_com_and_fb::<GetScoreNpIdRequest>(data)?;

		let db = Database::new(self.get_database_connection()?);

		let mut id_vec: Vec<(i64, i32)> = Vec::new();
		if let Some(npids) = score_req.npids() {
			for i in 0..npids.len() {
				let npid_and_pcid = npids.get(i);
				let user_id = db.get_user_id(npid_and_pcid.npid().unwrap_or("")).unwrap_or(0);
				id_vec.push((user_id, npid_and_pcid.pcId()));
			}
		}

		let getscore_result = self
			.shared
			.score_cache
			.get_score_ids(&com_id, score_req.boardId(), &id_vec, score_req.withComment(), score_req.withGameInfo());
		let finished_data = getscore_result.serialize();
		Client::add_data_packet(reply, &finished_data);

		Ok(ErrorType::NoError)
	}
}
