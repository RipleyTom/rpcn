use std::sync::atomic::{AtomicU64, Ordering};

use tokio::fs;

use crate::server::client::*;
use crate::server::database::DbError;
use crate::server::Server;

const TUS_DATA_DIRECTORY: &str = "tus_data";
const TUS_FILE_EXTENSION: &str = "tdt";
static TUS_DATA_ID_DISPENSER: AtomicU64 = AtomicU64::new(1);

#[repr(i32)]
#[derive(FromPrimitive)]
#[allow(non_camel_case_types)]
pub enum TusSorting {
	SCE_NP_TUS_VARIABLE_SORTTYPE_DESCENDING_DATE = 1,
	SCE_NP_TUS_VARIABLE_SORTTYPE_ASCENDING_DATE,
	SCE_NP_TUS_VARIABLE_SORTTYPE_DESCENDING_VALUE,
	SCE_NP_TUS_VARIABLE_SORTTYPE_ASCENDING_VALUE,
}

#[repr(i32)]
#[derive(FromPrimitive)]
#[allow(non_camel_case_types)]
pub enum TusOpeType {
	SCE_NP_TUS_OPETYPE_EQUAL = 1,
	SCE_NP_TUS_OPETYPE_NOT_EQUAL,
	SCE_NP_TUS_OPETYPE_GREATER_THAN,
	SCE_NP_TUS_OPETYPE_GREATER_OR_EQUAL,
	SCE_NP_TUS_OPETYPE_LESS_THAN,
	SCE_NP_TUS_OPETYPE_LESS_OR_EQUAL,
}

#[repr(i32)]
#[derive(FromPrimitive)]
#[allow(non_camel_case_types)]
pub enum TusStatusSorting {
	SCE_NP_TUS_DATASTATUS_SORTTYPE_DESCENDING_DATE = 1,
	SCE_NP_TUS_DATASTATUS_SORTTYPE_ASCENDING_DATE,
}

const SCE_NP_TUS_DATA_INFO_MAX_SIZE: usize = 387;
const UNDOCUMENTED_TUS_DATA_MAX_SIZE: usize = 1024 * 1024; // 1MiB

impl Server {
	pub fn initialize_tus_data_handler() -> Result<(), String> {
		let max = Server::create_data_directory(TUS_DATA_DIRECTORY, TUS_FILE_EXTENSION)?;
		TUS_DATA_ID_DISPENSER.store(max, Ordering::SeqCst);
		Ok(())
	}

	pub fn clean_tus_data(conn: r2d2::PooledConnection<r2d2_sqlite::SqliteConnectionManager>) -> Result<(), String> {
		let db = Database::new(conn);

		let dir_ids_list = Server::get_ids_from_directory(TUS_DATA_DIRECTORY, TUS_FILE_EXTENSION)?;
		let db_ids_list = db.tus_get_all_data_ids().map_err(|_| String::from("Failure to get tus data ids from database"))?;

		let unused_ids = dir_ids_list.difference(&db_ids_list);
		let mut num_deleted = 0;

		for unused_id in unused_ids {
			let filename = Client::tus_id_to_path(*unused_id);
			std::fs::remove_file(&filename).map_err(|e| format!("Failed to delete tus data file({}): {}", filename, e))?;
			num_deleted += 1;
		}

		if num_deleted != 0 {
			println!("Deleted {} tus data files", num_deleted);
		}

		Ok(())
	}
}

impl Client {
	fn tus_id_to_path(id: u64) -> String {
		format!("{}/{:020}.{}", TUS_DATA_DIRECTORY, id, TUS_FILE_EXTENSION)
	}

	async fn create_tus_data_file(data: &[u8]) -> u64 {
		let id = TUS_DATA_ID_DISPENSER.fetch_add(1, Ordering::SeqCst);
		let path = Client::tus_id_to_path(id);

		let file = fs::File::create(&path).await;
		if let Err(e) = file {
			error!("Failed to create tus data {}: {}", &path, e);
			return 0;
		}
		let mut file = file.unwrap();

		if let Err(e) = file.write_all(data).await {
			error!("Failed to write tus data {}: {}", &path, e);
			return 0;
		}

		id
	}

	async fn get_tus_data_file(id: u64) -> Result<Vec<u8>, ErrorType> {
		let path = Client::tus_id_to_path(id);

		fs::read(&path).await.map_err(|e| {
			error!("Failed to open/read tus data file {}: {}", &path, e);
			ErrorType::NotFound
		})
	}

	async fn delete_tus_data(id: u64) {
		let path = Client::tus_id_to_path(id);
		if let Err(e) = std::fs::remove_file(&path) {
			error!("Failed to delete tus data {}: {}", &path, e);
		}
	}

	pub async fn tus_set_multislot_variable(&mut self, data: &mut StreamExtractor) -> Result<ErrorType, ErrorType> {
		let (com_id, tus_req) = self.get_com_and_fb::<TusSetMultiSlotVariableRequest>(data)?;
		let (user, slot_array, variable_array) = validate_all!(tus_req.user(), tus_req.slotIdArray(), tus_req.variableArray());
		let npid = Client::validate_and_unwrap(user.npid())?;

		if slot_array.len() != variable_array.len() {
			warn!("Validation inconsistency: slot_array.len() != variable_array.len()");
			return Err(ErrorType::Malformed);
		}

		let db = Database::new(self.get_database_connection()?);

		if user.vuser() {
			for i in 0..slot_array.len() {
				if let Err(e) = db.tus_set_vuser_variable(&com_id, npid, slot_array.get(i), variable_array.get(i), self.client_info.user_id, Client::get_psn_timestamp()) {
					error!("Error tus_set_vuser_variable: {}", e);
				}
			}
		} else {
			let user_id = match db.get_user_id(npid) {
				Err(DbError::Empty) => return Ok(ErrorType::NotFound),
				Err(_) => return Err(ErrorType::DbFail),
				Ok(id) => id,
			};

			if user_id != self.client_info.user_id {
				return Ok(ErrorType::Unauthorized);
			}

			for i in 0..slot_array.len() {
				if let Err(e) = db.tus_set_user_variable(&com_id, user_id, slot_array.get(i), variable_array.get(i), self.client_info.user_id, Client::get_psn_timestamp()) {
					error!("Error tus_set_user_variable: {}", e);
				}
			}
		}

		Ok(ErrorType::NoError)
	}

	pub async fn tus_get_multislot_variable(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let (com_id, tus_req) = self.get_com_and_fb::<TusGetMultiSlotVariableRequest>(data)?;
		let (user, slot_array) = validate_all!(tus_req.user(), tus_req.slotIdArray());
		let npid = Client::validate_and_unwrap(user.npid())?;

		let db = Database::new(self.get_database_connection()?);

		let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);
		let mut vec_vars = Vec::with_capacity(slot_array.len());

		if user.vuser() {
			for i in 0..slot_array.len() {
				match db.tus_get_vuser_variable(&com_id, npid, slot_array.get(i)) {
					Ok(var) => vec_vars.push(Some(var)),
					Err(DbError::Empty) => vec_vars.push(None),
					Err(e) => {
						error!("Error tus_get_vuser_variable: {}", e);
						return Err(ErrorType::DbFail);
					}
				}
			}
		} else {
			let user_id = match db.get_user_id(npid) {
				Err(DbError::Empty) => return Ok(ErrorType::NotFound),
				Err(_) => return Err(ErrorType::DbFail),
				Ok(id) => id,
			};

			for i in 0..slot_array.len() {
				match db.tus_get_user_variable(&com_id, user_id, slot_array.get(i)) {
					Ok(var) => vec_vars.push(Some(var)),
					Err(DbError::Empty) => vec_vars.push(None),
					Err(e) => {
						error!("Error tus_get_user_variable: {}", e);
						return Err(ErrorType::DbFail);
					}
				}
			}
		}

		let mut final_vars = Vec::with_capacity(vec_vars.len());

		for var in &vec_vars {
			let owner_fb = Some(builder.create_string(npid));
			match var {
				Some(var) => {
					let author_id_string = db.get_username(var.author_id).map_err(|_| ErrorType::DbFail)?;
					let author_id_fb_string = Some(builder.create_string(&author_id_string));
					final_vars.push(TusVariable::create(
						&mut builder,
						&TusVariableArgs {
							ownerId: owner_fb,
							hasData: true,
							lastChangedDate: var.timestamp,
							lastChangedAuthorId: author_id_fb_string,
							variable: var.variable,
							oldVariable: var.variable,
						},
					));
				}
				None => final_vars.push(TusVariable::create(
					&mut builder,
					&TusVariableArgs {
						ownerId: owner_fb,
						hasData: false,
						lastChangedDate: 0,
						lastChangedAuthorId: None,
						variable: 0,
						oldVariable: 0,
					},
				)),
			}
		}

		let vars = Some(builder.create_vector(&final_vars));
		let tus_var_response = TusVarResponse::create(&mut builder, &TusVarResponseArgs { vars: vars });
		builder.finish(tus_var_response, None);
		let finished_data = builder.finished_data().to_vec();

		reply.extend(&(finished_data.len() as u32).to_le_bytes());
		reply.extend(finished_data);

		Ok(ErrorType::NoError)
	}

	pub async fn tus_get_multiuser_variable(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let (com_id, tus_req) = self.get_com_and_fb::<TusGetMultiUserVariableRequest>(data)?;
		let users = Client::validate_and_unwrap(tus_req.users())?;

		for i in 0..users.len() {
			if users.get(i).npid().is_none() {
				return Err(ErrorType::Malformed);
			}
		}

		let slot = tus_req.slotId();
		let db = Database::new(self.get_database_connection()?);
		let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);
		let mut vec_vars = Vec::with_capacity(users.len());

		for i in 0..users.len() {
			let user = users.get(i);
			let npid = user.npid().unwrap();

			if user.vuser() {
				match db.tus_get_vuser_variable(&com_id, npid, slot) {
					Ok(var) => vec_vars.push((npid, Some(var))),
					Err(DbError::Empty) => vec_vars.push((npid, None)),
					Err(e) => {
						error!("Error tus_get_vuser_variable: {}", e);
						return Err(ErrorType::DbFail);
					}
				}
			} else {
				let user_id = match db.get_user_id(npid) {
					Err(DbError::Empty) => return Ok(ErrorType::NotFound),
					Err(_) => return Err(ErrorType::DbFail),
					Ok(id) => id,
				};

				match db.tus_get_user_variable(&com_id, user_id, slot) {
					Ok(var) => vec_vars.push((npid, Some(var))),
					Err(DbError::Empty) => vec_vars.push((npid, None)),
					Err(e) => {
						error!("Error tus_get_user_variable: {}", e);
						return Err(ErrorType::DbFail);
					}
				}
			}
		}

		let mut final_vars = Vec::with_capacity(vec_vars.len());

		for (npid, var) in &vec_vars {
			let owner_fb = Some(builder.create_string(npid));
			match var {
				Some(var) => {
					let author_id_string = db.get_username(var.author_id).map_err(|_| ErrorType::DbFail)?;
					let author_id_fb_string = Some(builder.create_string(&author_id_string));
					final_vars.push(TusVariable::create(
						&mut builder,
						&TusVariableArgs {
							ownerId: owner_fb,
							hasData: true,
							lastChangedDate: var.timestamp,
							lastChangedAuthorId: author_id_fb_string,
							variable: var.variable,
							oldVariable: var.variable,
						},
					));
				}
				None => final_vars.push(TusVariable::create(
					&mut builder,
					&TusVariableArgs {
						ownerId: owner_fb,
						hasData: false,
						lastChangedDate: 0,
						lastChangedAuthorId: None,
						variable: 0,
						oldVariable: 0,
					},
				)),
			}
		}

		let vars = Some(builder.create_vector(&final_vars));
		let tus_var_response = TusVarResponse::create(&mut builder, &TusVarResponseArgs { vars: vars });
		builder.finish(tus_var_response, None);
		let finished_data = builder.finished_data().to_vec();

		reply.extend(&(finished_data.len() as u32).to_le_bytes());
		reply.extend(finished_data);

		Ok(ErrorType::NoError)
	}

	pub async fn tus_get_friends_variable(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let (com_id, tus_req) = self.get_com_and_fb::<TusGetFriendsVariableRequest>(data)?;

		let sorting = FromPrimitive::from_i32(tus_req.sortType());
		if sorting.is_none() {
			error!("Unsupported sorting value in TusGetFriendsVariableRequest");
			return Err(ErrorType::Malformed);
		}
		let sorting = sorting.unwrap();

		let mut user_list = self.signaling_infos.read().get(&self.client_info.user_id).unwrap().friends.clone();
		if tus_req.includeSelf() {
			user_list.insert(self.client_info.user_id, self.client_info.npid.clone());
		}

		let mut final_vars = Vec::with_capacity(std::cmp::min(user_list.len(), tus_req.arrayNum() as usize));
		let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);

		if !user_list.is_empty() {
			let db = Database::new(self.get_database_connection()?);
			let vec_vars = db
				.tus_get_user_variable_from_user_list(&com_id, &user_list.keys().map(|k| *k).collect::<Vec<i64>>(), tus_req.slotId(), sorting, tus_req.arrayNum())
				.map_err(|_| ErrorType::DbFail)?;

			for (user_id, var) in &vec_vars {
				let author_id_string = self.get_username_with_helper(var.author_id, &user_list, &db)?;

				let owner_fb = Some(builder.create_string(&user_list.get(user_id).unwrap()));
				let author_id_fb_string = Some(builder.create_string(&author_id_string));
				final_vars.push(TusVariable::create(
					&mut builder,
					&TusVariableArgs {
						ownerId: owner_fb,
						hasData: true,
						lastChangedDate: var.timestamp,
						lastChangedAuthorId: author_id_fb_string,
						variable: var.variable,
						oldVariable: var.variable,
					},
				));
			}
		}

		let vars = Some(builder.create_vector(&final_vars));
		let tus_var_response = TusVarResponse::create(&mut builder, &TusVarResponseArgs { vars: vars });
		builder.finish(tus_var_response, None);
		let finished_data = builder.finished_data().to_vec();

		reply.extend(&(finished_data.len() as u32).to_le_bytes());
		reply.extend(finished_data);

		Ok(ErrorType::NoError)
	}

	pub async fn tus_add_and_get_variable(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let (com_id, tus_req) = self.get_com_and_fb::<TusAddAndGetVariableRequest>(data)?;
		let user = Client::validate_and_unwrap(tus_req.user())?;
		let npid = Client::validate_and_unwrap(user.npid())?;

		let slot = tus_req.slotId();
		let to_add = tus_req.inVariable();
		let timestamp = Client::get_psn_timestamp();
		let author_id = self.client_info.user_id;

		let compare_author_str = tus_req.isLastChangedAuthorId();

		let compare_timestamp = if let Some(vec_compare_timestamp) = tus_req.isLastChangedDate() {
			if vec_compare_timestamp.len() != 1 {
				warn!("vec_compare_timestamp is not len 1");
				return Err(ErrorType::Malformed);
			}
			Some(vec_compare_timestamp.get(0))
		} else {
			None
		};

		let db = Database::new(self.get_database_connection()?);

		let compare_author = if let Some(compare_author_str) = compare_author_str {
			match db.get_user_id(compare_author_str) {
				Ok(user_id) => Some(user_id),
				Err(DbError::Empty) => None, // If we can't find the user it's never going to match
				Err(_) => return Err(ErrorType::DbFail),
			}
		} else {
			None
		};

		let var = if user.vuser() {
			db.tus_add_and_get_vuser_variable(&com_id, npid, slot, to_add, author_id, timestamp, compare_timestamp, compare_author).map_err(|e| {
				error!("Error tus_add_and_get_vuser_variable: {}", e);
				ErrorType::DbFail
			})?
		} else {
			let user_id = match db.get_user_id(npid) {
				Err(DbError::Empty) => return Ok(ErrorType::NotFound),
				Err(_) => return Err(ErrorType::DbFail),
				Ok(id) => id,
			};

			if user_id != self.client_info.user_id {
				return Ok(ErrorType::Unauthorized);
			}

			db.tus_add_and_get_user_variable(&com_id, user_id, slot, to_add, author_id, timestamp, compare_timestamp, compare_author).map_err(|e| {
				error!("Error tus_add_and_get_user_variable: {}", e);
				ErrorType::DbFail
			})?
		};

		let author_id_string = db.get_username(var.author_id).map_err(|_| ErrorType::DbFail)?;

		let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);

		let owner_fb = Some(builder.create_string(npid));
		let author_id_fb_string = Some(builder.create_string(&author_id_string));

		let tus_var = TusVariable::create(
			&mut builder,
			&TusVariableArgs {
				ownerId: owner_fb,
				hasData: true,
				lastChangedDate: var.timestamp,
				lastChangedAuthorId: author_id_fb_string,
				variable: var.variable,
				oldVariable: var.variable,
			},
		);

		builder.finish(tus_var, None);
		let finished_data = builder.finished_data().to_vec();
		reply.extend(&(finished_data.len() as u32).to_le_bytes());
		reply.extend(finished_data);

		Ok(ErrorType::NoError)
	}

	pub async fn tus_try_and_set_variable(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let (com_id, tus_req) = self.get_com_and_fb::<TusTryAndSetVariableRequest>(data)?;
		let user = Client::validate_and_unwrap(tus_req.user())?;
		let npid = Client::validate_and_unwrap(user.npid())?;

		let slot = tus_req.slotId();

		let op_type = FromPrimitive::from_i32(tus_req.opeType());
		if op_type.is_none() {
			warn!("Unsupported sorting value in TusTryAndSetVariableRequest");
			return Err(ErrorType::Malformed);
		}
		let op_type: TusOpeType = op_type.unwrap();

		let to_set = tus_req.variable();
		let compare_author_str = tus_req.isLastChangedAuthorId();

		let compare_value = if let Some(vec_compare_value) = tus_req.compareValue() {
			if vec_compare_value.len() != 1 {
				warn!("vec_compare_value is not len 1");
				return Err(ErrorType::Malformed);
			}
			vec_compare_value.get(0)
		} else {
			to_set
		};

		let compare_timestamp = if let Some(vec_compare_timestamp) = tus_req.isLastChangedDate() {
			if vec_compare_timestamp.len() != 1 {
				warn!("vec_compare_timestamp is not len 1");
				return Err(ErrorType::Malformed);
			}
			Some(vec_compare_timestamp.get(0))
		} else {
			None
		};

		let timestamp = Client::get_psn_timestamp();
		let author_id = self.client_info.user_id;

		let db = Database::new(self.get_database_connection()?);

		let compare_author = if let Some(compare_author_str) = compare_author_str {
			match db.get_user_id(compare_author_str) {
				Ok(user_id) => Some(user_id),
				Err(DbError::Empty) => None, // If we can't find the user it's never going to match
				Err(_) => return Err(ErrorType::DbFail),
			}
		} else {
			None
		};

		let var = if user.vuser() {
			db.tus_try_and_set_vuser_variable(&com_id, npid, slot, to_set, author_id, timestamp, compare_value, op_type, compare_timestamp, compare_author)
				.map_err(|e| {
					error!("Error tus_try_and_set_vuser_variable: {}", e);
					ErrorType::DbFail
				})?
		} else {
			let user_id = match db.get_user_id(npid) {
				Err(DbError::Empty) => return Ok(ErrorType::NotFound),
				Err(_) => return Err(ErrorType::DbFail),
				Ok(id) => id,
			};

			if user_id != self.client_info.user_id {
				return Ok(ErrorType::Unauthorized);
			}

			db.tus_try_and_set_user_variable(&com_id, user_id, slot, to_set, author_id, timestamp, compare_value, op_type, compare_timestamp, compare_author)
				.map_err(|e| {
					error!("Error tus_try_and_set_user_variable: {}", e);
					ErrorType::DbFail
				})?
		};

		let author_id_string = db.get_username(var.author_id).map_err(|_| ErrorType::DbFail)?;

		let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);

		let owner_fb = Some(builder.create_string(npid));
		let author_id_fb_string = Some(builder.create_string(&author_id_string));

		let tus_var = TusVariable::create(
			&mut builder,
			&TusVariableArgs {
				ownerId: owner_fb,
				hasData: true,
				lastChangedDate: var.timestamp,
				lastChangedAuthorId: author_id_fb_string,
				variable: var.variable,
				oldVariable: var.variable,
			},
		);

		builder.finish(tus_var, None);
		let finished_data = builder.finished_data().to_vec();
		reply.extend(&(finished_data.len() as u32).to_le_bytes());
		reply.extend(finished_data);

		Ok(ErrorType::NoError)
	}

	pub async fn tus_delete_multislot_variable(&mut self, data: &mut StreamExtractor) -> Result<ErrorType, ErrorType> {
		let (com_id, tus_req) = self.get_com_and_fb::<TusDeleteMultiSlotVariableRequest>(data)?;
		let (user, slots) = validate_all!(tus_req.user(), tus_req.slotIdArray());
		let npid = Client::validate_and_unwrap(user.npid())?;

		let db = Database::new(self.get_database_connection()?);

		let slots: Vec<i32> = slots.iter().collect();

		if user.vuser() {
			db.tus_delete_vuser_variable_with_slotlist(&com_id, npid, &slots).map_err(|_| ErrorType::DbFail)?
		} else {
			let user_id = match db.get_user_id(npid) {
				Err(DbError::Empty) => return Ok(ErrorType::NotFound),
				Err(_) => return Err(ErrorType::DbFail),
				Ok(id) => id,
			};

			if user_id != self.client_info.user_id {
				return Ok(ErrorType::Unauthorized);
			}

			db.tus_delete_user_variable_with_slotlist(&com_id, user_id, &slots.to_owned()).map_err(|_| ErrorType::DbFail)?
		}

		Ok(ErrorType::NoError)
	}

	fn preliminary_set_tus_data_check<F>(f: F, compare_timestamp: &Option<u64>, compare_author_id: &Option<i64>) -> Option<Result<ErrorType, ErrorType>>
	where
		F: FnOnce() -> Result<Option<(u64, i64)>, DbError>,
	{
		if compare_timestamp.is_some() || compare_author_id.is_some() {
			match f() {
				Err(DbError::Internal) => return Some(Err(ErrorType::DbFail)),
				Err(_) => unreachable!(),
				Ok(None) => return None,
				Ok(Some((db_timestamp, db_author_id))) => {
					if let Some(compare_timestamp) = compare_timestamp {
						if db_timestamp > *compare_timestamp {
							return Some(Ok(ErrorType::CondFail));
						}
					}

					if let Some(compare_author_id) = *compare_author_id {
						if compare_author_id != db_author_id {
							return Some(Ok(ErrorType::CondFail));
						}
					}
				}
			}
		}
		None
	}

	pub async fn tus_set_data(&mut self, data: &mut StreamExtractor) -> Result<ErrorType, ErrorType> {
		let (com_id, tus_req) = self.get_com_and_fb::<TusSetDataRequest>(data)?;
		let (user, data) = validate_all!(tus_req.user(), tus_req.data());
		let npid = Client::validate_and_unwrap(user.npid())?;

		if let Some(info) = &tus_req.info() {
			if info.len() > SCE_NP_TUS_DATA_INFO_MAX_SIZE {
				warn!("info len > SCE_NP_TUS_DATA_INFO_MAX_SIZE");
				return Err(ErrorType::Malformed);
			}
		}

		if data.len() > UNDOCUMENTED_TUS_DATA_MAX_SIZE {
			warn!("data len > UNDOCUMENTED_TUS_DATA_MAX_SIZE");
			return Err(ErrorType::Malformed);
		}

		let mut compare_timestamp = None;
		if let Some(last_changed_date) = &tus_req.isLastChangedDate() {
			if last_changed_date.len() != 1 {
				return Err(ErrorType::Malformed);
			}
			compare_timestamp = Some(last_changed_date.get(0));
		}

		let author_str = tus_req.isLastChangedAuthorId();
		let slot = tus_req.slotId();
		let info = tus_req.info();

		let db = Database::new(self.get_database_connection()?);

		let compare_author_id;
		if let Some(author_str) = author_str {
			match db.get_user_id(author_str) {
				Err(DbError::Empty) => return Ok(ErrorType::CondFail), // If we can't find the user it can't possibly match
				Err(_) => return Err(ErrorType::DbFail),
				Ok(id) => compare_author_id = Some(id),
			}
		} else {
			compare_author_id = None;
		}

		let new_timestamp = Client::get_psn_timestamp();

		if user.vuser() {
			if let Some(ret_value) = Client::preliminary_set_tus_data_check(|| db.tus_get_vuser_data_timestamp_and_author(&com_id, npid, slot), &compare_timestamp, &compare_author_id) {
				return ret_value;
			}

			let data_id = Client::create_tus_data_file(data).await;

			let res = db.tus_set_vuser_data(&com_id, npid, slot, data_id, &info, self.client_info.user_id, new_timestamp, compare_timestamp, compare_author_id);
			match res {
				Ok(tus_data) => {
					if tus_data.author_id != self.client_info.user_id || tus_data.data_id != data_id || tus_data.timestamp != new_timestamp {
						Client::delete_tus_data(data_id).await;
						Ok(ErrorType::CondFail)
					} else {
						Ok(ErrorType::NoError)
					}
				}
				Err(_) => Err(ErrorType::DbFail),
			}
		} else {
			let user_id = match db.get_user_id(npid) {
				Err(DbError::Empty) => return Ok(ErrorType::NotFound),
				Err(_) => return Err(ErrorType::DbFail),
				Ok(id) => id,
			};

			if user_id != self.client_info.user_id {
				return Ok(ErrorType::Unauthorized);
			}

			if let Some(ret_value) = Client::preliminary_set_tus_data_check(|| db.tus_get_user_data_timestamp_and_author(&com_id, user_id, slot), &compare_timestamp, &compare_author_id) {
				return ret_value;
			}

			let data_id = Client::create_tus_data_file(data).await;

			let res = db.tus_set_user_data(&com_id, user_id, slot, data_id, &info, user_id, new_timestamp, compare_timestamp, compare_author_id);
			match res {
				Ok(tus_data) => {
					if tus_data.author_id != user_id || tus_data.data_id != data_id || tus_data.timestamp != new_timestamp {
						Client::delete_tus_data(data_id).await;
						Ok(ErrorType::CondFail)
					} else {
						Ok(ErrorType::NoError)
					}
				}
				Err(_) => Err(ErrorType::DbFail),
			}
		}
	}

	pub async fn tus_get_data(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let (com_id, tus_req) = self.get_com_and_fb::<TusGetDataRequest>(data)?;
		let user = Client::validate_and_unwrap(tus_req.user())?;
		let npid = Client::validate_and_unwrap(user.npid())?;
		let slot = tus_req.slotId();

		let db = Database::new(self.get_database_connection()?);

		let res = if user.vuser() {
			db.tus_get_vuser_data(&com_id, npid, slot)
		} else {
			let user_id = match db.get_user_id(npid) {
				Err(DbError::Empty) => return Ok(ErrorType::NotFound),
				Err(_) => return Err(ErrorType::DbFail),
				Ok(id) => id,
			};

			db.tus_get_user_data(&com_id, user_id, slot)
		};

		let info = match res {
			Ok(info) => Some(info),
			Err(DbError::Empty) => None,
			Err(_) => return Err(ErrorType::DbFail),
		};

		let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);

		let owner_id = Some(builder.create_string(&npid));
		let has_data;
		let last_changed_date;
		let last_changed_author_id;
		let tus_info;
		let tus_data;

		if let Some((db_tus_data_status, db_tus_data_info)) = info {
			let data = Client::get_tus_data_file(db_tus_data_status.data_id).await.map_err(|_| ErrorType::DbFail)?;
			let author_id_string = db.get_username(db_tus_data_status.author_id).map_err(|_| ErrorType::DbFail)?;

			has_data = true;
			last_changed_date = db_tus_data_status.timestamp;
			last_changed_author_id = Some(builder.create_string(&author_id_string));
			tus_info = Some(builder.create_vector(&db_tus_data_info));
			tus_data = Some(builder.create_vector(&data));
		} else {
			has_data = false;
			last_changed_date = 0;
			last_changed_author_id = None;
			tus_info = None;
			tus_data = None;
		}

		let tus_status = TusDataStatus::create(
			&mut builder,
			&TusDataStatusArgs {
				ownerId: owner_id,
				hasData: has_data,
				lastChangedDate: last_changed_date,
				lastChangedAuthorId: last_changed_author_id,
				info: tus_info,
			},
		);

		let final_tus_data = TusData::create(
			&mut builder,
			&TusDataArgs {
				status: Some(tus_status),
				data: tus_data,
			},
		);

		builder.finish(final_tus_data, None);
		let finished_data = builder.finished_data().to_vec();
		reply.extend(&(finished_data.len() as u32).to_le_bytes());
		reply.extend(finished_data);
		Ok(ErrorType::NoError)
	}

	pub async fn tus_get_multislot_data_status(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let (com_id, tus_req) = self.get_com_and_fb::<TusGetMultiSlotDataStatusRequest>(data)?;
		let user = Client::validate_and_unwrap(tus_req.user())?;
		let npid = Client::validate_and_unwrap(user.npid())?;
		let slots = Client::validate_and_unwrap(tus_req.slotIdArray())?;

		let db = Database::new(self.get_database_connection()?);

		let vec_status: Vec<_> = if user.vuser() {
			(0..slots.len())
				.map(|i| match db.tus_get_vuser_data(&com_id, npid, slots.get(i)) {
					Ok(status) => Some(status),
					Err(_) => None,
				})
				.collect()
		} else {
			let user_id = match db.get_user_id(npid) {
				Err(DbError::Empty) => return Ok(ErrorType::NotFound),
				Err(_) => return Err(ErrorType::DbFail),
				Ok(id) => id,
			};

			(0..slots.len())
				.map(|i| match db.tus_get_user_data(&com_id, user_id, slots.get(i)) {
					Ok(status) => Some(status),
					Err(_) => None,
				})
				.collect()
		};

		let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);

		let mut vec_list_fb_status = Vec::new();

		for status in &vec_status {
			let owner_id = Some(builder.create_string(&npid));
			let has_data;
			let last_changed_date;
			let last_changed_author_id;
			let tus_info;

			if let Some((db_tus_data_status, db_tus_data_info)) = status {
				let author_id_string = db.get_username(db_tus_data_status.author_id).map_err(|_| ErrorType::DbFail)?;

				has_data = true;
				last_changed_date = db_tus_data_status.timestamp;
				last_changed_author_id = Some(builder.create_string(&author_id_string));
				tus_info = Some(builder.create_vector(&db_tus_data_info));
			} else {
				has_data = false;
				last_changed_date = 0;
				last_changed_author_id = None;
				tus_info = None;
			}

			vec_list_fb_status.push(TusDataStatus::create(
				&mut builder,
				&TusDataStatusArgs {
					ownerId: owner_id,
					hasData: has_data,
					lastChangedDate: last_changed_date,
					lastChangedAuthorId: last_changed_author_id,
					info: tus_info,
				},
			));
		}

		let final_fb_status_list = Some(builder.create_vector(&vec_list_fb_status));

		let resp = TusDataStatusResponse::create(&mut builder, &TusDataStatusResponseArgs { status: final_fb_status_list });

		builder.finish(resp, None);
		let finished_data = builder.finished_data().to_vec();
		reply.extend(&(finished_data.len() as u32).to_le_bytes());
		reply.extend(finished_data);
		Ok(ErrorType::NoError)
	}

	pub async fn tus_get_multiuser_data_status(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let (com_id, tus_req) = self.get_com_and_fb::<TusGetMultiUserDataStatusRequest>(data)?;
		let users = Client::validate_and_unwrap(tus_req.users())?;
		let slot = tus_req.slotId();

		let db = Database::new(self.get_database_connection()?);

		let mut vec_status = Vec::with_capacity(users.len());
		let mut vec_npids = Vec::with_capacity(users.len());
		for user in &users {
			let npid = Client::validate_and_unwrap(user.npid())?;
			vec_npids.push(npid);

			vec_status.push(if user.vuser() {
				match db.tus_get_vuser_data(&com_id, npid, slot) {
					Ok(status) => Some(status),
					Err(_) => None,
				}
			} else {
				let user_id = match db.get_user_id(npid) {
					Err(DbError::Empty) => return Ok(ErrorType::NotFound),
					Err(_) => return Err(ErrorType::DbFail),
					Ok(id) => id,
				};

				match db.tus_get_user_data(&com_id, user_id, slot) {
					Ok(status) => Some(status),
					Err(_) => None,
				}
			});
		}

		let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);

		let mut vec_list_fb_status = Vec::new();

		for (status, npid) in vec_status.iter().zip(vec_npids.iter()) {
			let owner_id = Some(builder.create_string(npid));
			let has_data;
			let last_changed_date;
			let last_changed_author_id;
			let tus_info;

			if let Some((db_tus_data_status, db_tus_data_info)) = status {
				let author_id_string = db.get_username(db_tus_data_status.author_id).map_err(|_| ErrorType::DbFail)?;

				has_data = true;
				last_changed_date = db_tus_data_status.timestamp;
				last_changed_author_id = Some(builder.create_string(&author_id_string));
				tus_info = Some(builder.create_vector(&db_tus_data_info));
			} else {
				has_data = false;
				last_changed_date = 0;
				last_changed_author_id = None;
				tus_info = None;
			}

			vec_list_fb_status.push(TusDataStatus::create(
				&mut builder,
				&TusDataStatusArgs {
					ownerId: owner_id,
					hasData: has_data,
					lastChangedDate: last_changed_date,
					lastChangedAuthorId: last_changed_author_id,
					info: tus_info,
				},
			));
		}

		let final_fb_status_list = Some(builder.create_vector(&vec_list_fb_status));

		let resp = TusDataStatusResponse::create(&mut builder, &TusDataStatusResponseArgs { status: final_fb_status_list });

		builder.finish(resp, None);
		let finished_data = builder.finished_data().to_vec();
		reply.extend(&(finished_data.len() as u32).to_le_bytes());
		reply.extend(finished_data);
		Ok(ErrorType::NoError)
	}

	pub async fn tus_get_friends_data_status(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let (com_id, tus_req) = self.get_com_and_fb::<TusGetFriendsDataStatusRequest>(data)?;

		let sorting = FromPrimitive::from_i32(tus_req.sortType());
		if sorting.is_none() {
			warn!("Unsupported sorting value in TusGetFriendsDataStatusRequest");
			return Err(ErrorType::Malformed);
		}
		let sorting: TusStatusSorting = sorting.unwrap();

		let mut user_list = self.signaling_infos.read().get(&self.client_info.user_id).unwrap().friends.clone();
		if tus_req.includeSelf() {
			user_list.insert(self.client_info.user_id, self.client_info.npid.clone());
		}

		let mut final_infos = Vec::with_capacity(std::cmp::min(user_list.len(), tus_req.arrayNum() as usize));
		let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);

		if !user_list.is_empty() {
			let db = Database::new(self.get_database_connection()?);
			let vec_vars = db
				.tus_get_user_data_from_user_list(&com_id, &user_list.keys().map(|k| *k).collect::<Vec<i64>>(), tus_req.slotId(), sorting, tus_req.arrayNum())
				.map_err(|_| ErrorType::DbFail)?;

			for (user_id, var, db_tus_data_info) in &vec_vars {
				let author_id_string = self.get_username_with_helper(var.author_id, &user_list, &db)?;

				let owner_fb = Some(builder.create_string(&user_list.get(user_id).unwrap()));
				let author_id_fb_string = Some(builder.create_string(&author_id_string));

				let tus_info = Some(builder.create_vector(&db_tus_data_info));

				final_infos.push(TusDataStatus::create(
					&mut builder,
					&TusDataStatusArgs {
						ownerId: owner_fb,
						hasData: true,
						lastChangedDate: var.timestamp,
						lastChangedAuthorId: author_id_fb_string,
						info: tus_info,
					},
				));
			}
		}

		let status = Some(builder.create_vector(&final_infos));
		let tus_var_response = TusDataStatusResponse::create(&mut builder, &TusDataStatusResponseArgs { status });
		builder.finish(tus_var_response, None);
		let finished_data = builder.finished_data().to_vec();

		reply.extend(&(finished_data.len() as u32).to_le_bytes());
		reply.extend(finished_data);

		Ok(ErrorType::NoError)
	}

	pub async fn tus_delete_multislot_data(&mut self, data: &mut StreamExtractor) -> Result<ErrorType, ErrorType> {
		let (com_id, tus_req) = self.get_com_and_fb::<TusDeleteMultiSlotDataRequest>(data)?;
		let (user, slots) = validate_all!(tus_req.user(), tus_req.slotIdArray());
		let npid = Client::validate_and_unwrap(user.npid())?;

		let db = Database::new(self.get_database_connection()?);

		let slots: Vec<i32> = slots.iter().collect();

		if user.vuser() {
			db.tus_delete_vuser_data_with_slotlist(&com_id, npid, &slots).map_err(|_| ErrorType::DbFail)?
		} else {
			let user_id = match db.get_user_id(npid) {
				Err(DbError::Empty) => return Ok(ErrorType::NotFound),
				Err(_) => return Err(ErrorType::DbFail),
				Ok(id) => id,
			};

			if user_id != self.client_info.user_id {
				return Ok(ErrorType::Unauthorized);
			}

			db.tus_delete_user_data_with_slotlist(&com_id, user_id, &slots.to_owned()).map_err(|_| ErrorType::DbFail)?
		}

		Ok(ErrorType::NoError)
	}
}
