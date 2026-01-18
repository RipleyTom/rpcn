use std::sync::atomic::{AtomicU64, Ordering};

use prost::Message;
use tokio::fs;

use crate::server::Server;
use crate::server::client::*;
use crate::server::database::DbError;

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
		let (com_id, tus_req) = self.get_com_and_pb::<TusSetMultiSlotVariableRequest>(data)?;
		let user = tus_req.user.as_ref().ok_or(ErrorType::Malformed)?;
		let npid = &user.npid;

		if npid.is_empty() {
			warn!("Validation: npid is empty");
			return Err(ErrorType::Malformed);
		}

		if tus_req.slot_id_array.len() != tus_req.variable_array.len() {
			warn!("Validation: slot_array.len() != variable_array.len()");
			return Err(ErrorType::Malformed);
		}

		let db = Database::new(self.get_database_connection()?);

		if user.vuser {
			for i in 0..tus_req.slot_id_array.len() {
				if let Err(e) = db.tus_set_vuser_variable(
					&com_id,
					npid,
					tus_req.slot_id_array[i],
					tus_req.variable_array[i],
					self.client_info.user_id,
					Client::get_psn_timestamp(),
				) {
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

			for i in 0..tus_req.slot_id_array.len() {
				if let Err(e) = db.tus_set_user_variable(
					&com_id,
					user_id,
					tus_req.slot_id_array[i],
					tus_req.variable_array[i],
					self.client_info.user_id,
					Client::get_psn_timestamp(),
				) {
					error!("Error tus_set_user_variable: {}", e);
				}
			}
		}

		Ok(ErrorType::NoError)
	}

	pub async fn tus_get_multislot_variable(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let (com_id, tus_req) = self.get_com_and_pb::<TusGetMultiSlotVariableRequest>(data)?;
		let user = tus_req.user.as_ref().ok_or(ErrorType::Malformed)?;
		let npid = &user.npid;

		if npid.is_empty() {
			warn!("Validation: npid is empty");
			return Err(ErrorType::Malformed);
		}

		let db = Database::new(self.get_database_connection()?);

		let mut vec_vars = Vec::with_capacity(tus_req.slot_id_array.len());

		if user.vuser {
			for slot in &tus_req.slot_id_array {
				match db.tus_get_vuser_variable(&com_id, npid, *slot) {
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

			for slot in &tus_req.slot_id_array {
				match db.tus_get_user_variable(&com_id, user_id, *slot) {
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
			match var {
				Some(var) => {
					let author_id_string = db.get_username(var.author_id).map_err(|_| ErrorType::DbFail)?;
					final_vars.push(TusVariable {
						owner_id: npid.clone(),
						has_data: true,
						last_changed_date: var.timestamp,
						last_changed_author_id: author_id_string,
						variable: var.variable,
						old_variable: var.variable,
					});
				}
				None => final_vars.push(TusVariable {
					owner_id: npid.clone(),
					has_data: false,
					last_changed_date: 0,
					last_changed_author_id: String::new(),
					variable: 0,
					old_variable: 0,
				}),
			}
		}

		let tus_var_response = TusVarResponse { vars: final_vars };
		let finished_data = tus_var_response.encode_to_vec();

		Client::add_data_packet(reply, &finished_data);

		Ok(ErrorType::NoError)
	}

	pub async fn tus_get_multiuser_variable(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let (com_id, tus_req) = self.get_com_and_pb::<TusGetMultiUserVariableRequest>(data)?;

		if tus_req.users.iter().any(|user| user.npid.is_empty()) {
			return Err(ErrorType::Malformed);
		}

		let slot = tus_req.slot_id;
		let db = Database::new(self.get_database_connection()?);
		let mut vec_vars = Vec::with_capacity(tus_req.users.len());

		for user in &tus_req.users {
			let npid = &user.npid;

			if user.vuser {
				match db.tus_get_vuser_variable(&com_id, npid, slot) {
					Ok(var) => vec_vars.push((npid.clone(), Some(var))),
					Err(DbError::Empty) => vec_vars.push((npid.clone(), None)),
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
					Ok(var) => vec_vars.push((npid.clone(), Some(var))),
					Err(DbError::Empty) => vec_vars.push((npid.clone(), None)),
					Err(e) => {
						error!("Error tus_get_user_variable: {}", e);
						return Err(ErrorType::DbFail);
					}
				}
			}
		}

		let mut final_vars = Vec::with_capacity(vec_vars.len());

		for (npid, var) in &vec_vars {
			match var {
				Some(var) => {
					let author_id_string = db.get_username(var.author_id).map_err(|_| ErrorType::DbFail)?;
					final_vars.push(TusVariable {
						owner_id: npid.clone(),
						has_data: true,
						last_changed_date: var.timestamp,
						last_changed_author_id: author_id_string,
						variable: var.variable,
						old_variable: var.variable,
					});
				}
				None => final_vars.push(TusVariable {
					owner_id: npid.clone(),
					has_data: false,
					last_changed_date: 0,
					last_changed_author_id: String::new(),
					variable: 0,
					old_variable: 0,
				}),
			}
		}

		let tus_var_response = TusVarResponse { vars: final_vars };
		let finished_data = tus_var_response.encode_to_vec();

		Client::add_data_packet(reply, &finished_data);

		Ok(ErrorType::NoError)
	}

	pub async fn tus_get_friends_variable(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let (com_id, tus_req) = self.get_com_and_pb::<TusGetFriendsVariableRequest>(data)?;

		let sorting = FromPrimitive::from_i32(tus_req.sort_type);
		if sorting.is_none() {
			warn!("Unsupported sorting value in TusGetFriendsVariableRequest");
			return Err(ErrorType::Malformed);
		}
		let sorting = sorting.unwrap();

		let mut user_list = self.shared.client_infos.read().get(&self.client_info.user_id).unwrap().friend_info.read().friends.clone();
		if tus_req.include_self {
			user_list.insert(self.client_info.user_id, self.client_info.npid.clone());
		}

		let mut final_vars = Vec::with_capacity(std::cmp::min(user_list.len(), tus_req.array_num as usize));

		if !user_list.is_empty() {
			let db = Database::new(self.get_database_connection()?);
			let vec_vars = db
				.tus_get_user_variable_from_user_list(&com_id, &user_list.keys().copied().collect::<Vec<i64>>(), tus_req.slot_id, sorting, tus_req.array_num)
				.map_err(|_| ErrorType::DbFail)?;

			for (user_id, var) in &vec_vars {
				let author_id_string = self.get_username_with_helper(var.author_id, &user_list, &db)?;

				final_vars.push(TusVariable {
					owner_id: user_list.get(user_id).unwrap().clone(),
					has_data: true,
					last_changed_date: var.timestamp,
					last_changed_author_id: author_id_string,
					variable: var.variable,
					old_variable: var.variable,
				});
			}
		}

		let tus_var_response = TusVarResponse { vars: final_vars };
		let finished_data = tus_var_response.encode_to_vec();

		Client::add_data_packet(reply, &finished_data);

		Ok(ErrorType::NoError)
	}

	pub async fn tus_add_and_get_variable(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let (com_id, tus_req) = self.get_com_and_pb::<TusAddAndGetVariableRequest>(data)?;
		let user = tus_req.user.as_ref().ok_or(ErrorType::Malformed)?;
		let npid = &user.npid;

		if npid.is_empty() {
			warn!("Validation: npid is empty");
			return Err(ErrorType::Malformed);
		}

		let slot = tus_req.slot_id;
		let to_add = tus_req.in_variable;
		let timestamp = Client::get_psn_timestamp();
		let author_id = self.client_info.user_id;

		let compare_author_str = if tus_req.is_last_changed_author_id.is_empty() {
			None
		} else {
			Some(&tus_req.is_last_changed_author_id)
		};

		let compare_timestamp = if tus_req.is_last_changed_date.len() == 1 {
			Some(tus_req.is_last_changed_date[0])
		} else if tus_req.is_last_changed_date.is_empty() {
			None
		} else {
			warn!("vec_compare_timestamp is not len 1");
			return Err(ErrorType::Malformed);
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

		let var = if user.vuser {
			db.tus_add_and_get_vuser_variable(&com_id, npid, slot, to_add, author_id, timestamp, compare_timestamp, compare_author)
				.map_err(|e| {
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

			db.tus_add_and_get_user_variable(&com_id, user_id, slot, to_add, author_id, timestamp, compare_timestamp, compare_author)
				.map_err(|e| {
					error!("Error tus_add_and_get_user_variable: {}", e);
					ErrorType::DbFail
				})?
		};

		let author_id_string = db.get_username(var.author_id).map_err(|_| ErrorType::DbFail)?;

		let tus_var = TusVariable {
			owner_id: npid.clone(),
			has_data: true,
			last_changed_date: var.timestamp,
			last_changed_author_id: author_id_string,
			variable: var.variable,
			old_variable: var.variable,
		};

		let finished_data = tus_var.encode_to_vec();
		Client::add_data_packet(reply, &finished_data);

		Ok(ErrorType::NoError)
	}

	pub async fn tus_try_and_set_variable(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let (com_id, tus_req) = self.get_com_and_pb::<TusTryAndSetVariableRequest>(data)?;
		let user = tus_req.user.as_ref().ok_or(ErrorType::Malformed)?;
		let npid = &user.npid;

		if npid.is_empty() {
			warn!("Validation: npid is empty");
			return Err(ErrorType::Malformed);
		}

		let slot = tus_req.slot_id;

		let op_type = FromPrimitive::from_i32(tus_req.ope_type);
		if op_type.is_none() {
			warn!("Validation: unsupported sorting value in TusTryAndSetVariableRequest");
			return Err(ErrorType::Malformed);
		}
		let op_type: TusOpeType = op_type.unwrap();

		let to_set = tus_req.variable;
		let compare_author_str = if tus_req.is_last_changed_author_id.is_empty() {
			None
		} else {
			Some(&tus_req.is_last_changed_author_id)
		};

		let compare_value = if tus_req.compare_value.len() == 1 {
			tus_req.compare_value[0]
		} else if tus_req.compare_value.is_empty() {
			to_set
		} else {
			warn!("Validation: vec_compare_value is not len 1");
			return Err(ErrorType::Malformed);
		};

		let compare_timestamp = if tus_req.is_last_changed_date.len() == 1 {
			Some(tus_req.is_last_changed_date[0])
		} else if tus_req.is_last_changed_date.is_empty() {
			None
		} else {
			warn!("Validation: vec_compare_timestamp is not len 1");
			return Err(ErrorType::Malformed);
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

		let var = if user.vuser {
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

		let tus_var = TusVariable {
			owner_id: npid.clone(),
			has_data: true,
			last_changed_date: var.timestamp,
			last_changed_author_id: author_id_string,
			variable: var.variable,
			old_variable: var.variable,
		};

		let finished_data = tus_var.encode_to_vec();
		Client::add_data_packet(reply, &finished_data);

		Ok(ErrorType::NoError)
	}

	pub async fn tus_delete_multislot_variable(&mut self, data: &mut StreamExtractor) -> Result<ErrorType, ErrorType> {
		let (com_id, tus_req) = self.get_com_and_pb::<TusDeleteMultiSlotVariableRequest>(data)?;
		let user = tus_req.user.as_ref().ok_or(ErrorType::Malformed)?;
		let npid = &user.npid;

		if tus_req.slot_id_array.is_empty() {
			warn!("Validation: slot_id_array is empty");
			return Err(ErrorType::Malformed);
		}

		if npid.is_empty() {
			warn!("Validation: npid is empty");
			return Err(ErrorType::Malformed);
		}

		let db = Database::new(self.get_database_connection()?);

		if user.vuser {
			db.tus_delete_vuser_variable_with_slotlist(&com_id, npid, &tus_req.slot_id_array).map_err(|_| ErrorType::DbFail)?
		} else {
			let user_id = match db.get_user_id(npid) {
				Err(DbError::Empty) => return Ok(ErrorType::NotFound),
				Err(_) => return Err(ErrorType::DbFail),
				Ok(id) => id,
			};

			if user_id != self.client_info.user_id {
				return Ok(ErrorType::Unauthorized);
			}

			db.tus_delete_user_variable_with_slotlist(&com_id, user_id, &tus_req.slot_id_array).map_err(|_| ErrorType::DbFail)?
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
					if let Some(compare_timestamp) = compare_timestamp
						&& db_timestamp > *compare_timestamp
					{
						return Some(Ok(ErrorType::CondFail));
					}

					if let Some(compare_author_id) = *compare_author_id
						&& compare_author_id != db_author_id
					{
						return Some(Ok(ErrorType::CondFail));
					}
				}
			}
		}
		None
	}

	pub async fn tus_set_data(&mut self, data: &mut StreamExtractor) -> Result<ErrorType, ErrorType> {
		let (com_id, tus_req) = self.get_com_and_pb::<TusSetDataRequest>(data)?;
		let user = tus_req.user.as_ref().ok_or(ErrorType::Malformed)?;
		let npid = &user.npid;

		if npid.is_empty() {
			warn!("Validation: npid is empty");
			return Err(ErrorType::Malformed);
		}

		if tus_req.data.is_empty() {
			warn!("Validation: data is empty");
			return Err(ErrorType::Malformed);
		}

		if tus_req.info.len() > SCE_NP_TUS_DATA_INFO_MAX_SIZE {
			warn!("info len > SCE_NP_TUS_DATA_INFO_MAX_SIZE");
			return Err(ErrorType::Malformed);
		}

		if tus_req.data.len() > UNDOCUMENTED_TUS_DATA_MAX_SIZE {
			warn!("data len > UNDOCUMENTED_TUS_DATA_MAX_SIZE");
			return Err(ErrorType::Malformed);
		}

		let compare_timestamp = if tus_req.is_last_changed_date.len() == 1 {
			Some(tus_req.is_last_changed_date[0])
		} else if tus_req.is_last_changed_date.is_empty() {
			None
		} else {
			return Err(ErrorType::Malformed);
		};

		let author_str = if tus_req.is_last_changed_author_id.is_empty() {
			None
		} else {
			Some(&tus_req.is_last_changed_author_id)
		};

		let slot = tus_req.slot_id;
		let info = if tus_req.info.is_empty() { None } else { Some(&tus_req.info[..]) };

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

		if user.vuser {
			if let Some(ret_value) = Client::preliminary_set_tus_data_check(|| db.tus_get_vuser_data_timestamp_and_author(&com_id, npid, slot), &compare_timestamp, &compare_author_id) {
				return ret_value;
			}

			let data_id = Client::create_tus_data_file(&tus_req.data).await;

			let res = db.tus_set_vuser_data(&com_id, npid, slot, data_id, &info, self.client_info.user_id, new_timestamp, compare_timestamp, compare_author_id);
			match res {
				Ok(()) => Ok(ErrorType::NoError),
				Err(DbError::Empty) => {
					Client::delete_tus_data(data_id).await;
					Ok(ErrorType::CondFail)
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

			let data_id = Client::create_tus_data_file(&tus_req.data).await;

			let res = db.tus_set_user_data(&com_id, user_id, slot, data_id, &info, user_id, new_timestamp, compare_timestamp, compare_author_id);
			match res {
				Ok(()) => Ok(ErrorType::NoError),
				Err(DbError::Empty) => {
					Client::delete_tus_data(data_id).await;
					Ok(ErrorType::CondFail)
				}
				Err(_) => Err(ErrorType::DbFail),
			}
		}
	}

	pub async fn tus_get_data(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let (com_id, tus_req) = self.get_com_and_pb::<TusGetDataRequest>(data)?;
		let user = tus_req.user.as_ref().ok_or(ErrorType::Malformed)?;
		let npid = &user.npid;

		if npid.is_empty() {
			warn!("Validation: npid is empty");
			return Err(ErrorType::Malformed);
		}

		let slot = tus_req.slot_id;

		let db = Database::new(self.get_database_connection()?);

		let res = if user.vuser {
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

		let (status, tus_data) = if let Some((db_tus_data_status, db_tus_data_info)) = info {
			let data = Client::get_tus_data_file(db_tus_data_status.data_id).await.map_err(|_| ErrorType::DbFail)?;
			let author_id_string = db.get_username(db_tus_data_status.author_id).map_err(|_| ErrorType::DbFail)?;

			(
				Some(TusDataStatus {
					owner_id: npid.clone(),
					has_data: true,
					last_changed_date: db_tus_data_status.timestamp,
					last_changed_author_id: author_id_string,
					info: db_tus_data_info,
				}),
				data,
			)
		} else {
			(
				Some(TusDataStatus {
					owner_id: npid.clone(),
					has_data: false,
					last_changed_date: 0,
					last_changed_author_id: String::new(),
					info: Vec::new(),
				}),
				Vec::new(),
			)
		};

		let final_tus_data = TusData { status, data: tus_data };

		let finished_data = final_tus_data.encode_to_vec();
		Client::add_data_packet(reply, &finished_data);
		Ok(ErrorType::NoError)
	}

	pub async fn tus_get_multislot_data_status(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let (com_id, tus_req) = self.get_com_and_pb::<TusGetMultiSlotDataStatusRequest>(data)?;
		let user = tus_req.user.as_ref().ok_or(ErrorType::Malformed)?;
		let npid = &user.npid;

		if npid.is_empty() {
			warn!("Validation: npid is empty");
			return Err(ErrorType::Malformed);
		}

		let db = Database::new(self.get_database_connection()?);

		let vec_status: Vec<_> = if user.vuser {
			tus_req.slot_id_array.iter().map(|slot| db.tus_get_vuser_data(&com_id, npid, *slot).ok()).collect()
		} else {
			let user_id = match db.get_user_id(npid) {
				Err(DbError::Empty) => return Ok(ErrorType::NotFound),
				Err(_) => return Err(ErrorType::DbFail),
				Ok(id) => id,
			};

			tus_req.slot_id_array.iter().map(|slot| db.tus_get_user_data(&com_id, user_id, *slot).ok()).collect()
		};

		let mut vec_list_status = Vec::new();

		for status in &vec_status {
			if let Some((db_tus_data_status, db_tus_data_info)) = status {
				let author_id_string = db.get_username(db_tus_data_status.author_id).map_err(|_| ErrorType::DbFail)?;

				vec_list_status.push(TusDataStatus {
					owner_id: npid.clone(),
					has_data: true,
					last_changed_date: db_tus_data_status.timestamp,
					last_changed_author_id: author_id_string,
					info: db_tus_data_info.clone(),
				});
			} else {
				vec_list_status.push(TusDataStatus {
					owner_id: npid.clone(),
					has_data: false,
					last_changed_date: 0,
					last_changed_author_id: String::new(),
					info: Vec::new(),
				});
			}
		}

		let resp = TusDataStatusResponse { status: vec_list_status };

		let finished_data = resp.encode_to_vec();
		Client::add_data_packet(reply, &finished_data);
		Ok(ErrorType::NoError)
	}

	pub async fn tus_get_multiuser_data_status(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let (com_id, tus_req) = self.get_com_and_pb::<TusGetMultiUserDataStatusRequest>(data)?;
		let slot = tus_req.slot_id;

		if tus_req.users.is_empty() {
			warn!("Validation: users is empty");
			return Err(ErrorType::Malformed);
		}

		if tus_req.users.iter().any(|user| user.npid.is_empty()) {
			warn!("Validation: npid is empty");
			return Err(ErrorType::Malformed);
		}

		let db = Database::new(self.get_database_connection()?);

		let mut vec_status = Vec::with_capacity(tus_req.users.len());
		let mut vec_npids = Vec::with_capacity(tus_req.users.len());
		for user in &tus_req.users {
			let npid = &user.npid;
			vec_npids.push(npid.clone());

			vec_status.push(if user.vuser {
				db.tus_get_vuser_data(&com_id, npid, slot).ok()
			} else {
				let user_id = match db.get_user_id(npid) {
					Err(DbError::Empty) => return Ok(ErrorType::NotFound),
					Err(_) => return Err(ErrorType::DbFail),
					Ok(id) => id,
				};

				db.tus_get_user_data(&com_id, user_id, slot).ok()
			});
		}

		let mut vec_list_status = Vec::new();

		for (status, npid) in vec_status.iter().zip(vec_npids.iter()) {
			if let Some((db_tus_data_status, db_tus_data_info)) = status {
				let author_id_string = db.get_username(db_tus_data_status.author_id).map_err(|_| ErrorType::DbFail)?;

				vec_list_status.push(TusDataStatus {
					owner_id: npid.clone(),
					has_data: true,
					last_changed_date: db_tus_data_status.timestamp,
					last_changed_author_id: author_id_string,
					info: db_tus_data_info.clone(),
				});
			} else {
				vec_list_status.push(TusDataStatus {
					owner_id: npid.clone(),
					has_data: false,
					last_changed_date: 0,
					last_changed_author_id: String::new(),
					info: Vec::new(),
				});
			}
		}

		let resp = TusDataStatusResponse { status: vec_list_status };

		let finished_data = resp.encode_to_vec();
		Client::add_data_packet(reply, &finished_data);
		Ok(ErrorType::NoError)
	}

	pub async fn tus_get_friends_data_status(&mut self, data: &mut StreamExtractor, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		let (com_id, tus_req) = self.get_com_and_pb::<TusGetFriendsDataStatusRequest>(data)?;

		let sorting = FromPrimitive::from_i32(tus_req.sort_type);
		if sorting.is_none() {
			warn!("Unsupported sorting value in TusGetFriendsDataStatusRequest");
			return Err(ErrorType::Malformed);
		}
		let sorting: TusStatusSorting = sorting.unwrap();

		let mut user_list = self.shared.client_infos.read().get(&self.client_info.user_id).unwrap().friend_info.read().friends.clone();
		if tus_req.include_self {
			user_list.insert(self.client_info.user_id, self.client_info.npid.clone());
		}

		let mut final_infos = Vec::with_capacity(std::cmp::min(user_list.len(), tus_req.array_num as usize));

		if !user_list.is_empty() {
			let db = Database::new(self.get_database_connection()?);
			let vec_vars = db
				.tus_get_user_data_from_user_list(&com_id, &user_list.keys().copied().collect::<Vec<i64>>(), tus_req.slot_id, sorting, tus_req.array_num)
				.map_err(|_| ErrorType::DbFail)?;

			for (user_id, var, db_tus_data_info) in &vec_vars {
				let author_id_string = self.get_username_with_helper(var.author_id, &user_list, &db)?;

				final_infos.push(TusDataStatus {
					owner_id: user_list.get(user_id).unwrap().clone(),
					has_data: true,
					last_changed_date: var.timestamp,
					last_changed_author_id: author_id_string,
					info: db_tus_data_info.clone(),
				});
			}
		}

		let tus_var_response = TusDataStatusResponse { status: final_infos };
		let finished_data = tus_var_response.encode_to_vec();

		Client::add_data_packet(reply, &finished_data);

		Ok(ErrorType::NoError)
	}

	pub async fn tus_delete_multislot_data(&mut self, data: &mut StreamExtractor) -> Result<ErrorType, ErrorType> {
		let (com_id, tus_req) = self.get_com_and_pb::<TusDeleteMultiSlotDataRequest>(data)?;
		let user = tus_req.user.as_ref().ok_or(ErrorType::Malformed)?;
		let npid = &user.npid;

		if npid.is_empty() {
			warn!("Validation: npid is empty");
			return Err(ErrorType::Malformed);
		}

		if tus_req.slot_id_array.is_empty() {
			warn!("Validation: slot_id_array is empty");
			return Err(ErrorType::Malformed);
		}

		let db = Database::new(self.get_database_connection()?);

		if user.vuser {
			db.tus_delete_vuser_data_with_slotlist(&com_id, npid, &tus_req.slot_id_array).map_err(|_| ErrorType::DbFail)?
		} else {
			let user_id = match db.get_user_id(npid) {
				Err(DbError::Empty) => return Ok(ErrorType::NotFound),
				Err(_) => return Err(ErrorType::DbFail),
				Ok(id) => id,
			};

			if user_id != self.client_info.user_id {
				return Ok(ErrorType::Unauthorized);
			}

			db.tus_delete_user_data_with_slotlist(&com_id, user_id, &tus_req.slot_id_array).map_err(|_| ErrorType::DbFail)?
		}

		Ok(ErrorType::NoError)
	}
}
