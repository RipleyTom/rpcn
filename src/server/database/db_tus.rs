use crate::server::database::*;

use crate::server::client::cmd_tus::{TusOpeType, TusSorting, TusStatusSorting};

pub struct DbTusVarInfo {
	pub timestamp: u64,
	pub author_id: i64,
	pub variable: i64,
}

pub struct DbTusDataStatus {
	pub timestamp: u64,
	pub author_id: i64,
	pub data_id: u64,
}

impl Database {
	pub fn tus_get_all_data_ids(&self) -> Result<HashSet<u64>, DbError> {
		let mut stmt = self.conn.prepare("SELECT data_id FROM tus_data").map_err(|_| DbError::Internal)?;
		let rows = stmt.query_map([], |r| Ok(r.get_unwrap(0))).map_err(|_| DbError::Internal)?;

		let mut hs_data_ids = HashSet::new();
		for row in rows {
			let to_insert = row.unwrap();
			if hs_data_ids.insert(to_insert) == false {
				println!("Duplicate tus data_id found: {}!", to_insert);
				return Err(DbError::Internal);
			}
		}

		let mut stmt = self.conn.prepare("SELECT data_id FROM tus_data_vuser").map_err(|_| DbError::Internal)?;
		let rows = stmt.query_map([], |r| Ok(r.get_unwrap(0))).map_err(|_| DbError::Internal)?;

		for row in rows {
			let to_insert = row.unwrap();
			if hs_data_ids.insert(to_insert) == false {
				println!("Duplicate tus data_id found: {}!", to_insert);
				return Err(DbError::Internal);
			}
		}

		Ok(hs_data_ids)
	}

	pub fn tus_set_vuser_variable(&self, com_id: &ComId, vuser: &str, slot: i32, value: i64, author_id: i64, timestamp: u64) -> Result<(), DbError> {
		let res = self.conn.execute(
			"INSERT INTO tus_var_vuser( vuser, communication_id, slot_id, var_value, timestamp, author_id ) VALUES ( ?1, ?2, ?3, ?4, ?5, ?6 ) ON CONFLICT( vuser, communication_id, slot_id ) DO UPDATE SET var_value = excluded.var_value, timestamp = excluded.timestamp, author_id = excluded.author_id",
			rusqlite::params![vuser, com_id, slot, value, timestamp, author_id],
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
				error!("Unexpected error setting vuser variable: {}", e);
				Err(DbError::Internal)
			}
		}
	}

	pub fn tus_set_user_variable(&self, com_id: &ComId, user: i64, slot: i32, value: i64, author_id: i64, timestamp: u64) -> Result<(), DbError> {
		let res = self.conn.execute(
			"INSERT INTO tus_var( owner_id, communication_id, slot_id, var_value, timestamp, author_id ) VALUES ( ?1, ?2, ?3, ?4, ?5, ?6 ) ON CONFLICT( owner_id, communication_id, slot_id ) DO UPDATE SET var_value = excluded.var_value, timestamp = excluded.timestamp, author_id = excluded.author_id",
			rusqlite::params![user, com_id, slot, value, timestamp, author_id],
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
				error!("Unexpected error setting vuser variable: {}", e);
				Err(DbError::Internal)
			}
		}
	}

	pub fn tus_get_vuser_variable(&self, com_id: &ComId, vuser: &str, slot: i32) -> Result<DbTusVarInfo, DbError> {
		let res = self.conn.query_row(
			"SELECT timestamp, author_id, var_value FROM tus_var_vuser WHERE communication_id = ?1 AND vuser = ?2 AND slot_id = ?3",
			rusqlite::params![com_id, vuser, slot],
			|r| {
				Ok(DbTusVarInfo {
					timestamp: r.get_unwrap(0),
					author_id: r.get_unwrap(1),
					variable: r.get_unwrap(2),
				})
			},
		);

		match res {
			Ok(tus_var_info) => Ok(tus_var_info),
			Err(rusqlite::Error::QueryReturnedNoRows) => Err(DbError::Empty),
			Err(e) => {
				error!("Unexpected error querying for tus vuser var: {}", e);
				Err(DbError::Internal)
			}
		}
	}

	pub fn tus_get_user_variable(&self, com_id: &ComId, user: i64, slot: i32) -> Result<DbTusVarInfo, DbError> {
		let res = self.conn.query_row(
			"SELECT timestamp, author_id, var_value FROM tus_var WHERE communication_id = ?1 AND owner_id = ?2 AND slot_id = ?3",
			rusqlite::params![com_id, user, slot],
			|r| {
				Ok(DbTusVarInfo {
					timestamp: r.get_unwrap(0),
					author_id: r.get_unwrap(1),
					variable: r.get_unwrap(2),
				})
			},
		);

		match res {
			Ok(tus_var_info) => Ok(tus_var_info),
			Err(rusqlite::Error::QueryReturnedNoRows) => Err(DbError::Empty),
			Err(e) => {
				error!("Unexpected error querying for tus var: {}", e);
				Err(DbError::Internal)
			}
		}
	}

	pub fn tus_get_user_variable_from_user_list(&self, com_id: &ComId, user_list: &[i64], slot: i32, sorting: cmd_tus::TusSorting, max: u32) -> Result<Vec<(i64, DbTusVarInfo)>, DbError> {
		let stmt_sorting = match sorting {
			TusSorting::SCE_NP_TUS_VARIABLE_SORTTYPE_DESCENDING_DATE => "timestamp DESC",
			TusSorting::SCE_NP_TUS_VARIABLE_SORTTYPE_ASCENDING_DATE => "timestamp ASC",
			TusSorting::SCE_NP_TUS_VARIABLE_SORTTYPE_DESCENDING_VALUE => "var_value DESC",
			TusSorting::SCE_NP_TUS_VARIABLE_SORTTYPE_ASCENDING_VALUE => "var_value ASC",
		};

		let stmt_string = format!(
			"SELECT owner_id, timestamp, author_id, var_value FROM tus_var WHERE communication_id = ? AND slot_id = ? AND owner_id IN ({}) ORDER BY {} LIMIT {}",
			generate_string_from_user_list(user_list),
			stmt_sorting,
			max
		);

		let mut stmt = self.conn.prepare(&stmt_string).map_err(|_| DbError::Internal)?;

		let rows = stmt
			.query_map(rusqlite::params! { com_id, slot }, |r| {
				Ok((
					r.get_unwrap(0),
					DbTusVarInfo {
						timestamp: r.get_unwrap(1),
						author_id: r.get_unwrap(2),
						variable: r.get_unwrap(3),
					},
				))
			})
			.map_err(|_| DbError::Internal)?;

		let mut vec_var_infos = Vec::new();
		for row in rows {
			vec_var_infos.push(row.unwrap());
		}

		Ok(vec_var_infos)
	}

	pub fn tus_add_and_get_user_variable(
		&self,
		com_id: &ComId,
		user: i64,
		slot: i32,
		value: i64,
		author_id: i64,
		timestamp: u64,
		compare_timestamp: Option<u64>,
		compare_author: Option<i64>,
	) -> Result<DbTusVarInfo, DbError> {
		let mut cond_string = String::new();

		if let Some(compare_timestamp) = compare_timestamp {
			let _ = write!(cond_string, "WHERE timestamp <= {}", compare_timestamp);
		}

		if let Some(compare_author) = compare_author {
			let _ = write!(cond_string, "{} author == {}", if cond_string.is_empty() { "WHERE" } else { "AND" }, compare_author);
		}

		let full_query = format!("INSERT INTO tus_var( owner_id, communication_id, slot_id, var_value, timestamp, author_id ) VALUES ( ?1, ?2, ?3, ?4, ?5, ?6 ) ON CONFLICT( owner_id, communication_id, slot_id ) DO UPDATE SET var_value = var_value + excluded.var_value, timestamp = excluded.timestamp, author_id = excluded.author_id {} RETURNING timestamp, author_id, var_value", cond_string);

		self.conn
			.query_row(&full_query, rusqlite::params![user, com_id, slot, value, timestamp, author_id], |r| {
				Ok(DbTusVarInfo {
					timestamp: r.get_unwrap(0),
					author_id: r.get_unwrap(1),
					variable: r.get_unwrap(2),
				})
			})
			.map_err(|e| {
				error!("Unexpected error in tus_add_and_get_user_variable: {}", e);
				DbError::Internal
			})
	}

	pub fn tus_add_and_get_vuser_variable(
		&self,
		com_id: &ComId,
		vuser: &str,
		slot: i32,
		value: i64,
		author_id: i64,
		timestamp: u64,
		compare_timestamp: Option<u64>,
		compare_author: Option<i64>,
	) -> Result<DbTusVarInfo, DbError> {
		let mut cond_string = String::new();

		if let Some(compare_timestamp) = compare_timestamp {
			let _ = write!(cond_string, "WHERE timestamp <= {}", compare_timestamp);
		}

		if let Some(compare_author) = compare_author {
			let _ = write!(cond_string, "{} author == {}", if cond_string.is_empty() { "WHERE" } else { "AND" }, compare_author);
		}

		let full_query = format!("INSERT INTO tus_var_vuser( vuser, communication_id, slot_id, var_value, timestamp, author_id ) VALUES ( ?1, ?2, ?3, ?4, ?5, ?6 ) ON CONFLICT( vuser, communication_id, slot_id ) DO UPDATE SET var_value = var_value + excluded.var_value, timestamp = excluded.timestamp, author_id = excluded.author_id {} RETURNING timestamp, author_id, var_value", cond_string);

		self.conn
			.query_row(&full_query, rusqlite::params![vuser, com_id, slot, value, timestamp, author_id], |r| {
				Ok(DbTusVarInfo {
					timestamp: r.get_unwrap(0),
					author_id: r.get_unwrap(1),
					variable: r.get_unwrap(2),
				})
			})
			.map_err(|e| {
				error!("Unexpected error in tus_add_and_get_vuser_variable: {}", e);
				DbError::Internal
			})
	}

	fn prepare_try_and_set_cond(compare_op: TusOpeType, compare_timestamp: Option<u64>, compare_author: Option<i64>) -> String {
		let op = match compare_op {
			TusOpeType::SCE_NP_TUS_OPETYPE_EQUAL => "==",
			TusOpeType::SCE_NP_TUS_OPETYPE_NOT_EQUAL => "!=",
			TusOpeType::SCE_NP_TUS_OPETYPE_GREATER_THAN => ">",
			TusOpeType::SCE_NP_TUS_OPETYPE_GREATER_OR_EQUAL => ">=",
			TusOpeType::SCE_NP_TUS_OPETYPE_LESS_THAN => "<",
			TusOpeType::SCE_NP_TUS_OPETYPE_LESS_OR_EQUAL => "<=",
		};

		let mut conditional = format!("var_value {} ?7", op);

		if let Some(compare_timestamp) = compare_timestamp {
			write!(conditional, " AND timestamp <= {}", compare_timestamp).unwrap();
		}

		if let Some(compare_author) = compare_author {
			write!(conditional, " AND author_id == {}", compare_author).unwrap();
		}

		conditional
	}

	pub fn tus_try_and_set_user_variable(
		&self,
		com_id: &ComId,
		user: i64,
		slot: i32,
		value_to_set: i64,
		author_id: i64,
		timestamp: u64,
		compare_value: i64,
		compare_op: TusOpeType,
		compare_timestamp: Option<u64>,
		compare_author: Option<i64>,
	) -> Result<DbTusVarInfo, DbError> {
		let conditional = Database::prepare_try_and_set_cond(compare_op, compare_timestamp, compare_author);

		let full_query = format!(
			"INSERT INTO tus_var( owner_id, communication_id, slot_id, var_value, timestamp, author_id ) VALUES ( ?1, ?2, ?3, ?4, ?5, ?6 ) \
			 ON CONFLICT ( owner_id, communication_id, slot_id ) DO \
			 UPDATE SET var_value = excluded.var_value, timestamp = excluded.timestamp, author_id = excluded.author_id \
			 WHERE {} RETURNING timestamp, author_id, var_value",
			conditional
		);

		self.conn
			.query_row(&full_query, rusqlite::params![user, com_id, slot, value_to_set, timestamp, author_id, compare_value], |r| {
				Ok(DbTusVarInfo {
					timestamp: r.get_unwrap(0),
					author_id: r.get_unwrap(1),
					variable: r.get_unwrap(2),
				})
			})
			.map_err(|e| {
				error!("Unexpected error in tus_try_and_set_user_variable: {}", e);
				DbError::Internal
			})
	}

	pub fn tus_try_and_set_vuser_variable(
		&self,
		com_id: &ComId,
		vuser: &str,
		slot: i32,
		value_to_set: i64,
		author_id: i64,
		timestamp: u64,
		compare_value: i64,
		compare_op: TusOpeType,
		compare_timestamp: Option<u64>,
		compare_author: Option<i64>,
	) -> Result<DbTusVarInfo, DbError> {
		let conditional = Database::prepare_try_and_set_cond(compare_op, compare_timestamp, compare_author);

		let full_query = format!(
			"INSERT INTO tus_var_vuser( vuser, communication_id, slot_id, var_value, timestamp, author_id ) VALUES ( ?1, ?2, ?3, ?4, ?5, ?6 ) \
			 ON CONFLICT ( vuser, communication_id, slot_id ) DO \
			 UPDATE SET var_value = excluded.var_value, timestamp = excluded.timestamp, author_id = excluded.author_id \
			 WHERE {} RETURNING timestamp, author_id, var_value",
			conditional
		);

		self.conn
			.query_row(&full_query, rusqlite::params![vuser, com_id, slot, value_to_set, timestamp, author_id, compare_value], |r| {
				Ok(DbTusVarInfo {
					timestamp: r.get_unwrap(0),
					author_id: r.get_unwrap(1),
					variable: r.get_unwrap(2),
				})
			})
			.map_err(|e| {
				error!("Unexpected error in tus_try_and_set_vuser_variable: {}", e);
				DbError::Internal
			})
	}

	pub fn tus_delete_user_variable_with_slotlist(&self, com_id: &ComId, user: i64, slot_list: &[i32]) -> Result<(), DbError> {
		let stmt = format!("DELETE FROM tus_var WHERE com_id = ?1 AND owner_id = ?2 AND slot IN ({})", generate_string_from_slot_list(slot_list));

		self.conn.execute(&stmt, rusqlite::params![com_id, user]).map_err(|e| {
			error!("Unexpected error deleting variable in tus_delete_user_variable_with_slotlist: {}", e);
			DbError::Internal
		})?;

		Ok(())
	}

	pub fn tus_delete_vuser_variable_with_slotlist(&self, com_id: &ComId, vuser: &str, slot_list: &[i32]) -> Result<(), DbError> {
		let stmt = format!("DELETE FROM tus_var_vuser WHERE com_id = ?1 AND vuser = ?2 AND slot IN ({})", generate_string_from_slot_list(slot_list));

		self.conn.execute(&stmt, rusqlite::params![com_id, vuser]).map_err(|e| {
			error!("Unexpected error deleting variable in tus_delete_vuser_variable_with_slotlist: {}", e);
			DbError::Internal
		})?;

		Ok(())
	}

	pub fn tus_get_user_data_timestamp_and_author(&self, com_id: &ComId, user: i64, slot: i32) -> Result<Option<(u64, i64)>, DbError> {
		let res = self.conn.query_row(
			"SELECT timestamp, author_id WHERE com_id = ?1 AND owner_id = ?2 AND slot = ?3",
			rusqlite::params![com_id, user, slot],
			|r| Ok((r.get_unwrap(0), r.get_unwrap(1))),
		);

		match res {
			Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
			Err(_) => Err(DbError::Internal),
			Ok(res) => Ok(Some(res)),
		}
	}

	pub fn tus_get_vuser_data_timestamp_and_author(&self, com_id: &ComId, vuser: &str, slot: i32) -> Result<Option<(u64, i64)>, DbError> {
		let res = self.conn.query_row(
			"SELECT timestamp, author_id WHERE com_id = ?1 AND vuser = ?2 AND slot = ?3",
			rusqlite::params![com_id, vuser, slot],
			|r| Ok((r.get_unwrap(0), r.get_unwrap(1))),
		);

		match res {
			Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
			Err(_) => Err(DbError::Internal),
			Ok(res) => Ok(Some(res)),
		}
	}

	pub fn tus_set_user_data(
		&self,
		com_id: &ComId,
		user: i64,
		slot: i32,
		data_id: u64,
		info: &Option<&[u8]>,
		author_id: i64,
		timestamp: u64,
		compare_timestamp: Option<u64>,
		compare_author: Option<i64>,
	) -> Result<DbTusDataStatus, DbError> {
		let mut cond_string = String::new();

		if let Some(compare_timestamp) = compare_timestamp {
			let _ = write!(cond_string, "WHERE timestamp <= {}", compare_timestamp);
		}

		if let Some(compare_author) = compare_author {
			let _ = write!(cond_string, "{} author == {}", if cond_string.is_empty() { "WHERE" } else { "AND" }, compare_author);
		}

		let full_query = format!(
			"INSERT INTO tus_data( owner_id, communication_id, slot_id, data_id, data_info, timestamp, author_id ) VALUES ( ?1, ?2, ?3, ?4, ?5, ?6, ?7 ) \
			 ON CONFLICT ( owner_id, communication_id, slot_id ) DO \
			 UPDATE SET data_id = excluded.data_id, data_info = excluded.data_info, timestamp = excluded.timestamp, author_id = excluded.author_id \
			 {} RETURNING timestamp, author_id, data_id",
			cond_string
		);

		self.conn
			.query_row(&full_query, rusqlite::params![user, com_id, slot, data_id, info.unwrap_or_default(), timestamp, author_id], |r| {
				Ok(DbTusDataStatus {
					timestamp: r.get_unwrap(0),
					author_id: r.get_unwrap(1),
					data_id: r.get_unwrap(2),
				})
			})
			.map_err(|e| {
				error!("Unexpected error in tus_set_user_data: {}", e);
				DbError::Internal
			})
	}

	pub fn tus_set_vuser_data(
		&self,
		com_id: &ComId,
		vuser: &str,
		slot: i32,
		data_id: u64,
		info: &Option<&[u8]>,
		author_id: i64,
		timestamp: u64,
		compare_timestamp: Option<u64>,
		compare_author: Option<i64>,
	) -> Result<DbTusDataStatus, DbError> {
		let mut cond_string = String::new();

		if let Some(compare_timestamp) = compare_timestamp {
			let _ = write!(cond_string, "WHERE timestamp <= {}", compare_timestamp);
		}

		if let Some(compare_author) = compare_author {
			let _ = write!(cond_string, "{} author == {}", if cond_string.is_empty() { "WHERE" } else { "AND" }, compare_author);
		}

		let full_query = format!(
			"INSERT INTO tus_data_vuser( vuser, communication_id, slot_id, data_id, data_info, timestamp, author_id ) VALUES ( ?1, ?2, ?3, ?4, ?5, ?6, ?7 ) \
			 ON CONFLICT ( vuser, communication_id, slot_id ) DO \
			 UPDATE SET data_id = excluded.data_id, data_info = excluded.data_info, timestamp = excluded.timestamp, author_id = excluded.author_id \
			 {} RETURNING timestamp, author_id, data_id",
			cond_string
		);

		self.conn
			.query_row(&full_query, rusqlite::params![vuser, com_id, slot, data_id, info.unwrap_or_default(), timestamp, author_id], |r| {
				Ok(DbTusDataStatus {
					timestamp: r.get_unwrap(0),
					author_id: r.get_unwrap(1),
					data_id: r.get_unwrap(2),
				})
			})
			.map_err(|e| {
				error!("Unexpected error in tus_set_vuser_data: {}", e);
				DbError::Internal
			})
	}

	pub fn tus_get_user_data(&self, com_id: &ComId, user: i64, slot: i32) -> Result<(DbTusDataStatus, Vec<u8>), DbError> {
		self.conn
			.query_row(
				"SELECT timestamp, author_id, data_id, data_info FROM tus_data WHERE communication_id = ?1 AND owner_id = ?2 AND slot_id = ?3",
				rusqlite::params![com_id, user, slot],
				|r| {
					Ok((
						DbTusDataStatus {
							timestamp: r.get_unwrap(0),
							author_id: r.get_unwrap(1),
							data_id: r.get_unwrap(2),
						},
						r.get_unwrap(3),
					))
				},
			)
			.map_err(|e| match e {
				rusqlite::Error::QueryReturnedNoRows => DbError::Empty,
				e => {
					error!("Unexpected error in tus_get_user_data: {}", e);
					DbError::Internal
				}
			})
	}

	pub fn tus_get_vuser_data(&self, com_id: &ComId, vuser: &str, slot: i32) -> Result<(DbTusDataStatus, Vec<u8>), DbError> {
		self.conn
			.query_row(
				"SELECT timestamp, author_id, data_id, data_info FROM tus_data_vuser WHERE communication_id = ?1 AND vuser = ?2 AND slot_id = ?3",
				rusqlite::params![com_id, vuser, slot],
				|r| {
					Ok((
						DbTusDataStatus {
							timestamp: r.get_unwrap(0),
							author_id: r.get_unwrap(1),
							data_id: r.get_unwrap(2),
						},
						r.get_unwrap(3),
					))
				},
			)
			.map_err(|e| match e {
				rusqlite::Error::QueryReturnedNoRows => DbError::Empty,
				e => {
					error!("Unexpected error in tus_get_vuser_data: {}", e);
					DbError::Internal
				}
			})
	}

	pub fn tus_get_user_data_from_user_list(
		&self,
		com_id: &ComId,
		user_list: &[i64],
		slot: i32,
		sorting: cmd_tus::TusStatusSorting,
		max: u32,
	) -> Result<Vec<(i64, DbTusDataStatus, Vec<u8>)>, DbError> {
		let stmt_sorting = match sorting {
			TusStatusSorting::SCE_NP_TUS_DATASTATUS_SORTTYPE_DESCENDING_DATE => "timestamp DESC",
			TusStatusSorting::SCE_NP_TUS_DATASTATUS_SORTTYPE_ASCENDING_DATE => "timestamp ASC",
		};

		let stmt_string = format!(
			"SELECT owner_id, timestamp, author_id, data_id, data_info FROM tus_data WHERE communication_id = ? AND slot_id = ? AND owner_id IN ({}) ORDER BY {} LIMIT {}",
			generate_string_from_user_list(user_list),
			stmt_sorting,
			max
		);

		let mut stmt = self.conn.prepare(&stmt_string).map_err(|_| DbError::Internal)?;

		let rows = stmt
			.query_map(rusqlite::params! { com_id, slot }, |r| {
				Ok((
					r.get_unwrap(0),
					DbTusDataStatus {
						timestamp: r.get_unwrap(1),
						author_id: r.get_unwrap(2),
						data_id: r.get_unwrap(3),
					},
					r.get_unwrap(4),
				))
			})
			.map_err(|_| DbError::Internal)?;

		let mut vec_var_status = Vec::new();
		for row in rows {
			vec_var_status.push(row.unwrap());
		}

		Ok(vec_var_status)
	}

	pub fn tus_delete_user_data_with_slotlist(&self, com_id: &ComId, user: i64, slot_list: &[i32]) -> Result<(), DbError> {
		let stmt = format!("DELETE FROM tus_data WHERE com_id = ?1 AND owner_id = ?2 AND slot IN ({})", generate_string_from_slot_list(slot_list));

		self.conn.execute(&stmt, rusqlite::params![com_id, user]).map_err(|e| {
			error!("Unexpected error deleting variable in tus_delete_user_data_with_slotlist: {}", e);
			DbError::Internal
		})?;

		Ok(())
	}

	pub fn tus_delete_vuser_data_with_slotlist(&self, com_id: &ComId, vuser: &str, slot_list: &[i32]) -> Result<(), DbError> {
		let stmt = format!(
			"DELETE FROM tus_data_vuser WHERE com_id = ?1 AND vuser = ?2 AND slot IN ({})",
			generate_string_from_slot_list(slot_list)
		);

		self.conn.execute(&stmt, rusqlite::params![com_id, vuser]).map_err(|e| {
			error!("Unexpected error deleting variable in tus_delete_vuser_data_with_slotlist: {}", e);
			DbError::Internal
		})?;

		Ok(())
	}
}
