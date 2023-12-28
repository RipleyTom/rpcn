use crate::server::Server;
use std::collections::HashSet;
use std::io;

impl Server {
	pub fn get_ids_from_directory(directory: &str, file_extension: &str) -> Result<HashSet<u64>, String> {
		let mut list_ids = HashSet::new();

		for file in std::fs::read_dir(directory).map_err(|e| format!("Failed to list score_data directory: {}", e))? {
			if let Err(e) = file {
				println!("Error reading file inside {}: {}", directory, e);
				continue;
			}
			let file = file.unwrap();

			let filename = file.file_name().into_string();
			if filename.is_err() {
				println!("A file inside {} contains invalid unicode", directory);
				continue;
			}
			let filename = filename.unwrap();

			let split_filename = filename.split_once('.');
			if split_filename.is_none() {
				println!("A file inside {} doesn't contain a dot", directory);
				continue;
			}
			let (file_prefix, file_suffix) = split_filename.unwrap();

			if file_suffix != file_extension {
				println!("A file in {} is not a .{}", directory, file_extension);
				continue;
			}

			let r = file_prefix.parse::<u64>();
			if r.is_err() {
				println!("A file inside {} doesn't have an integer filename: {}", directory, filename);
				continue;
			}
			list_ids.insert(r.unwrap());
		}

		Ok(list_ids)
	}

	pub fn create_data_directory(directory: &str, file_extension: &str) -> Result<u64, String> {
		match std::fs::create_dir(directory) {
			Ok(_) => {}
			Err(e) => match e.kind() {
				io::ErrorKind::AlreadyExists => {}
				other_error => return Err(format!("Failed to create directory(\"{}\"): {}", directory, other_error)),
			},
		}

		Ok(Server::get_ids_from_directory(directory, file_extension)?.iter().max().unwrap_or(&0) + 1)
	}
}
