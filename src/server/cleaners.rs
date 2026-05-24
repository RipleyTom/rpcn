use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tokio::sync::mpsc;
use tokio::time::sleep;

use crate::server::Server;
use crate::server::client::{Client, ClientSharedInfo, TerminateWatch};
use crate::server::database::Database;

use tracing::{error, info};

pub struct Cleaner {
	term_watch: TerminateWatch,
	db_pool: r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
	client_infos: Arc<RwLock<HashMap<i64, ClientSharedInfo>>>,
}

#[allow(unused)]
pub enum FileType {
	ScoreFile(u64), // Might not be necessary for score, todo: cleanup
	TusDataFile(u64),
}

pub struct FileCleaner {
	term_watch: TerminateWatch,
	receiver: mpsc::UnboundedReceiver<(u32, FileType)>,
}

impl Server {
	pub async fn start_cleaner_task(&self, term_watch: TerminateWatch, db_pool: r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>, client_infos: Arc<RwLock<HashMap<i64, ClientSharedInfo>>>) {
		let mut cleaner = Cleaner::new(term_watch, db_pool, client_infos);

		tokio::task::spawn(async move {
			cleaner.task_proc().await;
		});
	}

	pub async fn start_filecleaner_task(&self, term_watch: TerminateWatch, receiver: mpsc::UnboundedReceiver<(u32, FileType)>) {
		let mut file_cleaner = FileCleaner::new(term_watch, receiver);

		tokio::task::spawn(async move {
			file_cleaner.task_proc().await;
		});
	}
}

impl FileCleaner {
	fn new(term_watch: TerminateWatch, receiver: mpsc::UnboundedReceiver<(u32, FileType)>) -> FileCleaner {
		FileCleaner { term_watch, receiver }
	}

	async fn delete_file(file: FileType) {
		match file {
			FileType::ScoreFile(id) => Client::delete_score_data(id).await,
			FileType::TusDataFile(id) => Client::delete_tus_data(id).await,
		}
	}

	async fn task_proc(&mut self) {
		if *self.term_watch.recv.borrow_and_update() {
			return;
		}

		'task_loop: loop {
			tokio::select! {
				message = self.receiver.recv() => match message {
					Some((timestamp, file)) => {
						let cur_time = Client::get_timestamp_seconds();
						if timestamp <= cur_time {
							FileCleaner::delete_file(file).await;
							continue 'task_loop;
						}

						let duration = Duration::from_secs((timestamp - cur_time) as u64);

						tokio::select! {
							_ = sleep(duration) => {
								FileCleaner::delete_file(file).await;
								continue 'task_loop;
							}
							_ = self.term_watch.recv.changed() => {
								break 'task_loop;
							}
						}
					},
					None => break 'task_loop,
				},
				_ = self.term_watch.recv.changed() => {
							break 'task_loop;
					}
			}
		}
	}
}

impl Cleaner {
	fn new(term_watch: TerminateWatch, db_pool: r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>, client_infos: Arc<RwLock<HashMap<i64, ClientSharedInfo>>>) -> Cleaner {
		Cleaner { term_watch, db_pool, client_infos }
	}

	async fn task_proc(&mut self) {
		if *self.term_watch.recv.borrow_and_update() {
			return;
		}

		'task_loop: loop {
			tokio::select! {
				_ = sleep(Duration::from_secs(24 * 60 * 60)) => {
					// Perform cleanup
					match self.db_pool.get() {
						Ok(conn) => {
							let db = Database::new(conn);

							match db.cleanup_expired_hashes() {
								Ok((num_username, num_email)) => info!("Cleaner: Cleaned up {} username_hashes and {} email_hashes", num_username, num_email),
								Err(e) => error!("Cleaner: Failed to cleanup expired hashes: {}", e),
							}

							{
								// We lock client_infos to avoid user logging in while we clean
								let _lock = self.client_infos.write();

								match db.cleanup_never_used_accounts() {
									Ok(n) => info!("Cleaner: Cleaned up {} never used accounts", n),
									Err(e) => error!("Cleaner: Failed to clean never used accounts; {}", e),
								}
							}
						}
						Err(e) => {
							error!("Cleaner: Failed to get database connection: {}", e);
						}
					}
				}
				_ = self.term_watch.recv.changed() => {
							break 'task_loop;
					}
			}
		}
	}
}
