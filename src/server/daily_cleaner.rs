use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tokio::time::sleep;

use crate::server::Server;
use crate::server::client::{ClientSharedInfo, TerminateWatch};
use crate::server::database::Database;

use tracing::{error, info};

pub struct Cleaner {
	term_watch: TerminateWatch,
	db_pool: r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
	client_infos: Arc<RwLock<HashMap<i64, ClientSharedInfo>>>,
}

impl Server {
	pub async fn start_cleaner_task(&self, term_watch: TerminateWatch, db_pool: r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>, client_infos: Arc<RwLock<HashMap<i64, ClientSharedInfo>>>) {
		let mut cleaner = Cleaner::new(term_watch, db_pool, client_infos);

		tokio::task::spawn(async move {
			cleaner.task_proc().await;
		});
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
