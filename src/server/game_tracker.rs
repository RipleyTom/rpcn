use std::collections::HashMap;
use std::convert::Infallible;
use std::fmt::Write;
use std::io;
use std::net::ToSocketAddrs;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response};
use hyper_util::rt::TokioIo;
use parking_lot::RwLock;
use tokio::net::TcpListener;
use tracing::{info, warn};

use crate::server::client::{com_id_to_string, ComId, TerminateWatch};
use crate::server::Server;

pub struct GameTracker {
	num_users: Arc<AtomicU64>,
	psn_games: RwLock<HashMap<ComId, Arc<AtomicU64>>>,
	ticket_games: RwLock<HashMap<String, Arc<AtomicU64>>>,
}

impl Server {
	pub async fn start_stat_server(&self, term_watch: TerminateWatch, game_tracker: Arc<GameTracker>) -> io::Result<()> {
		let bind_addr = self.config.read().get_stat_server_binds().clone();

		if let Some((host, port)) = &bind_addr {
			let str_addr = host.to_owned() + ":" + port;
			let mut addr = str_addr
				.to_socket_addrs()
				.map_err(|e| io::Error::new(e.kind(), format!("Stat: {} is not a valid address", &str_addr)))?;
			let addr = addr
				.next()
				.ok_or_else(|| io::Error::new(io::ErrorKind::AddrNotAvailable, format!("Stat: {} is not a valid address", &str_addr)))?;

			let listener = TcpListener::bind(addr)
				.await
				.map_err(|e| io::Error::new(e.kind(), format!("Stat: error binding to <{}>: {}", &addr, e)))?;

			info!("Stat server now waiting for connections on {}", str_addr);

			tokio::task::spawn(async move {
				GameTracker::server_proc(listener, term_watch, game_tracker).await;
			});
		}

		Ok(())
	}
}

impl GameTracker {
	async fn server_proc(listener: TcpListener, mut term_watch: TerminateWatch, game_tracker: Arc<GameTracker>) {
		'stat_server_loop: loop {
			tokio::select! {
				accept_res = listener.accept() => {
					if let Err(e) = accept_res {
						warn!("Stat: Error accepting a client: {}", e);
						continue 'stat_server_loop;
					}

					let (stream, peer_addr) = accept_res.unwrap();
					let io = TokioIo::new(stream);

					info!("Stat: new client from {}", peer_addr);
					{
						let game_tracker = game_tracker.clone();
						tokio::task::spawn(async move {
							if let Err(err) = http1::Builder::new().keep_alive(false).serve_connection(io, service_fn(|r| GameTracker::handle_stat_server_req(r, game_tracker.clone()))).await {
								warn!("Stat: Error serving connection: {}", err);
							}
						});
					}
				}
				_ = term_watch.recv.changed() => {
					break 'stat_server_loop;
				}
			}
		}
		info!("GameTracker::server_proc terminating");
	}

	async fn handle_stat_server_req(req: Request<hyper::body::Incoming>, game_tracker: Arc<GameTracker>) -> Result<Response<String>, Infallible> {
		if req.method() != Method::GET || req.uri() != "/rpcn_stats" {
			return Ok(Response::new("".to_owned()));
		}

		Ok(Response::builder().header("Content-Type", "application/json").body(game_tracker.to_json()).unwrap())
	}

	fn to_json(&self) -> String {
		let psn_games: Vec<(String, u64)> = self
			.psn_games
			.read()
			.iter()
			.filter_map(|(name, num_users)| {
				let num_users = num_users.load(Ordering::SeqCst);
				if num_users != 0 {
					Some((com_id_to_string(name), num_users))
				} else {
					None
				}
			})
			.collect();

		let ticket_games: Vec<(String, u64)> = self
			.ticket_games
			.read()
			.iter()
			.filter_map(|(name, num_users)| {
				let num_users = num_users.load(Ordering::SeqCst);
				if num_users != 0 {
					Some((name.clone(), num_users))
				} else {
					None
				}
			})
			.collect();

		let mut res = String::from("{\n");
		let _ = write!(res, "    \"num_users\" : {}", self.num_users.load(Ordering::SeqCst));

		let add_games = |string: &mut String, section_name: &str, v: &Vec<(String, u64)>| {
			if !v.is_empty() {
				let _ = write!(string, ",\n    \"{}\": {{\n", section_name);

				for (index, (name, num)) in v.iter().enumerate() {
					let _ = write!(string, "        \"{}\": {}", name, num);
					*string += if index != (v.len() - 1) { ",\n" } else { "\n" };
				}

				*string += "    }"
			}
		};

		add_games(&mut res, "psn_games", &psn_games);
		add_games(&mut res, "ticket_games", &ticket_games);

		res += "\n}";

		res
	}

	pub fn new() -> GameTracker {
		GameTracker {
			num_users: Arc::new(AtomicU64::new(0)),
			psn_games: RwLock::new(HashMap::new()),
			ticket_games: RwLock::new(HashMap::new()),
		}
	}

	pub fn increase_num_users(&self) {
		self.num_users.fetch_add(1, Ordering::SeqCst);
	}

	pub fn decrease_num_users(&self) {
		self.num_users.fetch_sub(1, Ordering::SeqCst);
	}

	fn get_value_psn(&self, com_id: &ComId) -> Arc<AtomicU64> {
		{
			let psn_games = self.psn_games.read();
			if psn_games.contains_key(com_id) {
				return psn_games[com_id].clone();
			}
		}

		self.psn_games.write().entry(*com_id).or_insert_with(|| Arc::new(AtomicU64::new(0))).clone()
	}

	pub fn increase_count_psn(&self, com_id: &ComId) {
		let count = self.get_value_psn(com_id);
		count.fetch_add(1, Ordering::SeqCst);
	}

	pub fn decrease_count_psn(&self, com_id: &ComId) {
		let count = self.get_value_psn(com_id);
		count.fetch_sub(1, Ordering::SeqCst);
	}

	fn get_value_ticket(&self, service_id: &str) -> Arc<AtomicU64> {
		{
			let ticket_games = self.ticket_games.read();
			if ticket_games.contains_key(service_id) {
				return ticket_games[service_id].clone();
			}
		}

		self.ticket_games.write().entry(service_id.to_owned()).or_insert_with(|| Arc::new(AtomicU64::new(0))).clone()
	}

	pub fn increase_count_ticket(&self, service_id: &str) {
		let count = self.get_value_ticket(service_id);
		count.fetch_add(1, Ordering::SeqCst);
	}

	pub fn decrease_count_ticket(&self, service_id: &str) {
		let count = self.get_value_ticket(service_id);
		count.fetch_sub(1, Ordering::SeqCst);
	}
}
