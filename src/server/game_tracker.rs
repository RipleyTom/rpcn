use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::fmt::Write;
use std::io;
use std::net::ToSocketAddrs;
use std::sync::atomic::{AtomicI64, AtomicU32, Ordering};
use std::sync::Arc;

use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response};
use hyper_util::rt::TokioIo;
use parking_lot::{Mutex, RwLock};
use tokio::net::TcpListener;
use tracing::{error, info, warn};

use crate::server::client::{com_id_to_string, ComId, TerminateWatch};
use crate::server::Server;
use crate::Client;

struct GameInfo {
	num_users: AtomicI64,
	name_hints: RwLock<HashSet<String>>,
}

struct CachedResponse {
	timestamp: AtomicU32,
	cached_response: Mutex<Response<String>>,
}

pub struct GameTracker {
	num_users: AtomicI64,
	psn_games: RwLock<HashMap<ComId, GameInfo>>,
	ticket_games: RwLock<HashMap<String, AtomicI64>>,
	cached_response: CachedResponse,
}

impl Server {
	pub async fn start_stat_server(&self, term_watch: TerminateWatch, game_tracker: Arc<GameTracker>) -> io::Result<()> {
		let (bind_addr, timeout);
		{
			let config = self.config.read();
			bind_addr = config.get_stat_server_binds().clone();
			timeout = config.get_stat_server_timeout();
		}

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
				GameTracker::server_proc(listener, term_watch, game_tracker, timeout).await;
			});
		}

		Ok(())
	}
}

impl GameTracker {
	async fn server_proc(listener: TcpListener, mut term_watch: TerminateWatch, game_tracker: Arc<GameTracker>, timeout: u32) {
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
							if let Err(err) = http1::Builder::new().keep_alive(false).serve_connection(io, service_fn(|r| GameTracker::handle_stat_server_req(r, game_tracker.clone(), timeout))).await {
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

	async fn handle_stat_server_req(req: Request<hyper::body::Incoming>, game_tracker: Arc<GameTracker>, timeout: u32) -> Result<Response<String>, Infallible> {
		if req.method() != Method::GET || req.uri() != "/rpcn_stats" {
			return Ok(Response::new("".to_owned()));
		}

		if timeout == 0 {
			return Ok(Response::builder().header("Content-Type", "application/json").body(game_tracker.to_json()).unwrap());
		}

		let new_timestamp = Client::get_timestamp_seconds();
		let mut response = game_tracker.cached_response.cached_response.lock();

		if new_timestamp > game_tracker.cached_response.timestamp.load(Ordering::SeqCst) + timeout {
			*response = Response::builder().header("Content-Type", "application/json").body(game_tracker.to_json()).unwrap();
		}

		Ok((*response).clone())
	}

	fn to_json(&self) -> String {
		let psn_games: Vec<(String, i64, Vec<String>)> = self
			.psn_games
			.read()
			.iter()
			.filter_map(|(name, game_info)| {
				let num_users = game_info.num_users.load(Ordering::SeqCst);
				if num_users != 0 {
					Some((com_id_to_string(name), num_users, game_info.name_hints.read().iter().cloned().collect()))
				} else {
					None
				}
			})
			.collect();

		let ticket_games: Vec<(String, i64)> = self
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

		// The game id doesn't need to be sanitized as it is composed only of alphanumerical ascii chars(checked before being passed to game tracker)

		let sanitize_for_json = |s: &str| -> String {
			let mut res = String::with_capacity(s.len());

			for c in s.chars() {
				match c {
					'"' => {
						let _ = write!(res, "\\\"");
					}
					'\\' => {
						let _ = write!(res, "\\\\");
					}
					'\x08' | '\x0C' | '\n' | '\r' | '\t' => {} // \b and \f
					_ => res.push(c),
				}
			}

			res
		};

		let add_games_with_hints = |string: &mut String, section_name: &str, v: &Vec<(String, i64, Vec<String>)>| {
			if !v.is_empty() {
				let _ = write!(string, ",\n    \"{}\": {{\n", section_name);

				for (index, (name, num, name_hints)) in v.iter().enumerate() {
					let _ = write!(string, "        \"{}\": [{}", name, num);
					for hint in name_hints {
						let _ = write!(string, ", \"{}\"", sanitize_for_json(hint));
					}
					let _ = write!(string, "]");
					*string += if index != (v.len() - 1) { ",\n" } else { "\n" };
				}

				*string += "    }"
			}
		};

		let add_games = |string: &mut String, section_name: &str, v: &Vec<(String, i64)>| {
			if !v.is_empty() {
				let _ = write!(string, ",\n    \"{}\": {{\n", section_name);

				for (index, (name, num)) in v.iter().enumerate() {
					let _ = write!(string, "        \"{}\": {}", name, num);
					*string += if index != (v.len() - 1) { ",\n" } else { "\n" };
				}

				*string += "    }"
			}
		};

		add_games_with_hints(&mut res, "psn_games", &psn_games);
		add_games(&mut res, "ticket_games", &ticket_games);

		res += "\n}";

		res
	}

	pub fn new() -> GameTracker {
		GameTracker {
			num_users: AtomicI64::new(0),
			psn_games: RwLock::new(HashMap::new()),
			ticket_games: RwLock::new(HashMap::new()),
			cached_response: CachedResponse {
				timestamp: AtomicU32::new(0),
				cached_response: Mutex::new(Response::new("".to_string())),
			},
		}
	}

	pub fn add_gamename_hint(&self, com_id: &ComId, name_hint: &str) {
		let psn_games = self.psn_games.read();
		let game_info = psn_games.get(com_id);
		if game_info.is_none() {
			error!("Inconsistency in gametracker!");
			return;
		}
		let game_info = game_info.unwrap();

		if game_info.name_hints.read().contains(name_hint) {
			return;
		}

		game_info.name_hints.write().insert(name_hint.to_string());
	}

	pub fn increase_num_users(&self) {
		self.num_users.fetch_add(1, Ordering::SeqCst);
	}

	pub fn decrease_num_users(&self) {
		self.num_users.fetch_sub(1, Ordering::SeqCst);
	}

	fn add_value_psn(&self, com_id: &ComId, to_add: i64) {
		{
			let psn_games = self.psn_games.read();
			if psn_games.contains_key(com_id) {
				psn_games[com_id].num_users.fetch_add(to_add, Ordering::SeqCst);
				return;
			}
		}

		self.psn_games
			.write()
			.entry(*com_id)
			.or_insert_with(|| GameInfo {
				num_users: AtomicI64::new(0),
				name_hints: RwLock::new(HashSet::new()),
			})
			.num_users
			.fetch_add(to_add, Ordering::SeqCst);
	}

	pub fn increase_count_psn(&self, com_id: &ComId) {
		self.add_value_psn(com_id, 1);
	}

	pub fn decrease_count_psn(&self, com_id: &ComId) {
		self.add_value_psn(com_id, -1);
	}

	fn add_value_ticket(&self, service_id: &str, to_add: i64) {
		{
			let ticket_games = self.ticket_games.read();
			if ticket_games.contains_key(service_id) {
				ticket_games[service_id].fetch_add(to_add, Ordering::SeqCst);
				return;
			}
		}

		self.ticket_games
			.write()
			.entry(service_id.to_owned())
			.or_insert_with(|| AtomicI64::new(0))
			.fetch_add(to_add, Ordering::SeqCst);
	}

	pub fn increase_count_ticket(&self, service_id: &str) {
		self.add_value_ticket(service_id, 1);
	}

	pub fn decrease_count_ticket(&self, service_id: &str) {
		self.add_value_ticket(service_id, -1);
	}
}
