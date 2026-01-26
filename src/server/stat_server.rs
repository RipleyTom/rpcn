use std::collections::HashMap;
use std::convert::Infallible;
use std::fmt::Write;
use std::io;
use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response};
use hyper_util::rt::TokioIo;
use parking_lot::Mutex;
use tokio::net::TcpListener;
use tracing::{info, warn};

use crate::Client;
use crate::server::GameTracker;
use crate::server::Server;
use crate::server::client::{COMMUNICATION_ID_SIZE, ComId, TerminateWatch, com_id_to_string};
use crate::server::database::db_score::DbBoardInfo;
use crate::server::score_cache::{GetScoreResultCache, ScoresCache};

struct CachedResponse {
	timestamp: AtomicU32,
	cached_response: Mutex<Response<String>>,
}

impl CachedResponse {
	fn new() -> CachedResponse {
		CachedResponse {
			timestamp: AtomicU32::new(0),
			cached_response: Mutex::new(Response::new("".to_string())),
		}
	}
}

struct JsonScoreCache {
	table_cache: Mutex<HashMap<ComId, HashMap<u32, CachedResponse>>>,
	com_id_cache: Mutex<HashMap<ComId, CachedResponse>>,
}

impl JsonScoreCache {
	fn new() -> JsonScoreCache {
		JsonScoreCache {
			table_cache: Mutex::new(HashMap::new()),
			com_id_cache: Mutex::new(HashMap::new()),
		}
	}
}

struct JsonCache {
	usage_cache: CachedResponse,
	score_cache: JsonScoreCache,
}

impl JsonCache {
	fn new() -> JsonCache {
		JsonCache {
			usage_cache: CachedResponse::new(),
			score_cache: JsonScoreCache::new(),
		}
	}
}

pub struct StatServer {
	listener: TcpListener,
	term_watch: TerminateWatch,
	path: String,
	cache_life: u32,
	game_tracker: Arc<GameTracker>,
	score_cache: Arc<ScoresCache>,
	json_cache: Arc<JsonCache>,
}

fn sanitize_for_json(s: &str) -> String {
	let mut res = String::with_capacity(s.len());
	for c in s.chars() {
		match c {
			'"' => res.push_str("\\\""),
			'\\' => res.push_str("\\\\"),
			'\x08' | '\x0C' | '\n' | '\r' | '\t' => {}
			_ => res.push(c),
		}
	}
	res
}

impl Server {
	pub async fn start_stat_server(&self, term_watch: TerminateWatch, game_tracker: Arc<GameTracker>) -> io::Result<()> {
		let (bind_addr, cache_life, path);
		{
			let config = self.config.read();
			bind_addr = config.get_stat_server_binds().clone();
			cache_life = config.get_stat_server_cache_life();
			path = format!("/{}", config.get_stat_server_path());
		}

		let score_cache = self.score_cache.clone();

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

			let mut stat_server = StatServer::new(listener, term_watch, path, cache_life, game_tracker, score_cache);

			tokio::task::spawn(async move {
				stat_server.server_proc().await;
			});
		}

		Ok(())
	}
}

impl StatServer {
	fn new(listener: TcpListener, term_watch: TerminateWatch, path: String, cache_life: u32, game_tracker: Arc<GameTracker>, score_cache: Arc<ScoresCache>) -> StatServer {
		StatServer {
			listener,
			term_watch,
			path,
			cache_life,
			game_tracker,
			score_cache,
			json_cache: Arc::new(JsonCache::new()),
		}
	}

	async fn server_proc(&mut self) {
		if *self.term_watch.recv.borrow_and_update() {
			return;
		}

		'stat_server_loop: loop {
			tokio::select! {
				accept_res = self.listener.accept() => {
					if let Err(e) = accept_res {
						warn!("Stat: Error accepting a client: {}", e);
						continue 'stat_server_loop;
					}

					let (stream, peer_addr) = accept_res.unwrap();
					let io = TokioIo::new(stream);

					info!("Stat: new client from {}", peer_addr);
					{
						let path = self.path.clone();
						let cache_life = self.cache_life;
						let game_tracker = self.game_tracker.clone();
						let score_cache = self.score_cache.clone();
						let json_cache = self.json_cache.clone();

						tokio::task::spawn(async move {
							if let Err(err) = http1::Builder::new().keep_alive(false).serve_connection(io, service_fn(|r| StatServer::handle_stat_server_req(r, &path, cache_life, game_tracker.clone(), score_cache.clone(), json_cache.clone()))).await {
								warn!("Stat: Error serving connection: {}", err);
							}
						});
					}
				}
				_ = self.term_watch.recv.changed() => {
					break 'stat_server_loop;
				}
			}
		}
		info!("GameTracker::server_proc terminating");
	}

	fn handle_usage_req(cache_life: u32, game_tracker: &Arc<GameTracker>, json_cache: &Arc<JsonCache>) -> Result<Response<String>, Infallible> {
		if cache_life == 0 {
			return Ok(Response::builder()
				.header("Content-Type", "application/json")
				.body(StatServer::game_tracker_to_json(game_tracker))
				.unwrap());
		}

		let new_timestamp = Client::get_timestamp_seconds();
		let mut response = json_cache.usage_cache.cached_response.lock();

		let is_stale = new_timestamp > json_cache.usage_cache.timestamp.load(Ordering::SeqCst) + cache_life;
		if is_stale {
			*response = Response::builder()
				.header("Content-Type", "application/json")
				.body(StatServer::game_tracker_to_json(game_tracker))
				.unwrap();
		}

		Ok((*response).clone())
	}

	fn com_id_score_to_json(score_cache: &Arc<ScoresCache>, com_id: &ComId) -> String {
		let mut tables = score_cache.get_all_tables(com_id);
		if tables.is_empty() {
			return "[]".to_owned();
		}

		tables.sort_by_key(|(board_id, _)| *board_id);

		let mut res = String::from("[\n");
		for (index, (board_id, table)) in tables.iter().enumerate() {
			let table_infos = {
				let table = table.read();
				table.table_info.clone()
			};

			let result = score_cache.get_score_range(com_id, *board_id, 1, table_infos.rank_limit, true, true);
			let json = StatServer::score_result_to_json(&result, *board_id, &table_infos);
			res += &json;
			if index != tables.len() - 1 {
				res += ",\n";
			} else {
				res += "\n";
			}
		}
		res += "]";
		res
	}

	fn handle_com_id_score_req(cache_life: u32, score_cache: &Arc<ScoresCache>, json_cache: &Arc<JsonCache>, com_id: &ComId) -> Result<Response<String>, Infallible> {
		if cache_life == 0 {
			let json = StatServer::com_id_score_to_json(score_cache, com_id);
			return Ok(Response::builder().header("Content-Type", "application/json").body(json).unwrap());
		}

		let new_timestamp = Client::get_timestamp_seconds();
		let mut com_id_map = json_cache.score_cache.com_id_cache.lock();
		let cached = com_id_map.entry(*com_id).or_insert_with(CachedResponse::new);

		let is_stale = new_timestamp > cached.timestamp.load(Ordering::SeqCst) + cache_life;
		if is_stale {
			let json = StatServer::com_id_score_to_json(score_cache, com_id);
			*cached.cached_response.lock() = Response::builder().header("Content-Type", "application/json").body(json).unwrap();
			cached.timestamp.store(new_timestamp, Ordering::SeqCst);
		}

		Ok(cached.cached_response.lock().clone())
	}

	fn handle_table_score_req(cache_life: u32, score_cache: &Arc<ScoresCache>, json_cache: &Arc<JsonCache>, com_id: &ComId, table_id: u32) -> Result<Response<String>, Infallible> {
		if let Some(table_cache) = score_cache.get_table(com_id, table_id) {
			let table_infos = {
				let table_cache = table_cache.read();
				table_cache.table_info.clone()
			};

			if cache_life == 0 {
				let result = score_cache.get_score_range(com_id, table_id, 1, table_infos.rank_limit, true, true);
				let json = StatServer::score_result_to_json(&result, table_id, &table_infos);
				return Ok(Response::builder().header("Content-Type", "application/json").body(json).unwrap());
			}

			let new_timestamp = Client::get_timestamp_seconds();
			let mut score_map = json_cache.score_cache.table_cache.lock();
			let table_map = score_map.entry(*com_id).or_default();
			let cached = table_map.entry(table_id).or_insert_with(CachedResponse::new);

			let is_stale = new_timestamp > cached.timestamp.load(Ordering::SeqCst) + cache_life;
			if is_stale {
				let result = score_cache.get_score_range(com_id, table_id, 1, table_infos.rank_limit, true, true);
				let json = StatServer::score_result_to_json(&result, table_id, &table_infos);
				*cached.cached_response.lock() = Response::builder().header("Content-Type", "application/json").body(json).unwrap();
				cached.timestamp.store(new_timestamp, Ordering::SeqCst);
			}

			return Ok(cached.cached_response.lock().clone());
		}

		Ok(Response::new("".to_owned()))
	}

	async fn handle_stat_server_req(
		req: Request<hyper::body::Incoming>,
		path: &str,
		cache_life: u32,
		game_tracker: Arc<GameTracker>,
		score_cache: Arc<ScoresCache>,
		json_cache: Arc<JsonCache>,
	) -> Result<Response<String>, Infallible> {
		if req.method() != Method::GET {
			return Ok(Response::new("".to_owned()));
		}

		let req_path = req.uri().path();
		let usage_path = format!("{}/usage", path);
		let score_prefix = format!("{}/score/", path);

		if req_path == usage_path {
			return StatServer::handle_usage_req(cache_life, &game_tracker, &json_cache);
		}

		if let Some(rest) = req_path.strip_prefix(&score_prefix) {
			let parts: Vec<&str> = rest.splitn(2, '/').collect();
			let com_id_str = parts[0];
			if com_id_str.len() == COMMUNICATION_ID_SIZE {
				let mut com_id: ComId = [0u8; COMMUNICATION_ID_SIZE];
				com_id.copy_from_slice(com_id_str.as_bytes());

				if parts.len() == 2 {
					if let Ok(table_id) = parts[1].parse::<u32>() {
						return StatServer::handle_table_score_req(cache_life, &score_cache, &json_cache, &com_id, table_id);
					}
				} else {
					return StatServer::handle_com_id_score_req(cache_life, &score_cache, &json_cache, &com_id);
				}
			}
		}

		Ok(Response::new("".to_owned()))
	}

	fn game_tracker_to_json(game_tracker: &Arc<GameTracker>) -> String {
		let psn_games: Vec<(String, i64, Vec<String>)> = game_tracker
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

		let ticket_games: Vec<(String, i64)> = game_tracker
			.ticket_games
			.read()
			.iter()
			.filter_map(|(name, num_users)| {
				let num_users = num_users.load(Ordering::SeqCst);
				if num_users != 0 { Some((name.clone(), num_users)) } else { None }
			})
			.collect();

		let mut res = String::from("{\n");
		let _ = write!(res, "    \"num_users\" : {}", game_tracker.num_users.load(Ordering::SeqCst));

		// The game id doesn't need to be sanitized as it is composed only of alphanumerical ascii chars(checked before being passed to game tracker)
		let add_games_with_hints = |string: &mut String, section_name: &str, v: &Vec<(String, i64, Vec<String>)>| {
			if !v.is_empty() {
				let _ = writeln!(string, ",\n    \"{}\": {{", section_name);

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
				let _ = writeln!(string, ",\n    \"{}\": {{", section_name);

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

	fn score_result_to_json(result: &GetScoreResultCache, board_id: u32, table_infos: &DbBoardInfo) -> String {
		let mut res = String::from("{\n");
		let _ = writeln!(res, "    \"board_id\": {},", board_id);
		let _ = writeln!(res, "    \"rank_limit\": {},", table_infos.rank_limit);
		let _ = writeln!(res, "    \"update_mode\": {},", table_infos.update_mode);
		let _ = writeln!(res, "    \"sort_mode\": {},", table_infos.sort_mode);
		let _ = writeln!(res, "    \"upload_num_limit\": {},", table_infos.upload_num_limit);
		let _ = writeln!(res, "    \"upload_size_limit\": {},", table_infos.upload_size_limit);
		let _ = writeln!(res, "    \"total_records\": {},", result.total_records);
		let _ = writeln!(res, "    \"scores\": [");

		for (index, score) in result.scores.iter().enumerate() {
			// NPID is inherently json safe as it is verified at user creation
			let online_name = sanitize_for_json(&score.online_name);
			let comment = result.comments.as_ref().map(|c| sanitize_for_json(&c[index])).unwrap_or_default();
			let game_info = result
				.infos
				.as_ref()
				.map(|g| g[index].iter().map(|b| format!("{:02x}", b)).collect::<Vec<_>>().join(""))
				.unwrap_or_default();

			let _ = writeln!(res, "        {{\n");
			let _ = writeln!(res, "            \"rank\": {},", score.rank + 1);
			let _ = writeln!(res, "            \"npid\": \"{}\",", score.npid);
			let _ = writeln!(res, "            \"online_name\": \"{}\",", online_name);
			let _ = writeln!(res, "            \"pcid\": {},", score.pcid);
			let _ = writeln!(res, "            \"score\": {},", score.score);
			let _ = writeln!(res, "            \"has_gamedata\": {},", score.has_gamedata);
			let _ = writeln!(res, "            \"comment\": \"{}\",", comment);
			let _ = writeln!(res, "            \"info\": \"{}\",", game_info);
			let _ = writeln!(res, "            \"timestamp\": {}", score.timestamp);
			let _ = writeln!(res, "        }}");
			if index != result.scores.len() - 1 {
				res += ",\n";
			} else {
				res += "\n";
			}
		}

		res += "    ]\n}";
		res
	}
}
