use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{self, BufReader};
use std::net::ToSocketAddrs;
use std::sync::Arc;

use rustls_pemfile::{certs, pkcs8_private_keys};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::runtime;
use tokio::sync::watch;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use tokio_rustls::rustls::server::ServerConfig;
use tracing::{error, info, warn};

use parking_lot::RwLock;

use socket2::{SockRef, TcpKeepalive};

pub mod client;
use client::{Client, ClientSharedInfo, HEADER_SIZE, PacketType, SharedData, TerminateWatch};
mod database;
mod game_tracker;
use game_tracker::GameTracker;
mod gui_room_manager;
use gui_room_manager::GuiRoomManager;
mod room_manager;
use room_manager::RoomManager;
mod score_cache;
use score_cache::ScoresCache;
mod daily_cleaner;
mod stat_server;
mod udp_server;
mod utils;
use crate::Config;
use crate::server::database::Database;

#[allow(non_snake_case, dead_code)]
mod stream_extractor;

const PROTOCOL_VERSION: u32 = 30;

pub struct Server {
	config: Arc<RwLock<Config>>,
	db_pool: r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
	gui_room_manager: Arc<RwLock<GuiRoomManager>>,
	room_manager: Arc<RwLock<RoomManager>>,
	client_infos: Arc<RwLock<HashMap<i64, ClientSharedInfo>>>,
	score_cache: Arc<ScoresCache>,
	game_tracker: Arc<GameTracker>,
	cleanup_duty: Arc<RwLock<HashSet<i64>>>,
}

fn install_signal_handlers(term_watch: &TerminateWatch) {
	{
		let term_watch = term_watch.clone();
		tokio::spawn(async move {
			if tokio::signal::ctrl_c().await.is_ok() {
				info!("SIGINT received, shutting down");
				let _ = term_watch.send.lock().send(true);
			}
		});
	}

	#[cfg(unix)]
	{
		let term_watch = term_watch.clone();
		tokio::spawn(async move {
			let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("Failed to register SIGTERM handler");
			sigterm.recv().await;
			info!("SIGTERM received, shutting down");
			let _ = term_watch.send.lock().send(true);
		});
	}
}

impl Server {
	pub fn new(config: Config) -> Result<Server, String> {
		let config = Arc::new(RwLock::new(config));

		let db_pool = Server::initialize_database(config.read().get_admins_list())?;
		let score_cache = Server::initialize_score(db_pool.get().map_err(|e| format!("Failed to get a database connection: {}", e))?)?;
		Server::initialize_tus_data_handler()?;

		let db = Database::new(db_pool.get().map_err(|e| format!("Failed to get a database connection: {}", e))?);
		db.cleanup_never_used_accounts().map_err(|e| format!("Failed to clean never used accounts; {}", e))?;

		Server::clean_score_data(db_pool.get().map_err(|e| format!("Failed to get a database connection: {}", e))?)?;
		Server::clean_tus_data(db_pool.get().map_err(|e| format!("Failed to get a database connection: {}", e))?)?;

		let gui_room_manager = Arc::new(RwLock::new(GuiRoomManager::new()));
		let room_manager = Arc::new(RwLock::new(RoomManager::new()));
		let client_infos = Arc::new(RwLock::new(HashMap::new()));
		let game_tracker = Arc::new(GameTracker::new());
		let cleanup_duty = Arc::new(RwLock::new(HashSet::new()));

		Ok(Server {
			config,
			db_pool,
			gui_room_manager,
			room_manager,
			client_infos,
			score_cache,
			game_tracker,
			cleanup_duty,
		})
	}

	#[cfg(any(
		doc,
		target_os = "android",
		target_os = "dragonfly",
		target_os = "freebsd",
		target_os = "fuchsia",
		target_os = "illumos",
		target_os = "linux",
		target_os = "netbsd",
		target_vendor = "apple",
	))]
	fn set_socket_keepalive(stream: &tokio::net::TcpStream) -> Result<(), std::io::Error> {
		let socket_ref = SockRef::from(stream);
		socket_ref.set_tcp_keepalive(
			&TcpKeepalive::new()
				.with_time(std::time::Duration::new(30, 0))
				.with_interval(std::time::Duration::new(30, 0))
				.with_retries(4),
		)
	}

	#[cfg(target_os = "windows")]
	fn set_socket_keepalive(stream: &tokio::net::TcpStream) -> Result<(), std::io::Error> {
		let socket_ref = SockRef::from(stream);
		socket_ref.set_tcp_keepalive(&TcpKeepalive::new().with_time(std::time::Duration::new(30, 0)).with_interval(std::time::Duration::new(30, 0)))
	}

	fn load_certificate() -> io::Result<ServerConfig> {
		// Setup TLS
		let f_cert = File::open("cert.pem").map_err(|e| io::Error::new(e.kind(), "Failed to open certificate cert.pem"))?;
		let f_key = std::fs::File::open("key.pem").map_err(|e| io::Error::new(e.kind(), "Failed to open private key key.pem"))?;
		let certif = certs(&mut BufReader::new(&f_cert))
			.collect::<Result<Vec<CertificateDer<'static>>, io::Error>>()
			.map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "cert.pem is invalid"))?;
		let mut private_key = pkcs8_private_keys(&mut BufReader::new(&f_key))
			.collect::<Result<Vec<PrivatePkcs8KeyDer<'static>>, io::Error>>()
			.map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "key.pem is invalid"))?;
		if certif.is_empty() || private_key.is_empty() {
			return Err(io::Error::new(io::ErrorKind::InvalidInput, "key.pem doesn't contain a PKCS8 encoded private key!"));
		}

		ServerConfig::builder()
			.with_no_client_auth()
			.with_single_cert(certif, private_key.remove(0).into())
			.map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "Failed to setup certificate"))
	}

	pub fn start(&mut self) -> io::Result<()> {
		// Parse host address
		let str_addr = self.config.read().get_host_ipv4().clone() + ":" + self.config.read().get_port();
		let mut addr = str_addr.to_socket_addrs().map_err(|e| io::Error::new(e.kind(), format!("{} is not a valid address", &str_addr)))?;
		let addr = addr
			.next()
			.ok_or_else(|| io::Error::new(io::ErrorKind::AddrNotAvailable, format!("{} is not a valid address", &str_addr)))?;

		let server_config = Server::load_certificate().inspect_err(|_| {
			println!("A certificate error occurred. To generate a certificate, run:");
			let exe_path = crate::env::args().next().unwrap_or(String::from("rpcn"));
			println!("{} --cert-gen", exe_path);
		})?;

		// Setup Tokio
		let runtime = runtime::Builder::new_multi_thread().enable_all().build()?;
		let handle = runtime.handle().clone();
		let acceptor = TlsAcceptor::from(Arc::new(server_config));

		let mut servinfo_vec = vec![PacketType::ServerInfo as u8];
		servinfo_vec.extend(&0u16.to_le_bytes());
		servinfo_vec.extend(&(4 + HEADER_SIZE).to_le_bytes());
		servinfo_vec.extend(&0u64.to_le_bytes());
		servinfo_vec.extend(&PROTOCOL_VERSION.to_le_bytes());
		let servinfo_vec: Arc<Vec<u8>> = Arc::new(servinfo_vec);

		let fut_server = async {
			let (term_send, term_recv) = watch::channel(false);
			let mut term_watch = TerminateWatch::new(term_recv, term_send);

			install_signal_handlers(&term_watch);

			self.start_udp_server(term_watch.clone()).await?;
			self.start_stat_server(term_watch.clone(), self.game_tracker.clone()).await?;
			self.start_cleaner_task(term_watch.clone(), self.db_pool.clone(), self.client_infos.clone()).await;

			let listener = TcpListener::bind(&addr).await.map_err(|e| io::Error::new(e.kind(), format!("Error binding to <{}>: {}", &addr, e)))?;
			info!("Now waiting for connections on <{}>", &addr);

			'main_loop: loop {
				tokio::select! {
					accept_result = listener.accept() => {
						if let Err(e) = accept_result {
							warn!("Accept failed with: {}", e);
							continue 'main_loop;
						}
						let (stream, peer_addr) = accept_result.unwrap();

						{
							if let Err(e) = Server::set_socket_keepalive(&stream) {
									error!("set_tcp_keepalive() failed with: {}", e);
								}
						}

						info!("New client from {}", peer_addr);
						let acceptor = acceptor.clone();
						let config = self.config.clone();
						let db_pool = self.db_pool.clone();
						let shared = SharedData::new(self.gui_room_manager.clone(), self.room_manager.clone(), self.client_infos.clone(), self.score_cache.clone(), self.game_tracker.clone(), self.cleanup_duty.clone());
						let servinfo_vec = servinfo_vec.clone();
						let term_watch = term_watch.clone();
						let fut_client = async move {
							let mut stream = acceptor.accept(stream).await?;
							stream.write_all(&servinfo_vec).await?;
							let (mut client, mut tls_reader) = Client::new(config, stream, db_pool, shared, term_watch).await;
							client.process(&mut tls_reader).await;
							Ok(()) as io::Result<()>
						};
						handle.spawn(fut_client);
					}
					_ = term_watch.recv.changed() => {
						break 'main_loop;
					}
				}
			}

			Ok(())
		};
		let res = runtime.block_on(fut_server);
		runtime.shutdown_timeout(std::time::Duration::from_secs(120));
		res
	}
}
