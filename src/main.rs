use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::env;
use std::fs::File;
use std::io::Read;
use std::str::FromStr;

mod server;
use server::client::{Client, ComId, COMMUNICATION_ID_SIZE};
use server::Server;

use openssl::ec::EcKey;
use openssl::hash::MessageDigest;
use openssl::pkey::{PKey, Private};

pub struct TicketSignInfo {
	pub digest: MessageDigest,
	pub key: PKey<Private>,
}

pub struct Config {
	create_missing: bool, // Creates servers/worlds/lobbies if the client queries for ones but there are none or specific id queries
	verbosity: tracing::Level,
	host: String,
	port: String,
	email_validated: bool, // Requires email validation
	email_host: String,
	email_login: String,
	email_password: String,
	banned_domains: HashSet<String>,
	server_redirs: HashMap<ComId, ComId>,
	ticket_signature_info: Option<TicketSignInfo>,
	stat_server_host_and_port: Option<(String, String)>,
	admins_list: Vec<String>,
}

impl Config {
	pub fn new() -> Config {
		Config {
			create_missing: true,
			verbosity: tracing::Level::INFO,
			host: "0.0.0.0".to_string(),
			port: "31313".to_string(),
			email_validated: false,
			email_host: String::new(),
			email_login: String::new(),
			email_password: String::new(),
			banned_domains: HashSet::new(),
			server_redirs: HashMap::new(),
			ticket_signature_info: None,
			stat_server_host_and_port: None,
			admins_list: Vec::new(),
		}
	}

	pub fn load_config_file(&mut self) -> Result<(), std::io::Error> {
		let mut file = File::open("rpcn.cfg")?;
		let mut buf_file = String::new();
		file.read_to_string(&mut buf_file)?;

		let config_data: HashMap<&str, &str> = buf_file
			.lines()
			.filter_map(|l| {
				if l.trim().is_empty() || l.trim().chars().nth(0).unwrap() == '#' {
					return None;
				}

				let name_and_value: Vec<&str> = l.trim().splitn(2, '=').collect();
				if name_and_value.len() != 2 {
					return None;
				}
				Some((name_and_value[0], name_and_value[1]))
			})
			.collect();

		let set_bool = |name: &str, d_bool: &mut bool| {
			if let Some(data) = config_data.get(name) {
				match data {
					s if s.eq_ignore_ascii_case("true") => *d_bool = true,
					s if s.eq_ignore_ascii_case("false") => *d_bool = false,
					s => println!("Invalid value(<{}>) for configuration entry <{}>, defaulting to <{}>", s, name, *d_bool),
				}
			} else {
				println!("Configuration entry for <{}> was not found, defaulting to <{}>", name, d_bool);
			}
		};

		let set_string = |name: &str, d_str: &mut String| {
			if let Some(data) = config_data.get(name) {
				*d_str = String::from(*data);
			} else {
				println!("Configuration entry for <{}> was not found, defaulting to <{}>", name, d_str);
			}
		};

		let set_verbosity = |d_verbosity: &mut tracing::Level| {
			if let Some(data) = config_data.get("Verbosity") {
				if let Ok(level) = tracing::Level::from_str(data) {
					*d_verbosity = level;
				} else {
					println!("Config value given for Verbosity(<{}>) is invalid, defaulting to <{}>!", data, d_verbosity);
				}
			} else {
				println!("Configuration entry for Verbosity was not found, defaulting to <{}>", d_verbosity);
			}
		};

		let set_admins_list = |d_list: &mut Vec<String>| {
			if let Some(data) = config_data.get("AdminsList") {
				let admins_list: Vec<String> = data.split(',').map(|a| a.trim().to_string()).collect();

				if admins_list.iter().map(|username| Client::is_valid_username(username)).any(|r| !r) {
					println!("AdminsList contains an invalid username, the setting will be ignored!");
				} else {
					*d_list = admins_list;
				}
			} else {
				println!("Configuration entry for AdminsList was not found, leaving it empty");
			}
		};

		// Unused for now
		// let set_u32 = |name: &str, d_u32: &mut u32| {
		// 	if let Some(data) = config_data.get(name) {
		// 		match data.parse::<u32>() {
		// 			Ok(parsed) => *d_u32 = parsed,
		// 			Err(e) => println!("Failed to parse value for <{}>, defaulting to <{}>: {}", name, d_u32, e),
		// 		}
		// 	} else {
		// 		println!("Configuration entry for <{}> was not found, defaulting to <{}>", name, d_u32);
		// 	}
		// };

		set_bool("CreateMissing", &mut self.create_missing);
		set_verbosity(&mut self.verbosity);
		set_bool("EmailValidated", &mut self.email_validated);
		set_string("EmailHost", &mut self.email_host);
		set_string("EmailLogin", &mut self.email_login);
		set_string("EmailPassword", &mut self.email_password);
		set_string("Host", &mut self.host);
		set_string("Port", &mut self.port);
		set_string("EmailHost", &mut self.email_host);
		set_string("EmailLogin", &mut self.email_login);
		set_string("EmailPassword", &mut self.email_password);

		let mut sign_tickets = false;
		set_bool("SignTickets", &mut sign_tickets);

		if sign_tickets {
			let ticket_key = Config::load_ticket_private_key();
			if let Err(ref e) = ticket_key {
				println!("Error loading the ticket private key:\n{}", e);
			}
			let ticket_key = ticket_key.ok();

			let mut ticket_digest_str = String::new();
			set_string("SignTicketsDigest", &mut ticket_digest_str);
			let ticket_digest = MessageDigest::from_name(&ticket_digest_str);
			if ticket_digest.is_none() {
				println!("SignTicketsDigest value <{}> is invalid!", ticket_digest_str);
			}

			if ticket_key.is_none() || ticket_digest.is_none() {
				println!("Ticket signing is enabled but it's missing digest/key, disabling ticket signing!");
			} else {
				self.ticket_signature_info = Some(TicketSignInfo {
					digest: ticket_digest.unwrap(),
					key: ticket_key.unwrap(),
				});
			}
		}

		let mut run_stat_server = false;
		set_bool("StatServer", &mut run_stat_server);

		if run_stat_server {
			let mut stat_server_host = String::new();
			let mut stat_server_port = String::new();

			set_string("StatServerHost", &mut stat_server_host);
			set_string("StatServerPort", &mut stat_server_port);

			if stat_server_host.is_empty() || stat_server_port.is_empty() {
				println!("Stat server is enabled but it's missing host/port information, disabling it!");
			} else {
				self.stat_server_host_and_port = Some((stat_server_host, stat_server_port));
			}
		}

		set_admins_list(&mut self.admins_list);

		Ok(())
	}

	pub fn is_create_missing(&self) -> bool {
		self.create_missing
	}

	pub fn is_email_validated(&self) -> bool {
		self.email_validated
	}

	pub fn get_verbosity(&self) -> &tracing::Level {
		&self.verbosity
	}

	pub fn get_host(&self) -> &String {
		&self.host
	}

	pub fn get_port(&self) -> &String {
		&self.port
	}

	pub fn get_email_auth(&self) -> (String, String, String) {
		(self.email_host.clone(), self.email_login.clone(), self.email_password.clone())
	}

	pub fn load_domains_banlist(&mut self) {
		if let Ok(mut file_emails) = File::open("domains_banlist.txt") {
			let mut buf_file = String::new();
			let _ = file_emails.read_to_string(&mut buf_file);
			self.banned_domains = buf_file.lines().map(|x| x.trim().to_ascii_lowercase()).collect();
		}
	}
	pub fn is_banned_domain(&self, domain: &str) -> bool {
		self.banned_domains.contains(domain)
	}

	pub fn load_server_redirections(&mut self) {
		if let Ok(mut file_redirs) = File::open("server_redirs.cfg") {
			let mut buf_file = String::new();
			let _ = file_redirs.read_to_string(&mut buf_file);
			self.server_redirs = buf_file
				.lines()
				.filter_map(|line| {
					let parsed: Vec<&[u8]> = line.trim().split("=>").map(|x| x.trim()).map(|x| x.as_bytes()).collect();
					if line.is_empty() || line.chars().nth(0).unwrap() == '#' || parsed.len() != 2 || parsed[0].len() != COMMUNICATION_ID_SIZE || parsed[1].len() != COMMUNICATION_ID_SIZE {
						None
					} else {
						Some((parsed[0].try_into().unwrap(), parsed[1].try_into().unwrap()))
					}
				})
				.collect();
		}
	}

	pub fn get_server_redirection(&self, com_id: ComId) -> ComId {
		match self.server_redirs.get(&com_id) {
			Some(redir) => *redir,
			None => com_id,
		}
	}

	pub fn get_ticket_signing_info(&self) -> &Option<TicketSignInfo> {
		&self.ticket_signature_info
	}

	pub fn get_stat_server_binds(&self) -> &Option<(String, String)> {
		&self.stat_server_host_and_port
	}

	pub fn get_admins_list(&self) -> &Vec<String> {
		&self.admins_list
	}

	fn load_ticket_private_key() -> Result<PKey<Private>, String> {
		let mut private_key_file = File::open("ticket_private.pem").map_err(|e| format!("Failed to open ticket_private.pem: {}", e))?;
		let mut private_key_raw = Vec::new();
		private_key_file.read_to_end(&mut private_key_raw).map_err(|e| format!("Failed to read ticket_private.pem: {}", e))?;
		let ec_key = EcKey::private_key_from_pem(&private_key_raw).map_err(|e| format!("Failed to read private key from the file: {}", e))?;
		PKey::from_ec_key(ec_key).map_err(|e| format!("Failed to convert EC key to PKey: {}", e))
	}
}

fn main() {
	println!("RPCN v{}", env!("CARGO_PKG_VERSION"));

	let mut config = Config::new();
	if let Err(e) = config.load_config_file() {
		println!("An error happened reading the config file rpcn.cfg: {}\nDefault values will be used for every settings!", e);
	}

	config.load_domains_banlist();
	config.load_server_redirections();

	let subscriber = tracing_subscriber::FmtSubscriber::builder()
		.with_max_level(*config.get_verbosity())
		.without_time()
		.with_target(true)
		.with_ansi(true)
		.finish();
	tracing::subscriber::set_global_default(subscriber).expect("Setting default subscriber failed!");

	let serv = Server::new(config);
	if let Err(e) = serv {
		println!("Failed to create server: {}", e);
		return;
	}
	let mut serv = serv.unwrap();

	if let Err(e) = serv.start() {
		println!("Server terminated with error: {}", e);
	} else {
		println!("Server terminated normally");
	}
}
