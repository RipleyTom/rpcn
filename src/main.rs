use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::env;
use std::fs::File;
use std::io::Read;
use std::str::FromStr;

mod server;
use server::client::ComId;
use server::Server;

pub struct Config {
    create_missing: bool,  // Creates servers/worlds/lobbies if the client queries for ones but there are none or specific id queries
    run_udp_server: bool,
    verbosity: tracing::Level,
    host: String,
    port: String,
    email_validated: bool, // Requires email validation
    email_host: String,
    email_login: String,
    email_password: String,
    banned_domains: HashSet<String>,
    server_redirs: HashMap<ComId, ComId>,
}

impl Config {
    pub fn new() -> Config {
        Config {
            create_missing: true,
            run_udp_server: true,
            verbosity: tracing::Level::INFO,
            host: "0.0.0.0".to_string(),
            port: "31313".to_string(),
            email_validated: false,
            email_host: String::new(),
            email_login: String::new(),
            email_password: String::new(),
            banned_domains: HashSet::new(),
            server_redirs: HashMap::new(),
        }
	}

	pub fn load_config_file(&mut self) -> Result<(), std::io::Error> {
		let mut file = File::open("rpcn.cfg")?;
		let mut buf_file = String::new();
		file.read_to_string(&mut buf_file)?;

		let config_data: HashMap<&str, &str> = buf_file.lines().filter_map(|l| {
			if l.is_empty() || l.chars().nth(0).unwrap() == '#' {
				return None;
			}

			let name_and_value: Vec<&str> = l.trim().splitn(2, '=').collect();
			if name_and_value.len() != 2 {
				return None;
			}
			Some((name_and_value[0], name_and_value[1]))
		}).collect();

		let set_bool = |name: &str, d_bool: &mut bool| {
			if let Some(data) = config_data.get(name) {
				match data {
					s if s.eq_ignore_ascii_case("true") => *d_bool = true,
					s if s.eq_ignore_ascii_case("false") => *d_bool = false,
					s => println!("Invalid value({}) for configuration entry {}, defaulting to {}", s, name, *d_bool),
				}
			} else {
				println!("Configuration entry for {} was not found, defaulting to {}", name, d_bool);
			}
		};

		let set_string = |name: &str, d_str: &mut String| {
			if let Some(data) = config_data.get(name) {
				*d_str = String::from(*data);
			} else {
				println!("Configuration entry for {} was not found, defaulting to {}", name, d_str);
			}
		};

		let set_verbosity = |d_verbosity: &mut tracing::Level| {
			if let Some(data) = config_data.get("Verbosity") {
				if let Ok(level) = tracing::Level::from_str(data) {
					*d_verbosity = level;
				} else {
					println!("Config value given for Verbosity({}) is invalid, defaulting to {}!", data, d_verbosity);
				}
			} else {
				println!("Configuration entry for Verbosity was not found, defaulting to {}", d_verbosity);
			}
		};

		set_bool("CreateMissing", &mut self.create_missing);
		set_bool("RunUdpServer", &mut self.run_udp_server);
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

		Ok(())
	}
	
    pub fn is_create_missing(&self) -> bool {
        self.create_missing
    }

    pub fn is_email_validated(&self) -> bool {
        self.email_validated
    }

    pub fn is_run_udp_server(&self) -> bool {
        self.run_udp_server
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
            self.banned_domains = buf_file.lines().map(|x| x.trim().to_string()).collect();
        }
    }
    pub fn is_banned_domain(&self, domain: &str) -> bool {
        self.banned_domains.contains(domain)
    }

    pub fn load_server_redirections(&mut self) {
        if let Ok(mut file_redirs) = File::open("server_redirs.txt") {
			let mut buf_file = String::new();
			let _ = file_redirs.read_to_string(&mut buf_file);
            self.server_redirs = buf_file
                .lines()
                .filter_map(|line| {
                        let parsed: Vec<&[u8]> = line.trim().split("=>").map(|x| x.as_bytes()).collect();
                        if parsed.len() != 2 || parsed[0].len() != 9 || parsed[1].len() != 9 {
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
            Some(redir) => redir.clone(),
            None => com_id,
        }
    }
}

fn main() {
    println!("RPCN v{}", env!("CARGO_PKG_VERSION"));

	let mut config = Config::new();
	if let Err(e) = config.load_config_file() {
		println!("An error happened reading the config file rpcn.cfg: {}\nDefault values will be used for every settings!", e);
	}

    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(*config.get_verbosity())
        .without_time()
        .with_target(true)
        .with_ansi(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("Setting default subscriber failed!");

    config.load_domains_banlist();
    config.load_server_redirections();

    let mut serv = Server::new(config);

    if let Err(e) = serv.start() {
        println!("Server terminated with error: {}", e);
    } else {
        println!("Server terminated normally");
    }
}
