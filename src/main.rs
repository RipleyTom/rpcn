use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader};

use clap;
use clap::{App, Arg};

mod server;
use server::client::ComId;
use server::Server;

pub struct Config {
    create_missing: bool,  // Creates servers/worlds/lobbies if the client queries for ones but there are none or specific id queries
    email_validated: bool, // Requires email validation
    run_udp_server: bool,
    verbose: bool,
    host: String,
    port: String,
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
            email_validated: true,
            run_udp_server: true,
            verbose: false,
            host: "0.0.0.0".to_string(),
            port: "31313".to_string(),
            email_host: String::new(),
            email_login: String::new(),
            email_password: String::new(),
            banned_domains: HashSet::new(),
            server_redirs: HashMap::new(),
        }
    }

    pub fn set_create_missing(&mut self, create_missing: bool) {
        self.create_missing = create_missing;
    }
    pub fn is_create_missing(&self) -> bool {
        self.create_missing
    }

    pub fn set_email_validated(&mut self, email_validated: bool) {
        self.email_validated = email_validated;
    }
    pub fn is_email_validated(&self) -> bool {
        self.email_validated
    }

    pub fn set_run_udp_server(&mut self, udp_server: bool) {
        self.run_udp_server = udp_server;
    }
    pub fn is_run_udp_server(&self) -> bool {
        self.run_udp_server
    }

    pub fn set_verbose(&mut self, verbose: bool) {
        self.verbose = verbose;
    }
    pub fn is_verbose(&self) -> bool {
        self.verbose
    }

    pub fn set_host(&mut self, host: &str) {
        self.host = host.to_string();
    }
    pub fn get_host(&self) -> &String {
        &self.host
    }

    pub fn set_port(&mut self, port: &str) {
        self.port = port.to_string();
    }
    pub fn get_port(&self) -> &String {
        &self.port
    }

    pub fn set_email_auth(&mut self, email_data: &str) -> Result<(), ()> {
        let email_tokens: Vec<&str> = email_data.split("::").collect();
        if email_tokens.len() != 3 {
            return Err(());
        }

        self.email_host = String::from(email_tokens[0]);
        self.email_login = String::from(email_tokens[1]);
        self.email_password = String::from(email_tokens[2]);

        Ok(())
    }
    pub fn get_email_auth(&self) -> (String, String, String) {
        (self.email_host.clone(), self.email_login.clone(), self.email_password.clone())
    }

    pub fn load_domains_banlist(&mut self) {
        if let Ok(file_emails) = File::open("domains_banlist.txt") {
            let br = BufReader::new(file_emails);
            self.banned_domains = br.lines().map(|x| x.unwrap().trim().to_string()).collect();
        }
    }
    pub fn is_banned_domain(&self, domain: &str) -> bool {
        self.banned_domains.contains(domain)
    }

    pub fn load_server_redirections(&mut self) {
        if let Ok(file_redirs) = File::open("server_redirs.txt") {
            let br = BufReader::new(file_redirs);
            self.server_redirs = br
                .lines()
                .filter_map(|x| {
                    if let Ok(line_str) = x {
                        let parsed: Vec<&[u8]> = line_str.trim().split("=>").map(|x| x.as_bytes()).collect();
                        if parsed.len() != 2 || parsed[0].len() != 9 || parsed[1].len() != 9 {
                            None
                        } else {
                            Some((parsed[0].try_into().unwrap(), parsed[1].try_into().unwrap()))
                        }
                    } else {
                        None
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
    let matches = App::new("RPCN")
        .version(clap::crate_version!())
        .author(clap::crate_authors!())
        .about("Matchmaking server")
        .arg(Arg::with_name("verbose").short("v").long("verbose").takes_value(false).help("Enables verbose output"))
        .arg(Arg::with_name("nocreate").long("nocreate").takes_value(false).help("Disables automated creation on request"))
        .arg(Arg::with_name("noemail").long("noemail").takes_value(false).help("Disables email validation"))
        .arg(Arg::with_name("noudp").long("noudp").takes_value(false).help("Disables udp server"))
        .arg(Arg::with_name("emailauth").long("emailauth").takes_value(true).help("Host::Login::Password for email"))
        .arg(Arg::with_name("host").short("h").long("host").takes_value(true).help("Binding address(hostname)"))
        .arg(Arg::with_name("port").short("p").long("port").takes_value(true).help("Binding port"))
        .get_matches();

    println!("RPCN v{}", env!("CARGO_PKG_VERSION"));

    let mut config = Config::new();

    if matches.is_present("nocreate") {
        config.set_create_missing(false);
    }

    if matches.is_present("noemail") {
        config.set_email_validated(false);
    }

    if matches.is_present("noudp") {
        config.set_run_udp_server(false);
    }

    if matches.is_present("verbose") {
        config.set_verbose(true);
    }

    if let Some(p_host) = matches.value_of("host") {
        config.set_host(p_host);
    }

    if let Some(p_port) = matches.value_of("port") {
        config.set_port(p_port);
    }

    if let Some(email_data) = matches.value_of("emailauth") {
        if config.set_email_auth(&email_data).is_err() {
            println!("Invalid emailauth parameter used, expected Host::Login::Password");
            return;
        }
    }

    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::TRACE)
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
