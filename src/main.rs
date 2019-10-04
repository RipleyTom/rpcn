mod server;
use server::Server;

use clap;
use clap::{App, Arg};

use std::env;

// Replace config by lazy_static with RwLock maybe?
pub struct ConfigInner {
    create_empty: bool, // Creates servers/worlds/lobbies if the client queries for ones but there are none
    verbose: bool,
}

impl ConfigInner {
    pub const fn from_defaults() -> ConfigInner {
        ConfigInner { create_empty: true, verbose: false }
    }
}

pub struct Config {}

impl Config {
    pub fn new() -> Config {
        Config {}
    }

    pub fn set_create_empty(create_empty: bool) {
        unsafe {
            CONFIGINNER.create_empty = create_empty;
        }
    }
    pub fn is_create_empty() -> bool {
        unsafe { CONFIGINNER.create_empty }
    }

    pub fn set_verbose(verbose: bool) {
        unsafe {
            CONFIGINNER.verbose = verbose;
        }
    }
    pub fn is_verbose() -> bool {
        unsafe { CONFIGINNER.verbose }
    }
}

static mut CONFIGINNER: ConfigInner = ConfigInner::from_defaults();

fn main() {
    let matches = App::new("RPCN")
        .version(clap::crate_version!())
        .author(clap::crate_authors!())
        .about("Matchmaking server")
        .arg(Arg::with_name("verbose").short("v").long("verbose").takes_value(false).help("Enables verbose output"))
        .arg(Arg::with_name("nocreate").short("n").long("nocreate").takes_value(false).help("Disables automated creation on request"))
        .arg(Arg::with_name("host").short("h").long("host").takes_value(true).help("Binding address(hostname:port)"))
        .get_matches();

    println!("RPCN v{}", env!("CARGO_PKG_VERSION"));

    if matches.is_present("nocreate") {
        Config::set_create_empty(false);
    }

    if matches.is_present("verbose") {
        Config::set_verbose(true);
    }


    let mut host = "0.0.0.0:31313";

    if let Some(p_host) = matches.value_of("host") {
        host = p_host;
    }

    let mut serv = Server::new(host);

    serv.start();
}
