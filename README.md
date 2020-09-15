# RPCN

Server for matchmaking meant to be used with RPCS3(see PR for more information: https://github.com/RPCS3/rpcs3/pull/8663 ).

## Requirements
### macOS
```
$ brew install openssl@1.1
```

### Arch Linux
```
$ sudo pacman -S pkg-config openssl
```

### Debian and Ubuntu
```
$ sudo apt-get install pkg-config libssl-dev
```

### Fedora
```
$ sudo dnf install pkg-config openssl-devel
```

## Building
```
cargo build
```

## Running
```
cargo run
```

## SSL

The server currently needs a certificate and its corresponding private key to be generated for it, you can use openssl for this:
```
openssl req -newkey rsa:4096 -new -nodes -x509 -days 3650 -keyout key.pem -out cert.pem
```

## Obtaining user tokens when email doesn't work

You will need sqlite3 in order to execute queries on the rpcn.db file created on server startup:

```
sudo apt-get install sqlite3 libsqlite3-dev
```
