# RPCN

RPCN is a server that implements multiplayer functionality for RPCS3.  
It implements rooms which permit matchmaking, scoreboards, title user storage(ie cloud saves), etc.  
All the settings and their descriptions are in rpcn.cfg.

# FAQ

## RPCN complains about "Failed to open certificate cert.pem"

RPCN needs a certificate and its corresponding private key to be generated for the encrypted TLS connections to clients,  
you can use OpenSSL for this:
```
openssl req -newkey rsa:4096 -new -nodes -x509 -days 3650 -keyout key.pem -out cert.pem
```

You can, optionally and disabled by default, generate a key to sign generated tickets:
```
openssl ecparam -name secp224k1 -genkey -noout -out ticket_private.pem
openssl ec -in ticket_private.pem -pubout -out ticket_public.pem
```

## Will RPCN work with real PS3s?

No.

# Special Thanks

A special thanks to the various authors of the following libraries that RPCN is using:
- [Rusqlite](https://github.com/rusqlite/rusqlite)  
Perfect library if you plan to use SQLite with rust. The author has been incredibly helpful in diagnosing SQLite issues, thanks!
- [Tokio](https://github.com/tokio-rs/tokio)  
The king of async for Rust.
- [Flatbuffers](https://github.com/google/flatbuffers)  
This library has been pretty essential to the development of RPCN. Great serialization library, strict yet flexible!

And all the other libraries I'm forgetting(check Cargo.toml)!
Also thanks to everyone that contributed directly or indirectly to RPCN!
