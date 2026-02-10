# RPCN

RPCN is a server that implements multiplayer functionality for RPCS3.  
It implements rooms which permit matchmaking, scoreboards, title user storage(ie cloud saves), etc.  
All the settings and their descriptions are in rpcn.cfg.

# FAQ

## Will RPCN work with real PS3s?

No.

# Special Thanks

A special thanks to the various authors of the following libraries that RPCN is using:
- [Rusqlite](https://github.com/rusqlite/rusqlite)  
Perfect library if you plan to use SQLite with rust. The author has been incredibly helpful in diagnosing SQLite issues, thanks!
- [Tokio](https://github.com/tokio-rs/tokio)  
The king of async for Rust.

And all the other libraries I'm forgetting(check Cargo.toml)!
Also thanks to everyone that contributed directly or indirectly to RPCN!
