# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.2.1] - 2024-02-25

### Fixed

- Fixed some invalid SQL requests in db_tus.rs
- Fixed RETURNING queries(UPSERT aborts on constraint fail)
- Ninetime: Added a server for DBZ Battle of Z Invitation mode


## [1.2.0] - 2024-02-23

### Added

- Added a configuration file for scoreboards

### Misc

- Version change triggered by a protocol change on rpcs3's side


## [1.1.0] - 2024-02-04

### Added

- Added a notification to signal the target in RequestSignalingInfos to help connectivity


## [1.0.3] - 2024-01-30

### Fixed

- Added owner checks to SetRoomDataInternal and SetRoomDataExternal and rpcn now only sends notifications on actual modification


## [1.0.2] - 2024-01-29

### Fixed

- Add flush() after write_all() to ensure all data is sent


## [1.0.1] - 2024-01-29

### Fixed

- Fixed GetScoreData accidentally returning a u64 for size of data


## [1.0.0] - 2024-01-14

### Added

- Implemented SetUserInfo
- Implemented GetRoomMemberBinAttr
- Added proper public/private slot values
- Added a cleanup mechanism for accounts that have never been logged on and are older than a month

### Fixed

- Added FOREIGN_KEY constraints to TUS tables to ensure sanity
- Added a mechanism to ensure cleanup is done properly before letting the user back in the server

### Misc

- Updated dependencies
- Migrated all score tables into one unified table
- Added indexes for faster lookup to the SQL tables


## [0.9.2] - 2024-01-09

### Fixed

- Forced stack size to 8MB on Windows for parity with Linux


## [0.9.1] - 2024-01-05

### Fixed

- Add brackets around Message-ID sent with emails


## [0.9.0] - 2024-01-04

### Added

- Presence support

### Fixed

- Added Message-ID to emails sent to help with some SMTP relays

### Misc

- Updated Flatbuffers to v23.5.26
- Updated hyper to v1.1
- Refactored data hierarchy for Client
- Minor code cleanups


## [0.8.2] - 2023-12-30

### Fixed

- Fixed tus_add_and_get_vuser_variable db query


## [0.8.1] - 2023-12-29

### Fixed

- Fixed TUS data query failing because of info NOT NULL constraint fail


## [0.8.0] - 2023-12-28

### Added

- Added full TUS(Title User Storage) API support.
- Added GetNetworkTime support.
- Added cleanup of unused data files from score/tus on startup(note that they are not cleaned while the server is running as otherwise I can't guarantee atomicity of the db query + file access).

### Changed

- Improved code parsing by adding some wrappers.
- Improved friend queries by storing both user id and username of friends on login.
- Improved some database queries

### Fixed

- Fixed users getting stuck in logged in state if the thread panics by moving logging out procedures to Client Drop impl.

### Misc

- Added worlds for Playstation Home to config
- Added server_redirs.cfg that contains DeS example