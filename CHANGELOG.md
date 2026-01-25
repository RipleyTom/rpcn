# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.7.1] - 2026-01-24
### Added

- Added a configuration entry to specify the API path on the stat server(defaulting to rpcn_stats)
- Added a way to dump a score table through /api_path/score/com_id/table_id on the stat server
- Added a way to dump all of a communication_id score tables through /api_path/score/com_id on the stat server

### Changed

- The path to see the usage stats is now /api_path/usage


## [1.7.0] - 2026-01-23
### Changed

- Major migration from Flatbuffers to Protobuf (for a RPCS3 license issue)

### Fixed

- FlexBy: Fixed scoreboard ordering for Super Rub'a'Dub

### Misc

- Updated dependencies


## [1.6.0] - 2026-01-12
### Added

- Added Delete command to delete a user
- Added a cleaner task charged with cleaning up the database from expired hashes from deleted users and cleaning up never used accounts periodically
- Added BanUser and DelUser admin commands
- Ninetime: Added WorldIDs for Sonic & SEGA All-Stars Racing and Koihime Enbu

### Changed

- Altered tables referencing user_id to have ON CASCADE DELETE for faster deletion

### Fixed

- Fixed check order of client_infos and cleanup_duty in login
- Fixed INDEX tus_data_vuser_author_id referencing the wrong table

### Misc

- Updated dependencies


## [1.5.0] - 2025-12-08
### Added

- FlexBy420: Added WorldIDs for more games(Star Trek, The Darkness 2, Duke Nukem 3D, Spelunker HD)

### Fixed

- Fixed passwordSlotMask not being optional in SetRoomDataInternal request which led to it being possibly reset

### Misc

- Updated project to Rust 2024
- Updated dependencies


## [1.4.4] - 2025-10-17
### Added

- FlexBy420: Added WorldIDs for many games(see #115)

### Fixed

- Limited ServerIDs to 1, assumption that some games would hardcode those and request specifics seems to have been wrong
- WorldIDs now start at 65537, a few games have been found to hardcode those

### Misc

- Updated dependencies


## [1.4.3] - 2025-10-10
### Fixed

- Fixed Span not showing when config's trace level was > Info

### Misc

- Updated dependencies


## [1.4.2] - 2025-02-21

### Fixed

- Fixed Notification to target of RequestSignalingInfos sending the wrong IP information
- CookiePLMonster: Fixed scoreboard ordering for Super Hang-On

### Misc

- Updated dependencies


## [1.4.1] - 2025-02-12

### Fixed

- Use original config to hash new accounts


## [1.4.0] - 2025-02-11

### Added

- Implemented support for private slots and room passwords
- Added hint for game name in game tracker
- Added a cache to game tracker
- Added support for IPv6 for signaling
- Added support for groups
- Added a command to reset client's state without disconnecting

### Changed

- Removed target from logging
- Stopped printing error if connection stops because of TCP connection termination
- Got rid of unnecessary Arcs around atomics in game tracker
- Cleaned up logging

### Fixed

- Signaling information is now given out to the person joining a room in the room response and in the notification that a user has joined instead of separately
- Fixed Presence not being announced to friends when first set

### Misc

- Updated dependencies to all latest major versions


## [1.3.0] - 2024-09-19

### Added

- Added a configuration setting letting you set admins rights for specific usernames(set on launch or at account creation)
- Added commands to support old GUI API (CreateRoomGUI, JoinRoomGUI, LeaveRoomGUI, GetRoomListGUI, SetRoomSearchFlagGUI, GetRoomSearchFlagGUI, SetRoomInfoGUI, GetRoomInfoGUI, QuickMatchGUI, SearchJoinRoomGUI)

### Changed

- Changed the default ranking limit(number of scores ranked per table) from 100 to 250
- Ticket issuer ID was changed to 0x100(from 0x33333333) as Tony Hawk: SHRED was found to be checking this value

### Fixed

- Allow all messages but invites to be sent to non-friends
- Fixed score cache last insertion/update time not being updated on insert
- Fixed SceNpCommunicationId to reflect the sub_id
- Fixed startIndex in Matching2 search requests not being interpreted and being set wrong in the reply
- Fixed user_rooms not being appropriately updated when removed from the room forcefully(ie room destroyed)
- Fixed SCE_NP_MATCHING2_ROOMMEMBER_FLAG_ATTR_OWNER not being set for the member becoming the new owner if the succession didn't go through the list
- Ninetime: added servers for all Arcsys games so they should work now(BBCT, BBCSE, BBCPE, BBCF, P4A, P4AU, UNIEL) and for a few other games(Outrun Online Arcade, SCV, KOF13)
- CookiePLMonster: Fixed scoreboards ordering/rules for RR7, Crazy Taxi, GTHD, Daytona USA, Wrecked, Hotline Miami 2

### Misc

- Updated dependencies


## [1.2.4] - 2024-03-23

### Fixed

- Fixed delete TUS data queries


## [1.2.3] - 2024-03-20

### Fixed

- Fixed some TUS queries

### Misc

- Updated dependencies


## [1.2.2] - 2024-03-11

### Fixed

- Fixed some TUS queries

### Misc

- Updated dependencies


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