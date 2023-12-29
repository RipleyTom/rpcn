# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

<!---
## [Unreleased]
-->

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