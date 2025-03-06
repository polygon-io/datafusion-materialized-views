# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.2](https://github.com/polygon-io/datafusion-materialized-views/compare/v0.1.1...v0.1.2) - 2025-03-06

### Added
- `Decorator` trait ([#26](https://github.com/polygon-io/datafusion-materialized-views/pull/26)) (by @suremarc) - #26

### Other
- update datafusion fork rev (by @suremarc)
- update deps to use arrow/datafusion forks (by @suremarc)
- Add alternate analysis for MVs with no partition columns ([#39](https://github.com/polygon-io/datafusion-materialized-views/pull/39)) (by @suremarc) - #39
- upgrade to datafusion 45 ([#38](https://github.com/polygon-io/datafusion-materialized-views/pull/38)) (by @suremarc) - #38
- use nanosecond timestamps in file metadata ([#28](https://github.com/polygon-io/datafusion-materialized-views/pull/28)) (by @suremarc) - #28

### Contributors

* @suremarc

## [0.1.1](https://github.com/datafusion-contrib/datafusion-materialized-views/compare/v0.1.0...v0.1.1) - 2025-01-07

### Added
- view exploitation (#19) (by @suremarc) - #19
- SPJ normal form (#18) (by @suremarc) - #18

### Other
- add constructor for RowMetadataRegistry from FileMetadata ([#25](https://github.com/datafusion-contrib/datafusion-materialized-views/pull/25)) (by @suremarc) - #25
- add changelog manually ([#14](https://github.com/datafusion-contrib/datafusion-materialized-views/pull/14)) (by @suremarc) - #14
- don't use paths-ignore ([#15](https://github.com/datafusion-contrib/datafusion-materialized-views/pull/15)) (by @suremarc) - #15

### Contributors

* @suremarc

## [0.1.0](https://github.com/datafusion-contrib/datafusion-materialized-views/releases/tag/v0.1.0) - 2025-01-03

### Other
- some api improvements + remove manual changelog ([#12](https://github.com/datafusion-contrib/datafusion-materialized-views/pull/12)) (by @suremarc) - #12
- Integration test ([#10](https://github.com/datafusion-contrib/datafusion-materialized-views/pull/10)) (by @suremarc) - #10
- setup changelog ([#9](https://github.com/datafusion-contrib/datafusion-materialized-views/pull/9)) (by @suremarc) - #9
- Release plz ([#7](https://github.com/datafusion-contrib/datafusion-materialized-views/pull/7)) (by @suremarc) - #7
- stale_files + rename to mv_dependencies ([#6](https://github.com/datafusion-contrib/datafusion-materialized-views/pull/6)) (by @suremarc) - #6
- Incremental view maintenance ([#3](https://github.com/datafusion-contrib/datafusion-materialized-views/pull/3)) (by @suremarc) - #3
- Add `FileMetadata` table and `RowMetadataRegistry` ([#2](https://github.com/datafusion-contrib/datafusion-materialized-views/pull/2)) (by @suremarc) - #2
- Setup cargo + CI ([#1](https://github.com/datafusion-contrib/datafusion-materialized-views/pull/1)) (by @suremarc)
- Initial commit (by @matthewmturner)

### Contributors

* @suremarc
* @matthewmturner
