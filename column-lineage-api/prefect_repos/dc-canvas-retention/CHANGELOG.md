# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.4] - 2025-11-03
### Added
- Clean up orphaned views from deleted canvases immediately (is_deleted='T' with live views, no age check)

## [0.4.3] - 2025-10-08
### Fixed
- Handle canvases with empty view definition

## [0.4.2] - 2025-10-08
### Fixed
- Include canvases without data view in query_old_canvases

## [0.4.1] - 2025-09-23
### Fixed
- Remove force-reinstall flag

## [0.4.0] - 2025-09-01

### Added
- Flows versioning in Prefect deployment configurations
- Dist cleanup after deployment in README.md
- Version extraction using importlib.metadata

### Changed
- Updated deployment scripts to include version and cleanup steps

### Technical Details
- Added version echo step to Prefect deployment configuration
- Implemented consistent versioning across dev and prod environments
- Soft deactivation functionality for canvases exceeding cumulative count threshold
- New `row_count_threshold` parameter (required) to main flow and deployment configs
- `soft_deactivate_canvases.py` module with soft deactivation logic
- SQL queries for identifying and soft deactivating canvases by count in `queries.py`:
  - `get_soft_deactivation_candidates()` - Gets canvas_ids exceeding threshold
  - `query_soft_deactivation_candidates()` - SQL query with cumulative counting logic
  - `make_soft_deactivate_canvas_stmt()` - Updates canvas enabled=FALSE
- Comprehensive unit tests for soft deactivation functionality
- Integration test for soft deactivation behavior validating enabled flag effects on view row counts
- Updated flow to three-stage process: deactivate → delete → soft deactivate

### Changed
- Enhanced main flow to include soft deactivation as final stage
- Updated project documentation to reflect three-stage process
- Modified Settings class to require row_count_threshold parameter, main flow has default (1.75B)
- Updated deployment YAML configs (dev/prod) to include new parameter
- Simplified soft deactivation query to use only dc_canvas_hdr table (removed information_schema join)
- Optimized soft deactivation to update all canvases in single batch SQL statement instead of individual updates
- All DC_CANVAS_HDR update statements now include updated_by and update_dtm tracking

### Fixed
- Fixed view deactivation logic to append `AND 1=0` instead of `WHERE 1=0` to handle views with existing WHERE clauses
- Canvas deactivation now generates valid SQL when views contain `WHERE ch.enabled = TRUE` clauses
- Updated soft deactivation query to use `SUM(rowcount)` instead of `SUM(1)` for proper cumulative row counting

### Technical Details
- Soft deactivation sets `enabled = FALSE` on canvases without modifying DDL or names
- Uses cumulative row count (SUM of DC_CANVAS_HDR.rowcount) ordered by canvas_id DESC to identify candidates exceeding threshold
- Runs as final stage after deactivation and deletion to clean up remaining active canvases
- Integrates with existing database transaction management and logging patterns
- View deactivation appends `AND 1=0` to ensure 0 rows while maintaining valid SQL syntax
- Soft deactivation processes all canvas_ids in single batch UPDATE statement with IN clause for improved performance
- Enhanced logging shows canvas_ids list before processing and rowcount after batch update
- All canvas updates track modification timestamp (update_dtm) and user (updated_by = 'dc-canvas-retention')