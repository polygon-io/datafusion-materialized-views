// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#![deny(missing_docs)]

//! `datafusion-materialized-views` implements algorithms and functionality for materialized views in DataFusion.

/// Code for incremental view maintenance against Hive-partitioned tables.
///
/// An example of a Hive-partitioned table is the [`ListingTable`](datafusion::datasource::listing::ListingTable).
/// By analyzing the fragment of the materialized view query pertaining to the partition columns,
/// we can derive a build graph that relates the files of a materialized views and the files of the tables it depends on.
///
/// Two central traits are defined:
///
/// * [`ListingTableLike`](materialized::ListingTableLike): a trait that abstracts Hive-partitioned tables in object storage;
/// * [`Materialized`](materialized::Materialized): a materialized `ListingTableLike` defined by a user-provided query.
///
/// Note that all implementations of `ListingTableLike` and `Materialized` must be registered using the
/// [`register_listing_table`](materialized::register_listing_table) and
/// [`register_materialized`](materialized::register_materialized) functions respectively,
/// otherwise the tables may not be detected by the incremental view maintenance code,
/// including components such as [`FileMetadata`](materialized::file_metadata::FileMetadata),
/// [`RowMetadataRegistry`](materialized::row_metadata::RowMetadataRegistry), or the
/// [`mv_dependencies`](materialized::dependencies::mv_dependencies) UDTF.
///
/// By default, `ListingTableLike` is implemented for [`ListingTable`](datafusion::datasource::listing::ListingTable),
pub mod materialized;

/// An implementation of Query Rewriting, an optimization that rewrites queries to make use of materialized views.
pub mod rewrite;

/// Configuration options for materialized view related features.
///
/// # Materialized View Configuration
///
/// Query rewriting uses two configuration options that work together:
///
/// 1. **`use_in_query_rewrite` (on candidate MVs)**: Controls whether an MV is globally available
///    for query rewriting. MVs with `use_in_query_rewrite = false` are excluded from the
///    candidate pool entirely.
///
/// 2. **`rewrite_targets` (on queried tables)**: When querying a table, this field filters which
///    MVs from the available pool should be considered as rewrite candidates for that specific table.
///
/// The interaction works as follows:
/// - First, `use_in_query_rewrite` determines the global pool of available MVs
/// - Then, `rewrite_targets` on the queried table filters that pool for that specific query
/// - An MV must have `use_in_query_rewrite = true` **and** be in the `rewrite_targets` list
///   (or the list must be None) to be considered
///
/// # Example
///
/// ```ignore
/// // MV1: available for query rewriting
/// let mv1_config = MaterializedConfig {
///     use_in_query_rewrite: true,  // MV1 is in the global pool
///     rewrite_targets: None,
/// };
///
/// // MV2: not available for query rewriting
/// let mv2_config = MaterializedConfig {
///     use_in_query_rewrite: false, // MV2 is excluded from the pool
///     rewrite_targets: None,
/// };
///
/// // Base table: only considers MV1 for rewrites
/// let base_config = MaterializedConfig {
///     use_in_query_rewrite: true,
///     rewrite_targets: Some(vec!["mv1".to_string()]), // Only MV1 is considered
/// };
/// // When querying base_table:
/// // - MV1 will be considered (in pool + in targets list)
/// // - MV2 will NOT be considered (not in pool, even if added to targets list)
/// ```
#[derive(Debug, Clone)]
pub struct MaterializedConfig {
    /// Whether or not this materialized view is available for query rewriting.
    ///
    /// If `false`, this MV will not be loaded into the query rewrite engine and cannot be used
    /// as a rewrite candidate, regardless of any `rewrite_targets` settings on other tables.
    pub use_in_query_rewrite: bool,

    /// Optional candidate materialized views for query rewriting.
    ///
    /// When this table is queried, only the MVs listed here will be considered as rewrite candidates.
    /// These should be full table names (e.g., `atlas.us_stocks_sip.trades_by_ticker`).
    ///
    /// - If `None` (default): all eligible MVs in the catalog (where `use_in_query_rewrite = true`)
    ///   are considered as rewrite candidates
    /// - If `Some(vec![])`: no MVs are considered (effectively disables query rewriting for this table)
    /// - If `Some(vec!["mv1", "mv2"])`: only mv1 and mv2 (if they have `use_in_query_rewrite = true`)
    ///   are considered as rewrite candidates
    ///
    /// Note: This field is typically set on the **queried table** (which may itself be an MV).
    /// It acts as a whitelist that further filters the pool of available MVs for queries against this table.
    pub rewrite_targets: Option<Vec<String>>,
}

impl Default for MaterializedConfig {
    fn default() -> Self {
        Self {
            use_in_query_rewrite: true,
            rewrite_targets: None,
        }
    }
}
