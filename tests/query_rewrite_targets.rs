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
//   Unless required by applicable law or agreed to in writing,
//   software distributed under the License is distributed on an
//   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//   KIND, either express or implied.  See the License for the
//   specific language governing permissions and limitations
//   under the License.

//! Tests for the query rewrite targets feature
//!
//! This tests the two-stage filtering for query rewriting:
//! 1. First stage: `use_in_query_rewrite` filters the global pool of candidate MVs
//! 2. Second stage: `rewrite_targets` filters candidates for specific queries

use std::any::Any;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_common::Result;
use datafusion_expr::{Expr, LogicalPlan, TableType};
use datafusion_materialized_views::materialized::{
    cast_to_materialized, register_materialized, ListingTableLike, Materialized,
};
use datafusion_materialized_views::rewrite::exploitation::ViewMatcher;
use datafusion_materialized_views::MaterializedConfig;
use datafusion_sql::TableReference;

/// A mock materialized view for testing rewrite targets
#[derive(Debug)]
struct MockMaterializedView {
    table_path: ListingTableUrl,
    partition_columns: Vec<String>,
    query: LogicalPlan,
    file_ext: &'static str,
    config: MaterializedConfig,
}

#[async_trait::async_trait]
impl TableProvider for MockMaterializedView {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(self.query.schema().as_arrow().clone())
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }
}

impl ListingTableLike for MockMaterializedView {
    fn table_paths(&self) -> Vec<ListingTableUrl> {
        vec![self.table_path.clone()]
    }

    fn partition_columns(&self) -> Vec<String> {
        self.partition_columns.clone()
    }

    fn file_ext(&self) -> String {
        self.file_ext.to_string()
    }
}

impl Materialized for MockMaterializedView {
    fn query(&self) -> LogicalPlan {
        self.query.clone()
    }

    fn config(&self) -> MaterializedConfig {
        self.config.clone()
    }
}

async fn setup() -> Result<SessionContext> {
    let _ = env_logger::builder().is_test(true).try_init();

    register_materialized::<MockMaterializedView>();

    let ctx = SessionContext::new();

    // Create a base table
    ctx.sql("CREATE TABLE base_table (id INT, value INT)")
        .await?
        .collect()
        .await?;

    Ok(ctx)
}

/// Helper to create a fully qualified table reference
/// DataFusion creates tables in the default datafusion.public schema
fn table_ref(name: &str) -> TableReference {
    TableReference::full("datafusion", "public", name)
}

#[tokio::test]
async fn test_use_in_query_rewrite_filters_global_pool() -> Result<()> {
    // Test first stage filtering: use_in_query_rewrite filters the global pool
    let ctx = setup().await?;

    let mv1_query = ctx
        .sql("SELECT id, value FROM base_table WHERE value > 0")
        .await?
        .into_optimized_plan()?;

    let mv2_query = ctx
        .sql("SELECT id, value FROM base_table WHERE value > 10")
        .await?
        .into_optimized_plan()?;

    // MV1: available for rewriting
    let mv1 = Arc::new(MockMaterializedView {
        table_path: ListingTableUrl::parse("file:///mv1/")?,
        partition_columns: vec![],
        query: mv1_query,
        file_ext: ".parquet",
        config: MaterializedConfig {
            use_in_query_rewrite: true,
            rewrite_targets: None,
        },
    });

    // MV2: NOT available for rewriting (filtered out in first stage)
    let mv2 = Arc::new(MockMaterializedView {
        table_path: ListingTableUrl::parse("file:///mv2/")?,
        partition_columns: vec![],
        query: mv2_query,
        file_ext: ".parquet",
        config: MaterializedConfig {
            use_in_query_rewrite: false, // Excluded from global pool
            rewrite_targets: None,
        },
    });

    ctx.register_table("mv1", mv1 as Arc<dyn TableProvider>)?;
    ctx.register_table("mv2", mv2 as Arc<dyn TableProvider>)?;

    // Create ViewMatcher - this applies first stage filtering
    let view_matcher = ViewMatcher::try_new_from_state(&ctx.state()).await?;
    let mv_plans = view_matcher.mv_plans();

    // Only MV1 should be in the global pool
    assert_eq!(
        mv_plans.len(),
        1,
        "Expected only 1 MV (mv1) in the pool; mv2 should be excluded by use_in_query_rewrite = false"
    );

    assert!(
        mv_plans.contains_key(&table_ref("mv1")),
        "mv1 should be in the global pool"
    );
    assert!(
        !mv_plans.contains_key(&table_ref("mv2")),
        "mv2 should NOT be in the pool (use_in_query_rewrite = false)"
    );

    Ok(())
}

#[tokio::test]
async fn test_rewrite_targets_none_uses_all_available_mvs() -> Result<()> {
    // When rewrite_targets = None, all MVs in the pool are considered
    let ctx = setup().await?;

    let base_mv_query = ctx
        .sql("SELECT id, value FROM base_table")
        .await?
        .into_optimized_plan()?;

    let mv1_query = ctx
        .sql("SELECT id, value FROM base_table WHERE value > 0")
        .await?
        .into_optimized_plan()?;

    let mv2_query = ctx
        .sql("SELECT id, value FROM base_table WHERE value > 10")
        .await?
        .into_optimized_plan()?;

    // Base MV with rewrite_targets = None
    let base_mv = Arc::new(MockMaterializedView {
        table_path: ListingTableUrl::parse("file:///base_mv/")?,
        partition_columns: vec![],
        query: base_mv_query,
        file_ext: ".parquet",
        config: MaterializedConfig {
            use_in_query_rewrite: true,
            rewrite_targets: None, // All MVs from pool are considered
        },
    });

    let mv1 = Arc::new(MockMaterializedView {
        table_path: ListingTableUrl::parse("file:///mv1/")?,
        partition_columns: vec![],
        query: mv1_query,
        file_ext: ".parquet",
        config: MaterializedConfig {
            use_in_query_rewrite: true,
            rewrite_targets: None,
        },
    });

    let mv2 = Arc::new(MockMaterializedView {
        table_path: ListingTableUrl::parse("file:///mv2/")?,
        partition_columns: vec![],
        query: mv2_query,
        file_ext: ".parquet",
        config: MaterializedConfig {
            use_in_query_rewrite: true,
            rewrite_targets: None,
        },
    });

    ctx.register_table("base_mv", base_mv as Arc<dyn TableProvider>)?;
    ctx.register_table("mv1", mv1 as Arc<dyn TableProvider>)?;
    ctx.register_table("mv2", mv2 as Arc<dyn TableProvider>)?;

    let view_matcher = ViewMatcher::try_new_from_state(&ctx.state()).await?;

    // Get rewrite candidates for base_mv
    let candidates = view_matcher.get_rewrite_candidates_for_table(&table_ref("base_mv"))?;

    // Should include all 3 MVs (base_mv itself, mv1, and mv2)
    assert_eq!(
        candidates.len(),
        3,
        "Expected all 3 MVs to be candidates when rewrite_targets = None"
    );
    assert!(candidates.contains(&table_ref("base_mv")));
    assert!(candidates.contains(&table_ref("mv1")));
    assert!(candidates.contains(&table_ref("mv2")));

    Ok(())
}

#[tokio::test]
async fn test_rewrite_targets_empty_list() -> Result<()> {
    // When rewrite_targets = Some(vec![]), no MVs are considered
    let ctx = setup().await?;

    let base_mv_query = ctx
        .sql("SELECT id, value FROM base_table")
        .await?
        .into_optimized_plan()?;

    let mv1_query = ctx
        .sql("SELECT id, value FROM base_table WHERE value > 0")
        .await?
        .into_optimized_plan()?;

    // Base MV with empty rewrite_targets
    let base_mv = Arc::new(MockMaterializedView {
        table_path: ListingTableUrl::parse("file:///base_mv/")?,
        partition_columns: vec![],
        query: base_mv_query,
        file_ext: ".parquet",
        config: MaterializedConfig {
            use_in_query_rewrite: true,
            rewrite_targets: Some(vec![]), // No MVs considered
        },
    });

    let mv1 = Arc::new(MockMaterializedView {
        table_path: ListingTableUrl::parse("file:///mv1/")?,
        partition_columns: vec![],
        query: mv1_query,
        file_ext: ".parquet",
        config: MaterializedConfig {
            use_in_query_rewrite: true,
            rewrite_targets: None,
        },
    });

    ctx.register_table("base_mv", base_mv as Arc<dyn TableProvider>)?;
    ctx.register_table("mv1", mv1 as Arc<dyn TableProvider>)?;

    let view_matcher = ViewMatcher::try_new_from_state(&ctx.state()).await?;

    // Get rewrite candidates for base_mv
    let candidates = view_matcher.get_rewrite_candidates_for_table(&table_ref("base_mv"))?;

    // Should be empty because rewrite_targets = Some(vec![])
    assert_eq!(
        candidates.len(),
        0,
        "Expected no candidates when rewrite_targets = Some(vec![])"
    );

    Ok(())
}

#[tokio::test]
async fn test_rewrite_targets_filters_to_specific_mvs() -> Result<()> {
    // When rewrite_targets specifies MVs, only those are considered
    let ctx = setup().await?;

    let base_mv_query = ctx
        .sql("SELECT id, value FROM base_table")
        .await?
        .into_optimized_plan()?;

    let mv1_query = ctx
        .sql("SELECT id, value FROM base_table WHERE value > 0")
        .await?
        .into_optimized_plan()?;

    let mv2_query = ctx
        .sql("SELECT id, value FROM base_table WHERE value > 10")
        .await?
        .into_optimized_plan()?;

    let mv3_query = ctx
        .sql("SELECT id, value FROM base_table WHERE value > 20")
        .await?
        .into_optimized_plan()?;

    // Base MV that only wants mv1 and mv3
    // Use fully qualified names in rewrite_targets
    let base_mv = Arc::new(MockMaterializedView {
        table_path: ListingTableUrl::parse("file:///base_mv/")?,
        partition_columns: vec![],
        query: base_mv_query,
        file_ext: ".parquet",
        config: MaterializedConfig {
            use_in_query_rewrite: true,
            rewrite_targets: Some(vec![
                "datafusion.public.mv1".to_string(),
                "datafusion.public.mv3".to_string(),
            ]),
        },
    });

    let mv1 = Arc::new(MockMaterializedView {
        table_path: ListingTableUrl::parse("file:///mv1/")?,
        partition_columns: vec![],
        query: mv1_query,
        file_ext: ".parquet",
        config: MaterializedConfig {
            use_in_query_rewrite: true,
            rewrite_targets: None,
        },
    });

    let mv2 = Arc::new(MockMaterializedView {
        table_path: ListingTableUrl::parse("file:///mv2/")?,
        partition_columns: vec![],
        query: mv2_query,
        file_ext: ".parquet",
        config: MaterializedConfig {
            use_in_query_rewrite: true,
            rewrite_targets: None,
        },
    });

    let mv3 = Arc::new(MockMaterializedView {
        table_path: ListingTableUrl::parse("file:///mv3/")?,
        partition_columns: vec![],
        query: mv3_query,
        file_ext: ".parquet",
        config: MaterializedConfig {
            use_in_query_rewrite: true,
            rewrite_targets: None,
        },
    });

    ctx.register_table("base_mv", base_mv as Arc<dyn TableProvider>)?;
    ctx.register_table("mv1", mv1 as Arc<dyn TableProvider>)?;
    ctx.register_table("mv2", mv2 as Arc<dyn TableProvider>)?;
    ctx.register_table("mv3", mv3 as Arc<dyn TableProvider>)?;

    let view_matcher = ViewMatcher::try_new_from_state(&ctx.state()).await?;

    // Get rewrite candidates for base_mv
    let candidates = view_matcher.get_rewrite_candidates_for_table(&table_ref("base_mv"))?;

    // Should only include mv1 and mv3 (not base_mv itself, not mv2)
    assert_eq!(
        candidates.len(),
        2,
        "Expected only mv1 and mv3 as candidates"
    );
    assert!(candidates.contains(&table_ref("mv1")));
    assert!(candidates.contains(&table_ref("mv3")));
    assert!(
        !candidates.contains(&table_ref("mv2")),
        "mv2 should not be included"
    );
    assert!(
        !candidates.contains(&table_ref("base_mv")),
        "base_mv should not include itself"
    );

    Ok(())
}

#[tokio::test]
async fn test_excluded_mv_not_candidate_even_if_in_targets() -> Result<()> {
    // Test that an MV with use_in_query_rewrite = false is not a candidate,
    // even if listed in rewrite_targets
    let ctx = setup().await?;

    let base_mv_query = ctx
        .sql("SELECT id, value FROM base_table")
        .await?
        .into_optimized_plan()?;

    let mv1_query = ctx
        .sql("SELECT id, value FROM base_table WHERE value > 0")
        .await?
        .into_optimized_plan()?;

    let mv2_query = ctx
        .sql("SELECT id, value FROM base_table WHERE value > 10")
        .await?
        .into_optimized_plan()?;

    // Base MV that lists both mv1 and mv2 in rewrite_targets
    let base_mv = Arc::new(MockMaterializedView {
        table_path: ListingTableUrl::parse("file:///base_mv/")?,
        partition_columns: vec![],
        query: base_mv_query,
        file_ext: ".parquet",
        config: MaterializedConfig {
            use_in_query_rewrite: true,
            rewrite_targets: Some(vec![
                "datafusion.public.mv1".to_string(),
                "datafusion.public.mv2".to_string(),
            ]),
        },
    });

    // MV1: in global pool
    let mv1 = Arc::new(MockMaterializedView {
        table_path: ListingTableUrl::parse("file:///mv1/")?,
        partition_columns: vec![],
        query: mv1_query,
        file_ext: ".parquet",
        config: MaterializedConfig {
            use_in_query_rewrite: true,
            rewrite_targets: None,
        },
    });

    // MV2: NOT in global pool (excluded by use_in_query_rewrite = false)
    let mv2 = Arc::new(MockMaterializedView {
        table_path: ListingTableUrl::parse("file:///mv2/")?,
        partition_columns: vec![],
        query: mv2_query,
        file_ext: ".parquet",
        config: MaterializedConfig {
            use_in_query_rewrite: false, // Excluded from global pool
            rewrite_targets: None,
        },
    });

    ctx.register_table("base_mv", base_mv as Arc<dyn TableProvider>)?;
    ctx.register_table("mv1", mv1 as Arc<dyn TableProvider>)?;
    ctx.register_table("mv2", mv2 as Arc<dyn TableProvider>)?;

    let view_matcher = ViewMatcher::try_new_from_state(&ctx.state()).await?;

    // Verify mv2 is not in the global pool
    let mv_plans = view_matcher.mv_plans();
    assert!(!mv_plans.contains_key(&table_ref("mv2")));

    // Get rewrite candidates for base_mv
    let candidates = view_matcher.get_rewrite_candidates_for_table(&table_ref("base_mv"))?;

    // Should only include mv1 (mv2 is excluded from pool)
    assert_eq!(
        candidates.len(),
        1,
        "Expected only mv1 as candidate; mv2 should be excluded even though it's in rewrite_targets"
    );
    assert!(candidates.contains(&table_ref("mv1")));
    assert!(
        !candidates.contains(&table_ref("mv2")),
        "mv2 should not be a candidate (use_in_query_rewrite = false)"
    );

    Ok(())
}

#[tokio::test]
async fn test_config_default_values() -> Result<()> {
    // Test default configuration values
    let default_config = MaterializedConfig::default();

    assert!(
        default_config.use_in_query_rewrite,
        "Default use_in_query_rewrite should be true"
    );
    assert_eq!(
        default_config.rewrite_targets, None,
        "Default rewrite_targets should be None (consider all available MVs)"
    );

    Ok(())
}

#[tokio::test]
async fn test_different_tables_different_rewrite_targets() -> Result<()> {
    // Test that different tables can have different rewrite_targets
    let ctx = setup().await?;

    let table1_query = ctx
        .sql("SELECT id, value FROM base_table WHERE value > 0")
        .await?
        .into_optimized_plan()?;

    let table2_query = ctx
        .sql("SELECT id, value FROM base_table WHERE value > 10")
        .await?
        .into_optimized_plan()?;

    let mv1_query = ctx
        .sql("SELECT id, value FROM base_table WHERE value > 5")
        .await?
        .into_optimized_plan()?;

    let mv2_query = ctx
        .sql("SELECT id, value FROM base_table WHERE value > 15")
        .await?
        .into_optimized_plan()?;

    // Table1: only uses mv1
    let table1 = Arc::new(MockMaterializedView {
        table_path: ListingTableUrl::parse("file:///table1/")?,
        partition_columns: vec![],
        query: table1_query,
        file_ext: ".parquet",
        config: MaterializedConfig {
            use_in_query_rewrite: true,
            rewrite_targets: Some(vec!["datafusion.public.mv1".to_string()]),
        },
    });

    // Table2: only uses mv2
    let table2 = Arc::new(MockMaterializedView {
        table_path: ListingTableUrl::parse("file:///table2/")?,
        partition_columns: vec![],
        query: table2_query,
        file_ext: ".parquet",
        config: MaterializedConfig {
            use_in_query_rewrite: true,
            rewrite_targets: Some(vec!["datafusion.public.mv2".to_string()]),
        },
    });

    let mv1 = Arc::new(MockMaterializedView {
        table_path: ListingTableUrl::parse("file:///mv1/")?,
        partition_columns: vec![],
        query: mv1_query,
        file_ext: ".parquet",
        config: MaterializedConfig {
            use_in_query_rewrite: true,
            rewrite_targets: None,
        },
    });

    let mv2 = Arc::new(MockMaterializedView {
        table_path: ListingTableUrl::parse("file:///mv2/")?,
        partition_columns: vec![],
        query: mv2_query,
        file_ext: ".parquet",
        config: MaterializedConfig {
            use_in_query_rewrite: true,
            rewrite_targets: None,
        },
    });

    ctx.register_table("table1", table1 as Arc<dyn TableProvider>)?;
    ctx.register_table("table2", table2 as Arc<dyn TableProvider>)?;
    ctx.register_table("mv1", mv1 as Arc<dyn TableProvider>)?;
    ctx.register_table("mv2", mv2 as Arc<dyn TableProvider>)?;

    let view_matcher = ViewMatcher::try_new_from_state(&ctx.state()).await?;

    // Check candidates for table1
    let table1_candidates = view_matcher.get_rewrite_candidates_for_table(&table_ref("table1"))?;
    assert_eq!(table1_candidates.len(), 1);
    assert!(table1_candidates.contains(&table_ref("mv1")));

    // Check candidates for table2
    let table2_candidates = view_matcher.get_rewrite_candidates_for_table(&table_ref("table2"))?;
    assert_eq!(table2_candidates.len(), 1);
    assert!(table2_candidates.contains(&table_ref("mv2")));

    Ok(())
}

#[tokio::test]
async fn test_rewrite_targets_config_storage() -> Result<()> {
    // Test that rewrite_targets config is correctly stored and retrieved
    let ctx = setup().await?;

    let targets = vec![
        "datafusion.public.mv1".to_string(),
        "datafusion.public.mv2".to_string(),
    ];

    let mv_query = ctx
        .sql("SELECT id, value FROM base_table")
        .await?
        .into_optimized_plan()?;

    let mv = Arc::new(MockMaterializedView {
        table_path: ListingTableUrl::parse("file:///test_mv/")?,
        partition_columns: vec![],
        query: mv_query,
        file_ext: ".parquet",
        config: MaterializedConfig {
            use_in_query_rewrite: true,
            rewrite_targets: Some(targets.clone()),
        },
    });

    ctx.register_table("test_mv", mv as Arc<dyn TableProvider>)?;

    // Verify the config is properly stored and retrievable
    let mv_provider = ctx.table_provider(table_ref("test_mv")).await?;
    let mv_materialized = cast_to_materialized(mv_provider.as_ref())?.unwrap();

    assert!(mv_materialized.config().use_in_query_rewrite);
    assert_eq!(
        mv_materialized.config().rewrite_targets,
        Some(targets),
        "rewrite_targets should be stored and retrievable"
    );

    Ok(())
}
