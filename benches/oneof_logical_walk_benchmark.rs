//! Benchmarks the cost of running a no-op DataFusion logical-plan tree
//! rewriter over a [`OneOf`] node with varying numbers of candidate branches.
//!
//! Background: each post-ViewMatcher logical optimizer pass calls into
//! `rewrite_extension_inputs`, which iterates `node.inputs()`, clones each
//! child, and recursively rewrites it. With `inputs()` returning every
//! branch, this is O(num_branches × subtree_size) work per optimizer pass.
//!
//! After the change in this commit, `OneOf::inputs()` returns only the
//! primary branch, so the same rewrite is O(subtree_size) regardless of
//! the candidate count. This benchmark verifies that.
//!
//! Run with:
//! ```ignore
//! cargo bench --bench oneof_logical_walk_benchmark
//! ```
use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion_common::Result;
use datafusion_expr::{col, lit, Extension, LogicalPlan, LogicalPlanBuilder};
use datafusion_materialized_views::rewrite::exploitation::OneOf;

/// Build a non-trivial logical plan to use as a `OneOf` branch:
/// `Projection(Filter(Filter(Values))).`
fn build_branch(seed: i32) -> LogicalPlan {
    LogicalPlanBuilder::values(vec![vec![
        lit(seed),
        lit(seed.wrapping_mul(2)),
        lit("foo".to_string()),
    ]])
    .unwrap()
    .filter(col("column2").gt(lit(0)))
    .unwrap()
    .filter(col("column1").gt(lit(seed)))
    .unwrap()
    .project(vec![col("column1"), col("column3")])
    .unwrap()
    .build()
    .unwrap()
}

/// Construct `Filter(OneOf(branches))`. The outer `Filter` ensures the
/// rewriter walks into the extension node via `rewrite_extension_inputs`.
fn build_one_of_plan(num_branches: usize) -> LogicalPlan {
    let branches = (0..num_branches)
        .map(|i| build_branch(i as i32))
        .collect::<Vec<_>>();
    let one_of = OneOf::new(branches);
    let extension_plan = LogicalPlan::Extension(Extension {
        node: Arc::new(one_of),
    });
    LogicalPlanBuilder::from(extension_plan)
        .filter(col("column1").gt(lit(0)))
        .unwrap()
        .build()
        .unwrap()
}

/// A rewriter that performs no transformation; it merely walks the tree so
/// `rewrite_extension_inputs` is invoked for every Extension node.
struct NoOpRewriter;

impl TreeNodeRewriter for NoOpRewriter {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
        Ok(Transformed::no(node))
    }
}

fn bench_walk(c: &mut Criterion) {
    let mut group = c.benchmark_group("oneof_logical_walk_noop_rewrite");
    for num_branches in [1usize, 3, 5, 10] {
        let plan = build_one_of_plan(num_branches);
        group.bench_with_input(
            BenchmarkId::from_parameter(num_branches),
            &plan,
            |b, plan| {
                b.iter(|| {
                    let mut rewriter = NoOpRewriter;
                    let result = plan.clone().rewrite(&mut rewriter).unwrap();
                    black_box(result);
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_walk);
criterion_main!(benches);
