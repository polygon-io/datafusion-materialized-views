use std::sync::Arc;
use std::time::Duration;

use arrow::datatypes::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::catalog::memory::{DataSourceExec, MemorySourceConfig};
use datafusion::config::ConfigOptions;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_optimizer::enforce_distribution::EnforceDistribution;
use datafusion::physical_optimizer::enforce_sorting::EnforceSorting;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_materialized_views::rewrite::exploitation::{
    CostFn, OneOfExec, PruneCandidates, RewriteContext,
};

fn sort_options_asc() -> arrow::compute::SortOptions {
    arrow::compute::SortOptions {
        descending: false,
        nulls_first: false,
    }
}

/// Build a wide schema with `num_cols` columns.
fn build_schema(num_cols: usize) -> Arc<Schema> {
    let fields: Vec<Field> = (0..num_cols)
        .map(|i| Field::new(format!("col_{i}"), DataType::Utf8, true))
        .collect();
    Arc::new(Schema::new(fields))
}

/// Build a MemoryExec candidate with the given schema and sort order on col_0.
fn build_candidate(schema: &Arc<Schema>, sorted: bool) -> Arc<dyn ExecutionPlan> {
    let batch = arrow::array::RecordBatch::try_new(
        Arc::clone(schema),
        (0..schema.fields().len())
            .map(|_| {
                Arc::new(arrow::array::StringArray::from(vec!["a", "b", "c"]))
                    as arrow::array::ArrayRef
            })
            .collect(),
    )
    .unwrap();

    let mut config =
        MemorySourceConfig::try_new(&[vec![batch]], Arc::clone(schema), None).unwrap();

    if sorted {
        let sort_expr = LexOrdering::new(vec![PhysicalSortExpr::new(
            datafusion::physical_expr::expressions::col("col_0", schema).unwrap(),
            sort_options_asc(),
        )])
        .unwrap();
        config = config.try_with_sort_information(vec![sort_expr]).unwrap();
    }

    Arc::new(DataSourceExec::new(Arc::new(config)))
}

/// Build a plan: OneOfExec(num_candidates candidates)
fn build_plan(num_candidates: usize, num_cols: usize) -> Arc<dyn ExecutionPlan> {
    let schema = build_schema(num_cols);

    let candidates: Vec<Arc<dyn ExecutionPlan>> = (0..num_candidates)
        .map(|i| build_candidate(&schema, i == 0))
        .collect();

    // Cost function: candidate 0 is always cheapest
    let cost_fn: CostFn = Arc::new(|ctx| {
        ctx.into_candidate_plans()
            .enumerate()
            .map(|(i, _)| if i == 0 { 1.0 } else { 100.0 + i as f64 })
            .collect()
    });

    Arc::new(
        OneOfExec::try_new(candidates, None, cost_fn, RewriteContext::default()).unwrap(),
    )
}

fn run_optimizer_chain(plan: &Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    let config = ConfigOptions::default();

    // Simulate the atlas optimizer chain rules that run before PruneCandidates.
    // These are the expensive rules that traverse all OneOfExec children.
    let rules: Vec<Box<dyn PhysicalOptimizerRule>> = vec![
        Box::new(EnforceDistribution {}),
        Box::new(EnforceSorting {}),
        Box::new(EnforceDistribution {}),
        Box::new(EnforceSorting {}),
        Box::new(PruneCandidates),
    ];

    let mut current = Arc::clone(plan);
    for rule in &rules {
        current = rule.optimize(current, &config).unwrap();
    }
    current
}

fn benchmark_oneofexec_optimizer(c: &mut Criterion) {
    let mut group = c.benchmark_group("oneofexec_optimizer");
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(5));
    group.sample_size(50);

    let num_cols = 40; // tickers-like wide table

    for num_candidates in [1, 3, 5, 10] {
        let plan = build_plan(num_candidates, num_cols);

        group.bench_with_input(
            BenchmarkId::new("candidates", num_candidates),
            &plan,
            |b, plan| {
                b.iter(|| {
                    let result = run_optimizer_chain(plan);
                    std::hint::black_box(result);
                });
            },
        );
    }

    // Also benchmark varying column count with 5 candidates
    for num_cols in [10, 20, 40, 80] {
        let plan = build_plan(5, num_cols);

        group.bench_with_input(
            BenchmarkId::new("cols_5candidates", num_cols),
            &plan,
            |b, plan| {
                b.iter(|| {
                    let result = run_optimizer_chain(plan);
                    std::hint::black_box(result);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, benchmark_oneofexec_optimizer);
criterion_main!(benches);
