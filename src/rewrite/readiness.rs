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

//! Readiness-related types and helpers used by the query-rewrite optimizer.
//!
//! Two-stage readiness contract:
//!
//! 1. At LP-rewrite time, `ViewMatcher` consults
//!    [`Materialized::rewrite_readiness`](crate::materialized::Materialized::rewrite_readiness)
//!    and drops candidates whose readiness is [`RewriteReadiness::NotReady`].
//!    `Ready` and `Unknown` both enter the candidate set and their raw
//!    readiness is recorded on [`CandidateMetadata::Materialized::readiness`].
//! 2. At physical-planning time (once `TableProvider::scan()` has run on every
//!    branch), `ViewExploitationPlanner` refreshes the readiness on each
//!    candidate. Providers that opt into [`ReadinessAnnotatedExec`] have
//!    their readiness read straight off the plan tree (atomically captured
//!    at scan time); others fall back to sampling the current provider
//!    state, which is best-effort but racy under concurrent snapshot swaps.
//!
//! The cost function only ever sees `Base` or `Materialized { Ready | Unknown }`
//! entries — `NotReady` is filtered by both gates before it can reach cost
//! policy.

use std::sync::Arc;

use datafusion::execution::TaskContext;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::{DataFusionError, Result, TableReference};

/// Whether a materialized view is currently safe to route queries to. Reported
/// by [`Materialized::rewrite_readiness`](crate::materialized::Materialized::rewrite_readiness)
/// and consulted by
/// [`ViewMatcher`](crate::rewrite::exploitation::ViewMatcher) during LP rewrite
/// so that unpopulated / in-flight MVs are excluded from the candidate set
/// upstream of the cost function.
///
/// Keeping this a lifecycle abstraction (rather than a proxy such as file
/// count) means the trait doesn't couple to any specific storage layout —
/// providers describe their own readiness however they want (index loaded,
/// snapshot published, migration complete, staleness threshold satisfied,
/// etc.) and only report the answer.
// `PartialOrd, Ord` are derived so `CandidateMetadata` (which stores a
// `RewriteReadiness`) can keep its own `PartialOrd, Ord` derives. The
// variant ordering has no lifecycle meaning — callers must not depend on
// `Ready < NotReady < Unknown`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RewriteReadiness {
    /// The MV is populated and can safely answer the query. The
    /// `ViewMatchingRewriter` will include it as a rewrite candidate.
    Ready,
    /// The MV should not be used yet (index not loaded, ingest task never
    /// ran after a version bump, snapshot rebuild in progress, etc.).
    /// The `ViewMatchingRewriter` will drop the MV from the candidate set
    /// so a query never gets routed to it and silently returns empty.
    NotReady,
    /// The provider cannot cheaply determine readiness. The
    /// `ViewMatchingRewriter` treats this as "include as candidate" and
    /// propagates the `Unknown` value to the cost function via
    /// `CandidateMetadata::Materialized { readiness, .. }`, so the caller
    /// can pick whatever policy fits — e.g. fall back to the base scan
    /// cost rather than trust an EmptyExec candidate as predicate-pruned.
    /// Default value returned by the trait's blanket impl so
    /// backward-compatible providers keep the pre-existing "always a
    /// candidate" behaviour.
    Unknown,
}

/// Per-branch metadata inside a `OneOf` / `OneOfExec`. One variant per branch,
/// aligned by index with the containing `branches` / `candidates` vector.
///
/// Lifecycle judgement (populated / unpopulated / stale / ...) is made in two
/// stages via [`Materialized::rewrite_readiness`](crate::materialized::Materialized::rewrite_readiness):
///
/// 1. At LP-rewrite time,
///    [`ViewMatcher`](crate::rewrite::exploitation::ViewMatcher) drops `NotReady`
///    providers and admits `Ready` + `Unknown` as candidates.
/// 2. At physical-planning time, once DataFusion has invoked each provider's
///    `scan()` (giving lazy indexes a chance to warm),
///    [`ViewExploitationPlanner`](crate::rewrite::exploitation::ViewExploitationPlanner)
///    re-consults `rewrite_readiness()` and updates the metadata to the current
///    value; providers that transitioned to `NotReady` between the two stages are
///    dropped here.
///
/// So any `Materialized` variant that reaches the cost function reflects the
/// **post-scan** readiness, and `NotReady` is guaranteed to have been filtered
/// (upstream or downstream of scan) before the cost function runs. `Ready`
/// and `Unknown` remain distinguishable so cost policy for the two can diverge
/// (typically: trust `Ready` as safe to route, and fall back to a conservative
/// estimate for `Unknown`).
///
/// The invariant enforced at construction inside
/// [`ViewMatcher`](crate::rewrite::exploitation::ViewMatcher):
///
/// * Index 0 is always [`CandidateMetadata::Base`] — the query's original LP,
///   with no MV rewrite applied. `OneOf::schema()` reads `branches[0].schema()`
///   and therefore always exposes the query's schema (not an MV's).
/// * Indices 1..N are [`CandidateMetadata::Materialized`] entries, sorted
///   deterministically by `table_ref`. Sorting on the MV's registered
///   `TableReference` (rather than on the branch LP) keeps the order stable
///   under downstream LP transformations that happen between LP rewrite and
///   physical planning — identity projection elimination, always-true filter
///   folding, etc. change the LP but never the MV's registered name, so the
///   alignment between this metadata and the physical candidate the cost
///   function receives survives every optimizer pass.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Hash)]
pub enum CandidateMetadata {
    /// The query's original branch (index 0 in every OneOf).
    Base,
    /// A materialized-view rewrite of the query. Only MVs that reported
    /// `Ready` or `Unknown` readiness reach this state; `NotReady` MVs
    /// are filtered upstream by
    /// [`ViewMatcher`](crate::rewrite::exploitation::ViewMatcher).
    /// Cost functions consult the `readiness` field to distinguish the
    /// two cases: `Unknown` (the trait default returned by
    /// [`RewriteReadiness`]) must not be silently treated as `Ready`,
    /// since that would regress providers that haven't declared a
    /// lifecycle.
    Materialized {
        /// Registered `TableReference` of the source MV. Stored directly
        /// rather than as `.to_string()` so identifiers with dots or
        /// quoting round-trip losslessly through the physical-time
        /// catalog resolution. Stable across LP transformations; safe to
        /// use as a sort key.
        table_ref: TableReference,
        /// Provider readiness as observed at physical-planning time — i.e.
        /// after DataFusion has invoked `TableProvider::scan()` on this
        /// branch. Set once at LP rewrite from the provider's initial
        /// report and refreshed from the same provider inside
        /// [`ViewExploitationPlanner::plan_extension`](crate::rewrite::exploitation::ViewExploitationPlanner)
        /// so lazy-index providers surface their warmed-up value.
        /// Downstream cost functions branch on this to implement their
        /// `Unknown` policy (e.g. fall back to the base scan cost rather
        /// than trust an EmptyExec as predicate-pruned).
        readiness: RewriteReadiness,
    },
}

/// Wraps the `ExecutionPlan` returned by a `Materialized` provider's
/// `scan()` together with the readiness value captured at scan time.
///
/// Providers use this to close the race between "which snapshot did
/// scan read from" and "what does `rewrite_readiness()` return now".
/// Sampling `rewrite_readiness()` again after `scan()` has returned is
/// racy — a concurrent snapshot swap can publish new state between the
/// two calls, letting an `EmptyExec` from the old snapshot be labelled
/// with readiness from the new one. If the provider computes both
/// atomically (from the same state Arc, under the same read lock, etc.)
/// and wraps the returned plan with `ReadinessAnnotatedExec`, the
/// physical-time refresh in
/// [`ViewExploitationPlanner::plan_extension`](crate::rewrite::exploitation::ViewExploitationPlanner)
/// reads the annotated value verbatim instead of re-sampling.
///
/// The wrapper is transparent: all `ExecutionPlan` methods delegate to
/// the inner plan, so downstream operators see the same shape as if the
/// provider returned `inner` directly.
///
/// Providers that don't wrap fall back to the pre-existing (racy)
/// sampling path in the refresh, which remains supported for backward
/// compatibility.
#[derive(Debug)]
pub struct ReadinessAnnotatedExec {
    inner: Arc<dyn ExecutionPlan>,
    readiness: RewriteReadiness,
    properties: Arc<PlanProperties>,
}

impl ReadinessAnnotatedExec {
    /// Wrap `inner` with the readiness captured atomically at scan time.
    pub fn new(inner: Arc<dyn ExecutionPlan>, readiness: RewriteReadiness) -> Self {
        let properties = Arc::clone(inner.properties());
        Self {
            inner,
            readiness,
            properties,
        }
    }

    /// Readiness captured at the moment the provider produced `inner`.
    pub fn readiness(&self) -> RewriteReadiness {
        self.readiness
    }

    /// The wrapped plan.
    pub fn inner(&self) -> &Arc<dyn ExecutionPlan> {
        &self.inner
    }
}

impl DisplayAs for ReadinessAnnotatedExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "ReadinessAnnotatedExec: readiness={:?}", self.readiness)
            }
            DisplayFormatType::TreeRender => Ok(()),
        }
    }
}

impl ExecutionPlan for ReadinessAnnotatedExec {
    fn name(&self) -> &str {
        "ReadinessAnnotatedExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(format!(
                "ReadinessAnnotatedExec expects exactly one child, got {}",
                children.len()
            )));
        }
        Ok(Arc::new(ReadinessAnnotatedExec::new(
            children.into_iter().next().unwrap(),
            self.readiness,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.inner.execute(partition, context)
    }

    fn partition_statistics(
        &self,
        partition: Option<usize>,
    ) -> Result<Arc<datafusion_common::Statistics>> {
        self.inner.partition_statistics(partition)
    }
}

/// Depth-first search for the nearest `ReadinessAnnotatedExec` in a
/// physical plan tree. Returns its readiness value, or `None` if the
/// tree doesn't contain one (e.g. the provider hasn't opted in). Used
/// by the physical-time refresh to prefer atomically-captured readiness
/// over racy re-sampling.
pub fn readiness_from_plan(plan: &Arc<dyn ExecutionPlan>) -> Option<RewriteReadiness> {
    if let Some(annotated) = plan.downcast_ref::<ReadinessAnnotatedExec>() {
        return Some(annotated.readiness());
    }
    for child in plan.children() {
        if let Some(readiness) = readiness_from_plan(child) {
            return Some(readiness);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readiness_variants_are_distinct_and_comparable() {
        assert_ne!(RewriteReadiness::Ready, RewriteReadiness::NotReady);
        assert_ne!(RewriteReadiness::Ready, RewriteReadiness::Unknown);
        assert_ne!(RewriteReadiness::NotReady, RewriteReadiness::Unknown);
        assert_eq!(RewriteReadiness::Ready, RewriteReadiness::Ready);
    }

    #[test]
    fn readiness_is_copy_and_hashable() {
        // Ensure the variants can be stored / matched cheaply from the
        // rewrite path without cloning.
        fn requires_copy<T: Copy>() {}
        fn requires_hash<T: std::hash::Hash>() {}
        requires_copy::<RewriteReadiness>();
        requires_hash::<RewriteReadiness>();
    }

    #[test]
    fn readiness_annotated_exec_delegates_properties_and_children() {
        use arrow_schema::Schema;
        use datafusion::physical_plan::empty::EmptyExec;
        let schema = Arc::new(Schema::empty());
        let inner: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::clone(&schema)));
        let inner_props = Arc::clone(inner.properties());
        let annotated = ReadinessAnnotatedExec::new(Arc::clone(&inner), RewriteReadiness::Ready);

        assert_eq!(annotated.readiness(), RewriteReadiness::Ready);
        // Same schema/partitioning as inner — the wrapper is transparent.
        assert!(Arc::ptr_eq(annotated.properties(), &inner_props));
        // Inner exposed as the sole child.
        let children = annotated.children();
        assert_eq!(children.len(), 1);
        assert!(Arc::ptr_eq(children[0], &inner));
    }
}
