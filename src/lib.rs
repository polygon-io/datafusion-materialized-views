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
pub mod materialized;

/// An implementation of Query Rewriting, an optimization that rewrites queries to make use of materialized views.
pub mod rewrite;
