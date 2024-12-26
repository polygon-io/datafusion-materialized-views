use std::sync::Arc;

use datafusion::catalog::{CatalogProviderList, TableProvider};
use datafusion_common::{DataFusionError, Result};
use datafusion_sql::ResolvedTableReference;

pub fn get_table(
    catalog_list: &dyn CatalogProviderList,
    table_ref: &ResolvedTableReference,
) -> Result<Arc<dyn TableProvider>> {
    let catalog = catalog_list
        .catalog(table_ref.catalog.as_ref())
        .ok_or_else(|| DataFusionError::Plan(format!("no such catalog {}", table_ref.catalog)))?;

    let schema = catalog
        .schema(table_ref.schema.as_ref())
        .ok_or_else(|| DataFusionError::Plan(format!("no such schema {}", table_ref.schema)))?;

    // NOTE: this is bad, we are calling async code in a sync context.
    // We should file an issue about async in UDTFs.
    futures::executor::block_on(schema.table(table_ref.table.as_ref()))
        .map_err(|e| e.context(format!("couldn't get table '{}'", table_ref.table)))?
        .ok_or_else(|| DataFusionError::Plan(format!("no such table {}", table_ref.schema)))
}
