use anyhow::{bail, Result};
use tokio_postgres::error::SqlState;
use tokio_postgres::GenericClient;

pub fn quote_table_name(schema_name: &str, table_name: &str) -> String {
    format!("{}.{}", quote_ident(schema_name), quote_ident(table_name))
}

/// `quote_ident` quotes the given value as an identifier (table, schema) safely for use in a `simple_query` call.
/// Implementation matches that of `quote_identifier` in ruleutils.c of the `PostgreSQL` code,
/// with `quote_all_identifiers` = true.
pub fn quote_ident(value: &str) -> String {
    let mut result = String::with_capacity(value.len() + 4);
    result.push('"');
    for c in value.chars() {
        if c == '"' {
            result.push(c);
        }
        result.push(c);
    }
    result.push('"');
    result
}

pub async fn assert_regex<T: GenericClient>(client: &T, table_filter: &String) -> Result<()> {
    let x = client
        .query(
            "select regexp_like('this is only a test', $1::text)",
            &[&table_filter],
        )
        .await;
    if x.is_err()
        && x.unwrap_err().code().unwrap().code() == SqlState::INVALID_REGULAR_EXPRESSION.code()
    {
        bail!("filter argument '{table_filter}' is not a valid regular expression");
    }
    Ok(())
}
