use anyhow::Result;
use tokio_postgres::GenericClient;

pub async fn fetch_pg_version<T: GenericClient>(client: &T) -> Result<String> {
    // We replicate the same way TimescaleDB does for determining Postgres version.
    // https://github.com/timescale/timescaledb/blob/fb65086b5542a871dc3d9757724e886dca904ef6/src/telemetry/telemetry.c#L552-L574
    let pg_version = client
        .query_one(
            "SELECT (current_setting('server_version_num')::int/10000 || '.' || current_setting('server_version_num')::int%100)::text;",
            &[],
        )
        .await?
        .get(0);
    Ok(pg_version)
}

pub async fn fetch_pg_version_number<T: GenericClient>(client: &T) -> Result<i32> {
    // We replicate the same way TimescaleDB does for determining Postgres version.
    // https://github.com/timescale/timescaledb/blob/fb65086b5542a871dc3d9757724e886dca904ef6/src/telemetry/telemetry.c#L552-L574
    let pg_version = client
        .query_one("SELECT current_setting('server_version_num')::int;", &[])
        .await?
        .get(0);
    Ok(pg_version)
}
