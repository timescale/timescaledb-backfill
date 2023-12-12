use crate::connect::{Source, Target};
use crate::storage::backfill_schema_exists;
use crate::PanicError;
use anyhow::{Context, Error, Result};
use serde::Serialize;
use telemetry_client::{DbUuid, Telemetry as TimescaleTelemetry, TelemetryClient};
use tokio::time::Duration;
use tokio_postgres::{Client, GenericClient};
use uuid::Uuid;

const METADATA_KEY: &str = "timescaledb-backfill";
const BACKTRACE_DISABLED: &str = "disabled backtrace";

#[derive(Debug)]
struct Session {
    pub id: Uuid,
    pub created_at: String,
}

async fn fetch_session(target: &Target) -> Result<Option<Session>> {
    if !backfill_schema_exists(&target.client).await? {
        return Ok(None);
    }
    let query = "select id, created_at::text from __backfill.session";
    let row = target.client.query_one(query, &[]).await?;
    Ok(Some(Session {
        created_at: row.get("created_at"),
        id: row.get("id"),
    }))
}

async fn fetch_db_uuid(client: &mut Client) -> Result<Option<String>> {
    let tx = client.transaction().await?;
    let has_metadata_table_query = "SELECT EXISTS (SELECT true FROM information_schema.tables WHERE table_schema = '_timescaledb_catalog' AND table_name = 'metadata');";
    let row = tx.query_one(has_metadata_table_query, &[]).await?;
    let has_metadata_table: bool = row.get(0);
    if has_metadata_table {
        let query =
            "SELECT value as uuid FROM _timescaledb_catalog.metadata WHERE key = 'uuid' LIMIT 1";
        let row = tx.query_one(query, &[]).await?;
        Ok(Some(row.get("uuid")))
    } else {
        Ok(None)
    }
}

async fn fetch_pg_version<T: GenericClient>(client: &mut T) -> Result<String> {
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

async fn fetch_tsdb_version<T: GenericClient>(client: &mut T) -> Result<String> {
    let tx = client.transaction().await?;
    let tsdb_version = tx
        .query_one(
            "select extversion::text from pg_extension where extname = 'timescaledb'",
            &[],
        )
        .await?
        .get(0);
    tx.commit().await?;
    Ok(tsdb_version)
}

#[derive(Debug, Serialize)]
pub struct Telemetry {
    session_id: Option<Uuid>,
    session_created_at: Option<String>,
    timescaledb_backfill_version: String,
    command: String,
    debug_mode: bool,
    command_duration_secs: f64,
    source_db_pg_version: Option<String>,
    source_db_tsdb_version: Option<String>,
    source_db_uuid: Option<String>,
    target_db_pg_version: Option<String>,
    target_db_tsdb_version: Option<String>,
    target_db_uuid: Option<String>,

    refreshed_caggs: Option<usize>,
    staged_tasks: Option<usize>,
    copy_tasks_finished: Option<usize>,
    copy_tasks_total_bytes: Option<usize>,
    verify_tasks_finished: Option<usize>,
    verify_tasks_failures: Option<usize>,

    success: bool,
    error_reason: Option<Vec<String>>,
    error_backtrace: Option<String>,
}

impl Telemetry {
    pub async fn from_target_session(target: &mut Target) -> Result<Self> {
        let session = fetch_session(target).await?;
        let target_db_uuid = fetch_db_uuid(&mut target.client).await?;
        let target_db_pg_version = fetch_pg_version(&mut target.client).await.ok();
        let target_db_tsdb_version = fetch_tsdb_version(&mut target.client).await.ok();
        Ok(Self {
            session_id: session.as_ref().map(|s| s.id),
            session_created_at: session.map(|s| s.created_at),
            timescaledb_backfill_version: env!("CARGO_PKG_VERSION").to_string(),
            target_db_uuid,
            debug_mode: cfg!(debug_assertions),
            command: String::default(),
            command_duration_secs: 0f64,
            source_db_pg_version: None,
            source_db_tsdb_version: None,
            source_db_uuid: None,
            target_db_pg_version,
            target_db_tsdb_version,
            staged_tasks: None,
            copy_tasks_finished: None,
            copy_tasks_total_bytes: None,
            verify_tasks_finished: None,
            verify_tasks_failures: None,
            success: true,
            error_reason: None,
            error_backtrace: None,
            refreshed_caggs: None,
        })
    }

    pub async fn with_source_db(self, source: &mut Source) -> Telemetry {
        let pg_version = fetch_pg_version(&mut source.client).await;
        let tsdb_version = fetch_tsdb_version(&mut source.client).await;
        let db_uuid = fetch_db_uuid(&mut source.client).await.ok().flatten();

        Telemetry {
            // At this point the command finished executing. We rather report
            // the available telemetry than abort because we couldn't connect
            // to source at this point.
            source_db_pg_version: pg_version.ok(),
            source_db_tsdb_version: tsdb_version.ok(),
            source_db_uuid: db_uuid,
            ..self
        }
    }

    pub fn with_error(self, error: &Error) -> Telemetry {
        let mut reason = Vec::new();

        for cause in error.chain() {
            reason.push(cause.to_string());
        }

        let error_backtrace = match error.downcast_ref::<PanicError>() {
            Some(error) => error.backtrace.to_string(),
            None => error.backtrace().to_string(),
        };

        Telemetry {
            error_reason: Some(reason),
            error_backtrace: if error_backtrace == BACKTRACE_DISABLED {
                None
            } else {
                Some(error_backtrace)
            },
            success: false,
            ..self
        }
    }

    pub fn with_command(self, command: String) -> Telemetry {
        Telemetry { command, ..self }
    }

    pub fn with_command_duration(self, command_duration: Duration) -> Telemetry {
        Telemetry {
            command_duration_secs: command_duration.as_secs_f64(),
            ..self
        }
    }

    pub fn with_copied_tasks(
        self,
        copy_tasks_finished: usize,
        copy_tasks_total_bytes: usize,
    ) -> Telemetry {
        Telemetry {
            copy_tasks_finished: Some(copy_tasks_finished),
            copy_tasks_total_bytes: Some(copy_tasks_total_bytes),
            ..self
        }
    }

    pub fn with_verified_tasks(
        self,
        verify_tasks_finished: usize,
        verify_tasks_failures: usize,
    ) -> Telemetry {
        Telemetry {
            verify_tasks_finished: Some(verify_tasks_finished),
            verify_tasks_failures: Some(verify_tasks_failures),
            ..self
        }
    }

    pub fn with_staged_tasks(self, staged_tasks: usize) -> Telemetry {
        Telemetry {
            staged_tasks: Some(staged_tasks),
            ..self
        }
    }

    pub fn with_refreshed_caggs(self, refreshed_caggs: usize) -> Telemetry {
        Telemetry {
            refreshed_caggs: Some(refreshed_caggs),
            ..self
        }
    }
}

#[derive(Debug, Serialize)]
struct CustomTelemetry {
    version: usize,
    source_db_pg_version: Option<String>,
    source_db_tsdb_version: Option<String>,
    target_db_pg_version: Option<String>,
    target_db_tsdb_version: Option<String>,
    refreshed_caggs: Option<usize>,
    staged_tasks: Option<usize>,
    copy_tasks_finished: Option<usize>,
    copy_tasks_total_bytes: Option<usize>,
    verify_tasks_finished: Option<usize>,
    verify_tasks_failures: Option<usize>,
}

pub async fn report(target: &Target, telemetry: &Telemetry) -> Result<()> {
    let _ = report_telemetry(telemetry).await;
    let _ = store_in_target(target, telemetry).await;
    Ok(())
}

pub async fn store_in_target(target: &Target, telemetry: &Telemetry) -> Result<()> {
    let data =
        serde_json::to_value(telemetry).with_context(|| "error converting data to JSON string")?;

    // Instead of checking explicit versions, we check if the
    // telemetry_event table exists and that we have access to it. If
    // not, we assume that it does not exist and write messages to
    // metadata instead.
    //
    // This might generate a strange error message if the user does
    // not have access to either table since it will say that the user
    // cannot insert into the metadata table when in actuality she do
    // not have access to either table.
    let has_telemetry_event_table: bool = target
        .client
            .query_one(
                "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = '_timescaledb_catalog' AND tablename = 'telemetry_event')",
                &[],
            )
            .await?
            .get(0);

    if has_telemetry_event_table {
        target
            .client
            .execute(
                "INSERT INTO _timescaledb_catalog.telemetry_event(tag, body) VALUES ($1, $2)",
                &[&METADATA_KEY, &data],
            )
            .await
            .with_context(|| "error inserting into telemetry_event table")?;
        return Ok(());
    }

    let can_update_metadata: bool = target
        .client
        .query_one(
            "SELECT has_table_privilege('_timescaledb_catalog.metadata', 'UPDATE')",
            &[],
        )
        .await?
        .get(0);

    let query = if can_update_metadata {
        r"
        INSERT INTO _timescaledb_catalog.metadata (key, value, include_in_telemetry)
        VALUES ($1, json_build_array($2::jsonb)::text, TRUE)
        ON CONFLICT (key)
        DO UPDATE SET value = (metadata.value::jsonb || EXCLUDED.value::jsonb)::text
        "
    } else {
        r"
        INSERT INTO _timescaledb_catalog.metadata (key, value, include_in_telemetry)
        VALUES ($1, json_build_array($2::jsonb)::text, TRUE)
        ON CONFLICT (key)
        DO NOTHING
        "
    };

    target
        .client
        .execute(query, &[&METADATA_KEY, &data])
        .await
        .with_context(|| "error inserting into metadata table")?;

    Ok(())
}

pub async fn report_telemetry(telemetry: &Telemetry) -> Result<()> {
    let custom_telemetry = CustomTelemetry {
        // NOTE: increment version if you modify fields in this struct
        version: 1,
        // NOTE: increment version if you modify fields in this struct
        source_db_pg_version: telemetry.source_db_pg_version.clone(),
        // NOTE: increment version if you modify fields in this struct
        source_db_tsdb_version: telemetry.source_db_tsdb_version.clone(),
        // NOTE: increment version if you modify fields in this struct
        target_db_pg_version: telemetry.target_db_pg_version.clone(),
        // NOTE: increment version if you modify fields in this struct
        target_db_tsdb_version: telemetry.target_db_tsdb_version.clone(),
        // NOTE: increment version if you modify fields in this struct
        refreshed_caggs: telemetry.refreshed_caggs,
        // NOTE: increment version if you modify fields in this struct
        staged_tasks: telemetry.staged_tasks,
        // NOTE: increment version if you modify fields in this struct
        copy_tasks_finished: telemetry.copy_tasks_finished,
        // NOTE: increment version if you modify fields in this struct
        copy_tasks_total_bytes: telemetry.copy_tasks_total_bytes,
        // NOTE: increment version if you modify fields in this struct
        verify_tasks_finished: telemetry.verify_tasks_finished,
        // NOTE: increment version if you modify fields in this struct
        verify_tasks_failures: telemetry.verify_tasks_failures,
    };

    let db_uuids = [
        ("source_db", &telemetry.source_db_uuid),
        ("target_db", &telemetry.target_db_uuid),
    ]
    .iter()
    .filter_map(|item| {
        if let (label, Some(uuid)) = item {
            Some(DbUuid::new(label, &uuid.to_string().as_str()))
        } else {
            None
        }
    })
    .collect::<Vec<_>>();

    let telemetry = TimescaleTelemetry::builder()
        .program("timescaledb-backfill")
        .version(env!("CARGO_PKG_VERSION"))
        .duration(telemetry.command_duration_secs)
        .success(telemetry.success)
        .metadata(custom_telemetry)
        .db_uuids(db_uuids)
        .build()?;

    let client = TelemetryClient::default();
    if cfg!(not(debug_assertions)) {
        // Note: we don't report any telemetry when running a non-release build
        client.send(&telemetry).await?;
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use crate::telemetry::fetch_db_uuid;
    use anyhow::Result;
    use test_common::PgVersion::PG15;
    use test_common::{postgres, timescaledb, HasConnectionString};
    use testcontainers::clients::Cli;
    use tokio::task::JoinHandle;
    use tokio_postgres::{Client, NoTls};

    struct Connected {
        client: Client,
        #[allow(dead_code)]
        handle: JoinHandle<()>,
    }

    async fn connect(has_conn_str: &impl HasConnectionString) -> Result<Connected> {
        let (client, connection) =
            tokio_postgres::connect(has_conn_str.connection_string().as_str(), NoTls).await?;

        let handle = tokio::spawn(async move {
            let _ = connection.await;
        });

        Ok(Connected { client, handle })
    }

    #[tokio::test]
    async fn fetch_db_uuid_succeeds_with_timescaledb() -> Result<()> {
        let _ = pretty_env_logger::try_init();

        let docker = Cli::default();
        let source_container = docker.run(timescaledb(PG15));
        let mut connected = connect(&source_container).await?;

        let result = fetch_db_uuid(&mut connected.client).await;

        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_some());
        let result = result.unwrap();
        let db_uuid: String = connected
            .client
            .query_one(
                "SELECT value AS uuid FROM _timescaledb_catalog.metadata WHERE key = 'uuid'",
                &[],
            )
            .await?
            .get("uuid");
        assert_eq!(result, db_uuid);
        Ok(())
    }

    #[tokio::test]
    async fn fetch_db_uuid_returns_none_with_postgres() -> Result<()> {
        let _ = pretty_env_logger::try_init();

        let docker = Cli::default();
        let source_container = docker.run(postgres(PG15));
        let mut connected = connect(&source_container).await?;

        let result = fetch_db_uuid(&mut connected.client).await;

        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.is_none());
        Ok(())
    }
}
