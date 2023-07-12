use anyhow::{Context, Result};
use postgres::{Client, Config};
use std::str::FromStr;
use tokio_postgres::NoTls;

use crate::{HasConnectionString, TestConnectionString};

pub struct DbAssert {
    client: Client,
    name: Option<String>,
}

impl DbAssert {
    pub fn new(dsn: &TestConnectionString) -> Result<Self> {
        let config = Config::from_str(dsn.connection_string().as_str())?;
        let client = config.connect(NoTls)?;

        Ok(Self { client, name: None })
    }

    pub fn with_name<T: AsRef<str>>(mut self, name: T) -> Self {
        self.name = Some(String::from(name.as_ref()));
        self
    }

    pub fn connection(&mut self) -> &mut Client {
        &mut self.client
    }

    pub fn has_extension_version<T: AsRef<str>>(&mut self, extension: T, version: T) -> &mut Self {
        let extension = extension.as_ref();
        let version = version.as_ref();
        let ext_ver: Option<String> = self._get_installed_extension_version(extension).unwrap();
        assert!(
            ext_ver.is_some(),
            "{}extension '{}' is not installed",
            self.name(),
            extension
        );
        let ext_ver = ext_ver.unwrap();
        assert_eq!(
            ext_ver,
            version,
            "{}extension '{}' version is '{}', not '{}'",
            self.name(),
            extension,
            ext_ver,
            version
        );
        self
    }

    pub fn has_schema<T: AsRef<str>>(&mut self, schema: T) -> &mut Self {
        assert!(
            self._has_schema(schema.as_ref()).unwrap(),
            "{}schema '{}' doesn't exist",
            self.name(),
            schema.as_ref()
        );
        self
    }

    pub fn not_has_schema<T: AsRef<str>>(&mut self, schema: T) -> &mut Self {
        assert!(
            !self._has_schema(schema.as_ref()).unwrap(),
            "{}schema '{}' exists",
            self.name(),
            schema.as_ref()
        );
        self
    }

    pub fn has_table<T: AsRef<str>>(&mut self, schema: T, table: T) -> &mut Self {
        assert!(
            self._has_table(schema.as_ref(), table.as_ref()).unwrap(),
            "{}table '{}.{}' doesn't exist",
            self.name(),
            schema.as_ref(),
            table.as_ref()
        );
        self
    }

    pub fn not_has_table<T: AsRef<str>>(&mut self, schema: T, table: T) -> &mut Self {
        assert!(
            !self._has_table(schema.as_ref(), table.as_ref()).unwrap(),
            "{}table '{}.{}' exists",
            self.name(),
            schema.as_ref(),
            table.as_ref()
        );
        self
    }

    pub fn has_table_count<T: AsRef<str>>(&mut self, schema: T, table: T, count: i64) -> &mut Self {
        self.has_table(schema.as_ref(), table.as_ref());
        let table_count = self
            ._get_table_count(schema.as_ref(), table.as_ref())
            .unwrap();
        assert_eq!(
            table_count,
            count,
            "{}table '{}.{}' count is '{}', not '{}'",
            self.name(),
            schema.as_ref(),
            table.as_ref(),
            table_count,
            count
        );
        self
    }

    pub fn has_chunk_count<T: AsRef<str>>(&mut self, schema: T, table: T, count: i64) -> &mut Self {
        self.has_table(schema.as_ref(), table.as_ref());
        let chunk_count = self
            ._get_chunk_count(schema.as_ref(), table.as_ref())
            .unwrap();
        assert_eq!(
            chunk_count,
            count,
            "{}table '{}.{}' chunk count is '{}', not '{}'",
            self.name(),
            schema.as_ref(),
            table.as_ref(),
            chunk_count,
            count
        );
        self
    }

    pub fn has_compressed_chunk_count<T: AsRef<str>>(
        &mut self,
        schema: T,
        table: T,
        count: i64,
    ) -> &mut Self {
        self.has_table(schema.as_ref(), table.as_ref());
        let chunk_count = self
            ._get_compressed_chunk_count(schema.as_ref(), table.as_ref())
            .unwrap();
        assert_eq!(
            chunk_count,
            count,
            "{}table '{}.{}' compressed chunk count is '{}', not '{}'",
            self.name(),
            schema.as_ref(),
            table.as_ref(),
            chunk_count,
            count
        );
        self
    }

    pub fn has_sequence<T: AsRef<str>>(&mut self, schema: T, sequence: T) -> &mut Self {
        assert!(
            self._has_sequence(schema.as_ref(), sequence.as_ref())
                .unwrap(),
            "{}sequence '{}.{}' does not exist",
            self.name(),
            schema.as_ref(),
            sequence.as_ref()
        );
        self
    }

    pub fn not_has_sequence<T: AsRef<str>>(&mut self, schema: T, sequence: T) -> &mut Self {
        assert!(
            !self
                ._has_sequence(schema.as_ref(), sequence.as_ref())
                .unwrap(),
            "{}sequence '{}.{}' exists",
            self.name(),
            schema.as_ref(),
            sequence.as_ref()
        );
        self
    }

    /// Assert that the sequence '`schema`.`sequence` exists, and has the `last_value` as its
    /// last value. Sequences which have not yet been `nextval`'d have a `last_value` of `None`.
    pub fn has_sequence_last_value<T: AsRef<str>>(
        &mut self,
        schema: T,
        sequence: T,
        last_value: Option<i64>,
    ) -> &mut Self {
        self.has_sequence(schema.as_ref(), sequence.as_ref());

        let sequence_value = self
            ._sequence_value(schema.as_ref(), sequence.as_ref())
            .unwrap();

        assert_eq!(sequence_value, last_value);
        self
    }

    pub fn has_scheduled_job<T: AsRef<str>>(
        &mut self,
        schema: T,
        table: T,
        proc_name: T,
        owner: T,
    ) -> &mut Self {
        assert!(
            self._has_job(schema.as_ref(), table.as_ref(), proc_name.as_ref())
                .unwrap(),
            "{}job '{}' for '{}.{}' not found",
            self.name(),
            proc_name.as_ref(),
            schema.as_ref(),
            table.as_ref(),
        );
        assert!(
            self._has_job_owner(
                schema.as_ref(),
                table.as_ref(),
                proc_name.as_ref(),
                owner.as_ref()
            )
            .unwrap(),
            "{}job '{}' owned by {} for '{}.{}' not found",
            self.name(),
            proc_name.as_ref(),
            owner.as_ref(),
            schema.as_ref(),
            table.as_ref(),
        );
        assert!(
            self._has_job_scheduled(
                schema.as_ref(),
                table.as_ref(),
                proc_name.as_ref(),
                owner.as_ref()
            )
            .unwrap(),
            "{}job '{}' owned by {} for '{}.{}' is not scheduled",
            self.name(),
            proc_name.as_ref(),
            owner.as_ref(),
            schema.as_ref(),
            table.as_ref(),
        );
        self
    }

    pub fn job_runs_successfully<T: AsRef<str>>(
        &mut self,
        schema: T,
        table: T,
        proc_name: T,
        owner: T,
    ) -> &mut Self {
        let job_id = self._get_job_id(
            schema.as_ref(),
            table.as_ref(),
            proc_name.as_ref(),
            owner.as_ref(),
        );
        self.connection()
            .execute("CALL run_job($1)", &[&job_id])
            .unwrap();
        let last_run_status = self._last_run_status(job_id);

        assert_eq!(
            "Success",
            last_run_status,
            "{}job '{}' owned by {} for '{}.{}' last run has status {}",
            self.name(),
            proc_name.as_ref(),
            owner.as_ref(),
            schema.as_ref(),
            table.as_ref(),
            last_run_status,
        );

        self
    }

    fn name(&self) -> String {
        self.name
            .as_ref()
            .map(|n| format!("{n}: "))
            .unwrap_or(String::new())
    }

    fn _get_job_id(&mut self, schema: &str, name: &str, proc_name: &str, owner: &str) -> i32 {
        let query = "\
SELECT id
FROM timescaledb_information.job_stats s
JOIN _timescaledb_config.bgw_job j ON s.job_id = j.id
WHERE
  s.hypertable_schema = $1
  AND s.hypertable_name = $2
  AND j.proc_name = $3
  AND j.owner::text = $4
";

        let row = self
            .connection()
            .query_one(query, &[&schema, &name, &proc_name, &owner])
            .unwrap();
        row.get(0)
    }

    fn _last_run_status(&mut self, job_id: i32) -> String {
        let query = "\
SELECT last_run_status
FROM timescaledb_information.job_stats s
WHERE
  job_id = $1
";

        let row = self.connection().query_one(query, &[&job_id]).unwrap();
        row.get(0)
    }

    fn _has_job(&mut self, schema: &str, name: &str, proc_name: &str) -> Result<bool> {
        let query = "\
SELECT EXISTS (
  SELECT 1
  FROM timescaledb_information.job_stats s
  JOIN _timescaledb_config.bgw_job j ON s.job_id = j.id
  WHERE
    s.hypertable_schema = $1
    AND s.hypertable_name = $2
    AND j.proc_name = $3
);";

        let row = self
            .connection()
            .query_one(query, &[&schema, &name, &proc_name])
            .unwrap();
        Ok(row.get(0))
    }

    fn _has_job_owner(
        &mut self,
        schema: &str,
        name: &str,
        proc_name: &str,
        owner: &str,
    ) -> Result<bool> {
        let query = "\
SELECT EXISTS (
  SELECT 1
  FROM timescaledb_information.job_stats s
  JOIN _timescaledb_config.bgw_job j ON s.job_id = j.id
  WHERE
    s.hypertable_schema = $1
    AND s.hypertable_name = $2
    AND j.proc_name = $3
    AND j.owner::text = $4
);";

        let row = self
            .connection()
            .query_one(query, &[&schema, &name, &proc_name, &owner])
            .unwrap();
        Ok(row.get(0))
    }

    fn _has_job_scheduled(
        &mut self,
        schema: &str,
        name: &str,
        proc_name: &str,
        owner: &str,
    ) -> Result<bool> {
        let query = "\
SELECT EXISTS (
  SELECT 1
  FROM timescaledb_information.job_stats s
  JOIN _timescaledb_config.bgw_job j ON s.job_id = j.id
  WHERE
    s.job_status = 'Scheduled'
    AND s.next_start IS NOT NULL
    AND s.hypertable_schema = $1
    AND s.hypertable_name = $2
    AND j.proc_name = $3
    AND j.owner::text = $4
);";

        let row = self
            .connection()
            .query_one(query, &[&schema, &name, &proc_name, &owner])
            .unwrap();
        Ok(row.get(0))
    }

    fn _has_schema(&mut self, schema: &str) -> Result<bool> {
        let row = self.connection().query_one(
            "SELECT EXISTS (SELECT true FROM pg_namespace WHERE nspname = $1);",
            &[&schema],
        )?;
        Ok(row.get(0))
    }

    fn _has_table(&mut self, schema: &str, table: &str) -> Result<bool> {
        let row = self.connection().query_one(
            "SELECT EXISTS (SELECT true FROM pg_tables WHERE schemaname = $1 AND tablename = $2);",
            &[&schema, &table],
        )?;
        Ok(row.get(0))
    }

    fn _has_sequence(&mut self, schema: &str, sequence: &str) -> Result<bool> {
        let row = self.connection().query_one(
            "SELECT EXISTS (SELECT true FROM pg_sequences WHERE schemaname = $1 AND sequencename = $2);",
            &[&schema, &sequence],
        )?;
        Ok(row.get(0))
    }

    fn _sequence_value(&mut self, schema: &str, sequence: &str) -> Result<Option<i64>> {
        let row = self.connection().query_one(
            "SELECT pg_sequence_last_value((SELECT format('%I.%I', $1::text, $2::text)))",
            &[&schema, &sequence],
        )?;
        Ok(row.get(0))
    }

    fn _get_installed_extension_version(&mut self, extension: &str) -> Result<Option<String>> {
        self.connection()
            .query_opt(
                "SELECT extversion FROM pg_extension WHERE extname = $1",
                &[&extension],
            )
            .map(|r| r.map(|row| row.get::<_, String>(0)))
            .context("failed to get installed extension version")
    }

    fn _get_table_count(&mut self, schema: &str, table: &str) -> Result<i64> {
        let row = self
            .connection()
            .query_one(&format!("SELECT count(*) FROM {schema}.{table}"), &[])?;
        Ok(row.get("count"))
    }

    fn _get_chunk_count(&mut self, schema: &str, table: &str) -> Result<i64> {
        let row = self.connection().query_one(
            r#"
            SELECT count(*) FROM timescaledb_information.chunks
            WHERE hypertable_schema = $1
              AND hypertable_name = $2"#,
            &[&schema, &table],
        )?;
        Ok(row.get("count"))
    }

    fn _get_compressed_chunk_count(&mut self, schema: &str, table: &str) -> Result<i64> {
        let row = self.connection().query_one(
            r#"
            SELECT count(*) FROM timescaledb_information.chunks
            WHERE hypertable_schema = $1
              AND hypertable_name = $2
              AND is_compressed = true"#,
            &[&schema, &table],
        )?;
        Ok(row.get("count"))
    }
}
