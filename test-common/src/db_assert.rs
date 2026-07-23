use postgres::{Client, Config};
use serde_json::Value;
use std::str::FromStr;
use thiserror::Error;
use tokio_postgres::{NoTls, Row};

use crate::{HasConnectionString, JsonAssert, TestConnectionString};

#[derive(Debug, Error)]
pub enum DbAssertError {
    #[error("postgres error")]
    PostgresError(#[from] tokio_postgres::Error),
}

pub struct DbAssert {
    client: Client,
    name: Option<String>,
}

type Result<T> = std::result::Result<T, DbAssertError>;

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

    pub fn is_true<T: AsRef<str>>(&mut self, query: T) -> &mut Self {
        assert!(
            self._is_true(query.as_ref()),
            "{}query '{}' is not returning true",
            self.name(),
            query.as_ref()
        );
        self
    }

    pub fn is_false<T: AsRef<str>>(&mut self, query: T) -> &mut Self {
        assert!(
            !self._is_true(query.as_ref()),
            "{}query '{}' is not returning false",
            self.name(),
            query.as_ref()
        );
        self
    }

    pub fn has_hypertable<T: AsRef<str>>(&mut self, schema: T, table: T) -> &mut Self {
        let table_exists = self._has_table(schema.as_ref(), table.as_ref()).unwrap();
        assert!(
            table_exists,
            "{} table '{}.{}' does not exist",
            self.name(),
            schema.as_ref(),
            table.as_ref()
        );
        let is_hypertable = self
            ._has_hypertable(schema.as_ref(), table.as_ref())
            .unwrap();
        assert!(
            is_hypertable,
            "{} table '{}.{}' is not a hypertable",
            self.name(),
            schema.as_ref(),
            table.as_ref()
        );
        self
    }

    pub fn not_has_hypertable<T: AsRef<str>>(&mut self, schema: T, table: T) -> &mut Self {
        assert!(
            !self
                ._has_hypertable(schema.as_ref(), table.as_ref())
                .unwrap(),
            "{}hypertable '{}.{}' exists",
            self.name(),
            schema.as_ref(),
            table.as_ref(),
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

    pub fn has_fk_constraint_named<T: AsRef<str>>(&mut self, fk_constraint_name: T) -> &mut Self {
        assert!(
            self._has_fk_constraint_named(fk_constraint_name.as_ref())
                .unwrap(),
            "{}no foreign key constraint named '{}'",
            self.name(),
            fk_constraint_name.as_ref()
        );
        self
    }

    pub fn has_cagg_mt_chunk_count<T: AsRef<str>>(
        &mut self,
        schema: T,
        table: T,
        count: i64,
    ) -> &mut Self {
        let (mt_schema, mt_table) = self
            ._get_cagg_materialized_table(schema.as_ref(), table.as_ref())
            .unwrap();
        self.has_chunk_count(mt_schema, mt_table, count);
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

    pub fn has_user_defined_job<T: AsRef<str>>(&mut self, proc_name: T, owner: T) -> &mut Self {
        assert!(
            self._has_job(None, None, proc_name.as_ref()).unwrap(),
            "{}job '{}' not found",
            self.name(),
            proc_name.as_ref()
        );
        assert!(
            self._has_job_owner(None, None, proc_name.as_ref(), owner.as_ref())
                .unwrap(),
            "{}job '{}' not owned by '{}'",
            self.name(),
            proc_name.as_ref(),
            owner.as_ref(),
        );
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
            self._has_job(
                Some(schema.as_ref()),
                Some(table.as_ref()),
                proc_name.as_ref()
            )
            .unwrap(),
            "{}job '{}' for '{}.{}' not found",
            self.name(),
            proc_name.as_ref(),
            schema.as_ref(),
            table.as_ref(),
        );
        assert!(
            self._has_job_owner(
                Some(schema.as_ref()),
                Some(table.as_ref()),
                proc_name.as_ref(),
                owner.as_ref()
            )
            .unwrap(),
            "{}job '{}' owned by {} for '{:?}.{:?}' not found",
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
            "{}job '{}' owned by {} for '{:?}.{:?}' is not scheduled",
            self.name(),
            proc_name.as_ref(),
            owner.as_ref(),
            schema.as_ref(),
            table.as_ref(),
        );
        self
    }

    pub fn has_task_count(&mut self, count: i64) -> &mut Self {
        let task_count = self._get_task_count().unwrap();
        assert_eq!(
            task_count,
            count,
            "{}task count is '{}', not '{}'",
            self.name(),
            task_count,
            count
        );
        self
    }

    pub fn has_task_count_for_table<T: AsRef<str>>(
        &mut self,
        schema: T,
        table: T,
        count: i64,
    ) -> &mut Self {
        let task_count = self
            ._get_task_count_for_table(schema.as_ref(), table.as_ref())
            .unwrap();
        assert_eq!(
            task_count,
            count,
            "{}task count for '{}.{}' is '{}', not '{}'",
            self.name(),
            schema.as_ref(),
            table.as_ref(),
            task_count,
            count
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

    pub fn has_cagg_with_watermark<T: AsRef<str>>(
        &mut self,
        schema: T,
        name: T,
        watermark: i64,
    ) -> &mut Self {
        assert!(
            self._has_cagg(schema.as_ref(), name.as_ref()).unwrap(),
            "{}cagg '{}.{}' doesn't exist",
            self.name(),
            schema.as_ref(),
            name.as_ref(),
        );
        assert_eq!(
            watermark,
            self._fetch_cagg_watermark(schema.as_ref(), name.as_ref())
                .unwrap(),
            "{}cagg '{}.{}' watermark mismatch",
            self.name(),
            schema.as_ref(),
            name.as_ref(),
        );
        self
    }

    pub fn has_telemetry<F>(&mut self, asserts: Vec<F>) -> &mut Self
    where
        F: Fn(JsonAssert),
    {
        let rows = self._get_telemetry().unwrap();

        assert_eq!(
            asserts.len(),
            rows.len(),
            "expected {} telemetry items, got {}",
            asserts.len(),
            rows.len()
        );

        for (idx, row) in rows.iter().enumerate() {
            let telemetry_raw: Value = row.get(0);
            let telemetry = telemetry_raw.as_object().unwrap();
            let assert_fn = asserts.get(idx).unwrap();
            assert_fn(JsonAssert::new(telemetry));
        }
        self
    }

    pub fn has_index_count<T: AsRef<str>>(
        &mut self,
        schema: T,
        indexed_relname: T,
        columns: &[T],
        count: usize,
    ) -> &mut Self {
        let columns: Vec<&str> = columns.iter().map(|i| i.as_ref()).collect();
        let actual_count = self
            ._get_index_count(schema.as_ref(), indexed_relname.as_ref(), &columns)
            .unwrap();
        assert_eq!(
            actual_count,
            count,
            "{}index count on '{}.{}' ({}) is {actual_count}, not {count}",
            self.name(),
            schema.as_ref(),
            indexed_relname.as_ref(),
            columns.join(", ")
        );
        self
    }

    pub fn has_pk<T: AsRef<str>>(
        &mut self,
        table_schema: T,
        table_name: T,
        columns: Vec<T>,
    ) -> &mut Self {
        let columns: Vec<&str> = columns.iter().map(|c| c.as_ref()).collect();
        assert!(
            self._has_pk(table_schema.as_ref(), table_name.as_ref(), &columns)
                .unwrap(),
            "{}table '{}.{}' doesn't have a primary key with columns {}",
            self.name(),
            table_schema.as_ref(),
            table_name.as_ref(),
            columns.join(", "),
        );
        self
    }

    pub fn not_has_pk<T: AsRef<str>>(
        &mut self,
        table_schema: T,
        table_name: T,
        columns: Vec<T>,
    ) -> &mut Self {
        let columns: Vec<&str> = columns.iter().map(|c| c.as_ref()).collect();
        assert!(
            !self
                ._has_pk(table_schema.as_ref(), table_name.as_ref(), &columns)
                .unwrap(),
            "{}table '{}.{}' has a primary key with columns {}",
            self.name(),
            table_schema.as_ref(),
            table_name.as_ref(),
            columns.join(", "),
        );
        self
    }
    pub fn has_unique_constraint<T: AsRef<str>>(
        &mut self,
        table_schema: T,
        table_name: T,
        columns: Vec<T>,
    ) -> &mut Self {
        let columns: Vec<&str> = columns.iter().map(|c| c.as_ref()).collect();
        assert!(
            self._has_unique_constraint(table_schema.as_ref(), table_name.as_ref(), &columns)
                .unwrap(),
            "{}table '{}.{}' doesn't have a unique constraint with columns {}",
            self.name(),
            table_schema.as_ref(),
            table_name.as_ref(),
            columns.join(", "),
        );
        self
    }

    pub fn not_has_unique_constraint<T: AsRef<str>>(
        &mut self,
        table_schema: T,
        table_name: T,
        columns: Vec<T>,
    ) -> &mut Self {
        let columns: Vec<&str> = columns.iter().map(|c| c.as_ref()).collect();
        assert!(
            !self
                ._has_unique_constraint(table_schema.as_ref(), table_name.as_ref(), &columns)
                .unwrap(),
            "{}table '{}.{}' has a unique constraint with columns {}",
            self.name(),
            table_schema.as_ref(),
            table_name.as_ref(),
            columns.join(", "),
        );
        self
    }

    pub fn has_unique_index<T: AsRef<str>>(
        &mut self,
        table_schema: T,
        table_name: T,
        columns: Vec<T>,
    ) -> &mut Self {
        let columns: Vec<&str> = columns.iter().map(|c| c.as_ref()).collect();
        assert!(
            self._has_unique_index(table_schema.as_ref(), table_name.as_ref(), &columns)
                .unwrap(),
            "{}table '{}.{}' doesn't have a unique index with columns {}",
            self.name(),
            table_schema.as_ref(),
            table_name.as_ref(),
            columns.join(", "),
        );
        self
    }

    pub fn not_has_unique_index<T: AsRef<str>>(
        &mut self,
        table_schema: T,
        table_name: T,
        columns: Vec<T>,
    ) -> &mut Self {
        let columns: Vec<&str> = columns.iter().map(|c| c.as_ref()).collect();
        assert!(
            !self
                ._has_unique_index(table_schema.as_ref(), table_name.as_ref(), &columns)
                .unwrap(),
            "{}table '{}.{}' has a unique index with columns {}",
            self.name(),
            table_schema.as_ref(),
            table_name.as_ref(),
            columns.join(", "),
        );
        self
    }

    pub fn has_fk<T: AsRef<str>>(
        &mut self,
        referencing_schema: T,
        referencing_name: T,
        referencing_columns: Vec<T>,
        referenced_schema: T,
        referenced_name: T,
        referenced_columns: Vec<T>,
    ) -> &mut Self {
        let referencing_columns: Vec<&str> =
            referencing_columns.iter().map(|c| c.as_ref()).collect();
        let referenced_columns: Vec<&str> = referenced_columns.iter().map(|c| c.as_ref()).collect();
        assert!(
            self._has_fk(
                referencing_schema.as_ref(),
                referencing_name.as_ref(),
                &referencing_columns,
                referenced_schema.as_ref(),
                referenced_name.as_ref(),
                &referenced_columns
            )
            .unwrap(),
            "{}table '{}.{}' doesn't have a foreign key with columns {} to '{}.{}' on columns {}",
            self.name(),
            referencing_schema.as_ref(),
            referencing_name.as_ref(),
            referencing_columns.join(", "),
            referenced_schema.as_ref(),
            referenced_name.as_ref(),
            referenced_columns.join(", ")
        );

        self
    }

    pub fn not_has_fk<T: AsRef<str>>(
        &mut self,
        referencing_schema: T,
        referencing_name: T,
        referencing_columns: Vec<T>,
        referenced_schema: T,
        referenced_name: T,
        referenced_columns: Vec<T>,
    ) -> &mut Self {
        let referencing_columns: Vec<&str> =
            referencing_columns.iter().map(|c| c.as_ref()).collect();
        let referenced_columns: Vec<&str> = referenced_columns.iter().map(|c| c.as_ref()).collect();
        assert!(
            !self
                ._has_fk(
                    referencing_schema.as_ref(),
                    referencing_name.as_ref(),
                    &referencing_columns,
                    referenced_schema.as_ref(),
                    referenced_name.as_ref(),
                    &referenced_columns
                )
                .unwrap(),
            "{}table '{}.{}' has a foreign key with columns {} to '{}.{}' on columns {}",
            self.name(),
            referencing_schema.as_ref(),
            referencing_name.as_ref(),
            referencing_columns.join(", "),
            referenced_schema.as_ref(),
            referenced_name.as_ref(),
            referenced_columns.join(", ")
        );

        self
    }

    pub fn has_uuid(&mut self, uuid: &str) -> &mut Self {
        let actual_uuid = self._fetch_uuid().unwrap();
        assert_eq!(
            uuid,
            actual_uuid,
            "{} expected uuid {}, got {}",
            self.name(),
            uuid,
            actual_uuid,
        );
        self
    }

    fn name(&self) -> String {
        self.name
            .as_ref()
            .map(|n| format!("{n}: "))
            .unwrap_or_default()
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

    fn _is_true(&mut self, query: &str) -> bool {
        let client = self.connection();
        let v: bool = client.query_one(query, &[]).unwrap().get(0);
        v
    }

    fn _has_job(
        &mut self,
        schema: Option<&str>,
        name: Option<&str>,
        proc_name: &str,
    ) -> Result<bool> {
        let query = "\
SELECT EXISTS (
  SELECT 1
  FROM timescaledb_information.jobs j
  WHERE
    CASE WHEN $1::text IS NULL THEN j.hypertable_schema IS NULL ELSE j.hypertable_schema = $1 END
    AND CASE WHEN $2::text IS NULL THEN j.hypertable_name IS NULL ELSE j.hypertable_name = $2 END
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
        schema: Option<&str>,
        name: Option<&str>,
        proc_name: &str,
        owner: &str,
    ) -> Result<bool> {
        let query = "\
SELECT EXISTS (
  SELECT 1
  FROM timescaledb_information.jobs j
  WHERE
    CASE WHEN $1::text IS NULL THEN j.hypertable_schema IS NULL ELSE j.hypertable_schema = $1 END
    AND CASE WHEN $2::text IS NULL THEN j.hypertable_name IS NULL ELSE j.hypertable_name = $2 END
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
  FROM timescaledb_information.jobs j
  WHERE
    j.scheduled
    AND j.next_start IS NOT NULL
    AND j.hypertable_schema = $1
    AND j.hypertable_name = $2
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

    fn _has_fk_constraint_named(&mut self, fk_constraint_name: &str) -> Result<bool> {
        let row = self.connection().query_one(
            "SELECT EXISTS (SELECT true FROM pg_constraint WHERE contype = 'f' AND conname = $1);",
            &[&fk_constraint_name],
        )?;
        Ok(row.get(0))
    }

    fn _has_hypertable(&mut self, schema: &str, table: &str) -> Result<bool> {
        if self
            ._get_installed_extension_version("timescaledb")?
            .is_none()
        {
            return Ok(false);
        }
        let row = self.connection().query_one(
           "SELECT EXISTS (SELECT true FROM _timescaledb_catalog.hypertable WHERE schema_name = $1 and table_name = $2)",
         &[&schema, &table])?;
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
        let result = self.connection().query_opt(
            "SELECT extversion FROM pg_extension WHERE extname = $1",
            &[&extension],
        )?;
        Ok(result.map(|row| row.get::<_, String>(0)))
    }

    fn _get_table_count(&mut self, schema: &str, table: &str) -> Result<i64> {
        let row = self.connection().query_one(
            &format!(
                r#"
            SELECT count(*) FROM "{schema}"."{table}""#
            ),
            &[],
        )?;
        Ok(row.get("count"))
    }

    fn _get_chunk_count(&mut self, schema: &str, table: &str) -> Result<i64> {
        let row = self.connection().query_one(
            r#"
            SELECT count(*) FROM _timescaledb_catalog.chunk c
            JOIN _timescaledb_catalog.hypertable h ON c.hypertable_id = h.id
            WHERE h.schema_name = $1
              AND h.table_name = $2"#,
            &[&schema, &table],
        )?;
        Ok(row.get("count"))
    }

    fn _get_task_count(&mut self) -> Result<i64> {
        let row = self
            .connection()
            .query_one("SELECT count(*) FROM __backfill.task", &[])?;
        Ok(row.get("count"))
    }

    fn _get_task_count_for_table(&mut self, schema: &str, table: &str) -> Result<i64> {
        let row = self.connection().query_one(
            r#"
            SELECT count(*)
            FROM __backfill.task t
            WHERE (t.hypertable_schema, t.hypertable_name) IN
            (
                SELECT
                  $1 as hypertable_schema
                , $2 as hypertable_table
                UNION
                SELECT
                  h.schema_name
                , h.table_name
                FROM _timescaledb_catalog.continuous_agg c
                INNER JOIN _timescaledb_catalog.hypertable h ON (h.id = c.mat_hypertable_id)
                WHERE c.user_view_schema = $1
                AND c.user_view_name = $2
            )"#,
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

    fn _get_cagg_materialized_table(
        &mut self,
        schema: &str,
        table: &str,
    ) -> Result<(String, String)> {
        let row = self.connection().query_one(
            r"
            SELECT
              materialization_hypertable_schema as mt_schema
            , materialization_hypertable_name as mt_name
            FROM timescaledb_information.continuous_aggregates
            WHERE view_schema = $1
              AND view_name = $2
        ",
            &[&schema, &table],
        )?;
        let mt_schema = row.get("mt_schema");
        let mt_name = row.get("mt_name");
        Ok((mt_schema, mt_name))
    }

    fn _has_cagg(&mut self, schema: &str, name: &str) -> Result<bool> {
        let query: &str = r"SELECT EXISTS (
            SELECT true
            FROM _timescaledb_catalog.continuous_agg
            WHERE user_view_schema = $1 AND user_view_name = $2)";
        let row = self.connection().query_one(query, &[&schema, &name])?;
        Ok(row.get(0))
    }

    // TODO add support for the old format
    fn _fetch_cagg_watermark(&mut self, schema: &str, name: &str) -> Result<i64> {
        let query: &str = r"SELECT watermark
            FROM _timescaledb_catalog.continuous_agg
            JOIN _timescaledb_catalog.continuous_aggs_watermark USING (mat_hypertable_id)
            WHERE user_view_schema = $1 AND user_view_name = $2";
        let row = self.connection().query_one(query, &[&schema, &name])?;
        Ok(row.get(0))
    }

    fn _get_telemetry(&mut self) -> Result<Vec<Row>> {
        let rows = self.connection().query(
            r"
            SELECT body
            FROM _timescaledb_catalog.telemetry_event
            WHERE tag = 'timescaledb-backfill'
            ORDER BY created
        ",
            &[],
        )?;
        Ok(rows)
    }

    fn _get_index_count(&mut self, schema: &str, relname: &str, columns: &[&str]) -> Result<usize> {
        let query: &str = r"
          select
            count(*)
          from
            pg_class cls
          join
            pg_namespace n on n.oid = cls.relnamespace
          join
            pg_index i on i.indrelid = cls.oid
          join lateral (
            select
              array_agg(attname::text order by key.pos) as names
            from
              pg_attribute a
            join
              unnest(i.indkey::oid[]) with ordinality as key(attnum, pos) on key.attnum = a.attnum
            where
              cls.oid = a.attrelid
          ) as columns on (true)
          where
            n.nspname = $1
            and cls.relname = $2
            and columns.names = $3
        ";

        let row = self
            .connection()
            .query_one(query, &[&schema, &relname, &columns])?;
        Ok(row.get::<'_, _, i64>(0) as usize)
    }

    fn _has_pk(
        &mut self,
        table_schema: &str,
        table_name: &str,
        columns: &Vec<&str>,
    ) -> Result<bool> {
        self._has_constraint(table_schema, table_name, columns, "p")
    }

    fn _has_unique_constraint(
        &mut self,
        table_schema: &str,
        table_name: &str,
        columns: &Vec<&str>,
    ) -> Result<bool> {
        self._has_constraint(table_schema, table_name, columns, "u")
    }

    fn _has_constraint(
        &mut self,
        table_schema: &str,
        table_name: &str,
        columns: &Vec<&str>,
        constraint_type: &str,
    ) -> Result<bool> {
        let query: &str = r"select exists (
        select
        from
          pg_class cls
        join
          pg_namespace n on n.oid = cls.relnamespace
        join
          pg_constraint c on c.conrelid = cls.oid
        join lateral (
          select
            array_agg(attname::text order by key.pos) as names
          from
            pg_attribute a
          join
            unnest(c.conkey) with ordinality as key(attnum, pos) on key.attnum = a.attnum
          where
            cls.oid = a.attrelid
        ) as columns on (true)
        where
          n.nspname = $1
          and cls.relname = $2
          and columns.names = $3
          and c.contype = $4::char
        )";

        let row = self.connection().query_one(
            query,
            &[&table_schema, &table_name, &columns, &constraint_type],
        )?;
        Ok(row.get(0))
    }

    fn _has_unique_index(
        &mut self,
        table_schema: &str,
        table_name: &str,
        columns: &Vec<&str>,
    ) -> Result<bool> {
        let query: &str = r"select exists (
        select
        from
          pg_class cls
        join
          pg_namespace n on n.oid = cls.relnamespace
        join
          pg_index i on i.indrelid = cls.oid
        join lateral (
          select
            array_agg(attname::text order by key.pos) as names
          from
            pg_attribute a
          join
            unnest(i.indkey::oid[]) with ordinality as key(attnum, pos) on key.attnum = a.attnum
          where
            cls.oid = a.attrelid
        ) as columns on (true)
        where
          n.nspname = $1
          and cls.relname = $2
          and i.indisunique is true
          and columns.names = $3
        )";

        let row = self
            .connection()
            .query_one(query, &[&table_schema, &table_name, &columns])?;
        Ok(row.get(0))
    }

    fn _has_fk(
        &mut self,
        referencing_schema: &str,
        referencing_name: &str,
        referencing_columns: &Vec<&str>,
        referenced_schema: &str,
        referenced_name: &str,
        referenced_columns: &Vec<&str>,
    ) -> Result<bool> {
        let query: &str = r"select exists (
        select
        from
          pg_class referencing_cls
        join
          pg_namespace referencing_n on referencing_n.oid = referencing_cls.relnamespace
        join
          pg_constraint c on c.conrelid = referencing_cls.oid
        join
          pg_class referenced_cls on c.confrelid = referenced_cls.oid
        join
          pg_namespace referenced_n on referenced_n.oid = referenced_cls.relnamespace
        join lateral (
          select
            array_agg(attname::text order by key.pos) as names
          from
            pg_attribute a
          join
            unnest(c.conkey) with ordinality as key(attnum, pos) on key.attnum = a.attnum
          where
            referencing_cls.oid = a.attrelid
        ) as referencing_columns on (true)
        join lateral (
          select
            array_agg(attname::text order by key.pos) as names
          from
            pg_attribute a
          join
            unnest(c.conkey) with ordinality as key(attnum, pos) on key.attnum = a.attnum
          where
            referenced_cls.oid = a.attrelid
        ) as referenced_columns on (true)
        where
          referencing_n.nspname = $1
          and referencing_cls.relname = $2
          and referencing_columns.names = $3
          and c.contype = 'f'
          and referenced_n.nspname = $4
          and referenced_cls.relname = $5
          and referenced_columns.names = $6
        )";

        let row = self.connection().query_one(
            query,
            &[
                &referencing_schema,
                &referencing_name,
                &referencing_columns,
                &referenced_schema,
                &referenced_name,
                &referenced_columns,
            ],
        )?;
        Ok(row.get(0))
    }

    fn _fetch_uuid(&mut self) -> Result<String> {
        let row = self.connection().query_one(
            r"
SELECT value as uuid
FROM _timescaledb_catalog.metadata
WHERE key = 'uuid'
LIMIT 1
              ",
            &[],
        )?;
        Ok(row.get("uuid"))
    }
}
