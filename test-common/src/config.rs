use crate::{HasConnectionString, TestConnectionString};
use chrono::{DateTime, Utc};
use std::ffi::OsString;

#[derive(Default, Clone)]
pub struct TestConfig {
    source: TestConnectionString,
    target: TestConnectionString,
    parallelism: u16,
    until: DateTime<Utc>,
}

impl TestConfig {
    pub fn new<S: HasConnectionString, T: HasConnectionString>(
        source: &'_ S,
        target: &'_ T,
        until: DateTime<Utc>,
    ) -> Self {
        Self {
            source: source.connection_string().for_backfill(),
            target: target.connection_string().for_backfill(),
            parallelism: 8,
            until,
        }
    }

    pub fn with_parallel(&self, parallelism: u16) -> Self {
        Self {
            parallelism,
            ..self.clone()
        }
    }

    pub fn args(&self) -> Vec<OsString> {
        vec![
            OsString::from("--source"),
            OsString::from(self.source.as_str()),
            OsString::from("--target"),
            OsString::from(self.target.as_str()),
            OsString::from("--parallelism"),
            OsString::from(self.parallelism.to_string()),
            OsString::from("--until"),
            OsString::from(self.until.to_string()),
        ]
    }
}
