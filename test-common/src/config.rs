use crate::{HasConnectionString, TestConnectionString};
use std::ffi::OsString;

pub trait TestConfig {
    fn args(&self) -> Vec<OsString>;
    fn action(&self) -> &str;
}

#[derive(Default, Clone)]
pub struct TestConfigCopy {
    source: TestConnectionString,
    target: TestConnectionString,
    parallelism: u16,
}

impl TestConfigCopy {
    pub fn new<S: HasConnectionString, T: HasConnectionString>(
        source: &'_ S,
        target: &'_ T,
    ) -> Self {
        Self {
            source: source.connection_string(),
            target: target.connection_string(),
            parallelism: 8,
        }
    }

    pub fn with_parallel(&self, parallelism: u16) -> Self {
        Self {
            parallelism,
            ..self.clone()
        }
    }
}

impl TestConfig for TestConfigCopy {
    fn args(&self) -> Vec<OsString> {
        vec![
            OsString::from("--source"),
            OsString::from(self.source.as_str()),
            OsString::from("--target"),
            OsString::from(self.target.as_str()),
            OsString::from("--parallelism"),
            OsString::from(self.parallelism.to_string()),
        ]
    }

    fn action(&self) -> &str {
        "copy"
    }
}

#[derive(Default, Clone)]
pub struct TestConfigStage {
    source: TestConnectionString,
    target: TestConnectionString,
    until: Option<String>,
}

impl TestConfigStage {
    pub fn new<S: HasConnectionString, T: HasConnectionString>(
        source: &'_ S,
        target: &'_ T,
    ) -> Self {
        Self {
            source: source.connection_string(),
            target: target.connection_string(),
            until: None,
        }
    }

    pub fn with_completion_time(&self, until: &str) -> Self {
        Self {
            until: Some(until.into()),
            ..self.clone()
        }
    }
}

impl TestConfig for TestConfigStage {
    fn args(&self) -> Vec<OsString> {
        let mut args = vec![
            OsString::from("--source"),
            OsString::from(self.source.as_str()),
            OsString::from("--target"),
            OsString::from(self.target.as_str()),
        ];

        if let Some(until) = self.until.as_ref() {
            args.extend_from_slice(&["--until".into(), OsString::from(until)]);
        }
        args
    }

    fn action(&self) -> &str {
        "stage"
    }
}
