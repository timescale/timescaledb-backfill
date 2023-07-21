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
        return "copy";
    }
}

#[derive(Default, Clone)]
pub struct TestConfigStage {
    source: TestConnectionString,
    target: TestConnectionString,
}

impl TestConfigStage {
    pub fn new<S: HasConnectionString, T: HasConnectionString>(
        source: &'_ S,
        target: &'_ T,
    ) -> Self {
        Self {
            source: source.connection_string(),
            target: target.connection_string(),
        }
    }
}

impl TestConfig for TestConfigStage {
    fn args(&self) -> Vec<OsString> {
        vec![
            OsString::from("--source"),
            OsString::from(self.source.as_str()),
            OsString::from("--target"),
            OsString::from(self.target.as_str()),
        ]
    }

    fn action(&self) -> &str {
        return "stage";
    }
}
