use crate::{HasConnectionString, TestConnectionString};
use std::ffi::OsString;

pub trait TestConfig {
    fn args(&self) -> Vec<OsString>;
    fn action(&self) -> &str;
    fn envs(&self) -> Vec<(String, String)>;
}

#[derive(Default, Clone)]
pub struct TestConfigClean {
    target: TestConnectionString,
}

impl TestConfigClean {
    pub fn new<T: HasConnectionString>(target: &'_ T) -> Self {
        Self {
            target: target.connection_string(),
        }
    }
}

impl TestConfig for TestConfigClean {
    fn args(&self) -> Vec<OsString> {
        vec![
            OsString::from("--target"),
            OsString::from(self.target.as_str()),
        ]
    }

    fn action(&self) -> &str {
        "clean"
    }

    fn envs(&self) -> Vec<(String, String)> {
        vec![]
    }
}

#[derive(Default, Clone)]
pub struct TestConfigCopy {
    source: TestConnectionString,
    target: TestConnectionString,
    parallelism: u16,
    envs: Vec<(String, String)>,
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
            envs: vec![],
        }
    }

    pub fn with_parallel(&self, parallelism: u16) -> Self {
        Self {
            parallelism,
            ..self.clone()
        }
    }

    pub fn with_envs(&self, envs: Vec<(String, String)>) -> Self {
        Self {
            envs,
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

    fn envs(&self) -> Vec<(String, String)> {
        self.envs.clone()
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

    fn envs(&self) -> Vec<(String, String)> {
        vec![]
    }
}
