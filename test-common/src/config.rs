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
pub struct TestConfigVerify {
    source: TestConnectionString,
    target: TestConnectionString,
    parallelism: u16,
}

impl TestConfigVerify {
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

impl TestConfig for TestConfigVerify {
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
        "verify"
    }

    fn envs(&self) -> Vec<(String, String)> {
        vec![]
    }
}

#[derive(Default, Clone)]
pub struct TestConfigStage {
    source: TestConnectionString,
    target: TestConnectionString,
    until: String,
    filter: Option<String>,
    cascade_up: Option<bool>,
    cascade_down: Option<bool>,
}

impl TestConfigStage {
    pub fn new<S: HasConnectionString, T: HasConnectionString>(
        source: &'_ S,
        target: &'_ T,
        until: &str,
    ) -> Self {
        Self {
            source: source.connection_string(),
            target: target.connection_string(),
            until: until.to_string(),
            filter: None,
            cascade_up: None,
            cascade_down: None,
        }
    }

    pub fn with_filter(&mut self, filter: &str) -> Self {
        Self {
            filter: Some(filter.to_owned()),
            ..self.clone()
        }
    }

    pub fn with_cascading_up(&mut self) -> Self {
        Self {
            cascade_up: Some(true),
            ..self.clone()
        }
    }

    pub fn with_cascading_down(&mut self) -> Self {
        Self {
            cascade_down: Some(true),
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
            OsString::from("--until"),
            OsString::from(&self.until),
        ];

        if let Some(filter) = self.filter.as_ref() {
            args.extend_from_slice(&[OsString::from("--filter"), OsString::from(filter)]);
            if self.cascade_up.is_some_and(|b| b) {
                args.extend_from_slice(&[OsString::from("--cascade-up")]);
            }
            if self.cascade_down.is_some_and(|b| b) {
                args.extend_from_slice(&[OsString::from("--cascade-down")]);
            }
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

#[derive(Default, Clone)]
pub struct TestConfigRefreshCaggs {
    source: TestConnectionString,
    target: TestConnectionString,
    filter: Option<String>,
    cascade_up: bool,
    cascade_down: bool,
}

impl TestConfigRefreshCaggs {
    pub fn new<S: HasConnectionString, T: HasConnectionString>(
        source: &'_ S,
        target: &'_ T,
    ) -> Self {
        Self {
            source: source.connection_string(),
            target: target.connection_string(),
            filter: None,
            cascade_up: false,
            cascade_down: false,
        }
    }

    pub fn with_filter(&mut self, filter: &str) -> Self {
        Self {
            filter: Some(filter.to_owned()),
            ..self.clone()
        }
    }

    pub fn with_cascading_up(&mut self) -> Self {
        Self {
            cascade_up: true,
            ..self.clone()
        }
    }

    pub fn with_cascading_down(&mut self) -> Self {
        Self {
            cascade_down: true,
            ..self.clone()
        }
    }
}

impl TestConfig for TestConfigRefreshCaggs {
    fn args(&self) -> Vec<OsString> {
        let mut args = vec![
            OsString::from("--source"),
            OsString::from(self.source.as_str()),
            OsString::from("--target"),
            OsString::from(self.target.as_str()),
        ];
        if let Some(filter) = self.filter.as_ref() {
            args.extend_from_slice(&[OsString::from("--filter"), OsString::from(filter)]);
        }
        if self.cascade_up {
            args.extend_from_slice(&[OsString::from("--cascade-up")]);
        }
        if self.cascade_down {
            args.extend_from_slice(&[OsString::from("--cascade-down")]);
        }
        args
    }

    fn action(&self) -> &str {
        "refresh-caggs"
    }

    fn envs(&self) -> Vec<(String, String)> {
        vec![]
    }
}
