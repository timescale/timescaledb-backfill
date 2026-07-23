use crate::TelemetryBuilderError::MissingField;
use reqwest::header::CONTENT_TYPE;
use reqwest::{Response, StatusCode};
use serde::Serialize;
use serde_json::to_string;
use std::env::consts::{ARCH, FAMILY, OS};
use thiserror::Error;

const TELEMETRY_URL: &str = "https://telemetry.timescale.com/v1/executions";

#[derive(Clone, Debug, Serialize)]
pub struct DbUuid {
    uuid: String,
    label: String,
}

impl DbUuid {
    pub fn new<T: AsRef<str>>(label: T, uuid: T) -> Self {
        Self {
            label: String::from(label.as_ref()),
            uuid: String::from(uuid.as_ref()),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct Telemetry<T> {
    program: String,
    version: String,
    duration: f64,
    success: bool,
    metadata: Option<T>,
    os_family: String,
    os: String,
    arch: String,
    db_uuids: Vec<DbUuid>,
}

impl<T: Serialize> Telemetry<T> {
    pub fn builder() -> TelemetryBuilder<T> {
        TelemetryBuilder::default()
    }
}

#[derive(Debug, Error)]
pub enum TelemetryClientError {
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("reqwest error: {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("telemetry submission failed")]
    Submission,
}

pub struct TelemetryClient {
    client: reqwest::Client,
    url: String,
}

impl Default for TelemetryClient {
    fn default() -> Self {
        Self {
            client: reqwest::Client::new(),
            url: String::from(TELEMETRY_URL),
        }
    }
}

#[allow(dead_code)]
impl TelemetryClient {
    pub fn builder() -> TelemetryClientBuilder {
        TelemetryClientBuilder::default()
    }

    pub async fn send<T: Serialize>(
        &self,
        telemetry: &Telemetry<T>,
    ) -> Result<Response, TelemetryClientError> {
        let body = to_string(&telemetry)?;

        let response = self
            .client
            .post(&self.url)
            .header(CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await?;

        if response.status() != StatusCode::CREATED {
            Err(TelemetryClientError::Submission)
        } else {
            Ok(response)
        }
    }
}

#[derive(Debug, Error)]
pub enum TelemetryClientBuilderError {
    #[error("missing field: {0}")]
    MissingField(&'static str),
}

#[derive(Default)]
pub struct TelemetryClientBuilder {
    url: Option<String>,
}

impl TelemetryClientBuilder {
    pub fn url(mut self, url: impl AsRef<str>) -> Self {
        self.url = Some(String::from(url.as_ref()));
        self
    }

    pub fn build(self) -> Result<TelemetryClient, TelemetryClientBuilderError> {
        Ok(TelemetryClient {
            client: reqwest::Client::new(),
            url: self
                .url
                .ok_or(TelemetryClientBuilderError::MissingField("url"))?,
        })
    }
}

#[derive(Debug, Error)]
pub enum TelemetryBuilderError {
    #[error("missing field: {0}")]
    MissingField(&'static str),
}

#[derive(Debug)]
pub struct TelemetryBuilder<T> {
    program: Option<String>,
    version: Option<String>,
    duration: Option<f64>,
    success: Option<bool>,
    metadata: Option<T>,
    db_uuids: Vec<DbUuid>,
}

// Note: custom implementation required because otherwise the `Default` trait bound is applied to T
impl<T> Default for TelemetryBuilder<T> {
    fn default() -> Self {
        Self {
            program: None,
            version: None,
            duration: None,
            success: None,
            metadata: None,
            db_uuids: vec![],
        }
    }
}

impl<T: Serialize> TelemetryBuilder<T> {
    pub fn program<S: AsRef<str>>(mut self, program: S) -> Self {
        self.program = Some(String::from(program.as_ref()));
        self
    }

    pub fn version<S: AsRef<str>>(mut self, version: S) -> Self {
        self.version = Some(String::from(version.as_ref()));
        self
    }

    pub fn duration(mut self, duration: f64) -> Self {
        self.duration = Some(duration);
        self
    }

    pub fn success(mut self, success: bool) -> Self {
        self.success = Some(success);
        self
    }

    pub fn metadata(mut self, metadata: T) -> Self {
        self.metadata = Some(metadata);
        self
    }

    pub fn db_uuid(mut self, db_uuid: DbUuid) -> Self {
        self.db_uuids.push(db_uuid);
        self
    }

    pub fn db_uuids(mut self, mut db_uuids: Vec<DbUuid>) -> Self {
        self.db_uuids.append(&mut db_uuids);
        self
    }

    pub fn build(self) -> Result<Telemetry<T>, TelemetryBuilderError> {
        Ok(Telemetry {
            program: self.program.ok_or(MissingField("program"))?,
            version: self.version.ok_or(MissingField("version"))?,
            duration: self.duration.ok_or(MissingField("duration"))?,
            success: self.success.ok_or(MissingField("success"))?,
            metadata: self.metadata,
            os_family: FAMILY.into(),
            os: OS.into(),
            arch: ARCH.into(),
            db_uuids: self.db_uuids,
        })
    }
}

#[cfg(test)]
mod test {
    use crate::TelemetryBuilderError::MissingField;
    use crate::{Telemetry, TelemetryClient};
    use httpmock::Method::POST;
    use httpmock::MockServer;

    #[tokio::test]
    async fn it_works_end_to_end() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(POST).path("/v1/executions");
            then.status(201)
                .header("content-type", "text/html")
                .body("");
        });

        let client = TelemetryClient {
            client: reqwest::Client::new(),
            url: server.url("/v1/executions"),
        };

        let telemetry = Telemetry::builder()
            .program("test-program")
            .version("v0.1.0")
            .duration(1.234)
            .success(true)
            .metadata("hello")
            .build()
            .unwrap();

        let result = client.send(&telemetry).await;

        mock.assert();

        assert!(result.is_ok());
    }

    #[test]
    fn builder_fails_with_missing_required_field() {
        let result = Telemetry::<String>::builder().build();
        assert!(matches!(result, Err(MissingField("program"))));
        let result = Telemetry::<String>::builder().program("test").build();
        assert!(matches!(result, Err(MissingField("version"))));
        let result = Telemetry::<String>::builder()
            .program("test")
            .version("test")
            .build();
        assert!(matches!(result, Err(MissingField("duration"))));
        let result = Telemetry::<String>::builder()
            .program("test")
            .version("test")
            .duration(0.0)
            .build();
        assert!(matches!(result, Err(MissingField("success"))));
    }
}
