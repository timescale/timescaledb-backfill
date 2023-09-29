// Copyright 2023 Timescale, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
use std::{ffi::OsStr, str::FromStr};
use testcontainers::Container;

use tokio_postgres::Config as PgConfig;

pub use crate::*;

// Note: do not be tempted to replace this with "localhost".
// If you do, everything will work perfectly fine, except for occasional CI
// failures which you can't really explain, and spend weeks tracking down.
// The reason is that docker exposes the database's internal ports on both the
// ipv4 and ipv6 addresses of the host. There can be "overlap" in that the same
// port number is mapped for ipv4 and ipv6, but the ports belong to different
// containers. If the connection to the database uses "localhost", it can be
// (depending on the host's setup) that this resolves to the ipv6 address. By
// hard-coding the ipv4 loopback address, we ensure that we're always talking
// to the container that we think we are.
static LOOPBACK_IPV4_ADDRESS: &str = "127.0.0.1";

#[derive(Clone, Debug)]
struct TestConnectionParts {
    user: Option<String>,
    host: Option<String>,
    port: Option<u16>,
    dbname: Option<String>,
    password: Option<String>,
}

#[derive(Clone, Debug)]
pub struct TestConnectionString {
    internal: TestConnectionParts,
    formatted: String,
}

impl Default for TestConnectionString {
    fn default() -> Self {
        TestConnectionString::new(5432)
    }
}

impl TestConnectionString {
    #[allow(clippy::single_char_pattern)]
    fn escape_conn_value(value: &str) -> String {
        value.replace(r"\", r"\\").replace(r"'", r"\'")
    }

    fn build(internal: TestConnectionParts) -> Self {
        // this structure should enable us to add support
        // for more fields as we begin to require them
        let components = vec![
            internal
                .user
                .as_ref()
                .map(|v| format!("user='{}'", Self::escape_conn_value(v))),
            internal
                .host
                .as_ref()
                .map(|v| format!("host='{}'", Self::escape_conn_value(v))),
            internal.port.map(|v| format!("port={v}")),
            internal
                .dbname
                .as_ref()
                .map(|v| format!("dbname='{}'", Self::escape_conn_value(v))),
            internal
                .password
                .as_ref()
                .map(|v| format!("password='{}'", Self::escape_conn_value(v))),
        ];
        let formatted = components
            .into_iter()
            .map(|f| f.unwrap_or_else(|| "".into()) + " ")
            .collect();
        Self {
            internal,
            formatted,
        }
    }

    pub fn is_localhost(&self) -> bool {
        self.internal
            .host
            .as_ref()
            .map(|h| h == "localhost" || h == "127.0.0.1")
            .unwrap_or(false)
    }

    pub fn new(port: u16) -> Self {
        Self::build(TestConnectionParts {
            dbname: Some("postgres".into()),
            user: Some("postgres".into()),
            host: Some(LOOPBACK_IPV4_ADDRESS.into()),
            port: Some(port),
            password: None,
        })
    }

    pub fn dbname(&self, dbname: &str) -> Self {
        Self::build(TestConnectionParts {
            dbname: Some(dbname.into()),
            ..self.internal.clone()
        })
    }

    pub fn user(&self, user: &str) -> Self {
        Self::build(TestConnectionParts {
            user: Some(user.into()),
            ..self.internal.clone()
        })
    }

    pub fn as_str(&self) -> &str {
        self.formatted.as_str()
    }
}

impl FromStr for TestConnectionString {
    type Err = tokio_postgres::error::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        PgConfig::from_str(s)
            .map(|conf| TestConnectionParts {
                user: conf.get_user().map(|v| v.to_string()),
                host: conf.get_hosts().first().map(|h| {
                    let v: &str = match h {
                        tokio_postgres::config::Host::Tcp(ref addr) => addr.as_ref(),
                        tokio_postgres::config::Host::Unix(path) => {
                            path.as_os_str().to_str().unwrap()
                        }
                    };
                    v.to_string()
                }),
                port: conf.get_ports().first().copied(),
                dbname: conf.get_dbname().map(|v| v.to_string()),
                password: conf
                    .get_password()
                    .map(|v| std::str::from_utf8(v).unwrap().to_string()),
            })
            .map(TestConnectionString::build)
    }
}

impl AsRef<str> for TestConnectionString {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<OsStr> for TestConnectionString {
    fn as_ref(&self) -> &OsStr {
        self.formatted.as_ref()
    }
}

pub trait HasConnectionString {
    fn connection_string(&self) -> TestConnectionString;
}

impl<T: testcontainers::Image> HasConnectionString for Container<'_, T> {
    fn connection_string(&self) -> TestConnectionString {
        TestConnectionString::new(self.get_host_port_ipv4(5432))
    }
}

impl HasConnectionString for TestConnectionString {
    fn connection_string(&self) -> TestConnectionString {
        self.clone()
    }
}

impl<C> HasConnectionString for &'_ C
where
    C: HasConnectionString,
{
    fn connection_string(&self) -> TestConnectionString {
        (**self).connection_string()
    }
}
