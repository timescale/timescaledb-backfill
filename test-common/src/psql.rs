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
use anyhow::{anyhow, Result};
use std::ffi::OsStr;
use std::iter;
use std::process::Command;

use crate::*;

#[derive(Debug)]
pub enum PsqlInput<S: AsRef<OsStr>> {
    File(S),
    Sql(S),
}

impl<S: AsRef<OsStr>> IntoIterator for PsqlInput<S> {
    type Item = PsqlInput<S>;

    type IntoIter = iter::Once<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        iter::once(self)
    }
}

/// Launches a `psql` terminal-based front-end to PostgreSQL using
/// `has_url` to derive a connection string and executes SQL from `inputs`.
/// `inputs` can be any [`IntoIterator`] collection of [`PsqlInput`] elements:
///  - [`PsqlInput::File`] can be used to supply psql scripts and
///    is translated into `-f` flag
///  - [`PsqlInput::Sql`] is better suited for short SQL statements and
///    corresponds to `-C` flag.
///
/// If `PSQL_DOCKER` environment variable is set to `true` psql is be launched
/// inside a docker container using [`PSQL_IMAGE`]. The crate's root is mounted
/// as `/media` and set as a working directory, preserving relative paths.
pub fn psql<C: HasConnectionString, S: AsRef<OsStr>, I: IntoIterator<Item = PsqlInput<S>>>(
    has_url: &C,
    inputs: I,
) -> Result<()> {
    let inputs_vec = inputs.into_iter().collect::<Vec<PsqlInput<S>>>();
    let input_args = inputs_vec
        .iter()
        .flat_map(|input| match input {
            PsqlInput::File(file) => ["-f", file.as_ref().to_str().unwrap()],
            PsqlInput::Sql(sql) => ["-c", sql.as_ref().to_str().unwrap()],
        })
        .collect::<Vec<&str>>();

    let mut base_cmd = Command::new("psql");

    let output = base_cmd
        .arg("-AtXq")
        .arg("--set")
        .arg("ON_ERROR_STOP=1")
        .arg("-d")
        .arg(has_url.connection_string())
        .args(input_args)
        .output()?;

    if output.status.success() {
        Ok(())
    } else {
        Err(anyhow!(
            "psql command failed: {}",
            String::from_utf8(output.stderr).unwrap()
        ))
    }
}
