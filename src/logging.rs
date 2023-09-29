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
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;

pub fn setup_logging() {
    let builder = tracing_subscriber::fmt().with_env_filter(
        EnvFilter::builder()
            .with_default_directive(LevelFilter::WARN.into())
            .from_env_lossy(),
    );
    let builder = builder
        .with_line_number(true)
        .with_target(false)
        .with_file(true);
    builder.init();
}
