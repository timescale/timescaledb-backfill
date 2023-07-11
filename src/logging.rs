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
