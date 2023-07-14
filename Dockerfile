# syntax=docker/dockerfile:1.4-labs
ARG BASE_IMAGE=ubuntu:22.04

FROM ${BASE_IMAGE} as base

LABEL maintainer="Timescale Engineering"

WORKDIR /github.com/timescale/timescaledb-backfill

RUN apt-get update && apt-get install -y cmake libclang-11-dev curl clang

ENV RUST_VERSION=1.68.0
ENV PATH=/root/.cargo/bin:$PATH

RUN <<EOF
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --no-modify-path --profile minimal --default-toolchain ${RUST_VERSION}
    rustup --version
    rustc --version
    cargo --version
EOF

COPY ./Makefile ./

# Download all dependencies. By copying only Cargo.toml
# here, this step can be reused if only src or sql changes
# have been made. This is mostly useful for local development only
RUN cargo init .
COPY ./test-common test-common
COPY ./Cargo.toml Cargo.toml
RUN cargo build --release

##############################################################
# Builder Stage
##############################################################
FROM base as builder
COPY ./src src
RUN touch ./src/main.rs
RUN cargo build --release

###############################################################
# Release Stage
###############################################################
FROM ${BASE_IMAGE} as release

# Install postgres-15 client tools
# This runs as a single step to ensure minimal size
RUN <<EOF
apt-get update && apt-get install -y ca-certificates gpg lsb-release wget
wget -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor --output /usr/share/keyrings/postgresql.keyring
echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/postgresql.keyring] http://apt.postgresql.org/pub/repos/apt/ $(lsb_release -s -c)-pgdg main" > /etc/apt/sources.list.d/pgdg.list
apt-get clean
apt-get update
apt-get install -y postgresql-client-15=15.3-1.pgdg22.04+1
apt-get purge -y ca-certificates gpg lsb-release wget
apt-get autoremove -y
apt-get clean
rm -rf /var/lib/apt/lists/* \
    /var/cache/debconf/* \
    /usr/share/doc \
    /usr/share/man \
    /usr/share/locale/?? \
    /usr/share/locale/??_?? \
    /home/postgres/.pgx \
    /build/ \
    /usr/local/rustup \
    /usr/local/cargo
find /var/log -type f -exec truncate --size 0 {} \;
EOF

COPY entrypoint.sh /bin/entrypoint.sh

COPY --from=builder /github.com/timescale/timescaledb-backfill/target/release/timescaledb-backfill /bin/timescaledb-backfill

# Ensure the tool exists and can run
RUN /bin/timescaledb-backfill --help

ENV LANG=en_US.UTF-8

ENTRYPOINT ["/bin/entrypoint.sh"]
