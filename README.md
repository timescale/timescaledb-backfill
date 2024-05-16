# TimescaleDB Backfill

The TimescaleDB Backfill Tool is a command-line utility designed to support
the [dual-write and backfill] low-downtime migration process. It efficiently
copies data for a given time range directly from hypertable chunks, without
the need for intermediate storage or decompressing compressed chunks. The
tool operates transactionally, ensuring data integrity during the migration
process.

Note: Because the tool was intended for use in the dual-write and backfill
migration, it will delete data already in the target hypertable for the time
range that it is copying.

[dual-write and backfill]: https://docs.timescale.com/migrate/latest/dual-write-and-backfill/

## Limitations

- The tool only supports migrating hypertable data. Schema migrations and
  non-hypertable migrations should be handled separately before using this
  tool.
- The tool is optimized for append-only workloads, other scenarios may not
  be fully supported.

## How to Install

### Install precompiled binary

```sh
wget https://assets.timescale.com/releases/timescaledb-backfill-x86_64-linux.tar.gz
tar xf timescaledb-backfill-x86_64-linux.tar.gz
sudo mv timescaledb-backfill /usr/local/bin/
```

### Compile

Make sure you have Rust installed on your system.

Clone the TimescaleDB Backfill Tool repository from GitHub:

```sh
git clone https://github.com/timescale/timescaledb-backfill.git
cd timescaledb-backfill
```

Build and install the tool using Cargo:

```sh
cargo install --path .
```

### Amazon Linux 2023

```sh
sudo su
yum update -y
yum groupinstall "Development Tools"
yum install postgresql15
curl https://sh.rustup.rs -sSf | sh
echo 'source $HOME/.cargo/env' >> /root/.bashrc
echo 'source $HOME/.cargo/env' >> /home/ec2-user/.bashrc
git clone https://github.com/timescale/timescaledb-backfill.git
cd timescaledb-backfill
cargo install --path .
```

## How to Use

The TimescaleDB Backfill Tool offers three main commands: `stage`, `copy`, and
`clean`. The workflow involves creating tasks, copying chunks, and cleaning up
the administrative schema after the migration.

- **Stage Command:** The `stage` command is used to create copy tasks for
  hypertable chunks based on the specified completion time (`--until`) and,
  optionally, a regex filter (`--filter`). If no filter is provided, all
  hypertables will be backfilled.

  ```sh
  timescaledb-backfill stage --source $SOURCE_DB --target $TARGET_DB --until '2016-01-02T00:00:00'
  ```

- **Copy Command:** The `copy` command processes the tasks created during the
  staging phase and copies the corresponding hypertable chunks to the target
  TimescaleDB instance.

  ```sh
  timescaledb-backfill copy --source $SOURCE_DB --target $TARGET_DB
  ```

- **Clean Command:** The `clean` command removes the administrative schema
  (`__backfill`) that was used to store the tasks once the migration is completed
  successfully.

  ```sh
  timescaledb-backfill clean --target $TARGET_DB
  ```

### Usage examples

- Backfilling with a filter and until date:

  ```sh
  timescaledb-backfill stage --source $SOURCE_DB --target $TARGET_DB \
    --filter 'my_table.*' --until '2016-01-02T00:00:00'
  timescaledb-backfill copy --source $SOURCE_DB --target $TARGET_DB
  timescaledb-backfill clean --target $TARGET_DB
  ```

- Running multiple stages with different filters and until dates:

  ```sh
  timescaledb-backfill stage --source $SOURCE_DB --target $TARGET_DB \
    --filter 'schema1.table_with_time_as_timestampz' \
    --until '2015-01-01T00:00:00'
  timescaledb-backfill stage --source $SOURCE_DB --target $TARGET_DB \
    --filter 'schema1.table_with_time_as_bigint' \
    --until '91827364'
  timescaledb-backfill stage --source $SOURCE_DB --target $TARGET_DB \
    --filter 'schema2.*' \
    --until '2017-01-01T00:00:00'
  timescaledb-backfill copy --source $SOURCE_DB --target $TARGET_DB
  timescaledb-backfill clean --target $TARGET_DB
  ```

## License

The TimescaleDB Backfill Tool is open-source software licensed under the Apache
License 2.0. See the LICENSE file for more details.

## Contributing

We welcome contributions to TimescaleDB Backfill, which is licensed and
released under the open-source Apache License, Version 2. The same
[Contributor's
Agreement](https://github.com/timescale/timescaledb/blob/master/CONTRIBUTING.md)
applies as in TimescaleDB; please sign the [Contributor License
Agreement](https://cla-assistant.io/timescale/promscale) (CLA) if you're a new
contributor.
