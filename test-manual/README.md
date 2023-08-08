# Manual End-to-End Test

## The Source Database

### Configuring

To configure the source database, edit the `params` view in `source.sql`.

* num_metrics = the number of hypertables to generate
* start = the date at which to start generating sample data
* end = the last date for which to generate sample data
* compress_days = the number of days to compress
* refresh_days = the number of days in caggs to refresh

### Generating

Run the `source.sh` shell script to generate a source database in a docker container. This only needs to happen once 
unless you want to change the configuration above.

```bash
./source.sh
```

### Connecting

Connect to the source database via:

```bash
psql -d 'postgres::/postgres@localhost:6506/source'
```

### Features of the Source Database

* multiple hypertables
* hypertables with time AND space dimensions
* continuous aggregates
* hierarchical continuous aggregates
* compression enabled
* N compressed chunks
* compression policies (disabled)
* retention policies (disabled)

## The Test

The test does the following

1. creates a target database in docker
2. dumps the schema and non-chunk data out of the source
3. restores the schema and non-chunk data to the target
4. stages the chunks to be backfilled
5. backfills the chunks
6. dumps some query output to check the result

### Configuring

Update the `UNTIL` shell variable in `test.sh` to control how much data is backfilled.

### Running

```bash
./test.sh
```

### Checking

Compare the `target.out` file to the `source.out` file.

### Connecting to the Target Database

```bash
psql -d 'postgres://postgres@localhost:6599/target'
```