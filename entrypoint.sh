#!/usr/bin/env bash
set -Eeo pipefail

# if first arg looks like a flag, or is a hypershift action, assume we want to run hypershift
# Note: this is slightly fragile because it might break if we add more actions to hypershift
if [ "${1:0:1}" = '-' ] || [ "${1}" = "copy" ]; then
	set -- timescaledb-backfill "$@"
fi

exec "$@"
