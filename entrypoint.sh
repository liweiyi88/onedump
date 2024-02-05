#!/bin/sh

set -e

pg_versions set-default "${PG_VERSION:-16}"

onedump "$@"