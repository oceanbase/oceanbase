#!/bin/bash
DIR=$(cd "$(dirname "$0")"; pwd)
cd "$DIR" && ./run_test_table_api.sh "$@"
