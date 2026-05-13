#!/bin/bash

shopt -s nullglob

if [ $# -ne 1 ]; then
    echo "Usage: $0 <case_list>"
    echo "Examples:"
    echo "       $0 tpcds_3"
    echo "       $0 'tpch_20 tpcds_65'"
    exit 1
fi

CASE_LIST=$1
OUTPUT_DIR="report"
STAGE="peak"
LIB_SO="../../../src/observer/liboceanbase.so"
PERF_SCRIPT="../../../../tools/deploy/memperf2graph.py"
LIB_MD5=$(md5sum $LIB_SO | cut -d ' ' -f1)

mkdir -p $OUTPUT_DIR

for CASE_ID in $CASE_LIST; do
    LOG_FILE=$(ls log/optimizer_trace_*_test_sql_compile_${CASE_ID}.trac 2>/dev/null)
    if [ -z "$LOG_FILE" ] || [ ! -f "$LOG_FILE" ]; then
        echo "log file of $CASE_ID not found"
        exit 1
    fi
    cat "$LOG_FILE" | $PERF_SCRIPT addr $LIB_SO 0 $STAGE global 0 0 "" $LIB_MD5
done

for CASE_ID in $CASE_LIST; do
    LOG_FILE=$(ls log/optimizer_trace_*_test_sql_compile_${CASE_ID}.trac 2>/dev/null)
    if [ -z "$LOG_FILE" ] || [ ! -f "$LOG_FILE" ]; then
        echo "log file of $CASE_ID not found"
        exit 1
    fi
    cat "$LOG_FILE" | $PERF_SCRIPT svg $LIB_SO 0 $STAGE global 0 0 "" $LIB_MD5 > $OUTPUT_DIR/${CASE_ID}_${STAGE}.svg
done

echo "memory perf generated, results can be found in directory: $OUTPUT_DIR"
echo "you can open the svg file with web browser"