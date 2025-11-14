/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifdef SQL_MONITOR_STATNAME_DEF
SQL_MONITOR_STATNAME_DEF(MONITOR_STATNAME_BEGIN, metric::Unit::INVALID, "monitor begin", "monitor stat name begin", M_FIRST_VAL, metric::Level::AD_HOC)
// HASH
SQL_MONITOR_STATNAME_DEF(HASH_SLOT_MIN_COUNT, metric::Unit::INT, "min hash entry count", "element count in shortest hash slot", M_AVG, metric::Level::STANDARD)
SQL_MONITOR_STATNAME_DEF(HASH_SLOT_MAX_COUNT, metric::Unit::INT, "max hash entry count", "element count in longest hash slot", M_AVG, metric::Level::STANDARD)
SQL_MONITOR_STATNAME_DEF(HASH_SLOT_TOTAL_COUNT, metric::Unit::INT, "total hash entry count", "total element count in all slots", M_SUM, metric::Level::STANDARD)
SQL_MONITOR_STATNAME_DEF(HASH_BUCKET_COUNT, metric::Unit::INT, "bucket size",  "total hash bucket count", M_SUM, metric::Level::CRITICAL)
SQL_MONITOR_STATNAME_DEF(HASH_NON_EMPTY_BUCKET_COUNT, metric::Unit::INT, "non-empty bucket count", "non-empty hash bucket count", M_SUM, metric::Level::CRITICAL)
SQL_MONITOR_STATNAME_DEF(HASH_ROW_COUNT, metric::Unit::INT, "total row count", "total row count building hash table", M_SUM, metric::Level::CRITICAL)
SQL_MONITOR_STATNAME_DEF(HASH_POPULAR_MAP_SIZE, metric::Unit::INT, "popular hash group item cnt", "size of popular map", M_SUM, metric::Level::STANDARD)
SQL_MONITOR_STATNAME_DEF(HASH_BY_PASS_AGG_CNT, metric::Unit::INT, "agg row cnt when bypass", "hit count of popular map", M_SUM, metric::Level::STANDARD)
// DTL
SQL_MONITOR_STATNAME_DEF(DTL_LOOP_TOTAL_MISS, metric::Unit::INT, "dtl miss count", "the total count of dtl loop miss", M_SUM, metric::Level::AD_HOC)
SQL_MONITOR_STATNAME_DEF(DTL_LOOP_TOTAL_MISS_AFTER_DATA, metric::Unit::INT, "dtl miss count after data", "the total count of dtl loop miss after get data", M_SUM, metric::Level::AD_HOC)
SQL_MONITOR_STATNAME_DEF(HASH_INIT_BUCKET_COUNT, metric::Unit::INT, "hash bucket init size",  "init hash bucket count", M_SUM, metric::Level::CRITICAL)
SQL_MONITOR_STATNAME_DEF(DISTINCT_BLOCK_MODE, metric::Unit::INT, "hash distinct block mode",  "hash distinct block mode", M_FIRST_VAL, metric::Level::STANDARD)
// JOIN FILTER
SQL_MONITOR_STATNAME_DEF(JOIN_FILTER_FILTERED_COUNT, metric::Unit::INT, "filtered row count", "filtered row count in join filter", M_SUM | E_MIN | E_MAX, metric::Level::STANDARD)
SQL_MONITOR_STATNAME_DEF(JOIN_FILTER_TOTAL_COUNT, metric::Unit::INT, "total row count", "total row count in join filter", M_SUM | E_MIN | E_MAX, metric::Level::STANDARD)
SQL_MONITOR_STATNAME_DEF(JOIN_FILTER_CHECK_COUNT, metric::Unit::INT, "check row count", "the row count of participate in check bloom filter", M_SUM | E_MIN | E_MAX, metric::Level::STANDARD)
SQL_MONITOR_STATNAME_DEF(JOIN_FILTER_READY_TIMESTAMP, metric::Unit::TIMESTAMP, "filter ready time", "bloom filter ready time", E_MIN | E_MAX, metric::Level::AD_HOC)
SQL_MONITOR_STATNAME_DEF(JOIN_FILTER_ID, metric::Unit::INT, "filter id", "join filter id in plan", M_FIRST_VAL, metric::Level::AD_HOC)
SQL_MONITOR_STATNAME_DEF(JOIN_FILTER_LENGTH, metric::Unit::INT, "filter length", "join filter length", M_FIRST_VAL, metric::Level::STANDARD)
SQL_MONITOR_STATNAME_DEF(JOIN_FILTER_BIT_SET, metric::Unit::INT, "filter bitset", "join filter bitset", M_FIRST_VAL, metric::Level::AD_HOC)
SQL_MONITOR_STATNAME_DEF(JOIN_FILTER_BY_BASS_COUNT_BEFORE_READY, metric::Unit::INT, "by-pass row count", "by-pass row count before filter ready", M_SUM | E_MIN | E_MAX, metric::Level::AD_HOC)

// PDML
SQL_MONITOR_STATNAME_DEF(PDML_PARTITION_FLUSH_TIME, metric::Unit::TIME_NS, "clock time cost write storage", "total time cost writing data to storage by pdml op", M_AVG | E_MIN | E_MAX, metric::Level::STANDARD)
SQL_MONITOR_STATNAME_DEF(PDML_GET_ROW_COUNT_FROM_CHILD_OP, metric::Unit::INT, "clock time cost write storage", "total time cost writing data to storage by pdml op", M_SUM | E_MIN | E_MAX, metric::Level::STANDARD)
SQL_MONITOR_STATNAME_DEF(PDML_WRITE_DAS_BUFF_ROW_COUNT, metric::Unit::INT, "row_count write to das buff", "total row count writing data to das buff by pdml op", M_SUM | E_MIN | E_MAX, metric::Level::STANDARD)
SQL_MONITOR_STATNAME_DEF(PDML_SKIP_ROW_COUNT, metric::Unit::INT, "the count of skip write", "total row count which is not needed to write to storage by pdml op", M_SUM | E_MIN | E_MAX, metric::Level::AD_HOC)
SQL_MONITOR_STATNAME_DEF(PDML_STORAGE_RETURN_ROW_COUNT, metric::Unit::INT, "row_count storage return", "total write row count storage return to pdml op", M_SUM | E_MIN | E_MAX, metric::Level::STANDARD)

// reshuffle
SQL_MONITOR_STATNAME_DEF(EXCHANGE_DROP_ROW_COUNT, metric::Unit::INT, "drop row count", "total row dropped by exchange out op for unmatched partition", M_SUM | E_MIN | E_MAX, metric::Level::STANDARD)
// MONITORING DUMP
SQL_MONITOR_STATNAME_DEF(MONITORING_DUMP_SUM_OUTPUT_HASH, metric::Unit::INT, "sum output hash", "sum of output hash values of monitoring dump", M_FIRST_VAL, metric::Level::AD_HOC)
// DTL
SQL_MONITOR_STATNAME_DEF(DTL_SEND_RECV_COUNT, metric::Unit::INT, "processed buffer count", "the count of dtl buffer that received or sended", M_SUM, metric::Level::STANDARD)
SQL_MONITOR_STATNAME_DEF(EXCHANGE_EOF_TIMESTAMP, metric::Unit::TIMESTAMP, "eof timestamp", "the timestamp of send eof or receive eof", E_MIN | E_MAX, metric::Level::AD_HOC)
// Auto Memory Management (dump, metric::Level::STANDARD)
SQL_MONITOR_STATNAME_DEF(MEMORY_DUMP, metric::Unit::BYTES, "memory dump size", "dump memory to disk when exceeds memory limit", M_SUM | E_MIN | E_MAX, metric::Level::STANDARD)
// GI
SQL_MONITOR_STATNAME_DEF(FILTERED_GRANULE_COUNT, metric::Unit::INT, "filtered granule count", "filtered granule count in GI op", M_SUM, metric::Level::STANDARD)
SQL_MONITOR_STATNAME_DEF(TOTAL_GRANULE_COUNT, metric::Unit::INT, "total granule count", "total granule count in GI op", M_SUM | E_MIN | E_MAX, metric::Level::STANDARD)
// DDL
SQL_MONITOR_STATNAME_DEF(DDL_TASK_ID, metric::Unit::INT, "ddl task id", "sort ddl task id", M_FIRST_VAL, metric::Level::STANDARD)
// SORT
SQL_MONITOR_STATNAME_DEF(SORT_SORTED_ROW_COUNT, metric::Unit::INT, "sorted row count", "sorted row count in sort op", M_SUM | E_MIN | E_MAX, metric::Level::STANDARD)
SQL_MONITOR_STATNAME_DEF(SORT_MERGE_SORT_ROUND, metric::Unit::INT, "merge sort round", "merge sort round in sort op", E_MAX, metric::Level::STANDARD)
SQL_MONITOR_STATNAME_DEF(SORT_INMEM_SORT_TIME, metric::Unit::TIME_NS, "in memory sort time", "time taken by in memory sort", E_MAX, metric::Level::STANDARD)
SQL_MONITOR_STATNAME_DEF(SORT_DUMP_DATA_TIME, metric::Unit::TIME_NS, "sort dump data time", "time taken by dump data", E_MAX, metric::Level::AD_HOC)
SQL_MONITOR_STATNAME_DEF(ROW_COUNT, metric::Unit::INT, "row count", "row count in sort op", M_SUM | E_MIN | E_MAX, metric::Level::STANDARD)
SQL_MONITOR_STATNAME_DEF(SORT_EXPECTED_ROUND_COUNT, metric::Unit::INT, "expected sort round count", "expected sort round count in sort op", E_MAX, metric::Level::STANDARD)
SQL_MONITOR_STATNAME_DEF(MERGE_SORT_START_TIME, metric::Unit::TIMESTAMP, "merge sort start time", "merge sort start time", E_MIN | E_MAX, metric::Level::STANDARD)
SQL_MONITOR_STATNAME_DEF(COMPRESS_TYPE, metric::Unit::INT, "COMPRESS_TYPE", "COMPRESS_TYPE", E_MIN | E_MAX, metric::Level::AD_HOC)
// SSTABLE INSERT
SQL_MONITOR_STATNAME_DEF(SSTABLE_INSERT_ROW_COUNT, metric::Unit::INT, "sstable insert row count", "sstable insert row count", M_SUM, metric::Level::STANDARD)
SQL_MONITOR_STATNAME_DEF(SSTABLE_INSERT_CG_ROW_COUNT, metric::Unit::INT, "sstable insert cg_row count", "sstable insert cg row count", M_SUM, metric::Level::STANDARD)
SQL_MONITOR_STATNAME_DEF(VECTOR_INDEX_TASK_THREAD_POOL_COUNT, metric::Unit::INT, "vector index task thread pool count", "vector index task thread pool count", E_MAX, metric::Level::STANDARD)
SQL_MONITOR_STATNAME_DEF(VECTOR_INDEX_TASK_TOTAL_COUNT, metric::Unit::INT, "vector index task total count", "vector index task total count", M_SUM, metric::Level::STANDARD)
SQL_MONITOR_STATNAME_DEF(VECTOR_INDEX_TASK_FINISH_COUNT, metric::Unit::INT, "vector index task finish count", "vector index task finish count", M_SUM, metric::Level::STANDARD)
SQL_MONITOR_STATNAME_DEF(VECTOR_INDEX_TASK_TABLET_COUNT, metric::Unit::INT, "vector index task tablet count", "vector index task tablet count", M_SUM, metric::Level::STANDARD)
SQL_MONITOR_STATNAME_DEF(VECTOR_INDEX_TASK_KMEANS_INFO, metric::Unit::INT, "vector index task kmeans info", "vector index task kmeans info", E_MAX, metric::Level::STANDARD)
SQL_MONITOR_STATNAME_DEF(VECTOR_INDEX_TASK_IMBALANCE, metric::Unit::INT, "vector index task imbalance factor", "vector index task imbalance factor", E_MAX, metric::Level::STANDARD)
// Table Scan stat
SQL_MONITOR_STATNAME_DEF(IO_READ_BYTES, metric::Unit::BYTES, "total io bytes read from disk", "total io bytes read from storage", M_SUM | E_MIN | E_MAX, metric::Level::STANDARD)
SQL_MONITOR_STATNAME_DEF(SSSTORE_READ_BYTES, metric::Unit::BYTES, "total bytes processed by ssstore", "total bytes processed by ssstore", M_SUM | E_MIN | E_MAX, metric::Level::STANDARD)
SQL_MONITOR_STATNAME_DEF(BASE_READ_ROW_CNT, metric::Unit::INT, "total rows processed by major table", "total rows processed by major table", M_SUM | E_MIN | E_MAX, metric::Level::STANDARD)
SQL_MONITOR_STATNAME_DEF(DELTA_READ_ROW_CNT, metric::Unit::INT, "total rows processed by delta sstable", "total rows processed by minor sstable, mini sstable, memtable", M_SUM | E_MIN | E_MAX, metric::Level::STANDARD)
SQL_MONITOR_STATNAME_DEF(TOTAL_READ_ROW_COUNT, metric::Unit::INT, "total rows processed by storage", "total rows processed by storage", M_SUM | E_MIN | E_MAX, metric::Level::STANDARD)
SQL_MONITOR_STATNAME_DEF(BLOCKSCAN_BLOCK_CNT, metric::Unit::INT, "total blocks scanned by block scan", "total blocks scanned by block scan", M_SUM | E_MIN | E_MAX, metric::Level::STANDARD)
SQL_MONITOR_STATNAME_DEF(BLOCKSCAN_ROW_CNT, metric::Unit::INT, "total rows scanned by block scan", "total rows scanned by block scan", M_SUM | E_MIN | E_MAX, metric::Level::STANDARD)
SQL_MONITOR_STATNAME_DEF(STORAGE_FILTERED_ROW_CNT, metric::Unit::INT, "total rows filtered by storage", "total rows filtered by storage", M_SUM | E_MIN | E_MAX, metric::Level::STANDARD)
SQL_MONITOR_STATNAME_DEF(SKIP_INDEX_SKIP_BLOCK_CNT, metric::Unit::INT, "total blocks skipped by skip index", "total blocks skipped by skip index", M_SUM | E_MIN | E_MAX, metric::Level::STANDARD)

// Common Metrics
SQL_MONITOR_STATNAME_DEF(TOTAL_IO_TIME, metric::Unit::TIME_NS, "total io time", "total io time", M_AVG | E_MIN | E_MAX, metric::Level::CRITICAL)
SQL_MONITOR_STATNAME_DEF(OUTPUT_ROWS, metric::Unit::INT, "output rows", "output rows", M_SUM | E_MIN | E_MAX, metric::Level::CRITICAL)
SQL_MONITOR_STATNAME_DEF(OUTPUT_BATCHES, metric::Unit::INT, "output batches", "output batches", M_SUM | E_MIN | E_MAX, metric::Level::AD_HOC)
SQL_MONITOR_STATNAME_DEF(SKIPPED_ROWS, metric::Unit::INT, "skipped rows", "skipped rows", M_SUM | E_MIN | E_MAX, metric::Level::AD_HOC)
SQL_MONITOR_STATNAME_DEF(RESCAN_TIMES, metric::Unit::INT, "rescan times", "rescan times", M_SUM | E_MIN | E_MAX, metric::Level::STANDARD)
SQL_MONITOR_STATNAME_DEF(OPEN_TIME, metric::Unit::TIMESTAMP, "open time", "open time", E_MIN | E_MAX, metric::Level::CRITICAL)
SQL_MONITOR_STATNAME_DEF(FIRST_ROW_TIME, metric::Unit::TIMESTAMP, "first row time", "first row time", E_MIN | E_MAX, metric::Level::STANDARD)
SQL_MONITOR_STATNAME_DEF(LAST_ROW_TIME, metric::Unit::TIMESTAMP, "last row time", "last row time", E_MIN | E_MAX, metric::Level::STANDARD)
SQL_MONITOR_STATNAME_DEF(CLOSE_TIME, metric::Unit::TIMESTAMP, "close time", "close time", E_MIN | E_MAX, metric::Level::CRITICAL)
SQL_MONITOR_STATNAME_DEF(DB_TIME, metric::Unit::TIME_NS, "db time", "db time", M_AVG | E_MIN | E_MAX, metric::Level::CRITICAL)
SQL_MONITOR_STATNAME_DEF(WORKAREA_MEM, metric::Unit::BYTES, "work area memory", "work area memory", E_MIN | E_MAX, metric::Level::AD_HOC)
SQL_MONITOR_STATNAME_DEF(WORKAREA_MAX_MEM, metric::Unit::BYTES, "work area max memory", "work area max memory", E_MIN | E_MAX, metric::Level::AD_HOC)
SQL_MONITOR_STATNAME_DEF(WORKAREA_TEMPSEG, metric::Unit::BYTES, "work area temp seg", "work area temp seg", E_MIN | E_MAX, metric::Level::AD_HOC)
SQL_MONITOR_STATNAME_DEF(WORKAREA_MAX_TEMPSEG, metric::Unit::BYTES, "work area max temp seg", "work area max temp seg", E_MIN | E_MAX, metric::Level::AD_HOC)

// TopN Runtime Filter
SQL_MONITOR_STATNAME_DEF(TOPN_RUNTIME_FILTER_CHECK_ROWS, metric::Unit::INT, "top-n runtime filter check rows", "top-n runtime filter check rows", M_SUM | E_MIN | E_MAX, metric::Level::STANDARD)
SQL_MONITOR_STATNAME_DEF(TOPN_RUNTIME_FILTER_FILTER_ROWS, metric::Unit::INT, "top-n runtime filter filter rows", "top-n runtime filter filter rows", M_SUM | E_MIN | E_MAX, metric::Level::STANDARD)
SQL_MONITOR_STATNAME_DEF(TOPN_RUNTIME_FILTER_BYPASS_ROWS, metric::Unit::INT, "top-n runtime filter bypass rows", "top-n runtime filter bypass rows", M_SUM | E_MIN | E_MAX, metric::Level::STANDARD)

// Join Runtime Filter
SQL_MONITOR_STATNAME_DEF(RANGE_FILTER_CHECK_ROWS, metric::Unit::INT, "range filter check rows", "range filter check rows", M_SUM | E_MIN | E_MAX, metric::Level::AD_HOC)
SQL_MONITOR_STATNAME_DEF(RANGE_FILTER_FILTER_ROWS, metric::Unit::INT, "range filter filter rows", "range filter filter rows", M_SUM | E_MIN | E_MAX, metric::Level::AD_HOC)
SQL_MONITOR_STATNAME_DEF(RANGE_FILTER_BYPASS_ROWS, metric::Unit::INT, "range filter bypass rows", "range filter bypass rows", M_SUM | E_MIN | E_MAX, metric::Level::AD_HOC)
SQL_MONITOR_STATNAME_DEF(IN_FILTER_CHECK_ROWS, metric::Unit::INT, "in filter check rows", "in filter check rows", M_SUM | E_MIN | E_MAX, metric::Level::AD_HOC)
SQL_MONITOR_STATNAME_DEF(IN_FILTER_FILTER_ROWS, metric::Unit::INT, "in filter filter rows", "in filter filter rows", M_SUM | E_MIN | E_MAX, metric::Level::AD_HOC)
SQL_MONITOR_STATNAME_DEF(IN_FILTER_BYPASS_ROWS, metric::Unit::INT, "in filter bypass rows", "in filter bypass rows", M_SUM | E_MIN | E_MAX, metric::Level::AD_HOC)
SQL_MONITOR_STATNAME_DEF(BLOOM_FILTER_CHECK_ROWS, metric::Unit::INT, "bloom filter check rows", "bloom filter check rows", M_SUM | E_MIN | E_MAX, metric::Level::AD_HOC)
SQL_MONITOR_STATNAME_DEF(BLOOM_FILTER_FILTER_ROWS, metric::Unit::INT, "bloom filter filter rows", "bloom filter filter rows", M_SUM | E_MIN | E_MAX, metric::Level::AD_HOC)
SQL_MONITOR_STATNAME_DEF(BLOOM_FILTER_BYPASS_ROWS, metric::Unit::INT, "bloom filter bypass rows", "bloom filter bypass rows", M_SUM | E_MIN | E_MAX, metric::Level::AD_HOC)

// Lake Table Reader
SQL_MONITOR_STATNAME_DEF(LAKE_TABLE_SELECTED_FILE_COUNT, metric::Unit::INT, "lake table selected file count", "lake table selected file count", M_SUM, metric::Level::CRITICAL)
SQL_MONITOR_STATNAME_DEF(LAKE_TABLE_SKIPPED_FILE_COUNT, metric::Unit::INT, "lake table skipped file count", "lake table skipped file count", M_SUM, metric::Level::CRITICAL)
SQL_MONITOR_STATNAME_DEF(LAKE_TABLE_SELECTED_PAGE_COUNT, metric::Unit::INT, "lake table selected page count", "lake table selected page count", M_SUM, metric::Level::CRITICAL)
SQL_MONITOR_STATNAME_DEF(LAKE_TABLE_SKIPPED_PAGE_COUNT, metric::Unit::INT, "lake table skipped page count", "lake table skipped page count", M_SUM, metric::Level::CRITICAL)
SQL_MONITOR_STATNAME_DEF(LAKE_TABLE_SELECTED_ROW_GROUP_COUNT, metric::Unit::INT, "lake table selected row group count", "lake table selected row group count", M_SUM, metric::Level::CRITICAL)
SQL_MONITOR_STATNAME_DEF(LAKE_TABLE_SKIPPED_ROW_GROUP_COUNT, metric::Unit::INT, "lake table skipped file count", "lake table skipped file count", M_SUM, metric::Level::CRITICAL)
SQL_MONITOR_STATNAME_DEF(LAKE_TABLE_READ_ROW_COUNT, metric::Unit::INT, "lake table read row count", "lake table read row count", M_SUM, metric::Level::CRITICAL)

// Lake Table Prebuffer
SQL_MONITOR_STATNAME_DEF(LAKE_TABLE_PREBUFFER_COUNT, metric::Unit::INT, "lake table prebuffer count", "lake table prebuffer count", M_SUM, metric::Level::CRITICAL)
SQL_MONITOR_STATNAME_DEF(LAKE_TABLE_MISS_COUNT, metric::Unit::INT, "lake table miss count", "lake table miss count", M_SUM, metric::Level::CRITICAL)
SQL_MONITOR_STATNAME_DEF(LAKE_TABLE_HIT_COUNT, metric::Unit::INT, "lake table hit count", "lake table hit count", M_SUM, metric::Level::CRITICAL)
SQL_MONITOR_STATNAME_DEF(LAKE_TABLE_ASYNC_IO_COUNT, metric::Unit::INT, "lake table async io count", "lake table async io count", M_SUM, metric::Level::CRITICAL)
SQL_MONITOR_STATNAME_DEF(LAKE_TABLE_ASYNC_IO_SIZE, metric::Unit::BYTES, "lake table async io size", "lake table async io size", M_SUM, metric::Level::CRITICAL)
SQL_MONITOR_STATNAME_DEF(LAKE_TABLE_TOTAL_IO_WAIT_TIME, metric::Unit::TIME_NS, "lake table total io wait time", "lake table total io wait time", M_SUM, metric::Level::CRITICAL)
SQL_MONITOR_STATNAME_DEF(LAKE_TABLE_MAX_IO_WAIT_TIME, metric::Unit::TIME_NS, "lake table max io wait time", "lake table max io wait time", E_MAX, metric::Level::CRITICAL)
SQL_MONITOR_STATNAME_DEF(LAKE_TABLE_TOTAL_READ_SIZE, metric::Unit::BYTES, "lake table total read size", "lake table total read size", M_SUM, metric::Level::CRITICAL)

// Lake Table IO
SQL_MONITOR_STATNAME_DEF(LAKE_TABLE_READ_COUNT, metric::Unit::INT, "lake table read count", "lake table read count", M_SUM, metric::Level::CRITICAL)
SQL_MONITOR_STATNAME_DEF(LAKE_TABLE_SYNC_READ_COUNT, metric::Unit::INT, "lake table sync read count", "lake table sync read count", M_SUM, metric::Level::CRITICAL)
SQL_MONITOR_STATNAME_DEF(LAKE_TABLE_ASYNC_READ_COUNT, metric::Unit::INT, "lake table async read count", "lake table async read count", M_SUM, metric::Level::CRITICAL)
SQL_MONITOR_STATNAME_DEF(LAKE_TABLE_READ_IO_SIZE, metric::Unit::BYTES, "lake table read io size", "lake table read io size", M_SUM, metric::Level::CRITICAL)
SQL_MONITOR_STATNAME_DEF(LAKE_TABLE_MEM_CACHE_HIT_COUNT, metric::Unit::INT, "lake table memory cache hit count", "lake table memory cache hit count", M_SUM, metric::Level::CRITICAL)
SQL_MONITOR_STATNAME_DEF(LAKE_TABLE_MEM_CACHE_MISS_COUNT, metric::Unit::INT, "lake table memory cache miss count", "lake table memory cache miss count", M_SUM, metric::Level::CRITICAL)
SQL_MONITOR_STATNAME_DEF(LAKE_TABLE_MEM_CACHE_HIT_IO_SIZE, metric::Unit::BYTES, "lake table memory cache hit io size", "lake table memory cache hit io size", M_SUM, metric::Level::CRITICAL)
SQL_MONITOR_STATNAME_DEF(LAKE_TABLE_MEM_CACHE_MISS_IO_SIZE, metric::Unit::BYTES, "lake table memory cache miss io size", "lake table memory cache miss io size", M_SUM, metric::Level::CRITICAL)
SQL_MONITOR_STATNAME_DEF(LAKE_TABLE_DISK_CACHE_HIT_COUNT, metric::Unit::INT, "lake table disk cache hit count", "lake table disk cache hit count", M_SUM, metric::Level::CRITICAL)
SQL_MONITOR_STATNAME_DEF(LAKE_TABLE_DISK_CACHE_MISS_COUNT, metric::Unit::INT, "lake table disk cache miss count", "lake table disk cache miss count", M_SUM, metric::Level::CRITICAL)
SQL_MONITOR_STATNAME_DEF(LAKE_TABLE_DISK_CACHE_HIT_IO_SIZE, metric::Unit::BYTES, "lake table disk cache hit io size", "lake table disk cache hit io size", M_SUM, metric::Level::CRITICAL)
SQL_MONITOR_STATNAME_DEF(LAKE_TABLE_DISK_CACHE_MISS_IO_SIZE, metric::Unit::BYTES, "lake table disk cache miss io size", "lake table disk cache miss io size", M_SUM, metric::Level::CRITICAL)
SQL_MONITOR_STATNAME_DEF(LAKE_TABLE_MAX_IO_TIME, metric::Unit::TIME_NS, "lake table max io time", "lake table total io wait time", E_MAX, metric::Level::CRITICAL)
SQL_MONITOR_STATNAME_DEF(LAKE_TABLE_AVG_IO_TIME, metric::Unit::TIME_NS, "lake table avg io time", "lake table avg io time", M_AVG, metric::Level::CRITICAL)
SQL_MONITOR_STATNAME_DEF(LAKE_TABLE_TOTAL_IO_TIME, metric::Unit::TIME_NS, "lake table total io time", "lake table total io time", M_SUM, metric::Level::CRITICAL)
SQL_MONITOR_STATNAME_DEF(LAKE_TABLE_STORAGE_IO_COUNT, metric::Unit::INT, "lake table storage io count", "lake table storage io count", M_SUM, metric::Level::CRITICAL)

// Lake Table Reader
SQL_MONITOR_STATNAME_DEF(LAKE_TABLE_SELECTED_STRIPE_COUNT, metric::Unit::INT, "lake table selected stripe count", "lake table selected stripe count", M_SUM, metric::Level::CRITICAL)
SQL_MONITOR_STATNAME_DEF(LAKE_TABLE_SKIPPED_STRIPE_COUNT, metric::Unit::INT, "lake table skipped stripe count", "lake table skipped stripe count", M_SUM, metric::Level::CRITICAL)

//end
SQL_MONITOR_STATNAME_DEF(MONITOR_STATNAME_END, metric::Unit::INVALID, "monitor end", "monitor stat name end", E_MIN | E_MAX, metric::Level::AD_HOC)
#endif


#ifndef OB_SQL_MONITOR_STATNAME_H_
#define OB_SQL_MONITOR_STATNAME_H_

namespace oceanbase
{
namespace sql
{

struct ObSqlMonitorStatIds
{
  ObSqlMonitorStatIds();
  enum ObSqlMonitorStatEnum
  {
#define SQL_MONITOR_STATNAME_DEF(def, unit, name, desc, agg_type, collect_policy) def,
#include "share/diagnosis/ob_sql_monitor_statname.h"
#undef SQL_MONITOR_STATNAME_DEF
  };
};

#define MAX_MONITOR_STAT_NAME_LENGTH 40L
// static const int64_t MAX_MONITOR_STAT_DESC_LENGTH = 200;

struct ObMonitorStat
{
public:
  union {
    int type_;
    int unit_;
  };
  const char *name_;
  const char *description_;
  int agg_type_;
  int level_;
};

extern const ObMonitorStat OB_MONITOR_STATS[];

}
}

#endif /* OB_WAIT_EVENT_DEFINE_H_ */
