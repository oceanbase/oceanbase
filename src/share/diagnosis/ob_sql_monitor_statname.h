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
SQL_MONITOR_STATNAME_DEF(MONITOR_STATNAME_BEGIN, sql_monitor_statname::INVALID, "monitor begin", "monitor stat name begin")
// HASH
SQL_MONITOR_STATNAME_DEF(HASH_SLOT_MIN_COUNT, sql_monitor_statname::INT, "min hash entry count", "element count in shortest hash slot")
SQL_MONITOR_STATNAME_DEF(HASH_SLOT_MAX_COUNT, sql_monitor_statname::INT, "max hash entry count", "element count in longest hash slot")
SQL_MONITOR_STATNAME_DEF(HASH_SLOT_TOTAL_COUNT, sql_monitor_statname::INT, "total hash entry count", "total element count in all slots")
SQL_MONITOR_STATNAME_DEF(HASH_BUCKET_COUNT, sql_monitor_statname::INT, "bucket size",  "total hash bucket count")
SQL_MONITOR_STATNAME_DEF(HASH_NON_EMPTY_BUCKET_COUNT, sql_monitor_statname::INT, "non-empty bucket count", "non-empty hash bucket count")
SQL_MONITOR_STATNAME_DEF(HASH_ROW_COUNT, sql_monitor_statname::INT, "total row count", "total row count building hash table")
// DTL
SQL_MONITOR_STATNAME_DEF(DTL_LOOP_TOTAL_MISS, sql_monitor_statname::INT, "dtl miss count", "the total count of dtl loop miss")
SQL_MONITOR_STATNAME_DEF(DTL_LOOP_TOTAL_MISS_AFTER_DATA, sql_monitor_statname::INT, "dtl miss count after data", "the total count of dtl loop miss after get data")
SQL_MONITOR_STATNAME_DEF(HASH_INIT_BUCKET_COUNT, sql_monitor_statname::INT, "hash bucket init size",  "init hash bucket count")
SQL_MONITOR_STATNAME_DEF(DISTINCT_BLOCK_MODE, sql_monitor_statname::INT, "hash distinct block mode",  "hash distinct block mode")
// JOIN FILTER
SQL_MONITOR_STATNAME_DEF(JOIN_FILTER_FILTERED_COUNT, sql_monitor_statname::INT, "filtered row count", "filtered row count in join filter")
SQL_MONITOR_STATNAME_DEF(JOIN_FILTER_TOTAL_COUNT, sql_monitor_statname::INT, "total row count", "total row count in join filter")
SQL_MONITOR_STATNAME_DEF(JOIN_FILTER_CHECK_COUNT, sql_monitor_statname::INT, "check row count", "the row count of participate in check bloom filter")
SQL_MONITOR_STATNAME_DEF(JOIN_FILTER_READY_TIMESTAMP, sql_monitor_statname::TIMESTAMP, "filter ready time", "bloom filter ready time")
SQL_MONITOR_STATNAME_DEF(JOIN_FILTER_ID, sql_monitor_statname::INT, "filter id", "join filter id in plan")
SQL_MONITOR_STATNAME_DEF(JOIN_FILTER_LENGTH, sql_monitor_statname::INT, "filter length", "join filter length")
SQL_MONITOR_STATNAME_DEF(JOIN_FILTER_BIT_SET, sql_monitor_statname::INT, "filter bitset", "join filter bitset")

// PDML
SQL_MONITOR_STATNAME_DEF(PDML_PARTITION_FLUSH_TIME, sql_monitor_statname::INT, "clock time cost write storage", "total time cost writing data to storage by pdml op")
SQL_MONITOR_STATNAME_DEF(PDML_PARTITION_FLUSH_COUNT, sql_monitor_statname::INT, "times write to storage", "total times writing data to storage by pdml op")
// reshuffle
SQL_MONITOR_STATNAME_DEF(EXCHANGE_DROP_ROW_COUNT, sql_monitor_statname::INT, "drop row count", "total row dropped by exchange out op for unmatched partition")
// MONITORING DUMP
SQL_MONITOR_STATNAME_DEF(MONITORING_DUMP_SUM_OUTPUT_HASH, sql_monitor_statname::INT, "sum output hash", "sum of output hash values of monitoring dump")
// DTL
SQL_MONITOR_STATNAME_DEF(DTL_SEND_RECV_COUNT, sql_monitor_statname::INT, "processed buffer count", "the count of dtl buffer that received or sended")
SQL_MONITOR_STATNAME_DEF(EXCHANGE_EOF_TIMESTAMP, sql_monitor_statname::TIMESTAMP, "eof timestamp", "the timestamp of send eof or receive eof")
// Auto Memory Management (dump)
SQL_MONITOR_STATNAME_DEF(MEMORY_DUMP, sql_monitor_statname::CAPACITY, "memory dump size", "dump memory to disk when exceeds memory limit")
// GI
SQL_MONITOR_STATNAME_DEF(FILTERED_GRANULE_COUNT, sql_monitor_statname::INT, "filtered granule count", "filtered granule count in GI op")
SQL_MONITOR_STATNAME_DEF(TOTAL_GRANULE_COUNT, sql_monitor_statname::INT, "total granule count", "total granule count in GI op")
// sort
SQL_MONITOR_STATNAME_DEF(SORT_SORTED_ROW_COUNT, sql_monitor_statname::INT, "sorted row count", "sorted row count in sort op")
SQL_MONITOR_STATNAME_DEF(SORT_MERGE_SORT_ROUND, sql_monitor_statname::INT, "merge sort round", "merge sort round in sort op")
SQL_MONITOR_STATNAME_DEF(SORT_INMEM_SORT_TIME, sql_monitor_statname::INT, "in memory sort time", "time taken by in memory sort")
SQL_MONITOR_STATNAME_DEF(SORT_DUMP_DATA_TIME, sql_monitor_statname::INT, "sort dump data time", "time taken by dump data")
// SSTABLE INSERT
SQL_MONITOR_STATNAME_DEF(DDL_TASK_ID, sql_monitor_statname::INT, "ddl task id", "sort ddl task id")
SQL_MONITOR_STATNAME_DEF(SSTABLE_INSERT_ROW_COUNT, sql_monitor_statname::INT, "sstable insert row count", "sstable insert row count")
// Table Scan stat
SQL_MONITOR_STATNAME_DEF(IO_READ_BYTES, sql_monitor_statname::CAPACITY, "total io bytes read from disk", "total io bytes read from storage")
SQL_MONITOR_STATNAME_DEF(TOTAL_READ_BYTES, sql_monitor_statname::CAPACITY, "total bytes processed by storage", "total bytes processed by storage, including memtable")
SQL_MONITOR_STATNAME_DEF(TOTAL_READ_ROW_COUNT, sql_monitor_statname::INT, "total rows processed by storage", "total rows processed by storage, including memtable")

//end
SQL_MONITOR_STATNAME_DEF(MONITOR_STATNAME_END, sql_monitor_statname::INVALID, "monitor end", "monitor stat name end")
#endif


#ifndef OB_SQL_MONITOR_STATNAME_H_
#define OB_SQL_MONITOR_STATNAME_H_

namespace oceanbase
{
namespace sql
{

struct ObSqlMonitorStatIds
{
  enum ObSqlMonitorStatEnum
  {
#define SQL_MONITOR_STATNAME_DEF(def, type, name, desc) def,
#include "share/diagnosis/ob_sql_monitor_statname.h"
#undef SQL_MONITOR_STATNAME_DEF
  };
};

// static const int64_t MAX_MONITOR_STAT_NAME_LENGTH = 40;
// static const int64_t MAX_MONITOR_STAT_DESC_LENGTH = 200;

struct ObMonitorStat
{
public:
  int type_;
  const char *name_;
  const char *description_;
};

extern const ObMonitorStat OB_MONITOR_STATS[];

}
}

#endif /* OB_WAIT_EVENT_DEFINE_H_ */
