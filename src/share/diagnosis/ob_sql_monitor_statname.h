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
SQL_MONITOR_STATNAME_DEF(MONITOR_STATNAME_BEGIN, "monitor begin", "monitor stat name begin")
// HASH
SQL_MONITOR_STATNAME_DEF(HASH_SLOT_MIN_COUNT, "min hash entry count", "element count in shortest hash slot")
SQL_MONITOR_STATNAME_DEF(HASH_SLOT_MAX_COUNT, "max hash entry count", "element count in longest hash slot")
SQL_MONITOR_STATNAME_DEF(HASH_SLOT_TOTAL_COUNT, "total hash entry count", "total element count in all slots")
SQL_MONITOR_STATNAME_DEF(HASH_BUCKET_COUNT, "slot size", "total hash bucket count")
SQL_MONITOR_STATNAME_DEF(HASH_NON_EMPTY_BUCKET_COUNT, "non-empty bucket count", "non-empty hash bucket count")
SQL_MONITOR_STATNAME_DEF(HASH_ROW_COUNT, "total row count", "total row count building hash table")
SQL_MONITOR_STATNAME_DEF(DTL_LOOP_TOTAL_MISS, "total miss count", "the total count of dtl loop miss")
SQL_MONITOR_STATNAME_DEF(
    DTL_LOOP_TOTAL_MISS_AFTER_DATA, "total miss count", "the total count of dtl loop miss after get data")
SQL_MONITOR_STATNAME_DEF(HASH_INIT_BUCKET_COUNT, "hash bucket init size", "init hash bucket count")
SQL_MONITOR_STATNAME_DEF(DISTINCT_BLOCK_MODE, "hash distinct block mode", "hash distinct block mode")
// JOIN BLOOM FILTER
SQL_MONITOR_STATNAME_DEF(JOIN_FILTER_FILTERED_COUNT, "filtered row count", "filtered row count in join filter")
SQL_MONITOR_STATNAME_DEF(JOIN_FILTER_TOTAL_COUNT, "total row count", "total row count in join filter")
SQL_MONITOR_STATNAME_DEF(
    JOIN_FILTER_CHECK_COUNT, "check row count", "the row count of participate in check bloom filter")
// PDML
SQL_MONITOR_STATNAME_DEF(
    PDML_PARTITION_FLUSH_TIME, "clock time cost write storage", "total time cost writing data to storage by pdml op")
SQL_MONITOR_STATNAME_DEF(
    PDML_PARTITION_FLUSH_COUNT, "times write to storage", "total times writing data to storage by pdml op")
// reshuffle
SQL_MONITOR_STATNAME_DEF(
    EXCHANGE_DROP_ROW_COUNT, "drop row count", "total row dropped by exchange out op for unmatched partition")
// end
SQL_MONITOR_STATNAME_DEF(MONITOR_STATNAME_END, "monitor end", "monitor stat name end")
#endif

#ifndef OB_SQL_MONITOR_STATNAME_H_
#define OB_SQL_MONITOR_STATNAME_H_

namespace oceanbase {
namespace sql {

struct ObSqlMonitorStatIds {
  enum ObSqlMonitorStatEnum {
#define SQL_MONITOR_STATNAME_DEF(def, name, desc) def,
#include "share/diagnosis/ob_sql_monitor_statname.h"
#undef SQL_MONITOR_STATNAME_DEF
  };
};

// static const int64_t MAX_MONITOR_STAT_NAME_LENGTH = 40;
// static const int64_t MAX_MONITOR_STAT_DESC_LENGTH = 200;

struct ObMonitorStat {
public:
  const char* name_;
  const char* description_;
};

extern const ObMonitorStat OB_MONITOR_STATS[];

}  // namespace sql
}  // namespace oceanbase

#endif /* OB_WAIT_EVENT_DEFINE_H_ */
