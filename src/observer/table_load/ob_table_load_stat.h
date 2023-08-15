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

#pragma once

#include "lib/coro/co_var.h"
#include "lib/alloc/alloc_assist.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace observer
{

#define ENABLE_TABLE_LOAD_STAT 1

enum struct ObTableLoadStatLevel
{
  INFO = 0,
  TRACE = 1,
  DEBUG = 2
};

struct ObTableLoadStat
{
  static ObTableLoadStatLevel level_;

  ObTableLoadStat()
  {
    reset();
  }
  void reset()
  {
    MEMSET(this, 0, sizeof(*this));
  }
  int64_t execute_time_us_;
  int64_t add_task_time_us_;
  int64_t calc_part_time_us_;
  int64_t get_part_bucket_time_us_;
  int64_t bucket_add_row_time_us_;
  int64_t cast_obj_time_us_;
  int64_t check_rowkey_order_time_us_;
  int64_t sstable_append_row_time_us_;
  int64_t simple_sstable_append_row_time_us_;
  int64_t simple_sstable_set_last_key_time_us_;
  int64_t external_append_row_time_us_;
  int64_t external_write_buffer_time_us_;
  int64_t external_read_buffer_time_us_;
  int64_t external_flush_buffer_time_us_;
  int64_t external_fetch_buffer_time_us_;
  int64_t external_heap_compare_time_us_;
  int64_t transfer_external_row_time_us_;
  int64_t transfer_datum_row_time_us_;
  int64_t external_row_deep_copy_time_us_;
  int64_t external_row_serialize_time_us_;
  int64_t external_row_deserialize_time_us_;
  int64_t alloc_fragment_time_us_;
  int64_t memory_add_item_time_us_;
  int64_t memory_sort_item_time_us_;
  int64_t external_table_compact_merge_store_;
  int64_t table_compactor_time_us_;
  int64_t merge_time_us_;
  int64_t merge_get_next_row_time_us_;
  int64_t coordinator_write_time_us_;
  int64_t coordinator_flush_time_us_;
  int64_t store_write_time_us_;
  int64_t store_flush_time_us_;
  int64_t external_write_bytes_;
  int64_t external_serialize_bytes_;
  int64_t external_raw_bytes_;
  int64_t table_store_append_row_;
  int64_t table_store_get_bucket_;
  int64_t table_store_bucket_append_row_;
  int64_t table_store_row_count_;
  int64_t fast_heap_table_refresh_pk_cache_;

  TO_STRING_KV(K_(execute_time_us), K_(add_task_time_us), K_(calc_part_time_us),
               K_(get_part_bucket_time_us), K_(bucket_add_row_time_us), K_(cast_obj_time_us),
               K_(check_rowkey_order_time_us), K_(sstable_append_row_time_us),
               K_(simple_sstable_append_row_time_us), K_(simple_sstable_set_last_key_time_us),
               K_(external_append_row_time_us), K_(external_write_buffer_time_us),
               K_(external_read_buffer_time_us), K_(external_flush_buffer_time_us),
               K_(external_fetch_buffer_time_us), K_(external_heap_compare_time_us),
               K_(transfer_external_row_time_us), K_(transfer_datum_row_time_us),
               K_(external_row_deep_copy_time_us), K_(external_row_serialize_time_us),
               K_(external_row_deserialize_time_us), K_(alloc_fragment_time_us),
               K_(memory_add_item_time_us), K_(memory_sort_item_time_us),
               K_(table_compactor_time_us), K_(external_table_compact_merge_store), K_(merge_time_us), K_(merge_get_next_row_time_us),
               K_(coordinator_write_time_us), K_(coordinator_flush_time_us),
               K_(store_write_time_us), K_(store_flush_time_us), K_(external_write_bytes), K_(external_serialize_bytes), K_(external_raw_bytes),
               K_(table_store_append_row), K_(table_store_get_bucket), K_(table_store_bucket_append_row), K_(table_store_row_count),
               K_(fast_heap_table_refresh_pk_cache));
};

class ObTableLoadTimeCoster
{
public:
  ObTableLoadTimeCoster(int64_t &save_value, ObTableLoadStatLevel level) : save_value_(save_value)
  {
    level_ = level;
    if (level_ >= ObTableLoadStat::level_) {
      start_time_ = common::ObTimeUtil::current_time();
    }
  }
  ~ObTableLoadTimeCoster()
  {
    if (level_ >= ObTableLoadStat::level_) {
      save_value_ += common::ObTimeUtil::current_time() - start_time_;
    }
  }
private:
  ObTableLoadStatLevel level_;
  int64_t start_time_;
  int64_t &save_value_;
};

OB_INLINE ObTableLoadStat *get_local_table_load_stat()
{
  RLOCAL_INLINE(ObTableLoadStat, default_local_stat);
  return &default_local_stat;
}

#if defined(ENABLE_TABLE_LOAD_STAT) && ENABLE_TABLE_LOAD_STAT == 1

#define OB_TABLE_LOAD_STATISTICS_TIME_COST(level, field_name)      \
  observer::ObTableLoadTimeCoster field_name##_time_coster( \
    observer::get_local_table_load_stat()->field_name##_, observer::ObTableLoadStatLevel::level)

#define OB_TABLE_LOAD_STATISTICS_COUNTER(field_name) \
  ++observer::get_local_table_load_stat()->field_name##_

#define OB_TABLE_LOAD_STATISTICS_INC(field_name, value) \
  observer::get_local_table_load_stat()->field_name##_ += value

#define OB_TABLE_LOAD_STATISTICS_PRINT_AND_RESET()                           \
  {                                                                          \
    observer::ObTableLoadStat *stat = observer::get_local_table_load_stat(); \
    LOG_INFO("table load stat", KPC(stat));                                  \
    stat->reset();                                                           \
  }

#else

#define OB_TABLE_LOAD_STATISTICS_TIME_COST(level, field_name)

#define OB_TABLE_LOAD_STATISTICS_COUNTER(field_name)

#define OB_TABLE_LOAD_STATISTICS_INC(field_name, value)

#define OB_TABLE_LOAD_STATISTICS_PRINT_AND_RESET()

#endif

}  // namespace observer
}  // namespace oceanbase
