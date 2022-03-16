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

#ifndef OCEANBASE_TOOL_LOG_ENTRY_FILTER_H_
#define OCEANBASE_TOOL_LOG_ENTRY_FILTER_H_

#include <stdint.h>
#include "lib/utility/utility.h"

namespace oceanbase {
namespace clog {

class ObLogEntryFilter {
public:
  ObLogEntryFilter()
      : table_id_(0),
        partition_id_(0),
        trans_id_(0),
        log_id_(0),
        is_table_id_valid_(false),
        is_partition_id_valid_(false),
        is_trans_id_valid_(false),
        is_log_id_valid_(false)
  {}
  ~ObLogEntryFilter()
  {}
  int parse(const char* str);
  bool is_table_id_valid() const
  {
    return is_table_id_valid_;
  }
  bool is_partition_id_valid() const
  {
    return is_partition_id_valid_;
  }
  bool is_trans_id_valid() const
  {
    return is_trans_id_valid_;
  }
  bool is_log_id_valid() const
  {
    return is_log_id_valid_;
  }
  uint64_t get_table_id() const
  {
    return table_id_;
  }
  int64_t get_partition_id() const
  {
    return partition_id_;
  }
  uint64_t get_trans_id() const
  {
    return trans_id_;
  }
  int64_t get_log_id() const
  {
    return log_id_;
  }
  bool is_valid() const
  {
    return is_table_id_valid_ || is_partition_id_valid_ || is_trans_id_valid_ || is_log_id_valid_;
  }

public:
  TO_STRING_KV(K_(table_id), K_(partition_id), K_(trans_id), K_(log_id), K_(is_table_id_valid),
      K_(is_partition_id_valid), K_(is_trans_id_valid), K_(is_log_id_valid));

private:
  uint64_t table_id_;
  int64_t partition_id_;
  uint64_t trans_id_;
  int64_t log_id_;
  bool is_table_id_valid_;
  bool is_partition_id_valid_;
  bool is_trans_id_valid_;
  bool is_log_id_valid_;
};
}  // namespace clog
}  // end namespace oceanbase

#endif  // OCEANBASE_TOOL_LOG_ENTRY_FILTER_H_
