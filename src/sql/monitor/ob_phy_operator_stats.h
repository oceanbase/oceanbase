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

#ifndef OCEANBASE_SQL_OB_PHY_OPERATOR_STATS_H
#define OCEANBASE_SQL_OB_PHY_OPERATOR_STATS_H
#include "share/ob_define.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/utility/ob_macro_utils.h"
namespace oceanbase
{
namespace common
{
class ObIAllocator;
}
namespace sql
{
class ObPhyOperatorMonitorInfo;
struct ObOperatorStat;
class ObPhysicalPlan;
namespace StatId
{
enum StatId{
  INPUT_ROWS = 0,
  RESCAN_TIMES,
  OUTPUT_ROWS,
  MAX_STAT
};
}
class ObPhyOperatorStats
{
public:
  friend class TestPhyOperatorStats_init_Test;
  friend class TestPhyOperatorStats_test_add_Test;
  friend class TestPhyOperatorStats_test_accumulation_Test;
  ObPhyOperatorStats() : op_stats_array_(NULL),
    op_count_(0), array_size_(0), execution_times_(0)
  {}
  ~ObPhyOperatorStats() {}
  int init(common::ObIAllocator *alloc, int64_t op_count);
  int add_op_stat(ObPhyOperatorMonitorInfo &info);
  int get_op_stat_accumulation(ObPhysicalPlan *plan, int64_t op_id, ObOperatorStat &stat);
  static const int64_t COPY_COUNT = 10;
  int64_t count() { return op_count_; }
  void inc_execution_times() { ATOMIC_INC(&execution_times_); }
  int64_t get_execution_times() const { return execution_times_; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObPhyOperatorStats);
  int64_t *op_stats_array_;
  int64_t op_count_;
  int64_t array_size_;
  int64_t execution_times_;
};
} //namespace sql
} //namespace oceanbase
#endif
