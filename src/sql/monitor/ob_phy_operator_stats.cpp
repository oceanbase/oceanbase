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

#define USING_LOG_PREFIX SQL_MONITOR
#include "sql/monitor/ob_phy_operator_stats.h"
#include "sql/monitor/ob_phy_operator_monitor_info.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/plan_cache/ob_plan_cache_util.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/utility/utility.h"
using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{
int ObPhyOperatorStats::init(ObIAllocator *alloc, int64_t op_count)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  if (OB_ISNULL(alloc) || OB_UNLIKELY(op_count < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret),K(alloc), K(op_count));
  } else {
    array_size_ = op_count * StatId::MAX_STAT * COPY_COUNT;
    //2是为了保存plan_id 和operation_id
    //没有必须要存operation id , 可以通过下标计算出来
    if (OB_ISNULL(ptr = alloc->alloc(sizeof(int64_t) * array_size_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail to alloc memory for aray", K(ret), K_(array_size));
    } else {
      op_stats_array_ = static_cast<int64_t *>(ptr);
      memset(op_stats_array_, 0, sizeof(int64_t) * array_size_);
      op_count_ = op_count;
    }
  }
  return ret;
}

/*
 *
 * |-                     COPY1                -||-               COPY2               -|
 * |-       op1       -||-          op2        -||-  op1
 * |---------------------------------------------------------+---------------------------|
 * | E0 | I0 | O0 | R0 || E'1 | I'1 | O'1 | R'1 || ...
 * |-------------------------------------------------------------------------------------|
 *
 * E: EXEC_COUNT
 * I: INPUT_ROWS
 * O: OUTPUT_ROWS
 * R: RESCAN_TIMES
 * COPY = 10
 */

int ObPhyOperatorStats::add_op_stat(ObPhyOperatorMonitorInfo &info)
{
  int ret = OB_SUCCESS;
  const int64_t COPY_SIZE = op_count_ * StatId::MAX_STAT;
  int64_t copy_start_index = (get_cpu_id() % COPY_COUNT) * COPY_SIZE;
  int64_t stat_start_index = copy_start_index + info.get_op_id() * StatId::MAX_STAT;
  if (stat_start_index < 0 || stat_start_index + StatId::MAX_STAT > array_size_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid array index", K(stat_start_index), K(array_size_));
  } else {
    int64_t last_input_rows = 0;
    int64_t last_output_rows = 0;
    int64_t rescan_times = 0;
    info.get_value(INPUT_ROW_COUNT, last_input_rows);
    info.get_value(OUTPUT_ROW_COUNT, last_output_rows);
    info.get_value(RESCAN_TIMES, rescan_times);
    ATOMIC_AAF(&(op_stats_array_[stat_start_index + StatId::INPUT_ROWS]), last_input_rows);
    ATOMIC_AAF(&(op_stats_array_[stat_start_index + StatId::OUTPUT_ROWS]), last_output_rows);
    ATOMIC_AAF(&(op_stats_array_[stat_start_index + StatId::RESCAN_TIMES]), rescan_times);
  }
  return ret;
}

int ObPhyOperatorStats::get_op_stat_accumulation(ObPhysicalPlan *plan,
                                                 int64_t op_id,
                                                 ObOperatorStat &stat)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(plan)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(plan));
  } else {
    int64_t exec_times = 0;
    int64_t op_first_index = op_id * StatId::MAX_STAT;
    int64_t retry_times = 3;
    const int64_t COPY_SIZE = op_count_ * StatId::MAX_STAT;
    int64_t copy_start_index = 0;
    int64_t max_index = op_first_index + (COPY_COUNT - 1) * COPY_SIZE + StatId::MAX_STAT;
    if (op_first_index < 0 || max_index > array_size_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid array index", K(op_first_index), K(max_index), K(array_size_));
    } else {
      do {
        exec_times = ATOMIC_LOAD(&(execution_times_));
        stat.init();
        stat.execute_times_ = exec_times;
        retry_times --;
        for (int64_t i = 0; i < COPY_COUNT; i++) {
          copy_start_index = op_first_index + i * COPY_SIZE;
          stat.input_rows_ += ATOMIC_LOAD(&(op_stats_array_[copy_start_index + StatId::INPUT_ROWS]));
          stat.rescan_times_ += ATOMIC_LOAD(&(op_stats_array_[copy_start_index +StatId::RESCAN_TIMES]));
          stat.output_rows_ += ATOMIC_LOAD(&(op_stats_array_[copy_start_index + StatId::OUTPUT_ROWS]));
        }
      } while (exec_times != ATOMIC_LOAD(&(execution_times_))
               && retry_times > 0);
    }
    if (OB_SUCC(ret)) {
      stat.operation_id_ = op_id;
      stat.plan_id_ = plan->get_plan_id();
    }
  }
  return ret;
}
} //namespace sql
} //namespace oceanbase
