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

#define USING_LOG_PREFIX SQL_EXE

#include "ob_granule_util.h"
#include "share/ob_i_data_access_service.h"
#include "share/config/ob_server_config.h"
#include "lib/ob_errno.h"
#include "sql/ob_sql_define.h"
#include "sql/optimizer/ob_table_partition_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/px/ob_px_util.h"
#include "ob_granule_pump.h"

using namespace oceanbase::common;
namespace oceanbase {
namespace sql {

void ObParallelBlockRangeTaskParams::reset()
{
  parallelism_ = 0;
  expected_task_load_ = sql::OB_EXPECTED_TASK_LOAD;
  min_task_count_per_thread_ = sql::OB_MIN_PARALLEL_TASK_COUNT;
  max_task_count_per_thread_ = sql::OB_MAX_PARALLEL_TASK_COUNT;
  min_task_access_size_ = GCONF.px_task_size >> 20;
}

int ObParallelBlockRangeTaskParams::valid() const
{
  int ret = OB_SUCCESS;
  if (min_task_count_per_thread_ <= 0 || max_task_count_per_thread_ <= 0 || min_task_access_size_ <= 0 ||
      parallelism_ <= 0 || expected_task_load_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params is invalid", K(*this), K(ret));
  }
  return ret;
}

int ObGranuleUtil::compute_task_count(
    const ObParallelBlockRangeTaskParams& params, uint64_t marcos_count, int64_t& tasks_count)
{
  int ret = OB_SUCCESS;
  int64_t tmp_total_task_count = -1;
  // int64_t marcos_count = params.marcos_count_;
  int64_t macro_block_size = (OB_DEFAULT_MACRO_BLOCK_SIZE) >> 20;  // macro block size (MB)
  if (OB_FAIL(params.valid()) || macro_block_size <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params is invalid", K(OB_DEFAULT_MACRO_BLOCK_SIZE), K(ret));
  } else if (marcos_count == 0) {
    tmp_total_task_count = 1;
  } else {
    int64_t total_access_size = marcos_count * macro_block_size;
    // The minimum amount of data that must be read by a single task. The default is obtained
    // from the system configuration item, and the default is 2M
    int64_t min_task_access_size = NON_ZERO_VALUE(params.min_task_access_size_);  //(MB)
    int64_t expected_task_load = max(params.expected_task_load_, min_task_access_size);
    int64_t user_expect_task_count_ = NON_ZERO_VALUE(total_access_size / min_task_access_size);
    /**
     * The min_task_count_per_thread_ defaults to 13, which means that a thread is expected to have at least 13 tasks
     * lower_bound_size = parallelism * 100M * 13
     * When total_access_size is less than this value, you must ensure that min_task_access_size
     * is less than or equal to the amount of data read by a single task
     */
    int64_t lower_bound_size = params.parallelism_ * expected_task_load * params.min_task_count_per_thread_;
    /**
     * max_task_count_per_thread_ defaults to 100, which means that one thread is expected to have at most 100 tasks
     * upper_bound_size = parallelism * 100M * 100
     * When total_access_size is greater than this value, we expand the amount of data processed by each task
     */
    int64_t upper_bound_size = params.parallelism_ * expected_task_load * params.max_task_count_per_thread_;
    if (total_access_size < 0 || lower_bound_size < 0 || upper_bound_size < 0 || min_task_access_size <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("params is invalid", K(total_access_size), K(lower_bound_size), K(upper_bound_size), K(params));
    } else if (total_access_size < lower_bound_size) {
      // Try to divide it according to 13 tasks in a thread, but this task cannot be less
      // than micro-blocks, because a task contains at least one micro-block
      tmp_total_task_count = min(params.min_task_count_per_thread_ * params.parallelism_, marcos_count);
      // Ensure that the minimum amount of data that a single task must read is min_task_access_size
      tmp_total_task_count = min(tmp_total_task_count, user_expect_task_count_);
    } else if (total_access_size > upper_bound_size) {
      // Expand the amount of data processed by each task
      tmp_total_task_count = params.max_task_count_per_thread_ * params.parallelism_;
    } else {
      // Each task reads expected_task_load size data, the number of tasks per thread is between [13, 100]
      tmp_total_task_count = total_access_size / expected_task_load;
    }
  }
  if (OB_SUCC(ret)) {
    tasks_count = tmp_total_task_count;
  }
  return ret;
}

bool ObGranuleUtil::is_partition_granule(int64_t partition_count, int64_t parallelism, int64_t partition_scan_hold,
    int64_t hash_partition_scan_hold, bool hash_part)
{
  bool partition_granule = false;
  // if parallelism is too small, we use partition granule.
  if (hash_part) {
    partition_granule = partition_count >= hash_partition_scan_hold * parallelism || 1 == parallelism;
  } else {
    partition_granule = partition_count >= partition_scan_hold * parallelism || 1 == parallelism;
  }
  return partition_granule;
}

int ObGranuleUtil::split_block_ranges(ObIAllocator& allocator, const ObIArray<common::ObNewRange>& in_ranges,
    const ObIArray<ObPartitionKey>& pkeys, storage::ObPartitionService& partition_service, int64_t parallelism,
    int64_t tablet_size, bool force_partition_granule, common::ObIArray<common::ObNewRange>& granule_ranges,
    common::ObIArray<int64_t>& offsets, common::ObIArray<int64_t>& partition_offsets)
{
  int ret = OB_SUCCESS;
  int64_t total_macros_count = 0;
  int64_t total_task_count = 1;
  int64_t macros_count_per_task = 0;
  int64_t empty_partition_count = 0;
  common::ObSEArray<uint64_t, 16> macros_count_by_partition;
  common::ObSEArray<int64_t, 16> macros_count_by_partition_int64;
  common::ObSEArray<int64_t, 16> task_count_by_partition;
  common::ObSEArray<common::ObNewRange, 16> ranges;
  bool only_false_range = false;

  /**
   * prepare
   */
  if (in_ranges.count() <= 0 || pkeys.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ranges/pkeys is empty", K(in_ranges), K(pkeys), K(ret));
  } else if (OB_FAIL(remove_false_range(in_ranges, ranges, only_false_range))) {
    LOG_WARN("failed to remove false range", K(ret));
  } else if (force_partition_granule || only_false_range) {
    // partition granule iterator
    // in the case of splitting tasks according to partition granularity, the number of tasks
    // is equal to the number of partitions (`pkeys.count()`)
    FOREACH_CNT_X(pkey, pkeys, OB_SUCC(ret))
    {
      UNUSED(pkey);
      FOREACH_CNT_X(range, ranges, OB_SUCC(ret))
      {
        if (OB_FAIL(granule_ranges.push_back(*range))) {
          LOG_WARN("push back range failed", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(offsets.push_back(granule_ranges.count() - 1))) {
        LOG_WARN("push back range failed", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(partition_offsets.assign(offsets))) {
      LOG_WARN("assign partition offset failed", K(ret));
    }
    LOG_TRACE("gi partition granule");
  } else if (OB_FAIL(split_block_granule(allocator,
                 in_ranges,
                 pkeys,
                 partition_service,
                 parallelism,
                 tablet_size,
                 granule_ranges,
                 offsets,
                 partition_offsets))) {
    LOG_WARN("failed to split block granule tasks", K(ret));
  } else {
    LOG_TRACE("get the splited results through the new gi split method",
        K(ret),
        K(granule_ranges.count()),
        K(offsets.count()),
        K(offsets));
  }
  LOG_TRACE("split ranges to granule",
      K(ret),
      K(total_task_count),
      K(parallelism),
      K(total_macros_count),
      K(macros_count_by_partition),
      K(macros_count_per_task),
      K(granule_ranges.count()),
      K(granule_ranges),
      K(offsets.count()),
      K(offsets),
      K(partition_offsets.count()),
      K(partition_offsets),
      K(pkeys),
      K(task_count_by_partition));
  return ret;
}

int ObGranuleUtil::remove_false_range(const common::ObIArray<common::ObNewRange>& in_ranges,
    common::ObIArray<common::ObNewRange>& ranges, bool& only_false_range)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < in_ranges.count() && OB_SUCC(ret); ++i) {
    if (!in_ranges.at(i).is_false_range()) {
      if (OB_FAIL(ranges.push_back(in_ranges.at(i)))) {
        LOG_WARN("fail to push back ranges", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && ranges.empty()) {
    if (OB_FAIL(ranges.assign(in_ranges))) {
      LOG_WARN("failed to assign ranges", K(ret));
    } else {
      only_false_range = true;
    }
  }
  return ret;
}

int ObGranuleUtil::split_block_granule(ObIAllocator& allocator, const ObIArray<common::ObNewRange>& input_ranges,
    const ObIArray<ObPartitionKey>& pkeys, storage::ObPartitionService& partition_service, int64_t parallelism,
    int64_t tablet_size, common::ObIArray<common::ObNewRange>& tasks_ranges, common::ObIArray<int64_t>& tasks_offsets,
    common::ObIArray<int64_t>& tasks_partition_offsets)
{

  //  the step for split task by block granule method:
  //  1. check the validity of input parameters
  //  2. get size for each partition, and calc the total size for all partitions
  //  3. calculate the total number of tasks
  //  4. each partition gets its number of tasks by the weight of partition data in the total data
  //  5. calculate task ranges for each partition, and get the result

  int ret = OB_SUCCESS;
  // 1. check the validity of input parameters
  if (input_ranges.count() < 1 || pkeys.count() < 1 || parallelism < 1 || tablet_size < 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the invalid argument", K(ret), K(input_ranges.count()), K(pkeys.count()), K(parallelism), K(tablet_size));
  }

  // 2. get size for each partition, and calc the total size for all partitions
  common::ObSEArray<int64_t, 16> size_each_partitions;
  int64_t total_size = 0;
  int64_t empty_partition_cnt = 0;
  ObSEArray<ObStoreRange, 16> input_store_ranges;
  if (OB_SUCC(ret)) {
    // convert ObNewRange array to ObStoreRange array
    ObStoreRange store_range;
    for (int64_t i = 0; OB_SUCC(ret) && i < input_ranges.count(); i++) {
      store_range.assign(input_ranges.at(i));
      if (OB_FAIL(input_store_ranges.push_back(store_range))) {
        LOG_WARN("failed to push back input store range", K(ret));
      }
    }
    for (int i = 0; i < pkeys.count() && OB_SUCC(ret); i++) {
      ObPartitionKey partition_key = pkeys.at(i);
      int64_t partition_size = 0;
      // get partition size from storage
      if (OB_FAIL(partition_service.get_multi_ranges_cost(partition_key, input_store_ranges, partition_size))) {
        LOG_WARN("failed to get multi ranges cost", K(ret), K(partition_key));
      } else {
        // B to MB
        partition_size = partition_size / 1024 / 1024;
      }

      if (OB_SUCC(ret)) {
        if (partition_size == 0) {
          empty_partition_cnt++;
        }
        if (OB_FAIL(size_each_partitions.push_back(partition_size))) {
          LOG_WARN("failed to push partition size", K(ret));
        } else {
          total_size += partition_size;
        }
      }
    }
  }

  // 3. calc the total number of tasks for all partitions
  int64_t esti_task_cnt_by_data_size = 0;
  if (OB_SUCC(ret)) {
    ObParallelBlockRangeTaskParams params;
    params.parallelism_ = parallelism;
    params.expected_task_load_ = tablet_size / 1024 / 1024;
    if (OB_FAIL(compute_total_task_count(params, total_size, esti_task_cnt_by_data_size))) {
      LOG_WARN("compute task count failed", K(ret));
    } else {
      esti_task_cnt_by_data_size += empty_partition_cnt;
      // Ensure that the total task count is greater than or equal to the number of partitions
      if (esti_task_cnt_by_data_size < pkeys.count()) {
        esti_task_cnt_by_data_size = pkeys.count();
      }
    }
  }

  // 4. split the total number of tasks into each partition
  common::ObSEArray<int64_t, 16> task_cnt_each_partitions;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(compute_task_count_each_partition(
            total_size, esti_task_cnt_by_data_size, size_each_partitions, task_cnt_each_partitions))) {
      LOG_WARN("failed to compute task count for each partition", K(ret));
    }
  }

  // 5. calc task ranges for each partition, and get the result
  if (OB_SUCC(ret)) {
    for (int i = 0; i < pkeys.count() && OB_SUCC(ret); i++) {
      ObPartitionKey partition_key = pkeys.at(i);
      int64_t expected_task_cnt = task_cnt_each_partitions.at(i);
      // split input ranges to n task by PG interface
      if (OB_FAIL(get_tasks_for_partition(allocator,
              expected_task_cnt,
              partition_key,
              partition_service,
              input_store_ranges,
              tasks_ranges,
              tasks_offsets,
              tasks_partition_offsets))) {
        LOG_WARN("failed to get tasks for partition", K(ret));
      } else {
        LOG_TRACE("get tasks for partition",
            K(ret),
            K(partition_key),
            K(tasks_ranges.count()),
            K(tasks_offsets),
            K(tasks_partition_offsets));
      }
    }
    if (OB_SUCC(ret)) {
      if (tasks_ranges.empty() || tasks_offsets.empty() || tasks_partition_offsets.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the ranges or offsets are empty", K(ret), K(tasks_ranges.empty()), K(tasks_offsets.empty()));
      }
    }
  }
  return ret;
}

int ObGranuleUtil::compute_total_task_count(
    const ObParallelBlockRangeTaskParams& params, int64_t total_size, int64_t& total_task_count)
{
  int ret = OB_SUCCESS;
  int64_t tmp_total_task_count = -1;
  if (OB_FAIL(params.valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params is invalid", K(ret));
  } else {
    // total size
    int64_t total_access_size = total_size;
    // default value is 2 MB
    int64_t min_task_access_size = NON_ZERO_VALUE(params.min_task_access_size_);
    // default value of expected_task_load_ is 128 MB
    int64_t expected_task_load = max(params.expected_task_load_, min_task_access_size);

    // lower bound size: dop*128M*13
    int64_t lower_bound_size = params.parallelism_ * expected_task_load * params.min_task_count_per_thread_;
    // hight bound size: dop*128M*100
    int64_t upper_bound_size = params.parallelism_ * expected_task_load * params.max_task_count_per_thread_;

    if (total_access_size < 0 || lower_bound_size < 0 || upper_bound_size < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("params is invalid", K(total_access_size), K(lower_bound_size), K(upper_bound_size), K(params));
    } else if (total_access_size < lower_bound_size) {
      // the data size is less than lower bound size
      // when the amount of data is small,
      // more tasks can easily achieve better dynamic load balancing
      tmp_total_task_count =
          min(params.min_task_count_per_thread_ * params.parallelism_, total_access_size / min_task_access_size);
      tmp_total_task_count = max(tmp_total_task_count, total_access_size / expected_task_load);
      LOG_TRACE("the data is less than lower bound size", K(ret), K(tmp_total_task_count));
    } else if (total_access_size > upper_bound_size) {
      // the data size is greater than upper bound size
      tmp_total_task_count = params.max_task_count_per_thread_ * params.parallelism_;
      LOG_TRACE("the data size is greater upper bound size", K(ret), K(tmp_total_task_count));
    } else {
      // the data size is between lower bound size and upper bound size
      tmp_total_task_count = total_access_size / expected_task_load;
      LOG_TRACE("the data size is between lower bound size and upper bound size", K(ret), K(tmp_total_task_count));
    }
  }
  if (OB_SUCC(ret)) {
    // the result of task count must be greater than or equal to zero
    total_task_count = tmp_total_task_count;
  }
  return ret;
}

int ObGranuleUtil::compute_task_count_each_partition(int64_t total_size, int64_t total_task_cnt,
    const common::ObIArray<int64_t>& size_each_partition, common::ObIArray<int64_t>& task_cnt_each_partition)
{
  int ret = OB_SUCCESS;
  // must ensure at least one task per partition.
  if (total_size <= 0 || total_task_cnt == size_each_partition.count()) {
    // if the total count of tasks is equal to the number of partitions,
    // each partition just has one task.
    for (int i = 0; i < size_each_partition.count() && OB_SUCC(ret); i++) {
      // only one task for each partition
      if (OB_FAIL(task_cnt_each_partition.push_back(1))) {
        LOG_WARN("failed to push back array", K(ret));
      }
    }
    LOG_TRACE("compute task count for each partition, each partition has only one task", K(ret));
  } else {
    // allocate task count for each partition by the weight of partition data in the total data
    int64_t alloc_task_cnt = 0;
    for (int i = 0; i < size_each_partition.count() && OB_SUCC(ret); i++) {
      int64_t partition_size = size_each_partition.at(i);
      int64_t task_cnt = ((double)partition_size / (double)total_size) * total_task_cnt;
      // if the data volume of a partition is very small, but it still needs a task.
      if (task_cnt == 0) {
        task_cnt = 1;
      }
      alloc_task_cnt += task_cnt;
      if (OB_FAIL(task_cnt_each_partition.push_back(task_cnt))) {
        LOG_WARN("failed to push task cnt", K(ret));
      }
    }
    LOG_TRACE("compute task count for partition, allocate task count", K(ret), K(alloc_task_cnt), K(total_task_cnt));
  }
  // check the size of task_cnt_each_partition array
  if (OB_SUCC(ret) && task_cnt_each_partition.count() != size_each_partition.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the size of task count each partition is not right",
        K(ret),
        K(size_each_partition.count()),
        K(task_cnt_each_partition.count()));
  }
  // check the returned result
  for (int i = 0; i < task_cnt_each_partition.count() && OB_SUCC(ret); i++) {
    if (task_cnt_each_partition.at(i) < 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the partition has error task number", K(ret), K(task_cnt_each_partition.at(i)));
    }
  }

  return ret;
}

int ObGranuleUtil::get_tasks_for_partition(ObIAllocator& allocator, int64_t expected_task_cnt, ObPartitionKey& pkey,
    storage::ObPartitionService& partition_service, ObIArray<ObStoreRange>& input_storage_ranges,
    ObIArray<ObNewRange>& tasks_ranges, ObIArray<int64_t>& tasks_offsets, ObIArray<int64_t>& tasks_partition_offsets)
{
  int ret = OB_SUCCESS;
  ObArrayArray<ObStoreRange> multi_range_split_array;
  if (expected_task_cnt < 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(expected_task_cnt));
  } else if (expected_task_cnt == 1) {
    // no need to split the input_ranges, if the expected count of task.
    for (int i = 0; i < input_storage_ranges.count() && OB_SUCC(ret); i++) {
      ObNewRange new_range;
      input_storage_ranges.at(i).to_new_range(new_range);
      if (OB_FAIL(tasks_ranges.push_back(new_range))) {
        LOG_WARN("failed to push back range", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(tasks_offsets.push_back(tasks_ranges.count() - 1))) {
        LOG_WARN("failed to push back offset", K(ret));
      } else if (OB_FAIL(tasks_partition_offsets.push_back(tasks_ranges.count() - 1))) {
        LOG_WARN("failed to push back partition offset", K(ret));
      }
    }
  } else if (OB_FAIL(partition_service.split_multi_ranges(
                 pkey, input_storage_ranges, expected_task_cnt, allocator, multi_range_split_array))) {
    LOG_WARN("failed to split multi ranges", K(ret), K(pkey), K(expected_task_cnt));
  } else {
    LOG_TRACE("split multi ranges",
        K(ret),
        K(pkey),
        K(input_storage_ranges),
        K(expected_task_cnt == multi_range_split_array.count()),
        K(multi_range_split_array));
    // convert ObStoreRange array to ObNewRange array
    for (int i = 0; i < multi_range_split_array.count() && OB_SUCC(ret); i++) {
      ObIArray<ObStoreRange>& storage_task_ranges = multi_range_split_array.at(i);
      for (int j = 0; j < storage_task_ranges.count() && OB_SUCC(ret); j++) {
        ObNewRange new_range;
        storage_task_ranges.at(j).to_new_range(new_range);
        if (OB_INVALID_INDEX == new_range.table_id_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid table id", K(ret), K(new_range), K(multi_range_split_array.at(i)));
        } else if (OB_FAIL(tasks_ranges.push_back(new_range))) {
          LOG_WARN("failed to push back new task range", K(ret), K(new_range));
        }
      }
      // finish one task ranges, push the task offset
      if (OB_SUCC(ret)) {
        if (OB_FAIL(tasks_offsets.push_back(tasks_ranges.count() - 1))) {
          LOG_WARN("failed to push back tasks offset", K(ret));
        }
      }
    }
    // finish the whole partition ranges, push the partition task offset
    if (OB_SUCC(ret)) {
      if (OB_FAIL(tasks_partition_offsets.push_back(tasks_ranges.count() - 1))) {
        LOG_WARN("failed to push back tasks partition offset", K(ret));
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
