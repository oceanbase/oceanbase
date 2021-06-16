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

#ifndef OB_EXECUTOR_UTIL_H_
#define OB_EXECUTOR_UTIL_H_

#include "share/config/ob_server_config.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/utility.h"
#include "common/ob_partition_key.h"
#include "sql/ob_sql_define.h"

namespace oceanbase {
namespace common {
class ObIDataAccessService;
class ObStoreRange;
}  // namespace common
namespace storage {
class ObPartitionService;
}
namespace sql {

#define NON_ZERO_VALUE(num) ((num == 0) ? 1 : num)

/**
 *    ----JOIN----
 *    |          |
 *  pkey         |
 *    |GI        |GI
 *    A          B
 *  GI above A,B are affinitize.
 */
#define GI_AFFINITIZE (1ULL)
/**
 *         |GI
 *    ----JOIN---
 *    |         |
 *    A         B
 *  Join row by partition pairs.
 */
#define GI_PARTITION_WISE (1ULL << 1)
/**
 *         |
 *    ----NLJ----
 *    |         |
 *  B2HOST      |
 *    |         |GI(access all)
 *    A         B
 * Scan all partition for every row from left.
 */
#define GI_ACCESS_ALL (1ULL << 2)
// This is intended to be used in bloom filter scenarios to filter the data spit out upwards
// GI maintains a partition filter, and each line spit out is filtered if it is not in the partition list
// This is an enhanced of Bloom Filter
#define GI_USE_PARTITION_FILTER (1ULL << 3)
/**
 *         |
 *    ----NLJ----
 *    |         |
 *  Bcast       |
 *    |         |GI(nlj_param_down)
 *    A         B
 * Get a partition granule.
 */
#define GI_NLJ_PARAM_DOWN (1ULL << 4)
// Sort partition in asc partition order.
#define GI_ASC_PARTITION_ORDER (1ULL << 5)
// Sort partition in desc order.
#define GI_DESC_PARTITION_ORDER (1ULL << 6)
// Force to do partition
#define GI_FORCE_PARTITION_GRANULE (1ULL << 7)
// divide task into groups (slave mapping)
#define GI_SLAVE_MAPPING (1ULL << 8)
// Notify GI to use partition pruning mode, only process specific partitions
// and all other partitions will be cropped
#define GI_ENABLE_PARTITION_PRUNING (1ULL << 9)

class ObTablePartitionInfo;
class ObGranulePump;

enum ObGranuleType { OB_GRANULE_UNINITIALIZED, OB_BLOCK_RANGE_GRANULE, OB_PARTITION_GRANULE };

// the params used to decide the load of every task
struct ObParallelBlockRangeTaskParams {
  ObParallelBlockRangeTaskParams()
      : parallelism_(0),
        expected_task_load_(sql::OB_EXPECTED_TASK_LOAD),
        min_task_count_per_thread_(sql::OB_MIN_PARALLEL_TASK_COUNT),
        max_task_count_per_thread_(sql::OB_MAX_PARALLEL_TASK_COUNT),
        min_task_access_size_(GCONF.px_task_size >> 20),
        marcos_count_(0)
  {}
  virtual ~ObParallelBlockRangeTaskParams()
  {}
  void reset();
  int valid() const;
  TO_STRING_KV(K(parallelism_), K(expected_task_load_), K(min_task_count_per_thread_), K(max_task_count_per_thread_),
      K(min_task_access_size_), K(marcos_count_));
  int64_t parallelism_;
  /**
   * The unit is MB
   * The default is 100, expect a task to read 100M data from the disk.
   * Currently use tablet size instead of default value
   */
  int64_t expected_task_load_;
  /**
   * Expect the minimum number of tasks held by each thread, the default is 13
   */
  int64_t min_task_count_per_thread_;
  /**
   * Expect the maximum number of tasks held by each thread, the default is 100
   */
  int64_t max_task_count_per_thread_;
  /**
   * Each task is responsible for the smallest amount of data, the default is a micro-block
   * This value can be changed through the system item
   */
  int64_t min_task_access_size_;
  uint64_t marcos_count_;
};

class ObGranuleUtil {
public:
  /**
   *
   * params             IN    we got total task count by rules; the rule needs these params
   * marcos_count       IN    all macros count involved in
   * total_task_count   OUT   suggested task count
   */
  static int compute_task_count(
      const ObParallelBlockRangeTaskParams& params, uint64_t marcos_count, int64_t& total_task_count);

  /**
   *  table_partition_info  IN    partition info
   *  parallelism           IN    the parallelism from hint
   *  type                  OUT   granule type
   *
   */
  static int get_granule_type(
      const ObTablePartitionInfo* table_partition_info, int64_t parallelism, ObGranuleType& type);
  /**
   * allocator                  IN  memory allocator
   * ranges                     IN  ranges from the query range(by optimizer)
   * pkeys                      IN  partition keys
   * das                        IN  data access service
   * parallelism                IN  the parallelism from hint or optimizer
   * tablet_size                IN  the tablet size will deside the workload
   * force_partition_granule    IN  force to be partition granule iterator
   * granule_ranges             OUT the ranges info include ranges
   * offsets                    OUT the offset used to divide the granule ranges
   * partition_offsets          OUT splitted_ranges include all partition ranges info, so we need
   *                                partition_offsets to assign these ranges into every partition
   *
   */
  static int split_block_ranges(common::ObIAllocator& allocator, const common::ObIArray<common::ObNewRange>& ranges,
      const common::ObIArray<common::ObPartitionKey>& pkeys, storage::ObPartitionService& partition_service,
      int64_t parallelism, int64_t tablet_size, bool force_partition_granule,
      common::ObIArray<common::ObNewRange>& granule_ranges, common::ObIArray<int64_t>& offsets,
      common::ObIArray<int64_t>& partition_offsets);

  static bool is_partition_granule(int64_t partition_count, int64_t parallelism, int64_t partition_scan_hold,
      int64_t hash_partition_scan_hold, bool hash_part);

  static bool gi_has_attri(uint64_t bit_map, uint64_t attri)
  {
    return 0 != (bit_map & attri);
  }
  static bool partition_task_mode(uint64_t gi_attri_flag)
  {
    return ObGranuleUtil::gi_has_attri(gi_attri_flag, GI_PARTITION_WISE) ||
           ObGranuleUtil::gi_has_attri(gi_attri_flag, GI_AFFINITIZE) ||
           ObGranuleUtil::gi_has_attri(gi_attri_flag, GI_ACCESS_ALL) ||
           ObGranuleUtil::gi_has_attri(gi_attri_flag, GI_NLJ_PARAM_DOWN) ||
           ObGranuleUtil::gi_has_attri(gi_attri_flag, GI_ASC_PARTITION_ORDER) ||
           ObGranuleUtil::gi_has_attri(gi_attri_flag, GI_DESC_PARTITION_ORDER) ||
           ObGranuleUtil::gi_has_attri(gi_attri_flag, GI_FORCE_PARTITION_GRANULE);
  }
  static int remove_false_range(const common::ObIArray<common::ObNewRange>& in_ranges,
      common::ObIArray<common::ObNewRange>& ranges, bool& only_false_range);

public:
  /**
   * split tasks by block granule method
   * allocator                   IN  memory allocator
   * input_ranges                IN  query ranges extracted in optimizer stage
   * pkeys                       IN  the key of all partitions belonging to the query
   * partition_service           IN  utils for spliting tasks
   * parallelism                 IN  the parallelism which should be greater than 1
   * tablet_size                 IN  the expected size for each task,
   *                                  which default value is OB_DEFAULT_TABLET_SIZE(128MB)
   *
   * tasks_ranges                OUT the task ranges after being split
   *                                  which are order by partition key
   * tasks_offsets               OUT end offset corresponding to the tasks_ranges for each task
   * tasks_partition_offsets     OUT end offset corresponding to the tasks_ranges for each partition
   *
   */
  static int split_block_granule(common::ObIAllocator& allocator,
      const common::ObIArray<common::ObNewRange>& input_ranges, const common::ObIArray<common::ObPartitionKey>& pkeys,
      storage::ObPartitionService& partition_service, int64_t parallelism, int64_t tablet_size,
      common::ObIArray<common::ObNewRange>& tasks_ranges, common::ObIArray<int64_t>& tasks_offsets,
      common::ObIArray<int64_t>& tasks_partition_offsets);

private:
  /**
   * get the total task count for all partitions
   * params                     IN the parameters for splitting
   * total_size                 IN the estimated size for all partitions
   *
   * total_task_count           OUT the expected total count for tasks
   */
  static int compute_total_task_count(
      const ObParallelBlockRangeTaskParams& params, int64_t total_size, int64_t& total_task_count);

  /**
   * calc task count for each partition by the weight of partition data in the total data
   * total_size                 IN size for total data
   * total_task_size            IN the expected total count of tasks which will be splitted
   *
   * task_cnt_each_partition    OUT task count for each partition
   */
  static int compute_task_count_each_partition(int64_t total_size, int64_t total_task_cnt,
      const common::ObIArray<int64_t>& size_each_partition, common::ObIArray<int64_t>& task_cnt_each_partition);

  /**
   * get the splitted tasks for each partition
   * allocator                   IN  memory allocator
   * expected_task_cnt           IN  the expected count of tasks for this partition
   * pkey                        IN  the identifier for partition
   * partition_service           IN  utils for splitting tasks
   * input_storage_ranges        IN  query ranges extracted in optimizer stage
   *
   * tasks_ranges                OUT the task ranges after being split which are order by partition key
   * tasks_offsets               OUT end offset corresponding to the tasks_ranges for each task
   * tasks_partition_offsets     OUT end offset corresponding to the tasks_ranges for each partition
   */
  static int get_tasks_for_partition(common::ObIAllocator& allocator, int64_t expected_task_cnt,
      common::ObPartitionKey& pkey, storage::ObPartitionService& partition_service,
      common::ObIArray<common::ObStoreRange>& input_storage_ranges, common::ObIArray<common::ObNewRange>& tasks_ranges,
      common::ObIArray<int64_t>& tasks_offsets, common::ObIArray<int64_t>& tasks_partition_offsets);
};

}  // namespace sql
}  // namespace oceanbase

#endif
