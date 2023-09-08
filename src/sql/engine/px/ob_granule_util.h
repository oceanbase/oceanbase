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
#include "sql/das/ob_das_define.h"
#include "sql/ob_sql_define.h"
#include "sql/engine/ob_engine_op_traits.h"

namespace oceanbase
{
namespace share {
struct ObExternalFileInfo;
}
namespace common
{
class ObStoreRange;
}
namespace sql
{

#define NON_ZERO_VALUE(num) \
     ((num == 0) ? 1 : num)

/**
 *    ----JOIN----
 *    |          |
 *  pkey         |
 *    |GI        |GI
 *    A          B
 *  GI above A,B are affinitize.
 */
#define GI_AFFINITIZE                 (1ULL)
/**
*         |GI
*    ----JOIN---
*    |         |
*    A         B
*  Join row by partition pairs.
*/
#define GI_PARTITION_WISE             (1ULL << 1)
/**
*         |
*    ----NLJ----
*    |         |
*  B2HOST      |
*    |         |GI(access all)
*    A         B
* Scan all partition for every row from left.
*/
#define GI_ACCESS_ALL                 (1ULL << 2)
// Unused. 这个本意是用于 bloom filter 场景，用于过滤向上吐出的数据
// GI 维护了一个 partition filter，吐出的每一行如果不在 partition 名单中就过滤
// 这个是 Bloom Filter 的增强版
#define GI_USE_PARTITION_FILTER       (1ULL << 3)
/**
*         |
*    ----NLJ----
*    |         |
*  Bcast       |
*    |         |GI(nlj_param_down)
*    A         B
* Get a partition granule.
*/
#define GI_NLJ_PARAM_DOWN             (1ULL << 4)
// Sort partition in asc partition order.
#define GI_ASC_ORDER        (1ULL << 5)
// Sort partition in desc order.
#define GI_DESC_ORDER       (1ULL << 6)
// Force to do partition
#define GI_FORCE_PARTITION_GRANULE    (1ULL << 7)
// divide task into groups（slave mapping）
#define GI_SLAVE_MAPPING              (1ULL << 8)
// 通知 GI 使用 partition pruning 模式，只处理特定分区，其余分区都裁剪掉
#define GI_ENABLE_PARTITION_PRUNING (1ULL << 9)

class ObTablePartitionInfo;
class ObGranulePump;

enum ObGranuleType
{
  OB_GRANULE_UNINITIALIZED,
  // task中的granule为partition+range
  OB_BLOCK_RANGE_GRANULE,
  OB_PARTITION_GRANULE
};

// the params used to decide the load of every task
struct ObParallelBlockRangeTaskParams
{
  ObParallelBlockRangeTaskParams() :
    parallelism_(0),
    expected_task_load_(sql::OB_EXPECTED_TASK_LOAD),
    min_task_count_per_thread_(sql::OB_MIN_PARALLEL_TASK_COUNT),
    max_task_count_per_thread_(sql::OB_MAX_PARALLEL_TASK_COUNT),
    min_task_access_size_(GCONF.px_task_size >> 20),
    marcos_count_(0)
  { }
  virtual ~ObParallelBlockRangeTaskParams()
  { }
  void reset();
  int valid() const;
  TO_STRING_KV(K(parallelism_), K(expected_task_load_), K(min_task_count_per_thread_), K(max_task_count_per_thread_), K(min_task_access_size_), K(marcos_count_));
  /* 并行度 */
  int64_t parallelism_;
  /**
   * 单位为MB
   * 默认100，意味期待一个任务从磁盘读取100M的数据。
   * 目前使用时都使用tablet size，而不是使用默认值。
   */
  int64_t expected_task_load_;
  /**
   * 单位为个
   * 期望每个线程持有的最小任务数，默认为魔数13
   */
  int64_t min_task_count_per_thread_;
  /**
   * 单位为个
   * 期望每个线程持有的最大任务数，默认为魔数100
   */
  int64_t max_task_count_per_thread_;
  /**
   * 单位为MB
   * 每个任务最小的负责数据量，默认为一个微块，2M。
   * 可以通过系统项来改变这个值。
   */
  int64_t min_task_access_size_;
  /**
   * 总的宏块个数
   */
  uint64_t marcos_count_;
};


class ObGranuleUtil
{
public:
  /**
   *  table_partition_info  IN    partition info
   *  parallelism           IN    the parallelism from hint
   *  type                  OUT   granule type
   *
   */
  static int get_granule_type(const ObTablePartitionInfo *table_partition_info,
                              int64_t parallelism,
                              ObGranuleType &type);
  /**
   * allocator                  IN  memory allocator
   * ranges                     IN  ranges from the query range(by optimizer)
   * pkeys                      IN  partition keys
   * das                        IN  data access service
   * parallelism                IN  the parallelism from hint or optimizer
   * tablet_size                IN  the tablet size will deside the workload
   * force_partition_granule    IN  force to be partition granule iterator
   * granule_pkeys              OUT the pkey info of granule_ranges
   * granule_ranges             OUT the ranges info include ranges
   * granule_idx                OUT the idx used to divide the granule ranges
   * range_independent          IN  the random type witch affects the granule_idx
   *
   */
  static int split_block_ranges(ObExecContext &exec_ctx,
                                common::ObIAllocator &allocator,
                                const ObTableScanSpec *tsc,
                                const common::ObIArray<common::ObNewRange> &ranges,
                                const common::ObIArray<ObDASTabletLoc*> &tablets,
                                int64_t parallelism,
                                int64_t tablet_size,
                                bool force_partition_granule,
                                common::ObIArray<ObDASTabletLoc*> &granule_tablets,
                                common::ObIArray<common::ObNewRange> &granule_ranges,
                                common::ObIArray<int64_t> &granule_idx,
                                bool range_independent);

  static bool is_partition_granule(int64_t partition_count,
                                   int64_t parallelism,
                                   int64_t partition_scan_hold,
                                   int64_t hash_partition_scan_hold,
                                   bool hash_part);

  static bool gi_has_attri(uint64_t bit_map, uint64_t attri) { return 0 != (bit_map & attri); }

  static bool is_partition_task_mode(uint64_t gi_attri_flag)
  {
    return affinitize(gi_attri_flag) || pwj_gi(gi_attri_flag) ||
           access_all(gi_attri_flag) || with_param_down(gi_attri_flag);
  }
  static bool is_partition_granule_flag(uint64_t gi_attri_flag)
  {
    return is_partition_task_mode(gi_attri_flag) ||
           ObGranuleUtil::gi_has_attri(gi_attri_flag, GI_FORCE_PARTITION_GRANULE);
  }
  static bool partition_filter(uint64_t gi_attri_flag)
  {
    return gi_has_attri(gi_attri_flag, GI_USE_PARTITION_FILTER);
  }
  static bool pwj_gi(uint64_t gi_attri_flag)
  {
    return gi_has_attri(gi_attri_flag, GI_PARTITION_WISE);
  }
  static bool affinitize(uint64_t gi_attri_flag)
  {
    return gi_has_attri(gi_attri_flag, GI_AFFINITIZE);
  }
  static bool access_all(uint64_t gi_attri_flag)
  {
    return gi_has_attri(gi_attri_flag, GI_ACCESS_ALL);
  }
  static bool with_param_down(uint64_t gi_attri_flag)
  {
    return gi_has_attri(gi_attri_flag, GI_NLJ_PARAM_DOWN);
  }
  static bool asc_order(uint64_t gi_attri_flag)
  {
    return gi_has_attri(gi_attri_flag, GI_ASC_ORDER);
  }
  static bool desc_order(uint64_t gi_attri_flag)
  {
    return gi_has_attri(gi_attri_flag, GI_DESC_ORDER);
  }
  static bool force_partition_granule(uint64_t gi_attri_flag)
  {
    return gi_has_attri(gi_attri_flag, GI_FORCE_PARTITION_GRANULE);
  }
  static bool enable_partition_pruning(uint64_t gi_attri_flag)
  {
    return gi_has_attri(gi_attri_flag, GI_ENABLE_PARTITION_PRUNING);
  }


  static int remove_empty_range(const common::ObIArray<common::ObNewRange> &in_ranges,
                                common::ObIArray<common::ObNewRange> &ranges,
                                bool &only_empty_range);
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
   * granule_pkeys               OUT the pkey info of granule_ranges
   * granule_ranges              OUT the ranges info include ranges
   * granule_idx                 OUT the idx used to divide the granule ranges
   * range_independent           IN  the random type witch affects the granule_idx
   *
   */
  static int split_block_granule(ObExecContext &exec_ctx,
                                common::ObIAllocator &allocator,
                                const ObTableScanSpec *tsc,
                                const common::ObIArray<common::ObNewRange> &input_ranges,
                                const common::ObIArray<ObDASTabletLoc*> &tablet_array,
                                int64_t parallelism,
                                int64_t tablet_size,
                                common::ObIArray<ObDASTabletLoc*> &granule_tablets,
                                common::ObIArray<common::ObNewRange> &granule_ranges,
                                common::ObIArray<int64_t> &granule_idx,
                                bool range_independent);


  static int split_granule_for_external_table(common::ObIAllocator &allocator,
                                              const ObTableScanSpec *tsc,
                                              const common::ObIArray<common::ObNewRange> &input_ranges,
                                              const common::ObIArray<ObDASTabletLoc*> &tablet_array,
                                              const common::ObIArray<share::ObExternalFileInfo> &external_table_files,
                                              int64_t parallelism,
                                              common::ObIArray<ObDASTabletLoc*> &granule_tablets,
                                              common::ObIArray<common::ObNewRange> &granule_ranges,
                                              common::ObIArray<int64_t> &granule_idx);

  /**
   * get the total task count for all partitions
   * params                     IN the parameters for splitting
   * total_size                 IN the estimated size for all partitions
   *
   * total_task_count           OUT the expected total count for tasks
   */
  static int compute_total_task_count(const ObParallelBlockRangeTaskParams &params,
                                int64_t total_size,
                                int64_t &total_task_count);

private:
  /**
   * calc task count for each partition by the weight of partition data in the total data
   * total_size                 IN size for total data
   * total_task_size            IN the expected total count of tasks which will be splitted
   *
   * task_cnt_each_partition    OUT task count for each partition
   */
  static int compute_task_count_each_partition(int64_t total_size,
                                               int64_t total_task_cnt,
                                               const common::ObIArray<int64_t> &size_each_partition,
                                               common::ObIArray<int64_t> &task_cnt_each_partition);

  /**
   * get the splitted tasks for each partition
   * allocator                   IN  memory allocator
   * expected_task_cnt           IN  the expected count of tasks for this partition
   * pkey                        IN  the identifier for partition
   * partition_service           IN  utils for splitting tasks
   * input_storage_ranges        IN  query ranges extracted in optimizer stage
   *
   * granule_pkeys               OUT the pkey info of granule_ranges
   * granule_ranges              OUT the ranges info include ranges
   * granule_idx                 OUT the idx used to divide the granule ranges
   * pkey_idx                    OUT the idx in granule ranges
   * range_independent           IN  the random type witch affects the granule_idx
   */
  static int get_tasks_for_partition(ObExecContext &exec_ctx,
                                     common::ObIAllocator &allocator,
                                     int64_t expected_task_cnt,
                                     ObDASTabletLoc &tablet,
                                     common::ObIArray<common::ObStoreRange> &input_storage_ranges,
                                     common::ObIArray<ObDASTabletLoc*> &tablet_array,
                                     common::ObIArray<common::ObNewRange> &granule_ranges,
                                     common::ObIArray<int64_t> &granule_idx,
                                     int64_t &pkey_idx,
                                     bool range_independent);

  static int convert_new_range_to_store_range(common::ObIAllocator &allocator,
                                              const ObTableScanSpec *tsc,
                                              const common::ObTabletID &tablet_id,
                                              const common::ObIArray<common::ObNewRange> &input_ranges,
                                              common::ObIArray<common::ObStoreRange> &input_store_ranges,
                                              bool &need_convert_new_range);
};


}//sql
}//oceanbase

#endif
