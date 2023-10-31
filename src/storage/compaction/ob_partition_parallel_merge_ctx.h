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

#ifndef OB_PARTITION_PARALLEL_MERGE_CTX_H
#define OB_PARTITION_PARALLEL_MERGE_CTX_H

#include "storage/ob_storage_struct.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/container/ob_heap.h"
#include "common/rowkey/ob_rowkey.h"
#include "storage/blocksstable/ob_datum_range.h"

namespace oceanbase
{

namespace compaction
{
struct ObBasicTabletMergeCtx;
struct ObMediumCompactionInfo;
}
namespace blocksstable
{
class ObSSTable;
}

namespace storage
{

class ObParallelMergeCtx
{
public:
  static const int64_t MAX_MERGE_THREAD = 64;
  enum ParallelMergeType {
    PARALLEL_MAJOR = 0,
    PARALLEL_MINI = 1,
    PARALLEL_MINOR = 2,
    SERIALIZE_MERGE = 3,
    INVALID_PARALLEL_TYPE
  };
  ObParallelMergeCtx(common::ObIAllocator &allocator);
  virtual ~ObParallelMergeCtx();
  void reset();
  bool is_valid() const;
  int init(compaction::ObBasicTabletMergeCtx &merge_ctx);
  int init(const compaction::ObMediumCompactionInfo &medium_info);
  OB_INLINE int64_t get_concurrent_cnt() const { return concurrent_cnt_; }
  int get_merge_range(const int64_t parallel_idx, blocksstable::ObDatumRange &merge_range);
  static int get_concurrent_cnt(
      const int64_t tablet_size,
      const int64_t macro_block_cnt,
      int64_t &concurrent_cnt);
  TO_STRING_KV(K_(parallel_type), "array_cnt", range_array_.count(), K_(range_array), K_(concurrent_cnt), K_(is_inited));
private:
  static const int64_t MIN_PARALLEL_MINOR_MERGE_THREASHOLD = 2;
  static const int64_t MIN_PARALLEL_MERGE_BLOCKS = 32;
  static const int64_t PARALLEL_MERGE_TARGET_TASK_CNT = 20;

  //TODO @hanhui parallel in ai
  int init_serial_merge();
  int init_parallel_mini_merge(compaction::ObBasicTabletMergeCtx &merge_ctx);
  int init_parallel_mini_minor_merge(compaction::ObBasicTabletMergeCtx &merge_ctx);
  int init_parallel_major_merge(compaction::ObBasicTabletMergeCtx &merge_ctx);
  void calc_adaptive_parallel_degree(
      const int64_t prio,
      const int64_t mem_per_thread,
      const int64_t origin_degree,
      int64_t &parallel_degree);

  int get_major_parallel_ranges(
      const blocksstable::ObSSTable *first_major_sstable,
      const int64_t tablet_size,
      const ObITableReadInfo &rowkey_read_info);
private:
  common::ObIAllocator &allocator_;
  common::ObSEArray<blocksstable::ObDatumRange, 16, common::ObIAllocator&> range_array_;
  ParallelMergeType parallel_type_;
  int64_t concurrent_cnt_;
  bool is_inited_;
};

} //storage
} //oceanbase
#endif //OB_PARTITION_PARALLEL_MERGE_CTX_H
