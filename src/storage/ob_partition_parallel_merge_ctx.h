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

namespace oceanbase {
namespace storage {
class ObSSTableMergeCtx;

class ObParallelMergeCtx {
public:
  enum ParallelMergeType {
    PARALLEL_MAJOR = 0,
    PARALLEL_MINI = 1,
    PARALLEL_MINI_MINOR = 2,
    SERIALIZE_MERGE = 3,
    INVALID_PARALLEL_TYPE
  };
  ObParallelMergeCtx();
  virtual ~ObParallelMergeCtx();
  void reset();
  bool is_valid() const;
  int init(ObSSTableMergeCtx& merge_ctx);
  OB_INLINE int64_t get_concurrent_cnt() const
  {
    return concurrent_cnt_;
  }
  int get_merge_range(
      const int64_t parallel_idx, common::ObExtStoreRange& merge_range, common::ObIAllocator& allocator);
  TO_STRING_KV(K_(parallel_type), K_(range_array), K_(first_sstable), K_(concurrent_cnt), K_(is_inited));

private:
  static const int64_t MIN_PARALLEL_MINI_MINOR_MERGE_THREASHOLD = 2;
  static const int64_t MIN_PARALLEL_MERGE_BLOCKS = 32;
  static const int64_t PARALLEL_MERGE_TARGET_TASK_CNT = 20;
  // TODO  parallel in ai
  int init_serial_merge();
  int init_parallel_mini_merge(ObSSTableMergeCtx& merge_ctx);
  int init_parallel_mini_minor_merge(ObSSTableMergeCtx& merge_ctx);
  int init_parallel_major_merge(ObSSTableMergeCtx& merge_ctx);
  int calc_mini_minor_parallel_degree(
      const int64_t tablet_size, const int64_t total_size, const int64_t sstable_count, int64_t& parallel_degree);

private:
  ParallelMergeType parallel_type_;
  common::ObSEArray<common::ObExtStoreRange, 16> range_array_;
  ObSSTable* first_sstable_;
  int64_t concurrent_cnt_;
  common::ObArenaAllocator allocator_;
  bool is_inited_;
};

}  // namespace storage
}  // namespace oceanbase
#endif  // OB_PARTITION_PARALLEL_MERGE_CTX_H
