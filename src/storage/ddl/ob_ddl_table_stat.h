/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_DDL_OB_DDL_TABLE_STAT_H_
#define OCEANBASE_STORAGE_DDL_OB_DDL_TABLE_STAT_H_

#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace compaction
{
struct ObMergeBlockInfo;
}

namespace storage
{

struct ObDDLTableStat final
{
public:
  ObDDLTableStat() : row_count_(0), macro_block_count_(0), micro_block_count_(0) {}
  ~ObDDLTableStat() = default;
  void reset()
  {
    row_count_ = 0;
    macro_block_count_ = 0;
    micro_block_count_ = 0;
  }
  bool is_valid() const
  {
    return row_count_ >= 0 && macro_block_count_ >= 0 && micro_block_count_ >= 0;
  }
  void add(const int64_t row_count,
           const int64_t macro_block_count,
           const int64_t micro_block_count)
  {
    row_count_ += row_count;
    macro_block_count_ += macro_block_count;
    micro_block_count_ += micro_block_count;
  }
  void add(const ObDDLTableStat &other)
  {
    add(other.row_count_, other.macro_block_count_, other.micro_block_count_);
  }
  void atomic_add(const ObDDLTableStat &other);
  void add_merge_block_info(const compaction::ObMergeBlockInfo &merge_block_info);
  TO_STRING_KV(K_(row_count), K_(macro_block_count), K_(micro_block_count));

public:
  int64_t row_count_;
  int64_t macro_block_count_;
  int64_t micro_block_count_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_DDL_OB_DDL_TABLE_STAT_H_
