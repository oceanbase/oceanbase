/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "storage/ddl/ob_ddl_table_stat.h"
#include "lib/atomic/ob_atomic.h"
#include "storage/compaction/ob_sstable_merge_history.h"

namespace oceanbase
{
namespace storage
{

void ObDDLTableStat::atomic_add(const ObDDLTableStat &other)
{
  ATOMIC_AAF(&row_count_, other.row_count_);
  ATOMIC_AAF(&macro_block_count_, other.macro_block_count_);
  ATOMIC_AAF(&micro_block_count_, other.micro_block_count_);
}

void ObDDLTableStat::add_merge_block_info(
    const compaction::ObMergeBlockInfo &merge_block_info)
{
  add(merge_block_info.total_row_count_,
      merge_block_info.macro_block_count_,
      merge_block_info.new_micro_count_in_new_macro_
          + merge_block_info.multiplexed_micro_count_in_new_macro_);
}

} // namespace storage
} // namespace oceanbase
