/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_HA_SMALL_SSTABLE_WRITE_OPT_H_
#define OCEANBASE_STORAGE_HA_SMALL_SSTABLE_WRITE_OPT_H_

#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace storage
{

// Inputs for ObStorageHAMacroBlockWriter small-sstable flush optimization in physical copy.
struct ObStorageHASmallSSTableWriteOpt final
{
public:
  ObStorageHASmallSSTableWriteOpt()
    : copy_task_concurrent_cnt_(0),
      range_macro_block_cnt_(0)
  {
  }
  ObStorageHASmallSSTableWriteOpt(const int64_t copy_task_concurrent_cnt, const int64_t range_macro_block_cnt)
    : copy_task_concurrent_cnt_(copy_task_concurrent_cnt),
      range_macro_block_cnt_(range_macro_block_cnt)
  {
  }
  ~ObStorageHASmallSSTableWriteOpt()
  {
    reset();
  }
  OB_INLINE void reset()
  {
    copy_task_concurrent_cnt_ = 0;
    range_macro_block_cnt_ = 0;
  }
  OB_INLINE bool is_valid() const
  {
    return copy_task_concurrent_cnt_ > 0 && range_macro_block_cnt_ > 0;
  }
  ObStorageHASmallSSTableWriteOpt& operator =(const ObStorageHASmallSSTableWriteOpt &other)
  {
    copy_task_concurrent_cnt_ = other.copy_task_concurrent_cnt_;
    range_macro_block_cnt_ = other.range_macro_block_cnt_;
    return *this;
  }
  TO_STRING_KV(K_(copy_task_concurrent_cnt), K_(range_macro_block_cnt));
  // Concurrent degree of the physical copy task DAG
  int64_t copy_task_concurrent_cnt_;
  // Number of macro blocks in this writer's copy range; small-sstable path applies when count is 1.
  int64_t range_macro_block_cnt_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_HA_SMALL_SSTABLE_WRITE_OPT_H_
