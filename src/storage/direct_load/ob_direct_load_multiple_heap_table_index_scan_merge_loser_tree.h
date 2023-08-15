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
#pragma once

#include "storage/direct_load/ob_direct_load_multiple_heap_table_index_block.h"

namespace oceanbase
{
namespace storage
{

struct ObDirectLoadMultipleHeapTableIndexScanMergeLoserTreeItem
{
public:
  ObDirectLoadMultipleHeapTableIndexScanMergeLoserTreeItem()
    : index_(nullptr), iter_idx_(0), equal_with_next_(false)
  {
  }
  ~ObDirectLoadMultipleHeapTableIndexScanMergeLoserTreeItem() = default;
  void reset()
  {
    index_ = nullptr;
    iter_idx_ = 0;
    equal_with_next_ = false;
  }
  TO_STRING_KV(KPC_(index), K_(iter_idx), K_(equal_with_next));
public:
  const ObDirectLoadMultipleHeapTableTabletIndex *index_;
  int64_t iter_idx_;
  bool equal_with_next_; // for simple row merger
};

class ObDirectLoadMultipleHeapTableIndexScanMergeLoserTreeCompare
{
public:
  ObDirectLoadMultipleHeapTableIndexScanMergeLoserTreeCompare();
  ~ObDirectLoadMultipleHeapTableIndexScanMergeLoserTreeCompare();
  int cmp(const ObDirectLoadMultipleHeapTableIndexScanMergeLoserTreeItem &lhs,
          const ObDirectLoadMultipleHeapTableIndexScanMergeLoserTreeItem &rhs,
          int64_t &cmp_ret);
};

} // namespace storage
} // namespace oceanbase
