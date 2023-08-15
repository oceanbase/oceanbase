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

#include "storage/direct_load/ob_direct_load_multiple_datum_row.h"

namespace oceanbase
{
namespace blocksstable
{
class ObStorageDatumUtils;
} // namespace blocksstable
namespace storage
{

struct ObDirectLoadMultipleSSTableScanMergeLoserTreeItem
{
public:
  ObDirectLoadMultipleSSTableScanMergeLoserTreeItem()
    : row_(nullptr), iter_idx_(0), equal_with_next_(false)
  {
  }
  ~ObDirectLoadMultipleSSTableScanMergeLoserTreeItem() = default;
  void reset()
  {
    row_ = nullptr;
    iter_idx_ = 0;
    equal_with_next_ = false;
  }
  TO_STRING_KV(KPC_(row), K_(iter_idx), K_(equal_with_next));
public:
  const ObDirectLoadMultipleDatumRow *row_;
  int64_t iter_idx_;
  bool equal_with_next_; // for simple row merger
};

class ObDirectLoadMultipleSSTableScanMergeLoserTreeCompare
{
public:
  ObDirectLoadMultipleSSTableScanMergeLoserTreeCompare();
  ~ObDirectLoadMultipleSSTableScanMergeLoserTreeCompare();
  int init(const blocksstable::ObStorageDatumUtils *datum_utils);
  int cmp(const ObDirectLoadMultipleSSTableScanMergeLoserTreeItem &lhs,
          const ObDirectLoadMultipleSSTableScanMergeLoserTreeItem &rhs,
          int64_t &cmp_ret);
public:
  const blocksstable::ObStorageDatumUtils *datum_utils_;
  blocksstable::ObDatumRowkey lhs_rowkey_;
  blocksstable::ObDatumRowkey rhs_rowkey_;
};

} // namespace storage
} // namespace oceanbase
