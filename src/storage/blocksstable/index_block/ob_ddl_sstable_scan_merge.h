/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_DDL_SSTABLE_SCAN_MERGE_H
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_DDL_SSTABLE_SCAN_MERGE_H

#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/column_store/ob_column_store_util.h"
#include "ob_index_block_row_struct.h"
#include "storage/access/ob_simple_rows_merger.h"

namespace oceanbase
{
namespace blocksstable
{
struct ObDDLSSTableMergeLoserTreeItem final
{
public:
  ObDDLSSTableMergeLoserTreeItem()
    : equal_with_next_(false),
      is_scan_left_border_(false),
      is_scan_right_border_(false),
      end_key_(),
      header_(nullptr),
      iter_idx_(0),
      agg_buf_size_(0),
      row_offset_(0),
      idx_minor_info_(nullptr),
      agg_row_buf_(nullptr)
  {
  }
  ~ObDDLSSTableMergeLoserTreeItem() = default;
  void reset()
  {
    end_key_.reset();
    header_ = nullptr;
    idx_minor_info_ = nullptr;
    agg_row_buf_ = nullptr;
    iter_idx_ = 0;
    agg_buf_size_ = 0;
    row_offset_ = 0;
    equal_with_next_ = false;
    is_scan_left_border_ = false;
    is_scan_right_border_ = false;
  }
  TO_STRING_KV(K_(equal_with_next), K_(end_key), KPC(header_), K_(iter_idx), K_(is_scan_left_border), K_(is_scan_right_border),
               K_(agg_buf_size), K_(row_offset), KP_(idx_minor_info), KP_(agg_row_buf));
public:
  bool equal_with_next_; // for simple row merger
  bool is_scan_left_border_;
  bool is_scan_right_border_;
  ObCommonDatumRowkey end_key_;
  const blocksstable::ObIndexBlockRowHeader *header_;
  int64_t iter_idx_;
  int64_t agg_buf_size_;
  int64_t row_offset_;
  const ObIndexBlockRowMinorMetaInfo *idx_minor_info_;
  const char *agg_row_buf_;
};

class ObDDLSSTableMergeLoserTreeCompare final
{
public:
  ObDDLSSTableMergeLoserTreeCompare();
  ~ObDDLSSTableMergeLoserTreeCompare();
  void reset();
  int cmp(const ObDDLSSTableMergeLoserTreeItem &lhs,
          const ObDDLSSTableMergeLoserTreeItem &rhs,
          int64_t &cmp_ret);
  TO_STRING_KV(K(reverse_scan_), KPC(datum_utils_));
public:
  bool reverse_scan_;
  const blocksstable::ObStorageDatumUtils *datum_utils_;
};

} // end namespace blocksstable
} // end namespace oceanbase
#endif
