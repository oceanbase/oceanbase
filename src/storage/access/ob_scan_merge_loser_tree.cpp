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

#define USING_LOG_PREFIX STORAGE
#include "ob_scan_merge_loser_tree.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
namespace storage
{
void ObScanMergeLoserTreeCmp::reset()
{
  datum_utils_ = nullptr;
  rowkey_size_ = 0;
  reverse_ = false;
  is_inited_ = false;
}

int ObScanMergeLoserTreeCmp::init(const int64_t rowkey_size, const ObStorageDatumUtils &datum_utils, const bool reverse)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (rowkey_size <= 0 || datum_utils.get_rowkey_count() < rowkey_size) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(rowkey_size), K(datum_utils));
  } else {
    datum_utils_ = &datum_utils;
    rowkey_size_ = rowkey_size;
    reverse_ = reverse;
    is_inited_ = true;
  }
  return ret;
}

int ObScanMergeLoserTreeCmp::compare_rowkey(const ObDatumRow &l_row, const ObDatumRow &r_row, int64_t &cmp_result)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!l_row.is_valid() || !r_row.is_valid() || nullptr == datum_utils_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(l_row), K(r_row), KP(datum_utils_));
  } else if (OB_UNLIKELY(l_row.get_column_count() < rowkey_size_ || r_row.get_column_count() < rowkey_size_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected row column cnt", K(ret), K(l_row), K(r_row), K_(rowkey_size));
  } else {
    ObDatumRowkey l_key;
    ObDatumRowkey r_key;
    int temp_cmp_ret = 0;
    if (OB_FAIL(l_key.assign(l_row.storage_datums_, rowkey_size_))) {
      STORAGE_LOG(WARN, "Failed to assign store rowkey", K(ret), K(l_row), K_(rowkey_size));
    } else if (OB_FAIL(r_key.assign(r_row.storage_datums_, rowkey_size_))) {
      STORAGE_LOG(WARN, "Failed to assign store rowkey", K(ret), K(r_row), K_(rowkey_size));
    } else if (OB_FAIL(l_key.compare(r_key, *datum_utils_, temp_cmp_ret))) {
      STORAGE_LOG(WARN, "Failed to compare rowkey", K(ret), K(l_key), K(r_key), KPC(datum_utils_));
    } else {
      cmp_result = temp_cmp_ret;
    }
  }
  return ret;
}

int ObScanMergeLoserTreeCmp::cmp(
    const ObScanMergeLoserTreeItem &l,
    const ObScanMergeLoserTreeItem &r,
    int64_t &cmp_ret)
{
  int ret = OB_SUCCESS;
  cmp_ret = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (nullptr == l.row_ || nullptr == r.row_ || l.row_->scan_index_ < 0 || r.row_->scan_index_ < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(l.row_), KP(r.row_));
  } else {
    cmp_ret = l.row_->scan_index_ - r.row_->scan_index_;
    if (0 == cmp_ret) {
      if (OB_FAIL(compare_rowkey(*l.row_, *r.row_, cmp_ret))) {
        LOG_WARN("compare rowkey error", K(ret));
      } else if (reverse_) {
        cmp_ret = -cmp_ret;
      }
    }
  }
  return ret;
}

}
}

