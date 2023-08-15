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

#include "storage/direct_load/ob_direct_load_multiple_sstable_scan_merge_loser_tree.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;

/**
 * ObDirectLoadMultipleSSTableScanMergeLoserTreeCompare
 */

ObDirectLoadMultipleSSTableScanMergeLoserTreeCompare::
  ObDirectLoadMultipleSSTableScanMergeLoserTreeCompare()
  : datum_utils_(nullptr)
{
}

ObDirectLoadMultipleSSTableScanMergeLoserTreeCompare::
  ~ObDirectLoadMultipleSSTableScanMergeLoserTreeCompare()
{
}

int ObDirectLoadMultipleSSTableScanMergeLoserTreeCompare::init(
  const blocksstable::ObStorageDatumUtils *datum_utils)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == datum_utils)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(datum_utils));
  } else {
    datum_utils_ = datum_utils;
  }
  return ret;
}

int ObDirectLoadMultipleSSTableScanMergeLoserTreeCompare::cmp(
  const ObDirectLoadMultipleSSTableScanMergeLoserTreeItem &lhs,
  const ObDirectLoadMultipleSSTableScanMergeLoserTreeItem &rhs,
  int64_t &cmp_ret)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == datum_utils_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadMultipleSSTableScanMergeLoserTreeCompare not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == lhs.row_ || nullptr == rhs.row_ || !lhs.row_->is_valid() ||
                         !rhs.row_->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(lhs), K(rhs));
  } else {
    int tmp_cmp_ret = 0;
    if (OB_FAIL(lhs.row_->rowkey_.compare(rhs.row_->rowkey_, *datum_utils_, tmp_cmp_ret))) {
      LOG_WARN("fail to compare rowkey", K(ret), K(lhs.row_->rowkey_), K(rhs.row_->rowkey_),
               KPC(datum_utils_));
    } else {
      cmp_ret = tmp_cmp_ret;
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
