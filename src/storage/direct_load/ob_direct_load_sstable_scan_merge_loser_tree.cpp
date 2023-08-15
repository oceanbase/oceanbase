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

#include "storage/direct_load/ob_direct_load_sstable_scan_merge_loser_tree.h"
#include "storage/direct_load/ob_direct_load_external_row.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;

/**
 * ObDirectLoadSSTableScanMergeLoserTreeCompare
 */

ObDirectLoadSSTableScanMergeLoserTreeCompare::ObDirectLoadSSTableScanMergeLoserTreeCompare()
  : datum_utils_(nullptr)
{
}

ObDirectLoadSSTableScanMergeLoserTreeCompare::~ObDirectLoadSSTableScanMergeLoserTreeCompare()
{
}

int ObDirectLoadSSTableScanMergeLoserTreeCompare::init(
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

int ObDirectLoadSSTableScanMergeLoserTreeCompare::cmp(
  const ObDirectLoadSSTableScanMergeLoserTreeItem &lhs,
  const ObDirectLoadSSTableScanMergeLoserTreeItem &rhs, int64_t &cmp_ret)
{
  int ret = OB_SUCCESS;
  int tmp_cmp_ret = 0;
  if (OB_UNLIKELY(nullptr == datum_utils_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadSSTableScanMergeLoserTreeCompare not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == lhs.external_row_ || nullptr == rhs.external_row_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(lhs), K(rhs));
  } else if (OB_UNLIKELY(lhs.external_row_->rowkey_datum_array_.count_ !=
                           rhs.external_row_->rowkey_datum_array_.count_ ||
                         0 == lhs.external_row_->rowkey_datum_array_.count_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected rowkey datum count", KR(ret), K(lhs), K(rhs));
  } else {
    if (OB_FAIL(lhs_rowkey_.assign(lhs.external_row_->rowkey_datum_array_.datums_,
                                   lhs.external_row_->rowkey_datum_array_.count_))) {
      LOG_WARN("fail to assign rowkey", KR(ret), K(lhs));
    } else if (OB_FAIL(rhs_rowkey_.assign(rhs.external_row_->rowkey_datum_array_.datums_,
                                          rhs.external_row_->rowkey_datum_array_.count_))) {
      LOG_WARN("fail to assign rowkey", KR(ret), K(rhs));
    } else if (OB_FAIL(lhs_rowkey_.compare(rhs_rowkey_, *datum_utils_, tmp_cmp_ret))) {
      LOG_WARN("fail to compare rowkey", K(ret), K(lhs_rowkey_), K(rhs_rowkey_), KPC(datum_utils_));
    } else {
      cmp_ret = tmp_cmp_ret;
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
