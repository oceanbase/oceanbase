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

#define USING_LOG_PREFIX STORAGE
#include "storage/blocksstable/index_block/ob_ddl_sstable_scan_merge.h"
namespace oceanbase
{
namespace blocksstable
{

/******************             ObDDLSSTableMergeLoserTreeCompare              **********************/
ObDDLSSTableMergeLoserTreeCompare::ObDDLSSTableMergeLoserTreeCompare()
  : reverse_scan_(false),
    datum_utils_(nullptr)
{
}

ObDDLSSTableMergeLoserTreeCompare::~ObDDLSSTableMergeLoserTreeCompare()
{
  reset();
}

void ObDDLSSTableMergeLoserTreeCompare::reset()
{
  reverse_scan_ = false;
  datum_utils_ = nullptr;
}

int ObDDLSSTableMergeLoserTreeCompare::cmp(const ObDDLSSTableMergeLoserTreeItem &lhs,
                                           const ObDDLSSTableMergeLoserTreeItem &rhs,
                                           int64_t &cmp_ret)
{
  int ret = OB_SUCCESS;
  int tmp_cmp_ret = 0;
  if (OB_UNLIKELY(nullptr == datum_utils_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadSSTableScanMergeLoserTreeCompare not init", K(ret), KP(this));
  } else if (OB_UNLIKELY(!lhs.end_key_.is_valid() || !rhs.end_key_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(lhs), K(rhs));
  } else if (OB_FAIL(lhs.end_key_.compare(rhs.end_key_, *datum_utils_, tmp_cmp_ret))) {
    LOG_WARN("fail to compare rowkey", K(ret), K(lhs), K(rhs), KPC(datum_utils_));
  } else {
    cmp_ret = tmp_cmp_ret * (reverse_scan_ ? -1 : 1);
  }
  return ret;
}

} // end namespace blocksstable
} // end namespace oceanbase
