// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <>

#define USING_LOG_PREFIX STORAGE

#include "storage/direct_load/ob_direct_load_multiple_heap_table_index_scan_merge_loser_tree.h"

namespace oceanbase
{
namespace storage
{
using namespace common;

/**
 * ObDirectLoadMultipleHeapTableIndexScanMergeLoserTreeCompare
 */

ObDirectLoadMultipleHeapTableIndexScanMergeLoserTreeCompare::
  ObDirectLoadMultipleHeapTableIndexScanMergeLoserTreeCompare()
{
}

ObDirectLoadMultipleHeapTableIndexScanMergeLoserTreeCompare::
  ~ObDirectLoadMultipleHeapTableIndexScanMergeLoserTreeCompare()
{
}

int ObDirectLoadMultipleHeapTableIndexScanMergeLoserTreeCompare::cmp(
  const ObDirectLoadMultipleHeapTableIndexScanMergeLoserTreeItem &lhs,
  const ObDirectLoadMultipleHeapTableIndexScanMergeLoserTreeItem &rhs,
  int64_t &cmp_ret)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == lhs.index_ || nullptr == rhs.index_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(lhs), K(rhs));
  } else {
    cmp_ret = lhs.index_->tablet_id_.compare(rhs.index_->tablet_id_);
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
