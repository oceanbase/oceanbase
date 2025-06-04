//Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "storage/compaction/filter/ob_mds_info_compaction_filter.h"
#include "storage/truncate_info/ob_mds_info_distinct_mgr.h"
namespace oceanbase
{
using namespace common;
using namespace storage;
namespace compaction
{
int ObMdsInfoCompactionFilter::init(
  ObIAllocator &allocator,
  const ObTabletID &tablet_id,
  const int64_t schema_rowkey_cnt,
  const ObIArray<ObColDesc> &cols_desc,
  const ObMdsInfoDistinctMgr &truncate_info_mgr)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("is inited", KR(ret));
  } else if (OB_UNLIKELY(!truncate_info_mgr.is_valid() || truncate_info_mgr.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid or empty mds filter info", KR(ret), K(truncate_info_mgr));
  } else if (OB_FAIL(truncate_filter_.init(schema_rowkey_cnt, cols_desc, nullptr, truncate_info_mgr))) {
    LOG_WARN("failed to init truncate filter", KR(ret), K(tablet_id), K(schema_rowkey_cnt), K(cols_desc));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObMdsInfoCompactionFilter::filter(
      const blocksstable::ObDatumRow &row,
      ObFilterRet &filter_ret)
{
  int ret = OB_SUCCESS;
  bool filtered = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_FAIL(truncate_filter_.filter(row, filtered))) {
    LOG_WARN("failed to check row in truncate filter", KR(ret), K(row));
  } else if (filtered) {
    filter_ret = FILTER_RET_REMOVE;
    LOG_TRACE("[TRUNCATE_INFO] filter row", KR(ret), K(row), K(filtered)); // for debug, remove later
  } else {
    filter_ret = FILTER_RET_NOT_CHANGE;
    LOG_TRACE("[TRUNCATE_INFO] keep row", KR(ret), K(row), K(filtered)); // for debug, remove later
  }
  return OB_SUCCESS;
}


} // namespace compaction
} // namespace oceanbase
