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
  const ObMdsInfoDistinctMgr &mds_info_mgr)
{
  int ret = OB_SUCCESS;
  const ObTruncateInfoDistinctMgr &truncate_info_mgr = mds_info_mgr.get_truncate_info_distinct_mgr();
  const ObTTLFilterInfoDistinctMgr &ttl_info_mgr = mds_info_mgr.get_ttl_filter_info_distinct_mgr();

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("is inited", KR(ret));
  } else if (OB_UNLIKELY(mds_info_mgr.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid or empty mds filter info", KR(ret), K(mds_info_mgr));
  } else if (!truncate_info_mgr.empty()
      && OB_FAIL(truncate_filter_.init(schema_rowkey_cnt, cols_desc, nullptr/*cols_param*/, truncate_info_mgr))) {
    LOG_WARN("failed to init truncate filter", KR(ret), K(tablet_id), K(schema_rowkey_cnt), K(cols_desc));
  } else if (!ttl_info_mgr.empty()
      && OB_FAIL(ttl_filter_.init(schema_rowkey_cnt, cols_desc, nullptr/*cols_param*/, ttl_info_mgr))) {
    LOG_WARN("failed to init ttl filter", KR(ret), K(tablet_id), K(schema_rowkey_cnt), K(cols_desc));
  } else {
    schema_rowkey_cnt_ = schema_rowkey_cnt;
    is_inited_ = true;
  }
  return ret;
}

int ObMdsInfoCompactionFilter::filter(
      const blocksstable::ObDatumRow &row,
      ObFilterRet &filter_ret) const
{
  int ret = OB_SUCCESS;
  bool filtered = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (ttl_filter_.is_valid() && OB_FAIL(ttl_filter_.filter(row, filtered))) {
    LOG_WARN("failed to check row in ttl filter", KR(ret), K(row));
  } else if (filtered) {
    filter_ret = FILTER_RET_REMOVE;
    LOG_TRACE("[COMPACTION TTL] filter row", KR(ret), K(row), K(filtered));
  } else if (truncate_filter_.is_valid() && OB_FAIL(truncate_filter_.filter(row, filtered))) {
    LOG_WARN("failed to check row in truncate filter", KR(ret), K(row));
  } else if (filtered) {
    filter_ret = FILTER_RET_REMOVE;
    LOG_TRACE("[TRUNCATE INFO] filter row", KR(ret), K(row), K(filtered));
  } else {
    filter_ret = FILTER_RET_KEEP;
    LOG_TRACE("[TRUNCATE INFO] keep row", KR(ret), K(row), K(filtered));
  }
  return ret;
}

int ObMdsInfoCompactionFilter::get_filter_op(
  const int64_t min_merged_snapshot,
  const int64_t max_merged_snapshot,
  ObBlockOp &op) const
{
  int ret = OB_SUCCESS;
  op.set_none();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_UNLIKELY(min_merged_snapshot > max_merged_snapshot)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(min_merged_snapshot), K(max_merged_snapshot));
  } else {
    ObBlockOp op_ttl; // init as none
    ObBlockOp op_truncate; // init as none
    const ObTTLFilterVal &ttl_filter_val = ttl_filter_.get_filter_val();
    if (ttl_filter_.is_valid()) {
      const ObTTLFilterVal::TTLFilterPair &ttl_filter_pair = ttl_filter_val.at(0); // only one ttl filter pair now
      if (max_merged_snapshot <= ttl_filter_pair.filter_val_) {
        op_ttl.set_filter();
      } else if (ttl_filter_pair.filter_val_ >= min_merged_snapshot) {
        op_ttl.set_open();
      }
      op.fuse(op_ttl);
    }
    if (truncate_filter_.is_valid()) {
      if (truncate_filter_.get_filter_max_val() >= min_merged_snapshot) {
        op_truncate.set_open();
      }
      op.fuse(op_truncate);
    }
    if (OB_UNLIKELY((0 == min_merged_snapshot) && op.is_none())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("can't reuse block when min version is compat", KR(ret), K(min_merged_snapshot),
        K(max_merged_snapshot), K(op));
    } else {
      FLOG_INFO("[COMPACTION FILTER] get filter op type", KR(ret), K(min_merged_snapshot), K(max_merged_snapshot),
        K(op_ttl), K(op_truncate), K(op), K(ttl_filter_val));
    }
  }
  return ret;
}

} // namespace compaction
} // namespace oceanbase
