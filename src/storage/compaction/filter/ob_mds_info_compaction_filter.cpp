// Copyright (c) 2024 OceanBase
// SPDX-License-Identifier: Apache-2.0
#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "storage/compaction/filter/ob_mds_info_compaction_filter.h"
#include "storage/truncate_info/ob_mds_info_distinct_mgr.h"
#include "storage/blocksstable/index_block/ob_agg_row_struct.h"
#include "storage/ob_trans_version_skip_index_util.h"
namespace oceanbase
{
using namespace common;
using namespace storage;
namespace compaction
{

int ObMdsInfoCompactionFilter::init(
  ObIAllocator &allocator,
  const ObTabletID &tablet_id,
  const ObStorageSchema *schema,
  const int64_t schema_rowkey_cnt,
  const ObIArray<ObColDesc> &cols_desc,
  const ObMdsInfoDistinctMgr &mds_info_mgr)
{
  int ret = OB_SUCCESS;
  const ObTruncateInfoDistinctMgr &truncate_info_mgr = mds_info_mgr.get_truncate_info_distinct_mgr();
  const ObTTLFilterInfoDistinctMgr &ttl_info_mgr = mds_info_mgr.get_ttl_filter_info_distinct_mgr();
  const ObTTLFilterInitParams ttl_filter_init_params(cols_desc, nullptr/*column_index_array*/, nullptr/*column_params*/, schema);

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
      && OB_FAIL(ttl_filter_.init(schema_rowkey_cnt, ttl_filter_init_params, ttl_info_mgr))) {
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
  } else if (truncate_filter_.is_valid() && OB_FAIL(truncate_filter_.filter(row, filtered))) {
    LOG_WARN("failed to check row in truncate filter", KR(ret), K(row));
  } else if (filtered) {
    filter_ret = FILTER_RET_REMOVE;
    LOG_INFO("[TRUNCATE INFO] filter row", KR(ret), K(row), K(filtered));
  } else if (ttl_filter_.is_valid() && row.major_merge_flag_.exist_new_committed_row_) {
    filter_ret = FILTER_RET_KEEP;
    LOG_INFO("exist new committed row under ttl filter", KR(ret), K(row)); // remove later
  } else if (ttl_filter_.is_valid() && !row.major_merge_flag_.exist_new_committed_row_ && OB_FAIL(ttl_filter_.filter(row, filtered))) {
    LOG_WARN("failed to check row in ttl filter", KR(ret), K(row));
  } else if (filtered) {
    filter_ret = FILTER_RET_REMOVE;
    LOG_INFO("[COMPACTION TTL] filter row", KR(ret), K(row), K(filtered));
  } else {
    filter_ret = FILTER_RET_KEEP;
    LOG_INFO("[MDS INFO] keep row", KR(ret), K(row), K(filtered));
  }
  return ret;
}

int ObMdsInfoCompactionFilter::get_filter_op(
  ObAggRowCachedReader &agg_row_cached_reader,
  ObBlockOp &op) const
{
  int ret = OB_SUCCESS;
  op.set_none();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_UNLIKELY(!agg_row_cached_reader.is_inited())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(agg_row_cached_reader));
  } else {
    ObBlockOp op_ttl; // init as none
    sql::ObBoolMask ttl_bool_mask;
    ObBlockOp op_truncate; // init as none
    if (ttl_filter_.is_valid()) {
      if (OB_FAIL(ttl_filter_.skip_index_filter(agg_row_cached_reader, ttl_bool_mask))) {
        LOG_WARN("failed to skip index filter", KR(ret), K(agg_row_cached_reader));
      } else if (ttl_bool_mask.is_always_false()) {
        op_ttl.set_filter();
      } else if (ttl_bool_mask.is_always_true()) {
        op_ttl.set_none();
      } else if (ttl_bool_mask.is_uncertain()) {
        op_ttl.set_open();
      }
      op.fuse(op_ttl);
    }
    if (OB_SUCC(ret) && truncate_filter_.is_valid()) {
      ObTransVersionSkipIndexInfo skip_index_info;
      if (OB_FAIL(ObTransVersionSkipIndexReader::read_min_max_snapshot(
        schema_rowkey_cnt_, agg_row_cached_reader, skip_index_info))) {
        LOG_WARN("failed to read min max snapshot", KR(ret), K(agg_row_cached_reader));
      } else if (truncate_filter_.get_filter_max_val() >= skip_index_info.min_snapshot_) {
        op_truncate.set_open();
      }
      op.fuse(op_truncate);
    }
    if (OB_SUCC(ret)) {
      FLOG_INFO("[COMPACTION FILTER] get filter op type", KR(ret), K(op_ttl), K(op_truncate), K(op));
    }
  }
  return ret;
}

} // namespace compaction
} // namespace oceanbase
