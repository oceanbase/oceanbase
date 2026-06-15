// Copyright (c) 2025 OceanBase
// SPDX-License-Identifier: Apache-2.0
#define USING_LOG_PREFIX STORAGE_COMPACTION

#include "storage/compaction/filter/ob_rowscn_filter.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/index_block/ob_agg_row_struct.h"
#include "storage/ob_trans_version_skip_index_util.h"
namespace oceanbase
{
using namespace blocksstable;
using namespace storage;
namespace compaction
{

int ObRowscnFilter::init(
  const int64_t filter_val,
  const int64_t filter_col_idx,
  const CompactionFilterType filter_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(filter_col_idx < 0 || filter_val < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(filter_col_idx), K(filter_val));
  } else if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("is inited", K(ret), K(filter_col_idx));
  } else {
    filter_val_ = filter_val;
    filter_col_idx_ = filter_col_idx;
    filter_type_ = filter_type;
    is_inited_ = true;
  }
  return ret;
}

int ObRowscnFilter::filter(
    const blocksstable::ObDatumRow &row,
    ObFilterRet &filter_ret) const
{
  int ret = OB_SUCCESS;
  filter_ret = FILTER_RET_MAX;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(row.is_uncommitted_row()
      || row.count_ <= filter_col_idx_)) { // not filter uncommitted row
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("uncommitted row in trans state table or multi version row", K(ret), K(row));
  } else {
    int64_t rowscn = row.storage_datums_[filter_col_idx_].get_int();
    rowscn = rowscn < 0 ? -rowscn : rowscn;
    if (rowscn <= filter_val_) {
      filter_ret = FILTER_RET_REMOVE;
      LOG_DEBUG("filter row", K(ret), K(row), K(filter_val_));
    } else {
      filter_ret = FILTER_RET_KEEP;
      LOG_DEBUG("keep row", K(ret), K(row), K(filter_val_));
    }
  }
  return ret;
}

int ObRowscnFilter::get_filter_op(
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
    ObTransVersionSkipIndexInfo skip_index_info;
    if (OB_FAIL(ObTransVersionSkipIndexReader::read_min_max_snapshot(
        filter_col_idx_, agg_row_cached_reader, skip_index_info))) {
      LOG_WARN("failed to read min max snapshot", KR(ret), K(agg_row_cached_reader));
    } else if (skip_index_info.max_snapshot_ <= filter_val_) {
      op.set_filter();
    } else if (filter_val_ >= skip_index_info.min_snapshot_) {
      op.set_open();
    }

  }
  return ret;
}

void ObRowscnFilter::gene_info(char* buf, const int64_t buf_len, int64_t &pos) const
{
  if (OB_ISNULL(buf) || pos >= buf_len) {
  } else {
    J_OBJ_START();
    J_KV("filter_type", ObICompactionFilter::get_filter_type_str(filter_type_), "filter_val", filter_val_);
    J_OBJ_END();
  }
}

} // namespace compaction
} // namespace oceanbase
