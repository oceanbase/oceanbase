//Copyright (c) 2025 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define USING_LOG_PREFIX STORAGE_COMPACTION

#include "storage/compaction/filter/ob_rowscn_filter.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"

namespace oceanbase
{
using namespace blocksstable;
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
  const int64_t min_merged_snapshot,
  const int64_t max_merged_snapshot,
  ObBlockOp &op) const
{
  int ret = OB_SUCCESS;
  op.set_none();
  const bool is_compat_min_version = 0 == min_merged_snapshot;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_UNLIKELY(!is_compat_min_version && min_merged_snapshot > max_merged_snapshot)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(min_merged_snapshot), K(max_merged_snapshot));
  } else if (max_merged_snapshot <= filter_val_) {
    op.set_filter();
  } else if (is_compat_min_version || filter_val_ >= min_merged_snapshot) {
    op.set_open();
  }
  if (OB_UNLIKELY(is_compat_min_version && op.is_none())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can't reuse block when min version is compat", KR(ret), K(min_merged_snapshot),
      K(max_merged_snapshot), K(op));
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
