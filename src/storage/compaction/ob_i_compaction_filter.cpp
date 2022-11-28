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

#define USING_LOG_PREFIX STORAGE_COMPACTION

#include "ob_i_compaction_filter.h"
#include "storage/ob_i_store.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace share;

namespace compaction
{

const char *ObICompactionFilter::ObFilterRetStr[] =
{
  "NOT_CHANGE",
  "REMOVE",
};

void ObICompactionFilter::ObFilterStatistics::add(const ObFilterStatistics &other)
{
  for (int i = 0; i < FILTER_RET_MAX; ++i) {
    row_cnt_[i] += other.row_cnt_[i];
  }
}

void ObICompactionFilter::ObFilterStatistics::inc(ObFilterRet filter_ret)
{
  if (OB_LIKELY(filter_ret >= FILTER_RET_NOT_CHANGE && filter_ret < FILTER_RET_MAX)) {
    row_cnt_[filter_ret]++;
  }
}

void ObICompactionFilter::ObFilterStatistics::reset()
{
  MEMSET(row_cnt_, 0, sizeof(row_cnt_));
}

const char *ObICompactionFilter::get_filter_ret_str(const int64_t idx)
{
  STATIC_ASSERT(static_cast<int64_t>(FILTER_RET_MAX) == ARRAYSIZEOF(ObFilterRetStr), "filter ret string is mismatch");
  const char * ret_str = nullptr;
  if (idx < 0 || idx >= FILTER_RET_MAX) {
    ret_str = "invalid_ret";
  } else {
    ret_str = ObFilterRetStr[idx];
  }
  return ret_str;
}

int64_t ObICompactionFilter::ObFilterStatistics::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
  } else {
    J_NAME("CompactionFilterStatistics:");
    J_OBJ_START();
    for (int i = 0; i < FILTER_RET_MAX; ++i) {
      J_OBJ_START();
      J_KV("type", get_filter_ret_str(i), "info", row_cnt_[i]);
      J_OBJ_END();
      J_COMMA();
    }
    J_OBJ_END();
  }
  return pos;
}

int ObTransStatusFilter::init(const SCN &filter_val, const int64_t filter_col_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!filter_val.is_valid() || filter_val.is_min() || filter_col_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(filter_val), K(filter_col_idx));
  } else if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("is inited", K(ret), K(filter_val), K(filter_col_idx));
  } else {
    filter_val_ = filter_val;
    filter_col_idx_ = filter_col_idx;
    max_filtered_end_scn_.set_min();
    is_inited_ = true;
  }
  return ret;
}

int ObTransStatusFilter::filter(
    const blocksstable::ObDatumRow &row,
    ObFilterRet &filter_ret)
{
  int ret = OB_SUCCESS;
  filter_ret = FILTER_RET_MAX;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (row.is_uncommitted_row()
      || !row.is_last_multi_version_row()
      || !row.is_first_multi_version_row()
      || row.count_ <= filter_col_idx_) { // not filter uncommitted row
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("uncommitted row in trans state table or multi version row", K(ret), K(row));
  } else {
    const int64_t trans_end_scn_storage_val = row.storage_datums_[filter_col_idx_].get_int();
    SCN trans_end_scn;
    if (OB_FAIL(trans_end_scn.convert_for_tx(trans_end_scn_storage_val))) {
      LOG_WARN("failed to convert for tx", K(ret), K(trans_end_scn_storage_val));
    } else if (trans_end_scn <= filter_val_) {
      filter_ret = FILTER_RET_REMOVE;
      max_filtered_end_scn_ = SCN::max(max_filtered_end_scn_, trans_end_scn);
      LOG_DEBUG("filter row", K(ret), K(row), K(filter_val_));
    } else {
      filter_ret = FILTER_RET_NOT_CHANGE;
    }
  }
  return ret;
}


} // namespace compaction
} // namespace oceanbase
