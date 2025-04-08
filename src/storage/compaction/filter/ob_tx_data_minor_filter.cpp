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

#include "storage/compaction/filter/ob_tx_data_minor_filter.h"
#include "storage/ob_i_store.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace share;

namespace compaction
{

int ObTxDataMinorFilter::init(const SCN &filter_val, const int64_t filter_col_idx)
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

int ObTxDataMinorFilter::filter(
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
