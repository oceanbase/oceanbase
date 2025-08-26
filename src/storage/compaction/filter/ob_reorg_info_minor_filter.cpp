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

#include "storage/compaction/filter/ob_reorg_info_minor_filter.h"
#include "storage/ob_i_store.h"
#include "storage/reorganization_info_table/ob_tablet_reorg_info_table_schema_helper.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace share;

namespace compaction
{

int ObReorgInfoMinorFilter::init(const SCN &filter_val)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("is inited", K(ret), K(filter_val));
  } else if (OB_UNLIKELY(!filter_val.is_valid() || filter_val.is_min())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(filter_val));
  } else {
    filter_val_ = filter_val;
    filter_col_idx_ = ObTabletReorgInfoTableSchemaDef::TRANS_VERSION_COLUMN_ID;
    is_inited_ = true;
  }
  return ret;
}

int ObReorgInfoMinorFilter::filter(
    const blocksstable::ObDatumRow &row,
    ObFilterRet &filter_ret)
{
  int ret = OB_SUCCESS;
  filter_ret = FILTER_RET_MAX;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!row.is_last_multi_version_row()
      || !row.is_first_multi_version_row()
      || row.count_ <= filter_col_idx_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("uncommitted row in trans state table or multi version row", K(ret), K(row));
  } else if (row.is_uncommitted_row()) {
    // not filter uncommitted row
  } else {
    const int64_t commit_version = -row.storage_datums_[filter_col_idx_].get_int();
    SCN commit_scn;
    if (OB_FAIL(commit_scn.convert_for_tx(commit_version))) {
      LOG_WARN("failed to convert for tx", K(ret), K(commit_version));
    } else if (commit_scn <= filter_val_) {
      filter_ret = FILTER_RET_REMOVE;
      max_filtered_commit_scn_ = SCN::max(max_filtered_commit_scn_, commit_scn);
      LOG_DEBUG("filter row", K(ret), K(row), K(filter_val_), K(commit_version));
    } else {
      filter_ret = FILTER_RET_NOT_CHANGE;
    }
  }
  return ret;
}

} // namespace compaction
} // namespace oceanbase
