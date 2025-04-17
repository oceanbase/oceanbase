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

#define USING_LOG_PREFIX SERVER
#include "observer/table_load/backup/ob_table_load_backup_column_map_v2.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup
{
using namespace common;

ObColumnMapV2::ObColumnMapV2()
  : request_count_(0),
    store_count_(0),
    rowkey_store_count_(0),
    seq_read_column_count_(0),
    is_inited_(false)
{
  column_indexs_.set_tenant_id(MTL_ID());
}

int ObColumnMapV2::init(
    const int64_t schema_rowkey_cnt,
    const int64_t store_cnt,
    const ObIArray<share::schema::ObColDesc> &out_cols)
{
  int ret = OB_SUCCESS;
  const int64_t out_cols_cnt = out_cols.count();
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("The ObColumnMapV2 has been inited", KR(ret));
  } else if (OB_UNLIKELY(schema_rowkey_cnt <= 0 ||
                         store_cnt < 0 ||
                         (store_cnt > 0 && store_cnt < schema_rowkey_cnt) ||
                         store_cnt > OB_ROW_MAX_COLUMNS_COUNT ||
                         out_cols_cnt <= 0 ||
                         out_cols_cnt > OB_ROW_MAX_COLUMNS_COUNT)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", KR(ret), K(schema_rowkey_cnt), K(store_cnt), K(out_cols_cnt));
  } else {
    if (OB_FAIL(column_indexs_.reserve(out_cols_cnt))) {
      LOG_WARN("fail to reserve", KR(ret), K(out_cols_cnt));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < out_cols_cnt; ++i) {
        ObColumnIndexItemV2 item;
        item.column_id_ = out_cols.at(i).col_id_;
        item.request_column_type_ = out_cols.at(i).col_type_;
        item.macro_column_type_ = out_cols.at(i).col_type_;
        if (OB_FAIL(init_column_index(out_cols.at(i).col_id_, schema_rowkey_cnt, i, store_cnt, &item))) {
          LOG_WARN("init column index failed ", KR(ret), K(out_cols.at(i).col_id_), K(i));
        } else if (OB_FAIL(column_indexs_.push_back(item))) {
          LOG_WARN("fail to push back", KR(ret), K(i));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    seq_read_column_count_ = out_cols_cnt;
    request_count_ = out_cols_cnt;
    store_count_ = store_cnt;
    rowkey_store_count_ = schema_rowkey_cnt;
    is_inited_ = true;
  }
  return ret;
}

void ObColumnMapV2::reset()
{
  store_count_ = 0;
  rowkey_store_count_ = 0;
  seq_read_column_count_ = 0;
  request_count_ = 0;
  column_indexs_.reset();
  is_inited_ = false;
}

// make sure all parameters are valid
int ObColumnMapV2::init_column_index(
    const uint64_t col_id,
    const int64_t schema_rowkey_cnt,
    const int64_t store_index,
    const int64_t store_cnt,
    ObColumnIndexItemV2 *column_index)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(store_index >= OB_ROW_MAX_COLUMNS_COUNT || (store_cnt > 0 && store_index >= store_cnt))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid store index", KR(ret), K(store_index), K(store_cnt));
  } else if (OB_HIDDEN_TRANS_VERSION_COLUMN_ID == col_id) {
    column_index->store_index_ = -1;
  } else if (OB_HIDDEN_SQL_SEQUENCE_COLUMN_ID == col_id) {
    column_index->store_index_ = -1;
  } else {
    column_index->store_index_ = static_cast<int16_t>(store_index);
  }
  return ret;
}

} // table_load_backup
} // namespace observer
} // namespace oceanbase
