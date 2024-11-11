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
#include "observer/table_load/backup/v_3_x/ob_table_load_backup_column_map.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup_v_3_x
{
using namespace common;

ObColumnMap::ObColumnMap()
  : request_count_(0),
    store_count_(0),
    rowkey_store_count_(0),
    schema_version_(0),
    cols_id_map_(nullptr),
    seq_read_column_count_(0),
    cur_idx_(0),
    column_indexs_(nullptr),
    is_inited_(false),
    is_all_column_matched_(false),
    create_col_id_map_(false),
    is_output_not_exist_(false),
    allocator_(nullptr)
{
}

ObColumnMap::~ObColumnMap()
{
}

int ObColumnMap::init(
    ObIAllocator &allocator,
    const int64_t schema_version,
    const int64_t schema_rowkey_cnt,
    const int64_t store_cnt,
    const ObIArray<share::schema::ObColDesc> &out_cols)
{
  int ret = OB_SUCCESS;
  const int64_t out_cols_cnt = out_cols.count();

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("The ObColumnMap has been inited", KR(ret));
  } else if (OB_UNLIKELY(
      schema_version < 0 ||
      schema_rowkey_cnt <= 0 ||
      store_cnt < 0 ||
      (store_cnt > 0 && store_cnt < schema_rowkey_cnt) ||
      store_cnt > OB_ROW_MAX_COLUMNS_COUNT ||
      out_cols_cnt <= 0 ||
      out_cols_cnt > OB_ROW_MAX_COLUMNS_COUNT)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", KR(ret), K(schema_version), K(schema_rowkey_cnt), K(store_cnt), K(out_cols_cnt));
  } else if (OB_ISNULL(column_indexs_ = reinterpret_cast<ObColumnIndexItem *>(
      allocator.alloc(sizeof(ObColumnIndexItem) * out_cols_cnt)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to allocate column index", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < out_cols_cnt; ++i) {
      column_indexs_[i].column_id_ = out_cols.at(i).col_id_;
      column_indexs_[i].request_column_type_ = out_cols.at(i).col_type_;
      column_indexs_[i].macro_column_type_ = out_cols.at(i).col_type_;
      if (OB_FAIL(init_column_index(out_cols.at(i).col_id_, schema_rowkey_cnt, i, store_cnt, &column_indexs_[i]))) {
        LOG_WARN("init column index failed ", KR(ret), K(out_cols.at(i).col_id_), K(i));
      } else {
        column_indexs_[i].is_column_type_matched_ = true;
      }
    }

    seq_read_column_count_ = out_cols_cnt;
    // create col_id_map
    if (OB_SUCC(ret)) {
      void * buf = static_cast<share::schema::ColumnMap *>(allocator.alloc(sizeof(share::schema::ColumnMap)));
        if (OB_ISNULL(buf)) { // alloc failed
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("Fail to allocate col id map", KR(ret));
        } else {
          share::schema::ColumnMap *cols_id_map_ptr = new(buf) share::schema::ColumnMap(allocator);
          if (OB_FAIL(cols_id_map_ptr->init(out_cols))) {
            allocator.free(cols_id_map_ptr);
            LOG_WARN("Fail to init col id map", KR(ret));
          } else {
            create_col_id_map_ = true; // set create flag
            cols_id_map_ = cols_id_map_ptr;
          }
        }
      }
  }

  if (OB_SUCC(ret)) {
    request_count_ = out_cols_cnt;
    store_count_ = store_cnt;
    rowkey_store_count_ = schema_rowkey_cnt;
    schema_version_ = schema_version;
    is_all_column_matched_ = true;
    is_inited_ = true;
    allocator_ = &allocator;
  }
  return ret;
}

void ObColumnMap::reset()
{
  store_count_ = 0;
  cur_idx_ = 0;
  rowkey_store_count_ = 0;
  seq_read_column_count_ = 0;
  is_inited_ = false;
  is_all_column_matched_ = false;
  is_output_not_exist_ = false;
  if (create_col_id_map_ && OB_NOT_NULL(cols_id_map_)) {
    allocator_->free((void *)cols_id_map_);
    create_col_id_map_ = false;
  }
  cols_id_map_ = nullptr;

  if (OB_NOT_NULL(column_indexs_)) {
    allocator_->free(column_indexs_);
    column_indexs_ = nullptr;
  }
  schema_version_ = 0;
  request_count_ = 0;
}

// make sure all parameters are valid
int ObColumnMap::init_column_index(
    const uint64_t col_id,
    const int64_t schema_rowkey_cnt,
    const int64_t store_index,
    const int64_t store_cnt,
    ObColumnIndexItem *column_index)
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

} // table_load_backup_v_3_x
} // namespace observer
} // namespace oceanbase
