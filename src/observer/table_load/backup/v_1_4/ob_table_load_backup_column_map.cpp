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
#include "observer/table_load/backup/v_1_4/ob_table_load_backup_column_map.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup_v_1_4
{

/**
 * ObTableLoadBackupColumnMap
 */
ObTableLoadBackupColumnMap::ObTableLoadBackupColumnMap()
  : request_count_(0),
    store_count_(0),
    rowkey_store_count_(0),
    seq_read_column_count_(0),
    read_full_rowkey_(false),
    is_inited_(false)
{
  column_indexs_ = reinterpret_cast<ObTableLoadBackupColumnIndexItem *>(column_indexs_buf_);
}

int ObTableLoadBackupColumnMap::init(const ObTableLoadBackupMacroBlockMeta *meta)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("column map already init", KR(ret));
  } else if (OB_UNLIKELY(meta == nullptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (OB_UNLIKELY(meta->column_number_ <= 0) ||
             OB_UNLIKELY(meta->column_number_ > OB_TABLE_LOAD_PRE_ROW_MAX_COLUMNS_COUNT) ||
             OB_UNLIKELY(meta->column_number_ > meta->column_number_) ||
             OB_UNLIKELY(meta->column_index_scale_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(meta->column_number_), K(meta->column_number_),
              K(meta->column_index_scale_));
  } else if (OB_UNLIKELY(meta->column_index_scale_ > 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not support step number greater than 1", KR(ret), K(meta->column_index_scale_));
  } else {
    request_count_ = 0;
    store_count_ = meta->column_number_;
    rowkey_store_count_ = meta->rowkey_column_number_;
    for (int64_t i = 0; i < store_count_; i++, request_count_++) {
      column_indexs_[request_count_].request_column_type_ = meta->column_type_array_[i];
      column_indexs_[request_count_].store_index_ = static_cast<int16_t>(i);
      column_indexs_[request_count_].is_column_type_matched_ = true;
    }
    seq_read_column_count_ = request_count_;
    read_full_rowkey_ = true;
    is_inited_ = true;
  }
  return ret;
}

void ObTableLoadBackupColumnMap::reuse()
{
  request_count_ = 0;
  store_count_ = 0;
  rowkey_store_count_ = 0;
  seq_read_column_count_ = 0;
  is_inited_ = false;
  read_full_rowkey_ = false;
}

} // table_load_backup_v_1_4
} // namespace observer
} // namespace oceanbase
