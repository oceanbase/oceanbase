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
#include "observer/table_load/backup/ob_table_load_backup_column_map_v1.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup
{

/**
 * ObColumnMapV1
 */
ObColumnMapV1::ObColumnMapV1()
  : request_count_(0),
    store_count_(0),
    rowkey_store_count_(0),
    seq_read_column_count_(0),
    is_inited_(false)
{
  column_indexs_.set_tenant_id(MTL_ID());
}

int ObColumnMapV1::init(const ObMacroBlockMeta *meta)
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
    if (OB_FAIL(column_indexs_.reserve(meta->column_number_))) {
      LOG_WARN("fail to reserve", KR(ret), K(meta->column_number_));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < meta->column_number_; ++i) {
        ObColumnIndexItemV1 item;
        item.request_column_type_ = meta->column_type_array_[i];
        item.store_index_ = static_cast<int16_t>(i);
        item.is_column_type_matched_ = true;
        if (OB_FAIL(column_indexs_.push_back(item))) {
          LOG_WARN("fail to push back", KR(ret), K(i));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    request_count_ = meta->column_number_;
    store_count_ = meta->column_number_;
    rowkey_store_count_ = meta->rowkey_column_number_;
    seq_read_column_count_ = request_count_;
    is_inited_ = true;
  }
  return ret;
}

void ObColumnMapV1::reset()
{
  request_count_ = 0;
  store_count_ = 0;
  rowkey_store_count_ = 0;
  seq_read_column_count_ = 0;
  column_indexs_.reset();
  is_inited_ = false;
}

} // table_load_backup
} // namespace observer
} // namespace oceanbase
