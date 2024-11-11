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
#include "observer/table_load/backup/v_3_x/ob_table_load_backup_micro_block_scanner.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup_v_3_x
{
ObTableLoadBackupMicroBlockScanner::ObTableLoadBackupMicroBlockScanner()
  : allocator_("TLD_MBR_V_3_X"),
    reader_(nullptr),
    row_idx_(0),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
}

ObTableLoadBackupMicroBlockScanner::~ObTableLoadBackupMicroBlockScanner()
{
  if (reader_ != nullptr) {
    reader_->~ObIMicroBlockReader();
    allocator_.free(reader_);
    reader_ = nullptr;
  }
  allocator_.reset();
}

int ObTableLoadBackupMicroBlockScanner::init(
    const ObMicroBlockData *micro_block_data,
    const ObColumnMap *column_map)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(micro_block_data == nullptr || !micro_block_data->is_valid() || column_map == nullptr || !column_map->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(micro_block_data), KP(column_map));
  } else {
    if (micro_block_data->store_type_ == ObRowStoreType::FLAT_ROW_STORE) {
      if (reader_ == nullptr) {
        ObMicroBlockReader *block_reader = nullptr;
        if (OB_ISNULL(block_reader = OB_NEWx(ObMicroBlockReader, (&allocator_)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory", KR(ret));
        } else {
          reader_ = block_reader;
        }
      }
    } else {
      if (reader_ == nullptr) {
        ObMicroBlockDecoder *block_decoder = nullptr;
        if (OB_ISNULL(block_decoder = OB_NEWx(ObMicroBlockDecoder, (&allocator_)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory", KR(ret));
        } else {
          reader_ = block_decoder;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (row_.count_ <= 0) {
        if (OB_FAIL(ob_create_row(allocator_, column_map->get_request_count(), row_))) {
          LOG_WARN("fail to init row_", KR(ret));
        }
      } else {
        for (int32_t i = 0; i < row_.count_; i++) {
          row_.cells_[i].set_nop_value();
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(reader_->init(micro_block_data, column_map))) {
        LOG_WARN("fail to init reader_", KR(ret));
      } else {
        is_inited_ = true;
      }
    }
  }
  return ret;
}

void ObTableLoadBackupMicroBlockScanner::reuse()
{
  if (reader_ != nullptr) {
    reader_->reset();
  }
  row_idx_ = 0;
  is_inited_ = false;
}

bool ObTableLoadBackupMicroBlockScanner::is_valid() const
{
  return is_inited_;
}

int ObTableLoadBackupMicroBlockScanner::get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KP(this));
  } else if (OB_FAIL(reader_->get_row(row_idx_, row_))) {
    if (OB_UNLIKELY(ret != OB_ITER_END)) {
      LOG_WARN("fail to get tow", KR(ret), K(row_idx_));
    }
  } else {
    row_idx_++;
    row = &row_;
  }
  return ret;
}

} // table_load_backup_v_3_x
} // namespace observer
} // namespace oceanbase
