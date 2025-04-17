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
#include "observer/table_load/backup/ob_table_load_backup_micro_block_scanner.h"
#include "observer/table_load/backup/ob_table_load_backup_micro_block_reader.h"
#include "observer/table_load/backup/ob_table_load_backup_micro_block_decoder.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup
{
ObTableLoadBackupMicroBlockScanner::ObTableLoadBackupMicroBlockScanner()
  : allocator_("TLD_BMiBScan"),
    reader_(nullptr),
    column_map_(nullptr),
    row_idx_(0),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
}

ObTableLoadBackupMicroBlockScanner::~ObTableLoadBackupMicroBlockScanner()
{
  reset();
}

void ObTableLoadBackupMicroBlockScanner::reset()
{
  if (reader_ != nullptr) {
    reader_->~ObIMicroBlockReader();
    allocator_.free(reader_);
    reader_ = nullptr;
  }
  row_.reset();
  row_idx_ = 0;
  allocator_.reset();
  is_inited_ = false;
}

void ObTableLoadBackupMicroBlockScanner::reuse()
{
  if (reader_ != nullptr) {
    reader_->reset();
  }
  column_map_ = nullptr;
  row_idx_ = 0;
  is_inited_ = false;
}

int ObTableLoadBackupMicroBlockScanner::init(
    const ObMicroBlockData *micro_block_data,
    const ObTableLoadBackupVersion &backup_version,
    const ObIColumnMap *column_map)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(micro_block_data == nullptr || !micro_block_data->is_valid() ||
                         column_map == nullptr || !column_map->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(micro_block_data), K(backup_version), KPC(column_map));
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
      column_map_ = column_map;
      if (OB_FAIL(reader_->init(micro_block_data, backup_version, column_map))) {
        LOG_WARN("fail to init reader_", KR(ret));
      } else if (OB_FAIL(init_row())) {
        LOG_WARN("fail to init row", KR(ret));
      } else {
        is_inited_ = true;
      }
    }
  }
  return ret;
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
      LOG_WARN("fail to get row", KR(ret), K(row_idx_));
    }
  } else {
    row_idx_++;
    row = &row_;
  }
  return ret;
}

int ObTableLoadBackupMicroBlockScanner::init_row()
{
  int ret = OB_SUCCESS;
  int64_t column_count = column_map_->get_store_count();
  if (row_.count_ <= 0) {
    if (OB_FAIL(ob_create_row(allocator_, column_count, row_))) {
      LOG_WARN("fail to init row_", KR(ret));
    }
  } else if (OB_UNLIKELY(row_.count_ != column_count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected count", KR(ret), K(row_.count_), K(column_count));
  }
  for (int32_t i = 0; OB_SUCC(ret) && i < column_count; i++) {
    row_.cells_[i].set_nop_value();
  }
  return ret;
}

} // table_load_backup
} // namespace observer
} // namespace oceanbase
