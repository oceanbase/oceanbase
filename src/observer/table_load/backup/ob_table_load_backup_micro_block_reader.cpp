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
#include "observer/table_load/backup/ob_table_load_backup_micro_block_reader.h"
#include "observer/table_load/backup/ob_table_load_backup_flat_row_reader_v1.h"
#include "observer/table_load/backup/ob_table_load_backup_flat_row_reader_v2.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup
{
using namespace storage;

ObMicroBlockReader::ObMicroBlockReader()
  : allocator_("TLD_BMiBReader"),
    backup_version_(ObTableLoadBackupVersion::INVALID),
    column_map_(nullptr),
    row_reader_(nullptr),
    block_header_(nullptr),
    data_begin_(nullptr),
    index_data_(nullptr),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
}

ObMicroBlockReader::~ObMicroBlockReader()
{
  reset();
}

void ObMicroBlockReader::reset()
{
  backup_version_ = ObTableLoadBackupVersion::INVALID;
  column_map_ = nullptr;
  if (nullptr != row_reader_) {
    row_reader_->~ObIRowReader();
    allocator_.free(row_reader_);
    row_reader_ = nullptr;
  }
  block_header_ = nullptr;
  data_begin_ = nullptr;
  index_data_ = nullptr;
  is_inited_ = false;
  allocator_.reset();
}

int ObMicroBlockReader::init(
    const ObMicroBlockData *micro_block_data,
    const ObTableLoadBackupVersion &backup_version,
    const ObIColumnMap *column_map)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already init", KR(ret));
  } else if (OB_UNLIKELY(micro_block_data == nullptr || !micro_block_data->is_valid() ||
                         nullptr == column_map || !column_map->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("column_map is invalid", KR(ret), KP(micro_block_data), KP(column_map));
  } else {
    if (backup_version == ObTableLoadBackupVersion::V_1_4 || backup_version == ObTableLoadBackupVersion::V_2_X_LOG || backup_version == ObTableLoadBackupVersion::V_2_X_PHY) {
      ObFlatRowReaderV1 *row_reader = nullptr;
      if (OB_ISNULL(row_reader_ = row_reader = OB_NEWx(ObFlatRowReaderV1, &allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to new ObFlatRowReaderV1", KR(ret));
      }
    } else {
      ObFlatRowReaderV2 *row_reader = nullptr;
      if (OB_ISNULL(row_reader_ = row_reader = OB_NEWx(ObFlatRowReaderV2, &allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to new ObFlatRowReaderV2", KR(ret));
      }
    }
    if (OB_FAIL(ret)) {
      if (nullptr != row_reader_) {
        row_reader_->~ObIRowReader();
        allocator_.free(row_reader_);
        row_reader_ = nullptr;
      }
    } else {
      backup_version_ = backup_version;
      column_map_ = column_map;
      const char *buf = micro_block_data->get_buf();
      block_header_ = reinterpret_cast<const ObMicroBlockHeader*>(buf);
      data_begin_ = buf + block_header_->header_size_;
      index_data_ = reinterpret_cast<const int32_t *>(buf + block_header_->row_index_offset_);
      is_inited_ = true;
    }
  }
  return ret;
}

int ObMicroBlockReader::get_row(const int64_t idx, ObNewRow &row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(idx < 0 || idx > block_header_->row_count_ || !row.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(idx), K(block_header_->row_count_), K(row));
  } else if (idx == block_header_->row_count_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(row_reader_->read_row(data_begin_, index_data_[idx + 1], index_data_[idx], backup_version_, column_map_, allocator_, row))) {
    LOG_WARN("row reader read row failed", KR(ret));
  }
  return ret;
}

} // table_load_backup
} // namespace observer
} // namespace oceanbase
