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
#include "observer/table_load/backup/v_1_4/ob_table_load_backup_micro_block_scanner.h"
#include "common/ob_record_header.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup_v_1_4
{
using namespace common;

/**
 * ObTableLoadBackupMicroBlockRecordHeader
 */
int ObTableLoadBackupMicroBlockRecordHeader::check_header_checksum() const
{
  int ret = OB_SUCCESS;
  int16_t checksum = 0;

  checksum = checksum ^ magic_;
  checksum = static_cast<int16_t>(checksum ^ static_cast<int16_t>(header_length_));
  checksum = static_cast<int16_t>(checksum ^ static_cast<int16_t>(version_));
  checksum = checksum ^ header_checksum_;
  checksum = checksum ^ reserved16_;
  format_i64(data_length_, checksum);
  format_i64(data_zlength_, checksum);
  format_i64(data_checksum_, checksum);

  if (OB_UNLIKELY(checksum != 0)) {
    ret = OB_CHECKSUM_ERROR;
    LOG_WARN("record check checksum failed", K(*this));
  }

  return ret;
}

int ObTableLoadBackupMicroBlockRecordHeader::check_payload_checksum(const char *buf, const int64_t len) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(buf == nullptr || len < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KP(buf), K(len));
  } else if (OB_UNLIKELY(len == 0 && (data_zlength_ != 0 || data_length_ != 0 || data_checksum_ != 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KP(buf), K(len), K(data_zlength_), K(data_length_), K(data_checksum_));
  } else if ((OB_UNLIKELY(data_zlength_ != len))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("data length is not correct", K(data_zlength_), K(len));
  } else {
    int64_t crc_check_sum = ob_crc64_sse42(buf, len);
    if (OB_UNLIKELY(crc_check_sum !=  data_checksum_)) {
      ret = OB_CHECKSUM_ERROR;
      LOG_WARN("checksum error", K(crc_check_sum), K(data_checksum_));
    }
  }

  return ret;
}

int ObTableLoadBackupMicroBlockRecordHeader::check_record(const char *ptr, const int64_t size)
{
  int ret = OB_SUCCESS;
  const int64_t record_header_len = sizeof(ObTableLoadBackupMicroBlockRecordHeader);
  if (OB_UNLIKELY(ptr == nullptr || size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KP(ptr), K(size));
  } else if (OB_UNLIKELY(record_header_len > size)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("invalid args, header size too small", K(record_header_len), K(size));
  } else {
    const ObTableLoadBackupMicroBlockRecordHeader *header =
            reinterpret_cast<const ObTableLoadBackupMicroBlockRecordHeader*>(ptr);
    const char *payload_ptr = ptr + record_header_len;
    int64_t payload_size = size - record_header_len;
    if (header->magic_ != PRE_MICRO_BLOCK_RECORD_HEADER_MAGIC) {
      ret = OB_INVALID_DATA;
      LOG_WARN("record header magic is not match", K(*header), K(header->magic_));
    } else if (header->version_ != PRE_MICRO_BLOCK_RECORD_HEADER_VERSION) {
      ret = OB_INVALID_DATA;
      LOG_WARN("record header version is not match", K(*header));
    } else if (OB_FAIL(header->check_header_checksum())) {
      LOG_WARN("check header checksum failed", KR(ret), K(*header), K(record_header_len));
    } else if (OB_FAIL(header->check_payload_checksum(payload_ptr, payload_size))) {
      LOG_WARN("check data checksum failed", KR(ret), K(*header), KP(payload_ptr), K(payload_size));
    }
  }

  return ret;
}


/**
 * ObTableLoadBackupMicroBlockScanner
 */
int ObTableLoadBackupMicroBlockScanner::init(
    const char *buf,
    const ObTableLoadBackupColumnMap *column_map)
{
  int ret = OB_SUCCESS;
  // meta只在内部的scan中可能会用到，所以为nullptr是可能存在的
  if (OB_UNLIKELY(buf == nullptr ||
                  column_map == nullptr ||
                  !column_map->is_inited())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(buf), KP(column_map));
  } else {
    header_ = reinterpret_cast<const ObTableLoadBackupMicroBlockHeader_V_1_4*>(buf);
    if (OB_UNLIKELY(!header_->is_valid())) {
      LOG_WARN("header_ is invalid", KR(ret), K(*header_));
    } else {
      column_map_ = column_map;
      data_begin_ = buf + header_->header_size_;
      index_begin_ = reinterpret_cast<const int32_t*>(buf + header_->row_index_offset_);
      if (OB_FAIL(init_row())) {
        LOG_WARN("fail to init row", KR(ret));
      } else {
        is_inited_ = true;
      }
    }
  }

  return ret;
}

void ObTableLoadBackupMicroBlockScanner::reset()
{
  reader_.reset();
  header_ = nullptr;
  column_map_ = nullptr;
  data_begin_ = nullptr;
  index_begin_ = nullptr;
  row_.reset();
  cur_idx_ = 0;
  allocator_.reset();
  is_inited_ = false;
}

void ObTableLoadBackupMicroBlockScanner::reuse()
{
  reader_.reset();
  header_ = nullptr;
  column_map_ = nullptr;
  data_begin_ = nullptr;
  index_begin_ = nullptr;
  cur_idx_ = 0;
  is_inited_ = false;
}

int ObTableLoadBackupMicroBlockScanner::get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  if(!is_inited_){
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if(cur_idx_ >= header_->row_count_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(reader_.read_meta_row(column_map_,
                                           data_begin_,
                                           *(index_begin_ + cur_idx_ + 1),
                                           *(index_begin_ + cur_idx_),
                                           row_))) {
    LOG_WARN("row reader fail to read row", KR(ret));
  } else {
    cur_idx_++;
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

} // table_load_backup_v_1_4
} // namespace observer
} // namespace oceanbase
