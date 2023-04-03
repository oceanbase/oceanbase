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

#define USING_LOG_PREFIX STORAGE
#include "ob_micro_block_header.h"
#include "common/ob_store_format.h"
#include "ob_block_sstable_struct.h"

namespace oceanbase
{
namespace blocksstable
{
const int64_t ObMicroBlockHeader::COLUMN_CHECKSUM_PTR_OFFSET = 64;
ObMicroBlockHeader::ObMicroBlockHeader()
  : magic_(MICRO_BLOCK_HEADER_MAGIC),
    version_(MICRO_BLOCK_HEADER_VERSION),
    header_size_(0),
    header_checksum_(0),
    column_count_(0),
    has_column_checksum_(0),
    reserved16_(0),
    row_count_(0),
    row_store_type_(common::MAX_ROW_STORE),
    opt_(0),
    var_column_count_(0),
    row_offset_(0),
    original_length_(0),
    max_merged_trans_version_(0),
    data_length_(0),
    data_zlength_(0),
    data_checksum_(0),
    column_checksums_(nullptr)
	{
    // COLUMN_CHECKSUM_PTR_OFFSET is the length of fixed component in micro header,
    // column checksum will be stored from this offset. If assert failed, please check.
    STATIC_ASSERT(COLUMN_CHECKSUM_PTR_OFFSET == sizeof(ObMicroBlockHeader), "checksum offset mismatch");
  }

bool ObMicroBlockHeader::is_valid() const
{
  bool valid_data =
      header_size_ == get_serialize_size(column_count_, has_column_checksum_)
      && version_ >= MICRO_BLOCK_HEADER_VERSION_1
      && MICRO_BLOCK_HEADER_MAGIC == magic_
      && column_count_ >= rowkey_column_count_
      && rowkey_column_count_ > 0
      && var_column_count_ <= column_count_
      && row_store_type_ < common::MAX_ROW_STORE; // ObMicroBlockHeader::is_valid
  return valid_data;
}

void ObMicroBlockHeader::set_header_checksum()
{
  int16_t checksum = 0;
  header_checksum_ = 0;

  checksum = checksum ^ magic_;
  checksum = checksum ^ version_;
  checksum = checksum ^ static_cast<int16_t>(row_store_type_);
  checksum = checksum ^ static_cast<int16_t>(opt_);

  format_i32(column_count_, checksum);
  format_i32(rowkey_column_count_, checksum);
  format_i32(has_column_checksum_, checksum);
  format_i32(var_column_count_, checksum);

  format_i64(header_size_, checksum);
  format_i64(row_count_, checksum);
  format_i64(row_offset_, checksum);
  format_i64(original_length_, checksum);
  format_i64(max_merged_trans_version_, checksum);
  format_i64(data_length_, checksum);
  format_i64(data_zlength_, checksum);
  format_i64(data_checksum_, checksum);

  if (has_column_checksum_) {
    for (int64_t i = 0; i < column_count_; ++i) {
      format_i64(column_checksums_[i], checksum);
    }
  }
  header_checksum_ = checksum;
}

int ObMicroBlockHeader::check_header_checksum() const
{
  int ret = OB_SUCCESS;
  int16_t checksum = 0;

  checksum = checksum ^ magic_;
  checksum = checksum ^ version_;
  checksum = checksum ^ header_checksum_;
  checksum = checksum ^ static_cast<int16_t>(row_store_type_);
  checksum = checksum ^ static_cast<int16_t>(opt_);

  format_i32(column_count_, checksum);
  format_i32(rowkey_column_count_, checksum);
  format_i32(has_column_checksum_, checksum);
  format_i32(var_column_count_, checksum);

  format_i64(header_size_, checksum);
  format_i64(row_count_, checksum);
  format_i64(row_offset_, checksum);
  format_i64(original_length_, checksum);
  format_i64(max_merged_trans_version_, checksum);
  format_i64(data_length_, checksum);
  format_i64(data_zlength_, checksum);
  format_i64(data_checksum_, checksum);
  if (has_column_checksum_) {
    for (int64_t i = 0; i < column_count_; ++i) {
      format_i64(column_checksums_[i], checksum);
    }
  }

  if (0 != checksum) {
    ret = OB_PHYSIC_CHECKSUM_ERROR;
    LOG_DBA_ERROR(OB_PHYSIC_CHECKSUM_ERROR, "msg","record check checksum failed", K(ret), K(*this));
  }
  return ret;
}

int ObMicroBlockHeader::check_payload_checksum(const char *buf, const int64_t len) const
{
  int ret = OB_SUCCESS;
  if (NULL == buf || len < 0 || data_zlength_ != len
      || (0 == len && (0 != data_zlength_ || 0 != data_length_ || 0 != data_checksum_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(len), K(data_zlength_),
        K(data_length_), K(data_checksum_));
  } else {
    const int64_t data_checksum = ob_crc64_sse42(buf, len);
    if (data_checksum != data_checksum_) {
      ret = OB_PHYSIC_CHECKSUM_ERROR;
      LOG_DBA_ERROR(OB_PHYSIC_CHECKSUM_ERROR, "msg","checksum error", K(ret), K(data_checksum_), K(data_checksum));
    }
  }
  return ret;
}

int ObMicroBlockHeader::deserialize_and_check_record(
    const char *ptr, const int64_t size,
    const int16_t magic, const char *&payload_ptr, int64_t &payload_size)
{
  int ret = OB_SUCCESS;
  ObMicroBlockHeader header;
  int64_t pos = 0;
  if (NULL == ptr || size < 0 || magic < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(ptr), K(size), K(magic));
  } else if (OB_FAIL(header.deserialize(ptr, size, pos))) {
    LOG_WARN("fail to deserialize header", K(ret));
  } else if (OB_FAIL(header.check_and_get_record(ptr, size, magic, payload_ptr, payload_size))) {
    LOG_WARN("fail to check and get record", K(ret));
  }

  return ret;
}

int ObMicroBlockHeader::check_and_get_record(
    const char *ptr, const int64_t size, const int16_t magic,
    const char *&payload_ptr, int64_t &payload_size) const
{
  int ret = OB_SUCCESS;
  if (nullptr == ptr || magic < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(ptr));
  } else if (magic != magic_) {
    ret = OB_INVALID_DATA;
    LOG_WARN("record header magic is not match", K(ret), K(magic), K(magic_));
  } else if (OB_FAIL(check_header_checksum())) {
    LOG_WARN("fail to check header checksum", K(ret));
  } else {
    const int64_t header_size = header_size_;
    if (size < header_size) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("buffer not enough", K(ret), K(size), K(header_size));
    } else {
      payload_ptr = ptr;
      payload_size = size;
      if (OB_FAIL(check_payload_checksum(ptr + header_size, size - header_size))) {
        LOG_WARN("fail to check payload checksum", K(ret));
      }
    }
  }
  return ret;
}

int ObMicroBlockHeader::check_record(const char *ptr, const int64_t size, const int16_t magic) const
{
  int ret = OB_SUCCESS;
  const char *payload_ptr = nullptr;
  int64_t payload_size = 0;
  if (nullptr == ptr || size < 0 || magic < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(ptr), K(size), K(magic));
  } else if (OB_FAIL(check_and_get_record(ptr, size, magic, payload_ptr, payload_size))) {
    LOG_WARN("fail to check record", K(ret), KP(ptr), K(size), K(magic));
  }
  return ret;
}

int ObMicroBlockHeader::deserialize_and_check_record(const char *ptr, const int64_t size, const int16_t magic)
{
  int ret = OB_SUCCESS;
  const char *payload_buf = nullptr;
  int64_t payload_size = 0;
  if (nullptr == ptr || magic < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(ptr), K(magic));
  } else if (OB_FAIL(deserialize_and_check_record(ptr, size, magic, payload_buf, payload_size))) {
    LOG_WARN("fail to check record", K(ret));
  }
  return ret;
}

int ObMicroBlockHeader::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf
              || buf_len < pos + COLUMN_CHECKSUM_PTR_OFFSET
              || buf_len < pos + header_size_
              || !is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_len), K(pos), KPC(this));
  } else {
    buf += pos;
    int64_t new_pos = 0;

    const int64_t serialize_size = header_size_;
    ObMicroBlockHeader *header = reinterpret_cast<ObMicroBlockHeader *>(buf);
    *header = *this;
    new_pos += COLUMN_CHECKSUM_PTR_OFFSET ;
    if (has_column_checksum_) {
      header->column_checksums_ = reinterpret_cast<int64_t *>(buf + new_pos);
      if (OB_ISNULL(column_checksums_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expect not null checksums", K(ret), KPC(this));
      } else {
        MEMCPY(header->column_checksums_, column_checksums_, column_count_ * sizeof(int64_t));
        new_pos += column_count_ * sizeof(int64_t);
        header->column_checksums_ = nullptr; //always serialize nullptr
      }
    } else {
      header->column_checksums_ = nullptr;
    }
    pos += new_pos;
  }
  return ret;
}

int ObMicroBlockHeader::deserialize(const char *buf, int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (nullptr == buf || buf_len < pos + COLUMN_CHECKSUM_PTR_OFFSET) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_len));
  } else {
    buf += pos;
    int64_t new_pos = 0;

    const ObMicroBlockHeader *header = reinterpret_cast<const ObMicroBlockHeader *>(buf);
    if (OB_UNLIKELY(nullptr == header
                || !header->is_valid()
                || buf_len < pos + header->header_size_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid header to deserialize", K(ret), K(buf_len), K(pos), KPC(header));
    } else {
      *this = *header;
      new_pos += COLUMN_CHECKSUM_PTR_OFFSET ;
      if (has_column_checksum_) {
        const int64_t *column_checksums = nullptr;
        column_checksums = reinterpret_cast<const int64_t *>(buf + new_pos);
        column_checksums_ = const_cast<int64_t *>(column_checksums);
        new_pos += column_count_ * sizeof(int64_t);
      } else {
        column_checksums_ = nullptr;
      }
      pos += new_pos;
    }
  }
  return ret;
}

bool ObMicroBlockHeader::is_contain_hash_index() const
{
  return version_ >= MICRO_BLOCK_HEADER_VERSION_2 && contains_hash_index_ == 1;
}

}//end namespace blocksstable
}//end namespace oceanbase
