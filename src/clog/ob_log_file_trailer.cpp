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

#include "ob_log_file_trailer.h"
#include "lib/checksum/ob_crc64.h"
#include "lib/utility/serialization.h"

namespace oceanbase {
using namespace common;
namespace clog {
ObLogFileTrailer::ObLogFileTrailer()
    : block_meta_(), version_(0), start_pos_(OB_INVALID_OFFSET), file_id_(OB_INVALID_FILE_ID)
{}

ObLogFileTrailer::~ObLogFileTrailer()
{}

int ObLogFileTrailer::set_start_pos(const offset_t start_pos)
{
  int ret = OB_SUCCESS;
  if (!is_valid_offset(static_cast<offset_t>(start_pos))) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "set_start_pos", K(start_pos), K(ret));
  } else {
    start_pos_ = start_pos;
  }
  return ret;
}

int ObLogFileTrailer::set_file_id(const file_id_t file_id)
{
  int ret = OB_SUCCESS;
  if (!is_valid_file_id(file_id)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "set_file_id", K(file_id), K(ret));
  } else {
    file_id_ = file_id;
  }
  return ret;
}

int ObLogFileTrailer::build_serialized_trailer(
    char* buf, const int64_t buf_len, const offset_t start_pos, const file_id_t file_id, int64_t& pos)
{
  int ret = OB_SUCCESS;
  const int64_t meta_size = block_meta_.get_serialize_size();
  int64_t data_pos = pos + meta_size;
  int64_t meta_pos = pos;

  if (OB_ISNULL(buf) || (buf_len <= 0) || !is_valid_file_id(file_id) || !is_valid_offset(start_pos)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid argument", K(ret), KP(buf), K(buf_len), K(start_pos), K(file_id), K(pos));
  } else {
    version_ = CLOG_FILE_VERSION;
    start_pos_ = start_pos;
    file_id_ = file_id;

    char* data = reinterpret_cast<char*>(&(this->start_pos_));
    int64_t data_len = sizeof(start_pos_) + sizeof(file_id_);

    if ((OB_SUCCESS != serialization::encode_i16(buf, buf_len, data_pos, version_)) ||
        (OB_SUCCESS != serialization::encode_i32(buf, buf_len, data_pos, start_pos_)) ||
        (OB_SUCCESS != serialization::encode_i32(buf, buf_len, data_pos, file_id_))) {
      ret = OB_SERIALIZE_ERROR;
      CLOG_LOG(ERROR, "serialize error", K(ret), KP(buf), K(buf_len), K(pos), K(data_pos));
    } else if (OB_FAIL(block_meta_.build_serialized_block(buf, buf_len, data, data_len, OB_TRAILER_BLOCK, meta_pos))) {
      CLOG_LOG(ERROR,
          "build_serialized_block error",
          K(ret),
          KP(buf),
          K(buf_len),
          KP(data),
          K(data_len),
          K(pos),
          K(meta_pos));
    } else {
      pos = data_pos;
      CLOG_LOG(INFO, "serialize tailer", K(version_), K(start_pos_), K(file_id_));
    }
  }

  return ret;
}

DEFINE_DESERIALIZE(ObLogFileTrailer)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if ((NULL == buf) || (data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if ((OB_SUCCESS != block_meta_.deserialize(buf, data_len, new_pos)) ||
             (OB_SUCCESS != serialization::decode_i16(buf, data_len, new_pos, &version_)) ||
             (OB_SUCCESS != serialization::decode_i32(buf, data_len, new_pos, &start_pos_)) ||
             (OB_SUCCESS != serialization::decode_i32(buf, data_len, new_pos, reinterpret_cast<int32_t*>(&file_id_)))) {
    ret = OB_DESERIALIZE_ERROR;
    CLOG_LOG(ERROR, "deserialize error", K(buf), K(data_len), K(pos), K(new_pos));
  } else {
    pos = new_pos;
    // Hack: this is to fix the old format bug that verion_ is not included in the data_checksum_
    // so we have to manually construct buf address here
    const char* check_buf = reinterpret_cast<const char*>(&(this->start_pos_));
    int64_t check_len = sizeof(start_pos_) + sizeof(file_id_);
    if (!block_meta_.check_integrity(check_buf, check_len)) {
      ret = OB_INVALID_DATA;
      CLOG_LOG(ERROR, "check integrity fail", "trailer", to_cstring(*this));
    }
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObLogFileTrailer)
{
  int64_t size = 0;
  size += block_meta_.get_serialize_size();
  size += serialization::encoded_length_i16(version_);
  size += serialization::encoded_length_i64(start_pos_);
  size += serialization::encoded_length_i64(file_id_);
  return size;
}

ObIlogFileTrailerV2::ObIlogFileTrailerV2()
{
  reset();
}

ObIlogFileTrailerV2::~ObIlogFileTrailerV2()
{
  reset();
}

int ObIlogFileTrailerV2::init(const offset_t info_block_start_offset, const int32_t info_block_size,
    const offset_t memberlist_block_start_offset, const int32_t memberlist_block_size,
    const int64_t file_content_checksum)
{
  int ret = OB_SUCCESS;
  if (info_block_start_offset <= 0 || info_block_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid arguments", K(ret), K(info_block_start_offset), K(info_block_size));
  } else {
    magic_number_ = MAGIC_NUMBER;
    info_block_start_offset_ = info_block_start_offset;
    info_block_size_ = info_block_size;
    memberlist_block_start_offset_ = memberlist_block_start_offset;
    memberlist_block_size_ = memberlist_block_size;
    file_content_checksum_ = file_content_checksum;
    update_trailer_checksum_();
  }
  return ret;
}

void ObIlogFileTrailerV2::reset()
{
  magic_number_ = 0;
  padding1_ = 0;
  padding2_ = 0;
  info_block_start_offset_ = OB_INVALID_OFFSET;
  info_block_size_ = -1;
  memberlist_block_start_offset_ = OB_INVALID_OFFSET;
  memberlist_block_size_ = -1;
  file_content_checksum_ = 0;
  trailer_checksum_ = 0;
}

int ObIlogFileTrailerV2::check_magic_number(const char* buf, const int64_t len, bool& is_valid)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int16_t magic_number = 0;
  if (NULL == buf || len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid arguments", K(ret), KP(buf), K(len));
  } else if (OB_SUCCESS != serialization::decode_i16(buf, len, pos, &magic_number)) {
    ret = OB_SERIALIZE_ERROR;
    CLOG_LOG(ERROR, "deserialize failed", K(ret), KP(buf), K(len), K(pos));
  } else {
    is_valid = (magic_number == MAGIC_NUMBER);
  }
  return ret;
}

offset_t ObIlogFileTrailerV2::get_info_block_start_offset() const
{
  return info_block_start_offset_;
}

int32_t ObIlogFileTrailerV2::get_info_block_size() const
{
  return info_block_size_;
}

int64_t ObIlogFileTrailerV2::get_file_content_checksum() const
{
  return file_content_checksum_;
}

DEFINE_SERIALIZE(ObIlogFileTrailerV2)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (NULL == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid arguments", K(ret), KP(buf), K(buf_len));
  } else if (OB_SUCCESS != serialization::encode_i16(buf, buf_len, new_pos, magic_number_) ||
             OB_SUCCESS != serialization::encode_i16(buf, buf_len, new_pos, padding1_) ||
             OB_SUCCESS != serialization::encode_i32(buf, buf_len, new_pos, padding2_) ||
             OB_SUCCESS != serialization::encode_i32(buf, buf_len, new_pos, info_block_start_offset_) ||
             OB_SUCCESS != serialization::encode_i32(buf, buf_len, new_pos, info_block_size_) ||
             OB_SUCCESS != serialization::encode_i32(buf, buf_len, new_pos, memberlist_block_start_offset_) ||
             OB_SUCCESS != serialization::encode_i32(buf, buf_len, new_pos, memberlist_block_size_) ||
             OB_SUCCESS != serialization::encode_i64(buf, buf_len, new_pos, file_content_checksum_) ||
             OB_SUCCESS != serialization::encode_i64(buf, buf_len, new_pos, trailer_checksum_)) {
    ret = OB_SERIALIZE_ERROR;
    CLOG_LOG(ERROR, "serialize failed", K(ret), KP(buf), K(buf_len), K(pos), K(new_pos));
  } else {
    pos = new_pos;
    CLOG_LOG(INFO,
        "ObIlogFileTrailerV2 serialize success",
        K(ret),
        K(magic_number_),
        K(info_block_start_offset_),
        K(info_block_size_),
        K(memberlist_block_start_offset_),
        K(memberlist_block_size_),
        K(file_content_checksum_),
        K(trailer_checksum_));
  }
  return ret;
}

DEFINE_DESERIALIZE(ObIlogFileTrailerV2)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (NULL == buf || data_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid arguments", K(ret), KP(buf), K(data_len));
  } else if (OB_SUCCESS != serialization::decode_i16(buf, data_len, new_pos, &magic_number_) ||
             OB_SUCCESS != serialization::decode_i16(buf, data_len, new_pos, &padding1_) ||
             OB_SUCCESS != serialization::decode_i32(buf, data_len, new_pos, &padding2_) ||
             OB_SUCCESS != serialization::decode_i32(buf, data_len, new_pos, &info_block_start_offset_) ||
             OB_SUCCESS != serialization::decode_i32(buf, data_len, new_pos, &info_block_size_) ||
             OB_SUCCESS != serialization::decode_i32(buf, data_len, new_pos, &memberlist_block_start_offset_) ||
             OB_SUCCESS != serialization::decode_i32(buf, data_len, new_pos, &memberlist_block_size_) ||
             OB_SUCCESS != serialization::decode_i64(buf, data_len, new_pos, &file_content_checksum_) ||
             OB_SUCCESS != serialization::decode_i64(buf, data_len, new_pos, &trailer_checksum_)) {
    ret = OB_SERIALIZE_ERROR;
    CLOG_LOG(ERROR, "deserialize failed", K(ret), KP(buf), K(data_len), K(pos), K(new_pos));
  } else {
    pos = new_pos;
    CLOG_LOG(TRACE,
        "ObIlogFileTrailerV2 deserialize success",
        K(ret),
        K(magic_number_),
        K(info_block_start_offset_),
        K(info_block_size_),
        K(memberlist_block_start_offset_),
        K(memberlist_block_size_),
        K(file_content_checksum_),
        K(trailer_checksum_));
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObIlogFileTrailerV2)
{
  int64_t size = 0;
  size += serialization::encoded_length_i16(magic_number_);
  size += serialization::encoded_length_i16(padding1_);
  size += serialization::encoded_length_i32(padding2_);
  size += serialization::encoded_length_i32(info_block_start_offset_);
  size += serialization::encoded_length_i32(info_block_size_);
  size += serialization::encoded_length_i32(memberlist_block_start_offset_);
  size += serialization::encoded_length_i32(memberlist_block_size_);
  size += serialization::encoded_length_i64(file_content_checksum_);
  size += serialization::encoded_length_i64(trailer_checksum_);
  return size;
}

void ObIlogFileTrailerV2::update_trailer_checksum_()
{
  int64_t calc_checksum_len = sizeof(*this) - sizeof(trailer_checksum_);
  trailer_checksum_ = ob_crc64(this, calc_checksum_len);
}
}  // end namespace clog
}  // end namespace oceanbase
