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

#include "ob_log_block.h"

#include "common/ob_clock_generator.h"
#include "share/ob_define.h"
#include "lib/checksum/ob_crc64.h"
#include "lib/utility/utility.h"
#include "lib/utility/serialization.h"

namespace oceanbase {
using namespace common;
namespace clog {
ObLogBlockMetaV2::ObLogBlockMetaV2()
    : magic_(0),
      version_(BLOCK_VERSION),
      type_(OB_NONE_BLOCK),
      total_len_(0),
      padding_len_(0),
      timestamp_(0),
      data_checksum_(0),
      meta_checksum_(0)
{}

ObLogBlockMetaV2::~ObLogBlockMetaV2()
{
  reset();
}

int ObLogBlockMetaV2::generate_block(const char* buf, const int64_t data_len, const ObBlockType type)
{
  int ret = OB_SUCCESS;
  int64_t meta_len = get_serialize_size();
  int64_t total_len = upper_align((data_len + meta_len), CLOG_DIO_ALIGN_SIZE);

  magic_ = META_MAGIC;
  version_ = BLOCK_VERSION;
  type_ = type;
  total_len_ = total_len;
  padding_len_ = total_len - data_len - meta_len;
  timestamp_ = ObClockGenerator::getClock();  // Guaranteed no fallback
  if (OB_DATA_BLOCK == type) {
    data_checksum_ = 0;
  } else {
    data_checksum_ = calc_data_checksum_(buf, data_len);
  }
  meta_checksum_ = calc_meta_checksum_();

  return ret;
}

void ObLogBlockMetaV2::reset()
{
  magic_ = META_MAGIC;
  version_ = BLOCK_VERSION;
  type_ = OB_NONE_BLOCK;
  total_len_ = 0;
  padding_len_ = 0;
  timestamp_ = 0;
  data_checksum_ = 0;
  meta_checksum_ = 0;
}

bool ObLogBlockMetaV2::check_integrity(const char* data_buf, const int64_t data_len) const
{
  bool bool_ret = true;
  int16_t magic = META_MAGIC;
  if (magic != magic_) {
    bool_ret = false;
    CLOG_LOG(WARN, "magic is different", "magic", magic_, "expected", magic);
  } else if (!check_meta_checksum()) {
    bool_ret = false;
    CLOG_LOG(WARN, "check meta checksum fails", "meta", to_cstring(*this));
    // OB_DATA_BLOCK no need verify data_checksum
    // Every log in OB_DATA_BLOCK has own checksum, no need to do an overall checksum verification.
  } else if (OB_DATA_BLOCK != type_ && !check_data_checksum_(data_buf, data_len)) {
    bool_ret = false;
    CLOG_LOG(WARN, "check data checksum fails");
  } else {
    // do nothing
  }
  return bool_ret;
}

bool ObLogBlockMetaV2::check_data_checksum_(const char* buf, const int64_t data_len) const
{
  bool bool_ret = true;
  int64_t ori_data_len = get_data_len();

  // we do not allow null buf here
  if (NULL == buf) {
    bool_ret = false;
    CLOG_LOG(WARN, "buf is null", K(data_len), K(*this));
  } else if ((0 == data_len) && (ori_data_len == data_len) && (0 == data_checksum_)) {
    // the block with no data, correct and do nothing
  } else if ((data_len > 0) && (data_len == ori_data_len)) {
    int64_t crc_check_sum = ob_crc64(buf, data_len);
    if (crc_check_sum == data_checksum_) {
      // checksum pass
    } else {
      bool_ret = false;
      CLOG_LOG(WARN,
          "checksum different",
          "data",
          buf,
          "data_len",
          data_len,
          "checksum",
          crc_check_sum,
          "expected",
          data_checksum_);
    }
  } else {
    bool_ret = false;
    CLOG_LOG(WARN,
        "checksum fail",
        "data",
        buf,
        "data_len",
        data_len,
        "ori_data_len",
        ori_data_len,
        "data_checksum_",
        data_checksum_);
  }
  return bool_ret;
}

bool ObLogBlockMetaV2::check_meta_checksum() const
{
  bool bool_ret = true;
  int64_t calc_checksum = calc_meta_checksum_();
  if (meta_checksum_ != calc_checksum) {
    bool_ret = false;
    CLOG_LOG(WARN, "meta checksum fail", "calc_meta_checksum_", calc_checksum, "expected", meta_checksum_);
  }
  return bool_ret;
}

int64_t ObLogBlockMetaV2::calc_data_checksum_(const char* buf, const int64_t data_len) const
{
  int64_t checksum = 0;
  if ((NULL == buf) && (data_len == 0)) {
    // correct situation, checksum = 0 is expected
  } else if ((NULL != buf) && (data_len > 0)) {
    checksum = ob_crc64(buf, data_len);
  } else {
    // wrong situations, checksum = 0 is default
    CLOG_LOG(ERROR, "data checksum", "buf", buf, "data_len", data_len);
  }
  return checksum;
}

int64_t ObLogBlockMetaV2::calc_meta_checksum_() const
{
  int64_t checksum = 0;
  // excludes meta_checksum_, the calculation relies on the structure of the meta
  int64_t calc_checksum_len = sizeof(*this) - sizeof(meta_checksum_);
  checksum = ob_crc64(this, calc_checksum_len);
  return checksum;
}

DEFINE_SERIALIZE(ObLogBlockMetaV2)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if ((NULL == buf) || (buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if ((OB_SUCCESS != serialization::encode_i16(buf, buf_len, new_pos, magic_)) ||
             (OB_SUCCESS != serialization::encode_i16(buf, buf_len, new_pos, version_)) ||
             (OB_SUCCESS != serialization::encode_i32(buf, buf_len, new_pos, type_)) ||
             (OB_SUCCESS != serialization::encode_i64(buf, buf_len, new_pos, total_len_)) ||
             (OB_SUCCESS != serialization::encode_i64(buf, buf_len, new_pos, padding_len_)) ||
             (OB_SUCCESS != serialization::encode_i64(buf, buf_len, new_pos, timestamp_)) ||
             (OB_SUCCESS != serialization::encode_i64(buf, buf_len, new_pos, data_checksum_)) ||
             (OB_SUCCESS != serialization::encode_i64(buf, buf_len, new_pos, meta_checksum_))) {
    ret = OB_SERIALIZE_ERROR;
    CLOG_LOG(WARN, "serialize error", "buf", buf, "buf_len", buf_len, "pos", pos, "new_pos", new_pos);
  } else {
    pos = new_pos;
  }
  return ret;
}

DEFINE_DESERIALIZE(ObLogBlockMetaV2)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if ((NULL == buf) || (data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if ((OB_SUCCESS != serialization::decode_i16(buf, data_len, new_pos, &magic_)) ||
             (OB_SUCCESS != serialization::decode_i16(buf, data_len, new_pos, &version_)) ||
             (OB_SUCCESS != serialization::decode_i32(buf, data_len, new_pos, &type_)) ||
             (OB_SUCCESS != serialization::decode_i64(buf, data_len, new_pos, &total_len_)) ||
             (OB_SUCCESS != serialization::decode_i64(buf, data_len, new_pos, &padding_len_)) ||
             (OB_SUCCESS != serialization::decode_i64(buf, data_len, new_pos, &timestamp_)) ||
             (OB_SUCCESS != serialization::decode_i64(buf, data_len, new_pos, &data_checksum_)) ||
             (OB_SUCCESS != serialization::decode_i64(buf, data_len, new_pos, &meta_checksum_))) {
    ret = OB_DESERIALIZE_ERROR;
  } else {
    pos = new_pos;
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObLogBlockMetaV2)
{
  int64_t size = 0;
  size += serialization::encoded_length_i16(magic_);
  size += serialization::encoded_length_i16(version_);
  size += serialization::encoded_length_i32(type_);
  size += serialization::encoded_length_i64(total_len_);
  size += serialization::encoded_length_i64(padding_len_);
  size += serialization::encoded_length_i64(timestamp_);
  size += serialization::encoded_length_i64(data_checksum_);
  size += serialization::encoded_length_i64(meta_checksum_);
  return size;
}

int ObLogBlockMetaV2::build_serialized_block(char* buf, const int64_t buf_len, const char* data_buf,
    const int64_t data_len, const ObBlockType type, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || (buf_len <= 0) || OB_ISNULL(data_buf) || (data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid argument", K(ret), KP(buf), K(buf_len), KP(data_buf), K(data_len));
  } else if (OB_FAIL(generate_block(data_buf, data_len, type))) {
    CLOG_LOG(ERROR, "generate block failed.", K(ret), KP(buf), K(data_len), K(type));
  } else if (OB_FAIL(serialize(buf, buf_len, pos))) {
    CLOG_LOG(ERROR, "serialize block_meta_ failed.", K(ret), KP(buf), K(buf_len), K(pos));
  }
  return ret;
}
}  // end namespace clog
}  // end namespace oceanbase
