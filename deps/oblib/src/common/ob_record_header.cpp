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

#include "common/ob_record_header.h"

namespace oceanbase
{
namespace common
{
ObRecordHeader::ObRecordHeader()
    : magic_(0), header_length_(0), version_(0), header_checksum_(0)
    , timestamp_(0), data_length_(0), data_zlength_(0), data_checksum_(0)
{
}

void ObRecordHeader::set_header_checksum()
{
  header_checksum_ = 0;
  int16_t checksum = 0;

  format_i64(magic_, checksum);
  checksum = checksum ^ header_length_;
  checksum = checksum ^ version_;
  checksum = checksum ^ header_checksum_;
  checksum = static_cast<int16_t>(checksum ^ timestamp_);
  format_i32(data_length_, checksum);
  format_i32(data_zlength_, checksum);
  format_i64(data_checksum_, checksum);
  header_checksum_ = checksum;
}

int ObRecordHeader::check_header_checksum() const
{
  int ret           = OB_SUCCESS;
  int16_t checksum  = 0;

  format_i64(magic_, checksum);
  checksum = checksum ^ header_length_;
  checksum = checksum ^ version_;
  checksum = checksum ^ header_checksum_;
  checksum = static_cast<int16_t>(checksum ^ timestamp_);
  format_i32(data_length_, checksum);
  format_i32(data_zlength_, checksum);
  format_i64(data_checksum_, checksum);
  if (0 != checksum) {
    ret = OB_CHECKSUM_ERROR;
    COMMON_LOG(WARN, "record check checksum failed.", K(*this), K(ret));
  }

  return ret;
}

int ObRecordHeader::check_payload_checksum(const char *buf, const int64_t len) const
{
  int ret = OB_SUCCESS;

  /**
   * for network package, maybe there is only one recorder header
   * without payload data, so the payload data lenth is 0, and
   * checksum is 0. we skip this case and return success
   */
  if (NULL == buf || len < 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments.", KP(buf), K(len), K(ret));
  } else if (0 == len && (0 != data_zlength_ || 0 != data_length_ || 0 != data_checksum_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments.",
               KP(buf), K(len),
               K_(data_zlength), K_(data_length),
               K_(data_checksum), K(ret));
  } else if ((data_zlength_ != len)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "data length is not correct.",
               K_(data_zlength), K(len), K(ret));
  } else {
    int64_t crc_check_sum = ob_crc64(buf, len);
    if (crc_check_sum != data_checksum_) {
      ret = OB_CHECKSUM_ERROR;
      COMMON_LOG(WARN, "checksum error.",
                 K(crc_check_sum), K_(data_checksum), K(ret));
    }
  }

  return ret;
}


int ObRecordHeader::check_record(const char *buf, const int64_t len, const int16_t magic)
{
  int ret = OB_SUCCESS;
  ObRecordHeader record_header;
  int64_t pos = 0;

  if (NULL == buf || len < 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments.", KP(buf), K(len), K(ret));
  } else if (OB_FAIL(record_header.deserialize(buf, len, pos))) {
    COMMON_LOG(WARN, "deserialize record header failed.",
               KP(buf), K(len), K(pos), K(ret));
  } else if (OB_UNLIKELY(record_header.magic_ != magic)) {
    ret = OB_INVALID_DATA;
    COMMON_LOG(WARN, "record header magic is not match",
               K(record_header), K(magic), K(ret));
  } else if (OB_UNLIKELY(record_header.data_zlength_ != len - pos)) {
    ret = OB_INVALID_DATA;
    COMMON_LOG(WARN, "record header length is not match",
               K(record_header), "data length", (len - pos), K(ret));
  } else if (OB_FAIL(record_header.check_header_checksum())) {
    COMMON_LOG(WARN, "check header checksum failed.",
               K(record_header), "data length", (len - pos), K(ret));
  } else if (OB_FAIL(record_header.check_payload_checksum(buf + pos, len - pos))) {
    COMMON_LOG(WARN, "check data checksum failed.",
               K(record_header), "data length", (len - pos), K(ret));
  }

  return ret;
}

int ObRecordHeader::check_record(const ObRecordHeader &record_header,
                                 const char *payload_buf,
                                 const int64_t payload_len,
                                 const int16_t magic)
{
  int ret = OB_SUCCESS;
  if (NULL == payload_buf || payload_len < 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments.",
               KP(payload_buf), K(payload_len), K(ret));
  } else if (OB_UNLIKELY(record_header.magic_ != magic)) {
    ret = OB_INVALID_DATA;
    COMMON_LOG(WARN, "record header magic is not match",
               K(record_header), K(magic), K(ret));
  } else if (OB_UNLIKELY(record_header.data_zlength_ != payload_len)) {
    ret = OB_INVALID_DATA;
    COMMON_LOG(WARN, "record header length is not match",
               K(record_header), K(payload_len), K(ret));
  } else if (OB_FAIL(record_header.check_header_checksum())) {
    COMMON_LOG(WARN, "check header checksum failed.",
               K(record_header), K(payload_len), K(ret));
  } else if (OB_FAIL(record_header.check_payload_checksum(payload_buf, payload_len))) {
    COMMON_LOG(WARN, "check data checksum failed.",
               K(record_header), K(payload_len), K(ret));
  }

  return ret;
}

int ObRecordHeader::check_record(const char *ptr, const int64_t size,
                                 const int16_t magic, ObRecordHeader &header,
                                 const char *&payload_ptr, int64_t &payload_size)
{
  int ret = OB_SUCCESS;
  int64_t payload_pos = 0;

  if (NULL == ptr || size < OB_RECORD_HEADER_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments.",
               KP(ptr), K(size), K(ret));
  } else if (OB_FAIL(header.deserialize(ptr, size, payload_pos))) {
    COMMON_LOG(WARN, "deserialize header failed ",
               KP(ptr), K(size), K(payload_pos), K(ret));
  } else if (header.header_length_ != payload_pos) {
    ret = OB_INVALID_DATA;
    COMMON_LOG(WARN, "header length is not match.",
               K(header), K(payload_pos), K(ret));
  } else if (NULL == (payload_ptr = ptr + payload_pos)
             || (payload_size = (size - payload_pos)) <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "payload buffer size over flow",
               K(header), K(payload_pos), K(payload_size), K(ret));
  } else if (OB_UNLIKELY(header.magic_ != magic)) {
    ret = OB_INVALID_DATA;
    COMMON_LOG(WARN, "record header magic is not match",
               K(header), K(magic), K(ret));
  } else if (OB_UNLIKELY(header.data_zlength_ != payload_size)) {
    ret = OB_INVALID_DATA;
    COMMON_LOG(WARN, "record header length is not match",
               K(header), K(payload_size), K(ret));
  }  else if (OB_FAIL(header.check_header_checksum())) {
    COMMON_LOG(WARN, "check header checksum failed.",
               K(header), K(payload_size), K(ret));
  } else if (OB_FAIL(header.check_payload_checksum(payload_ptr, payload_size))) {
    COMMON_LOG(WARN, "check payload checksum failed.",
               K(header), K(payload_size), K(ret));
  }

  return ret;
}

int ObRecordHeader::get_record_header(const char *ptr,
                                      const int64_t size,
                                      ObRecordHeader &header,
                                      const char *&payload_ptr,
                                      int64_t &payload_size)
{
  int ret = OB_SUCCESS;
  int64_t payload_pos = 0;

  if (NULL == ptr || size < OB_RECORD_HEADER_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments.", KP(ptr), K(size), K(ret));
  } else if (OB_FAIL(header.deserialize(ptr, size, payload_pos))) {
    COMMON_LOG(WARN, "deserialize header failed ",
               KP(ptr), K(size), K(payload_pos), K(ret));
  } else {
    payload_ptr = ptr + payload_pos;
    payload_size = size - payload_pos;
    if (header.header_length_ != payload_pos) {
      ret = OB_INVALID_DATA;
      COMMON_LOG(WARN, "record header length is not match",
                 K(header), K(payload_size), K(ret));
    }
  }

  return ret;
}

DEFINE_SERIALIZE(ObRecordHeader)
{
  int ret = OB_SUCCESS;

  if ((NULL == buf) || (buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments.", KP(buf), K(buf_len), K(ret));
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, pos, magic_))) {
    COMMON_LOG(WARN, "encode data failed..", KP(buf), K_(magic), K(ret));
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, pos, header_length_))) {
    COMMON_LOG(WARN, "encode data failed..", KP(buf), K_(header_length), K(ret));
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, pos, version_))) {
    COMMON_LOG(WARN, "encode data failed..", KP(buf), K_(version), K(ret));
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, pos, header_checksum_))) {
    COMMON_LOG(WARN, "encode data failed..", KP(buf), K_(header_checksum), K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, timestamp_))) {
    COMMON_LOG(WARN, "encode data failed..", KP(buf), K_(timestamp), K(ret));
  } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, data_length_))) {
    COMMON_LOG(WARN, "encode data failed..", KP(buf), K_(data_length), K(ret));
  } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, data_zlength_))) {
    COMMON_LOG(WARN, "encode data failed..", KP(buf), K_(data_zlength), K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, data_checksum_))) {
    COMMON_LOG(WARN, "encode data failed..", KP(buf), K_(data_checksum), K(ret));
  }

  return ret;
}

DEFINE_DESERIALIZE(ObRecordHeader)
{
  int ret = OB_SUCCESS;

  if ((NULL == buf) || (data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments.", KP(buf), K(data_len), K(ret));
  } else if (OB_FAIL(serialization::decode_i16(buf, data_len, pos, &magic_))) {
    COMMON_LOG(WARN, "encode data failed..", KP(buf), K_(magic), K(ret));
  } else if (OB_FAIL(serialization::decode_i16(buf, data_len, pos, &header_length_))) {
    COMMON_LOG(WARN, "encode data failed..", KP(buf), K_(header_length), K(ret));
  }  else if (OB_FAIL(serialization::decode_i16(buf, data_len, pos, &version_))) {
    COMMON_LOG(WARN, "encode data failed..", KP(buf), K_(version), K(ret));
  } else if (OB_FAIL(serialization::decode_i16(buf, data_len, pos, &header_checksum_))) {
    COMMON_LOG(WARN, "encode data failed..", KP(buf), K_(header_checksum), K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &timestamp_))) {
    COMMON_LOG(WARN, "encode data failed..", KP(buf), K_(timestamp), K(ret));
  } else if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &data_length_))) {
    COMMON_LOG(WARN, "encode data failed..", KP(buf), K_(data_length), K(ret));
  } else if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &data_zlength_))) {
    COMMON_LOG(WARN, "encode data failed..", KP(buf), K_(data_zlength), K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &data_checksum_))) {
    COMMON_LOG(WARN, "encode data failed..", KP(buf), K_(data_checksum), K(ret));
  }
  // 后续新增成员时，应该使用header_length_作为反序列化长度的限制，以保证升级兼容

  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObRecordHeader)
{
  return (serialization::encoded_length_i16(magic_)
          + serialization::encoded_length_i16(header_length_)
          + serialization::encoded_length_i16(version_)
          + serialization::encoded_length_i16(header_checksum_)
          + serialization::encoded_length_i64(timestamp_)
          + serialization::encoded_length_i32(data_length_)
          + serialization::encoded_length_i32(data_zlength_)
          + serialization::encoded_length_i64(data_checksum_));
}

} // end namespace common
} // end namespace oceanbase
