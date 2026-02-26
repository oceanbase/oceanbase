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

#include "common/ob_file_common_header.h"
#include "common/ob_record_header.h" // format_i32 and format_i64

namespace oceanbase
{
namespace common
{

ObFileCommonHeader::ObFileCommonHeader()
  : magic_(OB_FILE_COMMON_HEADER_MAGIC), header_version_(OB_FILE_COMMON_HEADER_VERSION),
    header_checksum_(0), payload_version_(0), payload_length_(0), payload_zlength_(0),
    payload_checksum_(0)
{
  header_length_ = get_serialize_size();
}

ObFileCommonHeader::~ObFileCommonHeader()
{
}

void ObFileCommonHeader::set_header_checksum()
{
  header_checksum_ = 0;
  int16_t checksum = calc_header_checksum();
  header_checksum_ = checksum;
}

int ObFileCommonHeader::check_header_checksum() const
{
  int ret = OB_SUCCESS;
  int16_t checksum = calc_header_checksum();
  if (OB_UNLIKELY(0 != checksum)) {
    ret = OB_CHECKSUM_ERROR;
    COMMON_LOG(WARN, "check header checksum failed", K(ret), K(checksum), KPC(this));
  }

  return ret;
}

int16_t ObFileCommonHeader::calc_header_checksum() const
{
  int16_t checksum = 0;
  checksum = checksum ^ magic_;
  checksum = checksum ^ header_version_;
  checksum = checksum ^ header_length_;
  checksum = checksum ^ header_checksum_;
  checksum = checksum ^ payload_version_;
  format_i32(payload_length_, checksum);
  format_i32(payload_zlength_, checksum);
  format_i64(payload_checksum_, checksum);
  return checksum;
}

int ObFileCommonHeader::check_payload_checksum(const char *buf, const int64_t len) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments", K(ret), KP(buf), K(len));
  } else if ((payload_zlength_ != len)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "payload length is incorrect", K(ret), K_(payload_zlength), K(len));
  } else {
    int64_t payload_checksum = ob_crc64(buf, len);
    if (OB_UNLIKELY(payload_checksum != payload_checksum_)) {
      ret = OB_CHECKSUM_ERROR;
      COMMON_LOG(WARN, "payload checksum error", K(ret), K(payload_checksum), K_(payload_checksum));
    }
  }
  return ret;
}

DEFINE_SERIALIZE(ObFileCommonHeader)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY((buf_len <= 0) || (pos < 0))) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, pos, magic_))) {
    COMMON_LOG(WARN, "encode magic failed", K(ret), KP(buf), K(buf_len), K(pos), K_(magic));
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, pos, header_version_))) {
    COMMON_LOG(WARN, "encode header version failed", K(ret), KP(buf), K(buf_len), K(pos), K_(header_version));
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, pos, header_length_))) {
    COMMON_LOG(WARN, "encode header length failed", K(ret), KP(buf), K(buf_len), K(pos), K_(header_length));
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, pos, header_checksum_))) {
    COMMON_LOG(WARN, "encode header checksum failed", K(ret), KP(buf), K(buf_len), K(pos), K_(header_checksum));
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, pos, payload_version_))) {
    COMMON_LOG(WARN, "encode payload version failed", K(ret), KP(buf), K(buf_len), K(pos), K_(payload_version));
  } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, payload_length_))) {
    COMMON_LOG(WARN, "encode payload length failed", K(ret), KP(buf), K(buf_len), K(pos), K_(payload_length));
  } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, payload_zlength_))) {
    COMMON_LOG(WARN, "encode payload zlength failed", K(ret), KP(buf), K(buf_len), K(pos), K_(payload_zlength));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, payload_checksum_))) {
    COMMON_LOG(WARN, "encode payload checksum failed", K(ret), KP(buf), K(buf_len), K(pos), K_(payload_checksum));
  }
  return ret;
}

DEFINE_DESERIALIZE(ObFileCommonHeader)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY((data_len <= 0) || (pos < 0))) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments", K(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode_i16(buf, data_len, pos, &magic_))) {
    COMMON_LOG(WARN, "decode magic failed", K(ret), KP(buf), K(data_len), K(pos), K_(magic));
  } else if (OB_FAIL(serialization::decode_i16(buf, data_len, pos, &header_version_))) {
    COMMON_LOG(WARN, "decode header version failed", K(ret), KP(buf), K(data_len), K(pos), K_(header_version));
  }  else if (OB_FAIL(serialization::decode_i16(buf, data_len, pos, &header_length_))) {
    COMMON_LOG(WARN, "decode header length failed", K(ret), KP(buf), K(data_len), K(pos), K_(header_length));
  } else if (OB_FAIL(serialization::decode_i16(buf, data_len, pos, &header_checksum_))) {
    COMMON_LOG(WARN, "decode header checksum failed", K(ret), KP(buf), K(data_len), K(pos), K_(header_checksum));
  } else if (OB_FAIL(serialization::decode_i16(buf, data_len, pos, &payload_version_))) {
    COMMON_LOG(WARN, "decode payload checksum failed", K(ret), KP(buf), K(data_len), K(pos), K_(payload_version));
  } else if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &payload_length_))) {
    COMMON_LOG(WARN, "decode payload length failed", K(ret), KP(buf), K(data_len), K(pos), K_(payload_length));
  } else if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &payload_zlength_))) {
    COMMON_LOG(WARN, "decode payload zlength failed", K(ret), KP(buf), K(data_len), K(pos), K_(payload_zlength));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &payload_checksum_))) {
    COMMON_LOG(WARN, "decode payload checksum failed", K(ret), KP(buf), K(data_len), K(pos), K_(payload_checksum));
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObFileCommonHeader)
{
  return (serialization::encoded_length_i16(magic_)
          + serialization::encoded_length_i16(header_version_)
          + serialization::encoded_length_i16(header_length_)
          + serialization::encoded_length_i16(header_checksum_)
          + serialization::encoded_length_i16(payload_version_)
          + serialization::encoded_length_i32(payload_length_)
          + serialization::encoded_length_i32(payload_zlength_)
          + serialization::encoded_length_i64(payload_checksum_));
}

} // end namespace common
} // end namespace oceanbase
