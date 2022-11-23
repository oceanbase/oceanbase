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

#define USING_LOG_PREFIX STORAGE_REDO
#include "ob_storage_log_batch_header.h"

namespace oceanbase
{
namespace storage
{

ObStorageLogBatchHeader::ObStorageLogBatchHeader()
  : magic_(MAGIC_NUMBER), version_(HEADER_VERSION),
    header_len_(sizeof(ObStorageLogBatchHeader)),
    cnt_(0), rez_(0), total_len_(0), checksum_(0)
{
}

ObStorageLogBatchHeader::~ObStorageLogBatchHeader()
{
}

int ObStorageLogBatchHeader::check_batch_header()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(MAGIC_NUMBER != magic_)) {
    ret = OB_CHECKSUM_ERROR;
    STORAGE_REDO_LOG(WARN, "Magic number doesn't match", K(ret), K_(magic));
  }
  return ret;
}

int ObStorageLogBatchHeader::check_data(const char *data)
{
  int ret = OB_SUCCESS;

  uint64_t actual_checksum = 0;
  if (OB_UNLIKELY(nullptr == data)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_REDO_LOG(WARN, "Invalid arguments, ", K(ret), K(data));
  } else {
    actual_checksum = cal_checksum(data, total_len_);
    if (actual_checksum != checksum_) {
      ret = OB_CHECKSUM_ERROR;
      STORAGE_REDO_LOG(WARN, "checksum error, ", K(ret), K_(checksum),
          K(actual_checksum), KP(data), K(total_len_));
    }
  }

  return ret;
}

uint64_t ObStorageLogBatchHeader::cal_checksum(const char *log_data, const int32_t data_len)
{
  uint64_t checksum = 0;

  if(data_len > 0 && nullptr != log_data) {
    checksum = ob_crc64(reinterpret_cast<const void *>(log_data), data_len);
  }

  return checksum;
}

DEFINE_SERIALIZE(ObStorageLogBatchHeader)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(nullptr == buf) || OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_REDO_LOG(WARN, "Invalid arguments, ", K(ret), K(buf_len));
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, pos, magic_))) {
    STORAGE_REDO_LOG(WARN, "Fail to encode magic_, ", K(ret), K_(magic));
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, pos, version_))) {
    STORAGE_REDO_LOG(WARN, "Fail to encode version_, ", K(ret), K_(version));
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, pos, header_len_))) {
    STORAGE_REDO_LOG(WARN, "Fail to encode header_len_, ", K(ret), K_(header_len));
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, pos, cnt_))) {
    STORAGE_REDO_LOG(WARN, "Fail to encode cnt_, ", K(ret), K_(cnt));
  } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, rez_))) {
    STORAGE_REDO_LOG(WARN, "Fail to encode rez_, ", K(ret), K_(rez));
  } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, total_len_))) {
    STORAGE_REDO_LOG(WARN, "Fail to encode total_len_, ", K(ret), K_(total_len));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, checksum_))) {
    STORAGE_REDO_LOG(WARN, "Fail to encode checksum_, ", K(ret), K_(checksum));
  }

  return ret;
}

DEFINE_DESERIALIZE(ObStorageLogBatchHeader)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(nullptr == buf) || OB_UNLIKELY(data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_REDO_LOG(WARN, "Invalid arguments, ", K(ret), K(data_len));
  } else if (OB_FAIL(serialization::decode_i16(buf, data_len, pos, (int16_t *)&magic_))) {
    STORAGE_REDO_LOG(WARN, "Fail to decode magic_, ", K(ret), K_(magic));
  } else if (OB_FAIL(serialization::decode_i16(buf, data_len, pos, (int16_t *)&version_))) {
    STORAGE_REDO_LOG(WARN, "Fail to decode version_, ", K(ret), K_(version));
  } else if (OB_FAIL(serialization::decode_i16(buf, data_len, pos, (int16_t *)&header_len_))) {
    STORAGE_REDO_LOG(WARN, "Fail to decode header_len_, ", K(ret), K_(header_len));
  } else if (OB_FAIL(serialization::decode_i16(buf, data_len, pos, (int16_t *)&cnt_))) {
    STORAGE_REDO_LOG(WARN, "Fail to decode cnt_, ", K(ret), K_(cnt));
  } else if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, (int32_t *)&rez_))) {
    STORAGE_REDO_LOG(WARN, "Fail to decode rez_, ", K(ret), K_(rez));
  } else if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, (int32_t *)&total_len_))) {
    STORAGE_REDO_LOG(WARN, "Fail to decode total_len_, ", K(ret), K_(total_len));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, (int64_t *)&checksum_))) {
    STORAGE_REDO_LOG(WARN, "Fail to decode checksum_, ", K(ret), K_(checksum));
  }

  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObStorageLogBatchHeader)
{
  return  serialization::encoded_length_i16(header_len_)
          + serialization::encoded_length_i16(version_)
          + serialization::encoded_length_i16(magic_)
					+ serialization::encoded_length_i16(cnt_)
					+ serialization::encoded_length_i32(total_len_)
					+ serialization::encoded_length_i64(checksum_)
          + serialization::encoded_length_i32(rez_);
}

}
}