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

#include "ob_storage_log_entry.h"
#include "lib/checksum/ob_crc64.h"
#include "lib/utility/utility.h"
#include "lib/time/ob_time_utility.h"

namespace oceanbase
{
using namespace common;

namespace storage
{

ObStorageLogEntry::ObStorageLogEntry()
  : magic_(MAGIC_NUMBER), version_(ENTRY_VERSION),
    entry_len_(sizeof(ObStorageLogEntry)), rez_(0),
    cmd_(0), data_len_(0), seq_(0), timestamp_(0),
    data_checksum_(0), entry_checksum_(0)
{
}

ObStorageLogEntry::~ObStorageLogEntry()
{
}

void ObStorageLogEntry::reset()
{
  magic_ = MAGIC_NUMBER;
  version_ = ENTRY_VERSION;
  entry_len_ = sizeof(ObStorageLogEntry);
  rez_ = 0;
  cmd_ = 0;
  data_len_ = 0;
  seq_ = 0;
  timestamp_ = 0;
  data_checksum_ = 0;
  entry_checksum_ = 0;
}

int ObStorageLogEntry::fill_entry(
    const char *log_data,
    const int64_t data_len,
    const int32_t cmd,
    const uint64_t seq)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY((nullptr == log_data) || (data_len == 0))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_REDO_LOG(WARN, "invalid arguments", K(log_data), K(data_len), K(seq), K(ret));
  } else {
    cmd_ = cmd;
    data_len_ = data_len;
    seq_ = seq;
    timestamp_ = ObTimeUtility::fast_current_time();
    data_checksum_ = calc_data_checksum(log_data, data_len);
    entry_checksum_ = calc_entry_checksum();
  }

  return ret;
}

uint64_t ObStorageLogEntry::calc_data_checksum(const char *log_data, const int64_t data_len) const
{
  uint64_t data_checksum = 0;

  if (data_len > 0 && nullptr != log_data) {
    data_checksum = ob_crc64(reinterpret_cast<const void *>(log_data), data_len);
  }

  return data_checksum;
}

uint64_t ObStorageLogEntry::calc_entry_checksum() const
{
  uint64_t checksum = 0;

  checksum = ob_crc64(reinterpret_cast<const void *>(&magic_), sizeof(magic_));
  checksum = ob_crc64(checksum, reinterpret_cast<const void *>(&version_), sizeof(version_));
  checksum = ob_crc64(checksum, reinterpret_cast<const void *>(&entry_len_), sizeof(entry_len_));
  checksum = ob_crc64(checksum, reinterpret_cast<const void *>(&rez_), sizeof(rez_));
  checksum = ob_crc64(checksum, reinterpret_cast<const void *>(&cmd_), sizeof(cmd_));
  checksum = ob_crc64(checksum, reinterpret_cast<const void *>(&data_len_), sizeof(data_len_));
  checksum = ob_crc64(checksum, reinterpret_cast<const void *>(&seq_), sizeof(seq_));
  checksum = ob_crc64(checksum, reinterpret_cast<const void *>(&timestamp_), sizeof(timestamp_));
  checksum = ob_crc64(checksum, reinterpret_cast<const void *>(&data_checksum_), sizeof(data_checksum_));

  return checksum;
}

int ObStorageLogEntry::check_entry_integrity(bool dump_content) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(MAGIC_NUMBER != magic_)) {
    ret = OB_CHECKSUM_ERROR;
    STORAGE_REDO_LOG(WARN, "Magic doesn't match", K(ret), K_(magic));
  } else if (entry_checksum_ != calc_entry_checksum()) {
    ret = OB_CHECKSUM_ERROR;
    STORAGE_REDO_LOG(WARN, "Checksum error", K(ret), K_(entry_checksum));
  }
  if (OB_FAIL(ret) && dump_content) {
    STORAGE_REDO_LOG(WARN, "check_entry_integrity error: ", K(*this));
  }

  return ret;
}

int ObStorageLogEntry::check_data_integrity(const char *log_data, bool dump_content) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(nullptr == log_data)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_REDO_LOG(WARN, "invalid argument", K(log_data), K(dump_content), K(ret));
  } else {
    int64_t crc_check_sum = calc_data_checksum(log_data, data_len_);
    if (OB_UNLIKELY(crc_check_sum != data_checksum_)) {
      if (dump_content) {
        STORAGE_REDO_LOG(WARN, "Entry: ", K(*this), K(crc_check_sum));
        STORAGE_REDO_LOG(WARN, "Body: ");
        hex_dump(log_data, data_len_, true, OB_LOG_LEVEL_ERROR);
      }
      ret = OB_CHECKSUM_ERROR;
    }
  }

  return ret;
}

DEFINE_SERIALIZE(ObStorageLogEntry)
{
  int ret = OB_SUCCESS;

  int64_t serialize_size = get_serialize_size();
  if (OB_UNLIKELY(nullptr == buf) || OB_UNLIKELY((pos + serialize_size) > buf_len)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_REDO_LOG(WARN, "invalid arguments", K(buf), K(pos), K(buf_len), K(ret));
  }else {
    int64_t new_pos = pos;
    if (OB_FAIL(serialization::encode_i16(buf, buf_len, new_pos, magic_))) {
      STORAGE_REDO_LOG(WARN, "fail to serialize magic_", K_(magic), K(ret));
    } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, new_pos, version_))) {
      STORAGE_REDO_LOG(WARN, "fail to serialize version_", K_(version), K(ret));
    } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, new_pos, entry_len_))) {
      STORAGE_REDO_LOG(WARN, "fail to serialize entry_len_", K_(entry_len), K(ret));
    } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, new_pos, rez_))) {
      STORAGE_REDO_LOG(WARN, "fail to serialize rez_", K_(rez), K(ret));
    } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, new_pos, cmd_))) {
      STORAGE_REDO_LOG(WARN, "fail to serialize cmd_", K_(cmd), K(ret));
    } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, new_pos, data_len_))) {
      STORAGE_REDO_LOG(WARN, "fail to serialize data_len_", K_(data_len), K(ret));
    } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, seq_))) {
      STORAGE_REDO_LOG(WARN, "fail to serialize seq_", K_(seq), K(ret));
    } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, timestamp_))) {
      STORAGE_REDO_LOG(WARN, "fail to serialize timestamp_", K_(timestamp), K(ret));
    } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, data_checksum_))) {
      STORAGE_REDO_LOG(WARN, "fail to serialize data_checksum_", K_(data_checksum), K(ret));
    } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, entry_checksum_))) {
      STORAGE_REDO_LOG(WARN, "fail to serialize entry_checksum_", K_(entry_checksum), K(ret));
    } else {
      pos = new_pos;
    }
  }

  return ret;
}

DEFINE_DESERIALIZE(ObStorageLogEntry)
{
  int ret = OB_SUCCESS;

  int64_t serialize_size = get_serialize_size();
  if (OB_UNLIKELY(nullptr == buf) || OB_UNLIKELY(serialize_size > data_len - pos)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_REDO_LOG(WARN, "invalid arguments", K(buf), K(pos), K(data_len), K(ret));
  } else {
    int64_t new_pos = pos;
    if (OB_FAIL(serialization::decode_i16(buf, data_len, new_pos, (int16_t *)&magic_))) {
      STORAGE_REDO_LOG(WARN, "fail to decode magic_", K_(magic), K(ret));
    } else if (OB_FAIL(serialization::decode_i16(buf, data_len, new_pos, (int16_t *)&version_))) {
      STORAGE_REDO_LOG(WARN, "fail to decode version_", K_(version), K(ret));
    } else if (OB_FAIL(serialization::decode_i16(buf, data_len, new_pos, (int16_t *)&entry_len_))) {
      STORAGE_REDO_LOG(WARN, "fail to decode entry_len_", K_(entry_len), K(ret));
    } else if (OB_FAIL(serialization::decode_i16(buf, data_len, new_pos, (int16_t *)&rez_))) {
      STORAGE_REDO_LOG(WARN, "fail to deserialize rez_", K_(rez), K(ret));
    } else if (OB_FAIL(serialization::decode_i32(buf, data_len, new_pos, (int32_t *)&cmd_))) {
      STORAGE_REDO_LOG(WARN, "fail to decode cmd_", K_(cmd), K(ret));
    } else if (OB_FAIL(serialization::decode_i32(buf, data_len, new_pos, (int32_t *)&data_len_))) {
      STORAGE_REDO_LOG(WARN, "fail to deserialize data_len_", K_(data_len), K(ret));
    } else if (OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, (int64_t *)&seq_))) {
      STORAGE_REDO_LOG(WARN, "fail to deserialize seq_", K_(seq), K(ret));
    } else if (OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, (int64_t *)&timestamp_))) {
      STORAGE_REDO_LOG(WARN, "fail to deserialize timestamp_", K_(timestamp), K(ret));
    } else if (OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, (int64_t *)&data_checksum_))) {
      STORAGE_REDO_LOG(WARN, "fail to deserialize data_checksum_", K_(data_checksum), K(ret));
    } else if (OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, (int64_t *)&entry_checksum_))) {
      STORAGE_REDO_LOG(WARN, "fail to deserialize entry_checksum_", K_(entry_checksum), K(ret));
    } else {
      pos = new_pos;
    }
  }

  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObStorageLogEntry)
{
  return serialization::encoded_length_i16(magic_)
         + serialization::encoded_length_i16(version_)
         + serialization::encoded_length_i16(entry_len_)
         + serialization::encoded_length_i16(rez_)
         + serialization::encoded_length_i32(cmd_)
         + serialization::encoded_length_i32(data_len_)
         + serialization::encoded_length_i64(seq_)
         + serialization::encoded_length_i64(timestamp_)
         + serialization::encoded_length_i64(data_checksum_)
         + serialization::encoded_length_i64(entry_checksum_);
}

}
}


