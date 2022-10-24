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

#include "common/log/ob_log_entry.h"
#include "lib/checksum/ob_crc64.h"
#include "lib/utility/utility.h"

using namespace oceanbase::common;

const char *oceanbase::common::DEFAULT_CKPT_EXTENSION = "checkpoint";

int ObLogEntry::set_log_seq(const uint64_t seq)
{
  int ret = OB_SUCCESS;
  seq_ = seq;
  return ret;
}

int ObLogEntry::set_log_command(const int32_t cmd)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cmd < 0)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(cmd), K(ret));
  } else {
    cmd_ = cmd;
  }
  return ret;
}

int ObLogEntry::fill_header(const char *log_data, const int64_t data_len, const int64_t timestamp)
{
  int ret = OB_SUCCESS;

  if ((NULL == log_data && data_len != 0)
      || (NULL != log_data && data_len <= 0)
      || timestamp < 0) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid arguments", KP(log_data), K(data_len), K(timestamp), K(ret));
  } else {
    header_.set_magic_num(MAGIC_NUMER);
    header_.header_length_ = OB_RECORD_HEADER_LENGTH;
    header_.version_ = LOG_VERSION;
    header_.timestamp_ = timestamp;
    header_.data_length_ = static_cast<int32_t>(sizeof(uint64_t) + sizeof(LogCommand) + data_len);
    header_.data_zlength_ = header_.data_length_;
    if (NULL != log_data) {
      header_.data_checksum_ = calc_data_checksum(log_data, data_len);
    } else {
      header_.data_checksum_ = 0;
    }
    header_.set_header_checksum();
  }

  return ret;
}

int64_t ObLogEntry::calc_data_checksum(const char *log_data, const int64_t data_len) const
{
  uint64_t data_checksum = 0;

  if (data_len > 0) {
    data_checksum = ob_crc64(reinterpret_cast<const void *>(&seq_), sizeof(seq_));
    data_checksum = ob_crc64(data_checksum, reinterpret_cast<const void *>(&cmd_), sizeof(cmd_));
    data_checksum = ob_crc64(data_checksum, reinterpret_cast<const void *>(log_data), data_len);
  }

  return static_cast<int64_t>(data_checksum);
}

int ObLogEntry::check_header_integrity(bool dump_content) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(header_.check_header_checksum())) {
    SHARE_LOG(WARN, "check_header_checksum failed", K(ret));
  } else if (MAGIC_NUMER != header_.magic_) {
    ret = OB_CHECKSUM_ERROR;
    SHARE_LOG(WARN, "magic not match", K(ret), LITERAL_K(MAGIC_NUMER), K(header_.magic_));
  }
  if (OB_FAIL(ret) && dump_content) {
    SHARE_LOG(WARN, "check_header_integrity error: ");
    hex_dump(&header_, sizeof(header_), true, OB_LOG_LEVEL_WARN);
  }
  return ret;
}

int ObLogEntry::check_data_integrity(const char *log_data, bool dump_content) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(log_data == NULL)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", KP(log_data), K(dump_content), K(ret));
  } else {
    int64_t crc_check_sum = calc_data_checksum(log_data,
                                               header_.data_length_ - sizeof(uint64_t) - sizeof(LogCommand));
    if (OB_LIKELY(crc_check_sum == header_.data_checksum_)) {
      ret = OB_SUCCESS;
    } else {
      if (dump_content) {
        SHARE_LOG(WARN, "Header: ");
        hex_dump(&header_, sizeof(header_), true, OB_LOG_LEVEL_ERROR);
        SHARE_LOG(WARN, "Body: ");
        hex_dump(log_data - sizeof(uint64_t) - sizeof(LogCommand), header_.data_length_, true,
                 OB_LOG_LEVEL_ERROR);
      }
      ret = OB_CHECKSUM_ERROR;
    }
  }

  return ret;
}

DEFINE_SERIALIZE(ObLogEntry)
{
  int ret = OB_SUCCESS;
  int64_t serialize_size = get_serialize_size();
  if (OB_UNLIKELY(NULL == buf) || OB_UNLIKELY((pos + serialize_size) > buf_len)) {
    ret = OB_ERROR;
    SHARE_LOG(WARN, "invalid arguments", KP(buf), K(pos), K(buf_len), K(ret));
  } else {
    int64_t new_pos = pos;
    if ((OB_SUCCESS == header_.serialize(buf, buf_len, new_pos))
        && (OB_SUCCESS == serialization::encode_i64(buf, buf_len, new_pos, seq_))
        && (OB_SUCCESS == serialization::encode_i32(buf, buf_len, new_pos, cmd_))) {
      pos = new_pos;
    } else {
      ret = OB_ERROR;
    }
  }

  return ret;
}

DEFINE_DESERIALIZE(ObLogEntry)
{
  int ret = OB_SUCCESS;

  int64_t serialize_size = get_serialize_size();
  if (OB_UNLIKELY(NULL == buf) || OB_UNLIKELY((serialize_size > data_len - pos))) {
    ret = OB_ERROR;
    SHARE_LOG(WARN, "invalid arguments", KP(buf), K(pos), K(data_len), K(ret));
  } else {
    int64_t new_pos = pos;
    if ((OB_SUCCESS == header_.deserialize(buf, data_len, new_pos))
        && (OB_SUCCESS == serialization::decode_i64(buf, data_len, new_pos, (int64_t *)&seq_))
        && (OB_SUCCESS == serialization::decode_i32(buf, data_len, new_pos, (int32_t *)&cmd_))) {
      pos = new_pos;
    } else {
      ret = OB_ERROR;
    }
  }

  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObLogEntry)
{
  return header_.get_serialize_size()
         + serialization::encoded_length_i64(seq_)
         + serialization::encoded_length_i32(cmd_);
}

