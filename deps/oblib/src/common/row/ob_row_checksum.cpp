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

#define USING_LOG_PREFIX COMMON

#include "common/row/ob_row_checksum.h"

#include "lib/utility/ob_print_utils.h"
#include "common/row/ob_row.h"

namespace oceanbase
{
namespace common
{

DEFINE_GET_SERIALIZE_SIZE(ObRowChecksumValue)
{
  int64_t size = 0;
  size += serialization::encoded_length_i64(checksum_);
  size += serialization::encoded_length_vi64(column_count_);
  size += sizeof(column_checksum_array_[0]) * column_count_;
  return size;
}

DEFINE_SERIALIZE(ObRowChecksumValue)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, checksum_))) {
    LOG_WARN("encode int failed", K(ret));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, column_count_))) {
    LOG_WARN("encode int failed", K(ret));
  }
  if (OB_SUCC(ret) && column_count_ > 0) {
    const int64_t n = sizeof(column_checksum_array_[0]) * column_count_;
    if (buf_len - pos < n) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("serialize buf not enough", K(ret), "remain", buf_len - pos, "needed", n);
    } else {
      MEMCPY(buf + pos, column_checksum_array_, n);
      pos += n;
    }
  }

  return ret;
}

DEFINE_DESERIALIZE(ObRowChecksumValue)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::decode_i64(buf, data_len, pos,
      reinterpret_cast<int64_t *>(&checksum_)))) {
    LOG_WARN("decode int failed", K(ret));
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &column_count_))) {
    LOG_WARN("decode int failed", K(ret));
  }
  if (OB_SUCC(ret) && column_count_ > 0) {
    const int64_t n = sizeof(column_checksum_array_[0]) * column_count_;
    if (data_len - pos < n) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("serialize buf not enough", K(ret), "remain", data_len - pos, "needed", n);
    } else {
      column_checksum_array_ = reinterpret_cast<ObColumnIdChecksum *>(
          const_cast<char *>(buf + pos));
      pos += n;
    }
  }

  return ret;
}

int ObRowChecksumValue::column_checksum2string(char *buf, const int64_t buf_len,
                                               int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len));
  } else if (column_count_ == 0) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s", ""))) {
      LOG_WARN("print to buffer failed", K(ret), K(buf_len), K(pos));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_count_; i++) {
      if (i > 0) {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, ","))) {
          LOG_WARN("print to buffer failed", K(ret), K(buf_len), K(pos));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%lu:%lu",
            column_checksum_array_[i].first, column_checksum_array_[i].second))) {
          LOG_WARN("print to buffer failed", K(ret), K(buf_len), K(pos));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("column checksum to string failed", K(ret), K_(column_count));
  }
  return ret;
}

void ObRowChecksumValue::reset()
{
  checksum_ = 0;
  column_count_ = 0;
  column_checksum_array_ = NULL;
}

void ObRowChecksumValue::reset_checksum()
{
  checksum_ = 0;
  for (int64_t i = 0; i < column_count_; i++) {
    column_checksum_array_[i].second = 0;
  }
}

void ObRowChecksumValue::sort()
{
  bool sorted = true;
  int64_t prev = 0;
  if (NULL != column_checksum_array_) {
    for (int64_t i = 1; i < column_count_ && sorted; i++, prev++) {
      if (column_checksum_array_[prev].first > column_checksum_array_[i].first) {
        sorted = false;
      }
    }
    if (!sorted) {
      std::sort(column_checksum_array_, column_checksum_array_ + column_count_);
    }
  }
}

int64_t ObRowChecksumValue::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    // do not print log in to_string function.
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "row:[%lu]", checksum_))) {
  } else {
    if (column_count_ > 0) {
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, ",columns:["))) {
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < column_count_; i++) {
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%lu:%lu,",
          column_checksum_array_[i].first, column_checksum_array_[i].second))) {
      }
    }
    if (OB_SUCC(ret) && column_count_ > 0) {
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, "]"))) {
      }
    }
  }

  return pos;
}

} // end namespace common
} // end namespace oceanbase

