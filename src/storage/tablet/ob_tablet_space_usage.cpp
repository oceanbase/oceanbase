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

#include "storage/tablet/ob_tablet_space_usage.h"

namespace oceanbase
{
namespace storage
{
int ObTabletSpaceUsage::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  int32_t length = get_serialize_size();
  int64_t new_pos = pos;

  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0) || OB_UNLIKELY(pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_UNLIKELY(length > buf_len - pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buffer is not enough", K(ret), K(length), K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, new_pos, TABLET_SPACE_USAGE_INFO_VERSION))) {
    LOG_WARN("fail to serialize version", K(ret), K(buf_len), K(new_pos));
  } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, new_pos, length))) {
    LOG_WARN("fail to serialize length", K(ret), K(buf_len), K(new_pos), K(length));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, shared_data_size_))) {
    LOG_WARN("fail to serialize shared_data_size_", K(ret), K(buf_len), K(new_pos), K(length), K(shared_data_size_));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, data_size_))) {
    LOG_WARN("fail to serialize data_size_", K(ret), K(buf_len), K(new_pos), K(length), K(data_size_));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, shared_meta_size_))) {
    LOG_WARN("fail to serialize shared_meta_size_", K(ret), K(buf_len), K(new_pos), K(length), K(shared_meta_size_));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, meta_size_))) {
    LOG_WARN("fail to serialize meta_size_", K(ret), K(buf_len), K(new_pos), K(length), K(meta_size_));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, occupy_bytes_))) {
    LOG_WARN("fail to serialize occupy_size_", K(ret), K(buf_len), K(new_pos), K(length), K(occupy_bytes_));
  } else if (OB_UNLIKELY(length != new_pos - pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("length doesn't match", K(ret), K(length), K(new_pos), K(pos));
  } else {
    pos = new_pos;
  }
  return ret;
}

int ObTabletSpaceUsage::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  int32_t length = 0;
  int32_t version = -1;

  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0) || OB_UNLIKELY(pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode_i32(buf, data_len, new_pos, &version))) {
    LOG_WARN("fail to deserialize version", K(ret), K(data_len), K(new_pos));
  } else if (OB_UNLIKELY(TABLET_SPACE_USAGE_INFO_VERSION != version)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("version doesn't match", K(ret), K(version));
  } else if (OB_UNLIKELY(serialization::decode_i32(buf, data_len, new_pos, &length))) {
    LOG_WARN("fail to deserialize version", K(ret), K(data_len), K(new_pos), K(length));
  } else if (OB_UNLIKELY(length > data_len - pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buffer is not enough", K(ret), K(data_len), K(pos), K(length));
  } else if (new_pos - pos < length && OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &shared_data_size_))) {
    LOG_WARN("fail to deserialize shared_data_size_", K(ret), K(data_len), K(new_pos), K(length));
  } else if (new_pos - pos < length && OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &data_size_))) {
    LOG_WARN("fail to deserialize data_size_", K(ret), K(data_len), K(new_pos), K(length));
  } else if (new_pos - pos < length && OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &shared_meta_size_))) {
    LOG_WARN("fail to deserialize shared_meta_size_", K(ret), K(data_len), K(new_pos), K(length));
  } else if (new_pos - pos < length && OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &meta_size_))) {
    LOG_WARN("fail to deserialize shared_meta_size_", K(ret), K(data_len), K(new_pos), K(length));
  } else if (new_pos - pos < length && OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &occupy_bytes_))) {
    LOG_WARN("fail to serialize occupy_bytes_", K(ret), K(data_len), K(new_pos), K(length));
  } else if (OB_UNLIKELY(length != new_pos - pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("length doesn't match", K(ret), K(length), K(new_pos), K(pos));
  } else {
    pos = new_pos;
  }
  return ret;
}

int32_t ObTabletSpaceUsage::get_serialize_size() const
{
  int32_t len = 0;
  len += serialization::encoded_length_i32(TABLET_SPACE_USAGE_INFO_VERSION);
  len += serialization::encoded_length_i32(len);
  len += serialization::encoded_length_i64(shared_data_size_);
  len += serialization::encoded_length_i64(data_size_);
  len += serialization::encoded_length_i64(shared_meta_size_);
  len += serialization::encoded_length_i64(meta_size_);
  len += serialization::encoded_length_i64(occupy_bytes_);
  return len;
}
}
}