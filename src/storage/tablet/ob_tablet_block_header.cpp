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

#include "storage/tablet/ob_tablet_block_header.h"
#include "lib/checksum/ob_crc64.h"

namespace oceanbase
{
namespace storage
{

int ObTabletBlockHeader::init(const int32_t inline_meta_count)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTabletBlockHeader has inited", K(ret));
  } else if (inline_meta_count > MAX_INLINE_META_COUNT) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("inline meta count is too large", K(ret), K(inline_meta_count));
  } else {
    version_ = TABLET_VERSION_V3;
    inline_meta_count_ = inline_meta_count;
    is_inited_ = true;
  }
  return ret;
}

int ObTabletBlockHeader::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &version_))) {
    LOG_WARN("failed to deserialize tablet version", K(ret), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &length_))) {
    LOG_WARN("failed to deserialize tablet length", K(ret), K(data_len), K(pos));
  } else if (TABLET_VERSION_V3 != version_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only tablet v3 has header", K(ret), K(version_), K(length_));
  } else if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &checksum_))) {
    LOG_WARN("failed to deserialize checksum", K(ret), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &inline_meta_count_))) {
    LOG_WARN("failed to deserialize tablet secondary_meta_count", K(ret), K(data_len), K(pos));
  } else if (OB_UNLIKELY(inline_meta_count_ > MAX_INLINE_META_COUNT)) {
    ret = OB_DESERIALIZE_ERROR;
    LOG_WARN("inline_meta_count is too large", K(ret), K(inline_meta_count_));
  } else {
    const int64_t desc_array_len = inline_meta_count_ * sizeof(ObInlineSecondaryMetaDesc);
    if (desc_array_len > 0) {
      MEMCPY(desc_array_, buf + pos, desc_array_len);
      pos += desc_array_len;
    }
    if (OB_UNLIKELY(data_len - pos < length_)) {
      ret = OB_DESERIALIZE_ERROR;
      LOG_WARN("buffer's length is not enough", K(ret), K(data_len), K(pos), K(length_), K(desc_array_len));
    }
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

int64_t ObTabletBlockHeader::get_serialize_size(void) const
{
  int64_t len = 0;
  len += serialization::encoded_length_i32(version_);
  len += serialization::encoded_length_i32(length_);
  len += serialization::encoded_length_i32(checksum_);
  len += serialization::encoded_length_i32(inline_meta_count_);
  len += inline_meta_count_ * sizeof(ObInlineSecondaryMetaDesc);
  return len;
}

int ObTabletBlockHeader::serialize(char* buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  const int64_t size = get_serialize_size();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletBlockHeader not inited", K(ret));
  } else if (OB_UNLIKELY(pushed_inline_meta_cnt_ != inline_meta_count_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inline meta count mismatch", K(ret), K(inline_meta_count_), K(pushed_inline_meta_cnt_));
  } else if (buf_len - pos < size) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("serilize overflow", K(ret), K(buf_len), K(pos), K(size));
  } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, version_))) {
    LOG_WARN("fail to serialize verison", K(ret));
  } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, length_))) {
    LOG_WARN("fail to serialize length", K(ret));
  } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, checksum_))) {
    LOG_WARN("fail to serialize checksum", K(ret));
  } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, inline_meta_count_))) {
    LOG_WARN("fail to serialize inline_meta_count", K(ret));
  } else {
    if (inline_meta_count_ > 0) {
      MEMCPY(buf + pos, desc_array_, inline_meta_count_ * sizeof(ObInlineSecondaryMetaDesc));
      pos += inline_meta_count_ * sizeof(ObInlineSecondaryMetaDesc);
    }
  }
  return ret;
}

int ObTabletBlockHeader::push_inline_meta(const ObInlineSecondaryMetaDesc &desc)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletBlockHeader not inited", K(ret));
  } else if (OB_UNLIKELY(pushed_inline_meta_cnt_ >= inline_meta_count_)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("pushed meta count is overflow", K(ret), K(inline_meta_count_));
  } else {
    desc_array_[pushed_inline_meta_cnt_] = desc;
    pushed_inline_meta_cnt_++;
  }
  return ret;
}

void ObSecondaryMetaHeader::destroy()
{
  version_ = SECONDARY_META_HEADER_VERSION;
  size_ = sizeof(ObSecondaryMetaHeader);
  checksum_ = 0;
  payload_size_ = 0;
}

int ObSecondaryMetaHeader::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_ISNULL(buf) || OB_UNLIKELY(0 >= data_len || 0 > pos || data_len - pos < get_serialize_size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode_i32(buf, data_len, new_pos, &version_))) {
    LOG_WARN("fail to deserialize version", K(ret), K(data_len), K(new_pos));
  } else if (OB_UNLIKELY(SECONDARY_META_HEADER_VERSION != version_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("header version doesn't match", K(ret), K(version_));
  } else if (OB_FAIL(serialization::decode_i32(buf, data_len, new_pos, &size_))) {
    LOG_WARN("fail to deserialize size", K(ret), K(data_len), K(new_pos));
  } else if (new_pos - pos < size_ && OB_FAIL(serialization::decode_i32(buf, data_len, new_pos, &checksum_))) {
    LOG_WARN("fail to deserialize checksum", K(ret), K(data_len), K(new_pos));
  } else if (new_pos - pos < size_ && OB_FAIL(serialization::decode_i32(buf, data_len, new_pos, &payload_size_))) {
    LOG_WARN("fail to deserialize length", K(ret), K(data_len), K(new_pos));
  } else if (OB_UNLIKELY(new_pos - pos != size_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("header size doesn't match", K(ret), K(new_pos), K(pos), K(size_));
  } else {
    pos = new_pos;
  }
  return ret;
}

int ObSecondaryMetaHeader::serialize(char* buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(0 >= buf_len || 0 > pos || buf_len - pos < get_serialize_size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, version_))) {
    LOG_WARN("fail to serialize header version", K(ret), K(buf_len), K(pos), K_(version));
  } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, size_))) {
    LOG_WARN("fail to serialize header size", K(ret), K(buf_len), K(pos), K_(size));
  } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, checksum_))) {
    LOG_WARN("fail to serialize checksum", K(ret), K(buf_len), K_(checksum));
  } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, payload_size_))) {
    LOG_WARN("fail to serialize length", K(ret), K(buf_len), K_(payload_size));
  }
  return ret;
}

int64_t ObSecondaryMetaHeader::get_serialize_size() const
{
  return serialization::encoded_length_i32(version_)
         + serialization::encoded_length_i32(size_)
         + serialization::encoded_length_i32(checksum_)
         + serialization::encoded_length_i32(payload_size_);
}

} // namespace storage
} // namespace oceanbase
