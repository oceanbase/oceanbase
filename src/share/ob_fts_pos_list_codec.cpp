/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */


#define USING_LOG_PREFIX STORAGE_FTS

#include "share/ob_fts_pos_list_codec.h"

#include "lib/codec/ob_delta_zigzag_pfor.h"
#include "lib/utility/serialization.h"
#include "share/rc/ob_tenant_base.h"
#include "share/ob_fts_index_builder_util.h"
namespace oceanbase
{
namespace share
{


int ObFTSPositionListStore::serialize(const uint64_t data_version, char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == buf || buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(buf), K(buf_len));
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, pos, magic_))) {
    LOG_WARN("fail to serialize magic", K(ret));
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, pos, version_))) {
    LOG_WARN("fail to serialize version", K(ret));
  } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, length_))) {
    LOG_WARN("fail to serialize length", K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, checksum_))) {
    LOG_WARN("fail to serialize checksum", K(ret));
  } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, pos_list_cnt_))) {
    LOG_WARN("fail to serialize pos list cnt", K(ret));
  } else if (OB_FAIL(pos_list_str_.serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize pos list str", K(ret));
  }
  return ret;
}

int ObFTSPositionListStore::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == buf || data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(buf), K(data_len));
  } else if (OB_FAIL(serialization::decode_i16(buf, data_len, pos, &magic_))) {
    LOG_WARN("fail to deserialize magic", K(ret));
  } else if (OB_FAIL(serialization::decode_i16(buf, data_len, pos, &version_))) {
    LOG_WARN("fail to deserialize version", K(ret));
  } else if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &length_))) {
    LOG_WARN("fail to deserialize length", K(ret));
  } else if (pos < length_ && OB_FAIL(serialization::decode_i64(buf, data_len, pos, &checksum_))) {
    LOG_WARN("fail to deserialize checksum", K(ret));
  } else if (pos < length_ && OB_FAIL(serialization::decode_i32(buf, data_len, pos, &pos_list_cnt_))) {
    LOG_WARN("fail to deserialize pos list cnt", K(ret));
  } else if (pos < length_ && OB_FAIL(pos_list_str_.deserialize(buf, data_len, pos))) {
    LOG_WARN("fail to deserialize pos list str", K(ret));
  }

  // verify the checksum of the pos list str
  if (OB_SUCC(ret)) {
    int64_t checksum = static_cast<int64_t>(ob_crc64(pos_list_str_.ptr(), pos_list_str_.length()));
    if (checksum != checksum_) {
      ret = OB_CHECKSUM_ERROR;
      LOG_WARN("checksum mismatch", K(ret), K(checksum), K(checksum_));
    }
  }

  return ret;
}

int64_t ObFTSPositionListStore::get_serialize_size(const uint64_t data_version) const
{
  int64_t size = 0;
  size += serialization::encoded_length_i16(magic_);
  size += serialization::encoded_length_i16(version_);
  size += serialization::encoded_length_i32(length_);
  size += serialization::encoded_length_i64(checksum_);
  size += serialization::encoded_length_i32(pos_list_cnt_);
  size += pos_list_str_.get_serialize_size();
  return size;
}

int ObFTSPositionListStore::calculate_checksum(const ObString &pos_list_str, int64_t &checksum)
{
  int ret = OB_SUCCESS;
  checksum = static_cast<int64_t>(ob_crc64(pos_list_str.ptr(), pos_list_str.length()));
  return ret;
}

int ObFTSPositionListStore::encode_pos_list(ObIAllocator &allocator, const ObIArray<int64_t> &pos_list, ObString &pos_list_str)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(pos_list.count() > ObFTSConstants::MAX_POSITION_LIST_COUNT)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("pos list count is too large", K(ret), K(pos_list.count()));
  } else if (OB_UNLIKELY(pos_list.count() == 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid empty pos list for encode", K(ret));
  } else {
    if (OB_FAIL(encode_with_variable_int64(allocator, pos_list, pos_list_str))) {
      LOG_WARN("fail to encode pos list with variable int64", K(ret), K(pos_list), K(pos_list_str));
    }
  }
  return ret;
}

// before decode, you should better get the count of the pos list and reserve the capacity of the pos list
int ObFTSPositionListStore::decode_pos_list(const ObString &pos_list_str, const int64_t pos_count, ObIArray<int64_t> &pos_list)
{
  return decode_with_variable_int64(pos_list_str, pos_count, pos_list);
}

int ObFTSPositionListStore::encode_with_variable_int64(ObIAllocator &allocator, const ObIArray<int64_t> &pos_list, ObString &pos_list_str)
{
  int ret = OB_SUCCESS;

  int64_t total_encoding_size = 0;
  for (int64_t i = 0; i < pos_list.count(); ++i) {
    total_encoding_size += serialization::encoded_length_vi64(pos_list.at(i));
  }

  char *encoded_buffer = nullptr;
  if (OB_ISNULL(encoded_buffer = static_cast<char *>(allocator.alloc(total_encoding_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for encoded buffer", K(ret), K(total_encoding_size));
  } else {
    int64_t out_pos = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < pos_list.count(); ++i) {
      if (OB_FAIL(serialization::encode_vi64(encoded_buffer, total_encoding_size, out_pos, pos_list.at(i)))) {
        LOG_WARN("fail to encode pos with vi64", K(ret), K(i), K(pos_list.at(i)), K(out_pos));
      }
    }
    if (OB_SUCC(ret)) {
      pos_list_str.assign_ptr(encoded_buffer, total_encoding_size);
    }
  }
  return ret;
}

int ObFTSPositionListStore::decode_with_variable_int64(const ObString &pos_list_str, const int64_t pos_count, ObIArray<int64_t> &pos_list)
{
  int ret = OB_SUCCESS;
  const char *buffer = pos_list_str.ptr();
  const int64_t data_len = pos_list_str.length();
  int64_t pos = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < pos_count; ++i) {
    int64_t value = 0;
    if (OB_FAIL(serialization::decode_vi64(buffer, data_len, pos, &value))) {
      LOG_WARN("fail to decode pos with vi64", K(ret), K(i), K(pos), K(data_len));
    } else if (OB_FAIL(pos_list.push_back(value))) {
      LOG_WARN("fail to push back position value", K(ret), K(i), K(value));
    }
  }
  return ret;
}

int ObFTSPositionListStore::encode_with_delta_zigzag_pfor(ObIAllocator &allocator, const ObIArray<int64_t> &pos_list, ObString &pos_list_str)
{
  int ret = OB_SUCCESS;

  ObArenaAllocator tmp_allocator;
  tmp_allocator.set_attr(ObMemAttr(MTL_ID(), "PosListCompAlc"));

  int64_t *origin_data = nullptr;
  const int64_t orig_buffer_len = pos_list.count() * sizeof(int64_t); // maybe a stack char array is enough
  if (OB_ISNULL(origin_data = static_cast<int64_t *>(tmp_allocator.alloc(orig_buffer_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for origin data", K(ret), K(orig_buffer_len));
  } else {
    for (int64_t i = 0; i < pos_list.count(); ++i) {
      origin_data[i] = pos_list.at(i);
    }
  }

  char *encoded_buffer = nullptr;
  if (OB_SUCC(ret)) {
    ObDeltaZigzagPFor codec;
    codec.set_uint_bytes(sizeof(int64_t));
    codec.set_pfor_packing_type(ObCodec::PFoRPackingType::CPU_ARCH_INDEPENDANT_SCALAR);
    const int64_t max_encoding_size = codec.get_max_encoding_size((char *)origin_data, orig_buffer_len);
    if (OB_ISNULL(encoded_buffer = static_cast<char *>(tmp_allocator.alloc(max_encoding_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory for delta zigzag pfor encoding", K(ret), K(max_encoding_size));
    } else {
      uint64_t out_pos = 0;
      if (OB_FAIL(codec.encode((char *)origin_data, orig_buffer_len, encoded_buffer, max_encoding_size, out_pos))) {
        LOG_WARN(
            "fail to encode pos list with delta zigzag pfor",
            K(ret),
            K(orig_buffer_len),
            K(max_encoding_size),
            K(out_pos));
      } else {
        pos_list_str.assign_ptr(encoded_buffer, out_pos);
      }
    }
  }
  return ret;
}

int ObFTSPositionListStore::decode_with_delta_zigzag_pfor(const ObString &pos_list_str, const int64_t pos_count, ObIArray<int64_t> &pos_list)
{
  int ret = OB_SUCCESS;
  ObDeltaZigzagPFor codec;
  codec.set_uint_bytes(sizeof(int64_t));
  codec.set_pfor_packing_type(ObCodec::PFoRPackingType::CPU_ARCH_INDEPENDANT_SCALAR);
  const int64_t decompressed_buf_len = pos_count * sizeof(int64_t);
  int64_t *decompressed_buffer = nullptr;

  ObArenaAllocator tmp_allocator;
  tmp_allocator.set_attr(ObMemAttr(MTL_ID(), "PosListCompAlc"));

  uint64_t in_pos = 0;
  uint64_t out_pos = 0;

  if (OB_ISNULL(decompressed_buffer = static_cast<int64_t *>(tmp_allocator.alloc(decompressed_buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for decompressed buffer", K(ret), K(decompressed_buf_len), K(pos_count));
  } else {
    if (OB_FAIL(codec.decode(
            pos_list_str.ptr(),
            pos_list_str.length(),
            in_pos,
            pos_count,
            (char *)decompressed_buffer,
            decompressed_buf_len,
            out_pos))) {
      LOG_WARN("fail to decode with delta zigzag pfor", K(ret), K(in_pos), K(pos_count), K(out_pos));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < pos_count; ++i) {
        if (OB_FAIL(pos_list.push_back(decompressed_buffer[i]))) {
          LOG_WARN("fail to push back position value", K(ret), K(i), K(decompressed_buffer[i]));
        }
      }
    }
  }
  return ret;
}

int ObFTSPositionListStore::encode_and_serialize(ObIAllocator &allocator, const ObIArray<int64_t> &pos_list, char *&buf, int64_t &buf_len)
{
  int ret = OB_SUCCESS;

  ObString encoded_pos_buf;
  ObArenaAllocator tmp_allocator;
  tmp_allocator.set_attr(ObMemAttr(MTL_ID(), "PosListCompAlc"));
  uint64_t data_version = GET_MIN_CLUSTER_VERSION(); // currently not used.

  int64_t total_length = 0;
  if (OB_FAIL(ObFTSPositionListStore::encode_pos_list(tmp_allocator, pos_list, encoded_pos_buf))) {
    LOG_WARN("fail to encode pos list", K(ret), K(pos_list));
  } else {
    set_pos_list_str(encoded_pos_buf);
    set_pos_list_cnt(pos_list.count());
    int64_t checksum = 0;
    if (OB_FAIL(ObFTSPositionListStore::calculate_checksum(encoded_pos_buf, checksum))) {
      LOG_WARN("fail to calculate checksum", K(ret), K(encoded_pos_buf));
    } else {
      set_checksum(checksum);
    }
    total_length = get_serialize_size(data_version);
    set_length(total_length);
  }

  char *store_buf = nullptr;
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(store_buf = reinterpret_cast<char *>(allocator.alloc(total_length)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for full text index rows buffer", K(ret));
    } else {
      int64_t pos = 0;
      if (OB_FAIL(serialize(data_version, store_buf, total_length, pos))) {
        LOG_WARN("fail to serialize pos list store", K(ret), K(*this));
      }
    }
    buf = store_buf;
    buf_len = total_length;
  }
  return ret;
}

int ObFTSPositionListStore::deserialize_and_decode(const char *buf, const int64_t buf_len, ObIArray<int64_t> &pos_list)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_FAIL(deserialize(buf, buf_len, pos))) {
    LOG_WARN("fail to deserialize pos list store", K(ret), K(*this));
  } else if (OB_FAIL(pos_list.reserve(pos_list_cnt_))) {
    LOG_WARN("fail to reserve pos list", K(ret), K(pos_list_cnt_));
  } else if (OB_FAIL(decode_pos_list(pos_list_str_, pos_list_cnt_, pos_list))) {
    LOG_WARN("fail to decode pos list", K(ret), K(pos_list_str_), K(pos_list_cnt_));
  }

  return ret;
}

} // end namespace share
} // end namespace oceanbase