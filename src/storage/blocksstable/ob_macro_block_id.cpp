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
#include "ob_macro_block_id.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase {
using namespace common;
namespace blocksstable {
ObMacroBlockIdMode MacroBlockId::DEFAULT_MODE = ObMacroBlockIdMode::ID_MODE_LOCAL;

MacroBlockId::MacroBlockId()
{
  reset();
}

MacroBlockId::MacroBlockId(const int64_t block_id)
{
  reset();
  block_index_ = block_id;
}

MacroBlockId::MacroBlockId(const MacroBlockId& id)
{
  first_id_ = id.first_id_;
  second_id_ = id.second_id_;
  third_id_ = id.third_id_;
  fourth_id_ = id.fourth_id_;
}

MacroBlockId::MacroBlockId(
    const int32_t disk_no, const int32_t disk_install_count, const int32_t write_seq, const int32_t block_index)
{
  first_id_ = 0;
  second_id_ = 0;
  third_id_ = 0;
  fourth_id_ = 0;

  version_ = (MACRO_BLOCK_ID_VERSION & SF_MASK_VERSION);
  id_mode_ = ((uint64_t)ObMacroBlockIdMode::ID_MODE_LOCAL & SF_MASK_ID_MODE);
  disk_no_ = (disk_no & SF_MASK_DISK_NO);
  disk_install_count_ = (disk_install_count & SF_MASK_DISK_INSTALL_COUNT);
  write_seq_ = (write_seq & SF_MASK_WRITE_SEQ);
  block_index_ = block_index;
}

void MacroBlockId::reset()
{
  first_id_ = 0;
  second_id_ = 0;
  third_id_ = 0;
  fourth_id_ = 0;

  version_ = (MACRO_BLOCK_ID_VERSION & SF_MASK_VERSION);
  id_mode_ = ((uint64_t)DEFAULT_MODE & SF_MASK_ID_MODE);

  switch ((ObMacroBlockIdMode)id_mode_) {
    case ObMacroBlockIdMode::ID_MODE_LOCAL:
      block_index_ = INT64_MAX;
      break;
    case ObMacroBlockIdMode::ID_MODE_APPEND:
      file_offset_ = INT64_MAX;
      break;
    case ObMacroBlockIdMode::ID_MODE_BLOCK:
      second_id_ = INT64_MAX;
      break;
    default:
      break;
  }
}

bool MacroBlockId::is_valid() const
{
  bool b_ret = (MACRO_BLOCK_ID_VERSION == version_) && (id_mode_ < (uint64_t)ObMacroBlockIdMode::ID_MODE_MAX);

  if (b_ret) {
    switch ((ObMacroBlockIdMode)id_mode_) {
      case ObMacroBlockIdMode::ID_MODE_LOCAL:
        b_ret = block_index_ >= -1 && block_index_ < INT64_MAX;
        break;
      case ObMacroBlockIdMode::ID_MODE_APPEND:
        b_ret = file_offset_ >= -1 && file_offset_ < INT64_MAX;
        break;
      case ObMacroBlockIdMode::ID_MODE_BLOCK:
        b_ret = second_id_ >= -1 && second_id_ < INT64_MAX;
        break;
      default:
        b_ret = false;
        break;
    }
  }
  return b_ret;
}

int64_t MacroBlockId::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  switch ((ObMacroBlockIdMode)id_mode_) {
    case ObMacroBlockIdMode::ID_MODE_LOCAL:
      databuff_printf(buf,
          buf_len,
          pos,
          "[L][%lu-%lu-%lu-%lu-%lu][%ld][%ld][%ld]",
          version_ & SF_MASK_VERSION,
          id_mode_ & SF_MASK_ID_MODE,
          disk_no_ & SF_MASK_DISK_NO,
          disk_install_count_ & SF_MASK_DISK_INSTALL_COUNT,
          write_seq_ & SF_MASK_WRITE_SEQ,
          block_index_,
          third_id_,
          fourth_id_);
      break;
    case ObMacroBlockIdMode::ID_MODE_APPEND:
      databuff_printf(buf,
          buf_len,
          pos,
          "[A][%lu-%lu-%ld][%lu][%ld][%ld]",
          version_ & SF_MASK_VERSION,
          id_mode_ & SF_MASK_ID_MODE,
          file_id_ & SF_MASK_FILE_ID,
          file_offset_,
          third_id_,
          fourth_id_);
      break;
    case ObMacroBlockIdMode::ID_MODE_BLOCK:
      databuff_printf(buf,
          buf_len,
          pos,
          "[B][%lu-%lu-%ld][%ld][%lu-%lu][%ld]",
          version_ & SF_MASK_VERSION,
          id_mode_ & SF_MASK_ID_MODE,
          peg_id_ & SF_MASK_PEG_ID,
          second_id_,
          restart_seq_ & SF_MASK_RESTART_SEQ,
          rewrite_seq_ & SF_MASK_REWRITE_SEQ,
          fourth_id_);
      break;
    default:
      databuff_printf(buf, buf_len, pos, "[%ld][%ld][%ld][%ld]", first_id_, second_id_, third_id_, fourth_id_);
  }
  return pos;
}

void MacroBlockId::set_local_block_id(const int64_t block_index)
{
  if ((uint64_t)ObMacroBlockIdMode::ID_MODE_LOCAL != id_mode_) {
    LOG_ERROR("not local mode!", K(*this), K(block_index));
  }
  block_index_ = block_index;
}

void MacroBlockId::set_append_block_id(const int64_t file_id, const int64_t file_offset)
{
  if ((uint64_t)ObMacroBlockIdMode::ID_MODE_APPEND != id_mode_) {
    LOG_ERROR("not ofs append mode!", K(*this), K(file_id), K(file_offset));
  } else if (OB_UNLIKELY(file_id > SF_MASK_FILE_ID)) {
    LOG_ERROR("file id overflow", K(*this), K(file_id), K(file_offset));
  }
  file_id_ = file_id & SF_MASK_FILE_ID;
  file_offset_ = file_offset;
}

uint64_t MacroBlockId::hash() const
{
  uint64_t hash_val = 0;
  switch ((ObMacroBlockIdMode)id_mode_) {
    case ObMacroBlockIdMode::ID_MODE_LOCAL:
      hash_val = block_index_ * HASH_MAGIC_NUM;
      break;
    case ObMacroBlockIdMode::ID_MODE_APPEND:
      hash_val = file_offset_ / OB_DEFAULT_MACRO_BLOCK_SIZE * HASH_MAGIC_NUM;
      break;
    case ObMacroBlockIdMode::ID_MODE_BLOCK:
      hash_val = common::murmurhash(&first_id_, sizeof(int64_t), hash_val);
      hash_val = common::murmurhash(&second_id_, sizeof(int64_t), hash_val);
      hash_val = common::murmurhash(&third_id_, sizeof(int64_t), hash_val);
      hash_val = common::murmurhash(&fourth_id_, sizeof(int64_t), hash_val);
      break;
    default:
      LOG_ERROR("unexpected id mode!", K(*this));
      break;
  }
  return hash_val;
}

DEFINE_SERIALIZE(MacroBlockId)
{
  int ret = OB_SUCCESS;
  const int64_t ser_len = get_serialize_size();
  int64_t ser_id = first_id_;
  if (NULL == buf || buf_len <= 0 || (buf_len - pos) < ser_len) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", K(ret), KP(buf), K(buf_len), K(pos), K(ser_len));
  } else if ((uint64_t)ObMacroBlockIdMode::ID_MODE_LOCAL == id_mode_) {
    // local mode doesn't serialize below fields
    ser_id = (first_id_ & SF_FILTER_OCCUPY_LOCAL);
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, ser_id))) {
    LOG_WARN("serialize first id failed.", K(ret), K(pos), K(buf_len), K(ser_len), K(ser_id), K(*this));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, second_id_))) {
    LOG_WARN("serialize second id failed.", K(ret), K(pos), K(buf_len), K(ser_len), K(*this));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, third_id_))) {
    LOG_WARN("serialize second id failed.", K(ret), K(pos), K(buf_len), K(ser_len), K(*this));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, fourth_id_))) {
    LOG_WARN("serialize second id failed.", K(ret), K(pos), K(buf_len), K(ser_len), K(*this));
  }
  return ret;
}

DEFINE_DESERIALIZE(MacroBlockId)
{
  int ret = OB_SUCCESS;
  int64_t block_index = 0;
  bool is_old_format = true;
  if (NULL == buf || data_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", KP(buf), K(data_len), K(ret));
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &block_index))) {
    LOG_WARN("decode block_index failed.", K(ret), K(pos), K(data_len));
  } else {
    const int64_t version_id = block_index >> SF_BIT_RAW_ID;
    if (0 != version_id) {
      is_old_format = false;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (is_old_format) {
    reset();
    if (INVALID_BLOCK_INDEX_OLD_FORMAT != block_index) {
      block_index_ = block_index;
    }
    third_id_ = 0;
    fourth_id_ = 0;
  } else {
    first_id_ = block_index;
    second_id_ = 0;
    if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &second_id_))) {
      LOG_WARN("decode second_id_ failed.", K(ret), K(pos), K(data_len), K(*this));
    } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &third_id_))) {
      LOG_WARN("decode third_id_ failed.", K(ret), K(pos), K(data_len), K(*this));
    } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &fourth_id_))) {
      LOG_WARN("decode third_id_ failed.", K(ret), K(pos), K(data_len), K(*this));
    }
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(MacroBlockId)
{
  int64_t len = 0;
  len += serialization::encoded_length_vi64(first_id_);
  len += serialization::encoded_length_vi64(second_id_);
  len += serialization::encoded_length_vi64(third_id_);
  len += serialization::encoded_length_vi64(fourth_id_);
  return len;
}

int MacroBlockId::serialize_old(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  if (NULL == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, block_index_))) {
    LOG_WARN("serialize block_index failed.", K(ret), K(pos), K(buf_len), K(*this));
  }
  return ret;
}

bool MacroBlockId::operator<(const MacroBlockId& other) const
{
  bool bret = false;
  if (other.id_mode_ != id_mode_) {
    LOG_ERROR("different id_mode_!", K(*this), K(other));
  } else {
    switch ((ObMacroBlockIdMode)id_mode_) {
      case ObMacroBlockIdMode::ID_MODE_LOCAL:
        bret = (block_index_ < other.block_index_);
        break;
      case ObMacroBlockIdMode::ID_MODE_APPEND:
        bret = (file_id_ == other.file_id_) ? (file_id_ < other.file_id_) : (file_offset_ < other.file_offset_);
        break;
      case ObMacroBlockIdMode::ID_MODE_BLOCK:
        bret = (peg_id_ == other.peg_id_) ? (peg_id_ < other.peg_id_) : (second_id_ < other.second_id_);
        break;
      default:
        LOG_ERROR("unexpected id mode!", K(*this));
        break;
    }
  }
  return bret;
}
}  // namespace blocksstable
}  // namespace oceanbase
