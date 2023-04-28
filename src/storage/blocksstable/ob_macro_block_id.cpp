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
#include "lib/utility/ob_print_utils.h"
#include "storage/blocksstable/ob_macro_block_id.h"

namespace oceanbase
{
using namespace common;
namespace blocksstable
{
MacroBlockId::MacroBlockId()
  : first_id_(0),
    second_id_(INT64_MAX),
    third_id_(0)
{
}

MacroBlockId::MacroBlockId(const int64_t first_id, const int64_t second_id, const int64_t third_id)
{
  first_id_ = first_id;
  second_id_ = second_id;
  third_id_ = third_id;
}

MacroBlockId::MacroBlockId(const MacroBlockId &id)
{
  first_id_ = id.first_id_;
  second_id_ = id.second_id_;
  third_id_ = id.third_id_;
}

int64_t MacroBlockId::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  switch ((ObMacroBlockIdMode)id_mode_) {
  case ObMacroBlockIdMode::ID_MODE_LOCAL:
    databuff_printf(buf, buf_len, pos,
                    "[%ld](ver=%lu,mode=%lu,seq=%lu)",
                    second_id_,
                    (uint64_t) version_,
                    (uint64_t) id_mode_,
                    write_seq_);
    break;
  default:
    databuff_printf(buf, buf_len, pos,
                    "(ver=%lu,mode=%lu,1st=%ld,2nd=%ld,3rd=%ld)",
                    (uint64_t) version_,
                    (uint64_t) id_mode_,
                    first_id_, second_id_, third_id_);
    break;
  }
  return pos;
}

uint64_t MacroBlockId::hash() const
{
  uint64_t hash_val = 0;
  switch ((ObMacroBlockIdMode)id_mode_) {
  case ObMacroBlockIdMode::ID_MODE_LOCAL:
    hash_val = block_index_ * HASH_MAGIC_NUM;
    break;
  default:
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "unexpected id mode!", K(*this));
    break;
  }
  return hash_val;
}

int MacroBlockId::hash(uint64_t &hash_val) const
{
  int ret = OB_SUCCESS;
  hash_val = 0;
  switch ((ObMacroBlockIdMode)id_mode_) {
    case ObMacroBlockIdMode::ID_MODE_LOCAL:
      hash_val = block_index_ * HASH_MAGIC_NUM;
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected id mode!", K(ret), K(*this));
      break;
  }
  return ret;
}

DEFINE_SERIALIZE(MacroBlockId)
{
  int ret = OB_SUCCESS;
  const int64_t ser_len = get_serialize_size();
  int64_t new_pos = pos;
  if (NULL == buf || buf_len <= 0 || (buf_len - new_pos) < ser_len) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", K(ret), KP(buf), K(buf_len), K(new_pos), K(ser_len));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro block id.", K(ret), K(*this));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, first_id_))) {
    LOG_WARN("serialize first id failed.", K(ret), K(new_pos), K(buf_len), K(ser_len), K(*this));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, second_id_))) {
    LOG_WARN("serialize second id failed.", K(ret), K(new_pos), K(buf_len), K(ser_len), K(*this));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, third_id_))) {
    LOG_WARN("serialize third id failed.", K(ret), K(new_pos), K(buf_len), K(ser_len), K(*this));
  } else {
    pos = new_pos;
  }
  return ret;
}

DEFINE_DESERIALIZE(MacroBlockId)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0 || pos < 0 || pos >= data_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", KP(buf), K(data_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &first_id_))) {
    LOG_WARN("decode first_id_ failed.", K(ret), K(new_pos), K(data_len), K(*this));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &second_id_))) {
    LOG_WARN("decode second_id_ failed.", K(ret), K(new_pos), K(data_len), K(*this));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &third_id_))) {
    LOG_WARN("decode third_id_ failed.", K(ret), K(new_pos), K(data_len), K(*this));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro block id.", K(ret), K(*this));
  } else {
    pos = new_pos;
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(MacroBlockId)
{
  int64_t len = 0;
  len += serialization::encoded_length_i64(first_id_);
  len += serialization::encoded_length_i64(second_id_);
  len += serialization::encoded_length_i64(third_id_);
  return len;
}

bool MacroBlockId::operator <(const MacroBlockId &other) const
{
  bool bret = false;
  if (other.id_mode_ != id_mode_) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "different id_mode_!", K(*this), K(other));
  } else {
    switch ((ObMacroBlockIdMode)id_mode_) {
    case ObMacroBlockIdMode::ID_MODE_LOCAL:
      bret = (block_index_ < other.block_index_);
      break;
    default:
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "unexpected id mode!", K(*this));
      break;
    }
  }
  return bret;
}
} // namespace blocksstable
} // namespace oceanbase

