/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "storage/blocksstable/ob_logic_macro_id.h"
#include "common/ob_version_def.h"

namespace oceanbase
{
using namespace common;

namespace blocksstable
{
int64_t ObMacroDataSeq::get_serialize_size() const
{
  int64_t len = 0;
  len += serialization::encoded_length_vi64(macro_data_seq_);
  return len;
}

int ObMacroDataSeq::serialize(
    char *buf,
    const int64_t buf_len,
    int64_t &pos) const
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(buf_len));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, macro_data_seq_))) {
    LOG_WARN("failed to serialize data seq", K(ret));
  }
  return ret;
}

int ObMacroDataSeq::deserialize(
    const char *buf,
    const int64_t data_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len < 0 || data_len < pos)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &macro_data_seq_))) {
    LOG_WARN("failed to deserialize data seq", K(ret));
  }
  return ret;
}


int64_t ObLogicMacroBlockId::hash() const
{
  int64_t hash_val = 0;
  int64_t macro_data_seq = data_seq_.get_data_seq();
  hash_val = common::murmurhash(&macro_data_seq, sizeof(macro_data_seq), hash_val);
  hash_val = common::murmurhash(&logic_version_, sizeof(logic_version_), hash_val);
  hash_val = common::murmurhash(&tablet_id_, sizeof(tablet_id_), hash_val);
  hash_val = common::murmurhash(&info_, sizeof(info_), hash_val);
  return hash_val;
}

bool ObLogicMacroBlockId::operator==(const ObLogicMacroBlockId &other) const
{
  return data_seq_         == other.data_seq_
      && logic_version_    == other.logic_version_
      && tablet_id_        == other.tablet_id_
      && info_             == other.info_;
}

bool ObLogicMacroBlockId::operator!=(const ObLogicMacroBlockId &other) const
{
  return !(operator==(other));
}

bool ObLogicMacroBlockId::operator<(const ObLogicMacroBlockId &other) const
{
  bool bool_ret = false;
  if (tablet_id_ < other.tablet_id_) {
    bool_ret = true;
  } else if (tablet_id_ > other.tablet_id_) {
    bool_ret= false;
  } else if (logic_version_ < other.logic_version_) {
    bool_ret = true;
  } else if (logic_version_ > other.logic_version_) {
    bool_ret = false;
  } else if (data_seq_.macro_data_seq_ < other.data_seq_.macro_data_seq_) {
    bool_ret = true;
  } else if (data_seq_.macro_data_seq_ > other.data_seq_.macro_data_seq_) {
    bool_ret = false;
  } else if (column_group_idx_ < other.column_group_idx_) {
    bool_ret = true;
  } else if (column_group_idx_ > other.column_group_idx_) {
    bool_ret = false;
  } else if (!is_mds_ && other.is_mds_) {
    bool_ret = true;
  } else if (is_mds_ && !other.is_mds_) {
    bool_ret = false;
  }
  return bool_ret;
}

bool ObLogicMacroBlockId::operator>(const ObLogicMacroBlockId &other) const
{
  bool bool_ret = false;
  if (tablet_id_ < other.tablet_id_) {
    bool_ret = false;
  } else if (tablet_id_ > other.tablet_id_) {
    bool_ret= true;
  } else if (logic_version_ < other.logic_version_) {
    bool_ret = false;
  } else if (logic_version_ > other.logic_version_) {
    bool_ret = true;
  } else if (data_seq_.macro_data_seq_ < other.data_seq_.macro_data_seq_) {
    bool_ret = false;
  } else if (data_seq_.macro_data_seq_ > other.data_seq_.macro_data_seq_) {
    bool_ret = true;
  } else if (column_group_idx_ < other.column_group_idx_) {
    bool_ret = false;
  } else if (column_group_idx_ > other.column_group_idx_) {
    bool_ret = true;
  } else if (!is_mds_ && other.is_mds_) {
    bool_ret = false;
  } else if (is_mds_ && !other.is_mds_) {
    bool_ret = true;
  }
  return bool_ret;
}

void ObLogicMacroBlockId::reset() {
  logic_version_ = 0;
  data_seq_.reset();
  tablet_id_ = 0;
  info_ = 0;
}

OB_SERIALIZE_MEMBER(ObLogicMacroBlockId,
                    data_seq_, //FARM COMPAT WHITELIST: Type not match
                    logic_version_,
                    tablet_id_,
                    info_);

int64_t ObLogicMacroBlockId::get_serialize_size(const uint64_t data_version) const
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, static_cast<int64_t>(LOGIC_BLOCK_ID_VERSION));
  len += serialization::OB_SERIALIZE_SIZE_NEED_BYTES;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              data_seq_,
              logic_version_,
              tablet_id_);
  if (DATA_VERSION_4_3_0_0 <= data_version) {
    LST_DO_CODE(OB_UNIS_ADD_LEN, info_);
  }
  return len;
}

int ObLogicMacroBlockId::serialize(char *buf, const int64_t buf_len, int64_t &pos, const uint64_t data_version) const
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(static_cast<int64_t>(LOGIC_BLOCK_ID_VERSION));
  if (OB_SUCC(ret)) {
    const int64_t size_nbytes = serialization::OB_SERIALIZE_SIZE_NEED_BYTES;
    const int64_t pos_bak = (pos += size_nbytes);
    LST_DO_CODE(OB_UNIS_ENCODE,
                data_seq_,
                logic_version_,
                tablet_id_);
    if (OB_SUCC(ret) && DATA_VERSION_4_3_0_0 <= data_version) {
      LST_DO_CODE(OB_UNIS_ENCODE, info_);
    }
    if (OB_SUCC(ret)) {
      const int64_t serial_size = pos - pos_bak;
      int64_t tmp_pos = 0;
      if (OB_FAIL(serialization::encode_fixed_bytes_i64(buf + pos_bak - size_nbytes,
                                                       size_nbytes,
                                                       tmp_pos,
                                                       serial_size))) {
        LOG_WARN("fail to serialize logic macro block id", K(ret), K(buf_len), K(pos), KDV(data_version));
      }
    }
  }
  return ret;
}

uint64_t ObLogicMicroBlockId::hash() const
{
  uint64_t hash_val = logic_macro_id_.hash();
  hash_val = common::murmurhash(&info_, sizeof(info_), hash_val);
  return hash_val;
}

OB_SERIALIZE_MEMBER(ObLogicMicroBlockId, info_, logic_macro_id_);

} // blocksstable
} // oceanbase
