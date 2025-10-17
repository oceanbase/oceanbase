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
#include "storage/backup/ob_backup_data_struct.h"
#include "storage/blocksstable/ob_ss_obj_util.h"

namespace oceanbase
{
using namespace common;
namespace blocksstable
{
MacroBlockId::MacroBlockId()
  : first_id_(0),
    second_id_(INT64_MAX),
    third_id_(0),
    fourth_id_(0)
{
  version_ = MACRO_BLOCK_ID_VERSION_V2;
}

MacroBlockId::MacroBlockId(
    const int64_t first_id, const int64_t second_id, const int64_t third_id, const int64_t fourth_id)
{
  first_id_ = first_id;
  second_id_ = second_id;
  third_id_ = third_id;
  fourth_id_ = fourth_id;
}
MacroBlockId::MacroBlockId(
    const uint64_t write_seq, const int64_t block_index, const int64_t third_id)
{
  version_ = MACRO_BLOCK_ID_VERSION_V2;
  id_mode_ = (uint64_t)ObMacroBlockIdMode::ID_MODE_LOCAL;
  write_seq_ = write_seq;
  block_index_ = block_index;
  third_id_ = third_id;
  fourth_id_ = 0;
}

MacroBlockId::MacroBlockId(const MacroBlockId &id)
{
  first_id_ = id.first_id_;
  second_id_ = id.second_id_;
  third_id_ = id.third_id_;
  fourth_id_ = id.fourth_id_;
}
void MacroBlockId::first_id_to_string_(char *buf, const int64_t buf_len, int64_t &pos) const
{
  switch ((ObMacroBlockIdMode)id_mode_) {
  case ObMacroBlockIdMode::ID_MODE_LOCAL:
    databuff_printf(buf, buf_len, pos,
                    "{[ver=%lu,mode=%lu,seq=%lu]",
                    (uint64_t) version_,
                    (uint64_t) id_mode_,
                    (uint64_t) write_seq_);
    break;
  case ObMacroBlockIdMode::ID_MODE_SHARE:
    databuff_printf(buf, buf_len, pos,
        "{[ver=%lu,mode=%lu,obj_type=%lu,obj_type_str=%s,incar_id=%lu,cg_id=%lu]",
        (uint64_t) ss_version_,
        (uint64_t) ss_id_mode_,
        (uint64_t) storage_object_type_,
        STI(storage_object_type()).get_type_str(),
        (uint64_t) incarnation_id_,
        (uint64_t) column_group_id_);
    break;
  default:
    databuff_printf(buf, buf_len, pos,
                    "{[1st=%lu,mode=%lu]",
                    first_id_,
                    (uint64_t) id_mode_);
    break;
  }
}

bool MacroBlockId::is_valid() const
{
  bool is_valid = true;

  if (id_mode_ == (uint64_t)ObMacroBlockIdMode::ID_MODE_LOCAL) {
    is_valid &= MACRO_BLOCK_ID_VERSION_V2 == version_ && id_mode_ < (uint64_t)ObMacroBlockIdMode::ID_MODE_MAX;
    is_valid &= second_id_ >= AUTONOMIC_BLOCK_INDEX && second_id_ < INT64_MAX && third_id_ >= 0;
  } else if (id_mode_ == (uint64_t)ObMacroBlockIdMode::ID_MODE_BACKUP) {
    // BACKUP_MODE use BACKUP_MACRO_BLOCK_ID_VERSION
    is_valid &= backup::ObBackupDeviceMacroBlockId::check_valid(first_id_, second_id_, third_id_);
  } else if (is_valid && id_mode_ == (uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE) {
    is_valid &= MACRO_BLOCK_ID_VERSION_V2 == version_ && id_mode_ < (uint64_t)ObMacroBlockIdMode::ID_MODE_MAX;
    if (is_private_data_or_meta()) {
      is_valid &= meta_transfer_seq() != -1 &&  meta_version_id() != ObStorageObjectOpt::INVALID_TABLET_VERSION;
            //                   -1                       : INVLAID_TABLET_TRANSFER_SEQ;
            // ObStorageObjectOpt::INVALID_TABLET_VERSION : macro_seq / tablet_meta_version
    } else if (is_shared_data_or_meta()) {
      is_valid &= third_id_ != -1; // macro_seq != -1
    }
  }
  return is_valid;
}

int64_t MacroBlockId::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  // 1. print first id
  first_id_to_string_(buf, buf_len, pos);

  // 2. print other info
  switch ((ObMacroBlockIdMode)id_mode_) {
  case ObMacroBlockIdMode::ID_MODE_LOCAL:
    databuff_printf(buf, buf_len, pos,
                    "[2nd=%lu]}",
                    (uint64_t) second_id_);
    break;
  case ObMacroBlockIdMode::ID_MODE_SHARE:
    if (is_private_data_or_meta()) {
      databuff_printf(buf, buf_len, pos,
        "[2nd=%lu]"
        "[3rd=%lu]"
        "[4th=(trans_seq=%lu,sec_id=%lu)]}",
        (uint64_t) second_id_,
        (uint64_t) third_id_,
        (int64_t) macro_transfer_seq_,
        (uint64_t) tenant_seq_);
    } else if (is_shared_data_or_meta()) {
      databuff_printf(buf, buf_len, pos,
        "[2nd=%lu]"
        "[3rd=(op_id=%lu, macro_seq_id=%lu)]"
        "[4th=(%s=%lu)]",
        (uint64_t) second_id_,
        (uint64_t) third_id_ >> 32, // op_id
        (uint64_t) third_id_ & 0xFFFFFFFF, // seq_no
        (meta_is_inner_tablet() ? "ls_id" : "reorg_scn"),
        (uint64_t)fourth_id_);
    } else {
      databuff_printf(buf, buf_len, pos,
        "[2nd=%lu]"
        "[3rd=%lu]"
        "[4th=%lu]}",
        (uint64_t) second_id_,
        (uint64_t) third_id_,
        (uint64_t) fourth_id_);
    }
    break;
  default:
    databuff_printf(buf, buf_len, pos,
        "[2nd=%lu]"
        "[3rd=%lu]"
        "[4th=%lu]}",
        (uint64_t) second_id_,
        (uint64_t) third_id_,
        (uint64_t) fourth_id_);
    break;
  }
  return pos;
}

uint64_t MacroBlockId::hash() const
{
  uint64_t hash_val = 0;
  switch ((ObMacroBlockIdMode)id_mode_) {
  case ObMacroBlockIdMode::ID_MODE_LOCAL:
  case ObMacroBlockIdMode::ID_MODE_BACKUP:
    hash_val = block_index_ * HASH_MAGIC_NUM;
    break;
  case ObMacroBlockIdMode::ID_MODE_SHARE: {
    int64_t tmp_val = first_id_ ^ second_id_^ third_id_ ^ fourth_id_;
    hash_val = murmurhash(&tmp_val, sizeof(tmp_val), hash_val);
    break;
  }
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
    case ObMacroBlockIdMode::ID_MODE_BACKUP:
      hash_val = block_index_ * HASH_MAGIC_NUM;
      break;
    case ObMacroBlockIdMode::ID_MODE_SHARE: {
      int64_t tmp_val = first_id_ ^ second_id_^ third_id_ ^ fourth_id_;
      hash_val = murmurhash(&tmp_val, sizeof(tmp_val), hash_val);
      break;
    }
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected id mode!", K(ret), K(*this));
      break;
  }
  return ret;
}

bool MacroBlockId::is_id_mode_local() const
{
  ObMacroBlockIdMode mode = static_cast<ObMacroBlockIdMode>(id_mode_);
  return (ObMacroBlockIdMode::ID_MODE_LOCAL == mode);
}

bool MacroBlockId::is_id_mode_backup() const
{
  ObMacroBlockIdMode mode = static_cast<ObMacroBlockIdMode>(id_mode_);
  return (ObMacroBlockIdMode::ID_MODE_BACKUP == mode);
}

bool MacroBlockId::is_id_mode_share() const
{
  ObMacroBlockIdMode mode = static_cast<ObMacroBlockIdMode>(id_mode_);
  return (ObMacroBlockIdMode::ID_MODE_SHARE == mode);
}

const ObStorageObjectTypeBase &MacroBlockId::get_type_instance() const
{
  return ObStorageObjectTypeInstance::get_instance(storage_object_type());
}

/*
  SHARED_MICRO_DATA_MACRO
  SHARED_MICRO_META_MACRO
  SHARED_MINI_DATA_MACRO
  SHARED_MINI_META_MACRO
  SHARED_MINOR_DATA_MACRO
  SHARED_MINOR_META_MACRO
  SHARED_MDS_MINI_DATA_MACRO
  SHARED_MDS_MINI_META_MACRO
  SHARED_MDS_MINOR_DATA_MACRO
  SHARED_MDS_MINOR_META_MACRO
  SHARED_MAJOR_DATA_MACRO
  SHARED_MAJOR_META_MACRO
  SHARED_TABLET_SUB_META
*/
bool MacroBlockId::is_shared_data_or_meta() const
{
  return is_id_mode_share() && SSObjUtil::is_shared(storage_object_type()) && (SSObjUtil::is_macro(storage_object_type()) ||
         SSObjUtil::is_tablet_meta(storage_object_type()));
}
/*
  SHARED_MICRO_DATA_MACRO
  SHARED_MICRO_META_MACRO
  SHARED_MINI_DATA_MACRO
  SHARED_MINI_META_MACRO
  SHARED_MINOR_DATA_MACRO
  SHARED_MINOR_META_MACRO
  SHARED_MDS_MINI_DATA_MACRO
  SHARED_MDS_MINI_META_MACRO
  SHARED_MDS_MINOR_DATA_MACRO
  SHARED_MDS_MINOR_META_MACRO
  SHARED_MAJOR_DATA_MACRO
  SHARED_MAJOR_META_MACRO
*/
bool MacroBlockId::is_shared_data_block_or_meta_block() const
{
  return is_id_mode_share() && SSObjUtil::is_shared(storage_object_type()) && SSObjUtil::is_macro(storage_object_type());
}

bool MacroBlockId::is_shared_data_block_or_meta_block_except_mds() const
{
  return is_id_mode_share() && SSObjUtil::is_shared(storage_object_type()) && SSObjUtil::is_macro(storage_object_type()) &&
         !SSObjUtil::is_mds(storage_object_type());
}

/*
SHARED_MICRO_DATA_MACRO
SHARED_MINI_DATA_MACRO
SHARED_MINOR_DATA_MACRO
SHARED_MAJOR_DATA_MACRO
*/
bool MacroBlockId::is_shared_data_block_except_mds() const
{
  return is_id_mode_share() && SSObjUtil::is_shared(storage_object_type()) && SSObjUtil::is_macro_data(storage_object_type()) &&
         !SSObjUtil::is_mds(storage_object_type());
}
/*
  PRIVATE_DATA_MACRO
  PRIVATE_META_MACRO
  PRIVATE_CKPT_FILE
  PRIVATE_SLOG_FILE
  PRIVATE_TABLET_META
*/
bool MacroBlockId::is_private_data_or_meta() const
{
  return is_id_mode_share() && SSObjUtil::is_private(storage_object_type()) && (SSObjUtil::is_macro(storage_object_type()) ||
         SSObjUtil::is_tablet_meta(storage_object_type()) || SSObjUtil::is_tenant_meta(storage_object_type()));
}

/*
  PRIVATE_DATA_MACRO
  SHARED_MINI_DATA_MACRO
  SHARED_MINOR_DATA_MACRO
  SHARED_MICRO_DATA_MACRO  // new added
  SHARED_MDS_MINI_DATA_MACRO
  SHARED_MDS_MINOR_DATA_MACRO
  SHARED_MAJOR_DATA_MACRO
  EXTERNAL_TABLE_FILE
*/
bool MacroBlockId::is_data() const
{
  return is_id_mode_share() && (SSObjUtil::is_macro_data(storage_object_type()) || SSObjUtil::is_tenant_data(storage_object_type()));
}
/*
  PRIVATE_META_MACRO
  PRIVATE_TABLET_META
  SHARED_MINI_META_MACRO
  SHARED_MINOR_META_MACRO
  SHARED_MICRO_META_MACRO   // new added
  SHARED_MDS_MINI_META_MACRO
  SHARED_MDS_MINOR_META_MACRO
  SHARED_MAJOR_META_MACRO
  SHARED_TABLET_SUB_META
*/
bool MacroBlockId::is_meta() const
{
  return is_id_mode_share() && (SSObjUtil::is_macro_meta(storage_object_type()) || SSObjUtil::is_tablet_meta(storage_object_type()));
}
/*
  PRIVATE_DATA_MACRO
  PRIVATE_META_MACRO
  SHARED_MINI_DATA_MACRO
  SHARED_MINI_META_MACRO
  SHARED_MINOR_DATA_MACRO
  SHARED_MINOR_META_MACRO
  SHARED_MAJOR_DATA_MACRO
  SHARED_MAJOR_META_MACRO
  PRIVATE_TABLET_META
  SHARED_MDS_MINI_DATA_MACRO
  SHARED_MDS_MINI_META_MACRO
  SHARED_MDS_MINOR_DATA_MACRO
  SHARED_MDS_MINOR_META_MACRO
  SHARED_TABLET_SUB_META
*/
bool MacroBlockId::is_tablet_local_cache_object() const
{
  return is_id_mode_share() && SSObjUtil::has_effective_tablet_id(storage_object_type());
}
/*
  PRIVATE_DATA_MACRO
  PRIVATE_META_MACRO
*/
bool MacroBlockId::is_private_macro() const
{
  return is_id_mode_share() && SSObjUtil::is_macro(storage_object_type()) && SSObjUtil::is_private(storage_object_type());
}
/*
  PRIVATE_DATA_MACRO
  PRIVATE_META_MACRO
  PRIVATE_TABLET_META
*/
bool MacroBlockId::is_macro_write_cache_ctrl_obj_type() const
{
  return is_id_mode_share() && (SSObjUtil::is_macro(storage_object_type()) || SSObjUtil::is_tablet_meta(storage_object_type())) &&
         SSObjUtil::is_private(storage_object_type()) &&!SSObjUtil::is_direct_write(storage_object_type()) &&
         !SSObjUtil::use_reserved_disk_space(storage_object_type()) && !SSObjUtil::is_tmp_file(storage_object_type());
}

DEFINE_SERIALIZE(MacroBlockId)
{
  int ret = OB_SUCCESS;
  const int64_t ser_len = get_serialize_size();
  int64_t new_pos = pos;
  if (NULL == buf || buf_len <= 0 || (buf_len - new_pos) < ser_len) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", K(ret), KP(buf), K(buf_len), K(new_pos), K(ser_len));
  } else if (OB_UNLIKELY(!is_valid() || version_ != MACRO_BLOCK_ID_VERSION_V2)) { // serialize must use new version
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro block id.", K(ret), K(*this));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, first_id_))) {
    LOG_WARN("serialize first id failed.", K(ret), K(new_pos), K(buf_len), K(ser_len), K(*this));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, second_id_))) {
    LOG_WARN("serialize second id failed.", K(ret), K(new_pos), K(buf_len), K(ser_len), K(*this));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, third_id_))) {
    LOG_WARN("serialize third id failed.", K(ret), K(new_pos), K(buf_len), K(ser_len), K(*this));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, fourth_id_))) {
    LOG_WARN("serialize forth id failed.", K(ret), K(new_pos), K(buf_len), K(ser_len), K(*this));
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
  } else if (version_ == MACRO_BLOCK_ID_VERSION_V1) {
    version_ = MACRO_BLOCK_ID_VERSION_V2;
    fourth_id_ = 0;
  } else if (version_ == MACRO_BLOCK_ID_VERSION_V2) {
    if (OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &fourth_id_))) {
      LOG_WARN("decode fourth_id_ failed.", K(ret), K(new_pos), K(data_len), K(*this));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(!is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid macro block id.", K(ret), K(*this));
    } else {
      pos = new_pos;
    }
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(MacroBlockId)
{
  int64_t len = 0;
  len += serialization::encoded_length_i64(first_id_);
  len += serialization::encoded_length_i64(second_id_);
  len += serialization::encoded_length_i64(third_id_);
  len += serialization::encoded_length_i64(fourth_id_);
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
    case ObMacroBlockIdMode::ID_MODE_SHARE:
      bret = MEMCMP(this, &other, sizeof(MacroBlockId));
      break;
    default:
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "unexpected id mode!", K(*this));
      break;
    }
  }
  return bret;
}

int MacroBlockId::memcpy_deserialize(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0 || pos < 0 || pos >= data_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", KP(buf), K(data_len), K(pos), K(ret));
  }
  DESERIALIZE_MEMBER_WITH_MEMCPY(first_id_);
  if (OB_SUCC(ret) && OB_UNLIKELY(MACRO_BLOCK_ID_VERSION_V1 != version_)) {
    LOG_WARN("only the old version macro block may use memcopy when serialize", K(ret), K(version_));
  }
  DESERIALIZE_MEMBER_WITH_MEMCPY(second_id_);
  DESERIALIZE_MEMBER_WITH_MEMCPY(third_id_);
  version_ = MACRO_BLOCK_ID_VERSION_V2;
  fourth_id_  = 0;
  return ret;
}

bool MacroBlockId::meta_is_inner_tablet() const
{
  bool b_ret = false;
  if (SSObjUtil::need_check_inner_tablet_type(static_cast<ObStorageObjectType>(storage_object_type_))) {
    const ObTabletID tablet_id(second_id_);
    b_ret = tablet_id.is_ls_inner_tablet();
  }
  return b_ret;
}

} // namespace blocksstable
} // namespace oceanbase

