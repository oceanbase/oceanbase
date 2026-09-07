/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE
#include "ob_storage_object_type.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/meta_store/ob_tenant_storage_meta_service.h"

namespace oceanbase
{
namespace blocksstable
{

// Generated string array
static const char *ob_storage_object_type_strs[] = {
  "PRIVATE_DATA_MACRO",
  "PRIVATE_META_MACRO",
  "SHARED_MINI_DATA_MACRO",
  "SHARED_MINI_META_MACRO",
  "SHARED_MINOR_DATA_MACRO",
  "SHARED_MINOR_META_MACRO",
  "SHARED_MAJOR_DATA_MACRO",
  "SHARED_MAJOR_META_MACRO",
  "TMP_FILE",
  "SERVER_META",
  "PRIVATE_TABLET_META",
  "PRIVATE_SLOG_FILE",
  "PRIVATE_CKPT_FILE",
  "MAJOR_PREWARM_DATA",
  "MAJOR_PREWARM_DATA_INDEX",
  "MAJOR_PREWARM_META",
  "MAJOR_PREWARM_META_INDEX",
  "TENANT_DISK_SPACE_META",
  "IS_SHARED_TENANT_DELETED",
  "SHARED_MICRO_DATA_MACRO",
  "SHARED_MICRO_META_MACRO",
  "UNSEALED_REMOTE_SEG_FILE",
  "SHARED_MDS_MINI_DATA_MACRO",
  "SHARED_MDS_MINI_META_MACRO",
  "SHARED_MDS_MINOR_DATA_MACRO",
  "SHARED_MDS_MINOR_META_MACRO",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "SHARED_TABLET_META",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "SHARED_TABLET_SUB_META",
  "TENANT_ROOT_KEY",
  "EXTERNAL_TABLE_FILE",
  "MACRO_CACHE_CKPT_DATA",
  "MACRO_CACHE_CKPT_META",
  "SHARED_INC_MAJOR_DATA_MACRO",
  "SHARED_INC_MAJOR_META_MACRO",
  "SHARED_TABLET_SUB_META_IN_TABLE",
  "SHARED_MINI_V2_DATA_MACRO",
  "SHARED_MINI_V2_META_MACRO",
  "SHARED_MINOR_V2_DATA_MACRO",
  "SHARED_MINOR_V2_META_MACRO",
  "SHARED_TABLET_MACRO_DIFF",
  "MAX"
};


const char *get_storage_objet_type_str(const ObStorageObjectType type)
{
  return STI(type).get_type_str();
}

/**
 * ---------------------------------------ObStorageObjectTypeBase----------------------------------------
 */
const char *ObStorageObjectTypeBase::get_type_str() const
{
  STATIC_ASSERT(ARRAYSIZEOF(ob_storage_object_type_strs) == (int64_t)ObStorageObjectType::MAX + 1,
                "ob_storage_object_type string array size mismatch enum ObStorageObjectType count");
  const char *str = NULL;
    if (type_ < ObStorageObjectType::MAX) {
    str = ob_storage_object_type_strs[static_cast<int64_t>(type_)];
  } else {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "invalid ObStorageObjectType", K_(type));
  }
  return str;
}

int64_t ObStorageObjectTypeBase::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(type), "type_str", get_type_str());
  J_OBJ_END();
  return pos;
}


bool ObStorageObjectTypeBase::has_write_back_strategy() const
{
  return (write_strategy() & (1 << (uint8_t)ObStorageObjectWriteStrategy::WRITE_BACK));
}

bool ObStorageObjectTypeBase::has_write_through_and_try_write_lcache_strategy() const
{
  return (write_strategy() & (1 << (uint8_t)ObStorageObjectWriteStrategy::WRITE_THROUGH_AND_TRY_WRITE_LCACHE));
}

void ObStorageObjectTypeBase::set_ss_object_first_id_(
  const uint64_t incarnation_id, const uint64_t column_group_id, MacroBlockId &object_id) const
{
  object_id.set_version_v2();
  object_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  object_id.set_storage_object_type((uint64_t)type_);
  object_id.set_incarnation_id(incarnation_id);
  object_id.set_column_group_id(column_group_id);
}


/**
 * ---------------------------------------ObPrivateDataMacroType----------------------------------------
 */

bool ObPrivateDataMacroType::is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:server_id, fourth_id:macro_private_transfer_epoch+tenant_seq
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() > 0) &&
         (file_id.macro_private_transfer_epoch() >= 0) && (file_id.tenant_seq() >= 0);
}

int ObPrivateDataMacroType::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
  const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type:%s (tablet_id=%lu, private_transfer_epoch=%lu)",
             get_type_str(), opt.private_opt_.tablet_id_, opt.private_opt_.tablet_trasfer_seq_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()),
      K(opt.private_opt_.tablet_id_), K(opt.private_opt_.tablet_trasfer_seq_));
  }
  return ret;
}

int ObPrivateDataMacroType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  uint64_t seq = 0;
  const int64_t default_incarnation_id = 0;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_incarnation_id, default_cg_id, object_id);
  object_id.set_second_id(opt.private_opt_.tablet_id_);
  object_id.set_third_id(GCONF.observer_id);
  if (OB_FAIL(TENANT_SEQ_GENERATOR.get_private_object_seq(seq))) {
    LOG_WARN("fail to get private object seq", K(ret), K(opt));
  } else {
    object_id.set_tenant_seq(seq);
    object_id.set_macro_private_transfer_epoch(opt.private_opt_.tablet_trasfer_seq_);
  }
  return ret;
}

/**
 * ---------------------------------------ObPrivateMetaMacroType----------------------------------------
 */

bool ObPrivateMetaMacroType::is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:server_id, fourth_id:macro_private_transfer_epoch+tenant_seq
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() > 0) &&
         (file_id.macro_private_transfer_epoch() >= 0) && (file_id.tenant_seq() >= 0);
}

int ObPrivateMetaMacroType::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
  const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type:%s (tablet_id=%lu, private_transfer_epoch=%lu)", get_type_str(),
            opt.private_opt_.tablet_id_, opt.private_opt_.tablet_trasfer_seq_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()),
      K(opt.private_opt_.tablet_id_), K(opt.private_opt_.tablet_trasfer_seq_));
  }
  return ret;
}

int ObPrivateMetaMacroType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  uint64_t seq = 0;
  const int64_t default_incarnation_id = 0;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_incarnation_id, default_cg_id, object_id);
  object_id.set_second_id(opt.private_opt_.tablet_id_);
  object_id.set_third_id(GCONF.observer_id);
  if (OB_FAIL(TENANT_SEQ_GENERATOR.get_private_object_seq(seq))) {
    LOG_WARN("fail to get private object seq", K(ret), K(opt));
  } else {
    object_id.set_tenant_seq(seq);
    object_id.set_macro_private_transfer_epoch(opt.private_opt_.tablet_trasfer_seq_);
  }
  return ret;
}

/**
 * ---------------------------------------ObSharedMiniDataMacroType----------------------------------------
 */

bool ObSharedMiniDataMacroType::is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:op_id+macro_seq_id
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0);
}

int ObSharedMiniDataMacroType::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
  const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos,
            "object_type=%s (tablet_id=%lu,op_id=%lu,data_seq=%lu,reorganization_scn=%lu)",
            get_type_str(), opt.ss_share_opt_.tablet_id_, (opt.ss_share_opt_.data_seq_ >> 32),
            (opt.ss_share_opt_.data_seq_ & 0xFFFFFFFF), opt.ss_share_opt_.reorganization_scn_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()),
              K(opt.ss_share_opt_.tablet_id_), K(opt.ss_share_opt_.data_seq_), K(opt.ss_share_opt_.column_group_id_),
              K(opt.ss_share_opt_.reorganization_scn_));
  }
  return ret;
}

int ObSharedMiniDataMacroType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  set_ss_object_first_id_(default_incarnation_id, opt.ss_share_opt_.column_group_id_, object_id);
  object_id.set_second_id(opt.ss_share_opt_.tablet_id_);
  object_id.set_third_id(opt.ss_share_opt_.data_seq_);
  object_id.set_ss_fourth_id(opt.ss_share_opt_.is_ls_inner_tablet_,
                             opt.ss_share_opt_.ls_id_, opt.ss_share_opt_.reorganization_scn_);
  return ret;
}

/**
 * ---------------------------------------ObSharedMiniMetaMacroType----------------------------------------
 */

bool ObSharedMiniMetaMacroType::is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:op_id + seq_id
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0);
}

int ObSharedMiniMetaMacroType::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
  const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos,
            "object_type=%s (tablet_id=%lu,op_id=%lu,data_seq=%lu,reorganization_scn=%lu)",
            get_type_str(), opt.ss_share_opt_.tablet_id_, (opt.ss_share_opt_.data_seq_ >> 32),
            (opt.ss_share_opt_.data_seq_ & 0xFFFFFFFF), opt.ss_share_opt_.reorganization_scn_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()),
              K(opt.ss_share_opt_.tablet_id_), K(opt.ss_share_opt_.data_seq_), K(opt.ss_share_opt_.column_group_id_),
              K(opt.ss_share_opt_.reorganization_scn_));
  }
  return ret;
}

int ObSharedMiniMetaMacroType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  set_ss_object_first_id_(default_incarnation_id, opt.ss_share_opt_.column_group_id_, object_id);
  object_id.set_second_id(opt.ss_share_opt_.tablet_id_);
  object_id.set_third_id(opt.ss_share_opt_.data_seq_);
  object_id.set_ss_fourth_id(opt.ss_share_opt_.is_ls_inner_tablet_,
                             opt.ss_share_opt_.ls_id_, opt.ss_share_opt_.reorganization_scn_);
  return ret;
}

/**
 * ---------------------------------------ObSharedMinorDataMacroType----------------------------------------
 */

bool ObSharedMinorDataMacroType::is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:op_id + seq_id
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0);
}

int ObSharedMinorDataMacroType::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
  const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos,
            "object_type=%s (tablet_id=%lu,op_id=%lu,data_seq=%lu,reorganization_scn=%lu)",
            get_type_str(), opt.ss_share_opt_.tablet_id_, (opt.ss_share_opt_.data_seq_ >> 32),
            (opt.ss_share_opt_.data_seq_ & 0xFFFFFFFF), opt.ss_share_opt_.reorganization_scn_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()),
              K(opt.ss_share_opt_.tablet_id_), K(opt.ss_share_opt_.data_seq_), K(opt.ss_share_opt_.column_group_id_),
              K(opt.ss_share_opt_.reorganization_scn_));
  }
  return ret;
}

int ObSharedMinorDataMacroType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  set_ss_object_first_id_(default_incarnation_id, opt.ss_share_opt_.column_group_id_, object_id);
  object_id.set_second_id(opt.ss_share_opt_.tablet_id_);
  object_id.set_third_id(opt.ss_share_opt_.data_seq_);
  object_id.set_ss_fourth_id(opt.ss_share_opt_.is_ls_inner_tablet_,
                             opt.ss_share_opt_.ls_id_, opt.ss_share_opt_.reorganization_scn_);
  return ret;
}

/**
 * ---------------------------------------ObSharedMinorMetaMacroType----------------------------------------
 */

bool ObSharedMinorMetaMacroType::is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:op_id + seq_id
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0);
}

int ObSharedMinorMetaMacroType::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
  const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos,
            "object_type=%s (tablet_id=%lu,op_id=%lu,data_seq=%lu,reorganization_scn=%lu)",
            get_type_str(), opt.ss_share_opt_.tablet_id_, (opt.ss_share_opt_.data_seq_ >> 32),
            (opt.ss_share_opt_.data_seq_ & 0xFFFFFFFF), opt.ss_share_opt_.reorganization_scn_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()),
              K(opt.ss_share_opt_.tablet_id_), K(opt.ss_share_opt_.data_seq_), K(opt.ss_share_opt_.column_group_id_),
              K(opt.ss_share_opt_.reorganization_scn_));
  }
  return ret;
}

int ObSharedMinorMetaMacroType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  set_ss_object_first_id_(default_incarnation_id, opt.ss_share_opt_.column_group_id_, object_id);
  object_id.set_second_id(opt.ss_share_opt_.tablet_id_);
  object_id.set_third_id(opt.ss_share_opt_.data_seq_);
  object_id.set_ss_fourth_id(opt.ss_share_opt_.is_ls_inner_tablet_,
                             opt.ss_share_opt_.ls_id_, opt.ss_share_opt_.reorganization_scn_);
  return ret;
}

/**
 * ---------------------------------------ObSharedMajorDataMacroType----------------------------------------
 */

bool ObSharedMajorDataMacroType::is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:macro_seq_id
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0);
}

int ObSharedMajorDataMacroType::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
  const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s (tablet_id=%lu,data_seq=%lu,cg_id=%lu)",
            get_type_str(), opt.ss_share_opt_.tablet_id_, opt.ss_share_opt_.data_seq_,
            opt.ss_share_opt_.column_group_id_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()),
              K(opt.ss_share_opt_.tablet_id_), K(opt.ss_share_opt_.data_seq_), K(opt.ss_share_opt_.column_group_id_));
  }
  return ret;
}

int ObSharedMajorDataMacroType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  set_ss_object_first_id_(default_incarnation_id, opt.ss_share_opt_.column_group_id_, object_id);
  object_id.set_second_id(opt.ss_share_opt_.tablet_id_);
  object_id.set_third_id(opt.ss_share_opt_.data_seq_);
  object_id.set_ss_fourth_id(opt.ss_share_opt_.is_ls_inner_tablet_,
                             opt.ss_share_opt_.ls_id_, opt.ss_share_opt_.reorganization_scn_);
  return ret;
}

/**
 * ---------------------------------------ObSharedMajorMetaMacroType----------------------------------------
 */

bool ObSharedMajorMetaMacroType::is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:seq_id
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0);
}

int ObSharedMajorMetaMacroType::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
  const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s (tablet_id=%lu,data_seq=%lu,cg_id=%lu)",
            get_type_str(), opt.ss_share_opt_.tablet_id_, opt.ss_share_opt_.data_seq_,
            opt.ss_share_opt_.column_group_id_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()),
              K(opt.ss_share_opt_.tablet_id_), K(opt.ss_share_opt_.data_seq_), K(opt.ss_share_opt_.column_group_id_));
  }
  return ret;
}

int ObSharedMajorMetaMacroType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  set_ss_object_first_id_(default_incarnation_id, opt.ss_share_opt_.column_group_id_, object_id);
  object_id.set_second_id(opt.ss_share_opt_.tablet_id_);
  object_id.set_third_id(opt.ss_share_opt_.data_seq_);
  object_id.set_ss_fourth_id(opt.ss_share_opt_.is_ls_inner_tablet_,
                             opt.ss_share_opt_.ls_id_, opt.ss_share_opt_.reorganization_scn_);
  return ret;
}

/**
 * ---------------------------------------ObTmpFileType----------------------------------------
 */

bool ObTmpFileType::is_valid(const MacroBlockId &file_id) const
{
  // second_id:tmp_file_id, third_id:segment_id
  return (file_id.second_id() >= 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0);
}

int ObTmpFileType::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
  const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s", get_type_str()))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()));
  }
  return ret;
}

int ObTmpFileType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  uint64_t file_id = 0;
  const int64_t default_incarnation_id = 0;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_incarnation_id, default_cg_id, object_id);
  if (OB_FAIL(TENANT_SEQ_GENERATOR.get_tmp_file_seq(file_id))) {
    LOG_WARN("fail to get private tmp file seq", K(ret), K(opt));
  } else {
    object_id.set_second_id(file_id);
  }
  return ret;
}

/**
 * ---------------------------------------ObServerMetaType----------------------------------------
 */

bool ObServerMetaType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}

int ObServerMetaType::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
  const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s", get_type_str()))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()));
  }
  return ret;
}

int ObServerMetaType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  const int64_t default_incarnation_id = 0;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_incarnation_id, default_cg_id, object_id);
  return OB_SUCCESS;
}

/**
 * ---------------------------------------ObPrivateTabletMetaType----------------------------------------
 */

bool ObPrivateTabletMetaType::is_valid(const MacroBlockId &file_id) const
{
  // second_id:ls_id, third_id:tablet_id, fourth_id:meta_private_transfer_epoch+meta_version_id
  return (file_id.second_id() >= 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() > 0) &&
         (file_id.meta_private_transfer_epoch() >= 0) && (file_id.meta_version_id() >= 0);
}

int ObPrivateTabletMetaType::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
  const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s (ls_id=%lu,tablet_id=%lu,version=%lu,private_transfer_epoch=%lu)",
            get_type_str(), opt.ss_private_tablet_opt_.ls_id_, opt.ss_private_tablet_opt_.tablet_id_,
            opt.ss_private_tablet_opt_.version_, opt.ss_private_tablet_opt_.tablet_private_transfer_epoch_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()),
            K(opt.ss_private_tablet_opt_.ls_id_), K(opt.ss_private_tablet_opt_.tablet_id_),
            K(opt.ss_private_tablet_opt_.version_), K(opt.ss_private_tablet_opt_.tablet_private_transfer_epoch_));
  }
  return ret;
}

int ObPrivateTabletMetaType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_incarnation_id, default_cg_id, object_id);
  object_id.set_second_id(opt.ss_private_tablet_opt_.ls_id_);
  object_id.set_third_id(opt.ss_private_tablet_opt_.tablet_id_);
  object_id.set_meta_version_id(opt.ss_private_tablet_opt_.version_);
  object_id.set_meta_private_transfer_epoch(opt.ss_private_tablet_opt_.tablet_private_transfer_epoch_);
  return ret;
}

/**
 * ---------------------------------------ObPrivateSlogFileType----------------------------------------
 */

bool ObPrivateSlogFileType::is_valid(const MacroBlockId &file_id) const
{
  // second_id:tenant_id, third_id:tenant_epoch_id, fourth_id:file_id
  return (file_id.fourth_id() >= 0) && (file_id.fourth_id() < INT64_MAX) && (file_id.third_id() >= 0);
}

int ObPrivateSlogFileType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_incarnation_id, default_cg_id, object_id);
  object_id.set_second_id(opt.ss_slog_ckpt_obj_opt_.tenant_id_);
  object_id.set_third_id(opt.ss_slog_ckpt_obj_opt_.tenant_epoch_id_);
  object_id.set_fourth_id(opt.ss_slog_ckpt_obj_opt_.file_id_);
  return ret;
}

/**
 * ---------------------------------------ObPrivateCkptFileType----------------------------------------
 */

bool ObPrivateCkptFileType::is_valid(const MacroBlockId &file_id) const
{
  // second_id:tenant_id, third_id:tenant_epoch_id, fourth_id:file_id
  return (file_id.fourth_id() >= 0) && (file_id.fourth_id() < INT64_MAX) && (file_id.third_id() >= 0);
}

int ObPrivateCkptFileType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_incarnation_id, default_cg_id, object_id);
  object_id.set_second_id(opt.ss_slog_ckpt_obj_opt_.tenant_id_);
  object_id.set_third_id(opt.ss_slog_ckpt_obj_opt_.tenant_epoch_id_);
  object_id.set_fourth_id(opt.ss_slog_ckpt_obj_opt_.file_id_);
  return ret;
}

/**
 * ---------------------------------------ObMajorPrewarmDataType----------------------------------------
 */

bool ObMajorPrewarmDataType::is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:compaction_scn, fourth_id:reorganization_scn
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0) &&
         (file_id.fourth_id() >= 0);
}

int ObMajorPrewarmDataType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_incarnation_id, default_cg_id, object_id);
  object_id.set_second_id(opt.ss_major_prewarm_opt_.tablet_id_);
  object_id.set_third_id(opt.ss_major_prewarm_opt_.compaction_scn_);
  object_id.set_fourth_id(opt.ss_major_prewarm_opt_.reorganization_scn_);
  return ret;
}

/**
 * ---------------------------------------ObMajorPrewarmDataIndexType----------------------------------------
 */

bool ObMajorPrewarmDataIndexType::is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:compaction_scn, fourth_id:reorganization_scn
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0) &&
         (file_id.fourth_id() >= 0);
}

int ObMajorPrewarmDataIndexType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_incarnation_id, default_cg_id, object_id);
  object_id.set_second_id(opt.ss_major_prewarm_opt_.tablet_id_);
  object_id.set_third_id(opt.ss_major_prewarm_opt_.compaction_scn_);
  object_id.set_fourth_id(opt.ss_major_prewarm_opt_.reorganization_scn_);
  return ret;
}

/**
 * ---------------------------------------ObMajorPrewarmMetaType----------------------------------------
 */

bool ObMajorPrewarmMetaType::is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:compaction_scn, fourth_id:reorganization_scn
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0) &&
         (file_id.fourth_id() >= 0);
}

int ObMajorPrewarmMetaType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_incarnation_id, default_cg_id, object_id);
  object_id.set_second_id(opt.ss_major_prewarm_opt_.tablet_id_);
  object_id.set_third_id(opt.ss_major_prewarm_opt_.compaction_scn_);
  object_id.set_fourth_id(opt.ss_major_prewarm_opt_.reorganization_scn_);
  return ret;
}

/**
 * ---------------------------------------ObMajorPrewarmMetaIndexType----------------------------------------
 */

bool ObMajorPrewarmMetaIndexType::is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:compaction_scn, fourth_id:reorganization_scn
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0) &&
         (file_id.fourth_id() >= 0);
}

int ObMajorPrewarmMetaIndexType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_incarnation_id, default_cg_id, object_id);
  object_id.set_second_id(opt.ss_major_prewarm_opt_.tablet_id_);
  object_id.set_third_id(opt.ss_major_prewarm_opt_.compaction_scn_);
  object_id.set_fourth_id(opt.ss_major_prewarm_opt_.reorganization_scn_);
  return ret;
}

/**
 * ---------------------------------------ObTenantDiskSpaceMetaType----------------------------------------
 */

bool ObTenantDiskSpaceMetaType::is_valid(const MacroBlockId &file_id) const
{
  // second_id:tenant_id, third_id:tenant_epoch_id
  return (is_valid_tenant_id(file_id.second_id())) && (file_id.third_id() >= 0);
}

int ObTenantDiskSpaceMetaType::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
  const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s (tenant_id=%lu,tenant_epoch_id=%lu)",
               get_type_str(), opt.ss_tenant_level_opt_.tenant_id_, opt.ss_tenant_level_opt_.tenant_epoch_id_))) {
      LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()),
                K(opt.ss_tenant_level_opt_.tenant_id_), K(opt.ss_tenant_level_opt_.tenant_epoch_id_));
    }
  return ret;
}

int ObTenantDiskSpaceMetaType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_incarnation_id, default_cg_id, object_id);
  object_id.set_second_id(opt.ss_tenant_level_opt_.tenant_id_);
  object_id.set_third_id(opt.ss_tenant_level_opt_.tenant_epoch_id_);
  return ret;
}

/**
 * ---------------------------------------ObIsSharedTenantDeletedType----------------------------------------
 */

bool ObIsSharedTenantDeletedType::is_valid(const MacroBlockId &file_id) const
{
  // second_id:tenant_id
  return is_valid_tenant_id(file_id.second_id());
}

int ObIsSharedTenantDeletedType::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
  const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s (tenant_id=%lu)",
             get_type_str(), opt.ss_shared_tenant_id_opt_.tenant_id_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()),
              K(opt.ss_shared_tenant_id_opt_.tenant_id_));
  }
  return ret;
}

int ObIsSharedTenantDeletedType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_incarnation_id, default_cg_id, object_id);
  object_id.set_second_id(opt.ss_shared_tenant_id_opt_.tenant_id_);
  return ret;
}

/**
 * ---------------------------------------ObSharedMicroDataMacroType----------------------------------------
 */

/**
 * ---------------------------------------ObSharedMicroMetaMacroType----------------------------------------
 */

/**
 * ---------------------------------------ObUnsealedRemoteSegFileType----------------------------------------
 */

bool ObUnsealedRemoteSegFileType::is_valid(const MacroBlockId &file_id) const
{
  // second_id:tmp_file_id, third_id:segment_id, fourth_id:valid_length
  return (file_id.second_id() >= 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0) &&
         (file_id.fourth_id() > 0);
}

/**
 * ---------------------------------------ObSharedMdsMiniDataMacroType----------------------------------------
 */

bool ObSharedMdsMiniDataMacroType::is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:op_id + seq_id
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0);
}

int ObSharedMdsMiniDataMacroType::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
  const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos,
            "object_type=%s (tablet_id=%lu,op_id=%lu,data_seq=%lu,reorganization_scn=%lu)",
            get_type_str(), opt.ss_share_opt_.tablet_id_, (opt.ss_share_opt_.data_seq_ >> 32),
            (opt.ss_share_opt_.data_seq_ & 0xFFFFFFFF), opt.ss_share_opt_.reorganization_scn_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()),
              K(opt.ss_share_opt_.tablet_id_), K(opt.ss_share_opt_.data_seq_), K(opt.ss_share_opt_.column_group_id_),
              K(opt.ss_share_opt_.reorganization_scn_));
  }
  return ret;
}

int ObSharedMdsMiniDataMacroType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  set_ss_object_first_id_(default_incarnation_id, opt.ss_share_opt_.column_group_id_, object_id);
  object_id.set_second_id(opt.ss_share_opt_.tablet_id_);
  object_id.set_third_id(opt.ss_share_opt_.data_seq_);
  object_id.set_ss_fourth_id(opt.ss_share_opt_.is_ls_inner_tablet_,
                             opt.ss_share_opt_.ls_id_, opt.ss_share_opt_.reorganization_scn_);
  return ret;
}

/**
 * ---------------------------------------ObSharedMdsMiniMetaMacroType----------------------------------------
 */

bool ObSharedMdsMiniMetaMacroType::is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:op_id + seq_id
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0);
}

int ObSharedMdsMiniMetaMacroType::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
  const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos,
            "object_type=%s (tablet_id=%lu,op_id=%lu,data_seq=%lu,reorganization_scn=%lu)",
            get_type_str(), opt.ss_share_opt_.tablet_id_, (opt.ss_share_opt_.data_seq_ >> 32),
            (opt.ss_share_opt_.data_seq_ & 0xFFFFFFFF), opt.ss_share_opt_.reorganization_scn_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()),
              K(opt.ss_share_opt_.tablet_id_), K(opt.ss_share_opt_.data_seq_), K(opt.ss_share_opt_.column_group_id_),
              K(opt.ss_share_opt_.reorganization_scn_));
  }
  return ret;
}

int ObSharedMdsMiniMetaMacroType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  set_ss_object_first_id_(default_incarnation_id, opt.ss_share_opt_.column_group_id_, object_id);
  object_id.set_second_id(opt.ss_share_opt_.tablet_id_);
  object_id.set_third_id(opt.ss_share_opt_.data_seq_);
  object_id.set_ss_fourth_id(opt.ss_share_opt_.is_ls_inner_tablet_,
                             opt.ss_share_opt_.ls_id_, opt.ss_share_opt_.reorganization_scn_);
  return ret;
}

/**
 * ---------------------------------------ObSharedMdsMinorDataMacroType----------------------------------------
 */

bool ObSharedMdsMinorDataMacroType::is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:op_id + seq_id
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0);
}

int ObSharedMdsMinorDataMacroType::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
  const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos,
            "object_type=%s (tablet_id=%lu,op_id=%lu,data_seq=%lu,reorganization_scn=%lu)",
            get_type_str(), opt.ss_share_opt_.tablet_id_, (opt.ss_share_opt_.data_seq_ >> 32),
            (opt.ss_share_opt_.data_seq_ & 0xFFFFFFFF), opt.ss_share_opt_.reorganization_scn_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()),
              K(opt.ss_share_opt_.tablet_id_), K(opt.ss_share_opt_.data_seq_), K(opt.ss_share_opt_.column_group_id_),
              K(opt.ss_share_opt_.reorganization_scn_));
  }
  return ret;
}

int ObSharedMdsMinorDataMacroType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  set_ss_object_first_id_(default_incarnation_id, opt.ss_share_opt_.column_group_id_, object_id);
  object_id.set_second_id(opt.ss_share_opt_.tablet_id_);
  object_id.set_third_id(opt.ss_share_opt_.data_seq_);
  object_id.set_ss_fourth_id(opt.ss_share_opt_.is_ls_inner_tablet_,
                             opt.ss_share_opt_.ls_id_, opt.ss_share_opt_.reorganization_scn_);
  return ret;
}

/**
 * ---------------------------------------ObSharedMdsMinorMetaMacroType----------------------------------------
 */

bool ObSharedMdsMinorMetaMacroType::is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:op_id + seq_id
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0);
}

int ObSharedMdsMinorMetaMacroType::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
  const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos,
            "object_type=%s (tablet_id=%lu,op_id=%lu,data_seq=%lu,reorganization_scn=%lu)",
            get_type_str(), opt.ss_share_opt_.tablet_id_, (opt.ss_share_opt_.data_seq_ >> 32),
            (opt.ss_share_opt_.data_seq_ & 0xFFFFFFFF), opt.ss_share_opt_.reorganization_scn_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()),
              K(opt.ss_share_opt_.tablet_id_), K(opt.ss_share_opt_.data_seq_), K(opt.ss_share_opt_.column_group_id_),
              K(opt.ss_share_opt_.reorganization_scn_));
  }
  return ret;
}

int ObSharedMdsMinorMetaMacroType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  set_ss_object_first_id_(default_incarnation_id, opt.ss_share_opt_.column_group_id_, object_id);
  object_id.set_second_id(opt.ss_share_opt_.tablet_id_);
  object_id.set_third_id(opt.ss_share_opt_.data_seq_);
  object_id.set_ss_fourth_id(opt.ss_share_opt_.is_ls_inner_tablet_,
                             opt.ss_share_opt_.ls_id_, opt.ss_share_opt_.reorganization_scn_);
  return ret;
}

/**
 * ---------------------------------------ObSharedTabletMetaType----------------------------------------
 */

bool ObSharedTabletMetaType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}

int ObSharedTabletMetaType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_incarnation_id, default_cg_id, object_id);
  object_id.set_second_id(opt.ss_tablet_meta_opt_.tablet_id_);
  object_id.set_third_id(opt.ss_tablet_meta_opt_.op_id_);
  object_id.set_ss_fourth_id(opt.ss_tablet_meta_opt_.is_inner_tablet_,
  opt.ss_tablet_meta_opt_.ls_id_, opt.ss_tablet_meta_opt_.reorganization_scn_);
  return ret;
}

/**
 * ---------------------------------------ObSharedTabletSubMetaType----------------------------------------
 */

bool ObSharedTabletSubMetaType::is_valid(const MacroBlockId &file_id) const
{
  // user_tablet: second_id:tablet_id, third_id:op_id, fourth_id:N/A inner_tablet: second_id:tablet_id, third_id:op_id, fourth_id:ls_id
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0) &&
         (file_id.third_id() < INT64_MAX) && ((file_id.meta_is_inner_tablet() == true && (file_id.meta_ls_id() > 0 &&
         file_id.meta_ls_id() < INT64_MAX)) || (file_id.meta_is_inner_tablet() == false));
}

int ObSharedTabletSubMetaType::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
  const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s (tablet_id=%lu, ls_id=%lu, op_id=%u, data_seq=%u,"
             "is_inner_tablet=%d, reorganization_scn=%lu)",
             get_type_str(), opt.ss_tablet_sub_meta_opt_.tablet_id_,
             opt.ss_tablet_sub_meta_opt_.ls_id_, opt.ss_tablet_sub_meta_opt_.op_id_,
             opt.ss_tablet_sub_meta_opt_.data_seq_ , opt.ss_tablet_sub_meta_opt_.is_inner_tablet_,
             opt.ss_tablet_sub_meta_opt_.reorganization_scn_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()),
              K(opt.ss_tablet_sub_meta_opt_.tablet_id_),
              K(opt.ss_tablet_sub_meta_opt_.ls_id_),
              K(opt.ss_tablet_sub_meta_opt_.is_inner_tablet_),
              K(opt.ss_tablet_sub_meta_opt_.op_id_),
              K(opt.ss_tablet_sub_meta_opt_.data_seq_),
              K(opt.ss_tablet_sub_meta_opt_.reorganization_scn_));
  }
  return ret;
}

int ObSharedTabletSubMetaType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  const int64_t default_incarnation_id = 0;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_incarnation_id, default_cg_id, object_id);
  object_id.set_second_id(opt.ss_tablet_sub_meta_opt_.tablet_id_);
  object_id.set_third_id((uint64_t(opt.ss_tablet_sub_meta_opt_.op_id_) << 32) | (opt.ss_tablet_sub_meta_opt_.data_seq_ & 0xFFFFFFFF));
  object_id.set_ss_fourth_id(opt.ss_tablet_sub_meta_opt_.is_inner_tablet_,
  opt.ss_tablet_sub_meta_opt_.ls_id_, opt.ss_tablet_sub_meta_opt_.reorganization_scn_);
  return OB_SUCCESS;
}

/**
 * ---------------------------------------ObTenantRootKeyType----------------------------------------
 */

bool ObTenantRootKeyType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}

int ObTenantRootKeyType::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
  const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s", get_type_str()))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()));
  }
  return ret;
}

int ObTenantRootKeyType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  const int64_t default_incarnation_id = 0;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_incarnation_id, default_cg_id, object_id);
  return OB_SUCCESS;
}

/**
 * ---------------------------------------ObExternalTableFileType----------------------------------------
 */

bool ObExternalTableFileType::is_valid(const MacroBlockId &file_id) const
{
  // second_id:server-level seq id, third_id:offset / 2MB
  return (file_id.second_id() < UINT64_MAX) && (file_id.third_id() >= 0) && (file_id.third_id() < INT64_MAX);
}

int ObExternalTableFileType::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
  const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s (server_seq_id=%lu, offset_idx=%ld)",
              get_type_str(), opt.ss_external_table_file_opt_.server_seq_id_,
              opt.ss_external_table_file_opt_.offset_idx_))) {
    LOG_WARN("failed to print data into buf", KR(ret), K(buf_len), K(pos),
              K(get_type_str()),
              K(opt.ss_external_table_file_opt_.server_seq_id_),
              K(opt.ss_external_table_file_opt_.offset_idx_));
  }
  return ret;
}

int ObExternalTableFileType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  const int64_t default_incarnation_id = 0;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_incarnation_id, default_cg_id, object_id);
  object_id.set_second_id(opt.ss_external_table_file_opt_.server_seq_id_);
  object_id.set_third_id(opt.ss_external_table_file_opt_.offset_idx_);
  return OB_SUCCESS;
}

/**
 * ---------------------------------------ObMacroCacheCkptDataType----------------------------------------
 */

bool ObMacroCacheCkptDataType::is_valid(const MacroBlockId &file_id) const
{
  // second_id: version id, third_id: tenant-level seq id
  return (file_id.second_id() < UINT64_MAX) && (file_id.third_id() < UINT64_MAX);
}

int ObMacroCacheCkptDataType::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
  const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s (version_id_=%lu, seq_id=%lu)", get_type_str(),
              opt.ss_macro_cache_ckpt_opt_.version_id_, opt.ss_macro_cache_ckpt_opt_.seq_id_))) {
    LOG_WARN("failed to print data into buf", KR(ret), K(buf_len), K(pos), K(get_type_str()),
              K(opt.ss_macro_cache_ckpt_opt_.version_id_), K(opt.ss_macro_cache_ckpt_opt_.seq_id_));
  }
  return ret;
}

int ObMacroCacheCkptDataType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  const int64_t default_incarnation_id = 0;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_incarnation_id, default_cg_id, object_id);
  object_id.set_second_id(opt.ss_macro_cache_ckpt_opt_.version_id_);
  object_id.set_third_id(opt.ss_macro_cache_ckpt_opt_.seq_id_);
  return OB_SUCCESS;
}

/**
 * ---------------------------------------ObMacroCacheCkptMetaType----------------------------------------
 */

bool ObMacroCacheCkptMetaType::is_valid(const MacroBlockId &file_id) const
{
  // second_id: version id
  return (file_id.second_id() < UINT64_MAX);
}

int ObMacroCacheCkptMetaType::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
  const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s (version_id_=%lu)", get_type_str(),
              opt.ss_macro_cache_ckpt_opt_.version_id_))) {
    LOG_WARN("failed to print data into buf", KR(ret), K(buf_len), K(pos), K(get_type_str()),
              K(opt.ss_macro_cache_ckpt_opt_.version_id_));
  }
  return ret;
}

int ObMacroCacheCkptMetaType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  const int64_t default_incarnation_id = 0;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_incarnation_id, default_cg_id, object_id);
  object_id.set_second_id(opt.ss_macro_cache_ckpt_opt_.version_id_);
  return OB_SUCCESS;
}

/**
 * ---------------------------------------ObSharedIncMajorDataMacroType----------------------------------------
 */

bool ObSharedIncMajorDataMacroType::is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:macro_seq_id
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0);
}

int ObSharedIncMajorDataMacroType::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
  const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s (tablet_id=%lu,data_seq=%lu,cg_id=%lu)",
            get_type_str(), opt.ss_share_opt_.tablet_id_, opt.ss_share_opt_.data_seq_,
            opt.ss_share_opt_.column_group_id_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()),
              K(opt.ss_share_opt_.tablet_id_), K(opt.ss_share_opt_.data_seq_), K(opt.ss_share_opt_.column_group_id_));
  }
  return ret;
}

int ObSharedIncMajorDataMacroType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  set_ss_object_first_id_(default_incarnation_id, opt.ss_share_opt_.column_group_id_, object_id);
  object_id.set_second_id(opt.ss_share_opt_.tablet_id_);
  object_id.set_third_id(opt.ss_share_opt_.data_seq_);
  object_id.set_ss_fourth_id(opt.ss_share_opt_.is_ls_inner_tablet_,
                             opt.ss_share_opt_.ls_id_, opt.ss_share_opt_.reorganization_scn_);
  return ret;
}

/**
 * ---------------------------------------ObSharedIncMajorMetaMacroType----------------------------------------
 */

bool ObSharedIncMajorMetaMacroType::is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:seq_id
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0);
}

int ObSharedIncMajorMetaMacroType::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
  const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s (tablet_id=%lu,data_seq=%lu,cg_id=%lu)",
            get_type_str(), opt.ss_share_opt_.tablet_id_, opt.ss_share_opt_.data_seq_,
            opt.ss_share_opt_.column_group_id_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()),
              K(opt.ss_share_opt_.tablet_id_), K(opt.ss_share_opt_.data_seq_), K(opt.ss_share_opt_.column_group_id_));
  }
  return ret;
}

int ObSharedIncMajorMetaMacroType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  set_ss_object_first_id_(default_incarnation_id, opt.ss_share_opt_.column_group_id_, object_id);
  object_id.set_second_id(opt.ss_share_opt_.tablet_id_);
  object_id.set_third_id(opt.ss_share_opt_.data_seq_);
  object_id.set_ss_fourth_id(opt.ss_share_opt_.is_ls_inner_tablet_,
                             opt.ss_share_opt_.ls_id_, opt.ss_share_opt_.reorganization_scn_);
  return ret;
}

/**
 * ---------------------------------------ObSharedTabletSubMetaInTableType----------------------------------------
 */

bool ObSharedTabletSubMetaInTableType::is_valid(const MacroBlockId &file_id) const
{
  // user_tablet: second_id:tablet_id, third_id:op_id, fourth_id:N/A inner_tablet: second_id:tablet_id, third_id:op_id, fourth_id:ls_id
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0) &&
         (file_id.third_id() < INT64_MAX) && ((file_id.meta_is_inner_tablet() == true && (file_id.meta_ls_id() > 0 &&
         file_id.meta_ls_id() < INT64_MAX)) || (file_id.meta_is_inner_tablet() == false));
}

int ObSharedTabletSubMetaInTableType::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
  const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s (tablet_id=%lu, ls_id=%lu, op_id=%u, data_seq=%u,"
             "is_inner_tablet=%d, reorganization_scn=%lu)",
             get_type_str(), opt.ss_tablet_sub_meta_opt_.tablet_id_,
             opt.ss_tablet_sub_meta_opt_.ls_id_, opt.ss_tablet_sub_meta_opt_.op_id_,
             opt.ss_tablet_sub_meta_opt_.data_seq_ , opt.ss_tablet_sub_meta_opt_.is_inner_tablet_,
             opt.ss_tablet_sub_meta_opt_.reorganization_scn_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()),
              K(opt.ss_tablet_sub_meta_opt_.tablet_id_),
              K(opt.ss_tablet_sub_meta_opt_.ls_id_),
              K(opt.ss_tablet_sub_meta_opt_.is_inner_tablet_),
              K(opt.ss_tablet_sub_meta_opt_.op_id_),
              K(opt.ss_tablet_sub_meta_opt_.data_seq_),
              K(opt.ss_tablet_sub_meta_opt_.reorganization_scn_));
  }
  return ret;
}

int ObSharedTabletSubMetaInTableType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  const int64_t default_incarnation_id = 0;
  const int64_t default_cg_id = 0;
  set_ss_object_first_id_(default_incarnation_id, default_cg_id, object_id);
  object_id.set_second_id(opt.ss_tablet_sub_meta_opt_.tablet_id_);
  object_id.set_third_id((uint64_t(opt.ss_tablet_sub_meta_opt_.op_id_) << 32) | (opt.ss_tablet_sub_meta_opt_.data_seq_ & 0xFFFFFFFF));
  object_id.set_ss_fourth_id(opt.ss_tablet_sub_meta_opt_.is_inner_tablet_,
  opt.ss_tablet_sub_meta_opt_.ls_id_, opt.ss_tablet_sub_meta_opt_.reorganization_scn_);
  return OB_SUCCESS;
}

/**
 * ---------------------------------------ObSharedMiniV2DataMacroType----------------------------------------
 */

bool ObSharedMiniV2DataMacroType::is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:op_id+macro_seq_id
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0);
}

int ObSharedMiniV2DataMacroType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  set_ss_object_first_id_(default_incarnation_id, opt.ss_share_opt_.column_group_id_, object_id);
  object_id.set_second_id(opt.ss_share_opt_.tablet_id_);
  object_id.set_third_id(opt.ss_share_opt_.data_seq_);
  object_id.set_ss_fourth_id(!false && opt.ss_share_opt_.is_ls_inner_tablet_,
                             opt.ss_share_opt_.ls_id_, opt.ss_share_opt_.reorganization_scn_);
  return ret;
}

/**
 * ---------------------------------------ObSharedMiniV2MetaMacroType----------------------------------------
 */

bool ObSharedMiniV2MetaMacroType::is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:op_id+macro_seq_id
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0);
}

int ObSharedMiniV2MetaMacroType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  set_ss_object_first_id_(default_incarnation_id, opt.ss_share_opt_.column_group_id_, object_id);
  object_id.set_second_id(opt.ss_share_opt_.tablet_id_);
  object_id.set_third_id(opt.ss_share_opt_.data_seq_);
  object_id.set_ss_fourth_id(!false && opt.ss_share_opt_.is_ls_inner_tablet_,
                             opt.ss_share_opt_.ls_id_, opt.ss_share_opt_.reorganization_scn_);
  return ret;
}

/**
 * ---------------------------------------ObSharedMinorV2DataMacroType----------------------------------------
 */

bool ObSharedMinorV2DataMacroType::is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:op_id+macro_seq_id
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0);
}

int ObSharedMinorV2DataMacroType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  set_ss_object_first_id_(default_incarnation_id, opt.ss_share_opt_.column_group_id_, object_id);
  object_id.set_second_id(opt.ss_share_opt_.tablet_id_);
  object_id.set_third_id(opt.ss_share_opt_.data_seq_);
  object_id.set_ss_fourth_id(!false && opt.ss_share_opt_.is_ls_inner_tablet_,
                             opt.ss_share_opt_.ls_id_, opt.ss_share_opt_.reorganization_scn_);
  return ret;
}

/**
 * ---------------------------------------ObSharedMinorV2MetaMacroType----------------------------------------
 */

bool ObSharedMinorV2MetaMacroType::is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:op_id+macro_seq_id
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0);
}

int ObSharedMinorV2MetaMacroType::get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  set_ss_object_first_id_(default_incarnation_id, opt.ss_share_opt_.column_group_id_, object_id);
  object_id.set_second_id(opt.ss_share_opt_.tablet_id_);
  object_id.set_third_id(opt.ss_share_opt_.data_seq_);
  object_id.set_ss_fourth_id(!false && opt.ss_share_opt_.is_ls_inner_tablet_,
                             opt.ss_share_opt_.ls_id_, opt.ss_share_opt_.reorganization_scn_);
  return ret;
}

/**
 * ---------------------------------------ObSharedTabletMacroDiffType----------------------------------------
 */

bool ObSharedTabletMacroDiffType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}


const ObStorageObjectTypeBase &ObStorageObjectTypeInstance::get_instance(ObStorageObjectType type)
{
  switch (type) {
    case ObStorageObjectType::PRIVATE_DATA_MACRO: {
      static const ObPrivateDataMacroType instance;
      return instance;
    }
    case ObStorageObjectType::PRIVATE_META_MACRO: {
      static const ObPrivateMetaMacroType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MINI_DATA_MACRO: {
      static const ObSharedMiniDataMacroType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MINI_META_MACRO: {
      static const ObSharedMiniMetaMacroType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MINOR_DATA_MACRO: {
      static const ObSharedMinorDataMacroType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MINOR_META_MACRO: {
      static const ObSharedMinorMetaMacroType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MAJOR_DATA_MACRO: {
      static const ObSharedMajorDataMacroType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MAJOR_META_MACRO: {
      static const ObSharedMajorMetaMacroType instance;
      return instance;
    }
    case ObStorageObjectType::TMP_FILE: {
      static const ObTmpFileType instance;
      return instance;
    }
    case ObStorageObjectType::SERVER_META: {
      static const ObServerMetaType instance;
      return instance;
    }
    case ObStorageObjectType::PRIVATE_TABLET_META: {
      static const ObPrivateTabletMetaType instance;
      return instance;
    }
    case ObStorageObjectType::PRIVATE_SLOG_FILE: {
      static const ObPrivateSlogFileType instance;
      return instance;
    }
    case ObStorageObjectType::PRIVATE_CKPT_FILE: {
      static const ObPrivateCkptFileType instance;
      return instance;
    }
    case ObStorageObjectType::MAJOR_PREWARM_DATA: {
      static const ObMajorPrewarmDataType instance;
      return instance;
    }
    case ObStorageObjectType::MAJOR_PREWARM_DATA_INDEX: {
      static const ObMajorPrewarmDataIndexType instance;
      return instance;
    }
    case ObStorageObjectType::MAJOR_PREWARM_META: {
      static const ObMajorPrewarmMetaType instance;
      return instance;
    }
    case ObStorageObjectType::MAJOR_PREWARM_META_INDEX: {
      static const ObMajorPrewarmMetaIndexType instance;
      return instance;
    }
    case ObStorageObjectType::TENANT_DISK_SPACE_META: {
      static const ObTenantDiskSpaceMetaType instance;
      return instance;
    }
    case ObStorageObjectType::IS_SHARED_TENANT_DELETED: {
      static const ObIsSharedTenantDeletedType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MICRO_DATA_MACRO: {
      static const ObSharedMicroDataMacroType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MICRO_META_MACRO: {
      static const ObSharedMicroMetaMacroType instance;
      return instance;
    }
    case ObStorageObjectType::UNSEALED_REMOTE_SEG_FILE: {
      static const ObUnsealedRemoteSegFileType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MDS_MINI_DATA_MACRO: {
      static const ObSharedMdsMiniDataMacroType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MDS_MINI_META_MACRO: {
      static const ObSharedMdsMiniMetaMacroType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MDS_MINOR_DATA_MACRO: {
      static const ObSharedMdsMinorDataMacroType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MDS_MINOR_META_MACRO: {
      static const ObSharedMdsMinorMetaMacroType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_TABLET_META: {
      static const ObSharedTabletMetaType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_TABLET_SUB_META: {
      static const ObSharedTabletSubMetaType instance;
      return instance;
    }
    case ObStorageObjectType::TENANT_ROOT_KEY: {
      static const ObTenantRootKeyType instance;
      return instance;
    }
    case ObStorageObjectType::EXTERNAL_TABLE_FILE: {
      static const ObExternalTableFileType instance;
      return instance;
    }
    case ObStorageObjectType::MACRO_CACHE_CKPT_DATA: {
      static const ObMacroCacheCkptDataType instance;
      return instance;
    }
    case ObStorageObjectType::MACRO_CACHE_CKPT_META: {
      static const ObMacroCacheCkptMetaType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_INC_MAJOR_DATA_MACRO: {
      static const ObSharedIncMajorDataMacroType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_INC_MAJOR_META_MACRO: {
      static const ObSharedIncMajorMetaMacroType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_TABLET_SUB_META_IN_TABLE: {
      static const ObSharedTabletSubMetaInTableType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MINI_V2_DATA_MACRO: {
      static const ObSharedMiniV2DataMacroType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MINI_V2_META_MACRO: {
      static const ObSharedMiniV2MetaMacroType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MINOR_V2_DATA_MACRO: {
      static const ObSharedMinorV2DataMacroType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MINOR_V2_META_MACRO: {
      static const ObSharedMinorV2MetaMacroType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_TABLET_MACRO_DIFF: {
      static const ObSharedTabletMacroDiffType instance;
      return instance;
    }
    default: {
      static const ObStorageObjectTypeBase instance;
      return instance;
    }
  }
}

} // end namespace blocksstable
} // end namespace oceanbase
