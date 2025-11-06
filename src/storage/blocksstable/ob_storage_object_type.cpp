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
#define USING_LOG_PREFIX STORAGE
#include "ob_storage_object_type.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "storage/meta_store/ob_tenant_storage_meta_service.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "close_modules/shared_storage/storage/shared_storage/ob_file_helper.h"
#include "close_modules/shared_storage/storage/shared_storage/ob_dir_manager.h"
#include "close_modules/shared_storage/storage/shared_storage/storage_cache_policy/ob_storage_cache_service.h"
#endif

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
  "SHARED_MINI_SSTABLE_LIST",
  "SHARED_MINI_SSTABLE_LIST_TASK",
  "SHARED_MINI_SSTABLE_LIST_OP_ID",
  "SHARED_MINI_SSTABLE_LIST_CURRENT",
  "SHARED_MINI_SSTABLE_LIST_OLDEST",
  "SHARED_MINOR_SSTABLE_LIST",
  "SHARED_MINOR_SSTABLE_LIST_TASK",
  "SHARED_MINOR_SSTABLE_LIST_OP_ID",
  "SHARED_MINOR_SSTABLE_LIST_CURRENT",
  "SHARED_MINOR_SSTABLE_LIST_OLDEST",
  "SHARED_LS_META",
  "SHARED_LS_META_TASK",
  "SHARED_LS_META_OP_ID",
  "SHARED_LS_META_CURRENT",
  "SHARED_LS_META_OLDEST",
  "SHARED_TABLET_META",
  "SHARED_TABLET_META_TASK",
  "SHARED_TABLET_META_OP_ID",
  "SHARED_TABLET_META_CURRENT",
  "SHARED_TABLET_META_OLDEST",
  "SHARED_MDS_MINI_SSTABLE_LIST",
  "SHARED_MDS_MINI_SSTABLE_LIST_TASK",
  "SHARED_MDS_MINI_SSTABLE_LIST_OP_ID",
  "SHARED_MDS_MINI_SSTABLE_LIST_CURRENT",
  "SHARED_MDS_MINI_SSTABLE_LIST_OLDEST",
  "SHARED_MDS_MINOR_SSTABLE_LIST",
  "SHARED_MDS_MINOR_SSTABLE_LIST_TASK",
  "SHARED_MDS_MINOR_SSTABLE_LIST_OP_ID",
  "SHARED_MDS_MINOR_SSTABLE_LIST_CURRENT",
  "SHARED_MDS_MINOR_SSTABLE_LIST_OLDEST",
  "SHARED_DDLKV_MINI_SSTABLE_LIST",
  "SHARED_DDLKV_MINI_SSTABLE_LIST_TASK",
  "SHARED_DDLKV_MINI_SSTABLE_LIST_OP_ID",
  "SHARED_DDLKV_MINI_SSTABLE_LIST_CURRENT",
  "SHARED_DDLKV_MINI_SSTABLE_LIST_OLDEST",
  "SHARED_SPLIT_MINOR_SSTABLE",
  "SHARED_SPLIT_MINOR_SSTABLE_TASK",
  "SHARED_SPLIT_MINOR_SSTABLE_OP_ID",
  "SHARED_SPLIT_MINOR_SSTABLE_CURRENT",
  "SHARED_SPLIT_MINOR_SSTABLE_OLDEST",
  "SHARED_SPLIT_MDS_MINOR_SSTABLE",
  "SHARED_SPLIT_MDS_MINOR_SSTABLE_TASK",
  "SHARED_SPLIT_MDS_MINOR_SSTABLE_OP_ID",
  "SHARED_SPLIT_MDS_MINOR_SSTABLE_CURRENT",
  "SHARED_SPLIT_MDS_MINOR_SSTABLE_OLDEST",
  "SHARED_MAJOR_SSTABLE",
  "SHARED_MAJOR_SSTABLE_TASK",
  "SHARED_MAJOR_SSTABLE_OP_ID",
  "SHARED_MAJOR_SSTABLE_CURRENT",
  "SHARED_MAJOR_SSTABLE_OLDEST",
  "SHARED_TABLET_SUB_META",
  "TENANT_ROOT_KEY",
  "EXTERNAL_TABLE_FILE",
  "MACRO_CACHE_CKPT_DATA",
  "MACRO_CACHE_CKPT_META",
  "SHARED_INC_MAJOR_DATA_MACRO",
  "SHARED_INC_MAJOR_META_MACRO",
  "MAX"
};
static inline const char *get_ls_inner_tablet_name_(const int64_t tablet_id)
{
  ObTabletID id(tablet_id);
  if (id.is_ls_tx_ctx_tablet()) {
    return "TX_CTX";
  } else if (id.is_ls_tx_data_tablet()) {
    return "TX_DATA";
  } else if (id.is_ls_lock_tablet()) {
    return "TX_LOCK";
  } else if (id.is_ls_reorg_info_tablet()) {
    return "REORG_INFO";
  } else {
    OB_LOG_RET(WARN, OB_ERR_UNEXPECTED, "unexpected tablet_id", K(tablet_id));
    return "UNEXPECTED";
  }
}

static int get_ls_inner_tablet_id_(const char *str, int64_t &tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(str))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid str", KR(ret));
  } else if (0 == STRNCMP("TX_CTX", str, STRLEN("TX_CTX"))) {
    tablet_id = ObTabletID::LS_TX_CTX_TABLET_ID;
  } else if (0 == STRNCMP("TX_DATA", str, STRLEN("TX_DATA"))) {
    tablet_id = ObTabletID::LS_TX_DATA_TABLET_ID;
  } else if (0 == STRNCMP("TX_LOCK", str, STRLEN("TX_LOCK"))) {
    tablet_id = ObTabletID::LS_LOCK_TABLET_ID;
  } else if (0 == STRNCMP("REORG_INFO", str, STRLEN("REORG_INFO"))) {
    tablet_id = ObTabletID::LS_REORG_INFO_TABLET_ID;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected str", K(str));
  }
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
static int prewarm_file_to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id)
{
  int ret = OB_SUCCESS;
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/major/prewarm_info/scn%ld
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%lu/%s_%lu/%s/%ld/%ld/%s/%s/%s%ld",
              object_storage_root_dir, CLUSTER_DIR_STR, cluster_id,
              TENANT_DIR_STR, tenant_id, TABLET_DIR_STR, file_id.second_id(),
              file_id.fourth_id(), MAJOR_DIR_STR, PREWARM_INFO_DIR_STR, SCN_KEY_STR, file_id.third_id()))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

static int prewarm_file_remote_path_to_macro_id(const char *path, const ObStorageObjectType type, MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  // /tablet_id/reorganization_scn/major/prewarm_info/scn%ld
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 5))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else {
    int64_t tablet_id = 0;
    int64_t scn_id = 0;
    int64_t fourth_id = 0;
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld/%%ld/%s/%s/%s%%ld.T%hhu",
                MAJOR_DIR_STR, PREWARM_INFO_DIR_STR, SCN_KEY_STR, (uint8_t)type))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &fourth_id, &scn_id))) {
    } else if (OB_UNLIKELY(3 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id(scn_id);
      macro_id.set_fourth_id(fourth_id);
    }
  }
  return ret;
}
#endif

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

#ifdef OB_BUILD_SHARED_STORAGE
void ObStorageObjectTypeBase::get_ss_macro_block_type(
  const MacroBlockId &macro_id, storage::ObSSMacroBlockType &ss_macro_block_type) const
{
  ss_macro_block_type = ObSSMacroBlockType::MAX_TYPE;
  if (ObStorageObjectType::EXTERNAL_TABLE_FILE == type_) {
    ss_macro_block_type = ObSSMacroBlockType::EXTERNAL_TABLE;
  } else if (macro_id.is_shared_data_or_meta()) {
    ss_macro_block_type = ObSSMacroBlockType::SHARED_MACRO;
  } else if (macro_id.is_private_data_or_meta()) {
    ss_macro_block_type = ObSSMacroBlockType::PRIVATE_MACRO;
  }
}

int ObStorageObjectTypeBase::get_macro_cache_type(const uint64_t effective_tablet_id,
    const bool use_effective_tablet_id, storage::ObSSMacroCacheType &macro_cache_type) const
{
  int ret = OB_SUCCESS;
  macro_cache_type = ObSSMacroCacheType::MAX_TYPE;
  if (OB_UNLIKELY(ObStorageObjectType::MAX == type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid object type", KR(ret), K(type_));
  } else {
    switch (type_) {
      case ObStorageObjectType::PRIVATE_TABLET_META: {
        macro_cache_type = ObSSMacroCacheType::META_FILE;
        break;
      }
      case ObStorageObjectType::TMP_FILE: {
        macro_cache_type = ObSSMacroCacheType::TMP_FILE;
        break;
      }
      default: {
        macro_cache_type = ObSSMacroCacheType::MACRO_BLOCK;
        break;
      }
    }
  }
  if (macro_cache_type == ObSSMacroCacheType::MACRO_BLOCK) {
      // treat macro_cache_type as ObSSMacroCacheType::MACRO_BLOCK in default.
      // e.g., PRIVATE_DATA_MACRO, SHARED_MINI_DATA_MACRO, SHARED_MDS_MINI_DATA_MACRO, SHARED_MAJOR_DATA_MACRO...
    if (OB_UNLIKELY(use_effective_tablet_id && ObTabletID::INVALID_TABLET_ID == effective_tablet_id)) {
      // ObIndexBlockScanEstimator and ObSSTableSecMetaIterator do not fill effective_tablet_id.
      // preread io triggered by these routes has no effective_tablet_id.
      // treat these macros as ObSSMacroCacheType::MACRO_BLOCK.
    } else if (use_effective_tablet_id && is_user_tenant(MTL_ID())) {
      // 1. is_user_tenant, only user tenant has ObStorageCachePolicyService.
      // both oracle mode and mysql mode user tenants have ObStorageCachePolicyService,
      // although oracle mode user tenant's ObStorageCachePolicyService is empty.
      bool is_hot = false;
      ObStorageCachePolicyService *storage_cache_policy_service = nullptr;
      if (OB_ISNULL(storage_cache_policy_service = MTL(ObStorageCachePolicyService *))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("storage cache policy service is null", KR(ret));
      } else if (OB_FAIL(storage_cache_policy_service->is_hot_tablet(effective_tablet_id, is_hot))) {
        LOG_WARN("fail to check is hot tablet", KR(ret), K(effective_tablet_id));
      } else if (is_hot) {
        macro_cache_type = ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK;
      }
    }
  }
  return ret;
}

int ObStorageObjectTypeBase::get_open_flag_for_write() const
{
  int open_flag = ObSSIOCommonOp::SS_DEFAULT_WRITE_FLAG;
  switch (type_) {
    case ObStorageObjectType::TMP_FILE: {
      open_flag = ObSSIOCommonOp::SS_TMP_FILE_WRITE_FLAG;
      break;
    }
    case ObStorageObjectType::PRIVATE_SLOG_FILE: {
      open_flag = ObSSIOCommonOp::SS_SLOG_WRITE_FLAG;
      break;
    }
    default: {
      open_flag = ObSSIOCommonOp::SS_DEFAULT_WRITE_FLAG;
      break;
    }
  }
  return open_flag;
}

int ObStorageObjectTypeBase::get_open_flag_for_read() const
{
  int open_flag = ObSSIOCommonOp::SS_DEFAULT_READ_FLAG;
  switch (type_) {
    case ObStorageObjectType::TMP_FILE: {
      open_flag = ObSSIOCommonOp::SS_TMP_FILE_READ_FLAG;
      break;
    }
    default: {
      open_flag = ObSSIOCommonOp::SS_DEFAULT_READ_FLAG;
      break;
    }
  }
  return open_flag;
}

int ObStorageObjectTypeBase::aio_read(
  const ObStorageObjectReadInfo &read_info, ObStorageObjectHandle &object_handle) const
{
  int ret = OB_SUCCESS;
  if (is_direct_read()) {
    ObSSObjectStorageReader object_storage_reader;
    if (OB_FAIL(object_storage_reader.aio_read(read_info, object_handle))) {
      LOG_WARN("fail to aio read", KR(ret), K(read_info), K(object_handle));
    }
  } else if (is_pin_local()) {
    ObSSLocalCacheReader local_cache_reader;
    if (OB_FAIL(local_cache_reader.aio_read(read_info, object_handle))) {
      LOG_WARN("fail to aio read", KR(ret), K(read_info), K(object_handle));
    }
  } else {
    switch (type_) {
      case ObStorageObjectType::PRIVATE_DATA_MACRO:
      case ObStorageObjectType::PRIVATE_META_MACRO: {
        ObSSPrivateMacroReader private_macro_reader;
        if (OB_FAIL(private_macro_reader.aio_read(read_info, object_handle))) {
          LOG_WARN("fail to aio read", KR(ret), K(read_info), K(object_handle));
        }
        break;
      }
      case ObStorageObjectType::PRIVATE_TABLET_META: {
        ObSSPrivateTabletMetaReader private_tablet_meta_reader;
        if (OB_FAIL(private_tablet_meta_reader.aio_read(read_info, object_handle))) {
          LOG_WARN("fail to aio read", KR(ret), K(read_info), K(object_handle));
        }
        break;
      }
      case ObStorageObjectType::PRIVATE_SLOG_FILE: {
        ObSSPrivateSlogReader private_slog_reader;
        if (OB_FAIL(private_slog_reader.aio_read(read_info, object_handle))) {
          LOG_WARN("fail to aio read", KR(ret), K(read_info), K(object_handle));
        }
        break;
      }
      case ObStorageObjectType::SHARED_MINI_DATA_MACRO:
      case ObStorageObjectType::SHARED_MINI_META_MACRO:
      case ObStorageObjectType::SHARED_MINOR_DATA_MACRO:
      case ObStorageObjectType::SHARED_MINOR_META_MACRO:
      case ObStorageObjectType::SHARED_MDS_MINI_DATA_MACRO:
      case ObStorageObjectType::SHARED_MDS_MINI_META_MACRO:
      case ObStorageObjectType::SHARED_MDS_MINOR_DATA_MACRO:
      case ObStorageObjectType::SHARED_MDS_MINOR_META_MACRO:
      case ObStorageObjectType::SHARED_MAJOR_DATA_MACRO:
      case ObStorageObjectType::SHARED_MAJOR_META_MACRO:
      case ObStorageObjectType::SHARED_INC_MAJOR_DATA_MACRO:
      case ObStorageObjectType::SHARED_INC_MAJOR_META_MACRO: {
        ObSSShareMacroReader share_macro_reader;
        if (OB_FAIL(share_macro_reader.aio_read(read_info, object_handle))) {
          LOG_WARN("fail to aio read", KR(ret), K(read_info), K(object_handle));
        }
        break;
      }
      case ObStorageObjectType::TMP_FILE: {
        ObSSTmpFileReader tmp_file_reader;
        if (OB_FAIL(tmp_file_reader.aio_read(read_info, object_handle))) {
          LOG_WARN("fail to aio read", KR(ret), K(read_info), K(object_handle));
        }
        break;
      }
      case ObStorageObjectType::EXTERNAL_TABLE_FILE: {
        ObSSExternalFileReader external_file_reader;
        if (OB_FAIL(external_file_reader.init(&read_info))) {
          LOG_WARN("fail to init external data reader", KR(ret), K(read_info));
        } else if (OB_FAIL(external_file_reader.aio_read(read_info, object_handle))) {
          LOG_WARN("fail to aio read", KR(ret), K(read_info), K(object_handle));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected storage object type", KR(ret), K(type_), "object_type_str",
                 get_type_str(), K(read_info), K(object_handle));
        break;
      }
    }
  }
  return ret;
}

int ObStorageObjectTypeBase::aio_write(
  const ObStorageObjectWriteInfo &write_info, ObStorageObjectHandle &object_handle) const
{
  int ret = OB_SUCCESS;
  if (is_direct_read()) {
    ObSSObjectStorageWriter object_storage_writer;
    if (OB_FAIL(object_storage_writer.aio_write(write_info, object_handle))) {
      LOG_WARN("fail to aio write", KR(ret), K(write_info), K(object_handle));
    }
  } else if (is_pin_local()) {
    ObSSLocalCacheWriter local_cache_writer;
    if (OB_FAIL(local_cache_writer.aio_write(write_info, object_handle))) {
      if (OB_NO_SUCH_FILE_OR_DIRECTORY != ret) {
        LOG_WARN("fail to aio write", KR(ret), K(write_info), K(object_handle));
      }
    }
  } else {
    switch (type_) {
      case ObStorageObjectType::PRIVATE_DATA_MACRO:
      case ObStorageObjectType::PRIVATE_META_MACRO: {
        ObSSPrivateMacroWriter private_macro_writer;
        if (OB_FAIL(private_macro_writer.aio_write(write_info, object_handle))) {
          if (OB_NO_SUCH_FILE_OR_DIRECTORY != ret) {
            LOG_WARN("fail to aio write", KR(ret), K(write_info), K(object_handle));
          }
        }
        break;
      }
      case ObStorageObjectType::PRIVATE_TABLET_META: {
        ObSSPrivateTabletMetaWriter private_tablet_meta_writer;
        if (OB_FAIL(private_tablet_meta_writer.aio_write(write_info, object_handle))) {
          LOG_WARN("fail to aio write", KR(ret), K(write_info), K(object_handle));
        }
        break;
      }
      case ObStorageObjectType::PRIVATE_SLOG_FILE: {
        ObSSPrivateSlogWriter private_slog_writer;
        if (OB_FAIL(private_slog_writer.aio_write(write_info, object_handle))) {
          if (OB_NO_SUCH_FILE_OR_DIRECTORY != ret) {
            LOG_WARN("fail to aio write", KR(ret), K(write_info), K(object_handle));
          }
        }
        break;
      }
      case ObStorageObjectType::SHARED_MINI_DATA_MACRO:
      case ObStorageObjectType::SHARED_MINI_META_MACRO:
      case ObStorageObjectType::SHARED_MINOR_DATA_MACRO:
      case ObStorageObjectType::SHARED_MINOR_META_MACRO:
      case ObStorageObjectType::SHARED_MDS_MINI_DATA_MACRO:
      case ObStorageObjectType::SHARED_MDS_MINI_META_MACRO:
      case ObStorageObjectType::SHARED_MDS_MINOR_DATA_MACRO:
      case ObStorageObjectType::SHARED_MDS_MINOR_META_MACRO:
      case ObStorageObjectType::SHARED_MAJOR_DATA_MACRO:
      case ObStorageObjectType::SHARED_MAJOR_META_MACRO:
      case ObStorageObjectType::SHARED_INC_MAJOR_DATA_MACRO:
      case ObStorageObjectType::SHARED_INC_MAJOR_META_MACRO: {
        ObSSShareMacroWriter share_macro_writer;
        if (OB_FAIL(share_macro_writer.aio_write(write_info, object_handle))) {
          LOG_WARN("fail to aio write", KR(ret), K(write_info), K(object_handle));
        }
        break;
      }
      case ObStorageObjectType::TMP_FILE: {
        ObSSTmpFileWriter tmp_file_writer;
        if (OB_FAIL(tmp_file_writer.aio_write(write_info, object_handle))) {
          if (OB_NO_SUCH_FILE_OR_DIRECTORY != ret) {
            LOG_WARN("fail to aio write", KR(ret), K(write_info), K(object_handle));
          }
        }
        break;
      }
      case ObStorageObjectType::EXTERNAL_TABLE_FILE: {
        ObSSLocalCacheWriter local_cache_writer;
        if (OB_FAIL(local_cache_writer.aio_write(write_info, object_handle))) {
          if (OB_NO_SUCH_FILE_OR_DIRECTORY != ret) {
            LOG_WARN("fail to aio write", KR(ret), K(write_info), K(object_handle));
          }
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected storage object type", KR(ret), K(type_), "object_type_str",
                 get_type_str(), K(write_info), K(object_handle));
        break;
      }
    }
  }
  return ret;
}
#endif

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
  // second_id:tablet_id, third_id:server_id, fourth_id:macro_transfer_seq+tenant_seq
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() > 0) &&
         (file_id.macro_transfer_epoch() >= 0) && (file_id.tenant_seq() >= 0);
}
#ifdef OB_BUILD_SHARED_STORAGE
int ObPrivateDataMacroType::to_local_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  //tenant_id_epoch_id/tablet_data/scatter_id/tablet_id/transfer_seq/data/svr%ldseq%ld
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%02ld/%ld/%ld/%s/%s%ld%s%ld",
              OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, TABLET_DATA_DIR_STR,
              (file_id.second_id() % ObDirManager::PRIVATE_MACRO_SCATTER_DIR_NUM), file_id.second_id(),
              file_id.macro_transfer_epoch(), DATA_MACRO_DIR_STR, SVR_KEY_STR, file_id.third_id(), SEQ_KEY_STR,
              file_id.tenant_seq()))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
int ObPrivateDataMacroType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // cluster_id/server_id/tenant_id_epoch_id/tablet_data/tablet_id/transfer_seq/data/svr%ldseq%ld
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%ld/%lu_%ld/%s/%ld/%ld/%s/%s%ld%s%ld",
              object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR, file_id.third_id(), tenant_id,
              tenant_epoch_id, TABLET_DATA_DIR_STR, file_id.second_id(), file_id.macro_transfer_epoch(),
              DATA_MACRO_DIR_STR, SVR_KEY_STR, file_id.third_id(), SEQ_KEY_STR, file_id.tenant_seq()))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

int ObPrivateDataMacroType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  return local_path_to_macro_id(path, macro_id);
}
int ObPrivateDataMacroType::local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  const char *sub_path = nullptr;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 4))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else {
    char format[512] = {0};
    int num = 0;
    int64_t tablet_id = 0;
    int64_t transfer_seq = 0;
    int64_t server_id = 0;
    int64_t seq_id = 0;
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld/%%ld/%s/%s%%ld%s%%ld.T%hhu",
                DATA_MACRO_DIR_STR, SVR_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &transfer_seq, &server_id, &seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_macro_transfer_epoch(transfer_seq);
      macro_id.set_third_id(server_id);
      macro_id.set_tenant_seq(seq_id);
    }
  }
  return ret;
}
int ObPrivateDataMacroType::get_parent_dir(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id,
  const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // tenant_id_epoch_id/tablet_data/scatter_id/tablet_id/transfer_seq/data
  if (OB_FAIL(OB_DIR_MGR.get_local_tablet_id_macro_dir(path, length, tenant_id, tenant_epoch_id, file_id.second_id(),
              file_id.macro_transfer_epoch(), ObMacroType::DATA_MACRO))) {
    LOG_WARN("fail to get local tablet id macro dir", KR(ret));
  }
  return ret;
}
int ObPrivateDataMacroType::create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id,
  const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_DIR_MGR.create_tablet_data_tablet_id_transfer_seq_dir(tenant_id, tenant_epoch_id, file_id.second_id(),
              file_id.macro_transfer_epoch()))) {
    LOG_WARN("fail to create tablet data tablet id transfer seq dir", KR(ret));
  }
  return ret;
}
int ObPrivateDataMacroType::get_effective_tablet_id(const MacroBlockId &macro_id,
  uint64_t &effective_tablet_id) const
{
  int ret = OB_SUCCESS;
  size_t pos = 8; // offsetof(MacroBlockId, second_id_);
  size_t size = sizeof(uint64_t);
  if (pos + size > sizeof(MacroBlockId)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pos), K(size));
  } else {
    const char* macro_id_bytes = reinterpret_cast<const char*>(&macro_id);
    effective_tablet_id = 0;
    memcpy(&effective_tablet_id, macro_id_bytes + pos, size);
  }
  return ret;
}
#endif

int ObPrivateDataMacroType::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
  const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type:%s (tablet_id=%lu, transfer_seq=%lu)",
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
    object_id.set_macro_transfer_epoch(opt.private_opt_.tablet_trasfer_seq_);
  }
  return ret;
}
/**
 * ---------------------------------------ObPrivateMetaMacroType----------------------------------------
 */

bool ObPrivateMetaMacroType::is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:server_id, fourth_id:macro_transfer_seq+tenant_seq
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() > 0) &&
         (file_id.macro_transfer_epoch() >= 0) && (file_id.tenant_seq() >= 0);
}
#ifdef OB_BUILD_SHARED_STORAGE
int ObPrivateMetaMacroType::to_local_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // tenant_id_epoch_id/tablet_data/scatter_id/tablet_id/transfer_seq/meta/svr%ldseq%ld
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%02ld/%ld/%ld/%s/%s%ld%s%ld",
              OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
              TABLET_DATA_DIR_STR, (file_id.second_id() % ObDirManager::PRIVATE_MACRO_SCATTER_DIR_NUM),
              file_id.second_id(), file_id.macro_transfer_epoch(),
              META_MACRO_DIR_STR, SVR_KEY_STR, file_id.third_id(), SEQ_KEY_STR, file_id.tenant_seq()))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
int ObPrivateMetaMacroType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // cluster_id/server_id/tenant_id_epoch_id/tablet_data/tablet_id/transfer_seq/meta/svr%ldseq%ld
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%ld/%lu_%ld/%s/%ld/%ld/%s/%s%ld%s%ld",
              object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR, file_id.third_id(),
              tenant_id, tenant_epoch_id, TABLET_DATA_DIR_STR, file_id.second_id(), file_id.macro_transfer_epoch(),
              META_MACRO_DIR_STR, SVR_KEY_STR, file_id.third_id(), SEQ_KEY_STR, file_id.tenant_seq()))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

int ObPrivateMetaMacroType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  return local_path_to_macro_id(path, macro_id);
}
int ObPrivateMetaMacroType::local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  const char *sub_path = nullptr;
  if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 4))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else {
    char format[512] = {0};
    int num = 0;
    int64_t tablet_id = 0;
    int64_t transfer_seq = 0;
    int64_t server_id = 0;
    int64_t seq_id = 0;
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld/%%ld/%s/%s%%ld%s%%ld.T%hhu", META_MACRO_DIR_STR,
                SVR_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &transfer_seq, &server_id, &seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_macro_transfer_epoch(transfer_seq);
      macro_id.set_third_id(server_id);
      macro_id.set_tenant_seq(seq_id);
    }
  }
  return ret;
}
int ObPrivateMetaMacroType::get_parent_dir(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id,
  const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // tenant_id_epoch_id/tablet_data/scatter_id/tablet_id/transfer_seq/meta/
  if (OB_FAIL(OB_DIR_MGR.get_local_tablet_id_macro_dir(path, length, tenant_id, tenant_epoch_id, file_id.second_id(),
              file_id.macro_transfer_epoch(), ObMacroType::META_MACRO))) {
    LOG_WARN("fail to get local tablet id macro dir", KR(ret));
  }
  return ret;
}
int ObPrivateMetaMacroType::create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id,
  const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_DIR_MGR.create_tablet_data_tablet_id_transfer_seq_dir(
              tenant_id, tenant_epoch_id, file_id.second_id(), file_id.macro_transfer_epoch()))) {
    LOG_WARN("fail to create tablet data tablet id transfer seq dir", KR(ret));
  }
  return ret;
}
int ObPrivateMetaMacroType::get_effective_tablet_id(const MacroBlockId &macro_id,
  uint64_t &effective_tablet_id) const
{
  int ret = OB_SUCCESS;
  size_t pos = 8; // offsetof(MacroBlockId, second_id_);
  size_t size = sizeof(uint64_t);
  if (pos + size > sizeof(MacroBlockId)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pos), K(size));
  } else {
    const char* macro_id_bytes = reinterpret_cast<const char*>(&macro_id);
    effective_tablet_id = 0;
    memcpy(&effective_tablet_id, macro_id_bytes + pos, size);
  }
  return ret;
}
#endif

int ObPrivateMetaMacroType::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
  const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type:%s (tablet_id=%lu, transfer_seq=%lu)", get_type_str(),
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
    object_id.set_macro_transfer_epoch(opt.private_opt_.tablet_trasfer_seq_);
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
#ifdef OB_BUILD_SHARED_STORAGE
int ObSharedMiniDataMacroType::to_local_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // inner_tablet: tenant_id_epoch_id/shared_mini_macro_cache/ls/ls_id/tablet_name_op%ldseq%ld
  // user_tablet: tenant_id_epoch_id/shared_mini_macro_cache/scatter_id/tablet%ldreorg%ldop%ldseq%ld
  if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%s/%ld/%s_%s%ld%s%ld",
              OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
              SHARED_MINI_MACRO_CACHE_DIR_STR, LS_DIR_STR, file_id.meta_ls_id(),
              get_ls_inner_tablet_name_(file_id.second_id()), OP_KEY_STR, (file_id.third_id() >> 32)/*op_id*/,
              SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF)/*macro_seq_id*/))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld",
              OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
              SHARED_MINI_MACRO_CACHE_DIR_STR, (file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM),
              TABLET_KEY_STR, file_id.second_id(), REORG_KEY_STR, file_id.reorganization_scn(),
              OP_KEY_STR, (file_id.third_id() >> 32)/*op_id*/,
              SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF)/*macro_seq_id*/))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  }
  return ret;
}
int ObSharedMiniDataMacroType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // inner_tablet: cluster_id/tenant_id/ls/ls_id/tablet_name/mini/sstable/op_id/data/seq%ld
  // user_tablet: cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/mini/sstable/op_id/data/seq%ld
  if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%s/%s/%s/op_%ld/%s/%s%ld",
                object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id, LS_DIR_STR,
                file_id.meta_ls_id(), get_ls_inner_tablet_name_(file_id.second_id()), MINI_DIR_STR,
                SHARED_TABLET_SSTABLE_DIR_STR, (file_id.third_id() >> 32)/*op_id*/, DATA_MACRO_DIR_STR,
                SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF) /*macro_seq_id*/))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%ld/%s/%s/op_%ld/%s/%s%ld",
                object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id, TABLET_DIR_STR,
                file_id.second_id(), file_id.reorganization_scn(), MINI_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR,
                (file_id.third_id() >> 32)/*op_id*/, DATA_MACRO_DIR_STR, SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF) /*macro_seq_id*/))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  }
  return ret;
}

int ObSharedMiniDataMacroType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  bool is_inner_tablet = false;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t ls_id = 0;
  char tablet_name[512] = {0};
  int64_t op_id = 0;
  int64_t macro_seq_id = 0;
  const char *judge_path = nullptr;
  // inner_tablet: /ls/ls_id/tablet_name/mini/sstable/op_id/data/seq%ld
  // user_tablet: /tablet/tablet_id/reorganization_scn/mini/sstable/op_id/data/seq%ld
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(judge_path = ObString(path).reverse_find('/', 8))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (NULL != STRSTR(judge_path, LS_DIR_STR)) {
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s/%%ld/%[^/]/%s/%s/%s_%%ld/%s/%s%%ld.T%hhu",
                       LS_DIR_STR, MINI_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, OP_KEY_STR, DATA_MACRO_DIR_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(judge_path, format, &ls_id, tablet_name, &op_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(judge_path), K(path), K(format), K(num));
    } else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) {
      LOG_WARN("fail to get ls inner tablet id", KR(ret), K(tablet_name));
    } else {
      is_inner_tablet = true;
    }
  } else {
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s/%%ld/%%ld/%s/%s/%s_%%ld/%s/%s%%ld.T%hhu",
                TABLET_DIR_STR, MINI_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, OP_KEY_STR, DATA_MACRO_DIR_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(judge_path, format, &tablet_id, &reorganization_scn, &op_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(judge_path), K(path), K(format), K(num));
    } else {
      is_inner_tablet = false;
    }
  }
  if (OB_SUCC(ret)) {
    if (is_inner_tablet) {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id((op_id << 32) + macro_seq_id);
      macro_id.set_meta_ls_id(ls_id);
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id((op_id << 32) + macro_seq_id);
      macro_id.set_reorganization_scn(reorganization_scn);
    }
  }
  return ret;
}
int ObSharedMiniDataMacroType::local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  bool is_inner_tablet = false;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t ls_id = 0;
  char tablet_name_part1[256] = {0};
  char tablet_name_part2[256] = {0};
  char tablet_name[512] = {0};
  int64_t op_id = 0;
  int64_t macro_seq_id = 0;
  const char *judge_path = nullptr;
  if (OB_ISNULL(judge_path = ObString(path).reverse_find('/', 3))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (NULL != STRSTR(judge_path, LS_DIR_STR)) {
    const char *sub_path = nullptr;
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 2))) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
    } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld/%%[^_]_%%[^_]_%s%%ld%s%%ld.T%hhu",
                       OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &ls_id, tablet_name_part1, tablet_name_part2, &op_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(5 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else if (OB_FAIL(databuff_printf(tablet_name, sizeof(tablet_name), "%s_%s", tablet_name_part1, tablet_name_part2))) {
      LOG_WARN("fail to databuff printf", KR(ret), K(tablet_name_part1), K(tablet_name_part2));
    } else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) {
      LOG_WARN("fail to get ls inner tablet id", KR(ret), K(tablet_name));
    } else {
      is_inner_tablet = true;
    }
  } else {
    const char *sub_path = nullptr;
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
    } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu",
                TABLET_KEY_STR, REORG_KEY_STR, OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn, &op_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      is_inner_tablet = false;
    }
  }
  if (OB_SUCC(ret)) {
    if (is_inner_tablet) {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id((op_id << 32) + macro_seq_id);
      macro_id.set_meta_ls_id(ls_id);
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id((op_id << 32) + macro_seq_id);
      macro_id.set_reorganization_scn(reorganization_scn);
    }
  }
  return ret;
}
int ObSharedMiniDataMacroType::create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id,
  const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_DIR_MGR.create_shared_mini_ls_id_dir(tenant_id, tenant_epoch_id, file_id.meta_ls_id()))) {
    LOG_WARN("fail to create shared mini ls id dir", KR(ret));
  }
  return ret;
}
int ObSharedMiniDataMacroType::get_effective_tablet_id(const MacroBlockId &macro_id,
  uint64_t &effective_tablet_id) const
{
  int ret = OB_SUCCESS;
  size_t pos = 8; // offsetof(MacroBlockId, second_id_);
  size_t size = sizeof(uint64_t);
  if (pos + size > sizeof(MacroBlockId)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pos), K(size));
  } else {
    const char* macro_id_bytes = reinterpret_cast<const char*>(&macro_id);
    effective_tablet_id = 0;
    memcpy(&effective_tablet_id, macro_id_bytes + pos, size);
  }
  return ret;
}
#endif

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
#ifdef OB_BUILD_SHARED_STORAGE
int ObSharedMiniMetaMacroType::to_local_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // inner_tablet:tenant_id_epoch_id/shared_mini_macro_cache/ls/ls_id/tablet_name_op%ldseq%ld
  // user_tablet:tenant_id_epoch_id/shared_mini_macro_cache/scatter_id/tablet%ldreorg%ldop%ldseq%ld
  if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%s/%ld/%s_%s%ld%s%ld",
              OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
              SHARED_MINI_MACRO_CACHE_DIR_STR, LS_DIR_STR, file_id.meta_ls_id(),
              get_ls_inner_tablet_name_(file_id.second_id()), OP_KEY_STR, (file_id.third_id() >> 32)/*op_id*/,
              SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF)/*macro_seq_id*/))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld",
              OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
              SHARED_MINI_MACRO_CACHE_DIR_STR, (file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM),
              TABLET_KEY_STR, file_id.second_id(), REORG_KEY_STR, file_id.reorganization_scn(),
              OP_KEY_STR, (file_id.third_id() >> 32)/*op_id*/,
              SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF)/*macro_seq_id*/))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  }
  return ret;
}
int ObSharedMiniMetaMacroType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // inner_tablet:cluster_id/tenant_id/ls/ls_id/tablet_name/mini/sstable/op_id/meta/seq%ld
  // user_tablet:cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/mini/sstable/op_id/meta/seq%ld
  if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%s/%s/%s/op_%ld/%s/%s%ld",
                object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id, LS_DIR_STR,
                file_id.meta_ls_id(), get_ls_inner_tablet_name_(file_id.second_id()), MINI_DIR_STR,
                SHARED_TABLET_SSTABLE_DIR_STR, (file_id.third_id() >> 32)/*op_id*/, META_MACRO_DIR_STR,
                SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF) /*macro_seq_id*/))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%ld/%s/%s/op_%ld/%s/%s%ld",
                object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id, TABLET_DIR_STR,
                file_id.second_id(), file_id.reorganization_scn(), MINI_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR,
                (file_id.third_id() >> 32)/*op_id*/, META_MACRO_DIR_STR, SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF) /*macro_seq_id*/))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  }
  return ret;
}

int ObSharedMiniMetaMacroType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  bool is_inner_tablet = false;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t ls_id = 0;
  char tablet_name[512] = {0};
  int64_t op_id = 0;
  int64_t macro_seq_id = 0;
  const char *judge_path = nullptr;
  // inner_tablet: /ls/ls_id/tablet_name/mini/sstable/op_id/meta/seq%ld
  // user_tablet: /tablet/tablet_id/reorganization_scn/mini/sstable/op_id/meta/seq%ld
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(judge_path = ObString(path).reverse_find('/', 8))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (NULL != STRSTR(judge_path, LS_DIR_STR)) {
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s/%%ld/%[^/]/%s/%s/%s_%%ld/%s/%s%%ld.T%hhu",
                       LS_DIR_STR, MINI_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, OP_KEY_STR, META_MACRO_DIR_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(judge_path, format, &ls_id, tablet_name, &op_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(judge_path), K(path), K(format), K(num));
    } else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) {
      LOG_WARN("fail to get ls inner tablet id", KR(ret), K(tablet_name));
    } else {
      is_inner_tablet = true;
    }
  } else {
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s/%%ld/%%ld/%s/%s/%s_%%ld/%s/%s%%ld.T%hhu",
                TABLET_DIR_STR, MINI_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, OP_KEY_STR, META_MACRO_DIR_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(judge_path, format, &tablet_id, &reorganization_scn, &op_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(judge_path), K(path), K(format), K(num));
    } else {
      is_inner_tablet = false;
    }
  }
  if (OB_SUCC(ret)) {
    if (is_inner_tablet) {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id((op_id << 32) + macro_seq_id);
      macro_id.set_meta_ls_id(ls_id);
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id((op_id << 32) + macro_seq_id);
      macro_id.set_reorganization_scn(reorganization_scn);
    }
  }
  return ret;
}
int ObSharedMiniMetaMacroType::local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  bool is_inner_tablet = false;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t ls_id = 0;
  char tablet_name_part1[256] = {0};
  char tablet_name_part2[256] = {0};
  char tablet_name[512] = {0};
  int64_t op_id = 0;
  int64_t macro_seq_id = 0;
  const char *judge_path = nullptr;
  if (OB_ISNULL(judge_path = ObString(path).reverse_find('/', 3))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (NULL != STRSTR(judge_path, LS_DIR_STR)) {
    const char *sub_path = nullptr;
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 2))) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
    } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld/%%[^_]_%%[^_]_%s%%ld%s%%ld.T%hhu",
                       OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &ls_id, tablet_name_part1, tablet_name_part2, &op_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(5 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else if (OB_FAIL(databuff_printf(tablet_name, sizeof(tablet_name), "%s_%s", tablet_name_part1, tablet_name_part2))) {
      LOG_WARN("fail to databuff printf", KR(ret), K(tablet_name_part1), K(tablet_name_part2));
    } else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) {
      LOG_WARN("fail to get ls inner tablet id", KR(ret), K(tablet_name));
    } else {
      is_inner_tablet = true;
    }
  } else {
    const char *sub_path = nullptr;
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
    } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu",
                TABLET_KEY_STR, REORG_KEY_STR, OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn, &op_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      is_inner_tablet = false;
    }
  }
  if (OB_SUCC(ret)) {
    if (is_inner_tablet) {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id((op_id << 32) + macro_seq_id);
      macro_id.set_meta_ls_id(ls_id);
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id((op_id << 32) + macro_seq_id);
      macro_id.set_reorganization_scn(reorganization_scn);
    }
  }
  return ret;
}
int ObSharedMiniMetaMacroType::create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id,
  const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_DIR_MGR.create_shared_mini_ls_id_dir(tenant_id, tenant_epoch_id, file_id.meta_ls_id()))) {
    LOG_WARN("fail to create shared mini ls id dir", KR(ret));
  }
  return ret;
}
int ObSharedMiniMetaMacroType::get_effective_tablet_id(const MacroBlockId &macro_id,
  uint64_t &effective_tablet_id) const
{
  int ret = OB_SUCCESS;
  size_t pos = 8; // offsetof(MacroBlockId, second_id_);
  size_t size = sizeof(uint64_t);
  if (pos + size > sizeof(MacroBlockId)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pos), K(size));
  } else {
    const char* macro_id_bytes = reinterpret_cast<const char*>(&macro_id);
    effective_tablet_id = 0;
    memcpy(&effective_tablet_id, macro_id_bytes + pos, size);
  }
  return ret;
}
#endif

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
#ifdef OB_BUILD_SHARED_STORAGE
int ObSharedMinorDataMacroType::to_local_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // inner_tablet: tenant_id_epoch_id/shared_minor_macro_cache/ls/ls_id/tablet_name_op%ldseq%ld
  // user_tablet: tenant_id_epoch_id/shared_minor_macro_cache/scatter_id/tablet%ldreorg%ldop%ldseq%ld
  if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%s/%ld/%s_%s%ld%s%ld",
                OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, SHARED_MINOR_MACRO_CACHE_DIR_STR,
                LS_DIR_STR, file_id.meta_ls_id(), get_ls_inner_tablet_name_(file_id.second_id()), OP_KEY_STR,
                (file_id.third_id() >> 32)/*op_id*/, SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF)/*macro_seq_id*/))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld",
                OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
                SHARED_MINOR_MACRO_CACHE_DIR_STR, (file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM),
                TABLET_KEY_STR, file_id.second_id(), REORG_KEY_STR, file_id.reorganization_scn(), OP_KEY_STR,
                (file_id.third_id() >> 32)/*op_id*/, SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF)/*macro_seq_id*/))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  }
  return ret;
}
int ObSharedMinorDataMacroType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // inner_tablet:cluster_id/tenant_id/ls/ls_id/tablet_name/minor/sstable/op_id/data/seq%ld
  // user_tablet:cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/minor/sstable/op_id/data/seq%ld
  if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%s/%s/%s/op_%ld/%s/%s%ld",
                object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id, LS_DIR_STR,
                file_id.meta_ls_id(), get_ls_inner_tablet_name_(file_id.second_id()), MINOR_DIR_STR,
                SHARED_TABLET_SSTABLE_DIR_STR, (file_id.third_id() >> 32)/*op_id*/, DATA_MACRO_DIR_STR,
                SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF) /*macro_seq_id*/))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%ld/%s/%s/op_%ld/%s/%s%ld",
                object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id, TABLET_DIR_STR,
                file_id.second_id(), file_id.reorganization_scn(), MINOR_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR,
                (file_id.third_id() >> 32)/*op_id*/, DATA_MACRO_DIR_STR, SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF) /*macro_seq_id*/))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  }
  return ret;
}

int ObSharedMinorDataMacroType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  bool is_inner_tablet = false;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t ls_id = 0;
  char tablet_name[512] = {0};
  int64_t op_id = 0;
  int64_t macro_seq_id = 0;
  const char *judge_path = nullptr;
  // inner_tablet: /ls/ls_id/tablet_name/minor/sstable/op_id/data/seq%ld
  // user_tablet: /tablet/tablet_id/reorganization_scn/minor/sstable/op_id/data/seq%ld
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(judge_path = ObString(path).reverse_find('/', 8))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (NULL != STRSTR(judge_path, LS_DIR_STR)) {
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s/%%ld/%[^/]/%s/%s/%s_%%ld/%s/%s%%ld.T%hhu",
                       LS_DIR_STR, MINOR_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, OP_KEY_STR, DATA_MACRO_DIR_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(judge_path, format, &ls_id, tablet_name, &op_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(judge_path), K(path), K(format), K(num));
    } else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) {
      LOG_WARN("fail to get ls inner tablet id", KR(ret), K(tablet_name));
    } else {
      is_inner_tablet = true;
    }
  } else {
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s/%%ld/%%ld/%s/%s/%s_%%ld/%s/%s%%ld.T%hhu",
                TABLET_DIR_STR, MINOR_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, OP_KEY_STR, DATA_MACRO_DIR_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(judge_path, format, &tablet_id, &reorganization_scn, &op_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(judge_path), K(path), K(format), K(num));
    } else {
      is_inner_tablet = false;
    }
  }
  if (OB_SUCC(ret)) {
    if (is_inner_tablet) {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id((op_id << 32) + macro_seq_id);
      macro_id.set_meta_ls_id(ls_id);
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id((op_id << 32) + macro_seq_id);
      macro_id.set_reorganization_scn(reorganization_scn);
    }
  }
  return ret;
}
int ObSharedMinorDataMacroType::local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  bool is_inner_tablet = false;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t ls_id = 0;
  char tablet_name_part1[256] = {0};
  char tablet_name_part2[256] = {0};
  char tablet_name[512] = {0};
  int64_t op_id = 0;
  int64_t macro_seq_id = 0;
  const char *judge_path = nullptr;
  if (OB_ISNULL(judge_path = ObString(path).reverse_find('/', 3))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (NULL != STRSTR(judge_path, LS_DIR_STR)) {
    const char *sub_path = nullptr;
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 2))) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
    } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld/%%[^_]_%%[^_]_%s%%ld%s%%ld.T%hhu",
                       OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &ls_id, tablet_name_part1, tablet_name_part2, &op_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(5 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else if (OB_FAIL(databuff_printf(tablet_name, sizeof(tablet_name), "%s_%s", tablet_name_part1, tablet_name_part2))) {
      LOG_WARN("fail to databuff printf", KR(ret), K(tablet_name_part1), K(tablet_name_part2));
    } else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) {
      LOG_WARN("fail to get ls inner tablet id", KR(ret), K(tablet_name));
    } else {
      is_inner_tablet = true;
    }
  } else {
    const char *sub_path = nullptr;
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
    } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu",
                        TABLET_KEY_STR, REORG_KEY_STR, OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn, &op_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      is_inner_tablet = false;
    }
  }
  if (OB_SUCC(ret)) {
    if (is_inner_tablet) {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id((op_id << 32) + macro_seq_id);
      macro_id.set_meta_ls_id(ls_id);
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id((op_id << 32) + macro_seq_id);
      macro_id.set_reorganization_scn(reorganization_scn);
    }
  }
  return ret;
}
int ObSharedMinorDataMacroType::create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id,
  const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_DIR_MGR.create_shared_minor_ls_id_dir(tenant_id, tenant_epoch_id, file_id.meta_ls_id()))) {
    LOG_WARN("fail to create shared minor ls id dir", KR(ret));
  }
  return ret;
}
int ObSharedMinorDataMacroType::get_effective_tablet_id(const MacroBlockId &macro_id,
  uint64_t &effective_tablet_id) const
{
  int ret = OB_SUCCESS;
  size_t pos = 8; // offsetof(MacroBlockId, second_id_);
  size_t size = sizeof(uint64_t);
  if (pos + size > sizeof(MacroBlockId)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pos), K(size));
  } else {
    const char* macro_id_bytes = reinterpret_cast<const char*>(&macro_id);
    effective_tablet_id = 0;
    memcpy(&effective_tablet_id, macro_id_bytes + pos, size);
  }
  return ret;
}
#endif

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
#ifdef OB_BUILD_SHARED_STORAGE
int ObSharedMinorMetaMacroType::to_local_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // inner_tablet:tenant_id_epoch_id/shared_minor_macro_cache/ls/ls_id/tablet_name_op%ldseq%ld
  // user_tablet:tenant_id_epoch_id/shared_minor_macro_cache/scatter_id/tablet%ldreorg%ldop%ldseq%ld
  if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%s/%ld/%s_%s%ld%s%ld",
                OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, SHARED_MINOR_MACRO_CACHE_DIR_STR,
                LS_DIR_STR, file_id.meta_ls_id(), get_ls_inner_tablet_name_(file_id.second_id()), OP_KEY_STR,
                (file_id.third_id() >> 32)/*op_id*/, SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF)/*macro_seq_id*/))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld",
                OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
                SHARED_MINOR_MACRO_CACHE_DIR_STR, (file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM),
                TABLET_KEY_STR, file_id.second_id(), REORG_KEY_STR, file_id.reorganization_scn(), OP_KEY_STR,
                (file_id.third_id() >> 32)/*op_id*/, SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF)/*macro_seq_id*/))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  }
  return ret;
}
int ObSharedMinorMetaMacroType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // inner_tablet:cluster_id/tenant_id/ls/ls_id/tablet_name/minor/sstable/op_id/meta/seq%ld
  // user_tablet:cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/minor/sstable/op_id/meta/seq%ld
  if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%s/%s/%s/op_%ld/%s/%s%ld",
                object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id, LS_DIR_STR,
                file_id.meta_ls_id(), get_ls_inner_tablet_name_(file_id.second_id()), MINOR_DIR_STR,
                SHARED_TABLET_SSTABLE_DIR_STR, (file_id.third_id() >> 32)/*op_id*/, META_MACRO_DIR_STR,
                SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF) /*macro_seq_id*/))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%ld/%s/%s/op_%ld/%s/%s%ld",
                object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id, TABLET_DIR_STR,
                file_id.second_id(), file_id.reorganization_scn(), MINOR_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR,
                (file_id.third_id() >> 32)/*op_id*/, META_MACRO_DIR_STR, SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF) /*macro_seq_id*/))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  }
  return ret;
}

int ObSharedMinorMetaMacroType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  bool is_inner_tablet = false;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t ls_id = 0;
  char tablet_name[512] = {0};
  int64_t op_id = 0;
  int64_t macro_seq_id = 0;
  const char *judge_path = nullptr;
  // inner_tablet: /ls/ls_id/tablet_name/minor/sstable/op_id/meta/seq%ld
  // user_tablet: /tablet/tablet_id/reorganization_scn/minor/sstable/op_id/meta/seq%ld
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(judge_path = ObString(path).reverse_find('/', 8))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (NULL != STRSTR(judge_path, LS_DIR_STR)) {
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s/%%ld/%[^/]/%s/%s/%s_%%ld/%s/%s%%ld.T%hhu",
                       LS_DIR_STR, MINOR_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, OP_KEY_STR, META_MACRO_DIR_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(judge_path, format, &ls_id, tablet_name, &op_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(judge_path), K(path), K(format), K(num));
    } else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) {
      LOG_WARN("fail to get ls inner tablet id", KR(ret), K(tablet_name));
    } else {
      is_inner_tablet = true;
    }
  } else {
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s/%%ld/%%ld/%s/%s/%s_%%ld/%s/%s%%ld.T%hhu",
                TABLET_DIR_STR, MINOR_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, OP_KEY_STR, META_MACRO_DIR_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(judge_path, format, &tablet_id, &reorganization_scn, &op_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(judge_path), K(path), K(format), K(num));
    } else {
      is_inner_tablet = false;
    }
  }
  if (OB_SUCC(ret)) {
    if (is_inner_tablet) {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id((op_id << 32) + macro_seq_id);
      macro_id.set_meta_ls_id(ls_id);
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id((op_id << 32) + macro_seq_id);
      macro_id.set_reorganization_scn(reorganization_scn);
    }
  }
  return ret;
}
int ObSharedMinorMetaMacroType::local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  bool is_inner_tablet = false;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t ls_id = 0;
  char tablet_name_part1[256] = {0};
  char tablet_name_part2[256] = {0};
  char tablet_name[512] = {0};
  int64_t op_id = 0;
  int64_t macro_seq_id = 0;
  const char *judge_path = nullptr;
  if (OB_ISNULL(judge_path = ObString(path).reverse_find('/', 3))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (NULL != STRSTR(judge_path, LS_DIR_STR)) {
    const char *sub_path = nullptr;
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 2))) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
    } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld/%%[^_]_%%[^_]_%s%%ld%s%%ld.T%hhu",
                        OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &ls_id, tablet_name_part1, tablet_name_part2, &op_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(5 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else if (OB_FAIL(databuff_printf(tablet_name, sizeof(tablet_name), "%s_%s", tablet_name_part1, tablet_name_part2))) {
      LOG_WARN("fail to databuff printf", KR(ret), K(tablet_name_part1), K(tablet_name_part2));
    } else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) {
      LOG_WARN("fail to get ls inner tablet id", KR(ret), K(tablet_name));
    } else {
      is_inner_tablet = true;
    }
  } else {
    const char *sub_path = nullptr;
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
    } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu",
                        TABLET_KEY_STR, REORG_KEY_STR, OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn, &op_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      is_inner_tablet = false;
    }
  }
  if (OB_SUCC(ret)) {
    if (is_inner_tablet) {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id((op_id << 32) + macro_seq_id);
      macro_id.set_meta_ls_id(ls_id);
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id((op_id << 32) + macro_seq_id);
      macro_id.set_reorganization_scn(reorganization_scn);
    }
  }
  return ret;
}
int ObSharedMinorMetaMacroType::create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id,
  const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_DIR_MGR.create_shared_minor_ls_id_dir(tenant_id, tenant_epoch_id, file_id.meta_ls_id()))) {
    LOG_WARN("fail to create shared minor ls id dir", KR(ret));
  }
  return ret;
}
int ObSharedMinorMetaMacroType::get_effective_tablet_id(const MacroBlockId &macro_id,
  uint64_t &effective_tablet_id) const
{
  int ret = OB_SUCCESS;
  size_t pos = 8; // offsetof(MacroBlockId, second_id_);
  size_t size = sizeof(uint64_t);
  if (pos + size > sizeof(MacroBlockId)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pos), K(size));
  } else {
    const char* macro_id_bytes = reinterpret_cast<const char*>(&macro_id);
    effective_tablet_id = 0;
    memcpy(&effective_tablet_id, macro_id_bytes + pos, size);
  }
  return ret;
}
#endif

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
#ifdef OB_BUILD_SHARED_STORAGE
int ObSharedMajorDataMacroType::to_local_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // tenant_id_epoch_id/shared_major_macro_cache/scatter_id/tablet%ldreorg%ldcg%ldseq%ld
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld",
                    OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
                    MAJOR_DATA_DIR_STR, (file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM),
                    TABLET_KEY_STR, file_id.second_id(), REORG_KEY_STR, file_id.reorganization_scn(),
                    CG_KEY_STR, file_id.column_group_id(), SEQ_KEY_STR, file_id.third_id()))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
int ObSharedMajorDataMacroType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/major/sstable/cg_id/data/seq%ld
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%ld/%s/%s/%s_%ld/%s/%s%ld",
                object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id, TABLET_DIR_STR,
                file_id.second_id(), file_id.reorganization_scn(), MAJOR_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR,
                COLUMN_GROUP_STR, file_id.column_group_id(), DATA_MACRO_DIR_STR, SEQ_KEY_STR, file_id.third_id()))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

int ObSharedMajorDataMacroType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  // /tablet_id/reorganization_scn/major/sstable/cg_id/data/seq%ld
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 7))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else {
    int64_t tablet_id = 0;
    int64_t reorganization_scn = 0;
    int64_t cg_id = 0;
    int64_t macro_seq_id = 0;
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld/%%ld/%s/%s/%s_%%ld/%s/%s%%ld.T%hhu",
                MAJOR_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, COLUMN_GROUP_STR, DATA_MACRO_DIR_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn, &cg_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_column_group_id(cg_id);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id(macro_seq_id);
      macro_id.set_reorganization_scn(reorganization_scn);
    }
  }
  return ret;
}
int ObSharedMajorDataMacroType::local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else {
    int64_t tablet_id = 0;
    int64_t reorganization_scn = 0;
    int64_t cg_id = 0;
    int64_t macro_seq_id = 0;
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu",
                TABLET_KEY_STR, REORG_KEY_STR, CG_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn, &cg_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_column_group_id(cg_id);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id(macro_seq_id);
      macro_id.set_reorganization_scn(reorganization_scn);
    }
  }
  return ret;
}
int ObSharedMajorDataMacroType::get_effective_tablet_id(const MacroBlockId &macro_id,
  uint64_t &effective_tablet_id) const
{
  int ret = OB_SUCCESS;
  size_t pos = 8; // offsetof(MacroBlockId, second_id_);
  size_t size = sizeof(uint64_t);
  if (pos + size > sizeof(MacroBlockId)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pos), K(size));
  } else {
    const char* macro_id_bytes = reinterpret_cast<const char*>(&macro_id);
    effective_tablet_id = 0;
    memcpy(&effective_tablet_id, macro_id_bytes + pos, size);
  }
  return ret;
}
#endif

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
#ifdef OB_BUILD_SHARED_STORAGE
int ObSharedMajorMetaMacroType::to_local_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // tenant_id_epoch_id/shared_major_macro_cache/scatter_id/tablet%ldreorg%ldcg%ldseq%ld
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld",
                    OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
                    MAJOR_DATA_DIR_STR, (file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM),
                    TABLET_KEY_STR, file_id.second_id(), REORG_KEY_STR, file_id.reorganization_scn(),
                    CG_KEY_STR, file_id.column_group_id(), SEQ_KEY_STR, file_id.third_id()))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
int ObSharedMajorMetaMacroType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/major/sstable/cg_id/meta/seq%ld
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%ld/%s/%s/%s_%ld/%s/%s%ld",
                object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id, TABLET_DIR_STR,
                file_id.second_id(), file_id.reorganization_scn(), MAJOR_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR,
                COLUMN_GROUP_STR, file_id.column_group_id(), META_MACRO_DIR_STR, SEQ_KEY_STR, file_id.third_id()))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

int ObSharedMajorMetaMacroType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  // /tablet_id/reorganization_scn/major/sstable/cg_id/meta/seq%ld
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 7))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else {
    int64_t tablet_id = 0;
    int64_t reorganization_scn = 0;
    int64_t cg_id = 0;
    int64_t macro_seq_id = 0;
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld/%%ld/%s/%s/%s_%%ld/%s/%s%%ld.T%hhu",
                MAJOR_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, COLUMN_GROUP_STR, META_MACRO_DIR_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn, &cg_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_column_group_id(cg_id);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id(macro_seq_id);
      macro_id.set_reorganization_scn(reorganization_scn);
    }
  }
  return ret;
}
int ObSharedMajorMetaMacroType::local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else {
    int64_t tablet_id = 0;
    int64_t reorganization_scn = 0;
    int64_t cg_id = 0;
    int64_t macro_seq_id = 0;
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu",
                TABLET_KEY_STR, REORG_KEY_STR, CG_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn, &cg_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_column_group_id(cg_id);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id(macro_seq_id);
      macro_id.set_reorganization_scn(reorganization_scn);
    }
  }
  return ret;
}
int ObSharedMajorMetaMacroType::get_effective_tablet_id(const MacroBlockId &macro_id,
  uint64_t &effective_tablet_id) const
{
  int ret = OB_SUCCESS;
  size_t pos = 8; // offsetof(MacroBlockId, second_id_);
  size_t size = sizeof(uint64_t);
  if (pos + size > sizeof(MacroBlockId)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pos), K(size));
  } else {
    const char* macro_id_bytes = reinterpret_cast<const char*>(&macro_id);
    effective_tablet_id = 0;
    memcpy(&effective_tablet_id, macro_id_bytes + pos, size);
  }
  return ret;
}
#endif

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
#ifdef OB_BUILD_SHARED_STORAGE
int ObTmpFileType::to_local_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // tenant_id_epoch_id/tmp_data/tmp_file_id/seg%ld
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%ld/%s%ld",
              OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, TMP_DATA_DIR_STR,
              file_id.second_id(), SEG_KEY_STR, file_id.third_id()))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
int ObTmpFileType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // cluster_id/server_id/tenant_id_epoch_id/tmp_data/tmp_file_id/seg%ld
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%lu_%ld/%s/%ld/%s%ld",
                    object_storage_root_dir, CLUSTER_DIR_STR, cluster_id,
                    SERVER_DIR_STR, server_id, tenant_id, tenant_epoch_id,
                    TMP_DATA_DIR_STR, file_id.second_id(), SEG_KEY_STR, file_id.third_id()))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

int ObTmpFileType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  return local_path_to_macro_id(path, macro_id);
}
int ObTmpFileType::local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 2))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else {
    int64_t tmp_file_id = 0;
    int64_t segment_id = 0;
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld/%s%%ld.T%hhu", SEG_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &tmp_file_id, &segment_id))) {
    } else if (OB_UNLIKELY(2 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tmp_file_id);
      macro_id.set_third_id(segment_id);
    }
  }
  return ret;
}
int ObTmpFileType::get_parent_dir(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id,
  const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // tenant_id_epoch_id/tmp_data/tmp_file_id/
  if (OB_FAIL(OB_DIR_MGR.get_local_tmp_file_dir(path, length, tenant_id, tenant_epoch_id, file_id.second_id()))) {
    LOG_WARN("fail to get local tmp file dir", KR(ret));
  }
  return ret;
}
int ObTmpFileType::create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id,
  const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;

  // ERRSIM for testing: simulate disk space exhaustion during parent dir creation
  // This allows testing the scenario where file allocation succeeds but parent dir creation fails
  ret = OB_E(EventTable::EN_SHARED_STORAGE_DIR_DISK_OUTOF_SPACE_ERR) OB_SUCCESS;
  if (OB_FAIL(ret)) {
    ret = OB_SERVER_OUTOF_DISK_SPACE;
    LOG_INFO("ERRSIM: simulating disk space exhaustion during parent dir creation", KR(ret), K(file_id));
  } else if (OB_FAIL(OB_DIR_MGR.create_tmp_file_dir(tenant_id, tenant_epoch_id, file_id.second_id()))) {
    LOG_WARN("fail to create tmp file dir", KR(ret));
  }
  return ret;
}
#endif

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
#ifdef OB_BUILD_SHARED_STORAGE
int ObServerMetaType::to_local_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // super_block
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s", OB_DIR_MGR.get_local_cache_root_dir(),
              get_type_str()))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
int ObServerMetaType::get_parent_dir(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id,
  const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // local_cache_root_dir
  if (OB_FAIL(databuff_printf(path, length, pos, "%s", OB_DIR_MGR.get_local_cache_root_dir()))) {
    LOG_WARN("fail to get local cache root dir", KR(ret));
  }
  return ret;
}
#endif

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
  // second_id:ls_id, third_id:tablet_id, fourth_id:meta_transfer_seq+meta_version_id
  return (file_id.second_id() >= 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() > 0) &&
         (file_id.meta_transfer_epoch() >= 0) && (file_id.meta_version_id() >= 0);
}
#ifdef OB_BUILD_SHARED_STORAGE
int ObPrivateTabletMetaType::to_local_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // tenant_id_epoch_id/ls/ls_id_epoch_id/tablet_meta/scatter_id/tablet_id/transfer_seq/ver%ld
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%ld_%ld/%s/%02ld/%ld/%ld/%s%ld",
              OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, LS_DIR_STR, file_id.second_id(),
              ls_epoch_id, TABLET_META_DIR_STR, (file_id.third_id() % ObDirManager::PRIVATE_TABLET_META_SCATTER_DIR_NUM),
              file_id.third_id(), file_id.meta_transfer_epoch(), VER_KEY_STR, file_id.meta_version_id()))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
int ObPrivateTabletMetaType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // cluster_id/server_id/tenant_id_epoch_id/ls/ls_id_epoch_id/tablet_meta/tablet_id/transfer_seq/ver%ld
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%lu_%ld/%s/%ld_%ld/%s/%ld/%ld/%s%ld",
              object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR, server_id,
              tenant_id, tenant_epoch_id, LS_DIR_STR, file_id.second_id(), ls_epoch_id, TABLET_META_DIR_STR,
              file_id.third_id(), file_id.meta_transfer_epoch(), VER_KEY_STR, file_id.meta_version_id()))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

int ObPrivateTabletMetaType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  // /ls_id_epoch_id/tablet_meta/tablet_id/transfer_seq/ver%ld
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 5))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else {
    int64_t ls_id = 0;
    int64_t tablet_id = 0;
    int64_t meta_transfer_seq = 0;
    int64_t meta_version_id = 0;
    int64_t ls_epoch_id = 0;
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld_%%ld/%s/%%ld/%%ld/%s%%ld.T%hhu",
                TABLET_META_DIR_STR, VER_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &ls_id, &ls_epoch_id, &tablet_id, &meta_transfer_seq, &meta_version_id))) {
    } else if (OB_UNLIKELY(5 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(ls_id);
      macro_id.set_third_id(tablet_id);
      macro_id.set_meta_transfer_epoch(meta_transfer_seq);
      macro_id.set_meta_version_id(meta_version_id);
    }
  }
  return ret;
}
int ObPrivateTabletMetaType::local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 6))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else {
    int64_t ls_id = 0;
    int64_t epoch_id = 0;
    int64_t scatter_id = 0;
    int64_t tablet_id = 0;
    int64_t meta_transfer_seq = 0;
    int64_t meta_version_id = 0;
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld_%%ld/%s/%%ld/%%ld/%%ld/%s%%ld.T%hhu",
                TABLET_META_DIR_STR, VER_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &ls_id, &epoch_id, &scatter_id, &tablet_id, &meta_transfer_seq, &meta_version_id))) {
    } else if (OB_UNLIKELY(6 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(ls_id);
      macro_id.set_third_id(tablet_id);
      macro_id.set_meta_transfer_epoch(meta_transfer_seq);
      macro_id.set_meta_version_id(meta_version_id);
    }
  }
  return ret;
}
int ObPrivateTabletMetaType::get_parent_dir(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id,
  const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // tenant_id_epoch_id/ls/ls_id_epoch_id/tablet_meta/scatter_id/tablet_id/transfer_seq
  if (OB_FAIL(OB_DIR_MGR.get_tablet_meta_tablet_id_transfer_seq_dir(path, length, tenant_id, tenant_epoch_id,
              file_id.second_id(), ls_epoch_id, file_id.third_id(), file_id.meta_transfer_epoch()))) {
    LOG_WARN("fail to get tablet meta tablet id transfer seq dir", KR(ret));
  }
  return ret;
}
int ObPrivateTabletMetaType::create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id,
  const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_DIR_MGR.create_tablet_meta_tablet_id_transfer_seq_dir(tenant_id, tenant_epoch_id, file_id.second_id(),
              ls_epoch_id, file_id.third_id(), file_id.meta_transfer_epoch()))) {
    LOG_WARN("fail to create tablet meta tablet id transfer seq dir", KR(ret));
  }
  return ret;
}
int ObPrivateTabletMetaType::get_effective_tablet_id(const MacroBlockId &macro_id,
  uint64_t &effective_tablet_id) const
{
  int ret = OB_SUCCESS;
  size_t pos = 8; // offsetof(MacroBlockId, second_id_);
  size_t size = sizeof(uint64_t);
  if (pos + size > sizeof(MacroBlockId)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pos), K(size));
  } else {
    const char* macro_id_bytes = reinterpret_cast<const char*>(&macro_id);
    effective_tablet_id = 0;
    memcpy(&effective_tablet_id, macro_id_bytes + pos, size);
  }
  return ret;
}
#endif

int ObPrivateTabletMetaType::opt_to_string(char *buf, const int64_t buf_len, int64_t &pos,
  const ObStorageObjectOpt &opt) const
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s (ls_id=%lu,tablet_id=%lu,version=%lu,transfer_seq=%lu)",
            get_type_str(), opt.ss_private_tablet_opt_.ls_id_, opt.ss_private_tablet_opt_.tablet_id_,
            opt.ss_private_tablet_opt_.version_, opt.ss_private_tablet_opt_.tablet_transfer_seq_))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_type_str()),
            K(opt.ss_private_tablet_opt_.ls_id_), K(opt.ss_private_tablet_opt_.tablet_id_),
            K(opt.ss_private_tablet_opt_.version_), K(opt.ss_private_tablet_opt_.tablet_transfer_seq_));
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
  object_id.set_meta_transfer_epoch(opt.ss_private_tablet_opt_.tablet_transfer_seq_);
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
#ifdef OB_BUILD_SHARED_STORAGE
int ObPrivateSlogFileType::to_local_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // server_slog/seq%ld or tenant_id_epoch_id/slog/seq%ld
  if (OB_SERVER_TENANT_ID == file_id.second_id()) {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%s/%s%ld",
                OB_DIR_MGR.get_local_cache_root_dir(), SERVER_DIR_STR, SLOG_STR, SEQ_KEY_STR, file_id.fourth_id()))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%s%ld", OB_DIR_MGR.get_local_cache_root_dir(),
                tenant_id, file_id.third_id(), SLOG_STR, SEQ_KEY_STR, file_id.fourth_id()))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  }
  return ret;
}
int ObPrivateSlogFileType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // cluster_id/server_id/tenant_id_epoch_id/slog/seq%ld or cluster_id/server_id/server_slog/seq%ld
  if (OB_SERVER_TENANT_ID == file_id.second_id()) {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s_%s/%s%ld",
                object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR,
                server_id, SERVER_DIR_STR, SLOG_STR, SEQ_KEY_STR, file_id.fourth_id()))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%lu_%ld/%s/%s%ld",
                object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR,
                server_id, tenant_id, file_id.third_id(), SLOG_STR, SEQ_KEY_STR, file_id.fourth_id()))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  }
  return ret;
}

int ObPrivateSlogFileType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  // /tenant_id_epoch_id/slog/seq%ld or /server_id/server_slog/seq%ld
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  char tablet_name[512] = {0};
  int64_t macro_seq_id = 0;
  int64_t third_id = 0;
  const char *judge_path = nullptr;
  bool is_500_tenant = false;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(judge_path = ObString(path).reverse_find('/', 3))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (NULL != STRSTR(judge_path, SERVER_DIR_STR)) { // 500 tenant
    const char *sub_path = nullptr;
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 2))) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
    } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s_%s/%s%%ld.T%hhu",
                       SERVER_DIR_STR, SLOG_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &macro_seq_id))) {
    } else if (OB_UNLIKELY(1 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      is_500_tenant = true;
    }
  } else {
    int64_t tenant_id = 0;
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%lu_%%ld/%s/%s%%ld.T%hhu",
                SLOG_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(judge_path, format, &tenant_id, &third_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(3 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(judge_path), K(path), K(format), K(num));
    } else {
      is_500_tenant = false;
    }
  }
  if (OB_SUCC(ret)) {
    if (is_500_tenant) {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(OB_SERVER_TENANT_ID);
      macro_id.set_fourth_id(macro_seq_id);
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(0);
      macro_id.set_third_id(third_id);
      macro_id.set_fourth_id(macro_seq_id);
    }
  }
  return ret;
}
int ObPrivateSlogFileType::local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 3))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else {
    int64_t tenant_id = 0;
    int64_t epoch_id = 0;
    int64_t object_id = 0;
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%lu_%%ld/%s/%s%%ld.T%hhu",
                SLOG_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &tenant_id, &epoch_id, &object_id))) {
    } else if (OB_UNLIKELY(3 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_third_id(epoch_id);
      macro_id.set_fourth_id(object_id);
    }
  }
  return ret;
}
int ObPrivateSlogFileType::get_parent_dir(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id,
  const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // tenant_id_epoch_id/slog/
  if (OB_FAIL(OB_DIR_MGR.get_tenant_slog_dir(path, length, file_id.second_id(), file_id.third_id()))) {
    LOG_WARN("fail to get tenant slog dir", KR(ret));
  }
  return ret;
}
int ObPrivateSlogFileType::create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id,
  const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_DIR_MGR.create_slog_dir(file_id.second_id(), file_id.third_id()))) {
    LOG_WARN("fail to create slog dir", KR(ret));
  }
  return ret;
}
#endif

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
#ifdef OB_BUILD_SHARED_STORAGE
int ObPrivateCkptFileType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // cluster_id/server_id/tenant_id_epoch_id/ckpt/object_id or cluster_id/server_id/server_ckpt/object_id
  if (OB_SERVER_TENANT_ID == file_id.second_id()) {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s_%s/%ld", object_storage_root_dir,
                CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR, server_id, SERVER_DIR_STR, CKPT_STR, file_id.fourth_id()))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  } else {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%lu_%ld/%s/%ld",
                object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR, server_id, tenant_id,
                file_id.third_id(), CKPT_STR, file_id.fourth_id()))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    }
  }
  return ret;
}

int ObPrivateCkptFileType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  // /tenant_id_epoch_id/ckpt/object_id or /server_id/server_ckpt/object_id
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  char tablet_name[512] = {0};
  int64_t ckpt_id = 0;
  int64_t third_id = 0;
  const char *judge_path = nullptr;
  bool is_500_tenant = false;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(judge_path = ObString(path).reverse_find('/', 3))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (NULL != STRSTR(judge_path, SERVER_DIR_STR)) { // 500 tenant
    const char *sub_path = nullptr;
    if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 2))) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
    } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s_%s/%%ld.T%hhu",
                       SERVER_DIR_STR, CKPT_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &ckpt_id))) {
    } else if (OB_UNLIKELY(1 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      is_500_tenant = true;
    }
  } else {
    int64_t tenant_id = 0;
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%lu_%%ld/%s/%%ld.T%hhu",
                CKPT_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(judge_path, format, &tenant_id, &third_id, &ckpt_id))) {
    } else if (OB_UNLIKELY(3 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(judge_path), K(path), K(format), K(num));
    } else {
      is_500_tenant = false;
    }
  }
  if (OB_SUCC(ret)) {
    if (is_500_tenant) {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(OB_SERVER_TENANT_ID);
      macro_id.set_fourth_id(ckpt_id);
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(0);
      macro_id.set_third_id(third_id);
      macro_id.set_fourth_id(ckpt_id);
    }
  }
  return ret;
}
#endif

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
#ifdef OB_BUILD_SHARED_STORAGE
int ObMajorPrewarmDataType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/major/prewarm_info/scn%ld
  return prewarm_file_to_remote_path_format(path, length, pos, file_id, object_storage_root_dir, cluster_id, tenant_id,
                                            tenant_epoch_id, server_id);
}

int ObMajorPrewarmDataType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  return prewarm_file_remote_path_to_macro_id(path, type_, macro_id);
}
#endif

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
#ifdef OB_BUILD_SHARED_STORAGE
int ObMajorPrewarmDataIndexType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/major/prewarm_info/scn%ld
  return prewarm_file_to_remote_path_format(path, length, pos, file_id, object_storage_root_dir, cluster_id, tenant_id,
                                            tenant_epoch_id, server_id);
}

int ObMajorPrewarmDataIndexType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  return prewarm_file_remote_path_to_macro_id(path, type_, macro_id);
}
#endif

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
#ifdef OB_BUILD_SHARED_STORAGE
int ObMajorPrewarmMetaType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
// cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/major/prewarm_info/scn%ld
  return prewarm_file_to_remote_path_format(path, length, pos, file_id, object_storage_root_dir, cluster_id, tenant_id,
                                            tenant_epoch_id, server_id);
}

int ObMajorPrewarmMetaType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  return prewarm_file_remote_path_to_macro_id(path, type_, macro_id);
}
#endif

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
#ifdef OB_BUILD_SHARED_STORAGE
int ObMajorPrewarmMetaIndexType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/major/prewarm_info/scn%ld
  return prewarm_file_to_remote_path_format(path, length, pos, file_id, object_storage_root_dir, cluster_id, tenant_id,
                                            tenant_epoch_id, server_id);
}

int ObMajorPrewarmMetaIndexType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  return prewarm_file_remote_path_to_macro_id(path, type_, macro_id);
}
#endif

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
#ifdef OB_BUILD_SHARED_STORAGE
int ObTenantDiskSpaceMetaType::to_local_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // tenant_id_epoch_id/tenant_disk_space_meta
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%ld_%ld/%s",
              OB_DIR_MGR.get_local_cache_root_dir(), file_id.second_id(), file_id.third_id(), get_type_str()))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
int ObTenantDiskSpaceMetaType::get_parent_dir(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id,
  const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // tenant_id_epoch_id
  if (OB_FAIL(OB_DIR_MGR.get_local_tenant_dir(path, length, file_id.second_id(), file_id.third_id()))) {
    LOG_WARN("fail to get local tenant dir", KR(ret));
  }
  return ret;
}
int ObTenantDiskSpaceMetaType::create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id,
  const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_DIR_MGR.create_tenant_dir(file_id.second_id(), file_id.third_id()))) {
    LOG_WARN("fail to create tenant dir", KR(ret));
  }
  return ret;
}
#endif

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
#ifdef OB_BUILD_SHARED_STORAGE
int ObIsSharedTenantDeletedType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // cluster_id/tenant_id/is_shared_tenant_deleted
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%ld/%s",
              object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR,
              file_id.second_id(), get_type_str()))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

int ObIsSharedTenantDeletedType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  int64_t tenant_id = 0;
  const char *sub_path = nullptr;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 2))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s_%%ld/%s.T%hhu",
              TENANT_DIR_STR, get_type_str(), (uint8_t)type_))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (FALSE_IT(num = sscanf(sub_path, format, &tenant_id))) {
  } else if (OB_UNLIKELY(1 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
  } else {
    macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
    macro_id.set_storage_object_type((uint64_t)type_);
    macro_id.set_second_id(tenant_id);
  }
  return ret;
}
#endif

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
#ifdef OB_BUILD_SHARED_STORAGE
int ObUnsealedRemoteSegFileType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // cluster_id/server_id/tenant_id_epoch_id/tmp_data/tmp_file_id/seg%ldlen%ld
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%lu_%ld/%s/%ld/%s%ld%s%ld",
              object_storage_root_dir, CLUSTER_DIR_STR, cluster_id,
              SERVER_DIR_STR, server_id, tenant_id, tenant_epoch_id,
              TMP_DATA_DIR_STR, file_id.second_id(), SEG_KEY_STR, file_id.third_id(), LEN_KEY_STR, file_id.fourth_id()))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

int ObUnsealedRemoteSegFileType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  // /tmp_file_id/seg%ldlen%ld
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  int64_t second_id = 0;
  int64_t segment_id = 0;
  int64_t len_id = 0;
  const char *sub_path = nullptr;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 2))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld/%s%%ld%s%%ld.T%hhu",
                    SEG_KEY_STR, LEN_KEY_STR, (uint8_t)type_))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (FALSE_IT(num = sscanf(sub_path, format, &second_id, &segment_id, &len_id))) {
  } else if (OB_UNLIKELY(3 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
  } else {
    macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
    macro_id.set_storage_object_type((uint64_t)type_);
    macro_id.set_second_id(second_id);
    macro_id.set_third_id(segment_id);
    macro_id.set_fourth_id(len_id);
  }
  return ret;
}
#endif

/**
 * ---------------------------------------ObSharedMdsMiniDataMacroType----------------------------------------
 */

bool ObSharedMdsMiniDataMacroType::is_valid(const MacroBlockId &file_id) const
{
  // second_id:tablet_id, third_id:op_id + seq_id
  return (file_id.second_id() > 0) && (file_id.second_id() < INT64_MAX) && (file_id.third_id() >= 0);
}
#ifdef OB_BUILD_SHARED_STORAGE
int ObSharedMdsMiniDataMacroType::to_local_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // tenant_id_epoch_id/shared_mini_macro_cache/scatter_id/tablet%ldreorg%ldop%ldseq%ld
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld",
               OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
               SHARED_MINI_MACRO_CACHE_DIR_STR, (file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM),
               TABLET_KEY_STR, file_id.second_id(), REORG_KEY_STR, file_id.reorganization_scn(), OP_KEY_STR,
               (file_id.third_id() >> 32)/*op_id*/, SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF)/*macro_seq_id*/))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
int ObSharedMdsMiniDataMacroType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/mds/mini/sstable/op_id/data/seq%ld
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%ld/mds/%s/%s/op_%ld/%s/%s%ld",
             object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id, TABLET_DIR_STR,
             file_id.second_id(), file_id.reorganization_scn(), MINI_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR,
             (file_id.third_id() >> 32)/*op_id*/, DATA_MACRO_DIR_STR, SEQ_KEY_STR,
             (file_id.third_id() & 0xFFFFFFFF) /*macro_seq_id*/))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

int ObSharedMdsMiniDataMacroType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
// /tablet_id/reorganization_scn/mds/mini/sstable/op_id/data/seq%ld
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t op_id = 0;
  int64_t macro_seq_id = 0;
  const char *sub_path = nullptr;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 8))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld/%%ld/mds/%s/%s/%s_%%ld/%s/%s%%ld.T%hhu",
                     MINI_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, OP_KEY_STR, DATA_MACRO_DIR_STR, SEQ_KEY_STR, (uint8_t)type_))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn, &op_id, &macro_seq_id))) {
  } else if (OB_UNLIKELY(4 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
  } else {
    macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
    macro_id.set_storage_object_type((uint64_t)type_);
    macro_id.set_second_id(tablet_id);
    macro_id.set_third_id((op_id << 32) + macro_seq_id);
    macro_id.set_reorganization_scn(reorganization_scn);
  }
  return ret;
}
int ObSharedMdsMiniDataMacroType::local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else {
    int64_t tablet_id = 0;
    int64_t reorganization_scn = 0;
    int64_t op_id = 0;
    int64_t macro_seq_id = 0;
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu",
                TABLET_KEY_STR, REORG_KEY_STR, OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn, &op_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id((op_id << 32) + macro_seq_id);
      macro_id.set_reorganization_scn(reorganization_scn);
    }
  }
  return ret;
}
int ObSharedMdsMiniDataMacroType::get_effective_tablet_id(const MacroBlockId &macro_id,
  uint64_t &effective_tablet_id) const
{
  int ret = OB_SUCCESS;
  size_t pos = 8; // offsetof(MacroBlockId, second_id_);
  size_t size = sizeof(uint64_t);
  if (pos + size > sizeof(MacroBlockId)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pos), K(size));
  } else {
    const char* macro_id_bytes = reinterpret_cast<const char*>(&macro_id);
    effective_tablet_id = 0;
    memcpy(&effective_tablet_id, macro_id_bytes + pos, size);
  }
  return ret;
}
#endif

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
#ifdef OB_BUILD_SHARED_STORAGE
int ObSharedMdsMiniMetaMacroType::to_local_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // tenant_id_epoch_id/shared_mini_macro_cache/scatter_id/tablet%ldreorg%ldop%ldseq%ld
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld",
               OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
               SHARED_MINI_MACRO_CACHE_DIR_STR, (file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM),
               TABLET_KEY_STR, file_id.second_id(), REORG_KEY_STR, file_id.reorganization_scn(), OP_KEY_STR,
               (file_id.third_id() >> 32)/*op_id*/, SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF)/*macro_seq_id*/))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
int ObSharedMdsMiniMetaMacroType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/mds/mini/sstable/op_id/meta/seq%ld
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%ld/mds/%s/%s/op_%ld/%s/%s%ld",
             object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id, TABLET_DIR_STR,
             file_id.second_id(), file_id.reorganization_scn(), MINI_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR,
             (file_id.third_id() >> 32)/*op_id*/, META_MACRO_DIR_STR, SEQ_KEY_STR,
             (file_id.third_id() & 0xFFFFFFFF) /*macro_seq_id*/))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

int ObSharedMdsMiniMetaMacroType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t op_id = 0;
  int64_t macro_seq_id = 0;
  const char *sub_path = nullptr;
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/mds/mini/sstable/op_id/meta/seq%ld
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 8))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld/%%ld/mds/%s/%s/%s_%%ld/%s/%s%%ld.T%hhu",
                     MINI_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, OP_KEY_STR, META_MACRO_DIR_STR, SEQ_KEY_STR, (uint8_t)type_))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn, &op_id, &macro_seq_id))) {
  } else if (OB_UNLIKELY(4 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
  } else {
    macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
    macro_id.set_storage_object_type((uint64_t)type_);
    macro_id.set_second_id(tablet_id);
    macro_id.set_third_id((op_id << 32) + macro_seq_id);
    macro_id.set_reorganization_scn(reorganization_scn);
  }
  return ret;
}
int ObSharedMdsMiniMetaMacroType::local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else {
    int64_t tablet_id = 0;
    int64_t reorganization_scn = 0;
    int64_t op_id = 0;
    int64_t macro_seq_id = 0;
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu",
                TABLET_KEY_STR, REORG_KEY_STR, OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn, &op_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id((op_id << 32) + macro_seq_id);
      macro_id.set_reorganization_scn(reorganization_scn);
    }
  }
  return ret;
}
int ObSharedMdsMiniMetaMacroType::get_effective_tablet_id(const MacroBlockId &macro_id,
  uint64_t &effective_tablet_id) const
{
  int ret = OB_SUCCESS;
  size_t pos = 8; // offsetof(MacroBlockId, second_id_);
  size_t size = sizeof(uint64_t);
  if (pos + size > sizeof(MacroBlockId)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pos), K(size));
  } else {
    const char* macro_id_bytes = reinterpret_cast<const char*>(&macro_id);
    effective_tablet_id = 0;
    memcpy(&effective_tablet_id, macro_id_bytes + pos, size);
  }
  return ret;
}
#endif

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
#ifdef OB_BUILD_SHARED_STORAGE
int ObSharedMdsMinorDataMacroType::to_local_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // tenant_id_epoch_id/shared_minor_macro_cache/scatter_id/tablet%ldreorg%ldop%ldseq%ld
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld",
               OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
               SHARED_MINOR_MACRO_CACHE_DIR_STR, (file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM),
               TABLET_KEY_STR, file_id.second_id(), REORG_KEY_STR, file_id.reorganization_scn(), OP_KEY_STR,
               (file_id.third_id() >> 32)/*op_id*/, SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF)/*macro_seq_id*/))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
int ObSharedMdsMinorDataMacroType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/mds/minor/sstable/op_id/data/seq%ld
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%ld/mds/%s/%s/op_%ld/%s/%s%ld",
             object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id, TABLET_DIR_STR,
             file_id.second_id(), file_id.reorganization_scn(), MINOR_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR,
             (file_id.third_id() >> 32)/*op_id*/, DATA_MACRO_DIR_STR, SEQ_KEY_STR,
             (file_id.third_id() & 0xFFFFFFFF) /*macro_seq_id*/))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

int ObSharedMdsMinorDataMacroType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t op_id = 0;
  int64_t macro_seq_id = 0;
  const char *sub_path = nullptr;
  // /tablet_id/reorganization_scn/mds/minor/sstable/op_id/data/seq%ld
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 8))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld/%%ld/mds/%s/%s/%s_%%ld/%s/%s%%ld.T%hhu",
                     MINOR_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, OP_KEY_STR, DATA_MACRO_DIR_STR, SEQ_KEY_STR, (uint8_t)type_))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn, &op_id, &macro_seq_id))) {
  } else if (OB_UNLIKELY(4 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
  } else {
    macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
    macro_id.set_storage_object_type((uint64_t)type_);
    macro_id.set_second_id(tablet_id);
    macro_id.set_third_id((op_id << 32) + macro_seq_id);
    macro_id.set_reorganization_scn(reorganization_scn);
  }
  return ret;
}
int ObSharedMdsMinorDataMacroType::local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else {
    int64_t tablet_id = 0;
    int64_t reorganization_scn = 0;
    int64_t op_id = 0;
    int64_t macro_seq_id = 0;
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu",
                TABLET_KEY_STR, REORG_KEY_STR, OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn, &op_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id((op_id << 32) + macro_seq_id);
      macro_id.set_reorganization_scn(reorganization_scn);
    }
  }
  return ret;
}
int ObSharedMdsMinorDataMacroType::get_effective_tablet_id(const MacroBlockId &macro_id,
  uint64_t &effective_tablet_id) const
{
  int ret = OB_SUCCESS;
  size_t pos = 8; // offsetof(MacroBlockId, second_id_);
  size_t size = sizeof(uint64_t);
  if (pos + size > sizeof(MacroBlockId)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pos), K(size));
  } else {
    const char* macro_id_bytes = reinterpret_cast<const char*>(&macro_id);
    effective_tablet_id = 0;
    memcpy(&effective_tablet_id, macro_id_bytes + pos, size);
  }
  return ret;
}
#endif

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
#ifdef OB_BUILD_SHARED_STORAGE
int ObSharedMdsMinorMetaMacroType::to_local_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // tenant_id_epoch_id/shared_minor_macro_cache/scatter_id/tablet%ldreorg%ldop%ldseq%ld
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld",
               OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
               SHARED_MINOR_MACRO_CACHE_DIR_STR, (file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM),
               TABLET_KEY_STR, file_id.second_id(), REORG_KEY_STR, file_id.reorganization_scn(), OP_KEY_STR,
               (file_id.third_id() >> 32)/*op_id*/, SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF)/*macro_seq_id*/))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
int ObSharedMdsMinorMetaMacroType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/mds/minor/sstable/op_id/meta/seq%ld
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%ld/mds/%s/%s/op_%ld/%s/%s%ld",
             object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id, TABLET_DIR_STR,
             file_id.second_id(), file_id.reorganization_scn(), MINOR_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR,
             (file_id.third_id() >> 32)/*op_id*/, META_MACRO_DIR_STR, SEQ_KEY_STR,
             (file_id.third_id() & 0xFFFFFFFF) /*macro_seq_id*/))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

int ObSharedMdsMinorMetaMacroType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t op_id = 0;
  int64_t macro_seq_id = 0;
  const char *sub_path = nullptr;
  // /tablet_id/reorganization_scn/mds/minor/sstable/op_id/meta/seq%ld
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 8))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%ld/%%ld/mds/%s/%s/%s_%%ld/%s/%s%%ld.T%hhu",
                     MINOR_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR, OP_KEY_STR, META_MACRO_DIR_STR, SEQ_KEY_STR, (uint8_t)type_))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn, &op_id, &macro_seq_id))) {
  } else if (OB_UNLIKELY(4 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
  } else {
    macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
    macro_id.set_storage_object_type((uint64_t)type_);
    macro_id.set_second_id(tablet_id);
    macro_id.set_third_id((op_id << 32) + macro_seq_id);
    macro_id.set_reorganization_scn(reorganization_scn);
  }
  return ret;
}
int ObSharedMdsMinorMetaMacroType::local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else {
    int64_t tablet_id = 0;
    int64_t reorganization_scn = 0;
    int64_t op_id = 0;
    int64_t macro_seq_id = 0;
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu",
                TABLET_KEY_STR, REORG_KEY_STR, OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn, &op_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id((op_id << 32) + macro_seq_id);
      macro_id.set_reorganization_scn(reorganization_scn);
    }
  }
  return ret;
}
int ObSharedMdsMinorMetaMacroType::get_effective_tablet_id(const MacroBlockId &macro_id,
  uint64_t &effective_tablet_id) const
{
  int ret = OB_SUCCESS;
  size_t pos = 8; // offsetof(MacroBlockId, second_id_);
  size_t size = sizeof(uint64_t);
  if (pos + size > sizeof(MacroBlockId)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pos), K(size));
  } else {
    const char* macro_id_bytes = reinterpret_cast<const char*>(&macro_id);
    effective_tablet_id = 0;
    memcpy(&effective_tablet_id, macro_id_bytes + pos, size);
  }
  return ret;
}
#endif

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
 * ---------------------------------------ObSharedMiniSstableListType----------------------------------------
 */

bool ObSharedMiniSstableListType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}
/**
 * ---------------------------------------ObSharedMiniSstableListTaskType----------------------------------------
 */

bool ObSharedMiniSstableListTaskType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}
/**
 * ---------------------------------------ObSharedMiniSstableListOpIdType----------------------------------------
 */

bool ObSharedMiniSstableListOpIdType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}
/**
 * ---------------------------------------ObSharedMiniSstableListCurrentType----------------------------------------
 */

bool ObSharedMiniSstableListCurrentType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}
/**
 * ---------------------------------------ObSharedMiniSstableListOldestType----------------------------------------
 */

bool ObSharedMiniSstableListOldestType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}
/**
 * ---------------------------------------ObSharedMinorSstableListType----------------------------------------
 */

bool ObSharedMinorSstableListType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}
/**
 * ---------------------------------------ObSharedMinorSstableListTaskType----------------------------------------
 */

bool ObSharedMinorSstableListTaskType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}
/**
 * ---------------------------------------ObSharedMinorSstableListOpIdType----------------------------------------
 */

bool ObSharedMinorSstableListOpIdType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}
/**
 * ---------------------------------------ObSharedMinorSstableListCurrentType----------------------------------------
 */

bool ObSharedMinorSstableListCurrentType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}
/**
 * ---------------------------------------ObSharedMinorSstableListOldestType----------------------------------------
 */

bool ObSharedMinorSstableListOldestType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}
/**
 * ---------------------------------------ObSharedLsMetaType----------------------------------------
 */

bool ObSharedLsMetaType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}
/**
 * ---------------------------------------ObSharedLsMetaTaskType----------------------------------------
 */

bool ObSharedLsMetaTaskType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}
/**
 * ---------------------------------------ObSharedLsMetaOpIdType----------------------------------------
 */

bool ObSharedLsMetaOpIdType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}
/**
 * ---------------------------------------ObSharedLsMetaCurrentType----------------------------------------
 */

bool ObSharedLsMetaCurrentType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}
/**
 * ---------------------------------------ObSharedLsMetaOldestType----------------------------------------
 */

bool ObSharedLsMetaOldestType::is_valid(const MacroBlockId &file_id) const
{
  return true;
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
 * ---------------------------------------ObSharedTabletMetaTaskType----------------------------------------
 */

bool ObSharedTabletMetaTaskType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}
/**
 * ---------------------------------------ObSharedTabletMetaOpIdType----------------------------------------
 */

bool ObSharedTabletMetaOpIdType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}
/**
 * ---------------------------------------ObSharedTabletMetaCurrentType----------------------------------------
 */

bool ObSharedTabletMetaCurrentType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}
/**
 * ---------------------------------------ObSharedTabletMetaOldestType----------------------------------------
 */

bool ObSharedTabletMetaOldestType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}
/**
 * ---------------------------------------ObSharedMdsMiniSstableListType----------------------------------------
 */

bool ObSharedMdsMiniSstableListType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}
/**
 * ---------------------------------------ObSharedMdsMiniSstableListTaskType----------------------------------------
 */

bool ObSharedMdsMiniSstableListTaskType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}
/**
 * ---------------------------------------ObSharedMdsMiniSstableListOpIdType----------------------------------------
 */

bool ObSharedMdsMiniSstableListOpIdType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}
/**
 * ---------------------------------------ObSharedMdsMiniSstableListCurrentType----------------------------------------
 */

bool ObSharedMdsMiniSstableListCurrentType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}
/**
 * ---------------------------------------ObSharedMdsMiniSstableListOldestType----------------------------------------
 */

bool ObSharedMdsMiniSstableListOldestType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}
/**
 * ---------------------------------------ObSharedMdsMinorSstableListType----------------------------------------
 */

bool ObSharedMdsMinorSstableListType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}
/**
 * ---------------------------------------ObSharedMdsMinorSstableListTaskType----------------------------------------
 */

bool ObSharedMdsMinorSstableListTaskType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}
/**
 * ---------------------------------------ObSharedMdsMinorSstableListOpIdType----------------------------------------
 */

bool ObSharedMdsMinorSstableListOpIdType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}
/**
 * ---------------------------------------ObSharedMdsMinorSstableListCurrentType----------------------------------------
 */

bool ObSharedMdsMinorSstableListCurrentType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}
/**
 * ---------------------------------------ObSharedMdsMinorSstableListOldestType----------------------------------------
 */

bool ObSharedMdsMinorSstableListOldestType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}
/**
 * ---------------------------------------ObSharedDdlkvMiniSstableListType----------------------------------------
 */

bool ObSharedDdlkvMiniSstableListType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}
/**
 * ---------------------------------------ObSharedDdlkvMiniSstableListTaskType----------------------------------------
 */

bool ObSharedDdlkvMiniSstableListTaskType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}
/**
 * ---------------------------------------ObSharedDdlkvMiniSstableListOpIdType----------------------------------------
 */

bool ObSharedDdlkvMiniSstableListOpIdType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}
/**
 * ---------------------------------------ObSharedDdlkvMiniSstableListCurrentType----------------------------------------
 */

bool ObSharedDdlkvMiniSstableListCurrentType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}
/**
 * ---------------------------------------ObSharedDdlkvMiniSstableListOldestType----------------------------------------
 */

bool ObSharedDdlkvMiniSstableListOldestType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}
/**
 * ---------------------------------------ObSharedSplitMinorSstableType----------------------------------------
 */

bool ObSharedSplitMinorSstableType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}
/**
 * ---------------------------------------ObSharedSplitMinorSstableTaskType----------------------------------------
 */

bool ObSharedSplitMinorSstableTaskType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}
/**
 * ---------------------------------------ObSharedSplitMinorSstableOpIdType----------------------------------------
 */

bool ObSharedSplitMinorSstableOpIdType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}
/**
 * ---------------------------------------ObSharedSplitMinorSstableCurrentType----------------------------------------
 */

bool ObSharedSplitMinorSstableCurrentType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}
/**
 * ---------------------------------------ObSharedSplitMinorSstableOldestType----------------------------------------
 */

bool ObSharedSplitMinorSstableOldestType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}
/**
 * ---------------------------------------ObSharedSplitMdsMinorSstableType----------------------------------------
 */

bool ObSharedSplitMdsMinorSstableType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}
/**
 * ---------------------------------------ObSharedSplitMdsMinorSstableTaskType----------------------------------------
 */

bool ObSharedSplitMdsMinorSstableTaskType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}
/**
 * ---------------------------------------ObSharedSplitMdsMinorSstableOpIdType----------------------------------------
 */

bool ObSharedSplitMdsMinorSstableOpIdType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}
/**
 * ---------------------------------------ObSharedSplitMdsMinorSstableCurrentType----------------------------------------
 */

bool ObSharedSplitMdsMinorSstableCurrentType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}
/**
 * ---------------------------------------ObSharedSplitMdsMinorSstableOldestType----------------------------------------
 */

bool ObSharedSplitMdsMinorSstableOldestType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}
/**
 * ---------------------------------------ObSharedMajorSstableType----------------------------------------
 */

bool ObSharedMajorSstableType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}
/**
 * ---------------------------------------ObSharedMajorSstableTaskType----------------------------------------
 */

bool ObSharedMajorSstableTaskType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}
/**
 * ---------------------------------------ObSharedMajorSstableOpIdType----------------------------------------
 */

bool ObSharedMajorSstableOpIdType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}
/**
 * ---------------------------------------ObSharedMajorSstableCurrentType----------------------------------------
 */

bool ObSharedMajorSstableCurrentType::is_valid(const MacroBlockId &file_id) const
{
  return true;
}
/**
 * ---------------------------------------ObSharedMajorSstableOldestType----------------------------------------
 */

bool ObSharedMajorSstableOldestType::is_valid(const MacroBlockId &file_id) const
{
  return true;
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
#ifdef OB_BUILD_SHARED_STORAGE
int ObSharedTabletSubMetaType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // inner_tablet: cluster_id/tenant_id/ls/ls_id/tablet_name/meta/op%ldseq%lu
  // user_tablet: cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/meta/op%ldseq%lu
  if (file_id.meta_is_inner_tablet()) {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%s/%s/%s%ld%s%lu",
                object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id, LS_DIR_STR,
                file_id.meta_ls_id()/*ls_id*/, get_ls_inner_tablet_name_(file_id.second_id())/*tablet_name*/,
                META_MACRO_DIR_STR, OP_KEY_STR, (file_id.third_id() >> 32)/*op_id*/,
                SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF) /*macro_seq_id*/))) {
      LOG_WARN("failed to format path", K(ret), K(file_id));
    }
  } else {
    if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%ld/%s/%s%ld%s%lu",
                object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id, TABLET_DIR_STR,
                file_id.second_id()/*tablet_id*/, file_id.reorganization_scn(), META_MACRO_DIR_STR, OP_KEY_STR,
                (file_id.third_id() >> 32)/*op_id*/, SEQ_KEY_STR, (file_id.third_id() & 0xFFFFFFFF) /*macro_seq_id*/))) {
      LOG_WARN("failed to format path", K(ret), K(file_id));
    }
  }
  return ret;
}

int ObSharedTabletSubMetaType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  // inner_tablet: /ls/ls_id/tablet_name/meta/op%ldseq%lu -- 5
  // user_tablet: /tablet/tablet_id/reorganization_scn/meta/op%ldseq%lu -- 5
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  bool is_inner_tablet = false;
  int64_t tablet_id = 0;
  int64_t reorganization_scn = 0;
  int64_t ls_id = 0;
  char tablet_name[512] = {0};
  int64_t op_id = 0;
  int64_t macro_seq_id = 0;
  const char *judge_path = nullptr;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(judge_path = ObString(path).reverse_find('/', 5))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (NULL != STRSTR(judge_path, LS_DIR_STR)) {
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s/%%ld/%[^/]/%s/%s%%ld%s%%lu.T%hhu",
                       LS_DIR_STR, META_MACRO_DIR_STR, OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(judge_path, format, &ls_id, tablet_name, &op_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(judge_path), K(path), K(format), K(num));
    } else if (OB_FAIL(get_ls_inner_tablet_id_(tablet_name, tablet_id))) {
      LOG_WARN("fail to get ls inner tablet id", KR(ret), K(tablet_name));
    } else {
      is_inner_tablet = true;
    }
  } else {
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s/%%ld/%%ld/%s/%s%%ld%s%%lu.T%hhu",
                TABLET_DIR_STR, META_MACRO_DIR_STR, OP_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(judge_path, format, &tablet_id, &reorganization_scn, &op_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(judge_path), K(path), K(format), K(num));
    } else {
      is_inner_tablet = false;
    }
  }
  if (OB_SUCC(ret)) {
    if (is_inner_tablet) {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id((op_id << 32) + macro_seq_id);
      macro_id.set_meta_ls_id(ls_id);
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id((op_id << 32) + macro_seq_id);
      macro_id.set_reorganization_scn(reorganization_scn);
    }
  }
  return ret;
}
int ObSharedTabletSubMetaType::get_effective_tablet_id(const MacroBlockId &macro_id,
  uint64_t &effective_tablet_id) const
{
  int ret = OB_SUCCESS;
  size_t pos = 8; // offsetof(MacroBlockId, second_id_);
  size_t size = sizeof(uint64_t);
  if (pos + size > sizeof(MacroBlockId)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pos), K(size));
  } else {
    const char* macro_id_bytes = reinterpret_cast<const char*>(&macro_id);
    effective_tablet_id = 0;
    memcpy(&effective_tablet_id, macro_id_bytes + pos, size);
  }
  return ret;
}
#endif

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
#ifdef OB_BUILD_SHARED_STORAGE
int ObTenantRootKeyType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // cluster_id/tenant_id/TENANT_ROOT_KEY
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s", object_storage_root_dir, CLUSTER_DIR_STR,
              cluster_id, TENANT_DIR_STR, tenant_id, get_type_str()))) {
    LOG_WARN("failed to format path", K(ret), K(file_id));
  }
  return ret;
}
#endif

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
#ifdef OB_BUILD_SHARED_STORAGE
int ObExternalTableFileType::to_local_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // tenant_id_epoch_id/external_table_file/scatter_id/seq%ldidx%ld*
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%02lX/%s%lu%s%ld",
              OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
              EXTERNAL_TABLE_FILE_DIR_STR, (file_id.hash() % ObDirManager::EXTERNAL_TABLE_FILE_SCATTER_DIR_NUM),
              SEQ_KEY_STR, file_id.second_id(), IDX_KEY_STR, file_id.third_id()))) {
    LOG_WARN("failed to format path", K(ret), K(file_id));
  }
  return ret;
}
int ObExternalTableFileType::local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else {
    uint64_t server_seq_id = 0;
    int64_t offset_idx = 0;
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s%%ld%s%%ld.T%hhu",
                SEQ_KEY_STR, IDX_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &server_seq_id, &offset_idx))) {
    } else if (OB_UNLIKELY(2 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_second_id(server_seq_id);
      macro_id.set_third_id(offset_idx);
    }
  }
  return ret;
}
int ObExternalTableFileType::get_parent_dir(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id,
  const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(path, length, "%s/%lu_%ld/%s",
      OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id, EXTERNAL_TABLE_FILE_DIR_STR))) {
    LOG_WARN("failed to get external table file dir", KR(ret),
        K(path), K(length), K(tenant_id), K(tenant_epoch_id));
  }
  return ret;
}
#endif

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
#ifdef OB_BUILD_SHARED_STORAGE
int ObMacroCacheCkptDataType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // cluster_id/server_id/tenant_id_epoch_id/macro_cache_ckpt/data/version_id/seq_id
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%lu_%ld/%s/%s/%lu/%s%lu", object_storage_root_dir,
              CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR, server_id, tenant_id, tenant_epoch_id,
              MACRO_CACHE_CKPT_DIR_STR, DATA_MACRO_DIR_STR, file_id.second_id(), SEQ_KEY_STR, file_id.third_id()))) {
    LOG_WARN("failed to format path", K(ret), K(file_id));
  }
  return ret;
}

int ObMacroCacheCkptDataType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  // /version_id/seq_id
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  int64_t second_id = 0;
  int64_t seq_id = 0;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 2))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%%lu/%s%%lu.T%hhu", SEQ_KEY_STR, (uint8_t)type_))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (FALSE_IT(num = sscanf(sub_path, format, &second_id, &seq_id))) {
  } else if (OB_UNLIKELY(2 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
  } else {
    macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
    macro_id.set_storage_object_type((uint64_t)type_);
    macro_id.set_second_id(second_id);
    macro_id.set_third_id(seq_id);
  }
  return ret;
}
#endif

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
#ifdef OB_BUILD_SHARED_STORAGE
int ObMacroCacheCkptMetaType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // cluster_id/server_id/tenant_id_epoch_id/macro_cache_ckpt/meta/version_id
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%lu_%ld/%s/%s/%s%lu",
              object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR, server_id,
              tenant_id, tenant_epoch_id, MACRO_CACHE_CKPT_DIR_STR, META_MACRO_DIR_STR,
              VER_KEY_STR, file_id.second_id()))) {
    LOG_WARN("failed to format path", K(ret), K(file_id));
  }
  return ret;
}

int ObMacroCacheCkptMetaType::remote_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  // /version_id
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  int64_t version_id = 0;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(path));
  } else if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s%%lu.T%hhu", VER_KEY_STR, (uint8_t)type_))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (FALSE_IT(num = sscanf(sub_path, format, &version_id))) {
  } else if (OB_UNLIKELY(1 != num)) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
  } else {
    macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
    macro_id.set_storage_object_type((uint64_t)type_);
    macro_id.set_second_id(version_id);
  }
  return ret;
}
#endif

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
#ifdef OB_BUILD_SHARED_STORAGE
int ObSharedIncMajorDataMacroType::to_local_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // tenant_id_epoch_id/shared_inc_major_macro_cache/scatter_id/tablet%ldreorg%ldcg%ldseq%ld
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld",
                    OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
                    INC_MAJOR_DATA_DIR_STR, (file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM),
                    TABLET_KEY_STR, file_id.second_id(), REORG_KEY_STR, file_id.reorganization_scn(),
                    CG_KEY_STR, file_id.column_group_id(), SEQ_KEY_STR, file_id.third_id()))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
int ObSharedIncMajorDataMacroType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/inc_major/sstable/cg_id/data/seq%ld
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%ld/%s/%s/%s_%ld/%s/%s%ld",
                object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id, TABLET_DIR_STR,
                file_id.second_id(), file_id.reorganization_scn(), INC_MAJOR_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR,
                COLUMN_GROUP_STR, file_id.column_group_id(), DATA_MACRO_DIR_STR, SEQ_KEY_STR, file_id.third_id()))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
int ObSharedIncMajorDataMacroType::local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else {
    int64_t tablet_id = 0;
    int64_t reorganization_scn = 0;
    int64_t cg_id = 0;
    int64_t macro_seq_id = 0;
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu",
                TABLET_KEY_STR, REORG_KEY_STR, CG_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn, &cg_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_column_group_id(cg_id);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id(macro_seq_id);
      macro_id.set_reorganization_scn(reorganization_scn);
    }
  }
  return ret;
}
int ObSharedIncMajorDataMacroType::get_effective_tablet_id(const MacroBlockId &macro_id,
  uint64_t &effective_tablet_id) const
{
  int ret = OB_SUCCESS;
  size_t pos = 8; // offsetof(MacroBlockId, second_id_);
  size_t size = sizeof(uint64_t);
  if (pos + size > sizeof(MacroBlockId)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pos), K(size));
  } else {
    const char* macro_id_bytes = reinterpret_cast<const char*>(&macro_id);
    effective_tablet_id = 0;
    memcpy(&effective_tablet_id, macro_id_bytes + pos, size);
  }
  return ret;
}
#endif

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
#ifdef OB_BUILD_SHARED_STORAGE
int ObSharedIncMajorMetaMacroType::to_local_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const uint64_t tenant_id, const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // tenant_id_epoch_id/shared_inc_major_macro_cache/scatter_id/tablet%ldreorg%ldcg%ldseq%ld
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%02lX/%s%ld%s%ld%s%ld%s%ld",
                    OB_DIR_MGR.get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
                    INC_MAJOR_DATA_DIR_STR, (file_id.hash() % ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM),
                    TABLET_KEY_STR, file_id.second_id(), REORG_KEY_STR, file_id.reorganization_scn(),
                    CG_KEY_STR, file_id.column_group_id(), SEQ_KEY_STR, file_id.third_id()))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
int ObSharedIncMajorMetaMacroType::to_remote_path_format(char *path, const int64_t length, int64_t &pos,
  const MacroBlockId &file_id, const char *object_storage_root_dir, const uint64_t cluster_id,
  const uint64_t tenant_id, const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const
{
  int ret = OB_SUCCESS;
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/inc_major/sstable/cg_id/meta/seq%ld
  if (OB_FAIL(databuff_printf(path, length, pos, "%s/%s_%ld/%s_%lu/%s/%ld/%ld/%s/%s/%s_%ld/%s/%s%ld",
                object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id, TABLET_DIR_STR,
                file_id.second_id(), file_id.reorganization_scn(), INC_MAJOR_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR,
                COLUMN_GROUP_STR, file_id.column_group_id(), META_MACRO_DIR_STR, SEQ_KEY_STR, file_id.third_id()))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}
int ObSharedIncMajorMetaMacroType::local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  const char *sub_path = nullptr;
  if (OB_ISNULL(sub_path = ObString(path).reverse_find('/', 1))) {
    ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
    LOG_ERROR("unexpected file in macro cache path", KR(ret), K(path));
  } else {
    int64_t tablet_id = 0;
    int64_t reorganization_scn = 0;
    int64_t cg_id = 0;
    int64_t macro_seq_id = 0;
    if (OB_FAIL(databuff_printf(format, sizeof(format), "/%s%%ld%s%%ld%s%%ld%s%%ld.T%hhu",
                TABLET_KEY_STR, REORG_KEY_STR, CG_KEY_STR, SEQ_KEY_STR, (uint8_t)type_))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(sub_path, format, &tablet_id, &reorganization_scn, &cg_id, &macro_seq_id))) {
    } else if (OB_UNLIKELY(4 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(path));
    } else {
      macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      macro_id.set_storage_object_type((uint64_t)type_);
      macro_id.set_column_group_id(cg_id);
      macro_id.set_second_id(tablet_id);
      macro_id.set_third_id(macro_seq_id);
      macro_id.set_reorganization_scn(reorganization_scn);
    }
  }
  return ret;
}
int ObSharedIncMajorMetaMacroType::get_effective_tablet_id(const MacroBlockId &macro_id,
  uint64_t &effective_tablet_id) const
{
  int ret = OB_SUCCESS;
  size_t pos = 8; // offsetof(MacroBlockId, second_id_);
  size_t size = sizeof(uint64_t);
  if (pos + size > sizeof(MacroBlockId)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pos), K(size));
  } else {
    const char* macro_id_bytes = reinterpret_cast<const char*>(&macro_id);
    effective_tablet_id = 0;
    memcpy(&effective_tablet_id, macro_id_bytes + pos, size);
  }
  return ret;
}
#endif

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
    case ObStorageObjectType::SHARED_MINI_SSTABLE_LIST: {
      static const ObSharedMiniSstableListType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MINI_SSTABLE_LIST_TASK: {
      static const ObSharedMiniSstableListTaskType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MINI_SSTABLE_LIST_OP_ID: {
      static const ObSharedMiniSstableListOpIdType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MINI_SSTABLE_LIST_CURRENT: {
      static const ObSharedMiniSstableListCurrentType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MINI_SSTABLE_LIST_OLDEST: {
      static const ObSharedMiniSstableListOldestType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MINOR_SSTABLE_LIST: {
      static const ObSharedMinorSstableListType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MINOR_SSTABLE_LIST_TASK: {
      static const ObSharedMinorSstableListTaskType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MINOR_SSTABLE_LIST_OP_ID: {
      static const ObSharedMinorSstableListOpIdType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MINOR_SSTABLE_LIST_CURRENT: {
      static const ObSharedMinorSstableListCurrentType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MINOR_SSTABLE_LIST_OLDEST: {
      static const ObSharedMinorSstableListOldestType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_LS_META: {
      static const ObSharedLsMetaType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_LS_META_TASK: {
      static const ObSharedLsMetaTaskType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_LS_META_OP_ID: {
      static const ObSharedLsMetaOpIdType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_LS_META_CURRENT: {
      static const ObSharedLsMetaCurrentType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_LS_META_OLDEST: {
      static const ObSharedLsMetaOldestType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_TABLET_META: {
      static const ObSharedTabletMetaType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_TABLET_META_TASK: {
      static const ObSharedTabletMetaTaskType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_TABLET_META_OP_ID: {
      static const ObSharedTabletMetaOpIdType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_TABLET_META_CURRENT: {
      static const ObSharedTabletMetaCurrentType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_TABLET_META_OLDEST: {
      static const ObSharedTabletMetaOldestType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MDS_MINI_SSTABLE_LIST: {
      static const ObSharedMdsMiniSstableListType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MDS_MINI_SSTABLE_LIST_TASK: {
      static const ObSharedMdsMiniSstableListTaskType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MDS_MINI_SSTABLE_LIST_OP_ID: {
      static const ObSharedMdsMiniSstableListOpIdType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MDS_MINI_SSTABLE_LIST_CURRENT: {
      static const ObSharedMdsMiniSstableListCurrentType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MDS_MINI_SSTABLE_LIST_OLDEST: {
      static const ObSharedMdsMiniSstableListOldestType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MDS_MINOR_SSTABLE_LIST: {
      static const ObSharedMdsMinorSstableListType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MDS_MINOR_SSTABLE_LIST_TASK: {
      static const ObSharedMdsMinorSstableListTaskType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MDS_MINOR_SSTABLE_LIST_OP_ID: {
      static const ObSharedMdsMinorSstableListOpIdType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MDS_MINOR_SSTABLE_LIST_CURRENT: {
      static const ObSharedMdsMinorSstableListCurrentType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MDS_MINOR_SSTABLE_LIST_OLDEST: {
      static const ObSharedMdsMinorSstableListOldestType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_DDLKV_MINI_SSTABLE_LIST: {
      static const ObSharedDdlkvMiniSstableListType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_DDLKV_MINI_SSTABLE_LIST_TASK: {
      static const ObSharedDdlkvMiniSstableListTaskType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_DDLKV_MINI_SSTABLE_LIST_OP_ID: {
      static const ObSharedDdlkvMiniSstableListOpIdType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_DDLKV_MINI_SSTABLE_LIST_CURRENT: {
      static const ObSharedDdlkvMiniSstableListCurrentType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_DDLKV_MINI_SSTABLE_LIST_OLDEST: {
      static const ObSharedDdlkvMiniSstableListOldestType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_SPLIT_MINOR_SSTABLE: {
      static const ObSharedSplitMinorSstableType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_SPLIT_MINOR_SSTABLE_TASK: {
      static const ObSharedSplitMinorSstableTaskType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_SPLIT_MINOR_SSTABLE_OP_ID: {
      static const ObSharedSplitMinorSstableOpIdType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_SPLIT_MINOR_SSTABLE_CURRENT: {
      static const ObSharedSplitMinorSstableCurrentType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_SPLIT_MINOR_SSTABLE_OLDEST: {
      static const ObSharedSplitMinorSstableOldestType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_SPLIT_MDS_MINOR_SSTABLE: {
      static const ObSharedSplitMdsMinorSstableType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_SPLIT_MDS_MINOR_SSTABLE_TASK: {
      static const ObSharedSplitMdsMinorSstableTaskType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_SPLIT_MDS_MINOR_SSTABLE_OP_ID: {
      static const ObSharedSplitMdsMinorSstableOpIdType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_SPLIT_MDS_MINOR_SSTABLE_CURRENT: {
      static const ObSharedSplitMdsMinorSstableCurrentType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_SPLIT_MDS_MINOR_SSTABLE_OLDEST: {
      static const ObSharedSplitMdsMinorSstableOldestType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MAJOR_SSTABLE: {
      static const ObSharedMajorSstableType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MAJOR_SSTABLE_TASK: {
      static const ObSharedMajorSstableTaskType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MAJOR_SSTABLE_OP_ID: {
      static const ObSharedMajorSstableOpIdType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MAJOR_SSTABLE_CURRENT: {
      static const ObSharedMajorSstableCurrentType instance;
      return instance;
    }
    case ObStorageObjectType::SHARED_MAJOR_SSTABLE_OLDEST: {
      static const ObSharedMajorSstableOldestType instance;
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
    default: {
      static const ObStorageObjectTypeBase instance;
      return instance;
    }
  }
}

} // end namespace blocksstable
} // end namespace oceanbase
