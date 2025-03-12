/**
 * Copyright (c) 2023 OceanBase
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

#include "ob_object_manager.h"
#include "storage/meta_store/ob_tenant_storage_meta_service.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/shared_storage/ob_file_manager.h"
#endif

namespace oceanbase
{
namespace blocksstable
{
// ============================ ObStorageObjectOpt ======================================//

int64_t ObStorageObjectOpt::to_string(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  switch (object_type_) {
  case ObStorageObjectType::PRIVATE_DATA_MACRO:
  case ObStorageObjectType::PRIVATE_META_MACRO: {
    if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type:%s (tablet_id=%lu, transfer_seq=%lu)",
               get_storage_objet_type_str(object_type_), private_opt_.tablet_id_, private_opt_.tablet_trasfer_seq_))) {
      LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_storage_objet_type_str(object_type_)),
                                                K(private_opt_.tablet_id_), K(private_opt_.tablet_trasfer_seq_));
    }
    break;
  }
  case ObStorageObjectType::SHARED_MAJOR_DATA_MACRO:
  case ObStorageObjectType::SHARED_MAJOR_META_MACRO: {
    if(OB_FAIL(databuff_printf(buf, buf_len, pos,
               "object_type=%s (tablet_id=%lu,data_seq=%lu,cg_id=%lu)",
               get_storage_objet_type_str(object_type_),
               ss_share_opt_.tablet_id_, ss_share_opt_.data_seq_,
               ss_share_opt_.column_group_id_))) {
      LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_storage_objet_type_str(object_type_)),
                                                K(ss_share_opt_.tablet_id_), K(ss_share_opt_.data_seq_), K(ss_share_opt_.column_group_id_));
    }
    break;
  }
  case ObStorageObjectType::TMP_FILE: {
    if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s", get_storage_objet_type_str(object_type_)))) {
      LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_storage_objet_type_str(object_type_)));
    }
    break;
  }
  case ObStorageObjectType::SERVER_META: {
    if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s", get_storage_objet_type_str(object_type_)))) {
      LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_storage_objet_type_str(object_type_)));
    }
    break;
  }
  case ObStorageObjectType::TENANT_DISK_SPACE_META:
  case ObStorageObjectType::TENANT_SUPER_BLOCK:
  case ObStorageObjectType::TENANT_UNIT_META: {
    if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s (tenant_id=%lu,tenant_epoch_id=%lu)",
               get_storage_objet_type_str(object_type_),
               ss_tenant_level_opt_.tenant_id_, ss_tenant_level_opt_.tenant_epoch_id_))) {
      LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_storage_objet_type_str(object_type_)),
                                                K(ss_tenant_level_opt_.tenant_id_), K(ss_tenant_level_opt_.tenant_epoch_id_));
    }
    break;
  }
  case ObStorageObjectType::LS_META:
  case ObStorageObjectType::LS_DUP_TABLE_META:
  case ObStorageObjectType::LS_ACTIVE_TABLET_ARRAY:
  case ObStorageObjectType::LS_PENDING_FREE_TABLET_ARRAY:
  case ObStorageObjectType::LS_TRANSFER_TABLET_ID_ARRAY: {
    if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s (ls_id=%lu)",
               get_storage_objet_type_str(object_type_), ss_ls_level_opt_.ls_id_))) {
      LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_storage_objet_type_str(object_type_)),
                                                K(ss_ls_level_opt_.ls_id_));
    }
    break;
  }
  case ObStorageObjectType::PRIVATE_TABLET_META: {
    if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s (ls_id=%lu,tablet_id=%lu,version=%lu,transfer_seq=%lu)",
               get_storage_objet_type_str(object_type_),
               ss_private_tablet_opt_.ls_id_, ss_private_tablet_opt_.tablet_id_,
               ss_private_tablet_opt_.version_, ss_private_tablet_opt_.tablet_transfer_seq_))) {
      LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_storage_objet_type_str(object_type_)),
                                                K(ss_private_tablet_opt_.ls_id_), K(ss_private_tablet_opt_.tablet_id_),
                                                K(ss_private_tablet_opt_.version_), K(ss_private_tablet_opt_.tablet_transfer_seq_));
    }
    break;
  }
  case ObStorageObjectType::PRIVATE_TABLET_CURRENT_VERSION: {
    if(OB_FAIL(databuff_printf(buf, buf_len, pos,
        "object_type=%s (ls_id=%lu,tablet_id=%lu)", get_storage_objet_type_str(object_type_),
        ss_private_tablet_current_version_opt_.ls_id_, ss_private_tablet_current_version_opt_.tablet_id_))) {
      LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_storage_objet_type_str(object_type_)),
                                                K(ss_private_tablet_current_version_opt_.ls_id_),
                                                K(ss_private_tablet_current_version_opt_.tablet_id_));
    }
    break;
  }
  case ObStorageObjectType::SHARED_MAJOR_TABLET_META: {
    if(OB_FAIL(databuff_printf(buf, buf_len, pos,
               "object_type=%s (tablet_id=%lu,version=%lu)", get_storage_objet_type_str(object_type_),
               ss_share_tablet_opt_.tablet_id_, ss_share_tablet_opt_.version_))) {
      LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_storage_objet_type_str(object_type_)),
                                                K(ss_share_tablet_opt_.tablet_id_),
                                                K(ss_share_tablet_opt_.version_));
    }
    break;
  }
  case ObStorageObjectType::SHARED_TABLET_ID:
  case ObStorageObjectType::IS_SHARED_TABLET_DELETED: {
    if(OB_FAIL(databuff_printf(buf, buf_len, pos,
               "object_type=%s (tablet_id=%lu)", get_storage_objet_type_str(object_type_),
               ss_shared_tablet_id_opt_.tablet_id_))) {
      LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_storage_objet_type_str(object_type_)),
                                                K(ss_shared_tablet_id_opt_.tablet_id_));
    }
    break;
  }
  case ObStorageObjectType::IS_SHARED_TENANT_DELETED: {
    if(OB_FAIL(databuff_printf(buf, buf_len, pos,
               "object_type=%s (tenant_id=%lu)", get_storage_objet_type_str(object_type_),
               ss_shared_tenant_id_opt_.tenant_id_))) {
      LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_storage_objet_type_str(object_type_)),
                                                K(ss_shared_tenant_id_opt_.tenant_id_));
    }
    break;
  }
  case ObStorageObjectType::COMPACTION_SERVER:
  case ObStorageObjectType::LS_COMPACTION_STATUS:
  case ObStorageObjectType::LS_COMPACTION_LIST: {
    if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s (ls_id=%lu)",
               get_storage_objet_type_str(object_type_), ss_compaction_scheduler_opt_.ls_id_))) {
      LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_storage_objet_type_str(object_type_)),
                                                K(ss_compaction_scheduler_opt_.ls_id_));
    }
    break;
  }
  case ObStorageObjectType::LS_SVR_COMPACTION_STATUS: {
    if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s (ls_id=%lu, server_id=%lu)",
               get_storage_objet_type_str(object_type_), ss_ls_svr_compactor_opt_.ls_id_,
               ss_ls_svr_compactor_opt_.server_id_))) {
      LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_storage_objet_type_str(object_type_)),
                                                K(ss_ls_svr_compactor_opt_.ls_id_),
                                                K(ss_ls_svr_compactor_opt_.server_id_));
    }
    break;
  }
  case ObStorageObjectType::COMPACTION_REPORT: {
    if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s (server_id=%lu)",
               get_storage_objet_type_str(object_type_), ss_svr_compactor_opt_.server_id_))) {
      LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_storage_objet_type_str(object_type_)),
                                                K(ss_svr_compactor_opt_.server_id_));
    }
    break;
  }
  case ObStorageObjectType::SHARED_MAJOR_GC_INFO: {
    if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s (tablet_id=%lu)",
               get_storage_objet_type_str(object_type_), ss_gc_info_opt_.tablet_id_))) {
      LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_storage_objet_type_str(object_type_)),
                                                K(ss_gc_info_opt_.tablet_id_));
    }
    break;
  }
  case ObStorageObjectType::SHARED_MAJOR_META_LIST: {
    if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s (tablet_id=%lu)",
               get_storage_objet_type_str(object_type_), ss_meta_list_opt_.tablet_id_))) {
      LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_storage_objet_type_str(object_type_)),
                                                K(ss_meta_list_opt_.tablet_id_));
    }
    break;
  }
  case ObStorageObjectType::TABLET_COMPACTION_STATUS: {
    if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s (tablet_id=%lu, compaction_scn_id=%ld)",
               get_storage_objet_type_str(object_type_), ss_tablet_compaction_status_opt_.tablet_id_,
               ss_tablet_compaction_status_opt_.scn_id_))) {
      LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_storage_objet_type_str(object_type_)),
                                                K(ss_tablet_compaction_status_opt_.tablet_id_),
                                                K(ss_tablet_compaction_status_opt_.scn_id_));
    }
    break;
  }
  default:
    if(OB_FAIL(databuff_printf(buf, buf_len, pos, "object_type=%s", "unknow object type"))) {
      LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), K(get_storage_objet_type_str(object_type_)));
    }
    break;
  }
  return pos;
}

//================================ ObObjectManager =====================================//

ObObjectManager &ObObjectManager::ObObjectManager::get_instance()
{
  static ObObjectManager instance_;
  return instance_;
}

ObObjectManager::ObObjectManager()
  : is_inited_(false),
    is_shared_storage_(false),
    macro_object_size_(0),
    lock_(),
    super_block_(),
    super_block_buf_holder_(),
    resize_file_lock_()
{
}

ObObjectManager::~ObObjectManager()
{
}

int ObObjectManager::init(const bool is_shared_storage, const int64_t macro_object_size)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(super_block_buf_holder_.init(ObServerSuperBlockHeader::OB_MAX_SUPER_BLOCK_SIZE))) {
    LOG_WARN("fail to init super block buffer holder, ", K(ret));
  } else if (!is_shared_storage) {
    if (OB_FAIL(OB_SERVER_BLOCK_MGR.init(&LOCAL_DEVICE_INSTANCE, macro_object_size))) {
      LOG_WARN("fail to init block manager", K(ret), K(macro_object_size));
    }
  } else {
#ifdef OB_BUILD_SHARED_STORAGE
    if (OB_FAIL(OB_SERVER_FILE_MGR.init(OB_SERVER_TENANT_ID))) {
      LOG_WARN("fail to init server file manager", K(ret));
    }
#endif
  }

  if (OB_SUCC(ret)) {
    is_shared_storage_ = is_shared_storage;
    macro_object_size_ = macro_object_size;
    is_inited_ = true;
    LOG_INFO("succeed to init object mgr", K(is_shared_storage));
  }
  return ret;
}

int ObObjectManager::start(const int64_t reserved_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(reserved_size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("reserved size should not less than zero", K(ret), K(reserved_size));
  } else if (!is_shared_storage_) {
    bool need_format = false;
    if (OB_FAIL(OB_SERVER_BLOCK_MGR.start(reserved_size, need_format))) {
      LOG_WARN("fail to start block manager", K(ret), K(reserved_size));
    } else if (OB_FAIL(read_or_format_super_block_(need_format))) {
      LOG_WARN("fail to read or format super block", K(ret), K(need_format));
    }
  } else {
#ifdef OB_BUILD_SHARED_STORAGE
    int64_t reserved_size = 0;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(OB_SERVER_FILE_MGR.start(reserved_size))) {
      LOG_WARN("fail to start server file manager", KR(ret), K(reserved_size));
    } else if (OB_FAIL(ss_read_or_format_super_block_())) {
      LOG_WARN("fail to read or format super block", KR(ret));
    }
#endif
  }
  return ret;
}

void ObObjectManager::stop()
{
  if (!is_shared_storage_) {
    OB_SERVER_BLOCK_MGR.stop();
  }
}

void ObObjectManager::wait()
{
  if (!is_shared_storage_) {
    OB_SERVER_BLOCK_MGR.wait();
  }
}

void ObObjectManager::destroy()
{
  super_block_buf_holder_.reset();
  if (!is_shared_storage_) {
    OB_SERVER_BLOCK_MGR.destroy();
  }
}

int ObObjectManager::alloc_object(const ObStorageObjectOpt &opt, ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_shared_storage_) {
    if (ObStorageObjectType::PRIVATE_DATA_MACRO != opt.object_type_
        && ObStorageObjectType::PRIVATE_META_MACRO != opt.object_type_) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("only support private marco for shared-nothing", K(ret), K(opt));
    } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.alloc_object(object_handle))) {
      LOG_WARN("fail to alloc object", K(ret), K(opt));
    }
  } else {
#ifdef OB_BUILD_SHARED_STORAGE
    MacroBlockId object_id;
    if (OB_FAIL(ss_get_object_id(opt, object_id))) {
      LOG_WARN("fail to alloc object", K(ret), K(opt));
    } else if (OB_FAIL(object_handle.set_macro_block_id(object_id))) {
      LOG_WARN("fail to set macro id", K(ret), K(object_id));
    }
#endif
  }
  return ret;
}

int ObObjectManager::async_read_object(
    const ObStorageObjectReadInfo &read_info,
    ObStorageObjectHandle &object_handle)
{
  return object_handle.async_read(read_info);
}

int ObObjectManager::async_write_object(
    const ObStorageObjectOpt &opt,
    const ObStorageObjectWriteInfo &write_info,
    ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!write_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(write_info));
  } else if (OB_FAIL(OB_STORAGE_OBJECT_MGR.alloc_object(opt, object_handle))) {
    LOG_WARN("fail to alloc object from object manager", K(ret), K(opt));
  } else if (OB_FAIL(object_handle.async_write(write_info))) {
    LOG_WARN("Fail to async write block", K(ret), K(opt), K(object_handle));
  }
  return ret;
}

int ObObjectManager::read_object(
    const ObStorageObjectReadInfo &read_info,
    ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(async_read_object(read_info, object_handle))) {
    LOG_WARN("fail to sync read object", K(ret), K(read_info));
  } else if (OB_FAIL(object_handle.wait())) {
    LOG_WARN("Fail to wait io finish", K(ret), K(read_info));
  }
  return ret;
}
int ObObjectManager::write_object(
    const ObStorageObjectOpt &opt,
    const ObStorageObjectWriteInfo &write_info,
    ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(async_write_object(opt, write_info, object_handle))) {
    LOG_WARN("fail to sync write block", K(ret), K(write_info), K(object_handle));
  } else if (OB_FAIL(object_handle.wait())) {
    LOG_WARN("fail to wait io finish", K(ret), K(write_info));
  }
  return ret;
}

int ObObjectManager::inc_ref(const MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (is_shared_storage_) {
    // do nothing for ss
  } else {
    ret = OB_SERVER_BLOCK_MGR.inc_ref(object_id);
  }
  return ret;
}

int ObObjectManager::dec_ref(const MacroBlockId &object_id) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (is_shared_storage_) {
    // do nothing for ss
  } else {
    ret = OB_SERVER_BLOCK_MGR.dec_ref(object_id);
  }
  return ret;
}

int ObObjectManager::resize_local_device(
    const int64_t new_device_size,
    const int64_t new_device_disk_percentage,
    const int64_t reserved_size)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(resize_file_lock_); // lock resize file opt

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_shared_storage_) {
    SpinWLockGuard guard(lock_);
    HEAP_VAR(ObServerSuperBlock, tmp_super_block) {
      tmp_super_block = super_block_;
      if (OB_FAIL(OB_SERVER_BLOCK_MGR.resize_file(
          new_device_size, new_device_disk_percentage, reserved_size, tmp_super_block))) {
        LOG_WARN("fail to resize file", K(ret), K(new_device_size), K(new_device_disk_percentage), K(reserved_size));
      } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.write_super_block(tmp_super_block, super_block_buf_holder_))) {
        LOG_WARN("fail to write super block", K(ret), K(tmp_super_block));
      } else {
        super_block_ = tmp_super_block;
        FLOG_INFO("succeed to resize local device", K_(super_block));
      }
    }
#ifdef OB_BUILD_SHARED_STORAGE
  } else {
    SpinWLockGuard guard(lock_);
    HEAP_VAR(ObServerSuperBlock, tmp_super_block) {
      tmp_super_block = super_block_;
      if (OB_FAIL(OB_SERVER_FILE_MGR.resize_device_size(
          new_device_size, new_device_disk_percentage, reserved_size, tmp_super_block))) {
        LOG_WARN("fail to resize device size", KR(ret), K(new_device_size), K(new_device_disk_percentage), K(reserved_size));
      } else if (OB_FAIL(ss_write_super_block_(tmp_super_block))) {
        LOG_WARN("fail to write super block, ", KR(ret));
      } else {
        super_block_ = tmp_super_block;
        FLOG_INFO("succeed to resize local cache device", K_(super_block));
      }
    }
#endif
  }
  return ret;
}

int ObObjectManager::check_disk_space_available()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_shared_storage_) {
    // do nothing
  } else {
#ifdef OB_BUILD_SHARED_STORAGE
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(OB_SERVER_FILE_MGR.check_disk_space_available())) {
      LOG_WARN("fail to check disk space available", KR(ret));
    }
#endif
  }
  return ret;
}

int ObObjectManager::update_super_block(const common::ObLogCursor &replay_start_point,
                                        const blocksstable::MacroBlockId &tenant_meta_entry)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(is_shared_storage_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported for shared-storage", K(ret));
  } else {
    SpinWLockGuard guard(lock_);
    HEAP_VAR(ObServerSuperBlock, tmp_super_block) {
      tmp_super_block = super_block_;
      tmp_super_block.body_.modify_timestamp_ = ObTimeUtility::current_time();
      tmp_super_block.body_.replay_start_point_ = replay_start_point;
      tmp_super_block.body_.tenant_meta_entry_ = tenant_meta_entry;
      tmp_super_block.construct_header();
      if (OB_FAIL(OB_SERVER_BLOCK_MGR.write_super_block(tmp_super_block, super_block_buf_holder_))) {
        LOG_WARN("fail to write server super block", K(ret));
      } else if (OB_FAIL(LOCAL_DEVICE_INSTANCE.fsync_block())) {
        LOG_WARN("failed to fsync_block", K(ret));
      } else {
        super_block_ = tmp_super_block;
      }
    }
  }
  return ret;
}

int ObObjectManager::get_object_size(
    const MacroBlockId &object_id,
    const int64_t ls_epoch,
    int64_t &object_size) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_shared_storage_) {
    if (OB_UNLIKELY(ObStorageObjectType::PRIVATE_DATA_MACRO != object_id.storage_object_type()
        && ObMacroBlockIdMode::ID_MODE_LOCAL != static_cast<ObMacroBlockIdMode>(object_id.id_mode()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected object id", K(ret), K(object_id));
    } else {
      object_size = get_macro_object_size();
    }
  } else {
#ifdef OB_BUILD_SHARED_STORAGE
    if (OB_FAIL(OB_SERVER_FILE_MGR.get_file_length(object_id, ls_epoch, object_size))) {
      LOG_WARN("fail to get file size", K(ret), K(object_id), K(ls_epoch), K(object_size));
    }
#endif
  }
  return ret;
}

int  ObObjectManager::read_or_format_super_block_(const bool need_format)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  // read super block
  if (!need_format) {
    if (OB_FAIL(OB_SERVER_BLOCK_MGR.read_super_block(super_block_, super_block_buf_holder_))) {
      LOG_WARN("fail to read server super block", K(ret));
    } else {
      LOG_INFO("succeed to read super block", K_(super_block));
    }
  } else {
    if (OB_FAIL(super_block_.format_startup_super_block(
        macro_object_size_, OB_SERVER_BLOCK_MGR.get_total_block_size()))) {
      LOG_WARN("fail to format super block, ", K(ret));
    } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.write_super_block(super_block_, super_block_buf_holder_))) {
      LOG_WARN("fail to write super block, ", K(ret));
    }
  }
  return ret;
}


#ifdef OB_BUILD_SHARED_STORAGE

void ObObjectManager::set_ss_object_first_id_(
    const uint64_t object_type,
    const uint64_t incarnation_id,
    const uint64_t column_group_id,
    MacroBlockId &object_id)
{
  object_id.set_version_v2();
  object_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  object_id.set_storage_object_type(object_type);
  object_id.set_incarnation_id(incarnation_id);
  object_id.set_column_group_id(column_group_id);
}

int ObObjectManager::ss_is_exist_object(const MacroBlockId &object_id, const int64_t ls_epoch, bool &is_exist)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager *file_mgr = MTL(ObTenantFileManager*);
  if (OB_ISNULL(file_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTenantFileManager is null", K(ret));
  } else if (OB_FAIL(file_mgr->is_exist_file(object_id, ls_epoch, is_exist))) {
    LOG_WARN("fail to check existence", K(ret), K(object_id), K(ls_epoch));
  }
  return ret;
}

int ObObjectManager::seal_object(const MacroBlockId &object_id, const int64_t ls_epoch_id)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager *file_mgr = MTL(ObTenantFileManager *);
  if (OB_UNLIKELY(!object_id.is_valid() || (ls_epoch_id < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(object_id), K(ls_epoch_id));
  } else if (OB_ISNULL(file_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant file manager is null", KR(ret), "tenant_id", MTL_ID());
  } else if (OB_FAIL(file_mgr->push_to_flush_queue(object_id, ls_epoch_id))) {
    LOG_WARN("fail to push to flush queue", KR(ret), K(object_id), K(ls_epoch_id));
  }
  return ret;
}

int ObObjectManager::ss_get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id)
{
  int ret = OB_SUCCESS;
  const int64_t default_incarnation_id = 0;
  const int64_t default_cg_id = 0;
  const int64_t obj_type = static_cast<int64_t>(opt.object_type_);
  object_id.reset();

  switch (opt.object_type_) {
    case ObStorageObjectType::PRIVATE_DATA_MACRO:
    case ObStorageObjectType::PRIVATE_META_MACRO: {
      uint64_t seq = 0;
      set_ss_object_first_id_(obj_type, default_incarnation_id, default_cg_id, object_id);
      object_id.set_second_id(opt.private_opt_.tablet_id_);
      object_id.set_third_id(GCONF.observer_id);
      if (OB_FAIL(TENANT_SEQ_GENERATOR.get_private_object_seq(seq))) {
        LOG_WARN("fail to get private object seq", K(ret), K(opt));
      } else {
        object_id.set_tenant_seq(seq);
        object_id.set_macro_transfer_seq(opt.private_opt_.tablet_trasfer_seq_);
      }
      break;
    }
    case ObStorageObjectType::SHARED_MAJOR_DATA_MACRO:
    case ObStorageObjectType::SHARED_MAJOR_META_MACRO: {
      set_ss_object_first_id_(obj_type, default_incarnation_id, opt.ss_share_opt_.column_group_id_, object_id);
      object_id.set_second_id(opt.ss_share_opt_.tablet_id_);
      object_id.set_third_id(opt.ss_share_opt_.data_seq_);
      break;
    }
    case ObStorageObjectType::TMP_FILE: {
      uint64_t file_id = 0;
      set_ss_object_first_id_(obj_type, default_incarnation_id, default_cg_id, object_id);
      if (OB_FAIL(TENANT_SEQ_GENERATOR.get_tmp_file_seq(file_id))) {
        LOG_WARN("fail to get private tmp file seq", K(ret), K(opt));
      } else {
        object_id.set_second_id(file_id);
      }
      break;
    }
    case ObStorageObjectType::SERVER_META: {
      set_ss_object_first_id_(obj_type, default_incarnation_id, default_cg_id, object_id);
      break;
    }
    case ObStorageObjectType::TENANT_DISK_SPACE_META:
    case ObStorageObjectType::TENANT_SUPER_BLOCK:
    case ObStorageObjectType::TENANT_UNIT_META: {
      set_ss_object_first_id_(obj_type, default_incarnation_id, default_cg_id, object_id);
      object_id.set_second_id(opt.ss_tenant_level_opt_.tenant_id_);
      object_id.set_third_id(opt.ss_tenant_level_opt_.tenant_epoch_id_);
      break;
    }
    case ObStorageObjectType::LS_META:
    case ObStorageObjectType::LS_DUP_TABLE_META:
    case ObStorageObjectType::LS_ACTIVE_TABLET_ARRAY:
    case ObStorageObjectType::LS_PENDING_FREE_TABLET_ARRAY:
    case ObStorageObjectType::LS_TRANSFER_TABLET_ID_ARRAY: {
      set_ss_object_first_id_(obj_type, default_incarnation_id, default_cg_id, object_id);
      object_id.set_second_id(opt.ss_ls_level_opt_.ls_id_);
      break;
    }
    case ObStorageObjectType::PRIVATE_TABLET_META: {
      set_ss_object_first_id_(obj_type, default_incarnation_id, default_cg_id, object_id);
      object_id.set_second_id(opt.ss_private_tablet_opt_.ls_id_);
      object_id.set_third_id(opt.ss_private_tablet_opt_.tablet_id_);
      object_id.set_meta_version_id(opt.ss_private_tablet_opt_.version_);
      object_id.set_meta_transfer_seq(opt.ss_private_tablet_opt_.tablet_transfer_seq_);
      break;
    }
    case ObStorageObjectType::PRIVATE_TABLET_CURRENT_VERSION: {
      set_ss_object_first_id_(obj_type, default_incarnation_id, default_cg_id, object_id);
      object_id.set_second_id(opt.ss_private_tablet_current_version_opt_.ls_id_);
      object_id.set_third_id(opt.ss_private_tablet_current_version_opt_.tablet_id_);
      break;
    }
    case ObStorageObjectType::SHARED_MAJOR_TABLET_META: {
      set_ss_object_first_id_(obj_type, default_incarnation_id, default_cg_id, object_id);
      object_id.set_second_id(opt.ss_share_tablet_opt_.tablet_id_);
      object_id.set_third_id(opt.ss_share_tablet_opt_.version_);
      break;
    }
    case ObStorageObjectType::SHARED_TABLET_ID:
    case ObStorageObjectType::IS_SHARED_TABLET_DELETED: {
      set_ss_object_first_id_(obj_type, default_incarnation_id, default_cg_id, object_id);
      object_id.set_second_id(opt.ss_shared_tablet_id_opt_.tablet_id_);
      break;
    }
    case ObStorageObjectType::IS_SHARED_TENANT_DELETED: {
      set_ss_object_first_id_(obj_type, default_incarnation_id, default_cg_id, object_id);
      object_id.set_second_id(opt.ss_shared_tenant_id_opt_.tenant_id_);
      break;
    }
    case ObStorageObjectType::COMPACTION_SERVER:
    case ObStorageObjectType::LS_COMPACTION_STATUS:
    case ObStorageObjectType::LS_COMPACTION_LIST: {
      set_ss_object_first_id_(obj_type, default_incarnation_id, default_cg_id, object_id);
      object_id.set_second_id(opt.ss_compaction_scheduler_opt_.ls_id_);
      break;
    }
    case ObStorageObjectType::LS_SVR_COMPACTION_STATUS: {
      set_ss_object_first_id_(obj_type, default_incarnation_id, default_cg_id, object_id);
      object_id.set_second_id(opt.ss_ls_svr_compactor_opt_.ls_id_);
      object_id.set_third_id(opt.ss_ls_svr_compactor_opt_.server_id_);
      break;
    }
    case ObStorageObjectType::COMPACTION_REPORT: {
      set_ss_object_first_id_(obj_type, default_incarnation_id, default_cg_id, object_id);
      object_id.set_second_id(opt.ss_svr_compactor_opt_.server_id_);
      break;
    }
    case ObStorageObjectType::SHARED_MAJOR_GC_INFO: {
      set_ss_object_first_id_(obj_type, default_incarnation_id, default_cg_id, object_id);
      object_id.set_second_id(opt.ss_gc_info_opt_.tablet_id_);
      break;
    }
    case ObStorageObjectType::SHARED_MAJOR_META_LIST: {
      set_ss_object_first_id_(obj_type, default_incarnation_id, default_cg_id, object_id);
      object_id.set_second_id(opt.ss_meta_list_opt_.tablet_id_);
      break;
    }
    case ObStorageObjectType::TABLET_COMPACTION_STATUS: {
      set_ss_object_first_id_(obj_type, default_incarnation_id, default_cg_id, object_id);
      object_id.set_second_id(opt.ss_tablet_compaction_status_opt_.tablet_id_);
      object_id.set_third_id(opt.ss_tablet_compaction_status_opt_.scn_id_);
      break;
    }
    case ObStorageObjectType::MAJOR_PREWARM_DATA:
    case ObStorageObjectType::MAJOR_PREWARM_DATA_INDEX:
    case ObStorageObjectType::MAJOR_PREWARM_META:
    case ObStorageObjectType::MAJOR_PREWARM_META_INDEX: {
      set_ss_object_first_id_(obj_type, default_incarnation_id, default_cg_id, object_id);
      object_id.set_second_id(opt.ss_major_prewarm_opt_.tablet_id_);
      object_id.set_third_id(opt.ss_major_prewarm_opt_.compaction_scn_);
      break;
    }
    case ObStorageObjectType::CHECKSUM_ERROR_DUMP_MACRO: {
      set_ss_object_first_id_(obj_type, default_incarnation_id, opt.ss_ckm_error_dump_macro_id_opt_.cg_id_, object_id);
      object_id.set_second_id(opt.ss_ckm_error_dump_macro_id_opt_.tablet_id_);
      object_id.set_third_id(opt.ss_ckm_error_dump_macro_id_opt_.compaction_scn_);
      object_id.set_fourth_id(opt.ss_ckm_error_dump_macro_id_opt_.block_seq_);
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected share storage object type", K(ret), K(opt));
      break;
    }
  }
  return ret;
}

MacroBlockId ObObjectManager::ss_get_super_block_object_id_()
{
  MacroBlockId macro_id;
  set_ss_object_first_id_(
      static_cast<uint64_t>(ObStorageObjectType::SERVER_META),
      0/*incarnation_id*/, 0/*cg_id*/, macro_id);
  return macro_id;
}

int ObObjectManager::ss_read_or_format_super_block_()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  const MacroBlockId super_block_id = ss_get_super_block_object_id_();
  bool is_exist = false;
  if (OB_FAIL(OB_SERVER_FILE_MGR.is_exist_file(super_block_id, 0, is_exist))) {
    LOG_WARN("fail to check existence of super block", K(ret), K(super_block_id));
  } else {
    if (is_exist) {
      if (OB_FAIL(ss_read_super_block_(super_block_id, super_block_))) {
        LOG_WARN("fail to read super block", K(ret), K(super_block_id));
      }
    } else {
      if (OB_FAIL(super_block_.format_startup_super_block(
          macro_object_size_, OB_SERVER_DISK_SPACE_MGR.get_total_disk_size()))) {
        LOG_WARN("fail to format super block", K(ret));
      } else if (OB_FAIL(ss_write_super_block_(super_block_))) {
        LOG_WARN("fail to write super block, ", K(ret));
      }
    }
  }
  return ret;
}

int ObObjectManager::ss_read_super_block_(
    const MacroBlockId &macro_id, ObServerSuperBlock &super_block)
{
  int ret = OB_SUCCESS;
  ObStorageObjectHandle object_handle;
  ObStorageObjectReadInfo read_info;
  read_info.macro_block_id_ = macro_id;
  read_info.io_desc_.set_mode(ObIOMode::READ);
  read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_READ);
  read_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000L;
  read_info.buf_ = super_block_buf_holder_.get_buffer(),
  read_info.io_desc_.set_sys_module_id(ObIOModule::SLOG_IO);
  read_info.offset_ = 0;
  read_info.size_ = super_block_buf_holder_.get_len();
  read_info.mtl_tenant_id_ = OB_SERVER_TENANT_ID;
  int64_t pos = 0;
  if (OB_FAIL(OB_SERVER_FILE_MGR.pread_file(read_info, object_handle))) {
    LOG_WARN("fail to read super block", K(ret), K(read_info));
  } else if (OB_UNLIKELY(super_block_buf_holder_.get_len() != object_handle.get_data_size())) {
    ret = OB_IO_ERROR;
    LOG_WARN("read size not equal super block size", K(ret), K(object_handle));
  } else if (OB_FAIL(super_block.deserialize(
      super_block_buf_holder_.get_buffer(), super_block_buf_holder_.get_len(), pos))) {
    LOG_WARN("deserialize super block fail", K(ret), K(pos));
  } else {
    LOG_INFO("succeed read super block", K(ret), K(super_block), K(pos));
  }
  return ret;
}

int ObObjectManager::ss_write_super_block_(const ObServerSuperBlock &super_block)
{
  int ret = OB_SUCCESS;
  int64_t write_size = 0;
  if (OB_UNLIKELY(!super_block.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(super_block));
  } else if (OB_FAIL(super_block_buf_holder_.serialize_super_block(super_block))) {
    LOG_ERROR("failed to serialize super block", K(ret), K_(super_block_buf_holder), K(super_block));
  } else {
    ObStorageObjectOpt opt;
    opt.set_ss_sever_level_meta_object_opt(ObStorageObjectType::SERVER_META);
    ObStorageObjectWriteInfo write_info;
    ObStorageObjectHandle object_handle;
    write_info.buffer_ = super_block_buf_holder_.get_buffer();
    write_info.size_ = super_block_buf_holder_.get_len();
    write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
    write_info.io_desc_.set_sys_module_id(ObIOModule::SLOG_IO);
    write_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000L;
    write_info.mtl_tenant_id_ = OB_SERVER_TENANT_ID;

    if (OB_FAIL(write_object(opt, write_info, object_handle))) {
      LOG_WARN("fail to write super block", K(ret), K(opt), K(object_handle));
    } else {
      LOG_INFO("succeed to write super block", K(ret), K(opt), K(super_block));
    }
  }
  return ret;
}

int ObObjectManager::create_super_block_tenant_item(const uint64_t tenant_id, int64_t &tenant_epoch)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!is_shared_storage_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only available for shared-storage");
  } else {
    SpinWLockGuard guard(lock_);
    HEAP_VAR(ObServerSuperBlock, tmp_super_block) {
      tmp_super_block = super_block_;
      int64_t i = 0;
      for (; i < tmp_super_block.body_.tenant_cnt_; i++) {
        const ObTenantItem &item = tmp_super_block.body_.tenant_item_arr_[i];
        if (tenant_id == item.tenant_id_ &&
            item.status_ != ObTenantCreateStatus::DELETED &&
            item.status_ != ObTenantCreateStatus::CREATE_ABORT) {
          break;
        }
      }
      if (OB_UNLIKELY(i != tmp_super_block.body_.tenant_cnt_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant item already exist", K(ret), "tenant_item", tmp_super_block.body_.tenant_item_arr_[i]);
      } else if (OB_UNLIKELY(ServerSuperBlockBody::MAX_TENANT_COUNT == i)) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("too many tenants", K(ret), K(tenant_id), K(tmp_super_block));
      } else {
        ObTenantItem &item = tmp_super_block.body_.tenant_item_arr_[i];
        item.tenant_id_ = tenant_id;
        item.status_ = ObTenantCreateStatus::CREATING;
        item.epoch_ = tmp_super_block.body_.auto_inc_tenant_epoch_++;
        tenant_epoch = item.epoch_;
        tmp_super_block.body_.tenant_cnt_ = i + 1;
        tmp_super_block.body_.modify_timestamp_ = ObTimeUtility::current_time();
        tmp_super_block.construct_header();
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(ss_write_super_block_(tmp_super_block))) {
          LOG_WARN("fail to write super block", K(ret), K(tmp_super_block));
        } else {
          super_block_ = tmp_super_block;
        }
      }
      FLOG_INFO("create super block tenant item", K(ret), K(tenant_id), K(tenant_epoch));
    }
  }
  return ret;
}

int ObObjectManager::delete_super_block_tenant_item(
    const uint64_t tenant_id, const int64_t tenant_epoch)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!is_shared_storage_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only available for shared-storage");
  } else {
    SpinWLockGuard guard(lock_);
    HEAP_VAR(ObServerSuperBlock, tmp_super_block) {
      bool is_delete_hit = false;
      tmp_super_block = super_block_;
      tmp_super_block.body_.tenant_cnt_ = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < super_block_.body_.tenant_cnt_; i++) {
        const ObTenantItem &item = super_block_.body_.tenant_item_arr_[i];
        if (tenant_id == item.tenant_id_ && tenant_epoch == item.epoch_) {
          if (ObTenantCreateStatus::DELETED == item.status_ || ObTenantCreateStatus::CREATE_ABORT == item.status_) {
            is_delete_hit = true;
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("try to delete tenant_item whose status is not equal to deleted", K(ret), K(item), K(super_block_));
          }
        } else {
          tmp_super_block.body_.tenant_item_arr_[tmp_super_block.body_.tenant_cnt_++] = item;
        }
      }

      if (OB_FAIL(ret)) {
        // error occurred
      } else if (OB_LIKELY(is_delete_hit)) {
        tmp_super_block.body_.modify_timestamp_ = ObTimeUtility::current_time();
        tmp_super_block.construct_header();
        if (OB_FAIL(ss_write_super_block_(tmp_super_block))) {
          LOG_WARN("fail to write super block", K(ret), K(tmp_super_block));
        } else {
          FLOG_INFO("update super block tenant item", K(super_block_), K(tmp_super_block));
          super_block_ = tmp_super_block;
        }
      } else {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("tenant item not exist", K(ret), K(tenant_id), K(tenant_epoch), K(super_block_));
      }
    }
  }
  return ret;
}

int ObObjectManager::update_super_block_tenant_item(
    const uint64_t tenant_id, const int64_t tenant_epoch,
    const ObTenantCreateStatus status)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!is_shared_storage_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only available for shared-storage");
  } else {
    SpinWLockGuard guard(lock_);
    HEAP_VAR(ObServerSuperBlock, tmp_super_block) {
      tmp_super_block = super_block_;
      int64_t i = 0;
      for (; i < tmp_super_block.body_.tenant_cnt_; i++) {
        const ObTenantItem &item = tmp_super_block.body_.tenant_item_arr_[i];
        if (tenant_id == item.tenant_id_ && tenant_epoch == item.epoch_) {
          break;
        }
      }
      if (OB_UNLIKELY(i == tmp_super_block.body_.tenant_cnt_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant item not exist", K(ret), K(tenant_id), K(tenant_epoch), K(status));
      } else {
        const ObTenantItem old_item = super_block_.body_.tenant_item_arr_[i];
        ObTenantItem &new_item = tmp_super_block.body_.tenant_item_arr_[i];
        new_item.status_ = status;
        tmp_super_block.body_.modify_timestamp_ = ObTimeUtility::current_time();
        tmp_super_block.construct_header();
        if (OB_FAIL(ss_write_super_block_(tmp_super_block))) {
          LOG_WARN("fail to write super block", K(ret), K(tmp_super_block));
        } else {
          super_block_ = tmp_super_block;
          FLOG_INFO("update super block tenant item", K(old_item), K(new_item), K(tmp_super_block));
        }
      }
    }
  }
  return ret;
}

int ObObjectManager::async_write_object(
    const blocksstable::MacroBlockId &macro_block_id,
    const ObStorageObjectWriteInfo &write_info,
    ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!write_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(write_info));
  } else if (OB_FAIL(object_handle.set_macro_block_id(macro_block_id))) {
    LOG_WARN("failed to set macro block id", K(ret));
  } else if (OB_FAIL(object_handle.async_write(write_info))) {
    LOG_WARN("failed to write info", K(ret), K(object_handle));
  }
  return ret;
}

#endif // OB_BUILD_SHARED_STORAGE



int64_t ObObjectManager::get_max_macro_block_count(int64_t reserved_size) const
{
  int64_t block_count = 0;
  if (!is_shared_storage_) {
    block_count = OB_SERVER_BLOCK_MGR.get_max_macro_block_count(reserved_size);
  } else {
    block_count = 1000000; // TODO(fenggu.yh  临时代码
  }
  return block_count;
}

int64_t ObObjectManager::get_used_macro_block_count() const
{
  int64_t block_count = 0;
  if (!is_shared_storage_) {
    block_count = OB_SERVER_BLOCK_MGR.get_used_macro_block_count();
  } else {
    block_count = 1000; // TODO(fenggu.yh) 临时代码
  }
  return block_count;
}

int64_t ObObjectManager::get_free_macro_block_count() const
{
  int64_t block_count = 0;
  if (!is_shared_storage_) {
    block_count = OB_SERVER_BLOCK_MGR.get_free_macro_block_count();
  } else {
    block_count = 1000000; // TODO(fenggu.yh) 临时代码
  }
  return block_count;
}



} // namespace blocksstable
} // oceanbase
