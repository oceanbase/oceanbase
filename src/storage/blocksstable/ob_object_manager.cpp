/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_object_manager.h"
#include "ob_storage_object_type.h"
#include "share/ob_io_device_helper.h"
#include "share/ob_perf_stat.h"
#include "storage/blocksstable/ob_block_manager.h"
#include "storage/blocksstable/ob_ss_obj_util.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace blocksstable
{
// ============================ ObStorageObjectOpt ======================================//
#define OBJ_MGR_PERF_TIMEGUARD_INIT() PERF_TIMEGUARD_WITH_MOD_INIT(ObjectManager)

PERF_STAT_ITEM(perf_alloc_object);
PERF_STAT_ITEM(perf_async_read_object);
PERF_STAT_ITEM(perf_async_write_object);
PERF_STAT_ITEM(perf_read_object);
PERF_STAT_ITEM(perf_write_object);
PERF_STAT_ITEM(perf_get_object_size);

int64_t ObStorageObjectOpt::to_string(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_FAIL(SSObjUtil::opt_to_string(object_type_, buf, buf_len, pos, *this))) {
    LOG_WARN("failed to print data into buf", K(ret), K(buf_len), K(pos), "storage object type", STI(object_type_));
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
    lock_(common::ObLatchIds::OB_OBJECT_MANAGER_LOCK),
    super_block_(),
    super_block_buf_holder_(),
    resize_file_lock_(common::ObLatchIds::OB_OBJECT_MANAGER_RESIZE_FILE_LOCK)
{
}

ObObjectManager::~ObObjectManager()
{
}

int ObObjectManager::init(const bool is_shared_storage, const int64_t macro_object_size)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(super_block_buf_holder_.init(storage::ObServerSuperBlockHeader::OB_MAX_SUPER_BLOCK_SIZE))) {
    LOG_WARN("fail to init super block buffer holder, ", K(ret));
  } else if (!is_shared_storage) {
    if (OB_FAIL(OB_SERVER_BLOCK_MGR.init(&LOCAL_DEVICE_INSTANCE, macro_object_size))) {
      LOG_WARN("fail to init block manager", K(ret), K(macro_object_size));
    }
  } else {
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
  }
  return ret;
}

void ObObjectManager::stop()
{
  if (!is_shared_storage_) {
    OB_SERVER_BLOCK_MGR.stop();
  } else {
  }
}

void ObObjectManager::wait()
{
  if (!is_shared_storage_) {
    OB_SERVER_BLOCK_MGR.wait();
  } else {
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
  PERF_GUARD_INIT(perf_alloc_object);
  OBJ_MGR_PERF_TIMEGUARD_INIT();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_shared_storage_) {
    if (OB_UNLIKELY(!SSObjUtil::is_support_sn(opt.object_type_))) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported marco type for shared-nothing", K(ret), K(opt.object_type_), K(opt));
    } else if (CLICK_FAIL(OB_SERVER_BLOCK_MGR.alloc_object(object_handle))) {
      LOG_WARN("fail to alloc object", K(ret), K(opt));
    }
  } else {
  }
  return ret;
}

int ObObjectManager::async_read_object(
    const ObStorageObjectReadInfo &read_info,
    ObStorageObjectHandle &object_handle)
{
  PERF_GUARD_INIT(perf_async_read_object);
  OBJ_MGR_PERF_TIMEGUARD_INIT();
  return object_handle.async_read(read_info);
}

int ObObjectManager::async_write_object(
    const ObStorageObjectOpt &opt,
    const ObStorageObjectWriteInfo &write_info,
    ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  PERF_GUARD_INIT(perf_async_write_object);
  OBJ_MGR_PERF_TIMEGUARD_INIT();
  if (OB_UNLIKELY(!write_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(write_info));
  } else if (CLICK_FAIL(OB_STORAGE_OBJECT_MGR.alloc_object(opt, object_handle))) {
    LOG_WARN("fail to alloc object from object manager", K(ret), K(opt));
  } else if (CLICK_FAIL(object_handle.async_write(write_info))) {
    LOG_WARN("Fail to async write block", K(ret), K(opt), K(object_handle));
  }
  return ret;
}

int ObObjectManager::read_object(
    const ObStorageObjectReadInfo &read_info,
    ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  PERF_GUARD_INIT(perf_read_object);
  OBJ_MGR_PERF_TIMEGUARD_INIT();
  if (CLICK_FAIL(async_read_object(read_info, object_handle))) {
    LOG_WARN("fail to sync read object", K(ret), K(read_info));
  } else if (CLICK_FAIL(object_handle.wait())) {
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
  PERF_GUARD_INIT(perf_write_object);
  OBJ_MGR_PERF_TIMEGUARD_INIT();
  if (CLICK_FAIL(async_write_object(opt, write_info, object_handle))) {
    LOG_WARN("fail to sync write block", K(ret), K(write_info), K(object_handle));
  } else if (CLICK_FAIL(object_handle.wait())) {
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
    const int64_t expected_current_size,
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
    const int64_t current_size = get_total_macro_block_count() * get_macro_block_size();
    if (expected_current_size != current_size) {
      ret = OB_EAGAIN;
    }
    HEAP_VAR(storage::ObServerSuperBlock, tmp_super_block) {
      tmp_super_block = super_block_;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.resize_file(
          new_device_size, new_device_disk_percentage, reserved_size, tmp_super_block))) {
        LOG_WARN("fail to resize file", K(ret), K(new_device_size), K(new_device_disk_percentage), K(reserved_size));
      } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.write_super_block(tmp_super_block, super_block_buf_holder_))) {
        LOG_WARN("fail to write super block", K(ret), K(tmp_super_block));
      } else {
        super_block_ = tmp_super_block;
        FLOG_INFO("succeed to resize local device", K_(super_block));
      }
    }
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
  }
  return ret;
}

int ObObjectManager::update_super_block(
    const common::ObLogCursor &replay_start_point,
    const blocksstable::MacroBlockId &tenant_meta_entry,
    const storage::ObSlogCheckpointFdDispenser &fd_dispenser)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    SpinWLockGuard guard(lock_);
    HEAP_VAR(storage::ObServerSuperBlock, tmp_super_block) {
      tmp_super_block = super_block_;
      tmp_super_block.body_.modify_timestamp_ = ObTimeUtility::current_time();
      tmp_super_block.body_.replay_start_point_ = replay_start_point;
      tmp_super_block.body_.tenant_meta_entry_ = tenant_meta_entry;
      tmp_super_block.construct_header();
      if (is_shared_storage_) {
      } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.write_super_block(tmp_super_block, super_block_buf_holder_))) {
        LOG_WARN("fail to write server super block", K(ret));
      } else if (OB_FAIL(LOCAL_DEVICE_INSTANCE.fsync_block())) {
        LOG_WARN("failed to fsync_block", K(ret));
      }
      if (OB_SUCC(ret)) {
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
  PERF_GUARD_INIT(perf_get_object_size);
  OBJ_MGR_PERF_TIMEGUARD_INIT();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_shared_storage_) {
    if (OB_UNLIKELY(!(SSObjUtil::is_private(object_id) && SSObjUtil::is_macro_data(object_id))
        && ObMacroBlockIdMode::ID_MODE_LOCAL != static_cast<ObMacroBlockIdMode>(object_id.id_mode()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected object id", K(ret), K(object_id));
    } else {
      object_size = get_macro_object_size();
    }
  } else {
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
