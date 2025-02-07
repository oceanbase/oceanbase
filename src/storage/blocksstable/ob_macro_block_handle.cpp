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

#define USING_LOG_PREFIX STORAGE_BLKMGR

#include "storage/blocksstable/ob_macro_block_handle.h"

#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/ob_force_print_log.h"
#include "share/io/ob_io_manager.h"
#include "share/ob_io_device_helper.h"
#include "storage/blocksstable/ob_block_manager.h"
#include "share/io/ob_io_manager.h"
#include "share/ob_force_print_log.h"
#include "storage/backup/ob_backup_data_struct.h"
#include "storage/backup/ob_backup_device_wrapper.h"

namespace oceanbase
{
namespace blocksstable
{

/**
 * ---------------------------------------ObMacroBlockHandle----------------------------------------
 */

ObMacroBlockHandle::~ObMacroBlockHandle()
{
  reset();
}

ObMacroBlockHandle::ObMacroBlockHandle(const ObMacroBlockHandle &other)
{
  *this = other;
}

ObMacroBlockHandle& ObMacroBlockHandle::operator=(const ObMacroBlockHandle &other)
{
  int ret = OB_SUCCESS;
  if (&other != this) {
    reset();
    io_handle_ = other.io_handle_;
    macro_id_ = other.macro_id_;
    if (macro_id_.is_valid()) {
      if (OB_FAIL(OB_SERVER_BLOCK_MGR.inc_ref(macro_id_))) {
        LOG_ERROR("failed to inc macro block ref cnt", K(ret), K(macro_id_));
      }
      if (OB_FAIL(ret)) {
        macro_id_.reset();
      }
    }
  }
  return *this;
}

void ObMacroBlockHandle::reset()
{
  io_handle_.reset();
  reset_macro_id();
}

void ObMacroBlockHandle::reuse()
{
  reset();
}

void ObMacroBlockHandle::reset_macro_id()
{
  int ret = OB_SUCCESS;
  if (macro_id_.is_valid()) {
    if (OB_FAIL(OB_STORAGE_OBJECT_MGR.dec_ref(macro_id_))) {
      LOG_ERROR("failed to dec macro block ref cnt", K(ret), K(macro_id_));
    } else {
      macro_id_.reset();
    }
  }
  abort_unless(OB_SUCCESS == ret);
}

int ObMacroBlockHandle::report_bad_block() const
{
  int ret = OB_SUCCESS;
  int io_errno = 0;
  if (OB_FAIL(io_handle_.get_fs_errno(io_errno))) {
    LOG_WARN("fail to get io errno, ", K(macro_id_), K(ret));
  } else if (0 != io_errno) {
    LOG_ERROR("fail to io macro block, ", K(macro_id_), K(ret), K(io_errno));
    char error_msg[common::OB_MAX_ERROR_MSG_LEN];
    MEMSET(error_msg, 0, sizeof(error_msg));
    if (OB_FAIL(databuff_printf(error_msg,
                                sizeof(error_msg),
                                "Sys IO error, ret=%d, errno=%d, errstr=%s",
                                ret,
                                io_errno,
                                strerror(io_errno)))){
      LOG_WARN("error msg is too long, ", K(macro_id_), K(ret), K(sizeof(error_msg)), K(io_errno));
    } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.report_bad_block(macro_id_,
                                                            ret,
                                                            error_msg,
                                                            GCONF.data_dir))) {
      LOG_WARN("fail to report bad block, ", K(macro_id_), K(ret), "erro_type", ret, K(error_msg));
    }
  }
  return ret;
}

uint64_t ObMacroBlockHandle::get_tenant_id()
{
  uint64_t tenant_id = MTL_ID();
  if (is_virtual_tenant_id(tenant_id) || 0 == tenant_id) {
    tenant_id = OB_SERVER_TENANT_ID; // use 500 tenant in io manager
  }
  return tenant_id;
}

int ObMacroBlockHandle::async_read(const ObMacroBlockReadInfo &read_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(!read_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid io argument", K(ret), K(read_info), KCSTRING(lbt()));
  } else {
    reuse();
    ObIOInfo io_info;
    backup::ObBackupWrapperIODevice *backup_device = nullptr;
    io_info.tenant_id_ = get_tenant_id();
    io_info.offset_ = read_info.offset_;
    io_info.size_ = static_cast<int32_t>(read_info.size_);
    io_info.flag_ = read_info.io_desc_;
    io_info.callback_ = read_info.io_callback_;
    io_info.fd_.first_id_ = read_info.macro_block_id_.first_id();
    io_info.fd_.second_id_ = read_info.macro_block_id_.second_id();
    io_info.fd_.third_id_ = read_info.macro_block_id_.third_id();
    io_info.fd_.device_handle_ = &LOCAL_DEVICE_INSTANCE;
    const int64_t real_timeout_ms = min(read_info.io_timeout_ms_, GCONF._data_storage_io_timeout / 1000L);
    io_info.timeout_us_ = real_timeout_ms * 1000L;
    io_info.user_data_buf_ = read_info.buf_;
    io_info.buf_ = read_info.buf_; // for sync io
    // resource manager level is higher than default
    io_info.flag_.set_sys_module_id(read_info.io_desc_.get_sys_module_id());

    io_info.flag_.set_read();
    if (io_info.fd_.is_backup_block_file()) {
      ObStorageIdMod mod;
      mod.storage_used_mod_ = ObStorageUsedMod::STORAGE_USED_RESTORE;
      if (OB_FAIL(backup::ObBackupDeviceHelper::get_device_and_fd(io_info.tenant_id_,
                                                                  io_info.fd_.first_id_,
                                                                  io_info.fd_.second_id_,
                                                                  io_info.fd_.third_id_,
                                                                  mod,
                                                                  backup_device,
                                                                  io_info.fd_))) {
        LOG_WARN("failed to get backup device and fd", K(ret), K(read_info));
      }
    }

    if (FAILEDx(ObIOManager::get_instance().aio_read(io_info, io_handle_))) {
      LOG_WARN("Fail to aio_read", K(read_info), K(ret));
    } else if (OB_FAIL(set_macro_block_id(read_info.macro_block_id_))) {
      LOG_WARN("failed to set macro block id", K(ret));
    }

    if (OB_NOT_NULL(backup_device)) {
      // fd ctx is hold by io request, close backup device and fd is safe here.
      if (OB_TMP_FAIL(backup::ObBackupDeviceHelper::close_device_and_fd(backup_device,
                                                                        io_info.fd_))) {
        LOG_ERROR("failed to close backup device and fd", K(ret), K(tmp_ret), K(read_info));
        ret = COVER_SUCC(tmp_ret);
      }
    }
  }
  return ret;
}

int ObMacroBlockHandle::async_write(const ObMacroBlockWriteInfo &write_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!write_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(write_info));
  } else {
    ObIOInfo io_info;
    io_info.tenant_id_ = get_tenant_id();
    io_info.offset_ = write_info.offset_;
    io_info.size_ = write_info.size_;
    io_info.buf_ = write_info.buffer_;
    io_info.flag_ = write_info.io_desc_;
    io_info.fd_.device_handle_ = write_info.device_handle_;
    io_info.fd_.first_id_ = macro_id_.first_id();
    io_info.fd_.second_id_ = macro_id_.second_id();
    io_info.fd_.third_id_ = macro_id_.third_id();
    io_info.fd_.device_handle_ = &LOCAL_DEVICE_INSTANCE;
    if (OB_FAIL(write_info.fill_io_info_for_backup(macro_id_, io_info))) {
      LOG_WARN("failed to fill io info for backup", K(ret), K_(macro_id));
    }
    const int64_t real_timeout_ms = min(write_info.io_timeout_ms_, GCONF._data_storage_io_timeout / 1000L);
    io_info.timeout_us_ = real_timeout_ms * 1000L;
    io_info.flag_.set_sys_module_id(write_info.io_desc_.get_sys_module_id());

    io_info.flag_.set_write();
    if (FAILEDx(ObIOManager::get_instance().aio_write(io_info, io_handle_))) {
      LOG_WARN("Fail to aio_write", K(ret), K_(macro_id), K(write_info));
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(OB_SERVER_BLOCK_MGR.update_write_time(macro_id_))) {
        LOG_WARN("fail to update write time for macro block", K(tmp_ret), K(macro_id_));
      }
      FLOG_INFO("Async write macro block", K(macro_id_), K(io_info.fd_));
    }
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObMacroBlockHandle::wait(const int64_t wait_timeout_ms)
{
  int ret = OB_SUCCESS;
  if (io_handle_.is_empty()) {
    // do nothing
  } else if (OB_FAIL(io_handle_.wait(wait_timeout_ms))) {
    if (OB_EAGAIN != ret) {
      LOG_WARN("fail to wait block io, may be retry", K(macro_id_), K(ret));
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = report_bad_block())) {
        LOG_WARN("fail to report bad block", K(tmp_ret), K(ret));
      }
      io_handle_.reset();
    }
  }
  return ret;
}

int ObMacroBlockHandle::get_io_ret() const
{
  return io_handle_.get_io_ret();
}

int ObMacroBlockHandle::set_macro_block_id(const MacroBlockId &macro_block_id)
{
  int ret = common::OB_SUCCESS;

  if (macro_id_.is_valid()) {
    ret = common::OB_ERR_SYS;
    LOG_ERROR("cannot set macro block id twice", K(ret), K(macro_block_id), K(*this));
  } else if (!macro_block_id.is_valid()) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(macro_block_id));
  } else {
    macro_id_ = macro_block_id;
    if (macro_id_.is_valid()) {
      if (OB_FAIL(OB_SERVER_BLOCK_MGR.inc_ref(macro_id_))) {
        LOG_ERROR("failed to inc macro block ref cnt", K(ret), K(macro_id_));
      }
      if (OB_FAIL(ret)) {
        macro_id_.reset();
      }
    }
  }
  return ret;
}

/**
 * --------------------------------------ObStorageObjectsHandle----------------------------------------
 */
ObStorageObjectsHandle::ObStorageObjectsHandle()
  : macro_id_list_()
{
  macro_id_list_.set_attr(ObMemAttr(OB_SERVER_TENANT_ID, "MacroIdList"));
}

ObStorageObjectsHandle::~ObStorageObjectsHandle()
{
  reset();
}

int ObStorageObjectsHandle::add(const MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(macro_id_list_.push_back(macro_id))) {
    LOG_WARN("failed to add macro id", K(ret));
  } else {
    if (OB_FAIL(OB_STORAGE_OBJECT_MGR.inc_ref(macro_id))) {
      LOG_ERROR("failed to inc macro block ref cnt", K(ret), K(macro_id));
    }

    if (OB_FAIL(ret)) {
      macro_id_list_.pop_back();
    }
  }

  return ret;
}

int ObStorageObjectsHandle::assign(const common::ObIArray<MacroBlockId> &list)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < list.count(); ++i) {
    if (!list.at(i).is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("list.at(i) isn't valid, ", K(ret), K(i), K(list.at(i)));
    } else if (OB_FAIL(macro_id_list_.push_back(list.at(i)))) {
      LOG_WARN("failed to add macro", K(ret));
    } else {
      if (OB_FAIL(OB_STORAGE_OBJECT_MGR.inc_ref(list.at(i)))) {
        LOG_ERROR("failed to inc macro block ref cnt", K(ret), "macro_id", list.at(i));
      }
      if (OB_FAIL(ret)) {
        macro_id_list_.pop_back();
      }
    }
  }
  return ret;
}

void ObStorageObjectsHandle::reset()
{
  if (macro_id_list_.count() > 0) {
    int tmp_ret = OB_SUCCESS;

    for (int64_t i = 0; i < macro_id_list_.count(); ++i) {
      if (OB_SUCCESS != (tmp_ret = OB_STORAGE_OBJECT_MGR.dec_ref(macro_id_list_.at(i)))) {
        LOG_ERROR_RET(tmp_ret, "failed to dec macro block ref cnt", K(tmp_ret), "macro_id", macro_id_list_.at(i));
      }
    }
  }
  macro_id_list_.reset();
}

int ObStorageObjectsHandle::reserve(const int64_t block_cnt)
{
  int ret = OB_SUCCESS;
  if (block_cnt > 0) {
    if (OB_FAIL(macro_id_list_.reserve(block_cnt))) {
      LOG_WARN("fail to reserve macro id list", K(ret));
    }
  }
  return ret;
}

} // namespace blocksstable
} // namespace oceanbase
