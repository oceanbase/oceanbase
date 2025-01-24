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

#include "ob_storage_object_handle.h"
#include "storage/backup/ob_backup_device_wrapper.h"
#include "share/ob_io_device_helper.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/shared_storage/ob_file_manager.h"
#endif

namespace oceanbase
{
namespace blocksstable
{

using namespace oceanbase::storage;

ObStorageObjectHandle::~ObStorageObjectHandle()
{
  reset();
}

ObStorageObjectHandle::ObStorageObjectHandle(const ObStorageObjectHandle &other)
{
  *this = other;
}

ObStorageObjectHandle &ObStorageObjectHandle::operator=(const ObStorageObjectHandle &other)
{
  int ret = OB_SUCCESS;
  if (&other != this) {
    reset();
    io_handle_ = other.io_handle_;
    macro_id_ = other.macro_id_;
    if (macro_id_.is_valid()) {
      if (macro_id_.is_id_mode_local()) {
        if (OB_FAIL(OB_SERVER_BLOCK_MGR.inc_ref(macro_id_))) {
          LOG_ERROR("failed to inc macro block ref cnt", K(ret), K(macro_id_));
        }
        if (OB_FAIL(ret)) {
          macro_id_.reset();
        }
      } else if (macro_id_.is_id_mode_backup()) {
        // do nothing
      } else if (macro_id_.is_id_mode_share()) {
        // do nothing
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected id mode", K(ret), "id_mode", macro_id_.id_mode());
        macro_id_.reset();
      }
    }
  }
  return *this;
}

void ObStorageObjectHandle::reset()
{
  io_handle_.reset();
  reset_macro_id();
}

void ObStorageObjectHandle::reuse()
{
  reset();
}

void ObStorageObjectHandle::reset_macro_id()
{
  int ret = OB_SUCCESS;
  if (macro_id_.is_valid()) {
    if (macro_id_.is_id_mode_local()) {
      if (OB_FAIL(OB_SERVER_BLOCK_MGR.dec_ref(macro_id_))) {
        LOG_ERROR("failed to dec macro block ref cnt", K(ret), K(macro_id_));
      } else {
        macro_id_.reset();
      }
    } else if (macro_id_.is_id_mode_backup()) {
      macro_id_.reset();
    } else if (macro_id_.is_id_mode_share()) {
      macro_id_.reset();
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected id mode", K(ret), "id_mode", macro_id_.id_mode());
    }
  }
  abort_unless(OB_SUCCESS == ret);
}

int ObStorageObjectHandle::report_bad_block() const
{
  int ret = OB_SUCCESS;
  if (macro_id_.is_id_mode_local()) {
    int io_errno = 0;
    if (OB_FAIL(io_handle_.get_fs_errno(io_errno))) {
      LOG_WARN("fail to get io errno", K(macro_id_), K(ret));
    } else if (0 != io_errno) {
      LOG_ERROR("fail to io macro block", K(macro_id_), K(ret), K(io_errno));
      char error_msg[common::OB_MAX_ERROR_MSG_LEN];
      MEMSET(error_msg, 0, sizeof(error_msg));
      if (OB_FAIL(databuff_printf(error_msg,
                                  sizeof(error_msg),
                                  "Sys IO error, ret=%d, errno=%d, errstr=%s",
                                  ret,
                                  io_errno,
                                  strerror(io_errno)))){
        LOG_WARN("error msg is too long", K(macro_id_), K(ret), K(sizeof(error_msg)), K(io_errno));
      } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.report_bad_block(macro_id_,
                                                              ret,
                                                              error_msg,
                                                              GCONF.data_dir))) {
        LOG_WARN("fail to report bad block", K(macro_id_), K(ret), "erro_type", ret, K(error_msg));
      }
    }
  } else if (macro_id_.is_id_mode_backup()) {
    // do nothing
  } else if (macro_id_.is_id_mode_share()) {
    // do nothing
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected id mode", K(ret), "id_mode", macro_id_.id_mode());
  }
  return ret;
}

uint64_t ObStorageObjectHandle::get_tenant_id()
{
  uint64_t tenant_id = MTL_ID();
  if (is_virtual_tenant_id(tenant_id) || 0 == tenant_id) {
    tenant_id = OB_SERVER_TENANT_ID; // use 500 tenant in io manager
  }
  return tenant_id;
}

int ObStorageObjectHandle::async_read(const ObStorageObjectReadInfo &read_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!read_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid io argument", K(ret), K(read_info), KCSTRING(lbt()));
  } else {
    if (read_info.macro_block_id_.is_id_mode_local()) {
      if (OB_FAIL(sn_async_read(read_info))) {
        LOG_WARN("fail to sn_async_read", K(ret), K(read_info));
      }
    } else if (read_info.macro_block_id_.is_id_mode_backup()) {
      if (OB_FAIL(sn_async_read(read_info))) {
        LOG_WARN("fail to backup_async_read", K(ret), K(read_info));
      }
#ifdef OB_BUILD_SHARED_STORAGE
    } else if (read_info.macro_block_id_.is_id_mode_share()) {
      if (OB_FAIL(ss_async_read(read_info))) {
        LOG_WARN("fail to ss_async_read", K(ret), K(read_info));
      }
#endif
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected id mode", K(ret), "id_mode", read_info.macro_block_id_.id_mode(),
               K(read_info));
    }
  }
  return ret;
}

int ObStorageObjectHandle::async_write(const ObStorageObjectWriteInfo &write_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!write_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(write_info));
  } else {
    if (macro_id_.is_id_mode_local()) {
      if (OB_FAIL(sn_async_write(write_info))) {
        LOG_WARN("fail to sn_async_write", K(ret), K_(macro_id), K(write_info));
      }
    } else if (macro_id_.is_id_mode_backup()) {
      if (OB_FAIL(sn_async_write(write_info))) {
        LOG_WARN("fail to backup_async_write", K(ret), K_(macro_id), K(write_info));
      }
#ifdef OB_BUILD_SHARED_STORAGE
    } else if (macro_id_.is_id_mode_share()) {
      if (OB_FAIL(ss_async_write(write_info))) {
        LOG_WARN("fail to ss_async_write", K(ret), K_(macro_id), K(write_info));
      }
#endif
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected id mode", K(ret), "id_mode", macro_id_.id_mode(), K_(macro_id),
               K(write_info));
    }
  }
  return ret;
}

int ObStorageObjectHandle::sn_async_read(const ObStorageObjectReadInfo &read_info)
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
    io_info.flag_.set_sys_module_id(read_info.io_desc_.get_sys_module_id());
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
      } else {
        io_info.flag_.set_sync();
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

int ObStorageObjectHandle::sn_async_write(const ObStorageObjectWriteInfo &write_info)
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
    io_info.fd_.first_id_ = macro_id_.first_id();
    io_info.fd_.second_id_ = macro_id_.second_id();
    io_info.fd_.third_id_ = macro_id_.third_id();
    io_info.fd_.device_handle_ = (nullptr == write_info.device_handle_) ? &LOCAL_DEVICE_INSTANCE : write_info.device_handle_;
    if (OB_FAIL(write_info.fill_io_info_for_backup(macro_id_, io_info))) {
      LOG_WARN("failed to fill io info for backup", K(ret), K_(macro_id));
    }
    io_info.flag_.set_sys_module_id(write_info.io_desc_.get_sys_module_id());
    const int64_t real_timeout_ms = min(write_info.io_timeout_ms_, GCONF._data_storage_io_timeout / 1000L);
    io_info.timeout_us_ = real_timeout_ms * 1000L;

    io_info.flag_.set_write();
    if (FAILEDx(ObIOManager::get_instance().aio_write(io_info, io_handle_))) {
      LOG_WARN("Fail to aio_write", K(ret), K_(macro_id), K(write_info));
    } else {
      int tmp_ret = OB_SUCCESS;
      if (macro_id_.is_id_mode_local() && OB_TMP_FAIL(OB_SERVER_BLOCK_MGR.update_write_time(macro_id_))) {
        LOG_WARN("fail to update write time for macro block", K(tmp_ret), K(macro_id_));
      }
      FLOG_INFO("Async write macro block", K(macro_id_));
    }
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObStorageObjectHandle::ss_async_read(const ObStorageObjectReadInfo &read_info)
{
  int ret = OB_SUCCESS;
  ObBaseFileManager *file_manager = nullptr;
  if (OB_UNLIKELY(!read_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid io argument", K(ret), K(read_info), KCSTRING(lbt()));
  } else if (OB_FAIL(get_file_manager(read_info.mtl_tenant_id_, file_manager))) {
    LOG_WARN("fail to get file manager", KR(ret), "tenant_id", read_info.mtl_tenant_id_, K(read_info));
  } else if (OB_FAIL(file_manager->async_pread_file(read_info, *this))) {
    LOG_WARN("fail to async pread file", KR(ret), K(read_info), KPC(this));
  }
  return ret;
}

int ObStorageObjectHandle::ss_async_write(const ObStorageObjectWriteInfo &write_info)
{
  int ret = OB_SUCCESS;
  ObBaseFileManager *file_manager = nullptr;
  ObStorageObjectType object_type = macro_id_.storage_object_type();
  if (OB_UNLIKELY(!write_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(write_info));
  } else if (OB_FAIL(get_file_manager(write_info.mtl_tenant_id_, file_manager))) {
    LOG_WARN("fail to get file manager", KR(ret), "tenant_id", write_info.mtl_tenant_id_, K(write_info));
  } else if (ObStorageObjectType::TMP_FILE == object_type) {
    if (OB_FAIL(file_manager->async_append_file(write_info, *this))) {
      LOG_WARN("fail to async append file", KR(ret), K(write_info), KPC(this));
    }
  } else {
    if (OB_FAIL(file_manager->async_write_file(write_info, *this))) {
      LOG_WARN("fail to async write file", KR(ret), K(write_info), KPC(this));
    }
  }
  return ret;
}

int ObStorageObjectHandle::get_file_manager(
    const uint64_t tenant_id,
    ObBaseFileManager *&file_manager)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_SERVER_TENANT_ID == tenant_id) {
    file_manager = &OB_SERVER_FILE_MGR;
  } else if (OB_ISNULL(file_manager = MTL(ObTenantFileManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file manager is null", KR(ret), K(tenant_id));
  }
  return ret;
}
#endif

int ObStorageObjectHandle::wait()
{
  int ret = OB_SUCCESS;
  if (io_handle_.is_empty()) {
    // do nothing
  } else if (OB_FAIL(io_handle_.wait())) {
    LOG_WARN("fail to wait block io, may be retry", K(macro_id_), K(ret));
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = report_bad_block())) {
      LOG_WARN("fail to report bad block", K(tmp_ret), K(ret));
    }
    io_handle_.reset();
  }
  return ret;
}

int ObStorageObjectHandle::get_io_ret() const
{
  return io_handle_.get_io_ret();
}

int ObStorageObjectHandle::get_io_time_us(int64_t &io_time_us) const
{
  return io_handle_.get_io_time_us(io_time_us);
}

int ObStorageObjectHandle::check_is_finished(bool &is_finished)
{
  return io_handle_.check_is_finished(is_finished);
}

int ObStorageObjectHandle::set_macro_block_id(const MacroBlockId &macro_block_id)
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
      if (macro_id_.is_id_mode_local()) {
        if (OB_FAIL(OB_SERVER_BLOCK_MGR.inc_ref(macro_id_))) {
          LOG_ERROR("failed to inc macro block ref cnt", K(ret), K(macro_id_));
        }
        if (OB_FAIL(ret)) {
          macro_id_.reset();
        }
      } else if (macro_id_.is_id_mode_backup()) {
        // do nothing
      } else if (macro_id_.is_id_mode_share()) {
        // do nothing
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected id mode", K(ret), "id_mode", macro_id_.id_mode());
        macro_id_.reset();
      }
    }
  }
  return ret;
}

} // namespace blocksstable
} // namespace oceanbase
