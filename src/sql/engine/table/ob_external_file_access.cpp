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

#define USING_LOG_PREFIX SQL

#include "ob_external_file_access.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/ob_device_manager.h"
#include "share/backup/ob_backup_struct.h"
#include "share/external_table/ob_external_table_utils.h"
#include "sql/engine/expr/ob_expr_regexp_context.h"
#include "ob_external_table_access_service.h"
#include "ob_external_data_access_mgr.h"

namespace oceanbase
{
namespace sql
{

/***************** ObExternalReadInfo ****************/
void ObExternalReadInfo::reset()
{
  buffer_ = nullptr;
  offset_ = 0;
  size_ = 0;
  io_timeout_ms_ = 0;
  io_desc_.reset();
  io_callback_ = nullptr;
}

bool ObExternalReadInfo::is_valid() const
{
  return io_desc_.is_valid() && size_ > 0;
}

/***************** ObExternalFileAccess ****************/
void ObExternalFileReadHandle::reset()
{
  object_handles_.reset();
}
bool ObExternalFileReadHandle::is_valid() const
{
  return object_handles_.count() > 0;
}

int ObExternalFileReadHandle::wait()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < object_handles_.count(); ++i) {
    ObStorageObjectHandle object_handle = object_handles_.at(i);
    if (OB_UNLIKELY(!object_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected invalid object handle", K(ret), K(i), K(object_handle), KPC(this));
    } else if (OB_FAIL(object_handle.wait())) {
      if (OB_DATA_OUT_OF_RANGE == ret && object_handle.get_data_size() < expect_read_size_.at(i)) {
        ret = OB_SUCCESS;
        expect_read_size_[i] = object_handle.get_data_size();
      } else {
        LOG_WARN("Failt to wait object handle finish", K(ret), K(object_handle));
      }
    }
  }
  return ret;
}

int ObExternalFileReadHandle::add_object_handle(
    const blocksstable::ObStorageObjectHandle &object_handle,
    const int64_t user_rd_length)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!object_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(object_handle));
  } else if (OB_FAIL(object_handles_.push_back(object_handle))) {
    LOG_WARN("Fail to push back object handle", K(ret));
  } else if (OB_FAIL(expect_read_size_.push_back(user_rd_length))) {
    LOG_WARN("Fail to push back user_rd_length", K(ret));
  }
  return ret;
}

int ObExternalFileReadHandle::get_cache_read_data_size(int64_t &read_size) const
{
  int ret = OB_SUCCESS;
  read_size = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < object_handles_.count(); ++i) {
    ObStorageObjectHandle object_handle = object_handles_.at(i);
    if (OB_UNLIKELY(!object_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected invalid object handle", K(ret), K(i), K(object_handle), KPC(this));
    } else {
      read_size += object_handle.get_data_size();
    }
  }
  return ret;
}

int ObExternalFileReadHandle::get_user_buf_read_data_size(int64_t &read_size) const
{
  int ret = OB_SUCCESS;
  read_size = 0;
  for (int64_t i = 0; i < expect_read_size_.count(); ++i) {
    read_size += expect_read_size_.at(i);
  }
  read_size += cache_hit_size_;
  return ret;
}


/***************** ObExternalFileAccess ****************/

ObExternalFileAccess::~ObExternalFileAccess()
{
  int ret = OB_SUCCESS;
  if (is_opened()) {
    LOG_WARN("has file opened when deconstruct, file may fail to close", KPC(this));
  }
  reset();
}

int ObExternalFileAccess::reset()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(close())) {
    LOG_WARN("failed to close file", K(ret));
  }

  cache_options_.reset();
  fd_.reset();
  return ret;
}

int ObExternalFileAccess::open(
    const ObExternalFileUrlInfo &info,
    const ObExternalFileCacheOptions &cache_options)
{
  int ret = OB_SUCCESS;
  ObObjectStorageInfo *access_info = nullptr;
  ObHDFSStorageInfo hdfs_storage_info;
  ObBackupStorageInfo backup_storage_info;
  int64_t modify_time = -1;
  if (is_opened()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("file has opened in this Access, call close before open another one", K(ret), K(lbt()));
  } else if (OB_FAIL(get_storage_type_from_path_for_external_table(info.location_, storage_type_))) {
    LOG_WARN("fail to resove storage type", K(ret));
  } else {
    // see ObExternalDataAccessDriver::init
    ObArenaAllocator temp_allocator;
    ObString access_info_cstr;
    if (storage_type_ == OB_STORAGE_FILE) {
      access_info_cstr.assign_ptr(&DUMMY_EMPTY_CHAR_, strlen(&DUMMY_EMPTY_CHAR_));
    } else if (OB_FAIL(ob_write_string(temp_allocator, info.access_info_, access_info_cstr, true))) {
      LOG_WARN("failed to write string into access_info_cstr", K(ret), K(info));
    }

    if (storage_type_ == OB_STORAGE_HDFS) {
      access_info = &hdfs_storage_info;
    } else {
      access_info = &backup_storage_info;
    }

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(access_info)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("failed to get access info", K(ret), K(storage_type_));
    } else if (OB_FAIL(access_info->set(storage_type_, access_info_cstr.ptr()))) {
      LOG_WARN("failed to set access_info", K(ret), K(storage_type_));
    } else if (OB_FAIL(ObBackupIoAdapter::get_file_modify_time(info.url_, access_info, modify_time))) {
      LOG_WARN("failed to get modify_time", K(ret));
    }
  }

  ObExternalDataAccessMgr *exdam = nullptr;
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(exdam = MTL(ObExternalDataAccessMgr*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObExternalDataAccessMgr", K(ret), KP(exdam), K(MTL_ID()));
  } else if (OB_FAIL(exdam->open_and_reg_file(info.url_, access_info, modify_time, fd_))) {
    LOG_WARN("failed to register file to DAM", K(ret), K(info), K(access_info), K(modify_time), K(fd_));
  } else {
    cache_options_ = cache_options;
  }
  return ret;
}

int ObExternalFileAccess::close()
{
  int ret = OB_SUCCESS;
  if (is_opened()) {
    ObExternalDataAccessMgr *exdam = nullptr;
    if (OB_ISNULL(exdam = MTL(ObExternalDataAccessMgr*))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get ObExternalDataAccessMgr", K(ret), KP(exdam), K(MTL_ID()));
    } else if (OB_FAIL(exdam->close_file(fd_))) {
      LOG_ERROR("faile to register file to DAM", K(ret), K(fd_));
    } else if (fd_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("after close file, fd_ should be invalid", K(ret), K(fd_), K(cache_options_));
    }
  }
  return ret;
}

int ObExternalFileAccess::inner_process_read_info_(
    const ObExternalReadInfo &input_info,
    ObExternalReadInfo &out_info) const
{
  int ret = OB_SUCCESS;
  out_info = input_info;
  // TODO process io_desc_, TODO @shifangdan.sfd
  out_info.io_desc_.set_mode(ObIOMode::READ);
  out_info.io_desc_.set_wait_event(ObWaitEventIds::OBJECT_STORAGE_READ);
  // if (cache_options_.enable_disk_cache()) {
  //   out_info.io_desc_.set_buffered_read();
  // } else {
  //   out_info.io_desc_.set_buffered_read(false);
  // }
  return ret;
}


int ObExternalFileAccess::async_read(
    const ObExternalReadInfo &info,
    ObExternalFileReadHandle &handle)
{
  int ret = OB_SUCCESS;
  int64_t read_size = 0;
  ObExternalReadInfo new_info = info;
  ObExternalDataAccessMgr *exdam = nullptr;
  if (!is_opened()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no opened file", K(ret), K(fd_), K(lbt()));
  } else if (OB_FAIL(inner_process_read_info_(info, new_info))) {
    LOG_WARN("failed to construct new_read_info", K(ret), K(info), K(new_info));
  } else if (!new_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(new_info), K(info));
  } else if (OB_ISNULL(exdam = MTL(ObExternalDataAccessMgr*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObExternalDataAccessMgr", K(ret), K(lbt()));
  } else if (OB_FAIL(exdam->async_read(fd_, new_info, cache_options_.enable_page_cache(), handle))) {
    STORAGE_LOG(WARN, "fail to get file length", KR(ret), K(fd_));
  }
  return ret;
}

int ObExternalFileAccess::pread(
    const ObExternalReadInfo &info,
    int64_t &read_size)
{
  int ret = OB_SUCCESS;
  ObExternalReadInfo new_info = info;
  ObExternalDataAccessMgr *exdam = nullptr;
  ObExternalFileReadHandle read_handle;
  if (!is_opened()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no opened file", K(ret), K(fd_), K(lbt()));
  } else if (OB_FAIL(inner_process_read_info_(info, new_info))) {
    LOG_WARN("failed to construct new_read_info", K(ret), K(info), K(new_info));
  } else if (!new_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(new_info), K(info));
  } else if (OB_ISNULL(exdam = MTL(ObExternalDataAccessMgr*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObExternalDataAccessMgr", K(ret), K(lbt()));
  } else if (OB_FAIL(exdam->async_read(fd_, new_info, cache_options_.enable_page_cache(), read_handle))) {
    LOG_WARN("failed to async_read", K(ret), K(fd_), K(info), K(read_handle));
  } else if (OB_FAIL(read_handle.wait())) {
    LOG_WARN("wait failed", K(ret), K(fd_), K(info));
  } else if (OB_FAIL(read_handle.get_user_buf_read_data_size(read_size))) {
    LOG_WARN("failed to get read_size", K(ret), K(read_size), K(info), K(read_handle));
  }
  STORAGE_LOG(TRACE, "Read size and Real_read_size", K(ret), K(info), K(read_size), K(read_handle),
              K(cache_options_));
  return ret;
}

bool ObExternalFileAccess::is_opened() const
{
  return fd_.is_valid();
}

int ObExternalFileAccess::get_file_size(int64_t &file_size)
{
  int ret = OB_SUCCESS;
  ObExternalDataAccessMgr *exdam = nullptr;
  if (!is_opened()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should has a opened file", K(ret), K(fd_), K(lbt()));
  } else if (OB_ISNULL(exdam = MTL(ObExternalDataAccessMgr*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObExternalDataAccessMgr", K(ret), K(lbt()));
  } else if (OB_FAIL(exdam->get_file_size_by_fd(fd_, file_size))) {
    STORAGE_LOG(WARN, "fail to get file length", KR(ret), K(fd_));
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
