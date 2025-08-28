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

#define USING_LOG_PREFIX PALF
#include "log_io_adapter.h"
#include "share/ob_local_device.h"                            // ObLocalDevice
#include "share/resource_manager/ob_resource_manager.h"       // ObResourceManager
#include "share/io/ob_io_manager.h"                           // ObIOManager
#include "lib/string/ob_string.h"                             // ObString
#include "share/ob_device_manager.h"                          // ObDeviceManager

namespace oceanbase
{
using namespace share;
using namespace common;

namespace palf
{
// ========================= LogIODeviceWrapper=====================
int LogIODeviceWrapper::init(const char *clog_dir, 
                             const int64_t disk_io_thread_count,
                             const int64_t max_io_depth,
                             ObIOManager *io_manager,
                             ObDeviceManager *device_manager)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(clog_dir) || 0 >= disk_io_thread_count || 0 >= max_io_depth ||
      OB_ISNULL(io_manager) || OB_ISNULL(device_manager)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), KP(clog_dir), K(disk_io_thread_count), 
             K(max_io_depth), KP(io_manager), KP(device_manager));
  } else {
    const ObString storage_type_prefix(OB_LOCAL_PREFIX);
    const ObStorageIdMod storage_id_mod(0, ObStorageUsedMod::STORAGE_USED_CLOG);
    ObIODevice* device_handle = NULL;
    // useless except for being used to init log_local_device
    ObIODOpts iod_opts;
    if (OB_FAIL(device_manager->get_local_device(storage_type_prefix, storage_id_mod, device_handle))) {
      PALF_LOG(WARN, "failed to get device from ObDeviceManager", K(ret), K(storage_type_prefix), KP(device_handle));
    } else if (OB_ISNULL(log_local_device_ = static_cast<ObLocalDevice *>(device_handle))) {
      ret = OB_ERR_UNEXPECTED;
      PALF_LOG(WARN, "unexpected situations!", K(ret));
    } else if (OB_FAIL(log_local_device_->init(iod_opts))) {
      PALF_LOG(WARN, "fail to init io device", K(ret));
    } else if (OB_FAIL(io_manager->add_device_channel(log_local_device_, disk_io_thread_count, disk_io_thread_count, max_io_depth))) {
      PALF_LOG(WARN, "failed to add device channel", K(ret));
    } else {
      device_manager_ = device_manager;
      is_inited_ = true;
      PALF_LOG(INFO, "log_local_device_ init successfully", KP_(log_local_device));
    }

    if (OB_FAIL(ret) && OB_NOT_NULL(log_local_device_)) {
      (void) device_manager->release_device(device_handle);
      device_handle = NULL;
      log_local_device_ = NULL;
    }
  }
  return ret;
}

void LogIODeviceWrapper::destroy()
{
  if (is_inited_) {
    if (OB_NOT_NULL(log_local_device_)) {
      log_local_device_->destroy();
      device_manager_->release_device((ObIODevice*&) log_local_device_);
    }
    device_manager_ = NULL;
    is_inited_ = false;
    PALF_LOG(INFO, "LogIODeviceWrapper destory");
  }
}

share::ObLocalDevice *LogIODeviceWrapper::get_local_device()
{
  abort_unless(NULL != log_local_device_);
  return log_local_device_;
}
// ========================= LogIOAdapter=====================
int LogIOAdapter::init(const int64_t tenant_id, 
                       ObLocalDevice *log_local_device, 
                       ObResourceManager *resource_manager,
                       ObIOManager *io_manager)
{
  int ret = OB_SUCCESS;
  if (!is_valid_tenant_id(tenant_id) || OB_ISNULL(log_local_device) || OB_ISNULL(resource_manager) || OB_ISNULL(io_manager)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K(tenant_id), KP(log_local_device), KP(resource_manager), KP(io_manager));
  } else {
    tenant_id_ = tenant_id;
    log_local_device_ = log_local_device;
    resource_manager_ = resource_manager;
    io_manager_ = io_manager;
    is_inited_ = true;
  }

  return ret;
}

void LogIOAdapter::destroy()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  log_local_device_ = NULL;
  resource_manager_ = NULL;
  io_manager_ = NULL;
  is_inited_ = false;
}

int LogIOAdapter::open(const char *block_path, 
                       const int flags, 
                       const mode_t mode,
                       ObIOFd &io_fd)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LogIOAdapter is not inited", K(ret));
  } else if (OB_ISNULL(block_path) || 0 == STRLEN(block_path) || -1 == flags || -1 == mode) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument!", K(ret), KP(block_path), K(flags), K(mode));
  } else if (OB_FAIL(log_local_device_->open(block_path, flags, mode, io_fd))) {
    PALF_LOG(WARN, "failed to open file", K(ret), K(block_path), K(flags), K(mode));
  } else {
    io_fd.device_handle_ = log_local_device_;
    PALF_LOG(TRACE, "open file sucessfully", K(block_path), K(flags), K(mode), K(io_fd));
  }

  return ret;
}

int LogIOAdapter::close(ObIOFd &io_fd)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LogIOAdapter is not inited", K(ret));
  } else if (OB_ISNULL(log_local_device_)) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "the device does not exist or has been closed", K(ret));
  } else if (!io_fd.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, " the block has been closed", K(ret), K(io_fd));
  } else if (OB_FAIL(log_local_device_->close(io_fd))) {
    PALF_LOG(WARN, "close block failed", K(ret), K(io_fd));
  } else {
    PALF_LOG(TRACE, "close block successfully", K(io_fd));
    io_fd.reset();
  }

  return ret;
}

int LogIOAdapter::pwrite(const ObIOFd &io_fd, 
                         const char *buf, 
                         const int64_t count, 
                         const int64_t offset,
                         int64_t &write_size)
{
  int ret = OB_SUCCESS;
  write_size = 0;
  uint64_t consumer_group_id = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LogIOAdapter is not inited", K(ret));
  } else if (!io_fd.is_valid() || OB_ISNULL(buf) || 0 >= count || 0 > offset) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(io_fd), KP(buf), K(count), K(offset));
  } else {
    CONSUMER_GROUP_FUNC_GUARD(share::ObFunctionType::PRIO_CLOG_HIGH);

    ObIOInfo io_info;
    io_info.tenant_id_ = tenant_id_;
    io_info.fd_ = io_fd;
    io_info.offset_ = offset;
    io_info.size_ = count;
    io_info.flag_.set_mode(ObIOMode::WRITE);
    io_info.flag_.set_sys_module_id(ObIOModule::CLOG_WRITE_IO);
    io_info.flag_.set_wait_event(ObWaitEventIds::PALF_WRITE);
    io_info.buf_ = buf;
    io_info.callback_ = nullptr;
    if (OB_FAIL(io_manager_->pwrite(io_info, write_size))) {
      PALF_LOG(WARN, "io_manager_ pwrite failed", K(ret), K(io_info));
    } else if (write_size != count) {
      // partial write
      ret = OB_ERR_UNEXPECTED;
      PALF_LOG(WARN, "partital write", K(ret), K(io_info), K(write_size));
    } else {
      PALF_LOG(TRACE, "pwrite by io_manager_ successfully", K(io_info), K(write_size));
    }
  }
  return ret;
}

int LogIOAdapter::pread(const ObIOFd &io_fd, 
                        const int64_t count, 
                        const int64_t offset,
                        char *buf, 
                        int64_t &out_read_size,
                        LogIOContext &io_ctx)
{
  int ret = OB_SUCCESS;
  out_read_size = 0;
  uint64_t consumer_group_id = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LogIOAdapter is not inited", K(ret));
  } else if (!io_fd.is_valid() || OB_ISNULL(buf) || 0 > count || 0 > offset) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(io_fd), KP(buf), K(count), K(offset));
  } else {
    CONSUMER_GROUP_FUNC_GUARD(io_ctx.get_function_type());
    
    ObIOInfo io_info;
    io_info.tenant_id_ = tenant_id_;
    io_info.fd_ = io_fd;
    io_info.offset_ = offset;
    io_info.size_ = count;
    io_info.flag_.set_mode(ObIOMode::READ);
    io_info.flag_.set_sys_module_id(ObIOModule::CLOG_READ_IO);
    io_info.flag_.set_wait_event(ObWaitEventIds::PALF_READ);
    io_info.buf_ = buf;
    io_info.user_data_buf_ = buf;
    io_info.callback_ = nullptr;
    if (OB_FAIL(io_manager_->pread(io_info, out_read_size))) {
      PALF_LOG(WARN, "io_manager_ pread failed", K(ret), K(io_info), K(out_read_size));
    } else if (out_read_size != count) {
      // partial read
      ret = OB_ERR_UNEXPECTED;
      PALF_LOG(WARN, "the read size is not as same as read count, maybe concurrently with truncate",
               K(ret), K(io_info), K(out_read_size));
    } else {
      PALF_LOG(TRACE, "pread by io_manager_ successfully", K(io_info), K(out_read_size));
    }
  }

  return ret;
}

int LogIOAdapter::pread(const ObIOFd &io_fd, 
                        const int64_t count, 
                        const int64_t offset,
                        char *buf, 
                        int64_t &out_read_size)
{
  int ret = OB_SUCCESS;
  out_read_size = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LogIOAdapter is not inited", K(ret));
  } else if (!io_fd.is_valid() || OB_ISNULL(buf) || 0 > count || 0 > offset) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(io_fd), KP(buf), K(count), K(offset));
  } else if (count != (out_read_size = ob_pread(io_fd.second_id_, buf, count, offset))) {
    ret = convert_sys_errno();
    PALF_LOG(WARN, "ob_pread failed", K(ret), K(errno), K(offset), K(count), K(out_read_size), K(io_fd));
  } else {
    PALF_LOG(TRACE, "obpread successfully", K(offset), K(count), K(out_read_size), K(io_fd));
  }
  return ret;
}                                          

int LogIOAdapter::truncate(const ObIOFd &io_fd, const int64_t offset)
{
  int ret = OB_SUCCESS;
  if (!io_fd.is_valid() || 0 > offset) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(io_fd), K(offset));
  } else if (0 != ftruncate(io_fd.second_id_, offset)) {
    ret = convert_sys_errno();
    PALF_LOG(WARN, "ftruncate failed", K(ret), K(errno), K(io_fd), K(offset));
  }
  return ret;
}

} // end for palf
} // end for oceanbase
