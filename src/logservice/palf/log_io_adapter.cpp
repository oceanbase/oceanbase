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
#include "share/resource_manager/ob_resource_manager.h"       // ObResourceManager
#include "share/io/ob_io_manager.h"                           // ObIOManager
#include "logservice/ob_log_io_adapter.h"                     // LOG_IO_ADAPTER
namespace oceanbase
{
using namespace share;
using namespace common;
using namespace logservice;

namespace palf
{
// ========================= LogIOAdapter=====================
int LogIOAdapter::init(const int64_t tenant_id,
                       ObResourceManager *resource_manager,
                       ObIOManager *io_manager)
{
  int ret = OB_SUCCESS;
  if (!is_valid_tenant_id(tenant_id) || OB_ISNULL(resource_manager) || OB_ISNULL(io_manager)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K(tenant_id), KP(resource_manager), KP(io_manager));
  } else {
    tenant_id_ = tenant_id;
    resource_manager_ = resource_manager;
    io_manager_ = io_manager;
    is_inited_ = true;
  }

  return ret;
}

void LogIOAdapter::destroy()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  resource_manager_ = NULL;
  io_manager_ = NULL;
  is_inited_ = false;
}

// ========================== begin support iosolation interface ========================
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
  } else if (OB_FAIL(get_group_id_for_write_(consumer_group_id))){
    PALF_LOG(WARN, "get_group_id_for_write_ failed", K(ret), K(consumer_group_id));
  } else {
    ObIOInfo io_info;
    io_info.tenant_id_ = tenant_id_;
    io_info.fd_ = io_fd;
    io_info.offset_ = offset;
    io_info.size_ = count;
    io_info.flag_.set_mode(ObIOMode::WRITE);
    io_info.flag_.set_resource_group_id(consumer_group_id);
    io_info.flag_.set_sys_module_id(ObIOModule::CLOG_IO);
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
  } else if (OB_FAIL(get_group_id_for_read_(io_ctx, consumer_group_id))){
    PALF_LOG(WARN, "get_group_id_for_read_ failed", K(ret), K(io_ctx), K(consumer_group_id));
  } else {
    ObIOInfo io_info;
    io_info.tenant_id_ = tenant_id_;
    io_info.fd_ = io_fd;
    io_info.offset_ = offset;
    io_info.size_ = count;
    io_info.flag_.set_mode(ObIOMode::READ);
    io_info.flag_.set_resource_group_id(consumer_group_id);
    io_info.flag_.set_sys_module_id(ObIOModule::CLOG_IO);
    io_info.flag_.set_wait_event(ObWaitEventIds::PALF_READ);
    io_info.buf_ = buf;
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

// ========================== end support iosolation interface ========================


// ========================== begin not support iosolation interface ========================
int LogIOAdapter::open(const char *block_path,
                       const int flags,
                       const mode_t mode,
                       ObIOFd &io_fd)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LogIOAdapter is not inited", K(ret));
  } else if (OB_FAIL(LOG_IO_ADAPTER.open(block_path, flags, mode, io_fd))) {
    PALF_LOG(WARN, "open file failed", K(block_path), K(flags), K(mode), K(io_fd));
  } else {
  }
  return ret;
}

int LogIOAdapter::close(ObIOFd &io_fd)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LogIOAdapter is not inited", K(ret));
  } else if (OB_FAIL(LOG_IO_ADAPTER.close(io_fd))) {
    PALF_LOG(WARN, "close file failed", KR(ret), K(io_fd));
  } else {
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
  } else if (OB_FAIL(LOG_IO_ADAPTER.pread(io_fd, count, offset, buf, out_read_size))) {
    PALF_LOG(WARN, "pread failed", K(ret), K(io_fd), K(count), K(offset), KP(buf));
  } else {}
  return ret;
}

int LogIOAdapter::truncate(const ObIOFd &io_fd, const int64_t offset)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LogIOAdapter is not inited", K(ret));
  } else if (OB_FAIL(LOG_IO_ADAPTER.truncate(io_fd, offset))) {
    PALF_LOG(WARN, "truncate failed", K(ret));
  } else {}
  return ret;
}

int LogIOAdapter::fallocate(const ObIOFd &fd,
                            mode_t mode,
                            const int64_t offset,
                            const int64_t len)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LogIOAdapter is not inited", K(ret));
  } else if (OB_FAIL(LOG_IO_ADAPTER.fallocate(fd, mode, offset, len))) {
    PALF_LOG(WARN, "fallocate failed", K(ret));
  } else {}
  return ret;
}

int LogIOAdapter::fsync(const ObIOFd &fd)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LogIOAdapter is not inited", K(ret));
  } else if (OB_FAIL(LOG_IO_ADAPTER.fsync(fd))) {
    PALF_LOG(WARN, "fsync failed", K(ret));
  } else {}
  return ret;
}

int LogIOAdapter::stat(const char *pathname,
                       common::ObIODFileStat &statbuf)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LogIOAdapter is not inited", K(ret));
  } else if (OB_FAIL(LOG_IO_ADAPTER.stat(pathname, statbuf))) {
    PALF_LOG(WARN, "stat failed", K(ret));
  } else {}
  return ret;
}

int LogIOAdapter::fstat(const common::ObIOFd &fd,
                        common::ObIODFileStat &statbuf)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LogIOAdapter is not inited", K(ret));
  } else if (OB_FAIL(LOG_IO_ADAPTER.fstat(fd, statbuf))) {
    PALF_LOG(WARN, "fstat failed", K(ret));
  } else {}
  return ret;
}

int LogIOAdapter::rename(const char *oldpath,
                         const char *newpath)
{

  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LogIOAdapter is not inited", K(ret));
  } else if (OB_FAIL(LOG_IO_ADAPTER.rename(oldpath, newpath))) {
    PALF_LOG(WARN, "rename failed", K(ret));
  } else {}
  return ret;
}

int LogIOAdapter::unlink(const char *pathname)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LogIOAdapter is not inited", K(ret));
  } else if (OB_FAIL(LOG_IO_ADAPTER.unlink(pathname))) {
    PALF_LOG(WARN, "unlink failed", K(ret));
  } else {}
  return ret;
}

int LogIOAdapter::scan_dir(const char *dir_name,
                           common::ObBaseDirEntryOperator &op)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LogIOAdapter is not inited", K(ret));
  } else if (OB_FAIL(LOG_IO_ADAPTER.scan_dir(dir_name, op))) {
    PALF_LOG(WARN, "scan_dir failed", K(ret));
  } else {}
  return ret;
}

int LogIOAdapter::mkdir(const char *pathname,
                        mode_t mode)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LogIOAdapter is not inited", K(ret));
  } else if (OB_FAIL(LOG_IO_ADAPTER.mkdir(pathname, mode))) {
    PALF_LOG(WARN, "mkdir failed", K(ret));
  } else {}
  return ret;
}

int LogIOAdapter::rmdir(const char *pathname)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LogIOAdapter is not inited", K(ret));
  } else if (OB_FAIL(LOG_IO_ADAPTER.rmdir(pathname))) {
    PALF_LOG(WARN, "rmdir failed", K(ret));
  } else {}
  return ret;
}

int LogIOAdapter::register_cb(const char *log_dir,
                              SwitchLogIOModeCb &cb)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LogIOAdapter is not inited", K(ret));
  } else if (NULL == log_dir || !cb.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), KP(log_dir), K(cb));
  } else {
    SwitchLogIOModeCbKey key(log_dir);
    if (OB_FAIL(LOG_IO_ADAPTER.register_cb(key, cb))) {
      PALF_LOG(WARN, "register_cb failed", KR(ret), K(key));
    }
  }
  return ret;
}

int LogIOAdapter::unregister_cb(const char *log_dir)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LogIOAdapter is not inited", K(ret));
  } else if (NULL == log_dir) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), KP(log_dir));
  } else {
    SwitchLogIOModeCbKey key(log_dir);
    LOG_IO_ADAPTER.unregister_cb(key);
  }
  return ret;
}

int64_t LogIOAdapter::get_align_size() const
{
  return LOG_IO_ADAPTER.choose_align_size();
}
// ========================== end not support iosolation interface ========================

int LogIOAdapter::get_group_id_for_write_(uint64_t &consumer_group_id)
{
  int ret = OB_SUCCESS;
  consumer_group_id = 0;
  if (OB_FAIL(resource_manager_->get_mapping_rule_mgr().get_group_id_by_function_type(tenant_id_, share::ObFunctionType::PRIO_CLOG_HIGH, consumer_group_id))) {
    PALF_LOG(WARN, "fail to get group_id by function", K(ret), K_(tenant_id), K(consumer_group_id));
  } else if (FALSE_IT(SET_FUNCTION_TYPE(share::ObFunctionType::PRIO_CLOG_HIGH))) {
  } else {
    PALF_LOG(TRACE, "get group_id by function successfully", K_(tenant_id), K(consumer_group_id));
  }
  return ret;
}

int LogIOAdapter::get_group_id_for_read_(const LogIOContext &io_ctx, uint64_t &consumer_group_id)
{
  int ret = OB_SUCCESS;
  consumer_group_id = 0;
  const share::ObFunctionType function_type = io_ctx.get_function_type();
  if (OB_FAIL(resource_manager_->get_mapping_rule_mgr().get_group_id_by_function_type(tenant_id_, function_type, consumer_group_id))) {
    PALF_LOG(WARN, "fail to get group_id by function", K(ret), K_(tenant_id), K(function_type), K(consumer_group_id));
  } else if (FALSE_IT(SET_FUNCTION_TYPE(function_type))) {
  } else {
    PALF_LOG(TRACE, "get group_id by function successfully", K_(tenant_id), K(function_type), K(consumer_group_id));
  }
  return ret;
}

} // end for palf
} // end for oceanbase
