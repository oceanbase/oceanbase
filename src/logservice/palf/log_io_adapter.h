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

#ifndef OCEANBASE_LOGSERVICE_LOG_IO_ADAPTER_
#define OCEANBASE_LOGSERVICE_LOG_IO_ADAPTER_

#include "common/storage/ob_io_device.h"
#include "common/storage/ob_device_common.h"
#include "log_io_context.h"                                   // LogIOContext
#include "logservice/ob_log_io_adapter.h"                     // ObLogIOAdapter
namespace oceanbase
{
namespace common
{
class ObIOManager;
class ObDeviceManager;
}
namespace share
{
class ObLocalDevice;
class ObResourceManager;
}
namespace palf
{

class LogIOAdapter
{
public:
  LogIOAdapter() : tenant_id_(OB_INVALID_TENANT_ID),
                   resource_manager_(NULL),
                   io_manager_(NULL),
                   is_inited_(false) {}
  ~LogIOAdapter() {
    destroy();
  }
  int init(const int64_t tenant_id,
           share::ObResourceManager *resource_manager,
           common::ObIOManager *io_manager);
  void destroy();
  bool is_valid() const {
    return is_valid_tenant_id(tenant_id_) && NULL != resource_manager_ && NULL != io_manager_;
  }

  // support iosolation interface
public:
  int pwrite(const ObIOFd &io_fd,
             const char *buf,
             const int64_t count,
             const int64_t offset,
             int64_t &write_size);
  int pread(const ObIOFd &io_fd,
            const int64_t count,
            const int64_t offset,
            char *buf,
            int64_t &out_read_size,
            LogIOContext &io_ctx);

public:
  // not support iosolation interface
  int open(const char *block_path,
           const int flags,
           const mode_t mode,
           ObIOFd &io_fd);
  int close(ObIOFd &io_fd);
  int pread(const ObIOFd &io_fd,
            const int64_t count,
            const int64_t offset,
            char *buf,
            int64_t &out_read_size);
  int truncate(const ObIOFd &fd, const int64_t offset);
  int fallocate(const ObIOFd &fd, mode_t mode, const int64_t offset, const int64_t len);
  int fsync(const ObIOFd &fd);
  int stat(const char *pathname,
           common::ObIODFileStat &statbuf);
  int fstat(const common::ObIOFd &fd,
            common::ObIODFileStat &statbuf);
  int rename(const char *oldpath,
             const char *newpath);
  int unlink(const char *pathname);

  // dir interface
  int scan_dir(const char *dir_name,
               common::ObBaseDirEntryOperator &op);
  int mkdir(const char *pathname,
            mode_t mode);
  int rmdir(const char *pathname);

  int register_cb(const char *log_dir,
                  logservice::SwitchLogIOModeCb &cb);
  int unregister_cb(const char *log_dir);
  int64_t get_align_size() const;
private:
  int64_t tenant_id_;
  share::ObResourceManager *resource_manager_;
  common::ObIOManager *io_manager_;
  bool is_inited_;
};
} // namespace palf
} // namespace oceanbase

#endif
