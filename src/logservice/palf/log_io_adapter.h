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

class LogIODeviceWrapper
{
public:
  static LogIODeviceWrapper &get_instance() {
    static LogIODeviceWrapper instance;
    return instance;
  }
public:
  LogIODeviceWrapper() : log_local_device_(NULL), device_manager_(NULL), is_inited_(false) {}
  int init(const char *clog_dir, 
           const int64_t disk_io_thread_count,
           const int64_t max_io_depth,
           common::ObIOManager *io_manager,
           common::ObDeviceManager *device_manager);
  void destroy();
  share::ObLocalDevice* get_local_device();
private:
  share::ObLocalDevice *log_local_device_;
  common::ObDeviceManager *device_manager_;
  bool is_inited_;
};

class LogIOAdapter
{
public:
  LogIOAdapter() : tenant_id_(OB_INVALID_TENANT_ID), log_local_device_(NULL), 
                   resource_manager_(NULL), io_manager_(NULL), is_inited_(false) {}
  ~LogIOAdapter() {
    destroy();
  }
  int init(const int64_t tenant_id, 
           share::ObLocalDevice *log_local_device, 
           share::ObResourceManager *resource_manager,
           common::ObIOManager *io_manager);
  void destroy();
  bool is_valid() const {
    return is_valid_tenant_id(tenant_id_) && NULL != log_local_device_ && NULL != resource_manager_ && NULL != io_manager_;
  }
  int open(const char *block_path, 
           const int flags, 
           const mode_t mode,
           ObIOFd &io_fd);
  int close(ObIOFd &io_fd);
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
  // directly pread without iosolation
  int pread(const ObIOFd &io_fd, 
            const int64_t count, 
            const int64_t offset,
            char *buf, 
            int64_t &out_read_size);
  int truncate(const ObIOFd &fd, const int64_t offset);
private:
  int64_t tenant_id_;
  share::ObLocalDevice *log_local_device_;
  share::ObResourceManager *resource_manager_;
  common::ObIOManager *io_manager_;
  bool is_inited_;
};
}

#define LOG_IO_DEVICE_WRAPPER ::oceanbase::palf::LogIODeviceWrapper::get_instance()
}

#endif