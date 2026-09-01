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

#include "lib/utility/ob_macro_utils.h"                        // DISALLOW_COPY_AND_ASSIGN
#include "common/storage/ob_io_device.h"
#include "common/storage/ob_device_common.h"
#include "share/io/ob_io_define.h"                            // ObIOCallback
#include "log_io_context.h"                                   // LogIOContext
#include "log_async_io_struct.h"                              // AsyncPwriteRequest, FragmentRef

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
class IAsyncPalfIOCtx;

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
                   resource_manager_(NULL), io_manager_(NULL),
                   is_inited_(false) {}
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

  // ============================ Async write path ============================
  // Submit an async write to ObIOManager. On success, ObIOManager owns the
  // callback, so the caller buffer must stay valid and unchanged until IO
  // completion. The callback pins the async PALF ctx and reports completion
  // through on_aio_complete().
  //
  // On synchronous submit failure, this method releases the callback and
  // returns the original error. No completion event is generated.
  int aio_write(const ObIOFd &io_fd,
                const int64_t offset,
                const AsyncPwriteRequest &req,
                common::ObIOHandle &out_handle);
private:
  void init_write_io_info_(const ObIOFd &io_fd,
                           const char *buf,
                           const int64_t count,
                           const int64_t offset,
                           const bool use_caller_write_buf,
                           common::ObIOInfo &io_info) const;

  int64_t tenant_id_;
  share::ObLocalDevice *log_local_device_;
  share::ObResourceManager *resource_manager_;
  common::ObIOManager *io_manager_;
  bool is_inited_;
};

// ObIOManager owns this callback after submit success. It pins the async PALF
// ctx, reports completion directly, wakes the worker when more progress is
// needed, and releases the pin before destruction. The callback path must not
// do disk IO, wait, or run work without a bounded cost.
//
// Callback memory uses the process-lifetime allocator, so the IO framework can
// free it after callback destruction without depending on LogIOAdapter lifetime.
class LogAsyncIOCallback : public common::ObIOCallback
{
public:
  LogAsyncIOCallback();
  LogAsyncIOCallback(IAsyncPalfIOCtx *ctx,
                     const FragmentRef &fragment_ref,
                     const LSN &begin_lsn,
                     const LSN &end_lsn,
                     int64_t submit_ts);
  virtual ~LogAsyncIOCallback();

  // ObIOCallback interface
  virtual common::ObIAllocator *get_allocator() override;
  virtual const char *get_data() override { return NULL; }
  virtual int64_t size() const override { return sizeof(LogAsyncIOCallback); }
  virtual int alloc_data_buf(const char *io_data_buffer,
                             const int64_t data_size) override;
  virtual int inner_process(const char *data_buffer,
                            const int64_t size) override;
  virtual const char *get_cb_name() const override
  {
    return "PalfAsyncIOCallback";
  }
  TO_STRING_KV(KP_(ctx), K_(fragment_ref), K_(begin_lsn), K_(end_lsn),
               K_(submit_ts));
private:
  void release_ctx_pin_();

  IAsyncPalfIOCtx *ctx_;
  FragmentRef fragment_ref_;
  LSN begin_lsn_;
  LSN end_lsn_;
  int64_t submit_ts_;

  DISALLOW_COPY_AND_ASSIGN(LogAsyncIOCallback);
};
}

#define LOG_IO_DEVICE_WRAPPER ::oceanbase::palf::LogIODeviceWrapper::get_instance()
}

#endif
