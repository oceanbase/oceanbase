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

#ifndef OB_IO_MANAGER_H_
#define OB_IO_MANAGER_H_

#include "lib/io/ob_io_common.h"
#include "lib/io/ob_io_resource.h"
#include "lib/io/ob_io_request.h"
#include "lib/io/ob_io_disk.h"
#include "lib/io/ob_io_benchmark.h"
#include "lib/thread/thread_mgr_interface.h"

#define OB_IO_MANAGER (oceanbase::common::ObIOManager::get_instance())

namespace oceanbase {
namespace common {

struct ObIOPointRetCode {
  ObIOPoint io_point_;
  ObIORetCode ret_code_;
  TO_STRING_KV(K_(io_point), K_(ret_code));
};

/*
 * handle for io request, used for wait the asynchronous request finished and get the result
 *
 * not thread safe
 *
 */
class ObIOHandle {
public:
  ObIOHandle();
  virtual ~ObIOHandle();
  ObIOHandle(const ObIOHandle& other);
  ObIOHandle& operator=(const ObIOHandle& other);
  int wait(const int64_t timeout_ms);
  void reset();
  void cancel();
  const char* get_buffer();
  int64_t get_data_size();
  int64_t get_rt() const;
  int get_io_errno(int& io_errno);

  int set_master(ObIOMaster& master);
  void estimate();
  int get_fail_points(ObIArray<ObIOPointRetCode>& fail_points);
  inline bool is_empty() const
  {
    return NULL == master_;
  }
  inline bool is_valid() const
  {
    return NULL != master_;
  }
  int64_t to_string(char* buf, const int64_t buf_len) const;

private:
  static const int64_t LONG_IO_PRINT_TRIGGER_US = 3000 * 1000;
  ObIOMaster* master_;
};

class ObIOCallbackRunner {
public:
  ObIOCallbackRunner();
  virtual ~ObIOCallbackRunner();
  int init(const int32_t queue_depth);
  void destroy();

  int enqueue_callback(ObIOMaster& master);
  int dequeue_callback(int64_t queue_idx, ObIOMaster*& master);
  void do_callback(const int64_t queue_idx);

private:
  static const int64_t CALLBACK_WAIT_PERIOD_US = 1000 * 1000;
  bool inited_;
  int64_t callback_thread_cnt_;
  ObFixedQueue<ObIOMaster>* callback_queue_;
  ObThreadCond* callback_cond_;
  ObArenaAllocator allocator_;
};

class ObIOManager : public lib::TGRunnable {
public:
  static const int64_t DEFAULT_IO_MANAGER_MEMORY_LIMIT = 10L * 1024L * 1024L * 1024L;
  static const int64_t MAX_MEMORY_PERCENT_PER_DISK = 50;
  static const int64_t MAX_CALLBACK_THREAD_CNT = 64;
  static const int64_t MINI_MODE_MAX_CALLBACK_THREAD_CNT = 4;
  static const int64_t DEFAULT_IO_MASTER_SIZE = sizeof(ObIOMaster) + 1024;
  static const int32_t DEFAULT_IO_QUEUE_DEPTH = 100000;

  static ObIOManager& get_instance();
  /**
   * require:
   *          1. disk_number <= MAX_DISK_NUM, otherwise we will return OB_INVALID_ARGUMENT.
   *          2. disk_number <= submit_thread_cnt, otherwise we will increase submit_thread_cnt automatically.
   */
  int init(const int64_t mem_limit = DEFAULT_IO_MANAGER_MEMORY_LIMIT,
      const int32_t disk_number_limit = ObDiskManager::MAX_DISK_NUM,
      const int32_t queue_depth = DEFAULT_IO_QUEUE_DEPTH);
  void destroy();
  void set_no_working()
  {
    is_working_ = false;
  }

  // synchronously read
  //@param info: input, include fd, offset, size
  //@param handle: output, it will hold the memory of IO until the handle is reset or destructed.
  //@param timeout_ms: input, this function will block at most timeout_ms
  //@return OB_SUCCESS or other error code
  int read(const ObIOInfo& info, ObIOHandle& handle, const uint64_t timeout_ms);
  // synchronously write
  //@param info: input, include fd, offset, size, NOTE that the offset and size MUST be aligned.
  //@param timeout_ms: input, this function will block at most timeout_ms
  //@return OB_SUCCESS or other error code
  int write(const ObIOInfo& info, const uint64_t timeout_ms);
  // asynchronously read
  //@param info: input, include fd, offset, size
  //@param handle: output, it will hold the memory of IO until the handle is reset or destructed.
  //@return OB_SUCCESS or other error code
  int aio_read(const ObIOInfo& info, ObIOHandle& handle);
  // asynchronously read
  //@param info: input, include fd, offset, size. NOTE that the offset and size MUST be aligned.
  //@param callback: input
  //@param handle: output, can invoke handle.wait() to wait io finish
  //@return OB_SUCCESS or other error code
  int aio_read(const ObIOInfo& info, ObIOCallback& callback, ObIOHandle& handle);
  // asynchronously write
  //@param info: input, include fd, offset, size, NOTE that the offset and size MUST be aligned.
  //@param handle: output, can invoke handle.wait() to wait io finish
  //@return OB_SUCCESS or other error code
  int aio_write(const ObIOInfo& info, ObIOHandle& handle);
  // asynchronously write
  //@param info: input, include fd, offset, size, NOTE that the offset and size MUST be aligned.
  //@param callback: input
  //@param handle: output, can invoke handle.wait() to wait io finish
  //@return OB_SUCCESS or other error code
  int aio_write(const ObIOInfo& info, ObIOCallback& callback, ObIOHandle& handle);

  // config related, thread safe
  int set_io_config(const ObIOConfig& conf);
  ObIOConfig get_io_config() const
  {
    return io_conf_;
  }

  // work thread related, thread safe
  void run1() override;

  // admin releated, thread safe
  int add_disk(const ObDiskFd& fd, const int64_t sys_io_percent = ObDisk::DEFAULT_SYS_IO_PERCENT,
      const int64_t channel_count = ObDisk::MAX_DISK_CHANNEL_CNT, const int32_t queue_depth = DEFAULT_IO_QUEUE_DEPTH)
  {
    return disk_mgr_.add_disk(fd, sys_io_percent, channel_count, queue_depth);
  }
  int delete_disk(const ObDiskFd& fd)
  {
    return disk_mgr_.delete_disk(fd);
  }

  // disk error management
  int is_disk_error(bool& disk_error);
  int is_disk_error_definite(bool& disk_error);
  int reset_disk_error();

  ObDiskManager& get_disk_manager()
  {
    return disk_mgr_;
  }
  ObIOResourceManager& get_resource_manager()
  {
    return resource_mgr_;
  }
  ObIOCallbackRunner& get_callback_runner()
  {
    return callback_mgr_;
  }

private:
  ObIOManager();
  virtual ~ObIOManager();
  int inner_aio(
      const ObIOMode mode, const ObIOInfo& info, ObIOCallback* callback, ObIOMaster* master, ObIOHandle& handle);
  int check_disk_error();

private:
  bool inited_;
  bool is_working_;
  bool is_disk_error_;
  bool is_disk_error_definite_;
  ObIOResourceManager resource_mgr_;
  ObDiskManager disk_mgr_;
  ObIOCallbackRunner callback_mgr_;
  ObIOConfig io_conf_;
  lib::ObMutex conf_mutex_;
  ObConcurrentFIFOAllocator master_allocator_;
};

} /* namespace common */
} /* namespace oceanbase */

#endif /* OB_IO_MANAGER_H_ */
