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

#ifndef OB_IO_COMMON_H
#define OB_IO_COMMON_H

#include <libaio.h>
#include <sys/resource.h>
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "lib/allocator/ob_fixed_size_block_allocator.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/task/ob_timer.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/lock/ob_thread_cond.h"
#include "lib/wait_event/ob_wait_event.h"
#include "lib/coro/co_user_thread.h"
#include "lib/thread/ob_simple_thread_pool.h"
#include "lib/queue/ob_fixed_queue.h"
#include "lib/list/ob_dlist.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_array_wrap.h"
#include "lib/worker.h"

namespace oceanbase {
namespace common {

class ObIOAllocator;
class ObIIOErrorHandler;

const int64_t DEFAULT_IO_WAIT_TIME_MS = 5000;  // default wait 5 seconds
const int32_t MAX_IO_BATCH_NUM = 32;

extern int ob_io_setup(int maxevents, io_context_t* ctxp);
extern int ob_io_destroy(io_context_t ctx);
extern int ob_io_submit(io_context_t ctx, long nr, struct iocb** iocbpp);
extern int ob_io_cancel(io_context_t ctx, struct iocb* iocb, struct io_event* evt);
extern int ob_io_getevents(
    io_context_t ctx_id, long min_nr, long nr, struct io_event* events, struct timespec* timeout);

void align_offset_size(const int64_t offset, const int64_t size, int64_t& align_offset, int64_t& align_size);

enum ObIOMode {
  IO_MODE_READ = 0,
  IO_MODE_WRITE = 1,
  IO_MODE_MAX,
};

enum ObIOCategory { USER_IO = 0, SYS_IO = 1, PREWARM_IO = 2, LARGE_QUERY_IO = 3, MAX_IO_CATEGORY };

class ObIORequest;
class ObDisk;

struct ObDiskID final {
  static const int64_t OB_DISK_ID_VERSION = 1;

  explicit ObDiskID();
  bool is_valid() const;
  void reset();
  bool operator==(const ObDiskID& other) const;
  TO_STRING_KV(K_(disk_idx), K_(install_seq));

  int64_t disk_idx_;
  int64_t install_seq_;
  OB_UNIS_VERSION(OB_DISK_ID_VERSION);
};

enum class ObFileSystemType { LOCAL = 0, RAID = 1, OFS = 2, MAX };

struct ObDiskFd final {
  ObDiskFd();
  int32_t fd_;
  ObDiskID disk_id_;

  bool is_valid() const;
  bool operator==(const ObDiskFd& other) const;
  void reset();
  TO_STRING_KV(K_(fd), K_(disk_id));
};

struct ObIOConfig {
public:
  static const int64_t DEFAULT_SYS_IO_LOW_PERCENT = 10;
  static const int64_t DEFAULT_SYS_IO_HIGH_PERCENT = 90;
  static const int64_t DEFAULT_USER_IO_UP_PERCENT = 50;
  static const int64_t DEFAULT_CPU_HIGH_WATER_LEVEL = 4800;
  static const int64_t DEFAULT_WRITE_FAILURE_DETECT_INTERVAL = 60 * 1000 * 1000;         // 1 min
  static const int64_t DEFAULT_READ_FAILURE_IN_BLACK_LIST_INTERVAL = 300 * 1000 * 1000;  // 5 min
  static const int32_t DEFAULT_RETRY_WARN_LIMIT = 2;
  static const int32_t DEFAULT_RETRY_ERROR_LIMIT = 5;
  static const int64_t DEFAULT_DISK_IO_THREAD_COUNT = 8;
  static const int64_t DEFAULT_IO_CALLBACK_THREAD_COUNT = 8;
  static const int64_t DEFAULT_LARGE_QUERY_IO_PERCENT = 0;                 // 0 means unlimited
  static const int64_t DEFAULT_DATA_STORAGE_IO_TIMEOUT_MS = 120L * 1000L;  // 120s
public:
  ObIOConfig()
  {
    set_default_value();
  }
  void set_default_value();
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(sys_io_low_percent), K_(sys_io_high_percent), K_(user_iort_up_percent), K_(cpu_high_water_level),
      K_(write_failure_detect_interval), K_(read_failure_black_list_interval), K_(retry_warn_limit),
      K_(retry_error_limit), K_(disk_io_thread_count), K_(callback_thread_count), K_(large_query_io_percent),
      K_(data_storage_io_timeout_ms));

public:
  int64_t sys_io_low_percent_;
  int64_t sys_io_high_percent_;
  int64_t user_iort_up_percent_;
  int64_t cpu_high_water_level_;
  int64_t write_failure_detect_interval_;
  int64_t read_failure_black_list_interval_;
  int64_t retry_warn_limit_;
  int64_t retry_error_limit_;
  int64_t disk_io_thread_count_;
  int64_t callback_thread_count_;
  int64_t large_query_io_percent_;
  int64_t data_storage_io_timeout_ms_;
};

struct ObIODesc {
public:
  ObIODesc() : category_(USER_IO), mode_(IO_MODE_READ), wait_event_no_(0), req_deadline_time_(0)
  {}
  bool is_valid() const
  {
    return category_ >= USER_IO && category_ < MAX_IO_CATEGORY && mode_ >= IO_MODE_READ && mode_ < IO_MODE_MAX &&
           wait_event_no_ >= 0 && req_deadline_time_ >= 0;
  }
  bool is_read_mode() const
  {
    return mode_ == IO_MODE_READ;
  }
  bool is_write_mode() const
  {
    return mode_ == IO_MODE_WRITE;
  }
  TO_STRING_KV(K_(category), K_(mode), K_(wait_event_no), K_(req_deadline_time));

public:
  ObIOCategory category_;
  ObIOMode mode_;
  int64_t wait_event_no_;
  int64_t req_deadline_time_;
};

struct ObIOPoint {
public:
  ObIOPoint() : fd_(), size_(0), offset_(0), write_buf_(NULL)
  {}
  ~ObIOPoint()
  {}
  void reset()
  {
    *this = ObIOPoint();
  }
  inline bool is_valid() const
  {
    return fd_.is_valid() && offset_ >= 0 && size_ > 0;
  }
  inline bool is_aligned(const int64_t align_size) const
  {
    return align_size > 0 && 0 == offset_ % align_size && 0 == size_ % align_size;
  }
  TO_STRING_KV(K_(fd), K_(offset), K_(size), KP_(write_buf));

public:
  ObDiskFd fd_;
  int32_t size_;
  int64_t offset_;
  const char* write_buf_;  // write buf
};

// pure virtual interface of io callback, any not sub class of ObIOCallback should add it to get_io_master_buf_size
class ObIOCallback {
public:
  static const int64_t CALLBACK_BUF_SIZE = 1024;
  ObIOCallback() : compat_mode_(static_cast<lib::Worker::CompatMode>(lib::get_compat_mode()))
  {}
  virtual ~ObIOCallback()
  {}
  virtual int64_t size() const = 0;
  virtual int alloc_io_buf(char*& io_buf, int64_t& io_buf_size, int64_t& aligned_offset) = 0;
  virtual int inner_process(const bool is_success) = 0;
  virtual int process(const bool is_success)
  {
    lib::set_compat_mode(compat_mode_);
    return inner_process(is_success);
  }
  virtual int inner_deep_copy(char* buf, const int64_t buf_len, ObIOCallback*& callback) const = 0;
  virtual int deep_copy(char* buf, const int64_t buf_len, ObIOCallback*& callback) const
  {
    int ret = OB_SUCCESS;
    if (OB_SUCC(inner_deep_copy(buf, buf_len, callback))) {
      callback->compat_mode_ = compat_mode_;
    }
    return ret;
  }
  virtual const char* get_data() = 0;
  DECLARE_PURE_VIRTUAL_TO_STRING;
  lib::Worker::CompatMode compat_mode_;
};

class ObIOWriteFinishCallback {
public:
  virtual ~ObIOWriteFinishCallback()
  {}
  virtual int notice_finish(const int64_t finish_id) = 0;
};

struct ObIORetCode {
public:
  ObIORetCode() : io_ret_(0), sys_errno_(0)
  {}
  virtual ~ObIORetCode()
  {}
  TO_STRING_KV(K_(io_ret), K_(sys_errno));

public:
  int io_ret_;     // ob ret code, for example: OB_SUCCESS, OB_TIME_OUT, OB_IO_ERROR
  int sys_errno_;  // system error number, get error message via strerror().
};

struct ObIOStat {
public:
  ObIOStat() : io_cnt_(0), io_bytes_(0), io_rt_us_(0)
  {}
  virtual ~ObIOStat()
  {}
  void update_io_stat(const uint64_t io_bytes, const uint64_t io_rt_us);
  void reset()
  {
    *this = ObIOStat();
  };
  TO_STRING_KV(K_(io_cnt), K_(io_bytes), K_(io_rt_us));

public:
  uint64_t io_cnt_;
  uint64_t io_bytes_;
  uint64_t io_rt_us_;
};

class ObIOStatDiff {
public:
  ObIOStatDiff();
  virtual ~ObIOStatDiff()
  {}
  void set_io_stat(const ObIOStat& io_stat)
  {
    io_stat_ = &io_stat;
  }
  void estimate();
  inline int64_t get_average_size() const
  {
    return average_size_;
  }
  inline double get_average_rt() const
  {
    return average_rt_us_;
  }
  TO_STRING_KV(K_(average_size), K_(average_rt_us), K_(old_stat), K_(new_stat));

private:
  int64_t average_size_;
  double average_rt_us_;
  ObIOStat old_stat_;
  ObIOStat new_stat_;
  const ObIOStat* io_stat_;
};

class ObCpuStatDiff {
public:
  ObCpuStatDiff();
  virtual ~ObCpuStatDiff()
  {}
  void estimate();
  inline int64_t get_average_usage() const
  {
    return avg_usage_;
  }
  TO_STRING_KV(K_(avg_usage));

private:
  struct rusage old_usage_;
  struct rusage new_usage_;
  int64_t old_time_;
  int64_t new_time_;
  int64_t avg_usage_;
};

class ObIOQueue {
public:
  ObIOQueue();
  virtual ~ObIOQueue();
  int init(const int32_t queue_depth);
  void destroy();
  int push(ObIORequest& req);  // not thread safe, need caller ensure this
  int pop(ObIORequest*& req);  // not thread safe, need caller ensure this
  bool empty() const
  {
    return 0 == req_cnt_;
  }
  int64_t get_deadline();
  int direct_pop(ObIORequest*& req);
  int32_t get_req_count() const
  {
    return req_cnt_;
  }

private:
  struct ObIORequestCmp {
    bool operator()(const ObIORequest* a, const ObIORequest* b) const;
  };
  ObIORequest** req_array_;
  int32_t queue_depth_;
  int32_t req_cnt_;
  ObIORequestCmp cmp_;
  ObArenaAllocator allocator_;
  bool inited_;
};

class ObIOChannel {
public:
  ObIOChannel();
  virtual ~ObIOChannel();
  int init(const int32_t queue_depth);
  void destroy();
  int enqueue_request(ObIORequest& req);
  int dequeue_request(ObIORequest*& req);
  int clear_all_requests();
  void submit();
  void get_events();
  void cancel(ObIORequest& req);
  void stop_submit()
  {
    can_submit_request_ = false;
  }
  TO_STRING_KV(K_(inited), K_(submit_cnt), K_(can_submit_request));

private:
  int inner_submit(ObIORequest& req, int& sys_ret);
  void finish_flying_req(ObIORequest& req, int io_ret, int system_errno);
  int64_t get_pop_wait_timeout(const int64_t queue_deadline);

private:
  static const int32_t MAX_AIO_EVENT_CNT = 512;
  static const int64_t DISK_WAIT_PERIOD_US = 1000;
  static const int64_t AIO_TIMEOUT_NS = 1000L * 10000L;  // 10ms
  static const int64_t DEFAULT_SUBMIT_WAIT_US = 10 * 1000;
  bool inited_;
  io_context_t context_;
  int64_t submit_cnt_;
  ObIOQueue queue_;
  ObThreadCond queue_cond_;
  bool can_submit_request_;
};

struct ObIOInfo final {
public:
  ObIOInfo();
  ~ObIOInfo()
  {}
  void operator=(const ObIOInfo& r);
  bool is_valid() const;
  int add_fail_disk(const int64_t disk_id);
  void reset();
  int notice_finish() const;
  TO_STRING_KV(K_(size), K_(io_desc), K_(batch_count), "io_points",
      common::ObArrayWrap<ObIOPoint>(io_points_, batch_count_), K_(fail_disk_count), "fail_disk_ids",
      common::ObArrayWrap<int64_t>(fail_disk_ids_, fail_disk_count_));

public:
  int32_t size_;
  ObIODesc io_desc_;
  int32_t batch_count_;  // number of io_points_, 0 if not using batch io
  ObIOPoint io_points_[MAX_IO_BATCH_NUM];
  int64_t fail_disk_ids_[MAX_IO_BATCH_NUM];
  int64_t fail_disk_count_;
  int64_t offset_;  // offset in macro block
  ObIIOErrorHandler* io_error_handler_;
  ObIOWriteFinishCallback* finish_callback_;
  int64_t finish_id_;  // used for raid, same as block id
};

template <typename T>
class ObPointerHolder {
public:
  explicit ObPointerHolder() : ptr_(NULL)
  {}
  explicit ObPointerHolder(T* ptr) : ptr_(NULL)
  {
    hold(ptr);
  }
  virtual ~ObPointerHolder()
  {
    reset();
  }
  T* get_ptr()
  {
    return ptr_;
  }
  void hold(T* ptr)
  {
    if (NULL != ptr && ptr != ptr_) {
      ptr->inc_ref();
      reset();  // reset previous ptr, must after ptr->inc_ref()
      ptr_ = ptr;
    }
  }
  void reset()
  {
    if (NULL != ptr_) {
      ptr_->dec_ref();
      ptr_ = NULL;
    }
  }
  TO_STRING_KV(KP_(ptr));

private:
  T* ptr_;
};

// default io callback, only used to alloc memory, process do nothing
class ObDefaultIOCallback : public ObIOCallback {
public:
  ObDefaultIOCallback();
  virtual ~ObDefaultIOCallback();
  int init(common::ObIOAllocator* allocator, const int64_t offset, const int64_t buf_size);
  virtual int64_t size() const;
  virtual int alloc_io_buf(char*& io_buf, int64_t& io_buf_size, int64_t& aligned_offset);
  virtual int inner_process(const bool is_success);
  virtual int inner_deep_copy(char* buf, const int64_t buf_len, ObIOCallback*& callback) const;
  virtual const char* get_data();
  TO_STRING_KV(KP_(data_buf));

private:
  bool is_inited_;
  common::ObIOAllocator* allocator_;
  int64_t offset_;
  int64_t buf_size_;
  char* io_buf_;
  int64_t io_buf_size_;
  char* data_buf_;
  DISALLOW_COPY_AND_ASSIGN(ObDefaultIOCallback);
};

} /* namespace common */
} /* namespace oceanbase */

#endif
