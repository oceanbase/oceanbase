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

#ifndef OCEANBASE_LIB_STORAGE_IO_DEFINE
#define OCEANBASE_LIB_STORAGE_IO_DEFINE

#include "lib/worker.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/lock/ob_thread_cond.h"
#include "lib/container/ob_rbtree.h"
#include "common/storage/ob_io_device.h"
#include "lib/list/ob_list.h"
#include "lib/container/ob_heap.h"

namespace oceanbase
{
namespace common
{

static constexpr int64_t DEFAULT_IO_WAIT_TIME_MS = 5000L; // 5s
static constexpr int64_t MAX_IO_WAIT_TIME_MS = 300L * 1000L; // 5min

enum class ObIOMode : uint8_t
{
  READ = 0,
  WRITE = 1,
  MAX_MODE
};

const char *get_io_mode_string(const ObIOMode mode);
ObIOMode get_io_mode_enum(const char *mode_string);

enum class ObIOCategory : uint8_t
{
  LOG_IO = 0,
  USER_IO = 1,
  SYS_IO = 2,
  PREWARM_IO = 3,
  LARGE_QUERY_IO = 4,
  MAX_CATEGORY
};

const char *get_io_category_name(ObIOCategory category);
ObIOCategory get_io_category_enum(const char *category_name);

struct ObIOFlag final
{
public:
  ObIOFlag();
  ~ObIOFlag();
  void reset();
  bool is_valid() const;
  void set_mode(ObIOMode mode);
  ObIOMode get_mode() const;
  void set_category(ObIOCategory category);
  ObIOCategory get_category() const;
  void set_wait_event(int64_t wait_event_id);
  int64_t get_wait_event() const;
  void set_read();
  bool is_read() const;
  void set_write();
  bool is_write() const;
  void set_sync();
  void set_async();
  bool is_sync() const;
  void set_unlimited(const bool is_unlimited = true);
  bool is_unlimited() const;
  TO_STRING_KV("mode", common::get_io_mode_string(static_cast<ObIOMode>(mode_)),
               "category", common::get_io_category_name(static_cast<ObIOCategory>(category_)),
               K(wait_event_id_), K(is_sync_), K(is_unlimited_), K(reserved_));
private:
  static constexpr int64_t IO_MODE_BIT = 4; // read, write, append
  static constexpr int64_t IO_CATEGORY_BIT = 8; // clog, user, prewarm, large query, etc
  static constexpr int64_t IO_WAIT_EVENT_BIT = 32; // for performance monitor
  static constexpr int64_t IO_SYNC_FLAG_BIT = 1; // indicate if the caller is waiting io finished
  static constexpr int64_t IO_UNLIMITED_FLAG_BIT = 1; // indicate if the io is unlimited
  static constexpr int64_t IO_RESERVED_BIT = 64 - IO_MODE_BIT
                                                - IO_CATEGORY_BIT
                                                - IO_WAIT_EVENT_BIT
                                                - IO_SYNC_FLAG_BIT
                                                - IO_UNLIMITED_FLAG_BIT;

  union {
    int64_t flag_;
    struct {
      int64_t mode_ : IO_MODE_BIT;
      int64_t category_ : IO_CATEGORY_BIT;
      int64_t wait_event_id_ : IO_WAIT_EVENT_BIT;
      bool is_sync_ : IO_SYNC_FLAG_BIT;
      bool is_unlimited_ : IO_UNLIMITED_FLAG_BIT;
      int64_t reserved_ : IO_RESERVED_BIT;
    };
  };
};

class ObIOCallback
{
public:
  static const int64_t CALLBACK_BUF_SIZE = 1024;
  ObIOCallback();
  virtual ~ObIOCallback();

  int process(const bool is_success);
  int deep_copy(char *buf, const int64_t buf_len, ObIOCallback *&copied_callback) const;
  virtual const char *get_data() = 0;
  virtual int64_t size() const = 0;
  virtual int alloc_io_buf(char *&io_buf, int64_t &io_buf_size, int64_t &aligned_offset) = 0;
  virtual int inner_process(const bool is_success) = 0;
  virtual int inner_deep_copy(char *buf, const int64_t buf_len, ObIOCallback *&copied_callback) const = 0;
  DECLARE_PURE_VIRTUAL_TO_STRING;

private:
  lib::Worker::CompatMode compat_mode_;
};

struct ObIOInfo final
{
public:
  ObIOInfo();
  ~ObIOInfo();
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K(tenant_id_), K(fd_), K(offset_), K(size_), K(flag_), KP(buf_), KP(callback_));

public:
  uint64_t tenant_id_;
  ObIOFd fd_;
  int64_t offset_;
  int64_t size_;
  ObIOFlag flag_;
  const char *buf_;
  ObIOCallback *callback_;
};

template <typename T>
class ObRefHolder
{
public:
  explicit ObRefHolder(): ptr_(nullptr) {}
  explicit ObRefHolder(T *ptr): ptr_(nullptr) { hold(ptr); }
  virtual ~ObRefHolder() { reset(); }
  T *get_ptr() { return ptr_; }
  void hold(T *ptr) {
    if (nullptr != ptr && ptr != ptr_) {
      ptr->inc_ref();
      reset(); // reset previous ptr, must after ptr->inc_ref()
      ptr_ = ptr;
    }
  }
  void reset() {
    if (nullptr != ptr_) {
      ptr_->dec_ref();
      ptr_ = nullptr;
    }
  }
  TO_STRING_KV(KP_(ptr));
private:
  T *ptr_;
};

struct ObIOTimeLog final
{
public:
  ObIOTimeLog();
  ~ObIOTimeLog();
  void reset();
  TO_STRING_KV(K(begin_ts_), K(enqueue_ts_), K(dequeue_ts_), K(submit_ts_), K(return_ts_),
      K(callback_enqueue_ts_), K(callback_dequeue_ts_), K(callback_finish_ts_), K(end_ts_));
public:
  int64_t begin_ts_;
  int64_t enqueue_ts_;
  int64_t dequeue_ts_;
  int64_t submit_ts_;
  int64_t return_ts_;
  int64_t callback_enqueue_ts_;
  int64_t callback_dequeue_ts_;
  int64_t callback_finish_ts_;
  int64_t end_ts_;
};

int64_t get_io_interval(const int64_t &end_time, const int64_t &begin_time);

struct ObIORetCode final
{
public:
  ObIORetCode();
  ObIORetCode(int io_ret, int fs_errno = 0);
  ~ObIORetCode();
  void reset();
  TO_STRING_KV(K(io_ret_), K(fs_errno_));
public:
  int io_ret_;
  int fs_errno_;
};

class ObTenantIOManager;
class ObIOChannel;
class ObIOSender;
class ObIOUsage;

class ObIORequest : public common::ObDLinkBase<ObIORequest>
{
public:
  ObIORequest();
  virtual ~ObIORequest();
  int init(const ObIOInfo &info);
  virtual void destroy();
  int64_t get_data_size() const;
  const char *get_data();
  const ObIOFlag &get_flag() const;
  ObIOCategory get_category() const;
  ObIOMode get_mode() const;
  void cancel();
  int alloc_io_buf();
  int prepare();
  bool can_callback() const;
  void finish(const ObIORetCode &ret_code);
  void inc_ref(const char *msg = nullptr);
  void dec_ref(const char *msg = nullptr);
  void inc_out_ref();
  void dec_out_ref();
  VIRTUAL_TO_STRING_KV(K(is_inited_), K(is_finished_), K(is_canceled_), K(has_estimated_), K(io_info_), K(deadline_ts_),
      KP(control_block_), KP(raw_buf_), KP(io_buf_), K(io_offset_), K(io_size_), K(complete_size_),
      K(time_log_), KP(channel_), K(ref_cnt_), K(out_ref_cnt_),
      K(trace_id_), K(ret_code_), K(retry_count_), K(callback_buf_size_), KP(copied_callback_), K(tenant_io_mgr_));
private:
  int alloc_aligned_io_buf();
public:
  bool is_inited_;
  bool is_finished_;
  bool is_canceled_;
  bool has_estimated_;
  ObIOInfo io_info_;
  int64_t deadline_ts_;
  ObIOCB *control_block_;
  void *raw_buf_;
  char *io_buf_;
  int64_t io_offset_;
  int64_t io_size_;
  int64_t complete_size_;
  ObIOTimeLog time_log_;
  ObIOChannel *channel_;
  ObIOSender *sender_;
  volatile int64_t ref_cnt_;
  volatile int64_t out_ref_cnt_; // only for ObIOHandle, when handle reset, cancel IORequest.
  ObThreadCond cond_;
  ObCurTraceId::TraceId trace_id_;
  ObIORetCode ret_code_;
  int32_t retry_count_;
  ObRefHolder<ObTenantIOManager> tenant_io_mgr_;
  ObIOCallback *copied_callback_;
  int64_t callback_buf_size_;
  char callback_buf_[];
  };

class ObPhyQueue final
{
public:
  ObPhyQueue();
  ~ObPhyQueue();
  int init(const int index);
  void destroy();
  void reset_time_info();
  void reset_queue_info();
public:
  typedef common::ObDList<ObIORequest> IOReqList;
  TO_STRING_KV(K_(reservation_ts), K_(category_limitation_ts), K_(tenant_limitation_ts));
  bool is_inited_;
  int64_t reservation_ts_;
  int64_t category_limitation_ts_;
  int64_t tenant_limitation_ts_;
  int64_t proportion_ts_;
  bool is_category_ready_;
  bool is_tenant_ready_;
  int category_index_;
  int64_t reservation_pos_;
  int64_t category_limitation_pos_;
  int64_t tenant_limitation_pos_;
  int64_t proportion_pos_;
  IOReqList req_list_;  
};

class ObIOHandle final
{
public:
  ObIOHandle();
  ~ObIOHandle();
  ObIOHandle(const ObIOHandle &other);
  ObIOHandle &operator=(const ObIOHandle &other);
  int set_request(ObIORequest &req);
  bool is_empty() const;
  bool is_valid() const;

  int wait(const int64_t timeout_ms);
  const char *get_buffer();
  const char *get_physical_buffer();
  int64_t get_data_size() const;
  int64_t get_rt() const;
  int get_fs_errno(int &io_errno) const;
  void reset();
  void cancel();
  TO_STRING_KV("io_request", to_cstring(req_));
private:
  void estimate();

private:
  ObIORequest *req_;
};


struct ObTenantIOConfig final
{
public:
  struct UnitConfig
  {
    UnitConfig();
    bool is_valid() const;
    TO_STRING_KV(K_(min_iops), K_(max_iops), K_(weight));
    int64_t min_iops_;
    int64_t max_iops_;
    int64_t weight_;
  };
  struct CategoryConfig
  {
    CategoryConfig();
    bool is_valid() const;
    TO_STRING_KV(K_(min_percent), K_(max_percent), K_(weight_percent));
    int64_t min_percent_;
    int64_t max_percent_;
    int64_t weight_percent_;
  };
public:
  ObTenantIOConfig();
  static const ObTenantIOConfig &default_instance();
  bool is_valid() const;
  bool operator ==(const ObTenantIOConfig &other) const;
  int parse_category_config(const char *config_str);
  int get_category_config(const ObIOCategory category, int64_t &min_iops, int64_t &max_iops, int64_t &iops_weight) const;
  int64_t to_string(char* buf, const int64_t buf_len) const;
public:
  int64_t memory_limit_;
  int64_t callback_thread_count_;
  UnitConfig unit_config_;
  CategoryConfig category_configs_[static_cast<int>(ObIOCategory::MAX_CATEGORY)];
  CategoryConfig other_config_;
  bool enable_io_tracer_;
};


struct ObAtomIOClock final
{
  ObAtomIOClock() : iops_(0), last_ns_(0) {}
  void atom_update(const int64_t current_ts, const double iops_scale, int64_t &deadline_ts);
  void reset();
  TO_STRING_KV(K_(iops), K_(last_ns));
  int64_t iops_;
  int64_t last_ns_; // the unit is nano sescond for max iops of 1 billion
};

class ObIOClock
{
public:
  ObIOClock() {}
  virtual ~ObIOClock() {}
  virtual int init(const ObTenantIOConfig &io_config, const ObIOUsage *io_usage) = 0;
  virtual void destroy() = 0;
  virtual int update_io_config(const ObTenantIOConfig &io_config) = 0;
  virtual int sync_clocks(ObIArray<ObIOClock *> &io_clocks) = 0;
  DECLARE_PURE_VIRTUAL_TO_STRING;
};

class ObIOQueue
{
public:
  ObIOQueue() {}
  virtual ~ObIOQueue() {}
  virtual int init() = 0;
  virtual void destroy() = 0;
  DECLARE_PURE_VIRTUAL_TO_STRING;
};

class ObMClockQueue : public ObIOQueue
{
public:
  ObMClockQueue();
  virtual ~ObMClockQueue();
  virtual int init() override;
  virtual void destroy() override;
  int push_phyqueue(ObPhyQueue *phy_queue);
  int pop_phyqueue(ObIORequest *&req, int64_t &deadline_ts);
  TO_STRING_KV(K(is_inited_));

  int remove_from_heap(ObPhyQueue *phy_queue);
private:
  int pop_with_ready_queue(const int64_t current_ts, ObIORequest *&req, int64_t &deadline_ts);

  template<typename T, int64_t T::*member>
  struct HeapCompare {
    int get_error_code() { return OB_SUCCESS; }
    bool operator() (const T *left, const T *right) const {
      return left->*member != right->*member ? left->*member > right->*member: (int64_t)left > (int64_t)right;
    }
  };
private:
  bool is_inited_;
  HeapCompare<ObPhyQueue, &ObPhyQueue::reservation_ts_> r_cmp_;
  HeapCompare<ObPhyQueue, &ObPhyQueue::category_limitation_ts_> cl_cmp_;
  HeapCompare<ObPhyQueue, &ObPhyQueue::tenant_limitation_ts_> tl_cmp_;
  HeapCompare<ObPhyQueue, &ObPhyQueue::proportion_ts_> p_cmp_;
  ObRemovableHeap<ObPhyQueue *, HeapCompare<ObPhyQueue, &ObPhyQueue::reservation_ts_>, &ObPhyQueue::reservation_pos_> r_heap_;
  ObRemovableHeap<ObPhyQueue *, HeapCompare<ObPhyQueue, &ObPhyQueue::category_limitation_ts_>, &ObPhyQueue::category_limitation_pos_> cl_heap_;
  ObRemovableHeap<ObPhyQueue *, HeapCompare<ObPhyQueue, &ObPhyQueue::tenant_limitation_ts_>, &ObPhyQueue::tenant_limitation_pos_> tl_heap_;
  ObRemovableHeap<ObPhyQueue *, HeapCompare<ObPhyQueue, &ObPhyQueue::proportion_ts_>, &ObPhyQueue::proportion_pos_> ready_heap_;
};


} // namespace common
} // namespace oceanbase

#endif//OCEANBASE_LIB_STORAGE_IO_DEFINE
