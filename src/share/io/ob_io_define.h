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
#include "lib/list/ob_list.h"
#include "lib/container/ob_heap.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/container/ob_array_wrap.h"
#include "common/storage/ob_io_device.h"
#include "share/resource_manager/ob_resource_plan_info.h"

namespace oceanbase
{
namespace common
{
//the timestamp adjustment will not adjust until the queue is idle for more than this time
static constexpr int64_t CLOCK_IDLE_THRESHOLD_US = 3L * 1000L * 1000L; // 3s
static constexpr int64_t DEFAULT_IO_WAIT_TIME_MS = 5000L; // 5s
static constexpr int64_t MAX_IO_WAIT_TIME_MS = 300L * 1000L; // 5min
static constexpr int64_t GROUP_START_NUM = 8L;
static constexpr int64_t DEFAULT_IO_WAIT_TIME_US = 5000L * 1000L; // 5s
static constexpr int64_t MAX_DETECT_READ_WARN_TIMES = 10L;
static constexpr int64_t MAX_DETECT_READ_ERROR_TIMES = 100L;
enum class ObIOMode : uint8_t
{
  READ = 0,
  WRITE = 1,
  MAX_MODE
};

const char *get_io_mode_string(const ObIOMode mode);
ObIOMode get_io_mode_enum(const char *mode_string);

enum ObIOModule {
  SYS_RESOURCE_GROUP_START_ID = 0,
  SLOG_IO = SYS_RESOURCE_GROUP_START_ID,
  CALIBRATION_IO,
  DETECT_IO,
  DIRECT_LOAD_IO,
  SHARED_BLOCK_RW_IO,
  SSTABLE_WHOLE_SCANNER_IO,
  INSPECT_BAD_BLOCK_IO,
  SSTABLE_INDEX_BUILDER_IO,
  BACKUP_READER_IO,
  BLOOM_FILTER_IO,
  SHARED_MACRO_BLOCK_MGR_IO,
  INDEX_BLOCK_TREE_CURSOR_IO,
  MICRO_BLOCK_CACHE_IO,
  ROOT_BLOCK_IO,
  TMP_PAGE_CACHE_IO,
  INDEX_BLOCK_MICRO_ITER_IO,
  HA_COPY_MACRO_BLOCK_IO,
  LINKED_MACRO_BLOCK_IO,
  HA_MACRO_BLOCK_WRITER_IO,
  TMP_TENANT_MEM_BLOCK_IO,
  SSTABLE_MACRO_BLOCK_WRITE_IO,
  SYS_RESOURCE_GROUP_END_ID
};

const int64_t USER_RESOURCE_GROUP_START_ID = 10000;
const int64_t SYS_RESOURCE_GROUP_CNT = SYS_RESOURCE_GROUP_END_ID - SYS_RESOURCE_GROUP_START_ID;
const uint64_t USER_RESOURCE_OTHER_GROUP_ID = 0;
const uint64_t OB_INVALID_GROUP_ID = UINT64_MAX;
static constexpr char BACKGROUND_CGROUP[] = "background";

OB_INLINE bool is_valid_group(const uint64_t group_id)
{
  return group_id >= USER_RESOURCE_OTHER_GROUP_ID && group_id != OB_INVALID_GROUP_ID;
}

OB_INLINE bool is_user_group(const uint64_t group_id)
{
  return group_id >= USER_RESOURCE_GROUP_START_ID && group_id != OB_INVALID_GROUP_ID;
}

OB_INLINE bool is_valid_resource_group(const uint64_t group_id)
{
  //other group or user group
  return group_id == USER_RESOURCE_OTHER_GROUP_ID || is_user_group(group_id);
}

const char *get_io_sys_group_name(ObIOModule module);
struct ObIOFlag final
{
public:
  ObIOFlag();
  ~ObIOFlag();
  ObIOFlag &operator=(const ObIOFlag &other)
  {
    this->flag_ = other.flag_;
    this->group_id_ = other.group_id_;
    this->sys_module_id_ = other.sys_module_id_;
    return *this;
  }
  void reset();
  bool is_valid() const;
  void set_mode(ObIOMode mode);
  ObIOMode get_mode() const;
  void set_resource_group_id(const uint64_t group_id);
  void set_wait_event(int64_t wait_event_id);
  uint64_t get_resource_group_id() const;
  void set_sys_module_id(const uint64_t sys_module_id);
  uint64_t get_sys_module_id() const;
  bool is_sys_module() const;
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
  void set_detect(const bool is_detect = true);
  void set_time_detect(const bool is_time_detect = true);
  bool is_detect() const;
  bool is_time_detect() const;
  TO_STRING_KV("mode", common::get_io_mode_string(static_cast<ObIOMode>(mode_)),
               K(group_id_), K(wait_event_id_), K(is_sync_), K(is_unlimited_), K(reserved_), K(is_detect_), K(is_time_detect_));
private:
  static constexpr int64_t IO_MODE_BIT = 4; // read, write, append
  static constexpr int64_t IO_WAIT_EVENT_BIT = 32; // for performance monitor
  static constexpr int64_t IO_SYNC_FLAG_BIT = 1; // indicate if the caller is waiting io finished
  static constexpr int64_t IO_DETECT_FLAG_BIT = 1; // notify a retry task
  static constexpr int64_t IO_UNLIMITED_FLAG_BIT = 1; // indicate if the io is unlimited
  static constexpr int64_t IO_TIME_DETECT_FLAG_BIT = 1; // indicate if the io is unlimited
  static constexpr int64_t IO_RESERVED_BIT = 64 - IO_MODE_BIT
                                                - IO_WAIT_EVENT_BIT
                                                - IO_SYNC_FLAG_BIT
                                                - IO_UNLIMITED_FLAG_BIT
                                                - IO_TIME_DETECT_FLAG_BIT
                                                - IO_DETECT_FLAG_BIT;

  union {
    int64_t flag_;
    struct {
      int64_t mode_ : IO_MODE_BIT;
      int64_t wait_event_id_ : IO_WAIT_EVENT_BIT;
      bool is_sync_ : IO_SYNC_FLAG_BIT;
      bool is_unlimited_ : IO_UNLIMITED_FLAG_BIT;
      bool is_detect_ : IO_DETECT_FLAG_BIT;
      bool is_time_detect_ : IO_TIME_DETECT_FLAG_BIT;
      int64_t reserved_ : IO_RESERVED_BIT;
    };
  };
  uint64_t group_id_;
  uint64_t sys_module_id_;
};

template <typename T>
class ObIOObjectPool final
{
public:
  ObIOObjectPool();
  ~ObIOObjectPool();
  int init(const int64_t count, const int64_t size, ObIAllocator &allocator);
  void destroy();
  int alloc(T *&ptr);
  int recycle(T *ptr);
  bool contain(T *ptr);
  int64_t get_block_size() const { return obj_size_; }
  int64_t get_capacity() const { return capacity_; }
  int64_t get_free_cnt() const { return free_count_; }
  TO_STRING_KV(K(is_inited_), K(obj_size_), K(capacity_), K(free_count_), KP(allocator_), KP(start_ptr_));
private:
  bool is_inited_;
  int64_t obj_size_;
  int64_t capacity_;
  int64_t free_count_;
  ObIAllocator *allocator_;
  ObFixedQueue<T> pool_;
  char *start_ptr_;
};

class ObIOCallback
{
public:
  static const int64_t CALLBACK_BUF_SIZE = 1024;
  ObIOCallback();
  virtual ~ObIOCallback();

  int process(const char *data_buffer, const int64_t size);
  virtual ObIAllocator *get_allocator() = 0;
  virtual const char *get_data() = 0;
  virtual int64_t size() const = 0;
  virtual int alloc_data_buf(const char *io_data_buffer, const int64_t data_size) = 0;
  virtual int inner_process(const char *data_buffer, const int64_t size) = 0;
  DECLARE_PURE_VIRTUAL_TO_STRING;

protected:
  static int alloc_and_copy_data(const char *io_data_buffer, const int64_t data_size, common::ObIAllocator *allocator, char *&data_buffer);
private:
  friend class ObIOResult;
  lib::Worker::CompatMode compat_mode_;
};

struct ObIOInfo final
{
public:
  ObIOInfo();
  ~ObIOInfo();
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K(tenant_id_), K(fd_), K(offset_), K(size_), K(timeout_us_), K(flag_), KP(callback_), KP(buf_), KP(user_data_buf_));

public:
  uint64_t tenant_id_;
  ObIOFd fd_;
  int64_t offset_;
  int64_t size_;
  int64_t timeout_us_;
  ObIOFlag flag_;
  ObIOCallback *callback_;
  const char *buf_;
  char *user_data_buf_; //actual data buf without cb, allocated by the calling layer
};

template <typename T>
class ObRefHolder final
{
public:
  explicit ObRefHolder(): ptr_(nullptr) {}
  explicit ObRefHolder(T *ptr): ptr_(nullptr) { hold(ptr); }
  ~ObRefHolder() { reset(); }
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
  TO_STRING_KV(K(enqueue_ts_), K(dequeue_ts_), K(submit_ts_), K(return_ts_), K(callback_enqueue_ts_));
public:
  int64_t enqueue_ts_;
  int64_t dequeue_ts_;
  int64_t submit_ts_;
  int64_t return_ts_;
  int64_t callback_enqueue_ts_;
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
class ObIORequest;

class ObIOResult final
{
public:
  ObIOResult();
  ~ObIOResult();
  bool is_valid() const;
  int basic_init(); //for pool
  int init(const ObIOInfo &info);
  void reset();
  void destroy();
  void cancel();
  void finish(const ObIORetCode &ret_code, ObIORequest *req = nullptr);
  void calc_io_offset_and_size(int64_t &size, int32_t &offset);
  ObIOMode get_mode() const;
  uint64_t get_resource_group_id() const;
  uint64_t get_sys_module_id() const;
  bool is_sys_module() const;
  int64_t get_data_size() const;
  uint64_t get_io_usage_index();
  void inc_ref(const char *msg = nullptr);
  void dec_ref(const char *msg = nullptr);
  void inc_out_ref();
  void dec_out_ref();
  void finish_without_accumulate(const ObIORetCode &ret_code);
  ObThreadCond &get_cond() { return cond_; }

  TO_STRING_KV(K(is_inited_), K(is_finished_), K(is_canceled_), K(has_estimated_), K(complete_size_), K(offset_), K(size_),
               K(timeout_us_), K(result_ref_cnt_), K(out_ref_cnt_), K(flag_), K(ret_code_), K(tenant_io_mgr_),
               KP(user_data_buf_), KP(buf_), KP(io_callback_), K(begin_ts_), K(end_ts_), K_(time_log));
  DISALLOW_COPY_AND_ASSIGN(ObIOResult);

private:
  friend class ObIORequest;
  friend class ObIOHandle;
  friend class ObIOFaultDetector;
  friend class ObTenantIOManager;
  friend class ObAsyncIOChannel;
  friend class ObSyncIOChannel;
  friend class ObIORunner;
  bool is_inited_;
  bool is_finished_;
  bool is_canceled_;
  bool has_estimated_;
  volatile int32_t result_ref_cnt_; //for io_result and io_handle
  volatile int32_t out_ref_cnt_; //for io_handle
  int32_t complete_size_;
  int32_t offset_;
  int64_t size_;
  int64_t timeout_us_;
  int64_t begin_ts_;
  int64_t end_ts_;
  ObRefHolder<ObTenantIOManager> tenant_io_mgr_;
  const char *buf_;
  char *user_data_buf_; //actual data buf without cb, allocated by thpe calling layer
  ObIOCallback *io_callback_;
  ObIOFlag flag_;
  ObThreadCond cond_;
public:
  ObIOTimeLog time_log_;
  ObIORetCode ret_code_;
};

class ObIORequest : public common::ObDLinkBase<ObIORequest>
{
public:
  ObIORequest();
  ~ObIORequest();
  bool is_valid() const;
  int init(const ObIOInfo &info, ObIOResult *result);
  int basic_init(); //for pool
  void destroy();
  void reset();
  void free();
  void set_result(ObIOResult &io_result);
  void calc_io_offset_and_size(int64_t &size, int64_t &offset);
  bool is_canceled();
  int64_t timeout_ts() const;
  int64_t get_data_size() const;
  uint64_t get_resource_group_id() const;
  uint64_t get_sys_module_id() const;
  bool is_sys_module() const;
  uint64_t get_io_usage_index();
  char *calc_io_buf(); //calc the aligned io_buf of raw_buf_, which interact with the operating system
  const ObIOFlag &get_flag() const;
  ObIOMode get_mode() const;
  ObIOCallback *get_callback() const;
  int alloc_io_buf(char *&io_buf);
  int prepare(char *next_buffer = nullptr, int64_t next_size = 0, int64_t next_offset = 0);
  int recycle_buffer();
  int re_prepare();
  int try_alloc_buf_until_timeout(char *&io_buf);
  bool can_callback() const;
  void free_io_buffer();
  void inc_ref(const char *msg = nullptr);
  void dec_ref(const char *msg = nullptr);

  TO_STRING_KV(K(is_inited_), K(tenant_id_), KP(control_block_), K(ref_cnt_), KP(raw_buf_), K(fd_),
               K(trace_id_), K(retry_count_), K(tenant_io_mgr_), KPC(io_result_));
private:
  friend class ObIOResult;
  friend class ObIOSender;
  friend class ObIORunner;
  friend class ObMClockQueue;
  friend class ObAsyncIOChannel;
  friend class ObSyncIOChannel;
  friend class ObIOFaultDetector;
  friend class ObTenantIOManager;
  friend class ObIOUsage;
  friend class ObSysIOUsage;
  friend class ObIOScheduler;
  const char *get_io_data_buf(); //get data buf for MEMCPY before io_buf recycle
  int alloc_aligned_io_buf(char *&io_buf);

public:
  ObIOResult *io_result_;
private:
  bool is_inited_;
  int8_t retry_count_;
  volatile int32_t ref_cnt_;
  void *raw_buf_;//actual allocated io buf
  ObIOCB *control_block_;
  uint64_t tenant_id_;
  ObRefHolder<ObTenantIOManager> tenant_io_mgr_;
  ObIOFd fd_;
  ObCurTraceId::TraceId trace_id_;
};

typedef common::ObDList<ObIORequest> IOReqList;
class ObPhyQueue final
{
public:
  ObPhyQueue();
  ~ObPhyQueue();
  bool is_stop_accept() { return stop_accept_; }
  int init(const int64_t index);
  void destroy();
  void reset_time_info();
  void reset_queue_info();
  void set_stop_accept() { stop_accept_ = true; }
  bool reach_adjust_interval();
public:
  TO_STRING_KV(K_(reservation_ts), K_(group_limitation_ts), K_(tenant_limitation_ts),
               K_(stop_accept), K_(last_empty_ts));
  bool is_inited_;
  bool stop_accept_;
  int64_t reservation_ts_;
  int64_t group_limitation_ts_;
  int64_t tenant_limitation_ts_;
  int64_t proportion_ts_;
  int64_t last_empty_ts_;
  bool is_group_ready_;
  bool is_tenant_ready_;
  int64_t queue_index_; //index in array, INT64_MAX means other
  int64_t reservation_pos_;
  int64_t group_limitation_pos_;
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
  int set_result(ObIOResult &result);
  bool is_empty() const;
  bool is_valid() const;
  OB_INLINE bool is_finished() const { return nullptr != result_ && result_->is_finished_; }

  int wait();
  const char *get_buffer();
  int64_t get_data_size() const;
  int64_t get_rt() const;
  int get_fs_errno(int &io_errno) const;
  void reset();
  void cancel();
  TO_STRING_KV("io_result", to_cstring(result_));
private:
  void estimate();

private:
  ObIOResult *result_;
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

  struct GroupConfig
  {
  public:
    GroupConfig();
    ~GroupConfig();
    bool is_valid() const;
    TO_STRING_KV(K_(deleted), K_(cleared),K_(min_percent), K_(max_percent), K_(weight_percent));
  public:
    bool deleted_; //group被删除的标记
    bool cleared_; //group被清零的标记，以后有新的directive就会重置
    int64_t min_percent_;
    int64_t max_percent_;
    int64_t weight_percent_;
  };

public:
  ObTenantIOConfig();
  ~ObTenantIOConfig();
  void destroy();
  static const ObTenantIOConfig &default_instance();
  bool is_valid() const;
  bool operator ==(const ObTenantIOConfig &other) const;
  int deep_copy(const ObTenantIOConfig &other_config);
  int parse_group_config(const char *config_str);
  int add_single_group_config(const uint64_t tenant_id, const int64_t group_id, int64_t min_percent, int64_t max_percent, int64_t weight_percent);
  int get_group_config(const uint64_t index, int64_t &min_iops, int64_t &max_iops, int64_t &iops_weight) const;
  int64_t get_all_group_num() const;
  int64_t get_callback_thread_count() const;
  int64_t to_string(char* buf, const int64_t buf_len) const;
public:
  int64_t memory_limit_;
  int64_t callback_thread_count_;
  int64_t group_num_;
  UnitConfig unit_config_;
  ObSEArray <int64_t , GROUP_START_NUM> group_ids_;
  ObSEArray <GroupConfig, GROUP_START_NUM> group_configs_;
  GroupConfig other_group_config_;
  bool group_config_change_;
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
  int get_time_info(int64_t &reservation_ts,
                    int64_t &group_limitation_ts,
                    int64_t &tenant_limitation_ts,
                    int64_t &proportion_ts);
  int push_phyqueue(ObPhyQueue *phy_queue);
  int pop_phyqueue(ObIORequest *&req, int64_t &deadline_ts);
  TO_STRING_KV(K(is_inited_), K(r_heap_.count()), K(gl_heap_.count()), K(tl_heap_.count()), K(ready_heap_.count()));

  int remove_from_heap(ObPhyQueue *phy_queue);
private:
  int pop_with_ready_queue(const int64_t current_ts, ObIORequest *&req, int64_t &deadline_ts);

  template<typename T, int64_t T::*member_ts, IOReqList T::*list>
  struct HeapCompare {
    int get_error_code() { return OB_SUCCESS; }
    bool operator() (const T *left, const T *right) const {
      return left->*member_ts != right->*member_ts ? left->*member_ts > right->*member_ts :
             (left->*list).get_size() != (right->*list).get_size() ? (left->*list).get_size() < (right->*list).get_size() : (int64_t)left > (int64_t)right;
    }
  };
private:
  bool is_inited_;
  HeapCompare<ObPhyQueue, &ObPhyQueue::reservation_ts_, &ObPhyQueue::req_list_> r_cmp_;
  HeapCompare<ObPhyQueue, &ObPhyQueue::group_limitation_ts_, &ObPhyQueue::req_list_> gl_cmp_;
  HeapCompare<ObPhyQueue, &ObPhyQueue::tenant_limitation_ts_, &ObPhyQueue::req_list_> tl_cmp_;
  HeapCompare<ObPhyQueue, &ObPhyQueue::proportion_ts_, &ObPhyQueue::req_list_> p_cmp_;
  ObRemovableHeap<ObPhyQueue *, HeapCompare<ObPhyQueue, &ObPhyQueue::reservation_ts_, &ObPhyQueue::req_list_>, &ObPhyQueue::reservation_pos_> r_heap_;
  ObRemovableHeap<ObPhyQueue *, HeapCompare<ObPhyQueue, &ObPhyQueue::group_limitation_ts_, &ObPhyQueue::req_list_>, &ObPhyQueue::group_limitation_pos_> gl_heap_;
  ObRemovableHeap<ObPhyQueue *,HeapCompare<ObPhyQueue, &ObPhyQueue::tenant_limitation_ts_, &ObPhyQueue::req_list_>, &ObPhyQueue::tenant_limitation_pos_> tl_heap_;
  ObRemovableHeap<ObPhyQueue *, HeapCompare<ObPhyQueue, &ObPhyQueue::proportion_ts_, &ObPhyQueue::req_list_>, &ObPhyQueue::proportion_pos_> ready_heap_;
};

template <typename T>
ObIOObjectPool<T>::ObIOObjectPool()
  : is_inited_(false), obj_size_(0), capacity_(0), free_count_(0), allocator_(nullptr), start_ptr_(nullptr)
{

}

template <typename T>
ObIOObjectPool<T>::~ObIOObjectPool()
{
  destroy();
}

template <typename T>
int ObIOObjectPool<T>::init(const int64_t count, const int64_t size, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "init twice", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(count), K(size));
  } else {
    obj_size_ = size;
    allocator_ = &allocator;
    capacity_ = count;
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(pool_.init(capacity_, allocator_))) {
    COMMON_LOG(WARN, "fail to init memory pool", K(ret));
  } else if (OB_ISNULL(start_ptr_ = reinterpret_cast<char *>(allocator_->alloc(capacity_ * size)))){
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(WARN, "fail to allocate IOobj memory", K(ret), K(capacity_), K(size));
  } else {
    void *buf = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < capacity_; ++i) {
      buf = start_ptr_ + i * size;
      T *t = nullptr;
      t = new (reinterpret_cast<T *>(buf)) T;
      if (OB_FAIL(t->basic_init())) {
        COMMON_LOG(WARN, "basic init failed", K(ret), K(i));
      } else if (OB_FAIL(pool_.push(t))) {
        COMMON_LOG(WARN, "fail to push IOobj block to pool", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    free_count_ = capacity_;
    is_inited_ = true;
  } else {
    destroy();
  }
  return ret;
}

template <typename T>
void ObIOObjectPool<T>::destroy()
{
  is_inited_ = false;
  free_count_ = 0;
  pool_.destroy();

  if (nullptr != allocator_) {
    if (nullptr != start_ptr_) {
      allocator_->free(start_ptr_);
      start_ptr_ = nullptr;
    }
  }
  allocator_ = nullptr;
}

template <typename T>
int ObIOObjectPool<T>::alloc(T *&ptr)
{
  int ret = OB_SUCCESS;
  ptr = nullptr;
  T *ret_ptr = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(pool_.pop(ret_ptr))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      COMMON_LOG(WARN, "fail to pop IOobj", K(ret), K(capacity_), K(obj_size_));
    }
  } else {
    ATOMIC_DEC(&free_count_);
    ptr = ret_ptr;
  }
  return ret;
}

template <typename T>
int ObIOObjectPool<T>::recycle(T *ptr)
{
  int ret = OB_SUCCESS;
  const int64_t idx = ((char*) ptr - start_ptr_) / obj_size_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else if (nullptr == ptr || idx < 0 || idx >= capacity_) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(idx), KP(ptr));
  } else {
    if (OB_FAIL(pool_.push(ptr))) {
      COMMON_LOG(WARN, "fail to push IOobj", K(ret), K(capacity_), K(obj_size_));
    } else {
      ATOMIC_INC(&free_count_);
    }
  }
  return ret;
}

template <typename T>
bool ObIOObjectPool<T>::contain(T *ptr)
{
  bool bret = (uint64_t) ptr > 0
      && ((char *)ptr >= start_ptr_)
      && ((char *)ptr <  start_ptr_ + (capacity_ * obj_size_))
      && (((char *)ptr - start_ptr_) % obj_size_ == 0);
  return bret;
}

} // namespace common
} // namespace oceanbase

#endif//OCEANBASE_LIB_STORAGE_IO_DEFINE
