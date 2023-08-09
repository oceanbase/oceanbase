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

static constexpr int64_t DEFAULT_IO_WAIT_TIME_MS = 5000L; // 5s
static constexpr int64_t MAX_IO_WAIT_TIME_MS = 300L * 1000L; // 5min
static constexpr int64_t GROUP_START_NUM = 8L;
static constexpr int64_t MAX_DETECT_READ_TIMES = 10L;
enum class ObIOMode : uint8_t
{
  READ = 0,
  WRITE = 1,
  MAX_MODE
};

const char *get_io_mode_string(const ObIOMode mode);
ObIOMode get_io_mode_enum(const char *mode_string);

enum ObIOModule {
  SLOG_IO = 20000,
  CALIBRATION_IO = 20001,
  DETECT_IO = 20002,
  DIRECT_LOAD_IO = 20003,
  SHARED_BLOCK_RW_IO = 20004,
  SSTABLE_WHOLE_SCANNER_IO = 20005,
  INSPECT_BAD_BLOCK_IO = 20006,
  SSTABLE_INDEX_BUILDER_IO = 20007,
  BACKUP_READER_IO = 20008,
  BLOOM_FILTER_IO = 20009,
  SHARED_MACRO_BLOCK_MGR_IO = 20010,
  INDEX_BLOCK_TREE_CURSOR_IO = 20011,
  MICRO_BLOCK_CACHE_IO = 20012,
  ROOT_BLOCK_IO = 20013,
  TMP_PAGE_CACHE_IO = 20014,
  INDEX_BLOCK_MICRO_ITER_IO = 20015,
  HA_COPY_MACRO_BLOCK_IO = 20016,
  LINKED_MACRO_BLOCK_IO = 20017,
  HA_MACRO_BLOCK_WRITER_IO = 20018,
  TMP_TENANT_MEM_BLOCK_IO = 20019,
  MAX_IO = 20020
};

const char *get_io_sys_group_name(ObIOModule module);
struct ObIOFlag final
{
public:
  ObIOFlag();
  ~ObIOFlag();
  void reset();
  bool is_valid() const;
  void set_mode(ObIOMode mode);
  ObIOMode get_mode() const;
  void set_group_id(ObIOModule module);
  void set_group_id(int64_t group_id);
  void set_wait_event(int64_t wait_event_id);
  int64_t get_group_id() const;
  ObIOModule get_io_module() const;
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
  static constexpr int64_t IO_GROUP_ID_BIT = 16; // for consumer group in resource manager
  static constexpr int64_t IO_WAIT_EVENT_BIT = 32; // for performance monitor
  static constexpr int64_t IO_SYNC_FLAG_BIT = 1; // indicate if the caller is waiting io finished
  static constexpr int64_t IO_DETECT_FLAG_BIT = 1; // notify a retry task
  static constexpr int64_t IO_UNLIMITED_FLAG_BIT = 1; // indicate if the io is unlimited
  static constexpr int64_t IO_TIME_DETECT_FLAG_BIT = 1; // indicate if the io is unlimited
  static constexpr int64_t IO_RESERVED_BIT = 64 - IO_MODE_BIT
                                                - IO_GROUP_ID_BIT
                                                - IO_WAIT_EVENT_BIT
                                                - IO_SYNC_FLAG_BIT
                                                - IO_UNLIMITED_FLAG_BIT
                                                - IO_TIME_DETECT_FLAG_BIT
                                                - IO_DETECT_FLAG_BIT;

  union {
    int64_t flag_;
    struct {
      int64_t mode_ : IO_MODE_BIT;
      int64_t group_id_ : IO_GROUP_ID_BIT;
      int64_t wait_event_id_ : IO_WAIT_EVENT_BIT;
      bool is_sync_ : IO_SYNC_FLAG_BIT;
      bool is_unlimited_ : IO_UNLIMITED_FLAG_BIT;
      bool is_detect_ : IO_DETECT_FLAG_BIT;
      bool is_time_detect_ : IO_TIME_DETECT_FLAG_BIT;
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

  int process(const char *data_buffer, const int64_t size);
  int deep_copy(char *buf, const int64_t buf_len, ObIOCallback *&copied_callback) const;
  virtual const char *get_data() = 0;
  virtual int64_t size() const = 0;
  virtual int inner_process(const char *data_buffer, const int64_t size) = 0;
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
  int64_t get_group_id() const;
  uint64_t get_io_usage_index();
  const char *get_data(); //get data buf after io_buf recycle
  const ObIOFlag &get_flag() const;
  ObIOMode get_mode() const;
  void cancel();
  int alloc_io_buf();
  int prepare();
  bool can_callback() const;
  void free_io_buffer();
  void finish(const ObIORetCode &ret_code);
  void inc_ref(const char *msg = nullptr);
  void dec_ref(const char *msg = nullptr);
  void inc_out_ref();
  void dec_out_ref();
  VIRTUAL_TO_STRING_KV(K(is_inited_), K(is_finished_), K(is_canceled_), K(has_estimated_), K(io_info_), K(deadline_ts_),
      K(sender_index_), KP(control_block_), KP(raw_buf_), KP(io_buf_), K(io_offset_), K(io_size_), K(complete_size_),
      K(time_log_), KP(channel_), K(ref_cnt_), K(out_ref_cnt_),
      K(trace_id_), K(ret_code_), K(retry_count_), K(callback_buf_size_), KP(copied_callback_), K(tenant_io_mgr_));
private:
  friend class ObIORunner;
  const char *get_io_data_buf(); //get data buf for MEMCPY before io_buf recycle
  int alloc_aligned_io_buf();
public:
  bool is_inited_;
  bool is_finished_;
  bool is_canceled_;
  bool has_estimated_;
  ObIOInfo io_info_;
  int64_t deadline_ts_;
  int64_t sender_index_;
  ObIOCB *control_block_;
  void *raw_buf_;//actual allocated buf
  char *io_buf_;//the aligned one of raw_buf_, interact with the operating system
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
  bool is_stop_accept() { return stop_accept_; }
  int init(const int64_t index);
  void destroy();
  void reset_time_info();
  void reset_queue_info();
  void set_stop_accept() { stop_accept_ = true; }
public:
  typedef common::ObDList<ObIORequest> IOReqList;
  TO_STRING_KV(K_(reservation_ts), K_(group_limitation_ts), K_(tenant_limitation_ts), K_(stop_accept));
  bool is_inited_;
  bool stop_accept_;
  int64_t reservation_ts_;
  int64_t group_limitation_ts_;
  int64_t tenant_limitation_ts_;
  int64_t proportion_ts_;
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
  HeapCompare<ObPhyQueue, &ObPhyQueue::group_limitation_ts_> gl_cmp_;
  HeapCompare<ObPhyQueue, &ObPhyQueue::tenant_limitation_ts_> tl_cmp_;
  HeapCompare<ObPhyQueue, &ObPhyQueue::proportion_ts_> p_cmp_;
  ObRemovableHeap<ObPhyQueue *, HeapCompare<ObPhyQueue, &ObPhyQueue::reservation_ts_>, &ObPhyQueue::reservation_pos_> r_heap_;
  ObRemovableHeap<ObPhyQueue *, HeapCompare<ObPhyQueue, &ObPhyQueue::group_limitation_ts_>, &ObPhyQueue::group_limitation_pos_> gl_heap_;
  ObRemovableHeap<ObPhyQueue *, HeapCompare<ObPhyQueue, &ObPhyQueue::tenant_limitation_ts_>, &ObPhyQueue::tenant_limitation_pos_> tl_heap_;
  ObRemovableHeap<ObPhyQueue *, HeapCompare<ObPhyQueue, &ObPhyQueue::proportion_ts_>, &ObPhyQueue::proportion_pos_> ready_heap_;
};


} // namespace common
} // namespace oceanbase

#endif//OCEANBASE_LIB_STORAGE_IO_DEFINE
