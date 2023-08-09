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

#ifndef OCEANBASE_LIB_STORAGE_OB_IO_STRUCT_H
#define OCEANBASE_LIB_STORAGE_OB_IO_STRUCT_H

#include <sys/resource.h>
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "lib/lock/ob_mutex.h"
#include "lib/lock/ob_drw_lock.h"
#include "lib/thread/thread_mgr_interface.h"
#include "lib/queue/ob_fixed_queue.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/container/ob_array_wrap.h"
#include "lib/lock/ob_spin_lock.h"
#include "share/io/ob_io_define.h"
#include "share/io/io_schedule/ob_io_mclock.h"

namespace oceanbase
{
namespace common
{

struct ObIOConfig final
{
public:
  ObIOConfig();
  ~ObIOConfig();
  static const ObIOConfig &default_config();
  void set_default_value();
  bool is_valid() const;
  void reset();
  TO_STRING_KV(
      K(write_failure_detect_interval_),
      K(read_failure_black_list_interval_),
      K(data_storage_warning_tolerance_time_),
      K(data_storage_error_tolerance_time_),
      K(disk_io_thread_count_),
      K(data_storage_io_timeout_ms_));

public:
  static const int64_t MAX_IO_THREAD_COUNT = 32 * 2;
  // diagnose related
  int64_t write_failure_detect_interval_; // const literal
  int64_t read_failure_black_list_interval_; // const literal
  int64_t data_storage_warning_tolerance_time_;
  int64_t data_storage_error_tolerance_time_;
  // resource related
  int64_t disk_io_thread_count_;
  int64_t data_storage_io_timeout_ms_;
};

template<int64_t SIZE>
class ObIOMemoryPool final
{
public:
  ObIOMemoryPool();
  ~ObIOMemoryPool();
  int init(const int64_t block_count, ObIAllocator &allocator);
  void destroy();
  int alloc(void *&ptr);
  int free(void *ptr);
  bool contain(void *ptr);
  int64_t get_block_size() const { return SIZE; }
private:
  bool is_inited_;
  int64_t capacity_;
  int64_t free_count_;
  ObIAllocator *allocator_;
  ObFixedQueue<char> pool_;
  char *begin_ptr_;
};

class ObIOAllocator : public ObIAllocator
{
public:
  ObIOAllocator();
  virtual ~ObIOAllocator();
  int init(const uint64_t tenant_id, const int64_t memory_limit);
  void destroy();
  int update_memory_limit(const int64_t memory_limit);
  int64_t get_allocated_size() const;
  int64_t get_pre_allocated_count() const { return block_count_; }
  virtual void *alloc(const int64_t size, const lib::ObMemAttr &attr) override;
  virtual void *alloc(const int64_t size) override;
  virtual void free(void *ptr) override;
  template<typename T, typename... Args> int alloc(T *&instance, Args &...args);
  template<typename T> void free(T *instance);
  TO_STRING_KV(K(is_inited_), "allocated", inner_allocator_.allocated());
private:
  int init_macro_pool(const int64_t memory_limit);
  int calculate_pool_block_count(const int64_t memory_limit, int64_t &block_count);
private:
  static const int64_t MACRO_POOL_BLOCK_SIZE = 2L * 1024L * 1024L + DIO_READ_ALIGN_SIZE;
  bool is_inited_;
  int64_t memory_limit_;
  int64_t block_count_;
  ObConcurrentFIFOAllocator inner_allocator_;
  ObIOMemoryPool<MACRO_POOL_BLOCK_SIZE> macro_pool_;
};


struct ObIOStat final
{
public:
  ObIOStat();
  ~ObIOStat();
  void accumulate(const uint64_t io_count, const uint64_t io_bytes, const uint64_t io_rt_us);
  void reset();
  TO_STRING_KV(K(io_count_), K(io_bytes_), K(io_rt_us_));

public:
  uint64_t io_count_;
  uint64_t io_bytes_;
  uint64_t io_rt_us_;
};

class ObIOStatDiff final
{
public:
  ObIOStatDiff();
  ~ObIOStatDiff();
  void diff(const ObIOStat &io_stat, double &avg_iops, double &avg_bytes, double &avg_rt_us);
  void reset();
  TO_STRING_KV(K(last_stat_), K(last_ts_));
private:
  ObIOStat last_stat_;
  int64_t last_ts_;
};

class ObIOUsage final
{
public:
  ObIOUsage();
  ~ObIOUsage();
  int init(const int64_t group_num);
  int refresh_group_num (const int64_t group_num);
  void accumulate(ObIORequest &req);
  void calculate_io_usage();
  typedef ObSEArray<ObSEArray<double, GROUP_START_NUM>, 2> AvgItems;
  void get_io_usage(AvgItems &avg_iops, AvgItems &avg_bytes, AvgItems &avg_rt_us);
  void record_request_start(ObIORequest &req);
  void record_request_finish(ObIORequest &req);
  bool is_request_doing(const int64_t index) const;
  int64_t get_io_usage_num() const;
  int64_t to_string(char* buf, const int64_t buf_len) const;
private:
  ObSEArray<ObSEArray<ObIOStat, GROUP_START_NUM>, 2> io_stats_;
  ObSEArray<ObSEArray<ObIOStatDiff, GROUP_START_NUM>, 2> io_estimators_;
  AvgItems group_avg_iops_;
  AvgItems group_avg_byte_;
  AvgItems group_avg_rt_us_;
  int64_t group_num_;
  ObSEArray<int64_t, GROUP_START_NUM> doing_request_count_;
};

class ObSysIOUsage final
{
public:
  ObSysIOUsage();
  ~ObSysIOUsage();
  int init();
  void accumulate(ObIORequest &req);
  void calculate_io_usage();
  typedef ObSEArray<ObSEArray<double, SYS_RESOURCE_GROUP_CNT>, 2> SysAvgItems;
  void get_io_usage(SysAvgItems &avg_iops, SysAvgItems &avg_bytes, SysAvgItems &avg_rt_us);
  void record_request_start(ObIORequest &req);
  void record_request_finish(ObIORequest &req);
  TO_STRING_KV(K(io_stats_), K(io_estimators_), K(group_avg_iops_), K(group_avg_byte_), K(group_avg_rt_us_));
private:
  ObSEArray<ObSEArray<ObIOStat, SYS_RESOURCE_GROUP_CNT>, 2> io_stats_;
  ObSEArray<ObSEArray<ObIOStatDiff, SYS_RESOURCE_GROUP_CNT>, 2> io_estimators_;
  SysAvgItems group_avg_iops_;
  SysAvgItems group_avg_byte_;
  SysAvgItems group_avg_rt_us_;
};

class ObCpuUsage final
{
public:
  ObCpuUsage();
  ~ObCpuUsage();
  void get_cpu_usage(double &avg_usage_percentage);
  void reset();
private:
  struct rusage last_usage_;
  int64_t last_ts_;
};

class ObIOScheduler;
class ObIOTuner : public lib::TGRunnable
{
public:
  ObIOTuner(ObIOScheduler &io_scheduler);
  virtual ~ObIOTuner();
  int init();
  void destroy();
  int send_detect_task();
  virtual void run1() override;

private:
  void print_sender_status();
  void print_io_status();
private:
  bool is_inited_;
  ObCpuUsage cpu_usage_;
  ObIOScheduler &io_scheduler_;
};

struct ObIOGroupQueues final {
public:
  ObIOGroupQueues(ObIAllocator &allocator);
  ~ObIOGroupQueues();
  int init(const int64_t group_num);
  void destroy();
  TO_STRING_KV(K(is_inited_), K(other_phy_queue_), K(group_phy_queues_));
public:
  bool is_inited_;
  ObIAllocator &allocator_;
  ObSEArray<ObPhyQueue *, GROUP_START_NUM> group_phy_queues_;
  ObPhyQueue other_phy_queue_;
};


struct ObSenderInfo final
{
public:
  ObSenderInfo();
  ~ObSenderInfo();
public:
  int64_t queuing_count_;
  int64_t reservation_ts_;
  int64_t group_limitation_ts_;
  int64_t tenant_limitation_ts_;
  int64_t proportion_ts_;
};

class ObIOSender : public lib::TGRunnable
{
public:
  ObIOSender(ObIAllocator &allocator);
  virtual ~ObIOSender();
  int init(const int64_t sender_index);
  void stop();
  void wait();
  void destroy();

  // thread related
  int start();
  void stop_submit();
  virtual void run1() override;

  int alloc_mclock_queue(ObIAllocator &allocator, ObMClockQueue *&io_queue);
  int enqueue_request(ObIORequest &req);
  int enqueue_phy_queue(ObPhyQueue &phyqueue);
  int dequeue_request(ObIORequest *&req);
  int update_group_queue(const uint64_t tenant_id, const int64_t group_num);
  int remove_group_queues(const uint64_t tenant_id);
  int stop_phy_queue(const uint64_t tenant_id, const uint64_t index);
  int notify();
  int64_t get_queue_count() const;
  int get_sender_info(int64_t &reservation_ts,
                      int64_t &group_limitation_ts,
                      int64_t &tenant_limitation_ts,
                      int64_t &proportion_ts);
  int get_sender_status(const uint64_t tenant_id, const uint64_t index, ObSenderInfo &sender_info);
  TO_STRING_KV(K(is_inited_), K(stop_submit_), KPC(io_queue_), K(tg_id_), K(sender_index_));
//private:
  void pop_and_submit();
  int64_t calc_wait_timeout(const int64_t queue_deadline);
  int submit(ObIORequest &req);
  int64_t sender_req_count_;
  int64_t sender_index_;
  int tg_id_; // thread group id
  bool is_inited_;
  bool stop_submit_;
  ObIAllocator &allocator_;
  ObMClockQueue *io_queue_;
  ObThreadCond queue_cond_;
  hash::ObHashMap<uint64_t, ObIOGroupQueues *> tenant_groups_map_;
};


class ObIOScheduler final
{
public:
  ObIOScheduler(const ObIOConfig &io_config, ObIAllocator &allocator);
  ~ObIOScheduler();
  int init(const int64_t queue_count, const int64_t schedule_media_id = 0);
  void destroy();
  int start();
  void stop();
  void accumulate(const ObIORequest &req);
  int schedule_request(ObIORequest &req);
  int init_group_queues(const uint64_t tenant_id, const int64_t group_num, ObIOAllocator *io_allocator);
  int update_group_queues(const uint64_t tenant_id, const int64_t group_num);
  int remove_phyqueues(const uint64_t tenant_id);
  int stop_phy_queues(const uint64_t tenant_id, const int64_t index);
  ObIOSender *get_cur_sender(const int thread_id){ return senders_.at(thread_id); };
  int64_t get_senders_count() { return senders_.count(); }
  TO_STRING_KV(K(is_inited_), K(io_config_), K(senders_));
private:
  friend class ObIOTuner;
  bool is_inited_;
  const ObIOConfig &io_config_;
  ObIAllocator &allocator_;
  ObSEArray<ObIOSender *, 1> senders_;
  ObIOTuner io_tuner_;
  int64_t schedule_media_id_;
};

class ObDeviceChannel;

/**
 * worker to process sync io reuest and get result of async io request from file system
 * io channel has two independent threads, one for sync io operation, anather for polling events from file system
 */
class ObIOChannel : public lib::TGRunnable
{
public:
  ObIOChannel();
  virtual ~ObIOChannel();

  int base_init(ObDeviceChannel *device_channel);
  int start_thread();
  void destroy_thread();
  virtual int submit(ObIORequest &req) = 0;
  virtual void cancel(ObIORequest &req) = 0;
  virtual int64_t get_queue_count() const = 0;
  TO_STRING_KV(K(is_inited_), KP(device_handle_), K(tg_id_), "queue_count", get_queue_count());

protected:
  bool is_inited_;
  int tg_id_; // thread group id
  ObIODevice *device_handle_;
  ObDeviceChannel *device_channel_;
};


class ObAsyncIOChannel : public ObIOChannel
{
public:
  ObAsyncIOChannel();
  virtual ~ObAsyncIOChannel();

  int init(ObDeviceChannel *device_channel);
  void stop();
  void wait();
  void destroy();
  virtual void run1() override;
  virtual int submit(ObIORequest &req) override;
  virtual void cancel(ObIORequest &req) override;
  virtual int64_t get_queue_count() const override;
  INHERIT_TO_STRING_KV("IOChannel", ObIOChannel, KP(io_context_), KP(io_events_), K(submit_count_));

private:
  void get_events();
  int on_full_return(ObIORequest &req);
  int on_partial_return(ObIORequest &req, const int64_t complete_size);
  int on_partial_retry(ObIORequest &req, const int64_t complete_size);
  int on_full_retry(ObIORequest &req);
  int on_failed(ObIORequest &req, const ObIORetCode &ret_code);

private:
  static const int32_t MAX_AIO_EVENT_CNT = 512;
  static const int64_t AIO_POLLING_TIMEOUT_NS = 1000L * 1000L * 1000L - 1L; // almost 1s, for timespec_valid check
  ObIOContext *io_context_;
  ObIOEvents *io_events_;
  struct timespec polling_timeout_;
  int64_t submit_count_;
  ObThreadCond depth_cond_;
};

class ObSyncIOChannel : public ObIOChannel
{
public:
  ObSyncIOChannel();
  virtual ~ObSyncIOChannel();
  int init(ObDeviceChannel *device_channel);
  void destroy();
  virtual void run1() override;
  virtual int submit(ObIORequest &req) override;
  virtual void cancel(ObIORequest &req) override;
  virtual int64_t get_queue_count() const override;
  INHERIT_TO_STRING_KV("IOChannel", ObIOChannel, K(req_queue_.get_total()));
private:
  int do_sync_io(ObIORequest &req);
private:
  static const int64_t MAX_SYNC_IO_QUEUE_COUNT = 512;
  ObFixedQueue<ObIORequest> req_queue_;
  ObThreadCond cond_;
  bool is_wait_;
};

// each device has several channels, including async channels and sync channels.
// this interface better in ObIODevice and can be replaced by io_uring.
class ObDeviceChannel final
{
public:
  ObDeviceChannel();
  ~ObDeviceChannel();
  int init(ObIODevice *device_handle,
           const int64_t async_channel_count,
           const int64_t sync_channel_count,
           const int64_t max_io_depth,
           ObIAllocator &allocator);
  void destroy();
  int submit(ObIORequest &req);
  TO_STRING_KV(K(is_inited_), KP(allocator_), K(async_channels_), K(sync_channels_));
private:
  int get_random_io_channel(ObIArray<ObIOChannel *> &io_channels, ObIOChannel *&ch);

private:
  friend class ObIOChannel;
  friend class ObAsyncIOChannel;
  friend class ObSyncIOChannel;
  bool is_inited_;
  ObIAllocator *allocator_;
  ObSEArray<ObIOChannel *, 8> async_channels_;
  ObSEArray<ObIOChannel *, 8> sync_channels_;
  ObIODevice *device_handle_;
  int64_t used_io_depth_;
  int64_t max_io_depth_;
};

class ObIORunner : public lib::TGRunnable
{
public:
  ObIORunner();
  virtual ~ObIORunner();
  int init(const int64_t queue_capacity, ObIAllocator &allocator);
  void stop();
  void wait();
  void destroy();
  virtual void run1() override;
  int push(ObIORequest &req);
  int pop(ObIORequest *&req);
  int handle(ObIORequest *req);
  int64_t get_queue_count();
  TO_STRING_KV(K(is_inited_), K(queue_.get_total()), K(tg_id_));
private:
  static const int64_t CALLBACK_WAIT_PERIOD_US = 1000L * 1000L; // 1s
  bool is_inited_;
  int tg_id_;
  ObThreadCond cond_;
  ObFixedQueue<ObIORequest> queue_;
};


class ObIOCallbackManager final
{
public:
  ObIOCallbackManager();
  ~ObIOCallbackManager();
  int init(const int64_t tenant_id, int64_t thread_count,
           const int32_t queue_depth, ObIOAllocator *io_allocator);
  void destroy();

  int enqueue_callback(ObIORequest &req);
  int update_thread_count(const int64_t thread_count);
  int64_t get_thread_count() const;
  int64_t get_queue_depth() const;
  int get_queue_count(ObIArray<int64_t> &queue_count_array);
  TO_STRING_KV(K(is_inited_), K(config_thread_count_), K(queue_depth_), K(runners_), KPC(io_allocator_));

private:
  bool is_inited_;
  int32_t queue_depth_;
  int64_t config_thread_count_;
  ObArray<ObIORunner *> runners_;
  ObIOAllocator *io_allocator_;
};

// Device Health status
enum ObDeviceHealthStatus
{
  DEVICE_HEALTH_NORMAL = 0,
  DEVICE_HEALTH_WARNING,
  DEVICE_HEALTH_ERROR
};

const char *device_health_status_to_str(const ObDeviceHealthStatus dhs);

class ObIOFaultDetector : public lib::TGTaskHandler
{
public:
  ObIOFaultDetector(const ObIOConfig &io_config);
  virtual ~ObIOFaultDetector();
  int init();
  void destroy();
  int start();
  virtual void handle(void *task) override;
  int get_device_health_status(ObDeviceHealthStatus &dhs, int64_t &device_abnormal_time);
  void reset_device_health();
  void record_failure(const ObIORequest &req);
  int record_timing_task(const int64_t first_id, const int64_t second_id);

private:
  int record_read_failure(const ObIORequest &req);
  int record_write_failure();
  void set_device_warning();
  void set_device_error();

private:
  static const int64_t WRITE_FAILURE_DETECT_EVENT_COUNT = 100;
  bool is_inited_;
  ObSpinLock lock_;
  const ObIOConfig &io_config_;
  bool is_device_warning_;
  int64_t last_device_warning_ts_;
  bool is_device_error_;
  int64_t begin_device_error_ts_;
  int64_t last_device_error_ts_;
};

class ObIOTracer final
{
public:
  struct RefLog
  {
  public:
    RefLog();
    void click(const char *mod = NULL);
    TO_STRING_KV(K_(click_count), "ref_log", ObArrayWrap<const char *>(click_str_, min(MAX_CLICK_COUNT, click_count_)));
  private:
    static const int64_t MAX_CLICK_COUNT = 16;
    int64_t click_count_;
    const char *click_str_[MAX_CLICK_COUNT];
  };
  struct TraceInfo
  {
  public:
    TraceInfo();
    uint64_t hash() const;
    int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
    bool operator== (const TraceInfo &param) const;
    TO_STRING_KV(KCSTRING(bt_str_), K(ref_log_));
  public:
    char bt_str_[LBT_BUFFER_LENGTH];
    RefLog ref_log_;
  };
  enum TraceType
  {
    IS_FIRST,
    IS_LAST,
    OTHER
  };
  ObIOTracer();
  ~ObIOTracer();
  int init(const uint64_t tenant_id);
  void destroy();
  void reuse();
  int trace_request(const ObIORequest *req, const char *msg, const TraceType trace_type);
  void print_status();
private:
  bool is_inited_;
  uint64_t tenant_id_;
  hash::ObHashMap<int64_t /*request_ptr*/, TraceInfo> trace_map_;
};


// template function implementation

template<typename T, typename... Args>
int ObIOAllocator::alloc(T *&instance, Args &...args)
{
  int ret = OB_SUCCESS;
  instance = nullptr;
  void *buf = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "io allocator not init", K(ret), K(is_inited_));
  } else if (OB_ISNULL(buf = inner_allocator_.alloc(sizeof(T)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(WARN, "allocate memory failed", K(ret), KP(buf), K(sizeof(T)));
  } else {
    instance = new (buf) T(args...);
  }
  return ret;
}

template<typename T>
void ObIOAllocator::free(T *instance)
{
  if (nullptr != instance) {
    instance->~T();
    inner_allocator_.free(instance);
  }
}

// some utilities

inline bool is_io_aligned(const int64_t value)
{
  return 0 == value % DIO_READ_ALIGN_SIZE;
}

inline void align_offset_size(
    const int64_t offset,
    const int64_t size,
    int64_t &align_offset,
    int64_t &align_size)
{
  align_offset = lower_align(offset, DIO_READ_ALIGN_SIZE);
  align_size = upper_align(size + offset - align_offset, DIO_READ_ALIGN_SIZE);
}

typedef ObRefHolder<ObIORequest> RequestHolder;

class ObTraceIDGuard final
{
public:
  explicit ObTraceIDGuard(const ObCurTraceId::TraceId &trace_id)
  {
    saved_trace_id_ = *ObCurTraceId::get_trace_id();
    ObCurTraceId::set(trace_id);
  }
  ~ObTraceIDGuard()
  {
    ObCurTraceId::set(saved_trace_id_);
  }
private:
  ObCurTraceId::TraceId saved_trace_id_;
};


}// end namespace oceanbase
}// end namespace oceanbase

#endif//OCEANBASE_LIB_STORAGE_OB_IO_STRUCT_H
