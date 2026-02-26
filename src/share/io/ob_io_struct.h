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
#include "lib/thread/ob_simple_thread_pool.h"
#include "lib/queue/ob_fixed_queue.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/container/ob_array_wrap.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/lock/ob_qsync_lock.h"
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
      K(sync_io_thread_count_),
      K(data_storage_io_timeout_ms_));

private:
  // async io thread count
  static const int64_t MAX_IO_THREAD_COUNT = 32 * 2;
  // sync io thread count
  static const int64_t MAX_SYNC_IO_THREAD_COUNT = 1024;
public:
  // diagnose related
  int64_t write_failure_detect_interval_; // const literal
  int64_t read_failure_black_list_interval_; // const literal
  int64_t data_storage_warning_tolerance_time_;
  int64_t data_storage_error_tolerance_time_;
  // resource related
  int64_t disk_io_thread_count_;
  int64_t sync_io_thread_count_;
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
  virtual void *alloc(const int64_t size, const lib::ObMemAttr &attr) override;
  virtual void *alloc(const int64_t size) override;
  virtual void free(void *ptr) override;
  template<typename T, typename... Args> int alloc(T *&instance, Args &...args);
  template<typename T> void free(T *instance);
  TO_STRING_KV(K(is_inited_), "allocated", inner_allocator_.allocated());
private:
  bool is_inited_;
  int64_t memory_limit_;
  ObConcurrentFIFOAllocator inner_allocator_;
};


struct ObIOStat final
{
public:
  ObIOStat();
  ~ObIOStat();
  void accumulate(const uint64_t io_count, const uint64_t io_bytes, const uint64_t prepare_delay, const uint64_t schedule_delay, const uint64_t submit_delay, const int64_t device_delay, const int64_t total_delay);
  void reset();
  TO_STRING_KV(K(io_count_), K(io_bytes_), K(io_prepare_delay_us_), K(io_schedule_delay_us_), K(io_submit_delay_us_), K(io_device_delay_us_), K(io_total_delay_us_));

public:
  uint64_t io_count_;
  uint64_t io_bytes_;
  uint64_t io_prepare_delay_us_;
  uint64_t io_schedule_delay_us_;
  uint64_t io_submit_delay_us_;
  uint64_t io_device_delay_us_;
  uint64_t io_total_delay_us_;
};

class ObIOStatDiff final
{
public:
  ObIOStatDiff();
  ~ObIOStatDiff();
  void diff(const ObIOStat &io_stat, double &avg_iops, double &avg_bytes, int64_t &avg_prepare_delay,
      int64_t &avg_schedule_delay, int64_t &avg_submit_delay, int64_t &avg_device_delay, int64_t &avg_total_delay);
  void reset();
  TO_STRING_KV(K(last_stat_), K(last_ts_));

private:
  ObIOStat last_stat_;
  int64_t last_ts_;
};

struct ObIOUsageInfo
{
  ObIOStat io_stat_;
  ObIOStatDiff io_estimator_;
  double avg_iops_;
  double avg_byte_;
  int64_t avg_prepare_delay_us_;
  int64_t avg_schedule_delay_us_;
  int64_t avg_submit_delay_us_;
  int64_t avg_device_delay_us_; //Before 4.3.4, it was called avg_rt_us_.
  int64_t avg_total_delay_us_;
  TO_STRING_KV(K(io_stat_), K(io_estimator_), K(avg_iops_), K(avg_byte_), K(avg_prepare_delay_us_), K(avg_schedule_delay_us_), K(avg_submit_delay_us_), K(avg_device_delay_us_), K(avg_total_delay_us_));
};

// avg delay array (us)
struct ObIODelayArr
{
  ObIODelayArr():prepare_delay_us_(0), schedule_delay_us_(0), submit_delay_us_(0), device_delay_us_(0), total_delay_us_(0)
  {}
  ~ObIODelayArr(){}
  int64_t prepare_delay_us_;
  int64_t schedule_delay_us_;
  int64_t submit_delay_us_;
  int64_t device_delay_us_;
  int64_t total_delay_us_;
  void inc(const int prepare_delay, const int64_t schedule_delay, const int64_t submit_delay,
      const int64_t device_delay, const int64_t total_delay)
  {
    ATOMIC_FAA(&prepare_delay_us_, prepare_delay);
    ATOMIC_FAA(&schedule_delay_us_, schedule_delay);
    ATOMIC_FAA(&submit_delay_us_, submit_delay);
    ATOMIC_FAA(&device_delay_us_, device_delay);
    ATOMIC_FAA(&total_delay_us_, total_delay);
  }
  int reset()
  {
    ATOMIC_SET(&prepare_delay_us_, 0);
    ATOMIC_SET(&schedule_delay_us_, 0);
    ATOMIC_SET(&submit_delay_us_, 0);
    ATOMIC_SET(&device_delay_us_, 0);
    ATOMIC_SET(&total_delay_us_, 0);
    return OB_SUCCESS;
  }
  TO_STRING_KV(K(prepare_delay_us_), K(schedule_delay_us_), K(submit_delay_us_), K(device_delay_us_), K(total_delay_us_));
};

struct ObIOGroupUsage
{
  struct ObIOLastStat
  {
    ObIOLastStat() : avg_size_(0), avg_iops_(0), avg_bw_(0), avg_delay_arr_()
    {}
    ~ObIOLastStat()
    {}
    double avg_size_;
    double avg_iops_;
    int64_t avg_bw_;
    ObIODelayArr avg_delay_arr_;
    TO_STRING_KV(K(avg_size_), K(avg_iops_), K(avg_bw_), K(avg_delay_arr_));
  };
  ObIOGroupUsage()
      : last_ts_(ObTimeUtility::fast_current_time()), last_stat_(), io_count_(0), size_(0), total_delay_arr_()
  {}
  int calc(double &avg_size, double &avg_iops, int64_t &avg_bw, int64_t &avg_prepare_delay, int64_t &avg_schedule_delay,
      int64_t &avg_submit_delay, int64_t &avg_device_delay, int64_t &avg_total_delay);
  void inc(const int64_t size, const int prepare_delay, const int64_t schedule_delay, const int64_t submit_delay,
      const int64_t device_delay, const int64_t total_delay)
  {
    ATOMIC_FAA(&size_, size);
    ATOMIC_FAA(&io_count_, 1);
    total_delay_arr_.inc(prepare_delay, schedule_delay, submit_delay, device_delay, total_delay);
  }
  int clear()
  {
    int ret = total_delay_arr_.reset();
    return ret;
  }
  int record(const double avg_size, const double avg_iops, const int64_t avg_bw, const int64_t avg_prepare_delay,
      const int64_t avg_schedule_delay, const int64_t avg_submit_delay, const int64_t avg_device_delay,
      const int64_t avg_total_delay);
  int64_t last_ts_;  // us
  ObIOLastStat last_stat_;
  int64_t io_count_;
  int64_t size_;
  ObIODelayArr total_delay_arr_;  // us
  TO_STRING_KV(K(last_ts_), K(last_stat_), K(io_count_), K(size_), K(total_delay_arr_));
};

struct ObIOFailedReqUsageInfo : ObIOGroupUsage
{
};

typedef ObSEArray<ObIOUsageInfo, GROUP_START_NUM * static_cast<uint64_t>(ObIOGroupMode::MODECNT)> ObIOUsageInfoArray;
typedef ObSEArray<ObIOFailedReqUsageInfo, GROUP_START_NUM * static_cast<uint64_t>(ObIOGroupMode::MODECNT)> ObIOFailedReqInfoArray;
typedef ObSEArray<int64_t, GROUP_START_NUM> ObIOGroupThrottledTimeArray;
class ObIOUsage final
{
public:
  ObIOUsage() : info_(), failed_req_info_(), group_throttled_time_us_(), lock_() {}
  ~ObIOUsage();
  int init(const uint64_t tenant_id, const int64_t group_num);
  int refresh_group_num (const int64_t group_num);
  void accumulate(ObIORequest &request);
  void calculate_io_usage();
  const ObIOUsageInfoArray &get_io_usage() { return info_; }
  ObIOFailedReqInfoArray &get_failed_req_usage() { return failed_req_info_; }
  ObIOGroupThrottledTimeArray &get_group_throttled_time_us() { return group_throttled_time_us_; }
  int assign(const ObIOUsage &other);
  int64_t to_string(char* buf, const int64_t buf_len) const;
private:
  int assign_unsafe(const ObIOUsage &other);
private:
  ObIOUsageInfoArray info_;
  ObIOFailedReqInfoArray failed_req_info_;
  ObIOGroupThrottledTimeArray group_throttled_time_us_;
  mutable common::ObQSyncLock lock_;
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
  ObIOTuner();
  virtual ~ObIOTuner();
  int init();
  void stop();
  void wait();
  void destroy();
  int send_sn_detect_task();
  int send_ss_detect_task();
  virtual void run1() override;
private:
  int try_release_thread();
private:
  bool is_inited_;
  ObCpuUsage cpu_usage_;
};

struct ObIOGroupQueues final {
public:
  ObIOGroupQueues(ObIAllocator &allocator);
  ~ObIOGroupQueues();
  int init(const int64_t group_num);
  void destroy();
  TO_STRING_KV(K(is_inited_), K(group_phy_queues_));
public:
  bool is_inited_;
  ObIAllocator &allocator_;
  ObSEArray<ObPhyQueue *, GROUP_START_NUM> group_phy_queues_;
};


struct ObSenderInfo final
{
public:
  ObSenderInfo();
  ~ObSenderInfo();
public:
  int64_t queuing_count_;
  int64_t reservation_ts_;
  int64_t limitation_ts_;
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
  int get_sender_status(const uint64_t tenant_id, const uint64_t index, ObSenderInfo &sender_info);
  int inc_queue_count(const ObIORequest &req);
  int dec_queue_count(const ObIORequest &req);
  TO_STRING_KV(K(is_inited_), K(stop_submit_), K(is_retry_sender_), KPC(io_queue_), K(tg_id_), K(sender_index_));
//private:
  void pop_and_submit();
  int64_t calc_wait_timeout(const int64_t queue_deadline);
  int submit(ObIORequest &req);
  int64_t sender_req_count_;
  int64_t sender_req_local_r_count_;
  int64_t sender_req_local_w_count_;
  int64_t sender_req_remote_r_count_;
  int64_t sender_req_remote_w_count_;
  int64_t sender_index_;
  int tg_id_; // thread group id
  bool is_inited_;
  bool stop_submit_;
  bool is_retry_sender_;
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
  int init(const int64_t queue_count);
  void destroy();
  int start();
  void stop();
  void wait();
  void accumulate(const ObIORequest &req);
  int schedule_request(ObIORequest &req);
  int retry_request(ObIORequest &req) { return schedule_request_(req, 0); }
  int init_group_queues(const uint64_t tenant_id, const int64_t group_num, ObIOAllocator *io_allocator);
  int update_group_queues(const uint64_t tenant_id, const int64_t group_num);
  int remove_phyqueues(const uint64_t tenant_id);
  int stop_phy_queues(const uint64_t tenant_id, const int64_t index);
  ObIOSender *get_sender(const int idx_id){ return senders_.at(idx_id); };
  int64_t get_senders_count() { return senders_.count(); }
  TO_STRING_KV(K(is_inited_), K(io_config_), K(senders_));
private:
  int schedule_request_(ObIORequest &req, const int64_t schedule_request);
private:
  static const int64_t SENDER_QUEUE_WATERLEVEL = 64;
  friend class ObIOTuner;
  bool is_inited_;
  const ObIOConfig &io_config_;
  ObIAllocator &allocator_;
  ObSEArray<ObIOSender *, 1> senders_; //the first sender is for retry_alloc_memory to avoid blocking current sender
  ObIOTuner io_tuner_;
};

class ObDeviceChannel;

/**
 * worker to process sync io request and get result of async io request from file system
 * io channel has two independent threads, one for sync io operation, another for polling events from file system
 */
class ObIOChannel
{
public:
  ObIOChannel();
  virtual ~ObIOChannel();

  int base_init(ObDeviceChannel *device_channel);
  static int convert_sys_errno(const int system_errno);
  virtual int submit(ObIORequest &req) = 0;
  virtual void cancel(ObIORequest &req) = 0;
  virtual int64_t get_queue_count() const = 0;
  TO_STRING_KV(KP(device_handle_), "queue_count", get_queue_count());

protected:
  ObIODevice *device_handle_;
  ObDeviceChannel *device_channel_;
};


class ObAsyncIOChannel : public ObIOChannel, public lib::TGRunnable
{
public:
  ObAsyncIOChannel();
  virtual ~ObAsyncIOChannel();

  int init(ObDeviceChannel *device_channel);
  int start();
  void stop();
  void wait();
  void destroy();
  virtual void run1() override;
  virtual int submit(ObIORequest &req) override;
  virtual void cancel(ObIORequest &req) override;
  virtual int64_t get_queue_count() const override;
  INHERIT_TO_STRING_KV("IOChannel", ObIOChannel, KP(io_context_), KP(io_events_), K(submit_count_));

private:
  int start_thread();
  void destroy_thread();
  void get_events();
  int on_full_return(ObIORequest &req, const int64_t complete_size);
  int on_partial_return(ObIORequest &req, const int64_t complete_size);
  int on_partial_retry(ObIORequest &req, const int64_t complete_size);
  int on_full_retry(ObIORequest &req);
  int on_failed(ObIORequest &req, const ObIORetCode &ret_code);

private:
  static const int32_t MAX_AIO_EVENT_CNT = 512;
  static const int32_t MAX_DETECT_DISK_HUNG_IO_CNT = 10;
  static const int64_t AIO_POLLING_TIMEOUT_NS = 1000L * 1000L * 1000L - 1L; // almost 1s, for timespec_valid check
private:
  bool is_inited_;
  int tg_id_; // thread group id
  ObIOContext *io_context_;
  ObIOEvents *io_events_;
  struct timespec polling_timeout_;
  int64_t submit_count_;
  ObThreadCond depth_cond_;
};

class ObSyncIOChannel : public ObIOChannel, public common::ObSimpleThreadPool
{
public:
  ObSyncIOChannel();
  virtual ~ObSyncIOChannel();
  int init(ObDeviceChannel *device_channel, const int64_t thread_num);
  void destroy();
  virtual int submit(ObIORequest &req) override;
  virtual void cancel(ObIORequest &req) override;
  virtual int64_t get_queue_count() const override;
  int set_thread_count(const int64_t conf_thread_count);
  INHERIT_TO_STRING_KV("IOChannel", ObIOChannel, K(get_queue_num()));
private:
  virtual void handle(void *task) override;
  int start_thread(const int64_t thread_num, const int64_t task_num);
  void destroy_thread();
  int do_sync_io(ObIORequest &req);
private:
  static const int64_t SYNC_IO_TASK_COUNT = 1024;
  static int64_t cal_thread_count(const int64_t conf_thread_count);
private:
  bool is_inited_;
  static const int64_t MAX_SYNC_IO_QUEUE_COUNT = 512;
  ObFixedQueue<ObIORequest> req_queue_;
  ObThreadCond cond_;
  int64_t used_io_depth_;
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
  void print_status();
  int reload_config(const ObIOConfig &conf);
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
  ObIODevice *device_handle_; //local device_handle
  int64_t used_io_depth_;
  int64_t max_io_depth_;
  ObSpinLock lock_for_sync_io_;
};

class ObIORunner : public lib::TGRunnable
{
public:
  ObIORunner();
  virtual ~ObIORunner();
  int init(const int64_t queue_capacity, ObIAllocator &allocator, const int64_t idx);
  void stop();
  void wait();
  void destroy();
  virtual void run1() override;
  void stop_accept_req() { stop_accept_ = true; }
  void reuse_runner() { stop_accept_ = false; }
  bool is_stop_accept() { return stop_accept_; }
  int push(ObIORequest &req);
  int pop(ObIORequest *&req);
  int handle(ObIORequest *req);
  int64_t get_queue_count();

  int64_t get_tid() const { return tid_; }

  TO_STRING_KV(K(is_inited_), K(queue_.get_total()), K(tg_id_), K(idx_), K(tid_));
private:
  static const int64_t CALLBACK_WAIT_PERIOD_US = 1000L * 1000L; // 1s
  bool is_inited_;
  bool stop_accept_;
  int tg_id_;
  ObThreadCond cond_;
  ObFixedQueue<ObIORequest> queue_;
  int64_t idx_;
  int64_t tid_;
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
  void try_release_thread();
  int update_thread_count(const int64_t thread_count);
  int64_t to_string(char *buf, const int64_t len) const;
private:
  int64_t get_callback_queue_idx_(const ObIOCallbackType cb_type) const;
private:
  static const int64_t ATOMIC_WRITE_CALLBACK_THREAD_RATIO = 2;
private:
  bool is_inited_;
  int32_t queue_depth_;
  int64_t config_thread_count_;
  mutable common::DRWLock lock_;
  ObArray<ObIORunner *> runners_;
  ObIOAllocator *io_allocator_;
};

struct ObIOGroupMemInfo
{
public:
  ObIOGroupMemInfo() : total_cnt_(0), total_size_(0)
  {}
  int inc(const int size)
  {
    ATOMIC_INC(&total_cnt_);
    ATOMIC_AAF(&total_size_, size);
    return OB_SUCCESS;
  }
  int dec(const int size)
  {
    ATOMIC_DEC(&total_cnt_);
    ATOMIC_AAF(&total_size_, -size);
    return OB_SUCCESS;
  }
  int64_t total_cnt_;
  int64_t total_size_;
  TO_STRING_KV(K(total_cnt_), K(total_size_));
};

struct ObIOMemStat
{
  ObIOMemStat() : group_mem_infos_()
  {}
  ~ObIOMemStat()
  {}
  int init(const int64_t group_num);
  int refresh_group_num(const int64_t group_num);
  int inc(const ObIORequest &req);
  int dec(const ObIORequest &req);
  TO_STRING_KV(K(group_mem_infos_));
  ObSEArray<ObIOGroupMemInfo, GROUP_START_NUM> group_mem_infos_;
};

class ObIOMemStats
{
public:
  ObIOMemStats() : sys_mem_stat_(), mem_stat_()
  {}
  ~ObIOMemStats()
  {}
  int init(const int64_t sys_group_num, const int64_t group_num);
  int inc(const ObIORequest &req);
  int dec(const ObIORequest &req);
  ObIOMemStat &get_sys_mem_stat() { return sys_mem_stat_; }
  ObIOMemStat &get_mem_stat() { return mem_stat_; }
  TO_STRING_KV(K(sys_mem_stat_), K(mem_stat_));
private:
  ObIOMemStat sys_mem_stat_;
  ObIOMemStat mem_stat_;
};
struct ObIOFuncUsageByMode : ObIOGroupUsage
{
};
typedef ObSEArray<ObIOFuncUsageByMode, static_cast<uint8_t>(ObIOGroupMode::MODECNT)> ObIOFuncUsage;
typedef ObSEArray<ObIOFuncUsage, static_cast<uint8_t>(share::ObFunctionType::MAX_FUNCTION_NUM)> ObIOFuncUsageArr;
struct ObIOFuncUsages
{
public:
  ObIOFuncUsages();
  ~ObIOFuncUsages() = default;
  int init(const uint64_t tenant_id);
  int accumulate(ObIORequest &req);
  TO_STRING_KV(K(func_usages_));
  ObIOFuncUsageArr func_usages_;
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
  void stop();
  void wait();
  void destroy();
  int start();
  virtual void handle(void *task) override;
  int get_device_health_status(ObDeviceHealthStatus &dhs, int64_t &device_abnormal_time);
  void reset_device_health();
  void record_io_error(const ObIOResult &result, const ObIORequest &req);
  void record_io_timeout(const ObIOResult &result, const ObIORequest &req);
  int record_timing_task(const int64_t first_id, const int64_t second_id);

private:
  int record_read_failure_(const ObIOResult &result, const ObIORequest &req);
  int record_write_failure();
  void set_device_warning();
  void set_device_error();
  int set_detect_task_io_info_(ObIOInfo &io_info, const ObIOResult &result, const ObIORequest &req);
  // If executes the detect task in SS mode, checking if it's a read operation on the micro cache file.
  // In SN mode, always returns true.
  // In SS mode, returns true if fd == micro cache file fd.
  bool is_supported_detect_read_(const uint64_t tenant_id, const ObIOFd &fd);

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
  int64_t to_string(char *buf, const int64_t len) const;
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

inline bool is_io_aligned(const int64_t value, const int64_t align)
{
  return (align > 0) && (0 == (value % align));
}

inline void align_offset_size(
    const int64_t offset,
    const int64_t size,
    const int64_t align,
    int64_t &align_offset,
    int64_t &align_size)
{
  align_offset = lower_align(offset, align);
  align_size = upper_align(size + offset - align_offset, align);
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


}// end namespace common
}// end namespace oceanbase

#endif//OCEANBASE_LIB_STORAGE_OB_IO_STRUCT_H
