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

#ifndef OCEANBASE_LOGSERVIVE_LOG_IO_WORKER_
#define OCEANBASE_LOGSERVIVE_LOG_IO_WORKER_

#include <stdint.h>
#include "lib/queue/ob_lighty_queue.h"              // ObLightyQueue
#include "lib/utility/ob_macro_utils.h"             // DISALLOW_COPY_AND_ASSIGN
#include "lib/utility/ob_print_utils.h"             // TO_STRING_KV
#include "lib/lock/ob_spin_lock.h"                  // SpinLock
#include "lib/thread/thread_mgr_interface.h"        // TGTaskHandler
#include "lib/container/ob_fixed_array.h"           // ObSEArrayy
#include "lib/hash/ob_array_hash_map.h"             // ObArrayHashMap
#include "share/ob_thread_pool.h"                   // ObThreadPool
#include "common/ob_clock_generator.h"              //ObClockGenerator
#include "log_io_task.h"                            // LogBatchIOFlushLogTask
#include "log_define.h"                             // ALF_SLIDING_WINDOW_SIZE
#include "palf_options.h"                           // PalfThrottleOptions
namespace oceanbase
{
namespace common
{
class ObIAllocator;
}
namespace palf
{
class LogIOTask;
class IPalfEnvImpl;

struct LogIOWorkerConfig
{
  LogIOWorkerConfig()
  {
    reset();
  }
  ~LogIOWorkerConfig()
  {
    reset();
  }
  bool is_valid() const
  {
    return 0 < io_worker_num_ && 0 < io_queue_capcity_ && 0 <= batch_width_ && 0 <= batch_depth_;
  }
  void reset()
  {
    io_worker_num_ = 0;
    io_queue_capcity_ = 0;
    batch_width_ = 0;
    batch_depth_ = 0;
  }
  int64_t io_worker_num_;
  int64_t io_queue_capcity_;
  int64_t batch_width_;
  int64_t batch_depth_;
  TO_STRING_KV(K_(io_worker_num), K_(io_queue_capcity), K_(batch_width), K_(batch_depth));
};
class LogThrottlingStat
{
public:
  LogThrottlingStat() {reset();}
  ~LogThrottlingStat() {reset();}
  void reset();
  inline bool has_ever_throttled() const;
  inline void after_throttling(const int64_t throttling_interval, const int64_t throttling_size);
  inline void start_throttling();
  inline void stop_throttling();
  TO_STRING_KV(K_(start_ts),
               K_(stop_ts),
               K_(total_throttling_interval),
               K_(total_throttling_size),
               K_(total_throttling_task_cnt),
               K_(total_skipped_size),
               K_(total_skipped_task_cnt),
               K_(max_throttling_interval));
private:
  int64_t start_ts_;
  int64_t stop_ts_;
  int64_t total_throttling_interval_;
  int64_t total_throttling_size_;
  int64_t total_throttling_task_cnt_;

//log_size of tasks need for throttling but overlooked
  int64_t total_skipped_size_;
//count of tasks need for throttling but overlooked
  int64_t total_skipped_task_cnt_;
  int64_t max_throttling_interval_;
};

inline void LogThrottlingStat::after_throttling(const int64_t throttling_interval,
                                                const int64_t throttling_size)
{
  if (0 == throttling_interval) {
    total_skipped_size_ += throttling_size;
    total_skipped_task_cnt_++;
  } else {
    total_throttling_interval_ += throttling_interval;
    total_throttling_size_ += throttling_size;
    total_throttling_task_cnt_++;
    max_throttling_interval_ = MAX(max_throttling_interval_, throttling_interval);
  }
}

inline bool LogThrottlingStat::has_ever_throttled() const
{
  return common::OB_INVALID_TIMESTAMP != start_ts_;
}
inline void LogThrottlingStat::start_throttling()
{
  reset();
  start_ts_ = common::ObClockGenerator::getClock();
}
inline void LogThrottlingStat::stop_throttling()
{
  stop_ts_ = common::ObClockGenerator::getClock();
}

class LogWritingThrottle
{
public:
  LogWritingThrottle() {reset();}
  ~LogWritingThrottle() {reset();}
  void reset();
  //invoked by gc thread
  inline void notify_need_writing_throttling(const bool is_need);
  inline bool need_writing_throttling_notified() const;
  inline int64_t inc_and_fetch_submitted_seq();
  inline void dec_submitted_seq();
  int update_throttling_options(IPalfEnvImpl *palf_env_impl);
  int throttling(const int64_t io_size, IPalfEnvImpl *palf_env_impl);
  int after_append_log(const int64_t log_size, const int64_t seq);
  TO_STRING_KV(K_(last_update_ts),
               K_(need_writing_throttling_notified),
               K_(submitted_seq),
               K_(handled_seq),
               K_(appended_log_size_cur_round),
               K_(decay_factor),
               K_(throttling_options),
               K_(stat));

private:
  int update_throtting_options_(IPalfEnvImpl *palf_env_impl, bool &has_recycled_log_disk);
  inline bool need_throttling_with_options_() const;
  inline bool need_throttling_() const;
  //reset throttling related member
  void clean_up_();
private:
  static const int64_t UPDATE_INTERVAL_US = 500 * 1000L;//500ms
  const int64_t DETECT_INTERVAL_US = 30 * 1000L;//1ms
  const int64_t THROTTLING_DURATION_US = 1800 * 1000 * 1000L;//1800s
  const int64_t THROTTLING_CHUNK_SIZE = MAX_LOG_BUFFER_SIZE;
  //ts of lastest updating writing throttling info
  int64_t last_update_ts_;
  //ts when next log can be appended
  //log_size can be appended during current round, will be reset when unrecyclable_size changed
  // notified by gc, local meta may not be ready
  mutable bool need_writing_throttling_notified_;
  // local meta is ready after need_writing_throttling_locally_ set true
  //used for meta flush task
  //max_seq of task ever submitted
  mutable int64_t submitted_seq_;
  //max_seq of task ever handled
  mutable int64_t handled_seq_;
  int64_t appended_log_size_cur_round_;
  double decay_factor_;
  //append_speed during current round, Bytes per usecond
  PalfThrottleOptions throttling_options_;
  LogThrottlingStat stat_;
};

inline void LogWritingThrottle::notify_need_writing_throttling(const bool is_need) {
  ATOMIC_SET(&need_writing_throttling_notified_, is_need);
}

inline bool LogWritingThrottle::need_writing_throttling_notified() const {
  return ATOMIC_LOAD(&need_writing_throttling_notified_);
}

inline bool LogWritingThrottle::need_throttling_with_options_() const
{
  return ATOMIC_LOAD(&need_writing_throttling_notified_) && throttling_options_.need_throttling();
}
inline bool LogWritingThrottle::need_throttling_() const
{
  return need_throttling_with_options_() && ATOMIC_LOAD(&handled_seq_) >= ATOMIC_LOAD(&submitted_seq_);
}

int64_t LogWritingThrottle::inc_and_fetch_submitted_seq()
{
  return ATOMIC_AAF(&submitted_seq_, 1);
}

void LogWritingThrottle::dec_submitted_seq()
{
  ATOMIC_DEC(&submitted_seq_);
}

class LogIOWorker : public share::ObThreadPool
{
public:
  LogIOWorker();
  ~LogIOWorker();
  int init(const LogIOWorkerConfig &config,
           const int64_t tenant_id,
           int cb_thread_pool_tg_id,
           ObIAllocator *allocaotr,
           IPalfEnvImpl *palf_env_impl);
  void destroy();

  void run1() override final;
  int submit_io_task(LogIOTask *io_task);
  int64_t get_last_working_time() const { return ATOMIC_LOAD(&last_working_time_); }

 int notify_need_writing_throttling(const bool &need_throtting);
  static constexpr int64_t MAX_THREAD_NUM = 1;
  TO_STRING_KV(K_(log_io_worker_num), K_(cb_thread_pool_tg_id));
private:
  bool need_reduce_(LogIOTask *task);
  int reduce_io_task_(void *task);
  int handle_io_task_(LogIOTask *io_task);
  int handle_io_task_with_throttling_(LogIOTask *io_task);
  int update_throttling_options_();
  int run_loop_();
private:
  static constexpr int64_t QUEUE_WAIT_TIME = 100 * 1000;
private:
  typedef common::ObSpinLock SpinLock;
  typedef common::ObSpinLockGuard SpinLockGuard;

  class BatchLogIOFlushLogTaskMgr {
  public:
    BatchLogIOFlushLogTaskMgr();
    ~BatchLogIOFlushLogTaskMgr();
    int init(int64_t batch_width, int64_t batch_depth, ObIAllocator *allocator);
    void destroy();
    int insert(LogIOFlushLogTask *io_task);
    int handle(const int64_t tg_id, IPalfEnvImpl *palf_env_impl);
    bool empty();
    TO_STRING_KV(K_(batch_io_task_array), K_(usable_count), K_(batch_width));
  private:
    int find_usable_batch_io_task_(const int64_t palf_id, BatchLogIOFlushLogTask *&batch_io_task);
  private:
    typedef ObFixedArray<BatchLogIOFlushLogTask *, common::ObIAllocator> BatchLogIOFlushLogTaskArray;
    BatchLogIOFlushLogTaskArray batch_io_task_array_;
    int64_t handle_count_;
    int64_t has_batched_size_;
    int64_t usable_count_;
    int64_t batch_width_;
  };

  // TODO: io_task_queue used to store all LogIOTask objects, and the LogIOWorker
  //       will consume it, at nowdays, the io_task_queue is single consumer and mutil
  //       producers model.

  // NB: at nowdays, the default 'log_io_worker_num_' is 1.
  int64_t log_io_worker_num_;
  int cb_thread_pool_tg_id_;
  IPalfEnvImpl *palf_env_impl_;
  ObLightyQueue queue_;
  BatchLogIOFlushLogTaskMgr batch_io_task_mgr_;
  int64_t do_task_used_ts_;
  int64_t do_task_count_;
  int64_t print_log_interval_;
  int64_t last_working_time_;
  LogWritingThrottle throttle_;
  SpinLock throttling_lock_;
  ObMiniStat::ObStatItem log_io_worker_queue_size_stat_;
  bool is_inited_;
};
} // end namespace palf
} // end namespace oceanbase

#endif
