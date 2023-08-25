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
#include "lib/thread/thread_mgr_interface.h"        // TGTaskHandler
#include "lib/container/ob_fixed_array.h"           // ObSEArrayy
#include "lib/hash/ob_array_hash_map.h"             // ObArrayHashMap
#include "lib/atomic/ob_atomic.h"                   // ATOMIC_LOAD
#include "lib/function/ob_function.h"               // ObFunction
#include "share/ob_thread_pool.h"                   // ObThreadPool
#include "common/ob_clock_generator.h"              // ObClockGenerator
#include "log_io_task.h"                            // LogBatchIOFlushLogTask
#include "log_define.h"                             // PALF_SLIDING_WINDOW_SIZE
#include "palf_options.h"                           // PalfThrottleOptions
#include "log_throttle.h"                           // LogWritingThrottle
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

class LogIOWorker : public share::ObThreadPool
{
public:
  LogIOWorker();
  ~LogIOWorker();
  int init(const LogIOWorkerConfig &config,
           const int64_t tenant_id,
           const int cb_thread_pool_tg_id,
           ObIAllocator *allocaotr,
           LogWritingThrottle *throttle,
           const bool need_ignore_throttle,
           IPalfEnvImpl *palf_env_impl);
  void destroy();

  void run1() override final;
  int submit_io_task(LogIOTask *io_task);
  int64_t get_last_working_time() const { return ATOMIC_LOAD(&last_working_time_); }

 int notify_need_writing_throttling(const bool &need_throtting);
  static constexpr int64_t MAX_THREAD_NUM = 1;
  TO_STRING_KV(K_(log_io_worker_num), K_(cb_thread_pool_tg_id), K_(purge_throttling_task_handled_seq), K_(purge_throttling_task_submitted_seq));
private:
  bool need_reduce_(LogIOTask *task);
  int reduce_io_task_(void *task);
  int handle_io_task_(LogIOTask *io_task);
  int handle_io_task_with_throttling_(LogIOTask *io_task);
  int update_throttling_options_();
  int run_loop_();
  int64_t inc_and_fetch_purge_throttling_submitted_seq_();
  void dec_purge_throttling_submitted_seq_();
  bool has_purge_throttling_tasks_() const;
private:
  static constexpr int64_t QUEUE_WAIT_TIME = 100 * 1000;
private:

  class BatchLogIOFlushLogTaskMgr {
  public:
    BatchLogIOFlushLogTaskMgr();
    ~BatchLogIOFlushLogTaskMgr();
    int init(int64_t batch_width, int64_t batch_depth, ObIAllocator *allocator, ObMiniStat::ObStatItem *wait_cost_stat);
    void destroy();
    int insert(LogIOFlushLogTask *io_task);
    int handle(const int64_t tg_id, IPalfEnvImpl *palf_env_impl);
    bool empty();
    TO_STRING_KV(K_(batch_io_task_array), K_(usable_count), K_(batch_width));
  private:
    int find_usable_batch_io_task_(const int64_t palf_id, BatchLogIOFlushLogTask *&batch_io_task);
    int statistics_wait_cost_(int64_t first_handle_time, BatchLogIOFlushLogTask *batch_io_task);
  private:
    typedef ObFixedArray<BatchLogIOFlushLogTask *, common::ObIAllocator> BatchLogIOFlushLogTaskArray;
    BatchLogIOFlushLogTaskArray batch_io_task_array_;
    int64_t handle_count_;
    int64_t has_batched_size_;
    int64_t usable_count_;
    int64_t batch_width_;
    ObMiniStat::ObStatItem *wait_cost_stat_;
  };
  typedef common::ObSpinLock SpinLock;
  typedef common::ObSpinLockGuard SpinLockGuard;

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
  LogWritingThrottle *throttle_;
  ObMiniStat::ObStatItem log_io_worker_queue_size_stat_;
  // Each LogIOTask except LogIOFlushLogTask hold a unique sequence, when 'purge_throttling_task_submitted_seq_' minus
  // 'purge_throttling_task_handled_seq_' is greater than zero, purge throttling.
  // 1. Only incrementing 'purge_throttling_submitted_seq_' when submit LogIOTask which need purge throttling.
  // 2. Set 'purge_throttling_task_handled_seq_' to 'purge_throttling_task_submitted_seq_' after handle the LogIOTask successfully.
  mutable int64_t purge_throttling_task_submitted_seq_;
  mutable int64_t purge_throttling_task_handled_seq_;
  // ignoring throttline whatever(ie: for sys log stream, no need throttling)
  bool need_ignoring_throttling_;
  NeedPurgingThrottlingFunc need_purging_throttling_func_;
  SpinLock lock_;
  ObMiniStat::ObStatItem wait_cost_stat_;
  bool is_inited_;
};
} // end namespace palf
} // end namespace oceanbase

#endif
