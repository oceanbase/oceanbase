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

#ifndef OCEANBASE_LOGSERVIVE_LOG_THROTTLE_H
#define OCEANBASE_LOGSERVIVE_LOG_THROTTLE_H

#include "lib/utility/ob_macro_utils.h"             // DISALLOW_COPY_AND_ASSIGN
#include "lib/utility/ob_print_utils.h"             // TO_STRING_KV
#include "lib/lock/ob_spin_lock.h"                  // SpinLock
#include "common/ob_clock_generator.h"              // ObClockGenerator
#include "lib/function/ob_function.h"               // ObFunction
#include "log_define.h"                             // MAX_LOG_BUFFER_SIZE
#include "palf_options.h"                           // PalfThrottleOptions

namespace oceanbase
{
namespace palf
{
class IPalfEnvImpl;
typedef ObFunction<bool()> NeedPurgingThrottlingFunc;
class LogWritingThrottle;

// 租户级异步写限流上下文. 三个成员都由 LogIOWorkerWrapper 持有,
// AsyncPalfIOCtx 只在自身生命周期内借用; throttle 为空表示无需限流.
struct AsyncThrottleContext
{
  AsyncThrottleContext() : throttle(NULL), purge_func(NULL), purge_task_count(NULL) {}
  AsyncThrottleContext(LogWritingThrottle *throttle,
                       const NeedPurgingThrottlingFunc *purge_func,
                       int64_t *purge_task_count)
    : throttle(throttle), purge_func(purge_func), purge_task_count(purge_task_count) {}
  LogWritingThrottle *throttle;
  const NeedPurgingThrottlingFunc *purge_func;
  int64_t *purge_task_count;
  TO_STRING_KV(KP(throttle), KP(purge_func), KP(purge_task_count));
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

// Bytes admitted without sleeping, including purge bypass and the first async
// deferred-admission decision.
  int64_t total_skipped_size_;
// Number of decisions accounted in total_skipped_size_.
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
  LogWritingThrottle():lock_(common::ObLatchIds::OB_LOG_WRITER_THROTTLE_LOCK) {reset();}
  ~LogWritingThrottle() {reset();}
  void reset();
  //invoked by gc thread
  inline void notify_need_writing_throttling(const bool is_need);
  inline bool need_writing_throttling_notified() const;
  int update_throttling_options(IPalfEnvImpl *palf_env_impl);
  int throttling(const int64_t io_size,
                 const NeedPurgingThrottlingFunc &need_purging_throttling_func,
                 IPalfEnvImpl *palf_env_impl);
  int after_append_log(const int64_t log_size);
  // 异步写 admission 不在 worker 上 sleep. 本接口复用同步路径的限流计算,
  // 通过 can_admit=false 和 delay_us 返回下一次尝试时间, 错误原样返回调用方.
  int try_admit_async(const int64_t logical_bytes,
                      const NeedPurgingThrottlingFunc &need_purging_throttling_func,
                      IPalfEnvImpl *palf_env_impl,
                      bool &can_admit,
                      int64_t &delay_us);
  // deadline 到期前重新探测限流状态, 但不重复统计 skipped task;
  // 探测过程中仍会刷新共享限流参数及启停状态.
  int probe_admit_async(const int64_t logical_bytes,
                        const NeedPurgingThrottlingFunc &need_purging_throttling_func,
                        IPalfEnvImpl *palf_env_impl,
                        bool &can_admit,
                        int64_t &delay_us);
  TO_STRING_KV(K_(last_update_ts),
               K_(need_writing_throttling_notified),
               K_(appended_log_size_cur_round),
               K_(decay_factor),
               K_(throttling_options),
               K_(stat));

private:
  int update_throtting_options_guarded_by_lock_(IPalfEnvImpl *palf_env_impl,
                                                const bool force_update,
                                                bool &has_recycled_log_disk);
  inline bool need_throttling_with_options_not_guarded_by_lock_() const;
  inline bool need_throttling_not_guarded_by_lock_(const NeedPurgingThrottlingFunc &need_purge_throttling) const;
  int decide_throttle_(const int64_t io_size,
                       const NeedPurgingThrottlingFunc &need_purging_throttling_func,
                       IPalfEnvImpl *palf_env_impl,
                       bool &need_throttle,
                       int64_t &wait_us,
                       const bool need_account_skipped_task);
  int admit_async_(const int64_t logical_bytes,
                   const NeedPurgingThrottlingFunc &need_purging_throttling_func,
                   IPalfEnvImpl *palf_env_impl,
                   const bool need_account_skipped_task,
                   bool &can_admit,
                   int64_t &delay_us);
  // 同步 sleep 与异步 admission 共用的限流计算. 调用方必须持有 lock_;
  // 仅当 need_throttle=true 时 interval_us 才有效.
  int calc_throttling_interval_guarded_by_lock_(
      const int64_t io_size,
      const NeedPurgingThrottlingFunc &need_purging_throttling_func,
      bool &need_throttle,
      int64_t &interval_us);
  //reset throttling related member
  void clean_up_not_guarded_by_lock_();
private:
  typedef common::ObSpinLock SpinLock;
  typedef common::ObSpinLockGuard SpinLockGuard;
  static const int64_t UPDATE_INTERVAL_US = 500 * 1000L;//500ms
  const int64_t DETECT_INTERVAL_US = 30 * 1000L;//30ms
  const int64_t THROTTLING_CHUNK_SIZE = MAX_LOG_BUFFER_SIZE;
  // lock_ 串行保护限流参数、追加量和统计；GC 通知标记由原子操作更新，计算完整
  // 限流决定时再在 lock_ 内与 throttling_options_ 一起读取。
  int64_t last_update_ts_;
  mutable bool need_writing_throttling_notified_;
  // Log size appended in the current throttle round, reset when unrecyclable
  // disk space changes.
  int64_t appended_log_size_cur_round_;
  double decay_factor_;
  PalfThrottleOptions throttling_options_;
  LogThrottlingStat stat_;
  mutable SpinLock lock_;
};

inline void LogWritingThrottle::notify_need_writing_throttling(const bool is_need) {
  ATOMIC_SET(&need_writing_throttling_notified_, is_need);
}

inline bool LogWritingThrottle::need_writing_throttling_notified() const {
  return ATOMIC_LOAD(&need_writing_throttling_notified_);
}

inline bool LogWritingThrottle::need_throttling_with_options_not_guarded_by_lock_() const
{
  return ATOMIC_LOAD(&need_writing_throttling_notified_) && throttling_options_.need_throttling();
}

inline bool LogWritingThrottle::need_throttling_not_guarded_by_lock_(
  const NeedPurgingThrottlingFunc &need_purge_throttling) const
{
  // Only need throttle under following conditions:
  // 1. when the tenant writing throttling has been enabled and has been triggered;
  // 2. when there is no need to purge throttle.
  return need_throttling_with_options_not_guarded_by_lock_() && !need_purge_throttling();
}

} // end namespace palf
} // end namespace oceanbase
#endif
