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

#pragma once

#include "lib/lock/ob_mutex.h"
#include "lib/lock/ob_thread_cond.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/queue/ob_lighty_queue.h"
#include "share/ob_thread_pool.h"
#include "lib/allocator/page_arena.h"
#include "lib/oblog/ob_warning_buffer.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadTask;

class ObITableLoadTaskScheduler
{
public:
  ObITableLoadTaskScheduler() = default;
  virtual ~ObITableLoadTaskScheduler() = default;
  virtual int init() = 0;
  virtual int start() = 0;
  virtual void stop() = 0;
  virtual void wait() = 0;
  virtual int add_task(int64_t thread_idx, ObTableLoadTask *task) = 0;
  virtual int64_t get_thread_count() const = 0;
  virtual bool is_stopped() const = 0;
private:
  DISALLOW_COPY_AND_ASSIGN(ObITableLoadTaskScheduler);
};

class ObTableLoadTaskThreadPoolScheduler final : public ObITableLoadTaskScheduler
{
  static const int64_t DEFAULT_TIMEOUT_US = 10LL * 1000 * 1000; // 10s
  // 运行状态
  static const int STATE_ZERO = 0;
  static const int STATE_STARTING = 1;
  static const int STATE_RUNNING = 2;
  static const int STATE_STOPPING = 3;
  static const int STATE_STOPPED = 4;
  static const int STATE_STOPPED_NO_WAIT = 5;
public:
  ObTableLoadTaskThreadPoolScheduler(int64_t thread_count, uint64_t table_id, const char *label,
                                     int64_t session_queue_size = 64);
  virtual ~ObTableLoadTaskThreadPoolScheduler();
  int init() override;
  int start() override;
  void stop() override;
  void wait() override;
  int add_task(int64_t thread_idx, ObTableLoadTask *task) override;
  int64_t get_thread_count() const override { return thread_count_; }
  bool is_stopped() const override
  {
    return state_ == STATE_STOPPED || state_ == STATE_STOPPED_NO_WAIT;
  }
private:
  void run(uint64_t thread_idx);
  int init_worker_ctx_array();
  OB_INLINE bool is_running() const
  {
    return state_ == STATE_RUNNING;
  }
  // 启动成功才会调用
  void before_running();
  // 启动失败也可能调用
  void after_running();
  void clear_all_task();
private:
  class MyThreadPool : public share::ObThreadPool
  {
  public:
    MyThreadPool(ObTableLoadTaskThreadPoolScheduler *scheduler)
      : scheduler_(scheduler), running_thread_count_(0) {}
    virtual ~MyThreadPool() = default;
    void run1() override;
  private:
    ObTableLoadTaskThreadPoolScheduler * const scheduler_;
    int64_t running_thread_count_ CACHE_ALIGNED;
  };
  struct WorkerContext
  {
    WorkerContext() : need_signal_(false) {}
    int64_t worker_id_;
    common::ObThreadCond cond_;
    bool need_signal_;
    common::LightyQueue task_queue_; // 多线程安全
  };
  int execute_worker_tasks(WorkerContext &worker_ctx);
private:
  common::ObArenaAllocator allocator_;
  const int64_t thread_count_;
  const int64_t session_queue_size_;
  char name_[OB_THREAD_NAME_BUF_LEN];
  common::ObCurTraceId::TraceId trace_id_;
  int64_t timeout_ts_;
  MyThreadPool thread_pool_;
  WorkerContext *worker_ctx_array_;
  volatile int state_;
  bool is_inited_;
  lib::ObMutex state_mutex_;
  common::ObWarningBuffer warning_buffer_;
  lib::ObMutex wb_mutex_;
};

}  // namespace observer
}  // namespace oceanbase
