/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 *
 */

#include "lib/allocator/page_arena.h"
#include "lib/container/ob_se_array.h"
#include "lib/task/ob_timer.h"

#ifndef OCEANBASE_LOG_FETCHER_BG_WORKER_H_
#define OCEANBASE_LOG_FETCHER_BG_WORKER_H_

namespace oceanbase
{
namespace common
{
class ObISQLClient;
}

namespace logfetcher
{

class LogFileDataBufferPool;
class FetchLogARpc;
class ObLogFetcher;


class ObLogFetcherBGTask {
public:
  ObLogFetcherBGTask() { reset(); }
  explicit ObLogFetcherBGTask(
      const int64_t interval,
      bool need_repeat,
      ObTimerTask *task):
      schedule_interval_(interval),
      need_repeat_(need_repeat),
      task_(task) {}
  int64_t get_schedule_interval() const {
    return schedule_interval_;
  }
  bool need_repeat() const {
    return need_repeat_;
  }
  ObTimerTask *get_task() {
    return task_;
  }

  void reset() {
    schedule_interval_ = -1;
    need_repeat_ = false;
    task_ = nullptr;
  }

  TO_STRING_KV(
    K(schedule_interval_),
    K(need_repeat_),
    KP(task_)
  );
private:
  int64_t schedule_interval_;
  bool need_repeat_;
  ObTimerTask *task_;
};

class GetServerVersionTask: public ObTimerTask
{
public:
  static constexpr int64_t SCHEDULE_INTERVAL = 10 * 1000 * 1000; // 10 s
  static constexpr bool NEED_REPEAT = true;
public:
  GetServerVersionTask(const uint64_t source_tenant_id,
      const int timer_id,
      ObLogFetcher &fetcher,
      ObISQLClient &sql_proxy):
      source_tenant_id_(source_tenant_id), timer_id_(timer_id), fetcher_(fetcher), sql_proxy_(sql_proxy) {}
  virtual void runTimerTask();
private:
  uint64_t source_tenant_id_;
  int timer_id_;
  ObLogFetcher &fetcher_;
  ObISQLClient &sql_proxy_;
};

class UpdateProtoTask: public ObTimerTask
{
public:
  static constexpr int64_t SCHEDULE_INTERVAL = 10L * 1000 * 1000; // 10 s
  static constexpr bool NEED_REPEAT = true;
public:
  UpdateProtoTask(ObLogFetcher &fetcher):
      fetcher_(fetcher) {}
  virtual void runTimerTask();
private:
  ObLogFetcher &fetcher_;
};

class DataBufferRecycleTask: public ObTimerTask
{
public:
  static constexpr int64_t SCHEDULE_INTERVAL = 10L * 1000 * 1000; // 10s
  static constexpr bool NEED_REPEAT = true;
public:
  DataBufferRecycleTask(LogFileDataBufferPool &pool):
      buffer_pool_(pool) {}
  virtual void runTimerTask();
private:
  LogFileDataBufferPool &buffer_pool_;
};

class ObLogFetcherBGWorker {
public:
  ObLogFetcherBGWorker();
  virtual ~ObLogFetcherBGWorker() { destroy(); }

  int init(const uint64_t tenant_id,
      const uint64_t source_tenant_id,
      ObLogFetcher &log_fetcher,
      ObISQLClient &sql_proxy,
      LogFileDataBufferPool &pool);

  int start();

  void stop();

  void wait();

  void destroy();

private:
  int init_task_list_(ObLogFetcher &log_fetcher,
      ObISQLClient &sql_proxy,
      LogFileDataBufferPool &pool);

  int schedule_task_list_();

  void run_tasks_once_();

  void destroy_task_list_();

private:
  bool is_inited_;
  int timer_id_;
  uint64_t tenant_id_;
  uint64_t source_tenant_id_;
  ObArenaAllocator alloc_;
  ObSEArray<ObLogFetcherBGTask, 4> task_list_;
};

}

}

#endif