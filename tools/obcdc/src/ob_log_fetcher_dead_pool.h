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

#ifndef OCEANBASE_LIBOBLOG_OB_LOG_FETCHER_DEAD_POOL_H__
#define OCEANBASE_LIBOBLOG_OB_LOG_FETCHER_DEAD_POOL_H__

#include "lib/utility/ob_macro_utils.h"   // DISALLOW_COPY_AND_ASSIGN

#include "ob_log_config.h"                // ObLogConfig
#include "ob_map_queue_thread.h"          // ObMapQueueThread
#include "ob_log_part_fetch_ctx.h"        // FetchTaskList, PartFetchCtx

namespace oceanbase
{
namespace liboblog
{

class IObLogFetcherDeadPool
{
public:
  static const int64_t MAX_THREAD_NUM = ObLogConfig::max_dead_pool_thread_num;

public:
  virtual ~IObLogFetcherDeadPool() {}

public:
  virtual int push(PartFetchCtx *task) = 0;
  virtual int start() = 0;
  virtual void stop() = 0;
  virtual void mark_stop_flag() = 0;
};

/////////////////////////////////////////////////////////////////

typedef common::ObMapQueueThread<IObLogFetcherDeadPool::MAX_THREAD_NUM> DeadPoolThread;

class IObLogErrHandler;
class IObLogPartFetchMgr;

class ObLogFetcherDeadPool : public IObLogFetcherDeadPool, public DeadPoolThread
{
  static const int64_t IDLE_WAIT_TIME = 100 * 1000;

public:
  ObLogFetcherDeadPool();
  virtual ~ObLogFetcherDeadPool();

public:
  int init(const int64_t thread_num,
      IObLogPartFetchMgr &part_fetch_mgr,
      IObLogErrHandler &err_handler);
  void destroy();

public:
  // Implement the IObLogFetcherDeadPool virtual function
  virtual int push(PartFetchCtx *task);
  virtual int start();
  virtual void stop();
  virtual void mark_stop_flag();

public:
  // Implement the ObMapQueueThread virtual function
  // Overloading thread handling functions
  virtual void run(const int64_t thread_index);

private:
  void reset_task_list_array_();
  int retrieve_task_list_(const int64_t thread_index, FetchTaskList &list);
  int handle_task_list_(const int64_t thread_index, FetchTaskList &list);

private:
  bool                      inited_;
  IObLogErrHandler          *err_handler_;
  IObLogPartFetchMgr        *part_fetch_mgr_;

  // One task array per thread
  FetchTaskList             task_list_array_[MAX_THREAD_NUM];

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogFetcherDeadPool);
};


}
}

#endif
