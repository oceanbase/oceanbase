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
 *
 * Fetcher IDLE Pool
 */

#ifndef OCEANBASE_LIBOBCDC_OB_LOG_FETCHER_IDLE_POOL_H__
#define OCEANBASE_LIBOBCDC_OB_LOG_FETCHER_IDLE_POOL_H__

#include "lib/utility/ob_macro_utils.h"   // DISALLOW_COPY_AND_ASSIGN

#include "ob_log_config.h"                // ObLogConfig
#include "ob_map_queue_thread.h"          // ObMapQueueThread
#include "ob_log_ls_fetch_ctx.h"          // FetchTaskList, LSFetchCtx

namespace oceanbase
{
namespace libobcdc
{

class IObLogFetcherIdlePool
{
public:
  static const int64_t MAX_THREAD_NUM = ObLogConfig::max_idle_pool_thread_num;

public:
  virtual ~IObLogFetcherIdlePool() {}

public:
  virtual int push(LSFetchCtx *task) = 0;
  virtual int start() = 0;
  virtual void stop() = 0;
  virtual void mark_stop_flag() = 0;
};

/////////////////////////////////////////////////////////////////

typedef common::ObMapQueueThread<IObLogFetcherIdlePool::MAX_THREAD_NUM> IdlePoolThread;

class IObLogErrHandler;
class IObLSWorker;
class IObLogStartLSNLocator;

class ObLogFetcherIdlePool : public IObLogFetcherIdlePool, public IdlePoolThread
{
  static const int64_t IDLE_WAIT_TIME = 100 * 1000;

public:
  ObLogFetcherIdlePool();
  virtual ~ObLogFetcherIdlePool();

public:
  int init(const int64_t thread_num,
      IObLogErrHandler &err_handler,
      IObLSWorker &stream_worker,
      IObLogStartLSNLocator &start_lsn_locator);
  void destroy();

public:
  // Implement the IObLogFetcherIdlePool virtual function
  virtual int push(LSFetchCtx *task);
  virtual int start();
  virtual void stop();
  virtual void mark_stop_flag();

public:
  // Implement the ObMapQueueThread virtual function
  // Overloading thread handling functions
  virtual void run(const int64_t thread_index);

private:
  void reset_task_list_array_();
  int do_retrieve_(const int64_t thread_index, FetchTaskList &list);
  int do_request_(const int64_t thread_index, FetchTaskList &list);
  int handle_task_(LSFetchCtx *task, bool &need_dispatch);

private:
  bool                      inited_;
  IObLogErrHandler          *err_handler_;
  IObLSWorker               *stream_worker_;
  IObLogStartLSNLocator     *start_lsn_locator_;

  // One task array per thread
  FetchTaskList             task_list_array_[MAX_THREAD_NUM];

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogFetcherIdlePool);
};


}
}

#endif
