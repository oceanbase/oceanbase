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
 * Fetcher DEAD Pool: For processing fetch log tasks that are in the process of being deleted
 */

#ifndef OCEANBASE_LOG_FETCHER_DEAD_POOL_H__
#define OCEANBASE_LOG_FETCHER_DEAD_POOL_H__

#include "lib/utility/ob_macro_utils.h"   // DISALLOW_COPY_AND_ASSIGN
#include "lib/thread/thread_mgr_interface.h" // TGTaskHandler

#include "ob_log_config.h"                // ObLogFetcherConfig
#include "ob_log_ls_fetch_ctx.h"          // FetchTaskList, LSFetchCtx

namespace oceanbase
{
namespace logfetcher
{

class IObLogFetcherDeadPool
{
public:
  static const int64_t MAX_THREAD_NUM = ObLogFetcherConfig::max_dead_pool_thread_num;

public:
  virtual ~IObLogFetcherDeadPool() {}

public:
  virtual int push(LSFetchCtx *task) = 0;
  virtual int start() = 0;
  virtual void stop() = 0;
  virtual void mark_stop_flag() = 0;
};

/////////////////////////////////////////////////////////////////

class IObLogErrHandler;
class IObLogLSFetchMgr;

class ObLogFetcherDeadPool : public IObLogFetcherDeadPool, public lib::TGTaskHandler
{
public:
  ObLogFetcherDeadPool();
  virtual ~ObLogFetcherDeadPool();

public:
  int init(const int64_t thread_num,
      void *fetcher_host_host,
      IObLogLSFetchMgr &ls_fetch_mgr,
      IObLogErrHandler &err_handler);
  void destroy();

public:
  // Implement the IObLogFetcherDeadPool virtual function
  virtual int push(LSFetchCtx *task);
  virtual int start();
  virtual void stop();
  virtual void mark_stop_flag();

public:
  // Overloading thread handling functions
  virtual void handle(void *data) {}
  // Overloading thread handling functions
  virtual void handle(void *data, volatile bool &stop_flag) override;

private:
  static const int64_t IDLE_WAIT_TIME = 100 * 1000;
  int handle_task_list_(
      const int64_t thread_index,
      LSFetchCtx &ls_fetch_ctx,
      volatile bool &stop_flag);

private:
  bool                      inited_;
  int                       tg_id_;
  void                      *fetcher_host_;
  IObLogErrHandler          *err_handler_;
  IObLogLSFetchMgr          *ls_fetch_mgr_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogFetcherDeadPool);
};


}
}

#endif
