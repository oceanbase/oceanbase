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
 * Storager
 */

#ifndef OCEANBASE_LIBOBCDC_STORAGER_H_
#define OCEANBASE_LIBOBCDC_STORAGER_H_

#include "lib/thread/ob_multi_fixed_queue_thread.h" // ObMQThread
#include "ob_log_trans_stat_mgr.h"                  // TransRpsStatInfo
#include "ob_log_store_service_stat.h"              // StoreServiceStatInfo
#include "ob_log_batch_buffer.h"                    // IObLogBatchBufTask, IObBatchBufferConsumer

namespace oceanbase
{
namespace libobcdc
{
/////////////////////////////////////////////////////////////////////////////////////////
class IObLogStorager : public IObBatchBufferConsumer
{
public:
  enum
  {
    MAX_STORAGER_NUM = 64
  };

public:
  virtual ~IObLogStorager() {}

public:
  virtual int start() = 0;
  virtual void stop() = 0;
  virtual void mark_stop_flag() = 0;
  virtual int submit(IObLogBatchBufTask *task)
  {
    UNUSED(task);
    return OB_NOT_IMPLEMENT;
  }
  virtual void get_task_count(int64_t &block_count, int64_t &log_task_count) const = 0;
};

/////////////////////////////////////////////////////////////////////////////////////////

class IObStoreService;
class IObLogErrHandler;

typedef common::ObMQThread<IObLogStorager::MAX_STORAGER_NUM> StoragerThread;

class ObLogStorager : public IObLogStorager, public StoragerThread
{
public:
  ObLogStorager();
  virtual ~ObLogStorager();

public:
  virtual int thread_begin() override;
  virtual void thread_end() override;

public:
  int start();
  void stop();
  void mark_stop_flag() { StoragerThread::mark_stop_flag(); }
  virtual int submit(IObLogBatchBufTask *task);
  void get_task_count(int64_t &block_count, int64_t &log_task_count) const;
  int handle(void *data, const int64_t thread_index, volatile bool &stop_flag);

public:
  int init(const int64_t thread_num,
      const int64_t queue_size,
      IObStoreService &store_service,
      IObLogErrHandler &err_handler);
  void destroy();

private:
  static const int64_t DATA_OP_TIMEOUT = 1 * 1000 * 1000;
  static const int64_t PRINT_TASK_COUNT_INTERVAL = 10 * _SEC_;
  static const int64_t PRINT_RPS_STAT_INTERVAL   = 10 * _SEC_;

private:
  int handle_task_(IObLogBatchBufTask &batch_task,
      const int64_t thread_index,
      volatile bool &stop_flag);
  int write_store_service_(const char *key,
      const char *log_str,
      const size_t log_str_len,
      void *column_family_handle,
      const int64_t thread_index);

  void print_task_count_();
  void print_rps_();

  int read_store_service_(const std::string &key);

private:
  bool                      inited_;
  // To ensure that tasks are evenly distributed to threads
  uint64_t                  round_value_;

  TransRpsStatInfo          rps_stat_;
  int64_t                   last_stat_time_ CACHE_ALIGNED;
  int64_t                   block_count_ CACHE_ALIGNED;
  int64_t                   log_task_count_ CACHE_ALIGNED;
  StoreServiceStatInfo      store_service_stat_;

  IObStoreService           *store_service_;
  IObLogErrHandler          *err_handler_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogStorager);
};

} // namespace libobcdc
} // namespace oceanbase
#endif
