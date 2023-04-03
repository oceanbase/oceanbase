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
 * Reader: read data from storage.
 */

#ifndef OCEANBASE_LIBOBCDC_READER_H_
#define OCEANBASE_LIBOBCDC_READER_H_

#include "lib/thread/ob_multi_fixed_queue_thread.h" // ObMQThread
#include "ob_log_trans_stat_mgr.h"                  // TransRpsStatInfo
#include "ob_log_store_service_stat.h"              // StoreServiceStatInfo
#include "ob_log_part_trans_task.h"                 // ObLogEntryTask
#include "ob_log_work_mode.h"                       // WorkingMode

namespace oceanbase
{
namespace libobcdc
{
/////////////////////////////////////////////////////////////////////////////////////////
class IObLogReader
{
public:
  enum
  {
    MAX_PARSER_NUM = 32
  };
  const static int64_t PUSH_PARSER_TIMEOUT = 1 * _SEC_;

public:
  virtual ~IObLogReader() {}

public:
  virtual int start() = 0;
  virtual void stop() = 0;
  virtual void mark_stop_flag() = 0;
  virtual int push(ObLogEntryTask &task, const int64_t timeout) = 0;
  virtual void get_task_count(int64_t &log_entry_task_count) const = 0;
  virtual void print_stat_info() = 0;
};

/////////////////////////////////////////////////////////////////////////////////////////

class IObStoreService;
class IObLogErrHandler;

typedef common::ObMQThread<IObLogReader::MAX_PARSER_NUM> ReaderThread;

class ObLogReader : public IObLogReader, public ReaderThread
{
public:
  ObLogReader();
  virtual ~ObLogReader();

public:
  int start();
  void stop();
  void mark_stop_flag() { ReaderThread::mark_stop_flag(); }
  int push(ObLogEntryTask &task, const int64_t timeout);
  void get_task_count(int64_t &log_entry_task_count) const { log_entry_task_count = ATOMIC_LOAD(&log_entry_task_count_); }
  void print_stat_info();
  int handle(void *data, const int64_t thread_index, volatile bool &stop_flag);

public:
  int init(const int64_t thread_num,
      const int64_t queue_size,
      const WorkingMode working_mode,
      IObStoreService &store_service,
      IObLogErrHandler &err_handler);
  void destroy();

private:
  int handle_task_(ObLogEntryTask &task,
      const int64_t thread_index,
      volatile bool &stop_flag);
  int read_store_service_(ObLogEntryTask &log_entry_task,
      void *column_family_handle,
      std::string &key,
      std::string &value);
  int push_task_into_dml_parser_(ObLogEntryTask &task,
      const int64_t thread_index,
      volatile bool &stop_flag);

  void print_task_count_();

private:
  bool                      inited_;
  WorkingMode               working_mode_;
  // Used to ensure that tasks are evenly distributed to threads
  uint64_t                  round_value_;

  TransRpsStatInfo          rps_stat_;
  int64_t                   last_stat_time_ CACHE_ALIGNED;
  int64_t                   log_entry_task_count_ CACHE_ALIGNED;

  StoreServiceStatInfo      store_service_stat_;
  IObStoreService           *store_service_;
  IObLogErrHandler          *err_handler_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogReader);
};

} // namespace libobcdc
} // namespace oceanbase
#endif
