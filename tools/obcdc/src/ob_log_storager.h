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

#ifndef OCEANBASE_LIBOBLOG_STORAGER_H_
#define OCEANBASE_LIBOBLOG_STORAGER_H_

#include "lib/thread/ob_multi_fixed_queue_thread.h" // ObMQThread
#include "ob_log_trans_stat_mgr.h"                  // TransRpsStatInfo
#include "ob_log_store_service_stat.h"              // StoreServiceStatInfo
#include "ob_log_part_trans_task.h"

namespace oceanbase
{
namespace liboblog
{
/////////////////////////////////////////////////////////////////////////////////////////
class IObLogStorager
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
  virtual int push(ObLogEntryTask &task, int64_t timeout) = 0;
  virtual void get_task_count(int64_t &log_entry_task_count) const = 0;
};

/////////////////////////////////////////////////////////////////////////////////////////

class IObStoreService;
class IObLogErrHandler;
class ObLogRowDataIndex;

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
  int push(ObLogEntryTask &task, int64_t timeout);
  void get_task_count(int64_t &log_entry_task_count) const { log_entry_task_count = ATOMIC_LOAD(&log_entry_task_count_); }
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
  static const int64_t MAX_STORAG_DATA_BUF_SIZE = 2L * 1024L * 1024L; // 2M
  struct StoreBuf
  {
    StoreBuf();
    ~StoreBuf();

    int init(const int64_t size);
    void destroy();

    int alloc(const int64_t sz, void *&ptr);
    void free();

    int64_t get_buf_len() const { return buf_len_; }
    const char *get_buf() const;

    ObDataBuffer data_buf_;
    bool use_data_buf_;
    char *big_buf_;
    int64_t buf_len_;
  };

private:
  int init_store_buf_array_(const int64_t thread_num);
  void destroy_store_buf_array_(const int64_t thread_num);
  int handle_log_entry_task_(ObLogEntryTask &log_entry_task,
      const int64_t thread_index,
      volatile bool &stop_flag);
  // BinlogRecord serialisation to byte streams
  int to_string_binlog_record_(ObLogEntryTask &log_entry_task,
      ObLogRowDataIndex &row_data_index,
      const int64_t thread_index);
  int write_store_service_(ObLogRowDataIndex &row_data_index,
      void *column_family_handle,
      const int64_t thread_index);
  int handle_task_callback_(ObLogEntryTask &log_entry_task,
      volatile bool &stop_flag);

  void print_task_count_();
  void print_rps_();

  // for test and debug
  int print_unserilized_br_value_(ObLogBR &br,
      std::string &key);
  int print_serilized_br_value_(ObLogRowDataIndex &row_data_index,
      std::string &key,
      const char *buf,
      const size_t buf_len);
  int read_store_service_(const std::string &key);

private:
  bool                      inited_;
  // To ensure that tasks are evenly distributed to threads
  uint64_t                  round_value_;
  // Thread local buf
  StoreBuf                  store_buf_array_[IObLogStorager::MAX_STORAGER_NUM];

  TransRpsStatInfo          rps_stat_;
  int64_t                   last_stat_time_ CACHE_ALIGNED;
  int64_t                   log_entry_task_count_ CACHE_ALIGNED;
  StoreServiceStatInfo      store_service_stat_;

  IObStoreService           *store_service_;
  IObLogErrHandler          *err_handler_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogStorager);
};

} // namespace liboblog
} // namespace oceanbase
#endif
