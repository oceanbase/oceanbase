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
 * DML type task parser, work thread pool
 */

#ifndef OCEANBASE_LIBOBCDC_OB_LOG_DML_PARSER_H_
#define OCEANBASE_LIBOBCDC_OB_LOG_DML_PARSER_H_

#include "lib/thread/ob_multi_fixed_queue_thread.h" // ObMQThread
#include "ob_log_part_trans_task.h"                 // ObLogEntryTask

namespace oceanbase
{

namespace libobcdc
{
class PartTransTask;

class IObLogDmlParser
{
public:
  static const int64_t MAX_THREAD_NUM = 256;
public:
  virtual ~IObLogDmlParser() {}

public:
  virtual int start() = 0;
  virtual void stop() = 0;
  virtual void mark_stop_flag() = 0;
  virtual int push(ObLogEntryTask &task, const int64_t timeout) = 0;
  virtual int get_log_entry_task_count(int64_t &task_num) = 0;
};

// DML type tasks are assigned global task sequence numbers for the purpose of sequential consumption within Sequencer.
// Because Sequencer is a fixed-length seq queue internally, Parser has to ensure that threads are allocated by task
// sequence number to avoid starvation of tasks with small serial numbers.
//
// ObSeqThread is such a thread pool, which ensures concurrent consumption of tasks in order.
typedef common::ObMQThread<IObLogDmlParser::MAX_THREAD_NUM, IObLogDmlParser> DmlParserThread;

class IObLogFormatter;
class IObLogErrHandler;
class IObLogPartTransParser;
class ObLogDmlParser : public IObLogDmlParser, public DmlParserThread
{
  enum
  {
    DATA_OP_TIMEOUT = 1 * 1000 * 1000,
  };

public:
  ObLogDmlParser();
  virtual ~ObLogDmlParser();

public:
  // Handler functions for ObSeqThread
  virtual int handle(void *task, const int64_t thread_index, volatile bool &stop_flag);

public:
  int start();
  void stop();
  void mark_stop_flag() { DmlParserThread::mark_stop_flag(); }
  int push(ObLogEntryTask &task, const int64_t timeout);
  int get_log_entry_task_count(int64_t &task_num);

public:
  int init(const int64_t parser_thread_num,
      const int64_t parser_queue_size,
      IObLogFormatter &formatter,
      IObLogErrHandler &err_handler,
      IObLogPartTransParser &part_trans_parser);
  void destroy();

private:
  int dispatch_task_(ObLogEntryTask &log_entry_task, PartTransTask &part_trans_task, volatile bool &stop_flag);
  int handle_empty_stmt_(ObLogEntryTask &log_entry_task, PartTransTask &part_trans_task, volatile bool &stop_flag);
  int push_task_into_formatter_(ObLogEntryTask &log_entry_task, volatile bool &stop_flag);
  int wait_until_parser_pause_down_(const int64_t thread_index, volatile bool &stop_flag);

private:
  bool                  inited_;
  uint64_t              round_value_;
  IObLogFormatter       *formatter_;
  IObLogErrHandler      *err_handler_;
  IObLogPartTransParser *part_trans_parser_;
  int64_t               log_entry_task_count_ CACHE_ALIGNED;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogDmlParser);
};
}
}

#endif
