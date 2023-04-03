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

#ifndef OCEANBASE_LIBOBCDC_OB_LOG_DDL_PARSER_H_
#define OCEANBASE_LIBOBCDC_OB_LOG_DDL_PARSER_H_

#include "lib/thread/ob_multi_fixed_queue_thread.h"      // ObMQThread

namespace oceanbase
{

namespace libobcdc
{
class PartTransTask;

class IObLogDdlParser
{
public:
  static const int64_t MAX_THREAD_NUM = 256;
public:
  virtual ~IObLogDdlParser() {}

public:
  virtual int start() = 0;
  virtual void stop() = 0;
  virtual void mark_stop_flag() = 0;
  virtual int push(PartTransTask &task, const int64_t timeout) = 0;
  virtual int get_part_trans_task_count(int64_t &task_num) = 0;
};

typedef common::ObMQThread<IObLogDdlParser::MAX_THREAD_NUM, IObLogDdlParser> DdlParserThread;

class IObLogErrHandler;
class IObLogPartTransParser;
class ObLogDdlParser : public IObLogDdlParser, public DdlParserThread
{
  enum
  {
    DATA_OP_TIMEOUT = 1 * 1000 * 1000,
  };

public:
  ObLogDdlParser();
  virtual ~ObLogDdlParser();

public:
  // DdlParserThread handle function
  virtual int handle(void *task, const int64_t thread_index, volatile bool &stop_flag);

public:
  int start();
  void stop();
  void mark_stop_flag() { DdlParserThread::mark_stop_flag(); }
  int push(PartTransTask &task, const int64_t timeout);
  int get_part_trans_task_count(int64_t &task_num);

public:
  int init(const int64_t thread_num,
      const int64_t queue_size,
      IObLogErrHandler &err_handler,
      IObLogPartTransParser &part_trans_parser);
  void destroy();

private:
  bool                  inited_;
  IObLogErrHandler      *err_handler_;
  IObLogPartTransParser *part_trans_parser_;

  // The serial number of the currently processed task, used to rotate task push to the queue
  int64_t               push_seq_ CACHE_ALIGNED;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogDdlParser);
};
}
}

#endif
