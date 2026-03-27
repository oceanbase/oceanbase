/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOG_MINER_BR_CONVERTER_H_
#define OCEANBASE_LOG_MINER_BR_CONVERTER_H_

#include "lib/thread/ob_multi_fixed_queue_thread.h"
#include "ob_log_miner_error_handler.h"

namespace oceanbase
{
namespace oblogminer
{
class ObLogMinerBR;
class ObLogMinerRecord;
class ILogMinerDataManager;
class ILogMinerResourceCollector;
class ILogMinerAnalysisWriter;

class ILogMinerBRConverter {
public:
  virtual int start() = 0;
  virtual void stop() = 0;
  virtual void wait() = 0;
  virtual void destroy() = 0;
  virtual int push(ObLogMinerBR *logminer_br) = 0;
  virtual int get_total_task_count(int64_t &record_count) = 0;
};

typedef common::ObMQThread<1, ILogMinerBRConverter> BRConverterThreadPool;

class ObLogMinerBRConverter: public ILogMinerBRConverter, public BRConverterThreadPool
{
public:
  static const int64_t BR_CONVERTER_THREAD_NUM;
  static const int64_t BR_CONVERTER_QUEUE_SIZE;
  static const int64_t PUSH_BR_TIMEOUT_TIME;
public:
  virtual int start();
  virtual void stop();
  virtual void wait();
  virtual void destroy();
  virtual int push(ObLogMinerBR *logminer_br);
  virtual int get_total_task_count(int64_t &record_count);

public:
  ObLogMinerBRConverter();
  ~ObLogMinerBRConverter();
  int init(ILogMinerDataManager *data_manager,
      ILogMinerAnalysisWriter *writer,
      ILogMinerResourceCollector *resource_collector,
      ILogMinerErrorHandler *err_handle_);

  virtual int handle(void *data, const int64_t thread_index, volatile bool &stop_flag);

private:
  int generate_logminer_record_(ObLogMinerBR &br, ObLogMinerRecord &record);

private:
  bool is_inited_;
  ILogMinerDataManager *data_manager_;
  ILogMinerAnalysisWriter *writer_;
  ILogMinerResourceCollector *resource_collector_;
  ILogMinerErrorHandler *err_handle_;
};

}
}

#endif