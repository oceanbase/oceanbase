/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOG_MINER_RECORD_AGGREGATOR_H_
#define OCEANBASE_LOG_MINER_RECORD_AGGREGATOR_H_

#include "lib/thread/ob_multi_fixed_queue_thread.h"
#include "ob_log_miner_error_handler.h"
#include "ob_log_miner_batch_record.h"

namespace oceanbase
{
namespace oblogminer
{

class ObLogMinerRecord;
class ILogMinerAnalysisWriter;
class ILogMinerDataManager;
class ILogMinerBatchRecordWriter;
class ILogMinerResourceCollector;

class ILogMinerRecordAggregator
{
public:
  virtual int start() = 0;
  virtual void stop() = 0;
  virtual void wait() = 0;
  virtual void destroy() = 0;
  virtual int push(ObLogMinerRecord *record) = 0;
  virtual int get_total_record_count(int64_t &record_count) = 0;
};

typedef common::ObMQThread<1, ILogMinerRecordAggregator> RecordAggThreadPool;

class ObLogMinerRecordAggregator: public ILogMinerRecordAggregator, public RecordAggThreadPool
{
public:
  static const int64_t RECORD_AGG_THREAD_NUM;
  static const int64_t RECORD_AGG_QUEUE_SIZE;
  static const int64_t RECORD_FLUSH_THRESHOLD;
public:
  virtual int start();
  virtual void stop();
  virtual void wait();
  virtual void destroy();
  virtual int push(ObLogMinerRecord *record);
  virtual int get_total_record_count(int64_t &record_count);
public:
  virtual int handle(void *data, const int64_t thread_index, volatile bool &stop_flag);

public:
  ObLogMinerRecordAggregator();
  ~ObLogMinerRecordAggregator();

  int init(const int64_t start_time_us,
      ILogMinerBatchRecordWriter *writer,
      ILogMinerDataManager *data_manager,
      ILogMinerResourceCollector *resource_collector,
      ILogMinerErrorHandler *err_handle);

private:
  int check_need_flush_(bool &need_flush);

private:
  bool is_inited_;
  ObLogMinerBatchRecord *cur_batch_record_;
  int64_t push_record_count_ CACHE_ALIGNED;
  int64_t aggregated_record_count_ CACHE_ALIGNED;
  int64_t last_trans_end_ts_;
  ILogMinerBatchRecordWriter *writer_;
  ILogMinerDataManager *data_manager_;
  ILogMinerResourceCollector *resource_collector_;
  ILogMinerErrorHandler *err_handle_;
};

}
}

#endif