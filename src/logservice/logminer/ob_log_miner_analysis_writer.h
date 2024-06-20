/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_LOG_MINER_ANALYSIS_WRITER_H_
#define OCEANBASE_LOG_MINER_ANALYSIS_WRITER_H_

#include "lib/thread/ob_multi_fixed_queue_thread.h"
#include "ob_log_miner_error_handler.h"
namespace oceanbase
{

namespace oblogminer
{

class ObLogMinerRecord;
class ILogMinerFileManager;
class ILogMinerRecordAggregator;
class ILogMinerBatchRecordWriter;
class ILogMinerDataManager;
class ILogMinerResourceCollector;
class ILogMinerFileManager;

class ILogMinerAnalysisWriter
{
public:
  virtual int start() = 0;
  virtual void stop() = 0;
  virtual void wait() = 0;
  virtual void destroy() = 0;
  virtual int push(ObLogMinerRecord *logminer_rec) = 0;
  virtual int get_total_task_count(int64_t &task_count) = 0;
};

class ObLogMinerAnalysisWriter: public ILogMinerAnalysisWriter
{
public:
  virtual int start();
  virtual void stop();
  virtual void wait();
  virtual void destroy();
  virtual int push(ObLogMinerRecord *logminer_rec);
  virtual int get_total_task_count(int64_t &task_count);

public:

  void mark_stop_flag() { ATOMIC_STORE(&is_stopped_, true); }
  bool has_stopped() const { return ATOMIC_LOAD(&is_stopped_); }
public:
  ObLogMinerAnalysisWriter();
  virtual ~ObLogMinerAnalysisWriter();

  // aggregator and batch_writer is initialized outside analysis writer
  int init(const int64_t start_time_us,
      ILogMinerDataManager *data_manager,
      ILogMinerResourceCollector *resource_collector,
      ILogMinerFileManager *file_manager,
      ILogMinerErrorHandler *err_handle);
private:
  bool is_inited_;
  bool is_stopped_;
  ILogMinerRecordAggregator *aggregator_;
  ILogMinerBatchRecordWriter *batch_writer_;
};

}

}

#endif