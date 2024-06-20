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

#ifndef OCEANBASE_LOG_MINER_RESOURCE_COLLECTOR_H_
#define OCEANBASE_LOG_MINER_RESOURCE_COLLECTOR_H_

#include "lib/thread/ob_multi_fixed_queue_thread.h"
#include "ob_log_miner_error_handler.h"

namespace oceanbase
{
namespace oblogminer
{

class ObLogMinerBR;
class ObLogMinerRecord;
class ObLogMinerUndoTask;
class ObLogMinerBatchRecord;
class ObLogMinerRecyclableTask;
class ILogMinerBRProducer;
class ILogMinerDataManager;

class ILogMinerResourceCollector {
public:
  virtual int start() = 0;
  virtual void stop() = 0;
  virtual void wait() = 0;
  virtual void destroy() = 0;
  virtual int revert(ObLogMinerRecyclableTask *undo_task) = 0;
  virtual int get_total_task_count(int64_t &task_count) = 0;
};

typedef common::ObMQThread<1, ILogMinerResourceCollector> ResourceCollectorThread;

class ObLogMinerResourceCollector: public ILogMinerResourceCollector, ResourceCollectorThread
{
public:
  static const int64_t RC_THREAD_NUM;
  static const int64_t RC_QUEUE_SIZE;
public:
  virtual int start();
  virtual void stop();
  virtual void wait();
  virtual void destroy();
  virtual int revert(ObLogMinerRecyclableTask *undo_task);
  virtual int get_total_task_count(int64_t &task_count);

public:
  virtual int handle(void *data, const int64_t thread_index, volatile bool &stop_flag);

public:
  ObLogMinerResourceCollector();
  ~ObLogMinerResourceCollector();
  int init(ILogMinerDataManager *data_manager,
      ILogMinerErrorHandler *err_handle);

private:
  int push_task_(ObLogMinerRecyclableTask *task);
  int handle_binlog_record_(ObLogMinerBR *br);
  int handle_logminer_record_(ObLogMinerRecord *logminer_rec);
  int handle_batch_record_(ObLogMinerBatchRecord *batch_rec);

private:
  bool is_inited_;
  ILogMinerDataManager *data_manager_;
  ILogMinerErrorHandler *err_handle_;
};

}
}

#endif