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

#ifndef OCEANBASE_LOG_MINER_RECORD_BATCH_WRITER_H_
#define OCEANBASE_LOG_MINER_RECORD_BATCH_WRITER_H_

#include "lib/thread/ob_multi_fixed_queue_thread.h"
#include "lib/container/ob_ext_ring_buffer.h"
#include "ob_log_miner_file_manager.h"
#include "ob_log_miner_error_handler.h"

namespace oceanbase
{
namespace oblogminer
{

class ObLogMinerBatchRecord;
class ILogMinerDataManager;
class ILogMinerResourceCollector;
class ILogMinerFileManager;

class ILogMinerBatchRecordWriter
{
public:
  virtual int start() = 0;
  virtual void stop() = 0;
  virtual void wait() = 0;
  virtual void destroy() = 0;
  virtual int push(ObLogMinerBatchRecord *record) = 0;
  virtual int get_total_record_count(int64_t &record_count) = 0;
  virtual int get_total_batch_record_count(int64_t &batch_record_count) = 0;
};

struct BatchRecordMetaTask
{
  BatchRecordMetaTask() {reset();}
  ~BatchRecordMetaTask() { reset(); }
  void reset() {
    file_id_ = -1;
    last_trans_end_ts_ = OB_INVALID_TIMESTAMP;
    record_cnt_ = 0;
    is_file_complete_ = false;
  }

  TO_STRING_KV(
    K(file_id_),
    K(last_trans_end_ts_),
    K(record_cnt_),
    K(is_file_complete_)
  )

  int64_t file_id_;
  int64_t last_trans_end_ts_;
  int64_t record_cnt_;
  bool is_file_complete_;
};

struct BatchRecordMetaTaskPopFunc
{
  bool operator()(int64_t sn, BatchRecordMetaTask *task) {
    UNUSED(sn);
    return nullptr != task;
  }
};

typedef common::ObMQThread<1, ILogMinerBatchRecordWriter> BatchWriterThreadPool;
typedef ObExtendibleRingBuffer<BatchRecordMetaTask> BatchRecordSlidingWindow;
class ObLogMinerBatchRecordWriter: public ILogMinerBatchRecordWriter, BatchWriterThreadPool, lib::ThreadPool
{
public:
  static const int64_t BATCH_WRITER_THREAD_NUM = 1L;
  static const int64_t BATCH_WRITER_QUEUE_SIZE = 100000L;
  static const int64_t FILE_SPLIT_THRESHOLD = (64L << 20); // 64 MB
public:
  virtual int start();
  virtual void stop();
  virtual void wait();
  virtual void destroy();
  virtual int push(ObLogMinerBatchRecord *record);
  virtual int get_total_record_count(int64_t &record_count);
  virtual int get_total_batch_record_count(int64_t &batch_record_count);
public:
  ObLogMinerBatchRecordWriter();
  ~ObLogMinerBatchRecordWriter() {
    destroy();
  }

  virtual int handle(void *data, const int64_t thread_index, volatile bool &stop_flag);

  virtual void run1();

  int init(ILogMinerDataManager *data_mgr,
      ILogMinerResourceCollector *res_collector,
      ILogMinerFileManager *file_mgr,
      ILogMinerErrorHandler *err_handle);

private:
  int set_file_info_(ObLogMinerBatchRecord &record,
      const int64_t seq_no);

  int generate_meta_flush_task_(const ObLogMinerBatchRecord &record);

  int proceed_checkpoint_();

private:
  bool is_inited_;
  int64_t current_file_id_;
  int64_t current_file_offset_;
  BatchRecordSlidingWindow batch_record_sw_;

  int64_t record_count_;
  int64_t batch_record_count_;
  ILogMinerDataManager *data_manager_;
  ILogMinerResourceCollector *resource_collector_;
  ILogMinerFileManager *file_manager_;
  ILogMinerErrorHandler *err_handle_;
};

}
}

#endif