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

#ifndef OCEANBASE_LOG_MINER_DATA_MANAGER_H_
#define OCEANBASE_LOG_MINER_DATA_MANAGER_H_

#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "lib/objectpool/ob_small_obj_pool.h"
#include "lib/thread/threads.h"
#include "libobcdc.h"
#include "ob_log_miner_error_handler.h"
#include "ob_log_miner_mode.h"

#include "ob_log_miner_file_manager.h"

namespace oceanbase
{
namespace oblogminer
{

class ObLogMinerBR;
class ObLogMinerRecord;
class ObLogMinerBatchRecord;

class ILogMinerDataManager {
public:
  virtual int start() = 0;
  virtual void stop() = 0;
  virtual void wait() = 0;
  virtual void destroy() = 0;
  virtual int get_logminer_record(ObLogMinerRecord *&logminer_rec) = 0;
  virtual int release_log_miner_record(ObLogMinerRecord *logminer_rec) = 0;
  virtual int get_logminer_batch_record(ObLogMinerBatchRecord *&logminer_batch_rec) = 0;
  virtual int release_logminer_batch_record(ObLogMinerBatchRecord *logminer_batch_rec) = 0;
  virtual int update_output_progress(const int64_t timestamp) = 0;
  virtual int64_t get_output_progress() const = 0;
  virtual int64_t get_record_count() const = 0;
  virtual int increase_record_count(int64_t count) = 0;
  virtual bool reach_end_progress(const int64_t progress) const = 0;
};

class LogMinerDataManagerStat
{
public:
  LogMinerDataManagerStat() { reset(); }
  ~LogMinerDataManagerStat() { reset(); }

  void reset() {
    record_alloc_count_ = 0;
    record_release_count_ = 0;
    batch_record_alloc_count_ = 0;
    batch_record_release_count_ = 0;
  }

  // not atomic, just an estimate
  LogMinerDataManagerStat &operator==(const LogMinerDataManagerStat &that) {
    record_alloc_count_ = that.record_alloc_count_;
    record_release_count_ = that.record_release_count_;
    batch_record_alloc_count_ = that.batch_record_alloc_count_;
    batch_record_release_count_ = that.batch_record_release_count_;
    return *this;
  }

  TO_STRING_KV(
    K(record_alloc_count_),
    K(record_release_count_),
    K(batch_record_alloc_count_),
    K(batch_record_release_count_)
  );

  int64_t record_alloc_count_ CACHE_ALIGNED;
  int64_t record_release_count_ CACHE_ALIGNED;
  int64_t batch_record_alloc_count_ CACHE_ALIGNED;
  int64_t batch_record_release_count_ CACHE_ALIGNED;
};

class ObLogMinerDataManager: public ILogMinerDataManager, public lib::ThreadPool
{
  static const int64_t THREAD_NUM;
  static const int64_t LOGMINER_RECORD_LIMIT = 2 * 1024L * 1024 * 1024; // 2G
  static const int64_t BATCH_RECORD_LIMIT = 2 * 1024L * 1024 * 1024; // 2G
public:
  virtual int start();
  virtual void stop();
  virtual void wait();
  virtual void destroy();
  virtual int get_logminer_record(ObLogMinerRecord *&logminer_rec);
  virtual int release_log_miner_record(ObLogMinerRecord *logminer_rec);
  virtual int get_logminer_batch_record(ObLogMinerBatchRecord *&logminer_batch_rec);
  virtual int release_logminer_batch_record(ObLogMinerBatchRecord *logminer_batch_rec);
  virtual int update_output_progress(const int64_t timestamp);
  virtual int64_t get_output_progress() const
  {
    return output_progress_;
  }
  virtual int64_t get_record_count() const
  {
    return record_count_;
  }
  virtual int increase_record_count(int64_t count);
  virtual bool reach_end_progress(const int64_t progress) const;

public:
  virtual void run1();

public:
  ObLogMinerDataManager();
  ~ObLogMinerDataManager();
  int init(const LogMinerMode mode,
      const RecordFileFormat format,
      const int64_t start_progress,
      const int64_t end_progress,
      ILogMinerErrorHandler *err_handle);

private:
  void do_statistics_();

private:
  bool is_inited_;
  LogMinerDataManagerStat stat_;
  LogMinerMode mode_;
  RecordFileFormat format_;
  common::ObConcurrentFIFOAllocator logminer_record_alloc_;
  common::ObConcurrentFIFOAllocator logminer_batch_record_alloc_;
  int64_t start_progress_;
  int64_t end_progress_;
  int64_t output_progress_;
  int64_t record_count_;
  ILogMinerErrorHandler *err_handle_;
};

}
}

#endif