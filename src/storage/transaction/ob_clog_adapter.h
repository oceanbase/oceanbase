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

#ifndef OCEANBASE_TRANSACTION_OB_CLOG_ADAPTER_
#define OCEANBASE_TRANSACTION_OB_CLOG_ADAPTER_

#include "share/ob_errno.h"
#include "common/ob_partition_key.h"
#include "common/ob_range.h"
#include "common/ob_queue_thread.h"
#include "lib/thread/ob_simple_thread_pool.h"
#include "lib/thread/thread_mgr_interface.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "lib/utility/utility.h"
#include "share/config/ob_server_config.h"
#include "ob_trans_event.h"
#include "ob_trans_log.h"
#include "ob_trans_ctx.h"
#include "ob_trans_submit_log_cb.h"
#include "storage/ob_storage_log_type.h"
#include "clog/ob_batch_submit_ctx.h"

namespace oceanbase {
namespace common {
class ObPartitionKey;
class ObVersion;
}  // namespace common
namespace transaction {
class ObITransSubmitLogCb;
class ObTransTraceLog;
};  // namespace transaction
namespace storage {
class ObPartitionService;
class ObIPartitionGroup;
};  // namespace storage

namespace transaction {

class ObIClogAdapter {
public:
  ObIClogAdapter()
  {}
  virtual ~ObIClogAdapter(){};
  virtual int init(storage::ObPartitionService* partition_service) = 0;
  virtual int start() = 0;
  virtual void stop() = 0;
  virtual void wait() = 0;
  virtual void destroy() = 0;

  virtual int get_status(const common::ObPartitionKey& partition, const bool check_election, int& clog_status,
      int64_t& leader_epoch, common::ObTsWindows& changing_leader_windows) = 0;
  virtual int get_status(storage::ObIPartitionGroup* partition, const bool check_election, int& clog_status,
      int64_t& leader_epoch, common::ObTsWindows& changing_leader_windows) = 0;
  virtual int get_status_unsafe(const common::ObPartitionKey& partition, int& clog_status, int64_t& leader_epoch,
      common::ObTsWindows& changing_leader_windows) = 0;

  virtual int submit_log(const common::ObPartitionKey& partition, const common::ObVersion& version, const char* buff,
      const int64_t size, ObITransSubmitLogCb* cb, storage::ObIPartitionGroup* pg, uint64_t& cur_log_id,
      int64_t& cur_log_timestamp) = 0;
  virtual int submit_aggre_log(const common::ObPartitionKey& partition, const common::ObVersion& version,
      const char* buff, const int64_t size, ObITransSubmitLogCb* cb, storage::ObIPartitionGroup* pg,
      const int64_t base_ts, ObTransLogBufferAggreContainer& container) = 0;
  virtual int flush_aggre_log(const common::ObPartitionKey& partition, const common::ObVersion& version,
      storage::ObIPartitionGroup* pg, ObTransLogBufferAggreContainer& container) = 0;
  virtual int submit_log(const common::ObPartitionKey& partition, const common::ObVersion& version, const char* buff,
      const int64_t size, const int64_t base_ts, ObITransSubmitLogCb* cb, storage::ObIPartitionGroup* pg,
      uint64_t& cur_log_id, int64_t& cur_log_timestamp) = 0;
  virtual int batch_submit_log(const transaction::ObTransID& trans_id, const common::ObPartitionArray& partition_array,
      const clog::ObLogInfoArray& log_info_array, const clog::ObISubmitLogCbArray& cb_array) = 0;

  virtual int submit_log_id_alloc_task(const int64_t log_type, ObTransCtx* ctx) = 0;
  virtual int submit_log_task(const common::ObPartitionKey& partition, const common::ObVersion& version,
      const char* buff, const int64_t size, const bool with_need_update_version, const int64_t local_trans_version,
      const bool with_base_ts, const int64_t base_ts, ObITransSubmitLogCb* cb) = 0;
  virtual int get_log_id_timestamp(const ObPartitionKey& partition, const int64_t prepare_version,
      storage::ObIPartitionGroup* pg, clog::ObLogMeta& log_meta) = 0;
  virtual int backfill_nop_log(
      const common::ObPartitionKey& partition, storage::ObIPartitionGroup* pg, const clog::ObLogMeta& log_meta) = 0;
  virtual int submit_backfill_nop_log_task(
      const common::ObPartitionKey& partition, const clog::ObLogMeta& log_meta) = 0;
  virtual int get_last_submit_timestamp(const common::ObPartitionKey& partition, int64_t& timestamp) = 0;
};

class ObClogAdapter : public ObIClogAdapter, public lib::TGTaskHandler {
public:
  ObClogAdapter();
  ~ObClogAdapter()
  {
    destroy();
  }
  int init(storage::ObPartitionService* partition_service);

  int get_status(const common::ObPartitionKey& partition, const bool check_election, int& clog_status,
      int64_t& leader_epoch, common::ObTsWindows& changing_leader_windows);
  int get_status(storage::ObIPartitionGroup* partition, const bool check_election, int& clog_status,
      int64_t& leader_epoch, common::ObTsWindows& changing_leader_windows);
  int get_status_unsafe(const common::ObPartitionKey& partition, int& clog_status, int64_t& leader_epoch,
      common::ObTsWindows& changing_leader_windows);

  int submit_log(const common::ObPartitionKey& partition, const common::ObVersion& version, const char* buff,
      const int64_t size, ObITransSubmitLogCb* cb, storage::ObIPartitionGroup* pg, uint64_t& cur_log_id,
      int64_t& cur_log_timestamp);
  int submit_log(const common::ObPartitionKey& partition, const common::ObVersion& version, const char* buff,
      const int64_t size, const int64_t base_ts, ObITransSubmitLogCb* cb, storage::ObIPartitionGroup* pg,
      uint64_t& cur_log_id, int64_t& cur_log_timestamp);
  virtual int submit_aggre_log(const common::ObPartitionKey& partition, const common::ObVersion& version,
      const char* buff, const int64_t size, ObITransSubmitLogCb* cb, storage::ObIPartitionGroup* pg,
      const int64_t base_ts, ObTransLogBufferAggreContainer& container);
  virtual int flush_aggre_log(const common::ObPartitionKey& partition, const common::ObVersion& version,
      storage::ObIPartitionGroup* pg, ObTransLogBufferAggreContainer& container);
  int batch_submit_log(const transaction::ObTransID& trans_id, const common::ObPartitionArray& partition_array,
      const clog::ObLogInfoArray& log_info_array, const clog::ObISubmitLogCbArray& cb_array);

  int submit_log_id_alloc_task(const int64_t log_type, ObTransCtx* ctx);
  int submit_log_task(const common::ObPartitionKey& partition, const common::ObVersion& version, const char* buff,
      const int64_t size, const bool with_need_update_version, const int64_t local_trans_version,
      const bool with_base_ts, const int64_t base_ts, ObITransSubmitLogCb* cb);
  int get_log_id_timestamp(const ObPartitionKey& partition, const int64_t prepare_version,
      storage::ObIPartitionGroup* pg, clog::ObLogMeta& log_meta);
  int start();
  void stop();
  void wait();
  void destroy();
  void handle(void* task);
  int backfill_nop_log(
      const common::ObPartitionKey& partition, storage::ObIPartitionGroup* pg, const clog::ObLogMeta& log_meta);
  int submit_backfill_nop_log_task(const common::ObPartitionKey& partition, const clog::ObLogMeta& log_meta);
  int get_last_submit_timestamp(const common::ObPartitionKey& partition, int64_t& timestamp);

public:
  static bool need_retry(const int ret)
  {
    return OB_ALLOCATE_MEMORY_FAILED == ret || OB_EAGAIN == ret;
  }

private:
  void reset_statistics();
  void statistics();
  int submit_log_(const common::ObPartitionKey& partition, const common::ObVersion& version, const char* buf,
      const int64_t size, ObITransSubmitLogCb* cb, storage::ObIPartitionGroup* pg, uint64_t& cur_log_id,
      int64_t& cur_log_timestamp);
  int submit_log_(const common::ObPartitionKey& partition, const common::ObVersion& version, const char* buf,
      const int64_t size, const int64_t base_ts, ObITransSubmitLogCb* cb, storage::ObIPartitionGroup* pg,
      uint64_t& cur_log_id, int64_t& cur_log_timestamp);
  int submit_aggre_log_(const ObPartitionKey& partition, const ObVersion& version, storage::ObIPartitionGroup* pg,
      ObTransLogBufferAggreContainer& container);
  int get_status_impl_(const common::ObPartitionKey& partition, const bool need_safe, int& clog_status,
      int64_t& leader_epoch, common::ObTsWindows& changing_leader_windows);
  bool is_cluster_allow_submit_log_(const common::ObPartitionArray& partition_array) const;

private:
  // apply for a queue with TOTAL_TASK length, at most (TOTAL_TASK - RESERVE_TASK)
  // outside tasks is allowed, RESERVE_TASK reserved for inner tasks.
  static const int64_t RESERVE_TASK = 10000;
  // the upper limit of participants on a single observer is 500k, plus 10000 for reserved ctx
public:
  static const int64_t TOTAL_TASK = MAX_PART_CTX_COUNT + RESERVE_TASK;

private:
  bool is_inited_;
  bool is_running_;
  storage::ObPartitionService* partition_service_;
  // for statistics
  int64_t total_access_ CACHE_ALIGNED;
  int64_t error_count_ CACHE_ALIGNED;
  ObConcurrentFIFOAllocator allocator_;
  int tg_id_;
  DISALLOW_COPY_AND_ASSIGN(ObClogAdapter);
};

class AllocLogIdTask : public ObTransTask {
public:
  AllocLogIdTask() : ObTransTask(), log_type_(storage::ObStorageLogType::OB_LOG_UNKNOWN), ctx_(NULL)
  {}
  virtual ~AllocLogIdTask()
  {
    destroy();
  }
  void reset();
  void destroy()
  {
    reset();
  }
  int make(const int64_t log_type, ObTransCtx* ctx);
  int64_t get_log_type() const
  {
    return log_type_;
  }
  ObTransCtx* get_trans_ctx()
  {
    return ctx_;
  }

private:
  int64_t log_type_;
  ObTransCtx* ctx_;
  DISALLOW_COPY_AND_ASSIGN(AllocLogIdTask);
};

class SubmitLogTask : public ObTransTask {
public:
  SubmitLogTask() : ObTransTask(), with_need_update_version_(false), clog_buf_(NULL), cb_(NULL), allocator_(NULL)
  {
    reset();
  }
  virtual ~SubmitLogTask()
  {
    destroy();
  }
  void reset();
  void destroy();
  int make(const common::ObPartitionKey& partition, const common::ObVersion& version, const char* buf,
      const int64_t size, const bool with_need_update_version, const int64_t local_trans_version,
      const bool with_base_ts, const int64_t base_ts, ObITransSubmitLogCb* cb, ObConcurrentFIFOAllocator* allocator);

public:
  const common::ObPartitionKey& get_partition() const
  {
    return partition_;
  }
  const common::ObVersion& get_version() const
  {
    return version_;
  }
  char* get_clog_buf()
  {
    return clog_buf_;
  }
  int64_t get_clog_size() const
  {
    return clog_size_;
  }
  ObITransSubmitLogCb* get_cb() const
  {
    return cb_;
  }
  int64_t get_local_trans_version() const
  {
    return local_trans_version_;
  }
  bool with_need_update_version() const
  {
    return with_need_update_version_;
  }
  bool with_base_ts() const
  {
    return with_base_ts_;
  }
  int64_t get_base_ts() const
  {
    return base_ts_;
  }
  int64_t get_submit_task_ts() const
  {
    return submit_task_ts_;
  }
  void process_begin()
  {
    process_begin_ts_ = ObTimeUtility::current_time();
  }

public:
  TO_STRING_KV(KP(this), K_(partition), K_(version), K_(with_need_update_version), K_(local_trans_version),
      K_(with_base_ts), K_(base_ts), K_(submit_task_ts), K_(process_begin_ts), "cb",
      *(static_cast<ObTransSubmitLogCb*>(cb_)));

private:
  common::ObPartitionKey partition_;
  common::ObVersion version_;
  bool with_need_update_version_;
  int64_t local_trans_version_;
  bool with_base_ts_;
  int64_t base_ts_;
  char* clog_buf_;
  int64_t clog_size_;
  ObITransSubmitLogCb* cb_;
  ObConcurrentFIFOAllocator* allocator_;
  int64_t submit_task_ts_;
  int64_t process_begin_ts_;
  DISALLOW_COPY_AND_ASSIGN(SubmitLogTask);
};

class BackfillNopLogTask : public ObTransTask {
public:
  BackfillNopLogTask() : ObTransTask()
  {
    reset();
  }
  ~BackfillNopLogTask()
  {
    destroy();
  };
  void reset();
  void destroy()
  {
    reset();
  };
  int make(const common::ObPartitionKey& partition, const clog::ObLogMeta& log_meta);
  const common::ObPartitionKey& get_partition() const
  {
    return partition_;
  }
  const clog::ObLogMeta& get_log_meta() const
  {
    return log_meta_;
  }
  bool is_valid() const;

public:
  TO_STRING_KV(KP(this), K_(partition), K_(log_meta));

private:
  common::ObPartitionKey partition_;
  clog::ObLogMeta log_meta_;
};

class AggreLogTask : public ObTransTask {
public:
  AggreLogTask() : ObTransTask()
  {
    reset();
  }
  ~AggreLogTask()
  {
    destroy();
  }
  void reset();
  void destroy()
  {
    reset();
  }
  int make(const common::ObPartitionKey& partition, ObTransLogBufferAggreContainer* container);
  const common::ObPartitionKey& get_partition() const
  {
    return partition_;
  }
  ObTransLogBufferAggreContainer* get_container()
  {
    return container_;
  }
  void set_in_queue(const bool in_queue)
  {
    in_queue_ = in_queue;
  }
  bool get_in_queue() const
  {
    return in_queue_;
  }

public:
  TO_STRING_KV(KP(this), K_(partition));

private:
  common::ObPartitionKey partition_;
  ObTransLogBufferAggreContainer* container_;
  bool in_queue_;
};

}  // namespace transaction
}  // namespace oceanbase

#endif  // OCEANBASE_TRANSACTION_OB_CLOG_ADAPTER_
