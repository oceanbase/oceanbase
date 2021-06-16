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

#ifndef OCEANBASE_ARCHIVE_RESTORE_ENGINE_
#define OCEANBASE_ARCHIVE_RESTORE_ENGINE_

#include "lib/thread/ob_simple_thread_pool.h"
#include "lib/allocator/ob_small_allocator.h"
#include "lib/queue/ob_link_queue.h"
#include "lib/hash/ob_link_hashmap.h"
#include "lib/ob_define.h"
#include "archive/ob_archive_log_file_store.h"

namespace oceanbase {
namespace clog {
class ObLogEntryHeader;
class ObLogEntry;
class ObIPartitionLogService;
enum ObArchiveFetchLogResult;
enum ObLogType;
}  // namespace clog
namespace common {
struct ObPartitionKey;
}
namespace share {
struct ObPhysicalRestoreInfo;
}
namespace storage {
class ObPartitionService;
}

namespace archive {
class ObPGArchiveRestoreTask : public common::ObLink {
public:
  ObPGArchiveRestoreTask()
  {
    reset();
  }
  ~ObPGArchiveRestoreTask()
  {
    reset();
  }
  int init(const common::ObPGKey& restore_pg_key, const common::ObPGKey& archive_pg_key,
      ObArchiveLogFileStore* file_store, uint64_t start_log_id, int64_t start_log_ts, int64_t end_snapshot_version,
      int64_t leader_takeover_ts);
  void reset();
  void switch_file();

public:
  int locate_file_range();
  bool is_finished() const;
  bool is_expired() const
  {
    return is_expired_;
  }
  void set_is_expired(const bool is_expired)
  {
    is_expired_ = is_expired;
  }
  void set_cur_offset(const int64_t offset)
  {
    cur_offset_ = offset;
  }
  int set_last_fetched_log_info(
      const clog::ObLogType log_type, const uint64_t log_id, const int64_t checkpoint_ts, const int64_t log_submit_ts);
  bool has_located_file_range() const
  {
    return has_located_file_range_;
  }

  const common::ObPGKey& get_restore_pg_key() const
  {
    return restore_pg_key_;
  }
  const common::ObPGKey& get_archive_pg_key() const
  {
    return archive_pg_key_;
  }
  int64_t get_end_snapshot_version() const
  {
    return end_snapshot_version_;
  }
  int64_t get_leader_takeover_ts() const
  {
    return leader_takeover_ts_;
  }
  uint64_t get_last_fetched_log_id() const
  {
    return last_fetched_log_id_;
  }
  int64_t get_last_checkpoint_ts() const
  {
    return last_checkpoint_ts_;
  }
  int64_t get_last_fetched_log_submit_ts() const
  {
    return last_fetched_log_submit_ts_;
  }
  int64_t get_cur_offset() const
  {
    return cur_offset_;
  }
  uint64_t get_cur_log_file_id() const
  {
    return cur_log_file_id_;
  }
  uint64_t get_end_log_file_id() const
  {
    return end_log_file_id_;
  }
  int64_t get_retry_cnt() const
  {
    return retry_cnt_;
  }
  int64_t get_io_fail_cnt() const
  {
    return io_fail_cnt_;
  }
  void set_fetch_log_result(const clog::ObArchiveFetchLogResult result)
  {
    fetch_log_result_ = result;
  }
  clog::ObArchiveFetchLogResult get_fetch_log_result() const
  {
    return fetch_log_result_;
  }
  void inc_retry_cnt()
  {
    ++retry_cnt_;
  }
  void inc_io_fail_cnt()
  {
    ++io_fail_cnt_;
  }
  void reset_retry_fail_cnt()
  {
    io_fail_cnt_ = 0;
    retry_cnt_ = 0;
  }
  bool is_restoring_last_log_file() const
  {
    return (cur_log_file_id_ == end_log_file_id_);
  }
  ObArchiveLogFileStore* get_file_store()
  {
    return file_store_;
  };
  int reconfirm_fetch_log_result();

public:
  TO_STRING_KV(K(restore_pg_key_), K(archive_pg_key_), K(is_expired_), K(has_located_file_range_), K(start_log_id_),
      K(start_log_ts_), K(end_snapshot_version_), K(leader_takeover_ts_), K(last_fetched_log_id_),
      K(last_checkpoint_ts_), K(last_fetched_log_submit_ts_), K(cur_offset_), K(cur_log_file_id_), K(end_log_file_id_),
      K(retry_cnt_),    // Number of consecutive failed retries
      K(io_fail_cnt_),  // Number of consecutive IO failures
      K(fetch_log_result_));

private:
  int locate_start_file_id_(uint64_t& start_file_id);

private:
  common::ObPGKey restore_pg_key_;  // restore partition pkey
  common::ObPGKey archive_pg_key_;  // archive partition pkey, pure key same as restore partition and tenant_id is
                                    // different
  bool is_expired_;
  bool has_located_file_range_;
  uint64_t start_log_id_;               // the start log_id to pull logs
  uint64_t start_log_ts_;               // the start log_ts to pull logs
  int64_t end_snapshot_version_;        // the largest log to be pulled
  int64_t leader_takeover_ts_;          // restore leader takeover time
  uint64_t last_fetched_log_id_;        // the last log_id to be fetched and submited
  int64_t last_checkpoint_ts_;          // the last checkpoint_ts fetched
  int64_t last_fetched_log_submit_ts_;  // the last log_TS to be fetched and submited
  int64_t cur_offset_;
  uint64_t cur_log_file_id_;  // current file id to be pulled, 0 is inited
  uint64_t end_log_file_id_;  // the max file id to be pulled
  int64_t retry_cnt_;
  int64_t io_fail_cnt_;  // io fail count, task stop if it > MAX_FETCH_LOG_IO_FAIL_CNT
  clog::ObArchiveFetchLogResult fetch_log_result_;
  ObArchiveLogFileStore* file_store_;
};

class ObArchiveRestoreEngine : public common::ObSimpleThreadPool {
public:
  ObArchiveRestoreEngine()
      : is_inited_(false),
        is_stopped_(true),
        lock_(),
        task_queue_(),
        partition_service_(NULL),
        task_allocator_(),
        restore_meta_allocator_(),
        restore_meta_map_(*this)
  {}

  virtual ~ObArchiveRestoreEngine()
  {
    destroy();
  }
  virtual int init(storage::ObPartitionService* partition_service);

public:
  virtual void stop();
  virtual void wait();
  virtual void destroy();
  virtual void handle(void* task);

public:
  int submit_restore_task(
      common::ObPartitionKey& pg_key, uint64_t start_log_id, int64_t start_log_ts, int64_t leader_takeover_ts);
  int try_advance_restoring_clog();

public:
  typedef common::ObIntWarp ObTenantID;
  typedef common::LinkHashNode<ObTenantID> RestoreMetaNode;
  typedef common::LinkHashValue<ObTenantID> RestoreMetaValue;
  class TenantRestoreMeta : public RestoreMetaValue {
  public:
    TenantRestoreMeta()
        : is_inited_(false),
          cur_restore_concurrency_(0),
          restore_concurrency_threshold_(-1),
          restore_info_(),
          file_store_()
    {}
    ~TenantRestoreMeta()
    {
      destroy();
    }
    void destroy();
    bool is_inited() const
    {
      return is_inited_;
    }
    int init(const uint64_t tenant_id);
    ObArchiveLogFileStore* get_file_store()
    {
      return &file_store_;
    };
    uint64_t get_tenant_id() const
    {
      return restore_info_.tenant_id_;
    }
    int64_t get_snapshot_version() const
    {
      return restore_info_.restore_snapshot_version_;
    }
    int64_t get_cur_restore_concurrency() const
    {
      return ATOMIC_LOAD(&cur_restore_concurrency_);
    }
    int64_t get_restore_concurrency_threshold() const
    {
      return ATOMIC_LOAD(&restore_concurrency_threshold_);
    }
    void set_restore_concurrency_threshold(int64_t concurrency)
    {
      ATOMIC_SET(&restore_concurrency_threshold_, concurrency);
    }
    void inc_cur_restore_concurrency()
    {
      ATOMIC_INC(&cur_restore_concurrency_);
    }
    void dec_cur_restore_concurrency()
    {
      ATOMIC_DEC(&cur_restore_concurrency_);
    }
    TO_STRING_KV(K_(restore_info), K_(file_store), K(cur_restore_concurrency_), K(restore_concurrency_threshold_));

  public:
    static const int64_t DEFAULT_RESTORE_CONCURRENCY_THRESHOLD = 4;

  private:
    bool is_inited_;
    int64_t cur_restore_concurrency_;        // concurrency of pg restore tasks
    int64_t restore_concurrency_threshold_;  // the upper limit of the concurrency of pg restore tasks
    share::ObPhysicalRestoreInfo restore_info_;
    ObArchiveLogFileStore file_store_;
  };

  // for ObLinkHashMap
  TenantRestoreMeta* alloc_value()
  {
    return NULL;
  }
  void free_value(TenantRestoreMeta* meta)
  {
    if (NULL != meta) {
      meta->destroy();
      restore_meta_allocator_.free(meta);
      meta = NULL;
    }
  }

  RestoreMetaNode* alloc_node(TenantRestoreMeta* meta)
  {
    UNUSED(meta);
    return op_reclaim_alloc(RestoreMetaNode);
  }

  void free_node(RestoreMetaNode* node)
  {
    if (NULL != node) {
      op_reclaim_free(node);
      node = NULL;
    }
  }

  struct ObPhysicalRestoreCLogStat {
  public:
    ObPhysicalRestoreCLogStat()
    {
      reset();
    }
    ~ObPhysicalRestoreCLogStat()
    {
      reset();
    }
    void reset();

  public:
    int64_t retry_sleep_interval_;   // sleep time before retry
    int64_t fetch_log_entry_count_;  // fetch log entry count
    int64_t fetch_log_entry_cost_;   // fetch log entry time cost
    int64_t io_cost_;                // toal io time cost
    int64_t io_count_;               // total io count
    int64_t limit_bandwidth_cost_;   // wait time because of bandwidth limit
    int64_t total_cost_;             // total time cost
  };

private:
  bool need_retry_ret_code_(const int ret);
  bool is_io_fail_ret_code_(const int ret);
  int get_tenant_restore_meta_(const uint64_t tenant_id, TenantRestoreMeta*& restore_meta);
  int handle_single_task_(ObPGArchiveRestoreTask& task, ObPhysicalRestoreCLogStat& stat);
  int convert_pkey_(const common::ObPGKey& src_pkey, const uint64_t dest_tenant_id, common::ObPGKey& dest_pkey);
  int try_replace_tenant_id_(const uint64_t new_tenant_id, clog::ObLogEntryHeader& log_entry_header);
  int fetch_and_submit_archive_log_(
      ObPGArchiveRestoreTask& task, ObPhysicalRestoreCLogStat& stat, clog::ObIPartitionLogService* log_service);
  int process_normal_clog_(const clog::ObLogEntry& log_entry, const bool is_batch_committed,
      clog::ObIPartitionLogService* log_service, ObPGArchiveRestoreTask& task);
  int push_into_task_queue_(ObPGArchiveRestoreTask* task);
  int calc_restore_concurrency_threshold_(const uint64_t tenant_id, int64_t& new_threshold);
  int report_fatal_error_(const ObPartitionKey& restore_pkey, const ObPartitionKey& archive_pkey, const int err_ret);
  void revert_tenant_restore_meta_(TenantRestoreMeta*& restore_meta);
  void statistic_(const ObPhysicalRestoreCLogStat& inc_stat);

private:
  typedef common::ObLinkHashMap<ObTenantID, TenantRestoreMeta, ObArchiveRestoreEngine&> TenantRestoreMetaMap;
  const int64_t MAX_FETCH_LOG_IO_FAIL_CNT = 5;
  const int64_t TASK_NUM_LIMIT = 1000000;
  const int64_t ACCESS_LOG_FILE_STORE_TIMEOUT = 3 * 1000 * 1000L;
  const int64_t MINI_MODE_RESTORE_THREAD_NUM = 1;
  const int64_t MEMSTORE_RESERVED_PER_PG = 128 * 1024 * 1024L;
  bool is_inited_;
  bool is_stopped_;
  common::ObLatch lock_;                // to guarantee task_queue_ poped one thread by one thread
  common::ObSpScLinkQueue task_queue_;  // global pg fetch task queue
  storage::ObPartitionService* partition_service_;
  // TODO: cleanup expired restore_meta
  common::ObSmallAllocator task_allocator_;
  common::ObSmallAllocator restore_meta_allocator_;
  TenantRestoreMetaMap restore_meta_map_;
};
}  // end of namespace archive
}  // namespace oceanbase
#endif  // OCEANBASE_ARCHIVE_RESTORE_ENGINE_
