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

#ifndef OCEANBASE_STORAGE_OB_REPLAY_STATUS_
#define OCEANBASE_STORAGE_OB_REPLAY_STATUS_

#include <stdint.h>
#include "lib/lock/ob_spin_lock.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/atomic/atomic128.h"
#include "lib/queue/ob_link_queue.h"
#include "lib/thread/ob_thread_lease.h"
#include "lib/utility/ob_print_utils.h"
#include "common/ob_range.h"
#include "common/ob_partition_key.h"
#include "clog/ob_log_define.h"
#include "storage/ob_safe_ref.h"
#include "share/ob_errno.h"
#include "share/ob_define.h"
#include "clog/ob_log_define.h"
#include "storage/ob_storage_log_type.h"

namespace oceanbase {
namespace common {
class ObPartitionKey;
class ObIReplayTaskAllocator;
class ObBaseStorageInfo;
}  // namespace common
namespace replayengine {
class ObILogReplayEngine;
}
namespace storage {
class ObIPartitionGroup;
struct SafeRef2;
class ObReplayStatus;

enum ObReplayPostBarrierStatus {
  // just submit a start membershop replay task
  POST_BARRIER_SUBMITTED = 0,
  // start membership  replay task
  POST_BARRIER_FINISHED,  // 1
};
typedef ObReplayPostBarrierStatus PostBarrierStatus;
// for debug
class ObReplayTaskInfo {
public:
  ObReplayTaskInfo() : log_id_(common::OB_INVALID_ID), log_type_(OB_LOG_UNKNOWN)
  {}
  ObReplayTaskInfo(uint64_t log_id, ObStorageLogType log_type) : log_id_(log_id), log_type_(log_type)
  {}
  bool is_valid() const
  {
    return common::OB_INVALID_ID != log_id_ && ObStorageLogTypeChecker::is_valid_log_type(log_type_);
  }
  void reset()
  {
    log_id_ = common::OB_INVALID_ID;
    log_type_ = OB_LOG_UNKNOWN;
  }
  uint64_t get_log_id() const
  {
    return log_id_;
  }
  ObStorageLogType get_log_type() const
  {
    return log_type_;
  }
  OB_INLINE bool operator==(const ObReplayTaskInfo& other) const
  {
    return (this->log_id_ == other.log_id_ && this->log_type_ == other.log_type_);
  }
  TO_STRING_KV(K(log_id_), K(log_type_));

private:
  uint64_t log_id_;
  ObStorageLogType log_type_;
};

enum ObReplayTaskType {
  INVALID_LOG_TASK = 0,
  FETCH_AND_SUBMIT_LOG_TASK = 1,
  REPLAY_LOG_TASK = 2,
};

struct ObReplayTask {
public:
  ObReplayTask()
      : type_(INVALID_LOG_TASK), enqueue_ts_(0), throttled_ts_(OB_INVALID_TIMESTAMP), replay_status_(NULL), fail_info_()
  {}
  virtual ~ObReplayTask();
  virtual int init(ObReplayStatus* replay_status);
  void reuse();
  void reset();

public:
  // record info after replay failed
  struct FailRoundInfo {
  public:
    FailRoundInfo()
    {
      reset();
    }
    ~FailRoundInfo()
    {
      reset();
    }
    void reset()
    {
      has_encount_fatal_error_ = false;
      table_version_ = 0;
      fail_ts_ = 0;
      log_id_ = 0;
      log_type_ = storage::OB_LOG_UNKNOWN;
      ret_code_ = common::OB_SUCCESS;
    }

    void set(const int64_t table_version, const int64_t fail_ts, const uint64_t log_id, const ObStorageLogType log_type,
        const int ret_code)
    {
      table_version_ = table_version;
      fail_ts_ = fail_ts;
      log_id_ = log_id;
      log_type_ = log_type;
      ret_code_ = ret_code;
    }
    TO_STRING_KV(K(has_encount_fatal_error_), K(table_version_), K(fail_ts_), K(log_id_), K(log_type_), K(ret_code_));

  public:
    bool has_encount_fatal_error_;
    int64_t table_version_;
    int64_t fail_ts_;
    uint64_t log_id_;
    ObStorageLogType log_type_;
    int ret_code_;
  };

  virtual ObReplayStatus* get_replay_status()
  {
    return replay_status_;
  }
  virtual bool acquire_lease()
  {
    return lease_.acquire();
  }
  virtual bool revoke_lease()
  {
    return lease_.revoke();
  }
  virtual ObReplayTaskType get_type() const
  {
    return type_;
  }
  virtual void set_enqueue_ts(int64_t ts)
  {
    enqueue_ts_ = ts;
  }
  virtual int64_t get_enqueue_ts() const
  {
    return enqueue_ts_;
  }

  const FailRoundInfo& get_fail_info() const
  {
    return fail_info_;
  }
  void reset_fail_info()
  {
    fail_info_.reset();
  }
  void set_fail_info(const int64_t table_version, const int64_t fail_ts, const uint64_t log_id,
      const ObStorageLogType log_type, const int ret_code)
  {
    fail_info_.set(table_version, fail_ts, log_id, log_type, ret_code);
  }

  void set_fail_ret_code(const int ret_code)
  {
    fail_info_.ret_code_ = ret_code;
  }

  void set_simple_fail_info(const int ret_code, const int64_t fail_ts)
  {
    fail_info_.ret_code_ = ret_code;
    if (0 == fail_info_.fail_ts_) {
      fail_info_.fail_ts_ = fail_ts;
    }
  }
  void set_encount_fatal_error(const int ret_code, const int64_t fail_ts)
  {
    fail_info_.ret_code_ = ret_code;
    fail_info_.has_encount_fatal_error_ = true;
    fail_info_.fail_ts_ = fail_ts;
  }
  bool has_encount_fatal_error() const
  {
    return true == fail_info_.has_encount_fatal_error_;
  }
  bool has_been_throttled() const
  {
    return common::OB_INVALID_TIMESTAMP != throttled_ts_;
  }

  bool is_last_round_failed() const
  {
    return common::OB_SUCCESS != fail_info_.ret_code_;
  }
  VIRTUAL_TO_STRING_KV(K(type_), K(enqueue_ts_), KP(replay_status_), K(fail_info_));

public:
  ObReplayTaskType type_;
  int64_t enqueue_ts_;
  int64_t throttled_ts_;  // the lastest time of writing throttle
  ObReplayStatus* replay_status_;
  FailRoundInfo fail_info_;
  // control state transition of queue
  common::ObThreadLease lease_;
};

struct ObSubmitReplayLogTask : public ObReplayTask {
public:
  ObSubmitReplayLogTask()
      : ObReplayTask(),
        storage_log_type_(ObStorageLogType::OB_LOG_UNKNOWN),
        accum_checksum_(0),
        last_slide_out_log_info_(),
        next_submit_log_info_()
  {
    set_last_slide_out_log_info(0, 0);
    set_next_submit_log_info(0, 0);
    type_ = FETCH_AND_SUBMIT_LOG_TASK;
  }

  ~ObSubmitReplayLogTask()
  {
    reset();
  }
  void reuse();
  void reset();

  void set_last_slide_out_log_info(const uint64_t log_id, const int64_t log_ts);
  void set_next_submit_log_info(const uint64_t log_id, const int64_t log_ts);
  int update_last_slide_out_log_info(const uint64_t log_id, const int64_t log_ts);
  int update_next_submit_log_info(const uint64_t log_id, const int64_t log_ts);
  inline void set_storage_log_type(ObStorageLogType log_type)
  {
    storage_log_type_ = log_type;
  }

  uint64_t get_next_submit_log_id() const;
  int64_t get_next_submit_log_ts() const;
  uint64_t get_last_slide_out_log_id() const;
  int64_t get_last_slide_out_log_ts() const;

  bool need_submit_log() const;

  int64_t get_pending_submit_task_count() const;

  int64_t get_accum_checksum() const
  {
    return accum_checksum_;
  }
  void set_accum_checksum(int64_t checksum)
  {
    accum_checksum_ = checksum;
  }

  INHERIT_TO_STRING_KV("ObReplayTask", ObReplayTask, K(storage_log_type_), K(accum_checksum_),
      K(get_next_submit_log_id()), K(get_next_submit_log_ts()), K(get_last_slide_out_log_id()),
      K(get_last_slide_out_log_ts()));

public:
  ObStorageLogType storage_log_type_;  // recode log_type when failed to check condition before submit
  int64_t accum_checksum_;
  //-----------new added members for decoupling of  replay engine and sliding window---------//
  // the info of last log that sliding window submitted
  struct types::uint128_t last_slide_out_log_info_;
  // the info of last log that replay engine submit to replay queue
  struct types::uint128_t next_submit_log_info_;
};

struct ObReplayLogTaskQueue : public ObReplayTask {
public:
  typedef common::ObLink Link;

public:
  ObReplayLogTaskQueue() : ref_(0), index_(0)
  {
    type_ = REPLAY_LOG_TASK;
  }
  ~ObReplayLogTaskQueue();
  int init(ObReplayStatus* replay_status, const int64_t index);
  void reuse();
  void reset();

  void acquire_ref()
  {
    (void)ATOMIC_FAA(&ref_, 1);
  }
  void release_ref()
  {
    (void)ATOMIC_FAA(&ref_, -1);
  }
  void wait_sync()
  {
    while (ATOMIC_LOAD(&ref_) > 0) {};
  }
  Link* top()
  {
    return queue_.top();
  }
  Link* pop()
  {
    return queue_.pop();
  }
  void push(Link* p)
  {
    queue_.push(p);
  }
  INHERIT_TO_STRING_KV("ObReplayTask", ObReplayTask, K(ref_), K(index_));

public:
  // guarantee the effectiveness of  the object when invoked in function get_min_unreplay_log_id()
  int64_t ref_;
  int64_t index_;
  common::ObSpScLinkQueue queue_;
};

struct ObReplayLogTask : common::ObLink {
public:
  struct RefBuf {
  public:
    RefBuf() : ref_cnt_(0), buf_(NULL)
    {}
    ~RefBuf()
    {}
    inline void inc_ref()
    {
      ATOMIC_INC(&ref_cnt_);
    }
    inline int64_t dec_ref()
    {
      return ATOMIC_SAF(&ref_cnt_, 1);
    }

  public:
    int64_t ref_cnt_;
    void* buf_;
  };

public:
  ObReplayLogTask()
  {
    reset();
  }
  virtual ~ObReplayLogTask()
  {
    reset();
  }
  void reset();
  void set_ref_buf(RefBuf* buf)
  {
    if (NULL != buf && NULL != buf->buf_) {
      buf->inc_ref();
      ref_buf_ = buf;
    }
  }
  int64_t get_trans_mutator_size() const;
  bool is_ref_self()
  {
    bool bool_ret = false;
    if (NULL == ref_buf_) {
      bool_ret = false;
    } else if (ref_buf_->buf_ == this) {
      bool_ret = true;
    } else {
      bool_ret = false;
    }
    return bool_ret;
  }

  bool is_valid()
  {
    return pk_.is_valid() && ObStorageLogTypeChecker::is_valid_log_type(log_type_) &&
           common::OB_INVALID_TIMESTAMP != log_submit_timestamp_ && common::OB_INVALID_ID != log_id_ && log_size_ > 0 &&
           common::OB_INVALID_TIMESTAMP != task_submit_timestamp_ && NULL != log_buf_;
  }
  int64_t get_trans_inc_no() const;

public:
  common::ObPartitionKey pk_;
  ObStorageLogType log_type_;
  int64_t log_submit_timestamp_;
  uint64_t log_id_;
  int64_t log_size_;
  bool batch_committed_;
  int64_t task_submit_timestamp_;
  char* log_buf_;
  RefBuf* ref_buf_;

  TO_STRING_KV("partition_key", pk_, "log_type", log_type_, "log_submit_timestamp", log_submit_timestamp_, "log_id",
      log_id_, "log_size", log_size_, "batch_committed", batch_committed_, "task_submit_timestamp",
      task_submit_timestamp_, KP(ref_buf_));
};

struct ObReplayLogTaskEx {
  ObReplayLogTask* task_;
  int64_t trans_id_;
  TO_STRING_KV("trans_id", trans_id_);
};

struct ObReplayErrInfo {
public:
  ObReplayErrInfo()
  {
    reset();
  }
  ~ObReplayErrInfo()
  {
    reset();
  }
  void reset()
  {
    task_info_.reset();
    err_ts_ = 0;
    err_ret_ = common::OB_SUCCESS;
  }
  void set(uint64_t log_id, ObStorageLogType log_type, int64_t err_ts, int err_ret)
  {
    ObReplayTaskInfo task_info(log_id, log_type);
    task_info_ = task_info;
    err_ts_ = err_ts;
    err_ret_ = err_ret;
  }
  TO_STRING_KV(K(task_info_), K(err_ts_), K(err_ret_));

public:
  ObReplayTaskInfo task_info_;
  int64_t err_ts_;  // the timestamp that partition encounts fatal error
  int err_ret_;     // the ret code of fatal error
};

class ObReplayStatus {
public:
  typedef common::SpinRWLock RWLock;
  typedef common::SpinRLockGuard RLockGuard;
  typedef common::SpinWLockGuard WLockGuard;
  typedef common::ObSpinLockGuard SpinLockGuard;
  struct CheckCanReplayResult {
  public:
    CheckCanReplayResult(common::ObPartitionKey pkey, ObReplayTaskInfo& task_info);
    CheckCanReplayResult()
    {
      reset();
    }
    ~CheckCanReplayResult()
    {
      reset();
    }
    void reset();
    bool is_valid() const;
    TO_STRING_KV(K(ret_code_), K(is_out_of_memstore_mem_), K(has_barrier_), K(is_pre_barrier_),
        K(need_wait_schema_refresh_), K(pkey_), K(task_info_));

  public:
    int ret_code_;
    bool is_out_of_memstore_mem_;
    bool has_barrier_;
    bool is_pre_barrier_;
    bool need_wait_schema_refresh_;
    common::ObPartitionKey pkey_;
    ObReplayTaskInfo task_info_;
  };

public:
  ObReplayStatus();
  ~ObReplayStatus();
  int init(const uint64_t tenant_id, replayengine::ObILogReplayEngine* rp_eg_, SafeRef2& safe_ref);
  void destroy();

public:
  void reuse();
  void reset();

  bool is_enabled() const
  {
    return is_enabled_;
  }
  bool need_filter_trans_log() const
  {
    return need_filter_trans_log_;
  }
  bool can_receive_log() const
  {
    return can_receive_log_;
  }
  void set_can_receive_log(bool can_receive)
  {
    can_receive_log_ = can_receive;
  }
  int enable(const common::ObPartitionKey& pkey);
  int disable(const common::ObPartitionKey& pkey);
  int set_need_filter_trans_log(const common::ObPartitionKey& pkey, const bool need_filter);
  void set_need_filter_trans_log(const bool need_filter)
  {
    need_filter_trans_log_ = need_filter;
  }

  int64_t get_min_unreplay_log_timestamp();
  uint64_t get_min_unreplay_log_id();
  void get_min_unreplay_log(uint64_t& unreplay_log_id, int64_t& timestamp);
  RWLock& get_rwlock()
  {
    return rwlock_;
  }
  RWLock& get_submit_log_info_rwlock()
  {
    return submit_log_info_rwlock_;
  }
  int can_replay_log(ObReplayStatus::CheckCanReplayResult& result);

  bool need_submit_log_task_without_lock();
  int64_t get_pending_submit_task_count() const
  {
    return submit_log_task_.get_pending_submit_task_count();
  }
  int check_and_submit_task(const common::ObPartitionKey& pkey, const uint64_t log_id, const int64_t log_ts,
      const bool need_replay, const clog::ObLogType log_type, const int64_t next_replay_log_ts);
  int submit_restore_task(const common::ObPartitionKey& pkey, const uint64_t log_id, const int64_t log_ts);
  int push_task(ObReplayLogTask& task, uint64_t task_sign);
  void add_task(ObReplayLogTask& task);
  void remove_task(ObReplayLogTask& task);
  void dec_task_count(const common::ObPartitionKey& pkey);

  bool has_pending_task(const common::ObPartitionKey& pkey);
  bool has_pending_abort_task(const common::ObPartitionKey& pkey) const
  {
    return has_pending_abort_task_(pkey);
  }
  int64_t get_pending_task_count() const
  {
    return pending_task_count_;
  }
  int64_t get_retried_task_count() const
  {
    return eagain_count_;
  }

  int64_t get_total_submitted_task_num() const
  {
    return ATOMIC_LOAD(&total_submitted_task_num_);
  }
  int64_t get_total_replayed_task_num() const
  {
    return ATOMIC_LOAD(&total_replayed_task_num_);
  }
  void inc_x_total_submitted_task_num(int64_t x);
  void inc_total_submitted_task_num()
  {
    ATOMIC_INC(&total_submitted_task_num_);
  }
  void inc_total_replayed_task_num()
  {
    ATOMIC_INC(&total_replayed_task_num_);
  }

  PostBarrierStatus get_post_barrier_status() const
  {
    return post_barrier_status_;
  }
  void set_post_barrier_status(const PostBarrierStatus& post_barrier_status);
  int set_post_barrier_finished(const ObReplayTaskInfo& task_info);
  void set_last_task_info(const ObReplayTaskInfo& task_info);
  bool is_post_barrier_finished() const
  {
    return POST_BARRIER_FINISHED == post_barrier_status_;
  }

  uint64_t get_last_task_log_id() const
  {
    return last_task_info_.get_log_id();
  }
  ObStorageLogType get_last_task_log_type() const
  {
    return last_task_info_.get_log_type();
  }

  void offline_partition_task_submitted(const uint64_t log_id)
  {
    if (offline_partition_task_submitted_) {
      if (log_id != offline_partition_log_id_) {
        STORAGE_LOG(WARN, "offline_partition_task already submitted", K(log_id), K(*this));
      }
    } else {
      offline_partition_task_submitted_ = true;
      offline_partition_log_id_ = log_id;
      STORAGE_LOG(INFO, "offline_partition_task first submitted", K(*this));
    }
  }

  bool is_offline_partition_task_submitted() const
  {
    return offline_partition_task_submitted_;
  }

  int update_last_slide_out_log_info(const uint64_t log_id, const int64_t log_ts)
  {
    return submit_log_task_.update_last_slide_out_log_info(log_id, log_ts);
  }

  int update_next_submit_log_info(const uint64_t log_id, const int64_t log_ts)
  {
    return submit_log_task_.update_next_submit_log_info(log_id, log_ts);
  }

  void set_last_slide_out_log_info(const uint64_t log_id, const int64_t log_ts)
  {
    submit_log_task_.set_last_slide_out_log_info(log_id, log_ts);
  }

  void set_next_submit_log_info(const uint64_t log_id, const int64_t log_ts)
  {
    submit_log_task_.set_next_submit_log_info(log_id, log_ts);
  }

  uint64_t get_next_submit_log_id() const
  {
    return submit_log_task_.get_next_submit_log_id();
  }

  int64_t get_next_submit_log_ts() const
  {
    return submit_log_task_.get_next_submit_log_ts();
  }

  uint64_t get_last_slide_out_log_id()
  {
    return submit_log_task_.get_last_slide_out_log_id();
  }

  uint64_t get_last_slide_out_log_ts()
  {
    return submit_log_task_.get_last_slide_out_log_ts();
  }

  void set_submit_log_task_info(const common::ObPartitionKey& pkey, common::ObBaseStorageInfo& clog_info);
  //-----------begin for submit replay task-----------

  //-----------end for submit replay task-----------
  inline void inc_ref()
  {
    ATOMIC_INC(&ref_cnt_);
  }
  inline int64_t dec_ref()
  {
    return ATOMIC_SAF(&ref_cnt_, 1);
  }
  inline int64_t get_ref()
  {
    return ATOMIC_LOAD(&ref_cnt_);
  }
  SafeRef2& get_safe_ref()
  {
    return safe_ref_;
  }

  void set_replay_err_info(uint64_t log_id, ObStorageLogType log_type, int64_t err_ts, int err_ret)
  {
    replay_err_info_.set(log_id, log_type, err_ts, err_ret);
  }
  bool has_encount_fatal_error() const
  {
    return is_fatal_error(replay_err_info_.err_ret_);
  }
  bool is_fatal_error(int ret) const
  {
    return (common::OB_SUCCESS != ret && common::OB_ALLOCATE_MEMORY_FAILED != ret && common::OB_EAGAIN != ret &&
            common::OB_TRANS_WAIT_SCHEMA_REFRESH != ret && common::OB_NOT_RUNNING != ret);
  }
  void* alloc_replay_task_buf(const int64_t size, const common::ObPartitionKey& pkey);
  void free_replay_task(ObReplayLogTask* task);
  bool can_alloc_replay_task(const int64_t size, const common::ObPartitionKey& pkey);

  int erase_pending(const int64_t timeout = INT64_MAX);
  int set_pending(const int64_t timeout = INT64_MAX);
  bool is_pending() const
  {
    return is_pending_;
  }
  void update_last_replay_log_id(const common::ObPartitionKey& pkey, const uint64_t log_id)
  {
    if (!ATOMIC_BCAS(&last_replay_log_id_, common::OB_INVALID_ID, log_id)) {
      REPLAY_LOG(INFO, "no need to update last replay log id", K(log_id), K_(last_replay_log_id), K(pkey));
    } else {
      REPLAY_LOG(INFO, "update last replay log id success", K(log_id), K_(last_replay_log_id), K(pkey));
    }
  }
  uint64_t get_last_replay_log_id() const
  {
    return ATOMIC_LOAD(&last_replay_log_id_);
  }
  bool is_tenant_out_of_memory() const;

  TO_STRING_KV(K(ref_cnt_), K(replay_err_info_), K(last_task_info_), K(post_barrier_status_), K(pending_task_count_),
      K(pending_abort_task_count_), K(pending_task_mutator_size_), K(eagain_count_), K(eagain_start_ts_),
      K(total_submitted_task_num_), K(total_replayed_task_num_), K(is_enabled_), K(is_pending_), K(can_receive_log_),
      K(offline_partition_log_id_), K(offline_partition_task_submitted_), K(submit_log_task_));

private:
  int check_barrier_(const ObStorageLogType log_type, const common::ObPartitionKey& pkey);
  void check_eagain_too_many_(const ObReplayStatus::CheckCanReplayResult& result);

  void inc_pending_task_count_(const common::ObPartitionKey& pkey);
  void dec_pending_task_count_(const common::ObPartitionKey& pkey);
  void inc_pending_task_mutator_size_(ObReplayLogTask& task, const int64_t mutator_size);
  void dec_pending_task_mutator_size_(ObReplayLogTask& task, const int64_t mutator_size);
  void clear_pending_task_mutator_size_();

  void inc_pending_abort_task_count_(const common::ObPartitionKey& pkey);
  void dec_pending_abort_task_count_(const common::ObPartitionKey& pkey);
  bool has_pending_abort_task_(const common::ObPartitionKey& pkey) const;
  bool need_wait_schema_refresh_() const;

  int push_(ObReplayLogTask& task, uint64_t task_sign);
  bool is_tenant_out_of_memory_() const;

private:
  static const int64_t PENDING_COUNT_THRESHOLD = 100;
  static const int64_t EAGAIN_COUNT_THRESHOLD = 50000;
  static const int64_t EAGAIN_INTERVAL_NORMAL_THRESHOLD = 10 * 1000 * 1000LL;    // 10s for normal retry
  static const int64_t EAGAIN_INTERVAL_BIG_THRESHOLD = 10 * 60 * 1000 * 1000LL;  // 10min for wait_schema_refresh
  bool is_inited_;
  int64_t ref_cnt_;                  // guarantee the effectiveness of self memory
  SafeRef2 safe_ref_;                // guarntee the effectiveness of partition when accesing partition
  ObReplayErrInfo replay_err_info_;  // record replay error info
  // last replayed task info
  ObReplayTaskInfo last_task_info_;
  // status of about submitting replay start membership logs
  PostBarrierStatus post_barrier_status_;
  // number of replay task to be replayed in queue
  int64_t pending_task_count_;  // atomic
  int64_t pending_task_mutator_size_;
  int64_t pending_abort_task_count_;
  int64_t eagain_count_;
  int64_t eagain_start_ts_;
  int64_t total_submitted_task_num_;
  int64_t total_replayed_task_num_;
  RWLock rwlock_;  // for is_enabled_

  bool is_enabled_;
  bool is_pending_;
  // whether replay trans log or not: Log replica only needs replaying add_partition_to_pg and remove_partition_to_pg
  bool need_filter_trans_log_;
  bool can_receive_log_;
  int64_t tenant_id_;
  uint64_t offline_partition_log_id_;
  bool offline_partition_task_submitted_;

  replayengine::ObILogReplayEngine* rp_eg_;
  // be sure to clear these queues when the partition is offline to prevent old replay task is replayed in situation of
  // migrating out and then migrating in
  ObReplayLogTaskQueue task_queues_[common::REPLAY_TASK_QUEUE_SIZE];  // queues of replay task

  RWLock submit_log_info_rwlock_;  // protect submit_log_info
  ObSubmitReplayLogTask submit_log_task_;
  common::ObIReplayTaskAllocator* allocator_;
  // use for log filtering in situation of restarting or migrating or rebuilding or replication
  uint64_t last_replay_log_id_;

  DISALLOW_COPY_AND_ASSIGN(ObReplayStatus);
};

}  // namespace storage
}  // namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_REPLAY_STATUS_
