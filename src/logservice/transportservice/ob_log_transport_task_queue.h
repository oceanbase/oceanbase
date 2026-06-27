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

#ifndef OCEANBASE_LOGSERVICE_OB_LOG_TRANSPORT_TASK_QUEUE_H_
#define OCEANBASE_LOGSERVICE_OB_LOG_TRANSPORT_TASK_QUEUE_H_

#include "lib/atomic/ob_atomic.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/hash/ob_linear_hash_map.h"
#include "share/ob_ls_id.h"
#include "share/scn.h"
#include "logservice/palf/lsn.h"
#include "logservice/transportservice/ob_log_transport_rpc_define.h"

#if defined(ENABLE_DEBUG_LOG) && !defined(OB_LOG_RESTORE_QUEUE_TEST_INFRA)
#define OB_LOG_RESTORE_QUEUE_TEST_INFRA
#endif

#ifdef OB_LOG_RESTORE_QUEUE_TEST_INFRA
#include <functional>
#endif

namespace oceanbase
{
namespace common
{
class ObILogAllocator;
}
namespace logservice
{

class ObLogRestoreHandler;

struct LogReceivedTransportTask
{
  explicit LogReceivedTransportTask(share::ObLSID ls_id, palf::LSN start_lsn, palf::LSN end_lsn,
    share::SCN scn, const char *log_data, int64_t log_size);
  ~LogReceivedTransportTask();
  bool is_valid() const;
  TO_STRING_KV(K_(ls_id), K_(start_lsn), K_(end_lsn), K_(scn), KP_(log_data), K_(log_size), K(ref_cnt_));

  int64_t inc_ref();
  int64_t dec_ref();

  const share::ObLSID ls_id_;
  const palf::LSN start_lsn_;
  const palf::LSN end_lsn_;
  const share::SCN scn_;
  const char *log_data_;
  const int64_t log_size_;

  int64_t ref_cnt_;
  DISABLE_COPY_ASSIGN(LogReceivedTransportTask);
};

class ObLogTransportTaskHandle
{
public:
  ObLogTransportTaskHandle();
  ~ObLogTransportTaskHandle();
  ObLogTransportTaskHandle(const ObLogTransportTaskHandle &other);
  ObLogTransportTaskHandle &operator=(const ObLogTransportTaskHandle &other);
  void reset();
  bool is_valid() const;
  const LogReceivedTransportTask *task() const;
  // Deep Copy |task| to LogReceivedTransportTask.
  int init(const ObLogTransportReq *task);
  TO_STRING_KV(KP_(task), KPC_(task));
private:
  ObLogTransportTaskHandle(ObLogTransportTaskHandle &&other) = delete;
  ObLogTransportTaskHandle &operator=(ObLogTransportTaskHandle &&other) = delete;
  LogReceivedTransportTask *task_;
};

class ObLogTransportTaskQueue
{
public:
  static constexpr int64_t MAX_QUEUE_SIZE = (1 << 11); // 2048
  static constexpr int64_t DEFAULT_MAX_CACHED_BYTES = palf::FOLLOWER_DEFAULT_GROUP_BUFFER_SIZE;
  // 分批处理大小：每次处理每个日志流的任务数量，控制排序开销
  static constexpr int64_t BATCH_SIZE = 512;

public:

  ObLogTransportTaskQueue();
  ~ObLogTransportTaskQueue();

  int init(const int64_t id, const int64_t queue_size);
  void stop();
  void destroy();
  void switch_to_follower();
  void switch_to_leader(const palf::LSN &end_lsn);

  int push(const ObLogTransportReq *task);
  int update_end_lsn(const palf::LSN &end_lsn);
  int front(ObLogTransportTaskHandle &handle);
  int success(const ObLogTransportTaskHandle &handle);
  int failure(const ObLogTransportTaskHandle &handle);

  uint64_t count() const;
  void clear_stats();

  void get_queued_end_position(palf::LSN &lsn, share::SCN &scn) const
  {
    common::ObSpinLockGuard guard(queued_end_lock_);
    lsn = queued_end_lsn_;
    scn = queued_end_scn_;
  }

  TO_STRING_KV(KP(this),
               "is_inited", ATOMIC_LOAD(&is_inited_),
               "is_stopped", ATOMIC_LOAD(&is_stopped_),
               K_(id),
               K_(next_submit_lsn),
               K_(queued_end_lsn),
               K_(queued_end_scn),
               "task_count", task_map_.count(),
               "task_load_factor", task_map_.get_load_factor(),
               "task_bkt_count", task_map_.get_bkt_cnt(),
               K_(queue_size),
               K_(cached_bytes),
               K_(max_cached_bytes),
               K_(drop_duplicate_cnt),
               K_(early_drop_far_lsn_cnt),
               K_(total_inserted_cnt),
               K_(total_inserted_bytes),
               K_(total_processed_cnt),
               K_(total_success_cnt),
               K_(total_success_bytes),
               K_(total_skipped_cnt));
private:
  class LSNWarp
  {
  public:
    LSNWarp() : lsn_() {}
    explicit LSNWarp(const palf::LSN lsn) : lsn_(lsn) {}
    ~LSNWarp() { reset(); }
    void reset() { lsn_.reset(); }
    uint64_t hash() const { return common::do_hash(lsn_.val_); }
    int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
    palf::LSN get_value() const { return lsn_; }
    int compare(const LSNWarp &other) const
    {
      int ret = 0;
      if (lsn_ == other.lsn_) {
        ret = 0;
      } else if (lsn_ > other.lsn_) {
        ret = 1;
      } else {
        ret = -1;
      }
      return ret;
    }
    bool operator==(const LSNWarp &other) const
    {
      return 0 == compare(other);
    }
    bool operator!=(const LSNWarp &other) const
    {
      return !operator==(other);
    }
    TO_STRING_KV(K_(lsn));
  private:
    palf::LSN lsn_;
  };
  struct TaskRemoveFunctor
  {
    TaskRemoveFunctor(const palf::LSN &end_lsn);
    ~TaskRemoveFunctor();
    bool operator()(const LSNWarp &key, const ObLogTransportTaskHandle &value);
    palf::LSN end_lsn_;
    int64_t removed_cnt_;
    int64_t removed_bytes_;
  };
private:
  static void add_container_cached_bytes_(int64_t *container_cached_bytes,
                                          const int64_t bytes);
  static void sub_container_cached_bytes_(int64_t *container_cached_bytes,
                                          const ObLogTransportTaskHandle &task_handle);
private:
  common::ObLinearHashMap<LSNWarp, ObLogTransportTaskHandle> task_map_;
private:
  int can_push_task_(const ObLogTransportReq *task);
private:

  bool is_inited_;
  int64_t is_stopped_;
  int64_t id_;
  palf::LSN next_submit_lsn_;
  int64_t queue_size_;
  // approximate and not used for actual control
  int64_t cached_bytes_;
  int64_t max_cached_bytes_;
  mutable common::SpinRWLock lock_;

  // semi-sync: running MAX enqueued end LSN/SCN (may be non-continuous across holes),
  // used as the optimistic early-ACK position. Advanced on any push past the current max.
  // Protected by queued_end_lock_ for atomicity of LSN+SCN pair under concurrent push/read.
  mutable common::ObSpinLock queued_end_lock_;
  palf::LSN queued_end_lsn_;
  share::SCN queued_end_scn_;

  // stats
  int64_t drop_duplicate_cnt_;
  int64_t early_drop_far_lsn_cnt_;
  int64_t total_inserted_cnt_;
  int64_t total_inserted_bytes_;
  int64_t total_processed_cnt_;
  int64_t total_success_cnt_;
  int64_t total_success_bytes_;
  int64_t total_skipped_cnt_;
};

static_assert((ObLogTransportTaskQueue::MAX_QUEUE_SIZE & (ObLogTransportTaskQueue::MAX_QUEUE_SIZE - 1)) == 0,
              "MAX_QUEUE_SIZE must be power of two");

} // namespace logservice
} // namespace oceanbase

#endif /* OCEANBASE_LOGSERVICE_OB_LOG_TRANSPORT_TASK_QUEUE_H_ */
