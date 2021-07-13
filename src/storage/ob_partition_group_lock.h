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

#ifndef OCEANBASE_STORAGE_OB_PARTITION_GROUP_LOCK_
#define OCEANBASE_STORAGE_OB_PARTITION_GROUP_LOCK_

#include "lib/lock/ob_latch.h"
#include "storage/ob_replay_status.h"

namespace oceanbase {
namespace storage {

static const int64_t PGLOCKSTORAGE = 1L;
static const int64_t PGLOCKCLOG = 1L << 1;
static const int64_t PGLOCKTRANS = 1L << 2;
static const int64_t PGLOCKREPLAY = 1L << 3;

static const int64_t PGLOCKSIZE = 4;
static const int64_t PGLOCKMASK = (1L << PGLOCKSIZE) - 1;

class ObPartitionGroupLockGuard;
class ObPartitionGroupTryLockGuard;
class ObPGLockWithPendingReplayGuard;

// ATTENTION:
// TODO: the solution now is not graceful, better solution is
// needed.
// the partition group lock should be used carefully with replay engine lock for
// the deadlock issue.
//
// the deadlock will happen as follows:
// 1. the lock will be added from replay engine to partition group (replay
//    ADD_PARTITION_TO_PG clog)
// 2. the lock will be added from partition group to replay engine (stop
//    partition group during rebuild)
//
// so the replay engine lock needs to guarantee the graceful exit until the
// partition group lock and the action under the lock needs to provide the
// reentrant execution.
class ObPartitionGroupLock {
  friend ObPartitionGroupLockGuard;
  friend ObPartitionGroupTryLockGuard;
  friend ObPGLockWithPendingReplayGuard;

public:
  typedef common::ObLatch RWLock;

  ObPartitionGroupLock();
  ~ObPartitionGroupLock();

  ObPartitionGroupLock(const ObPartitionGroupLock&) = delete;
  ObPartitionGroupLock& operator=(const ObPartitionGroupLock&) = delete;

private:
  int64_t lock(int64_t hold, int64_t change);
  int64_t try_lock(int64_t hold, int64_t change);
  void unlock(int64_t target);

  RWLock locks_[PGLOCKSIZE];
};

class ObPartitionGroupLockGuard {
public:
  ObPartitionGroupLockGuard(ObPartitionGroupLock& lock, int64_t hold, int64_t change, const bool trylock = false);
  ~ObPartitionGroupLockGuard();

  void unlock(int64_t target);
  bool locked() const
  {
    return mark_ != 0;
  }

  ObPartitionGroupLockGuard(const ObPartitionGroupLockGuard&) = delete;
  ObPartitionGroupLockGuard& operator=(const ObPartitionGroupLockGuard&) = delete;

private:
  ObPartitionGroupLock& lock_;
  int64_t mark_;
  int64_t start_ts_;
};

// ATTENTION:
// The lock is designed to resolve the deadlock of code design between replay
// engine and partition group. You need use the guard when adding replay engine
// lock inside partition group
class ObPGLockWithPendingReplayGuard {
public:
  ObPGLockWithPendingReplayGuard(ObPartitionGroupLock& lock, ObReplayStatus& replay_status, const ObPartitionKey& key,
      int64_t hold, int64_t change);
  ~ObPGLockWithPendingReplayGuard();

  // the following two functions will only return OB_SUCCESS on success or
  // OB_EAGAIN if you fail to pending the replay engine
  int try_pending_replay_engine_();
  int wait_follower_no_pending_task_();
  void unset_pending_replay_engine_();

  ObPGLockWithPendingReplayGuard(const ObPGLockWithPendingReplayGuard&) = delete;
  ObPGLockWithPendingReplayGuard& operator=(const ObPGLockWithPendingReplayGuard&) = delete;

public:
  static const int64_t SLEEP_FOR_PENDING_REPLAY = 10L * 1000L;  // 10ms
  static const int64_t SLEEP_FOR_PENDING_REPLAY_CNT = 5000L;
  static const int64_t TIMEOUT_FOR_PENDING_REPLAY = 100L * 1000L;  // 100ms
private:
  ObPartitionGroupLock& lock_;
  ObReplayStatus& replay_status_;
  ObPartitionKey pkey_;
  int64_t mark_;
  int64_t start_ts_;
};

}  // namespace storage
}  // namespace oceanbase

#endif /* OCEANBASE_STORAGE_OB_PARTITION_GROUP_LOCK_ */
