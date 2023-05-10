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

#ifndef OCEANBASE_STORAGE_OB_LS_LOCK_
#define OCEANBASE_STORAGE_OB_LS_LOCK_

#include "share/ob_ls_id.h"
#include "lib/lock/ob_latch.h"

namespace oceanbase
{
namespace storage
{

static const int64_t LSLOCKLS      = 1L;
static const int64_t LSLOCKLOG     = 1L << 1;
static const int64_t LSLOCKTX      = 1L << 2;
static const int64_t LSLOCKSTORAGE = 1L << 3;
static const int64_t LSLOCKLOGMETA = 1L << 4;

static const int64_t LSLOCKSIZE = 5;
static const int64_t LSLOCKMASK = (1L << LSLOCKSIZE) - 1;
static const int64_t LSLOCKALL = LSLOCKMASK;

class ObLSLockGuard;
class ObLSTryLockGuard;
class ObLSLockWithPendingReplayGuard;
class ObLS;

// ATTENTION:
// TODO(handora.qc): the solution now is not graceful, better solution is
// needed.
// the LS lock should be used carefully with replay engine lock for
// the deadlock issue.
//
// the deadlock will happen as follows:
// 1. the lock will be added from replay engine to LS (replay
//    ADD_TABLET_TO_LS clog)
// 2. the lock will be added from LS to replay engine (stop
//    LS during rebuild)
//
// so the replay engine lock needs to guarantee the graceful exit until the
// LS lock and the action under the lock needs to provide the
// reentrant execution.
class ObLSLock
{
  friend ObLSLockGuard;
  friend ObLSTryLockGuard;
  friend ObLSLockWithPendingReplayGuard;
  static const int64_t LOCK_CONFLICT_WARN_TIME = 100 * 1000; // 100 ms
public:
  typedef common::ObLatch RWLock;

  ObLSLock();
  ~ObLSLock();

  ObLSLock(const ObLSLock&) = delete;
  ObLSLock& operator=(const ObLSLock&) = delete;
private:
  int64_t lock(const ObLS *ls, int64_t hold, int64_t change, const int64_t abs_timeout_us = INT64_MAX);
  int64_t try_lock(const ObLS *ls, int64_t hold, int64_t change);
  void unlock(int64_t target);

  RWLock locks_[LSLOCKSIZE];
};

class ObLSLockGuard
{
public:
  ObLSLockGuard(ObLS *ls,
                ObLSLock &lock,
                int64_t hold,
                int64_t change,
                const bool trylock = false);
  ObLSLockGuard(ObLS *ls,
                ObLSLock &lock,
                int64_t hold,
                int64_t change,
                const int64_t abs_timeout_us);
  // lock all by default.
  // WARNING: make sure ls is not null.
  ObLSLockGuard(ObLS *ls, const bool rdlock = false);
  ~ObLSLockGuard();

  void unlock(int64_t target);
  bool locked() const { return mark_ != 0; }

  ObLSLockGuard(const ObLSLockGuard&) = delete;
  ObLSLockGuard& operator=(const ObLSLockGuard&) = delete;
private:
  ObLSLock &lock_;
  int64_t mark_;
  int64_t start_ts_;
  const ObLS *ls_;
};

// ATTENTION:
// The lock is designed to resolve the deadlock of code desgin between replay
// engine and partition group. You need use the guard when adding replay engine
// lock inside LS
class ObLSLockWithPendingReplayGuard
{
public:
  ObLSLockWithPendingReplayGuard(ObLSLock &lock,
                                 const share::ObLSID &ls_id,
                                 int64_t hold,
                                 int64_t change);
  ~ObLSLockWithPendingReplayGuard();

  ObLSLockWithPendingReplayGuard(const ObLSLockWithPendingReplayGuard&) = delete;
  ObLSLockWithPendingReplayGuard& operator=(const ObLSLockWithPendingReplayGuard&) = delete;
public:
  static const int64_t SLEEP_FOR_PENDING_REPLAY = 1000L * 1000L; // 1s
  static const int64_t SLEEP_FOR_WAIT_EMPTY = 10L * 1000L; // 10ms
  static const int64_t SLEEP_FOR_PENDING_REPLAY_CNT = 5000L;
  static const int64_t TIMEOUT_FOR_PENDING_REPLAY = 100L * 1000L; // 100ms
private:
  ObLSLock &lock_;
  //ObReplayStatus &replay_status_;
  share::ObLSID ls_id_;
  int64_t mark_;
  int64_t start_ts_;
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_OB_LS_LOCK_ */
