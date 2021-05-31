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

#include "ob_partition_group_lock.h"

#include "lib/stat/ob_latch_define.h"
#include "lib/utility/utility.h"

namespace oceanbase {

using namespace common;

namespace storage {

ObPartitionGroupLock::ObPartitionGroupLock() : locks_()
{}

ObPartitionGroupLock::~ObPartitionGroupLock()
{}

int64_t ObPartitionGroupLock::lock(int64_t hold, int64_t change)
{
  int ret = OB_SUCCESS;  // tmp_ret, rewrite every time
  int64_t pos = 0;
  int64_t res = 0;

  hold &= PGLOCKMASK;
  change &= PGLOCKMASK;
  ObTimeGuard tg("ObPartitionGroupLock::lock", 100 * 1000);
  while (hold | change) {
    if (change & 1) {
      if (OB_FAIL(locks_[pos].wrlock(ObLatchIds::PARTITION_LOCK))) {
        STORAGE_LOG(ERROR, "wrlock error", K(ret), K(pos));
      } else {
        res |= 1L << pos;
      }
    } else if (hold & 1) {
      if (OB_FAIL(locks_[pos].rdlock(ObLatchIds::PARTITION_LOCK))) {
        STORAGE_LOG(ERROR, "rdlock error", K(ret), K(pos));
      } else {
        res |= 1L << pos;
      }
    }

    tg.click();

    hold >>= 1;
    change >>= 1;
    pos++;
  }

  return res;
}

int64_t ObPartitionGroupLock::try_lock(int64_t hold, int64_t change)
{
  int ret = OB_SUCCESS;  // tmp_ret, rewrite every time
  int64_t pos = 0;
  int64_t res = 0;

  hold &= PGLOCKMASK;
  change &= PGLOCKMASK;
  ObTimeGuard tg("ObPartitionGroupLock::try_lock", 100 * 1000);
  while (hold | change) {
    if (change & 1) {
      if (OB_FAIL(locks_[pos].try_wrlock(ObLatchIds::PARTITION_LOCK))) {
        break;
      } else {
        res |= 1L << pos;
      }
    } else if (hold & 1) {
      if (OB_FAIL(locks_[pos].try_rdlock(ObLatchIds::PARTITION_LOCK))) {
        break;
      } else {
        res |= 1L << pos;
      }
    }

    tg.click();

    hold >>= 1;
    change >>= 1;
    pos++;
  }

  return res;
}

void ObPartitionGroupLock::unlock(int64_t target)
{
  int ret = OB_SUCCESS;  // tmp_ret, rewrite every time
  int64_t pos = 0;

  target &= PGLOCKMASK;
  while (target) {
    if ((target & 1) && OB_FAIL(locks_[pos].unlock())) {
      STORAGE_LOG(ERROR, "unlock error", K(ret), K(pos));
    }

    target >>= 1;
    pos++;
  }
}

// <=================== lock guard =======================>

ObPartitionGroupLockGuard::ObPartitionGroupLockGuard(
    ObPartitionGroupLock& lock, int64_t hold, int64_t change, const bool trylock)
    : lock_(lock), mark_(0), start_ts_(INT64_MAX)
{
  hold &= PGLOCKMASK;
  change &= PGLOCKMASK;
  // upgrade hold to change
  hold ^= hold & change;

  if (!trylock) {
    mark_ = lock_.lock(hold, change);
    start_ts_ = ObTimeUtility::current_time();
  } else {
    if ((hold | change) != (mark_ = lock_.try_lock(hold, change))) {
      // try lock failed
      lock_.unlock(mark_);
      mark_ = 0;
    }
  }
}

ObPartitionGroupLockGuard::~ObPartitionGroupLockGuard()
{
  lock_.unlock(mark_);
  const int64_t end_ts = ObTimeUtility::current_time();
  if (end_ts - start_ts_ > 5 * 1000 * 1000) {
    STORAGE_LOG(INFO, "partition group lock cost too much time", K_(start_ts), "cost_us", end_ts - start_ts_, K(lbt()));
  }
  start_ts_ = INT64_MAX;
}

void ObPartitionGroupLockGuard::unlock(int64_t target)
{
  target &= mark_;
  mark_ ^= target;
  lock_.unlock(target);
  const int64_t end_ts = ObTimeUtility::current_time();
  if (end_ts - start_ts_ > 5 * 1000 * 1000) {
    STORAGE_LOG(INFO, "partition group lock cost too much time", K_(start_ts), "cost_us", end_ts - start_ts_, K(lbt()));
  }
}

ObPGLockWithPendingReplayGuard::ObPGLockWithPendingReplayGuard(
    ObPartitionGroupLock& lock, ObReplayStatus& replay_status, const ObPartitionKey& pkey, int64_t hold, int64_t change)
    : lock_(lock), replay_status_(replay_status), pkey_(pkey), mark_(0), start_ts_(INT64_MAX)
{
  int tmp_ret = OB_EAGAIN;
  hold &= PGLOCKMASK;
  change &= PGLOCKMASK;
  // upgrade hold to change
  hold ^= hold & change;

  while (OB_EAGAIN == tmp_ret) {
    mark_ = lock_.lock(hold, change);

    if (OB_SUCCESS != (tmp_ret = try_pending_replay_engine_())) {
      // encounter EAGAIN
    } else if (OB_SUCCESS != (tmp_ret = wait_follower_no_pending_task_())) {
      STORAGE_LOG(WARN, "wait follower no pending task failed", K(tmp_ret), K(pkey_));
    }

    if (OB_SUCCESS != tmp_ret) {
      if (OB_EAGAIN != tmp_ret) {
        STORAGE_LOG(ERROR, "unexpected error", K(tmp_ret), K(pkey_));
      }
      lock_.unlock(mark_);
      usleep(SLEEP_FOR_PENDING_REPLAY);
    }
  }

  start_ts_ = ObTimeUtility::current_time();
}

int ObPGLockWithPendingReplayGuard::wait_follower_no_pending_task_()
{
  int ret = OB_SUCCESS;
  int64_t cnt = 0;
  int64_t task_cnt = replay_status_.get_pending_task_count();

  while (replay_status_.has_pending_task(pkey_) && OB_SUCC(ret)) {
    usleep(SLEEP_FOR_PENDING_REPLAY);
    cnt++;

    if (cnt >= SLEEP_FOR_PENDING_REPLAY_CNT) {
      int last_task_cnt = task_cnt;
      if (last_task_cnt == (task_cnt = replay_status_.get_pending_task_count())) {
        ret = OB_EAGAIN;
        STORAGE_LOG(WARN, "replay too slow, retreat!", K(ret), K(pkey_));
      } else {
        STORAGE_LOG(WARN, "follower wait replay too long", K(ret), K(pkey_), K(task_cnt));
      }

      cnt = 0;
    }
  }

  return ret;
}

int ObPGLockWithPendingReplayGuard::try_pending_replay_engine_()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(replay_status_.set_pending(ObTimeUtility::current_time() + TIMEOUT_FOR_PENDING_REPLAY))) {
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      STORAGE_LOG(WARN, "try lock replay engine failed", K(ret), K(pkey_));
    }
    ret = OB_EAGAIN;
  }

  return ret;
}

void ObPGLockWithPendingReplayGuard::unset_pending_replay_engine_()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(replay_status_.erase_pending())) {
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      STORAGE_LOG(WARN, "try unlock replay engine failed", K(ret), K(pkey_));
    }
  }
}

ObPGLockWithPendingReplayGuard::~ObPGLockWithPendingReplayGuard()
{
  unset_pending_replay_engine_();
  lock_.unlock(mark_);
  const int64_t end_ts = ObTimeUtility::current_time();
  if (end_ts - start_ts_ > 5 * 1000 * 1000) {
    STORAGE_LOG(INFO,
        "partition group lock cost too much time",
        K_(start_ts),
        "cost_us",
        end_ts - start_ts_,
        K(pkey_),
        K(lbt()));
  }
  start_ts_ = INT64_MAX;
}

}  // namespace storage
}  // namespace oceanbase
