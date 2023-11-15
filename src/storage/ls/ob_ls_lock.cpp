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

#define USING_LOG_PREFIX STORAGE

#include "ob_ls_lock.h"

#include "lib/stat/ob_latch_define.h"
#include "lib/utility/utility.h"
#include "storage/ls/ob_ls.h"

namespace oceanbase
{

using namespace common;

namespace storage
{

ObLSLock::ObLSLock()
  : locks_()
{
}

ObLSLock::~ObLSLock()
{
}

int64_t ObLSLock::lock(const ObLS *ls, int64_t hold, int64_t change, const int64_t abs_timeout_us)
{
  int ret = OB_SUCCESS; // tmp_ret, rewrite every time
  int64_t pos = 0;
  int64_t res = 0;

  hold &= LSLOCKMASK;
  change &= LSLOCKMASK;
  ObTimeGuard tg("ObLSLock::lock", LOCK_CONFLICT_WARN_TIME);
  while (hold | change) {
    if (change & 1) {
      if (OB_FAIL(locks_[pos].wrlock(ObLatchIds::LS_LOCK, abs_timeout_us))) {
        // maybe timeout, it is expected error code.
        LOG_WARN("wrlock failed", KR(ret), K(pos));
      } else {
        res |= 1L << pos;
      }
    } else if (hold & 1) {
      if (OB_FAIL(locks_[pos].rdlock(ObLatchIds::LS_LOCK, abs_timeout_us))) {
        // maybe timeout, it is expected error code.
        LOG_WARN("rdlock failed", KR(ret), K(pos));
      } else {
        res |= 1L << pos;
      }
    }

    tg.click();

    hold >>= 1;
    change >>= 1;
    pos++;
  }
  if (tg.get_diff() >= LOCK_CONFLICT_WARN_TIME) {
    LOG_WARN("get lock cost too much time", KP(ls), "ls_id", OB_ISNULL(ls) ? ObLSID(0) : ls->get_ls_id());
  }
  return res;
}

int64_t ObLSLock::try_lock(const ObLS *ls, int64_t hold, int64_t change)
{
  int ret = OB_SUCCESS; // tmp_ret, rewrite every time
  int64_t pos = 0;
  int64_t res = 0;

  hold &= LSLOCKMASK;
  change &= LSLOCKMASK;
  ObTimeGuard tg("ObLSLock::try_lock", LOCK_CONFLICT_WARN_TIME);
  while (hold | change) {
    if (change & 1) {
      if (OB_FAIL(locks_[pos].try_wrlock(ObLatchIds::LS_LOCK))) {
        break;
      } else {
        res |= 1L << pos;
      }
    } else if (hold & 1) {
      if (OB_FAIL(locks_[pos].try_rdlock(ObLatchIds::LS_LOCK))) {
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
  if (tg.get_diff() >= LOCK_CONFLICT_WARN_TIME) {
    LOG_WARN("get lock cost too much time", KP(ls), "ls_id", OB_ISNULL(ls) ? ObLSID(0) : ls->get_ls_id());
  }

  return res;
}

void ObLSLock::unlock(int64_t target)
{
  int ret = OB_SUCCESS; // tmp_ret, rewrite every time
  int64_t pos = 0;

  target &= LSLOCKMASK;
  while (target) {
    if ((target & 1) && OB_FAIL(locks_[pos].unlock())) {
      LOG_ERROR("unlock error", K(ret), K(pos));
    }

    target >>= 1;
    pos++;
  }
}

// <=================== lock guard =======================>

ObLSLockGuard::ObLSLockGuard(ObLS *ls,
                             ObLSLock &lock,
                             int64_t hold,
                             int64_t change,
                             const bool trylock)
  : lock_(lock),
    mark_(0),
    start_ts_(INT64_MAX),
    ls_(ls)
{
  hold &= LSLOCKMASK;
  change &= LSLOCKMASK;
  // upgrade hold to change
  hold ^= hold & change;

  if (!trylock) {
    mark_ = lock_.lock(ls, hold, change);
    start_ts_ = ObTimeUtility::current_time();
  } else {
    if ((hold | change) != (mark_ = lock_.try_lock(ls, hold, change))) {
      // try lock failed
      lock_.unlock(mark_);
      mark_ = 0;
    }
  }
}

ObLSLockGuard::ObLSLockGuard(ObLS *ls,
                             ObLSLock &lock,
                             int64_t hold,
                             int64_t change,
                             const int64_t abs_timeout_us)
  : lock_(lock),
    mark_(0),
    start_ts_(INT64_MAX),
    ls_(ls)
{
  hold &= LSLOCKMASK;
  change &= LSLOCKMASK;
  // upgrade hold to change
  hold ^= hold & change;

  if ((hold | change) != (mark_ = lock_.lock(ls, hold, change, abs_timeout_us))) {
    // reset the the one we have locked.
    lock_.unlock(mark_);
    mark_ = 0;
  }
  start_ts_ = ObTimeUtility::current_time();
}

ObLSLockGuard::ObLSLockGuard(ObLS *ls, const bool rdlock)
  : lock_(ls->lock_),
    mark_(0),
    start_ts_(INT64_MAX),
    ls_(ls)
{
  int64_t wrlock_mask = LSLOCKALL;
  int64_t rdlock_mask = 0;
  if (rdlock) {
    wrlock_mask = 0;
    rdlock_mask = LSLOCKALL;
  }
  wrlock_mask &= LSLOCKMASK;
  rdlock_mask &= LSLOCKMASK;
  mark_ = lock_.lock(ls, rdlock_mask, wrlock_mask);
  start_ts_ = ObTimeUtility::current_time();
}

ObLSLockGuard::~ObLSLockGuard()
{
  lock_.unlock(mark_);
  const int64_t end_ts = ObTimeUtility::current_time();
  if (end_ts - start_ts_ > 5 * 1000 * 1000) {
    FLOG_INFO("ls lock cost too much time", K_(start_ts),
              "cost_us", end_ts - start_ts_, KP(ls_),
              "ls_id", OB_ISNULL(ls_) ? ObLSID(0) : ls_->get_ls_id(), K(lbt()));
  }
  start_ts_ = INT64_MAX;
  ls_ = nullptr;
}

void ObLSLockGuard::unlock(int64_t target)
{
  target &= mark_;
  mark_ ^= target;
  lock_.unlock(target);
  const int64_t end_ts = ObTimeUtility::current_time();
  if (end_ts - start_ts_ > 5 * 1000 * 1000) {
    FLOG_INFO("ls lock cost too much time", K_(start_ts),
              "cost_us", end_ts - start_ts_, KP(ls_),
              "ls_id", OB_ISNULL(ls_) ? ObLSID(0) : ls_->get_ls_id(), K(lbt()));
  }
}

ObLSLockWithPendingReplayGuard::ObLSLockWithPendingReplayGuard(
    ObLSLock &lock,
    const share::ObLSID &ls_id,
    int64_t hold,
    int64_t change)
  : lock_(lock),
    ls_id_(ls_id),
    mark_(0),
    start_ts_(INT64_MAX)
{
  UNUSEDx(lock_, mark_, hold, change);
  // TODO: cxf262476 wait the replay engine to be empty
}

ObLSLockWithPendingReplayGuard::~ObLSLockWithPendingReplayGuard()
{
  // TODO: cxf262476 unlock
  const int64_t end_ts = ObTimeUtility::current_time();
  if (end_ts - start_ts_ > 5 * 1000 * 1000) {
    LOG_INFO("ls lock cost too much time", K_(start_ts),
             "cost_us", end_ts - start_ts_, K(ls_id_), K(lbt()));
  }
  start_ts_ = INT64_MAX;
}

// ================== ls state guard =====================
ObLSStateGuard::ObLSStateGuard(ObLS *ls)
  : ls_(ls),
    begin_state_seq_(-1)
{
  if (OB_NOT_NULL(ls_)) {
    begin_state_seq_ = ls_->get_state_seq();
  }
}

ObLSStateGuard::~ObLSStateGuard()
{
  ls_ = nullptr;
  begin_state_seq_ = -1;
}

int ObLSStateGuard::check()
{
  int ret = OB_SUCCESS;
  int64_t curr_seq = -1;
  if (OB_ISNULL(ls_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (FALSE_IT(curr_seq = ls_->get_state_seq())) {
  } else if (begin_state_seq_ != curr_seq) {
    ret = OB_STATE_NOT_MATCH;
    STORAGE_LOG(WARN, "sequence not match", KR(ret), KPC(ls_));
  }
  return ret;
}

} // storage
} // oceanbase
