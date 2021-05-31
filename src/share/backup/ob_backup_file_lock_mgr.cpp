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

#define USING_LOG_PREFIX SHARE
#include "ob_backup_file_lock_mgr.h"

using namespace oceanbase;
using namespace lib;
using namespace common;
using namespace share;

ObBackupFileLockMgr::ObBackupFileLockMgr() : is_inited_(false), lock_mgr_()
{}

ObBackupFileLockMgr::~ObBackupFileLockMgr()
{}

void ObBackupFileLockMgr::destroy()
{
  if (is_inited_) {
    lock_mgr_.destroy();
    is_inited_ = false;
  }
}

ObBackupFileLockMgr& ObBackupFileLockMgr::get_instance()
{
  static ObBackupFileLockMgr instance;
  return instance;
}

int ObBackupFileLockMgr::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("block file lock mgr init twice", K(ret));
  } else if (OB_FAIL(lock_mgr_.create(MAX_BUCKET_NUM))) {
    LOG_WARN("failed to create lock mgr map", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObBackupFileLockMgr::try_lock(const ObBackupPath& path)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;

  if (path.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("path should not be empty", K(ret), K(path));
  } else {
    hash_ret = lock_mgr_.exist_refactored(path);
    if (OB_HASH_EXIST == hash_ret) {
      ret = OB_EAGAIN;
    } else if (hash_ret == OB_HASH_NOT_EXIST) {
      if (OB_FAIL(lock_mgr_.set_refactored(path))) {
        LOG_WARN("failed to set lock", K(ret), K(path));
      }
    } else {
      ret = hash_ret;
      LOG_WARN("failed to try lock", K(ret), K(path));
    }
  }
  return ret;
}

int ObBackupFileLockMgr::low_try_lock(const ObBackupPath& path, const int64_t max_spin_cnt, uint64_t& spin_cnt)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  spin_cnt = 0;
  for (; OB_SUCC(ret) && spin_cnt < max_spin_cnt; ++spin_cnt) {
    hash_ret = lock_mgr_.exist_refactored(path);
    if (OB_HASH_EXIST == hash_ret) {
      // do nothing
    } else if (OB_HASH_NOT_EXIST == hash_ret) {
      if (OB_FAIL(lock_mgr_.set_refactored(path))) {
        LOG_WARN("failed to set lock", K(ret), K(path));
      } else {
        break;
      }
    } else {
      spin_cnt = max_spin_cnt;
      ret = hash_ret;
      LOG_ERROR("failed to get lock", K(ret), K(path));
    }
    PAUSE();
  }
  return ret;
}

int ObBackupFileLockMgr::lock(const ObBackupPath& path, const int64_t abs_timeout_us)
{
  int ret = OB_SUCCESS;
  uint64_t i = 0;
  uint64_t yield_cnt = 0;
  if (path.is_empty() || OB_UNLIKELY(abs_timeout_us <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument", K(ret), K(path), K(abs_timeout_us));
  } else {
    while (OB_SUCC(ret)) {
      // spin
      if (OB_FAIL(low_try_lock(path, MAX_SPIN_TIMES, i))) {
        LOG_WARN("failed to low try lock", K(ret), K(i), K(path));
      } else if (OB_LIKELY(i < MAX_SPIN_TIMES)) {
        // success lock
        break;
      } else if (yield_cnt < MAX_YIELD_CNT) {
        sched_yield();
        ++yield_cnt;
        continue;
      } else {
        // wait
        if (OB_FAIL(wait(path, abs_timeout_us))) {
          if (OB_TIMEOUT != ret) {
            COMMON_LOG(WARN, "Fail to wait the lock, ", K(ret));
          }
        } else {
          break;
        }
      }
    }
  }
  return ret;
}

int ObBackupFileLockMgr::wait(const ObBackupPath& path, const int64_t abs_timeout_us)
{
  int ret = OB_SUCCESS;
  int64_t timeout = 0;
  uint64_t spin_cnt = 0;

  while (OB_SUCC(ret)) {
    timeout = abs_timeout_us - ObTimeUtility::current_time();
    if (OB_UNLIKELY(timeout <= 0)) {
      ret = OB_TIMEOUT;
      COMMON_LOG(DEBUG, "wait lock mutex timeout, ", K(abs_timeout_us), K(ret));
    } else {
      // spin try lock
      if (OB_FAIL(low_try_lock(path, MAX_SPIN_CNT_AFTER_WAIT, spin_cnt))) {
        LOG_WARN("failed to low try lock", K(ret), K(path), K(spin_cnt));
      } else if (MAX_SPIN_CNT_AFTER_WAIT > spin_cnt) {
        ret = OB_SUCCESS;
        break;
      } else {
        usleep(1 * 1000 * 1000);  // 1s
      }
    }
  }
  return ret;
}

int ObBackupFileLockMgr::unlock(const ObBackupPath& path)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  if (path.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup path should not be empty", K(ret), K(path));
  } else if (OB_FAIL(lock_mgr_.erase_refactored(path))) {
    LOG_ERROR("failed to erase path from lock mgr", K(ret), K(path));
  }
  return ret;
}

ObBackupFileSpinLock::ObBackupFileSpinLock() : is_inited_(false), is_locked_(false), path_()
{}

ObBackupFileSpinLock::~ObBackupFileSpinLock()
{
  int ret = OB_SUCCESS;
  if (is_locked_) {
    if (OB_FAIL(unlock())) {
      LOG_ERROR("failed to unlock", K(ret), K(is_locked_), K(path_));
    } else {
      is_locked_ = false;
    }
  }
}

int ObBackupFileSpinLock::init(const ObBackupPath& path)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup file spin loc init twice", K(ret));
  } else {
    is_locked_ = false;
    path_ = path;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupFileSpinLock::lock()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObBackupFileLockMgr::get_instance().lock(path_))) {
    LOG_WARN("failed to lock", K(ret), K(path_));
  } else {
    is_locked_ = true;
  }
  return ret;
}

int ObBackupFileSpinLock::lock(const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObBackupFileLockMgr::get_instance().lock(path_, ObTimeUtility::current_time() + timeout_us))) {
    LOG_WARN("failed to lock", K(ret), K(path_));
  } else {
    is_locked_ = true;
  }
  return ret;
}

int ObBackupFileSpinLock::trylock()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObBackupFileLockMgr::get_instance().try_lock(path_))) {
    LOG_WARN("failed to try lock", K(ret), K(path_));
  } else {
    is_locked_ = true;
  }
  return ret;
}

int ObBackupFileSpinLock::unlock()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObBackupFileLockMgr::get_instance().unlock(path_))) {
    LOG_WARN("failed to unlock", K(ret), K(path_));
  } else {
    is_locked_ = false;
  }
  return ret;
}
