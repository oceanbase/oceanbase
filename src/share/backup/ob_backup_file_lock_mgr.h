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

#ifndef OCEANBASE_SHARE_BACKUP_OB_BACKUP_FILE_LOCK_MGR_H_
#define OCEANBASE_SHARE_BACKUP_OB_BACKUP_FILE_LOCK_MGR_H_

#include "lib/container/ob_array.h"
#include "lib/restore/ob_storage.h"
#include "lib/utility/utility.h"
#include "lib/worker.h"
#include "lib/hash/ob_hashset.h"
#include "share/ob_define.h"
#include "share/ob_force_print_log.h"
#include "ob_backup_path.h"
namespace oceanbase {
namespace share {

class ObBackupFileLockMgr {
public:
  ObBackupFileLockMgr();
  virtual ~ObBackupFileLockMgr();
  static ObBackupFileLockMgr& get_instance();
  void destroy();
  int init();
  int lock(const ObBackupPath& path, const int64_t abs_timeout_us = INT64_MAX);
  int try_lock(const ObBackupPath& path);
  int unlock(const ObBackupPath& path);

private:
  int low_try_lock(const ObBackupPath& path, const int64_t max_spin_cnt, uint64_t& spin_cnt);
  int wait(const ObBackupPath& path, const int64_t abs_timeout_us);

private:
  static const int64_t MAX_BUCKET_NUM = 1024;
  static const int64_t MAX_SPIN_TIMES = 1024;
  static const int64_t MAX_YIELD_CNT = 2;
  static const int64_t MAX_SPIN_CNT_AFTER_WAIT = 1;
  bool is_inited_;
  common::hash::ObHashSet<ObBackupPath> lock_mgr_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupFileLockMgr);
};

class ObBackupFileSpinLock {
public:
  ObBackupFileSpinLock();
  ~ObBackupFileSpinLock();
  int init(const ObBackupPath& path);
  int lock();
  int lock(const int64_t timeout_us);
  int trylock();
  int unlock();
  ObBackupPath& get_path()
  {
    return path_;
  }

private:
  // data members
  bool is_inited_;
  bool is_locked_;
  ObBackupPath path_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupFileSpinLock);
};

typedef lib::ObLockGuard<ObBackupFileSpinLock> ObBackupFileLockGuard;

}  // namespace share
}  // namespace oceanbase
#endif /* OCEANBASE_SHARE_BACKUP_OB_BACKUP_FILE_LOCK_MGR_H_ */
