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

#ifndef OCEANBASE_MVCC_OB_ROW_LOCK_
#define OCEANBASE_MVCC_OB_ROW_LOCK_
#include "lib/lock/ob_latch.h"
#include "lib/stat/ob_latch_define.h"
#include "lib/atomic/atomic128.h"
#include "lib/time/ob_time_utility.h"

namespace oceanbase {
namespace memtable {
class ObMemtableKey;
class ObMvccRow;

class ObRowLock {
public:
  friend class ObMvccRow;

public:
  explicit ObRowLock(ObMvccRow& row) : row_(row)
  {}
  ~ObRowLock()
  {}

  int64_t to_string(char* buf, const int64_t len) const
  {
    return latch_.to_string(buf, len);
  }

  bool is_locked();
  int try_exclusive_lock(const uint32_t uid);
  bool is_exclusive_locked_by(const uint32_t uid);
  uint32_t get_exclusive_uid();
  int exclusive_lock(const uint32_t uid, const int64_t abs_timeout);

  int exclusive_unlock(const ObMemtableKey* key, const uint32_t uid)
  {
    int ret = common::OB_SUCCESS;
    const uint32_t exclusive_uid = latch_.get_wid();
    if (uid == exclusive_uid) {
      ret = latch_.unlock(&uid);
      after_unlock(key);
    } else {
      if (EXECUTE_COUNT_PER_SEC(16)) {
        TRANS_LOG(WARN, "lock uid not match", K(uid), K(exclusive_uid), K(common::lbt()));
      }
    }
    return ret;
  }

  int revert_lock(const uint32_t uid)
  {
    int ret = common::OB_SUCCESS;
    const uint32_t exclusive_uid = latch_.get_wid();
    if (uid == exclusive_uid) {
      ret = latch_.unlock(&uid);
    } else {
      TRANS_LOG(ERROR, "lock uid not match", K(uid), K(exclusive_uid));
    }
    return ret;
  }
  void after_unlock(const ObMemtableKey* key);

private:
  bool is_locked_()
  {
    return latch_.is_locked();
  }

  bool is_exclusive_locked_by_(const uint32_t uid)
  {
    return latch_.is_wrlocked_by(&uid);
  }

  uint32_t get_exclusive_uid_()
  {
    return latch_.get_wid();
  }

  int try_exclusive_lock_(const uint32_t uid)
  {
    return latch_.try_wrlock(common::ObLatchIds::ROW_LOCK, &uid);
  }

  int exclusive_lock_(const uint32_t uid, const int64_t abs_timeout)
  {
    return latch_.wrlock(common::ObLatchIds::ROW_LOCK, abs_timeout, &uid);
  }

private:
  common::ObLatch latch_;
  ObMvccRow& row_;
};
};  // namespace memtable
};  // end namespace oceanbase

#endif /* OCEANBASE_MVCC_OB_ROW_LOCK_ */
