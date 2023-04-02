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

#ifndef OCEANBASE_STORAGE_OB_TABLET_ID_SET
#define OCEANBASE_STORAGE_OB_TABLET_ID_SET

#include <stdint.h>
#include "lib/hash/ob_hashset.h"
#include "lib/lock/ob_bucket_lock.h"
#include "common/ob_tablet_id.h"

namespace oceanbase
{
namespace storage
{
class ObTabletIDSet final
{
  typedef common::hash::ObHashSet<common::ObTabletID, hash::NoPthreadDefendMode> NonLockedHashSet;
public:
  ObTabletIDSet();
  ~ObTabletIDSet();
  ObTabletIDSet(const ObTabletIDSet&) = delete;
  ObTabletIDSet &operator=(const ObTabletIDSet&) = delete;
public:
  int init(const uint64_t bucket_lock_bucket_cnt, const uint64_t tenant_id);
  int set(const common::ObTabletID &tablet_id);
  int erase(const common::ObTabletID &tablet_id);
  int clear() { return id_set_.clear(); }
  int64_t size() const { return id_set_.size(); }

  template<class Operator>
  int foreach(Operator &op)
  {
    int ret = common::OB_SUCCESS;
    bool locked = false;
    while (OB_SUCC(ret) && !locked) {
      common::ObBucketTryRLockAllGuard lock_guard(bucket_lock_);
      if (OB_FAIL(lock_guard.get_ret()) && common::OB_EAGAIN != ret) {
        STORAGE_LOG(WARN, "fail to lock all tablet id set", K(ret));
      } else if (common::OB_EAGAIN == ret) {
        // try again after 1ms sleep.
        ob_usleep(1000);
        ret = common::OB_SUCCESS;
      } else {
        locked = true;
        NonLockedHashSet::const_iterator iter = id_set_.begin();
        for (; OB_SUCC(ret) && iter != id_set_.end(); ++iter) {
          const ObTabletID &tablet_id = iter->first;
          if (OB_FAIL(op(tablet_id))) {
            STORAGE_LOG(WARN, "fail to do operator", K(ret), K(tablet_id));
          }
        }
      }
    }
    return ret;
  }
  void destroy();
private:
  NonLockedHashSet id_set_;
  common::ObBucketLock bucket_lock_;
  bool is_inited_;
};
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_ID_SET
