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

#include "storage/tablet/ob_tablet_id_set.h"

#include "lib/oblog/ob_log.h"
#include "storage/tablet/ob_tablet_common.h"

namespace oceanbase
{
namespace storage
{
ObTabletIDSet::ObTabletIDSet()
  : id_set_(),
    bucket_lock_(),
    is_inited_(false)
{
}

ObTabletIDSet::~ObTabletIDSet()
{
  destroy();
}

int ObTabletIDSet::init(const uint64_t bucket_lock_bucket_cnt, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  const int64_t bucket_num = common::hash::cal_next_prime(bucket_lock_bucket_cnt);
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K_(is_inited));
  } else if (OB_FAIL(id_set_.create(bucket_num, "TabletIDSetBkt", "TabletIDSetNode", tenant_id))) {
    LOG_WARN("fail to create tablet id set", K(ret));
  } else if (OB_FAIL(bucket_lock_.init(bucket_num, ObLatchIds::TABLET_BUCKET_LOCK,
      "TabletIDSetBkt", tenant_id))) {
    LOG_WARN("fail to init bucket lock", K(ret), K(bucket_num));
  } else {
    is_inited_ = true;
  }
  if (OB_UNLIKELY(!is_inited_)) {
    destroy();
  }
  return ret;
}

int ObTabletIDSet::set(const common::ObTabletID &tablet_id)
{
  ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_id.hash());
  return id_set_.set_refactored(tablet_id);
}

int ObTabletIDSet::erase(const common::ObTabletID &tablet_id)
{
  ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_id.hash());
  return id_set_.erase_refactored(tablet_id);
}

void ObTabletIDSet::destroy()
{
  bucket_lock_.destroy();
  id_set_.destroy();
  is_inited_ = false;
}
} // namespace storage
} // namespace oceanbase
