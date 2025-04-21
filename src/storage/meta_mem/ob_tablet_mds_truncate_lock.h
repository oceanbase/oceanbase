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

#ifndef OCEANBASE_STORAGE_OB_TABLET_MDS_TRUNCATE_LOCK_H
#define OCEANBASE_STORAGE_OB_TABLET_MDS_TRUNCATE_LOCK_H

#include "deps/oblib/src/lib/lock/ob_spin_rwlock.h"
#include "storage/ls/ob_ls_meta.h"

namespace oceanbase
{
namespace storage
{

using ObTabletMDSTruncateLock = common::ObLatch;
using ObTabletMdsSharedLockGuard = ObLSMeta::ObReentrantRLockGuard;
using ObTabletMdsExclusiveLockGuard = ObLSMeta::ObReentrantWLockGuard;

}
}

#endif