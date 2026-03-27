/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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