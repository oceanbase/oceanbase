/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_LOCK_MEMTABLE_MGR_
#define OCEANBASE_STORAGE_OB_LOCK_MEMTABLE_MGR_

#include "storage/ob_i_memtable_mgr.h"

namespace oceanbase
{
namespace common
{
class ObTabletID;
}

namespace share
{
class ObLSID;
}

namespace memtable
{
}

namespace storage
{
class ObIMemtable;
class ObFreezer;
class ObTenantMetaMemMgr;
}

namespace transaction
{
namespace tablelock
{
class ObLockMemtable;

class ObLockMemtableMgr : public storage::ObIMemtableMgr
{
public:
  ObLockMemtableMgr();
  virtual ~ObLockMemtableMgr();

  // ================== Unified Class Method ==================
  //
  // Init the memtable mgr, we use logstream id to fetch the ls_ctx_mgr and t3m
  // to alloc the memtable.
  virtual int init(const common::ObTabletID &tablet_id,
                   const share::ObLSID &ls_id,
                   storage::ObFreezer *freezer,
                   storage::ObTenantMetaMemMgr *t3m) override;
  virtual void destroy() override;

  virtual int create_memtable(const storage::CreateMemtableArg &arg) override;

  virtual int reuse() override;

  DECLARE_VIRTUAL_TO_STRING;
private:
  const ObLockMemtable *get_memtable_(const int64_t pos) const;
private:
  virtual int release_head_memtable_(storage::ObIMemtable *imemtable,
                                     const bool force = false) override;

private:
  share::ObLSID ls_id_;
  common::ObQSyncLock lock_def_;
};

} // namespace tablelock
} // transaction
} // oceanbase

#endif
