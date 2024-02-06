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

#ifndef OCEANBASE_STORAGE_OB_TX_CTX_MEMTABLE_MGR
#define OCEANBASE_STORAGE_OB_TX_CTX_MEMTABLE_MGR

#include "storage/ob_i_memtable_mgr.h"
#include "storage/tx_table/ob_tx_ctx_memtable.h"

namespace oceanbase
{
namespace storage
{
// """
// Computer engineering is not limited to operating computer systems but is
// aimed at creating a broad way to design more comprehensive technological
// solutions.
//                                                   -- Great Architecturer
// """
//
// In the design of 4.0, multiple storage types with different behaviors from
// user data (sstable + memtable) are introduced at the log stream level, such
// as transaction context tables, data state tables, partition lists, etc,
// although they are different in data usage and user data The way to write
// memtable through the SQL layer call is different, but in terms of persistent
// storage, it is still hoped to be structurally consistent with the current
// storage structure for unified management, and to use sstable to achieve data
// persistence.
//
// After all, We need to adapt to the general storage type with LSM as the
// design prototype, the goal is a unified abstraction of the dump module.
//
class ObTxCtxMemtableMgr : public ObIMemtableMgr
{
public:
  ObTxCtxMemtableMgr() : ObIMemtableMgr(LockType::OB_SPIN_RWLOCK, &lock_def_) {}
  ~ObTxCtxMemtableMgr() {}
  void reset();

  // ================== Unified Class Method ==================
  //
  // Init the memtable mgr, we use logstream id to fetch the ls_ctx_mgr and t3m
  // to alloc the memtable.
  virtual int init(const common::ObTabletID &tablet_id,
                   const share::ObLSID &ls_id,
                   ObFreezer *freezer,
                   ObTenantMetaMemMgr *t3m) override;
  virtual void destroy() override;

  // create_memtable is used for creating the only memtable for CheckpointMgr
  virtual int create_memtable(const share::SCN last_replay_scn,
                              const int64_t schema_version,
                              const share::SCN newest_clog_checkpoint_scn,
                              const bool for_replay=false) override;

  const ObTxCtxMemtable *get_tx_ctx_memtable_(const int64_t pos) const;

  DECLARE_VIRTUAL_TO_STRING;
protected:
  virtual int release_head_memtable_(memtable::ObIMemtable *imemtable,
                                     const bool force) override;

  int unregister_from_common_checkpoint_(const ObTxCtxMemtable *memtable);
private:
  share::ObLSID ls_id_;
  common::SpinRWLock lock_def_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TX_CTX_MEMTABLE_MGR

