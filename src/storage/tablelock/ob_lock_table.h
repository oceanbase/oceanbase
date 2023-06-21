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

#ifndef OCEANBASE_STORAGE_OB_LOCK_TABLE_H_
#define OCEANBASE_STORAGE_OB_LOCK_TABLE_H_
#include <stdint.h>
#include "lib/lock/ob_spin_lock.h"
#include "lib/worker.h"
#include "storage/ob_i_table.h"
#include "storage/tablelock/ob_obj_lock.h"
#include "logservice/ob_log_base_type.h"

namespace oceanbase
{
namespace obrpc
{
class ObBatchCreateTabletArg;
class ObBatchRemoveTabletArg;
}

namespace common
{
class ObTabletID;
}

namespace share
{
class ObLSID;
namespace schema
{
class ObTableSchema;
}
}

namespace storage
{
class ObLS;
struct ObStoreCtx;
class ObITable;
class ObStoreRow;
class ObStoreCtx;
}

namespace memtable
{
class ObMemtableCtx;
}

namespace transaction
{
namespace tablelock
{
struct ObLockParam;
class ObTableLockOp;
class ObLockMemtable;

class ObLockTable : public logservice::ObIReplaySubHandler,
                    public logservice::ObIRoleChangeSubHandler,
                    public logservice::ObICheckpointSubHandler
{
public:
  ObLockTable()
    : parent_(nullptr),
      is_inited_(false)
  {}
  ~ObLockTable() {}
  int init(storage::ObLS *parent);
  int prepare_for_safe_destroy();
  void destroy();
  int offline();
  int online();
  // create lock table tablet.
  int create_tablet(const lib::Worker::CompatMode compat_mode, const share::SCN &create_scn);
  // remove lock table tablet.
  int remove_tablet();
  // load lock for tablet.
  int load_lock();
  int get_lock_memtable(storage::ObTableHandleV2 &handle);
  // check whether the lock op is conflict with exist dml lock.
  // @param[in] ctx, the store ctx of current transaction.
  // @param[in] param, contain the parameters need.
  int check_lock_conflict(storage::ObStoreCtx &ctx,
                          const ObLockParam &param);
  // check whether the lock op is conflict with exist lock.
  // @param[in] mem_ctx, the memtable ctx of current transaction.
  // @param[in] lock_op, which lock op will try to execute.
  // @param[in] include_finish_tx, whether include the finished transaction.
  // @param[out] conflict_tx_set, contain the conflict transaction it.
  int check_lock_conflict(
      const memtable::ObMemtableCtx *mem_ctx,
      const ObTableLockOp &lock_op,
      ObTxIDSet &conflict_tx_set,
      const bool include_finish_tx = true);
  // lock a object
  // @param[in] ctx, store ctx for trans.
  // @param[in] param, contain the lock id, lock type and so on.
  int lock(storage::ObStoreCtx &ctx,
           const ObLockParam &param);
  // unlock a object
  // @param[in] ctx, store ctx for trans.
  // @param[in] unlock_op, contain the lock id, lock type and so on.
  int unlock(storage::ObStoreCtx &ctx,
             const ObLockParam &param);
  // get all the lock id in the lock map
  // @param[out] iter, the iterator returned.
  // int get_lock_id_iter(ObLockIDIterator &iter);
  int get_lock_id_iter(ObLockIDIterator &iter);

  // get the lock op iterator of a obj lock
  // @param[in] lock_id, which obj lock's lock op will be iterated.
  // @param[out] iter, the iterator returned.
  int get_lock_op_iter(const ObLockID &lock_id,
                       ObLockOpIterator &iter);
  // used by admin tool. remove lock records.
  // @param[in] op_info, the lock/unlock op will be removed by admin.
  int admin_remove_lock_op(const ObTableLockOp &op_info);
  // used by admin tool. update lock op status.
  // @param[in] op_info, the lock/unlock op will be update by admin.
  // @param[in] commit_version, set the commit version.
  // @param[in] commit_scn, set the commit logts.
  // @param[in] status, the lock op status will be set.
  int admin_update_lock_op(const ObTableLockOp &op_info,
                           const share::SCN &commit_version,
                           const share::SCN &commit_scn,
                           const ObTableLockOpStatus status);
  // check and clear paired lock ops which can be compacted,
  // and clear empty obj locks to recycle resources.
  // See the ObLockMemtable::check_and_clear_obj_lock for deatails.
  int check_and_clear_obj_lock(const bool force_compact);
  // for replay
  int replay(const void *buffer,
             const int64_t nbytes,
             const palf::LSN &lsn,
             const share::SCN &scn) override { return OB_SUCCESS; }
  // for checkpoint
  share::SCN get_rec_scn() override { return share::SCN::max_scn(); }
  int flush(share::SCN &rec_scn) override { return OB_SUCCESS; }
  // for role change
  void switch_to_follower_forcedly() override{};
  int switch_to_leader() override;
  int switch_to_follower_gracefully() override { return OB_SUCCESS; }
  int resume_leader() override { return OB_SUCCESS; }

private:
  // We use the method to recover the lock_table for reboot.
  int restore_lock_table_(storage::ObITable &sstable);
  int recover_(const blocksstable::ObDatumRow &row);
  int get_table_schema_(const uint64_t tenant_id,
                        share::schema::ObTableSchema &schema);

private:
  static const int64_t LOCKTABLE_SCHEMA_VERSION = 0;
  static const int64_t LOCKTABLE_SCHEMA_ROEKEY_CNT = 1;
  static const int64_t LOCKTABLE_SCHEMA_COLUMN_CNT = 2;
  storage::ObLS *parent_;
  bool is_inited_;
};

} // tablelock
} // transaction
} // oceanbase


#endif
