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

#ifndef OCEANBASE_STORAGE_TABLELOCK_OB_MEM_CTX_TABLE_LOCK_
#define OCEANBASE_STORAGE_TABLELOCK_OB_MEM_CTX_TABLE_LOCK_

#include "share/scn.h"
#include "storage/memtable/mvcc/ob_mvcc_ctx.h"
#include "storage/tablelock/ob_table_lock_common.h"
#include "storage/tablelock/ob_table_lock_callback.h"
#include "storage/tablelock/ob_arena_object_pool.h"
#include "storage/ob_i_table.h"

namespace oceanbase
{
namespace transaction
{
namespace tablelock
{
class ObLockMemtable;

class ObMemCtxLockOpLinkNode : public common::ObDLinkBase<ObMemCtxLockOpLinkNode>
{
public:
  ObMemCtxLockOpLinkNode()
    : lock_op_(),
      logged_(false)
  {}
  void set_logged() { logged_ = true; }
  bool is_logged() const { return logged_; }
  int init(const ObTableLockOp &op_info);
  bool is_valid() const { return lock_op_.is_valid(); }
  TO_STRING_KV(K_(lock_op), K_(logged));
public:
  ObTableLockOp lock_op_;
  struct {
    bool logged_;
  };
};

typedef common::ObDList<ObMemCtxLockOpLinkNode> ObLockNodeList;
class ObLockMemCtx
{
  using RWLock = common::SpinRWLock;
  using WRLockGuard = common::SpinWLockGuard;
  using RDLockGuard = common::SpinRLockGuard;
private:
  static const int64_t MAGIC_NUM = -0xBEEF;
public:
  ObLockMemCtx(common::ObIAllocator &allocator) :
      node_pool_(allocator),
      callback_pool_(allocator),
      lock_list_(),
      is_killed_(false),
      max_durable_scn_(),
      memtable_handle_() {}
  ObLockMemCtx() = delete;
  ~ObLockMemCtx() { reset(); }
  int init(storage::ObTableHandleV2 &handle);
  int get_lock_memtable(ObLockMemtable *&memtable);
  void reset();
  void set_log_synced(ObMemCtxLockOpLinkNode *lock_op, const share::SCN &scn);

  int add_lock_record(
      const ObTableLockOp &lock_op,
      ObMemCtxLockOpLinkNode *&lock_op_node,
      const bool logged = false);
  void remove_lock_record(
      const ObTableLockOp &lock_op);
  void remove_lock_record(
      ObMemCtxLockOpLinkNode *lock_op);
  int check_lock_need_replay(
      const share::SCN &scn,
      const ObTableLockOp &lock_op,
      bool &need_replay);
  int check_lock_exist(
      const ObLockID &lock_id,
      const ObTableLockOwnerID &owner_id,
      const ObTableLockMode mode,
      const ObTableLockOpType op_type,
      bool &is_exist,
      ObTableLockMode &lock_mode_in_same_trans) const;
  // wait all the trans that modify with a smaller schema_version finished.
  int check_modify_schema_elapsed(
      const ObLockID &lock_id,
      const int64_t schema_version);
  // wait all the trans that modify with a smaller timestamp finished.
  int check_modify_time_elapsed(
      const ObLockID &lock_id,
      const int64_t timestamp);
  int iterate_tx_obj_lock_op(ObLockOpIterator &iter) const;
  int clear_table_lock(
      const bool is_committed,
      const share::SCN &commit_version,
      const share::SCN &commit_scn);
  int rollback_table_lock(const ObTxSEQ seq_no);
  void *alloc_lock_op_callback();
  void free_lock_op_callback(void *cb);
  int get_table_lock_store_info(ObTableLockInfo &table_lock_info);
  // used by deadlock detector to kill the trans.
  void set_killed()
  { is_killed_ = true; }
  // used to check whether the tx is killed by deadlock detector.
  bool is_killed() const
  { return is_killed_; }
private:
  void *alloc_lock_op();
  void free_lock_op(void *op);
  void free_lock_op_(void *op);
  void print() const;
  void rollback_table_lock_(const ObTxSEQ seq_no);
  int commit_table_lock_(const share::SCN &commit_version, const share::SCN &commit_scn);
  void abort_table_lock_();
private:
  // for performance.
  ObArenaObjPool<ObMemCtxLockOpLinkNode, 1> node_pool_;
  ObArenaObjPool<ObOBJLockCallback, 1> callback_pool_;
  // protect the lock_list_
  RWLock list_rwlock_;
  // the lock list of this tx.
  ObLockNodeList lock_list_;
  // the flag of whether the tx is killed.
  // used by deadlock detector.
  bool is_killed_;
  share::SCN max_durable_scn_;
  // the lock memtable pointer point to LS lock table's memtable.
  storage::ObTableHandleV2 memtable_handle_;
};

}
}
}

#endif
