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
#include "storage/ob_arena_object_pool.h"
#include "storage/ob_i_table.h"
#include "storage/memtable/mvcc/ob_mvcc_ctx.h"
#include "storage/tablelock/ob_table_lock_common.h"
#include "storage/tablelock/ob_table_lock_callback.h"

namespace oceanbase
{

namespace memtable
{
class ObITransCallback;
class ObIMvccCtx;
};

namespace transaction
{
class ObMemtableCtxObjPool;

namespace tablelock
{
class ObLockMemtable;

class ObMemCtxLockOpLinkNode : public common::ObDLinkBase<ObMemCtxLockOpLinkNode>
{
public:
  ObMemCtxLockOpLinkNode()
    : lock_op_()
  {}
  int init(const ObTableLockOp &op_info);
  bool is_valid() const { return lock_op_.is_valid(); }
  TO_STRING_KV(K_(lock_op));
public:
  ObTableLockOp lock_op_;
};
typedef common::ObDList<ObMemCtxLockOpLinkNode> ObLockNodeList;

class ObMemCtxLockPrioOpLinkNode : public common::ObDLinkBase<ObMemCtxLockPrioOpLinkNode>
{
public:
  ObMemCtxLockPrioOpLinkNode()
    : prio_op_()
  {}
  int init(const ObTableLockOp &op_info, const ObTableLockPriority priority);
  bool is_valid() const { return prio_op_.is_valid(); }
  TO_STRING_KV(K_(prio_op));
public:
  ObTableLockPrioOp prio_op_;
};
typedef common::ObDList<ObMemCtxLockPrioOpLinkNode> ObPrioLockNodeList;

class ObLockMemCtx
{
  using RWLock = common::SpinRWLock;
  using WRLockGuard = common::SpinWLockGuard;
  using RDLockGuard = common::SpinRLockGuard;
private:
  static const int64_t MAGIC_NUM = -0xBEEF;
public:
  ObLockMemCtx(memtable::ObMemtableCtx &host) :
      host_(host),
      lock_list_(),
      is_killed_(false),
      max_durable_scn_(),
      memtable_handle_(),
      priority_list_(),
      add_lock_latch_() {}
  ObLockMemCtx() = delete;
  ~ObLockMemCtx() { reset(); }
  int init(ObLSTxCtxMgr *ls_tx_ctx_mgr);
  // for mintest
  int init(ObTableHandleV2 &handle);
  int get_lock_memtable(ObLockMemtable *&memtable);
  void reset();
  int add_lock_record(
      const ObTableLockOp &lock_op,
      ObMemCtxLockOpLinkNode *&lock_op_node);
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
      uint64_t lock_mode_cnt_in_same_trans[]) const;
  int64_t get_lock_op_count() { return lock_list_.get_size(); }
  int check_contain_tablet(ObTabletID tablet_id, bool &contain);
  // wait all the trans that modify with a smaller schema_version finished.
  int check_modify_schema_elapsed(
      const ObLockID &lock_id,
      const int64_t schema_version);
  // wait all the trans that modify with a smaller timestamp finished.
  int check_modify_time_elapsed(
      const ObLockID &lock_id,
      const int64_t timestamp);
  int iterate_tx_obj_lock_op(ObLockOpIterator &iter) const;
  int iterate_tx_lock_priority_list(ObPrioOpIterator &iter) const;
  int clear_table_lock(
      const bool is_committed,
      const share::SCN &commit_version,
      const share::SCN &commit_scn);
  int rollback_table_lock(const ObTxSEQ to_seq_no, const ObTxSEQ from_seq_no);
  int sync_log_succ(const share::SCN &scn);
  int get_table_lock_store_info(ObTableLockInfo &table_lock_info);
  int get_table_lock_for_transfer(ObTableLockInfo &table_lock_info, const ObIArray<ObTabletID> &tablet_list);
  // used by deadlock detector to kill the trans.
  void set_killed()
  { is_killed_ = true; }
  // used to check whether the tx is killed by deadlock detector.
  bool is_killed() const
  { return is_killed_; }
  int add_priority_record(
      const ObTableLockPrioArg &arg,
      const ObTableLockOp &lock_op,
      ObMemCtxLockPrioOpLinkNode *&prio_op_node);
  int prepare_priority_task(
      const ObTableLockPrioArg &arg,
      const ObTableLockOp &lock_op,
      ObMemCtxLockPrioOpLinkNode *&prio_op_node);
  void remove_priority_record(
      ObMemCtxLockPrioOpLinkNode *prio_op);
  void remove_priority_record(
      const ObTableLockOp &lock_op);
  int get_priority_array(ObTableLockPrioOpArray &prio_op_array);
  int clear_priority_list();

  ObOBJLockCallback *create_table_lock_callback(memtable::ObIMvccCtx &ctx, ObLockMemtable *memtable);

public:
 class AddLockGuard
 {
   // use to serialize multi thread try to add one lock for same transaction
 public:
   AddLockGuard(ObLockMemCtx &ctx): ctx_(NULL)
   {
     if (OB_SUCCESS == (ret_ = ctx.add_lock_latch_.lock())) {
       ctx_ = &ctx;
     }
   }
   ~AddLockGuard()
   {
     if (ctx_) {
       ctx_->add_lock_latch_.unlock();
     }
   }
   int ret() const { return ret_; }
 private:
   int ret_;
   ObLockMemCtx *ctx_;
 };
private:
  void print() const;
  int rollback_table_lock_(const ObTxSEQ to_seq_no, const ObTxSEQ from_seq_no);
  int commit_table_lock_(const share::SCN &commit_version, const share::SCN &commit_scn);
  void abort_table_lock_();

  void *alloc_lock_link_node_();
  void *alloc_table_lock_callback_();
  void free_lock_link_node_(void *ptr);
  void free_table_lock_callback_(memtable::ObITransCallback *cb);
  void *alloc_prio_link_node_();
  void free_prio_link_node_(void *ptr);
private:
  memtable::ObMemtableCtx &host_;
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
  // the priority list of this tx.
  ObPrioLockNodeList priority_list_;
protected:
  // serialze multiple thread try add lock for same transaction
  ObSpinLock add_lock_latch_;
};

}
}
}

#endif
