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

#ifndef OCEANBASE_STORAGE_TABLELOCK_OB_TABLE_LOCK_CALLBACK_
#define OCEANBASE_STORAGE_TABLELOCK_OB_TABLE_LOCK_CALLBACK_

#include "share/scn.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/memtable/mvcc/ob_mvcc_trans_ctx.h"
#include "storage/memtable/ob_memtable_mutator.h"
#include "storage/tablelock/ob_table_lock_common.h"
//#include "storage/tablelock/ob_mem_ctx_table_lock.h"

namespace oceanbase
{
namespace memtable
{
class ObIMvccCtx;
class ObMemtableKey;
}

namespace transaction
{
namespace tablelock
{
class ObLockMemtable;
class ObMemCtxLockOpLinkNode;

class ObOBJLockCallback final : public memtable::ObITransCallback
{
public:
  // JUST FOR PERFORMANCE.
  ObOBJLockCallback()
      : ctx_(nullptr),
      key_(),
      lock_op_(nullptr),
      memtable_(nullptr) {}

  ObOBJLockCallback(memtable::ObIMvccCtx &ctx, ObLockMemtable *memtable)
      : ctx_(&ctx),
        key_(),
        lock_op_(NULL),
        memtable_(memtable) {}

  virtual ~ObOBJLockCallback() {}
  void set(const memtable::ObMemtableKey &key,
           ObMemCtxLockOpLinkNode *lock_op)
  {
    key_.encode(key);
    lock_op_ = lock_op;
  }
  bool on_memtable(const storage::ObIMemtable * const memtable) override;
  storage::ObIMemtable* get_memtable() const override;
  virtual int del() override;
  transaction::ObTxSEQ get_seq_no() const override;
  bool is_table_lock_callback() const override { return true; }
  bool must_log() const;
  int64_t get_data_size() override { return 0; } // size of trans node.
  memtable::MutatorType get_mutator_type() const override
  { return memtable::MutatorType::MUTATOR_TABLE_LOCK; }
  const common::ObTabletID get_tablet_id();
  int get_redo(memtable::TableLockRedoDataNode &node);

  // write log
  int get_trans_id(ObTransID &trans_id) const;

  virtual int trans_commit() override;
  virtual int trans_abort() override;
  virtual int rollback_callback() override;
  virtual int print_callback() override;

private:
  int lock_abort_();
private:
  memtable::ObIMvccCtx *ctx_;
  memtable::ObMemtableKey key_;

  ObMemCtxLockOpLinkNode *lock_op_;
  // memctx will have the ref of memtable.
  // lock memtable of ls.
  ObLockMemtable *memtable_;
};

}

}

}


#endif /* OCEANBASE_STORAGE_TABLELOCK_OB_TABLE_LOCK_CALLBACK_ */
