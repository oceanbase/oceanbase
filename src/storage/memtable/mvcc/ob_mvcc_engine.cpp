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

#include "common/storage/ob_sequence.h"
#include "storage/memtable/mvcc/ob_mvcc_engine.h"
#include "storage/memtable/mvcc/ob_mvcc_acc_ctx.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/memtable/ob_row_conflict_handler.h"
#include "storage/tx_table/ob_tx_table.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/access/ob_index_sstable_estimator.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace storage;
using namespace transaction;
namespace memtable
{

ObMvccEngine::ObMvccEngine()
    : is_inited_(false),
      kv_builder_(NULL),
      query_engine_(NULL),
      engine_allocator_(NULL),
      memtable_(NULL)
{
}

ObMvccEngine::~ObMvccEngine()
{
  destroy();
}

int ObMvccEngine::init(
    common::ObIAllocator *allocator,
    ObMTKVBuilder *kv_builder,
    ObQueryEngine *query_engine,
    ObMemtable *memtable)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    TRANS_LOG(WARN, "init twice", KP(this));
    ret = OB_INIT_TWICE;
  } else if (NULL == allocator
             || NULL == kv_builder
             || NULL == query_engine
             || NULL == memtable) {
    TRANS_LOG(WARN, "invalid param", K(allocator), K(kv_builder), K(query_engine), K(memtable));
    ret = OB_INVALID_ARGUMENT;
  } else {
    engine_allocator_ = allocator;
    kv_builder_ = kv_builder;
    query_engine_ = query_engine;
    memtable_ = memtable;
    is_inited_ = true;
  }
  if (OB_SUCCESS != ret && IS_NOT_INIT) {
    destroy();
  }
  return ret;
}

void ObMvccEngine::destroy()
{
  is_inited_ = false;
  kv_builder_ = NULL;
  query_engine_ = NULL;
  engine_allocator_ = NULL;
  memtable_ = NULL;
}

int ObMvccEngine::try_compact_row_when_mvcc_read_(const SCN &snapshot_version,
                                                  ObMvccRow &row)
{
  int ret = OB_SUCCESS;
  const int64_t latest_compact_ts = row.latest_compact_ts_;
  const int64_t WEAK_READ_COMPACT_THRESHOLD = 3 * 1000 * 1000;
  if (SCN::min_scn() >= snapshot_version) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "invalid snapshot version", K(ret), K(snapshot_version));
  } else if (SCN::max_scn() == snapshot_version
      || ObTimeUtility::current_time() < latest_compact_ts + WEAK_READ_COMPACT_THRESHOLD) {
    // do not compact row when merging
  } else {
    ObRowLatchGuard guard(row.latch_);
    if (OB_FAIL(row.row_compact(memtable_,
                                snapshot_version,
                                engine_allocator_))) {
      TRANS_LOG(WARN, "row compact error", K(ret), K(snapshot_version));
    }
  }
  return ret;
}

int ObMvccEngine::get(ObMvccAccessCtx &ctx,
                      const ObQueryFlag &query_flag,
                      const ObMemtableKey *parameter_key,
                      const share::ObLSID memtable_ls_id,
                      ObMemtableKey *returned_key,
                      ObMvccValueIterator &value_iter,
                      ObStoreRowLockState &lock_state)
{
  int ret = OB_SUCCESS;
  ObMvccRow *value = NULL;
  const bool for_read = true;
  const bool for_replay = false;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", KP(this));
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(parameter_key)) {
    TRANS_LOG(WARN, "invalid param");
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(query_engine_->get(parameter_key, value, returned_key))) {
    if (OB_LIKELY(OB_ENTRY_NOT_EXIST == ret)) {
      // rewrite ret
      ret = OB_SUCCESS;
    }
  } else if (!query_flag.is_prewarm() && value->need_compact(for_read, for_replay)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = try_compact_row_when_mvcc_read_(ctx.get_snapshot_version(), *value))) {
      TRANS_LOG(WARN, "fail to try to compact row", K(tmp_ret));
    }
  } else if (query_flag.is_for_foreign_key_check()) {
    ret = ObRowConflictHandler::check_foreign_key_constraint_for_memtable(ctx, value, lock_state);
  } else {
    // do nothing
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(value_iter.init(ctx,
                                returned_key,
                                value,
                                memtable_ls_id,
                                query_flag))) {
      TRANS_LOG(WARN, "ObMvccValueIterator init fail", KR(ret));
    }
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "get fail", KR(ret), K(ctx), KP(parameter_key), KP(&value_iter));
  }
  return ret;
}

int ObMvccEngine::scan(
    ObMvccAccessCtx &ctx,
    const ObQueryFlag &query_flag,
    const ObMvccScanRange &range,
    const share::ObLSID memtable_ls_id,
    ObMvccRowIterator &row_iter)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", KP(this));
    ret = OB_NOT_INIT;
  } else if (!range.is_valid()) {
    TRANS_LOG(WARN, "invalid param");
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(row_iter.init(*query_engine_,
                                   ctx,
                                   range,
                                   memtable_ls_id,
                                   query_flag))) {
    TRANS_LOG(WARN, "row_iter init fail", K(ret));
  } else {
    // do nothing
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "get fail", K(ret), K(ctx), K(range));
  }
  return ret;
}

int ObMvccEngine::scan(
    ObMvccAccessCtx &ctx,
    const ObMvccScanRange &range,
    const common::ObVersionRange &version_range,
    ObMultiVersionRowIterator &row_iter)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", KP(this));
    ret = OB_NOT_INIT;
  } else if (!range.is_valid()) {
    TRANS_LOG(WARN, "invalid param");
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(row_iter.init(*query_engine_,
                                   ctx,
                                   version_range,
                                   range))) {
    TRANS_LOG(WARN, "row_iter init fail", K(ret));
  } else {
    // do nothing
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "get fail", K(ret), K(ctx), K(range));
  }
  return ret;
}

int ObMvccEngine::estimate_scan_row_count(
    const transaction::ObTransID &tx_id,
    const ObMvccScanRange &range,
    ObPartitionEst &part_est) const
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", KP(this));
    ret = OB_NOT_INIT;
  } else if (!range.is_valid()) {
    TRANS_LOG(WARN, "invalid param");
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(query_engine_->estimate_row_count(
              tx_id,
              range.start_key_,  !range.border_flag_.inclusive_start(),
              range.end_key_,    !range.border_flag_.inclusive_end(),
              part_est.logical_row_count_, part_est.physical_row_count_))) {
    TRANS_LOG(WARN, "query engine estimate row count fail", K(ret));
  }

  return ret;
}

int ObMvccEngine::check_row_locked(ObMvccAccessCtx &ctx,
                                   const ObMemtableKey *key,
                                   ObStoreRowLockState &lock_state,
                                   ObRowState &row_state)
{
  int ret = OB_SUCCESS;
  ObMemtableKey stored_key;
  ObMvccRow *value = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "mvcc_engine not init", K(this));
  } else if (OB_FAIL(query_engine_->get(key, value, &stored_key))) {
    if (OB_LIKELY(OB_ENTRY_NOT_EXIST == ret)) {
      // rewrite ret
      ret = OB_SUCCESS;
    }
  } else if (OB_FAIL(value->check_row_locked(ctx, lock_state, row_state))) {
    TRANS_LOG(WARN, "check row locked fail", K(ret), KPC(value), K(ctx), K(lock_state));
  }

  return ret;
}

int ObMvccEngine::create_kv(
    const ObMemtableKey *key,
    const bool is_insert,
    ObMemtableKey *stored_key,
    ObMvccRow *&value,
    bool &is_new_add)
{
  int64_t loop_cnt = 0;
  int ret = OB_SUCCESS;
  value = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "mvcc_engine not init", K(this));
  } else {
    is_new_add = false;
    while (OB_SUCCESS == ret && NULL == value) {
      ObStoreRowkey *tmp_key = nullptr;
      // We optimize the create_kv operation by skipping the first hash table
      // get for insert operation because it is unnecessary at most cases. Under
      // the concurrent inserts, we rely on the conflict on the hash table set
      // and the while loops for the next hash table get to maintain the origin
      // create_kv semantic
      if (!(0 == loop_cnt // is the first try in the loop
            && is_insert) // is insert dml operation
          && OB_SUCC(query_engine_->get(key, value, stored_key))) {
        if (NULL == value) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "get NULL value");
        }
      } else if (OB_FAIL(kv_builder_->dup_key(tmp_key,
                                              *engine_allocator_,
                                              key->get_rowkey()))) {
        TRANS_LOG(WARN, "key dup fail", K(ret));
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (OB_FAIL(stored_key->encode(tmp_key))) {
        TRANS_LOG(WARN, "key encode fail", K(ret));
      } else if (NULL == (value = (ObMvccRow *)engine_allocator_->alloc(sizeof(*value)))) {
        TRANS_LOG(WARN, "alloc ObMvccRow fail");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        value = new(value) ObMvccRow();
        ObRowLatchGuard guard(value->latch_);
        if (OB_SUCCESS == (ret = query_engine_->set(stored_key, value))) {
          is_new_add = true;
        } else if (OB_ENTRY_EXIST == ret) {
          ret = OB_SUCCESS;
          value = NULL;
        } else {
          value = NULL;
        }
      }
      loop_cnt++;
    }
    if (loop_cnt > 2) {
      if (REACH_TIME_INTERVAL(10 * 1000 * 1000) || 3 == loop_cnt) {
        TRANS_LOG(ERROR, "unexpected loop cnt when preparing kv", K(ret), K(loop_cnt), K(*key), K(*stored_key));
      }
    }
  }

  return ret;
}

int ObMvccEngine::mvcc_write(storage::ObStoreCtx &ctx,
                             const transaction::ObTxSnapshot &snapshot,
                             ObMvccRow &value,
                             const ObTxNodeArg &arg,
                             ObMvccWriteResult &res)
{
  int ret = OB_SUCCESS;
  ObMvccTransNode *node = NULL;
  ObMemtableCtx *mem_ctx = ctx.mvcc_acc_ctx_.get_mem_ctx();

  if (OB_FAIL(build_tx_node_(arg, node))) {
    TRANS_LOG(WARN, "build tx node failed", K(ret), K(ctx), K(arg));
  } else if (OB_FAIL(value.mvcc_write(ctx,
                                      snapshot,
                                      *node,
                                      res))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret &&
        OB_TRANSACTION_SET_VIOLATION != ret) {
      TRANS_LOG(WARN, "mvcc write failed", K(ret), KPC(mem_ctx), K(arg));
    }
  } else {
    TRANS_LOG(DEBUG, "mvcc write succeed", K(ret), KPC(mem_ctx), K(arg), K(*node));
  }

  return ret;
}

int ObMvccEngine::mvcc_replay(const ObTxNodeArg &arg,
                              ObMvccReplayResult &res)
{
  int ret = OB_SUCCESS;
  ObMvccTransNode *node = NULL;

  if (OB_FAIL(build_tx_node_(arg, node))) {
    TRANS_LOG(WARN, "build tx node failed", K(ret), K(arg));
  } else {
    res.tx_node_ = node;
    TRANS_LOG(DEBUG, "mvcc replay succeed", K(ret), K(arg));
  }
  return ret;
}

int ObMvccEngine::build_tx_node_(const ObTxNodeArg &arg,
                                 ObMvccTransNode *&node)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(kv_builder_->dup_data(node, *engine_allocator_, arg.data_))) {
    TRANS_LOG(WARN, "MvccTranNode dup fail", K(ret), "node", node);
  } else {
    node->tx_id_ = arg.tx_id_;
    node->trans_version_ = SCN::max_scn();
    node->modify_count_ = arg.modify_count_;
    node->acc_checksum_ = arg.acc_checksum_;
    node->version_ = arg.memstore_version_;
    node->scn_ = arg.scn_;
    node->seq_no_ = arg.seq_no_;
    node->prev_ = NULL;
    node->next_ = NULL;
  }

  return ret;
}

int ObMvccEngine::ensure_kv(const ObMemtableKey *stored_key,
                            ObMvccRow *value)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "mvcc_engine not init", K(this));
  } else {
    ObRowLatchGuard guard(value->latch_);
    if (OB_FAIL(query_engine_->ensure(stored_key,
                                      value))) {
      TRANS_LOG(WARN, "ensure_row fail", K(ret));
    }
  }
  return ret;
}

void ObMvccEngine::mvcc_undo(ObMvccRow *value)
{
  value->mvcc_undo();
}
}
}
