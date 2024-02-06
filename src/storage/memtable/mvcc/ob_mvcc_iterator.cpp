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

#include "lib/stat/ob_diagnose_info.h"

#include "storage/memtable/mvcc/ob_mvcc_iterator.h"
#include "storage/memtable/mvcc/ob_mvcc_ctx.h"
#include "storage/memtable/mvcc/ob_query_engine.h"
#include "storage/memtable/ob_memtable_data.h"
#include "storage/memtable/ob_memtable_context.h"
#include "storage/memtable/ob_row_conflict_handler.h"
#include "storage/tx/ob_trans_ctx.h"
#include "common/ob_clock_generator.h"

namespace oceanbase
{
using namespace storage;
using namespace share;
using namespace transaction;
using namespace common;
namespace memtable
{

int ObMvccValueIterator::init(ObMvccAccessCtx &ctx,
                              const ObMemtableKey *key,
                              ObMvccRow *value,
                              const ObQueryFlag &query_flag)
{
  int ret = OB_SUCCESS;
  reset();
  int64_t lock_for_read_start = ObClockGenerator::getClock();
  ctx_ = &ctx;
  if (OB_UNLIKELY(!ctx.get_snapshot_version().is_valid())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(value)) {
    // row not exist
    is_inited_ = true;
  } else if (query_flag.iter_uncommitted_row()) {
    value_ = value;
    is_inited_ = true;
    version_iter_ = value->get_list_head();
  } else {
    value_ = value;
    if (OB_FAIL(lock_for_read_(query_flag))) {
      TRANS_LOG(WARN, "fail to find start pos for iterator", K(ret));
    } else {
      is_inited_ = true;
    }
  }
  TRANS_LOG(TRACE, "value_iter.init", K(ret),
          KPC(value),
          KPC_(version_iter),
          K(query_flag.is_read_latest()),
          KPC(key),
          K(ctx),
          K(lbt()));
  return ret;
}

int ObMvccValueIterator::lock_for_read_(const ObQueryFlag &flag)
{
  int ret = OB_SUCCESS;
  int64_t lock_start_time = OB_INVALID_TIMESTAMP;
  // the head of the read position
  ObMvccTransNode *iter = value_->get_list_head();
  // the resolved mvcc read position
  version_iter_ = NULL;
  if (GCONF.enable_sql_audit) {
    lock_start_time =  ObClockGenerator::getClock();
  }

  while (OB_SUCC(ret) && NULL != iter && NULL == version_iter_) {
    if (OB_FAIL(lock_for_read_inner_(flag, iter))) {
      TRANS_LOG(WARN, "lock for read failed", K(ret));
    }
  }

  // add barrier snapshot version for defensive check
  if (NULL != version_iter_) {
    if (ctx_->is_weak_read()) {
      version_iter_->set_safe_read_barrier(true);
      version_iter_->set_snapshot_version_barrier(ctx_->snapshot_.version_,
                                                  ObMvccTransNode::WEAK_READ_BIT);
    } else if (!flag.is_prewarm() && !version_iter_->is_elr()) {
      version_iter_->set_snapshot_version_barrier(ctx_->snapshot_.version_,
                                                  ObMvccTransNode::NORMAL_READ_BIT);
    }
  }

  return ret;
}

void ObMvccValueIterator::lock_for_read_end(const int64_t lock_start_time, int64_t ret) const
{
  // TODO: Add ELR check back
  if (GCONF.enable_sql_audit && OB_INVALID_TIMESTAMP != lock_start_time) {
    const int64_t lock_use_time = ObClockGenerator::getClock() - lock_start_time;
    EVENT_ADD(MEMSTORE_WAIT_READ_LOCK_TIME, lock_use_time);
    if (OB_FAIL(ret)) {
      EVENT_INC(MEMSTORE_READ_LOCK_FAIL_COUNT);
    } else {
      EVENT_INC(MEMSTORE_READ_LOCK_SUCC_COUNT);
    }
  }
}

int ObMvccValueIterator::lock_for_read_inner_(const ObQueryFlag &flag,
                                              ObMvccTransNode *&iter)
{
  int ret = OB_SUCCESS;
  // Tip 0: We need notice that the difference between the snapshot_tx_id and
  //        the reader_tx_id. It respectively represents snapshot(eg: cursor)
  //        and reader(eg: reader txn) for the data.
  //
  //        For example, if Txn T1 write data into LS1 and read data from LS1 in
  //        SQL1 and Txn T2 read data from ls2 in SQL2. SQL1 will have snapshot
  //        for its data and SQL2 will not have snapshot and using reader for
  //        reading.
  //
  //        NB: For cursor, it will have its snapshot_tx_id different from
  //        reader_tx_id.
  const ObTransID &snapshot_tx_id = ctx_->snapshot_.tx_id_;
  const ObTransID &reader_tx_id = ctx_->tx_id_;
  const ObTxSEQ snapshot_seq_no = ctx_->snapshot_.scn_;

  const SCN snapshot_version = ctx_->get_snapshot_version();
  const bool read_latest = flag.is_read_latest();
  const ObTransID &data_tx_id = iter->get_tx_id();

  // NB: We need pay much attention to the order of the reads to the different
  // variables. Although we update the version before the state for the tnodes
  // and read the state before the version. It may appear that the compiled code
  // execution may rearrange its order and fail to obey its origin logic(You can
  // read the Dependency Definiation of the ARM architecture book to understand
  // it). So the synchronization primitive below is much important.
  const bool is_committed = iter->is_committed();
  const bool is_aborted = iter->is_aborted();
  const bool is_elr = iter->is_elr();
  const bool is_delayed_cleanout = iter->is_delayed_cleanout();
  const SCN scn = iter->get_scn();
  // Opt1: data is decided
  if ((is_committed || is_aborted || is_elr)
      // Opt2: data is not decided while we donot need cleanout
      || (!is_delayed_cleanout
          && (// Opt2.1: snapshot reads the data written by snapshot
            data_tx_id == ctx_->snapshot_.tx_id_ ||
            // Opt2.2: read reader's latest is matched
            (read_latest && data_tx_id == ctx_->tx_id_)))) {
    // Case 1: Cleanout can be skipped
    //         because inner tx read only care whether tx node rollbacked
    if (is_committed || is_elr) {
      // Case 2: Data is committed, so the state is decided
      const SCN data_version = iter->trans_version_.atomic_load();
      if (ctx_->get_snapshot_version() >= data_version) {
        // Case 2.1 Read the version if it is smaller than read version
        version_iter_ = iter;
      } else {
        // Case 2.2: Otherwise, skip to the next version
        iter = iter->prev_;
      }
    } else if (is_aborted) {
      // Case 3: Data is aborted, so the state is decided. So we skip aborted data
      //         version
      iter = iter->prev_;
    } else {
      // Case 4: data is during execution
      if (read_latest && data_tx_id == ctx_->tx_id_) {
        // Case 4.1: data is written by the current txn and we also need read the
        //           latest data(eg: check existence), then we can read it if it
        //           is not undone
        version_iter_ = iter;
      } else if (ctx_->snapshot_.tx_id_ == data_tx_id) {
        if (iter->get_seq_no() <= ctx_->snapshot_.scn_) {
          // Case 4.2.1: data's sequence number is smaller or equal than the read
          //             txn's sequence number, so we can read it if it is not
          //             undone
          version_iter_ = iter;
        } else {
          // Case 4.1.2: data's sequence number is bigger than the read txn's
          //             sequence number and we need not read the latest data(to
          //             prevent Halloween problem), so we cannot read it
          iter = iter->prev_;
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "lock for read never go here", KPC(iter), KPC(ctx_), K(flag));
      }
    }
  } else {
    // Case 5: data is undecided, and we need cleanout its state, then check
    //         whether we can read based on the result of the cleanout. We need
    //         pay attention to the rule that data must only be cleanouted by
    //         one operation, whether cleanout or normal processing, so we use
    //         is_delay_cleanout() to check the state and we only cleanout it
    //         when data is delay cleanout
    bool can_read = false;
    SCN data_version;
    data_version.set_max();
    bool is_determined_state = false;

    // Opt3: we only cleanout tx node who is delay cleanout
    ObCleanoutOp *cleanout_op;
    ObCleanoutTxNodeOperation clean_tx_node_op(*value_, *iter, true /*need_row_latch*/);
    ObCleanoutNothingOperation clean_nothing_op;
    if (iter->is_delayed_cleanout()) {
      cleanout_op = &clean_tx_node_op;
    } else {
      cleanout_op = &clean_nothing_op;
    }

    ObReCheckTxNodeForLockForReadOperation recheck_tx_node_op(*iter, can_read, data_version, is_determined_state);
    ObReCheckOp *recheck_op = &recheck_tx_node_op;

    ObLockForReadArg lock_for_read_arg(*ctx_,
                                       data_tx_id,
                                       iter->get_seq_no(),
                                       read_latest,
                                       scn);

    if (OB_FAIL(ctx_->get_tx_table_guards().lock_for_read(lock_for_read_arg,
                                        can_read,
                                        data_version,
                                        is_determined_state,
                                        *cleanout_op,
                                        *recheck_op))) {
      TRANS_LOG(WARN, "lock for read failed", KPC(iter), K(lock_for_read_arg));
    } else if (can_read && ctx_->get_snapshot_version() >= data_version) {
      // Case 5.1: data is cleanout by lock for read and can be read by reader's
      //           snapshot
      int counter = 0;
      while (OB_SUCC(ret)
             && !ctx_->is_standby_read_
             && is_determined_state
             && !(iter->is_committed() || iter->is_aborted() || iter->is_elr())) {
        if (OB_FAIL(try_cleanout_tx_node_(iter))) {
          TRANS_LOG(WARN, "cleanout tx state failed", K(ret), KPC(value_), KPC(iter));
        }
        // NB: We rely on the row_scn and state on the tx node if we really can
        // read from the tx node. So if the tx node is not cleanout, we must
        // wait until the tx node is written back its state.
        if (0 == (++counter) % 10000
            && REACH_TIME_INTERVAL(1_s)) {
          TRANS_LOG(WARN, "waiting for the iter to be cleanout", K(ret),
                    KPC(iter), K(lock_for_read_arg), KPC(value_), KPC(ctx_));
        }
        usleep(10); // 10us
      }
      version_iter_ = iter;
    } else {
      // Case 5.1: data is cleanout by lock for read and cannot be read by
      //           reader's snapshot, so we need go to next
      iter = iter->prev_;
    }
  }

  return ret;
}

int ObMvccValueIterator::try_cleanout_tx_node_(ObMvccTransNode *tnode)
{
  int ret = OB_SUCCESS;
  auto tx_table_guards = ctx_->get_tx_table_guards();
  if (!(tnode->is_committed() || tnode->is_aborted())
      && tnode->is_delayed_cleanout()
      && OB_FAIL(tx_table_guards.cleanout_tx_node(tnode->tx_id_,
                                            *value_,
                                            *tnode,
                                            true     /*need_row_latch*/))) {
    TRANS_LOG(WARN, "cleanout tx state failed", K(ret), K(*value_), K(*tnode));
  }
  return ret;
}

void ObMvccValueIterator::get_trans_stat_row(concurrency_control::ObTransStatRow &row)
{
  if (OB_ISNULL(version_iter_)) {
    row.reset();
  } else {
    row.set(version_iter_->get_tx_version(),
            version_iter_->get_scn(),
            version_iter_->get_tx_id(),
            version_iter_->get_seq_no());
  }
}

int ObMvccValueIterator::get_next_node(const void *&tnode)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", KP(this));
    ret = OB_NOT_INIT;
  } else {
    tnode = NULL;
    while (OB_SUCC(ret) && (NULL == tnode)) {
      bool is_lock_node = false;
      if (NULL == version_iter_) {
        ret = OB_ITER_END;
      } else if (OB_FAIL(try_cleanout_tx_node_(version_iter_))) {
        TRANS_LOG(WARN, "fail to cleanout tnode", K(ret), K(*version_iter_));
      } else if (OB_FAIL(version_iter_->is_lock_node(is_lock_node))) {
        TRANS_LOG(WARN, "fail to check is lock node", K(ret), K(*version_iter_));
      } else if (!(version_iter_->is_aborted()              // skip abort version
                   || is_lock_node)) {                      // skip lock node
        tnode = static_cast<const void *>(version_iter_);
      }

      move_to_next_node_();
    }
  }

  return ret;
}

void ObMvccValueIterator::move_to_next_node_()
{
  if (OB_ISNULL(version_iter_)) {
  } else if (NDT_COMPACT == version_iter_->type_) {
    version_iter_ = NULL;
  } else {
    version_iter_ = version_iter_->prev_;
  }
}

int ObMvccValueIterator::check_row_locked(ObStoreRowLockState &lock_state)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", KP(this));
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(value_)) {
    ret = OB_SUCCESS;
    TRANS_LOG(WARN, "get value iter but mvcc row in it is null", K(ret));
  } else if (OB_FAIL(value_->check_row_locked(*ctx_, lock_state))){
    TRANS_LOG(WARN, "check row locked fail", K(ret), KPC(value_), KPC(ctx_), K(lock_state));
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

ObMvccRowIterator::ObMvccRowIterator()
    : is_inited_(false),
      ctx_(NULL),
      query_flag_(),
      value_iter_(),
      query_engine_(NULL),
      query_engine_iter_(NULL)
{
}

ObMvccRowIterator::~ObMvccRowIterator()
{
  reset();
}

int ObMvccRowIterator::init(
    ObQueryEngine &query_engine,
    ObMvccAccessCtx &ctx,
    const ObMvccScanRange &range,
    const ObQueryFlag &query_flag)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    reset();
  }
  if (OB_FAIL(query_engine.scan(
      range.start_key_,  !range.border_flag_.inclusive_start(),
      range.end_key_,    !range.border_flag_.inclusive_end(),
      ctx.snapshot_.version_.get_val_for_tx(),
      query_engine_iter_))) {
    TRANS_LOG(WARN, "query engine scan fail", K(ret));
  } else {
    ctx_ = &ctx;
    query_flag_ = query_flag;
    query_engine_ = &query_engine;
    query_engine_iter_->set_version(ctx.snapshot_.version_.get_val_for_tx());
    is_inited_ = true;
  }
  return ret;
}

int ObMvccRowIterator::get_next_row(
    const ObMemtableKey *&key,
    ObMvccValueIterator *&value_iter,
    uint8_t& iter_flag,
    ObStoreRowLockState &lock_state)
{
  int ret = OB_SUCCESS;
  uint8_t read_partial_row = 0;
  const bool skip_purge_memtable = false;
  iter_flag = 0;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", KP(this));
    ret = OB_NOT_INIT;
  }
  while (OB_SUCC(ret)) {
    const ObMemtableKey *tmp_key = NULL;
    ObMvccRow *value = NULL;
    if (OB_FAIL(query_engine_iter_->next(skip_purge_memtable))) {
      if (OB_ITER_END != ret) {
        TRANS_LOG(WARN, "query engine iter next fail", K(ret), "ctx", *ctx_);
      }
      iter_flag = read_partial_row;
    } else if (NULL == (tmp_key = query_engine_iter_->get_key())) {
      TRANS_LOG(ERROR, "unexpected key null pointer", "ctx", *ctx_);
      ret = OB_ERR_UNEXPECTED;
    } else if (NULL == (value = query_engine_iter_->get_value())) {
      TRANS_LOG(ERROR, "unexpected value null pointer", "ctx", *ctx_);
      ret = OB_ERR_UNEXPECTED;
    } else if (query_flag_.is_for_foreign_key_check()) {
      if (OB_FAIL(ObRowConflictHandler::check_foreign_key_constraint_for_memtable(*ctx_, value, lock_state))) {
        // we will throw error code if it's failed here, but we need to
        // post lock with key outside, so we have to set it here.
        key = tmp_key;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(value_iter_.init(*ctx_,
                                   tmp_key,
                                   value,
                                   query_flag_))) {
        TRANS_LOG(WARN, "value iter init fail", K(ret), "ctx", *ctx_, KP(value), K(*value));
      } else if (!value_iter_.is_exist()) {
        read_partial_row = (query_engine_iter_->get_iter_flag() & STORE_ITER_ROW_PARTIAL);
        // continue
      } else {
        key = tmp_key;
        value_iter = &value_iter_;
        iter_flag = (query_engine_iter_->get_iter_flag() | read_partial_row);
        break;
      }
    }
  }
  return ret;
}

void ObMvccRowIterator::reset()
{
  is_inited_ = false;
  ctx_ = NULL;
  query_flag_.reset();
  value_iter_.reset();
  if (NULL != query_engine_iter_) {
    query_engine_iter_->reset();
    query_engine_->revert_iter(query_engine_iter_);
    query_engine_iter_ = NULL;
  }
  query_engine_ = NULL;
}

int ObMvccRowIterator::get_key_val(const ObMemtableKey*& key, ObMvccRow*& row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ObIQueryEngineIterator *iter = query_engine_iter_;
    key = iter->get_key();
    row = iter->get_value();
  }
  return ret;
}

int ObMvccRowIterator::try_purge(const ObTxSnapshot &snapshot_info,
                                 const ObMemtableKey* key,
                                 ObMvccRow* row)
{
  int ret = OB_SUCCESS;
  bool purged = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (NULL == key || NULL == row) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(query_engine_->check_and_purge(key,
                                                    row,
                                                    snapshot_info.version_.get_val_for_tx(),
                                                    purged))) {
    STORAGE_LOG(ERROR, "check_and_purge", K(ret), K(key), K(row), K(snapshot_info));
  } else if (purged) {
    TRANS_LOG(TRACE, "RangePurger: purge", K(*key), K(row));
    EVENT_INC(MEMSTORE_ROW_PURGE_COUNT);
  }
  return ret;
}

} // namespace memtable
} // namespace oceanbase
