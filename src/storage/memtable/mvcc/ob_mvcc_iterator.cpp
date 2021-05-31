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
#include "storage/transaction/ob_trans_ctx.h"
#include "common/ob_clock_generator.h"

namespace oceanbase {
using namespace storage;
using namespace transaction;
using namespace common;
namespace memtable {

ObMvccValueIterator::ObMvccValueIterator()
    : is_inited_(false),
      ctx_(NULL),
      value_(NULL),
      version_iter_(NULL),
      last_trans_version_(INT64_MAX),
      skip_compact_(false)
{}

ObMvccValueIterator::~ObMvccValueIterator()
{}

int ObMvccValueIterator::init(const ObIMvccCtx& ctx, const ObTransSnapInfo& snapshot_info, const ObMemtableKey* key,
    const ObMvccRow* value, const ObQueryFlag& query_flag, const bool skip_compact)
{
  int ret = OB_SUCCESS;
  skip_compact_ = skip_compact;
  reset();
  int64_t lock_for_read_start = ObClockGenerator::getClock();
  // snapshot version equal INT64_MAX is unexpected
  // maybe sub_trans_begin not called
  if (OB_UNLIKELY(INT64_MAX == snapshot_info.get_snapshot_version())) {
    ret = OB_ERR_UNEXPECTED;
    if (value) {
      TRANS_LOG(ERROR, "unexpected snapshot version", K(ret), K(snapshot_info), K(*value), K(ctx));
    } else {
      TRANS_LOG(ERROR, "unexpected snapshot version", K(ret), K(snapshot_info), K(ctx));
    }
  } else if (OB_ISNULL(value)) {
    // row not exist
    is_inited_ = true;
  } else if (query_flag.iter_uncommitted_row()) {
    ctx_ = &ctx;
    value_ = value;
    is_inited_ = true;
    version_iter_ = value->get_list_head();
  } else if (read_by_sql_no(ctx, snapshot_info, const_cast<ObMvccRow*>(value), query_flag)) {
    ctx_ = &ctx;
    value_ = value;
    is_inited_ = true;
  } else {
    // read by snapshot
    ctx_ = &ctx;
    value_ = value;
    // not need lock_for_read when row is not locked
    if (value->row_lock_.is_locked() && OB_FAIL(value->lock_for_read(key, const_cast<ObIMvccCtx&>(ctx)))) {
      TRANS_LOG(WARN, "wait row lock fail", K(ret), K(ctx), KP(value), K(*value));
    } else if (OB_FAIL(find_start_pos(snapshot_info.get_snapshot_version()))) {
      TRANS_LOG(WARN, "fail to find start pos for iterator", K(ret));
    } else {
      if (GCONF.enable_sql_audit) {
        mark_trans_node_for_elr(snapshot_info.get_snapshot_version(), query_flag.is_prewarm());
      }
      // set has_read_relocated_row flag true when reading relocated row
      if (value->is_has_relocated()) {
        const_cast<ObIMvccCtx*>(&ctx)->set_has_read_relocated_row();
      }
      is_inited_ = true;
    }
  }
  // stat lock for read time
  const_cast<ObIMvccCtx&>(ctx).add_lock_for_read_elapse(ObClockGenerator::getClock() - lock_for_read_start);
  return ret;
}

void ObMvccValueIterator::mark_trans_node_for_elr(const int64_t read_snapshot, const bool is_prewarm)
{
  if (NULL != version_iter_) {
    // do not set barrier info when transaction node is ELR node
    if (!is_prewarm && !version_iter_->is_elr()) {
      version_iter_->clear_safe_read_barrier();
      ObMemtableCtx* curr_mt_ctx = static_cast<ObMemtableCtx*>(const_cast<ObIMvccCtx*>(ctx_));
      transaction::ObTransCtx* trans_ctx = curr_mt_ctx->get_trans_ctx();
      if (NULL != trans_ctx) {
        if (!trans_ctx->is_bounded_staleness_read() && curr_mt_ctx->is_for_replay()) {
          TRANS_LOG(WARN, "strong consistent read follower", K(*trans_ctx), K(ctx_));
        }
        version_iter_->set_safe_read_barrier(trans_ctx->is_bounded_staleness_read());
        version_iter_->set_inc_num(trans_ctx->get_trans_id().get_inc_num());
      }
      version_iter_->set_snapshot_version_barrier(read_snapshot);
    }
  }
}

int ObMvccValueIterator::find_start_pos(const int64_t read_snapshot)
{
  int ret = OB_SUCCESS;

  do {
    if (OB_FAIL(find_trans_node_below_version(read_snapshot, ctx_->get_is_safe_read()))) {
      TRANS_LOG(WARN, "fail to find trans node", K(ret), K(read_snapshot));
    } else if (OB_FAIL(check_trans_node_readable(read_snapshot))) {
      if (OB_ERR_SHARED_LOCK_CONFLICT == ret) {
        const int64_t abs_stmt_timeout = ctx_->get_abs_expired_time();
        if (OB_UNLIKELY(observer::SS_STOPPING == GCTX.status_) || OB_UNLIKELY(observer::SS_STOPPED == GCTX.status_)) {
          // rewrite ret
          ret = OB_SERVER_IS_STOPPING;
          TRANS_LOG(WARN, "observer is stopped", K(ret));
        } else if (ObTimeUtility::current_time() >= abs_stmt_timeout) {
          print_conflict_trace_log();
          break;
        } else if (ctx_->is_can_elr()) {
          PAUSE();
        } else {
          usleep(WAIT_COMMIT_US);
        }
      }
    }
  } while (OB_ERR_SHARED_LOCK_CONFLICT == ret);

  return ret;
}

int ObMvccValueIterator::find_trans_node_below_version(const int64_t read_snapshot, const bool is_safe_read)
{
  int ret = OB_SUCCESS;

  ObMvccTransNode* iter = value_->get_list_head();
  TRANS_LOG(DEBUG, "start to get iter", K(*value_), KP(iter), K(read_snapshot));

  version_iter_ = NULL;

  while (OB_SUCC(ret) && NULL != iter && NULL == version_iter_) {
    TRANS_LOG(DEBUG, "get iter", K(*iter));
    if (OB_FAIL(iter->try_cleanout(value_))) {
      TRANS_LOG(ERROR, "fail to cleanout tnode", K(ret), K(*iter));
    } else if (iter->is_aborted()) {
      // 1. skip aborted node
      iter = iter->prev_;
    } else if (iter->is_committed()) {
      if (read_snapshot >= iter->trans_version_) {
        // 2.1 save iterator if the version of iterator is less than read snapshot version
        version_iter_ = iter;
      } else {
        // 2.2 skip it
        iter = iter->prev_;
      }
    } else {
      if (is_safe_read) {
        // 3.1 skip uncommitted transaction node
        iter = iter->prev_;
      } else if (read_snapshot >= iter->trans_version_) {
        // 3.2 save iterator if the version of iterator is less than read snapshot version
        version_iter_ = iter;
      } else {
        // 3.3 skip it
        iter = iter->prev_;
      }
    }
  }

  return ret;
}

int ObMvccValueIterator::check_trans_node_readable(const int64_t read_snapshot)
{
  int ret = OB_SUCCESS;

  if (NULL != version_iter_) {
    if (!version_iter_->is_committed()) {
      if (!version_iter_->is_elr()) {
        ret = OB_ERR_SHARED_LOCK_CONFLICT;
      } else {
        ObMemtableCtx* curr_mt_ctx = static_cast<ObMemtableCtx*>(const_cast<ObIMvccCtx*>(ctx_));
        if (NULL != curr_mt_ctx->get_trans_ctx()) {
          ret = const_cast<ObIMvccCtx*>(ctx_)->insert_prev_trans(version_iter_->ctx_descriptor_);
        } else {
          ret = OB_ERR_SHARED_LOCK_CONFLICT;
        }
      }
    }

    if (OB_SUCC(ret) && version_iter_->trans_version_ > read_snapshot) {
      ret = OB_ERR_SHARED_LOCK_CONFLICT;
      TRANS_LOG(INFO, "need retry search trans node", K(ret), K(*version_iter_), K(read_snapshot));
    }
  }

  return ret;
}

void ObMvccValueIterator::print_conflict_trace_log()
{
  ObMemtableCtx* curr_mt_ctx = static_cast<ObMemtableCtx*>(const_cast<ObIMvccCtx*>(ctx_));
  ObIMemtableCtx* conflict_ctx = NULL;

  if (NULL != curr_mt_ctx->get_ctx_map() && NULL != version_iter_) {
    const uint32_t ctx_id = version_iter_->get_ctx_descriptor();
    if (NULL == (conflict_ctx = curr_mt_ctx->get_ctx_map()->fetch(ctx_id))) {
      TRANS_LOG(INFO, "transaction ctx not exist", K(ctx_id), K(*version_iter_), K(*curr_mt_ctx));
    } else {
      static_cast<ObMemtableCtx*>(conflict_ctx)->set_need_print_trace_log();
      curr_mt_ctx->get_ctx_map()->revert(ctx_id);
    }
  }
}

bool ObMvccValueIterator::is_exist()
{
  return (NULL != version_iter_);
}

int ObMvccValueIterator::get_next_node(const void*& tnode)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", KP(this));
    ret = OB_NOT_INIT;
  } else {
    tnode = NULL;
    while (OB_SUCC(ret) && (NULL == tnode)) {
      bool is_lock_node = false;
      if (NULL == version_iter_ || version_iter_->trans_version_ <= ctx_->get_base_version()) {
        ret = OB_ITER_END;
      } else if (value_ && OB_FAIL(version_iter_->try_cleanout(value_))) {
        TRANS_LOG(ERROR, "fail to cleanout tnode", K(ret), K(*version_iter_));
      } else if (OB_FAIL(version_iter_->is_lock_node(is_lock_node))) {
        TRANS_LOG(WARN, "fail to check is lock node", K(ret), K(*version_iter_));
      } else if (!(version_iter_->is_aborted() || is_lock_node ||
                     (NDT_COMPACT == version_iter_->type_ && skip_compact_))) {
        tnode = static_cast<const void*>(version_iter_);
      }

      move_to_next_node();
    }
  }

  return ret;
}

void ObMvccValueIterator::move_to_next_node()
{
  if (OB_ISNULL(version_iter_)) {
  } else if (NDT_COMPACT == version_iter_->type_) {
    if (skip_compact_) {
      version_iter_ = version_iter_->prev_;
    } else {
      version_iter_ = NULL;
    }
  } else {
    version_iter_ = version_iter_->prev_;
  }
}

// check if read self transaction when fetch cursor
bool ObMvccValueIterator::read_by_sql_no(
    const ObIMvccCtx& ctx, const ObTransSnapInfo& snapshot_info, ObMvccRow* value, const ObQueryFlag& query_flag)
{
  int ret = OB_SUCCESS;
  bool can_read_by_sql_no = false;
  bool is_locked = false;

  ObMvccTransNode* iter = value->get_list_head();
  ObTransID iter_trans_id;
  const ObMvccRowCallback* mvcc_row_cb = nullptr;
  if (value->row_lock_.is_exclusive_locked_by(ctx.get_ctx_descriptor())) {
    can_read_by_sql_no = true;
  } else if (snapshot_info.is_cursor_or_nested()) {
    value->latch_.lock();
    is_locked = true;
    mvcc_row_cb = (OB_NOT_NULL(iter)) ? iter->get_mvcc_row_cb() : nullptr;
    if (NULL == mvcc_row_cb || iter->is_committed()) {
      can_read_by_sql_no = false;
    } else if (OB_FAIL(mvcc_row_cb->get_trans_id(iter_trans_id))) {
      TRANS_LOG(WARN, "mvcc row cb get trans id error", K(ret), K(*iter));
      can_read_by_sql_no = false;
    } else if (iter_trans_id != snapshot_info.get_trans_id()) {
      can_read_by_sql_no = false;
    } else {
      can_read_by_sql_no = true;
    }
  }
  if (can_read_by_sql_no) {
    if (query_flag.is_read_latest()) {
      version_iter_ = iter;
    } else {
      if (!is_locked) {
        value->latch_.lock();
        is_locked = true;
      }
      version_iter_ = nullptr;
      TRANS_LOG(DEBUG, "start to get iter", K(*value), KP(iter), K(snapshot_info));
      while (OB_NOT_NULL(iter)) {
        TRANS_LOG(DEBUG, "get iter", K(*iter));
        const ObMvccRowCallback* mvcc_row_cb = iter->get_mvcc_row_cb();
        // Tow Scenario:
        // find transaction node that meet sql no
        // find transction node that is committed
        if (iter->is_committed() || snapshot_info.get_read_sql_no() > mvcc_row_cb->get_sql_no()) {
          version_iter_ = iter;
          TRANS_LOG(DEBUG, "get version iter", K(*version_iter_));
          break;
        } else {
          iter = iter->prev_;
        }
      }
    }
  }

  if (is_locked) {
    value->latch_.unlock();
  }
  return can_read_by_sql_no;
}

void ObMvccValueIterator::reset()
{
  is_inited_ = true;
  ctx_ = NULL;
  value_ = NULL;
  version_iter_ = NULL;
  last_trans_version_ = INT64_MAX;
}

enum ObRowDml ObMvccValueIterator::get_first_dml()
{
  return NULL == value_ ? T_DML_UNKNOWN : value_->get_first_dml();
}

////////////////////////////////////////////////////////////////////////////////////////////////////

ObMvccRowIterator::ObMvccRowIterator()
    : is_inited_(false),
      ctx_(NULL),
      snapshot_info_(),
      query_flag_(),
      value_iter_(),
      query_engine_(NULL),
      query_engine_iter_(NULL)
{}

ObMvccRowIterator::~ObMvccRowIterator()
{
  reset();
}

int ObMvccRowIterator::init(ObQueryEngine& query_engine, const ObIMvccCtx& ctx, const ObTransSnapInfo& snapshot_info,
    const ObMvccScanRange& range, const ObQueryFlag& query_flag)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    reset();
  }
  if (OB_FAIL(query_engine.scan(range.start_key_,
          !range.border_flag_.inclusive_start(),
          range.end_key_,
          !range.border_flag_.inclusive_end(),
          snapshot_info.get_snapshot_version(),
          query_engine_iter_))) {
    TRANS_LOG(WARN, "query engine scan fail", K(ret));
  } else {
    ctx_ = &ctx;
    snapshot_info_ = snapshot_info;
    query_flag_ = query_flag;
    query_engine_ = &query_engine;
    query_engine_iter_->set_version(snapshot_info.get_snapshot_version());
    is_inited_ = true;
  }
  return ret;
}

int ObMvccRowIterator::get_next_row(
    const ObMemtableKey*& key, ObMvccValueIterator*& value_iter, uint8_t& iter_flag, const bool skip_compact)
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
    const ObMemtableKey* tmp_key = NULL;
    ObMvccRow* value = NULL;
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
    } else if (OB_FAIL(value_iter_.init(*ctx_, snapshot_info_, tmp_key, value, query_flag_, skip_compact))) {
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

  return ret;
}

void ObMvccRowIterator::reset()
{
  is_inited_ = false;
  ctx_ = NULL;
  snapshot_info_.reset();
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
    ObIQueryEngineIterator* iter = query_engine_iter_;
    key = iter->get_key();
    row = iter->get_value();
  }
  return ret;
}

int ObMvccRowIterator::try_purge(const ObTransSnapInfo& snapshot_info, const ObMemtableKey* key, ObMvccRow* row)
{
  int ret = OB_SUCCESS;
  bool purged = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (NULL == key || NULL == row) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(query_engine_->check_and_purge(key, row, snapshot_info.get_snapshot_version(), purged))) {
    STORAGE_LOG(ERROR, "check_and_purge", K(ret), K(key), K(row), K(snapshot_info));
  } else if (purged) {
    TRANS_LOG(TRACE, "RangePurger: purge", K(*key), K(row));
    EVENT_INC(MEMSTORE_ROW_PURGE_COUNT);
  }
  return ret;
}

int ObMvccRowIterator::get_end_gap_key(const ObTransSnapInfo& snapshot_info, const ObStoreRowkey*& key, int64_t& size)
{
  ObIQueryEngineIterator* iter = query_engine_iter_;
  bool is_reverse = iter->is_reverse_scan();
  return query_engine_->skip_gap(iter->get_key(), key, snapshot_info.get_snapshot_version(), is_reverse, size);
}
}  // namespace memtable
}  // namespace oceanbase
