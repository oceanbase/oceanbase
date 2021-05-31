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

#include "ob_mvcc_trans_ctx.h"
#include "ob_mvcc_ctx.h"
#include "ob_mvcc_row.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/memtable/ob_memtable_context.h"
#include "storage/memtable/ob_memtable_data.h"
#include "storage/memtable/ob_memtable_util.h"
#include "lib/atomic/atomic128.h"
#include "storage/memtable/ob_lock_wait_mgr.h"
#include "storage/transaction/ob_trans_ctx.h"
#include "storage/transaction/ob_trans_part_ctx.h"
#include "ob_mvcc_ctx.h"
#include "storage/memtable/ob_memtable_interface.h"

namespace oceanbase {
using namespace common;
using namespace transaction;
namespace memtable {

static ObFakeStoreRowKey savepoint_fake_rowkey("savepoint", 9);

int ObTransCallbackMgr::rollback_to(const ObPartitionKey& pkey, const int32_t sql_no, const bool is_replay,
    const bool need_write_log, const int64_t max_durable_log_ts, bool& has_calc_checksum)
{
  int ret = OB_SUCCESS;

  if (!pkey.is_valid() || 0 > sql_no) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(pkey), K(sql_no));
  } else {
    bool hit = false;
    ObITransCallback* end = callback_list_.search_callback(sql_no, need_write_log, sub_trans_begin_, hit);

    if (OB_FAIL(rollback_calc_checksum_(is_replay, need_write_log, max_durable_log_ts, has_calc_checksum))) {
      TRANS_LOG(WARN, "calc checksum failed", K(ret), K(pkey), K(is_replay), K(max_durable_log_ts), K(need_write_log));
    } else if (!is_replay && OB_FAIL(rollback_pending_log_size_(need_write_log, end))) {
      TRANS_LOG(WARN, "fail to fix pending log size", K(ret), K(pkey), K(need_write_log), KP(end));
    } else {
      truncate_to_(end, !is_replay || hit);

      if (!is_replay && need_write_log) {
        if (OB_FAIL(register_savepoint_callback_(pkey, sql_no))) {
          TRANS_LOG(WARN, "register savepoint callback failed", K(ret), K(sql_no));
        }
      }
    }
  }

  return ret;
}

int ObTransCallbackMgr::rollback_calc_checksum_(
    const bool is_replay, const bool need_write_log, const int64_t max_durable_log_ts, bool& has_calc_checksum)
{
  int ret = OB_SUCCESS;
  has_calc_checksum = false;

  if (is_replay) {
    if (0 == max_durable_log_ts) {
      TRANS_LOG(ERROR, "max durable log id is unset after replay rollback log", K(max_durable_log_ts));
    } else if (OB_FAIL(calc_checksum_before_log_ts(max_durable_log_ts - 1))) {
      TRANS_LOG(WARN, "calc checksum before log id failed", K(max_durable_log_ts));
    } else {
      has_calc_checksum = true;
    }
  } else {
    if (need_write_log) {
      if (0 == max_durable_log_ts) {
        TRANS_LOG(ERROR, "max durable log id is unset after replay rollback log", K(max_durable_log_ts));
      } else if (OB_FAIL(calc_checksum_before_log_ts(max_durable_log_ts))) {
        TRANS_LOG(WARN, "calc checksum before log id failed", K(max_durable_log_ts));
      } else {
        has_calc_checksum = true;
      }
    }
  }

  return ret;
}

int ObTransCallbackMgr::rollback_pending_log_size_(const bool need_write_log, const ObITransCallback* const end)
{
  int ret = OB_SUCCESS;

  if (need_write_log) {
    clear_pending_log_size();
  } else {
    int64_t rollback_data_size = 0;
    if (OB_FAIL(fetch_rollback_data_size(end, rollback_data_size))) {
      TRANS_LOG(WARN, "fetch rollback data size failed", KP(end));
    } else {
      inc_pending_log_size(-1 * rollback_data_size);
    }
  }

  return ret;
}

int ObTransCallbackMgr::truncate_to(const int32_t sql_no)
{
  int ret = OB_SUCCESS;

  if (0 > sql_no) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(sql_no));
  } else {
    bool hit = false;
    ObITransCallback* end = callback_list_.search_callback(sql_no, false, sub_trans_begin_, hit);
    truncate_to_(end, hit);
  }

  return ret;
}

void ObTransCallbackMgr::truncate_to_(ObITransCallback* end, const bool change_sub_trans_end)
{
  lifo_callback(end, TCB_STMT_ABORT);
  lifo_callback(end, TCB_UNLOCK);
  if (change_sub_trans_end) {
    if (guard() != end) {
      sub_trans_begin_ = end;
    } else {
      sub_trans_begin_ = NULL;
    }
  }
  callback_list_.truncate(end);
}

int ObTransCallbackMgr::get_cur_max_sql_no(int64_t& sql_no)
{
  int ret = OB_SUCCESS;

  if (callback_list_.count() > 0) {
    ObITransCallback* tail = callback_list_.get_tail();
    if (OB_UNLIKELY(0 == tail->get_sql_no())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected sql no", K(ret));
    } else {
      sql_no = tail->get_sql_no();
    }
  }

  return ret;
}

int ObTransCallbackMgr::data_relocate(ObMemtable* dst_memtable, const int data_relocate_type)
{
  int ret = common::OB_SUCCESS;
  ObITransCallback* last_trans_node = guard()->get_prev();
  bool for_replay = for_replay_;
  bool need_lock_for_write = (for_replay_ ? false : true);
  if (REPLAY_REDO == data_relocate_type || LEADER_TAKEOVER == data_relocate_type ||
      TRANS_REPLAY_END == data_relocate_type) {
    for_replay = true;
    need_lock_for_write = false;
  } else if (TRANS_STMT == data_relocate_type || TRANS_END == data_relocate_type) {
    for_replay = false;
    need_lock_for_write = true;
  } else {
    TRANS_LOG(ERROR, "UNEXPECTED data relocate type", K(data_relocate_type));
  }
  if (NULL == dst_memtable) {
    TRANS_LOG(WARN, "invalid argument", KP(dst_memtable));
    ret = common::OB_INVALID_ARGUMENT;
  } else if (TRANS_STMT == data_relocate_type || REPLAY_REDO == data_relocate_type ||
             LEADER_TAKEOVER == data_relocate_type) {
    if (OB_FAIL(callback_list_.data_relocate_fifo_callback(guard(),
            last_trans_node,
            TCB_STMT_DATA_RELOCATE,
            for_replay,
            need_lock_for_write,
            dst_memtable,
            sub_trans_begin_))) {
      TRANS_LOG(WARN, "stmt data relocate fifo callback error", K(ret), KP(dst_memtable));
    }
  } else {
    if (TRANS_END == data_relocate_type) {
      if (OB_FAIL(callback_list_.data_relocate_fifo_callback(guard(),
              last_trans_node,
              TCB_TRANS_DATA_RELOCATE,
              for_replay,
              need_lock_for_write,
              dst_memtable,
              sub_trans_begin_))) {
        TRANS_LOG(WARN, "trans end relocate fifo callback error", K(ret), KP(dst_memtable));
      }
    } else if (TRANS_REPLAY_END == data_relocate_type) {
      if (OB_FAIL(callback_list_.data_relocate_fifo_callback(guard(),
              last_trans_node,
              TCB_TRANS_DATA_RELOCATE,
              for_replay,
              need_lock_for_write,
              dst_memtable,
              sub_trans_begin_))) {
        TRANS_LOG(WARN, "trans replay end relocate fifo callback error", K(ret), KP(dst_memtable));
      }
    } else {
      // do nothing
    }
  }
  if (OB_SUCC(ret)) {
    if (TRANS_STMT == data_relocate_type || LEADER_TAKEOVER == data_relocate_type ||
        REPLAY_REDO == data_relocate_type || TRANS_REPLAY_END == data_relocate_type) {
      ObITransCallback* end = last_trans_node->get_next();
      callback_list_.lifo_callback(end, guard(), TCB_STMT_ABORT, for_replay_);
      callback_list_.lifo_callback(end, guard(), TCB_UNLOCK, for_replay_);
      callback_list_.lifo_callback(end, guard(), TCB_RELOCATE_DELETE, for_replay_);
    } else {
      // do nothing
    }
  } else {
    if (TRANS_STMT == data_relocate_type || LEADER_TAKEOVER == data_relocate_type ||
        REPLAY_REDO == data_relocate_type) {
      callback_list_.lifo_callback(guard(), last_trans_node, TCB_STMT_ABORT, for_replay_);
      callback_list_.lifo_callback(guard(), last_trans_node, TCB_UNLOCK, for_replay_);
      callback_list_.lifo_callback(guard(), last_trans_node, TCB_RELOCATE_DELETE, for_replay_);
    } else {
      callback_list_.lifo_callback(guard(), last_trans_node, TCB_TRANS_ABORT, for_replay_);
      callback_list_.lifo_callback(guard(), last_trans_node, TCB_UNLOCK, for_replay_);
      callback_list_.lifo_callback(guard(), last_trans_node, TCB_RELOCATE_DELETE, for_replay_);
    }
  }

  return ret;
}

int ObTransCallbackMgr::remove_callback_for_uncommited_txn(ObMemtable* memtable, int64_t& cnt)
{
  int ret = common::OB_SUCCESS;
  bool move_forward = false;
  cnt = 0;

  if (OB_ISNULL(memtable)) {
    ret = common::OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "memtable is null", K(ret));
  } else if (OB_FAIL(callback_list_.remove_callback_fifo_callback(
                 guard(), guard(), memtable, sub_trans_begin_, move_forward, cnt))) {
    TRANS_LOG(WARN, "fifo remove callback fail", K(ret), K(*memtable));
  }

  if (move_forward) {
    sub_trans_begin_ = guard()->get_next();
  }

  return ret;
}

int ObTransCallbackMgr::calc_checksum_before_log_ts(const int64_t log_ts)
{
  int ret = OB_SUCCESS;

  if (INT64_MAX == log_ts) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "log ts is invalid", K(log_ts));
  } else if (OB_FAIL(callback_list_.calc_checksum_before_log_ts(guard(), guard(), log_ts))) {
    TRANS_LOG(WARN, "calc checksum with minor freeze failed", K(ret), K(log_ts));
  }

  return ret;
}

int ObTransCallbackMgr::fetch_rollback_data_size(const ObITransCallback* point, int64_t& rollback_size)
{
  int ret = OB_SUCCESS;
  rollback_size = 0;

  if (OB_FAIL(callback_list_.fetch_rollback_data_size(point, guard(), rollback_size))) {
    TRANS_LOG(WARN, "fetch rollback data size failed", K(ret), K(*point));
  }

  return ret;
}

void ObTransCallbackMgr::inc_pending_log_size(const int64_t size)
{
  if (!for_replay_) {
    int64_t old_size = ATOMIC_FAA(&pending_log_size_, size);
    int64_t new_size = ATOMIC_LOAD(&pending_log_size_);
    if (old_size < 0 || new_size < 0) {
      ObIMemtableCtx* mt_ctx = NULL;
      transaction::ObTransCtx* trans_ctx = NULL;
      if (NULL == (mt_ctx = static_cast<ObIMemtableCtx*>(&host_))) {
        TRANS_LOG(ERROR, "mt_ctx is null", K(size), K(old_size), K(new_size), K(host_));
      } else if (NULL == (trans_ctx = mt_ctx->get_trans_ctx())) {
        TRANS_LOG(ERROR, "trans ctx get failed", K(size), K(old_size), K(new_size), K(*mt_ctx));
      } else {
        TRANS_LOG(ERROR, "increase remaining data size less than 0!", K(size), K(old_size), K(new_size), K(*trans_ctx));
      }
    }
  }
}

int ObTransCallbackMgr::get_memtable_key_arr(transaction::ObMemtableKeyArray& memtable_key_arr, ObMemtable* memtable)
{
  int ret = common::OB_SUCCESS;

  if (OB_ISNULL(memtable)) {
    ret = common::OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "memtable is null", K(ret));
  } else if (OB_FAIL(callback_list_.iterate_memtablekey_fifo_callback(memtable_key_arr, memtable))) {
    if (common::OB_ITER_STOP == ret) {
      ret = common::OB_SUCCESS;
    } else {
      TRANS_LOG(WARN, "lifo callback get memtablekey fail", K(ret), K(memtable_key_arr));
    }
  } else {
    // do nothing
  }

  return ret;
}

int ObTransCallbackMgr::register_savepoint_callback_(const ObPartitionKey& pkey, const int32_t sql_no)
{
  int ret = OB_SUCCESS;
  const uint64_t table_id = pkey.get_table_id();
  const ObStoreRowkey& rowkey = savepoint_fake_rowkey.get_rowkey();
  ObMemtableKey* mt_key = NULL;
  ObMvccRow* row = NULL;
  ObMvccTransNode* node = NULL;
  const int64_t data_size = sizeof(ObMemtableData);
  ObRowData* old_row = NULL;
  if (OB_ISNULL(mt_key = host_.alloc_memtable_key())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "alloc memtable key failed", K(ret));
  } else if (OB_ISNULL(row = host_.alloc_mvcc_row())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "alloc mvcc row failed", K(ret));
  } else if (OB_ISNULL(node = host_.alloc_trans_node())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "alloc trans node failed", K(ret));
  } else if (OB_FAIL(mt_key->encode(table_id, &rowkey))) {
    TRANS_LOG(WARN, "encode memtable key failed", K(ret));
  } else if (OB_FAIL(host_.register_savepoint_cb(mt_key, row, node, data_size, old_row, sql_no))) {
    TRANS_LOG(WARN, "register savepoint callback failed", K(ret), K(host_), K(sql_no));
  } else {
    TRANS_LOG(INFO, "register savepoint callback success", K(host_), K(sql_no), K(*mt_key), K(*node));
  }
  return ret;
}

int ObTransCallbackMgr::mark_frozen_data(
    const ObMemtable* const frozen_memtable, const ObMemtable* const active_memtable, bool& marked, int64_t& cb_cnt)
{
  int ret = OB_SUCCESS;

  marked = false;
  cb_cnt = 0;

  if (marked_mt_ == frozen_memtable) {
    TRANS_LOG(INFO, "already marked, skip mark frozen data", KP(marked_mt_));
  } else if (sub_stmt_begin_) {
    ret = OB_EAGAIN;
    TRANS_LOG(INFO, "sub stmt exist, try later", K(ret));
  } else if (OB_FAIL(callback_list_.mark_frozen_data(frozen_memtable, active_memtable, marked, cb_cnt))) {
    TRANS_LOG(ERROR, "fail to mark frozen callback", K(ret));
  } else {
    marked_mt_ = frozen_memtable;
  }

  return ret;
}

int ObTransCallbackList::mark_frozen_data(
    const ObMemtable* const frozen_memtable, const ObMemtable* const active_memtable, bool& marked, int64_t& cb_cnt)
{
  int ret = OB_SUCCESS;
  const ObIMemtable* first_memtable = NULL;

  marked = false;
  cb_cnt = 0;

  if (OB_ISNULL(frozen_memtable) || OB_ISNULL(active_memtable)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "memtable is null", KP(frozen_memtable), KP(active_memtable), K(ret));
  } else if (FALSE_IT(first_memtable = get_first_memtable())) {
  } else if (OB_ISNULL(first_memtable) || first_memtable == active_memtable) {
    TRANS_LOG(INFO,
        "skip mark frozen data",
        K(callback_mgr_.get_ctx()),
        KP(first_memtable),
        KP(frozen_memtable),
        KP(active_memtable));
  } else {
    int64_t iter_cnt = 0;
    ObITransCallback* start = get_guard()->get_prev();
    ObITransCallback* end = get_guard();
    bool found = false;
    for (ObITransCallback* iter = start; NULL != iter && iter != end; iter = iter->get_prev()) {
      iter_cnt++;
      if (iter->on_memtable(frozen_memtable)) {
        found = true;
        if (iter->need_mark_logging() && iter->mark_for_logging()) {
          cb_cnt++;
        }
      } else if (found) {
        break;
      }
    }

    marked = found;

    if (!marked) {
      int64_t i = 0;
      for (ObITransCallback* iter = start; NULL != iter && iter != end; iter = iter->get_prev()) {
        TRANS_LOG(INFO, "debug: iter callback", K(*iter));
      }
    }

    TRANS_LOG(INFO, "iterate callbacks", K(callback_mgr_.get_ctx()), K(iter_cnt), K(cb_cnt), K(marked));
  }

  return ret;
}

void ObTransCallbackMgr::release_rowlocks()
{
  ATOMIC_STORE(&rowlocks_released_, true);
  get_global_lock_wait_mgr().wakeup(host_.get_ctx_descriptor());
}

bool ObTransCallbackMgr::is_rowlocks_released() const
{
  return ATOMIC_LOAD(&rowlocks_released_);
}

const ObMemtable* ObTransCallbackList::get_first_memtable()
{
  const ObMemtable* memtable = NULL;

  ObITransCallback* start = get_guard()->get_next();
  ObITransCallback* end = get_guard();
  for (ObITransCallback* iter = start; NULL != iter && iter != end; iter = iter->get_next()) {
    if (NULL != (memtable = iter->get_memtable())) {
      break;
    }
  }

  return memtable;
}

void ObTransCallbackList::truncate(ObITransCallback* end, const bool verbose)
{
  ObITransCallback* iter = get_tail();
  while (end != iter) {
    if (verbose) {
      TRANS_LOG(DEBUG, "truncate transaction node", K_(head), K(*end), "node", *iter);
    }
    iter->del();
    if (!iter->is_savepoint()) {
      callback_mgr_.get_ctx().callback_free(iter);
    }
    iter = get_tail();
    ATOMIC_DEC(&length_);
  }
}

int ObTransCallbackList::data_relocate_fifo_callback(ObITransCallback* start, const ObITransCallback* end,
    const int type, const bool for_replay, const bool need_lock_for_write, ObMemtable* memtable,
    ObITransCallback*& sub_trans_begin)
{
  int ret = common::OB_SUCCESS;
  ObITransCallback* tmp_sub_trans_begin = sub_trans_begin;
  if (NULL != start) {
    ObITransCallback* iter = start->get_next();
    while (NULL != iter && OB_SUCC(ret)) {
      ObITransCallback* next = iter->get_next();
      if (OB_FAIL(iter->callback(type, for_replay, need_lock_for_write, memtable))) {
        TRANS_LOG(WARN, "callback error", K(ret), K(type), K(for_replay));
      } else if (iter == tmp_sub_trans_begin) {
        tmp_sub_trans_begin = head_.get_prev();
      } else {
        // do nothing
      }
      if (OB_SUCC(ret)) {
        if (iter == end) {
          sub_trans_begin = tmp_sub_trans_begin;
          break;
        } else {
          if (TCB_DELETE == type || TCB_RELOCATE_DELETE == type) {
            if (!iter->is_savepoint()) {
              callback_mgr_.get_ctx().callback_free(iter);
            }
          }
          iter = next;
        }
      }
    }
  }
  return ret;
}

int ObTransCallbackList::remove_callback_fifo_callback(const ObITransCallback* start, const ObITransCallback* end,
    ObMemtable* release_memtable, const ObITransCallback* sub_trans_begin, bool& move_forward, int64_t& cnt)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t visted_cb_cnt = 0;
  int64_t same_mem_cb_cnt = 0;
  move_forward = false;
  cnt = 0;

  if (NULL != start) {
    for (ObITransCallback* iter = start->get_next(); OB_SUCCESS == tmp_ret && NULL != iter && iter != end;) {
      ObITransCallback* next = iter->get_next();
      bool unused = true;
      tmp_ret = iter->callback(TCB_REMOVE_CALLBACK, unused, unused, release_memtable);
      visted_cb_cnt++;
      if (OB_ITEM_NOT_MATCH != tmp_ret) {
        if (iter == sub_trans_begin) {
          move_forward = true;
        }
        length_--;
        cnt++;
        callback_mgr_.get_ctx().callback_free(iter);
        iter = next;
        same_mem_cb_cnt++;
      } else {
        iter = next;
        tmp_ret = OB_SUCCESS;
      }
    }

    ret = tmp_ret;
  }

  if (OB_SUCC(ret)) {
    TRANS_LOG(INFO,
        "remove callback",
        K(visted_cb_cnt),
        K(same_mem_cb_cnt),
        K(length_),
        K(callback_mgr_.get_ctx()),
        KP(release_memtable),
        K(cnt));
  } else {
    TRANS_LOG(ERROR, "remove callback fifo error", K(ret), KP(release_memtable), K(callback_mgr_.get_ctx()));
  }

  return ret;
}

int ObTransCallbackList::calc_checksum_before_log_ts(
    const ObITransCallback* start, const ObITransCallback* end, const int64_t log_ts)
{
  int ret = OB_SUCCESS;

  if (NULL != start) {
    for (ObITransCallback* iter = start->get_next(); OB_SUCC(ret) && NULL != iter && iter != end;
         iter = iter->get_next()) {
      if (OB_FAIL(iter->calc_checksum_before_log_ts(log_ts))) {
        if (OB_ITER_END != ret) {
          TRANS_LOG(ERROR, "calc checksum failed", K(log_ts), KP(iter));
        }
      }
    }

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

int ObTransCallbackList::fetch_rollback_data_size(
    const ObITransCallback* start, const ObITransCallback* end, int64_t& total_data_size)
{
  int ret = OB_SUCCESS;
  total_data_size = 0;

  if (NULL != start) {
    for (ObITransCallback* iter = start->get_next(); OB_SUCC(ret) && NULL != iter && iter != end;
         iter = iter->get_next()) {
      int64_t data_size = 0;
      if (OB_FAIL(iter->fetch_rollback_data_size(data_size))) {
        TRANS_LOG(ERROR, "fetch rollback data size failed", K(*iter));
      } else {
        total_data_size += data_size;
      }
    }
  }

  return ret;
}

int ObTransCallbackList::fifo_callback(
    ObITransCallback* start, const ObITransCallback* end, const int type, const bool for_replay)
{
  int ret = common::OB_SUCCESS;
  int64_t count = 0;
  const bool need_lock_for_write = (for_replay ? false : true);
  if (NULL != start) {
    for (ObITransCallback* iter = start->get_next(); common::OB_SUCCESS == ret && NULL != iter && iter != end;) {
      ObITransCallback* next = iter->get_next();
      ret = iter->callback(type, for_replay, need_lock_for_write);
      if ((++count & 0xFFFFF) == 0) {
        TRANS_LOG(WARN,
            "memtable fifo callback too long",
            K(count),
            KP(iter),
            KP(start),
            KP(end),
            K(type),
            K(for_replay),
            K(common::lbt()));
      }
      if (TCB_DELETE == type || TCB_RELOCATE_DELETE == type) {
        if (!iter->is_savepoint()) {
          callback_mgr_.get_ctx().callback_free(iter);
        }
      }
      iter = next;
    }
  }
  return ret;
}

int ObTransCallbackList::lifo_callback(
    ObITransCallback* start, ObITransCallback* end, const int type, const bool for_replay)
{
  int ret = common::OB_SUCCESS;
  int64_t count = 0;
  const bool need_lock_for_write = (for_replay ? false : true);
  if (NULL != start) {
    for (ObITransCallback* iter = start->get_prev(); common::OB_SUCCESS == ret && NULL != iter && iter != end;) {
      ObITransCallback* next = iter->get_prev();
      ret = iter->callback(type, for_replay, need_lock_for_write);
      if ((++count & 0xFFFFF) == 0) {
        TRANS_LOG(WARN,
            "memtable lifo callback too long",
            K(count),
            KP(iter),
            KP(start),
            KP(end),
            K(type),
            K(for_replay),
            K(common::lbt()));
      }
      if (TCB_DELETE == type || TCB_RELOCATE_DELETE == type) {
        if (!iter->is_savepoint()) {
          callback_mgr_.get_ctx().callback_free(iter);
        }
      }
      iter = next;
    }
  }
  return ret;
}

int ObMvccRowCallback::dec_pending_cb_count()
{
  int ret = OB_SUCCESS;

  if (marked_for_logging_) {
    if (OB_NOT_NULL(memtable_)) {
      if (OB_FAIL(memtable_->dec_pending_cb_count())) {
        TRANS_LOG(ERROR, "dec pending cb count failed", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "memtable is NULL", K(ret));
    }
  }

  return ret;
}

int ObMvccRowCallback::del()
{
  int ret = OB_SUCCESS;

  if (!is_savepoint()) {
    if (NULL != old_row_.data_) {
      ctx_.callback_free((void*)(old_row_.data_));
      old_row_.data_ = NULL;
    }
  }

  dec_pending_cb_count();

  ret = remove(this);
  return ret;
}

int ObMvccRowCallback::get_memtable_key(uint64_t& table_id, common::ObStoreRowkey& rowkey) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(key_.decode(table_id, rowkey))) {
    TRANS_LOG(WARN, "memtable key decode failed", K(ret));
  }
  return ret;
}

int ObMvccRowCallback::check_sequential_relocate(const ObMemtable* memtable, bool& is_sequential_relocate) const
{
  int ret = OB_SUCCESS;
  if (NULL == memtable_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected memstore", K(ret), KP(memtable_), K(*this));
  } else {
    const int64_t cur_memstore_snapshot_version = memtable_->get_snapshot_version();
    const int64_t dst_memstore_snapshot_version = memtable->get_snapshot_version();
    if (cur_memstore_snapshot_version < dst_memstore_snapshot_version) {
      is_sequential_relocate = true;
    } else if (cur_memstore_snapshot_version > dst_memstore_snapshot_version) {
      is_sequential_relocate = false;
    } else {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected snapshot version", K(ret), K(*memtable), K(*this));
    }
  }
  return ret;
}

int ObMvccRowCallback::callback(
    const int type, const bool for_replay, const bool need_lock_for_write, ObMemtable* memtable)
{
  int ret = OB_SUCCESS;

  if (TCB_STMT_DATA_RELOCATE == type || TCB_TRANS_DATA_RELOCATE == type) {
    common::ObTimeGuard tg("ObMvccRowCallback::callback:relocate", 50 * 1000);
    if (NULL == memtable) {
      TRANS_LOG(WARN, "invalid argument", KP(memtable));
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_UNLIKELY(is_savepoint())) {
      if (OB_FAIL(ctx_.register_savepoint_cb(*this, memtable))) {
        TRANS_LOG(WARN, "register savepoint cb failed when relocating", K(ret), K(*this));
      } else {
        TRANS_LOG(INFO, "register savepoint cb when relocating", K(*this));
      }
    } else {
      ObRowLatchGuard guard(value_.latch_);
      ret = data_relocate(for_replay, need_lock_for_write, memtable);
    }
  } else {
    ObRowLatchGuard guard(value_.latch_);
    if (TCB_TRANS_COMMIT == type) {
      ret = trans_commit(for_replay);
    } else if (TCB_TRANS_ABORT == type) {
      ret = trans_abort();
    } else if (TCB_STMT_COMMIT == type) {
      ret = stmt_commit(false);
    } else if (TCB_HALF_STMT_COMMIT == type) {
      ret = stmt_commit(true);
    } else if (TCB_STMT_ABORT == type) {
      ret = stmt_abort();
    } else if (TCB_SUB_STMT_ABORT == type) {
      ret = sub_stmt_abort();
    } else if (TCB_UNLOCK == type) {
      if (ctx_.is_can_elr()) {
        ret = row_unlock_v2();
      } else {
        ret = row_unlock();
      }
    } else if (TCB_PENDING == type) {
      ret = row_pending();
    } else if (TCB_LINK == type) {
      ret = row_link();
    } else if (TCB_CRC == type) {
      ret = row_crc();
    } else if (TCB_CRC2 == type) {
      ret = row_crc2();
    } else if (TCB_CRC4 == type) {
      ret = row_crc4();
    } else if (TCB_DELETE == type) {
      ret = row_delete();
    } else if (TCB_RELOCATE_DELETE == type) {
      ret = row_delete();
      (void)ctx_.inc_truncate_cnt();
    } else if (TCB_REMOVE_CALLBACK == type) {
      if (memtable == memtable_) {
        ret = row_remove_callback();
      } else {
        ret = OB_ITEM_NOT_MATCH;
      }
    } else if (TCB_ELR_TRANS_PREPARING == type) {
      ret = elr_trans_preparing();
    } else if (TCB_PRINT_CALLBACK == type) {
      ret = print_callback();
    } else {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "invalid callback type", K(ret), K(type), K(for_replay));
    }
  }
  return ret;
}

int ObMvccRowCallback::print_callback()
{
  TRANS_LOG(INFO, "print callback", K(*this));
  return OB_SUCCESS;
}

int ObMvccRowCallback::merge_memtable_key(ObMemtableKeyArray& memtable_key_arr, ObMemtableKey& memtable_key)
{
  int ret = common::OB_SUCCESS;

  int64_t count = memtable_key_arr.count();
  int64_t i = 0;
  for (; i < count; i++) {
    if (memtable_key_arr.at(i).get_table_id() == memtable_key.get_table_id() &&
        memtable_key_arr.at(i).get_hash_val() == memtable_key.hash()) {
      break;
    }
  }
  if (i == count) {
    ObMemtableKeyInfo memtable_key_info;
    if (OB_FAIL(memtable_key_info.init(memtable_key.get_table_id(), memtable_key.hash()))) {
      TRANS_LOG(WARN, "memtable key info init fail", K(ret));
    } else {
      memtable_key_info.set_row_lock(&value_.row_lock_);
      memtable_key.to_string(memtable_key_info.get_buf(), ObMemtableKeyInfo::MEMTABLE_KEY_INFO_BUF_SIZE);
      if (OB_FAIL(memtable_key_arr.push_back(memtable_key_info))) {
        TRANS_LOG(WARN, "memtable_key_arr push item fail", K(ret), K(memtable_key_arr), K(memtable_key_info));
      }
    }
  }

  return ret;
}

int ObMvccRowCallback::callback(ObMemtableKeyArray& memtable_key_arr, ObMemtable* memtable)
{
  int ret = common::OB_SUCCESS;

  if (OB_UNLIKELY(is_savepoint())) {
    // do nothing
  } else if (OB_ISNULL(memtable)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "memtable is null", K(ret));
  } else if (memtable != memtable_) {
    ret = common::OB_ITER_STOP;
  } else if (OB_ISNULL(key_.get_rowkey())) {
    ret = common::OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "static_cast key to ObMemtableKey* error", K(ret), "context", *this);
  } else if (OB_FAIL(merge_memtable_key(memtable_key_arr, key_))) {
    TRANS_LOG(WARN, "memtable_key_arr push item fail", K(ret), K(key_));
  } else {
    // do nothing
  }

  return ret;
}

int ObMvccRowCallback::elr_trans_preparing()
{
  if (NULL != tnode_) {
    value_.elr(ctx_.get_ctx_descriptor(), ctx_.get_commit_version(), (ObMemtableKey*)&key_);
  }
  return OB_SUCCESS;
}

int ObMvccRowCallback::row_unlock_v2()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_savepoint())) {
    TRANS_LOG(INFO, "row_unlock_v2: ignore do savepoint callback", K(*this));
  } else if (is_write_lock_) {
    const uint32_t exclusive_id = value_.row_lock_.get_exclusive_uid();
    if (value_.ctx_descriptor_ != exclusive_id) {
      TRANS_LOG(INFO,
          "different ctx descriptor, maybe current transaction abort",
          K(ret),
          K(exclusive_id),
          K_(ctx),
          K_(value));
    }
    if (ctx_.get_ctx_descriptor() != exclusive_id) {
      TRANS_LOG(DEBUG, "current trans's lock has been unlocked", K(exclusive_id), K_(ctx), K_(value));
      value_.dec_elr_trans_count();
    } else {
      value_.set_ctx_descriptor(0);
      value_.row_lock_.exclusive_unlock((ObMemtableKey*)&key_, ctx_.get_ctx_descriptor());
    }
    is_write_lock_ = false;
  }
  return ret;
}

int ObMvccRowCallback::row_unlock()
{
  if (OB_UNLIKELY(is_savepoint())) {
    TRANS_LOG(INFO, "row_unlock: ignore do savepoint callback", K(*this));
  } else if (is_write_lock_) {
    value_.row_lock_.exclusive_unlock((ObMemtableKey*)&key_, ctx_.get_ctx_descriptor());
    is_write_lock_ = false;
  }
  return OB_SUCCESS;
}

int ObMvccRowCallback::row_pending()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_savepoint())) {
    TRANS_LOG(INFO, "row_pending: ignore do savepoint callback", K(*this));
  } else {
    if (NULL != tnode_) {
      if (INT64_MAX == ctx_.get_trans_version()) {
        unlink_trans_node();
      } else if (OB_FAIL(tnode_->fill_trans_version(ctx_.get_trans_version()))) {
        TRANS_LOG(WARN, "fill trans version failed", K(ret), K_(ctx));
      } else {
        // do nothing
      }
    }
  }
  return ret;
}

int ObMvccRowCallback::row_link()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_savepoint())) {
    TRANS_LOG(INFO, "row_link: ignore do savepoint callback", K(*this));
  } else if (NULL != tnode_) {
    ObMvccTransNode* unused = NULL;
    if (OB_FAIL(tnode_->fill_trans_version(ctx_.get_trans_version()))) {
      TRANS_LOG(WARN, "fill trans version failed", K(ret), K_(ctx));
    } else if (OB_FAIL(link_and_get_next_node(unused))) {
      TRANS_LOG(WARN, "link trans node failed", K(ret));
    } else {
      set_stmt_committed(true);
    }
    if (OB_SUCCESS == ret && !is_write_lock_) {
      bool can_lock = false;
      const uint32_t exclusive_uid = value_.row_lock_.get_exclusive_uid();
      if (0 == exclusive_uid) {
        can_lock = true;
      } else {
        ObMemtableCtx& mt_ctx = static_cast<ObMemtableCtx&>(ctx_);
        MemtableIDMap* id_map = mt_ctx.get_ctx_map();
        ObIMemtableCtx* ctx = id_map->fetch(exclusive_uid);
        if (NULL != ctx) {
          if (ctx->get_trans_version() < ctx_.get_trans_version()) {
            value_.set_ctx_descriptor(0);
            value_.row_lock_.exclusive_unlock(&key_, exclusive_uid);
            can_lock = true;
          }
          id_map->revert(exclusive_uid);
        }
      }
      if (can_lock) {
        const uint32_t ctx_descriptor = ctx_.get_ctx_descriptor();
        if (OB_FAIL(value_.row_lock_.exclusive_lock(ctx_descriptor, ObTimeUtility::current_time() + 1000000))) {
          TRANS_LOG(WARN, "exclusive lock failed", K(ret), K(ctx_descriptor), K(exclusive_uid));
        } else {
          value_.set_ctx_descriptor(ctx_descriptor);
          is_write_lock_ = true;
        }
      }
    }
  }
  if (OB_SUCCESS != ret) {
    TRANS_LOG(ERROR, "row link failed", K(ret), K(ctx_));
  }
  return ret;
}

int ObMvccRowCallback::get_trans_id(ObTransID& trans_id) const
{
  int ret = OB_SUCCESS;
  ObMemtableCtx* mem_ctx = static_cast<ObMemtableCtx*>(&ctx_);
  ObTransCtx* trans_ctx = NULL;

  if (OB_ISNULL(mem_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected mem ctx", K(ret));
  } else if (OB_ISNULL(trans_ctx = mem_ctx->get_trans_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected trans ctx", K(ret), K(ctx_));
  } else {
    trans_id = trans_ctx->get_trans_id();
  }

  return ret;
}

ObTransCtx* ObMvccRowCallback::get_trans_ctx() const
{
  int ret = OB_SUCCESS;
  ObMemtableCtx* mem_ctx = static_cast<ObMemtableCtx*>(&ctx_);
  ObTransCtx* trans_ctx = NULL;

  if (OB_ISNULL(mem_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected mem ctx", K(ret));
  } else if (OB_ISNULL(trans_ctx = mem_ctx->get_trans_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected trans ctx", K(ret), K(ctx_));
  } else {
    // do nothing
  }

  return trans_ctx;
}

int ObMvccRowCallback::row_crc()
{
  if (is_relocate_out_) {
    TRANS_LOG(WARN, "row callback is relocated", K(*this), K(lbt()));
  }
  if (OB_UNLIKELY(is_savepoint())) {
    TRANS_LOG(INFO, "row_crc: ignore do savepoint callback", K(*this));
  } else {
    if (NULL != key_.get_rowkey() && NULL != tnode_) {
      ctx_.add_crc(&key_, &value_, tnode_, data_size_);
    }
  }
  return OB_SUCCESS;
}

int ObMvccRowCallback::row_crc2()
{
  if (is_relocate_out_) {
    TRANS_LOG(WARN, "row callback is relocated", K(*this), K(lbt()));
  }
  if (OB_UNLIKELY(is_savepoint())) {
    TRANS_LOG(INFO, "row_crc2: ignore do savepoint callback", K(*this));
  } else {
    if (NULL != tnode_) {
      ctx_.add_crc2(&value_, tnode_, data_size_);
    }
  }
  return OB_SUCCESS;
}

int ObMvccRowCallback::row_crc4()
{
  if (OB_UNLIKELY(is_savepoint())) {
    TRANS_LOG(INFO, "row_crc4: ignore do savepoint callback", K(*this));
  } else {
    if (NULL != tnode_) {
      ctx_.add_crc4(&value_, tnode_, data_size_, log_ts_);
    }
  }
  return OB_SUCCESS;
}

int ObMvccRowCallback::row_remove_callback()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_savepoint())) {
  } else if (OB_FAIL(value_.remove_callback(*this))) {
    TRANS_LOG(ERROR, "remove callback from trans node failed", K(ret));
  }

  if (OB_SUCC(ret)) {
    ret = row_crc4();
  }
  tmp_ret = row_delete();

  if (OB_SUCCESS == ret && OB_SUCCESS != tmp_ret) {
    ret = tmp_ret;
  }

  return ret;
}

static storage::ObRowDml get_dml_type(ObMvccTransNode* node)
{
  return NULL == node ? storage::T_DML_UNKNOWN : reinterpret_cast<ObMemtableDataHeader*>(node->buf_)->dml_type_;
}

int ObMvccRowCallback::trans_commit(const bool for_replay)
{
  int ret = OB_SUCCESS;
  ObMvccTransNode* prev = NULL;
  ObMvccTransNode* next = NULL;
  const bool for_read = false;
  if (OB_UNLIKELY(is_savepoint())) {
    TRANS_LOG(INFO, "trans_commit: ignore do savepoint callback", K(*this));
  } else if (NULL != tnode_) {
    if (OB_FAIL(tnode_->fill_trans_version(ctx_.get_commit_version()))) {
      TRANS_LOG(WARN, "fill trans version failed", K(ret), K_(ctx));
    } else if (OB_FAIL(link_and_get_next_node(next))) {
      TRANS_LOG(WARN, "link trans node failed", K(ret));
    } else {
      value_.update_max_trans_version(ctx_.get_commit_version());
      set_stmt_committed(true);
      if (NULL == tnode_->prev_) {
        // frist node
        value_.set_first_dml(get_dml_type(tnode_));
      }
      value_.set_last_dml(get_dml_type(tnode_));
      if (for_replay) {
        // verify current node checksum by previous node
        prev = tnode_->prev_;
        if (NULL == prev) {
          // do nothing
        } else if (prev->is_committed() && prev->version_ == tnode_->version_ &&
                   prev->modify_count_ + 1 == tnode_->modify_count_) {
          if (OB_FAIL(tnode_->verify_acc_checksum(prev->acc_checksum_))) {
            TRANS_LOG(ERROR, "current row checksum error", K(ret), K(value_), K(*prev), K(*tnode_));
            if (ObServerConfig::get_instance().ignore_replay_checksum_error) {
              // rewrite ret
              ret = OB_SUCCESS;
            }
          }
        } else {
          // do nothing
        }
        if (OB_SUCC(ret)) {
          // verify next node checksum by current node
          if (NULL == next) {
            // do nothing
          } else if (next->is_committed() && tnode_->version_ == next->version_ &&
                     tnode_->modify_count_ + 1 == next->modify_count_) {
            if (OB_FAIL(next->verify_acc_checksum(tnode_->acc_checksum_))) {
              TRANS_LOG(ERROR, "next row checksum error", K(ret), K(value_), K(*tnode_), K(*next));
              if (ObServerConfig::get_instance().ignore_replay_checksum_error) {
                // rewrite ret
                ret = OB_SUCCESS;
              }
            }
          } else {
            // do nothing
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(value_.trans_commit(ctx_.get_commit_version(), *tnode_))) {
          TRANS_LOG(WARN, "mvcc trans ctx trans commit error", K(ret), K_(ctx), K_(value));
        } else {
          const int64_t MAX_TRANS_NODE_CNT = 2 * GCONF._ob_elr_fast_freeze_threshold;
          if (value_.total_trans_node_cnt_ >= MAX_TRANS_NODE_CNT) {
            ctx_.set_contain_hotspot_row();
          }
          (void)ATOMIC_FAA(&value_.update_since_compact_, 1);
          if (value_.need_compact(for_read, for_replay)) {
            if (for_replay) {
              if (0 != ctx_.get_read_snapshot() && INT64_MAX != ctx_.get_read_snapshot()) {
                ctx_.row_compact(&value_, ctx_.get_read_snapshot());
              }
            } else {
              ctx_.row_compact(&value_, INT64_MAX - 100);
            }
          }
          ctx_.after_link_trans_node(&key_, &value_);
        }
      }
    }
  }
  return ret;
}

int ObMvccRowCallback::trans_abort()
{
  if (OB_UNLIKELY(is_savepoint())) {
    TRANS_LOG(INFO, "trans_abort: ignore do savepoint callback", K(*this));
  } else {
    if (NULL != tnode_) {
      tnode_->set_aborted();
    }
    unlink_trans_node();
  }
  return OB_SUCCESS;
}

int ObMvccRowCallback::stmt_commit(const bool half_commit)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_savepoint())) {
    TRANS_LOG(INFO, "stmt_commit: ignore do savepoint callback", K(*this));
  } else {
    ObMvccTransNode* unused = NULL;
    if (value_.row_lock_.is_exclusive_locked_by(ctx_.get_ctx_descriptor())) {
      ret = link_and_get_next_node(unused);
      if (!half_commit) {
        set_stmt_committed(true);
      }
    }
  }
  return ret;
}

int ObMvccRowCallback::stmt_abort()
{
  if (OB_UNLIKELY(is_savepoint())) {
    TRANS_LOG(INFO, "stmt_abort: ignore do savepoint callback", K(*this));
  } else {
    if (NULL != tnode_) {
      tnode_->set_aborted();
    }
    unlink_trans_node();
  }
  return OB_SUCCESS;
}

int ObMvccRowCallback::sub_stmt_abort()
{
  if (NULL != tnode_) {
    tnode_->set_aborted();
  }
  unlink_trans_node();
  return OB_SUCCESS;
}

int ObMvccRowCallback::get_redo(RedoDataNode& redo_node)
{
  int ret = OB_SUCCESS;
  if (NULL == key_.get_rowkey() || NULL == tnode_) {
    ret = OB_ENTRY_NOT_EXIST;
  } else if (!is_link_) {
    ret = OB_STATE_NOT_MATCH;
    TRANS_LOG(ERROR, "get_redo: trans_nod not link", K(ret), K(*this));
  } else {
    uint32_t last_acc_checksum = 0;
    if (NULL != tnode_->prev_) {
      last_acc_checksum = tnode_->prev_->acc_checksum_;
    } else {
      last_acc_checksum = value_.acc_checksum_;
      if (0 != value_.acc_checksum_) {
        TRANS_LOG(ERROR, "acc checksum is not 0", K_(value));
      }
    }
    tnode_->cal_acc_checksum(last_acc_checksum);
    const ObMemtableDataHeader* mtd = reinterpret_cast<const ObMemtableDataHeader*>(tnode_->buf_);
    ObRowData new_row;
    new_row.set(mtd->buf_, (int32_t)(data_size_ - sizeof(*mtd)));
    redo_node.set(&key_,
        old_row_,
        new_row,
        mtd->dml_type_,
        tnode_->modify_count_,
        tnode_->acc_checksum_,
        tnode_->version_,
        sql_no_,
        generate_redo_flag(is_savepoint_));
  }
  return ret;
}

int ObMvccRowCallback::link_and_get_next_node(ObMvccTransNode*& next)
{
  int ret = OB_SUCCESS;
  if (NULL == tnode_) {
  } else if (is_link_) {
    if (tnode_->modify_count_ > 0 && NULL == tnode_->prev_) {
      TRANS_LOG(DEBUG, "trans node maybe not linked", K_(ctx), K(*tnode_), K_(value));
    }
  } else {
    undo_ = value_;
    if (UINT32_MAX == tnode_->modify_count_) {
      tnode_->modify_count_ = value_.modify_count_;
    } else {
      if (value_.modify_count_ < tnode_->modify_count_) {
        value_.modify_count_ = tnode_->modify_count_;
      }
    }
    if (OB_ISNULL(memtable_)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "memtable_ is NULL", K(ret), K_(ctx));
    } else if (OB_FAIL(value_.insert_trans_node(ctx_, *tnode_, memtable_->get_allocator(), next))) {
      TRANS_LOG(ERROR, "insert trans node failed", K(ret), K_(ctx));
    } else {
      value_.update_row_meta();
      is_link_ = true;
    }
  }
  return ret;
}

int ObMvccRowCallback::link_trans_node()
{
  bool ret = OB_SUCCESS;
  if (!is_savepoint()) {
    ObMvccTransNode* unused = NULL;
    ret = link_and_get_next_node(unused);
  }
  return ret;
}

void ObMvccRowCallback::unlink_trans_node()
{
  int ret = OB_SUCCESS;
  if (!is_savepoint() && is_link_) {
    if (NULL == tnode_) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "trans node is NULL", K(ret), K_(ctx), K_(value));
    } else if (tnode_->is_committed()) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unlink committed trans node", K(ret), K_(ctx), K_(value), K(*tnode_));
    } else if (FALSE_IT(value_.unset_overflow_node(tnode_))) {
    } else if (OB_FAIL(value_.unlink_trans_node(ctx_, *tnode_, undo_))) {
      TRANS_LOG(ERROR, "unlink trans node failed", K(ret), K_(ctx), K_(value), K(*tnode_));
    } else {
      is_link_ = false;
    }
  }
}

int ObMvccRowCallback::row_delete()
{
  return del();
}

int ObMvccRowCallback::data_relocate(const bool for_replay, const bool need_lock_for_write, ObMemtable* dst_memtable)
{
  common::ObTimeGuard tg("ObMvccRowCallback::data_relocate", 50 * 1000);
  int ret = OB_SUCCESS;
  value_.set_relocated();

  if (OB_FAIL(dst_memtable->row_relocate(for_replay, need_lock_for_write, need_fill_redo_, ctx_, this))) {
    TRANS_LOG(WARN, "ObMemtable row relocate error", K(ret), K(for_replay), K_(ctx));
  } else {
    tg.click();
    is_relocate_out_ = true;
    if (NULL != tnode_) {
      (void)ctx_.inc_relocate_cnt();
      if (!stmt_committed_) {
        if (EXECUTE_COUNT_PER_SEC(32)) {
          TRANS_LOG(INFO, "row relocating, but current stmt not end", "callback", *this, K(for_replay), K_(ctx));
        }
      }
      TRANS_LOG(
          DEBUG, "ObMemtable row relocate success", "trans_node", *tnode_, "callback", *this, K(for_replay), K_(ctx));
    } else {
      TRANS_LOG(DEBUG, "ObMemtable row relocate success, no trans_node", "callback", *this, K(for_replay), K_(ctx));
    }
  }

  return ret;
}

int64_t ObMvccRowCallback::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf,
      buf_len,
      pos,
      "[this=%p, ctx=%s, is_link=%d, is_write_lock=%d, is_relocate_out=%d, need_fill_redo=%d, "
      "value=%s, tnode=%s, stmt_committed=%d, "
      "sql_no=%d, is_savepoint=%d, memtable=%p, log_ts=%lu",
      this,
      to_cstring(ctx_),
      is_link_,
      is_write_lock_,
      is_relocate_out_,
      need_fill_redo_,
      to_cstring(value_),
      NULL == tnode_ ? "null" : to_cstring(*tnode_),
      stmt_committed_,
      sql_no_,
      is_savepoint_,
      memtable_,
      log_ts_);
  return pos;
}

void ObMvccRowCallback::mark_tnode_overflow(const int64_t log_ts)
{
  if (tnode_ && memtable_) {
    int64_t freeze_log_ts = memtable_->get_freeze_log_ts();
    tnode_->log_timestamp_ = log_ts;

    if (log_ts >= freeze_log_ts) {
      memtable_->update_max_log_ts(log_ts);
    }

    if (log_ts_ > freeze_log_ts) {
      tnode_->set_overflow();
      value_.set_overflow_node(tnode_);
    }
  }
}

int ObMvccRowCallback::log_sync(const int64_t log_ts)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(INT64_MAX == log_ts_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "log ts should not be invalid", K(ret), K(log_ts_), K(*this));
  } else {
    ctx_.update_max_durable_sql_no(sql_no_);

    mark_tnode_overflow(log_ts);
    dec_pending_cb_count();

    marked_for_logging_ = false;
    need_fill_redo_ = false;
  }

  return ret;
}

int ObMvccRowCallback::calc_checksum_before_log_ts(const int64_t log_ts)
{
  int ret = OB_SUCCESS;

  ObRowLatchGuard guard(value_.latch_);

  if (OB_ISNULL(tnode_)) {
  } else if (INT64_MAX == log_ts_ || log_ts < log_ts_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(row_crc4())) {
    TRANS_LOG(ERROR, "row crc4 failed", K(log_ts), KR(ret), K(*this));
  }

  return ret;
}

int ObMvccRowCallback::fetch_rollback_data_size(int64_t& rollback_data_size)
{
  int ret = OB_SUCCESS;

  ObRowLatchGuard guard(value_.latch_);
  rollback_data_size = 0;

  if (is_savepoint()) {
    rollback_data_size = 0;
  } else if (INT64_MAX == log_ts_) {
    rollback_data_size = data_size_;
  } else {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected log id", K(log_ts_), K(*this));
  }

  return ret;
}

uint32_t ObMvccRowCallback::get_ctx_descriptor() const
{
  return ctx_.get_ctx_descriptor();
}

};  // namespace memtable
};  // end namespace oceanbase
