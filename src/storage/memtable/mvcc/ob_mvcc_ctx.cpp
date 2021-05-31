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

#include "storage/memtable/mvcc/ob_mvcc_ctx.h"
#include "storage/memtable/ob_memtable_key.h"
#include "storage/memtable/ob_memtable_data.h"
#include "storage/memtable/ob_memtable.h"

namespace oceanbase {
using namespace common;
using namespace storage;
namespace memtable {

int ObIMvccCtx::after_link_trans_node(const void* key, ObMvccRow* value)
{
  int ret = OB_SUCCESS;
  UNUSED(key);
  UNUSED(value);
  return ret;
}

void ObIMvccCtx::before_prepare()
{
#ifdef TRANS_ERROR
  const int random = (int)ObRandom::rand(1, 1000);
  usleep(random);
#endif
  set_trans_version(0);
}

bool ObIMvccCtx::is_prepared() const
{
  const int64_t prepare_version = ATOMIC_LOAD(&trans_version_);
  return (prepare_version >= 0 && INT64_MAX != prepare_version);
}

int ObIMvccCtx::register_row_lock_release_cb(
    const ObMemtableKey* key, ObMvccRow* value, bool is_replay, ObMemtable* memtable, const int32_t sql_no)
{
  int ret = OB_SUCCESS;
  ObMvccRowCallback* cb = NULL;
  if (OB_ISNULL(key) || OB_ISNULL(value) || OB_ISNULL(memtable)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(key), K(value), K(memtable));
  } else if (OB_ISNULL(cb = alloc_row_callback(*this, *value, memtable, false))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "alloc row callback failed", K(ret));
  } else {
    cb->set(key, NULL, 0, NULL, is_replay, false, sql_no);
    if (!is_replay) {
      cb->set_write_locked();
    }
    if (OB_FAIL(append_callback(cb))) {
      callback_free(cb);
      TRANS_LOG(WARN, "append callback failed", K(ret));
    }
  }
  return ret;
}

int ObIMvccCtx::register_row_commit_cb(const ObMemtableKey* key, ObMvccRow* value, ObMvccTransNode* node,
    const int64_t data_size, const ObRowData* old_row, const bool is_stmt_committed, const bool need_fill_redo,
    ObMemtable* memtable, const int32_t sql_no, const bool is_sequential_relocate)
{
  int ret = OB_SUCCESS;
  const bool is_replay = false;
  ObMvccRowCallback* cb = NULL;
  common::ObTimeGuard tg("ObIMvccCtx::register_row_commit_cb", 1000 * 1000);
  if (OB_ISNULL(key) || OB_ISNULL(value) || OB_ISNULL(node) || data_size <= 0 || OB_ISNULL(memtable)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(key), K(value), K(node), K(data_size), K(memtable));
  } else if (OB_ISNULL(cb = alloc_row_callback(*this, *value, memtable, false))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "alloc row callback failed", K(ret));
  } else {
    tg.click();
    // count up memory size of current transaction node
    add_trans_mem_total_size(data_size);
    node->set_mvcc_row_cb(cb);
    cb->set(key, node, data_size, old_row, is_replay, need_fill_redo, sql_no);
    if (is_sequential_relocate) {
      {
        ObRowLatchGuard guard(value->latch_);
        tg.click();
        cb->link_trans_node();
      }
      // set stmt_committed flag of callback
      // set stmt_committed true only if data relocation
      cb->set_stmt_committed(is_stmt_committed);
      if (OB_FAIL(append_callback(cb))) {
        ObRowLatchGuard guard(value->latch_);
        cb->unlink_trans_node();
      }
    } else {
      if (value->latch_.try_lock()) {
        tg.click();
        cb->link_trans_node();
        // set stmt_committed flag of callback
        // set stmt_committed true only if data relocation
        cb->set_stmt_committed(is_stmt_committed);
        if (OB_FAIL(append_callback(cb))) {
          cb->unlink_trans_node();
          TRANS_LOG(WARN, "append callback failed", K(ret));
        }
      } else {
        ret = OB_TRY_LOCK_ROW_CONFLICT;
        TRANS_LOG(DEBUG,
            "register_row_commit_cb try lock fail, need to retry",
            K(*key),
            K(*value),
            K(memtable),
            K(is_sequential_relocate));
      }
    }
    tg.click();
    if (OB_FAIL(ret)) {
      callback_free(cb);
      TRANS_LOG(WARN, "append callback failed", K(ret), K(is_sequential_relocate));
    }
  }
  return ret;
}

int ObIMvccCtx::register_row_replay_cb(const ObMemtableKey* key, ObMvccRow* value, ObMvccTransNode* node,
    const int64_t data_size, ObMemtable* memtable, const int32_t sql_no, const bool is_sequential_relocate,
    const int64_t log_ts)
{
  int ret = OB_SUCCESS;
  const bool is_replay = true;
  const bool need_fill_redo = false;
  const ObRowData* old_row = NULL;
  ObMvccRowCallback* cb = NULL;
  common::ObTimeGuard tg("ObIMvccCtx::register_row_replay_cb", 1000 * 1000);
  if (OB_ISNULL(key) || OB_ISNULL(value) || OB_ISNULL(node) || data_size <= 0 || OB_ISNULL(memtable)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(key), K(value), K(node), K(data_size), K(memtable));
  } else if (OB_ISNULL(cb = alloc_row_callback(*this, *value, memtable, false))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "alloc row callback failed", K(ret));
  } else {
    tg.click();
    node->set_mvcc_row_cb(cb);
    cb->set(key, node, data_size, old_row, is_replay, need_fill_redo, sql_no);
    if (is_sequential_relocate) {
      {
        ObRowLatchGuard guard(value->latch_);
        tg.click();
        cb->link_trans_node();
      }
      // all statement is finished when replaying log
      cb->set_stmt_committed(true);
      cb->set_log_ts(log_ts);
      if (OB_FAIL(append_callback(cb))) {
        {
          ObRowLatchGuard guard(value->latch_);
          cb->unlink_trans_node();
        }
        TRANS_LOG(WARN, "append callback failed", K(ret));
      } else {
        update_max_durable_sql_no(sql_no);
        // TODO: defense inspection
        int64_t redo_log_ts = (0 == lob_start_log_ts_ ? log_ts : lob_start_log_ts_);
        if (redo_log_ts > memtable->get_freeze_log_ts()) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "replay should not overflow", K(ret), K(redo_log_ts), K(*memtable));
        }
      }
    } else {
      // try lock destination memstore row when relocating data from active memstore to frozen memstore
      if (value->latch_.try_lock()) {
        tg.click();
        cb->link_trans_node();
        // all statement is finished when replaying log
        cb->set_stmt_committed(true);
        cb->set_log_ts(log_ts);
        if (OB_FAIL(append_callback(cb))) {
          cb->unlink_trans_node();
        }
        value->latch_.unlock();
      } else {
        ret = OB_TRY_LOCK_ROW_CONFLICT;
        TRANS_LOG(DEBUG,
            "register_row_replay_cb try lock fail, need to retry",
            K(*key),
            K(*value),
            K(memtable),
            K(is_sequential_relocate));
      }
    }
    if (OB_FAIL(ret)) {
      callback_free(cb);
      TRANS_LOG(WARN, "append callback failed", K(ret), K(is_sequential_relocate));
    }
    tg.click();
  }
  return ret;
}

int ObIMvccCtx::register_savepoint_cb(const ObMemtableKey* key, ObMvccRow* value, ObMvccTransNode* node,
    const int64_t data_size, const ObRowData* old_row, const int32_t sql_no)
{
  int ret = OB_SUCCESS;
  const bool is_replay = false;
  const bool need_fill_redo = true;
  const bool is_stmt_committed = false;
  ObMvccRowCallback* cb = NULL;
  if (OB_ISNULL(key) || OB_ISNULL(value) || OB_ISNULL(node) || data_size < 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(key), K(value), K(node), K(data_size));
  } else if (OB_ISNULL(cb = alloc_row_callback(*this, *value, NULL, true))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "alloc row callback failed", K(ret));
  } else {
    // count up memory size of transaction node
    add_trans_mem_total_size(data_size);
    cb->set(key, node, data_size, old_row, is_replay, need_fill_redo, sql_no);
    cb->set_stmt_committed(is_stmt_committed);
    cb->set_savepoint();
    cb->set_is_link();
    if (OB_FAIL(append_callback(cb))) {
      TRANS_LOG(WARN, "append savepoint callback failed", K(ret), K(*cb));
    } else {
      TRANS_LOG(INFO, "append savepoint callback", K(*cb));
    }
  }
  return ret;
}

int ObIMvccCtx::register_savepoint_cb(ObMvccRowCallback& cb, ObMemtable* memtable)
{
  int ret = OB_SUCCESS;
  ObMvccRowCallback* new_cb = NULL;
  if (OB_ISNULL(new_cb = alloc_row_callback(cb, memtable, true))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "alloc row callback failed", K(ret));
  } else {
    if (OB_FAIL(append_callback(new_cb))) {
      TRANS_LOG(WARN, "append savepoint callback failed", K(ret), K(*new_cb));
    } else {
      TRANS_LOG(INFO, "append savepoint callback", K(*new_cb));
    }
  }
  return ret;
}

ObMvccRowCallback* ObIMvccCtx::alloc_row_callback(
    ObIMvccCtx& ctx, ObMvccRow& value, ObMemtable* memtable, const bool is_savepoint)
{
  int ret = OB_SUCCESS;
  void* cb_buffer = NULL;
  ObMvccRowCallback* cb = NULL;
  if (!is_savepoint) {
    if (NULL == (cb_buffer = callback_alloc(sizeof(*cb)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TRANS_LOG(WARN, "alloc ObRowCB cb_buffer fail", K(ret));
    }
  } else {
    if (NULL == (cb_buffer = arena_alloc(sizeof(*cb)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TRANS_LOG(WARN, "alloc ObRowCB cb_buffer fail", K(ret));
    }
  }
  if (NULL != cb_buffer) {
    if (NULL == (cb = new (cb_buffer) ObMvccRowCallback(ctx, value, memtable))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TRANS_LOG(WARN, "construct ObRowCB object fail", K(ret), "cb_buffer", cb_buffer);
    }
  }
  return cb;
}

ObMvccRowCallback* ObIMvccCtx::alloc_row_callback(ObMvccRowCallback& cb, ObMemtable* memtable, const bool is_savepoint)
{
  int ret = OB_SUCCESS;
  void* cb_buffer = NULL;
  ObMvccRowCallback* new_cb = NULL;
  if (!is_savepoint) {
    if (NULL == (cb_buffer = callback_alloc(sizeof(*new_cb)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TRANS_LOG(WARN, "alloc ObRowCB cb_buffer fail", K(ret));
    }
  } else {
    if (NULL == (cb_buffer = arena_alloc(sizeof(*new_cb)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TRANS_LOG(WARN, "alloc ObRowCB cb_buffer fail", K(ret));
    }
  }
  if (NULL != cb_buffer) {
    if (NULL == (new_cb = new (cb_buffer) ObMvccRowCallback(cb, memtable))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TRANS_LOG(WARN, "construct ObRowCB object fail", K(ret), "cb_buffer", cb_buffer);
    }
  }
  return new_cb;
}

ObMemtableKey* ObIMvccCtx::alloc_memtable_key()
{
  int ret = OB_SUCCESS;
  void* buffer = NULL;
  ObMemtableKey* mt_key = NULL;
  if (NULL == (buffer = arena_alloc(sizeof(*mt_key)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "alloc memtable key fail", K(ret));
  } else if (NULL == (mt_key = new (buffer) ObMemtableKey())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "construct memtable key fail", K(ret), KP(buffer));
  } else {
    // do nothing
  }
  return mt_key;
}

ObMvccRow* ObIMvccCtx::alloc_mvcc_row()
{
  int ret = OB_SUCCESS;
  void* buffer = NULL;
  ObMvccRow* row = NULL;
  if (NULL == (buffer = arena_alloc(sizeof(*row)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "alloc mvcc row fail", K(ret));
  } else if (NULL == (row = new (buffer) ObMvccRow())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "construct mvcc row fail", K(ret), KP(buffer));
  } else {
    // do nothing
  }
  return row;
}

ObMvccTransNode* ObIMvccCtx::alloc_trans_node()
{
  int ret = OB_SUCCESS;
  char* buffer = NULL;
  ObMvccTransNode* node = NULL;
  ObMemtableData* data = NULL;
  if (NULL == (buffer = (char*)arena_alloc(sizeof(*node) + sizeof(*data)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "alloc trans node fail", K(ret));
  } else if (NULL == (node = new (buffer) ObMvccTransNode())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "construct trans node fail", K(ret), KP(buffer));
  } else {
    data = reinterpret_cast<ObMemtableData*>(node->buf_);
    new (data) ObMemtableData(T_DML_UNKNOWN, 0, NULL);
  }
  return node;
}

int ObIMvccCtx::append_callback(ObITransCallback* cb)
{
  return trans_mgr_.append(cb);
}

int64_t ObIMvccCtx::get_query_abs_lock_wait_timeout(const int64_t lock_wait_start_ts) const
{
  int64_t abs_timeout = 0;

  if (trx_lock_timeout_ < 0) {
    abs_timeout = abs_lock_wait_timeout_;
  } else {
    abs_timeout = MIN(abs_lock_wait_timeout_, trx_lock_timeout_ + lock_wait_start_ts);
  }

  return abs_timeout;
}

}  // namespace memtable
}  // namespace oceanbase
