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

#include "storage/memtable/mvcc/ob_mvcc_engine.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/transaction/ob_trans_define.h"

namespace oceanbase {
using namespace common;
using namespace storage;
using namespace transaction;
namespace memtable {

ObMvccEngine::ObMvccEngine()
    : is_inited_(false), kv_builder_(NULL), query_engine_(NULL), engine_allocator_(NULL), memtable_(NULL)
{}

ObMvccEngine::~ObMvccEngine()
{
  destroy();
}

int ObMvccEngine::init(
    common::ObIAllocator* allocator, ObMTKVBuilder* kv_builder, ObQueryEngine* query_engine, ObMemtable* memtable)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    TRANS_LOG(WARN, "init twice", KP(this));
    ret = OB_INIT_TWICE;
  } else if (NULL == allocator || NULL == kv_builder || NULL == query_engine || NULL == memtable) {
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

int ObMvccEngine::try_compact_row(ObIMvccCtx& ctx, const ObTransSnapInfo& snapshot_info, ObMvccRow& row)
{
  UNUSED(ctx);
  int ret = OB_SUCCESS;
  const int64_t snapshot_version = snapshot_info.get_snapshot_version();
  const int64_t latest_compact_ts = row.latest_compact_ts_;
  const int64_t WEAK_READ_COMPACT_THRESHOLD = 3 * 1000 * 1000;
  if (0 >= snapshot_version) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "invalid snapshot version", K(ret), K(snapshot_version));
  } else if (INT64_MAX == snapshot_version ||
             ObTimeUtility::current_time() < latest_compact_ts + WEAK_READ_COMPACT_THRESHOLD) {
    // do not compact row when merging
  } else {
    ObRowLatchGuard guard(row.latch_);
    if (OB_FAIL(row.row_compact(snapshot_version, engine_allocator_))) {
      TRANS_LOG(WARN, "row compact error", K(ret), K(snapshot_version));
    }
  }
  return ret;
}

int ObMvccEngine::get_trans_version(ObIMvccCtx& ctx, const ObTransSnapInfo& snapshot_info,
    const ObQueryFlag& query_flag, const ObMemtableKey* key, ObMvccRow* row, int64_t& trans_version)
{
  int ret = OB_SUCCESS;
  ObMvccValueIterator value_iter;
  const void* tnode = nullptr;
  const bool skip_compact = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObMvccEngine has not been inited", K(ret));
  } else if (nullptr == key || nullptr == row) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), KP(key), KP(row));
  } else if (OB_FAIL(value_iter.init(ctx, snapshot_info, key, row, query_flag, skip_compact))) {
    TRANS_LOG(WARN, "fail to init ObMvccValueIterator", K(ret));
  } else if (OB_FAIL(value_iter.get_next_node(tnode))) {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      TRANS_LOG(DEBUG, "get trans version end", K(trans_version));
    }
  } else if (OB_ISNULL(tnode)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "error unexpected, tnode must not be NULL", K(ret));
  } else {
    trans_version = reinterpret_cast<const ObMvccTransNode*>(tnode)->trans_version_;
    TRANS_LOG(DEBUG, "get trans version", K(trans_version));
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Public Functions: set/lock

int ObMvccEngine::get(ObIMvccCtx& ctx, const ObTransSnapInfo& snapshot_info, const ObQueryFlag& query_flag,
    const bool skip_compact, const ObMemtableKey* parameter_key, ObMemtableKey* returned_key,
    ObMvccValueIterator& value_iter)
{
  int ret = OB_SUCCESS;
  ObMvccRow* value = NULL;
  const bool for_read = true;
  const bool for_replay = false;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", KP(this));
    ret = OB_NOT_INIT;
  } else if (NULL == parameter_key) {
    TRANS_LOG(WARN, "invalid param");
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(query_engine_->get(parameter_key, value, returned_key))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      value = NULL;
      // rewrite ret
      ret = OB_SUCCESS;
    } else {
      TRANS_LOG(WARN, "query_engine get fail", K(ret));
    }
  } else if (!query_flag.is_prewarm() && value->need_compact(for_read, for_replay)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = try_compact_row(ctx, snapshot_info, *value))) {
      TRANS_LOG(WARN, "fail to try to compact row", K(tmp_ret));
    }
  } else {
    // do nothing
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(value_iter.init(ctx, snapshot_info, returned_key, value, query_flag, skip_compact))) {
      TRANS_LOG(WARN, "ObMvccValueIterator init fail", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "get fail", K(ret), K(ctx), KP(parameter_key), KP(&value_iter));
  }
  return ret;
}

int ObMvccEngine::prefix_exist(ObIMvccCtx& ctx, const ObMemtableKey* parameter_key, bool& may_exist)
{
  UNUSED(ctx);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", KP(this));
    ret = OB_NOT_INIT;
  } else if (NULL == parameter_key) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid param", K(ret), KP(parameter_key));
  } else if (OB_FAIL(query_engine_->prefix_exist(parameter_key, may_exist))) {
    TRANS_LOG(WARN, "query_engine prefix exist fail", K(ret));
  }
  return ret;
}

int ObMvccEngine::scan(ObIMvccCtx& ctx, const ObTransSnapInfo& snapshot_info, const ObQueryFlag& query_flag,
    const ObMvccScanRange& range, ObMvccRowIterator& row_iter)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", KP(this));
    ret = OB_NOT_INIT;
  } else if (!range.is_valid()) {
    TRANS_LOG(WARN, "invalid param");
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(row_iter.init(*query_engine_, ctx, snapshot_info, range, query_flag))) {
    TRANS_LOG(WARN, "row_iter init fail", K(ret));
  } else {
    // do nothing
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "get fail", K(ret), K(ctx), K(snapshot_info), K(range));
  }
  return ret;
}

int ObMvccEngine::scan(ObIMvccCtx& ctx, const ObTransSnapInfo& snapshot_info, const ObMvccScanRange& range,
    ObTransStateTableGuard& trans_table_guard, ObMultiVersionRowIterator& row_iter)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", KP(this));
    ret = OB_NOT_INIT;
  } else if (!range.is_valid()) {
    TRANS_LOG(WARN, "invalid param");
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(row_iter.init(*query_engine_, ctx, snapshot_info, range, trans_table_guard))) {
    TRANS_LOG(WARN, "row_iter init fail", K(ret));
  } else {
    // do nothing
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "get fail", K(ret), K(ctx), K(snapshot_info), K(range));
  }
  return ret;
}

int ObMvccEngine::estimate_scan_row_count(const ObMvccScanRange& range, ObPartitionEst& part_est)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", KP(this));
    ret = OB_NOT_INIT;
  } else if (!range.is_valid()) {
    TRANS_LOG(WARN, "invalid param");
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(query_engine_->estimate_row_count(range.start_key_,
                 !range.border_flag_.inclusive_start(),
                 range.end_key_,
                 !range.border_flag_.inclusive_end(),
                 part_est.logical_row_count_,
                 part_est.physical_row_count_))) {
    TRANS_LOG(WARN, "query engine estimate row count fail", K(ret));
  }
  return ret;
}

int ObMvccEngine::replay(ObIMvccCtx& ctx, const ObMemtableKey* key, const ObMemtableData* data,
    const uint32_t modify_count, const uint32_t acc_checksum, const int64_t version, const int32_t sql_no,
    const int64_t log_timestamp)
{
  int ret = OB_SUCCESS;
  ObMemtableKey stored_key;
  ObMvccRow* value = NULL;
  bool is_new_add = false;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", KP(this));
    ret = OB_NOT_INIT;
  } else if (NULL == key || NULL == data) {
    TRANS_LOG(WARN, "invalid param");
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(m_prepare_kv(ctx, key, &stored_key, value, true, sql_no, is_new_add))) {
    TRANS_LOG(WARN, "m_prepare_kv fail", K(ret));
    // FIXME
    //} else if (value->modify_count_ > 0 && value->modify_count_ >= modify_count) {
    //  TRANS_LOG(INFO, "ignore replay transnode", K(*value), K(modify_count), K(acc_checksum));
  } else if (OB_FAIL(m_store_data(ctx,
                 &stored_key,
                 *value,
                 data,
                 NULL,
                 true,
                 modify_count,
                 acc_checksum,
                 version,
                 sql_no,
                 log_timestamp))) {
    TRANS_LOG(WARN,
        "m_store_data fail",
        K(ret),
        "stored_key",
        stored_key,
        "value_ptr",
        value,
        "value",
        *value,
        "modify_count",
        modify_count,
        "acc_checksum",
        acc_checksum,
        "version",
        version);
  } else {
    // do nothing
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "set fail", K(ret), "ctx", ctx, "key", key, "data", data, "modify_count", modify_count);
  }
  return ret;
}

int ObMvccEngine::store_data(ObIMvccCtx& ctx, const ObMemtableKey* key, ObMvccRow& value, const ObMemtableData* data,
    const ObRowData* old_row, const int64_t version, const int32_t sql_no)
{
  const int64_t log_timestamp = INT64_MAX;
  return m_store_data(ctx, key, value, data, old_row, false, 0, 0, version, sql_no, log_timestamp);
}

int ObMvccEngine::check_row_locked(
    ObIMvccCtx& ctx, const ObMemtableKey* key, bool& is_locked, uint32_t& lock_descriptor, int64_t& max_trans_version)
{
  int ret = OB_SUCCESS;
  ObMemtableKey stored_key;
  ObMvccRow* value = NULL;
  is_locked = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "mvcc_engine not init", K(this));
  } else {
    if (OB_FAIL(query_engine_->get(key, value, &stored_key))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        // rewrite ret
        ret = OB_SUCCESS;
      }
    } else if (NULL == value) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "get NULL value");
    } else if (OB_FAIL(value->check_row_locked(key, ctx, is_locked, lock_descriptor, max_trans_version))) {
      // do nothing
    }
  }

  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

int ObMvccEngine::m_prepare_kv(ObIMvccCtx& ctx, const ObMemtableKey* key, ObMemtableKey* stored_key, ObMvccRow*& value,
    const bool is_replay, const int32_t sql_no, bool& is_new_add)
{
  int ret = OB_SUCCESS;
  value = NULL;
  is_new_add = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "mvcc_engine not init", K(this));
  } else {
    while (OB_SUCCESS == ret && NULL == value) {
      ObStoreRowkey* tmp_key = nullptr;
      if (OB_SUCCESS == (ret = query_engine_->get(key, value, stored_key))) {
        if (NULL == value) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "get NULL value");
        }
      } else if (OB_FAIL(kv_builder_->dup_key(tmp_key, *engine_allocator_, key->get_rowkey()))) {
        TRANS_LOG(WARN, "key dup fail", K(ret));
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (OB_FAIL(stored_key->encode(key->get_table_id(), tmp_key))) {
        TRANS_LOG(WARN, "key encode fail", K(ret));
      } else if (NULL == (value = (ObMvccRow*)engine_allocator_->alloc(sizeof(*value)))) {
        TRANS_LOG(WARN, "alloc ObMvccRow fail");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        value = new (value) ObMvccRow();
        value->reset();
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
    }
  }

  if (OB_SUCC(ret)) {
    if (is_replay) {
      if (OB_FAIL(ctx.register_row_lock_release_cb(stored_key, value, is_replay, memtable_, sql_no))) {
        TRANS_LOG(WARN, "register_row_lock_cb fail", K(ret));
      }
    } else {
      if (value->row_lock_.is_exclusive_locked_by(ctx.get_ctx_descriptor())) {
        // has been locked itself
      } else if (OB_FAIL(value->lock_for_write(stored_key, ctx))) {
      } else if (OB_FAIL(ctx.register_row_lock_release_cb(stored_key, value, is_replay, memtable_, sql_no))) {
        TRANS_LOG(WARN, "register_row_lock_cb fail", K(ret));
        value->unlock_for_write(stored_key, ctx);
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObRowLatchGuard guard(value->latch_);
    if (OB_FAIL(query_engine_->ensure(stored_key, value))) {
      TRANS_LOG(WARN, "ensure_row fail", K(ret), K(is_new_add));
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      TRANS_LOG(WARN,
          "m_prepare_kv fail",
          "ctx",
          ctx,
          "lock_conflict_ctx",
          value ? ctx.log_conflict_ctx(value->row_lock_.get_exclusive_uid()) : "nil",
          "key",
          key,
          "stored_key",
          stored_key,
          "value_ptr",
          value,
          "value",
          value ? to_cstring(*value) : "nil",
          "is_replay",
          STR_BOOL(is_replay));
    }
  }
  return ret;
}

int ObMvccEngine::create_kv(ObIMvccCtx& ctx, const ObMemtableKey* key, ObMemtableKey* stored_key, ObMvccRow*& value,
    RowHeaderGetter& getter, bool& is_new_add)
{
  UNUSED(ctx);
  common::ObTimeGuard tg(__func__, 1000 * 1000);
  int64_t loop_cnt = 0;
  int ret = OB_SUCCESS;
  value = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "mvcc_engine not init", K(this));
  } else {
    is_new_add = false;
    while (OB_SUCCESS == ret && NULL == value) {
      ObStoreRowkey* tmp_key = nullptr;
      if (OB_SUCCESS == (ret = query_engine_->get(key, value, stored_key))) {
        if (NULL == value) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "get NULL value");
        }
      } else if (OB_FAIL(getter.get())) {
        TRANS_LOG(WARN, "get row header error");
      } else if (OB_FAIL(kv_builder_->dup_key(tmp_key, *engine_allocator_, key->get_rowkey()))) {
        TRANS_LOG(WARN, "key dup fail", K(ret));
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (OB_FAIL(stored_key->encode(key->get_table_id(), tmp_key))) {
        TRANS_LOG(WARN, "key encode fail", K(ret));
      } else if (NULL == (value = (ObMvccRow*)engine_allocator_->alloc(sizeof(*value)))) {
        TRANS_LOG(WARN, "alloc ObMvccRow fail");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        value = new (value) ObMvccRow();
        value->set_row_header(getter.get_modify_count(), getter.get_acc_checksum());
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
        TRANS_LOG(ERROR, "unexpected loop cnt when preparing kv", K(ret), K(loop_cnt), K(*key), K(*stored_key), K(tg));
      }
    }
  }

  return ret;
}

// for purge range
int ObMvccEngine::get_max_trans_version(const ObMemtableKey* key, bool& locked, int64_t& max_trans_version)
{
  int ret = OB_SUCCESS;
  ObMemtableKey stored_key;
  ObMvccRow* value = NULL;
  bool is_locked = false;
  int64_t tmp_max_trans_version = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "mvcc_engine not init", K(this));
  } else {
    if (OB_FAIL(query_engine_->get(key, value, &stored_key))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        // rewrite ret
        ret = OB_SUCCESS;
      }
    } else if (NULL == value) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "get NULL value");
    } else {
      is_locked = value->row_lock_.is_locked();
      tmp_max_trans_version = value->get_max_trans_version();
    }
  }
  if (OB_SUCC(ret)) {
    locked = is_locked;
    max_trans_version = tmp_max_trans_version;
  }
  return ret;
}

int ObMvccEngine::create_kv(const ObMemtableKey* key, ObMemtableKey* stored_key, ObMvccRow*& value)
{
  int ret = OB_SUCCESS;
  value = NULL;
  while (OB_SUCCESS == ret && NULL == value) {
    ObStoreRowkey* tmp_key = nullptr;
    if (OB_SUCCESS == (ret = query_engine_->get(key, value, stored_key))) {
      if (NULL == value) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "get NULL value");
      }
    } else if (OB_FAIL(kv_builder_->dup_key(tmp_key, *engine_allocator_, key->get_rowkey()))) {
      TRANS_LOG(WARN, "key dup fail", K(ret));
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(stored_key->encode(key->get_table_id(), tmp_key))) {
      TRANS_LOG(WARN, "key encode fail", K(ret));
    } else if (NULL == (value = (ObMvccRow*)engine_allocator_->alloc(sizeof(*value)))) {
      TRANS_LOG(WARN, "alloc ObMvccRow fail");
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      value = new (value) ObMvccRow();
      ObRowLatchGuard guard(value->latch_);
      if (OB_SUCCESS == (ret = query_engine_->set(stored_key, value))) {
      } else if (OB_ENTRY_EXIST == ret) {
        ret = OB_SUCCESS;
        value = NULL;
      } else {
        value = NULL;
      }
    }
  }
  return ret;
}

int ObMvccEngine::store_data(
    ObMvccRow& value, const ObMemtableData* data, const int64_t version, const int64_t timestamp)
{
  int ret = OB_SUCCESS;
  ObMvccTransNode* node = NULL;
  if (OB_FAIL(kv_builder_->dup_data(node, *engine_allocator_, data))) {
    TRANS_LOG(WARN, "MvccTranNode dup fail", K(ret), "node", node);
  } else {
    node->trans_version_ = version;
    node->version_ = timestamp;
    node->prev_ = NULL;
    node->next_ = NULL;
    value.set_head(node);
  }
  return ret;
}

int ObMvccEngine::relocate_lock(ObIMvccCtx& ctx, const ObMemtableKey* stored_key, const ObMvccRow* value,
    const bool is_replay, const bool need_lock_for_write, const bool is_sequential_relocate, bool& new_locked)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "mvcc_engine not init", K(this));
  } else if (is_replay) {
    new_locked = true;
  } else {
    if (value->row_lock_.is_exclusive_locked_by(ctx.get_ctx_descriptor())) {
      new_locked = false;
    } else if (!need_lock_for_write) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "no need to relocate lock, unexpected error", K(ret), K(ctx), K(*stored_key), K(*value));
    } else if (OB_FAIL(const_cast<ObMvccRow*>(value)->relocate_lock(ctx, is_sequential_relocate))) {
      // do nothing
    } else {
      new_locked = true;
    }
  }
  if (OB_SUCCESS != ret && OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
    TRANS_LOG(WARN,
        "relocate lock fail",
        K(ret),
        "ctx",
        ctx,
        "lock_conflict_ctx",
        value ? ctx.log_conflict_ctx(value->row_lock_.get_exclusive_uid()) : "nil",
        "stored_key",
        stored_key,
        "value_ptr",
        value,
        "value",
        value ? to_cstring(*value) : "nil",
        "is_replay",
        STR_BOOL(is_replay));
  }
  return ret;
}

int ObMvccEngine::lock(
    ObIMvccCtx& ctx, const ObMemtableKey* stored_key, const ObMvccRow* value, const bool is_replay, bool& new_locked)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "mvcc_engine not init", K(this));
  } else if (is_replay) {
    new_locked = true;
  } else {
    if (value->row_lock_.is_exclusive_locked_by(ctx.get_ctx_descriptor())) {
      new_locked = false;
    } else if (OB_FAIL(const_cast<ObMvccRow*>(value)->lock_for_write(stored_key, ctx))) {
      // do nothing
    } else {
      new_locked = true;
    }
  }
  if (OB_SUCCESS != ret && OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
    TRANS_LOG(WARN,
        "lock fail",
        K(ret),
        "ctx",
        ctx,
        "lock_conflict_ctx",
        value ? ctx.log_conflict_ctx(value->row_lock_.get_exclusive_uid()) : "nil",
        "stored_key",
        stored_key,
        "value_ptr",
        value,
        "value",
        value ? to_cstring(*value) : "nil",
        "is_replay",
        STR_BOOL(is_replay));
  }
  return ret;
}

int ObMvccEngine::unlock(
    ObIMvccCtx& ctx, const ObMemtableKey* key, const ObMvccRow* value, const bool is_replay, const bool new_locked)
{
  UNUSED(key);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "mvcc_engine not init", K(this));
  } else if (new_locked) {
    if (!is_replay) {
      const_cast<ObMvccRow*>(value)->revert_lock_for_write(ctx);
    }
  } else {
    // do nothing
  }
  return ret;
}

int ObMvccEngine::append_kv(ObIMvccCtx& ctx, const ObMemtableKey* stored_key, ObMvccRow* value, const bool is_replay,
    const bool new_locked, const int32_t sql_no, const bool is_sequential_relocate)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "mvcc_engine not init", K(this));
  } else if (new_locked) {
    if (is_replay) {
      if (OB_FAIL(ctx.register_row_lock_release_cb(stored_key, value, is_replay, memtable_, sql_no))) {
        TRANS_LOG(WARN, "register_row_lock_cb fail", K(ret));
      }
    } else {
      if (OB_FAIL(ctx.register_row_lock_release_cb(stored_key, value, is_replay, memtable_, sql_no))) {
        TRANS_LOG(WARN, "register_row_lock_cb fail", K(ret));
        value->unlock_for_write(stored_key, ctx);
      }
    }
  } else {
    // do nothing
  }
  if (OB_SUCC(ret)) {
    if (is_sequential_relocate) {
      ObRowLatchGuard guard(value->latch_);
      if (OB_FAIL(query_engine_->ensure(stored_key, value))) {
        TRANS_LOG(WARN, "ensure_row fail", K(ret));
      }
    } else {
      if (value->latch_.try_lock()) {
        if (OB_FAIL(query_engine_->ensure(stored_key, value))) {
          TRANS_LOG(WARN, "ensure_row fail", K(ret));
        }
        value->latch_.unlock();
      }
    }
  }
  return ret;
}

int ObMvccEngine::m_store_data(ObIMvccCtx& ctx, const ObMemtableKey* key, ObMvccRow& value, const ObMemtableData* data,
    const ObRowData* old_row, const bool is_replay, const uint32_t modify_count, const uint32_t acc_checksum,
    const int64_t version, const int32_t sql_no, const int64_t log_timestamp)
{
  int ret = OB_SUCCESS;
  int64_t data_size = 0;
  ObMvccTransNode* node = NULL;
  const bool is_stmt_committed = false;
  if (OB_FAIL(kv_builder_->get_data_size(data, data_size))) {
    TRANS_LOG(WARN, "get_data_size failed", K(ret), KP(data), K(data_size));
  } else if (OB_FAIL(kv_builder_->dup_data(node, *engine_allocator_, data))) {
    TRANS_LOG(WARN, "MvccTranNode dup fail", K(ret), "node", node);
  } else {
    node->trans_version_ = INT64_MAX;
    node->modify_count_ = UINT32_MAX;
    node->acc_checksum_ = acc_checksum;
    node->version_ = version;
    node->log_timestamp_ = log_timestamp;
    node->prev_ = NULL;
    node->next_ = NULL;

    if (!is_replay) {
      if (OB_FAIL(ctx.register_row_commit_cb(key,
              &value,
              node,
              data_size,
              old_row,
              is_stmt_committed,
              true, /*need_fill_redo*/
              memtable_,
              sql_no,
              true /*is_sequential_relocate*/))) {
        TRANS_LOG(WARN, "register_row_commit_cb fail", K(ret));
      }
    } else {
      node->modify_count_ = modify_count;
      if (0 != ctx.get_redo_log_timestamp()) {
        // set transaction version with log timestamp for ordering
        node->trans_version_ = ctx.get_redo_log_timestamp();
      }
      const bool is_sequential_relocate = true;
      if (OB_FAIL(ctx.register_row_replay_cb(
              key, &value, node, data_size, memtable_, sql_no, is_sequential_relocate, log_timestamp))) {
        TRANS_LOG(WARN, "register_row_replay_cb fail", K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
    if (OB_ALLOCATE_MEMORY_FAILED != ret || REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      TRANS_LOG(WARN,
          "m_store_data fail",
          K(ret),
          "data_size",
          data_size,
          "node",
          node,
          "ctx",
          ctx,
          "key",
          key,
          "value_ptr",
          &value,
          "value",
          value,
          "data",
          data,
          "is_replay",
          STR_BOOL(is_replay),
          "modify_count",
          modify_count,
          "acc_checksum",
          acc_checksum);
    }
  }
  return ret;
}

// assign transaction node for data relocation
int ObMvccEngine::relocate_data(ObIMvccCtx& ctx, const ObMemtableKey* key, ObMvccRow& value,
    const ObMvccTransNode* trans_node, const bool is_replay, const bool need_fill_redo, const ObRowData* old_row,
    const int64_t version, const bool is_stmt_committed, const int32_t sql_no, const int64_t log_ts,
    const bool is_sequential_relocate)
{
  int ret = OB_SUCCESS;
  int64_t data_size = 0;
  ObMvccTransNode* node = NULL;
  const ObMemtableDataHeader* data = NULL;
  common::ObTimeGuard tg("ObMvccEngine::relocate_data", 1000 * 1000);

  if (NULL == old_row || NULL == trans_node || version <= 0) {
    TRANS_LOG(WARN, "invalid argument", KP(old_row), KP(trans_node));
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == (data = reinterpret_cast<const ObMemtableDataHeader*>(trans_node->buf_))) {
    TRANS_LOG(ERROR, "trans node or buf is null, unexpected error", KP(data), K(ctx), K(key), K(value));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(kv_builder_->get_data_size(data, data_size))) {
    TRANS_LOG(WARN, "get_data_size failed", K(ret), KP(data), K(data_size));
  } else if (OB_FAIL(kv_builder_->dup_data(node, *engine_allocator_, data))) {
    TRANS_LOG(WARN, "MvccTranNode dup fail", K(ret), "node", node);
  } else {
    tg.click();
    node->trans_version_ = trans_node->trans_version_;
    // whether transaction node is compacted or not, assign transaction node
    node->type_ = trans_node->type_;
    // must remain modify count
    node->modify_count_ = trans_node->modify_count_;
    node->acc_checksum_ = trans_node->acc_checksum_;
    node->version_ = version;
    node->log_timestamp_ = trans_node->log_timestamp_;
    // set if transaction node is relocated or not
    node->set_relocated();
    node->prev_ = NULL;
    node->next_ = NULL;
    TRANS_LOG(DEBUG, "relocate data", K(ctx), K(value), K(*node), K(is_replay), K(is_stmt_committed));
    tg.click();

    if (!is_replay) {
      if (OB_FAIL(ctx.register_row_commit_cb(key,
              &value,
              node,
              data_size,
              old_row,
              is_stmt_committed,
              need_fill_redo,
              memtable_,
              sql_no,
              is_sequential_relocate))) {
        TRANS_LOG(WARN, "register_row_commit_cb fail", K(ret));
      }
      tg.click();
    } else {
      node->modify_count_ = trans_node->modify_count_;
      if (OB_FAIL(ctx.register_row_replay_cb(
              key, &value, node, data_size, memtable_, sql_no, is_sequential_relocate, log_ts))) {
        TRANS_LOG(WARN, "register_row_replay_cb fail", K(ret));
      }
      tg.click();
    }
    TRANS_LOG(DEBUG, "new trans node in data relocation", K(ret), K(is_replay), K(value), K(*node));
  }
  if (OB_FAIL(ret)) {
    if (OB_ALLOCATE_MEMORY_FAILED != ret || REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      TRANS_LOG(WARN,
          "relocate data fail",
          K(ret),
          "data_size",
          data_size,
          "node",
          node,
          "ctx",
          ctx,
          "key",
          key,
          "value_ptr",
          &value,
          "value",
          value,
          "data",
          data,
          "is_replay",
          STR_BOOL(is_replay));
    }
  }
  return ret;
}

}  // namespace memtable
}  // namespace oceanbase
