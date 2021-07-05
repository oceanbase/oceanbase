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

#include "common/rowkey/ob_store_rowkey.h"

#include "storage/memtable/ob_memtable.h"

#include "lib/stat/ob_diagnose_info.h"
#include "lib/time/ob_time_utility.h"
#include "share/ob_worker.h"
#include "share/rc/ob_context.h"

#include "storage/ob_partition_scheduler.h"

#include "storage/memtable/mvcc/ob_mvcc_engine.h"
#include "storage/memtable/mvcc/ob_mvcc_iterator.h"

#include "storage/memtable/ob_memtable_compact_writer.h"
#include "storage/memtable/ob_memtable_iterator.h"
#include "storage/memtable/ob_memtable_sparse_iterator.h"
#include "storage/memtable/ob_memtable_mutator.h"
#include "storage/memtable/ob_memtable_util.h"
#include "storage/memtable/ob_memtable_context.h"
#include "storage/memtable/ob_lock_wait_mgr.h"

#include "storage/transaction/ob_trans_define.h"
#include "storage/transaction/ob_trans_part_ctx.h"

namespace oceanbase {
using namespace common;
using namespace share::schema;

using namespace storage;
using namespace transaction;
namespace memtable {
class ObStoreRowWrapper : public ObStoreRow, public ObWithArena {
public:
  static const int64_t PAGE_SIZE = OB_MALLOC_NORMAL_BLOCK_SIZE;
  explicit ObStoreRowWrapper(ObIAllocator& allocator)
      : ObStoreRow(), ObWithArena(allocator, PAGE_SIZE), allocator_(allocator)
  {}

  ObStoreRow& get_store_row()
  {
    return *this;
  }
  ObIAllocator& get_allocator()
  {
    return allocator_;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObStoreRowWrapper);
  ObIAllocator& allocator_;
};

class ObGlobalMtAlloc {
public:
  ObGlobalMtAlloc()
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(allocator_.init(OB_MALLOC_NORMAL_BLOCK_SIZE, ObNewModIds::OB_MEMSTORE))) {
      TRANS_LOG(ERROR, "global mt alloc init fail", K(ret));
    }
  }
  ~ObGlobalMtAlloc()
  {}
  void* alloc(const int64_t size)
  {
    return allocator_.alloc(size);
  }
  void free(void* ptr)
  {
    allocator_.free(ptr);
    ptr = NULL;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObGlobalMtAlloc);
  ObLfFIFOAllocator allocator_;
};

ObGlobalMtAlloc& get_global_mt_alloc()
{
  static ObGlobalMtAlloc s_alloc;
  return s_alloc;
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Public Functions

ObMemtable::ObMemtable()
    : is_inited_(false),
      local_allocator_(*this),
      query_engine_(local_allocator_),
      mvcc_engine_(),
      storage_info_(),
      max_schema_version_(0),
      max_trans_version_(0),
      active_trx_count_(0),
      pending_cb_cnt_(0),
      freeze_log_ts_(INT64_MAX),
      state_(ObMemtableState::INVALID),
      timestamp_(0),
      read_barrier_(false),
      write_barrier_(false),
      write_ref_cnt_(0),
      row_relocate_count_(0),
      mode_(share::ObWorker::CompatMode::INVALID),
      minor_merged_time_(0),
      contain_hotspot_row_(false),
      pending_lob_cnt_(0),
      pending_batch_commit_cnt_(0),
      pending_elr_cnt_(0),
      frozen_log_applied_(false),
      mark_finish_(false),
      frozen_(false),
      emergency_(false),
      with_accurate_log_ts_range_(false),
      is_split_(false)
{
  mt_stat_.reset();
  ObMemtableStat::get_instance().register_memtable(this);
}

ObMemtable::~ObMemtable()
{
  ObMemtableStat::get_instance().unregister_memtable(this);
  destroy();
}

int ObMemtable::init(const ObITable::TableKey& table_key)
{
  int ret = OB_SUCCESS;
  const bool skip_version_range = true;
  const bool skip_log_ts_range = true;
  if (is_inited_) {
    TRANS_LOG(WARN, "init twice", K(*this));
    ret = OB_INIT_TWICE;
  } else if (!table_key.is_valid(skip_version_range, skip_log_ts_range)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid param", K(ret), K(table_key), K(skip_version_range));
  } else if (table_key.pkey_.get_table_id() != table_key.table_id_) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "memtable table id must match pkey", K(ret), K(table_key));
  } else if (OB_FAIL(local_allocator_.init(extract_tenant_id(table_key.pkey_.table_id_)))) {
    TRANS_LOG(WARN,
        "fail to init memstore allocator",
        K(ret),
        "tenant id",
        extract_tenant_id(table_key.pkey_.get_table_id()));
  } else if (OB_FAIL(query_engine_.init(extract_tenant_id(table_key.pkey_.get_table_id())))) {
    TRANS_LOG(WARN, "query_engine.init fail", K(ret), "tenant_id", extract_tenant_id(table_key.pkey_.get_table_id()));
  } else if (OB_FAIL(mvcc_engine_.init(&local_allocator_, &kv_builder_, &query_engine_, this))) {
    TRANS_LOG(WARN, "query engine init fail", "ret", ret);
  } else if (OB_FAIL(ObITable::init(table_key, skip_version_range, skip_log_ts_range))) {
    TRANS_LOG(WARN, "failed to set_table_key", K(ret), K(table_key));
  } else {
    // obtaining compat mode for data write, including leader and follower
    CREATE_WITH_TEMP_ENTITY(TABLE_SPACE, table_key.pkey_.get_table_id())
    {
      mode_ = THIS_WORKER.get_compatibility_mode();
    }
    else if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2100)
    {
      mode_ = share::ObWorker::CompatMode::MYSQL;
      ret = OB_SUCCESS;
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "compact mode not ready", K(ret), K(mode_), K(*this));
    }
    state_ = ObMemtableState::ACTIVE;
    timestamp_ = ObTimeUtility::current_time();
    is_inited_ = true;
    TRANS_LOG(DEBUG, "memtable init success", K(*this));
  }
  // avoid calling destroy() when ret is OB_INIT_TWICE
  if (OB_SUCCESS != ret && IS_NOT_INIT) {
    destroy();
  }
  return ret;
}

void ObMemtable::destroy()
{
  ObTimeGuard time_guard("ObMemtable::destroy()", 100 * 1000);
  if (is_inited_) {
    STORAGE_LOG(INFO, "memtable destroyed", K(*this), K_(row_relocate_count));
    if (active_trx_count_ > 0) {
      TRANS_LOG(WARN, "some active transaction not finish", K(*this));
    }
    time_guard.click();
    time_guard.click();
  }
  key_.reset();
  mvcc_engine_.destroy();
  time_guard.click();
  query_engine_.destroy();
  time_guard.click();
  local_allocator_.destroy();
  max_schema_version_ = 0;
  max_trans_version_ = 0;
  active_trx_count_ = 0;
  mt_stat_.reset();
  state_ = ObMemtableState::INVALID;
  is_inited_ = false;
}

int ObMemtable::fake(const ObIMemtable& mt)
{
  int ret = OB_SUCCESS;

  // if ObIMemtable has more than one implementation, must use RTTI to do judgment
  const ObMemtable& o = dynamic_cast<const ObMemtable&>(mt);
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", K(*this));
    ret = OB_NOT_INIT;
  } else if (o.is_frozen_memtable() && OB_FAIL(storage_info_.deep_copy(o.storage_info_))) {
    TRANS_LOG(WARN, "deep copy save storage info failed", K(ret), K_(key_.pkey));
  } else {
    state_ = o.state_;
    key_ = o.key_;
    TRANS_LOG(INFO, "fake memtable success", K_(key), K_(storage_info), K_(state));
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Public Functions: set/lock

void ObMemtable::set_begin(const ObStoreCtx& ctx)
{
  if (NULL != ctx.mem_ctx_) {
    (static_cast<ObMemtableCtx*>(ctx.mem_ctx_))->set_handle_start_time(ObTimeUtility::current_time());
    EVENT_INC(MEMSTORE_APPLY_COUNT);
  }
}

void ObMemtable::set_end(const ObStoreCtx& ctx, int ret)
{

  if (NULL != ctx.mem_ctx_) {
    if (OB_SUCC(ret)) {
      EVENT_INC(MEMSTORE_APPLY_SUCC_COUNT);
    } else {
      EVENT_INC(MEMSTORE_APPLY_FAIL_COUNT);
    }
    EVENT_ADD(MEMSTORE_APPLY_TIME,
        ObTimeUtility::current_time() - (static_cast<ObMemtableCtx*>(ctx.mem_ctx_))->get_handle_start_time());
  }
}

int ObMemtable::set(const ObStoreCtx& ctx, const uint64_t table_id, const int64_t rowkey_len,
    const ObIArray<ObColDesc>& columns, ObStoreRowIterator& row_iter)
{
  int ret = OB_SUCCESS;
  ObMvccWriteGuard guard;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", K(*this));
    ret = OB_NOT_INIT;
  } else if (NULL == ctx.mem_ctx_ || 0 >= rowkey_len || rowkey_len > columns.count()) {
    TRANS_LOG(WARN, "invalid param");
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(guard.write_auth(*ctx.mem_ctx_))) {
    TRANS_LOG(WARN, "not allow to write", K(ctx));
  } else {
    share::CompatModeGuard compat_guard(mode_);
    const bool for_replay = false;
    ObMemtableCtx* mt_ctx = static_cast<ObMemtableCtx*>(ctx.mem_ctx_);
    ret = mt_ctx->set_leader_host(this, for_replay);
    const ObStoreRow* row = NULL;
    while (OB_SUCCESS == ret && OB_SUCCESS == (ret = row_iter.get_next_row(row))) {
      if (NULL == row) {
        TRANS_LOG(WARN, "row iter get next row null pointer");
        ret = OB_ERR_UNEXPECTED;
      } else {
        ret = set_(ctx, table_id, rowkey_len, columns, *row, NULL, NULL);
      }
    }
    ret = (OB_ITER_END == ret) ? OB_SUCCESS : ret;
  }
  if (OB_FAIL(ret)) {
    if ((OB_TRANSACTION_SET_VIOLATION != ret && OB_ALLOCATE_MEMORY_FAILED != ret) ||
        REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      TRANS_LOG(WARN,
          "set fail",
          "ret",
          ret,
          "table_id",
          key_.table_id_,
          "ctx",
          STR_PTR(ctx.mem_ctx_),
          "table_id",
          table_id,
          "rowkey_len",
          rowkey_len,
          "columns",
          strarray<ObColDesc>(columns),
          "row_iter_ptr",
          &row_iter);
    }
  }
  return ret;
}

int ObMemtable::set(const storage::ObStoreCtx& ctx, const uint64_t table_id, const int64_t rowkey_len,
    const common::ObIArray<share::schema::ObColDesc>& columns, const storage::ObStoreRow& row)
{
  int ret = OB_SUCCESS;
  ObMvccWriteGuard guard;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", K(*this));
    ret = OB_NOT_INIT;
  } else if (NULL == ctx.mem_ctx_ || 0 >= rowkey_len || rowkey_len > columns.count() ||
             row.row_val_.count_ < columns.count()) {
    TRANS_LOG(WARN, "invalid param");
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(guard.write_auth(*ctx.mem_ctx_))) {
    TRANS_LOG(WARN, "not allow to write", K(ctx));
  } else {
    share::CompatModeGuard compat_guard(mode_);
    const bool for_replay = false;
    ObMemtableCtx* mt_ctx = static_cast<ObMemtableCtx*>(ctx.mem_ctx_);
    ret = mt_ctx->set_leader_host(this, for_replay);
    if (OB_SUCC(ret)) {
      ret = set_(ctx, table_id, rowkey_len, columns, row, NULL, NULL);
    }
  }
  return ret;
}

int ObMemtable::set(const ObStoreCtx& ctx, const uint64_t table_id, const int64_t rowkey_len,
    const ObIArray<ObColDesc>& columns, const ObIArray<int64_t>& update_idx, const ObStoreRow& old_row,
    const ObStoreRow& new_row)
{
  int ret = OB_SUCCESS;
  ObMvccWriteGuard guard;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", K(*this));
    ret = OB_NOT_INIT;
  } else if (NULL == ctx.mem_ctx_ || 0 >= rowkey_len || rowkey_len > columns.count()) {
    TRANS_LOG(WARN, "invalid param");
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(guard.write_auth(*ctx.mem_ctx_))) {
    TRANS_LOG(WARN, "not allow to write", K(ctx));
  } else {
    share::CompatModeGuard compat_guard(mode_);
    const bool for_replay = false;
    ObMemtableCtx* mt_ctx = static_cast<ObMemtableCtx*>(ctx.mem_ctx_);
    ret = mt_ctx->set_leader_host(this, for_replay);
    if (OB_SUCC(ret)) {
      ret = set_(ctx, table_id, rowkey_len, columns, new_row, &old_row, &update_idx);
    }
  }
  return ret;
}

int ObMemtable::set_(const ObStoreCtx& ctx, const uint64_t table_id, const int64_t rowkey_len,
    const ObIArray<ObColDesc>& columns, const ObStoreRow& new_row,
    const ObStoreRow* old_row,  // old_rowcan be NULL, which means don't generate full log
    const ObIArray<int64_t>* update_idx)
{
  int ret = OB_SUCCESS;
  ObMemtableCompactWriter col_ccw;
  ObMemtableKey mtk;
  ObRowData old_row_data;
  ObStoreRowkey tmp_key(new_row.row_val_.cells_, rowkey_len);
  ObMemtableKey stored_key;
  ObMvccRow* value = NULL;
  bool is_new_add = false;
  bool is_lob_row = false;
  bool is_new_locked = false;
  bool need_explict_record_rowkey = false;
  set_begin(ctx);
  if (OB_FAIL(col_ccw.init())) {
    TRANS_LOG(WARN, "compact writer init fail", KR(ret));
  } else if (OB_FAIL(mtk.encode(table_id, columns, &tmp_key))) {
    TRANS_LOG(WARN, "mtk encode fail", "ret", ret);
    // There is no need to explicitly record the rowkey information in the old row
    // becausse rowkey is already included in old row
  } else if (NULL != old_row &&
             OB_FAIL(m_column_compact(need_explict_record_rowkey, mtk, columns, NULL, *old_row, col_ccw, is_lob_row))) {
    TRANS_LOG(WARN, "compact old_row fail", K(ret));
  } else if (NULL != old_row && OB_FAIL(m_clone_row_data(*ctx.mem_ctx_, col_ccw, old_row_data))) {
    TRANS_LOG(WARN, "clone_row_data fail", "ret", ret);
  } else {
    // 1. If there is no old row, it means that it is an insert scenario,
    //    and the sql layer has its own rowkey information, there is no need to explicitly record
    // 2. The scenario with old row is update-like, and the persistence of new row
    //    needs to record rowkey explicitly
    need_explict_record_rowkey = ((NULL == old_row) ? false : true);
    RowHeaderGetter getter;
    if (OB_FAIL(m_prepare_kv(ctx, &mtk, &stored_key, value, getter, false, columns, is_new_add, is_new_locked))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
        TRANS_LOG(WARN, "prepare_kv fail", K(ret));
      }
    } else if (OB_FAIL(m_column_compact(
                   need_explict_record_rowkey, mtk, columns, update_idx, new_row, col_ccw, is_lob_row))) {
      TRANS_LOG(WARN, "column compact fail", K(ret));
    } else {
      ObMemtableData mtd(new_row.get_dml(), col_ccw.size(), col_ccw.get_buf());
      if (OB_FAIL(mvcc_engine_.store_data(*ctx.mem_ctx_,
              &stored_key,
              *value,
              &mtd,
              NULL == old_row ? NULL : &old_row_data,
              timestamp_,
              ctx.sql_no_))) {
        TRANS_LOG(WARN, "mvcc engine set fail", "ret", ret);
      }
    }
  }
  // release memory applied for old row
  if (OB_FAIL(ret) && NULL != old_row && old_row_data.size_ > 0 && NULL != old_row_data.data_) {
    if (NULL == ctx.mem_ctx_) {
      TRANS_LOG(ERROR, "mem_ctx is null, unexpected error", K(ctx));
    } else {
      ctx.mem_ctx_->callback_free((void*)(old_row_data.data_));
      old_row_data.reset();
    }
  }
  set_end(ctx, ret);
  if (OB_SUCC(ret)) {
    set_max_schema_version(ctx.mem_ctx_->get_max_table_version());
  }

  if (OB_FAIL(ret) && (OB_TRY_LOCK_ROW_CONFLICT != ret)) {
    if (OB_TRANSACTION_SET_VIOLATION == ret &&
        (ObTransIsolation::SERIALIZABLE == ctx.isolation_ || ObTransIsolation::REPEATABLE_READ == ctx.isolation_)) {
      ret = OB_TRANS_CANNOT_SERIALIZE;
    }
    TRANS_LOG(WARN,
        "set end, fail",
        "ret",
        ret,
        "table_id_",
        key_.table_id_,
        "table_id",
        table_id,
        "rowkey_len",
        rowkey_len,
        "columns",
        strarray<ObColDesc>(columns),
        "new_row",
        (is_lob_row ? "" : to_cstring(new_row)),
        "ctx",
        STR_PTR(ctx.mem_ctx_));
  }
  return ret;
}

int ObMemtable::lock(const ObStoreCtx& ctx, const uint64_t table_id,
    const common::ObIArray<share::schema::ObColDesc>& columns, ObNewRowIterator& row_iter)
{
  int ret = OB_SUCCESS;
  ObMvccWriteGuard guard;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", K(*this));
    ret = OB_NOT_INIT;
  } else if (NULL == ctx.mem_ctx_) {
    TRANS_LOG(WARN, "invalid param");
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(guard.write_auth(*ctx.mem_ctx_))) {
    TRANS_LOG(WARN, "not allow to write", K(ctx));
  } else {
    ObNewRow* row = NULL;
    ObMemtableKey mtk;
    const bool for_replay = false;
    ObMemtableCtx* mt_ctx = static_cast<ObMemtableCtx*>(ctx.mem_ctx_);
    ObMemtableCompactWriter col_ccw;
    if (OB_FAIL(col_ccw.init())) {
      TRANS_LOG(WARN, "compact writer init fail", KR(ret));
    } else if (OB_FAIL(mt_ctx->set_leader_host(this, for_replay))) {
      TRANS_LOG(WARN, "set leader host fail", KR(ret));
    }
    while (OB_SUCCESS == ret && OB_SUCCESS == (ret = row_iter.get_next_row(row))) {
      if (OB_ISNULL(row)) {
        TRANS_LOG(WARN, "row iter get next row null pointer");
        ret = OB_ERR_UNEXPECTED;
      } else {
        ObStoreRowkey tmp_key(row->cells_, columns.count());
        ObMemtableKey stored_key;
        bool is_new_locked = false;
        mtk.reset();
        col_ccw.reset();
        ObMvccRow* value = NULL;
        if (OB_FAIL(mtk.encode(table_id, columns, &tmp_key))) {
          TRANS_LOG(WARN, "mtk encode fail", "ret", ret);
        } else if (OB_FAIL(m_column_compact(col_ccw, mtk, columns))) {
          TRANS_LOG(WARN, "column compact failed", K(ret));
        } else if (OB_FAIL(m_lock(ctx, table_id, columns, &mtk, &stored_key, value, is_new_locked))) {
          TRANS_LOG(WARN, "mvcc engine lock fail", "ret", ret);
        } else if (cluster_version_before_2100_()) {
          TRANS_LOG(DEBUG, "cluster version before 2100", K(table_id), K(ctx.mem_ctx_));
        } else if (OB_UNLIKELY(!is_new_locked)) {
          TRANS_LOG(DEBUG, "lock twice, no need to store lock trans node", K(table_id), K(ctx.mem_ctx_));
        } else {
          ObMemtableData mtd(T_DML_LOCK, col_ccw.size(), col_ccw.get_buf());
          if (OB_FAIL(
                  mvcc_engine_.store_data(*ctx.mem_ctx_, &stored_key, *value, &mtd, NULL, timestamp_, ctx.sql_no_))) {
            TRANS_LOG(WARN, "store data failed", K(ret), K(table_id), K(ctx.mem_ctx_));
          } else {
            TRANS_LOG(DEBUG, "store data success", K(ret), K(table_id), K(ctx.mem_ctx_));
          }
        }
      }
    }
    ret = (OB_ITER_END == ret) ? OB_SUCCESS : ret;
  }
  if (OB_FAIL(ret) && (OB_TRY_LOCK_ROW_CONFLICT != ret)) {
    TRANS_LOG(WARN,
        "lock fail",
        "ret",
        ret,
        "table_id",
        key_.table_id_,
        "ctx",
        STR_PTR(ctx.mem_ctx_),
        "table_id",
        table_id,
        "row_iter_ptr",
        &row_iter);
  }
  return ret;
}

int ObMemtable::lock(const ObStoreCtx& ctx, const uint64_t table_id,
    const common::ObIArray<share::schema::ObColDesc>& columns, const ObNewRow& row)
{
  int ret = OB_SUCCESS;
  ObMvccWriteGuard guard;
  ObStoreRowkey tmp_key(row.cells_, columns.count());
  ObMemtableKey mtk;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", K(*this));
    ret = OB_NOT_INIT;
  } else if (NULL == ctx.mem_ctx_ || row.count_ < columns.count()) {
    TRANS_LOG(WARN, "invalid param");
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(guard.write_auth(*ctx.mem_ctx_))) {
    TRANS_LOG(WARN, "not allow to write", K(ctx));
  } else if (OB_FAIL(mtk.encode(table_id, columns, &tmp_key))) {
    TRANS_LOG(WARN, "mtk encode fail", "ret", ret);
  } else {
    const bool for_replay = false;
    bool is_new_locked = false;
    ObMemtableCtx* mt_ctx = static_cast<ObMemtableCtx*>(ctx.mem_ctx_);
    ObMemtableKey stored_key;
    ObMvccRow* value = NULL;
    ObMemtableCompactWriter col_ccw;
    if (OB_FAIL(col_ccw.init())) {
      TRANS_LOG(WARN, "compact writer init fail", KR(ret));
    } else if (OB_FAIL(m_column_compact(col_ccw, mtk, columns))) {
      TRANS_LOG(WARN, "column compact failed", K(ret));
    } else if (OB_FAIL(mt_ctx->set_leader_host(this, for_replay))) {
      TRANS_LOG(WARN, "set host failed", K(ret));
    } else if (OB_FAIL(m_lock(ctx, table_id, columns, &mtk, &stored_key, value, is_new_locked))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        TRANS_LOG(WARN, "mvcc engine lock fail", "ret", ret);
      }
    } else if (cluster_version_before_2100_()) {
      TRANS_LOG(DEBUG, "cluster version before 2100", K(table_id), K(ctx.mem_ctx_));
    } else if (OB_UNLIKELY(!is_new_locked)) {
      TRANS_LOG(DEBUG, "lock twice, no need to store lock trans node", K(table_id), K(ctx.mem_ctx_));
    } else {
      ObMemtableData mtd(T_DML_LOCK, col_ccw.size(), col_ccw.get_buf());
      if (OB_FAIL(mvcc_engine_.store_data(*ctx.mem_ctx_, &stored_key, *value, &mtd, NULL, timestamp_, ctx.sql_no_))) {
        TRANS_LOG(WARN, "store data failed", K(ret), K(table_id), K(ctx.mem_ctx_));
      } else {
        TRANS_LOG(DEBUG, "store data success", K(ret), K(table_id), K(ctx.mem_ctx_));
      }
    }
  }
  if (OB_FAIL(ret) && (OB_TRY_LOCK_ROW_CONFLICT != ret)) {
    TRANS_LOG(WARN,
        "lock fail",
        "ret",
        ret,
        "table_id",
        key_.table_id_,
        "ctx",
        STR_PTR(ctx.mem_ctx_),
        "table_id",
        table_id,
        "row",
        row,
        "mtk",
        mtk);
  }
  return ret;
}

int ObMemtable::lock(const ObStoreCtx& ctx, const uint64_t table_id,
    const common::ObIArray<share::schema::ObColDesc>& columns, const ObStoreRowkey& rowkey)
{
  int ret = OB_SUCCESS;
  ObMemtableKey mtk;
  ObMvccWriteGuard guard;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", K(*this));
    ret = OB_NOT_INIT;
  } else if (NULL == ctx.mem_ctx_ || 0 >= rowkey.get_obj_cnt() || rowkey.get_obj_cnt() > columns.count()) {
    TRANS_LOG(WARN, "invalid param");
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(guard.write_auth(*ctx.mem_ctx_))) {
    TRANS_LOG(WARN, "not allow to write", K(ctx));
  } else if (OB_FAIL(mtk.encode(table_id, columns, &rowkey))) {
    TRANS_LOG(WARN, "mtk encode fail", "ret", ret);
  } else {
    const bool for_replay = false;
    bool is_new_locked = false;
    ObMemtableCtx* mt_ctx = static_cast<ObMemtableCtx*>(ctx.mem_ctx_);
    ObMemtableKey stored_key;
    ObMvccRow* value = NULL;
    ObMemtableCompactWriter col_ccw;
    if (OB_FAIL(col_ccw.init())) {
      TRANS_LOG(WARN, "compact writer init fail", KR(ret));
    } else if (OB_FAIL(m_column_compact(col_ccw, mtk, columns))) {
      TRANS_LOG(WARN, "column compact failed", K(ret));
    } else if (OB_FAIL(mt_ctx->set_leader_host(this, for_replay))) {
      TRANS_LOG(WARN, "set host failed", K(ret));
    } else if (OB_FAIL(m_lock(ctx, table_id, columns, &mtk, &stored_key, value, is_new_locked))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
        TRANS_LOG(WARN, "mvcc engine lock fail", "ret", ret);
      }
    } else if (cluster_version_before_2100_()) {
      TRANS_LOG(DEBUG, "cluster version before 2100", K(table_id), K(ctx.mem_ctx_));
    } else if (OB_UNLIKELY(!is_new_locked)) {
      TRANS_LOG(DEBUG, "lock twice, no need to store lock trans node", K(table_id), K(ctx.mem_ctx_));
    } else {
      ObMemtableData mtd(T_DML_LOCK, col_ccw.size(), col_ccw.get_buf());
      if (OB_FAIL(mvcc_engine_.store_data(*ctx.mem_ctx_, &stored_key, *value, &mtd, NULL, timestamp_, ctx.sql_no_))) {
        TRANS_LOG(WARN, "store data failed", K(ret), K(table_id), K(ctx.mem_ctx_));
      } else {
        TRANS_LOG(DEBUG, "store data success", K(ret), K(table_id), K(ctx.mem_ctx_));
      }
    }
  }
  if (OB_FAIL(ret) && (OB_TRY_LOCK_ROW_CONFLICT != ret) && (OB_TRANSACTION_SET_VIOLATION != ret)) {
    TRANS_LOG(WARN,
        "lock fail",
        "ret",
        ret,
        "table_id",
        key_.table_id_,
        "ctx",
        STR_PTR(ctx.mem_ctx_),
        "table_id",
        table_id,
        "row",
        rowkey,
        "mtk",
        mtk);
  }
  return ret;
}

int ObMemtable::check_row_locked_by_myself(const ObStoreCtx& ctx, const uint64_t table_id,
    const common::ObIArray<share::schema::ObColDesc>& columns, const ObStoreRowkey& rowkey, bool& locked)
{
  int ret = OB_SUCCESS;
  ObMemtableKey mtk;
  ObMvccWriteGuard guard;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", K(*this));
    ret = OB_NOT_INIT;
  } else if (NULL == ctx.mem_ctx_ || 0 >= rowkey.get_obj_cnt() || rowkey.get_obj_cnt() > columns.count()) {
    TRANS_LOG(WARN, "invalid param");
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(guard.write_auth(*ctx.mem_ctx_))) {
    TRANS_LOG(WARN, "not allow to write", K(ctx));
  } else if (OB_FAIL(mtk.encode(table_id, columns, &rowkey))) {
    TRANS_LOG(WARN, "mtk encode fail", "ret", ret);
  } else {
    bool is_locked = false;
    uint32_t lock_descriptor = 0;
    bool tmp_is_locked = false;
    int64_t trans_version = 0;
    ObStoreRowLockState lock_state;
    const ObIArray<ObITable*>* stores = ctx.tables_;
    for (int64_t i = stores->count() - 1; OB_SUCC(ret) && !is_locked && i >= 0; i--) {
      lock_state.reset();
      if (NULL == stores->at(i)) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "ObIStore is null", K(ret), K(i));
      } else if (stores->at(i)->is_memtable()) {
        ObMemtable* memtable = static_cast<ObMemtable*>(stores->at(i));
        if (OB_FAIL(memtable->get_mvcc_engine().check_row_locked(
                *ctx.mem_ctx_, &mtk, tmp_is_locked, lock_descriptor, trans_version))) {
          if (ret == OB_ERR_EXCLUSIVE_LOCK_CONFLICT || ret == OB_TRY_LOCK_ROW_CONFLICT) {
            // it is locked by others. will not locked by myself
            ret = OB_SUCCESS;
            break;
          } else {
            TRANS_LOG(WARN, "mvcc engine check row lock fail", K(ret), K(tmp_is_locked), K(lock_descriptor));
          }
        } else if (tmp_is_locked && lock_descriptor == ctx.mem_ctx_->get_ctx_descriptor()) {
          is_locked = true;
        }
      } else if (stores->at(i)->is_sstable()) {
        ObSSTable* sstable = static_cast<ObSSTable*>(stores->at(i));
        if (OB_FAIL(sstable->check_row_locked(ctx, rowkey, columns, lock_state))) {
          if (ret == OB_ERR_EXCLUSIVE_LOCK_CONFLICT || ret == OB_TRY_LOCK_ROW_CONFLICT) {
            // it is locked by others. will not locked by myself
            ret = OB_SUCCESS;
            break;
          } else {
            TRANS_LOG(WARN, "sstable check row lock fail", K(ret), K(rowkey), K(lock_state));
          }
        } else if (lock_state.is_locked_ && lock_state.lock_trans_id_ == ctx.trans_id_) {
          is_locked = true;
        }
        TRANS_LOG(DEBUG, "check_row_locked meet sstable", K(ret), K(rowkey), K(*sstable), K(is_locked));
      } else {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "unknown store type", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      locked = is_locked;
    }
  }

  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////////////
void ObMemtable::get_begin(const ObStoreCtx& ctx)
{
  if (NULL != ctx.mem_ctx_) {
    static_cast<ObMemtableCtx*>(ctx.mem_ctx_)->set_handle_start_time(ObTimeUtility::current_time());
    EVENT_INC(MEMSTORE_GET_COUNT);
  }
}

void ObMemtable::get_end(const ObStoreCtx& ctx, int ret)
{

  if (NULL != ctx.mem_ctx_) {
    if (OB_SUCC(ret)) {
      EVENT_INC(MEMSTORE_GET_SUCC_COUNT);
    } else {
      EVENT_INC(MEMSTORE_GET_FAIL_COUNT);
    }
    EVENT_ADD(MEMSTORE_GET_TIME,
        ObTimeUtility::current_time() - (static_cast<ObMemtableCtx*>(ctx.mem_ctx_))->get_handle_start_time());
  }
}
int ObMemtable::exist(const ObStoreCtx& ctx, const uint64_t table_id, const common::ObStoreRowkey& rowkey,
    const common::ObIArray<share::schema::ObColDesc>& columns, bool& is_exist, bool& has_found)
{
  int ret = OB_SUCCESS;
  ObMemtableKey parameter_mtk;
  ObMemtableKey returned_mtk;
  ObTransSnapInfo snapshot_info;
  ObMvccValueIterator value_iter;
  ObQueryFlag query_flag;
  query_flag.read_latest_ = true;
  query_flag.prewarm_ = false;
  get_begin(ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(*this), K(ret));
  } else if (NULL == ctx.mem_ctx_ || 0 >= rowkey.get_obj_cnt() || NULL == rowkey.get_obj_ptr() ||
             rowkey.get_obj_cnt() > columns.count()) {
    TRANS_LOG(WARN, "invalid param");
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ctx.mem_ctx_->get_trans_status())) {
    TRANS_LOG(WARN, "trans already end", K(ret));
  } else if (OB_FAIL(ctx.get_snapshot_info(snapshot_info))) {
    TRANS_LOG(WARN, "get snapshot info failed", K(ret));
  } else if (OB_FAIL(parameter_mtk.encode(table_id, columns, &rowkey))) {
    TRANS_LOG(WARN, "mtk encode fail", "ret", ret);
  } else if (OB_FAIL(mvcc_engine_.get(*ctx.mem_ctx_,
                 snapshot_info,
                 query_flag,
                 // skip_compact
                 false,
                 &parameter_mtk,
                 &returned_mtk,
                 value_iter))) {
    TRANS_LOG(WARN, "get value iter fail, ", K(ret));
  } else if (OB_FAIL(ObReadRow::exist(value_iter, is_exist, has_found))) {
    TRANS_LOG(WARN, "Fail to check rowkey exist, ", K(ret));
  } else {
    TRANS_LOG(DEBUG, "Check memtable exist rowkey, ", K(table_id), K(rowkey), K(columns), K(is_exist), K(has_found));
  }
  get_end(ctx, ret);
  return ret;
}

int ObMemtable::prefix_exist(ObRowsInfo& rows_info, bool& may_exist)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!rows_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to prefix exist scan", K(rows_info), K(ret));
  } else {
    const ObStoreCtx& ctx = rows_info.exist_helper_.get_store_ctx();
    ObMemtableKey parameter_mtk;
    const ObStoreRowkey& rowkey = rows_info.get_prefix_rowkey().get_store_rowkey();
    get_begin(ctx);
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      TRANS_LOG(WARN, "not init", K(*this), K(ret));
    } else if (NULL == ctx.mem_ctx_ || !rowkey.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid param", K(ret), KP(ctx.mem_ctx_), K(rowkey));
    } else if (OB_FAIL(ctx.mem_ctx_->get_trans_status())) {
      TRANS_LOG(WARN, "trans already end", K(ret));
    } else if (OB_FAIL(parameter_mtk.encode(rows_info.table_id_, rows_info.exist_helper_.get_col_desc(), &rowkey))) {
      TRANS_LOG(WARN, "mtk encode fail", "ret", ret);
    } else if (OB_FAIL(mvcc_engine_.prefix_exist(*ctx.mem_ctx_, &parameter_mtk, may_exist))) {
      TRANS_LOG(WARN, "failed to prefix exist memtable", K(ret));
    } else {
      TRANS_LOG(DEBUG, "Check memtable prefix exist rowkey, ", K(rowkey), K(may_exist), K(rows_info));
    }
    get_end(ctx, ret);
  }
  return ret;
}

int ObMemtable::exist(storage::ObRowsInfo& rows_info, bool& is_exist, bool& all_rows_found)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!rows_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to prefix exist scan", K(rows_info), K(ret));
  } else {
    const ObStoreCtx& ctx = rows_info.exist_helper_.get_store_ctx();
    const ObIArray<share::schema::ObColDesc>& columns = rows_info.exist_helper_.get_col_desc();

    is_exist = false;
    all_rows_found = false;
    for (int64_t i = 0; OB_SUCC(ret) && !is_exist && !all_rows_found && i < rows_info.ext_rowkeys_.count(); i++) {
      const common::ObStoreRowkey& rowkey = rows_info.ext_rowkeys_.at(i).get_store_rowkey();
      bool row_found = false;
      if (rowkey.is_max()) {  // skip deleted rowkey
      } else if (OB_FAIL(exist(ctx, rows_info.table_id_, rowkey, columns, is_exist, row_found))) {
        TRANS_LOG(WARN, "fail to check rowkey exist", K((ret)));
      } else if (is_exist) {
        rows_info.get_duplicate_rowkey() = rowkey;
        all_rows_found = true;
      } else if (!row_found) {  // skip
      } else if (OB_FAIL(rows_info.clear_found_rowkey(i))) {
        STORAGE_LOG(WARN, "Failed to clear rowkey in rowsinfo", K(ret));
      } else {
        all_rows_found = rows_info.all_rows_found();
      }
    }
  }
  return ret;
}
////////////////////////////////////////////////////////////////////////////////////////////////////

void ObMemtable::scan_begin(const ObStoreCtx& ctx)
{
  if (NULL != ctx.mem_ctx_) {
    (static_cast<ObMemtableCtx*>(ctx.mem_ctx_))->set_handle_start_time(ObTimeUtility::current_time());
    EVENT_INC(MEMSTORE_SCAN_COUNT);
  }
}

void ObMemtable::scan_end(const ObStoreCtx& ctx, int ret)
{
  if (NULL != ctx.mem_ctx_) {
    if (OB_SUCC(ret)) {
      EVENT_INC(MEMSTORE_SCAN_SUCC_COUNT);
    } else {
      EVENT_INC(MEMSTORE_SCAN_FAIL_COUNT);
    }
    EVENT_ADD(MEMSTORE_SCAN_TIME,
        ObTimeUtility::current_time() - (static_cast<ObMemtableCtx*>(ctx.mem_ctx_))->get_handle_start_time());
  }
}

int ObMemtable::get(const storage::ObTableIterParam& param, storage::ObTableAccessContext& context,
    const common::ObExtStoreRowkey& rowkey, storage::ObStoreRow& row)
{
  int ret = OB_SUCCESS;
  ObMemtableKey parameter_mtk;
  ObMemtableKey returned_mtk;
  ObMvccValueIterator value_iter;
  const common::ObIArray<share::schema::ObColDesc>* out_cols = nullptr;
  bool need_query_memtable = false;
  ObTransSnapInfo snapshot_info;
  const bool skip_compact = false;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", K(*this));
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!param.is_valid()) || OB_UNLIKELY(!context.is_valid()) ||
             OB_UNLIKELY(!rowkey.get_store_rowkey().is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid param, ", K(param), K(context), K(rowkey));
  } else if (OB_FAIL(context.store_ctx_->mem_ctx_->get_trans_status())) {
    TRANS_LOG(WARN, "trans already end", K(ret));
  } else if (OB_FAIL(param.get_out_cols(context.use_fuse_row_cache_, out_cols))) {
    TRANS_LOG(WARN, "fail to get out cols", K(ret));
  } else if (OB_FAIL(parameter_mtk.encode(param.table_id_, *out_cols, &(rowkey.get_store_rowkey())))) {
    TRANS_LOG(WARN, "mtk encode fail", "ret", ret);
  } else if (OB_FAIL(context.store_ctx_->get_snapshot_info(snapshot_info))) {
    TRANS_LOG(WARN, "get snapshot info failed", K(ret));
  } else {
    bool fast_query = false;
    const ObFastQueryContext* fq_ctx = nullptr;
    if (nullptr != context.fq_ctx_) {
      fq_ctx = context.fq_ctx_;
      fast_query =
          timestamp_ == fq_ctx->get_timestamp() && this == fq_ctx->get_memtable() && nullptr != fq_ctx->get_mvcc_row();
    }
    if (fast_query && !context.store_ctx_->mem_ctx_->is_can_elr()) {
      int64_t trans_version = 0L;
      if (OB_FAIL(mvcc_engine_.get_trans_version(*context.store_ctx_->mem_ctx_,
              snapshot_info,
              context.query_flag_,
              &parameter_mtk,
              reinterpret_cast<ObMvccRow*>(fq_ctx->get_mvcc_row()),
              trans_version))) {
        TRANS_LOG(WARN, "fail to do mvcc engine fast get", K(ret));
      } else {
        if (trans_version == fq_ctx->get_row_version()) {
          // just read from row cache
          // do not set memtable row
          row.flag_ = common::ObActionFlag::OP_ROW_DOES_NOT_EXIST;
          row.fq_ctx_.set_timestamp(-1L);
          row.fq_ctx_.set_memtable(nullptr);
          row.fq_ctx_.set_mvcc_row(nullptr);
          row.fq_ctx_.set_row_version(0L);
          row.snapshot_version_ = trans_version;
          TRANS_LOG(DEBUG, "do fast get successfully", K(rowkey), K(trans_version), K(*fq_ctx));
        } else {
          if (OB_FAIL(mvcc_engine_.get(*context.store_ctx_->mem_ctx_,
                  snapshot_info,
                  context.query_flag_,
                  skip_compact,
                  &parameter_mtk,
                  &returned_mtk,
                  value_iter))) {
            TRANS_LOG(WARN, "fail to do mvcc engine get", K(ret));
          } else {
            need_query_memtable = true;
          }
        }
      }
    } else {
      if (OB_FAIL(mvcc_engine_.get(*context.store_ctx_->mem_ctx_,
              snapshot_info,
              context.query_flag_,
              skip_compact,
              &parameter_mtk,
              &returned_mtk,
              value_iter))) {
        TRANS_LOG(WARN, "fail to do mvcc engine get", K(ret));
      } else {
        need_query_memtable = true;
      }
    }
  }

  if (OB_FAIL(ret) || !need_query_memtable) {
  } else {
    ColumnMap* local_map = NULL;
    const ColumnMap* param_column_map = nullptr;
    if (nullptr == row.row_val_.cells_) {
      if (nullptr ==
          (row.row_val_.cells_ = static_cast<ObObj*>(context.allocator_->alloc(sizeof(ObObj) * out_cols->count())))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TRANS_LOG(WARN, "Fail to allocate memory, ", K(ret));
      } else {
        row.row_val_.count_ = out_cols->count();
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(param.get_column_map(context.use_fuse_row_cache_, param_column_map))) {
      TRANS_LOG(WARN, "fail to get column map", K(ret));
    } else if (NULL == param_column_map) {
      void* buf = NULL;
      if (NULL == (buf = context.allocator_->alloc(sizeof(ColumnMap)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TRANS_LOG(WARN, "Fail to allocate memory, ", K(ret));
      } else {
        local_map = new (buf) ColumnMap(*context.allocator_);
        if (OB_FAIL(local_map->init(*out_cols))) {
          TRANS_LOG(WARN, "Fail to build column map, ", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      const ObStoreRowkey* rowkey = NULL;
      if (NULL != returned_mtk.get_rowkey()) {
        returned_mtk.get_rowkey(rowkey);
      } else {
        parameter_mtk.get_rowkey(rowkey);
      }
      const int64_t col_cnt = out_cols->count();
      for (int64_t i = 0; i < col_cnt; ++i) {
        row.row_val_.cells_[i].copy_meta_type(out_cols->at(i).col_type_);
      }
      ObNopBitMap bitmap;
      bitmap.init(out_cols->count(), rowkey->get_obj_cnt());
      bool key_has_null = false;
      bool has_null = false;
      int64_t row_scn = 0;
      if (OB_FAIL(ObReadRow::iterate_row(nullptr == param_column_map ? *local_map : *param_column_map,
              *out_cols,
              *rowkey,
              &value_iter,
              row,
              bitmap,
              has_null,
              key_has_null,
              row_scn))) {
        TRANS_LOG(WARN, "iterate row value fail", "ret", ret);
      } else {
        if (param.need_scn_) {
          for (int64_t i = 0; i < out_cols->count(); i++) {
            if (out_cols->at(i).col_id_ == OB_HIDDEN_TRANS_VERSION_COLUMN_ID) {
              row.row_val_.cells_[i].set_int(row_scn);
              TRANS_LOG(DEBUG, "set row scn is", K(i), K(row_scn), K(row));
            }
          }
        }
        row.fq_ctx_.set_timestamp(timestamp_);
        row.fq_ctx_.set_memtable(this);
        row.fq_ctx_.set_mvcc_row(const_cast<ObMvccRow*>(value_iter.get_mvcc_row()));
        row.fq_ctx_.set_row_version(row.snapshot_version_);
      }
    }
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(
        WARN, "get end, fail", "ret", ret, "table_id_", key_.table_id_, "table_id", param.table_id_, "rowkey", rowkey);
  }
  return ret;
}

/* as an example
void ObMemtable::test_range_split(int64_t table_id, ObStoreRowkey* start_key, ObStoreRowkey* end_key)
{
  int ret = 0;
  int32_t first_key = 0;
  ObObj first_obj = start_key->get_rowkey().ptr()[0];
  if ((table_id & 0xffffff) < 50000 || 0 != first_obj.get_int32(first_key) || 1234567 != first_key) return;
  TRANS_LOG(ERROR, "XXXX, first key match", K(table_id));
  int64_t total_bytes = 0, total_rows = 0;
  if (OB_FAIL(estimate_phy_size(table_id, start_key, end_key, total_bytes, total_rows))) {
    TRANS_LOG(ERROR, "XXXX, estimate phy size fail", K(ret));
  }
  ObStoreRowkey key_array[64];
  if (OB_FAIL(get_range_split_keys(table_id, start_key, end_key, 7, key_array))) {
    TRANS_LOG(ERROR, "XXXX, split range fail", K(ret));
  } else {
    for(int i = 0; i < 6; i++) {
      TRANS_LOG(INFO, "XXXX, split key", K(i), K(key_array[i]));
    }
  }
}
*/

int ObMemtable::get(const storage::ObTableIterParam& param, storage::ObTableAccessContext& context,
    const common::ObExtStoreRowkey& rowkey, ObStoreRowIterator*& row_iter)
{
  int ret = OB_SUCCESS;
  void* get_iter_buffer = NULL;
  ObMemtableGetIterator* get_iter_ptr = NULL;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", K(*this));
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!param.is_valid() || !context.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument, ", K(ret), K(param), K(context));
  } else if (OB_FAIL(context.store_ctx_->mem_ctx_->get_trans_status())) {
    TRANS_LOG(WARN, "trans already end", K(ret));
  } else if (NULL == (get_iter_buffer = context.allocator_->alloc(sizeof(ObMemtableGetIterator))) ||
             NULL == (get_iter_ptr = new (get_iter_buffer) ObMemtableGetIterator())) {
    TRANS_LOG(WARN, "construct ObMemtableGetIterator fail");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  if (OB_SUCC(ret) && NULL != get_iter_ptr) {
    if (OB_FAIL(get_iter_ptr->init(param, context, this, &rowkey))) {
      TRANS_LOG(WARN, "get iter init fail", K(ret), K(param), K(context), K(rowkey));
    } else {
      row_iter = get_iter_ptr;
    }
  }
  if (OB_FAIL(ret)) {
    if (NULL != get_iter_ptr) {
      get_iter_ptr->~ObMemtableGetIterator();
      get_iter_ptr = NULL;
    }
    TRANS_LOG(WARN, "get fail", K(ret), K_(key), K(param.table_id_));
  }
  return ret;
}

// multi-version scan interface
// usage:
// set is_multi_version_merge_ in ObTableAccessParam as true(default false)
// set mem_ctx sub_trans_begin
// set start version of multi-version in mem ctx
// check validity of mem ctx multi version range, is_multi_version_range_valid
int ObMemtable::scan(const storage::ObTableIterParam& param, storage::ObTableAccessContext& context,
    const ObExtStoreRange& ext_range, ObStoreRowIterator*& row_iter)
{
  int ret = OB_SUCCESS;
  ObStoreRange real_range;
  const ObStoreRange& range = ext_range.get_range();
  void* scan_iter_buffer = NULL;
  ObIMemtableScanIterator* scan_iter_ptr = NULL;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", K(*this));
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!param.is_valid() || !context.is_valid() || 0 >= range.get_start_key().get_obj_cnt() ||
                         NULL == range.get_start_key().get_obj_ptr() || 0 >= range.get_end_key().get_obj_cnt() ||
                         NULL == range.get_end_key().get_obj_ptr())) {
    TRANS_LOG(WARN, "invalid param", K(param), K(context), K(range));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(context.store_ctx_->mem_ctx_->get_trans_status())) {
    TRANS_LOG(WARN, "trans already end", K(ret));
  } else {
    if (param.is_multi_version_minor_merge_) {
      if (GCONF._enable_sparse_row) {
        if (NULL == (scan_iter_buffer = context.allocator_->alloc(sizeof(ObMemtableMultiVersionScanSparseIterator))) ||
            NULL == (scan_iter_ptr = new (scan_iter_buffer) ObMemtableMultiVersionScanSparseIterator())) {
          TRANS_LOG(WARN,
              "construct ObMemtableMultiVersionScanSparseIterator fail",
              "scan_iter_buffer",
              scan_iter_buffer,
              "scan_iter_ptr",
              scan_iter_ptr);
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else if (OB_FAIL(((ObMemtableMultiVersionScanSparseIterator*)scan_iter_ptr)
                               ->init(param,
                                   context,
                                   this,
                                   &m_get_real_range_(real_range, range, context.query_flag_.is_reverse_scan())))) {
          TRANS_LOG(WARN, "scan iter init fail", "ret", ret, K(real_range), K(param), K(context));
        }
      } else {
        if (NULL == (scan_iter_buffer = context.allocator_->alloc(sizeof(ObMemtableMultiVersionScanIterator))) ||
            NULL == (scan_iter_ptr = new (scan_iter_buffer) ObMemtableMultiVersionScanIterator())) {
          TRANS_LOG(WARN,
              "construct ObMemtableScanIterator fail",
              "scan_iter_buffer",
              scan_iter_buffer,
              "scan_iter_ptr",
              scan_iter_ptr);
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else if (OB_FAIL(((ObMemtableMultiVersionScanIterator*)scan_iter_ptr)
                               ->init(param,
                                   context,
                                   this,
                                   &m_get_real_range_(real_range, range, context.query_flag_.is_reverse_scan())))) {
          TRANS_LOG(WARN, "scan iter init fail", "ret", ret, K(real_range), K(param), K(context));
        }
      }
    } else {
      if (NULL == (scan_iter_buffer = context.allocator_->alloc(sizeof(ObMemtableScanIterator))) ||
          NULL == (scan_iter_ptr = new (scan_iter_buffer) ObMemtableScanIterator())) {
        TRANS_LOG(WARN,
            "construct ObMemtableScanIterator fail",
            "scan_iter_buffer",
            scan_iter_buffer,
            "scan_iter_ptr",
            scan_iter_ptr);
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(((ObMemtableScanIterator*)scan_iter_ptr)->init(param, context, this, &ext_range))) {
          TRANS_LOG(WARN, "scan iter init fail", "ret", ret, K(param), K(context), K(ext_range));
        }
      }
    }

    if (OB_SUCC(ret)) {
      row_iter = scan_iter_ptr;
    } else {
      if (NULL != scan_iter_ptr) {
        scan_iter_ptr->~ObIMemtableScanIterator();
        scan_iter_ptr = NULL;
      }
      TRANS_LOG(
          WARN, "scan end, fail", "ret", ret, "table_id_", key_.table_id_, "table_id", param.table_id_, "range", range);
    }
  }
  return ret;
}

int ObMemtable::multi_get(const storage::ObTableIterParam& param, storage::ObTableAccessContext& context,
    const ObIArray<ObExtStoreRowkey>& rowkeys, ObStoreRowIterator*& row_iter)
{
  int ret = OB_SUCCESS;
  void* mget_iter_buffer = NULL;
  ObMemtableMGetIterator* mget_iter_ptr = NULL;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", K(*this));
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!param.is_valid() || !context.is_valid() || (0 == rowkeys.count()))) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument, ", K(ret), K(param), K(context), K(rowkeys));
  } else if (OB_FAIL(context.store_ctx_->mem_ctx_->get_trans_status())) {
    TRANS_LOG(WARN, "trans already end", K(ret));
  } else if (NULL == (mget_iter_buffer = context.allocator_->alloc(sizeof(ObMemtableMGetIterator))) ||
             NULL == (mget_iter_ptr = new (mget_iter_buffer) ObMemtableMGetIterator())) {
    TRANS_LOG(WARN,
        "construct ObMemtableMGetIterator fail",
        "mget_iter_buffer",
        mget_iter_buffer,
        "mget_iter_ptr",
        mget_iter_ptr);
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  if (OB_SUCC(ret) && NULL != mget_iter_ptr) {
    if (OB_FAIL(mget_iter_ptr->init(param, context, this, &rowkeys))) {
      TRANS_LOG(WARN, "mget iter init fail", "ret", ret, K(param), K(context), K(rowkeys));
    } else {
      row_iter = mget_iter_ptr;
    }
  }
  if (OB_FAIL(ret)) {
    if (NULL != mget_iter_ptr) {
      mget_iter_ptr->~ObMemtableMGetIterator();
      mget_iter_ptr = NULL;
    }
    TRANS_LOG(WARN,
        "mget fail",
        "ret",
        ret,
        "table_id",
        key_.table_id_,
        "table_id",
        param.table_id_,
        "rowkeys",
        strarray<ObExtStoreRowkey>(rowkeys));
  }
  return ret;
}

int ObMemtable::multi_scan(const storage::ObTableIterParam& param, storage::ObTableAccessContext& context,
    const ObIArray<ObExtStoreRange>& ranges, ObStoreRowIterator*& row_iter)
{
  int ret = OB_SUCCESS;
  void* mscan_iter_buffer = NULL;
  ObMemtableMScanIterator* mscan_iter_ptr = NULL;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", "this", this);
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!param.is_valid() || !context.is_valid() || (0 == ranges.count()))) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument, ", K(ret), K(param), K(context), K(ranges));
  } else if (OB_FAIL(context.store_ctx_->mem_ctx_->get_trans_status())) {
    TRANS_LOG(WARN, "trans already end", K(ret));
  } else if (NULL == (mscan_iter_buffer = context.allocator_->alloc(sizeof(ObMemtableMScanIterator))) ||
             NULL == (mscan_iter_ptr = new (mscan_iter_buffer) ObMemtableMScanIterator())) {
    TRANS_LOG(WARN,
        "construct ObMemtableMScanIterator fail",
        "mscan_iter_buffer",
        mscan_iter_buffer,
        "mscan_iter_ptr",
        mscan_iter_ptr);
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  if (OB_SUCC(ret) && NULL != mscan_iter_ptr) {
    if (OB_FAIL(((ObMemtableMScanIterator*)mscan_iter_ptr)->init(param, context, this, &ranges))) {
      TRANS_LOG(WARN, "mscan iter init fail", "ret", ret);
    } else {
      row_iter = mscan_iter_ptr;
      STORAGE_LOG(DEBUG, "multiscan iterator inited");
    }
  }
  if (OB_FAIL(ret)) {
    if (NULL != mscan_iter_ptr) {
      mscan_iter_ptr->~ObMemtableMScanIterator();
      mscan_iter_ptr = NULL;
    }
    TRANS_LOG(WARN,
        "mscan fail",
        "ret",
        ret,
        "table_id",
        key_.table_id_,
        "table_id",
        param.table_id_,
        "ranges",
        strarray<ObExtStoreRange>(ranges));
  }
  return ret;
}

int ObMemtable::get_row_header(const ObStoreCtx& ctx, const uint64_t table_id, const common::ObStoreRowkey& rowkey,
    const common::ObIArray<share::schema::ObColDesc>& columns, uint32_t& modify_count, uint32_t& acc_checksum)
{
  int ret = OB_SUCCESS;
  ObMemtableKey parameter_mtk;
  ObMemtableKey returned_mtk;
  ObMvccValueIterator value_iter;
  ObTransSnapInfo snapshot_info;
  ObQueryFlag query_flag;
  const bool skip_compact = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(*this), K(ret));
  } else if (NULL == ctx.mem_ctx_ || 0 >= rowkey.get_obj_cnt() || NULL == rowkey.get_obj_ptr()) {
    TRANS_LOG(WARN, "invalid param");
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ctx.mem_ctx_->get_trans_status())) {
    TRANS_LOG(WARN, "trans already end", K(ret));
  } else if (OB_FAIL(ctx.get_snapshot_info(snapshot_info))) {
    TRANS_LOG(WARN, "get snapshot info failed", K(ret));
  } else if (OB_FAIL(parameter_mtk.encode(table_id, columns, &rowkey))) {
    TRANS_LOG(WARN, "mtk encode fail", K(ret), K(table_id), K(rowkey));
  } else if (OB_FAIL(mvcc_engine_.get(
                 *ctx.mem_ctx_, snapshot_info, query_flag, skip_compact, &parameter_mtk, &returned_mtk, value_iter))) {
    TRANS_LOG(WARN, "get value iter fail", K(ret));
  } else if (OB_FAIL(ObReadRow::get_row_header(value_iter, modify_count, acc_checksum))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      TRANS_LOG(WARN, "get row header erorr", K(ret));
    }
  } else {
    TRANS_LOG(DEBUG, "get row header success", K(table_id), K(rowkey), K(modify_count), K(acc_checksum));
  }
  return ret;
}

int ObMemtable::replay_schema_version_change_log(const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", K(*this));
    ret = OB_NOT_INIT;
  } else if (schema_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(schema_version));
  } else {
    set_max_schema_version(schema_version);
  }
  return ret;
}

int ObMemtable::replay(const ObStoreCtx& ctx, const char* data, const int64_t data_len)
{
  int ret = OB_SUCCESS;
  const int64_t start_us = ObTimeUtility::current_time();
  int64_t snapshot = 0;
  int64_t abs_expired_time = INT64_MAX;
  int64_t pos = 0;
  ObMvccWriteGuard guard;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", K(*this));
    ret = OB_NOT_INIT;
  } else if (NULL == ctx.mem_ctx_) {
    TRANS_LOG(WARN, "invalid param");
    ret = OB_INVALID_ARGUMENT;
  } else {
    share::CompatModeGuard compat_guard(mode_);
    const bool for_replay = true;
    ObMemtableCtx* mt_ctx = static_cast<ObMemtableCtx*>(ctx.mem_ctx_);
    // for non-lob cases, use local variable
    ObMemtableMutatorIterator tmp_mmi;
    ObMemtableMutatorIterator* mmi = NULL;
    const int64_t log_timestamp = mt_ctx->get_redo_log_timestamp();
    // In principle, failure is not allowed here, but from implementation aspect,
    // the logic inside needs to deal with failures due to lack of memory
    if (OB_UNLIKELY(0 >= log_timestamp) || OB_UNLIKELY(INT64_MAX == log_timestamp)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected log timestamp", K(ret), K(*mt_ctx));
    } else if (OB_FAIL(mt_ctx->set_replay_host(this, for_replay))) {
      TRANS_LOG(WARN, "memtable context set host error", K(ret), K(ctx));
    } else if (OB_FAIL(mt_ctx->sub_trans_begin(snapshot, abs_expired_time))) {
      TRANS_LOG(ERROR, "sub_trans_begin fail", K(ret));
    } else {
      if (NULL != mt_ctx->get_memtable_mutator_iter()) {
        mmi = mt_ctx->get_memtable_mutator_iter();
      } else if (data_len < (OB_MAX_LOG_ALLOWED_SIZE / 2)) {
        // the first redo log of lob row, 1.875M in practice
        mmi = &tmp_mmi;
      } else if (NULL == (mmi = mt_ctx->alloc_memtable_mutator_iter())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TRANS_LOG(WARN, "alloc memtable mutator error", K(ret), KP(mmi), KP(data), K(data_len), K(ctx));
      } else {
        // do nothing
      }

      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(mmi->deserialize(data, data_len, pos)) || data_len != pos) {
        TRANS_LOG(WARN, "deserialize fail or pos does not match data_len", K(ret));
        // for big row cases, it must be memory allocation failure, and don't need to handle.
        // since the replay retry would still start from the first redo log. in order to make code
        // more rigorous, still apply undo operations here.
        if (mmi->need_undo_redo_log()) {
          (void)mmi->undo_redo_log(data_len);
        }
        ret = (OB_SUCCESS == ret) ? OB_ERR_UNEXPECTED : ret;
      } else if (ctx.is_tenant_id_valid() && ctx.tenant_id_ != mmi->get_tenant_id() &&
                 OB_FAIL(mmi->try_replace_tenant_id(ctx.tenant_id_))) {
        TRANS_LOG(WARN, "try_replace_tenant_id failed", K(ret), K(ctx));
      } else {
        ObStoreRowkey rowkey;
        ObRowData row;
        while (OB_SUCCESS == ret) {
          uint64_t table_id = OB_INVALID_ID;
          int64_t table_version = 0;
          uint32_t modify_count = 0;
          uint32_t acc_checksum = 0;
          int64_t version = 0;
          int32_t sql_no = 0;
          int32_t flag = 0;
          ObRowDml dml_type = T_DML_UNKNOWN;
          rowkey.reset();
          row.reset();
          if (OB_FAIL(mmi->get_next_row(
                  table_id, rowkey, table_version, row, dml_type, modify_count, acc_checksum, version, sql_no, flag))) {
            if (OB_ITER_END != ret) {
              TRANS_LOG(WARN, "get next row error", K(ret));
            }
          } else if (OB_FAIL(check_standby_cluster_schema_condition_(ctx, table_id, table_version))) {
            TRANS_LOG(WARN, "failed to check standby_cluster_schema_condition", K(ret), K(table_id), K(table_version));
          } else {
            // FIXME.
            if (0 != flag) {
              transaction::ObPartTransCtx* part_ctx =
                  static_cast<transaction::ObPartTransCtx*>(mt_ctx->get_trans_ctx());
              if (OB_FAIL(part_ctx->replay_rollback_to(sql_no, ctx.log_ts_))) {
                TRANS_LOG(WARN, "replay rollback savepoint failed", K(ret), K(*mt_ctx), K(sql_no));
              } else {
                TRANS_LOG(INFO, "replay rollback savepoint success", K(*mt_ctx), K(sql_no));
              }
            } else {
              ret = m_replay_row(ctx,
                  table_id,
                  rowkey,
                  row.data_,
                  row.size_,
                  dml_type,
                  modify_count,
                  acc_checksum,
                  version,
                  sql_no,
                  flag,
                  log_timestamp);
              if (OB_SUCCESS != ret) {
                if (OB_ALLOCATE_MEMORY_FAILED != ret || REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
                  TRANS_LOG(WARN,
                      "m_replay_row fail",
                      K(ret),
                      K(table_id),
                      K(rowkey),
                      K(row),
                      K(dml_type),
                      K(modify_count),
                      K(acc_checksum));
                }
              } else {
                ctx.mem_ctx_->set_table_version(table_version);
                set_max_schema_version(table_version);
              }
            }
          }
        }

        // For big row replay, there may be an error in the first/middle/last redo log.
        // In theory, there will be no middle log replay failure,
        // but the code logic still needs to guarantee
        if (OB_FAIL(ret) && OB_ITER_END != ret) {
          // 1. for big row, substract the pos of big_row_buf by data_len
          // 2. for normal row, the mmw would be overwritten in the retry process,
          //   so don't need to handle
          (void)mmi->undo_redo_log(data_len);
        }
        ret = (OB_ITER_END == ret) ? OB_SUCCESS : ret;
        // free ObRowKey's objs's memory
        THIS_WORKER.get_sql_arena_allocator().reset();
      }
      if (OB_FAIL(ret)) {
        mt_ctx->sub_trans_end(false);
      } else {
        mt_ctx->sub_trans_end(true);
      }
    }
  }
  if (OB_FAIL(ret)) {
    if (OB_ALLOCATE_MEMORY_FAILED != ret || REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      TRANS_LOG(WARN,
          "replay fail",
          "ret",
          ret,
          "pos",
          pos,
          "ctx",
          STR_PTR(ctx.mem_ctx_),
          "data",
          OB_P(data),
          "data_len",
          data_len);
    }
  }
  const int64_t end_us = ObTimeUtility::current_time();
  EVENT_INC(MEMSTORE_MUTATOR_REPLAY_COUNT);
  EVENT_ADD(MEMSTORE_MUTATOR_REPLAY_TIME, end_us - start_us);
  return ret;
}

int ObMemtable::estimate_get_row_count(const common::ObQueryFlag query_flag, const uint64_t table_id,
    const common::ObIArray<common::ObExtStoreRowkey>& rowkeys, storage::ObPartitionEst& part_est)
{
  // the number of lines returned by multi-get is the number of rowkey
  part_est.reset();
  part_est.logical_row_count_ = rowkeys.count();

  UNUSED(query_flag);
  UNUSED(table_id);
  UNUSED(rowkeys);
  UNUSED(part_est);
  return OB_SUCCESS;
}

int ObMemtable::estimate_scan_row_count(const common::ObQueryFlag query_flag, const uint64_t table_id,
    const common::ObExtStoreRange& key_range, storage::ObPartitionEst& part_est)
{
  int ret = OB_SUCCESS;
  const ObStoreRange& range = key_range.get_range();
  ObArenaAllocator allocator(ObModIds::OB_TABLE_SCAN_ITER);
  ObStoreRange real_range;
  ObMvccScanRange mvcc_scan_range;
  ObMemtableKey start_key(table_id, &(real_range.get_start_key()));
  ObMemtableKey end_key(table_id, &(real_range.get_end_key()));

  part_est.reset();
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", K(*this));
    ret = OB_NOT_INIT;
  } else if (0 >= range.get_start_key().get_obj_cnt() || NULL == range.get_start_key().get_obj_ptr() ||
             0 >= range.get_end_key().get_obj_cnt() || NULL == range.get_end_key().get_obj_ptr()) {
    TRANS_LOG(WARN, "invalid range");
    ret = OB_INVALID_ARGUMENT;
  } else {
    m_get_real_range_(real_range, range, query_flag.is_reverse_scan());
    mvcc_scan_range.border_flag_ = real_range.get_border_flag();
    mvcc_scan_range.start_key_ = &start_key;
    mvcc_scan_range.end_key_ = &end_key;
    if (OB_FAIL(mvcc_engine_.estimate_scan_row_count(mvcc_scan_range, part_est))) {
      STORAGE_LOG(WARN, "Fail to estimate cost of scan.", K(ret), K(table_id));
    }
  }

  return ret;
}

int ObMemtable::estimate_multi_scan_row_count(const common::ObQueryFlag query_flag, const uint64_t table_id,
    const common::ObIArray<common::ObExtStoreRange>& ranges, storage::ObPartitionEst& part_est)
{
  int ret = OB_SUCCESS;
  ObPartitionEst scan_est;
  part_est.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < ranges.count(); ++i) {
    if (OB_FAIL(estimate_scan_row_count(query_flag, table_id, ranges.at(i), scan_est))) {
      STORAGE_LOG(WARN, "fail to estimate scan cost", K(ret), K(table_id));
    } else {
      part_est.add(scan_est);
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

int ObMemtable::m_clone_row_data(ObIMemtableCtx& ctx, ObRowData& src_row, ObRowData& dst_row)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  if (OB_ISNULL(buf = (char*)ctx.callback_alloc(src_row.size_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "alloc row_data fail", K(src_row));
  } else {
    MEMCPY(buf, src_row.data_, src_row.size_);
    dst_row.set(buf, (int32_t)src_row.size_);
  }
  return ret;
}

int ObMemtable::m_clone_row_data(ObIMemtableCtx& ctx, ObMemtableCompactWriter& ccw, ObRowData& row)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  if (OB_ISNULL(buf = (char*)ctx.callback_alloc(ccw.size()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "alloc row_data fail", K(ccw.size()));
  } else {
    MEMCPY(buf, ccw.get_buf(), ccw.size());
    row.set(buf, (int32_t)ccw.size());
  }
  return ret;
}

int ObMemtable::record_rowkey_(
    ObMemtableCompactWriter& col_ccw, const ObMemtableKey& mtk, const ObIArray<ObColDesc>& columns)
{
  int ret = OB_SUCCESS;
  const int64_t rowkey_len = const_cast<ObStoreRowkey*>(mtk.get_rowkey())->get_rowkey().length();
  ObObj* rowkey_ptr = NULL;

  if (rowkey_len <= 0 || columns.count() < rowkey_len) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected rowkey len", K(ret), K(rowkey_len), K(columns));
  } else if (NULL == (rowkey_ptr = const_cast<ObStoreRowkey*>(mtk.get_rowkey())->get_obj_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected rowkey obj ptr", K(ret), K(mtk), K(columns));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_len; ++i) {
      const ObColDesc& column = columns.at(i);
      ObObj* obj = rowkey_ptr + i;
      if (obj->get_type() != column.col_type_.get_type() && !obj->is_null() && !obj->is_max_value() &&
          !obj->is_min_value() && !obj->is_nop_value()) {
        TRANS_LOG(ERROR, "object type does not match schema", K(column), K(i), K(obj->get_meta()));
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(col_ccw.append(column.col_id_, *obj))) {
        TRANS_LOG(WARN, "col_ccw append fail", K(ret), K(i), K(column), K(*obj));
      } else {
        // do nothing
      }
    }
  }
  return ret;
}

int ObMemtable::m_column_compact(
    const bool need_explict_record_rowkey, /*whether need to record rowkey information explicitly*/
    const ObMemtableKey& mtk, const ObIArray<ObColDesc>& columns, const ObIArray<int64_t>* update_idx,
    const ObStoreRow& row, ObMemtableCompactWriter& col_ccw, bool& is_lob_row)
{
  int ret = OB_SUCCESS;
  col_ccw.reset();
  if (T_DML_DELETE == row.get_dml()) {
    if (OB_FAIL(col_ccw.row_delete())) {
      TRANS_LOG(WARN, "col_ccw append row_delete fail", "ret", ret);
    }
  } else if (need_explict_record_rowkey && OB_FAIL(record_rowkey_(col_ccw, mtk, columns))) {
    TRANS_LOG(WARN, "record rowkey error", K(ret), K(mtk), K(columns));
  } else {
    int64_t idx_cnt = (NULL == update_idx) ? columns.count() : update_idx->count();
    for (int64_t i = 0; OB_SUCC(ret) && i < idx_cnt; ++i) {
      int64_t idx = (NULL == update_idx) ? i : update_idx->at(i);
      const ObColDesc& column = columns.at(idx);
      if (idx >= columns.count()) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        ObObj* obj = row.row_val_.cells_ + idx;
        if (obj->get_type() != column.col_type_.get_type() && !obj->is_null() && !obj->is_max_value() &&
            !obj->is_min_value() && !obj->is_nop_value()) {
          if (obj->is_lob_locator() && ObLongTextType == column.col_type_.get_type()) {
            ObString lob_string;
            ObLobLocator* lob_locator = NULL;
            if (OB_FAIL(obj->get_lob_locator(lob_locator))) {
              TRANS_LOG(WARN, "get lob locator failed", K(ret));
            } else if (OB_ISNULL(lob_locator)) {
              ret = OB_ERR_UNEXPECTED;
              TRANS_LOG(WARN, "get null lob locator", K(ret));
            } else if (lob_locator->get_payload(lob_string)) {
              TRANS_LOG(WARN, "get lob locator payload failed", K(ret));
            } else {
              obj->set_string(ObLongTextType, lob_string);
              if (OB_FAIL(col_ccw.append(column.col_id_, *obj))) {
                TRANS_LOG(WARN, "col_ccw append fail", K(ret), K(idx), K(column), K(*obj));
              }
            }
          } else {
            TRANS_LOG(ERROR, "object type does not match schema", K(column), K(idx), K(i), K(obj->get_meta()));
            ret = OB_ERR_UNEXPECTED;
          }
        } else if (OB_FAIL(col_ccw.append(column.col_id_, *obj))) {
          TRANS_LOG(WARN, "col_ccw append fail", K(ret), K(idx), K(column), K(*obj));
        } else {
          // do nothing
        }
      }
    }
  }
  if (OB_SUCCESS == ret && OB_FAIL(col_ccw.row_finish())) {
    TRANS_LOG(WARN, "col_ccw append row_finish fail", "ret", ret);
  }
  is_lob_row = ObMemtableCompactWriter::BIG_ROW_BUFFER_SIZE == col_ccw.get_buf_size();
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN,
        "m_column_compact fail",
        "ret",
        ret,
        K(mtk),
        "columns",
        strarray<ObColDesc>(columns),
        "update_idx",
        NULL == update_idx ? "nil" : strarray<int64_t>(*update_idx),
        "row",
        (is_lob_row ? "" : to_cstring(row)),
        "col_ccw_ptr",
        &col_ccw);
  }
  return ret;
}

int ObMemtable::m_column_compact(
    ObMemtableCompactWriter& col_ccw, const ObMemtableKey& mtk, const ObIArray<ObColDesc>& columns)
{
  int ret = OB_SUCCESS;
  col_ccw.reset();

  if (OB_FAIL(record_rowkey_(col_ccw, mtk, columns))) {
    TRANS_LOG(WARN, "record rowkey error", K(ret), K(mtk), K(columns));
  } else if (OB_FAIL(col_ccw.row_finish())) {
    TRANS_LOG(WARN, "col_ccw append row_finish fail", "ret", ret);
  } else {
    // do nothing
  }
  return ret;
}

int ObMemtable::row_relocate(const bool for_replay, const bool need_lock_for_write, const bool need_fill_redo,
    ObIMvccCtx& ctx, const ObMvccRowCallback* callback)
{
  common::ObTimeGuard tg("ObMemtable::row_relocate", 50 * 1000);
  int ret = OB_SUCCESS;
  uint64_t table_id = 0;
  common::ObStoreRowkey row_key;
  ObMemtableKey mtk;
  share::CompatModeGuard compat_guard(mode_);

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", K(*this));
    ret = OB_NOT_INIT;
  } else if (NULL == callback) {
    TRANS_LOG(WARN, "invalid param", K(ctx), KP(callback));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(callback->get_memtable_key(table_id, row_key))) {
    TRANS_LOG(WARN, "get memtable key error", K(ctx));
  } else if (OB_FAIL(mtk.encode(table_id, &row_key))) {
    TRANS_LOG(WARN, "mtk encode fail", "ret", ret);
  } else {
    tg.click();
    ObMemtableKey stored_key;
    ObMvccRow* value = NULL;
    RowHeaderGetter getter;
    bool is_new_add = false;
    int64_t max_trans_version = 0;
    int32_t sql_no = callback->get_sql_no();
    bool new_locked = false;
    bool is_sequential_relocate = true;
    // set_begin(ctx);
    if (OB_FAIL(mvcc_engine_.create_kv(ctx, &mtk, &stored_key, value, getter, is_new_add))) {
      TRANS_LOG(WARN, "row relocate prepare kv before lock fail", K(ret));
    } else if (OB_FAIL(callback->check_sequential_relocate(this, is_sequential_relocate))) {
      TRANS_LOG(WARN, "check sequential relocate error", K(ret), K(*this), K(ctx), K(value));
    } else {
      tg.click();
      if (OB_FAIL(mvcc_engine_.relocate_lock(
              ctx, &stored_key, value, for_replay, need_lock_for_write, is_sequential_relocate, new_locked))) {
        TRANS_LOG(WARN, "row relocate lock fail", K(ret));
      } else {
        tg.click();
        if (OB_FAIL(mvcc_engine_.append_kv(
                ctx, &stored_key, value, for_replay, new_locked, sql_no, is_sequential_relocate))) {
          TRANS_LOG(WARN, "rew relocate append kv error", K(ret), K(ctx), K(stored_key), K(value));
        }
        tg.click();
      }
    }
    if (OB_SUCC(ret)) {
      const ObMvccTransNode* trans_node = callback->get_trans_node();
      if (NULL == trans_node) {
        // do nothing
      } else {
        ObRowData src_old_row = callback->get_old_row();
        ObRowData dst_old_row;
        if (src_old_row.size_ > 0 && !for_replay &&
            OB_FAIL(m_clone_row_data(static_cast<ObIMemtableCtx&>(ctx), src_old_row, dst_old_row))) {
          TRANS_LOG(WARN, "memtable clone row data error", K(ret), K(ctx), K(src_old_row));
        } else if (OB_FAIL(callback->check_sequential_relocate(this, is_sequential_relocate))) {
          TRANS_LOG(WARN, "check sequential relocate error", K(ret), K(*this), K(ctx), K(value));
        } else if (OB_FAIL(mvcc_engine_.relocate_data(ctx,
                       &stored_key,
                       *value,
                       trans_node,
                       for_replay,
                       need_fill_redo,
                       &dst_old_row,
                       for_replay ? trans_node->version_ : timestamp_,
                       callback->is_stmt_committed(),
                       sql_no,
                       callback->get_log_ts(),
                       is_sequential_relocate))) {
          TRANS_LOG(WARN, "relocate data error", K(ret), K(ctx), K(stored_key), K(value), K(trans_node));
        } else {
          // do nothing
        }
        if (OB_FAIL(ret)) {
          if (dst_old_row.size_ > 0 && NULL != dst_old_row.data_) {
            ctx.callback_free((void*)(dst_old_row.data_));
            dst_old_row.reset();
          }
        }
      }
      tg.click();
      if (OB_SUCC(ret)) {
        max_trans_version = callback->get_mvcc_row().get_max_trans_version();
        value->update_max_trans_version(max_trans_version);
        ATOMIC_FAA(&row_relocate_count_, 1);
        if (EXECUTE_COUNT_PER_SEC(16)) {
          if (NULL == trans_node) {
            TRANS_LOG(INFO,
                "row relocate success",
                K(ctx),
                K(max_trans_version),
                K(*value),
                KP(trans_node),
                "total_relocate_count",
                row_relocate_count_);
          } else {
            TRANS_LOG(INFO,
                "row relocate success",
                K(ctx),
                K(max_trans_version),
                K(*value),
                K(*trans_node),
                "total_relocate_count",
                row_relocate_count_);
          }
        }
      }
    }
    if (tg.get_diff() > 10 * 1000 * 1000) {
      TRANS_LOG(ERROR,
          "current row relocate cost too much time",
          K(ctx),
          K(max_trans_version),
          "total_relocate_count",
          row_relocate_count_);
    }
    static int64_t seq_cnt = 0;
    static int64_t non_seq_cnt = 0;
    if (is_sequential_relocate) {
      ATOMIC_INC(&seq_cnt);
    } else {
      ATOMIC_INC(&non_seq_cnt);
    }
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
      TRANS_LOG(INFO, "memtable relocate statistic", K(seq_cnt), K(non_seq_cnt), K(*this));
    }
  }

  return ret;
}

int ObMemtable::m_replay_row(const ObStoreCtx& ctx, const uint64_t table_id, const ObStoreRowkey& rowkey,
    const char* data, const int64_t data_len, const ObRowDml dml_type, const uint32_t modify_count,
    const uint32_t acc_checksum, const int64_t version, const int32_t sql_no, const int32_t flag,
    const int64_t log_timestamp)
{
  UNUSED(flag);
  int ret = OB_SUCCESS;
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = E(EventTable::EN_REPLAY_ROW) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      TRANS_LOG(ERROR, "m replay row error", K(ret), K(ctx), K(table_id));
      return ret;
    }
  }
#endif
  ObMemtableKey mtk;
  ObMemtableData mtd(dml_type, data_len, data);
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", K(*this));
    ret = OB_NOT_INIT;
  } else if (NULL == ctx.mem_ctx_) {
    TRANS_LOG(WARN, "invalid param");
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(mtk.encode(table_id, &rowkey))) {
    TRANS_LOG(WARN, "mtk encode fail", "ret", ret);
  } else if (OB_FAIL(mvcc_engine_.replay(
                 *ctx.mem_ctx_, &mtk, &mtd, modify_count, acc_checksum, version, sql_no, log_timestamp))) {
    TRANS_LOG(WARN, "mvcc engine set fail", "ret", ret);
  } else {
    // do nothing
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN,
        "replay fail",
        "ret",
        ret,
        "table_id",
        key_.table_id_,
        "ctx",
        STR_PTR(ctx.mem_ctx_),
        "table_id",
        table_id,
        "rowkey",
        rowkey,
        "mtk",
        mtk,
        "data",
        OB_P(data),
        "data_len",
        data_len,
        "dml_type",
        dml_type,
        "modify_count",
        modify_count);
  }
  return ret;
}

int ObMemtable::m_prepare_kv(const storage::ObStoreCtx& ctx, const ObMemtableKey* key, ObMemtableKey* stored_key,
    ObMvccRow*& value, RowHeaderGetter& getter, const bool is_replay, const ObIArray<ObColDesc>& columns,
    bool& is_new_add, bool& is_new_locked)
{
  int ret = OB_SUCCESS;
  bool new_locked = false;
  const bool is_sequential_relocate = true;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", K(*this));
    ret = OB_NOT_INIT;
  } else if (NULL == ctx.mem_ctx_) {
    TRANS_LOG(WARN, "invalid param");
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(mvcc_engine_.create_kv(*ctx.mem_ctx_, key, stored_key, value, getter, is_new_add))) {
    TRANS_LOG(WARN, "prepare kv before lock fail", K(ret));
  } else if (OB_FAIL(mvcc_engine_.lock(*ctx.mem_ctx_, stored_key, value, is_replay, new_locked))) {
    // TRANS_LOG(WARN, "prepare kv lock fail", K(ret));
  } else if (OB_FAIL(lock_row_on_frozen_stores(ctx, key, value, columns))) {
    (void)mvcc_engine_.unlock(*ctx.mem_ctx_, key, value, is_replay, new_locked);
  } else if (OB_FAIL(mvcc_engine_.append_kv(
                 *ctx.mem_ctx_, stored_key, value, is_replay, new_locked, ctx.sql_no_, is_sequential_relocate))) {
    TRANS_LOG(WARN, "prepare kv after lock fail", K(ret));
  } else {
    is_new_locked = new_locked;
  }

  return ret;
}

int ObMemtable::lock_row_on_frozen_stores(const ObStoreCtx& ctx, const ObMemtableKey* key, ObMvccRow* value,
    const ObIArray<share::schema::ObColDesc>& columns)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(value) || value->is_lower_lock_scaned()) {
  } else if (NULL == ctx.mem_ctx_ || NULL == key) {
    TRANS_LOG(WARN, "invalid param", KP(ctx.mem_ctx_), KP(key));
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == ctx.tables_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "stores in context is null", K(ret));
  } else {
    int64_t max_trans_version = 0;
    const ObIArray<ObITable*>* stores = ctx.tables_;

    // ignore active memtable
    for (int64_t i = stores->count() - 2; OB_SUCC(ret) && i >= 0; i--) {
      int64_t current_version = 0;
      bool is_locked = false;
      uint32_t lock_descriptor = 0;
      ObStoreRowLockState lock_state;

      if (NULL == stores->at(i)) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "ObIStore is null", K(ret), K(i));
      } else if (stores->at(i)->is_memtable()) {
        ObMemtable* memtable = static_cast<ObMemtable*>(stores->at(i));
        if (OB_FAIL(memtable->get_mvcc_engine().check_row_locked(
                *ctx.mem_ctx_, key, is_locked, lock_descriptor, current_version))) {
          TRANS_LOG(WARN, "mvcc engine check row lock fail", K(ret), K(is_locked), K(lock_descriptor));
        }
      } else if (stores->at(i)->is_sstable()) {
        ObSSTable* sstable = static_cast<ObSSTable*>(stores->at(i));
        if (OB_FAIL(sstable->check_row_locked(ctx, *key->get_rowkey(), columns, lock_state))) {
          TRANS_LOG(WARN, "failed to check row lock by other", K(ret), K(*key), K(lock_state));
        } else {
          current_version = lock_state.trans_version_;
        }
        TRANS_LOG(DEBUG, "check_row_locked meet sstable", K(ret), K(*key), K(*sstable), K(current_version));
      } else {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "unknown store type", K(ret));
      }

      max_trans_version = max(max_trans_version, current_version);
      TRANS_LOG(DEBUG, "check_row_locked", K(i), K(stores->count()), K(stores->at(i)));
    }

    if (OB_SUCC(ret)) {
      value->update_max_trans_version(max_trans_version);
      value->set_lower_lock_scaned();

      if (max_trans_version > ctx.mem_ctx_->get_read_snapshot()) {
        ret = OB_TRANSACTION_SET_VIOLATION;
        TRANS_LOG(WARN, "TRANS_SET_VIOLATION", K(ret), K(max_trans_version), "ctx", ctx.mem_ctx_);
      }
    }
  }

  return ret;
}

int ObMemtable::m_lock(const ObStoreCtx& ctx, const uint64_t table_id,
    const ObIArray<share::schema::ObColDesc>& columns, const ObMemtableKey* key, ObMemtableKey* stored_key,
    ObMvccRow*& value, bool& is_new_locked)
{
  UNUSED(table_id);
  UNUSED(columns);
  int ret = OB_SUCCESS;
  const bool is_replay = false;
  bool is_new_add = false;
  RowHeaderGetter getter;
  ret = m_prepare_kv(ctx, key, stored_key, value, getter, is_replay, columns, is_new_add, is_new_locked);
  if (ret == OB_TRANSACTION_SET_VIOLATION &&
      (ObTransIsolation::SERIALIZABLE == ctx.isolation_ || ObTransIsolation::REPEATABLE_READ == ctx.isolation_)) {
    ret = OB_TRANS_CANNOT_SERIALIZE;
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// no allocator here for it, so we desirialize and save the struct info
int ObMemtable::save_base_storage_info(const ObSavedStorageInfoV2& info)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(storage_info_.deep_copy(info))) {
    STORAGE_LOG(WARN, "deep copy storage info failed", K(info), K(ret));
  }
  storage_info_.get_data_info().set_schema_version(max_schema_version_);

  return ret;
}

int ObMemtable::get_base_storage_info(ObSavedStorageInfoV2& info)
{
  int ret = OB_SUCCESS;
  ObDataStorageInfo& data_info = storage_info_.get_data_info();
  data_info.set_schema_version(max_schema_version_);
  if (OB_FAIL(info.deep_copy(storage_info_))) {
    STORAGE_LOG(WARN, "deep copy storage info failed", K_(storage_info), K(ret));
  }
  return ret;
}

int ObMemtable::set_emergency(const bool emergency)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(*this));
  } else if (ObMemtableState::MINOR_FROZEN != state_) {
    ret = OB_STATE_NOT_MATCH;
    TRANS_LOG(WARN, "memtable not frozen yet", K(ret), K(*this));
  } else {
    emergency_ = emergency;
  }

  return ret;
}

int ObMemtable::minor_freeze(const bool emergency)
{
  int ret = OB_SUCCESS;
  bool is_reentrant = false;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", K(*this));
    ret = OB_NOT_INIT;
  } else if (ObMemtableState::MINOR_FROZEN == state_) {
    is_reentrant = true;
    emergency_ = emergency;
  } else if (ObMemtableState::ACTIVE != state_) {
    TRANS_LOG(WARN, "switch state error", K(*this), K_(state));
    ret = OB_ERR_UNEXPECTED;
  } else {
    state_ = ObMemtableState::MINOR_FROZEN;
    emergency_ = emergency;
    local_allocator_.set_frozen();
  }
  if (OB_SUCC(ret)) {
    TRANS_LOG(INFO, "memtable minor freeze success", K(*this), K(is_reentrant));
  } else {
    TRANS_LOG(WARN, "memtable minor freeze error", K(ret), K(*this), K(is_reentrant));
  }
  return ret;
}

int64_t ObMemtable::get_size() const
{
  return local_allocator_.get_size();
}

int64_t ObMemtable::get_occupied_size() const
{
  return local_allocator_.get_occupied_size();
}

ObStoreRange& ObMemtable::m_get_real_range_(ObStoreRange& real_range, const ObStoreRange& range, bool is_reverse)
{
  real_range = range;
  if (is_reverse) {
    real_range.get_start_key() = range.get_end_key();
    real_range.get_end_key() = range.get_start_key();
    if (range.get_border_flag().inclusive_start()) {
      real_range.get_border_flag().set_inclusive_end();
    } else {
      real_range.get_border_flag().unset_inclusive_end();
    }
    if (range.get_border_flag().inclusive_end()) {
      real_range.get_border_flag().set_inclusive_start();
    } else {
      real_range.get_border_flag().unset_inclusive_start();
    }
  }
  return real_range;
}

int ObMemtable::row_compact(ObMvccRow* row, const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  ObMemtableRowCompactor row_compactor;
  if (OB_ISNULL(row)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "row is NULL");
  } else if (OB_FAIL(row_compactor.init(row, &local_allocator_))) {
    TRANS_LOG(WARN, "row compactor init error", K(ret));
  } else if (OB_FAIL(row_compactor.compact(snapshot_version))) {
    TRANS_LOG(WARN, "row_compact fail", K(ret), K(*row), K(snapshot_version));
  } else {
    // do nothing
  }
  return ret;
}

int64_t ObMemtable::get_hash_item_count() const
{
  return query_engine_.hash_size();
}

int64_t ObMemtable::get_hash_alloc_memory() const
{
  return query_engine_.hash_alloc_memory();
}

int64_t ObMemtable::get_btree_item_count() const
{
  return query_engine_.btree_size();
}

int64_t ObMemtable::get_btree_alloc_memory() const
{
  return query_engine_.btree_alloc_memory();
}

int ObMemtable::inc_active_trx_count()
{
  int ret = OB_SUCCESS;
  const int64_t count = ATOMIC_AAF(&active_trx_count_, 1);

  if (!is_active_memtable()) {
    TRANS_LOG(WARN, "memtable state not match", K(ret), K(*this));
  }
  if (0 >= count) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "active transaction count illegal", K(ret), K(count));
  } else {
    inc_ref();
  }

  return ret;
}

int ObMemtable::dec_active_trx_count()
{
  int ret = OB_SUCCESS;
  const int64_t count = ATOMIC_AAF(&active_trx_count_, -1);
  if (0 > count) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "active transaction count illegal", K(ret), K(count), K(*this));
  } else {
    dec_ref();
  }
  return ret;
}

int ObMemtable::dec_pending_cb_count()
{
  int ret = OB_SUCCESS;

  bool mark_finish = mark_finish_;
  int64_t pending_cb_cnt = ATOMIC_SAF(&pending_cb_cnt_, 1);

  if (mark_finish && 0 == pending_cb_cnt) {
    TRANS_LOG(INFO, "memtable log sync finish", K(frozen_log_applied_), K(*this));
  }

  if (OB_UNLIKELY(mark_finish && pending_cb_cnt < 0)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "pending callback count not match", K(ret), K(pending_cb_cnt), K(*this));
  }

  return ret;
}

int ObMemtable::add_pending_cb_count(const int64_t cnt, const bool finish)
{
  int ret = OB_SUCCESS;

  if (!mark_finish_) {
    int64_t pending_cb_cnt = ATOMIC_AAF(&pending_cb_cnt_, cnt);

    if (finish) {
      mark_finish_ = true;
      if (0 == pending_cb_cnt) {
        TRANS_LOG(INFO, "memtable log sync finish", K(frozen_log_applied_), K(*this));
      }
      if (OB_UNLIKELY(pending_cb_cnt < 0)) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "pending callback count not match", K(ret), K(pending_cb_cnt), K(*this));
      }
    }
  } else if (cnt > 0) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "no more pending callback!", K(ret), K(cnt), K(finish));
  }

  return ret;
}

void ObMemtable::dec_pending_lob_count()
{
  ATOMIC_SAF(&pending_lob_cnt_, 1);
  if (OB_UNLIKELY(pending_lob_cnt_ < 0)) {
    TRANS_LOG(ERROR, "pending_lob_cnt not match", K(pending_lob_cnt_), K(*this));
  }
}

void ObMemtable::inc_pending_lob_count()
{
  ATOMIC_AAF(&pending_lob_cnt_, 1);
}

void ObMemtable::inc_pending_batch_commit_count()
{
  ATOMIC_AAF(&pending_batch_commit_cnt_, 1);
}

void ObMemtable::dec_pending_batch_commit_count()
{
  ATOMIC_SAF(&pending_batch_commit_cnt_, 1);
  if (OB_UNLIKELY(pending_batch_commit_cnt_ < 0)) {
    TRANS_LOG(ERROR, "pending_batch_commit_cnt not match", K(pending_batch_commit_cnt_), K(*this), K(lbt()));
  }
}

void ObMemtable::inc_pending_elr_count()
{
  ATOMIC_AAF(&pending_elr_cnt_, 1);
}

void ObMemtable::dec_pending_elr_count()
{
  ATOMIC_SAF(&pending_elr_cnt_, 1);
  if (OB_UNLIKELY(pending_elr_cnt_ < 0)) {
    TRANS_LOG(ERROR, "pending_elr_cnt not match", K(pending_elr_cnt_), K(*this), K(lbt()));
  }
}

bool ObMemtable::can_be_minor_merged() const
{
  bool can_merge = ObMemtableState::MINOR_FROZEN == state_;

  can_merge = (frozen_ && frozen_log_applied_ && mark_finish_ && 0 == pending_cb_cnt_ && 0 == pending_lob_cnt_ &&
               0 == pending_batch_commit_cnt_ && 0 == pending_elr_cnt_);
  MEM_BARRIER();

  if (is_split_) {
    can_merge = true;
  }

  if (!can_merge && REACH_TIME_INTERVAL(1000 * 1000)) {
    TRANS_LOG(WARN, "memtable cannot be minor merged", K(*this));
  }

  return can_merge;
}

int ObMemtable::get_frozen_schema_version(int64_t& schema_version) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not inited", K(ret));
  } else if (!this->is_frozen_memtable()) {
    ret = OB_STATE_NOT_MATCH;
    TRANS_LOG(WARN, "cannot get schema version from not frozen memtable", K(ret), K(*this));
  } else {
    schema_version = storage_info_.get_data_info().get_schema_version();
  }
  return ret;
}

int ObMemtable::set_snapshot_version(const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not inited", K(ret));
  } else if (ObVersionRange::MAX_VERSION == snapshot_version || snapshot_version <= ObVersionRange::MIN_VERSION) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid args", K(ret), K(snapshot_version));
  } else if (key_.trans_version_range_.snapshot_version_ != ObVersionRange::MAX_VERSION) {
    ret = OB_STATE_NOT_MATCH;
    TRANS_LOG(WARN, "cannot set snapshot version now", K(ret), K(snapshot_version), K(*this));
  } else {
    key_.trans_version_range_.snapshot_version_ = snapshot_version;
  }
  return ret;
}

int ObMemtable::set_base_version(const int64_t base_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not inited", K(ret));
  } else if (ObVersionRange::MAX_VERSION == base_version || base_version <= ObVersionRange::MIN_VERSION) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid args", K(ret), K(base_version));
  } else if (!is_active_memtable() || key_.trans_version_range_.snapshot_version_ != ObVersionRange::MAX_VERSION) {
    ret = OB_STATE_NOT_MATCH;
    TRANS_LOG(ERROR, "cannot set base_version now", K(ret), K(base_version), K(*this));
  } else {
    key_.trans_version_range_.base_version_ = base_version;
    key_.trans_version_range_.multi_version_start_ = base_version;
  }
  return ret;
}

int ObMemtable::set_start_log_ts(const int64_t start_ts)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not inited", K(ret));
  } else if (ObLogTsRange::MAX_TS == start_ts) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid args", K(ret), K(start_ts));
  } else if (!is_active_memtable() || ObLogTsRange::MAX_TS != key_.log_ts_range_.end_log_ts_ ||
             0 != max_schema_version_) {
    ret = OB_STATE_NOT_MATCH;
    TRANS_LOG(ERROR, "cannot set start ts now", K(ret), K(start_ts), K(*this));
  } else {
    key_.log_ts_range_.start_log_ts_ = start_ts;
  }
  return ret;
}

int ObMemtable::set_end_log_ts(const int64_t freeze_ts)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not inited", K(ret));
  } else if (ObLogTsRange::MAX_TS == freeze_ts) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid args", K(ret), K(freeze_ts));
  } else if (ObLogTsRange::MAX_TS != key_.log_ts_range_.end_log_ts_) {
    ret = OB_STATE_NOT_MATCH;
    TRANS_LOG(ERROR, "cannot set end ts now", K(ret), K(freeze_ts), K(*this));
  } else if (freeze_ts < key_.log_ts_range_.start_log_ts_) {
    ret = OB_STATE_NOT_MATCH;
    TRANS_LOG(ERROR, "cannot set freeze log ts smaller to start log ts", K(ret), K(freeze_ts), K(*this));
  } else {
    key_.log_ts_range_.end_log_ts_ = freeze_ts;
    freeze_log_ts_ = freeze_ts;
  }
  return ret;
}

int ObMemtable::update_max_log_ts(const int64_t log_ts)
{
  int ret = OB_SUCCESS;

  if (ObLogTsRange::MAX_TS != key_.log_ts_range_.end_log_ts_ && log_ts < key_.log_ts_range_.end_log_ts_) {
    ret = OB_STATE_NOT_MATCH;
    TRANS_LOG(ERROR, "cannot set max log ts smaller to end log_ts", K(ret), K(log_ts), K(*this));
  } else {
    inc_update(&key_.log_ts_range_.max_log_ts_, log_ts);
  }

  return ret;
}

bool ObMemtable::is_active_memtable() const
{
  return ObMemtableState::ACTIVE == state_;
}
bool ObMemtable::is_frozen_memtable() const
{
  return ObMemtableState::MAJOR_FROZEN == state_ || ObMemtableState::MAJOR_MERGING == state_ ||
         ObMemtableState::MINOR_FROZEN == state_ || ObMemtableState::MINOR_MERGING == state_;
}

int ObMemtable::get_sdr(const ObMemtableKey* key, ObMemtableKey* start, ObMemtableKey* end, int64_t& max_version)
{
  return query_engine_.get_sdr(key, start, end, max_version);
}

int ObMemtable::estimate_phy_size(const uint64_t table_id, const ObStoreRowkey* start_key, const ObStoreRowkey* end_key,
    int64_t& total_bytes, int64_t& total_rows)
{
  int ret = OB_SUCCESS;
  int64_t level = 0;
  int64_t branch_count = 0;
  total_bytes = 0;
  total_rows = 0;
  ObMemtableKey start_mtk;
  ObMemtableKey end_mtk;
  if (NULL == start_key) {
    start_key = &ObStoreRowkey::MIN_STORE_ROWKEY;
  }
  if (NULL == end_key) {
    end_key = &ObStoreRowkey::MAX_STORE_ROWKEY;
  }
  if (OB_FAIL(start_mtk.encode(table_id, start_key)) || OB_FAIL(end_mtk.encode(table_id, end_key))) {
    TRANS_LOG(WARN, "encode key fail", K(table_id), K(ret));
  } else if (OB_FAIL(query_engine_.estimate_size(&start_mtk, &end_mtk, level, branch_count, total_bytes, total_rows))) {
    TRANS_LOG(WARN, "estimate row count fail", K(ret), K(table_id));
  }
  return ret;
}

int ObMemtable::get_split_ranges(const uint64_t table_id, const ObStoreRowkey* start_key, const ObStoreRowkey* end_key,
    const int64_t part_cnt, ObIArray<ObStoreRange>& range_array)
{
  int ret = OB_SUCCESS;
  ObMemtableKey start_mtk;
  ObMemtableKey end_mtk;
  if (NULL == start_key) {
    start_key = &ObStoreRowkey::MIN_STORE_ROWKEY;
  }
  if (NULL == end_key) {
    end_key = &ObStoreRowkey::MAX_STORE_ROWKEY;
  }
  if (part_cnt < 1) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "part cnt need be greater than 1", K(table_id), K(part_cnt), K(ret));
  } else if (OB_FAIL(start_mtk.encode(table_id, start_key)) || OB_FAIL(end_mtk.encode(table_id, end_key))) {
    TRANS_LOG(WARN, "encode key fail", K(table_id), K(ret));
  } else if (OB_FAIL(query_engine_.split_range(&start_mtk, &end_mtk, part_cnt, range_array))) {
    TRANS_LOG(WARN, "estimate row count fail", K(ret), K(table_id));
  }
  return ret;
}

int ObMemtable::print_stat() const
{
  int ret = OB_SUCCESS;
  TRANS_LOG(INFO, "[memtable stat]", K_(key));
  const char* fname_prefix = "/tmp/stat";  // Stored in /tmp/stat.<table id>.<tstamp>
  char fname[OB_MAX_FILE_NAME_LENGTH];
  FILE* fd = NULL;
  if ((int64_t)sizeof(fname) <=
      snprintf(fname, sizeof(fname), "%s.%lu.%ld", fname_prefix, key_.table_id_, ObTimeUtility::current_time())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(ERROR, "fname too long", K(fname_prefix));
  } else if (NULL == (fd = fopen(fname, "w"))) {
    ret = OB_IO_ERROR;
    TRANS_LOG(ERROR, "open file fail for memtable stat", K(fname));
  } else {
    fprintf(fd, "[memtable stat] table_id:%lu\n", key_.table_id_);
    query_engine_.dump_keyhash(fd);
    fprintf(fd, "[end]\n");
  }
  if (NULL != fd) {
    fclose(fd);
    fd = NULL;
  }
  return ret;
}

int ObMemtable::dump2text(const char* fname)
{
  int ret = OB_SUCCESS;
  char real_fname[OB_MAX_FILE_NAME_LENGTH];
  FILE* fd = NULL;

  TRANS_LOG(INFO, "dump2text", K_(key));
  if (OB_ISNULL(fname)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "fanme is NULL");
  } else if (snprintf(
                 real_fname, sizeof(real_fname), "%s.%ld", fname, ::oceanbase::common::ObTimeUtility::current_time()) >=
             (int64_t)sizeof(real_fname)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "fname too long", K(fname));
  } else if (NULL == (fd = fopen(real_fname, "w"))) {
    ret = OB_IO_ERROR;
    TRANS_LOG(WARN, "open file fail:", K(fname));
  } else {
    fprintf(fd, "memtable: pkey=%s version=%s\n", S(key_.pkey_), to_cstring(get_version()));
    fprintf(fd, "hash_item_count=%ld, hash_alloc_size=%ld\n", get_hash_item_count(), get_hash_alloc_memory());
    fprintf(fd, "btree_item_count=%ld, btree_alloc_size=%ld\n", get_btree_item_count(), get_btree_alloc_memory());
    query_engine_.dump2text(fd);
  }
  if (NULL != fd) {
    fprintf(fd, "end of memtable\n");
    fclose(fd);
    fd = NULL;
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "dump_memtable fail", K(fname), K(ret));
  }
  return ret;
}

void ObMemtable::set_max_schema_version(const int64_t schema_version)
{
  if (INT64_MAX == schema_version) {
    TRANS_LOG(ERROR, "invalid schema version", K(schema_version), K(*this));
  } else {
    inc_update(&max_schema_version_, schema_version);
  }
}

int64_t ObMemtable::get_max_schema_version() const
{
  return ATOMIC_LOAD(&max_schema_version_);
}

int ObMemtable::update_max_trans_version(const int64_t trans_version)
{
  int ret = OB_SUCCESS;
  if (0 >= trans_version) {
    TRANS_LOG(WARN, "invalid argument", K(trans_version));
  } else {
    if (get_base_version() >= trans_version) {
      TRANS_LOG(INFO, "snapshot_version is small, maybe data relocating scenario", K(trans_version), K(*this));
    }
    while (true) {
      const int64_t tmp_version = ATOMIC_LOAD(&max_trans_version_);
      if (tmp_version >= trans_version || ATOMIC_BCAS(&max_trans_version_, tmp_version, trans_version)) {
        break;
      }
    }
  }
  return ret;
}

int64_t ObMemtable::get_max_trans_version() const
{
  return ATOMIC_LOAD(&max_trans_version_);
}

int ObMemtable::inc_write_ref()
{
  int ret = OB_SUCCESS;

  if (write_barrier_) {
    ret = OB_STATE_NOT_MATCH;
  } else {
    ATOMIC_INC(&write_ref_cnt_);
    if (write_barrier_) {
      ret = OB_STATE_NOT_MATCH;
      dec_write_ref();
    }
  }

  return ret;
}

int ObMemtable::get_merge_priority_info(ObMergePriorityInfo& merge_priority_info) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not inited", K(ret));
  } else {
    merge_priority_info.tenant_id_ = local_allocator_.mt_.get_key().get_tenant_id();
    merge_priority_info.last_freeze_timestamp_ = local_allocator_.get_last_freeze_timestamp();
    merge_priority_info.handle_id_ = local_allocator_.get_group_id();
    merge_priority_info.emergency_ = emergency_;
    merge_priority_info.protection_clock_ = local_allocator_.get_protection_clock();
  }
  return ret;
}

void ObMemtable::set_minor_merged()
{
  minor_merged_time_ = ObTimeUtility::current_time();
}

int ObMemtable::check_standby_cluster_schema_condition_(
    const ObStoreCtx& ctx, const int64_t table_id, const int64_t table_version)
{
  int ret = OB_SUCCESS;
#ifdef ERRSIM
  ret = E(EventTable::EN_CHECK_STANDBY_CLUSTER_SCHEMA_CONDITION) OB_SUCCESS;
  if (OB_FAIL(ret) && !common::is_inner_table(table_id)) {
    TRANS_LOG(WARN, "ERRSIM, replay row failed", K(ret));
    return ret;
  }
#endif
  if (GCTX.is_standby_cluster()) {
    // only stand_by cluster need to be check
    uint64_t tenant_id = extract_tenant_id(table_id);
    if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "invalid tenant_id", K(ret), K(tenant_id), K(table_id), K(table_version));
    } else if (OB_SYS_TENANT_ID == tenant_id) {
      // sys tenant do not need check
    } else {
      // user tables of normal tenants(not sys tenant) need to be checked by schema version of
      // itself;
      // sys tables of normal tenants(not sys tenant) need to be checked by schema version of sys tenent;
      uint64_t referred_tenant_id = common::is_inner_table(table_id) ? OB_SYS_TENANT_ID : tenant_id;
      int64_t tenant_schema_version = 0;
      if (OB_FAIL(GSCHEMASERVICE.get_tenant_refreshed_schema_version(referred_tenant_id, tenant_schema_version))) {
        TRANS_LOG(WARN,
            "get_tenant_schema_version failed",
            K(ret),
            K(referred_tenant_id),
            K(table_id),
            K(tenant_id),
            K(table_version));
        if (OB_ENTRY_NOT_EXIST == ret) {
          // tenant schema hasn't been flushed in the case of restart, rewrite OB_ENTRY_NOT_EXIST
          ret = OB_TRANS_WAIT_SCHEMA_REFRESH;
        }
      } else if (table_version > tenant_schema_version) {
        // replay is not allowed when data's table version is greater than tenant's schema version
        ret = OB_TRANS_WAIT_SCHEMA_REFRESH;
        ctx.mem_ctx_->set_table_version(table_version);  // return to be used by replay engine
        TRANS_LOG(WARN,
            "local table schema version is too small, cannot replay",
            K(ret),
            K(tenant_id),
            K(referred_tenant_id),
            K(table_version),
            K(tenant_schema_version));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObMemtable::prepare_freeze_log_ts()
{
  int ret = OB_SUCCESS;

  if (!ATOMIC_BCAS(&freeze_log_ts_, INT64_MAX, 0)) {
    ret = OB_ERR_UNEXPECTED;
  }

  return ret;
}

void ObMemtable::clear_freeze_log_ts()
{
  UNUSED(ATOMIC_BCAS(&freeze_log_ts_, 0, INT64_MAX));
}

int64_t ObMemtable::get_freeze_log_ts() const
{
  int64_t freeze_log_ts = ATOMIC_LOAD(&freeze_log_ts_);

  while (0 == freeze_log_ts) {
    PAUSE();
    freeze_log_ts = ATOMIC_LOAD(&freeze_log_ts_);
  }

  return freeze_log_ts;
}

void ObMemtable::log_applied(const int64_t applied_log_ts)
{
  if (!frozen_log_applied_ && applied_log_ts >= get_freeze_log_ts()) {
    frozen_log_applied_ = true;
    TRANS_LOG(INFO, "memtable frozen log applied", K(applied_log_ts), K(get_freeze_log_ts()), K(*this));
  }
}

void ObMemtable::set_with_accurate_log_ts_range(const bool with_accurate_log_ts_range)
{
  with_accurate_log_ts_range_ = with_accurate_log_ts_range;
}

int64_t ObMemtable::get_upper_trans_version() const
{
  return (active_trx_count_ > 0 ? INT64_MAX : std::max(max_trans_version_, get_snapshot_version()));
}

int ObMemtable::get_active_table_ids(common::ObIArray<uint64_t>& table_ids)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", K(*this));
    ret = OB_NOT_INIT;
  } else {
    ret = query_engine_.get_active_table_ids(table_ids);
  }
  return ret;
}

ObMemtableStat::ObMemtableStat() : lock_(), memtables_()
{}

ObMemtableStat::~ObMemtableStat()
{}

ObMemtableStat& ObMemtableStat::get_instance()
{
  static ObMemtableStat s_instance;
  return s_instance;
}

int ObMemtableStat::register_memtable(ObMemtable* memtable)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (OB_ISNULL(memtable)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(memtable));
  } else if (OB_FAIL(memtables_.push_back(memtable))) {
    TRANS_LOG(ERROR, "err push memtable ptr", K(ret));
  }
  return ret;
}

int ObMemtableStat::unregister_memtable(ObMemtable* memtable)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  bool done = false;
  if (OB_ISNULL(memtable)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(memtable));
  } else {
    for (int64_t idx = 0, cnt = memtables_.size(); (OB_SUCC(ret)) && !done && (idx < cnt); ++idx) {
      if (memtable == memtables_.at(idx)) {
        memtables_.at(idx) = memtables_.at(cnt - 1);
        if (OB_FAIL(memtables_.remove(cnt - 1))) {
          TRANS_LOG(WARN, "memtable remove fail", K(ret), K(idx));
        } else {
          done = true;
        }
      }
    }
    if (!done && OB_SUCCESS == ret) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

int ObMemtableStat::print_stat()
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  TRANS_LOG(INFO, "[memtable stat]", "memtable_cnt", memtables_.size());
  for (int64_t idx = 0, cnt = memtables_.size(); OB_SUCC(ret) && (idx < cnt); ++idx) {
    if (OB_FAIL(memtables_.at(idx)->print_stat())) {
      TRANS_LOG(ERROR, "print memtable stat fail", K(ret));
    }
  }
  TRANS_LOG(INFO, "[memtable stat] end.");
  return ret;
}

int RowHeaderGetter::get()
{
  int ret = OB_ENTRY_NOT_EXIST;
  // const ObIArray<ObITable *> *stores = ctx_.tables_;
  // FIXME.
  //  if (NULL != stores) {
  // ignore active memtable
  /*
  for (int64_t i = stores->count() - 2; OB_ENTRY_NOT_EXIST == ret && i >= 0; i--) {
    ObITable *store = stores->at(i);
    if (OB_FAIL(store->get_row_header(ctx_, table_id_, rowkey_, columns_,
        modify_count_, acc_checksum_))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        TRANS_LOG(WARN, "get row heade error", K(ret), K(i), KP(stores->at(i)));
      }
    }
  }
  */
  //  }
  if (OB_ENTRY_NOT_EXIST == ret) {
    modify_count_ = 0;
    acc_checksum_ = 0;
    // rewrite ret
    ret = OB_SUCCESS;
  }
  return ret;
}

}  // namespace memtable
}  // namespace oceanbase
