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

#define USING_LOG_PREFIX STORAGE

#include "common/rowkey/ob_store_rowkey.h"

#include "storage/memtable/ob_memtable.h"

#include "lib/stat/ob_diagnose_info.h"
#include "lib/time/ob_time_utility.h"
#include "lib/worker.h"
#include "share/rc/ob_context.h"
#include "share/stat/ob_opt_stat_monitor_manager.h"

#include "storage/memtable/mvcc/ob_mvcc_engine.h"
#include "storage/memtable/mvcc/ob_mvcc_iterator.h"
#include "storage/memtable/mvcc/ob_mvcc_row.h"

#include "storage/memtable/ob_memtable_compact_writer.h"
#include "storage/memtable/ob_memtable_iterator.h"
#include "storage/memtable/ob_memtable_mutator.h"
#include "storage/memtable/ob_memtable_util.h"
#include "storage/memtable/ob_memtable_context.h"
#include "storage/memtable/ob_lock_wait_mgr.h"
#include "storage/memtable/ob_row_conflict_handler.h"
#include "storage/memtable/ob_concurrent_control.h"
#include "storage/memtable/ob_row_compactor.h"
#include "storage/compaction/ob_tablet_merge_task.h"
#include "storage/compaction/ob_schedule_dag_func.h"
#include "storage/compaction/ob_compaction_diagnose.h"
#include "storage/access/ob_rows_info.h"
#include "storage/access/ob_sstable_row_lock_checker.h"

#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx_storage/ob_tenant_freezer.h"
#include "storage/tablet/ob_tablet_memtable_mgr.h"
#include "storage/tx_storage/ob_tenant_freezer.h"
#include "storage/column_store/ob_column_oriented_sstable.h"
#include "storage/access/ob_row_sample_iterator.h"
#include "storage/concurrency_control/ob_trans_stat_row.h"
#include "storage/ddl/ob_tablet_ddl_kv.h"

#include "logservice/ob_log_service.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace compaction;
using namespace share::schema;

using namespace storage;
using namespace transaction;
namespace memtable
{

class ObGlobalMtAlloc
{
public:
  ObGlobalMtAlloc()
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(allocator_.init(OB_MALLOC_NORMAL_BLOCK_SIZE,
                                ObNewModIds::OB_MEMSTORE))) {
      TRANS_LOG(ERROR, "global mt alloc init fail", K(ret));
    }
  }
  ~ObGlobalMtAlloc() {}
  void *alloc(const int64_t size)
  {
    return allocator_.alloc(size);
  }
  void free(void *ptr)
  {
    allocator_.free(ptr);
    ptr = NULL;
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObGlobalMtAlloc);
  ObLfFIFOAllocator allocator_;
};

ObGlobalMtAlloc &get_global_mt_alloc()
{
  static ObGlobalMtAlloc s_alloc;
  return s_alloc;
}

class ObDirectLoadMemtableRowsLockedChecker
{
public:
  ObDirectLoadMemtableRowsLockedChecker(ObMemtable &memtable,
                                        const bool check_exist,
                                        const storage::ObTableIterParam &param,
                                        storage::ObTableAccessContext &context,
                                        ObRowsInfo &rows_info)
    : memtable_(memtable),
      check_exist_(check_exist),
      param_(param),
      context_(context),
      rows_info_(rows_info)
  {
  }
  int operator()(ObDDLMemtable *ddl_memtable)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(ddl_memtable)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected ddl memtable is null", K(ret));
    } else if (OB_FAIL(memtable_.check_rows_locked_on_ddl_merge_sstable(
                  ddl_memtable, check_exist_, param_, context_, rows_info_))) {
      TRANS_LOG(WARN, "Failed to check rows locked for sstable", K(ret), KPC(ddl_memtable));
    }
    return ret;
  }
private:
  ObMemtable &memtable_;
  const bool check_exist_;
  const storage::ObTableIterParam &param_;
  storage::ObTableAccessContext &context_;
  ObRowsInfo &rows_info_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////
// Public Functions

ObMemtable::ObMemtable()
  :   is_inited_(false),
      ls_handle_(),
      local_allocator_(*this),
      query_engine_(local_allocator_),
      mvcc_engine_(),
      reported_dml_stat_(),
      max_data_schema_version_(0),
      transfer_freeze_flag_(false),
      recommend_snapshot_version_(share::SCN::invalid_scn()),
      state_(ObMemtableState::INVALID),
      mode_(lib::Worker::CompatMode::INVALID),
      minor_merged_time_(0),
      contain_hotspot_row_(false),
      encrypt_meta_(nullptr),
      encrypt_meta_lock_(ObLatchIds::DEFAULT_SPIN_RWLOCK),
      max_column_cnt_(0) {}

ObMemtable::~ObMemtable()
{
  reset();
}

int ObMemtable::init(const ObITable::TableKey &table_key,
                     ObLSHandle &ls_handle,
                     storage::ObFreezer *freezer,
                     storage::ObTabletMemtableMgr *memtable_mgr,
                     const int64_t schema_version,
                     const uint32_t freeze_clock)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    TRANS_LOG(WARN, "init twice", K(*this));
    ret = OB_INIT_TWICE;
  } else if (!table_key.is_valid() ||
             OB_ISNULL(freezer) ||
             OB_ISNULL(memtable_mgr) ||
             schema_version < 0 ||
             OB_UNLIKELY(!ls_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid param", K(ret), K(table_key), KP(freezer), KP(memtable_mgr),
              K(schema_version), K(freeze_clock), K(ls_handle));
  } else if (OB_FAIL(set_memtable_mgr_(memtable_mgr))) {
    TRANS_LOG(WARN, "fail to set memtable mgr", K(ret), KP(memtable_mgr));
  } else if (FALSE_IT(set_freeze_clock(freeze_clock))) {
  } else if (FALSE_IT(set_max_schema_version(schema_version))) {
  } else if (OB_FAIL(set_freezer(freezer))) {
    TRANS_LOG(WARN, "fail to set freezer", K(ret), KP(freezer));
  } else if (OB_FAIL(local_allocator_.init())) {
    TRANS_LOG(WARN, "fail to init memstore allocator", K(ret), "tenant id", MTL_ID());
  } else if (OB_FAIL(query_engine_.init())) {
    TRANS_LOG(WARN, "query_engine.init fail", K(ret), "tenant_id", MTL_ID());
  } else if (OB_FAIL(mvcc_engine_.init(&local_allocator_,
                                       &kv_builder_,
                                       &query_engine_,
                                       this))) {
    TRANS_LOG(WARN, "query engine init fail", "ret", ret);
  } else if (OB_FAIL(ObITable::init(table_key))) {
    TRANS_LOG(WARN, "failed to set_table_key", K(ret), K(table_key));
  } else {
    ls_handle_ = ls_handle;
    ls_id_ = ls_handle_.get_ls()->get_ls_id();
    if (table_key.get_tablet_id().is_sys_tablet()) {
      mode_ = lib::Worker::CompatMode::MYSQL;
    } else {
      mode_ = MTL(lib::Worker::CompatMode);
    }
    state_ = ObMemtableState::ACTIVE;
    init_timestamp_ = ObTimeUtility::current_time();
    contain_hotspot_row_ = false;
    (void)set_freeze_state(TabletMemtableFreezeState::ACTIVE);
    is_inited_ = true;
    TRANS_LOG(DEBUG, "memtable init success", K(*this));
  }

  //avoid calling destroy() when ret is OB_INIT_TWICE
  if (OB_SUCCESS != ret && IS_NOT_INIT) {
    destroy();
  }

  return ret;
}

void ObMemtable::pre_batch_destroy_keybtree()
{
  (void)query_engine_.pre_batch_destroy_keybtree();
}

int ObMemtable::batch_remove_unused_callback_for_uncommited_txn(
  const ObLSID ls_id, const memtable::ObMemtableSet *memtable_set)
{
  int ret = OB_SUCCESS;
  // NB: Do not use cache here, because the trans_service may be destroyed under
  // MTL_DESTROY() and the cache is pointing to a broken memory.
  transaction::ObTransService *txs_svr =
    MTL_CTX()->get<transaction::ObTransService *>();

  if (NULL != txs_svr
      && OB_FAIL(txs_svr->remove_callback_for_uncommited_txn(ls_id, memtable_set))) {
    TRANS_LOG(WARN, "remove callback for uncommited txn failed", K(ret), KPC(memtable_set));
  }

  return ret;
}

void ObMemtable::destroy()
{
  ObTimeGuard time_guard("ObMemtable::destroy()", 100 * 1000);
  int ret = OB_SUCCESS;
  if (is_inited_) {
    const int64_t cost_time = ObTimeUtility::current_time() - mt_stat_.release_time_;
    if (cost_time > 1 * 1000 * 1000) {
      STORAGE_LOG(WARN, "it costs too much time from release to destroy", K(cost_time), KP(this));
    }
    set_allow_freeze(true);
    STORAGE_LOG(INFO, "memtable destroyed", K(*this));
    time_guard.click();
  }
  is_inited_ = false;
  mvcc_engine_.destroy();
  time_guard.click();
  query_engine_.destroy();
  time_guard.click();
  local_allocator_.destroy();
  time_guard.click();
  ls_handle_.reset();
  max_data_schema_version_ = 0;
  max_column_cnt_ = 0;
  state_ = ObMemtableState::INVALID;
  reported_dml_stat_.reset();
  transfer_freeze_flag_ = false;
  recommend_snapshot_version_.reset();
  contain_hotspot_row_ = false;
  encrypt_meta_ = nullptr;
  ObITabletMemtable::reset();
}

int ObMemtable::safe_to_destroy(bool &is_safe)
{
  int ret = OB_SUCCESS;
  int64_t ref_cnt = get_ref();
  int64_t write_ref_cnt = get_write_ref();
  int64_t unsubmitted_cnt = get_unsubmitted_cnt();

  is_safe = (0 == ref_cnt && 0 == write_ref_cnt);
  if (is_safe) {
    is_safe = (0 == unsubmitted_cnt);
  }

  if (is_safe) {
    // In scenarios where the memtable is forcefully remove (such as when the
    // table is dropped or the ls goes offline), relying solely on the
    // previously mentioned conditions(write_ref and unsubmitted_cnt) cannot
    // guarantee that all the data on the memtable has been synced. This can
    // lead to the memtable being destroyed prematurely, which in turn can cause
    // later txns to encounter the coredump when trying to access data from the
    // destroyed memtable. Therefore, we need to ensure that all data has indeed
    // been synced before the memtable is safe to destroy. The solutions to
    // the problem can be unified into the following two scenarios:
    // 1. If the ls hasnot gone offline:
    //   In this case, we can rely on max decided scn to ensure that all data on
    //   the memtable has been synced.
    // 2. If the ls has gone offline:
    //   In this case, the ls cannot provide a decided scn. Therefore, we rely
    //   on the apply status of apply service to decide whether all data have
    //   been synced.
    share::SCN max_decided_scn = share::ObScnRange::MIN_SCN;
    if (!is_inited_) {
      is_safe = true;
      TRANS_LOG(INFO, "memtable is not inited and safe to destroy", KPC(this));
    } else if (OB_FAIL(ls_handle_.get_ls()->get_max_decided_scn(max_decided_scn))) {
      TRANS_LOG(WARN, "fail to get max decided scn", K(ret), K(max_decided_scn));
      is_safe = false;
    } else {
      is_safe = max_decided_scn >= get_max_end_scn();
    }

    // STATE_NOT_MATCH means ls is offlined and we need replies on the apply
    // service to guarantee all logs have been applied
    if (!is_safe && ret == OB_STATE_NOT_MATCH) {
      ret = OB_SUCCESS;
      bool is_done = false;
      LSN end_lsn;
      if (OB_FAIL(MTL(logservice::ObLogService*)->get_log_apply_service()->
                  is_apply_done(ls_handle_.get_ls()->get_ls_id(),
                                is_done,
                                end_lsn))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          is_safe = true;
          TRANS_LOG(INFO, "apply is decided after ls removed when safe to destroy",
                    K(ret), K(end_lsn), K(is_done));
        } else {
          TRANS_LOG(WARN, "fail to is_apply_done", K(ret), K(max_decided_scn));
        }
      } else {
        TRANS_LOG(INFO, "apply is decided after ls offlined when safe to destroy",
                  K(ret), K(end_lsn), K(is_done));
        if (is_done) {
          is_safe = true;
        } else {
          is_safe = false;
        }
      }
    }
  }

  return ret;
}
////////////////////////////////////////////////////////////////////////////////////////////////////
// Public Functions: set/lock


void ObMemtable::set_begin(ObMvccAccessCtx &ctx)
{
  ctx.handle_start_time_ = ObTimeUtility::current_time();
  EVENT_INC(MEMSTORE_APPLY_COUNT);
}

void ObMemtable::set_end(ObMvccAccessCtx &ctx, int ret)
{

  if (OB_SUCC(ret)) {
    EVENT_INC(MEMSTORE_APPLY_SUCC_COUNT);
  } else {
    EVENT_INC(MEMSTORE_APPLY_FAIL_COUNT);
  }
  EVENT_ADD(MEMSTORE_APPLY_TIME, ObTimeUtility::current_time() - ctx.handle_start_time_);
}

int ObMemtable::multi_set(
    const storage::ObTableIterParam &param,
	  storage::ObTableAccessContext &context,
    const common::ObIArray<share::schema::ObColDesc> &columns,
    const storage::ObStoreRow *rows,
    const int64_t row_count,
    const bool check_exist,
    const share::ObEncryptMeta *encrypt_meta,
    storage::ObRowsInfo &rows_info)
{
  int ret = OB_SUCCESS;
  ObMvccWriteGuard guard(ret);
  ObMemtableKeyGenerator mtk_generator;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "Not inited", K(*this));
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!param.is_valid() || !context.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "Invalid argument", K(ret), K(param), K(context));
  } else if (OB_UNLIKELY(nullptr == context.store_ctx_->mvcc_acc_ctx_.get_mem_ctx()
                         || param.get_schema_rowkey_count() > columns.count())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "Invalid param", K(ret), K(param), K(columns.count()));
#ifdef OB_BUILD_TDE_SECURITY
  //TODO: table_id is just used as encrypt_index, we may rename it in the future.
  //      If the function(set) no longer passes in this parameter(table_id),
  //      we need to construct ObTxEncryptMeta in advance, and pass tx_encrypt_meta(ObTxEncryptMeta*)
  //      instead of encrypt_meta(ObEncryptMeta*) into this function(set)
  } else if (need_for_save(encrypt_meta) && OB_FAIL(save_encrypt_meta(param.table_id_, encrypt_meta))) {
    TRANS_LOG(WARN, "store encrypt meta to memtable failed", KPC(encrypt_meta), KR(ret));
#endif
  } else if (OB_FAIL(mtk_generator.init(rows, row_count, param.get_schema_rowkey_count(), columns))) {
    TRANS_LOG(WARN, "fail to generate memtable keys", KPC(encrypt_meta), K(*context.store_ctx_), KR(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(guard.write_auth(*context.store_ctx_))) {
    TRANS_LOG(WARN, "not allow to write", K(*context.store_ctx_));
  } else {
    lib::CompatModeGuard compat_guard(mode_);
    if (row_count > 1) {
      ret = multi_set_(param, columns, rows, row_count, check_exist, mtk_generator, context, rows_info);
    } else {
      ret = set_(param,
                 columns,
                 rows[0],
                 nullptr, /*old_row*/
                 nullptr, /*update_idx*/
                 mtk_generator[0],
                 check_exist,
                 context,
                 nullptr /*mvcc_row*/);
    }
    guard.set_memtable(this);
  }

  if (OB_SUCC(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(try_report_dml_stat_(param.table_id_))) {
      TRANS_LOG_RET(WARN, tmp_ret, "fail to report dml stat", K_(reported_dml_stat));
    }
    /*****[for deadlock]*****/
    // recored this row is hold by this trans for deadlock detector
    if (param.is_non_unique_local_index_) {
      // no need to detect deadlock for non-unique local index table
    } else {
      for (int64_t idx = 0; idx < mtk_generator.count(); ++idx) {
        MTL(ObLockWaitMgr*)->set_hash_holder(key_.get_tablet_id(),
                                             mtk_generator[idx],
                                             context.store_ctx_->mvcc_acc_ctx_.get_mem_ctx()->get_tx_id());
      }
    }
    /***********************/
  }
  return ret;
}

int ObMemtable::check_rows_locked(
    const bool check_exist,
    const storage::ObTableIterParam &param,
    storage::ObTableAccessContext &context,
    ObRowsInfo &rows_info)
{
  int ret = OB_SUCCESS;
  ObStoreCtx &ctx = *(context.store_ctx_);
  ObTransID my_tx_id = ctx.mvcc_acc_ctx_.get_tx_id();
  SCN snapshot_version = ctx.mvcc_acc_ctx_.get_snapshot_version();
  ObMemtableKey mtk;
  for (int64_t i = 0; OB_SUCC(ret) && i < rows_info.rowkeys_.count(); i++) {
    const blocksstable::ObDatumRowkey &rowkey = rows_info.get_rowkey(i);
    ObStoreRowLockState &lock_state = rows_info.get_row_lock_state(i);
    ObRowState row_state;
    if (rows_info.is_row_checked(i)) {
    } else if (OB_FAIL(mtk.encode(param.get_read_info()->get_columns_desc(), &rowkey.get_store_rowkey()))) {
      TRANS_LOG(WARN, "Failed to enocde memtable key", K(ret));
    } else if (OB_FAIL(get_mvcc_engine().check_row_locked(ctx.mvcc_acc_ctx_,
                                                          &mtk,
                                                          lock_state,
                                                          row_state))) {
      TRANS_LOG(WARN, "Failed to check row lock in mvcc engine", K(ret), K(mtk));
    } else if (lock_state.is_lock_decided()) {
      if (lock_state.is_locked_ && lock_state.lock_trans_id_ != my_tx_id) {
        rows_info.set_conflict_rowkey(i);
        rows_info.set_error_code(OB_TRY_LOCK_ROW_CONFLICT);
        break;
      } else if (lock_state.trans_version_ > snapshot_version) {
        rows_info.set_conflict_rowkey(i);
        rows_info.set_error_code(OB_TRANSACTION_SET_VIOLATION);
        break;
      } else if (check_exist && !row_state.is_delete()) {
        rows_info.set_conflict_rowkey(i);
        rows_info.set_error_code(OB_ERR_PRIMARY_KEY_DUPLICATE);
        break;
      } else {
        rows_info.set_row_checked(i);
      }
    }
  }
  return ret;
}

// TODO(handora.qc): remove after ddl merge sstable finished
// the interface of batch_check_row_lock is not supported on ddl merge sstable
int ObMemtable::check_rows_locked_on_ddl_merge_sstable(
    ObSSTable *sstable,
    const bool check_exist,
    const storage::ObTableIterParam &param,
    storage::ObTableAccessContext &context,
    ObRowsInfo &rows_info)
{
  int ret = OB_SUCCESS;
  ObStoreCtx &ctx = *(context.store_ctx_);
  ObTransID my_tx_id = ctx.mvcc_acc_ctx_.get_tx_id();
  SCN snapshot_version = ctx.mvcc_acc_ctx_.get_snapshot_version();

  for (int64_t i = 0; OB_SUCC(ret) && i < rows_info.rowkeys_.count(); i++) {
    const blocksstable::ObDatumRowkey &rowkey = rows_info.get_rowkey(i);
    ObStoreRowLockState &lock_state = rows_info.get_row_lock_state(i);
    ObRowState row_state;
    if (rows_info.is_row_checked(i)) {
    } else if (OB_FAIL(sstable->check_row_locked(param,
                                                 rowkey,
                                                 context,
                                                 lock_state,
                                                 row_state,
                                                 check_exist))) {
      TRANS_LOG(WARN, "Failed to check row lock in sstable", K(ret), KPC(this));
    } else if (lock_state.is_lock_decided()) {
      if (lock_state.is_locked_ && lock_state.lock_trans_id_ != my_tx_id) {
        rows_info.set_conflict_rowkey(i);
        rows_info.set_error_code(OB_TRY_LOCK_ROW_CONFLICT);
        break;
      } else if (lock_state.trans_version_ > snapshot_version) {
        rows_info.set_conflict_rowkey(i);
        rows_info.set_error_code(OB_TRANSACTION_SET_VIOLATION);
        break;
      } else if (check_exist && !row_state.is_delete()) {
        rows_info.set_conflict_rowkey(i);
        rows_info.set_error_code(OB_ERR_PRIMARY_KEY_DUPLICATE);
        break;
      } else {
        rows_info.set_row_checked(i);
      }
    }
  }
  return ret;
}

int ObMemtable::set(
  const storage::ObTableIterParam &param,
  storage::ObTableAccessContext &context,
  const common::ObIArray<share::schema::ObColDesc> &columns,
  const storage::ObStoreRow &row,
  const share::ObEncryptMeta *encrypt_meta,
  const bool check_exist)
{
  int ret = OB_SUCCESS;
  ObMvccWriteGuard guard(ret);
  ObMemtableKeyGenerator mtk_generator;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", K(*this));
    ret = OB_NOT_INIT;
  } else if (!param.is_valid() || !context.is_valid()) {
	ret = OB_INVALID_ARGUMENT;
	TRANS_LOG(WARN, "invalid argument, ", K(ret), K(param), K(context));
  } else if (NULL == context.store_ctx_->mvcc_acc_ctx_.get_mem_ctx()
             || param.get_schema_rowkey_count() > columns.count()
             || row.row_val_.count_ < columns.count()) {
    TRANS_LOG(WARN, "invalid param", K(param),
              K(columns.count()), K(row.row_val_.count_));
    ret = OB_INVALID_ARGUMENT;
#ifdef OB_BUILD_TDE_SECURITY
  //TODO: table_id is just used as encrypt_index, we may rename it in the future.
  //      If the function(set) no longer passes in this parameter(table_id),
  //      we need to construct ObTxEncryptMeta in advance, and pass tx_encrypt_meta(ObTxEncryptMeta*)
  //      instead of encrypt_meta(ObEncryptMeta*) into this function(set)
  } else if (need_for_save(encrypt_meta) && OB_FAIL(save_encrypt_meta(param.table_id_, encrypt_meta))) {
    TRANS_LOG(WARN, "store encrypt meta to memtable failed", KPC(encrypt_meta), KR(ret));
#endif
  } else if (OB_FAIL(mtk_generator.init(&row, 1, param.get_schema_rowkey_count(), columns))) {
    TRANS_LOG(WARN, "fail to generate memtable keys", KPC(encrypt_meta), K(*context.store_ctx_), KR(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(guard.write_auth(*context.store_ctx_))) {
    TRANS_LOG(WARN, "not allow to write", K(*context.store_ctx_));
  } else {
    lib::CompatModeGuard compat_guard(mode_);

    ret = set_(param,
               columns,
               row,
               NULL, /*old_row*/
               NULL, /*update_idx*/
               mtk_generator[0],
               check_exist,
               context,
               nullptr /*mvcc_row*/);
    guard.set_memtable(this);
  }

  if (OB_SUCC(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(try_report_dml_stat_(param.table_id_))) {
      TRANS_LOG_RET(WARN, tmp_ret, "fail to report dml stat", K_(reported_dml_stat));
    }

    /*****[for deadlock]*****/
    // recored this row is hold by this trans for deadlock detector
    if (param.is_non_unique_local_index_) {
      // no need to detect deadlock for non-unique local index table
    } else {
       MTL(ObLockWaitMgr*)->set_hash_holder(key_.get_tablet_id(),
                                            mtk_generator[0],
                                            context.store_ctx_->mvcc_acc_ctx_.get_mem_ctx()->get_tx_id());
    }
    /***********************/
  }
  return ret;
}

int ObMemtable::set(
    const storage::ObTableIterParam &param,
    storage::ObTableAccessContext &context,
    const ObIArray<ObColDesc> &columns,
    const ObIArray<int64_t> &update_idx,
    const storage::ObStoreRow &old_row,
    const storage::ObStoreRow &new_row,
    const share::ObEncryptMeta *encrypt_meta)
{
  int ret = OB_SUCCESS;
  ObMvccWriteGuard guard(ret);
  ObMemtableKeyGenerator mtk_generator;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", K(*this));
    ret = OB_NOT_INIT;
  } else if (!param.is_valid() || !context.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument, ", K(ret), K(param), K(context));
  } else if (NULL == context.store_ctx_->mvcc_acc_ctx_.get_mem_ctx()
             || param.get_schema_rowkey_count() > columns.count()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(ERROR, "invalid param", K(ret), K(param));
#ifdef OB_BUILD_TDE_SECURITY
  //TODO: table_id is just used as encrypt_index, we may rename it in the future.
  //      If the function(set) no longer passes in this parameter(table_id),
  //      we need to construct ObTxEncryptMeta in advance, and pass tx_encrypt_meta(ObTxEncryptMeta*)
  //      instead of encrypt_meta(ObEncryptMeta*) into this function(set)
  } else if (need_for_save(encrypt_meta) && OB_FAIL(save_encrypt_meta(param.table_id_, encrypt_meta))) {
      TRANS_LOG(WARN, "store encrypt meta to memtable failed", KPC(encrypt_meta), KR(ret));
#endif
  } else if (OB_FAIL(mtk_generator.init(&new_row, 1, param.get_schema_rowkey_count(), columns))) {
    TRANS_LOG(WARN, "fail to generate memtable keys", KPC(encrypt_meta), K(*context.store_ctx_), KR(ret));
  }

  if (OB_FAIL(ret)){
  } else if (OB_FAIL(guard.write_auth(*context.store_ctx_))) {
    TRANS_LOG(WARN, "not allow to write", K(*context.store_ctx_));
  } else {
    lib::CompatModeGuard compat_guard(mode_);

    ret = set_(param,
               columns,
               new_row,
               &old_row,
               &update_idx,
               mtk_generator[0],
               false/*check_exist*/,
               context,
               nullptr /*mvcc_row*/);
    guard.set_memtable(this);
  }

  if (OB_SUCC(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(try_report_dml_stat_(param.table_id_))) {
      TRANS_LOG_RET(WARN, tmp_ret, "fail to report dml stat", K_(reported_dml_stat));
    }
    /*****[for deadlock]*****/
    // recored this row is hold by this trans for deadlock detector
    if (param.is_non_unique_local_index_) {
      // no need to detect deadlock for local index table
    } else {
      MTL(ObLockWaitMgr*)->set_hash_holder(key_.get_tablet_id(),
                                           mtk_generator[0],
                                           context.store_ctx_->mvcc_acc_ctx_.get_mem_ctx()->get_tx_id());
    }
    /***********************/
  }
  return ret;
}

int ObMemtable::lock(
    const storage::ObTableIterParam &param,
    storage::ObTableAccessContext &context,
    const common::ObNewRow &row)
{
  int ret = OB_SUCCESS;
  ObMvccWriteGuard guard(ret);
  ObStoreRowkey tmp_key;
  ObMemtableKey mtk;
  ObMvccAccessCtx &acc_ctx = context.store_ctx_->mvcc_acc_ctx_;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", K(*this));
    ret = OB_NOT_INIT;
  } else if (!acc_ctx.is_write() || row.count_ < param.get_schema_rowkey_count()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid param", K(ret), K(row), K(param));
  } else if (OB_UNLIKELY(param.is_non_unique_local_index_)) {
    // since checking lock on non-unique local index is optimized out, so do a defensive judgment here,
    // actually, there is no circumstance in where locking the index table is need.
    ret = OB_NOT_SUPPORTED;
    TRANS_LOG(WARN, "locking the non-unique local index is not supported", K(ret), K(row), K(param));
  } else if (OB_FAIL(tmp_key.assign(row.cells_, param.get_schema_rowkey_count()))) {
    TRANS_LOG(WARN, "Failed to assign rowkey", K(row), K(param));
  } else if (OB_FAIL(mtk.encode(param.get_read_info()->get_columns_desc(), &tmp_key))) {
    TRANS_LOG(WARN, "encode mtk failed", K(ret), K(param));
  } else if (acc_ctx.write_flag_.is_check_row_locked()) {
    if (OB_FAIL(ObRowConflictHandler::check_foreign_key_constraint(param, context, tmp_key))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
        TRANS_LOG(WARN, "meet unexpected return code in check_row_locked", K(ret), K(context), K(mtk));
      }
    }
  } else if (OB_FAIL(guard.write_auth(*context.store_ctx_))) {
    TRANS_LOG(WARN, "not allow to write", K(*context.store_ctx_));
  } else if (OB_FAIL(lock_(param, context, tmp_key, mtk))) {
    TRANS_LOG(WARN, "lock_ failed", K(ret), K(param));
  } else {
    guard.set_memtable(this);
  }


  if (OB_FAIL(ret) && (OB_TRY_LOCK_ROW_CONFLICT != ret)) {
    TRANS_LOG(WARN, "lock fail", K(ret), K(row), K(mtk));
  }

  if (OB_SUCC(ret)) {
    /*****[for deadlock]*****/
    // recored this row is hold by this trans for deadlock detector
    MTL(ObLockWaitMgr*)->set_hash_holder(key_.get_tablet_id(),
                                         mtk,
                                         context.store_ctx_->mvcc_acc_ctx_.get_mem_ctx()->get_tx_id());
    /***********************/
  }
  return ret;
}

int ObMemtable::lock(
    const storage::ObTableIterParam &param,
    storage::ObTableAccessContext &context,
    const blocksstable::ObDatumRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  ObMvccWriteGuard guard(ret);
  ObMemtableKey mtk;
  ObMvccAccessCtx &acc_ctx = context.store_ctx_->mvcc_acc_ctx_;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", K(*this));
    ret = OB_NOT_INIT;
  } else if (!acc_ctx.is_write() || !rowkey.is_memtable_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid param", K(ret), K(rowkey));
  } else if (OB_UNLIKELY(param.is_non_unique_local_index_)) {
    // since checking lock on non-unique local index is optimized out, so do a defensive judgment here,
    // actually, there is no circumstance in where locking the index table is need.
    ret = OB_NOT_SUPPORTED;
    TRANS_LOG(WARN, "locking the non-unique local index is not supported", K(ret), K(param));
  } else if (OB_FAIL(mtk.encode(param.get_read_info()->get_columns_desc(), &rowkey.get_store_rowkey()))) {
    TRANS_LOG(WARN, "encode mtk failed", K(ret), K(param));
  } else if (acc_ctx.write_flag_.is_check_row_locked()) {
    if (OB_FAIL(ObRowConflictHandler::check_foreign_key_constraint(param, context, rowkey.get_store_rowkey()))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
        TRANS_LOG(WARN, "meet unexpected return code in check_row_locked", K(ret), K(context), K(mtk));
      }
    }
  } else if (OB_FAIL(guard.write_auth(*context.store_ctx_))) {
    TRANS_LOG(WARN, "not allow to write", K(*context.store_ctx_));
  } else if (OB_FAIL(lock_(param, context, rowkey.get_store_rowkey(), mtk))) {
    TRANS_LOG(WARN, "lock_ failed", K(ret), K(param));
  } else {
    guard.set_memtable(this);
  }

  if (OB_FAIL(ret) && (OB_TRY_LOCK_ROW_CONFLICT != ret) && (OB_TRANSACTION_SET_VIOLATION != ret)) {
    TRANS_LOG(WARN, "lock fail", K(ret), K(rowkey));
  }

  if (OB_SUCC(ret)) {
    /*****[for deadlock]*****/
    // recored this row is hold by this trans for deadlock detector
    MTL(ObLockWaitMgr*)->set_hash_holder(key_.get_tablet_id(),
                                         mtk,
                                         context.store_ctx_->mvcc_acc_ctx_.get_mem_ctx()->get_tx_id());
    /***********************/
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////////////
void ObMemtable::get_begin(ObMvccAccessCtx &ctx)
{
  ctx.handle_start_time_ = ObTimeUtility::current_time();
  EVENT_INC(MEMSTORE_GET_COUNT);
}

void ObMemtable::get_end(ObMvccAccessCtx &ctx, int ret)
{
  if (OB_SUCC(ret)) {
    EVENT_INC(MEMSTORE_GET_SUCC_COUNT);
  } else {
    EVENT_INC(MEMSTORE_GET_FAIL_COUNT);
  }
  EVENT_ADD(MEMSTORE_GET_TIME, ObTimeUtility::current_time() - ctx.handle_start_time_);
}

int ObMemtable::exist(
    const ObTableIterParam &param,
    ObTableAccessContext &context,
    const blocksstable::ObDatumRowkey &rowkey,
    bool &is_exist,
    bool &has_found)
{
  int ret = OB_SUCCESS;
  ObMemtableKey parameter_mtk;
  ObMvccValueIterator value_iter;
  ObQueryFlag query_flag;
  ObStoreRowLockState lock_state;
  query_flag.read_latest_ = true;
  query_flag.prewarm_ = false;
  //get_begin(context.store_ctx_->mvcc_acc_ctx_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(*this), K(ret));
  } else if (!param.is_valid()
             || !rowkey.is_memtable_valid()
             || !context.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid param", K(ret), K(param), K(rowkey), K(context));
  } else if (OB_FAIL(parameter_mtk.encode(param.get_read_info()->get_columns_desc(),
                                          &rowkey.get_store_rowkey()))) {
    TRANS_LOG(WARN, "mtk encode fail", "ret", ret);
  } else if (OB_FAIL(mvcc_engine_.get(context.store_ctx_->mvcc_acc_ctx_,
                                      query_flag,
                                      &parameter_mtk,
                                      ls_id_,
                                      NULL,
                                      value_iter,
                                      lock_state))) {
    TRANS_LOG(WARN, "get value iter fail, ", K(ret));
    if (OB_TRY_LOCK_ROW_CONFLICT == ret || OB_TRANSACTION_SET_VIOLATION == ret) {
      if (!query_flag.is_for_foreign_key_check()) {
        ret = OB_ERR_UNEXPECTED;  // to prevent retrying casued by throwing 6005
        TRANS_LOG(WARN, "should not meet row conflict if it's not for foreign key check",
                  K(ret), K(query_flag));
      } else if (OB_TRY_LOCK_ROW_CONFLICT == ret) {
        ObRowConflictHandler::post_row_read_conflict(
          context.store_ctx_->mvcc_acc_ctx_,
          *parameter_mtk.get_rowkey(),
          lock_state,
          key_.tablet_id_,
          get_ls_id(),
          0, 0 /* these two params get from mvcc_row, and for statistics, so we ignore them */,
          lock_state.trans_scn_);
      }
    } else {
      TRANS_LOG(WARN, "fail to do mvcc engine get", K(ret));
    }
  } else {
    const void *tnode = nullptr;
    const ObMemtableDataHeader *mtd = nullptr;
    while (OB_SUCC(ret) && !has_found) {
      if (OB_FAIL(value_iter.get_next_node(tnode))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          TRANS_LOG(WARN, "Fail to get next trans node", K(ret));
        }
      } else if (OB_ISNULL(tnode)) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "trans node is null", K(ret), KP(tnode));
      } else if (OB_ISNULL(mtd = reinterpret_cast<const ObMemtableDataHeader *>(reinterpret_cast<const ObMvccTransNode *>(tnode)->buf_))) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "trans node value is null", K(ret), KP(tnode), KP(mtd));
      } else {
        has_found = true;
        is_exist = mtd->dml_flag_ != blocksstable::ObDmlFlag::DF_DELETE;
      }
    }
    TRANS_LOG(DEBUG, "Check memtable exist rowkey, ", K(rowkey), K(is_exist),
        K(has_found));
  }
  //get_end(context.store_ctx_->mvcc_acc_ctx_, ret);
  return ret;
}


int ObMemtable::exist(
    storage::ObRowsInfo &rows_info,
    bool &is_exist,
    bool &all_rows_found)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!rows_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to exist scan", K(rows_info), K(ret));
  } else {
    is_exist = false;
    all_rows_found = false;
    for (int64_t i = 0; OB_SUCC(ret) && !is_exist && !all_rows_found && i < rows_info.rowkeys_.count(); i++) {
      const blocksstable::ObDatumRowkey &rowkey = rows_info.get_rowkey(i);
      bool row_found = false;
      if (rows_info.is_row_exist_checked(i)) {
      } else if (OB_FAIL(exist(rows_info.exist_helper_.table_iter_param_,
		                       rows_info.exist_helper_.table_access_context_,
                               rowkey,
                               is_exist,
                               row_found))) {
        TRANS_LOG(WARN, "fail to check rowkey exist", K((ret)));
      } else if (is_exist) {
        rows_info.set_conflict_rowkey(i);
        all_rows_found = true;
      } else if (row_found) {
        rows_info.set_row_exist_checked(i);
      }
    }
  }
  return ret;
}
////////////////////////////////////////////////////////////////////////////////////////////////////

void ObMemtable::scan_begin(ObMvccAccessCtx &ctx)
{
  ctx.handle_start_time_ = ObTimeUtility::current_time();
  EVENT_INC(MEMSTORE_SCAN_COUNT);
}

void ObMemtable::scan_end(ObMvccAccessCtx &ctx, int ret)
{
  if (OB_SUCC(ret)) {
    EVENT_INC(MEMSTORE_SCAN_SUCC_COUNT);
  } else {
    EVENT_INC(MEMSTORE_SCAN_FAIL_COUNT);
  }
  EVENT_ADD(MEMSTORE_SCAN_TIME, ObTimeUtility::current_time() - ctx.handle_start_time_);
}

int ObMemtable::get(
    const storage::ObTableIterParam &param,
    storage::ObTableAccessContext &context,
    const ObDatumRowkey &rowkey,
    blocksstable::ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  ObMemtableKey parameter_mtk;
  ObMemtableKey returned_mtk;
  ObMvccValueIterator value_iter;
  const ObITableReadInfo *read_info = nullptr;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", K(*this));
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!param.is_valid())
             || OB_UNLIKELY(!context.is_valid())
             || OB_UNLIKELY(!rowkey.is_memtable_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid param, ", K(param), K(context), K(rowkey));
  } else if (OB_ISNULL(read_info = param.get_read_info(context.use_fuse_row_cache_))) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "Unexpected null read info", K(ret), K(param), K(context.use_fuse_row_cache_));
  } else {
    const ObColDescIArray &out_cols = read_info->get_columns_desc();
    ObStoreRowLockState lock_state;
    if (OB_FAIL(parameter_mtk.encode(out_cols, &rowkey.get_store_rowkey()))) {
      TRANS_LOG(WARN, "mtk encode fail", "ret", ret);
    } else if (OB_FAIL(mvcc_engine_.get(context.store_ctx_->mvcc_acc_ctx_,
                                        context.query_flag_,
                                        &parameter_mtk,
                                        ls_id_,
                                        &returned_mtk,
                                        value_iter,
                                        lock_state))) {
      if (OB_TRY_LOCK_ROW_CONFLICT == ret || OB_TRANSACTION_SET_VIOLATION == ret) {
        if (!context.query_flag_.is_for_foreign_key_check()) {
          ret = OB_ERR_UNEXPECTED;  // to prevent retrying casued by throwing 6005
          TRANS_LOG(WARN, "should not meet lock conflict if it's not for foreign key check",
                    K(ret), K(context.query_flag_));
        } else if (OB_TRY_LOCK_ROW_CONFLICT == ret){
          ObRowConflictHandler::post_row_read_conflict(
                        context.store_ctx_->mvcc_acc_ctx_,
                        *parameter_mtk.get_rowkey(),
                        lock_state,
                        key_.tablet_id_,
                        get_ls_id(),
                        0, 0 /* these two params get from mvcc_row, and for statistics, so we ignore them */,
                        lock_state.trans_scn_);
        }
      } else {
        TRANS_LOG(WARN, "fail to do mvcc engine get", K(ret));
      }
    } else {
      const int64_t request_cnt = read_info->get_request_count();
      if (OB_UNLIKELY(!row.is_valid())) {
        char *trans_info_ptr = nullptr;
        if (param.need_trans_info()) {
          int64_t length = concurrency_control::ObTransStatRow::MAX_TRANS_STRING_SIZE;
          if (OB_ISNULL(trans_info_ptr = static_cast<char *>(context.stmt_allocator_->alloc(length)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            STORAGE_LOG(WARN, "fail to alloc memory", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
          // do nothing
        } else if (OB_FAIL(row.init(*context.allocator_, request_cnt, trans_info_ptr))) {
          STORAGE_LOG(WARN, "Failed to init datum row", K(ret), K(param.need_trans_info()));
        }
      }
      if (OB_SUCC(ret)) {
        const ObStoreRowkey *store_rowkey = nullptr;
        concurrency_control::ObTransStatRow trans_stat_row;
        (void)value_iter.get_trans_stat_row(trans_stat_row);
        if (NULL != returned_mtk.get_rowkey()) {
          returned_mtk.get_rowkey(store_rowkey);
        } else {
          parameter_mtk.get_rowkey(store_rowkey);
        }
        ObNopBitMap bitmap;
        int64_t row_scn = 0;
        if (OB_FAIL(bitmap.init(request_cnt, store_rowkey->get_obj_cnt()))) {
          TRANS_LOG(WARN, "Failed to innt bitmap", K(ret), K(request_cnt), KPC(store_rowkey));
        } else if (OB_FAIL(ObReadRow::iterate_row(*read_info, *store_rowkey, *context.allocator_, value_iter, row, bitmap, row_scn))) {
          TRANS_LOG(WARN, "Failed to iterate row, ", K(ret), K(rowkey));
        } else {
          if (param.need_scn_) {
            if (row_scn == share::SCN::max_scn().get_val_for_tx()) {
              // TODO(handora.qc): remove it as if we confirmed no problem according to row_scn
              TRANS_LOG(INFO, "use max row scn", K(context.store_ctx_->mvcc_acc_ctx_), K(trans_stat_row));
            }
            for (int64_t i = 0; i < out_cols.count(); i++) {
              if (out_cols.at(i).col_id_ == OB_HIDDEN_TRANS_VERSION_COLUMN_ID) {
                row.storage_datums_[i].set_int(row_scn);
                TRANS_LOG(DEBUG, "set row scn is", K(i), K(row_scn), K(row));
              }
            }
          }

          // generate trans stat datum for 4377 check
          concurrency_control::build_trans_stat_datum(&param, row, trans_stat_row);
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "get end, fail",
              "ret", ret,
              "tablet_id_", key_.tablet_id_,
              "table_id", param.table_id_,
              "rowkey", rowkey);
  }
  return ret;
}

int ObMemtable::get(
    const storage::ObTableIterParam &param,
    storage::ObTableAccessContext &context,
    const ObDatumRowkey &rowkey,
    ObStoreRowIterator *&row_iter)
{
  TRANS_LOG(TRACE, "memtable.get", K(rowkey));
  int ret = OB_SUCCESS;
  ObStoreRowIterator *get_iter_ptr = NULL;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", K(*this));
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!param.is_valid()
             || !context.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument, ", K(ret), K(param), K(context));
  } else {
    ALLOCATE_TABLE_STORE_ROW_IETRATOR(context,
        ObMemtableGetIterator,
        get_iter_ptr);
  }

  if (OB_SUCC(ret)) {
    if (NULL == get_iter_ptr) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "get iter init fail", "ret", ret);
    } else if (OB_FAIL(get_iter_ptr->init(param, context, this, &rowkey))) {
      TRANS_LOG(WARN, "get iter init fail", K(ret), K(param), K(context), K(rowkey));
    } else {
      row_iter = get_iter_ptr;
    }
  }
  if (OB_FAIL(ret)) {
    if (NULL != get_iter_ptr) {
      get_iter_ptr->~ObStoreRowIterator();
      FREE_TABLE_STORE_ROW_IETRATOR(context, get_iter_ptr);
      get_iter_ptr = NULL;
    }
    TRANS_LOG(WARN, "get fail", K(ret), K_(key), K(param.table_id_));
  }
  return ret;
}

// multi-version scan interface
int ObMemtable::scan(
    const storage::ObTableIterParam &param,
    storage::ObTableAccessContext &context,
    const ObDatumRange &range,
    ObStoreRowIterator *&row_iter)
{
  int ret = OB_SUCCESS;
  ObDatumRange real_range;
  ObStoreRowIterator *scan_iter_ptr = NULL;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", K(*this));
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!param.is_valid() || !context.is_valid() || !range.is_valid())) {
    TRANS_LOG(WARN, "invalid param", K(param), K(context), K(range));
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (param.is_multi_version_minor_merge_) {
      ALLOCATE_TABLE_STORE_ROW_IETRATOR(context,
          ObMemtableMultiVersionScanIterator,
          scan_iter_ptr);
      if (OB_SUCC(ret)) {
        if (NULL == scan_iter_ptr) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "scan iter init fail", "ret", ret, K(real_range), K(param), K(context));
        } else if (OB_FAIL(scan_iter_ptr->init(param,
                                               context,
                                               this,
                                               &m_get_real_range(real_range,
                                                                 range,
                                                                 context.query_flag_.is_reverse_scan())))) {
          TRANS_LOG(WARN, "scan iter init fail", "ret", ret, K(real_range), K(param), K(context));
        }
      }
    } else {
      ALLOCATE_TABLE_STORE_ROW_IETRATOR(context,
            ObMemtableScanIterator,
            scan_iter_ptr);
      if (OB_SUCC(ret)) {
        if (NULL == scan_iter_ptr) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "scan iter init fail", "ret", ret, K(real_range), K(param), K(context));
        } else if (OB_FAIL(scan_iter_ptr->init(param,
                                               context,
                                               this,
                                               &range))) {
          TRANS_LOG(WARN, "scan iter init fail", "ret", ret, K(param), K(context), K(range));
        }
      }
    }

    if (OB_SUCC(ret)) {
      row_iter = scan_iter_ptr;
    } else {
      if (NULL != scan_iter_ptr) {
        scan_iter_ptr->~ObStoreRowIterator();
        FREE_TABLE_STORE_ROW_IETRATOR(context, scan_iter_ptr);
        scan_iter_ptr = NULL;
      }
      TRANS_LOG(WARN, "scan end, fail",
                "ret", ret,
                "tablet_id_", key_.tablet_id_,
                "table_id", param.table_id_,
                "range", range);
    }
  }
  return ret;
}

int ObMemtable::multi_get(
    const storage::ObTableIterParam &param,
    storage::ObTableAccessContext &context,
    const ObIArray<ObDatumRowkey> &rowkeys,
    ObStoreRowIterator *&row_iter)
{
  int ret = OB_SUCCESS;
  ObStoreRowIterator *mget_iter_ptr = NULL;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", K(*this));
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!param.is_valid()
             || !context.is_valid()
             || (0 == rowkeys.count()))) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument, ", K(ret), K(param), K(context), K(rowkeys));
  } else {
    ALLOCATE_TABLE_STORE_ROW_IETRATOR(context,
        ObMemtableMGetIterator,
        mget_iter_ptr);
  }

  if (OB_SUCC(ret)) {
    if (NULL == mget_iter_ptr) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "mget iter init fail", "ret", ret);
    } else if (OB_FAIL(mget_iter_ptr->init(param, context, this, &rowkeys))) {
      TRANS_LOG(WARN, "mget iter init fail", "ret", ret, K(param), K(context), K(rowkeys));
    } else {
      row_iter = mget_iter_ptr;
    }
  }
  if (OB_FAIL(ret)) {
    if (NULL != mget_iter_ptr) {
      mget_iter_ptr->~ObStoreRowIterator();
      FREE_TABLE_STORE_ROW_IETRATOR(context, mget_iter_ptr);
      mget_iter_ptr = NULL;
    }
    TRANS_LOG(WARN, "mget fail",
              "ret", ret,
              "tablet_id", key_.tablet_id_,
              "table_id", param.table_id_,
              "rowkeys", strarray<ObDatumRowkey>(rowkeys));
  }
  return ret;
}

int ObMemtable::multi_scan(
    const storage::ObTableIterParam &param,
    storage::ObTableAccessContext &context,
    const ObIArray<ObDatumRange> &ranges,
    ObStoreRowIterator *&row_iter)
{
  int ret = OB_SUCCESS;
  ObStoreRowIterator *mscan_iter_ptr = NULL;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", "this", this);
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!param.is_valid()
             || !context.is_valid()
             || (0 == ranges.count()))) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument, ", K(ret), K(param), K(context), K(ranges));
  } else {
    ALLOCATE_TABLE_STORE_ROW_IETRATOR(context,
        ObMemtableMScanIterator,
        mscan_iter_ptr);
  }
  if (OB_SUCC(ret)) {
    if (NULL == mscan_iter_ptr) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "scan iter init fail", "ret", ret);
    } else if (OB_FAIL((mscan_iter_ptr->init(param, context, this, &ranges)))) {
      TRANS_LOG(WARN, "mscan iter init fail", "ret", ret);
    } else {
      row_iter = mscan_iter_ptr;
      STORAGE_LOG(DEBUG, "multiscan iterator inited");
    }
  }
  if (OB_FAIL(ret)) {
    if (NULL != mscan_iter_ptr) {
      mscan_iter_ptr->~ObStoreRowIterator();
      FREE_TABLE_STORE_ROW_IETRATOR(context, mscan_iter_ptr);
      mscan_iter_ptr = NULL;
    }
    TRANS_LOG(WARN, "mscan fail",
              "ret", ret,
              "tablet_id", key_.tablet_id_,
              "table_id", param.table_id_,
              "ranges", strarray<ObDatumRange>(ranges));
  }
  return ret;
}

int ObMemtable::replay_row(ObStoreCtx &ctx,
                           const share::SCN &scn,
                           ObMemtableMutatorIterator *mmi)
{
  int ret = OB_SUCCESS;

  uint64_t table_id = OB_INVALID_ID;
  int64_t table_version = 0;
  uint32_t modify_count = 0;
  uint32_t acc_checksum = 0;
  int64_t version = 0;
  int32_t flag = 0;
  transaction::ObTxSEQ seq_no;
  int64_t column_cnt = 0;
  ObStoreRowkey rowkey;
  ObRowData row;
  ObRowData old_row;
  blocksstable::ObDmlFlag dml_flag = blocksstable::ObDmlFlag::DF_NOT_EXIST;
  ObMemtableCtx *mt_ctx = ctx.mvcc_acc_ctx_.mem_ctx_;
  ObPartTransCtx *part_ctx = static_cast<ObPartTransCtx *>(mt_ctx->get_trans_ctx());
  common::ObTimeGuard timeguard("ObMemtable::replay_row", 5 * 1000);

  if (OB_FAIL(mmi->get_mutator_row().copy(table_id, rowkey, table_version, row,
                                        old_row, dml_flag, modify_count, acc_checksum, version,
                                        flag, seq_no, column_cnt))) {
    if (OB_ITER_END != ret) {
      TRANS_LOG(WARN, "get next row error", K(ret));
    }
  } else if (FALSE_IT(timeguard.click("mutator_row copy"))) {
  } else if (OB_FAIL(check_standby_cluster_schema_condition_(ctx, table_id, table_version))) {
    TRANS_LOG(WARN, "failed to check standby_cluster_schema_condition", K(ret), K(table_id),
              K(table_version));
  } else if (FALSE_IT(timeguard.click("check_standby_cluster_schema_condition"))) {
  } else if (OB_UNLIKELY(dml_flag == blocksstable::ObDmlFlag::DF_NOT_EXIST)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "Unexpected not exist trans node", K(ret), K(dml_flag), K(rowkey));
  } else {
    lib::CompatModeGuard compat_guard(mode_);
    ObMemtableData mtd(dml_flag, row.size_, row.data_);
    ObMemtableKey mtk;
    const transaction::ObTransID tx_id = ctx.mvcc_acc_ctx_.tx_id_;
    ObTxNodeArg arg(tx_id,        /*trans id*/
                    &mtd,         /*memtable_data*/
                    NULL,         /*old_row*/
                    version,      /*memstore_version*/
                    seq_no,       /*seq_no*/
                    modify_count, /*modify_count*/
                    acc_checksum, /*acc_checksum*/
                    scn,          /*scn*/
                    column_cnt    /*column_cnt*/);

    if (OB_FAIL(mtk.encode(&rowkey))) {
      TRANS_LOG(WARN, "mtk encode fail", "ret", ret);
    } else if (OB_FAIL(mvcc_replay_(ctx, &mtk, arg))) {
      TRANS_LOG(WARN, "mvcc replay failed", K(ret), K(ctx), K(arg));
    } else if (FALSE_IT(timeguard.click("mvcc_replay_"))) {
    }

    if (OB_SUCCESS != ret) {
      if (OB_ALLOCATE_MEMORY_FAILED != ret || REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
        TRANS_LOG(WARN, "m_replay_row fail", K(ret), K(table_id), K(rowkey), K(row), K(dml_flag),
                  K(modify_count), K(acc_checksum));
      }
    } else {
      ctx.mvcc_acc_ctx_.mem_ctx_->set_table_version(table_version);
      if (dml_flag != blocksstable::ObDmlFlag::DF_LOCK) {
        set_max_data_schema_version(table_version);
        set_max_column_cnt(column_cnt);
      }
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

int ObMemtable::lock_row_on_frozen_stores_(
    const storage::ObTableIterParam &param,
    const ObTxNodeArg &arg,
    const ObMemtableKey *key,
    const bool check_exist,
    storage::ObTableAccessContext &context,
    ObMvccRow *value,
    ObMvccWriteResult &res)
{
  int ret = OB_SUCCESS;
  ObStoreRowLockState &lock_state = res.lock_state_;
  ObStoreCtx &ctx = *(context.store_ctx_);
  const ObTxSEQ reader_seq_no = ctx.mvcc_acc_ctx_.snapshot_.scn_;

  if (OB_ISNULL(value) || !ctx.mvcc_acc_ctx_.is_write() || NULL == key) {
    TRANS_LOG(WARN, "invalid param", KP(value), K(ctx), KP(key));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(ctx.table_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "tables handle or iterator in context is null", K(ret), K(ctx));
  } else if (lock_state.row_exist_decided()) {
    // in the suitation, row already exists, so we need not examine any
    // existance or lock status of the row in the following tables
  } else if (!check_exist && value->is_lower_lock_scaned()) {
  } else if (!check_exist && param.is_non_unique_local_index_) {
    // skip if it is non-unique index for which the lock has been checked in primary table
  } else {
    ObRowState row_state;
    common::ObSEArray<ObITable *, 4> iter_tables;
    if (OB_FAIL(get_all_tables_(ctx, iter_tables))) {
      TRANS_LOG(WARN, "get all tables from table iter failed", KR(ret));
    } else if (OB_FAIL(internal_lock_row_on_frozen_stores_(
                   check_exist, key, param, iter_tables, context, res, row_state))) {
      TRANS_LOG(WARN, "internal lock row failed", KR(ret));
    } else {
      lock_state.trans_version_ = MAX(lock_state.trans_version_, row_state.get_max_trans_version());
      if (OB_FAIL(lock_row_on_frozen_stores_on_success(lock_state.is_locked_, arg.data_->dml_flag_,
                                                       lock_state.trans_version_, context, value, res))) {
        if (OB_UNLIKELY(OB_TRANSACTION_SET_VIOLATION != ret)) {
          TRANS_LOG(WARN, "Failed to call on success callback after lock row on frozen stores", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObMemtable::get_all_tables_(ObStoreCtx &ctx, ObIArray<ObITable *> &iter_tables)
{
  int ret = OB_SUCCESS;
  ctx.table_iter_->resume();
  while (OB_SUCC(ret)) {
    ObITable *table_ptr = nullptr;
    if (OB_FAIL(ctx.table_iter_->get_next(table_ptr))) {
      if (OB_ITER_END != ret) {
        TRANS_LOG(WARN, "failed to get next tables", K(ret));
      } else {
        ret = OB_SUCCESS;
        break;
      }
    } else if (OB_ISNULL(table_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "table must not be null", K(ret), KPC(ctx.table_iter_));
    } else if (OB_FAIL(iter_tables.push_back(table_ptr))) {
      TRANS_LOG(WARN, "rowkey_exists check::", K(ret), KPC(table_ptr));
    }
  }
  return ret;
}

int ObMemtable::internal_lock_row_on_frozen_stores_(const bool check_exist,
                                                    const ObMemtableKey *key,
                                                    const storage::ObTableIterParam &param,
                                                    const ObIArray<ObITable *> &iter_tables,
                                                    storage::ObTableAccessContext &context,
                                                    ObMvccWriteResult &res,
                                                    ObRowState &row_state)

{
  int ret = OB_SUCCESS;

  // lock_is_decided means we have found either the lock that is locked by
  // an active txn or the latest unlocked txn data. And after we have found
  // either of the above situations, whether the row is locked is determined.
  bool lock_is_decided = false;

  ObStoreCtx &ctx = *(context.store_ctx_);
  ObStoreRowLockState tmp_lock_state;
  ObStoreRowLockState &lock_state = res.lock_state_;
  lock_state.reset();
  // ignore active memtable
  for (int64_t i = iter_tables.count() - 2; OB_SUCC(ret) && i >= 0; i--) {
    tmp_lock_state.reset();
    if (NULL == iter_tables.at(i)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "ObIStore is null", K(ret), K(i));
    } else if (iter_tables.at(i)->is_data_memtable()) {
      ObMemtable *memtable = static_cast<ObMemtable *>(iter_tables.at(i));
      ObMvccEngine &mvcc_engine = memtable->get_mvcc_engine();
      // FIXME(handora.qc): two active memtable in transfer
      if (OB_FAIL(mvcc_engine.check_row_locked(ctx.mvcc_acc_ctx_, key, tmp_lock_state, row_state))) {
        TRANS_LOG(WARN, "mvcc engine check row lock fail", K(ret), K(lock_state), K(tmp_lock_state));
      }
    } else if (iter_tables.at(i)->is_sstable()) {
      blocksstable::ObDatumRowkeyHelper rowkey_converter;
      blocksstable::ObDatumRowkey datum_rowkey;
      ObITable *sstable = iter_tables.at(i);
      if (OB_FAIL(rowkey_converter.convert_datum_rowkey(key->get_rowkey()->get_rowkey(), datum_rowkey))) {
        STORAGE_LOG(WARN, "Failed to convert datum rowkey", K(ret), KPC(key));
      } else if (OB_FAIL(static_cast<ObSSTable *>(sstable)->check_row_locked(
                     param, datum_rowkey, context, tmp_lock_state, row_state, check_exist))) {
        TRANS_LOG(WARN,
                  "sstable check row lock fail",
                  K(ret),
                  KPC(key),
                  K(check_exist),
                  K(datum_rowkey),
                  K(lock_state),
                  K(tmp_lock_state),
                  K(row_state));
      }
      TRANS_LOG(DEBUG,
                "sstable check row lock debug",
                K(ret),
                KPC(key),
                KPC(sstable),
                K(check_exist),
                K(datum_rowkey),
                K(lock_state),
                K(tmp_lock_state),
                K(row_state));
    } else if (iter_tables.at(i)->is_direct_load_memtable()) {
      ObDDLKV *ddl_kv = static_cast<ObDDLKV *>(iter_tables.at(i));
      blocksstable::ObDatumRowkeyHelper rowkey_converter;
      blocksstable::ObDatumRowkey datum_rowkey;
      if (OB_FAIL(rowkey_converter.convert_datum_rowkey(key->get_rowkey()->get_rowkey(), datum_rowkey))) {
        STORAGE_LOG(WARN, "Failed to convert datum rowkey", K(ret), KPC(key));
      } else if (OB_FAIL(ddl_kv->check_row_locked(
                     param, datum_rowkey, context, tmp_lock_state, row_state, check_exist))) {
        TRANS_LOG(WARN,
                  "direct load memtable check row lock fail",
                  K(ret),
                  KPC(key),
                  K(check_exist),
                  K(datum_rowkey),
                  K(lock_state),
                  K(tmp_lock_state),
                  K(row_state));
      }
      TRANS_LOG(DEBUG,
                "direct load memtable check row lock debug",
                K(ret),
                KPC(key),
                KPC(ddl_kv),
                K(check_exist),
                K(datum_rowkey),
                K(lock_state),
                K(tmp_lock_state),
                K(row_state));
    } else {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unknown store type", K(ret), K(iter_tables), K(i));
    }

    if (OB_SUCC(ret)) {
      // 1. Check duplication.
      if (check_exist) {
        if (row_state.is_not_exist()) {
        } else if (row_state.is_delete()) {
          break;
        } else {
          ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
        }
      }
      // 2. Check write-write conflict.
      if (OB_SUCC(ret) && !row_state.is_lock_decided()) {
        lock_is_decided = tmp_lock_state.is_lock_decided();
        if (lock_is_decided) {
          row_state.set_lock_decided();
          lock_state = tmp_lock_state;
          if (tmp_lock_state.is_locked(ctx.mvcc_acc_ctx_.get_tx_id())) {
            ret = OB_TRY_LOCK_ROW_CONFLICT;
          } else if (!check_exist) {
            break;
          }
        }
      }
    }
  }
  return ret;
}

int ObMemtable::lock_row_on_frozen_stores_on_success(
    const bool row_locked,
    const blocksstable::ObDmlFlag writer_dml_flag,
    const share::SCN &max_trans_version,
    storage::ObTableAccessContext &context,
    ObMvccRow *value,
    ObMvccWriteResult &res)
{
  int ret = OB_SUCCESS;
  ObStoreCtx &ctx = *(context.store_ctx_);
  ObStoreRowLockState &lock_state = res.lock_state_;
  const ObTxSEQ &reader_seq_no = ctx.mvcc_acc_ctx_.snapshot_.scn_;
  const ObTxSEQ &writer_seq_no = ctx.mvcc_acc_ctx_.tx_scn_;
  // use tx_id = 0 indicate MvccRow's max_trans_version inherit from old table
  transaction::ObTransID tx_id(0);
  value->update_max_trans_version(max_trans_version, tx_id);
  ObTransID my_tx_id = ctx.mvcc_acc_ctx_.get_tx_id();
  if (!row_locked) {
    // there is no locks on frozen stores
    if (max_trans_version > ctx.mvcc_acc_ctx_.get_snapshot_version()) {
      ret = OB_TRANSACTION_SET_VIOLATION;
      TRANS_LOG(WARN, "TRANS_SET_VIOLATION", K(ret), K(max_trans_version), "ctx", ctx);
    }
    value->set_lower_lock_scaned();
    TRANS_LOG(DEBUG, "lower lock check finish", K(*value));
  } else {
    // There is the lock on frozen stores by my self
    // If the lock is locked by myself and the locker's tnode is DF_LOCK, it
    // means the row is under my control and new lock is unnecessary for the
    // semantic of the LOCK dml. So we remove the lock tnode here and report
    // the success of the mvcc_write .
    // NB: You need pay attention to the requirement of the parallel das
    // update. It may insert two locks on the same row in the same sql. So
    // it will cause two upside down lock(which means smaller lock tnode
    // lies ahead the bigger one). So the optimization here is essential.
    if (res.has_insert()
         && lock_state.lock_trans_id_ == my_tx_id
         && blocksstable::ObDmlFlag::DF_LOCK == writer_dml_flag) {
      (void)mvcc_engine_.mvcc_undo(value);
      res.need_insert_ = false;
    }

    // We need check whether the same row is operated by same txn
    // concurrently to prevent undesirable resuly.
    if (res.has_insert() &&     // we only need check when the node is exactly inserted
         !res.is_checked_ &&     // we only need check when the active memtable check is missed
         OB_FAIL(concurrent_control::check_sequence_set_violation(ctx.mvcc_acc_ctx_.write_flag_,
                                                                  reader_seq_no,
                                                                  my_tx_id,
                                                                  writer_dml_flag,
                                                                  writer_seq_no,
                                                                  lock_state.lock_trans_id_,
                                                                  lock_state.lock_dml_flag_,
                                                                  lock_state.lock_data_sequence_))) {
      TRANS_LOG(WARN, "check sequence set violation failed", K(ret), KPC(this));
    }
  }
  return ret;
}

void ObMemtable::lock_row_on_frozen_stores_on_failure(
    const blocksstable::ObDmlFlag writer_dml_flag,
    const ObMemtableKey &key,
    int &ret,
    ObMvccRow *value,
    storage::ObTableAccessContext &context,
    ObMvccWriteResult &res)
{
  ObStoreCtx &ctx = *(context.store_ctx_);
  ObIMemtableCtx *mem_ctx = ctx.mvcc_acc_ctx_.get_mem_ctx();
  const SCN snapshot_version = ctx.mvcc_acc_ctx_.get_snapshot_version();
  // Double lock detection is used to prevent that the row who has been
  // operated by the same txn before will be unexpectedly conflicted with
  // other writes in sstable. So we report the error when conflict is
  // discovered with the data operation is not the first time.
  // TIP: While we need notice that only the tnode which has been operated
  // successfully this time need to be checked with double lock detection.
  // Because under the case of parallel lock(the same row may be locked by the
  // different threads under the same txn parallelly. You can understand the
  // behavior through das for update), two lock operation may be inserted
  // parallelly for the same row in the memtable, while both lock may fail
  // with conflicts even the second lock operate successfully(the lock will be
  // pretended to insert successfully at the beginning of mvcc_write and fail
  // to pass the sstable row lock check for performance issue).
  if (OB_UNLIKELY(!res.is_new_locked_)
       && res.has_insert()
       && OB_TRY_LOCK_ROW_CONFLICT == ret) {
    TRANS_LOG(ERROR, "double lock detected", K(key), KPC(value), K(ctx), K(res));
  }
  if (!res.has_insert()) {
    if (blocksstable::ObDmlFlag::DF_LOCK != writer_dml_flag) {
      TRANS_LOG(ERROR, "sstable conflict will occurred when already inserted",
                K(ctx), KPC(this), K(writer_dml_flag));
    } else {
      TRANS_LOG(WARN, "sstable conflict will occurred when lock operation",
                K(ctx), KPC(this), K(writer_dml_flag));
    }
  }
  if (OB_TRY_LOCK_ROW_CONFLICT == ret) {
    ret = post_row_write_conflict_(ctx.mvcc_acc_ctx_,
                                   key,
                                   res.lock_state_,
                                   value->get_last_compact_cnt(),
                                   value->get_total_trans_node_cnt());
  } else if (OB_TRANSACTION_SET_VIOLATION == ret) {
    mem_ctx->on_tsc_retry(key,
                          snapshot_version,
                          value->get_max_trans_version(),
                          value->get_max_trans_id());
  } else if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
  } else {
    TRANS_LOG(WARN, "Failed to lock row on frozen store", K(ret));
  }
}

int ObMemtable::lock_rows_on_frozen_stores_(
    const bool check_exist,
    const storage::ObTableIterParam &param,
    const ObMemtableKeyGenerator &memtable_keys,
    storage::ObTableAccessContext &context,
    ObMvccRowAndWriteResults &mvcc_rows,
    ObRowsInfo &rows_info)
{
  int ret = OB_SUCCESS;
  ObStoreCtx &ctx = *(context.store_ctx_);
  if (OB_ISNULL(ctx.table_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "Table iterator in context is null", K(ret), K(ctx));
  } else if (!check_exist && param.is_non_unique_local_index_) {
    // skip if it is non-unique index table for which the transaction conflict has checked in primary table,
    // so there is no need to check transaction conflict again.
  } else {
    common::ObSEArray<ObITable *, 4> iter_tables;
    if (OB_FAIL(get_all_tables_(ctx, iter_tables))) {
      TRANS_LOG(WARN, "get all tables from table iter failed", KR(ret));
    } else {
      share::SCN max_trans_version = SCN::min_scn();
      if (OB_FAIL(internal_lock_rows_on_frozen_stores_(check_exist, iter_tables, param, context,
                                                       max_trans_version, rows_info))) {
        TRANS_LOG(WARN, "Failed to lock rows on frozen stores", K(ret), K(check_exist), K(iter_tables));
      } else if (!rows_info.have_conflict()) {
        for (int64_t i = 0; OB_SUCC(ret) && i < mvcc_rows.count(); ++i) {
          ObMvccWriteResult &write_result = mvcc_rows[i].write_result_;
          ObMvccRow* mvcc_row = mvcc_rows[i].mvcc_row_;
          if (mvcc_row->is_lower_lock_scaned()) {
            continue;
          }
          ObStoreRowLockState &row_lock_state = rows_info.get_row_lock_state(i);
          row_lock_state.trans_version_ = MAX(row_lock_state.trans_version_, max_trans_version);
          if (OB_FAIL(lock_row_on_frozen_stores_on_success(row_lock_state.is_locked_,
                                                           blocksstable::ObDmlFlag::DF_INSERT,
                                                           row_lock_state.trans_version_, context,
                                                           mvcc_row, write_result))) {
            TRANS_LOG(WARN, "Failed to lock rows on frozen stores", K(ret), K(check_exist), K(iter_tables));
            // The upper layer determines whether to call lock_row_on_frozen_stores_on_failure based on whether rows_info has a conflict.
            // Therefore, here we convert the error code to success and store the error code in rows_info.
            // TODO(hanling), this part of the logic needs to be discussed again.
            rows_info.set_conflict_rowkey(i);
            rows_info.set_error_code(ret);
            ret = OB_SUCCESS;
            break;
          }
        }
      }
    }
  }
  return ret;
}

int ObMemtable::internal_lock_rows_on_frozen_stores_(
    const bool check_exist,
    const ObIArray<ObITable *> &iter_tables,
    const storage::ObTableIterParam &param,
    storage::ObTableAccessContext &context,
    share::SCN &max_trans_version,
    ObRowsInfo &rows_info)
{
  int ret = OB_SUCCESS;
  for (int64_t i = iter_tables.count() - 2; OB_SUCC(ret) && i >= 0; i--) {
    ObITable *i_table = iter_tables.at(i);
    if (i_table->is_data_memtable()) {
      ObMemtable *memtable = static_cast<ObMemtable *>(i_table);
      if (OB_FAIL(memtable->check_rows_locked(check_exist, param, context, rows_info))) {
        TRANS_LOG(WARN, "Failed to check rows locked and duplication in memtable", K(ret), K(i),
                  K(iter_tables));
      }
    } else if (i_table->is_sstable()) {
      ObSSTable *sstable = static_cast<ObSSTable *>(i_table);
      if (sstable->is_ddl_merge_sstable()) {
        if (OB_FAIL(check_rows_locked_on_ddl_merge_sstable(sstable,
                                                           check_exist,
                                                           param,
                                                           context,
                                                           rows_info))) {
          TRANS_LOG(WARN, "Failed to check rows locked for sstable", K(ret), K(i), K(iter_tables));
        }
      } else {
        if (OB_FAIL(sstable->check_rows_locked(check_exist,
                                               context,
                                               max_trans_version,
                                               rows_info))) {
          TRANS_LOG(WARN, "Failed to check rows locked for sstable", K(ret), K(i), K(iter_tables));
        }
      }
    } else if (i_table->is_direct_load_memtable()) {
      ObDDLKV *ddl_kv = static_cast<ObDDLKV *>(i_table);
      ObDirectLoadMemtableRowsLockedChecker checker(*this, check_exist, param, context, rows_info);
      if (OB_FAIL(ddl_kv->access_first_ddl_memtable(checker))) {
        if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
          STORAGE_LOG(WARN, "fail to access first ddl memtable", K(ret), K(i), K(iter_tables));
        } else {
          ret = OB_SUCCESS;
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "Unknown store type", K(ret), K(iter_tables), K(i));
    }

    if (OB_SUCC(ret) && (rows_info.all_rows_found() || rows_info.have_conflict())) {
      break;
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// no allocator here for it, so we desirialize and save the struct info

int64_t ObMemtable::get_size() const
{
  return local_allocator_.get_size();
}

int64_t ObMemtable::get_occupied_size() const
{
  return local_allocator_.get_occupied_size();
}

ObDatumRange &ObMemtable::m_get_real_range(ObDatumRange &real_range, const ObDatumRange &range,
                                          bool is_reverse) const
{
  real_range = range;
  if (is_reverse) {
    real_range.start_key_ = range.get_end_key();
    real_range.end_key_ = range.get_start_key();
    if (range.get_border_flag().inclusive_start()) {
      real_range.border_flag_.set_inclusive_end();
    } else {
      real_range.border_flag_.unset_inclusive_end();
    }
    if (range.get_border_flag().inclusive_end()) {
      real_range.border_flag_.set_inclusive_start();
    } else {
      real_range.border_flag_.unset_inclusive_start();
    }
  }
  return real_range;
}

int ObMemtable::row_compact(ObMvccRow *row,
                            const SCN snapshot_version,
                            const int64_t flag)
{
  int ret = OB_SUCCESS;
  ObMemtableRowCompactor row_compactor;
  if (OB_ISNULL(row)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "row is NULL");
  } else if (OB_FAIL(row_compactor.init(row, this, &local_allocator_))) {
    TRANS_LOG(WARN, "row compactor init error", K(ret));
  } else if (OB_FAIL(row_compactor.compact(snapshot_version, flag))) {
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

void ObMemtable::set_allow_freeze(const bool allow_freeze)
{
  int ret = OB_SUCCESS;
  if (get_allow_freeze_() != allow_freeze) {
    const common::ObTabletID tablet_id = key_.tablet_id_;
    const int64_t retire_clock = local_allocator_.get_retire_clock();
    ObTenantFreezer *freezer = nullptr;
    freezer = MTL(ObTenantFreezer *);

    if (allow_freeze) {
      set_allow_freeze_();
    } else {
      clear_allow_freeze_();
    }

    if (allow_freeze) {
      if (OB_FAIL(freezer->unset_tenant_slow_freeze(tablet_id))) {
        LOG_WARN("unset tenant slow freeze failed.", KPC(this));
      }
    } else {
      if (OB_FAIL(freezer->set_tenant_slow_freeze(tablet_id, retire_clock))) {
        LOG_WARN("set tenant slow freeze failed.", KPC(this));
      }
    }
  }
}

int ObMemtable::get_frozen_schema_version(int64_t &schema_version) const
{
  UNUSED(schema_version);
  return OB_NOT_SUPPORTED;
}

int ObMemtable::set_snapshot_version(const SCN snapshot_version)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not inited", K(ret));
  } else if (snapshot_version.is_max()
             || !snapshot_version.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid args", K(ret), K(snapshot_version));
  } else if (snapshot_version_.is_max()) {
    snapshot_version_ = snapshot_version;
  }
  return ret;
}


bool ObMemtable::ready_for_flush()
{
  int ret = OB_SUCCESS;
  bool bool_ret = ready_for_flush_();

  if (bool_ret) {
    int tmp_ret = OB_SUCCESS;
    // dml stat is periodically reported, so need to report residual stat when freeze finished
    if (OB_TMP_FAIL(report_residual_dml_stat_())) {
      TRANS_LOG_RET(WARN, tmp_ret, "fail to report dml stat", K_(reported_dml_stat));
    }
    local_allocator_.set_frozen();
  }

  return bool_ret;
}

bool ObMemtable::ready_for_flush_()
{
  bool is_frozen = is_frozen_memtable();
  int64_t write_ref_cnt = get_write_ref();
  int64_t unsubmitted_cnt = get_unsubmitted_cnt();
  bool bool_ret = is_frozen && 0 == write_ref_cnt && 0 == unsubmitted_cnt;

  int ret = OB_SUCCESS;
  SCN current_right_boundary = ObScnRange::MIN_SCN;
  share::ObLSID ls_id = get_ls_id();
  if (bool_ret) {
    if (OB_FAIL(resolve_snapshot_version_())) {
      TRANS_LOG(WARN, "fail to resolve snapshot version", K(ret), KPC(this), K(ls_id));
    } else if (OB_FAIL(resolve_max_end_scn_())) {
      TRANS_LOG(WARN, "fail to resolve max_end_scn", K(ret), KPC(this), K(ls_id));
    } else {
      TRANS_LOG(INFO, "[resolve_right_boundary] ready_for_flush_", K(ls_id), KPC(this));
      if (OB_FAIL(get_ls_current_right_boundary_(current_right_boundary))) {
        TRANS_LOG(WARN, "fail to get current right boundary", K(ret));
      }
      if ((bool_ret = (current_right_boundary >= get_max_end_scn()))) {
        resolve_right_boundary();
        if (!get_resolved_active_memtable_left_boundary()) {
          resolve_left_boundary_for_active_memtable_();
        }
        bool_ret = (is_empty() || get_resolved_active_memtable_left_boundary());
      }
      if (bool_ret) {
        set_freeze_state(TabletMemtableFreezeState::READY_FOR_FLUSH);
        if (0 == mt_stat_.ready_for_flush_time_) {
          mt_stat_.ready_for_flush_time_ = ObTimeUtility::current_time();
          freezer_->get_stat().remove_memtable_info(get_tablet_id());
        }
      }

      TRANS_LOG(INFO, "ready for flush", K(bool_ret), K(ret), K(current_right_boundary), K(ls_id), KPC(this));
    }
  } else if (is_frozen && get_logging_blocked()) {
    // ensure unset all frozen memtables'logging_block
    ObTableHandleV2 handle;
    ObITabletMemtable *first_frozen_memtable = nullptr;
    ObTabletMemtableMgr *memtable_mgr = get_memtable_mgr();
    if (OB_ISNULL(memtable_mgr)) {
    } else if (OB_FAIL(memtable_mgr->get_first_frozen_memtable(handle))) {
      TRANS_LOG(WARN, "fail to get first_frozen_memtable", K(ret));
    } else if (OB_FAIL(handle.get_tablet_memtable(first_frozen_memtable))) {
      TRANS_LOG(WARN, "fail to get memtable", K(ret));
    } else if (first_frozen_memtable == this) {
      (void)clear_logging_blocked();
      TRANS_LOG(WARN, "unset logging_block in ready_for_flush", KPC(this));
    }
  }

  if (!bool_ret &&
      (mt_stat_.frozen_time_ != 0 &&
      ObTimeUtility::current_time() - mt_stat_.frozen_time_ > 10 * 1000 * 1000L)) {
    if (ObTimeUtility::current_time() - mt_stat_.last_print_time_ > 10 * 1000) {
      STORAGE_LOG(WARN, "memtable not ready for flush for long time",
                  K(get_ls_id()), K(*this), K(mt_stat_.frozen_time_),
                  K(current_right_boundary));
      mt_stat_.last_print_time_ = ObTimeUtility::current_time();
    }
    freezer_->get_stat().add_memtable_info(get_tablet_id(),
                                           get_start_scn(),
                                           get_end_scn(),
                                           get_write_ref(),
                                           get_unsubmitted_cnt(),
                                           current_right_boundary.get_val_for_tx());

    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(ADD_SUSPECT_INFO(MINI_MERGE, ObDiagnoseTabletType::TYPE_MINI_MERGE,
                    ls_id, get_tablet_id(),
                    ObSuspectInfoType::SUSPECT_NOT_READY_FOR_FLUSH,
                    static_cast<int64_t>(is_frozen_memtable()), get_write_ref(), get_unsubmitted_cnt(),
                    current_right_boundary.get_val_for_tx(), get_end_scn().get_val_for_tx()))) {
      STORAGE_LOG(WARN, "failed to add suspcet info", K(tmp_ret));
    }
  }

  return bool_ret;
}

void ObMemtable::print_ready_for_flush()
{
  int ret = OB_SUCCESS;
  share::ObLSID ls_id = get_ls_id();
  const common::ObTabletID tablet_id = key_.tablet_id_;
  bool frozen_memtable_flag = is_frozen_memtable();
  int64_t write_ref = get_write_ref();
  SCN end_scn = get_end_scn();
  SCN current_right_boundary;
  uint32_t logstream_freeze_clock = freezer_->get_freeze_clock();
  uint32_t memtable_freeze_clock = get_freeze_clock();
  if (OB_FAIL(get_ls_current_right_boundary_(current_right_boundary))) {
    TRANS_LOG(WARN, "fail to get current right boundary", K(ret));
  }
  bool bool_ret = frozen_memtable_flag &&
                  0 == write_ref &&
                  current_right_boundary >= end_scn;

  TRANS_LOG(INFO, "[ObFreezer] print_ready_for_flush",
            KP(this), K(ls_id), K(tablet_id),
            K(ret), K(bool_ret),
            K(frozen_memtable_flag), K(write_ref),
            K(current_right_boundary), K(end_scn),
            K(logstream_freeze_clock), K(memtable_freeze_clock), K_(trace_id));
}

// The freeze_snapshot_version is needed for mini merge, which represents that
// all tx of the previous version has been committed.
// First, it should obey 3 rules:
// 1. It should be picked before the freezer is triggered otherwise there will
//    be tx with smaller version resides in the next memtable
// 2. It should be as large as possible because the choice with tables of the
//    read and the merge is based on the snapshot
// 3. It should be picked before the mini merge otherwise mini merge cannot
//    decide for the snapshot version
// Then, we must consume the freeze_snapshot_version before the freeze finish
// for the code quality. There are 3 cases:
// 1. memtable is created before logstream begin(flag is set), it guaranteed
//    that memtable will be counted in the road_to_flush and be consumed before
//    the freeze finish
// 2. memtable is created after logstream, we donot care it at all
// 3. memtable is created during logstream, there are two actions for both the
//    freeze and the create. The freeze contains 'set_flag' and 'traverse', and
//    the create contains 'read_flag' and 'create'.
//    3.1. If the 'read_flag' is before 'set_flag', the memtable will not be
//         used for freeze, and will not set freeze_snapshot_version until the
//         next freeze
//    3.2. If the 'read_flag' is after 'set_flag' and 'traverse' is after
//         'create', the memtable will be counted in the road_to_flush and be
//         consumed before the freeze finish
//    3.3. If the 'read_flag' is after 'set_flag' and 'create' is after
//         'traverse', the memtable will not be counted in the road_to_flush
//         while may be resolve_for_right_boundary. While it will be counted in
//         the next freeze, so it will not be called ready_for_flush with the
//         need_flush equals ture.
// Overall, we put set_freeze_version in the ready_for_flush with the need_flush
// equals ture.
int ObMemtable::resolve_snapshot_version_()
{
  int ret = OB_SUCCESS;
  SCN freeze_snapshot_version;

  if (snapshot_version_ != SCN::max_scn()) {
    // Pass if snapshot is already set
  } else if (OB_ISNULL(freezer_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "freezer should not be null", K(ret));
  } else if (FALSE_IT(freeze_snapshot_version = freezer_->get_freeze_snapshot_version())) {
    TRANS_LOG(ERROR, "fail to get freeze_snapshot_version", K(ret));
  } else if (SCN::invalid_scn() == freeze_snapshot_version) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "fail to get freeze_snapshot_version", K(ret), KPC(this));
  } else if (is_transfer_freeze()) {
    // freeze_snapshot_version is used for read tables decision which guarantees
    // that all version smaller than the freeze_snapshot_version belongs to
    // table before the memtable. While the transfer want the it to be smaller
    // than the transfer_scn which require we ignore the input snapshot version.
    //
    // NOTICE: While the recommend snapshot may be unsafe, so user must ensure
    // its correctness.
    //
    // So use recommend snapshot version if transfer freeze
    // recommend snapshot maybe smaller than data commit version when transfer rollback,
    // but it will not has any bad effect when major freeze which relay on snapshot version.
    if (!recommend_snapshot_version_.is_valid()
        || ObScnRange::MAX_SCN == recommend_snapshot_version_
        || ObScnRange::MIN_SCN == recommend_snapshot_version_) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "recommend_snapshot_version is invalid", K(ret), KPC(this));
    } else if (OB_FAIL(set_snapshot_version(recommend_snapshot_version_))) {
      TRANS_LOG(ERROR, "fail to set snapshot_version", K(ret));
    } else {
      TRANS_LOG(INFO, "use recommend snapshot version set snapshot_version", K(ret),
                K(recommend_snapshot_version_), KPC(this));
    }
  } else if (OB_FAIL(set_snapshot_version(freeze_snapshot_version))) {
    TRANS_LOG(ERROR, "fail to set snapshot_version", K(ret));
  }

  return ret;
}

// The max_decided log ts is used to push up the end_scn of the memtable
// using the max decided log ts.
// Before the revision, the end_scn of the memtable is the max committed log
// ts of the data on the memtable. So for all 2pc txn and some 1pc txn whose
// data log is seperated with the commit log, the end_scn of the memtable is
// smaller than the commit_scn of the txn. And when the merge happens, the
// txn node will therefore not be cleanout. And the read after merge will be
// very slow due to tx data table lookup.
// So finally we decide to use the max decoded log ts of the ls to update the
// end_scn of the memtable
int ObMemtable::resolve_max_end_scn_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  SCN max_decided_scn;
  bool use_max_decided_scn = false;

  if (OB_ISNULL(freezer_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "freezer should not be null", K(ret));
  } else if (FALSE_IT(max_decided_scn = freezer_->get_max_decided_scn())) {
    TRANS_LOG(ERROR, "fail to get freeze_snapshot_version", K(ret));
  } else if (SCN::invalid_scn() == max_decided_scn) {
    // Pass if not necessary
  } else if (is_transfer_freeze()) {
    // max_decided_scn is critial for sstable read performance using larger
    // right boundary of memtable(You can learn from the comments that follow
    // the class member). While the transfer want the right boundary smaller
    // than the transfer_out_scn which require we ignore the max_decided_scn.
    // NOTICE: You should notice that we must double check the concurrency issue
    // between transfer handler set_transfer_freeze then submit transfer out log
    // and freezer get_transfer_freeze and decide its max_decided scn.
    //
    // So pass if transfer freeze
  } else if (OB_TMP_FAIL(set_max_end_scn(max_decided_scn))) {
    TRANS_LOG(WARN, "fail to set max_end_scn", K(ret));
  }

  return ret;
}


DEF_REPORT_CHEKCPOINT_DIAGNOSE_INFO(UpdateScheduleDagTime, update_schedule_dag_time)
int ObMemtable::flush(share::ObLSID ls_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  int64_t cur_time = ObTimeUtility::current_time();
  if (get_is_flushed()) {
    ret = OB_NO_NEED_UPDATE;
  } else {
    ObTabletMergeDagParam param;
    param.ls_id_ = ls_id;
    param.tablet_id_ = key_.tablet_id_;
    param.merge_type_ = MINI_MERGE;
    param.merge_version_ = ObVersion::MIN_VERSION;
    fill_compaction_param_(cur_time, param);

    if (OB_FAIL(compaction::ObScheduleDagFunc::schedule_tablet_merge_dag(param))) {
      if (OB_EAGAIN != ret && OB_SIZE_OVERFLOW != ret) {
        TRANS_LOG(WARN, "failed to schedule tablet merge dag", K(ret));
      }
    } else {
      mt_stat_.create_flush_dag_time_ = cur_time;
      report_memtable_diagnose_info(UpdateScheduleDagTime());
      TRANS_LOG(INFO, "schedule tablet merge dag successfully", K(ret), K(param), KPC(this));
    }

    if (OB_FAIL(ret) && mt_stat_.create_flush_dag_time_ == 0 &&
        mt_stat_.ready_for_flush_time_ != 0 &&
        cur_time - mt_stat_.ready_for_flush_time_ > 30 * 1000 * 1000) {
      STORAGE_LOG(WARN, "memtable can not create dag successfully for long time",
                K(ls_id), K(*this), K(mt_stat_.ready_for_flush_time_));
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(ADD_SUSPECT_INFO(MINI_MERGE, ObDiagnoseTabletType::TYPE_MINI_MERGE,
                      ls_id, get_tablet_id(),
                      ObSuspectInfoType::SUSPECT_MEMTABLE_CANT_CREATE_DAG,
                      static_cast<int64_t>(ret),
                      cur_time - mt_stat_.ready_for_flush_time_, mt_stat_.ready_for_flush_time_))) {
        STORAGE_LOG(WARN, "failed to add suspect info", K(tmp_ret));
      }
    }
  }

  return ret;
}

void ObMemtable::fill_compaction_param_(
    const int64_t current_time,
    ObTabletMergeDagParam &param)
{
  ObCompactionParam &compaction_param = param.compaction_param_;
  compaction_param.occupy_size_ = get_occupied_size();
  compaction_param.replay_interval_ = get_start_scn().get_val_for_tx() - ls_handle_.get_ls()->get_ls_meta().get_clog_checkpoint_scn().get_val_for_tx();
  compaction_param.last_end_scn_ = get_end_scn();
  compaction_param.add_time_ = current_time;
  compaction_param.estimate_phy_size_ = mt_stat_.row_size_;
}

int ObMemtable::estimate_phy_size(const ObStoreRowkey* start_key, const ObStoreRowkey* end_key, int64_t& total_bytes, int64_t& total_rows)
{
  int ret = OB_SUCCESS;
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
  if (OB_FAIL(start_mtk.encode(start_key)) || OB_FAIL(end_mtk.encode(end_key))) {
    TRANS_LOG(WARN, "encode key fail", K(ret), K_(key));
  } else if (OB_FAIL(query_engine_.estimate_size(&start_mtk, &end_mtk, total_bytes, total_rows))) {
    TRANS_LOG(WARN, "estimate row count fail", K(ret), K_(key));
  }
  return ret;
}

int ObMemtable::get_split_ranges(const ObStoreRange &input_range,
                                 const int64_t part_cnt,
                                 ObIArray<ObStoreRange> &range_array)
{
  int ret = OB_SUCCESS;
  range_array.reuse();
  const ObStoreRowkey *start_key = &input_range.get_start_key();
  const ObStoreRowkey *end_key = &input_range.get_end_key();
  ObMemtableKey start_mtk;
  ObMemtableKey end_mtk;

  if (part_cnt < 1) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "part cnt need be greater than 1", K(ret), K(part_cnt));
  } else if (OB_FAIL(start_mtk.encode(start_key)) || OB_FAIL(end_mtk.encode(end_key))) {
    TRANS_LOG(WARN, "encode key fail", K(ret), K_(key));
  } else if (OB_FAIL(query_engine_.split_range(&start_mtk, &end_mtk, part_cnt, range_array))) {
    TRANS_LOG(WARN, "estimate row count fail", K(ret), K_(key));
  }

  if (OB_ENTRY_NOT_EXIST == ret) {
    // construct a single range if split failed
    ret = OB_SUCCESS;
    ObStoreRange merge_range;
    merge_range.set_start_key(*start_key);
    merge_range.set_end_key(*end_key);
    if (OB_FAIL(range_array.push_back(merge_range))) {
      STORAGE_LOG(WARN, "push back merge range to range array failed", KR(ret), K(merge_range));
    }
  }

  if (OB_SUCC(ret) && !range_array.empty()) {
    // set range closed or open
    ObStoreRange &first_range = range_array.at(0);
    ObStoreRange &last_range = range_array.at(range_array.count() - 1);
    input_range.get_border_flag().inclusive_start() ? first_range.set_left_closed() : first_range.set_left_open();
    input_range.get_border_flag().inclusive_end() ? last_range.set_right_closed() : last_range.set_right_open();
  }
  return ret;
}

// The logic for sampling in the memtable is as follows, as shown in the diagram: We set a constant variable
// SAMPLE_MEMTABLE_RANGE_COUNT, which represents the number of intervals to be read during sampling. Currently, it is
// set to 10. Then, based on the sampling rate, we calculate the total number of ranges to be divided, such that the
// ratio of the data within the chosen ranges to the total data is equal to the sampling rate. In the diagram,
// let's assume a sampling rate of 1%. The entire memtable would be divided into 1000 ranges, and 10 ranges would
// be evenly selected for sampling, including the first and last ranges.
//
// +-------+------------+-------+------------+-------+-----------+-------+-----------+-------+
// |       |            |       |            |       |           |       |           |       |
// |chosen |            |chosen |            |chosen |           |chosen |           |chosen |
// |range 1| .........  |range 3|  ......... |range 5| ......... |range 7| ......... |range10|
// | idx:0 |            |idx:299|            |idx:499|           |idx:699|           |idx:999|
// |       |            |       |            |       |           |       |           |       |
// +-------+------------+-------+------------+-------+-----------+-------+-----------+-------+
// |                                                                                         |
// +<------------------------      all splited ranges in memtable    ----------------------->+
// |                                                                                         |
// +                                                                                         +
int ObMemtable::split_ranges_for_sample(const blocksstable::ObDatumRange &table_scan_range,
                                        const double sample_rate_percentage,
                                        ObIAllocator &allocator,
                                        ObIArray<blocksstable::ObDatumRange> &sample_memtable_ranges)
{
  int ret = OB_SUCCESS;
  if (sample_rate_percentage == 0 || sample_rate_percentage > 100) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "invalid sample_rate_percentage", KR(ret), K(sample_rate_percentage));
  } else {
    // The logic here for calculating the number of split ranges based on the sampling rate might be confusing.
    // For example, assuming our sampling rate is 1%, the variable "sample_rate_percentage" would be 1. At the same
    // time, if we have a total number of intervals to be divided, denoted as "total_split_range_count," with an equal
    // number of rowkeys within each range, we can obtain the equation:
    //
    // SAMPLE_MEMTABLE_RANGE_COUNT / total_split_range_count = sample_rate_percentage / 100.
    //
    int total_split_range_count =
        ObMemtableRowSampleIterator::SAMPLE_MEMTABLE_RANGE_COUNT * 100 / sample_rate_percentage;
    if (total_split_range_count > ObQueryEngine::MAX_RANGE_SPLIT_COUNT) {
      total_split_range_count = ObQueryEngine::MAX_RANGE_SPLIT_COUNT;
    }

    // loop to split range
    bool split_succ = false;
    while (!split_succ && total_split_range_count > ObMemtableRowSampleIterator::SAMPLE_MEMTABLE_RANGE_COUNT) {
      int tmp_ret = OB_SUCCESS;
      sample_memtable_ranges.reuse();
      ObStoreRange input_range;
      input_range.set_start_key(table_scan_range.get_start_key().get_store_rowkey());
      input_range.set_end_key(table_scan_range.get_end_key().get_store_rowkey());
      input_range.is_left_open()  ? input_range.set_left_open()  : input_range.set_left_closed();
      input_range.is_right_open() ? input_range.set_right_open() : input_range.set_right_closed();

      if (OB_TMP_FAIL(
              try_split_range_for_sample_(input_range, total_split_range_count, allocator, sample_memtable_ranges))) {
        total_split_range_count = total_split_range_count / 10;
        TRANS_LOG(WARN,
                  "try split range for sampling failed, shrink split range count and retry",
                  KR(tmp_ret),
                  K(total_split_range_count));

      } else {
        TRANS_LOG(INFO, "split range finish", K(total_split_range_count), K(sample_memtable_ranges));
        split_succ = true;
      }
    }

    // set ret code to ENTRY_NOT_EXIST if split failed
    if (!split_succ) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

int64_t ObMemtable::try_split_range_for_sample_(const ObStoreRange &input_range,
                                                const int64_t range_count,
                                                ObIAllocator &allocator,
                                                ObIArray<blocksstable::ObDatumRange> &sample_memtable_ranges)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObStoreRange, 64> store_range_array;
  if (OB_FAIL(get_split_ranges(input_range, range_count, store_range_array))) {
    TRANS_LOG(WARN, "try split ranges for sample failed", KR(ret));
  } else if (store_range_array.count() != range_count) {
    ret = OB_ENTRY_NOT_EXIST;
    TRANS_LOG(INFO, "memtable row is not enough for splitting", KR(ret), K(range_count), KPC(this));
  } else {
    const int64_t range_count_each_chosen =
        range_count / (ObMemtableRowSampleIterator::SAMPLE_MEMTABLE_RANGE_COUNT - 1);

    // chose some ranges and push back to sample_memtable_ranges
    int64_t chose_range_idx = 0;
    bool generate_datum_range_done = false;
    while (OB_SUCC(ret) && !generate_datum_range_done) {
      if (chose_range_idx >= range_count - 1 ||
          sample_memtable_ranges.count() == ObMemtableRowSampleIterator::SAMPLE_MEMTABLE_RANGE_COUNT - 1) {
        chose_range_idx = range_count - 1;
        generate_datum_range_done = true;
      }

      ObDatumRange datum_range;
      if (OB_FAIL(datum_range.from_range(store_range_array.at(chose_range_idx), allocator))) {
        STORAGE_LOG(WARN,
                    "Failed to transfer store range to datum range",
                    K(ret),
                    K(chose_range_idx),
                    K(store_range_array.at(chose_range_idx)));
      } else if (OB_FAIL(sample_memtable_ranges.push_back(datum_range))) {
        STORAGE_LOG(WARN, "Failed to push back merge range to array", K(ret), K(datum_range));
      } else {
        // chose the next store range
        chose_range_idx += range_count_each_chosen;
      }
    }
  }
  return ret;
}

int ObMemtable::print_stat() const
{
  int ret = OB_SUCCESS;
  TRANS_LOG(INFO, "[memtable stat]", K_(key));
  const char *fname_prefix = "/tmp/stat"; // Stored in /tmp/stat.<table id>.<tstamp>
  char fname[OB_MAX_FILE_NAME_LENGTH];
  FILE *fd = NULL;
  if ((int64_t)sizeof(fname) <= snprintf(fname, sizeof(fname), "%s.%lu.%ld",
                                         fname_prefix, key_.tablet_id_.id(), ObTimeUtility::current_time())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(ERROR, "fname too long", K(fname_prefix));
  } else if (NULL == (fd = fopen(fname, "w"))) {
    ret = OB_IO_ERROR;
    TRANS_LOG(ERROR, "open file fail for memtable stat", K(fname));
  } else {
    fprintf(fd, "[memtable stat] tablet_id:%lu\n", key_.tablet_id_.id());
    query_engine_.dump_keyhash(fd);
    fprintf(fd, "[end]\n");
  }
  if (NULL != fd) {
    fclose(fd);
    fd = NULL;
  }
  return ret;
}

int ObMemtable::check_cleanout(bool &is_all_cleanout,
                               bool &is_all_delay_cleanout,
                               int64_t &count)
{
  int ret = OB_SUCCESS;

  TRANS_LOG(INFO, "check_cleanout", K_(key));

  query_engine_.check_cleanout(is_all_cleanout,
                               is_all_delay_cleanout,
                               count);

  return ret;
}

int ObMemtable::dump2text(const char *fname)
{
  int ret = OB_SUCCESS;
  char real_fname[OB_MAX_FILE_NAME_LENGTH];
  FILE *fd = NULL;

  TRANS_LOG(INFO, "dump2text", K_(key));
  if (OB_ISNULL(fname)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "fanme is NULL");
  } else if (snprintf(real_fname, sizeof(real_fname), "%s.%ld", fname,
                      ::oceanbase::common::ObTimeUtility::current_time()) >= (int64_t)sizeof(real_fname)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "fname too long", K(fname));
  } else if (NULL == (fd = fopen(real_fname, "w"))) {
    ret = OB_IO_ERROR;
    TRANS_LOG(WARN, "open file fail:", K(fname));
  } else {
    fprintf(fd, "memtable: key=%s\n", S(key_));
    fprintf(fd, "hash_item_count=%ld, hash_alloc_size=%ld\n",
            get_hash_item_count(), get_hash_alloc_memory());
    fprintf(fd, "btree_item_count=%ld, btree_alloc_size=%ld\n",
            get_btree_item_count(), get_btree_alloc_memory());
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


void ObMemtable::set_max_data_schema_version(const int64_t schema_version)
{
  if (INT64_MAX == schema_version) {
    TRANS_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "invalid schema version", K(schema_version), KPC(this));
  } else {
    inc_update(&max_data_schema_version_, schema_version);
  }
}

int64_t ObMemtable::get_max_data_schema_version() const
{
  return ATOMIC_LOAD(&max_data_schema_version_);
}

void ObMemtable::set_max_column_cnt(const int64_t column_cnt)
{
  inc_update(&max_column_cnt_, column_cnt);
}

int64_t ObMemtable::get_max_column_cnt() const
{
  return ATOMIC_LOAD(&max_column_cnt_);
}

int ObMemtable::get_schema_info(
    const int64_t input_column_cnt,
    int64_t &max_schema_version_on_memtable,
    int64_t &max_column_cnt_on_memtable) const
{
  int ret = OB_SUCCESS;
  // rows on memtable are not including virtual generated column
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not inited", K(ret));
  } else if (get_max_column_cnt() >= input_column_cnt) {
    TRANS_LOG(INFO, "column cnt or schema version is updated by memtable", KPC(this),
      K(max_column_cnt_on_memtable), K(max_schema_version_on_memtable));
    max_column_cnt_on_memtable = MAX(max_column_cnt_on_memtable, get_max_column_cnt());
    max_schema_version_on_memtable = MAX(max_schema_version_on_memtable, get_max_data_schema_version());
  }
  return ret;
}

uint32_t ObMemtable::get_freeze_flag()
{
  return freezer_->get_freeze_flag();
}

void ObMemtable::set_minor_merged()
{
  minor_merged_time_ = ObTimeUtility::current_time();
}

int ObMemtable::check_standby_cluster_schema_condition_(ObStoreCtx &ctx,
                                                        const int64_t table_id,
                                                        const int64_t table_version)
{
  int ret = OB_SUCCESS;
#ifdef ERRSIM
  ret = OB_E(EventTable::EN_CHECK_STANDBY_CLUSTER_SCHEMA_CONDITION) OB_SUCCESS;
  if (OB_FAIL(ret) && !common::is_inner_table(table_id)) {
    TRANS_LOG(WARN, "ERRSIM, replay row failed", K(ret));
    return ret;
  }
#endif
  if (GCTX.is_standby_cluster()) {
    //only stand_by cluster need to be check
    uint64_t tenant_id = MTL_ID();
    if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "invalid tenant_id", K(ret), K(tenant_id), K(table_id), K(table_version));
    } else if (OB_SYS_TENANT_ID == tenant_id) {
      //sys tenant do not need check
    } else {
      int64_t tenant_schema_version = 0;
      if (OB_FAIL(GSCHEMASERVICE.get_tenant_refreshed_schema_version(tenant_id, tenant_schema_version))) {
        TRANS_LOG(WARN, "get_tenant_schema_version failed", K(ret), K(tenant_id),
                  K(table_id), K(tenant_id), K(table_version));
        if (OB_ENTRY_NOT_EXIST == ret) {
          // tenant schema hasn't been flushed in the case of restart, rewrite OB_ENTRY_NOT_EXIST
          ret = OB_TRANS_WAIT_SCHEMA_REFRESH;
        }
      } else if (table_version > tenant_schema_version) {
        // replay is not allowed when data's table version is greater than tenant's schema version
        //remove by msy164651, in 4.0 no need to check schema version
      } else {/*do nothing*/}
    }
  }
  return ret;
}

int64_t ObMemtable::get_upper_trans_version() const
{
  return INT64_MAX;
}

#ifdef OB_BUILD_TDE_SECURITY
int ObMemtable::save_encrypt_meta(const uint64_t table_id, const share::ObEncryptMeta *encrypt_meta)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(encrypt_meta_lock_);
  if (OB_NOT_NULL(encrypt_meta)) {
    if (OB_ISNULL(encrypt_meta_) &&
        (OB_ISNULL(encrypt_meta_ = (ObTxEncryptMeta *)local_allocator_.alloc(sizeof(ObTxEncryptMeta))) ||
        OB_ISNULL(new(encrypt_meta_) ObTxEncryptMeta()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TRANS_LOG(WARN, "alloc failed", KP(encrypt_meta), K(ret));
    } else {
      ret = encrypt_meta_->store_encrypt_meta(table_id, *encrypt_meta);
    }
  } else {
    //maybe the table is removed from encrypted tablespace
    local_allocator_.free((void *)encrypt_meta_);
    encrypt_meta_ = nullptr;
  }
  return ret;
}

int ObMemtable::get_encrypt_meta(transaction::ObTxEncryptMeta *&encrypt_meta)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(encrypt_meta_lock_);
  if (NULL != encrypt_meta_) {
    if (NULL == encrypt_meta && NULL == (encrypt_meta = op_alloc(ObTxEncryptMeta))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      ret = encrypt_meta->assign(*encrypt_meta_);
    }
  } else {
    if (NULL != encrypt_meta) {
      encrypt_meta->reset();
    }
  }
  return ret;
}

bool ObMemtable::need_for_save(const share::ObEncryptMeta *encrypt_meta)
{
  bool need_save = true;
  SpinRLockGuard guard(encrypt_meta_lock_);
  if (encrypt_meta == NULL && encrypt_meta_ == NULL) {
    need_save = false;
  } else if (encrypt_meta != NULL && encrypt_meta_ != NULL &&
             encrypt_meta_->is_memtable_equal(*encrypt_meta)) {
    need_save = false;
  }
  return need_save;
}
#endif

int ObMemtable::multi_set_(
    const storage::ObTableIterParam &param,
    const common::ObIArray<share::schema::ObColDesc> &columns,
    const storage::ObStoreRow *rows,
    const int64_t row_count,
    const bool check_exist,
    const ObMemtableKeyGenerator &memtable_keys,
    storage::ObTableAccessContext &context,
    storage::ObRowsInfo &rows_info)
{
  int ret = OB_SUCCESS;
  ObStoreCtx &ctx = *(context.store_ctx_);
  int64_t conflict_idx = -1;
  int64_t row_size_stat = 0;
  ObMvccRowAndWriteResults mvcc_rows;
  if (OB_FAIL(mvcc_rows.prepare_allocate(row_count))) {
    TRANS_LOG(WARN, "Failed to prepare allocate mvcc rows", K(ret), K(row_count));
  }

  // 1. Check write conflict in memtables.
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < row_count; ++i) {
      const uint32_t permutation_idx = rows_info.get_permutation_idx(i);
      if (OB_FAIL(set_(param,
                       columns,
                       rows[i],
                       nullptr, /*old_row*/
                       nullptr, /*update_idx*/
                       memtable_keys[i],
                       check_exist,
                       context,
                       &(mvcc_rows[permutation_idx])))) {
        if (OB_UNLIKELY(OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret)) {
          TRANS_LOG(WARN, "Failed to insert new row", K(ret), K(i), K(permutation_idx), K(rows[i]));
        }
        rows_info.set_conflict_rowkey(permutation_idx);
      } else {
        row_size_stat += mvcc_rows[permutation_idx].write_result_.tx_node_->get_data_size();
      }
    }
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0 ; i < row_count; ++i) {
      ObMvccRowAndWriteResult &result = mvcc_rows[i];
      if (result.write_result_.lock_state_.row_exist_decided()) {
        rows_info.set_row_checked(i);
      } else if (result.mvcc_row_->is_lower_lock_scaned()) {
        rows_info.set_row_lock_checked(i, check_exist);
      }
      TRANS_LOG(DEBUG, "set row lock state", K(result), K(i), K(rows_info));
      rows_info.set_row_lock_state(i, &(result.write_result_.lock_state_));
    }
  }

  // 2. Check uniqueness constraint and write conflict in sstables.
  if (OB_FAIL(ret)) {
  } else if (rows_info.all_rows_found()) {
  } else if (OB_FAIL(lock_rows_on_frozen_stores_(check_exist, param, memtable_keys, context, mvcc_rows, rows_info))) {
    TRANS_LOG(WARN, "Failed to lock rows on frozen stores", K(ret));
  } else if (rows_info.have_conflict()) {
    conflict_idx = rows_info.get_conflict_idx();
    blocksstable::ObDatumRowkey& rowkey = rows_info.get_conflict_rowkey();
    ObMemtableKey mtk;
    if (OB_FAIL(mtk.encode(&rowkey.store_rowkey_))) {
      TRANS_LOG(WARN, "Failed to encode memory table key", K(ret));
    } else {
      ret = rows_info.get_error_code();
      lock_row_on_frozen_stores_on_failure(blocksstable::ObDmlFlag::DF_INSERT,
                                           mtk,
                                           ret,
                                           mvcc_rows[conflict_idx].mvcc_row_,
                                           context,
                                           mvcc_rows[conflict_idx].write_result_);
    }
  }

  if (OB_TRANSACTION_SET_VIOLATION == ret) {
    ObTxIsolationLevel iso = ctx.mvcc_acc_ctx_.tx_desc_->get_isolation_level();
    if (ObTxIsolationLevel::SERIAL == iso || ObTxIsolationLevel::RR == iso) {
      ret = OB_TRANS_CANNOT_SERIALIZE;
    }
  }

  return ret;
}

int ObMemtable::set_(
    const storage::ObTableIterParam &param,
    const common::ObIArray<share::schema::ObColDesc> &columns,
    const storage::ObStoreRow &new_row,
    const storage::ObStoreRow *old_row,
    const common::ObIArray<int64_t> *update_idx,
    const ObMemtableKey &mtk,
    const bool check_exist,
    storage::ObTableAccessContext &context,
    ObMvccRowAndWriteResult *mvcc_row)
{
  int ret = OB_SUCCESS;
  blocksstable::ObRowWriter row_writer;
  char *buf = nullptr;
  int64_t len = 0;
  ObRowData old_row_data;
  ObStoreCtx &ctx = *(context.store_ctx_);
  ObMemtableCtx *mem_ctx = ctx.mvcc_acc_ctx_.get_mem_ctx();

  //set_begin(ctx.mvcc_acc_ctx_);

  if (nullptr != old_row) {
    char *new_buf = nullptr;
    if(OB_FAIL(row_writer.write(param.get_schema_rowkey_count(), *old_row, nullptr, buf, len))) {
      TRANS_LOG(WARN, "Failed to write old row", K(ret), KPC(old_row));
    } else if (OB_ISNULL(new_buf = (char *)mem_ctx->old_row_alloc(len))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TRANS_LOG(WARN, "alloc row_data fail", K(len));
    } else {
      MEMCPY(new_buf, buf, len);
      old_row_data.set(new_buf, len);
      // for elr optimization
      ctx.mvcc_acc_ctx_.get_mem_ctx()->set_row_updated();
    }
  }
  if (OB_SUCC(ret)) {
    bool is_new_locked = false;
    row_writer.reset();
    if (OB_FAIL(ret)) {
      //do nothing
    } else if (OB_FAIL(row_writer.write(param.get_schema_rowkey_count(), new_row, update_idx, buf, len))) {
      TRANS_LOG(WARN, "Failed to write new row", K(ret), K(new_row));
    } else if (OB_UNLIKELY(new_row.flag_.is_not_exist())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "Unexpected not exist trans node", K(ret), K(new_row));
    } else {
      ObMemtableData mtd(new_row.flag_.get_dml_flag(), len, buf);
      ObTxNodeArg arg(
          ctx.mvcc_acc_ctx_.tx_id_, /*trans id*/
          &mtd,        /*memtable_data*/
          NULL == old_row ? NULL : &old_row_data,
          init_timestamp_,  /*memstore_version*/
          ctx.mvcc_acc_ctx_.tx_scn_,  /*seq_no*/
          new_row.row_val_.count_ /*column_cnt*/);
      if (OB_FAIL(mvcc_write_(param,
                              context,
                              &mtk,
                              arg,
                              check_exist,
                              is_new_locked,
                              mvcc_row))) {
        if (OB_TRY_LOCK_ROW_CONFLICT != ret &&
            OB_TRANSACTION_SET_VIOLATION != ret &&
            OB_ERR_PRIMARY_KEY_DUPLICATE != ret) {
          TRANS_LOG(WARN, "mvcc write fail", K(mtk), K(ret));
        }
      } else {
        TRANS_LOG(DEBUG, "set end, success",
                  "ret", ret,
                  "tablet_id_", key_.tablet_id_,
                  "dml_flag", new_row.flag_.get_dml_flag(),
                  "columns", strarray<ObColDesc>(columns),
                  "old_row", to_cstring(old_row),
                  "new_row", to_cstring(new_row),
                  "update_idx", (update_idx == NULL ? "" : to_cstring(update_idx)),
                  "mtd", to_cstring(mtd),
                  K(arg),
                  KPC(mvcc_row));
      }
    }
  }

  // release memory applied for old row
  if (OB_FAIL(ret)
      && NULL != old_row
      && old_row_data.size_ > 0
      && NULL != old_row_data.data_) {
    mem_ctx->old_row_free((void *)(old_row_data.data_));
    old_row_data.reset();
  }

  if (OB_FAIL(ret) &&
      OB_TRY_LOCK_ROW_CONFLICT != ret &&
      OB_TRANSACTION_SET_VIOLATION != ret &&
      OB_ERR_PRIMARY_KEY_DUPLICATE != ret) {
    TRANS_LOG(WARN, "set end, fail",
        "ret", ret,
        "tablet_id_", key_.tablet_id_,
        "columns", strarray<ObColDesc>(columns),
        "new_row", to_cstring(new_row),
        "mem_ctx", STR_PTR(mem_ctx),
        "store_ctx", ctx);
  }

  //set_end(ctx.mvcc_acc_ctx_, ret);
  if (OB_SUCC(ret)) {
    set_max_data_schema_version(ctx.table_version_);
    set_max_column_cnt(new_row.row_val_.count_);
  }

  return ret;
}

int ObMemtable::lock_(
    const storage::ObTableIterParam &param,
    storage::ObTableAccessContext &context,
    const common::ObStoreRowkey &rowkey,
    const ObMemtableKey &mtk)
{
  int ret = OB_SUCCESS;
  bool is_new_locked = false;
  blocksstable::ObRowWriter row_writer;
  char *buf = NULL;
  int64_t len = 0;

  if (OB_FAIL(row_writer.write_rowkey(rowkey, buf, len))) {
    TRANS_LOG(WARN, "Failed to writer rowkey", K(ret), K(rowkey));
  } else {
    // for elr optimization
    ObMvccAccessCtx &acc_ctx = context.store_ctx_->mvcc_acc_ctx_;
    acc_ctx.get_mem_ctx()->set_row_updated();
    ObMemtableData mtd(blocksstable::ObDmlFlag::DF_LOCK, len, buf);
    ObTxNodeArg arg(acc_ctx.tx_id_,          /*trans id*/
                    &mtd,                    /*memtable_data*/
                    NULL,                    /*old_data*/
                    init_timestamp_,              /*memstore_version*/
                    acc_ctx.tx_scn_,         /*seq_no*/
                    rowkey.get_obj_cnt());   /*column_cnt*/
    if (OB_FAIL(mvcc_write_(param,
                            context,
                            &mtk,
                            arg,
                            false, /*check_exist*/
                            is_new_locked,
                            nullptr /*mvcc_row*/))) {
    } else if (OB_UNLIKELY(!is_new_locked)) {
      TRANS_LOG(DEBUG, "lock twice, no need to store lock trans node");
    }
  }

  return ret;
}

int ObMemtable::mvcc_replay_(storage::ObStoreCtx &ctx,
                             const ObMemtableKey *key,
                             const ObTxNodeArg &arg)
{
  int ret = OB_SUCCESS;
  ObMemtableKey stored_key;
  ObMvccRow *value = NULL;
  bool is_new_add = false;
  ObMemtableCtx *mem_ctx = ctx.mvcc_acc_ctx_.get_mem_ctx();
  ObMvccReplayResult res;
  common::ObTimeGuard timeguard("ObMemtable::mvcc_replay_", 5 * 1000);

  if (OB_FAIL(mvcc_engine_.create_kv(key,
                                     false, // is_insert
                                     &stored_key,
                                     value,
                                     is_new_add))) {
    TRANS_LOG(WARN, "prepare kv before lock fail", K(ret));
  } else if (FALSE_IT(timeguard.click("mvcc_engine_.create_kv"))) {
  } else if (OB_FAIL(mvcc_engine_.mvcc_replay(arg, res))) {
    TRANS_LOG(WARN, "mvcc replay fail", K(ret));
  } else if (FALSE_IT(timeguard.click("mvcc_engine_.mvcc_replay"))) {
  } else if (OB_FAIL(mvcc_engine_.ensure_kv(&stored_key, value))) {
    TRANS_LOG(WARN, "prepare kv after lock fail", K(ret));
  } else if (FALSE_IT(timeguard.click("mvcc_engine_.ensure_kv"))) {
  } else if (OB_FAIL(mem_ctx->register_row_replay_cb(&stored_key,
                                                     value,
                                                     res.tx_node_,
                                                     arg.data_->dup_size(),
                                                     this,
                                                     arg.seq_no_,
                                                     arg.scn_,
                                                     arg.column_cnt_))) {
    TRANS_LOG(WARN, "register_row_replay_cb fail", K(ret));
  } else if (FALSE_IT(timeguard.click("register_row_replay_cb"))) {
  }

  return ret;
}

int ObMemtable::mvcc_write_(
    const storage::ObTableIterParam &param,
    storage::ObTableAccessContext &context,
    const ObMemtableKey *key,
    const ObTxNodeArg &arg,
    const bool check_exist,
    bool &is_new_locked,
    ObMvccRowAndWriteResult *mvcc_row)
{
  int ret = OB_SUCCESS;
  bool is_new_add = false;
  ObMemtableKey stored_key;
  ObMvccRow *value = NULL;
  ObMvccWriteResult res;
  ObStoreCtx &ctx = *(context.store_ctx_);
  ObMemtableCtx *mem_ctx = ctx.mvcc_acc_ctx_.get_mem_ctx();
  SCN snapshot_version = ctx.mvcc_acc_ctx_.get_snapshot_version();
  transaction::ObTxSnapshot &snapshot = ctx.mvcc_acc_ctx_.snapshot_;

  if (OB_FAIL(mvcc_engine_.create_kv(key,
                                     // is_insert
                                     blocksstable::ObDmlFlag::DF_INSERT == arg.data_->dml_flag_,
                                     &stored_key,
                                     value,
                                     is_new_add))) {
    TRANS_LOG(WARN, "create kv failed", K(ret), K(arg), K(*key));
  } else if (OB_FAIL(mvcc_engine_.mvcc_write(ctx,
                                             snapshot,
                                             *value,
                                             arg,
                                             res))) {
    if (OB_TRY_LOCK_ROW_CONFLICT == ret) {
      ret = post_row_write_conflict_(ctx.mvcc_acc_ctx_,
                                     *key,
                                     res.lock_state_,
                                     value->get_last_compact_cnt(),
                                     value->get_total_trans_node_cnt());
    } else if (OB_TRANSACTION_SET_VIOLATION == ret) {
      mem_ctx->on_tsc_retry(*key,
                            snapshot_version,
                            value->get_max_trans_version(),
                            value->get_max_trans_id());
    } else if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
      mem_ctx->on_key_duplication_retry(*key);
    } else {
      TRANS_LOG(WARN, "mvcc write fail", K(ret));
    }
  } else if (nullptr == mvcc_row && OB_FAIL(lock_row_on_frozen_stores_(param,
                                                                       arg,
                                                                       key,
                                                                       check_exist,
                                                                       context,
                                                                       value,
                                                                       res))) {
    lock_row_on_frozen_stores_on_failure(arg.data_->dml_flag_,
                                         *key,
                                         ret,
                                         value,
                                         context,
                                         res);
    if (res.has_insert()) {
      (void)mvcc_engine_.mvcc_undo(value);
      res.is_mvcc_undo_ = true;
    }
  } else if (OB_FAIL(mvcc_engine_.ensure_kv(&stored_key, value))) {
    if (res.has_insert()) {
      (void)mvcc_engine_.mvcc_undo(value);
      res.is_mvcc_undo_ = true;
    }
    TRANS_LOG(WARN, "prepare kv after lock fail", K(ret));
  } else if (res.has_insert()
             && OB_FAIL(mem_ctx->register_row_commit_cb(&stored_key,
                                                        value,
                                                        res.tx_node_,
                                                        arg.data_->dup_size(),
                                                        arg.old_row_,
                                                        this,
                                                        arg.seq_no_,
                                                        arg.column_cnt_,
                                                        param.is_non_unique_local_index_))) {
    (void)mvcc_engine_.mvcc_undo(value);
    res.is_mvcc_undo_ = true;
    TRANS_LOG(WARN, "register row commit failed", K(ret));
  }

  // cannot be serializable when transaction set violation
  if (OB_TRANSACTION_SET_VIOLATION == ret) {
    ObTxIsolationLevel iso = ctx.mvcc_acc_ctx_.tx_desc_->get_isolation_level();
    if (ObTxIsolationLevel::SERIAL == iso || ObTxIsolationLevel::RR == iso) {
      ret = OB_TRANS_CANNOT_SERIALIZE;
    }
  }

  if (OB_SUCC(ret) && mvcc_row) {
    mvcc_row->mvcc_row_ = value;
    mvcc_row->write_result_ = res;
  }

  if (OB_FAIL(ret) || NULL == res.tx_node_ || !res.has_insert()) {
  } else {
    const blocksstable::ObDmlFlag &dml_flag = res.tx_node_->get_dml_flag();
    mt_stat_.row_size_ += res.tx_node_->get_data_size();
    if (blocksstable::ObDmlFlag::DF_INSERT == dml_flag) {
      ++mt_stat_.insert_row_count_;
    } else if (blocksstable::ObDmlFlag::DF_UPDATE == dml_flag) {
      ++mt_stat_.update_row_count_;
    } else if (blocksstable::ObDmlFlag::DF_DELETE == dml_flag) {
      ++mt_stat_.delete_row_count_;
    }
  }
  return ret;
}

int ObMemtable::post_row_write_conflict_(ObMvccAccessCtx &acc_ctx,
                                         const ObMemtableKey &row_key,
                                         ObStoreRowLockState &lock_state,
                                         const int64_t last_compact_cnt,
                                         const int64_t total_trans_node_cnt)
{
  int ret = OB_TRY_LOCK_ROW_CONFLICT;
  ObLockWaitMgr *lock_wait_mgr = NULL;
  ObTransID conflict_tx_id = lock_state.lock_trans_id_;
  ObMemtableCtx *mem_ctx = acc_ctx.get_mem_ctx();
  int64_t current_ts = common::ObClockGenerator::getClock();
  int64_t lock_wait_start_ts = mem_ctx->get_lock_wait_start_ts() > 0
    ? mem_ctx->get_lock_wait_start_ts()
    : current_ts;
  int64_t lock_wait_expire_ts = acc_ctx.eval_lock_expire_ts(lock_wait_start_ts);
  if (current_ts >= lock_wait_expire_ts) {
    ret = OB_ERR_EXCLUSIVE_LOCK_CONFLICT;
    TRANS_LOG(WARN, "exclusive lock conflict", K(ret), K(row_key),
              K(conflict_tx_id), K(acc_ctx), K(lock_wait_expire_ts));
  } else if (OB_ISNULL(lock_wait_mgr = MTL_WITH_CHECK_TENANT(ObLockWaitMgr*,
                                                  mem_ctx->get_tenant_id()))) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "can not get tenant lock_wait_mgr MTL", K(mem_ctx->get_tenant_id()));
  } else {
    mem_ctx->add_conflict_trans_id(conflict_tx_id);
    mem_ctx->on_wlock_retry(row_key, conflict_tx_id);
    int tmp_ret = OB_SUCCESS;
    transaction::ObPartTransCtx *tx_ctx = acc_ctx.tx_ctx_;
    transaction::ObTransID tx_id = acc_ctx.get_tx_id();
    bool remote_tx = tx_ctx->get_scheduler() != tx_ctx->get_addr();
    ObFunction<int(bool&, bool&)> recheck_func([&](bool &locked, bool &wait_on_row) -> int {
      int ret = OB_SUCCESS;
      lock_state.is_locked_ = false;
      storage::ObRowState row_state;
      if (lock_state.is_delayed_cleanout_) {
        transaction::ObTxSEQ lock_data_sequence = lock_state.lock_data_sequence_;
        storage::ObTxTableGuards &tx_table_guards = acc_ctx.get_tx_table_guards();
        if (OB_FAIL(tx_table_guards.check_row_locked(
                tx_id, conflict_tx_id, lock_data_sequence, lock_state.trans_scn_, lock_state))) {
          TRANS_LOG(WARN, "re-check row locked via tx_table fail", K(ret), K(tx_id), K(lock_state));
        }
      } else {
        if (OB_FAIL(lock_state.mvcc_row_->check_row_locked(acc_ctx, lock_state, row_state))) {
          TRANS_LOG(WARN, "re-check row locked via mvcc_row fail", K(ret), K(tx_id), K(lock_state));
        }
      }
      if (OB_SUCC(ret)) {
        locked = lock_state.is_locked_ && lock_state.lock_trans_id_ != tx_id;
        wait_on_row = !lock_state.is_delayed_cleanout_;
      }
      return ret;
    });
    tmp_ret = lock_wait_mgr->post_lock(OB_TRY_LOCK_ROW_CONFLICT,
                                       key_.get_tablet_id(),
                                       *row_key.get_rowkey(),
                                       lock_wait_expire_ts,
                                       remote_tx,
                                       last_compact_cnt,
                                       total_trans_node_cnt,
                                       acc_ctx.tx_desc_->get_assoc_session_id(),
                                       tx_id,
                                       conflict_tx_id,
                                       get_ls_id(),
                                       recheck_func);
    if (OB_SUCCESS != tmp_ret) {
      TRANS_LOG(WARN, "post_lock after tx conflict failed",
                K(tmp_ret), K(tx_id), K(conflict_tx_id));
    } else if (mem_ctx->get_lock_wait_start_ts() <= 0) {
      mem_ctx->set_lock_wait_start_ts(lock_wait_start_ts);
    }
  }
  return ret;
}

int ObMemtable::get_tx_table_guard(ObTxTableGuard &tx_table_guard)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!ls_handle_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "ls_handle is invalid", K(ret));
  } else if (OB_FAIL(ls_handle_.get_ls()->get_tx_table_guard(tx_table_guard))) {
    TRANS_LOG(WARN, "Get tx table guard from ls failed.", KR(ret));
  }

  return ret;
}

bool ObMemtable::rec_scn_is_stable()
{
  int ret = OB_SUCCESS;
  bool rec_scn_is_stable = false;
  if (SCN::max_scn() == get_rec_scn()) {
    rec_scn_is_stable = (is_frozen_memtable() && get_write_ref() == 0 && get_unsubmitted_cnt() == 0);
  } else {
    SCN max_consequent_callbacked_scn;
    if (OB_FAIL(freezer_->get_max_consequent_callbacked_scn(max_consequent_callbacked_scn))) {
      STORAGE_LOG(WARN, "get_max_consequent_callbacked_scn failed", K(ret), K(get_ls_id()));
    } else {
      rec_scn_is_stable = (max_consequent_callbacked_scn >= get_rec_scn());
    }

    if (!rec_scn_is_stable &&
        (mt_stat_.frozen_time_ != 0 &&
        ObTimeUtility::current_time() - mt_stat_.frozen_time_ > 10 * 1000 * 1000L)) {
      STORAGE_LOG(WARN, "memtable rec_scn not stable for long time",
                  K(get_ls_id()), K(*this), K(mt_stat_.frozen_time_),
                  K(max_consequent_callbacked_scn));

      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(ADD_SUSPECT_INFO(compaction::ObMergeType::MINI_MERGE,
                                       ObDiagnoseTabletType::TYPE_MINI_MERGE,
                                       get_ls_id(),
                                       get_tablet_id(),
                                       ObSuspectInfoType::SUSPECT_REC_SCN_NOT_STABLE,
                                       get_rec_scn().get_val_for_tx(),
                                       max_consequent_callbacked_scn.get_val_for_tx()))) {
        STORAGE_LOG(WARN, "failed to add suspect info", K(tmp_ret));
      }
    }
  }
  return rec_scn_is_stable;
}

int64_t ObMemtable::dec_write_ref()
{
  int ret = OB_SUCCESS;
  share::ObLSID ls_id = get_ls_id();

  // fix issue 47021079
  // To avoid the following case where logging_block cannot be unset:
  // -----------------------------------------------------
  // dec_write_ref()             dec_unsubmitted_cnt()
  // -----------------------------------------------------
  // is_frozen							     is_frozen
  //                             get write_ref_cnt 1
  // dec write_ref to 0
  // get unsubmitted_cnt 1
  //                             dec unsubmitted_cnt to 0
  // -----------------------------------------------------
  int64_t old_write_ref_cnt = dec_write_ref_();

  // must maintain the order of getting variables to avoid concurrency problems
  // is_frozen_memtable() can affect wirte_ref_cnt
  // write_ref_cnt can affect unsubmitted_cnt
  bool is_frozen = is_frozen_memtable();
  int64_t new_write_ref_cnt = get_write_ref();
  int64_t unsubmitted_cnt = get_unsubmitted_cnt();
  if (is_frozen &&
      0 == new_write_ref_cnt &&
      0 == unsubmitted_cnt) {
    (void)unset_logging_blocked_for_active_memtable_();
    share::SCN right_boundary;
    if (OB_FAIL(get_ls_current_right_boundary_(right_boundary))) {
      TRANS_LOG(WARN, "get ls right bound fail", K(ret), K(ls_id), KPC(this));
    } else if (right_boundary >= get_max_end_scn()) {
      resolve_right_boundary();
      if (OB_LIKELY(!get_resolved_active_memtable_left_boundary())) {
        resolve_left_boundary_for_active_memtable_();
      }
    }
  }

  return old_write_ref_cnt;
}

int ObMemtable::try_report_dml_stat_(const int64_t table_id)
{
  int ret = OB_SUCCESS;
  const int64_t current_ts = common::ObClockGenerator::getClock();
  reported_dml_stat_.table_id_ = table_id;  // record the table id for reporting residual dml stat
  if (current_ts - reported_dml_stat_.last_report_time_ > ObReportedDmlStat::REPORT_INTERVAL) {
    if (ATOMIC_BCAS(&reported_dml_stat_.is_reporting_, false, true)) {
      // double check
      if (current_ts - reported_dml_stat_.last_report_time_ > ObReportedDmlStat::REPORT_INTERVAL) {
        ObOptDmlStat dml_stat;
        dml_stat.tenant_id_ = MTL_ID();
        dml_stat.table_id_ = table_id;
        dml_stat.tablet_id_ = get_tablet_id().id();
        const int64_t current_insert_row_cnt = mt_stat_.insert_row_count_;
        const int64_t current_update_row_cnt = mt_stat_.update_row_count_;
        const int64_t current_delete_row_cnt = mt_stat_.delete_row_count_;
        dml_stat.insert_row_count_ = current_insert_row_cnt - reported_dml_stat_.insert_row_count_;
        dml_stat.update_row_count_ = current_update_row_cnt - reported_dml_stat_.update_row_count_;
        dml_stat.delete_row_count_ = current_delete_row_cnt - reported_dml_stat_.delete_row_count_;
        if (OB_FAIL(MTL(ObOptStatMonitorManager*)->update_local_cache(dml_stat))) {
          TRANS_LOG(WARN, "failed to update local cache", K(ret), K(dml_stat));
        } else {
          reported_dml_stat_.insert_row_count_ = current_insert_row_cnt;
          reported_dml_stat_.update_row_count_ = current_update_row_cnt;
          reported_dml_stat_.delete_row_count_ = current_delete_row_cnt;
          reported_dml_stat_.last_report_time_ = current_ts;
        }
      }
      ATOMIC_STORE(&reported_dml_stat_.is_reporting_, false);
    }
  }
  return ret;
}

int ObMemtable::finish_freeze()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObFreezeCheckpoint::finish_freeze())) {
    TRANS_LOG(WARN, "fail to finish_freeze", KR(ret));
  } else {
    report_memtable_diagnose_info(TabletMemtableUpdateFreezeInfo(*this));
  }
  return ret;
}

bool ObMemtable::is_frozen_memtable()
{
  // Note (yanyuan.cxf) log_frozen_memstore_info() will use this func after local_allocator_ init
  // Now freezer_ and ls_ will not be released before memtable
  const uint32_t logstream_freeze_clock = OB_NOT_NULL(freezer_) ? freezer_->get_freeze_clock() : 0;
  const uint32_t memtable_freeze_clock = get_freeze_clock();
  if (!allow_freeze() && logstream_freeze_clock > memtable_freeze_clock) {
    ATOMIC_STORE(&freeze_clock_, logstream_freeze_clock);
    TRANS_LOG(INFO,
              "inc freeze_clock because the memtable cannot be freezed",
              K(memtable_freeze_clock),
              K(logstream_freeze_clock),
              KPC(this));
  }
  const bool bool_ret = logstream_freeze_clock > get_freeze_clock() || get_is_tablet_freeze();

  if (bool_ret && 0 == get_frozen_time()) {
    set_frozen_time(ObClockGenerator::getClock());
  }

  return bool_ret;
}

int ObMemtable::report_residual_dml_stat_()
{
  int ret = OB_SUCCESS;
  if (reported_dml_stat_.table_id_ != OB_INVALID_ID) {
    if (mt_stat_.insert_row_count_ > reported_dml_stat_.insert_row_count_ ||
        mt_stat_.update_row_count_ > reported_dml_stat_.update_row_count_ ||
        mt_stat_.delete_row_count_ > reported_dml_stat_.delete_row_count_) {
      ObOptDmlStat dml_stat;
      dml_stat.tenant_id_ = MTL_ID();
      dml_stat.table_id_ = reported_dml_stat_.table_id_;
      dml_stat.tablet_id_ = get_tablet_id().id();
      dml_stat.insert_row_count_ = mt_stat_.insert_row_count_ - reported_dml_stat_.insert_row_count_;
      dml_stat.update_row_count_ = mt_stat_.update_row_count_ - reported_dml_stat_.update_row_count_;
      dml_stat.delete_row_count_ = mt_stat_.delete_row_count_ - reported_dml_stat_.delete_row_count_;
      if (OB_FAIL(MTL(ObOptStatMonitorManager*)->update_local_cache(dml_stat))) {
        TRANS_LOG(WARN, "failed to update local cache", K(ret), K(dml_stat), K(reported_dml_stat_));
      } else {
        reported_dml_stat_.insert_row_count_ = mt_stat_.insert_row_count_;
        reported_dml_stat_.update_row_count_ =  mt_stat_.update_row_count_;
        reported_dml_stat_.delete_row_count_ = mt_stat_.delete_row_count_;
      }
    }
  }
  return ret;
}

} // namespace memtable
} // namespace ocenabase
