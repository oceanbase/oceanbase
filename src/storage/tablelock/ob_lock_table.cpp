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

#define USING_LOG_PREFIX TABLELOCK

#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_trans_define_v4.h"
#include "storage/tablelock/ob_table_lock_common.h"
#include "storage/tablelock/ob_lock_table.h"
#include "storage/tablelock/ob_table_lock_service.h"

#include "common/ob_tablet_id.h"               // ObTabletID
#include "share/ob_rpc_struct.h"               // ObBatchCreateTabletArg
#include "storage/ls/ob_ls.h"                  // ObLS
#include "storage/tablelock/ob_table_lock_rpc_struct.h"
#include "share/schema/ob_table_schema.h"
#include "storage/access/ob_store_row_iterator.h"
#include "storage/tablelock/ob_table_lock_iterator.h"
#include "storage/tablelock/ob_lock_memtable.h"
#include "storage/tablelock/ob_obj_lock.h"
#include "storage/tablet/ob_tablet.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace share;
using namespace memtable;

namespace transaction
{
namespace tablelock
{

int ObLockTable::restore_lock_table_(ObITable &sstable)
{
  LOG_INFO("ObLockTable::restore_lock_table", K(sstable));

  int ret = OB_SUCCESS;
  ObStoreRowIterator *row_iter = nullptr;
  const ObDatumRow *row = nullptr;

  ObArenaAllocator allocator;
  blocksstable::ObDatumRange whole_range;
  whole_range.set_whole_range();

  ObStoreCtx store_ctx;
  ObTableAccessContext access_context;

  common::ObQueryFlag query_flag;
  query_flag.use_row_cache_ = ObQueryFlag::DoNotUseCache;

  common::ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 0;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.snapshot_version_ = MERGE_READ_SNAPSHOT_VERSION;


  common::ObSEArray<share::schema::ObColDesc, 2> columns;
  ObTableReadInfo read_info;
  share::schema::ObColDesc key;
  key.col_id_ = OB_APP_MIN_COLUMN_ID;
  key.col_type_.set_int();
  key.col_order_ = ObOrderType::ASC;

  share::schema::ObColDesc value;
  value.col_id_ = OB_APP_MIN_COLUMN_ID + 1;
  value.col_type_.set_binary();

  ObTableIterParam iter_param;
  iter_param.table_id_ = ObTabletID::LS_LOCK_TABLET_ID;
  iter_param.tablet_id_ = LS_LOCK_TABLET;

  ObTableHandleV2 handle;
  ObLockMemtable *memtable = nullptr;

  if (OB_FAIL(access_context.init(query_flag,
                                  store_ctx,
                                  allocator,
                                  trans_version_range))) {
    LOG_WARN("failed to init access context", K(ret));
  } else if (OB_FAIL(columns.push_back(key))) {
    LOG_WARN("failed to push back key", K(ret), K(key));
  } else if (OB_FAIL(columns.push_back(value))) {
    LOG_WARN("failed to push back value", K(ret), K(value));
  } else if (OB_FAIL(read_info.init(allocator, LOCKTABLE_SCHEMA_COLUMN_CNT, LOCKTABLE_SCHEMA_ROEKEY_CNT, lib::is_oracle_mode(), columns, nullptr/*storage_cols_index*/))) {
    LOG_WARN("Fail to init read_info", K(ret));
  } else if (FALSE_IT(iter_param.read_info_ = &read_info)) {
  } else if (OB_FAIL(sstable.scan(iter_param,
                                    access_context,
                                    whole_range,
                                    row_iter))) {
    LOG_WARN("failed to scan trans table", K(ret));
  } else if (NULL == row_iter) {
    LOG_INFO("NULL == row_ite, do nothing");
  } else if (OB_FAIL(get_lock_memtable(handle))) {
    LOG_WARN("get_lock_memtable_handle fail.", KR(ret));
  } else if (OB_FAIL(handle.get_lock_memtable(memtable))) {
    LOG_WARN("get_lock_memtable_ fail.", KR(ret));
  } else {
    memtable->set_flushed_scn(sstable.get_end_scn());
    while (OB_SUCC(ret)) {
      if (OB_FAIL(row_iter->get_next_row(row))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next row", K(ret));
        }
      } else if (OB_FAIL(recover_(*row))) {
        LOG_WARN("failed to recover table lock", K(ret));
      }
    }

    if (OB_ITER_END == ret) {
      LOG_INFO("reload lock table in memory OK", KR(ret), K(sstable));
      ret = OB_SUCCESS;
    }
  }

  if (OB_NOT_NULL(row_iter)) {
    row_iter->~ObStoreRowIterator();
    row_iter = nullptr;
  }

  return ret;
}

int ObLockTable::recover_(const blocksstable::ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t idx = row.storage_datums_[TABLE_LOCK_KEY_COLUMN].get_int();
  ObString obj_str = row.storage_datums_[TABLE_LOCK_KEY_COLUMN + 1].get_string();
  ObTableLockOp store_info;
  const int64_t curr_timestamp = ObTimeUtility::current_time();

  ObTableHandleV2 handle;
  ObLockMemtable *memtable = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLockTable not inited", K(ret));
  } else if (OB_FAIL(store_info.deserialize(obj_str.ptr(), obj_str.length(), pos))) {
    LOG_WARN("failed to deserialize ObTableLockOp", K(ret));
    // we may recover from a sstable that copy from other ls replica,
    // the create timestamp need to be fixed.
  } else if (FALSE_IT(store_info.create_timestamp_ = OB_MIN(store_info.create_timestamp_,
                                                            curr_timestamp))) {
  } else if (OB_FAIL(get_lock_memtable(handle))) {
    LOG_WARN("get lock memtable failed", K(ret));
  } else if (OB_FAIL(handle.get_lock_memtable(memtable))) {
    LOG_WARN("get lock memtable from lock handle failed", K(ret));
  } else if (OB_FAIL(memtable->recover_obj_lock(store_info))) {
    LOG_WARN("failed to recover_obj_lock", K(ret), K(store_info));
  }
  LOG_INFO("ObLockTable::recover_ finished", K(ret), K(store_info));

  return ret;
}

int ObLockTable::get_table_schema_(
    const uint64_t tenant_id,
    ObTableSchema &schema)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = ObTabletID::LS_LOCK_TABLET_ID;
  const char *const AUTO_INC_ID = "id";
  const char *const VALUE_NAME = "lock_info";
  const int64_t SCHEMA_VERSION = 1;
  const char *const TABLE_NAME = "lock_table";
  const int64_t MAX_ID_LENGTH = 100; // the real length is no more than 64 + 1
  const int64_t MAX_LOCK_INFO_LENGTH = OB_MAX_USER_ROW_LENGTH - MAX_ID_LENGTH;
  ObObjMeta INC_ID_TYPE;
  INC_ID_TYPE.set_int();
  ObObjMeta DATA_TYPE;
  DATA_TYPE.set_binary();

  ObColumnSchemaV2 id_column;
  id_column.set_tenant_id(tenant_id);
  id_column.set_table_id(table_id);
  id_column.set_column_id(OB_APP_MIN_COLUMN_ID);
  id_column.set_schema_version(SCHEMA_VERSION);
  id_column.set_rowkey_position(1);
  id_column.set_order_in_rowkey(ObOrderType::ASC);
  id_column.set_meta_type(INC_ID_TYPE); // int64_t

  ObColumnSchemaV2 value_column;
  value_column.set_tenant_id(tenant_id);
  value_column.set_table_id(table_id);
  value_column.set_column_id(OB_APP_MIN_COLUMN_ID + 1);
  value_column.set_schema_version(SCHEMA_VERSION);
  value_column.set_data_length(MAX_LOCK_INFO_LENGTH);
  value_column.set_meta_type(DATA_TYPE);

  schema.set_tenant_id(tenant_id);
  schema.set_database_id(OB_SYS_DATABASE_ID);
  schema.set_table_id(table_id);
  schema.set_schema_version(SCHEMA_VERSION);

  if (OB_FAIL(id_column.set_column_name(AUTO_INC_ID))) {
    LOG_WARN("failed to set column name", K(ret), K(AUTO_INC_ID));
  } else if (OB_FAIL(value_column.set_column_name(VALUE_NAME))) {
    LOG_WARN("failed to set column name", K(ret), K(VALUE_NAME));
  } else if (OB_FAIL(schema.set_table_name(TABLE_NAME))) {
    LOG_WARN("failed to set table name", K(ret), K(TABLE_NAME));
  } else if (OB_FAIL(schema.add_column(id_column))) {
    LOG_WARN("failed to add column", K(ret), K(id_column));
  } else if (OB_FAIL(schema.add_column(value_column))) {
    LOG_WARN("failed to add column", K(ret), K(value_column));
  }
  return ret;
}

int ObLockTable::init(ObLS *parent)
{
  int ret = OB_SUCCESS;
  storage::ObMemtableMgrHandle memtable_mgr_handle;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLockTable init twice.", K(ret));
  } else if (OB_ISNULL(parent)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(parent));
  } else if (OB_FAIL(parent->get_tablet_svr()->get_lock_memtable_mgr(memtable_mgr_handle))) {
    LOG_WARN("get_lock_memtable_mgr failed", K(ret));
  } else if (OB_ISNULL(lock_mt_mgr_ = static_cast<ObLockMemtableMgr*>(memtable_mgr_handle.get_memtable_mgr()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lock memtable mgr pointer", KR(ret), KPC(parent_));
  } else {
    parent_ = parent;
    is_inited_ = true;
  }

  FLOG_INFO("finish init lock table", K(ret), KP(lock_mt_mgr_), KPC(lock_mt_mgr_), KP(parent_), KPC(parent_));
  return ret;
}

int ObLockTable::prepare_for_safe_destroy()
{
  // do nothing
  return OB_SUCCESS;
}

void ObLockTable::destroy()
{
  parent_ = nullptr;
  lock_mt_mgr_ = nullptr;
  lock_memtable_handle_.reset();
  is_inited_ = false;
}

int ObLockTable::offline()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(parent_)) {
    LOG_INFO("lock table offline", K(parent_->get_ls_id()));
  }

  // release all lock memtables before clean cache
  if (OB_NOT_NULL(lock_mt_mgr_)) {
    if (OB_FAIL(lock_mt_mgr_->release_memtables())) {
      if (OB_NOT_INIT == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("modify ret code to success because lock memtable mgr is not init and do not need offline.");
      } else {
        LOG_WARN("release all memtable in lock memtable mgr failed", KR(ret), KPC(lock_mt_mgr_));
      }
    }
  }

  // reset lock memtable handle
  TCWLockGuard guard(rw_lock_);
  lock_memtable_handle_.reset();
  return ret;
}

int ObLockTable::online()
{
  int ret = OB_SUCCESS;
  ObTabletHandle handle;
  ObTablet *tablet;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ObLSTabletService *ls_tablet_svr = nullptr;
  if (OB_NOT_NULL(parent_)) {
    LOG_INFO("online lock table", K(parent_->get_ls_id()));
  }

  if (OB_ISNULL(ls_tablet_svr = parent_->get_tablet_svr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_INFO("get ls tablet svr failed", K(ret));
  } else if (OB_FAIL(ls_tablet_svr->get_tablet(LS_LOCK_TABLET,
                                               handle))) {
    LOG_WARN("get tablet failed", K(ret));
  } else if (FALSE_IT(tablet = handle.get_obj())) {
  } else if (OB_FAIL(ls_tablet_svr->create_memtable(
                 LS_LOCK_TABLET, 0 /* schema_version */, false /* for_inc_direct_load */, false /*for_replay*/))) {
    LOG_WARN("failed to create memtable", K(ret));
  } else if (OB_FAIL(tablet->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else {
    const ObSSTableArray &sstables = table_store_wrapper.get_member()->get_minor_sstables();
    if (!sstables.empty()) {
      ObStorageMetaHandle loaded_sstable_handle;
      ObSSTable *loaded_sstable = nullptr;
      if (OB_FAIL(ObTabletTableStore::load_sstable_on_demand(
          table_store_wrapper.get_meta_handle(),
          *sstables[0],
          loaded_sstable_handle,
          loaded_sstable))) {
        LOG_WARN("fail to load sstable on demand", K(ret));
      } else if (OB_FAIL(restore_lock_table_(*loaded_sstable))) {
        LOG_WARN("fail to restore lock table", K(ret));
      }
    }
  }

  return ret;
}

int ObLockTable::create_tablet(const lib::Worker::CompatMode compat_mode, const SCN &create_scn)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = parent_->get_tenant_id();
  const share::ObLSID &ls_id = parent_->get_ls_id();
  share::schema::ObTableSchema table_schema;
  ObIMemtableMgr *memtable_mgr = nullptr;
  ObMemtableMgrHandle memtable_mgr_handle;
  ObArenaAllocator arena_allocator;
  ObCreateTabletSchema create_tablet_schema;
  uint64_t tenant_data_version = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLockTable not inited", K(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_data_version))) {
    LOG_WARN("get data version failed", K(ret));
  } else if (OB_FAIL(get_table_schema_(tenant_id, table_schema))) {
    LOG_WARN("get lock table schema failed", K(ret));
  } else if (OB_FAIL(create_tablet_schema.init(arena_allocator, table_schema, compat_mode,
        false/*skip_column_info*/, ObCreateTabletSchema::STORAGE_SCHEMA_VERSION_V3))) {
    LOG_WARN("failed to init storage schema", KR(ret), K(table_schema));
  } else if (OB_FAIL(parent_->create_ls_inner_tablet(ls_id,
                                                     LS_LOCK_TABLET,
                                                     ObLS::LS_INNER_TABLET_FROZEN_SCN,
                                                     create_tablet_schema,
                                                     create_scn))) {
    LOG_WARN("failed to create lock tablet", K(ret), K(ls_id), K(LS_LOCK_TABLET),
             K(table_schema), K(compat_mode), K(create_scn));
  } else if (OB_FAIL(parent_->get_tablet_svr()->
                     get_lock_memtable_mgr(memtable_mgr_handle))) {
    LOG_WARN("get_lock_memtable_mgr failed", K(ret));
  } else if (OB_ISNULL(memtable_mgr = memtable_mgr_handle.get_memtable_mgr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get_memtable_mgr from memtable mgr handle failed", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObLockTable::remove_tablet()
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = parent_->get_ls_id();
  if (IS_NOT_INIT) {
    LOG_WARN("lock table does not inited, remove do nothing");
  } else if (OB_FAIL(parent_->remove_ls_inner_tablet(ls_id, LS_LOCK_TABLET))) {
    LOG_WARN("failed to remove ls inner tablet", K(ret), K(ls_id), K(LS_LOCK_TABLET));
    ob_usleep(1000 * 1000);
    ob_abort();
  }
  return ret;
}

int ObLockTable::load_lock()
{
  int ret = OB_SUCCESS;
  ObTabletHandle handle;
  ObTablet *tablet;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ObLSTabletService *ls_tablet_svr = nullptr;
  LOG_INFO("load_lock_table()", K(parent_->get_ls_id()));

  if (OB_ISNULL(ls_tablet_svr = parent_->get_tablet_svr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_INFO("get ls tablet svr failed", K(ret));
  } else if (OB_FAIL(ls_tablet_svr->get_tablet(LS_LOCK_TABLET,
                                               handle))) {
    LOG_WARN("get tablet failed", K(ret));
  } else if (FALSE_IT(tablet = handle.get_obj())) {
  } else if (OB_FAIL(ls_tablet_svr->create_memtable(
                 LS_LOCK_TABLET, 0 /* schema_version */, false /* for_inc_direct_load */, false /*for_replay*/))) {
    LOG_WARN("failed to create memtable", K(ret));
  } else if (OB_FAIL(tablet->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else {
    const ObSSTableArray &sstables = table_store_wrapper.get_member()->get_minor_sstables();
    if (!sstables.empty()) {
      ObStorageMetaHandle loaded_sstable_handle;
      ObSSTable *loaded_sstable = nullptr;
      if (OB_FAIL(ObTabletTableStore::load_sstable_on_demand(
          table_store_wrapper.get_meta_handle(),
          *sstables[0],
          loaded_sstable_handle,
          loaded_sstable))) {
        LOG_WARN("fail to load sstable on demand", K(ret));
      } else if (OB_FAIL(restore_lock_table_(*loaded_sstable))) {
        LOG_WARN("fail to restore lock table", K(ret));
      }
    }
  }

  return ret;
}

int ObLockTable::get_lock_memtable(ObTableHandleV2 &handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("lock table is not inited", KR(ret));
  } else {
    while (OB_SUCC(ret)) {
      {
        // most case : acquire read lock to copy assigne lock_memtable_handle
        TCRLockGuard guard(rw_lock_);
        if (lock_memtable_handle_.is_valid()) {
          handle = lock_memtable_handle_;
          break;
        }
      }

      {
        // acquire write lock to get active lock memtable from lock memtable mgr
        TCWLockGuard guard(rw_lock_);
        if (OB_ISNULL(lock_mt_mgr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("lock memtable mgr is unexpected nullptr", KR(ret), K(is_inited_), KPC(parent_), KPC(lock_mt_mgr_));
        } else if (OB_FAIL(lock_mt_mgr_->get_active_memtable(lock_memtable_handle_))) {
          LOG_WARN("get active lock memtable failed", KR(ret), K(is_inited_), KPC(parent_), KPC(lock_mt_mgr_));
        } else {
          // loop and get memtable handle
        }
      }
    }
  }
  return ret;
}

int ObLockTable::check_lock_conflict(
    ObStoreCtx &ctx,
    const ObLockParam &param)
{
  int ret = OB_SUCCESS;
  ObTableHandleV2 handle;
  ObLockMemtable *memtable = nullptr;
  ObMemtableCtx *mem_ctx = nullptr;
  ObTxIDSet unused_conflict_tx_set;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLockTable not inited", K(ret));
  } else if (OB_UNLIKELY(!ctx.is_write()) ||
             OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ctx), K(param));
  } else if (FALSE_IT(mem_ctx = static_cast<ObMemtableCtx *>(ctx.mvcc_acc_ctx_.mem_ctx_))) {
  } else if (OB_FAIL(get_lock_memtable(handle))) {
    LOG_WARN("get lock memtable failed", K(ret));
  } else if (OB_FAIL(handle.get_lock_memtable(memtable))) {
    LOG_ERROR("get lock memtable from lock handle failed", K(ret));
  } else {
    const int64_t lock_timestamp = ObTimeUtility::current_time();
    const bool include_finish_tx = false;
    const bool only_check_dml_lock = true;
    ObTableLockOp lock_op(param.lock_id_,
                          param.lock_mode_,
                          param.owner_id_,
                          ctx.mvcc_acc_ctx_.get_tx_id(),
                          param.op_type_,
                          LOCK_OP_DOING,
                          ctx.mvcc_acc_ctx_.tx_scn_,
                          lock_timestamp,
                          param.schema_version_);
    if (OB_FAIL(memtable->check_lock_conflict(mem_ctx,
                                              lock_op,
                                              unused_conflict_tx_set,
                                              include_finish_tx,
                                              only_check_dml_lock))) {
      if (ret != OB_TRY_LOCK_ROW_CONFLICT) {
        LOG_WARN("lock failed.", K(ret), K(lock_op));
      }
    }
    LOG_DEBUG("finish check_lock_conflict", K(ret), K(param), K(ctx));
  }
  return ret;
}

int ObLockTable::check_lock_conflict(
    const ObMemtableCtx *mem_ctx,
    const ObTableLockOp &lock_op,
    ObTxIDSet &conflict_tx_set,
    const bool include_finish_tx)
{
  int ret = OB_SUCCESS;
  ObTableHandleV2 handle;
  ObLockMemtable *memtable = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLockTable not inited", K(ret));
  } else if (OB_ISNULL(mem_ctx) ||
             OB_UNLIKELY(!lock_op.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(mem_ctx), K(lock_op));
  } else if (OB_FAIL(get_lock_memtable(handle))) {
    LOG_WARN("get lock memtable failed", K(ret));
  } else if (OB_FAIL(handle.get_lock_memtable(memtable))) {
    LOG_ERROR("get lock memtable from lock handle failed", K(ret));
  } else if (OB_FAIL(memtable->check_lock_conflict(mem_ctx,
                                                   lock_op,
                                                   conflict_tx_set,
                                                   include_finish_tx))) {
    if (ret != OB_TRY_LOCK_ROW_CONFLICT) {
      LOG_WARN("check_lock_conflict failed.", K(ret), K(lock_op));
    }
  } else {
    // do nothing
  }
  LOG_DEBUG("finish check lock conflict", K(ret), K(lock_op));
  return ret;
}

int ObLockTable::lock(
    ObStoreCtx &ctx,
    const ObLockParam &param)
{
  int ret = OB_SUCCESS;
  ObLockMemtable *memtable = nullptr;
  ObTransID tx_id = ctx.mvcc_acc_ctx_.get_tx_id();
  TCRLockGuard guard(rw_lock_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLockTable not inited", K(ret));
  } else if (OB_UNLIKELY(!ctx.is_write()) ||
             OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ctx), K(param));
  } else if (OB_UNLIKELY(!tx_id.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid argument", K(ret), K(ctx), K(param), K(ctx.mvcc_acc_ctx_));
    ob_abort();
  } else if (OB_FAIL(ctx.mvcc_acc_ctx_.mem_ctx_->get_lock_mem_ctx().get_lock_memtable(memtable))) {
    LOG_WARN("get lock memtable failed", K(ret));
  } else if (OB_ISNULL(memtable)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("lock memtable is null", K(ret), K(ctx));
  } else {
    const int64_t lock_timestamp = ObTimeUtility::current_time();
    ObTableLockOp lock_op(param.lock_id_,
                          param.lock_mode_,
                          param.owner_id_,
                          tx_id,
                          param.op_type_,
                          LOCK_OP_DOING,
                          ctx.mvcc_acc_ctx_.tx_scn_,
                          lock_timestamp,
                          param.schema_version_);
    if (OB_FAIL(memtable->lock(param,
                               ctx,
                               lock_op))) {
      if (ret != OB_TRY_LOCK_ROW_CONFLICT) {
        LOG_WARN("lock failed.", K(ret), K(lock_op));
      }
    }
    LOG_DEBUG("finish lock", K(ret), K(param), K(ctx));
  }
  return ret;
}

int ObLockTable::unlock(
    ObStoreCtx &ctx,
    const ObLockParam &param)
{
  int ret = OB_SUCCESS;
  ObTableHandleV2 handle;
  ObLockMemtable *memtable = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLockTable not inited", K(ret));
  } else if (OB_UNLIKELY(!ctx.is_write()) ||
             OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ctx), K(param));
  } else if (OB_FAIL(get_lock_memtable(handle))) {
    LOG_WARN("get lock memtable failed", K(ret));
  } else if (OB_FAIL(handle.get_lock_memtable(memtable))) {
    LOG_ERROR("get lock memtable from lock handle failed", K(ret));
  } else {
    const bool is_try_lock = param.is_try_lock_;
    const int64_t expired_time = param.expired_time_;
    const int64_t unlock_timestamp = ObTimeUtility::current_time();
    ObTableLockOp unlock_op(param.lock_id_,
                            param.lock_mode_,
                            param.owner_id_,
                            ctx.mvcc_acc_ctx_.get_tx_id(),
                            param.op_type_,
                            LOCK_OP_DOING,
                            ctx.mvcc_acc_ctx_.tx_scn_,
                            unlock_timestamp,
                            param.schema_version_);
    if (OB_FAIL(memtable->unlock(ctx,
                                 unlock_op,
                                 is_try_lock,
                                 expired_time))) {
      LOG_WARN("unlock failed.", K(ret), K(unlock_op));
    }
  }
  LOG_DEBUG("ObLockTable::unlock ", K(ret), K(param), K(ctx));
  return ret;
}

int ObLockTable::get_lock_id_iter(ObLockIDIterator &iter)
{
  int ret = OB_SUCCESS;
  ObTableHandleV2 handle;
  ObLockMemtable *memtable = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TABLELOCK_LOG(WARN, "ObLockTable not inited", K(ret));
  } else if (OB_FAIL(get_lock_memtable(handle))) {
    TABLELOCK_LOG(WARN, "get lock memtable failed", K(ret));
  } else if (OB_FAIL(handle.get_lock_memtable(memtable))) {
    TABLELOCK_LOG(ERROR, "get lock memtable from lock handle failed", K(ret));
  } else {
    if (OB_FAIL(memtable->get_lock_id_iter(iter))) {
      TABLELOCK_LOG(WARN, "get lock id iter failed.", K(ret));
    }
  }
  TABLELOCK_LOG(DEBUG, "ObLockTable::get_lock_id_iter", K(ret));
  return ret;
}

int ObLockTable::get_lock_op_iter(const ObLockID &lock_id,
                                  ObLockOpIterator &iter)
{
  int ret = OB_SUCCESS;
  ObTableHandleV2 handle;
  ObLockMemtable *memtable = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TABLELOCK_LOG(WARN, "ObLockTable not inited", K(ret));
  } else if (OB_FAIL(get_lock_memtable(handle))) {
    TABLELOCK_LOG(WARN, "get lock memtable failed", K(ret));
  } else if (OB_FAIL(handle.get_lock_memtable(memtable))) {
    TABLELOCK_LOG(ERROR, "get lock memtable from lock handle failed", K(ret));
  } else {
    if (OB_FAIL(memtable->get_lock_op_iter(lock_id,
                                           iter))) {
      TABLELOCK_LOG(WARN, "get lock op iter failed.", K(ret), K(lock_id));
    }
  }
  TABLELOCK_LOG(DEBUG, "ObLockTable::get_lock_op_iter", K(ret));
  return ret;
}

int ObLockTable::admin_remove_lock_op(const ObTableLockOp &op_info)
{
  int ret = OB_SUCCESS;
  ObTableHandleV2 handle;
  ObLockMemtable *memtable = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TABLELOCK_LOG(WARN, "ObLockTable not inited", K(ret));
  } else if (OB_FAIL(get_lock_memtable(handle))) {
    TABLELOCK_LOG(WARN, "get lock memtable failed", K(ret));
  } else if (OB_FAIL(handle.get_lock_memtable(memtable))) {
    TABLELOCK_LOG(ERROR, "get lock memtable from lock handle failed", K(ret));
  } else {
    memtable->remove_lock_record(op_info);
  }
  TABLELOCK_LOG(INFO, "ObLockTable::admin_remove_lock_op", K(ret), K(op_info));
  return ret;
}

int ObLockTable::admin_update_lock_op(const ObTableLockOp &op_info,
                                      const share::SCN &commit_version,
                                      const share::SCN &commit_scn,
                                      const ObTableLockOpStatus status)
{
  int ret = OB_SUCCESS;
  ObTableHandleV2 handle;
  ObLockMemtable *memtable = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TABLELOCK_LOG(WARN, "ObLockTable not inited", K(ret));
  } else if (OB_FAIL(get_lock_memtable(handle))) {
    TABLELOCK_LOG(WARN, "get lock memtable failed", K(ret));
  } else if (OB_FAIL(handle.get_lock_memtable(memtable))) {
    TABLELOCK_LOG(ERROR, "get lock memtable from lock handle failed", K(ret));
  } else if (OB_FAIL(memtable->update_lock_status(op_info,
                                                  commit_version,
                                                  commit_scn,
                                                  status))) {
    LOG_WARN("update lock status failed", KR(ret), K(op_info), K(status));
  }
  TABLELOCK_LOG(INFO, "ObLockTable::admin_update_lock_op", K(ret), K(op_info));
  return ret;
}

int ObLockTable::check_and_clear_obj_lock(const bool force_compact)
{
  int ret = OB_SUCCESS;
  ObTableHandleV2 handle;
  ObLockMemtable *lock_memtable = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLockTable is not inited", K(ret));
  } else if (OB_FAIL(get_lock_memtable(handle))) {
    LOG_WARN("get lock memtable failed", K(ret));
  } else if (OB_FAIL(handle.get_lock_memtable(lock_memtable))) {
    LOG_WARN("get lock memtable from lock handle failed", K(ret));
  } else if (OB_FAIL(lock_memtable->check_and_clear_obj_lock(force_compact))) {
    LOG_WARN("check and clear obj lock failed", K(ret));
  }
  return ret;
}

int ObLockTable::switch_to_leader()
{
  int ret = OB_SUCCESS;
  ObTableLockService::ObOBJLockGarbageCollector *obj_lock_gc = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLockTable is not inited", K(ret));
  } else if (OB_FAIL(MTL(ObTableLockService *)
                         ->get_obj_lock_garbage_collector(obj_lock_gc))) {
    LOG_WARN("can not get ObOBJLockGarbageCollector", K(ret));
  } else if (OB_ISNULL(obj_lock_gc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObOBJLockGarbageCollector is null", K(ret));
  } else {
    if (OB_NOT_NULL(parent_)) {
      LOG_INFO("start to check and clear obj lock when switch to leader", K(ret),
              K(parent_->get_ls_id()));
    }
    ret = obj_lock_gc->obj_lock_gc_thread_pool_.commit_task_ignore_ret(
        [this]() { return check_and_clear_obj_lock(true); });
  }

  if (OB_FAIL(ret)) {
    if (OB_ISNULL(parent_)) {
      // ignore ret
      LOG_WARN("parent ls of ObLockTable is null", K(ret));
    } else {
      LOG_WARN("collect obj lock garbage when switch to leader failed", K(ret),
               K(parent_->get_ls_id()));
    }
  }
  return ret;
}

int ObLockTable::enable_check_tablet_status(const bool need_check)
{
  int ret = OB_SUCCESS;
  ObTableHandleV2 handle;
  ObLockMemtable *lock_memtable = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLockTable is not inited", K(ret));
  } else if (OB_FAIL(get_lock_memtable(handle))) {
    LOG_WARN("get lock memtable failed", K(ret));
    // to disable check just skip when no active memtable
    if (!need_check && OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    }
  } else if (OB_FAIL(handle.get_lock_memtable(lock_memtable))) {
    LOG_WARN("get lock memtable from lock handle failed", K(ret));
  } else if (FALSE_IT(lock_memtable->enable_check_tablet_status(need_check))) {
  }
  return ret;
}

} // tablelock
} // transaction
} // oceanbase
