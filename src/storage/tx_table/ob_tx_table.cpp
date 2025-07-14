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

#include "ob_tx_table.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx_table/ob_tx_data_cache.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/incremental/ob_shared_meta_service.h"
#include "storage/incremental/share/ob_shared_ls_meta.h"
#endif

namespace oceanbase {
using namespace share;
using namespace palf;
using namespace transaction;

namespace storage {

int64_t ObTxTable::UPDATE_MIN_START_SCN_INTERVAL = 5 * 1000 * 1000; // 5 seconds

int ObTxTable::init(ObLS *ls)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_ISNULL(ls)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), KP(ls));
  } else if (OB_FAIL(tx_data_table_.init(ls, &tx_ctx_table_))) {
    LOG_WARN("tx data table init failed", K(ret));
  } else if (OB_FAIL(tx_ctx_table_.init(ls->get_ls_id()))) {
    LOG_WARN("tx ctx table init failed", K(ret));
  } else {
    ls_ = ls;
    ls_id_ = ls->get_ls_id();
    epoch_ = 0;
    state_ = TxTableState::OFFLINE;
    mini_cache_hit_cnt_ = 0;
    kv_cache_hit_cnt_ = 0;
    read_tx_data_table_cnt_ = 0;
    recycle_scn_cache_.reset();
    recycle_record_.reset();
#ifdef OB_BUILD_SHARED_STORAGE
    ss_upload_scn_cache_.reset();
#endif
    LOG_INFO("init tx table successfully", K(ret), K(ls->get_ls_id()));
    calc_upper_trans_is_disabled_ = false;
    is_inited_ = true;
  }
  if (OB_FAIL(ret)) {
    destroy();
  }
  return ret;
}

int ObTxTable::start()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tx_data_table_.start())) {
    LOG_WARN("start tx data table failed.", KR(ret));
  } else {
    LOG_INFO("tx table start finish", KR(ret), KPC(this));
  }
  return ret;
}

void ObTxTable::stop()
{
  ATOMIC_STORE(&state_, TxTableState::OFFLINE);
  tx_data_table_.stop();
  LOG_INFO("tx table stop finish", KPC(this));
}

int ObTxTable::prepare_offline()
{
  int ret = OB_SUCCESS;
  ATOMIC_STORE(&state_, TxTableState::PREPARE_OFFLINE);
  TRANS_LOG(INFO, "tx table prepare offline succeed", KPC(this));
  return ret;
}

int ObTxTable::offline_tx_ctx_table_()
{
  int ret = OB_SUCCESS;
  ObTabletHandle handle;
  ObTablet *tablet;
  ObLSTabletService *ls_tablet_svr = ls_->get_tablet_svr();

  if (NULL == ls_tablet_svr) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get ls tablet svr failed", K(ret));
  } else if (OB_FAIL(ls_tablet_svr->get_tablet(LS_TX_CTX_TABLET, handle))) {
    LOG_WARN("get tablet failed", K(ret));
    if (OB_TABLET_NOT_EXIST == ret) {
      // a ls that of migrate does not have tx ctx tablet
      ret = OB_SUCCESS;
    }
  } else if (FALSE_IT(tablet = handle.get_obj())) {
  } else if (OB_FAIL(tablet->release_memtables())) {
    LOG_WARN("failed to release memtables", K(ret), KPC(ls_));
  } else if (OB_FAIL(tx_ctx_table_.offline())) {
    LOG_WARN("failed to offline tx ctx table", K(ret), KPC(ls_));
  } else {
    // do nothing
  }

  return ret;
}

int ObTxTable::offline_tx_data_table_()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "tx table is not init", KR(ret));
  } else if (OB_FAIL(tx_data_table_.offline())) {
    STORAGE_LOG(WARN, "tx data table offline failed", KR(ret), K(ls_id_));
  }
  return ret;
}

int ObTxTable::offline()
{
  int ret = OB_SUCCESS;
  ObTabletHandle handle;
  ObTablet *tablet;
  ObLSTabletService *ls_tablet_svr = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tx table is not init.", KR(ret));
  } else if (OB_FAIL(offline_tx_ctx_table_())) {
    LOG_WARN("offline tx ctx table failed", K(ret));
  } else if (OB_FAIL(offline_tx_data_table_())) {
    LOG_WARN("offline tx data table failed", K(ret));
  } else {
#ifdef OB_BUILD_SHARED_STORAGE
    ss_upload_scn_cache_.reset();
#endif
    recycle_scn_cache_.reset();
    recycle_record_.reset();
    (void)disable_upper_trans_calculation();
    ATOMIC_STORE(&state_, TxTableState::OFFLINE);
    LOG_INFO("tx table offline succeed", K(ls_id_), KPC(this));
  }

  return ret;
}

int ObTxTable::online()
{
  int ret = OB_SUCCESS;
  SCN tmp_max_tablet_clog_checkpiont = SCN::min_scn();

  ATOMIC_INC(&epoch_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tx table is not init.", KR(ret));
  } else if (OB_FAIL(tx_data_table_.online())) {
    LOG_WARN("failed to online tx data table", K(ret));
  } else if (OB_FAIL(load_tx_ctx_table_())) {
    LOG_WARN("failed to load tx ctx table", K(ret));
  } else {
#ifdef OB_BUILD_SHARED_STORAGE
    ss_upload_scn_cache_.reset();
#endif
    recycle_scn_cache_.reset();
    recycle_record_.reset();
    (void)reset_ctx_min_start_scn_info_();
    ATOMIC_STORE(&state_, ObTxTable::ONLINE);
    ATOMIC_STORE(&calc_upper_trans_is_disabled_, false);
    LOG_INFO("tx table online succeed", K(ls_id_), KPC(this));
  }

  return ret;
}

int ObTxTable::prepare_for_safe_destroy()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tx_data_table_.prepare_for_safe_destroy())) {
    LOG_WARN("tx data table prepare for safe destory failed", KR(ret));
  }
  return ret;
}

int ObTxTable::create_tablet(const lib::Worker::CompatMode compat_mode, const SCN &create_scn)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    const uint64_t tenant_id = ls_->get_tenant_id();
    const share::ObLSID &ls_id = ls_->get_ls_id();
    if (OB_FAIL(create_data_tablet_(tenant_id, ls_id, compat_mode, create_scn))) {
      LOG_WARN("create data tablet failed", K(ret));
    } else if (OB_FAIL(create_ctx_tablet_(tenant_id, ls_id, compat_mode, create_scn))) {
      LOG_WARN("create ctx tablet failed", K(ret));
    }
    if (OB_FAIL(ret)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(remove_tablet())) {
        LOG_WARN("remove tablet failed", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObTxTable::get_ctx_table_schema_(const uint64_t tenant_id, share::schema::ObTableSchema &schema)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = ObTabletID::LS_TX_CTX_TABLET_ID;
  const char *const AUTO_INC_ID = "id";
  const char *const META_NAME = "tx_info_meta";
  const char *const VALUE_NAME = "tx_info";
  const int64_t SCHEMA_VERSION = 1;
  const char *const TABLE_NAME = "tx_ctx_table";

  common::ObObjMeta INC_ID_TYPE;
  INC_ID_TYPE.set_int();
  ObColumnSchemaV2 id_column;
  id_column.set_tenant_id(tenant_id);
  id_column.set_table_id(table_id);
  id_column.set_column_id(common::OB_APP_MIN_COLUMN_ID);
  id_column.set_schema_version(SCHEMA_VERSION);
  id_column.set_rowkey_position(1);
  id_column.set_order_in_rowkey(ObOrderType::ASC);
  id_column.set_meta_type(INC_ID_TYPE);  // int64_t

  common::ObObjMeta META_TYPE;
  META_TYPE.set_binary();
  ObColumnSchemaV2 meta_column;
  meta_column.set_tenant_id(tenant_id);
  meta_column.set_table_id(table_id);
  meta_column.set_column_id(common::OB_APP_MIN_COLUMN_ID + 1);
  meta_column.set_schema_version(SCHEMA_VERSION);
  meta_column.set_data_length(MAX_TX_CTX_TABLE_META_LENGTH);
  meta_column.set_meta_type(META_TYPE);

  common::ObObjMeta DATA_TYPE;
  DATA_TYPE.set_binary();
  ObColumnSchemaV2 value_column;
  value_column.set_tenant_id(tenant_id);
  value_column.set_table_id(table_id);
  value_column.set_column_id(common::OB_APP_MIN_COLUMN_ID + 2);
  value_column.set_schema_version(SCHEMA_VERSION);
  value_column.set_data_length(MAX_TX_CTX_TABLE_VALUE_LENGTH);
  value_column.set_meta_type(DATA_TYPE);

  schema.set_tenant_id(tenant_id);
  schema.set_database_id(OB_SYS_DATABASE_ID);
  schema.set_table_id(table_id);
  schema.set_schema_version(SCHEMA_VERSION);

  if (OB_FAIL(id_column.set_column_name(AUTO_INC_ID))) {
    LOG_WARN("failed to set column name", K(ret), K(AUTO_INC_ID));
  } else if (OB_FAIL(meta_column.set_column_name(META_NAME))) {
    LOG_WARN("failed to set column name", K(ret), K(META_NAME));
  } else if (OB_FAIL(value_column.set_column_name(VALUE_NAME))) {
    LOG_WARN("failed to set column name", K(ret), K(VALUE_NAME));
  } else if (OB_FAIL(schema.set_table_name(TABLE_NAME))) {
    LOG_WARN("failed to set table name", K(ret), K(TABLE_NAME));
  } else if (OB_FAIL(schema.add_column(id_column))) {
    LOG_WARN("failed to add column", K(ret), K(id_column));
  } else if (OB_FAIL(schema.add_column(meta_column))) {
    LOG_WARN("failed to add column", K(ret), K(meta_column));
  } else if (OB_FAIL(schema.add_column(value_column))) {
    LOG_WARN("failed to add column", K(ret), K(value_column));
  } else {
    schema.set_micro_index_clustered(false);
  }
  return ret;
}

int ObTxTable::create_ctx_tablet_(
    const uint64_t tenant_id,
    const ObLSID ls_id,
    const lib::Worker::CompatMode compat_mode,
    const share::SCN &create_scn)
{
  int ret = OB_SUCCESS;
  share::schema::ObTableSchema table_schema;
  ObArenaAllocator arena_allocator;
  ObCreateTabletSchema create_tablet_schema;
  if (OB_FAIL(get_ctx_table_schema_(tenant_id, table_schema))) {
    LOG_WARN("get ctx table schema failed", K(ret));
  } else if (OB_FAIL(create_tablet_schema.init(arena_allocator, table_schema, compat_mode,
        false/*skip_column_info*/, DATA_CURRENT_VERSION))) {
    LOG_WARN("failed to init storage schema", KR(ret), K(table_schema));
  } else if (OB_FAIL(ls_->create_ls_inner_tablet(ls_id,
                                                 LS_TX_CTX_TABLET,
                                                 ObLS::LS_INNER_TABLET_FROZEN_SCN,
                                                 create_tablet_schema,
                                                 create_scn))) {
    LOG_WARN("create tx ctx tablet failed", K(ret), K(ls_id),
             K(LS_TX_CTX_TABLET), K(ObLS::LS_INNER_TABLET_FROZEN_SCN),
             K(table_schema), K(compat_mode), K(create_scn));
  }
  return ret;
}

int ObTxTable::get_data_table_schema_(const uint64_t tenant_id, share::schema::ObTableSchema &schema)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = ObTabletID::LS_TX_DATA_TABLET_ID;
  const char *const TX_ID_NAME = "tx_id";
  const char *const IDX_NAME = "idx";
  const char *const TOTAL_ROW_CNT_NAME = "total_row_cnt";
  const char *const END_SCN_NAME = "end_scn";
  const char *const VALUE_NAME = "tx_info";
  const char *const TABLE_NAME = "tx_data_table";
  const int64_t SCHEMA_VERSION = 1;
  const int64_t MAX_TX_ID_LENGTH = 100;  // the real length is no more than 64 + 1
  const int64_t MAX_TX_INFO_LENGTH = common::OB_MAX_VARCHAR_LENGTH;

  common::ObObjMeta TX_ID_TYPE;
  TX_ID_TYPE.set_int();
  common::ObObjMeta IDX_TYPE;
  IDX_TYPE.set_int();
  common::ObObjMeta TOTAL_ROW_CNT_TYPE;
  TOTAL_ROW_CNT_TYPE.set_int();
  common::ObObjMeta END_TS_TYPE;
  END_TS_TYPE.set_int();
  common::ObObjMeta DATA_TYPE;
  DATA_TYPE.set_binary();

  ObColumnSchemaV2 tx_id_column;
  tx_id_column.set_tenant_id(tenant_id);
  tx_id_column.set_table_id(table_id);
  tx_id_column.set_column_id(ObTxDataTable::TX_ID);
  tx_id_column.set_schema_version(SCHEMA_VERSION);
  tx_id_column.set_rowkey_position(1);
  tx_id_column.set_order_in_rowkey(ObOrderType::ASC);
  tx_id_column.set_meta_type(TX_ID_TYPE);  // int64_t

  ObColumnSchemaV2 idx_column;
  idx_column.set_tenant_id(tenant_id);
  idx_column.set_table_id(table_id);
  idx_column.set_column_id(ObTxDataTable::IDX);
  idx_column.set_schema_version(SCHEMA_VERSION);
  idx_column.set_rowkey_position(2);
  idx_column.set_order_in_rowkey(ObOrderType::ASC);
  idx_column.set_meta_type(IDX_TYPE);  // int64_t

  ObColumnSchemaV2 total_row_cnt_column;
  total_row_cnt_column.set_tenant_id(tenant_id);
  total_row_cnt_column.set_table_id(table_id);
  total_row_cnt_column.set_column_id(ObTxDataTable::TOTAL_ROW_CNT);
  total_row_cnt_column.set_schema_version(SCHEMA_VERSION);
  total_row_cnt_column.set_meta_type(TOTAL_ROW_CNT_TYPE);
  total_row_cnt_column.set_rowkey_position(0);

  ObColumnSchemaV2 end_ts_column;
  end_ts_column.set_tenant_id(tenant_id);
  end_ts_column.set_table_id(table_id);
  end_ts_column.set_column_id(ObTxDataTable::END_LOG_TS);
  end_ts_column.set_schema_version(SCHEMA_VERSION);
  end_ts_column.set_meta_type(END_TS_TYPE);
  end_ts_column.set_rowkey_position(0);

  ObColumnSchemaV2 value_column;
  value_column.set_tenant_id(tenant_id);
  value_column.set_table_id(table_id);
  value_column.set_column_id(ObTxDataTable::VALUE);
  value_column.set_schema_version(SCHEMA_VERSION);
  value_column.set_data_length(MAX_TX_INFO_LENGTH);
  value_column.set_meta_type(DATA_TYPE);
  value_column.set_rowkey_position(0);

  schema.set_tenant_id(tenant_id);
  schema.set_database_id(OB_SYS_DATABASE_ID);
  schema.set_table_id(table_id);
  schema.set_schema_version(SCHEMA_VERSION);

  if (OB_FAIL(tx_id_column.set_column_name(TX_ID_NAME))) {
    LOG_WARN("failed to set column name", KR(ret), K(TX_ID_NAME));
  } else if (OB_FAIL(idx_column.set_column_name(IDX_NAME))) {
    LOG_WARN("failed to set column name", KR(ret), K(IDX_NAME));
  } else if (OB_FAIL(total_row_cnt_column.set_column_name(TOTAL_ROW_CNT_NAME))) {
    LOG_WARN("failed to set column name", KR(ret), K(TOTAL_ROW_CNT_NAME));
  } else if (OB_FAIL(end_ts_column.set_column_name(END_SCN_NAME))) {
    LOG_WARN("failed to set column name", KR(ret), K(END_SCN_NAME));
  } else if (OB_FAIL(value_column.set_column_name(VALUE_NAME))) {
    LOG_WARN("failed to set column name", KR(ret), K(VALUE_NAME));
  } else if (OB_FAIL(schema.set_table_name(TABLE_NAME))) {
    LOG_WARN("failed to set table name", K(ret), K(TABLE_NAME));
  } else if (OB_FAIL(schema.add_column(tx_id_column))) {
    LOG_WARN("failed to add column", K(ret), K(tx_id_column));
  } else if (OB_FAIL(schema.add_column(idx_column))) {
    LOG_WARN("failed to add column", K(ret), K(idx_column));
  } else if (OB_FAIL(schema.add_column(total_row_cnt_column))) {
    LOG_WARN("failed to add column", K(ret), K(total_row_cnt_column));
  } else if (OB_FAIL(schema.add_column(end_ts_column))) {
    LOG_WARN("failed to add column", K(ret), K(end_ts_column));
  } else if (OB_FAIL(schema.add_column(value_column))) {
    LOG_WARN("failed to add column", K(ret), K(value_column));
  } else {
    schema.set_micro_index_clustered(false);
  }
  return ret;
}

int ObTxTable::create_data_tablet_(const uint64_t tenant_id,
                                   const ObLSID ls_id,
                                   const lib::Worker::CompatMode compat_mode,
                                   const share::SCN &create_scn)
{
  int ret = OB_SUCCESS;
  share::schema::ObTableSchema table_schema;
  ObArenaAllocator arena_allocator;
  ObCreateTabletSchema create_tablet_schema;
  if (OB_FAIL(get_data_table_schema_(tenant_id, table_schema))) {
    LOG_WARN("get data table schema failed", K(ret));
  } else if (OB_FAIL(create_tablet_schema.init(arena_allocator, table_schema, compat_mode,
        false/*skip_column_info*/, DATA_CURRENT_VERSION))) {
    LOG_WARN("failed to init storage schema", KR(ret), K(table_schema));
  } else if (OB_FAIL(ls_->create_ls_inner_tablet(ls_id,
                                                 LS_TX_DATA_TABLET,
                                                 ObLS::LS_INNER_TABLET_FROZEN_SCN,
                                                 create_tablet_schema,
                                                 create_scn))) {
    LOG_WARN("create tx data tablet failed", K(ret), K(ls_id),
             K(LS_TX_DATA_TABLET), K(ObLS::LS_INNER_TABLET_FROZEN_SCN),
             K(table_schema), K(compat_mode), K(create_scn));
  }
  return ret;
}

int ObTxTable::remove_tablet_(const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = ls_->get_ls_id();
  if (OB_FAIL(ls_->remove_ls_inner_tablet(ls_id, tablet_id))) {
    LOG_WARN("remove ls inner tablet failed", K(ret), K(ls_id), K(tablet_id));
  }
  return ret;
}

int ObTxTable::remove_tablet()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(ls_)) {
    if (OB_FAIL(remove_tablet_(LS_TX_DATA_TABLET))) {
      LOG_WARN("remove tx data tablet failed", K(ret));
      ob_usleep(1000 * 1000);
      ob_abort();
    } else if (OB_FAIL(remove_tablet_(LS_TX_CTX_TABLET))) {
      LOG_WARN("remove tx ctx tablet failed", K(ret));
      ob_usleep(1000 * 1000);
      ob_abort();
    }
  }
  return ret;
}

int ObTxTable::load_tx_ctx_table_()
{
  int ret = OB_SUCCESS;
  ObTabletHandle handle;
  ObTablet *tablet = nullptr;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ObLSTabletService *ls_tablet_svr = ls_->get_tablet_svr();
  ObMemtableMgrHandle mgr_handle;
  ObTableHandleV2 table_handle;
  ObTxCtxMemtable *memtable = nullptr;

  CreateMemtableArg arg;
  if (NULL == ls_tablet_svr) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get ls tablet svr failed", K(ret));
  } else if (OB_FAIL(ls_tablet_svr->get_tablet(LS_TX_CTX_TABLET, handle))) {
    LOG_WARN("get tablet failed", K(ret));
  } else if (FALSE_IT(tablet = handle.get_obj())) {
  } else if (OB_FAIL(tablet->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_FAIL(ls_tablet_svr->create_memtable(LS_TX_CTX_TABLET, arg/* use default arg */))) {
    LOG_WARN("failed to create memtable", K(ret));
  } else if (OB_FAIL(ls_tablet_svr->get_tx_ctx_memtable_mgr(mgr_handle))) {
    LOG_WARN("failed to get memtable mgr", K(ret));
  } else if (OB_ISNULL(mgr_handle.get_memtable_mgr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get memtable mgr", K(ret));
  } else if (OB_FAIL(mgr_handle.get_memtable_mgr()->get_active_memtable(table_handle))) {
    LOG_WARN("failed to get memtable handle", K(ret));
  } else if (OB_FAIL(table_handle.get_tx_ctx_memtable(memtable))) {
    LOG_WARN("failed to get tx ctx memtable", K(ret));
  } else {
    const ObSSTableArray &sstables = table_store_wrapper.get_member()->get_minor_sstables();

    if (!sstables.empty()) {
      ObStorageMetaHandle sstable_handle;
      ObSSTable *sstable = static_cast<ObSSTable *>(sstables[0]);
      if (sstable->is_loaded()) {
      } else if (OB_FAIL(ObTabletTableStore::load_sstable(sstable->get_addr(), false/*load_co_sstable*/, sstable_handle))) {
        LOG_WARN("fail to load sstable", K(ret), KPC(sstable));
      } else if (OB_FAIL(sstable_handle.get_sstable(sstable))) {
        LOG_WARN("fail to get sstable", K(ret), K(sstable_handle));
      }
      if (FAILEDx(restore_tx_ctx_table_(*sstable))) {
        LOG_WARN("fail to restore tx ctx table", K(ret), KPC(sstable));
      } else {
        memtable->set_max_end_scn(sstable->get_end_scn());
      }
    }
  }

  return ret;
}

int ObTxTable::restore_tx_ctx_table_(ObITable &trans_sstable)
{
  int ret = OB_SUCCESS;
  ObStoreRowIterator *row_iter = NULL;
  const blocksstable::ObDatumRow *row = NULL;

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

  share::schema::ObColDesc meta;
  meta.col_id_ = common::OB_APP_MIN_COLUMN_ID + 1;
  meta.col_type_.set_binary();
  meta.col_order_ = ObOrderType::ASC;

  share::schema::ObColDesc value;
  value.col_id_ = common::OB_APP_MIN_COLUMN_ID + 2;
  value.col_type_.set_binary();
  value.col_order_ = ObOrderType::ASC;

  ObTableIterParam iter_param;
  iter_param.tablet_id_ = LS_TX_CTX_TABLET;
  iter_param.table_id_ = 1;

  if (OB_FAIL(access_context.init(query_flag, store_ctx, allocator, trans_version_range))) {
    LOG_WARN("failed to init access context", K(ret));
  } else if (OB_FAIL(columns.push_back(key))) {
    LOG_WARN("failed to push back key", K(ret), K(key));
  } else if (OB_FAIL(columns.push_back(meta))) {
    LOG_WARN("failed to push back meta", K(ret), K(meta));
  } else if (OB_FAIL(columns.push_back(value))) {
    LOG_WARN("failed to push back value", K(ret), K(value));
  } else if (OB_FAIL(read_info.init(
              allocator,
              LS_TX_CTX_SCHEMA_COLUMN_CNT,
              LS_TX_CTX_SCHEMA_ROWKEY_CNT,
              lib::is_oracle_mode(),
              columns,
              nullptr/*storage_cols_index*/))) {
    LOG_WARN("Fail to init read_info", K(ret));
  } else if (FALSE_IT(iter_param.read_info_ = &read_info)) {
  } else if (OB_FAIL(trans_sstable.scan(iter_param,
                                        access_context,
                                        whole_range,
                                        row_iter))) {
    LOG_WARN("failed to scan trans table", K(ret));
  } else if (NULL == row_iter) {
    // do nothing
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(row_iter->get_next_row(row))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next row", K(ret));
        }
      } else if (OB_FAIL(tx_ctx_table_.recover(*row, tx_data_table_))) {
        LOG_WARN("failed to recover tx ctx table", K(ret));
      }
    }

    if (OB_ITER_END == ret) {
      FLOG_INFO("restore trans table in memory", K(ret), K(trans_sstable));
      ret = OB_SUCCESS;
    }
  }

  if (OB_NOT_NULL(row_iter)) {
    row_iter->~ObStoreRowIterator();
    row_iter = nullptr;
  }

  return ret;
}

void ObTxTable::destroy()
{
  tx_data_table_.destroy();
  tx_ctx_table_.reset();
  ls_id_.reset();
  ls_ = nullptr;
  epoch_ = 0;
  ctx_min_start_scn_info_.reset();
  is_inited_ = false;
}

int ObTxTable::alloc_tx_data(ObTxDataGuard &tx_data_guard, const bool enable_throttle, const int64_t abs_expire_time)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tx table is not init.", KR(ret));
  } else if (OB_FAIL(tx_data_table_.alloc_tx_data(tx_data_guard, enable_throttle, abs_expire_time))) {
    LOG_WARN("allocate tx data from tx data table fail.", KR(ret));
  }
  return ret;
}

int ObTxTable::deep_copy_tx_data(const ObTxDataGuard &in_tx_data_guard, ObTxDataGuard &out_tx_data_guard)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tx table is not init.", KR(ret));
  } else if (OB_FAIL(tx_data_table_.deep_copy_tx_data(in_tx_data_guard, out_tx_data_guard))) {
    LOG_WARN("deep copy tx data from tx data table fail", KR(ret));
  }
  return ret;
}

int ObTxTable::insert(ObTxData *&tx_data)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tx table is not init.", KR(ret), KPC(tx_data), KP(this));
  } else if (OB_FAIL(tx_data_table_.insert(tx_data))) {
    LOG_WARN("allocate tx data from tx data table fail.", KR(ret), KPC(tx_data));
  }
  return ret;
}

int ObTxTable::check_with_tx_data(ObReadTxDataArg &read_tx_data_arg, ObITxDataCheckFunctor &fn)
{
  int ret = OB_SUCCESS;
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_check_tx_status);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tx table is not init.", KR(ret), K(read_tx_data_arg));
    return ret;
  }

  // step 1 : read tx data in mini cache
  int tmp_ret = OB_SUCCESS;
  bool find_tx_data_in_cache = false;
  if (read_tx_data_arg.skip_cache_) {
  } else if (OB_TMP_FAIL(check_tx_data_in_mini_cache_(read_tx_data_arg, fn))) {
    if (OB_TRANS_CTX_NOT_EXIST != tmp_ret) {
      STORAGE_LOG(WARN, "check tx data in mini cache failed", KR(tmp_ret), K(read_tx_data_arg));
    }
  } else {
    STORAGE_LOG(DEBUG, "check tx data in mini cache success", K(read_tx_data_arg), K(fn));
    find_tx_data_in_cache = true;
  }

  // step 2 : read tx data in kv cache
  if (read_tx_data_arg.skip_cache_) {
  } else if (find_tx_data_in_cache) {
    // already find tx data and do function with mini cache
  } else if (OB_TMP_FAIL(check_tx_data_in_kv_cache_(read_tx_data_arg, fn))) {
    if (OB_TRANS_CTX_NOT_EXIST != tmp_ret) {
      STORAGE_LOG(WARN, "check tx data in kv cache failed", KR(tmp_ret), K(read_tx_data_arg));
    }
  } else {
    STORAGE_LOG(DEBUG, "check tx data in kv cache success", K(read_tx_data_arg), K(fn));
    find_tx_data_in_cache = true;
  }

  // step 3 : read tx data in tx_ctx table and tx_data table
  if (find_tx_data_in_cache) {
    // already find tx data and do function with cache
  } else if (OB_FAIL(check_tx_data_in_tables_(read_tx_data_arg, fn))) {
    if (OB_TRANS_CTX_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "check tx data in tables failed", KR(ret), K(ls_id_), K(read_tx_data_arg));
    }
  }

  // step 4 : make sure tx table can be read
  if (OB_SUCC(ret) || OB_TRANS_CTX_NOT_EXIST == ret) {
    check_state_and_epoch_(read_tx_data_arg.tx_id_, read_tx_data_arg.read_epoch_, true /*need_log_error*/, ret);
  }
  return ret;
}

int ObTxTable::check_tx_data_in_mini_cache_(ObReadTxDataArg &read_tx_data_arg, ObITxDataCheckFunctor &fn)
{
  int ret = OB_SUCCESS;
  ObTxData tx_data;
  if (OB_FAIL(read_tx_data_arg.tx_data_mini_cache_.get(read_tx_data_arg.tx_id_, tx_data))) {
    if (OB_LIKELY(OB_TRANS_CTX_NOT_EXIST == ret)) {
      // do nothing when get tx data from mini cache failed
    } else {
      STORAGE_LOG(WARN, "check tx data in mini cache failed", KR(ret), K(read_tx_data_arg), K(tx_data));
    }
  } else {
    EVENT_INC(ObStatEventIds::TX_DATA_HIT_MINI_CACHE_COUNT);
    if (OB_FAIL(fn(tx_data))) {
      STORAGE_LOG(WARN, "check tx data in mini cache failed", KR(ret), K(read_tx_data_arg), K(tx_data));
    }
  }
  return ret;
}

int ObTxTable::check_tx_data_in_kv_cache_(ObReadTxDataArg &read_tx_data_arg, ObITxDataCheckFunctor &fn)
{
  int ret = OB_SUCCESS;
  ObTxDataCacheKey key(MTL_ID(), ls_id_, read_tx_data_arg.tx_id_);
  ObTxDataValueHandle val_handle;

  if (OB_FAIL(OB_TX_DATA_KV_CACHE.get_row(key, val_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_TRANS_CTX_NOT_EXIST;
    } else {
      STORAGE_LOG(WARN, "get row from tx data kv cache failed", KR(ret), K(read_tx_data_arg));
    }
  } else {
    // get tx data from kv cache succeed. fetch tx data from cache value and do functor
    const ObTxDataCacheValue *cache_val = val_handle.value_;
    const ObTxData *tx_data = nullptr;
    if (OB_ISNULL(cache_val)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "cache value is nullptr", KR(ret), K(read_tx_data_arg), K(ls_id_), K(val_handle));
    } else if (OB_ISNULL(tx_data = cache_val->get_tx_data())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "tx data in cache value is nullptr", KR(ret), K(read_tx_data_arg), K(ls_id_), KPC(cache_val));
    } else {
      EVENT_INC(ObStatEventIds::TX_DATA_HIT_KV_CACHE_COUNT);
      ret = fn(*tx_data);

      if (ObTxData::RUNNING == tx_data->state_) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "read an unexpected state tx data from kv cache");
      } else if (!tx_data->op_guard_.is_valid()) {
        // put into mini cache only if this tx data do not have undo actions
        read_tx_data_arg.tx_data_mini_cache_.set(*tx_data);
      }
    }
  }

  return ret;
}

int ObTxTable::check_tx_data_in_tables_(ObReadTxDataArg &read_tx_data_arg, ObITxDataCheckFunctor &fn)
{
  int ret = OB_SUCCESS;

  if (OB_SUCC(tx_ctx_table_.check_with_tx_data(read_tx_data_arg.tx_id_, fn))) {
    EVENT_INC(ObStatEventIds::TX_DATA_READ_TX_CTX_COUNT);
    TRANS_LOG(DEBUG, "tx ctx table check with tx data succeed", K(read_tx_data_arg), K(fn));
  } else if (OB_TRANS_CTX_NOT_EXIST == ret) {
    ObTxDataGuard tx_data_guard;
    ObTxData *tx_data = nullptr;
    SCN recycled_scn;

    // read tx data from tx data table and get tx data guard
    if (OB_FAIL(tx_data_table_.check_with_tx_data(read_tx_data_arg.tx_id_, fn, tx_data_guard, recycled_scn))) {
      if (OB_ITER_END == ret) {
        ret = OB_TRANS_CTX_NOT_EXIST;
      }
      STORAGE_LOG(WARN,
                  "check with tx data in tx data table failed",
                  KR(ret),
                  K(ls_id_),
                  K(read_tx_data_arg),
                  K(tx_data_guard),
                  K(recycled_scn),
                  "state", get_state_string(state_));
    } else if (OB_NOT_NULL(tx_data = tx_data_guard.tx_data())) {
      // if tx data is not null, put tx data into cache
      if (ObTxData::RUNNING == tx_data->state_) {
      } else {
        if (!tx_data->op_guard_.is_valid()) {
          read_tx_data_arg.tx_data_mini_cache_.set(*tx_data);
        }

        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(put_tx_data_into_kv_cache_(*tx_data))) {
          STORAGE_LOG(WARN, "put tx data into kv cache failed", KR(tmp_ret), KPC(tx_data));
        }
      }
    }
  }

  return ret;
}

int ObTxTable::put_tx_data_into_kv_cache_(const ObTxData &tx_data)
{
  int ret = OB_SUCCESS;
  const ObTxDataCacheKey key(MTL_ID(), ls_id_, tx_data.tx_id_);
  ObTxDataCacheValue cache_value;

  if (OB_FAIL(cache_value.init(tx_data))) {
    STORAGE_LOG(WARN, "init tx data cache value failed", KR(ret), K(key), K(cache_value));
  } else if (OB_FAIL(OB_TX_DATA_KV_CACHE.put_row(key, cache_value))) {
    STORAGE_LOG(WARN, "put tx data cache value failed", KR(ret), K(key), K(cache_value));
  } else {
    STORAGE_LOG(INFO, "finish put tx data into kv cache", K(key), K(cache_value));
    // put tx data into cache succeed
  }

  return ret;
}

void ObTxTable::check_state_and_epoch_(const transaction::ObTransID tx_id,
                                       const int64_t read_epoch,
                                       const bool need_log_error,
                                       int &ret)
{
  UNUSED(need_log_error);
  // do not need atomic load because:
  // 1. read_epoch is acquired before check_with_tx_data()
  // 2. state is modified after epoch
  TxTableState state = state_;
  int64_t epoch = epoch_;

  if (OB_UNLIKELY(read_epoch != epoch)) {
    // offline or online has been executed on this tx table, return a specific error code to retry
    ret = OB_REPLICA_NOT_READABLE;
    LOG_WARN("tx table epoch changed", KR(ret), K(ls_id_), "state", get_state_string(state), K(read_epoch), K(epoch));
  } else if (OB_UNLIKELY(TxTableState::ONLINE != state)) {
    ret = OB_REPLICA_NOT_READABLE;
    LOG_WARN("tx table is not online", KR(ret), K(ls_id_), "state", get_state_string(state), K(read_epoch), K(epoch));
  } else if (OB_FAIL(ret)) {
    SCN max_decided_scn = SCN::invalid_scn();
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(ls_->get_max_decided_scn(max_decided_scn))) {
      LOG_WARN("get max decided scn failed", KR(tmp_ret));
    }
    LOG_WARN("check with tx data failed.", KR(ret), K(tx_id), K(read_epoch), K(max_decided_scn), K(ls_id_), KPC(this));
  }
}

int ObTxTable::get_tx_table_guard(ObTxTableGuard &guard) { return guard.init(this); }

int64_t ObTxTable::get_filter_col_idx()
{
  return TX_DATA_END_TS_COLUMN + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
}

int ObTxTable::check_row_locked(ObReadTxDataArg &read_tx_data_arg,
                                const transaction::ObTransID &read_tx_id,
                                const transaction::ObTxSEQ sql_sequence,
                                storage::ObStoreRowLockState &lock_state)
{
  CheckRowLockedFunctor fn(read_tx_id, read_tx_data_arg.tx_id_, sql_sequence, lock_state);
  int ret = check_with_tx_data(read_tx_data_arg, fn);
  LOG_DEBUG("finish check row locked", K(read_tx_data_arg), K(read_tx_id), K(sql_sequence), K(lock_state));
  return ret;
}

int ObTxTable::check_sql_sequence_can_read(ObReadTxDataArg &read_tx_data_arg,
                                           const transaction::ObTxSEQ &sql_sequence,
                                           bool &can_read)
{
  CheckSqlSequenceCanReadFunctor fn(sql_sequence, can_read);
  int ret = check_with_tx_data(read_tx_data_arg, fn);
  LOG_DEBUG("finish check sql sequence can read", K(read_tx_data_arg), K(sql_sequence), K(can_read));
  return ret;
}

int ObTxTable::get_tx_state_with_scn(ObReadTxDataArg &read_tx_data_arg,
                                     const SCN scn,
                                     int64_t &state,
                                     SCN &trans_version)
{
  GetTxStateWithSCNFunctor fn(scn, state, trans_version);
  int ret = check_with_tx_data(read_tx_data_arg, fn);
  LOG_DEBUG("finish get tx state with scn", K(read_tx_data_arg), K(scn), K(state), K(trans_version));
  return ret;
}

int ObTxTable::try_get_tx_state(ObReadTxDataArg &read_tx_data_arg,
                                int64_t &state,
                                SCN &trans_version,
                                SCN &recycled_scn)
{
  int ret = OB_SUCCESS;
  GetTxStateWithSCNFunctor fn(SCN::max_scn(), state, trans_version);
  fn.set_may_exist_undecided_state_in_tx_data_table();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tx table is not init.", KR(ret), K(read_tx_data_arg));
  } else {
    ObTxDataGuard tx_data_guard;
    ret = tx_data_table_.check_with_tx_data(read_tx_data_arg.tx_id_, fn, tx_data_guard, recycled_scn);
    if (OB_ITER_END == ret) {
      ret = OB_TRANS_CTX_NOT_EXIST;
    }
  }

  check_state_and_epoch_(read_tx_data_arg.tx_id_, read_tx_data_arg.read_epoch_, false /*need_log_error*/, ret);
  return ret;
}

int ObTxTable::lock_for_read(ObReadTxDataArg &read_tx_data_arg,
                             const transaction::ObLockForReadArg &lock_for_read_arg,
                             bool &can_read,
                             SCN &trans_version,
                             ObCleanoutOp &cleanout_op,
                             ObReCheckOp &recheck_op)
{
  LockForReadFunctor fn(lock_for_read_arg,
                        can_read,
                        trans_version,
                        ls_id_,
                        cleanout_op,
                        recheck_op);
  int ret = check_with_tx_data(read_tx_data_arg, fn);
  LOG_DEBUG("finish lock for read", K(lock_for_read_arg), K(can_read), K(trans_version));
  return ret;
}

int ObTxTable::get_recycle_scn(SCN &sn_recycle_scn)
{
  int ret = OB_SUCCESS;
  sn_recycle_scn = SCN::min_scn();

  int64_t current_time_us = ObClockGenerator::getClock();
  // update recycle_scn_cache_ if needed
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
#else
  if (current_time_us - recycle_scn_cache_.update_ts_ > MIN_INTERVAL_OF_TX_DATA_RECYCLE_US) {
#endif
    int64_t tx_result_retention_s = DEFAULT_TX_RESULT_RETENTION_S;
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    if (tenant_config.is_valid()) {
      // use config value if config is valid
      tx_result_retention_s = tenant_config->_tx_result_retention;
    }

    SCN new_recycle_scn = SCN::min_scn();
    if (OB_FAIL(get_recycle_scn_(tx_result_retention_s, new_recycle_scn))) {
      STORAGE_LOG(WARN, "get recycle scn failed", KR(ret), K(ls_id_), KP(this));
    } else if (new_recycle_scn.is_valid_and_not_min()) {
      recycle_scn_cache_.val_.atomic_store(new_recycle_scn);
      recycle_scn_cache_.update_ts_ = ObClockGenerator::getClock();
    } else {
      ret = OB_EAGAIN;
      STORAGE_LOG(WARN,
                  "min_scn no need update to cache",
                  KR(ret),
                  K(new_recycle_scn),
                  K(ls_id_),
                  K(recycle_scn_cache_),
                  K(recycle_record_));
    }
    FLOG_INFO("finish update recycle_scn_cache", K(ret), K(ls_id_), K(new_recycle_scn));
  }

  if (OB_FAIL(ret)) {
    sn_recycle_scn.set_min();
  } else {
    sn_recycle_scn = recycle_scn_cache_.val_.atomic_load();
  }
  FLOG_INFO("finish get recycle_scn", K(ret), K(sn_recycle_scn));

  // always return OB_SUCCESS
  return OB_SUCCESS;
}

int ObTxTable::get_recycle_scn_(const int64_t tx_result_retention_s, share::SCN &real_recycle_scn)
{
  int ret = OB_SUCCESS;
  TxTableState state;
  int64_t prev_epoch = ATOMIC_LOAD(&epoch_);
  int64_t after_epoch = 0;
  SCN tablet_recycle_scn = SCN::min_scn();

  if (OB_FAIL(tx_data_table_.get_recycle_scn(tablet_recycle_scn))) {
    TRANS_LOG(WARN, "get recycle scn from tx data table failed.", KR(ret));
  } else if (FALSE_IT(state = ATOMIC_LOAD(&state_))) {
  } else if (FALSE_IT(after_epoch = ATOMIC_LOAD(&epoch_))) {
  } else if (TxTableState::ONLINE != state || prev_epoch != after_epoch) {
    real_recycle_scn = SCN::min_scn();
    ret = OB_REPLICA_NOT_READABLE;
    STORAGE_LOG(WARN,
                "this tx table is migrating or has migrated",
                KR(ret),
                K(ls_id_),
                K(state),
                K(prev_epoch),
                K(after_epoch));
  } else {
    SCN delay_recycle_scn = SCN::max_scn();
    const int64_t current_time_ns = ObClockGenerator::getClock() * 1000L;
    delay_recycle_scn.convert_for_tx(current_time_ns - (tx_result_retention_s * 1000L * 1000L * 1000L));
    real_recycle_scn = SCN::min(delay_recycle_scn, tablet_recycle_scn);
  }

  return ret;
}

void ObTxTable::reset_ctx_min_start_scn_info_()
{
  SpinWLockGuard lock_guard(ctx_min_start_scn_info_.lock_);
  ctx_min_start_scn_info_.reset();
}

int ObTxTable::get_uncommitted_tx_min_start_scn(share::SCN &min_start_scn, share::SCN &effective_scn)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard lock_guard(ctx_min_start_scn_info_.lock_);
  min_start_scn = ctx_min_start_scn_info_.min_start_scn_in_ctx_;
  effective_scn = ctx_min_start_scn_info_.keep_alive_scn_;
  if (effective_scn.is_min()) {
    ret = OB_EAGAIN;
  }
  return ret;
}

void ObTxTable::update_min_start_scn_info(const SCN &max_decided_scn)
{
  if (true == ATOMIC_LOAD(&calc_upper_trans_is_disabled_)) {
    // quit updating if calculate upper trans versions disabled
    STORAGE_LOG(INFO, "skip update min start scn", K(max_decided_scn), KPC(this));
    return;
  }

  int64_t cur_ts = ObClockGenerator::getClock();
  SpinWLockGuard lock_guard(ctx_min_start_scn_info_.lock_);

  // recheck update condition and do update calc_upper_info
  if (cur_ts - ctx_min_start_scn_info_.update_ts_ > ObTxTable::UPDATE_MIN_START_SCN_INTERVAL &&
      max_decided_scn > ctx_min_start_scn_info_.keep_alive_scn_) {
    SCN min_start_scn = SCN::min_scn();
    SCN keep_alive_scn = SCN::min_scn();
    MinStartScnStatus status;
    (void)ls_->get_min_start_scn(min_start_scn, keep_alive_scn, status);

    if (MinStartScnStatus::UNKOWN == status) {
      // do nothing
    } else {
      int ret = OB_SUCCESS;
      CtxMinStartScnInfo tmp_min_start_scn_info;
      tmp_min_start_scn_info.keep_alive_scn_ = keep_alive_scn;
      tmp_min_start_scn_info.update_ts_ = cur_ts;
      if (MinStartScnStatus::NO_CTX == status) {
        // use the previous keep_alive_scn as min_start_scn
        tmp_min_start_scn_info.min_start_scn_in_ctx_ = ctx_min_start_scn_info_.keep_alive_scn_;
      } else if (MinStartScnStatus::HAS_CTX == status) {
        tmp_min_start_scn_info.min_start_scn_in_ctx_ = min_start_scn;
      } else {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "invalid min start scn status", K(min_start_scn), K(keep_alive_scn), K(status));
      }

      if (OB_FAIL(ret)) {
      } else if (tmp_min_start_scn_info.min_start_scn_in_ctx_ < ctx_min_start_scn_info_.min_start_scn_in_ctx_) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "invalid min start scn", K(tmp_min_start_scn_info), K(ctx_min_start_scn_info_));
      } else {
        ctx_min_start_scn_info_ = tmp_min_start_scn_info;
      }
    }
  }

  STORAGE_LOG(INFO, "finish update min start scn", K(max_decided_scn), K(ctx_min_start_scn_info_));
}


bool ObTxTable::can_recycle_tx_data(const share::SCN current_recycle_scn, const bool is_local_exec_mode)
{
  bool can_recycle = false;
  SCN last_recycled_scn;
  int64_t last_recycled_ts;
  if (is_local_exec_mode) {
    last_recycled_scn = recycle_record_.sn_recycled_scn_;
    last_recycled_ts = recycle_record_.sn_recycled_ts_;
  } else {
    last_recycled_scn = recycle_record_.ss_recycled_scn_;
    last_recycled_ts = recycle_record_.ss_recycled_ts_;
  }

  const int64_t current_time = ObClockGenerator::getClock();
  if (current_recycle_scn.is_min() || current_time - last_recycled_ts < MIN_INTERVAL_OF_TX_DATA_RECYCLE_US ||
      current_recycle_scn.convert_to_ts() - last_recycled_scn.convert_to_ts() < MIN_INTERVAL_OF_TX_DATA_RECYCLE_US) {
    can_recycle = false;
    if (REACH_TIME_INTERVAL(10LL * 1000LL * 1000LL)) {
      FLOG_INFO("current_recycle_scn is close to last recycle scn, skip recycle once",
                K(ls_id_),
                KTIME(current_recycle_scn.convert_to_ts()),
                KTIME(last_recycled_scn.convert_to_ts()));
    }
  } else {
    can_recycle = true;
  }

  return can_recycle;
}

void ObTxTable::record_tx_data_recycle_scn(const share::SCN current_recycle_scn, const bool is_local_exec_mode)
{
  int64_t const current_ts = ObClockGenerator::getClock();
  SCN last_recycled_scn;
  int64_t last_recycled_ts;

  if (is_local_exec_mode) {
    last_recycled_scn = recycle_record_.sn_recycled_scn_;
    last_recycled_ts = recycle_record_.sn_recycled_ts_;
    recycle_record_.sn_recycled_scn_ = current_recycle_scn;
    recycle_record_.sn_recycled_ts_ = current_ts;
  } else {
    last_recycled_scn = recycle_record_.ss_recycled_scn_;
    last_recycled_ts = recycle_record_.ss_recycled_ts_;
    recycle_record_.ss_recycled_scn_ = current_recycle_scn;
    recycle_record_.ss_recycled_ts_ = current_ts;
  }

  FLOG_INFO("finish record tx data recycle scn",
            K(is_local_exec_mode),
            K(ls_id_),
            K(last_recycled_scn),
            KTIME(last_recycled_ts),
            K(current_recycle_scn),
            KTIME(current_ts));
}

#define SKIP_CALC_LOG(LOG_LEVEL) \
  STORAGE_LOG(LOG_LEVEL, "get upper trans version", K(ls_id), K(calc_upper_trans_is_disabled_), K(sstable_end_scn))
int ObTxTable::get_upper_trans_version_before_given_scn(const SCN sstable_end_scn,
                                                        SCN &upper_trans_version,
                                                        const bool force_print_log)
{
  int ret = OB_SUCCESS;
  const ObLSID ls_id = ls_->get_ls_id();
  if (ATOMIC_LOAD(&calc_upper_trans_is_disabled_)) {
    // cannot calculate upper trans version right now
    if (TC_REACH_TIME_INTERVAL(1LL * 1000LL * 1000LL)) {
      STORAGE_LOG(INFO, "calc upper trans version is disabled",
                  K(ls_id), K(calc_upper_trans_is_disabled_),
                  K(sstable_end_scn));
    }
    if (force_print_log) {
      SKIP_CALC_LOG(INFO);
    } else {
      SKIP_CALC_LOG(TRACE);
    }
  } else {
    ret = tx_data_table_.get_upper_trans_version_before_given_scn(sstable_end_scn,
                                                                  upper_trans_version,
                                                                  force_print_log);
  }
  return ret;
}
#undef SKIP_CALC_LOG

void ObTxTable::disable_upper_trans_calculation()
{
  ATOMIC_STORE(&calc_upper_trans_is_disabled_, true);
  (void)tx_data_table_.disable_upper_trans_calculation();
  reset_ctx_min_start_scn_info_();
  FLOG_INFO("disable upper trans version calculation", KPC(this));
}

void ObTxTable::enable_upper_trans_calculation(const share::SCN latest_transfer_scn)
{
  reset_ctx_min_start_scn_info_();
  (void)tx_data_table_.enable_upper_trans_calculation(latest_transfer_scn);
  ATOMIC_STORE(&calc_upper_trans_is_disabled_, false);
  FLOG_INFO("enable upper trans version calculation", KPC(this));
}

int ObTxTable::get_start_tx_scn(SCN &start_tx_scn)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tx_data_table_.get_start_tx_scn(start_tx_scn))) {
    STORAGE_LOG(WARN, "get start tx scn failed", KR(ret));
  }
  return ret;
}

int ObTxTable::cleanout_tx_node(ObReadTxDataArg &read_tx_data_arg,
                                memtable::ObMvccRow &value,
                                memtable::ObMvccTransNode &tnode,
                                const bool need_row_latch)
{
  ObCleanoutTxNodeOperation op(value, tnode, need_row_latch);
  CleanoutTxStateFunctor fn(tnode.seq_no_, op);
  int ret = check_with_tx_data(read_tx_data_arg, fn);
  if (OB_TRANS_CTX_NOT_EXIST == ret) {
    if (tnode.is_committed() || tnode.is_aborted()) {
      // may be the concurrent case between cleanout and commit/abort
      ret = OB_SUCCESS;
    }
  }

  if (OB_SUCC(ret)) {
    if (op.need_cleanout()) {
      op(fn.get_tx_data_check_data());
    }
  }
  return ret;
}

int ObTxTable::supplement_tx_op_if_exist(ObTxData *tx_data)
{
  return tx_data_table_.supplement_tx_op_if_exist(tx_data);
}

int ObTxTable::self_freeze_task() { return tx_data_table_.self_freeze_task(); }

int ObTxTable::generate_virtual_tx_data_row(const transaction::ObTransID tx_id, observer::VirtualTxDataRow &row_data)
{
  GenerateVirtualTxDataRowFunctor fn(row_data);
  ObTxDataMiniCache mini_cache;
  ObReadTxDataArg read_tx_data_arg(tx_id, epoch_, mini_cache, true);
  int ret = check_with_tx_data(read_tx_data_arg, fn);
  return ret;
}

int ObTxTable::dump_single_tx_data_2_text(const int64_t tx_id_int, const char *fname)
{
  int ret = OB_SUCCESS;
  STORAGE_LOG(INFO, "start dump single tx data");
  char real_fname[OB_MAX_FILE_NAME_LENGTH];
  FILE *fd = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tx table is not init.", KR(ret), K(tx_id_int));
  } else if (OB_ISNULL(fname)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "fanme is NULL");
  } else if (snprintf(
                 real_fname, sizeof(real_fname), "%s.%ld", fname, ::oceanbase::common::ObTimeUtility::current_time()) >=
             (int64_t)sizeof(real_fname)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "fname too long", K(fname));
  } else if (NULL == (fd = fopen(real_fname, "w"))) {
    ret = OB_IO_ERROR;
    STORAGE_LOG(WARN, "open file fail:", K(fname));
  } else {
    int64_t ls_id = ls_->get_ls_id().id();
    int64_t tenant_id = MTL_ID();
    fprintf(fd, "tenant_id=%ld ls_id=%ld\n", tenant_id, ls_id);

    if (OB_SUCC(tx_ctx_table_.dump_single_tx_data_2_text(tx_id_int, fd))) {
    } else if (OB_TRANS_CTX_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      ret = tx_data_table_.dump_single_tx_data_2_text(tx_id_int, fd);
    }
  }

  if (NULL != fd) {
    fclose(fd);
    fd = NULL;
  }
  if (OB_FAIL(ret)) {
    STORAGE_LOG(WARN, "dump single tx data fail", K(fname), KR(ret));
  }

  return ret;
}

const char *ObTxTable::get_state_string(const int64_t state) const
{
  STATIC_ASSERT(TxTableState::OFFLINE == 0, "Invalid State Enum");
  STATIC_ASSERT(TxTableState::ONLINE == 1, "Invalid State Enum");
  STATIC_ASSERT(TxTableState::PREPARE_OFFLINE == 2, "Invalid State Enum");
  STATIC_ASSERT(TxTableState::STATE_CNT == 3, "Invalid State Enum");
  const static int64_t cnt = TxTableState::STATE_CNT;
  const static char STATE_TO_CHAR[cnt][32] = {"OFFLINE", "ONLINE", "PREPARE_OFFLINE"};
  return STATE_TO_CHAR[int(state)];
}

int ObTxTable::get_tx_data_sstable_recycle_scn(share::SCN &recycle_scn)
{
  int ret = OB_SUCCESS;
  recycle_scn.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tx table is not init.", KR(ret));
  } else if (OB_FAIL(tx_data_table_.get_sstable_recycle_scn(recycle_scn))) {
    TRANS_LOG(WARN, "get recycle scn from tx data table failed.", KR(ret));
  }
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE

/**
 * @brief In shared storage mode, local TxData is not recycled,
 *       leaving the RecycleRecord empty. Before retrieving the ss_recycle_scn,
 *       first obtain the sn_recycle_scn via get_recycle_scn(), record it into
 *       recycle_record, then read the final value from the record.
 */
int ObTxTable::get_ss_recycle_scn(share::SCN &ss_recycle_scn)
{
  int ret = OB_SUCCESS;
  ss_recycle_scn.set_min();
  SCN sn_recycle_scn;
  if (!GCTX.is_shared_storage_mode()) {
    ret = OB_NOT_SUPPORTED;
    TRANS_LOG(ERROR, "invalid get ss recycle scn", KR(ret));
  } else if (OB_FAIL(get_recycle_scn(sn_recycle_scn))) {
    TRANS_LOG(WARN, "get recycle_scn failed", KR(ret), K(ss_recycle_scn));
  } else if (FALSE_IT(ss_recycle_scn = sn_recycle_scn)) {
  } else if (OB_FAIL(resolve_shared_storage_upload_info_(ss_recycle_scn))) {
    TRANS_LOG(ERROR, "failed to resolve shared_storage_upload_info", KR(ret), K(ss_recycle_scn));
  }
  FLOG_INFO("finish get ss_recycle_scn", K(ret), K(sn_recycle_scn), K(ss_recycle_scn), K(ls_id_));
  return ret;
}

int ObTxTable::resolve_shared_storage_upload_info_(share::SCN &ss_recycle_scn)
{
  int ret = OB_SUCCESS;
  const int64_t SHARED_STORAGE_USE_CACHE_DURATION = 1_hour;

  SCN ss_checkpoint_scn = SCN::invalid_scn();
  SCN tx_data_table_upload_scn = SCN::invalid_scn();
  SCN data_upload_min_end_scn = SCN::invalid_scn();
  int64_t origin_time = ATOMIC_LOAD(&(ss_upload_scn_cache_.update_ts_));
  int64_t current_time = ObClockGenerator::getClock();

  if (current_time - origin_time > SHARED_STORAGE_USE_CACHE_DURATION
      && ATOMIC_BCAS(&(ss_upload_scn_cache_.update_ts_), origin_time, current_time)) {
    share::SCN transfer_scn;
    {
      ObTabletHandle tablet_handle;
      if (OB_FAIL(ls_->get_tablet_svr()->get_tablet(LS_TX_DATA_TABLET, tablet_handle))) {
        TRANS_LOG(WARN, "get tx data tablet fail", K(ret));
      } else {
        transfer_scn = tablet_handle.get_obj()->get_reorganization_scn();
      }
    }
    // Tip1: may output min_scn if no uploads exists or max_uploaded_scn if
    // there exists
    ObSSLSMeta ss_ls_meta;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(MTL(ObSSMetaService *)->get_ls_meta(ls_id_, ss_ls_meta))) {
      TRANS_LOG(WARN, "get ls meta failed", KR(ret), K(ss_ls_meta));
    } else if (FALSE_IT(ss_checkpoint_scn = SCN::scn_dec(ss_ls_meta.get_ss_checkpoint_scn()))) {
    } else if (OB_FAIL(ls_->get_inc_sstable_uploader().
                get_tablet_upload_pos(LS_TX_DATA_TABLET,
                                      transfer_scn,
                                      tx_data_table_upload_scn))) {
      TRANS_LOG(WARN, "get tablet upload pos failed", K(ret));
    // Tip2: may output min_scn if no uploads exists or max_uploaded_scn if
    // there exists
    } else if (OB_FAIL(ls_->get_inc_sstable_uploader().
                       get_upload_min_end_scn_from_ss(data_upload_min_end_scn))) {
      TRANS_LOG(WARN, "get tablet upload min end scn failed", K(ret));
    // We need ensure that no concurrent user after an hour later will
    // concurrently change the cache
    } else if (ATOMIC_BCAS(&(ss_upload_scn_cache_.update_ts_),
                           current_time, current_time + 1)) {
      ss_upload_scn_cache_.ss_checkpoint_scn_cache_.atomic_store(ss_checkpoint_scn);
      ss_upload_scn_cache_.tx_table_upload_max_scn_cache_.atomic_store(tx_data_table_upload_scn);
      ss_upload_scn_cache_.data_upload_min_end_scn_cache_.atomic_store(data_upload_min_end_scn);
      TRANS_LOG(WARN, "ss_upload_scn updated successfully", K(ss_upload_scn_cache_));
    } else {
      TRANS_LOG(WARN, "ss_upload_scn_cache is updated by a concurrent user after an hour!!!",
                K(ss_upload_scn_cache_));
    }

    if (OB_FAIL(ret)) {
      if (!ATOMIC_BCAS(&(ss_upload_scn_cache_.update_ts_), current_time, origin_time)) {
        TRANS_LOG(WARN, "ss_upload_scn_cache is updated by a concurrent user after an hour!!!",
                  K(ss_upload_scn_cache_));
      }
      // under error during get upload info, we choose to use cache instead
      ret = OB_SUCCESS;
    }
  }


  if (OB_SUCC(ret)) {
    ss_checkpoint_scn = ss_upload_scn_cache_.ss_checkpoint_scn_cache_.atomic_load();
    tx_data_table_upload_scn = ss_upload_scn_cache_.tx_table_upload_max_scn_cache_.atomic_load();
    data_upload_min_end_scn = ss_upload_scn_cache_.data_upload_min_end_scn_cache_.atomic_load();

    // Tip1: we need to obtain the ss checkpoint location for the ls to ensuare
    // that all data before the checkpoint has been upload. It is important to
    // note that this is a correctness requirement. Otherwise the tablet with no
    // mini and minor will not be calculated
    if (!ss_checkpoint_scn.is_valid()) {
      TRANS_LOG(WARN, "ss_checkpoint_scn is invalid", K(ss_upload_scn_cache_));
    } else if (ss_checkpoint_scn < ss_recycle_scn) {
      ss_recycle_scn = ss_checkpoint_scn;
    }

    // Tip2: we need to obtain the upload location for the tx_data_table to
    // ensure that transaction data that hasn't been uploaded is not recycled,
    // thereby maintaining the integrity of transaction data on shared storage.
    // It is important to note that this is not a correctness requirement but
    // rather for facilitating better troubleshooting in the future.
    if (!tx_data_table_upload_scn.is_valid()) {
      // we havenot aleady upload anything in cache
    } else if (tx_data_table_upload_scn < ss_recycle_scn) {
      ss_recycle_scn = tx_data_table_upload_scn;
    }

    // Tip3: we need to obtain the smallest end_scn among all tablets that have
    // uploaded its sstables. This ensures that uncommitted data, which has not
    // been backfilled, can certainly be interpreted by the transaction data on
    // shared storage. It is important to note that this is a correctness
    // requirement. By also ensuring that computing nodes do not reclaim
    // transaction data, we can guarantee that any data retrieved from shared
    // storage can be interpreted properly.
    if (!data_upload_min_end_scn.is_valid()) {
      // we havenot aleady upload anything in cache
    } else if (data_upload_min_end_scn < ss_recycle_scn) {
      ss_recycle_scn = data_upload_min_end_scn;
    }

    if (!ss_checkpoint_scn.is_valid() &&
        !tx_data_table_upload_scn.is_valid() &&
        !data_upload_min_end_scn.is_valid()) {
      ss_recycle_scn = share::SCN::min_scn();
      LOG_INFO("resolve_shared_storage_upload_info_ find invalid tx data info",
               K(ss_upload_scn_cache_), K(ss_recycle_scn));
    }

    FLOG_INFO("resolve_shared_storage_upload_info_", K(ss_upload_scn_cache_), K(ss_recycle_scn), K(ls_id_));
  }

  return ret;
}
#endif
// *********************** ObTxTable end. ************************

}  // namespace storage
}  // namespace oceanbase
