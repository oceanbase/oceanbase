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
#include "storage/tx_table/ob_tx_table.h"

#include "share/ob_ls_id.h"
#include "share/schema/ob_table_schema.h"
#include "storage/ls/ob_ls.h"
#include "storage/memtable/mvcc/ob_mvcc_ctx.h"
#include "storage/access/ob_store_row_iterator.h"
#include "storage/tx/ob_trans_ctx_mgr.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx/ob_tx_data_functor.h"
#include "storage/tx_table/ob_tx_data_cache.h"
#include "storage/tx_table/ob_tx_table_define.h"
#include "storage/tx_table/ob_tx_table_iterator.h"
#include "storage/tx_table/ob_tx_table_interface.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_iterator.h"

namespace oceanbase {
using namespace share;
using namespace palf;
namespace storage {
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
    state_ = TxTableState::ONLINE;
    mini_cache_hit_cnt_ = 0;
    kv_cache_hit_cnt_ = 0;
    read_tx_data_table_cnt_ = 0;
    recycle_scn_cache_.reset();
    LOG_INFO("init tx table successfully", K(ret), K(ls->get_ls_id()));
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
    recycle_scn_cache_.reset();
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
  } else if (OB_FAIL(load_tx_data_table_())) {
    LOG_WARN("failed to load tx data table", K(ret));
  } else if (OB_FAIL(load_tx_ctx_table_())) {
    LOG_WARN("failed to load tx ctx table", K(ret));
  } else {
    recycle_scn_cache_.reset();
    ATOMIC_STORE(&state_, ObTxTable::ONLINE);
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
  if (OB_FAIL(get_ctx_table_schema_(tenant_id, table_schema))) {
    LOG_WARN("get ctx table schema failed", K(ret));
  } else if (OB_FAIL(ls_->create_ls_inner_tablet(ls_id,
                                                 LS_TX_CTX_TABLET,
                                                 ObLS::LS_INNER_TABLET_FROZEN_SCN,
                                                 table_schema,
                                                 compat_mode,
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
  if (OB_FAIL(get_data_table_schema_(tenant_id, table_schema))) {
    LOG_WARN("get data table schema failed", K(ret));
  } else if (OB_FAIL(ls_->create_ls_inner_tablet(ls_id,
                                                 LS_TX_DATA_TABLET,
                                                 ObLS::LS_INNER_TABLET_FROZEN_SCN,
                                                 table_schema,
                                                 compat_mode,
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
    }
    if (OB_FAIL(remove_tablet_(LS_TX_CTX_TABLET))) {
      LOG_WARN("remove tx ctx tablet failed", K(ret));
    }
  }
  return ret;
}

int ObTxTable::load_tx_ctx_table_()
{
  int ret = OB_SUCCESS;
  ObTabletHandle handle;
  ObTablet *tablet;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ObLSTabletService *ls_tablet_svr = ls_->get_tablet_svr();

  if (NULL == ls_tablet_svr) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get ls tablet svr failed", K(ret));
  } else if (OB_FAIL(ls_tablet_svr->get_tablet(LS_TX_CTX_TABLET, handle))) {
    LOG_WARN("get tablet failed", K(ret));
  } else if (FALSE_IT(tablet = handle.get_obj())) {
  } else if (OB_FAIL(tablet->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_FAIL(ls_tablet_svr->create_memtable(LS_TX_CTX_TABLET, 0 /* schema_version */))) {
    LOG_WARN("failed to create memtable", K(ret));
  } else {
    const ObSSTableArray &sstables = table_store_wrapper.get_member()->get_minor_sstables();

    if (!sstables.empty()) {
      ObStorageMetaHandle sstable_handle;
      ObSSTable *sstable = static_cast<ObSSTable *>(sstables[0]);
      if (sstable->is_loaded()) {
      } else if (OB_FAIL(ObTabletTableStore::load_sstable(sstable->get_addr(), sstable_handle))) {
        LOG_WARN("fail to load sstable", K(ret), KPC(sstable));
      } else if (OB_FAIL(sstable_handle.get_sstable(sstable))) {
        LOG_WARN("fail to get sstable", K(ret), K(sstable_handle));
      }
      if (FAILEDx(restore_tx_ctx_table_(*sstable))) {
        LOG_WARN("fail to restore tx ctx table", K(ret), KPC(sstable));
      }
    }
  }

  return ret;
}

int ObTxTable::load_tx_data_table_()
{
  int ret = OB_SUCCESS;
  ObTabletHandle handle;
  ObTablet *tablet;
  ObLSTabletService *ls_tablet_svr = ls_->get_tablet_svr();

  if (NULL == ls_tablet_svr) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get ls tablet svr failed", K(ret));
  } else if (OB_FAIL(ls_tablet_svr->get_tablet(LS_TX_DATA_TABLET, handle))) {
    LOG_WARN("get tablet failed", K(ret));
  } else if (FALSE_IT(tablet = handle.get_obj())) {
  } else if (OB_FAIL(ls_tablet_svr->create_memtable(LS_TX_DATA_TABLET, 0 /* schema_version */))) {
    LOG_WARN("failed to create memtable", K(ret));
  } else {
    // load tx data table succed
  }

  return ret;
}

int ObTxTable::load_tx_table()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tx table is not init.", KR(ret));
  } else if (OB_FAIL(load_tx_data_table_())) {
    LOG_WARN("failed to load tx data table", K(ret));
  } else if (OB_FAIL(load_tx_ctx_table_())) {
    LOG_WARN("failed to load tx ctx table", K(ret));
  } else {
    // do nothing
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
  is_inited_ = false;
}

int ObTxTable::alloc_tx_data(ObTxDataGuard &tx_data_guard)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tx table is not init.", KR(ret));
  } else if (OB_FAIL(tx_data_table_.alloc_tx_data(tx_data_guard))) {
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

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tx table is not init.", KR(ret), K(read_tx_data_arg));
    return ret;
  }

  // step 1 : read tx data in mini cache
  int tmp_ret = OB_SUCCESS;
  bool find_tx_data_in_cache = false;
  if (OB_TMP_FAIL(check_tx_data_in_mini_cache_(read_tx_data_arg, fn))) {
    if (OB_TRANS_CTX_NOT_EXIST != tmp_ret) {
      STORAGE_LOG(WARN, "check tx data in mini cache failed", KR(tmp_ret), K(read_tx_data_arg));
    }
  } else {
    STORAGE_LOG(DEBUG, "check tx data in mini cache success", K(read_tx_data_arg), K(fn));
    find_tx_data_in_cache = true;
  }

  // step 2 : read tx data in kv cache
  if (find_tx_data_in_cache) {
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
      STORAGE_LOG(ERROR, "tx data in cache value is nullptr", KR(ret), K(read_tx_data_arg), K(ls_id_), KPC(cache_val));
    } else {
      EVENT_INC(ObStatEventIds::TX_DATA_HIT_KV_CACHE_COUNT);
      ret = fn(*tx_data);

      if (ObTxData::RUNNING == tx_data->state_) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "read an unexpected state tx data from kv cache");
      } else if (OB_ISNULL(tx_data->undo_status_list_.head_)) {
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
        if (OB_ISNULL(tx_data->undo_status_list_.head_)) {
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
  // TODO(handora.qc): remove it
  LOG_DEBUG("finish check row locked", K(read_tx_data_arg), K(read_tx_id), K(sql_sequence), K(lock_state));
  return ret;
}

int ObTxTable::check_sql_sequence_can_read(ObReadTxDataArg &read_tx_data_arg,
                                           const transaction::ObTxSEQ &sql_sequence,
                                           bool &can_read)
{
  CheckSqlSequenceCanReadFunctor fn(sql_sequence, can_read);
  int ret = check_with_tx_data(read_tx_data_arg, fn);
  // TODO(handora.qc): remove it
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
  // TODO(handora.qc): remove it
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
                             bool &is_determined_state,
                             ObCleanoutOp &cleanout_op,
                             ObReCheckOp &recheck_op)
{
  LockForReadFunctor fn(
      lock_for_read_arg, can_read, trans_version, is_determined_state, ls_id_, cleanout_op, recheck_op);
  int ret = check_with_tx_data(read_tx_data_arg, fn);
  // TODO(handora.qc): remove it
  LOG_DEBUG("finish lock for read", K(lock_for_read_arg), K(can_read), K(trans_version), K(is_determined_state));
  return ret;
}

int ObTxTable::get_recycle_scn(SCN &real_recycle_scn)
{
  int ret = OB_SUCCESS;
  real_recycle_scn = SCN::min_scn();

  int64_t current_time_us = ObClockGenerator::getCurrentTime();
  int64_t tx_result_retention = DEFAULT_TX_RESULT_RETENTION_S;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (tenant_config.is_valid()) {
    // use config value if config is valid
    tx_result_retention = tenant_config->_tx_result_retention;
  }

  TxTableState state;
  int64_t prev_epoch = ATOMIC_LOAD(&epoch_);
  int64_t after_epoch = 0;
  SCN tablet_recycle_scn = SCN::min_scn();
  const int64_t retain_tx_data_us = tx_result_retention * 1000L * 1000L;

  if (current_time_us - recycle_scn_cache_.update_ts_ < retain_tx_data_us && recycle_scn_cache_.val_.is_valid()) {
    // cache is valid, get recycle scn from cache
    real_recycle_scn = recycle_scn_cache_.val_;
    STORAGE_LOG(INFO, "use recycle scn cache", K(ls_id_), K(recycle_scn_cache_));
  } else if (OB_FAIL(tx_data_table_.get_recycle_scn(tablet_recycle_scn))) {
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
    const int64_t current_time_ns = current_time_us * 1000L;
    delay_recycle_scn.convert_for_tx(current_time_ns - (tx_result_retention * 1000L * 1000L * 1000L));
    if (delay_recycle_scn < tablet_recycle_scn) {
      real_recycle_scn = delay_recycle_scn;
    } else {
      real_recycle_scn = tablet_recycle_scn;
    }

    // update cache
    recycle_scn_cache_.val_ = real_recycle_scn;
    recycle_scn_cache_.update_ts_ = ObClockGenerator::getCurrentTime();
  }

  return ret;
}

int ObTxTable::get_upper_trans_version_before_given_scn(const SCN sstable_end_scn, SCN &upper_trans_version)
{
  return tx_data_table_.get_upper_trans_version_before_given_scn(sstable_end_scn, upper_trans_version);
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
  CleanoutTxStateFunctor fn(op);
  int ret = check_with_tx_data(read_tx_data_arg, fn);
  if (OB_TRANS_CTX_NOT_EXIST == ret) {
    if (tnode.is_committed() || tnode.is_aborted()) {
      // may be the concurrent case between cleanout and commit/abort
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObTxTable::supplement_undo_actions_if_exist(ObTxData *tx_data)
{
  return tx_data_table_.supplement_undo_actions_if_exist(tx_data);
}

int ObTxTable::self_freeze_task() { return tx_data_table_.self_freeze_task(); }

int ObTxTable::generate_virtual_tx_data_row(const transaction::ObTransID tx_id, observer::VirtualTxDataRow &row_data)
{
  GenerateVirtualTxDataRowFunctor fn(row_data);
  ObTxDataMiniCache mini_cache;
  ObReadTxDataArg read_tx_data_arg(tx_id, epoch_, mini_cache);
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
    auto ls_id = ls_->get_ls_id().id();
    auto tenant_id = MTL_ID();
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

// *********************** ObTxTable end. ************************

}  // namespace storage
}  // namespace oceanbase
