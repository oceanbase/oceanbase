/**
 * Copyright (c) 2024 OceanBase
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

#include "ob_direct_load_mgr_v3.h"
#include "storage/column_store/ob_column_oriented_sstable.h"
#include "storage/ddl/ob_ddl_merge_task.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "observer/report/ob_tablet_table_updater.h"
#include "storage/compaction/ob_schedule_dag_func.h"
#include "share/compaction/ob_shared_storage_compaction_util.h"
#include "share/ob_tablet_autoincrement_service.h"
#include "storage/ddl/ob_direct_load_mgr_utils.h"
#include "observer/ob_server_event_history_table_operator.h"
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::sql;

ObTabletDirectLoadMgrV3::ObTabletDirectLoadMgrV3():
  ObBaseTabletDirectLoadMgr(), arena_allocator_("TDL_V3_INIT", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()), execution_id_(0), storage_schema_(nullptr), micro_index_clustered_(false),
  dir_id_(-1), task_finish_count_(0), schema_item_(), column_items_(), lob_column_idxs_(), lob_col_types_(), data_block_desc_(), index_builder_(nullptr), build_param_(),
  seq_interval_task_id_(0), role_(ObDirectLoadMgrRole::INVALID_TYPE), is_schema_item_ready_(false), is_inited_(false)
{
  column_items_.set_attr(ObMemAttr(MTL_ID(), "DL_COL_SCHEMA"));
  lob_column_idxs_.set_attr(ObMemAttr(MTL_ID(), "DL_LOB_IDX"));
  lob_col_types_.set_attr(ObMemAttr(MTL_ID(), "DL_LOG_TYPE"));
}

ObTabletDirectLoadMgrV3::~ObTabletDirectLoadMgrV3()
{
  seq_interval_task_id_ = 0;
  if (nullptr != index_builder_) {
    index_builder_->~ObSSTableIndexBuilder();
    arena_allocator_.free(index_builder_);
    index_builder_ = nullptr;
  }
  data_block_desc_.reset();
  lob_col_types_.reset();
  lob_column_idxs_.reset();
  column_items_.reset();
  schema_item_.reset();
  if (nullptr != storage_schema_) {
    storage_schema_->~ObStorageSchema();
    arena_allocator_.free(storage_schema_);
    storage_schema_ = nullptr;
  }
  execution_id_ = 0;
  is_schema_item_ready_ = false;
  arena_allocator_.reset();
}

int ObTabletDirectLoadMgrV3::get_target_table_type(const ObStorageSchema &storage_schema,
                                                   const ObDirectLoadType &direct_load_type,
                                                   ObITable::TableType &table_type)
{
  int ret = OB_SUCCESS;
  bool is_column_group_store = false;
  if (!storage_schema.is_valid() ||
      ObDirectLoadType::DIRECT_LOAD_INVALID > direct_load_type ||
      ObDirectLoadType::DIRECT_LOAD_MAX <= direct_load_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(direct_load_type), K(storage_schema));
  } else if (OB_FAIL(ObCODDLUtil::need_column_group_store(storage_schema, is_column_group_store))) {
    LOG_WARN("failed to check need column group store", K(ret), K(storage_schema));
  } else if (DIRECT_LOAD_INCREMENTAL == direct_load_type) {
    if (is_column_group_store) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("increment direct load not support column store", K(ret));
    } else {
      table_type = ObITable::MINOR_SSTABLE;
    }
  } else if (DIRECT_LOAD_INCREMENTAL != direct_load_type) {
    if (is_column_group_store) {
      table_type = ObITable::COLUMN_ORIENTED_SSTABLE;
    } else {
      table_type = ObITable::MAJOR_SSTABLE;
    }
  }
  return ret;
}

/* prepare index builder for building macro block */
int ObTabletDirectLoadMgrV3::prepare_index_builder(const ObTabletDirectLoadInsertParam &build_param,
                                                   const ObITable::TableKey &table_key,
                                                   const ObTableSchema &table_schema,
                                                   ObIAllocator &allocator,
                                                   blocksstable::ObSSTableIndexBuilder *&index_builder,
                                                   blocksstable::ObWholeDataStoreDesc &data_block_desc)
{
  int ret = OB_SUCCESS;
  const uint64_t &data_format_version = build_param.common_param_.data_format_version_;
  const ObLSID &ls_id = build_param.common_param_.ls_id_;
  const ObTabletID &tablet_id = build_param.common_param_.tablet_id_;
  const ObDirectLoadType &direct_load_type = build_param.common_param_.direct_load_type_;
  blocksstable::ObWholeDataStoreDesc index_block_desc;
  index_builder = nullptr;
  data_block_desc.reset();

  if (!build_param.is_valid() || !table_key.is_valid() || !table_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(build_param), K(table_schema), K(table_key));
  } else if (OB_FAIL(index_block_desc.init(true/*is ddl*/, table_schema, ls_id, tablet_id,
          is_full_direct_load(direct_load_type) ? compaction::ObMergeType::MAJOR_MERGE : compaction::ObMergeType::MINOR_MERGE,
          is_full_direct_load(direct_load_type) ? table_key.get_snapshot_version() : 1L,
          data_format_version, table_schema.get_micro_index_clustered(), get_private_transfer_epoch(), 0/*concurrent_cnt*/,
          is_full_direct_load(direct_load_type) ? SCN::invalid_scn() : table_key.get_end_scn()))) {
    LOG_WARN("fail to init data desc", K(ret));
  } else {
    void *builder_buf = nullptr;
    update_store_desc_exec_mode(index_block_desc.get_static_desc());
    if (OB_ISNULL(builder_buf = allocator.alloc(sizeof(ObSSTableIndexBuilder)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret));
    } else if (OB_ISNULL(index_builder = new (builder_buf) ObSSTableIndexBuilder(true /*use buffer*/))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to new ObSSTableIndexBuilder", K(ret));
    } else if (OB_FAIL(index_builder->init(
            index_block_desc.get_desc(), // index_block_desc is copied in index_builder
            ObSSTableIndexBuilder::DISABLE))) {
      LOG_WARN("failed to init index builder", K(ret), K(index_block_desc));
    } else if (OB_FAIL(data_block_desc.init(true/*is ddl*/, table_schema, ls_id, tablet_id,
            compaction::ObMergeType::MAJOR_MERGE, // TODO @zhuora.zzr to set virtual
            is_full_direct_load(direct_load_type) ? table_key.get_snapshot_version() : 1L,
            data_format_version, table_schema.get_micro_index_clustered(), get_private_transfer_epoch(), 0/*concurrent_cnt*/,
            is_full_direct_load(direct_load_type) ? SCN::invalid_scn() : table_key.get_end_scn()))) {
      LOG_WARN("fail to init data block desc", K(ret));
    } else {
      data_block_desc.get_desc().sstable_index_builder_ = index_builder; // for build the tail index block in macro block
      update_store_desc_exec_mode(data_block_desc.get_static_desc());
    }

    if (OB_FAIL(ret)) {
      if (nullptr != index_builder) {
        index_builder->~ObSSTableIndexBuilder();
        index_builder = nullptr;
      }
      if (nullptr != builder_buf) {
        allocator.free(builder_buf);
        builder_buf = nullptr;
      }
      data_block_desc.reset();
    }
  }
  return ret;
}

int ObTabletDirectLoadMgrV3::prepare_schema_item_on_demand(const blocksstable::ObWholeDataStoreDesc &data_block_desc,
                                                           const ObTabletDirectLoadInsertParam &build_param,
                                                           const ObTableSchema &table_schema,
                                                           const ObSchemaGetterGuard &schema_guard,
                                                           ObIAllocator &allocator,
                                                           ObTableSchemaItem &schema_item,
                                                           ObIArray<ObColumnSchemaItem> &column_items,
                                                           ObIArray<int64_t> &lob_column_idxs,
                                                           ObIArray<common::ObObjMeta> &lob_col_types)
{
  int ret = OB_SUCCESS;
  schema_item.reset();
  column_items.reset();
  lob_column_idxs.reset();
  lob_col_types.reset();
  const int64_t &table_id = build_param.runtime_only_param_.table_id_;
  if (!data_block_desc.is_valid() || !build_param.is_valid() || !table_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(data_block_desc), K(build_param), K(table_schema));
  } else {
    const uint64_t tenant_id = MTL_ID();
    ObSchemaGetterGuard schema_guard;
    const ObDataStoreDesc &data_desc = data_block_desc.get_desc();
    const ObTableSchema *data_table_schema = nullptr;
    bool is_vector_data_complement = ObDirectLoadMgrUtil::need_process_vec_index(table_schema.get_index_type());

    if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("failed to get tenant schema guard", K(ret), K(tenant_id));
    } else if (is_vector_data_complement && OB_FAIL(ObDirectLoadMgrUtil::prepare_schema_item_for_vec_idx_data(MTL_ID(), schema_guard, &table_schema,
                                                     data_table_schema, allocator, schema_item))) {
      LOG_WARN("failed to prepare schema item for vec idx data", K(ret), K(table_id), K(table_schema), KPC(data_table_schema));
    } else if (OB_FAIL(table_schema.get_is_column_store(schema_item.is_column_store_))) {
      LOG_WARN("fail to get is column store", K(ret));
    } else {
      schema_item.is_index_table_      = table_schema.is_index_table();
      schema_item.rowkey_column_num_   = table_schema.get_rowkey_column_num();
      schema_item.is_unique_index_     = table_schema.is_unique_index();
      schema_item.lob_inrow_threshold_ = is_vector_data_complement ?
                                            data_table_schema->get_lob_inrow_threshold() :
                                            table_schema.get_lob_inrow_threshold();

      if (OB_FAIL(column_items.reserve(data_desc.get_col_desc_array().count()))) {
        LOG_WARN("reserve column schema array failed", K(ret), K(data_desc.get_col_desc_array().count()), K(column_items));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < data_desc.get_col_desc_array().count(); ++i) {
          const ObColDesc &col_desc = data_desc.get_col_desc_array().at(i);
          const schema::ObColumnSchemaV2 *column_schema = nullptr;
          const schema::ObColumnSchemaV2 *data_column_schema = nullptr;
          ObColumnSchemaItem column_item;
          if (i >= table_schema.get_rowkey_column_num() && i < table_schema.get_rowkey_column_num() + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt()) {
            // skip multi version column, keep item invalid
          } else if (OB_ISNULL(column_schema = table_schema.get_column_schema(col_desc.col_id_))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("column schema is null", K(ret), K(i), K(data_desc.get_col_desc_array()), K(col_desc.col_id_));
          } else if (is_vector_data_complement && OB_ISNULL(data_column_schema = data_table_schema->get_column_schema(col_desc.col_id_))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("data column schema is null", K(ret), K(i), K(data_desc.get_col_desc_array()), K(col_desc.col_id_));
          } else {
            column_item.is_valid_ = true;
            column_item.col_type_ = column_schema->get_meta_type();
            column_item.col_accuracy_ = column_schema->get_accuracy();
            if (is_vector_data_complement) {
              column_item.column_flags_ = data_column_schema->get_column_flags();
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(column_items.push_back(column_item))) {
              LOG_WARN("push back null column schema failed", K(ret));
            } else if (OB_NOT_NULL(column_schema) && column_schema->get_meta_type().is_lob_storage()) { // not multi version column
              if (OB_FAIL(lob_column_idxs.push_back(i))) {
                LOG_WARN("push back lob column idx failed", K(ret), K(i));
              } else if (OB_FAIL(lob_col_types.push_back(column_schema->get_meta_type()))) {
                LOG_WARN("push back lob col_type  failed", K(ret), K(i));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObTabletDirectLoadMgrV3::init_v2(const ObTabletDirectLoadInsertParam &build_param,
                                     const int64_t execution_id,
                                     const ObDirectLoadMgrRole direct_load_role)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  const int64_t tenant_id = MTL_ID();
  ObTabletHandle tablet_handle;
  bool is_column_store = false;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("direct load mgr has been inited for twice", K(ret));
  } else if (!build_param.is_valid() || !is_idem_type(build_param.common_param_.direct_load_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid build param", K(ret), K(build_param));
  } else if (OB_FAIL(ObDirectLoadMgrUtil::get_tablet_handle(build_param.common_param_.ls_id_, build_param.common_param_.tablet_id_, tablet_handle))) {
      LOG_WARN("failed to get tablet handle", K(ret), K(build_param));
  /* get tabel schema */
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get tenant schema failed", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, build_param.runtime_only_param_.table_id_, table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(tenant_id), K(build_param.runtime_only_param_.table_id_));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", K(ret), K(tenant_id), K(build_param.runtime_only_param_.table_id_));
  } else if (OB_FAIL(table_schema->get_is_column_store(is_column_store))) {
    LOG_WARN("failed to check is column store", K(ret));
  } else if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.alloc_dir(MTL_ID(), dir_id_))) {
    LOG_WARN("failed to get direct_load ");
  } else {
    /* prepare table key*/
    int64_t base_cg_idx = 0;
    share::SCN mock_start_scn;
    if (OB_FAIL(mock_start_scn.convert_for_tx(SS_DDL_START_SCN_VAL))) {
      LOG_WARN("failed to convert for tx", K(ret));
    } else if (OB_FAIL(tablet_handle.get_obj()->load_storage_schema(arena_allocator_, storage_schema_))) {
      LOG_WARN("failed to get tablet handle", K(ret));
    } else if (OB_ISNULL(storage_schema_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("storage schema is null", K(ret));
    } else if (FALSE_IT(storage_schema_->schema_version_ = build_param.runtime_only_param_.schema_version_)) {
    } else if (OB_FAIL(get_target_table_type(*storage_schema_, build_param.common_param_.direct_load_type_, table_key_.table_type_))) {
      LOG_WARN("failed to get target table type", K(ret), K(storage_schema_), K(build_param));
    } else if (OB_FAIL(ObCODDLUtil::get_base_cg_idx(storage_schema_, base_cg_idx))) {
      LOG_WARN("failed to get base cg idx", K(ret), K(storage_schema_));
    } else {
      table_key_.tablet_id_ = build_param.common_param_.tablet_id_;
      table_key_.scn_range_.start_scn_ = tablet_handle.get_obj()->get_tablet_meta().ddl_start_scn_;
      table_key_.scn_range_.end_scn_ = tablet_handle.get_obj()->get_tablet_meta().ddl_start_scn_;
      table_key_.version_range_.snapshot_version_ = build_param.common_param_.read_snapshot_;
      table_key_.column_group_idx_ = static_cast<uint16_t> (base_cg_idx);
      start_scn_ = mock_start_scn;
      micro_index_clustered_ = tablet_handle.get_obj()->get_tablet_meta().micro_index_clustered_;
    }
  }

  /* prpeare basic info */
  if (OB_FAIL(ret)) {
  } else {
    /* basic info */
    build_param_.assign(build_param);
    ls_id_ = build_param.common_param_.ls_id_;
    tablet_id_ =  build_param.common_param_.tablet_id_;
    tenant_data_version_ = build_param.common_param_.data_format_version_;
    direct_load_type_ = build_param.common_param_.direct_load_type_;
    execution_id_ = execution_id;
    role_ = direct_load_role;
  }
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle, true /*try_create]*/))) {
    LOG_WARN("failed to create ddl kv mgr", K(ret));
  } else {
    is_inited_ = true;
    FLOG_INFO("[DIRECT_LOAD_MGR] success to create tablet direct load mgr", K(table_key_), K(ls_id_), K(tablet_id_), K(execution_id),
                                                                            K(direct_load_type_), KP(index_builder_), KPC(index_builder_), KP(storage_schema_));
  }
  return ret;
}

int ObTabletDirectLoadMgrV3::prepare_index_builder()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  const int64_t tenant_id = MTL_ID();
  uint32_t lock_tid = 0;
  bool is_ready_for_write = ATOMIC_LOAD(&is_schema_item_ready_);
  if (is_ready_for_write) {
    /* already build skip*/
  } else if (OB_FAIL(wrlock(TRY_LOCK_TIMEOUT, lock_tid))) {
      LOG_WARN("failed to wrlock", K(ret), KPC(this));
  } else if (is_schema_item_ready_) {
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get tenant schema failed", K(ret), K(tenant_id), K(get_table_id()));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, get_table_id(), table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(tenant_id), K(get_table_id()));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
  } else if (OB_FAIL(prepare_index_builder(build_param_, table_key_, *table_schema, arena_allocator_, index_builder_, data_block_desc_))) {
    LOG_WARN("failed to prepare index builder", K(ret), K(build_param_), K(table_key_), KPC(table_schema));
  } else if (OB_FAIL(prepare_schema_item_on_demand(data_block_desc_,
                                                   build_param_,
                                                   *table_schema,
                                                   schema_guard,
                                                   arena_allocator_,
                                                   schema_item_,
                                                   column_items_,
                                                   lob_column_idxs_,
                                                   lob_col_types_))) {
    LOG_WARN("failed to parepare schema item on demand", K(ret));
  } else {
    ATOMIC_STORE(&is_schema_item_ready_, true);
  }

  if (0 != lock_tid) {
    unlock(lock_tid);
  }
  return ret;
}

int ObTabletDirectLoadMgrV3::prepare_lob_param(const ObTabletDirectLoadInsertParam &build_param, ObTabletDirectLoadInsertParam &lob_param)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObSchemaGetterGuard schema_guard;
  ObTabletBindingMdsUserData ddl_data;
  const ObTableSchema *table_schema = nullptr;
  if (!build_param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(build_param));
  } else if (OB_FAIL(lob_param.assign(build_param))) {
    LOG_WARN("failed to assign lob param", K(ret), K(build_param));
  } else if (OB_FAIL(ObDirectLoadMgrUtil::get_tablet_handle(build_param.common_param_.ls_id_, build_param.common_param_.tablet_id_, tablet_handle))) {
    LOG_WARN("failed to get tablet hanlde", K(ret), K(build_param));
  } else if (OB_FAIL(tablet_handle.get_obj()->ObITabletMdsInterface::get_ddl_data(share::SCN::max_scn(), ddl_data))) {
    LOG_WARN("get ddl data failed", K(ret));
  } else if (OB_FALSE_IT(lob_param.common_param_.tablet_id_ = ddl_data.lob_meta_tablet_id_)) {
    LOG_WARN("failed to get lob meta tablet id", K(ret));
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(MTL_ID(), schema_guard))) {
    LOG_WARN("get tenant schema failed", K(ret), K(MTL_ID()), K(lob_param));
  } else if (OB_FAIL(schema_guard.get_table_schema(MTL_ID(), build_param.runtime_only_param_.table_id_, table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(build_param));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", K(ret), K(lob_param));
  } else if (!table_schema->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid table schema", K(ret), K(table_schema));
  } else {
    lob_param.runtime_only_param_.table_id_ = table_schema->get_aux_lob_meta_tid();
  }
  return ret;
}

int ObTabletDirectLoadMgrV3::fill_sstable_slice_v2(const ObDirectLoadSliceInfo &slice_info,
                                                   ObIStoreRowIterator *iter,
                                                   ObDirectLoadSliceWriter &slice_writer,
                                                   blocksstable::ObMacroDataSeq &next_seq,
                                                   ObInsertMonitor *insert_monitor,
                                                   int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(iter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), KP(iter));
  } else if (OB_FAIL(slice_writer.fill_sstable_slice(start_scn_, get_table_id(), tablet_id_, storage_schema_, iter, schema_item_,
                                                     direct_load_type_, column_items_, dir_id_, get_parallel(), slice_info.context_id_,
                                                     affected_rows, insert_monitor))) {
    LOG_WARN("failed to fill sstable slice", K(ret));
  } else {
    next_seq = blocksstable::ObMacroDataSeq(slice_writer.get_next_block_start_seq());
  }
  FLOG_INFO("[DIRECT_LOAD_MGR] fill sstable slice", K(ret), K(ls_id_), K(tablet_id_), K(execution_id_), K(slice_info), K(affected_rows), K(next_seq));
  return ret;
}

int ObTabletDirectLoadMgrV3::fill_sstable_slice_v2(const ObDirectLoadSliceInfo &slice_info,
                                                   const ObBatchDatumRows &datum_rows,
                                                   ObDirectLoadSliceWriter &slice_writer,
                                                   ObInsertMonitor *insert_monitor)
{
  int ret = OB_SUCCESS;
  share::SCN commit_scn;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!slice_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(slice_info));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), KPC(this));
  } else if (OB_FAIL(slice_writer.fill_sstable_slice(start_scn_, get_table_id(), tablet_id_, storage_schema_, datum_rows, schema_item_,
                                                      direct_load_type_, column_items_, dir_id_, get_parallel(), slice_info.context_id_, insert_monitor))) {
    LOG_WARN("fill sstable slice failed", K(ret), KPC(this));
  }
  FLOG_INFO("[DIRECTL_LOAD_MGR] fill sstable slice", K(ret), K(ls_id_), K(tablet_id_), K(execution_id_), K(slice_info)); //TODO @zhuoran.zzr wait to remove to the sub class
  return ret;
}


int ObTabletDirectLoadMgrV3::fill_lob_meta_sstable_slice(const ObDirectLoadSliceInfo &slice_info,
                                                           ObIStoreRowIterator *iter,
                                                           ObDirectLoadSliceWriter &slice_writer,
                                                           int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!slice_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(slice_info));
  } else if (ObDirectLoadMgrRole::LOB_TABLET_TYPE != role_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("func should be called only by lob direct load mgr", K(ret), KPC(this));
  } else if (OB_UNLIKELY(!is_incremental_direct_load(direct_load_type_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected direct load type", K(ret), K(direct_load_type_));
  } else if (OB_FAIL(slice_writer.fill_lob_meta_sstable_slice(start_scn_,
                                                                 build_param_.runtime_only_param_.table_id_,
                                                                 tablet_id_,
                                                                 iter,
                                                                 affected_rows))) {
    LOG_WARN("fail to fill lob meta sstable slice", K(ret), K(tablet_id_), K(build_param_.runtime_only_param_));
  }
  return ret;
}

/*
* we still kept the close sstable slice
* but it should be used to control the sync action for differrent slice
*/
int ObTabletDirectLoadMgrV3::close_sstable_slice_v2(const ObDirectLoadSliceInfo &slice_info,
                                                    ObDirectLoadSliceWriter &slice_writer,
                                                    blocksstable::ObMacroDataSeq &next_seq,
                                                    ObInsertMonitor *insert_monitor,
                                                    bool &is_all_task_finish)
{
  int ret = OB_SUCCESS;
  is_all_task_finish = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), KPC(this));
  } else if (OB_FAIL(slice_writer.close())) {
    LOG_WARN("failed to close_sstable_slice", K(ret), K(ls_id_), K(tablet_id_));
  } else if (FALSE_IT(next_seq = slice_writer.get_next_block_start_seq())) {
  } else if (!slice_info.is_lob_slice_ && is_ddl_direct_load(direct_load_type_)) {
    int64_t task_finish_count = -1;
    {
      ObLatchRGuard guard(lock_, ObLatchIds::TABLET_DIRECT_LOAD_MGR_LOCK);
      if (slice_info.is_task_finish_) {
        task_finish_count = ATOMIC_AAF(&task_finish_count_, 1);
      }
    }
    LOG_INFO("inc task finish count", K(tablet_id_), K(task_finish_count), K(build_param_.runtime_only_param_.task_cnt_));
    if (OB_ISNULL(storage_schema_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid tablet handle", K(ret), KP(storage_schema_));
    } else if (task_finish_count >= build_param_.runtime_only_param_.task_cnt_) {
      is_all_task_finish = true;
      if (ObDirectLoadMgrUtil::need_process_vec_index(storage_schema_->get_index_type())) {
        if (OB_FAIL(slice_writer.fill_vector_index_data(build_param_.common_param_.read_snapshot_,
                                                        storage_schema_,
                                                        start_scn_,
                                                        schema_item_,
                                                        insert_monitor,
                                                        slice_info.context_id_))) {
          LOG_WARN("fail to fill vector index data", K(ret));
        }
      }
    }
  }
  FLOG_INFO("[DIRECT_LOAD_MGR] close sstable slice", K(ret), K(ls_id_), K(tablet_id_), K(execution_id_), K(slice_info), K(is_all_task_finish));
  return ret;
}

int ObTabletDirectLoadMgrV3::fill_lob_sstable_slice_row_v2(ObIAllocator &allocator,
                                                           const ObDirectLoadSliceInfo &slice_info,
                                                           share::ObTabletCacheInterval &pk_interval,
                                                           blocksstable::ObDatumRow &datum_row,
                                                           ObDirectLoadSliceWriter &slice_writer,
                                                           ObTabletDirectLoadMgrHandle &data_direct_load_handle)
{
  int ret = OB_SUCCESS;
  share::SCN commit_scn;
  ObTabletDirectLoadMgrV3 *data_direct_load_mgr = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!slice_info.is_valid() || !data_direct_load_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(slice_info));
  } else if (FALSE_IT(data_direct_load_mgr = static_cast<ObTabletDirectLoadMgrV3*>(data_direct_load_handle.get_base_obj()))) {
  } else if (nullptr == data_direct_load_mgr ||
            ObDirectLoadMgrRole::DATA_TABLET_TYPE != data_direct_load_mgr->get_role() ||
            ObDirectLoadMgrRole::LOB_TABLET_TYPE != role_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), KPC(data_direct_load_mgr), K(data_direct_load_mgr->get_role()), K(role_));
  }
  if (OB_SUCC(ret)) {
    ObTabletDirectLoadBuildCtx::SliceKey slice_key(slice_info.context_id_, slice_info.slice_id_);
    const int64_t trans_version = is_full_direct_load(direct_load_type_) ? table_key_.get_snapshot_version() : INT64_MAX;

    /* prepare batch slice info with data table direct load mgr*/
    ObBatchSliceWriteInfo info(data_direct_load_mgr->tablet_id_,
                               data_direct_load_mgr->ls_id_,
                               trans_version,
                               direct_load_type_,
                               data_direct_load_mgr->build_param_.runtime_only_param_.trans_id_,
                               data_direct_load_mgr->build_param_.runtime_only_param_.seq_no_,
                               slice_info.src_tenant_id_,
                               data_direct_load_mgr->build_param_.runtime_only_param_.tx_desc_);
    if (OB_FAIL(slice_writer.fill_lob_sstable_slice(build_param_.runtime_only_param_.table_id_, allocator, allocator,
                                                     start_scn_,info, pk_interval,
                                                     data_direct_load_mgr->lob_column_idxs_,
                                                     data_direct_load_mgr->lob_col_types_,
                                                     data_direct_load_mgr->schema_item_,
                                                     datum_row))) {
      LOG_WARN("fail to fill batch sstable slice", K(ret), K(tablet_id_), K(pk_interval));
    }
  }
  return ret;
}

int ObTabletDirectLoadMgrV3::fill_lob_sstable_slice_row_v2(ObIAllocator &allocator,
                                    const ObDirectLoadSliceInfo &slice_info,
                                    share::ObTabletCacheInterval &pk_interval,
                                    ObDirectLoadSliceWriter &slice_writer,
                                    ObTabletDirectLoadMgrHandle &data_direct_load_handle,
                                    blocksstable::ObBatchDatumRows &datum_rows)
{
  int ret = OB_SUCCESS;
  ObTabletDirectLoadMgrV3 *data_direct_load_mgr = static_cast<ObTabletDirectLoadMgrV3*>(data_direct_load_handle.get_base_obj());
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!slice_info.is_valid() || ObDirectLoadMgrRole::DATA_TABLET_TYPE != data_direct_load_mgr->get_role()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(slice_info), K(slice_writer));
  } else if (ObDirectLoadMgrRole::LOB_TABLET_TYPE != role_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("this direct load mgr is not lob direct load mgr", K(ret), KPC(this));
  } else {
    const int64_t trans_version = is_full_direct_load(direct_load_type_) ? table_key_.get_snapshot_version() : INT64_MAX;
    ObBatchSliceWriteInfo info(data_direct_load_mgr->tablet_id_,
                               data_direct_load_mgr->ls_id_,
                               trans_version,
                               direct_load_type_,
                               data_direct_load_mgr->build_param_.runtime_only_param_.trans_id_,
                               data_direct_load_mgr->build_param_.runtime_only_param_.seq_no_,
                               slice_info.src_tenant_id_,
                               data_direct_load_mgr->build_param_.runtime_only_param_.tx_desc_);
    if (OB_FAIL(slice_writer.fill_lob_sstable_slice(build_param_.runtime_only_param_.table_id_,
                                                    allocator, allocator,
                                                    start_scn_, info, pk_interval,
                                                    data_direct_load_mgr->lob_column_idxs_,
                                                    data_direct_load_mgr->lob_col_types_,
                                                    data_direct_load_mgr->schema_item_,
                                                    datum_rows))) {
      LOG_WARN("fail to fill batch sstable slice", K(ret), KPC(this));
    }
  }
  return ret;
}

int ObTabletDirectLoadMgrV3::close()
{
  int ret = OB_SUCCESS;
  SCN commit_scn;
  bool is_remote_write = false;
  ObLSService *ls_service = nullptr;
  ObLSHandle ls_handle;
  ObTabletHandle new_tablet_handle;
  bool sstable_already_created = false;
  const uint64_t tenant_id = MTL_ID();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls service should not be null", K(ret));
  } else if (OB_FAIL(ls_service->get_ls(ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("get ls failed", K(ret), K(ls_id_));
  } else if (OB_FAIL(inner_close())) {
    LOG_WARN("failed to inner close", K(ret), KPC(this));
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle, tablet_id_, new_tablet_handle))) {
    LOG_WARN("fail to get tablet handle", K(ret), K(tablet_id_));
  } else {
    ObSSTableMetaHandle sst_meta_hdl;
    ObSSTable *first_major_sstable = nullptr;
    ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
    if (OB_FAIL(new_tablet_handle.get_obj()->fetch_table_store(table_store_wrapper))) {
      LOG_WARN("fetch table store failed", K(ret));
    } else if (OB_ISNULL(first_major_sstable = static_cast<ObSSTable *>
      (table_store_wrapper.get_member()->get_major_sstables().get_boundary_table(false/*first*/)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no major after wait merge success", K(ret), K(ls_id_), K(tablet_id_));
    } else if (OB_UNLIKELY(first_major_sstable->get_key() != table_key_)) {
      ret = OB_SNAPSHOT_DISCARDED;
      LOG_WARN("ddl major sstable dropped, snapshot holding may have bug",
        K(ret), KPC(first_major_sstable), K(table_key_), K(tablet_id_), K(build_param_), K(build_param_.runtime_only_param_.task_id_));
    } else if (OB_FAIL(first_major_sstable->get_meta(sst_meta_hdl))) {
      LOG_WARN("fail to get sstable meta handle", K(ret));
    } else {
      const int64_t *column_checksums = sst_meta_hdl.get_sstable_meta().get_col_checksum();
      int64_t column_count = sst_meta_hdl.get_sstable_meta().get_col_checksum_cnt();
      ObArray<int64_t> co_column_checksums;
      co_column_checksums.set_attr(ObMemAttr(MTL_ID(), "TblDL_Ccc"));

      if (OB_FAIL(ObCODDLUtil::get_co_column_checksums_if_need(new_tablet_handle, first_major_sstable, co_column_checksums))) {
        LOG_WARN("get column checksum from co sstable failed", K(ret));
      } else {
        for (int64_t retry_cnt = 10; retry_cnt > 0; retry_cnt--) { // overwrite ret
          if (OB_FAIL(ObTabletDDLUtil::report_ddl_checksum(
                ls_id_,
                tablet_id_,
                build_param_.runtime_only_param_.table_id_,
                get_execution_id(),
                build_param_.runtime_only_param_.task_id_,
                co_column_checksums.empty() ? column_checksums : co_column_checksums.get_data(),
                co_column_checksums.empty() ? column_count : co_column_checksums.count(),
                tenant_data_version_))) {
            LOG_WARN("report ddl column checksum failed", K(ret), K(ls_id_), K(tablet_id_));
          } else {
            break;
          }
        }
        ob_usleep(100L * 1000L);
      }
    }
  }
  return ret;
}

int ObTabletDirectLoadMgrV3::get_tablet_cache_interval(ObTabletCacheInterval &interval)
{
  int ret = OB_SUCCESS;
  ObLatchWGuard guard(lock_, ObLatchIds::TABLET_DIRECT_LOAD_MGR_LOCK);
  ObTabletAutoincrementService &auto_inc_service = ObTabletAutoincrementService::get_instance();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("direct_load_mgr not init", K(ret));
  } else if (OB_FAIL(auto_inc_service.get_tablet_cache_interval(MTL_ID(), interval))) {
    LOG_WARN("falied to get tablet cache interval", K(ret), K(MTL_ID()));
  } else {
    interval.task_id_ = seq_interval_task_id_++;
  }
  return ret;
}

int ObTabletDirectLoadMgrV3::wrlock(const int64_t timeout_us, uint32_t &tid)
{
  int ret = OB_SUCCESS;
  const int64_t abs_timeout_us = timeout_us + ObTimeUtility::current_time();
  if (OB_SUCC(lock_.wrlock(ObLatchIds::TABLET_DIRECT_LOAD_MGR_LOCK, abs_timeout_us))) {
    tid = static_cast<uint32_t>(GETTID());
  }
  if (OB_TIMEOUT == ret) {
    ret = OB_EAGAIN;
  }
  return ret;
}

int ObTabletDirectLoadMgrV3::rdlock(const int64_t timeout_us, uint32_t &tid)
{
  int ret = OB_SUCCESS;
  const int64_t abs_timeout_us = timeout_us + ObTimeUtility::current_time();
  if (OB_SUCC(lock_.rdlock(ObLatchIds::TABLET_DIRECT_LOAD_MGR_LOCK, abs_timeout_us))) {
    tid = static_cast<uint32_t>(GETTID());
  }
  if (OB_TIMEOUT == ret) {
    ret = OB_EAGAIN;
  }
  return ret;
}

void ObTabletDirectLoadMgrV3::unlock(const uint32_t tid)
{
  if (OB_SUCCESS != lock_.unlock(&tid)) {
    ob_abort();
  }
}

int ObSNTabletDirectLoadMgr::schedule_merge_tablet_task(const ObTabletDDLCompleteArg &arg, const bool wait_major_generated)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else {
    const int64_t wait_start_ts = ObTimeUtility::fast_current_time();
    ObTabletHandle tablet_handle;
    ObDDLKvMgrHandle ddl_kv_mgr_handle;
    if (OB_FAIL(ObDirectLoadMgrUtil::get_tablet_handle(arg.ls_id_, arg.tablet_id_, tablet_handle))) {
      LOG_WARN("failed to get tablet_handle", K(ret));
    } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle, false /* not for repaly*/))) {
      LOG_WARN("failed to get ddl kv mgr", K(ret));
    }
    while (OB_SUCC(ret)) {
      ObDDLTableMergeDagParam param;
      ObArenaAllocator arena(ObMemAttr(MTL_ID(), "Ddl_Com_DLMgr"));
      ObTabletDDLCompleteMdsUserData data;
      if (OB_FAIL(THIS_WORKER.check_status())) {
        LOG_WARN("check status failed", K(ret), K(arg));
      } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_complete(share::SCN::max_scn(), arena, data))) {
        LOG_WARN("failed to get ddl complete", K(ret), K(arg));
      } else if (!data.has_complete_) {
        LOG_WARN("ddl complete has not been set, wait to schedule merge", K(ret), K(arg));
      } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->freeze_ddl_kv(arg.start_scn_, arg.table_key_.get_snapshot_version(), arg.data_format_version_))) {
        LOG_WARN("failed freeze ddl kv", K(ret), K(arg.start_scn_));
      } else if (OB_FAIL(ObDirectLoadMgrUtil::generate_merge_param(arg, param))) {
        LOG_WARN("failed to generate merge param", K(ret), K(arg), K(param));
      } else if (FALSE_IT(param.rec_scn_ = ddl_kv_mgr_handle.get_obj()->get_max_freeze_scn())) {
      } else {
        if (OB_FAIL(compaction::ObScheduleDagFunc::schedule_ddl_table_merge_dag(param))) {
          if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
            LOG_WARN("schedule ddl merge dag failed", K(ret), K(param));
          } else {
            ret = OB_SUCCESS;
          }
        } else if (!wait_major_generated) {
          // schedule successfully and no need to wait physical major generates.
          break;
        }
      }
      if (OB_SUCC(ret)) {
        const ObSSTable *first_major_sstable = nullptr;
        ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
        if (OB_FAIL(ObTabletDDLUtil::check_and_get_major_sstable(ls_id_, tablet_id_, first_major_sstable, table_store_wrapper))) {
          LOG_WARN("check if major sstable exist failed", K(ret));
        } else if (nullptr != first_major_sstable) {
          FLOG_INFO("major has already existed", K(ls_id_), K(tablet_id_));
          break;
        }
      }
      if (REACH_TIME_INTERVAL(10L * 1000L * 1000L)) {
        LOG_INFO("wait build ddl sstable", K(ret), K(ls_id_), K(tablet_id_),
            "wait_elpased_s", (ObTimeUtility::fast_current_time() - wait_start_ts) / 1000000L);
      }
    }
  }
  return ret;
}

/*
 * inner close is used for update tablet multi data source status
 * and schedule ddl merge task
*/
int ObSNTabletDirectLoadMgr::inner_close()
{
  int ret = OB_SUCCESS;
  ObTabletDDLCompleteArg complete_arg;
  ObTabletHandle tablet_handle;
  ObStorageSchema *storage_schema = nullptr;
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "DLM_CLOSE"));
  ObDDLWriteStat write_stats;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("direct load mgr have not been inited", K(ret));
  } else if (OB_FAIL(ObDirectLoadMgrUtil::get_tablet_handle(ls_id_, tablet_id_, tablet_handle))) {
    LOG_WARN("failed to get tablet handle", K(ret), K(ls_id_), K(tablet_id_));
  } else if (!tablet_handle.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet handle", K(ret));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle, false /* not for repaly*/))) {
      LOG_WARN("failed to get ddl kv mgr", K(ret));
  } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->freeze_ddl_kv(start_scn_, table_key_.get_snapshot_version(), tenant_data_version_))) {
    LOG_WARN("failed to freeze ddl kv", K(ret), K(start_scn_), K(table_key_), K(tenant_data_version_));
  } else if (OB_FAIL(tablet_handle.get_obj()->load_storage_schema(allocator, storage_schema))) {
    LOG_WARN("failed to load storage schema", K(ret), K(ls_id_), K(tablet_id_));
  } else {
    complete_arg.has_complete_ = true;
    complete_arg.ls_id_ = ls_id_;
    complete_arg.tablet_id_ = tablet_id_;
    complete_arg.direct_load_type_ = direct_load_type_;
    complete_arg.start_scn_ = start_scn_;
    complete_arg.data_format_version_ = tenant_data_version_;
    complete_arg.snapshot_version_ = table_key_.get_snapshot_version();
    complete_arg.table_key_ = table_key_;
    if (OB_FAIL(complete_arg.set_write_stat(write_stats))) {
      LOG_WARN("failed to set write stat", K(ret), K(write_stats));
    } else if (OB_FAIL(complete_arg.set_storage_schema(*storage_schema))) {
      LOG_WARN("failed to set storage_schema", K(ret), K(ls_id_), K(tablet_id_), KPC(storage_schema));
    } else if (OB_FAIL(ObTabletDDLCompleteMdsHelper::record_ddl_complete_arg_to_mds(complete_arg, allocator))) {
      LOG_WARN("failed to record ddl complete arg to mds", KR(ret), K(complete_arg));
    } else {
      LOG_INFO("ddl write commit log", K(ret), "ddl_event_info", ObDDLEventInfo());
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(schedule_merge_tablet_task(complete_arg, true /*wait major merge*/))) {
    LOG_WARN("failed to wait merge task", K(ret),  K(MTL_ID()), K(ls_id_), K(tablet_id_));
  }
  return ret;
}

ObSNTabletDirectLoadMgr::ObSNTabletDirectLoadMgr()
{ }

ObSNTabletDirectLoadMgr::~ObSNTabletDirectLoadMgr()
{ }

int ObSNTabletDirectLoadMgr::init_v2(const ObTabletDirectLoadInsertParam &build_param,
                                     const int64_t execution_id,
                                     const ObDirectLoadMgrRole direct_load_role)
{
  int ret = OB_SUCCESS;
  if (!build_param.is_valid() || execution_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(build_param), K(execution_id));
  } else if (OB_FAIL(ObTabletDirectLoadMgrV3::init_v2(build_param, execution_id, direct_load_role))) {
    LOG_WARN("failed to init direct load mgr", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

ObSSTabletDirectLoadMgr::ObSSTabletDirectLoadMgr():
ObTabletDirectLoadMgrV3(), last_data_seq_(0), last_meta_seq_(0), last_lob_id_(0), total_slice_cnt_(0), private_transfer_epoch_(-1)
{}

ObSSTabletDirectLoadMgr::~ObSSTabletDirectLoadMgr()
{}

int ObSSTabletDirectLoadMgr::init_v2(const ObTabletDirectLoadInsertParam &build_param,
                                     const int64_t execution_id,
                                     const ObDirectLoadMgrRole role)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  if (!build_param.is_valid() || execution_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(build_param), K(execution_id));
  } else if (OB_FAIL(ObTabletDirectLoadMgrV3::init_v2(build_param, execution_id, role))) {
    LOG_WARN("failed to init direct load mgr", K(ret));
  } else if (OB_FAIL(ObDirectLoadMgrUtil::get_tablet_handle(build_param.common_param_.ls_id_, build_param.common_param_.tablet_id_, tablet_handle))) {
    LOG_WARN("failed to get tablet handle", K(ret), K(build_param));
  } else if (!tablet_handle.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet handle", K(ret));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_private_transfer_epoch(private_transfer_epoch_))) {
    LOG_WARN("failed to get private transfer epoch from tablet", K(ret),
      "tablet_meta", tablet_handle.get_obj()->get_tablet_meta());
  } else {
    is_inited_ = true;
  }
  return ret;
}
void ObSSTabletDirectLoadMgr::update_max_data_macro_seq(const int64_t cur_data_seq)
{
  ObLatchWGuard guard(lock_, ObLatchIds::TABLET_DIRECT_LOAD_MGR_LOCK);
  last_data_seq_ = max(last_data_seq_, cur_data_seq);
}

void ObSSTabletDirectLoadMgr::update_max_meta_macro_seq(const int64_t cur_meta_seq)
{
  ObLatchWGuard guard(lock_, ObLatchIds::TABLET_DIRECT_LOAD_MGR_LOCK);
  last_meta_seq_ = max(last_meta_seq_, cur_meta_seq);
}

int ObSSTabletDirectLoadMgr::fill_sstable_slice_v2(const ObDirectLoadSliceInfo &slice_info,
                                                   ObIStoreRowIterator *iter,
                                                   ObDirectLoadSliceWriter &slice_writer,
                                                   blocksstable::ObMacroDataSeq &next_seq,
                                                   ObInsertMonitor *insert_monitor,
                                                   int64_t &affected_row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTabletDirectLoadMgrV3::fill_sstable_slice_v2(slice_info, iter, slice_writer, next_seq, insert_monitor, affected_row))){
    LOG_WARN("failed to fill sstable slice", K(ret));
  } else if (FALSE_IT(update_max_data_macro_seq(next_seq.data_seq_))){
  } else {
    ATOMIC_AAF(&total_slice_cnt_, 1);
    FLOG_INFO("inc total slice cnt", K(ret), K(execution_id_));
  }
  return ret;
}

int ObSSTabletDirectLoadMgr::calc_root_macro_seq(int64_t &root_seq)
{
  int ret = OB_SUCCESS;
  root_seq = 0;
  uint32_t lock_tid = 0;

  if (OB_FAIL(rdlock(TRY_LOCK_TIMEOUT, lock_tid))) {
    LOG_WARN("failed to wrlock", K(ret), KPC(this));
  } else {
    root_seq = MAX(last_meta_seq_, last_data_seq_) + compaction::MACRO_STEP_SIZE;
  }

  if (0 != lock_tid) {
    unlock(lock_tid);
  }
  return ret ;
}

int ObSSTabletDirectLoadMgr::create_ddl_ro_sstable(ObTablet &tablet,
                                                   common::ObArenaAllocator &allocator,
                                                   ObTableHandleV2 &sstable_handle)
{
  int ret = OB_SUCCESS;
  sstable_handle.reset();
  if (OB_ISNULL(index_builder_) || OB_UNLIKELY(!index_builder_->is_inited() || OB_ISNULL(storage_schema_) || !table_key_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(index_builder_), KP(storage_schema_), K(table_key_));
  } else if (table_key_.table_type_ != ObITable::MAJOR_SSTABLE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid cg idx", K(ret), K(table_key_));
  } else {

    /* build index layer */
    SMART_VARS_2((ObSSTableMergeRes, res), (ObTabletCreateSSTableParam, param)) {
      ObDDLRedoLogWriterCallbackInitParam init_param;
      init_param.ls_id_ = ls_id_;
      init_param.tablet_id_ = tablet_id_;
      init_param.direct_load_type_ = direct_load_type_;
      init_param.block_type_ = DDL_MB_INDEX_TYPE;
      init_param.table_key_ = table_key_;
      init_param.start_scn_ = start_scn_;
      init_param.task_id_ = build_param_.runtime_only_param_.task_id_;
      init_param.data_format_version_ = tenant_data_version_;
      init_param.parallel_cnt_ = get_task_cnt();
      init_param.cg_cnt_ = get_cg_cnt();
      init_param.row_id_offset_ = 0;

      storage::ObDDLRedoLogWriterCallback flush_callback;
      share::ObPreWarmerParam pre_warm_param;
      ObMacroDataSeq tmp_seq(total_slice_cnt_ * compaction::MACRO_STEP_SIZE);
      tmp_seq.set_index_block();

      if (pre_warm_param.init(ls_id_, tablet_id_)) {
         LOG_WARN("failed to init pre warm param", K(ret), K(ls_id_), K(tablet_id_));
      } else if (!index_builder_->is_inited()) {
        ret = OB_NOT_INIT;
        LOG_WARN("index builder should be inited", K(ret), KPC(index_builder_));
      } else if (index_builder_->is_closed()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index builder has been closed already", K(ret));
      } else  if (OB_FAIL(flush_callback.init(init_param))) {
        // The row_id_offset of cg index_macro_block has no effect, so set 0 to pass the defensive check.
        LOG_WARN("fail to init redo log writer callback", K(ret), K(init_param));
      } else  if (OB_FAIL(index_builder_->close_with_macro_seq(
                   res, tmp_seq.macro_data_seq_,
                   OB_DEFAULT_MACRO_BLOCK_SIZE /*nested_size*/,
                   0 /*nested_offset*/, pre_warm_param, &flush_callback))) {
        LOG_WARN("fail to close", K(ret), KPC(index_builder_), K(tmp_seq));
      } else if (FALSE_IT(update_max_meta_macro_seq(tmp_seq.macro_data_seq_))) {
      } else if (OB_FAIL(calc_root_macro_seq(res.root_macro_seq_))) {
          LOG_WARN("failed calc root macro seq", K(ret));
      } else {
        LOG_INFO("[SHARED STORAGE]build ddl sstable res success", K(last_meta_seq_), K(last_data_seq_), K(res), K(table_key_));
      }

      if (OB_FAIL(ret)) {
      } else {
        const int64_t create_schema_version_on_tablet = tablet.get_tablet_meta().create_schema_version_;
        if (OB_FAIL(param.init_for_ss_ddl(res,
                                          table_key_,
                                          *storage_schema_,
                                          create_schema_version_on_tablet,
                                          direct_load_type_,
                                          transaction::ObTransID(),
                                          transaction::ObTxSEQ()))) {
          LOG_WARN("fail to init param for ddl", K(ret), K(create_schema_version_on_tablet),
            KPC(index_builder_), K(table_key_), KPC(storage_schema_));
        } else if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable<ObSSTable>(param, allocator, sstable_handle))) {
          LOG_WARN("create sstable failed", K(ret), K(param));
        }

        if (OB_SUCC(ret)) {
          LOG_INFO("[SHARED STORAGE]create ddl sstable success ro", K(table_key_), K(sstable_handle),
              "create_schema_version", create_schema_version_on_tablet);
        }
      }
    }
  }
  return ret;
}

int ObSSTabletDirectLoadMgr::update_table_store(
  const blocksstable::ObSSTable *sstable,
  const ObStorageSchema *storage_schema,
  ObITable::TableKey &table_key,
  ObTablet &tablet)
{
  return OB_NOT_SUPPORTED;
}

int ObSSTabletDirectLoadMgr::inner_close()
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTableHandleV2 sstable_handle;
  ObSSTable *ddl_major_sstable = nullptr;
  if (OB_FAIL(ObDirectLoadMgrUtil::get_tablet_handle(ls_id_, tablet_id_, tablet_handle))) {
    LOG_WARN("failed to get tablet handle", K(ret), K(tablet_handle));
  } else if (OB_FAIL(create_ddl_ro_sstable(*tablet_handle.get_obj(), arena_allocator_, sstable_handle))) {
    LOG_WARN("failed to create ddl ro sstable ", K(ret));
  }

  // update table store
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(sstable_handle.get_sstable(ddl_major_sstable))) {
    LOG_WARN("failed to get sstable", K(ret));
  } else if (OB_FAIL(update_table_store(ddl_major_sstable, storage_schema_, table_key_, *(tablet_handle.get_obj())))) {
    LOG_WARN("failed to update table store", K(ret));
  }

  // sync max lob id
  if (OB_FAIL(ret) || ObDirectLoadMgrRole::LOB_TABLET_TYPE!= role_) {
  } else if (OB_FAIL(ObDDLUtil::set_tablet_autoinc_seq(ls_id_,tablet_id_, last_lob_id_))) {
    LOG_WARN("failed toset lob tablet autoinc seq", K(ret), K(ls_id_), K(tablet_id_), K(role_), K(last_lob_id_));
  }
  return ret;
}


int ObSSTabletDirectLoadMgr::update_max_lob_id(const int64_t lob_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObLatchWGuard guard(lock_, ObLatchIds::TABLET_DIRECT_LOAD_MGR_LOCK);
    last_lob_id_ = MAX(last_lob_id_, lob_id);
  }
  return ret;
}
