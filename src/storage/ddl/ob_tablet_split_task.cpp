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
#include "ob_tablet_split_task.h"
#include "logservice/ob_log_service.h"
#include "lib/ob_define.h"
#include "share/ob_ddl_sim_point.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "storage/access/ob_multiple_scan_merge.h"

#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/compaction_v2/ob_ss_compact_helper.h"
#include "storage/shared_storage/prewarm/ob_macro_prewarm_struct.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/shared_storage/ob_ss_local_cache_service.h"
#include "storage/incremental/share/ob_shared_meta_iter_guard.h"
#endif

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace compaction;
using namespace share;
using namespace share::schema;
using namespace blocksstable;

namespace storage
{

bool is_data_split_dag(const ObDagType::ObDagTypeEnum &dag_type)
{
  return ObDagType::DAG_TYPE_TABLET_SPLIT == dag_type
      || ObDagType::DAG_TYPE_LOB_SPLIT == dag_type;
}

#define DESTROY_BUILT_MAP(alloc, builded_map, KEY, VALUE)                \
({                                                                       \
  GetMapItemKeyFn<KEY, VALUE*> get_map_item_key_fn;                      \
  if (builded_map.created()                                              \
      && OB_FAIL(builded_map.foreach_refactored(get_map_item_key_fn))) { \
    LOG_ERROR("foreach refactored failed", K(ret));                      \
  }                                                                      \
  for (int64_t i = 0; i < get_map_item_key_fn.map_keys_.count(); i++) {  \
    VALUE *value = nullptr;                                              \
    const KEY &key = get_map_item_key_fn.map_keys_.at(i);                \
    if (OB_FAIL(builded_map.erase_refactored(key, &value))) {            \
      LOG_ERROR("erase refactored failed", K(ret), K(key));              \
    }                                                                    \
    if (OB_NOT_NULL(value)) {                                            \
      value->~VALUE();                                                   \
      alloc.free(value);                                                 \
      value = nullptr;                                                   \
    }                                                                    \
  }                                                                      \
  builded_map.destroy();                                                 \
})


ObTabletSplitParam::ObTabletSplitParam()
  : rowkey_allocator_("SplitRangePar", OB_MALLOC_NORMAL_BLOCK_SIZE /*8KB*/, MTL_ID()),
    is_inited_(false),
    tenant_id_(OB_INVALID_ID), ls_id_(), table_id_(OB_INVALID_ID),
    schema_version_(0), task_id_(0), source_tablet_id_(),
    dest_tablets_id_(), compaction_scn_(0), user_parallelism_(0),
    compat_mode_(lib::Worker::CompatMode::INVALID),  data_format_version_(0), consumer_group_id_(0),
    can_reuse_macro_block_(false), split_sstable_type_(share::ObSplitSSTableType::SPLIT_BOTH),
    parallel_datum_rowkey_list_(), min_split_start_scn_()
{
}

ObTabletSplitParam::~ObTabletSplitParam()
{
  parallel_datum_rowkey_list_.reset();
  rowkey_allocator_.reset();
}

bool ObTabletSplitParam::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && ls_id_.is_valid() && OB_INVALID_ID != table_id_
      && schema_version_ > 0 && task_id_ > 0 && source_tablet_id_.is_valid()
      && dest_tablets_id_.count() > 0 && user_parallelism_ > 0
      && compat_mode_ != lib::Worker::CompatMode::INVALID && data_format_version_ > 0 && consumer_group_id_ >= 0
      && parallel_datum_rowkey_list_.count() > 0;
}

int ObTabletSplitParam::init(
    const ObTabletSplitParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(param));
  } else if (OB_FAIL(dest_tablets_id_.assign(param.dest_tablets_id_))) {
    LOG_WARN("assign failed", K(ret));
  } else if (OB_FAIL(parallel_datum_rowkey_list_.prepare_allocate(param.parallel_datum_rowkey_list_.count()))) {
    LOG_WARN("prepare alloc failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < param.parallel_datum_rowkey_list_.count(); i++) {
      if (OB_FAIL(param.parallel_datum_rowkey_list_.at(i).deep_copy(parallel_datum_rowkey_list_.at(i), rowkey_allocator_))) {
        // deep copy needed.
        LOG_WARN("alloc range buf failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    tenant_id_           = param.tenant_id_;
    ls_id_               = param.ls_id_;
    table_id_            = param.table_id_;
    schema_version_      = param.schema_version_;
    task_id_             = param.task_id_;
    source_tablet_id_    = param.source_tablet_id_;
    compaction_scn_      = param.compaction_scn_;
    user_parallelism_    = param.user_parallelism_;
    compat_mode_         = param.compat_mode_;
    data_format_version_ = param.data_format_version_;
    consumer_group_id_   = param.consumer_group_id_;
    split_sstable_type_  = param.split_sstable_type_;
    can_reuse_macro_block_ = param.can_reuse_macro_block_;
    min_split_start_scn_   = param.min_split_start_scn_;
    lib::ob_sort(dest_tablets_id_.begin(), dest_tablets_id_.end());
    is_inited_ = true;
  }
  return ret;
}

int ObTabletSplitParam::init(const obrpc::ObDDLBuildSingleReplicaRequestArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(arg));
  } else {
    tenant_id_             = MTL_ID();
    ls_id_                 = arg.ls_id_;
    table_id_              = arg.dest_schema_id_;
    schema_version_        = arg.schema_version_;
    task_id_               = arg.task_id_;
    source_tablet_id_      = arg.source_tablet_id_;
    compaction_scn_        = arg.compaction_scn_;
    user_parallelism_      = arg.parallel_datum_rowkey_list_.count() - 1;
    data_format_version_   = arg.data_format_version_;
    consumer_group_id_     = arg.consumer_group_id_;
    split_sstable_type_    = arg.split_sstable_type_;
    can_reuse_macro_block_ = arg.can_reuse_macro_block_;
    min_split_start_scn_   = arg.min_split_start_scn_;
    if (OB_FAIL(parallel_datum_rowkey_list_.assign(arg.parallel_datum_rowkey_list_))) { // shallow cpy.
      LOG_WARN("convert to range failed", K(ret), "parall_info", arg.parallel_datum_rowkey_list_);
    } else if (OB_FAIL(ObTabletSplitUtil::get_split_dest_tablets_info(ls_id_, source_tablet_id_, dest_tablets_id_, compat_mode_))) {
      LOG_WARN("get split dest tablets failed", K(ret), K(arg));
    }
  }
  return ret;
}

int ObTabletSplitParam::init(const obrpc::ObTabletSplitArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(arg));
  } else {
    tenant_id_             = MTL_ID();
    ls_id_                 = arg.ls_id_;
    table_id_              = arg.table_id_;
    schema_version_        = arg.schema_version_;
    task_id_               = arg.task_id_;
    source_tablet_id_      = arg.source_tablet_id_;
    compaction_scn_        = arg.compaction_scn_;
    user_parallelism_      = arg.parallel_datum_rowkey_list_.count() - 1;
    data_format_version_   = arg.data_format_version_;
    consumer_group_id_     = arg.consumer_group_id_;
    split_sstable_type_    = arg.split_sstable_type_;
    can_reuse_macro_block_ = arg.can_reuse_macro_block_;
    min_split_start_scn_   = arg.min_split_start_scn_;
    ObArray<ObTabletID> unused_tablet_ids;
    if (OB_FAIL(ObTabletSplitUtil::get_split_dest_tablets_info(ls_id_, source_tablet_id_, unused_tablet_ids, compat_mode_))) {
      LOG_WARN("get split dest tablets failed", K(ret), K(arg));
    } else if (OB_FAIL(parallel_datum_rowkey_list_.assign(arg.parallel_datum_rowkey_list_))) { // shallow cpy.
      LOG_WARN("convert to range failed", K(ret), "parall_info", arg.parallel_datum_rowkey_list_);
    } else if (OB_FAIL(dest_tablets_id_.assign(arg.dest_tablets_id_))) {
      LOG_WARN("assign failed", K(ret), K(arg));
    }
  }
  return ret;
}

ObTabletSplitCtx::ObTabletSplitCtx() :
    range_allocator_("SplitRangeCtx", OB_MALLOC_NORMAL_BLOCK_SIZE /*8KB*/, MTL_ID()),
    allocator_("SplitCtx", OB_MALLOC_NORMAL_BLOCK_SIZE /*8KB*/, MTL_ID()),
    lock_(ObLatchIds::TABLET_SPLIT_CONTEXT_LOCK),
    is_inited_(false), complement_data_ret_(OB_SUCCESS), ls_handle_(), tablet_handle_(),
    index_builder_map_(), clipped_schemas_map_(),
    skipped_split_major_keys_(), row_inserted_(0), cg_row_inserted_(0), physical_row_count_(0),
    split_point_major_macros_(), split_point_minor_macros_(), parallel_cnt_of_each_sstable_(-1),
    split_scn_(), reorg_scn_(), ls_rebuild_seq_(-1),
    split_majors_count_(-1), max_major_snapshot_(-1)
  #ifdef OB_BUILD_SHARED_STORAGE
    , ss_split_helper_(), is_data_split_executor_(false)
  #endif
{
}

ObTabletSplitCtx::~ObTabletSplitCtx()
{
  int ret = OB_SUCCESS;
  is_inited_ = false;
  is_split_finish_with_meta_flag_ = false;
  ls_rebuild_seq_ = -1;
  split_majors_count_ = -1;
  max_major_snapshot_ = -1;
  complement_data_ret_ = OB_SUCCESS;
  ls_handle_.reset();
  tablet_handle_.reset();
  DESTROY_BUILT_MAP(allocator_, index_builder_map_, ObSplitSSTableTaskKey, ObSSTableIndexBuilder);
  DESTROY_BUILT_MAP(allocator_, clipped_schemas_map_, ObITable::TableKey, ObStorageSchema);
  table_store_iterator_.reset();
  data_split_ranges_.reset();
  skipped_split_major_keys_.reset();
  range_allocator_.reset();
  allocator_.reset();
}

bool ObTabletSplitCtx::is_valid() const
{
  return is_inited_ && ls_handle_.is_valid() && tablet_handle_.is_valid()
    && parallel_cnt_of_each_sstable_ >= 1;
}

int ObTabletSplitCtx::init(const ObTabletSplitParam &param)
{
  int ret = OB_SUCCESS;
  ObTabletHandle local_tablet_hdl;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(param));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(param.ls_id_, ls_handle_, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(param));
  } else if (OB_FAIL(ObTabletSplitUtil::get_tablet(allocator_, ls_handle_,
      param.source_tablet_id_, false/*is_shared_mode*/, local_tablet_hdl,
      ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    LOG_WARN("get local tablet failed", K(ret), K(param));
  } else if (OB_FAIL(ObTabletSplitUtil::check_satisfy_split_condition(ls_handle_, local_tablet_hdl,
      param.dest_tablets_id_, param.compaction_scn_, param.min_split_start_scn_
#ifdef OB_BUILD_SHARED_STORAGE
      , is_data_split_executor_
#endif
      ))) {
    if (OB_NEED_RETRY != ret) {
      LOG_WARN("check satisfy split condition failed", K(ret), K(param));
    }
  } else if (OB_FAIL(ObTabletSplitUtil::get_tablet(allocator_, ls_handle_,
      param.source_tablet_id_, GCTX.is_shared_storage_mode()/*is_shared_mode*/,
      tablet_handle_,
      ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    LOG_WARN("get tablet failed", K(ret), "tablet_id", param.source_tablet_id_, K(GCTX.is_shared_storage_mode()));
  } else if (OB_FAIL(tablet_handle_.get_obj()->get_all_tables(table_store_iterator_))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_FAIL(get_split_majors_infos())) {
    LOG_WARN("get split majors infos failed", K(ret));
  } else if (OB_FAIL(ObTabletSplitUtil::check_sstables_skip_data_split(
      ls_handle_, table_store_iterator_, param.dest_tablets_id_, OB_INVALID_VERSION/*lob_major_snapshot*/, skipped_split_major_keys_))) {
    LOG_WARN("check sstables skip data split failed", K(ret));
  } else if (OB_FAIL(ObTabletSplitUtil::convert_rowkey_to_range(range_allocator_, param.parallel_datum_rowkey_list_, data_split_ranges_))) {
    LOG_WARN("convert to range failed", K(ret), K(param));
  } else if (OB_FAIL(ObTabletSplitUtil::check_data_split_finished(param.ls_id_, param.source_tablet_id_,
      param.dest_tablets_id_ , param.can_reuse_macro_block_, is_split_finish_with_meta_flag_))) {
    LOG_WARN("check all tablets major exist failed", K(ret), K(param.ls_id_), K(param.dest_tablets_id_));
  } else {
    common::ObArenaAllocator tmp_arena("GetSplitTab", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObTabletHandle local_dest_tablet_hdl;
    if (OB_UNLIKELY(param.dest_tablets_id_.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected err", K(ret), K(param));
    } else if (OB_FAIL(ObTabletSplitUtil::get_tablet(tmp_arena, ls_handle_,
        param.dest_tablets_id_.at(0), false/*is_shared_mode*/,
        local_dest_tablet_hdl,
        ObMDSGetTabletMode::READ_ALL_COMMITED))) {
      LOG_WARN("get tablet failed", K(ret), "tablet_id", param.dest_tablets_id_.at(0));
    } else if (OB_UNLIKELY(!local_dest_tablet_hdl.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected err", K(ret), K(local_dest_tablet_hdl));
    } else {
      split_scn_ = local_dest_tablet_hdl.get_obj()->get_tablet_meta().split_info_.get_split_start_scn();
      reorg_scn_ = local_dest_tablet_hdl.get_obj()->get_reorganization_scn();
      if (OB_UNLIKELY((!split_scn_.is_valid() && param.data_format_version_ >= DATA_VERSION_4_4_0_0) || !reorg_scn_.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected err", K(ret), K(split_scn_), K(reorg_scn_), "data_format_version", param.data_format_version_);
      } else if (!split_scn_.is_valid()) {
        FLOG_INFO("invalid split scn from tablet meta", K(ret), K(param));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ls_rebuild_seq_ = ls_handle_.get_ls()->get_rebuild_seq();
    complement_data_ret_ = OB_SUCCESS;
    parallel_cnt_of_each_sstable_ = param.can_reuse_macro_block_ ?
        1 : data_split_ranges_.count();
    is_inited_ = true;
  }
  return ret;
}

int ObTabletSplitCtx::get_clipped_storage_schema_on_demand(
    const ObTabletID &src_tablet_id,
    const ObSSTable &src_sstable,
    const ObStorageSchema &latest_schema,
    const bool try_create,
    const ObStorageSchema *&storage_schema)
{
  int ret = OB_SUCCESS;
  storage_schema = nullptr;
  ObSSTableMetaHandle meta_handle;
  if (OB_UNLIKELY(!latest_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(latest_schema));
  } else if (!src_sstable.is_major_sstable()) {
    storage_schema = &latest_schema;
  } else if (!clipped_schemas_map_.created()
      && OB_FAIL(clipped_schemas_map_.create(8/*bucket_num*/, "ClippedSchema"))) {
    LOG_WARN("create sstable record map failed", K(ret));
  } else if (OB_FAIL(src_sstable.get_meta(meta_handle))) {
    LOG_WARN("get meta failed", K(ret));
  } else {
    int64_t schema_stored_cols_cnt = 0;
    ObStorageSchema *target_storage_schema = nullptr;
    schema_stored_cols_cnt = meta_handle.get_sstable_meta().get_schema_column_count();
    const ObITable::TableKey &source_table_key = src_sstable.get_key();
    if (OB_FAIL(clipped_schemas_map_.get_refactored(source_table_key, target_storage_schema))) {
      void *buf = nullptr;
      target_storage_schema = nullptr;
      ObUpdateCSReplicaSchemaParam update_param;
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("get storage schema failed", K(ret), K(source_table_key));
      } else if (OB_UNLIKELY(!try_create)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("clipped storage schema found failed", K(ret), K(schema_stored_cols_cnt), K(src_sstable));
      } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObStorageSchema)))) { // override ret is expected.
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc mem failed", K(ret));
      } else if (OB_FALSE_IT(target_storage_schema = new(buf)ObStorageSchema())) {
      } else if (OB_FAIL(update_param.init(src_tablet_id,
          schema_stored_cols_cnt + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt()/*major_column_cnt*/,
          ObUpdateCSReplicaSchemaParam::UpdateType::TRUNCATE_COLUMN_ARRAY))) {
        LOG_WARN("update param failed", K(ret), K(src_tablet_id));
      } else if (OB_FAIL(target_storage_schema->init(allocator_,
            latest_schema/*old_schema*/,
            false/*skip_column_info*/,
            nullptr/*column_group_schema*/,
            false/*generate_cs_replica_cg_array*/,
            &update_param/*ObUpdateCSReplicaSchemaParam*/))) {
        LOG_WARN("init storage schema for tablet split failed", K(ret), K(update_param));
      } else if (OB_FAIL(clipped_schemas_map_.set_refactored(source_table_key, target_storage_schema))) {
        LOG_WARN("set refactored failed", K(ret));
      } else {
        target_storage_schema->schema_version_ = meta_handle.get_sstable_meta().get_schema_version();
        target_storage_schema->progressive_merge_round_ = meta_handle.get_sstable_meta().get_progressive_merge_round();
        storage_schema = target_storage_schema;
      }
      if (OB_FAIL(ret) && nullptr != buf) {
        if (nullptr != target_storage_schema) {
          target_storage_schema->~ObStorageSchema();
          target_storage_schema = nullptr;
        }
        allocator_.free(buf);
        buf = nullptr;
      }
    } else {
      storage_schema = target_storage_schema;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(nullptr == storage_schema || !storage_schema->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected storage schema", K(ret), K(src_sstable), KPC(storage_schema));
    }
  }
  return ret;
}

int ObTabletSplitCtx::append_split_point_macros(
    const bool is_major_macros,
    const ObIArray<blocksstable::MacroBlockId> &additional_macros)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(additional_macros.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(additional_macros));
  } else {
    ObSpinLockGuard guard(lock_);
    ObIArray<blocksstable::MacroBlockId> &split_point_macros =
      is_major_macros ? split_point_major_macros_ : split_point_minor_macros_;
    if (OB_FAIL(append(split_point_macros/*dst*/, additional_macros/*src*/))) {
      LOG_WARN("append failed", K(ret));
    }
  }
  return ret;
}

int ObTabletSplitCtx::get_split_majors_infos()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObITable *, MAX_SSTABLE_CNT_IN_STORAGE> major_sstables;
  if (OB_FAIL(ObTabletSplitUtil::get_participants(
      ObSplitSSTableType::SPLIT_MAJOR, table_store_iterator_,
      false/*is_table_restore*/,
      ObArray<ObITable::TableKey>()/*skip_split_majors*/,
      major_sstables))) {
    LOG_WARN("get participant sstables failed", K(ret));
  } else if (OB_UNLIKELY(major_sstables.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no major sstables", K(ret), KPC(this));
  } else {
    split_majors_count_ = major_sstables.count();
    max_major_snapshot_ = major_sstables.at(major_sstables.count() - 1)->get_snapshot_version();
  }
  if (OB_SUCC(ret) && (split_majors_count_ <= 0 || max_major_snapshot_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no major index found", K(ret), K(split_majors_count_), K(max_major_snapshot_));
  }
  return ret;
}

int ObTabletSplitCtx::get_index_in_source_sstables(
    const ObSSTable &src_sstable,
    int64_t &sstable_index)
{
  int ret = OB_SUCCESS;
  sstable_index = -1;
  ObSEArray<ObITable *, MAX_SSTABLE_CNT_IN_STORAGE> source_sstables;
  share::ObSplitSSTableType tmp_type = src_sstable.is_major_sstable() ?
      ObSplitSSTableType::SPLIT_MAJOR : ObSplitSSTableType::SPLIT_MINOR;
  if (OB_UNLIKELY(!src_sstable.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(src_sstable));
  } else if (OB_FAIL(ObTabletSplitUtil::get_participants(
      tmp_type/*split_sstable_type*/, table_store_iterator_,
      false/*is_table_restore*/,
      ObArray<ObITable::TableKey>()/*skip_split_majors*/,
      source_sstables))) {
    LOG_WARN("get participant sstables failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && (sstable_index == -1) && i < source_sstables.count(); i++) {
      if (src_sstable.get_key() == source_sstables.at(i)->get_key()) {
        sstable_index = i;
      }
    }
  }
  if (OB_SUCC(ret) && sstable_index < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no major index found", K(ret), K(sstable_index), K(src_sstable), K(source_sstables));
  }
  return ret;
}

int ObTabletSplitCtx::prepare_index_builder(
    const ObTabletSplitParam &param)
{
  int ret = OB_SUCCESS;
  const int64_t bucket_num = 16;
  ObSEArray<ObITable *, MAX_SSTABLE_CNT_IN_STORAGE> sstables;
  common::ObArenaAllocator tmp_arena("SplitScm", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());

  ObStorageSchema *storage_schema = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(param));
  } else if (OB_UNLIKELY(index_builder_map_.created())) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(index_builder_map_.create(bucket_num, "SplitSstIdxMap"))) {
    LOG_WARN("create sstable record map failed", K(ret));
  } else if (OB_FAIL(ObTabletSplitUtil::get_participants(
    param.split_sstable_type_, table_store_iterator_, false, skipped_split_major_keys_, sstables))) {
    LOG_WARN("get participant sstables failed", K(ret));
  } else if (OB_FAIL(ObTabletSplitUtil::get_storage_schema_from_mds(tablet_handle_, param.data_format_version_, storage_schema, tmp_arena))) {
    LOG_WARN("failed to get storage schema", K(ret));
  } else if (OB_ISNULL(storage_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr of storage_schema", K(ret));
  } else {
    ObWholeDataStoreDesc data_desc;
    ObTabletHandle tablet_handle;
    compaction::ObExecMode exec_mode = GCTX.is_shared_storage_mode() ?
        ObExecMode::EXEC_MODE_OUTPUT : ObExecMode::EXEC_MODE_LOCAL;
    const share::SCN split_reorganization_scn = split_scn_.is_valid() ? split_scn_ : SCN::min_scn()/*use min_scn to avoid invalid*/;
    for (int64_t i = 0; OB_SUCC(ret) && i < sstables.count(); i++) {
      ObSSTable *sstable = static_cast<ObSSTable *>(sstables.at(i));
      for (int64_t j = 0; OB_SUCC(ret) && j < param.dest_tablets_id_.count(); j++) {
        data_desc.reset();
        tablet_handle.reset();
        void *buf = nullptr;
        ObSplitSSTableTaskKey key;
        ObSSTableIndexBuilder *sstable_index_builder = nullptr;
        const ObTabletID dst_tablet_id = param.dest_tablets_id_.at(j);
        ObITable::TableKey dest_table_key = sstable->get_key();
        dest_table_key.tablet_id_ = dst_tablet_id;
        key.src_sst_key_ = sstable->get_key();
        key.dest_tablet_id_ = dst_tablet_id;
        const ObMergeType merge_type = sstable->is_major_sstable() ? MAJOR_MERGE : MINOR_MERGE;
        const int64_t snapshot_version = sstable->is_major_sstable() ?
          sstable->get_snapshot_version() : sstable->get_end_scn().get_val_for_tx();
        const ObStorageSchema *clipped_storage_schema = nullptr;
        int32_t private_transfer_epoch = -1;
        if (OB_FAIL(get_clipped_storage_schema_on_demand(
            param.source_tablet_id_, *sstable, *storage_schema,
            true/*try_create*/, clipped_storage_schema))) {
          LOG_WARN("get storage schema via sstable failed", K(ret));
        } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle_, dst_tablet_id, tablet_handle))) {
          LOG_WARN("get tablet failed", K(ret));
        } else if (OB_FAIL(tablet_handle.get_obj()->get_private_transfer_epoch(private_transfer_epoch))) {
          LOG_WARN("failed to get private transfer epoch", K(ret), "tablet_meta", tablet_handle.get_obj()->get_tablet_meta());
        } else if (OB_FAIL(data_desc.init(
            true/*is_ddl*/, *clipped_storage_schema, param.ls_id_,
            dst_tablet_id, merge_type, snapshot_version, param.data_format_version_,
            tablet_handle.get_obj()->get_tablet_meta().micro_index_clustered_,
            private_transfer_epoch,
            0/*concurrent_cnt*/,
            split_reorganization_scn, sstable->get_end_scn(),
            nullptr/*cg_schema*/,
            0/*table_cg_idx*/,
            exec_mode))) {
          LOG_WARN("fail to init data store desc", K(ret), K(dst_tablet_id), K(param));
        } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObSSTableIndexBuilder)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc memory failed", K(ret));
        } else if (FALSE_IT(sstable_index_builder = new (buf) ObSSTableIndexBuilder(false/*use double write buffer*/))) {
        } else if (OB_FAIL(sstable_index_builder->init(data_desc.get_desc(), ObSSTableIndexBuilder::DISABLE))) {
          LOG_WARN("init sstable index builder failed", K(ret));
        } else if (OB_FAIL(index_builder_map_.set_refactored(key, sstable_index_builder))) {
          LOG_WARN("set refactored failed", K(ret));
        }

        if (OB_FAIL(ret)) {
          // other newly-allocated sstable index builders will be deconstructed when deconstruct the ctx.
          if (nullptr != sstable_index_builder) {
            sstable_index_builder->~ObSSTableIndexBuilder();
            sstable_index_builder = nullptr;
          }
          if (nullptr != buf) {
            allocator_.free(buf);
            buf = nullptr;
          }
        }
      }
    }
  }
  return ret;
}

ObTabletSplitDag::ObTabletSplitDag()
  : ObIDataSplitDag(ObDagType::DAG_TYPE_TABLET_SPLIT), is_inited_(false), param_(), context_()
{
}

ObTabletSplitDag::~ObTabletSplitDag()
{
}


int ObTabletSplitDag::calc_total_row_count() {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("has not been inited ", K(ret));
  } else if (OB_UNLIKELY(!param_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(param_));
  } else if (context_.physical_row_count_ != 0) {
    ret =  OB_INIT_TWICE;
    LOG_WARN("has calculated the row_count", K(ret), K(context_.physical_row_count_));
  } else if (OB_FAIL(ObDDLUtil::get_tablet_physical_row_cnt(
                                  param_.ls_id_,
                                  param_.source_tablet_id_,
                                  true, // calc_sstable = true;
                                  false, // calc_memtable = false;  because memtable has been frozen.
                                  context_.physical_row_count_))) {
    LOG_WARN("failed to get physical row count of tablet", K(ret), K(param_), K(context_));
  }
  LOG_INFO("calc row count of the src tablet", K(ret), K(context_));
  return ret;
}

int ObTabletSplitDag::init_by_param(const share::ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  const ObTabletSplitParam *tmp_param = static_cast<const ObTabletSplitParam *>(param);
  if (OB_UNLIKELY(nullptr == tmp_param || !tmp_param->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), KPC(tmp_param));
  } else if (OB_FAIL(param_.init(*tmp_param))) {
    LOG_WARN("init tablet split param failed", K(ret));
  } else if (OB_UNLIKELY(!param_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), K(param_));
  } else if (OB_FAIL(context_.init(param_))) {
    if (OB_NEED_RETRY != ret) {
      LOG_WARN("init failed", K(ret));
    } else if (REACH_COUNT_INTERVAL(1000L)) {
      LOG_INFO("wait conditions satisfied", K(ret), KPC(tmp_param));
    }
  } else {
    consumer_group_id_ = tmp_param->consumer_group_id_;
    is_inited_ = true;
  }
  return ret;
}

int ObIDataSplitDag::alloc_and_add_common_task(
  ObITask *last_task,
  const int64_t rebuild_seq,
  const ObLSID &ls_id,
  const ObTabletID &src_tablet_id,
  const ObIArray<ObTabletID> &dst_tablet_ids,
  const bool can_reuse_macro_block,
  const share::SCN &dest_reorg_scn,
  const share::SCN &split_start_scn)
{
  int ret = OB_SUCCESS;
  ObSplitFinishTask *finish_task = nullptr;
  if (OB_FAIL(alloc_task(finish_task))) {
    LOG_WARN("alloc task failed", K(ret));
  } else if (OB_FAIL(finish_task->init())) {
    LOG_WARN("init failed", K(ret));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (GCTX.is_shared_storage_mode()) {
    ObSplitDownloadSSTableTask *download_sstable_task = nullptr;
    if (OB_FAIL(alloc_task(download_sstable_task))) {
      LOG_WARN("alloc failed", K(ret));
    } else if (OB_ISNULL(download_sstable_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null task", K(ret));
    } else if (OB_FAIL(download_sstable_task->init(
        rebuild_seq,
        ls_id,
        src_tablet_id,
        dst_tablet_ids,
        can_reuse_macro_block,
        dest_reorg_scn,
        split_start_scn))) {
      LOG_WARN("init failed", K(ret));
    } else if (nullptr != last_task
        && OB_FAIL(last_task->add_child(*download_sstable_task))) {
      LOG_WARN("add child task failed", K(ret));
    } else if (OB_FAIL(download_sstable_task->add_child(*finish_task))) {
      LOG_WARN("add child task failed", K(ret));
    } else if (OB_FAIL(add_task(*download_sstable_task))) {
      LOG_WARN("add task failed", K(ret));
    } else if (OB_FAIL(add_task(*finish_task))) {
      LOG_WARN("add task failed", K(ret));
    }
#endif
  } else {
    if (OB_ISNULL(last_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null last", K(ret), KPC(last_task));
    } else if (OB_FAIL(last_task->add_child(*finish_task))) {
      LOG_WARN("add child task failed", K(ret));
    } else if (OB_FAIL(add_task(*finish_task))) {
      LOG_WARN("add task failed", K(ret));
    }
  }
  return ret;
}

int ObTabletSplitDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObITable *, MAX_SSTABLE_CNT_IN_STORAGE> source_sstables;
  ObTabletSplitPrepareTask *prepare_task = nullptr;
  ObTabletSplitMergeTask *merge_task = nullptr;
  ObSplitFinishTask *finish_task = nullptr;

  int64_t epoch = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (GCTX.is_shared_storage_mode() && !context_.is_data_split_executor_) {
    // only has DownLoadSSTableTask and FinishTask for non data split executor.
    if (OB_FAIL(alloc_and_add_common_task(
          nullptr/*last_task*/,
          context_.ls_rebuild_seq_,
          param_.ls_id_,
          param_.source_tablet_id_,
          param_.dest_tablets_id_,
          param_.can_reuse_macro_block_,
          context_.reorg_scn_,
          context_.split_scn_))) {
      LOG_WARN("alloc and add common task failed", K(ret));
    }
    // TODO YIREN, CHANGE TO TRACE LOG LATER.
    LOG_INFO("non data-split executor, execute download op only",
      "ls_id", param_.ls_id_,
      "src_tablet_id", param_.source_tablet_id_,
      K(epoch));
#endif
  } else if (OB_FAIL(ObTabletSplitUtil::get_participants(
      param_.split_sstable_type_, context_.table_store_iterator_, false/*is_table_restore*/,
      context_.skipped_split_major_keys_, source_sstables))) {
    LOG_WARN("get all sstables failed", K(ret));
  } else if (OB_FAIL(alloc_task(prepare_task))) {
    LOG_WARN("allocate task failed", K(ret));
  } else if (OB_ISNULL(prepare_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr task", K(ret));
  } else if (OB_FAIL(prepare_task->init(param_, context_))) {
    LOG_WARN("init prepare task failed", K(ret));
  } else if (OB_FAIL(add_task(*prepare_task))) {
    LOG_WARN("add task failed", K(ret));
  } else if (OB_FAIL(alloc_task(merge_task))) {
    LOG_WARN("alloc task failed", K(ret));
  } else if (OB_ISNULL(merge_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr task", K(ret));
  } else if (OB_FAIL(merge_task->init(param_, context_))) {
    LOG_WARN("init merge task failed", K(ret));
  } else if (OB_FAIL(alloc_and_add_common_task(
      merge_task/*last_task*/,
      context_.ls_rebuild_seq_,
      param_.ls_id_,
      param_.source_tablet_id_,
      param_.dest_tablets_id_,
      param_.can_reuse_macro_block_,
      context_.reorg_scn_,
      context_.split_scn_))) {
    LOG_WARN("alloc and add common failed", K(ret));
  } else if (param_.can_reuse_macro_block_) {
    // concurrent cnt equals to the count of sstables.
    for (int64_t i = 0; OB_SUCC(ret) && i < source_sstables.count(); i++) {
      ObTabletSplitWriteTask *write_task = nullptr;
      if (OB_ISNULL(source_sstables.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr sstable", K(ret), K(param_));
      } else if (OB_FAIL(alloc_task(write_task))) {
        LOG_WARN("alloc task failed", K(ret));
      } else if (OB_ISNULL(write_task)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr task", K(ret));
      } else if (OB_FAIL(write_task->init(0/*task_id*/, param_, context_, source_sstables.at(i)))) {
        LOG_WARN("init write task failed", K(ret));
      } else if (OB_FAIL(prepare_task->add_child(*write_task))) {
        LOG_WARN("add child task failed", K(ret));
      } else if (OB_FAIL(add_task(*write_task))) {
        LOG_WARN("add task failed", K(ret));
      } else if (OB_FAIL(write_task->add_child(*merge_task))) {
        LOG_WARN("add child task failed", K(ret));
      }
    }
    // child task is recommended to be added after all the other tasks are added.
    if (FAILEDx(add_task(*merge_task))) {
      LOG_WARN("add task failed", K(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < source_sstables.count(); i++) {
      if (OB_ISNULL(source_sstables.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error", K(ret), "sstable", source_sstables.at(i));
      } else {
        ObTabletSplitWriteTask *write_task = nullptr;
        if (OB_FAIL(alloc_task(write_task))) {
          LOG_WARN("alloc task failed", K(ret));
        } else if (OB_ISNULL(write_task)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected nullptr task", K(ret));
        } else if (OB_FAIL(write_task->init(0 /*task_id*/, param_, context_, source_sstables.at(i)))) {
          LOG_WARN("init write task failed", K(ret));
        } else if (OB_FAIL(prepare_task->add_child(*write_task))) {
          LOG_WARN("add child task failed", K(ret));
        } else if (OB_FAIL(add_task(*write_task))) {
          LOG_WARN("add task failed", K(ret));
        } else if (OB_FAIL(write_task->add_child(*merge_task))) {
          LOG_WARN("add child task failed", K(ret));
        }
      }
    }
    // child task is recommended to be added after all the other tasks are added.
    if (FAILEDx(add_task(*merge_task))) {
      LOG_WARN("add task failed", K(ret));
    }
  }

  FLOG_INFO("create first task finish", K(ret),
#ifdef OB_BUILD_SHARED_STORAGE
    K(context_.is_data_split_executor_),
#endif
    "can_reuse_macro_block", param_.can_reuse_macro_block_, "sstables_count", source_sstables.count(), K(param_), K(context_));
  return ret;
}

uint64_t ObTabletSplitDag::hash() const
{
  int ret = OB_SUCCESS;
  uint64_t hash_val = 0;
  if (OB_UNLIKELY(!is_inited_ || !param_.is_valid())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("invalid argument", K(ret), K(is_inited_), K(param_));
  } else {
    hash_val = param_.tenant_id_ + param_.ls_id_.hash()
             + param_.table_id_ + param_.schema_version_
             + param_.source_tablet_id_.hash() + ObDagType::DAG_TYPE_TABLET_SPLIT;
  }
  return hash_val;
}

bool ObTabletSplitDag::operator==(const ObIDag &other) const
{
  int ret = OB_SUCCESS;
  bool is_equal = false;
  if (OB_UNLIKELY(this == &other)) {
    is_equal = true;
  } else if (get_type() == other.get_type()) {
    const ObTabletSplitDag &dag = static_cast<const ObTabletSplitDag &>(other);
    if (OB_UNLIKELY(!param_.is_valid() || !dag.param_.is_valid())) {
      ret = OB_ERR_SYS;
      LOG_WARN("invalid argument", K(ret), K(param_), K(dag.param_));
    } else {
      is_equal = param_.tenant_id_ == dag.param_.tenant_id_
              && param_.ls_id_ == dag.param_.ls_id_
              && param_.schema_version_ == dag.param_.schema_version_
              && param_.source_tablet_id_ == dag.param_.source_tablet_id_;
    }
  }
  return is_equal;
}

int ObTabletSplitDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObComplementDataDag has not been initialized", K(ret));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
      static_cast<int64_t>(param_.ls_id_.id()), static_cast<int64_t>(param_.source_tablet_id_.id())))) {
    LOG_WARN("failed to fill info param", K(ret));
  }
  return ret;
}

int ObTabletSplitDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletSplitDag has not been initialized", K(ret));
  } else if (OB_UNLIKELY(!param_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid params", K(ret), K(param_));
  } else if (param_.can_reuse_macro_block_) {
    if (OB_FAIL(databuff_printf(buf, buf_len,
      "Reuse macro block split: src_tablet_id=%ld, parallelism=%ld, tenant_id=%lu, ls_id=%ld, schema_version=%ld",
      param_.source_tablet_id_.id(), param_.user_parallelism_,
      param_.tenant_id_, param_.ls_id_.id(), param_.schema_version_))) {
      LOG_WARN("fail to fill comment", K(ret), K(param_));
    }
  } else {
    if (OB_FAIL(databuff_printf(buf, buf_len,
      "Regen macro block split: src_tablet_id=%ld, parallelism=%ld, tenant_id=%lu, ls_id=%ld, schema_version=%ld",
      param_.source_tablet_id_.id(), param_.user_parallelism_,
      param_.tenant_id_, param_.ls_id_.id(), param_.schema_version_))) {
      LOG_WARN("fail to fill comment", K(ret), K(param_));
    }
  }
  return ret;
}

int ObTabletSplitDag::report_replica_build_status() const
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(AFTER_TABLET_SPLIT_MERGE_TASK);
  obrpc::ObDDLBuildSingleReplicaResponseArg arg;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletSplitDag has not been inited", K(ret));
  } else if (OB_UNLIKELY(!param_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param", K(ret), K(param_));
  } else {
#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = OB_E(EventTable::EN_DDL_REPORT_REPLICA_BUILD_STATUS_FAIL) OB_SUCCESS;
      LOG_INFO("report replica build status errsim", K(ret));
    }
#endif
    ObAddr rs_addr;
    arg.tenant_id_        = param_.tenant_id_;
    arg.dest_tenant_id_   = param_.tenant_id_;
    arg.ls_id_            = param_.ls_id_;
    arg.dest_ls_id_       = param_.ls_id_;
    arg.tablet_id_        = param_.source_tablet_id_;
    arg.source_table_id_  = param_.table_id_;
    arg.dest_schema_id_   = context_.tablet_handle_.get_obj()->get_tablet_meta().data_tablet_id_.id(); // to fetch DDL Task.
    arg.ret_code_         = context_.complement_data_ret_;
    arg.snapshot_version_ = 1L;
    arg.schema_version_   = param_.schema_version_;
    arg.dest_schema_version_ = param_.schema_version_;
    arg.task_id_          = param_.task_id_;
    arg.execution_id_     = 1L; /*execution_id*/
    arg.server_addr_      = GCTX.self_addr();
    arg.row_inserted_     = context_.row_inserted_;
    arg.physical_row_count_  = context_.physical_row_count_;
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(GCTX.rs_rpc_proxy_) || OB_ISNULL(GCTX.rs_mgr_)) {
      ret = OB_ERR_SYS;
      LOG_WARN("inner system error, rootserver rpc proxy or rs mgr must not be NULL", K(ret), K(GCTX));
    } else if (OB_FAIL(GCTX.rs_mgr_->get_master_root_server(rs_addr))) {
      LOG_WARN("fail to get rootservice address", K(ret));
    } else if (OB_FAIL(GCTX.rs_rpc_proxy_->to(rs_addr).build_ddl_single_replica_response(arg))) {
      LOG_WARN("fail to send build ddl single replica response", K(ret), K(arg));
    }
    bool is_split_executor = true;
  #ifdef OB_BUILD_SHARED_STORAGE
    is_split_executor = GCTX.is_shared_storage_mode() ? context_.is_data_split_executor_ : is_split_executor;
  #endif
    char split_basic_info[common::MAX_ROOTSERVICE_EVENT_VALUE_LENGTH/*512*/];
    memset(split_basic_info, 0, sizeof(split_basic_info));
    snprintf(split_basic_info, sizeof(split_basic_info),
      "tenant_id: %ld, ls_id: %ld, src_tablet_id: %ld, dst_tablet_ids: %ld, %ld, can_reuse_macro: %d, is_split_executor: %d",
      MTL_ID(), param_.ls_id_.id(), param_.source_tablet_id_.id(),
      param_.dest_tablets_id_.empty() ? 0 : param_.dest_tablets_id_.at(0).id(),
      param_.dest_tablets_id_.empty() ? 0 : param_.dest_tablets_id_.at(param_.dest_tablets_id_.count() - 1).id(),
      param_.can_reuse_macro_block_,
      is_split_executor);
    SERVER_EVENT_ADD("ddl", "split_resp",
        "result", context_.complement_data_ret_,
        "split_basic_info", split_basic_info,
        "physical_row_count", context_.physical_row_count_,
        "split_total_rows", context_.row_inserted_,
        "trace_id", *ObCurTraceId::get_trace_id());
  }
  FLOG_INFO("send tablet split response to RS", K(ret), K(context_), K(arg));
  return ret;
}

int ObTabletSplitPrepareTask::init(
    ObTabletSplitParam &param, ObTabletSplitCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid() || !ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(param), K(ctx));
  } else {
    param_ = &param;
    context_ = &ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObTabletSplitPrepareTask::prepare_context()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(context_->prepare_index_builder(*param_))) {
    LOG_WARN("prepare index builder failed", K(ret), KPC_(param));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (GCTX.is_shared_storage_mode()) {
    ObSSDataSplitHelper &ss_split_helper = context_->ss_split_helper_;
    if (OB_FAIL(ss_split_helper.start_add_minor_op(
        param_->ls_id_,
        context_->split_scn_,
        context_->parallel_cnt_of_each_sstable_,
        context_->table_store_iterator_,
        param_->dest_tablets_id_))) {
      LOG_WARN("start add minor op failed", K(ret), KPC(param_));
    } else if (OB_FAIL(ss_split_helper.persist_majors_gc_rely_info(
        param_->ls_id_,
        param_->dest_tablets_id_,
        context_->reorg_scn_,
        context_->split_majors_count_,
        context_->max_major_snapshot_,
        context_->parallel_cnt_of_each_sstable_/*parallel_cnt*/))) {
      LOG_WARN("persist majors gc rely info failed", K(ret), KPC_(param), KPC(context_));
    }
#endif
  }
  return ret;
}

int ObTabletSplitPrepareTask::process()
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(BEFORE_TABLET_SPLIT_PREPARE_TASK);
  bool is_data_split_finished = false;
  ObIDag *tmp_dag = get_dag();
  ObTabletSplitDag *dag = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(tmp_dag) || ObDagType::DAG_TYPE_TABLET_SPLIT != tmp_dag->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), KP(tmp_dag));
  } else if (OB_ISNULL(dag = static_cast<ObTabletSplitDag *>(tmp_dag))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), KP(tmp_dag), KP(dag));
  } else if (OB_SUCCESS != (context_->complement_data_ret_)) {
    LOG_WARN("complement data has already failed", KPC(context_));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (GCTX.is_shared_storage_mode()
      && OB_FAIL(ObSSDataSplitHelper::create_shared_tablet_if_not_exist(
      param_->ls_id_,
      param_->dest_tablets_id_,
      context_->reorg_scn_))) {
    LOG_WARN("create shared tablet if not exist failed", K(ret));
#endif
  } else if (OB_FAIL(ObTabletSplitUtil::check_dest_data_completed(
      context_->ls_handle_,
      param_->dest_tablets_id_,
      GCTX.is_shared_storage_mode()/*check_remote*/,
      is_data_split_finished))) {
    LOG_WARN("check all major exist failed", K(ret));
  } else if (is_data_split_finished) {
    LOG_INFO("split task has alreay finished", KPC(param_));
  } else if (OB_FAIL(prepare_context())) {
    LOG_WARN("prepare index builder map failed", K(ret), KPC(param_));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(dag->calc_total_row_count())) { // only calc row count once time for a task
    LOG_WARN("failed to calc task row count", K(ret));
  } else {
    #ifdef ERRSIM
      ret = OB_E(EventTable::EN_BLOCK_SPLIT_BEFORE_SSTABLES_SPLIT) OB_SUCCESS;
      if (OB_SUCC(ret)) {
      } else if (OB_EAGAIN == ret) { // ret=-4023, errsim trigger to test orthogonal ls rebuild.
        common::ObZone self_zone;
        ObString zone1_str("z1");
        if (OB_FAIL(SVR_TRACER.get_server_zone(GCTX.self_addr(), self_zone))) { // overwrite ret is expected.
          LOG_WARN("get server zone failed", K(ret));
        } else if (0 != ObCharset::instr(ObCollationType::CS_TYPE_UTF8MB4_GENERAL_CI, self_zone.str().ptr(), self_zone.str().length(),
            zone1_str.ptr(), zone1_str.length())) {
          ret = OB_EAGAIN;
          LOG_INFO("[ERRSIM] set eagain for tablet split", K(ret));
        }
      } else if (OB_DDL_TASK_EXECUTE_TOO_MUCH_TIME == ret) { // ret=-4192, errsim trigger to test orthogonal ls migration.
        common::ObAddr addr;
        const ObAddr &my_addr = GCONF.self_addr_;
        const ObString &errsim_migration_src_server_addr = GCONF.errsim_migration_src_server_addr.str();
        if (!errsim_migration_src_server_addr.empty() && OB_FAIL(addr.parse_from_string(errsim_migration_src_server_addr))) {
          LOG_WARN("failed to parse from string to addr", K(ret), K(errsim_migration_src_server_addr));
        } else if (addr == my_addr) {
          ret = OB_EAGAIN;
          LOG_INFO("[ERRSIM] stuck split task", K(ret));
        } else {
          LOG_INFO("[ERRSIM] skip stuck split task", K(errsim_migration_src_server_addr), K(my_addr));
        }
      } else {
        LOG_WARN("[ERRSIM] unknown errsim type", K(ret));
      }
    #endif
  }

  if (OB_FAIL(ret)) {
    context_->complement_data_ret_ = ret;
    ret = OB_SUCCESS;
  }
  return ret;
}

ObTabletSplitWriteTask::ObTabletSplitWriteTask()
  : ObITask(TASK_TYPE_DDL_SPLIT_WRITE), is_inited_(false),
      param_(nullptr), context_(nullptr), sstable_(nullptr),
      rowkey_read_info_(nullptr),
      write_row_(), default_row_(), task_id_(0),
      allocator_("SplitWriteRow", OB_MALLOC_NORMAL_BLOCK_SIZE /*8KB*/, MTL_ID())
{

}

ObTabletSplitWriteTask::~ObTabletSplitWriteTask()
{
  write_row_.reset();
  default_row_.reset();
  allocator_.reset();
}

int ObTabletSplitWriteTask::init(
    const int64_t task_id,
    ObTabletSplitParam &param,
    ObTabletSplitCtx &ctx,
    storage::ObITable *sstable)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(task_id < 0 || !param.is_valid() || !ctx.is_valid() || nullptr == sstable)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(task_id), K(param), K(ctx), KP(sstable));
  } else {
    task_id_ = task_id;
    param_ = &param;
    context_ = &ctx;
    sstable_ = static_cast<ObSSTable *>(sstable);
    rowkey_read_info_ = &context_->tablet_handle_.get_obj()->get_rowkey_read_info();
    is_inited_ = true;
    LOG_TRACE("init write task successfully", K(ret),
      "end_scn", sstable_->get_end_scn(), K(default_row_));
  }
  return ret;
}

int ObTabletSplitWriteTask::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  next_task = nullptr;
  ObIDag *tmp_dag = get_dag();
  ObTabletSplitDag *dag = nullptr;
  ObTabletSplitWriteTask *next_write_task = nullptr;
  const int64_t next_task_id = task_id_ + 1;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == tmp_dag || ObDagType::DAG_TYPE_TABLET_SPLIT != tmp_dag->get_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), KPC(tmp_dag));
  } else if (FALSE_IT(dag = static_cast<ObTabletSplitDag *> (tmp_dag))) {
  } else if (param_->can_reuse_macro_block_) {
    ret = OB_ITER_END;
  } else if (next_task_id >= context_->data_split_ranges_.count()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(dag->alloc_task(next_write_task))) {
    LOG_WARN("alloc task failed", K(ret));
  } else if (OB_FAIL(next_write_task->init(next_task_id, *param_, *context_, sstable_))) {
    LOG_WARN("init next write task failed", K(ret), K(next_task_id), KPC(param_));
  } else {
    next_task = next_write_task;
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(context_)) {
    if (OB_ITER_END != ret) {
      context_->complement_data_ret_ = ret;
    }
  }
  return ret;
}

int ObTabletSplitWriteTask::prepare_context(const ObStorageSchema *&clipped_storage_schema)
{
  int ret = OB_SUCCESS;
  clipped_storage_schema = nullptr;
  ObArenaAllocator tmp_arena("TmpInitSplitW", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  int64_t column_cnt = 0;
  ObStorageSchema *storage_schema = nullptr;
  ObDatumRow tmp_default_row;
  ObArray<ObColDesc> multi_version_cols_desc;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ObTabletSplitUtil::get_storage_schema_from_mds(context_->tablet_handle_, param_->data_format_version_, storage_schema, allocator_))) {
    LOG_WARN("failed to get storage schema from mds", K(ret));
  } else if (OB_FAIL(context_->get_clipped_storage_schema_on_demand(
      param_->source_tablet_id_, *sstable_, *storage_schema,
      false/*try_create*/, clipped_storage_schema))) {
    LOG_WARN("get storage schema via sstable failed", K(ret));
  } else if (OB_FAIL(clipped_storage_schema->get_multi_version_column_descs(multi_version_cols_desc))) {
    LOG_WARN("get multi version column descs failed", K(ret));
  } else if (OB_FAIL(write_row_.init(allocator_, multi_version_cols_desc.count()))) {
    LOG_WARN("Fail to init write row", K(ret));
  } else if (OB_FAIL(tmp_default_row.init(tmp_arena, multi_version_cols_desc.count()))) { // tmp arena to alloc, and reset after.
    LOG_WARN("init tmp default row failed", K(ret));
  } else if (OB_FAIL(default_row_.init(allocator_, multi_version_cols_desc.count()))) {
    LOG_WARN("init default row failed", K(ret));
  } else if (OB_FAIL(clipped_storage_schema->get_orig_default_row(multi_version_cols_desc, true/*need_trim*/, tmp_default_row))) {
    LOG_WARN("init default row failed", K(ret), KPC(sstable_), KPC(clipped_storage_schema));
  } else if (OB_FAIL(default_row_.deep_copy(tmp_default_row/*src*/, allocator_))) {
    LOG_WARN("failed to deep copy default row", K(ret), KPC(sstable_), KPC(clipped_storage_schema));
  } else if (OB_FAIL(ObLobManager::fill_lob_header(allocator_, multi_version_cols_desc, default_row_))) {
    LOG_WARN("fail to fill lob header for default row", K(ret));
  }
  return ret;
}

int ObTabletSplitWriteTask::process()
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(BEFORE_TABLET_SPLIT_WRITE_TASK);
  bool is_data_split_finished = false;
  ObFixedArray<ObWholeDataStoreDesc, common::ObIAllocator> data_desc_arr;
  ObFixedArray<ObMacroBlockWriter *, common::ObIAllocator> macro_block_writer_arr;
  data_desc_arr.set_allocator(&allocator_);
  macro_block_writer_arr.set_allocator(&allocator_);
  ObTabletSplitInfoMdsUserData split_info_data;
  const ObStorageSchema *clipped_storage_schema = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_SUCCESS != (context_->complement_data_ret_)) {
    LOG_WARN("complement data has already failed", KPC(context_));
  } else if (OB_FAIL(ObTabletSplitUtil::check_dest_data_completed(
      context_->ls_handle_,
      param_->dest_tablets_id_,
      GCTX.is_shared_storage_mode()/*check_remote*/,
      is_data_split_finished))) {
    LOG_WARN("check all major exist failed", K(ret));
  } else if (is_data_split_finished) {
    LOG_INFO("split task has alreay finished", KPC(param_));
  } else if (OB_FAIL(prepare_context(clipped_storage_schema))) {
    LOG_WARN("prepare context failed", K(ret), KPC(this));
  } else if (OB_FAIL(data_desc_arr.init(param_->dest_tablets_id_.count()))) {
    LOG_WARN("init failed", K(ret));
  } else if (OB_FAIL(macro_block_writer_arr.init(param_->dest_tablets_id_.count()))) {
    LOG_WARN("init failed", K(ret));
  } else if (OB_FAIL(prepare_macro_block_writer(*clipped_storage_schema, data_desc_arr, macro_block_writer_arr))) {
    LOG_WARN("prepare macro block writer failed", K(ret));
  } else if (param_->can_reuse_macro_block_
      && OB_FAIL(process_reuse_macro_block_task(macro_block_writer_arr, *clipped_storage_schema))) {
    LOG_WARN("complement data for reuse macro block task failed", K(ret));
  } else if (!param_->can_reuse_macro_block_
      && OB_FAIL(process_rewrite_macro_block_task(macro_block_writer_arr, *clipped_storage_schema))) {
    LOG_WARN("complement data for rewrite macro block task failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < macro_block_writer_arr.count(); i++) {
      if (OB_ISNULL(macro_block_writer_arr.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected err", K(ret));
      } else if (OB_FAIL(macro_block_writer_arr.at(i)->close())) {
        LOG_WARN("close macro block writer failed", K(ret));
      }
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(context_)) {
    context_->complement_data_ret_ = ret;
    ret = OB_SUCCESS;
  }
  // free.
  for (int64_t i = 0; i < macro_block_writer_arr.count(); i++) {
    if (nullptr != macro_block_writer_arr.at(i)) {
      macro_block_writer_arr.at(i)->~ObMacroBlockWriter();
      allocator_.free(macro_block_writer_arr.at(i));
      macro_block_writer_arr.at(i) = nullptr;
    }
  }
  macro_block_writer_arr.reset();
  data_desc_arr.reset();

  if (OB_FAIL(ret) && OB_NOT_NULL(context_)) {
    context_->complement_data_ret_ = ret;
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObTabletSplitWriteTask::prepare_macro_seq_param(
    const int64_t dest_tablet_index,
    ObMacroSeqParam &macro_seq_param)
{
  int ret = OB_SUCCESS;
  ObMacroDataSeq macro_start_seq(0);
  const int64_t parallel_cnt = context_->parallel_cnt_of_each_sstable_;
#ifdef OB_BUILD_SHARED_STORAGE
  const ObSSDataSplitHelper &ss_split_helper = context_->ss_split_helper_;
  if (GCTX.is_shared_storage_mode()) {
    int64_t sstable_index = -1;
    if (OB_FAIL(context_->get_index_in_source_sstables(*sstable_, sstable_index))) {
      LOG_WARN("get major/minor index from sstables failed", K(ret), KPC(sstable_));
    } else if (sstable_->is_major_sstable()) {
      if (OB_FAIL(ss_split_helper.generate_major_macro_seq_info(
            sstable_index/*the index in the generated majors*/,
            parallel_cnt/*the parallel cnt in one sstable*/,
            task_id_/*the parallel idx in one sstable*/,
            macro_start_seq.macro_data_seq_))) {
        LOG_WARN("generate macro start seq failed", K(ret), K(sstable_index), K(parallel_cnt), K(task_id_));
      }
    } else {
       // minor sstable.
      if (OB_FAIL(ss_split_helper.generate_minor_macro_seq_info(
          dest_tablet_index/*index in dest_tables_id*/,
          sstable_index/*the index in the generated minors*/,
          parallel_cnt/*the parallel cnt in one sstable*/,
          task_id_/*the parallel idx in one sstable*/,
          macro_start_seq.macro_data_seq_))) {
        LOG_WARN("generate macro start seq failed", K(ret));
      }
    }
    FLOG_INFO("DEBUG CODE, CHANGE TO TRACE LATER", K(ret),
          K(dest_tablet_index),
          K(sstable_index),
          K(parallel_cnt),
          K(task_id_),
          "end_scn", sstable_->get_end_scn(),
          "macro_start_seq", macro_start_seq.macro_data_seq_);
  } else {
#endif
    ObSSTableMetaHandle meta_handle;
    meta_handle.reset();
    if (OB_FAIL(sstable_->get_meta(meta_handle))) {
      LOG_WARN("get sstable meta failed", K(ret));
    } else if (OB_FAIL(macro_start_seq.set_sstable_seq(meta_handle.get_sstable_meta().get_sstable_seq()))) {
      LOG_WARN("set sstable logical seq failed", K(ret), "sst_meta", meta_handle.get_sstable_meta());
    } else if (OB_FAIL(macro_start_seq.set_parallel_degree(task_id_))) {
      LOG_WARN("set parallel degree failed", K(ret));
    }
#ifdef OB_BUILD_SHARED_STORAGE
  }
#endif

  if (OB_SUCC(ret)) {
    macro_seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
    macro_seq_param.start_ = macro_start_seq.macro_data_seq_;
  }
  return ret;
}

int ObTabletSplitWriteTask::prepare_macro_block_writer(
    const ObStorageSchema &clipped_storage_schema,
    ObIArray<ObWholeDataStoreDesc> &data_desc_arr,
    ObIArray<ObMacroBlockWriter *> &macro_block_writer_arr)
{
  int ret = OB_SUCCESS;
  ObMacroSeqParam macro_seq_param;
  ObPreWarmerParam pre_warm_param;
  ObWholeDataStoreDesc data_desc;
  ObTabletHandle tablet_handle;
  const bool micro_index_clustered = context_->tablet_handle_.get_obj()->get_tablet_meta().micro_index_clustered_;
  const share::SCN split_reorganization_scn = context_->split_scn_.is_valid() ? context_->split_scn_ : SCN::min_scn();
  for (int64_t i = 0; OB_SUCC(ret) && i < param_->dest_tablets_id_.count(); i++) {
    macro_seq_param.reset();
    pre_warm_param.reset();
    data_desc.reset();
    tablet_handle.reset();
    void *buf = nullptr;
    ObMacroBlockWriter *macro_block_writer = nullptr;
    ObSSTableIndexBuilder *sst_idx_builder = nullptr;
    ObSSTablePrivateObjectCleaner *object_cleaner = nullptr;
    const ObTabletID &dst_tablet_id = param_->dest_tablets_id_.at(i);
    ObSplitSSTableTaskKey task_key(sstable_->get_key(), dst_tablet_id);
    const ObMergeType merge_type = sstable_->is_major_sstable() ? MAJOR_MERGE : MINOR_MERGE;
    const int64_t snapshot_version = sstable_->is_major_sstable() ?
        sstable_->get_snapshot_version() : sstable_->get_end_scn().get_val_for_tx();
    compaction::ObExecMode exec_mode = GCTX.is_shared_storage_mode() ?
        ObExecMode::EXEC_MODE_OUTPUT : ObExecMode::EXEC_MODE_LOCAL;
    int32_t private_transfer_epoch = -1;
    if (OB_FAIL(prepare_macro_seq_param(i/*dest_tablet_index*/, macro_seq_param))) {
      LOG_WARN("prepare macro seq par failed", K(ret), K(macro_seq_param));
    } else if (OB_FAIL(context_->index_builder_map_.get_refactored(task_key, sst_idx_builder))) {
      LOG_WARN("get refactored failed", K(ret));
    } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(context_->ls_handle_, dst_tablet_id, tablet_handle))) {
      LOG_WARN("get tablet failed", K(ret));
    } else if (OB_FAIL(tablet_handle.get_obj()->get_private_transfer_epoch(private_transfer_epoch))) {
      LOG_WARN("failed to get private transfer epoch", K(ret), "tablet_meta", tablet_handle.get_obj()->get_tablet_meta());
    } else if (OB_FAIL(data_desc.init(true/*is_ddl*/, clipped_storage_schema,
                                      param_->ls_id_,
                                      dst_tablet_id,
                                      merge_type,
                                      snapshot_version,
                                      param_->data_format_version_,
                                      micro_index_clustered,
                                      private_transfer_epoch,
                                      0/*concurrent cnt*/,
                                      split_reorganization_scn,
                                      sstable_->get_end_scn(),
                                      nullptr/*cg_schema*/,
                                      0/*table_cg_idx*/,
                                      exec_mode))) {
      LOG_WARN("fail to init data store desc", K(ret), K(dst_tablet_id), KPC(param_));
    } else if (FALSE_IT(data_desc.get_desc().sstable_index_builder_ = sst_idx_builder)) {
    } else if (OB_FAIL(data_desc_arr.push_back(data_desc))) {  // copy_and_assign.
      LOG_WARN("push back data store desc failed", K(ret));
    } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObMacroBlockWriter)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc mem failed", K(ret));
    } else if (FALSE_IT(macro_block_writer = new (buf) ObMacroBlockWriter(true/*is_need_macro_buffer*/))) {
    } else if (OB_FAIL(pre_warm_param.init(param_->ls_id_, dst_tablet_id))) {
      LOG_WARN("failed to init pre warm param", K(ret), K(dst_tablet_id), KPC(param_));
    } else if (OB_FAIL(ObSSTablePrivateObjectCleaner::get_cleaner_from_data_store_desc(
                                data_desc.get_desc(),
                                object_cleaner))) {
      LOG_WARN("failed to get cleaner from data store desc", K(ret));
    } else if (OB_FAIL(macro_block_writer->open(data_desc_arr.at(i).get_desc(), task_id_/*parallel_idx*/,
          macro_seq_param, pre_warm_param, *object_cleaner))) {
      LOG_WARN("open macro_block_writer failed", K(ret), K(data_desc));
    } else if (OB_FAIL(macro_block_writer_arr.push_back(macro_block_writer))) {
      LOG_WARN("push back failed", K(ret));
    }
    if (OB_FAIL(ret)) {
      // allocated memory in array will be freed by the caller.
      if (nullptr != macro_block_writer) {
        macro_block_writer->~ObMacroBlockWriter();
        macro_block_writer = nullptr;
      }
      if (nullptr != buf) {
        allocator_.free(buf);
        buf = nullptr;
      }
    }
  }
  return ret;
}

// TODO yiren, remove it when mds prepared.
int ObTabletSplitWriteTask::prepare_sorted_high_bound_pair(
    common::ObSArray<TabletBoundPair> &tablet_bound_arr)
{
  int ret = OB_SUCCESS;
  tablet_bound_arr.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!param_->can_reuse_macro_block_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KPC(param_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < param_->dest_tablets_id_.count(); i++) {
      ObDatumRowkey high_bound;
      const common::ObTabletID &tablet_id = param_->dest_tablets_id_.at(i);
      ObTabletHandle tablet_handle;
      ObTabletSplitMdsUserData data;
      ObDatumRowkey data_tablet_end_partkey;
      if (OB_FAIL(ObDDLUtil::ddl_get_tablet(context_->ls_handle_, tablet_id, tablet_handle, ObMDSGetTabletMode::READ_ALL_COMMITED))) {
        LOG_WARN("failed to get tablet", K(ret), K(tablet_id));
      } else if (OB_FAIL(tablet_handle.get_obj()->ObITabletMdsInterface::get_split_data(data, ObTabletCommon::DEFAULT_GET_TABLET_DURATION_10_S))) {
        LOG_WARN("failed to get split data", K(ret));
      } else if (OB_FAIL(data.get_end_partkey(data_tablet_end_partkey))) {
        LOG_WARN("failed to get end partkey", K(ret), K(tablet_handle.get_obj()->get_tablet_meta()));
      } else if (OB_FAIL(data_tablet_end_partkey.deep_copy(high_bound, allocator_))) {
        LOG_WARN("failed to deep copy", K(ret));
      } else if (OB_FAIL(tablet_bound_arr.push_back(std::make_pair(tablet_id, high_bound)))) {
        LOG_WARN("push back failed", K(ret), K(tablet_id), K(high_bound));
      }
    }
  }
  if (OB_SUCC(ret)) {
    // check in ASC rowkey order.
    for (int64_t i = 1; OB_SUCC(ret) && i < tablet_bound_arr.count(); i++) {
      int cmp_ret = 0;
      ObDatumRowkey &prev_bound = tablet_bound_arr.at(i-1).second;
      if (OB_FAIL(tablet_bound_arr.at(i).second.compare(prev_bound, rowkey_read_info_->get_datum_utils(), cmp_ret))) {
        LOG_WARN("failed to compare", K(ret));
      } else if (OB_UNLIKELY(cmp_ret < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet bound arr not in asc order", K(ret));
      }
    }
  }
  return ret;
}

int ObTabletSplitWriteTask::process_reuse_macro_block_task(
    const ObIArray<ObMacroBlockWriter *> &macro_block_writer_arr,
    const ObStorageSchema &clipped_storage_schema)
{
  int ret = OB_SUCCESS;
  ObSArray<TabletBoundPair> tablet_bound_arr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(macro_block_writer_arr.count() != param_->dest_tablets_id_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), "writer count", macro_block_writer_arr.count(), KPC(param_));
  } else if (OB_FAIL(prepare_sorted_high_bound_pair(tablet_bound_arr))) {
    LOG_WARN("get split dest tablets info failed", K(ret));
  } else {
    // iterate all macro blocks in sstable.
    SMART_VAR(ObDualMacroMetaIterator, meta_iter) {
      int64_t dest_tablet_index = 0;
      ObDatumRange whole_range;
      ObDatumRange tmp_range;
      whole_range.set_whole_range();
      const bool is_small_sstable = sstable_->is_small_sstable();
      if (OB_FAIL(meta_iter.open(
        *sstable_, whole_range, *rowkey_read_info_, allocator_))) {
        LOG_WARN("open dual macro meta iter failed", K(ret), K(*sstable_));
      } else {
        ObSEArray<blocksstable::MacroBlockId, DEFAULT_MACRO_BLOCK_CNT> split_point_macros;
        while (OB_SUCC(ret)) {
          ObDataMacroBlockMeta macro_meta;
          ObMacroBlockDesc data_macro_desc;
          data_macro_desc.reset();
          tmp_range.reset();
          data_macro_desc.macro_meta_ = &macro_meta;
          int cmp_ret = 0;
          ObDatumRowkey macro_end_key;
          if (OB_FAIL(meta_iter.get_next_macro_block(data_macro_desc))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("get data macro meta failed", K(ret));
            } else {
              ret = OB_SUCCESS;
              break;
            }
          } else if (!is_small_sstable) {
            const ObDatumRowkey &high_bound = tablet_bound_arr.at(dest_tablet_index).second;
            if (OB_FAIL(data_macro_desc.macro_meta_->get_rowkey(macro_end_key))) {
              LOG_WARN("get macro block end key failed", K(ret), K(data_macro_desc));
            } else if (OB_FAIL(macro_end_key.compare(high_bound, rowkey_read_info_->get_datum_utils(), cmp_ret))) {
              LOG_WARN("compare failed", K(ret), K(macro_end_key), K(high_bound));
            }
          }

          if (FAILEDx(!is_small_sstable && cmp_ret < 0)) {
            // reuse whole macro block.
            const ObMicroBlockData *micro_block_data = nullptr;
            if (data_macro_desc.is_clustered_index_tree_ && OB_FAIL(meta_iter.get_current_clustered_index_info(micro_block_data))) {
              LOG_WARN("get micro data failed", K(ret));
            } else if (OB_FAIL(macro_block_writer_arr.at(dest_tablet_index)->append_macro_block(data_macro_desc, micro_block_data))) {
              LOG_WARN("append macro row failed", K(ret));
            } else {
              (void) ATOMIC_AAFx(&context_->row_inserted_, data_macro_desc.row_count_, 0/*unused id*/);
              LOG_INFO("process current macro block finish", K(ret), K(dest_tablet_index), K(data_macro_desc),
                      K(tablet_bound_arr));
            }
          } else if (OB_FAIL(process_rows_for_reuse_task(clipped_storage_schema,
              tablet_bound_arr, macro_block_writer_arr, data_macro_desc, dest_tablet_index))) {
            LOG_WARN("process rows for rewrite macro block failed", K(ret), K(data_macro_desc));
          } else if (OB_FAIL(split_point_macros.push_back(data_macro_desc.macro_block_id_))) {
            LOG_WARN("push back failed", K(ret));
          }
        } // end while.

        if (OB_SUCC(ret) && !split_point_macros.empty()) {
          if (OB_FAIL(context_->append_split_point_macros(sstable_->is_major_sstable(), split_point_macros))) {
            LOG_WARN("append split point macros failed", K(ret));
          } else {
            FLOG_INFO("DEBUG CODE, CHANGE TO TRACE LATER, a split point macro in reuse split scenario",
                  "end_scn", sstable_->get_end_scn(),
                  K(is_small_sstable),
                  K(split_point_macros));
          }
        }
      }
    }
  }
  return ret;
}

// rewrite macro block task like split local index.
int ObTabletSplitWriteTask::process_rewrite_macro_block_task(
    const ObIArray<ObMacroBlockWriter *> &macro_block_writer_arr,
    const ObStorageSchema &clipped_storage_schema)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (OB_UNLIKELY(task_id_ >= context_->data_split_ranges_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected err", K(ret), K(task_id_), KPC(param_));
    } else if (OB_FAIL(process_rows_for_rewrite_task(clipped_storage_schema, macro_block_writer_arr, context_->data_split_ranges_.at(task_id_)))) {
      LOG_WARN("process each row of rewrite task failed", K(ret), K(task_id_), "query_range", context_->data_split_ranges_.at(task_id_), KPC(context_));
    }
  }
  return ret;
}

int ObTabletSplitWriteTask::process_rows_for_reuse_task(
    const ObStorageSchema &clipped_storage_schema,
    const ObIArray<TabletBoundPair> &tablet_bound_arr,
    const ObIArray<ObMacroBlockWriter *> &macro_block_writer_arr,
    const ObMacroBlockDesc &data_macro_desc,
    int64_t &dest_tablet_index)
{
  int ret = OB_SUCCESS;
  ObRowScan row_scan_iter;
  ObDataStoreDesc data_desc;
  int64_t rewrite_row_cnt = 0;
  ObDatumRange whole_range;
  whole_range.set_whole_range();
  ObSplitScanParam row_scan_param(param_->table_id_, *(context_->tablet_handle_.get_obj()), whole_range, clipped_storage_schema);
  if (OB_FAIL(row_scan_iter.init(row_scan_param, data_macro_desc, *sstable_))) {
    LOG_WARN("init row scan iterator failed", K(ret), K(data_macro_desc));
  } else {
    while (OB_SUCC(ret)) { // exit when iter row end.
      ObDatumRowkey cur_row_key;
      const ObDatumRow *cur_row = nullptr;
      if (OB_FAIL(row_scan_iter.get_next_row(cur_row))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next row failed", K(ret), K(dest_tablet_index), K(data_macro_desc), KPC(sstable_));
        } else {
          ret = OB_SUCCESS;
          LOG_INFO("process the rewrite macro block finished", K(ret), K(rewrite_row_cnt));
          break;
        }
      } else if (OB_FAIL(cur_row_key.assign(cur_row->storage_datums_, cur_row->get_column_count()))) {
        LOG_WARN("construct datum rowkey failed", K(ret));
      } else {
        int cmp_ret = 0;
        bool is_row_append = false;
        while (OB_SUCC(ret) && !is_row_append && dest_tablet_index < tablet_bound_arr.count()) {
          const ObDatumRowkey &high_bound = tablet_bound_arr.at(dest_tablet_index).second;
          if (OB_FAIL(cur_row_key.compare(high_bound, rowkey_read_info_->get_datum_utils(), cmp_ret))) {
            LOG_WARN("compare failed", K(ret), K(cur_row_key), K(high_bound));
          } else if (cmp_ret < 0) {
            // find the dest tablet the row belongs to.
            if (OB_FAIL(fill_tail_column_datums(*cur_row))) {
              LOG_WARN("fill tail column datums failed", K(ret));
            } else if (OB_FAIL(macro_block_writer_arr.at(dest_tablet_index)->append_row(write_row_))) {
              LOG_WARN("append row failed", K(ret), KPC(cur_row), K(write_row_), K(data_desc));
            }

            if (OB_SUCC(ret)) {
              if (++rewrite_row_cnt % 100 == 0) {
                (void) ATOMIC_AAFx(&context_->row_inserted_, 100, 0/*unused id*/);
              }
              is_row_append = true;
              LOG_TRACE("append row successfully", "tablet_id", tablet_bound_arr.at(dest_tablet_index).first,
                  "row_count", rewrite_row_cnt, "row_inserted_", context_->row_inserted_,
                  KPC(cur_row), K(write_row_), K(default_row_));
            }
          } else {
            // switch to next dest tablet.
            LOG_INFO("prepare to switch to next tablet", K(ret), K(dest_tablet_index),
              "tablet_id", tablet_bound_arr.at(dest_tablet_index).first, K(rewrite_row_cnt));
            dest_tablet_index++;
            if (rewrite_row_cnt != 0) {
              (void) ATOMIC_AAFx(&context_->row_inserted_, rewrite_row_cnt % 100, 0/*unused id*/);
              rewrite_row_cnt = 0;
            }
          }
        }
        if (rewrite_row_cnt != 0) {
          (void) ATOMIC_AAFx(&context_->row_inserted_, rewrite_row_cnt % 100, 0/*unused id*/);
          rewrite_row_cnt = 0;
        }
        if (!is_row_append) {
          ret = OB_SUCC(ret) ? OB_ERR_UNEXPECTED : ret;
          LOG_WARN("row not append", K(ret), KPC(cur_row));
        }
      }
    }
  }
  return ret;
}

int ObTabletSplitWriteTask::process_rows_for_rewrite_task(
    const ObStorageSchema &clipped_storage_schema,
    const ObIArray<ObMacroBlockWriter *> &macro_block_writer_arr,
    const ObDatumRange &query_range)
{
  int ret = OB_SUCCESS;
  const ObIArray<common::ObTabletID> &dest_tablets_id = param_->dest_tablets_id_;
  const ObITableReadInfo &rowkey_read_info = context_->tablet_handle_.get_obj()->get_rowkey_read_info();
  const int64_t schema_rowkey_cnt = context_->tablet_handle_.get_obj()->get_rowkey_read_info().get_schema_rowkey_count();
  ObTabletSplitMdsUserData src_split_data;
  ObSEArray<ObTabletSplitMdsUserData, 2> dst_split_datas;
  if (OB_UNLIKELY(dest_tablets_id.count() != macro_block_writer_arr.count() || !query_range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(dest_tablets_id), K(macro_block_writer_arr), K(query_range));
  } else if (OB_FAIL(ObTabletSplitMdsHelper::prepare_calc_split_dst(
          *context_->ls_handle_.get_ls(),
          *context_->tablet_handle_.get_obj(),
          ObTimeUtility::current_time() + (1 + dest_tablets_id.count()) * ObTabletCommon::DEFAULT_GET_TABLET_DURATION_10_S,
          src_split_data,
          dst_split_datas))) {
    LOG_WARN("failed to prepare calc split dst", K(ret), K(dest_tablets_id));
  } else {
    // rewrite each row.
    ObRowScan row_scan_iter;
    ObSplitScanParam row_scan_param(param_->table_id_, *(context_->tablet_handle_.get_obj()), query_range, clipped_storage_schema);
    if (OB_FAIL(row_scan_iter.init(row_scan_param, *sstable_))) {
      LOG_WARN("init row scan iterator failed", K(ret));
    } else {
      ObArenaAllocator new_row_allocator;
      int64_t tmp_row_inserted = 0;
      while (OB_SUCC(ret)) { // exit when iter row end.
        new_row_allocator.reuse();
        const ObDatumRow *datum_row = nullptr;
        ObDatumRowkey rowkey;
        ObTabletID tablet_id;
        int64_t dst_idx = 0;
        if (OB_FAIL(row_scan_iter.get_next_row(datum_row))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("get next row failed", K(ret));
          } else {
            ret = OB_SUCCESS;
            break;
          }
        } else if (OB_FAIL(rowkey.assign(datum_row->storage_datums_, schema_rowkey_cnt))) {
          LOG_WARN("Failed to assign rowkey", K(ret), K(schema_rowkey_cnt));
        } else if (OB_FAIL(src_split_data.calc_split_dst(rowkey_read_info, dst_split_datas, rowkey, tablet_id, dst_idx))) {
          LOG_WARN("failed to calc split dst tablet", K(ret));
        } else {
          bool is_row_append = false;
          for (int64_t i = 0; OB_SUCC(ret) && !is_row_append && i < dest_tablets_id.count(); i++) {
            if (dest_tablets_id.at(i) == tablet_id) {
              if (OB_FAIL(fill_tail_column_datums(*datum_row))) {
                LOG_WARN("fill tail column datums failed", K(ret));
              } else if (OB_FAIL(macro_block_writer_arr.at(i)->append_row(write_row_))) {
                LOG_WARN("append row failed", K(ret), KPC(datum_row), K(write_row_));
              }

              if (OB_SUCC(ret)) {
                is_row_append = true;
              }
            }
          }
          if (!is_row_append) {
            // defensive code.
            ret = OB_SUCC(ret) ? OB_ERR_UNEXPECTED : ret;
            LOG_WARN("append row failed", K(ret), K(tablet_id), K(write_row_), KPC(datum_row));
          } else if (++tmp_row_inserted % 100 == 0) {
            (void) ATOMIC_AAFx(&context_->row_inserted_, 100, 0/*placeholder*/);
          }
        }
      }
      (void) ATOMIC_AAFx(&context_->row_inserted_, tmp_row_inserted % 100, 0/*unused id*/);
    }
  }
  return ret;
}

int ObTabletSplitWriteTask::fill_tail_column_datums(const blocksstable::ObDatumRow &scan_row)
{
  int ret = OB_SUCCESS;
  write_row_.reuse();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!scan_row.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(scan_row));
  } else if (OB_UNLIKELY(scan_row.get_column_count() > default_row_.get_column_count()
                      || default_row_.get_column_count() != write_row_.get_column_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(scan_row), K(default_row_), K(write_row_));
  } else if (OB_FAIL(write_row_.copy_attributes_except_datums(scan_row))) {
    LOG_WARN("copy attribute except storage datums failed", K(ret), K(scan_row));
  } else {
    write_row_.count_ = default_row_.get_column_count();
    for (int64_t i = 0; OB_SUCC(ret) && i < default_row_.get_column_count(); i++) {
      if (i < scan_row.get_column_count()) {
        // scan columns number exceeds columns number of the macro block, the scanner
        // will return default NOP datums.
        const ObStorageDatum &scan_datum = scan_row.storage_datums_[i];
        if (sstable_->is_major_sstable() && scan_datum.is_nop()) {
          write_row_.storage_datums_[i] = default_row_.storage_datums_[i];
        } else {
          write_row_.storage_datums_[i] = scan_datum;
        }
      } else if (sstable_->is_major_sstable()) {
        write_row_.storage_datums_[i] = default_row_.storage_datums_[i];
      } else {
        write_row_.storage_datums_[i].set_nop();
      }
    }
  }
  return ret;
}

int ObTabletSplitMergeTask::init(
    ObTabletSplitParam &param, ObTabletSplitCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid() || !ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(param), K(ctx));
  } else {
    param_ = &param;
    context_ = &ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObTabletSplitMergeTask::process()
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(BEFORE_TABLET_SPLIT_MERGE_TASK);
  bool is_data_split_finished = false;
  ObIDag *tmp_dag = get_dag();
  ObTabletSplitDag *dag = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(tmp_dag) || ObDagType::DAG_TYPE_TABLET_SPLIT != tmp_dag->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), KP(tmp_dag));
  } else if (OB_ISNULL(dag = static_cast<ObTabletSplitDag *>(tmp_dag))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), KP(tmp_dag), KP(dag));
  } else if (OB_SUCCESS != (context_->complement_data_ret_)) {
    LOG_WARN("complement data has already failed", KPC(context_));
  } else if (OB_FAIL(ObTabletSplitUtil::check_dest_data_completed(
      context_->ls_handle_,
      param_->dest_tablets_id_,
      GCTX.is_shared_storage_mode()/*check_remote*/,
      is_data_split_finished))) {
    LOG_WARN("check all major exist failed", K(ret));
  } else if (is_data_split_finished) {
    LOG_INFO("split task has alreay finished", KPC(param_));
  } else if (OB_SUCCESS != (context_->complement_data_ret_)) {
    LOG_WARN("complement data has already failed", "ret", context_->complement_data_ret_);
  } else if (share::ObSplitSSTableType::SPLIT_BOTH == param_->split_sstable_type_) {
    if (OB_FAIL(create_sstable(share::ObSplitSSTableType::SPLIT_MINOR))) {
      LOG_WARN("create sstable failed", K(ret));
    } else {
      DEBUG_SYNC(BEFORE_TABLET_SPLIT_MAJOR_SSTABLE);
      if (OB_FAIL(create_sstable(share::ObSplitSSTableType::SPLIT_MAJOR))) {
        LOG_WARN("create sstable failed", K(ret));
      }
    }
  } else if (OB_FAIL(create_sstable(param_->split_sstable_type_))) {
    LOG_WARN("create sstable failed", K(ret));
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(context_)) {
    context_->complement_data_ret_ = ret;
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObTabletSplitMergeTask::create_sstable(
    const share::ObSplitSSTableType &split_sstable_type)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObITable *, MAX_SSTABLE_CNT_IN_STORAGE> participants;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(share::ObSplitSSTableType::SPLIT_MAJOR != split_sstable_type
      && share::ObSplitSSTableType::SPLIT_MINOR != split_sstable_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(split_sstable_type), KPC(param_));
  } else if (OB_FAIL(ObTabletSplitUtil::get_participants(
      split_sstable_type, context_->table_store_iterator_, false/*is_table_restore*/,
      context_->skipped_split_major_keys_, participants))) {
    LOG_WARN("get participants failed", K(ret));
  } else {
    ObTableHandleV2 table_handle;
    ObTablesHandleArray batch_sstables_handle;
    ObSEArray<ObTabletID, 1> check_major_exist_tablets;
    ObSplitSSTableTaskKey split_sstable_key;
    const int64_t multi_version_start = context_->tablet_handle_.get_obj()->get_multi_version_start();
    const compaction::ObMergeType merge_type = share::ObSplitSSTableType::SPLIT_MINOR == split_sstable_type ?
        compaction::ObMergeType::MINOR_MERGE : compaction::ObMergeType::MAJOR_MERGE;
    const int64_t src_table_cnt = participants.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < param_->dest_tablets_id_.count(); i++) {
      bool is_data_split_finished = false;
      const int64_t dest_tablet_index = i;
      const ObTabletID &dest_tablet_id = param_->dest_tablets_id_.at(i);
      batch_sstables_handle.reset();
      check_major_exist_tablets.reset();
      if (OB_FAIL(check_major_exist_tablets.push_back(dest_tablet_id))) {
        LOG_WARN("push back failed", K(ret));
      } else if (OB_FAIL(ObTabletSplitUtil::check_dest_data_completed(
          context_->ls_handle_,
          check_major_exist_tablets,
          GCTX.is_shared_storage_mode()/*check_remote*/,
          is_data_split_finished))) {
        LOG_WARN("check all major exist failed", K(ret));
      } else if (is_data_split_finished) {
        FLOG_INFO("skip to create sstable", K(ret), K(dest_tablet_id));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < src_table_cnt; j++) { // keep destination sstable commit versions incremental.
          const ObSSTable *src_sstable = static_cast<ObSSTable *>(participants.at(j));
          if (OB_ISNULL(src_sstable)) {
            ret = OB_ERR_SYS;
            LOG_WARN("Error sys", K(ret), K(dest_tablet_id), K(participants));
          } else {
            split_sstable_key.reset();
            split_sstable_key.dest_tablet_id_ = dest_tablet_id;
            split_sstable_key.src_sst_key_ = src_sstable->get_key();
            ObSSTableIndexBuilder *index_builder = nullptr;
            table_handle.reset();
            // record the split point macros into the last dest tablets' sstable with the smallest scn.
            const bool need_record_split_point_macros = GCTX.is_shared_storage_mode()
                && (param_->dest_tablets_id_.count() - 1 == i && j == 0);
            const ObIArray<MacroBlockId> &reuse_split_point_macros =
              !need_record_split_point_macros ? ObArray<MacroBlockId>() :
              is_major_merge_type(merge_type) ? context_->split_point_major_macros_ : context_->split_point_minor_macros_;
            HEAP_VAR(ObTabletCreateSSTableParam, create_sstable_param) {
              if (OB_FAIL(context_->index_builder_map_.get_refactored(split_sstable_key, index_builder))) {
                LOG_WARN("get refactored failed", K(ret), K(split_sstable_key));
              } else if (OB_FAIL(build_create_sstable_param(
                *src_sstable, dest_tablet_index, index_builder, reuse_split_point_macros, create_sstable_param))) {
                LOG_WARN("build create sstable param failed", K(ret));
              } else if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable(create_sstable_param, allocator_, table_handle))) {
                LOG_WARN("create sstable failed", K(ret), K(create_sstable_param));
              } else if (OB_FAIL(batch_sstables_handle.add_table(table_handle))) {
                LOG_WARN("add table failed", K(ret));
              }
            }
          }
          // fill empty minor sstable if scn not continous
          if (OB_SUCC(ret) && j == src_table_cnt - 1 && !is_major_merge_type(merge_type)) {
            bool need_fill_empty_sstable = false;
            SCN end_scn;
            if (OB_FAIL(check_need_fill_empty_sstable(context_->ls_handle_, src_sstable->is_minor_sstable(), src_sstable->get_key(), dest_tablet_id, need_fill_empty_sstable, end_scn))) {
              LOG_WARN("failed to check need fill", K(ret));
            } else if (need_fill_empty_sstable) {
              table_handle.reset();
              ObSSTableMetaHandle meta_handle;
              HEAP_VAR(ObTabletCreateSSTableParam, create_sstable_param) {
                if (OB_FAIL(src_sstable->get_meta(meta_handle))) {
                  LOG_WARN("get meta failed", K(ret));
                } else if (OB_FAIL(build_create_empty_sstable_param(meta_handle.get_sstable_meta().get_basic_meta(), src_sstable->get_key(), dest_tablet_id, end_scn, create_sstable_param))) {
                  LOG_WARN("failed to build create empty sstable param", K(ret));
                } else if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable(create_sstable_param, allocator_, table_handle))) {
                  LOG_WARN("create sstable failed", K(ret), K(create_sstable_param));
                } else if (OB_FAIL(batch_sstables_handle.add_table(table_handle))) {
                  LOG_WARN("add table failed", K(ret));
                }
              }
            }
          }
        }
        if (OB_SUCC(ret) && (src_table_cnt > 0 || is_major_merge_type(merge_type))) {
          // empty major result should also need to swap tablet, to update data_split_status and restore status.
          int64_t op_id = -1;
        #ifdef OB_BUILD_SHARED_STORAGE
          if (GCTX.is_shared_storage_mode() && is_minor_merge(merge_type)) {
            if (OB_FAIL(context_->ss_split_helper_.get_op_id(dest_tablet_index, op_id))) {
              LOG_WARN("get op id failed", K(ret));
            }
          }
        #endif
          if (FAILEDx(ObTabletSplitMergeTask::update_table_store_with_batch_tables(
                context_->ls_rebuild_seq_,
                context_->ls_handle_,
                context_->tablet_handle_,
                dest_tablet_id,
                batch_sstables_handle,
                merge_type,
                context_->skipped_split_major_keys_,
                is_major_merge_type(merge_type) ? context_->max_major_snapshot_ : op_id,
                context_->reorg_scn_))) {
            LOG_WARN("update table store with batch tables failed", K(ret), K(batch_sstables_handle), K(split_sstable_type));
          }
        #ifdef OB_BUILD_SHARED_STORAGE
          int tmp_ret = OB_SUCCESS;
          if (!is_major_merge_type(merge_type)
                && GCTX.is_shared_storage_mode()
                && OB_TMP_FAIL(context_->ss_split_helper_.finish_add_op(
              dest_tablet_index/*dest_tablet_index*/,
              OB_SUCC(ret)/*need_finish*/))) {
            LOG_WARN("finish add op failed", K(ret), K(tmp_ret));
          }
        #endif
        }
        if (OB_SUCC(ret) && !is_major_merge_type(merge_type)) {
          // build lost mds sstable after minor merge.
          if (OB_FAIL(check_and_create_mds_sstable(dest_tablet_id))) {
            LOG_WARN("check and create mds sstable failed", K(ret), K(dest_tablet_id));
          }
        }
      }
    }
  }
  return ret;
}

int ObTabletSplitMergeTask::check_and_create_mds_sstable(
    const ObTabletID &dest_tablet_id)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator build_mds_arena("SplitBuildMds",
      OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObTableHandleV2 table_handle;
  ObTablesHandleArray batch_sstables_handle;
  int64_t op_id = -1;
#ifdef OB_BUILD_SHARED_STORAGE
  ObSSDataSplitHelper ss_mds_split_helper;
#endif
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!dest_tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(dest_tablet_id));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (GCTX.is_shared_storage_mode() && OB_FAIL(ss_mds_split_helper.start_add_mds_op(
      param_->ls_id_,
      context_->split_scn_,
      1/*parallel_cnt_of_each_sstable*/,
      1/*sstables_cnt*/,
      dest_tablet_id))) {
    LOG_WARN("start op failed", K(ret));
  } else if (GCTX.is_shared_storage_mode()
      && OB_FAIL(ss_mds_split_helper.get_op_id(0/*dest_tablet_index*/, op_id))) {
    LOG_WARN("get op id failed", K(ret));
#endif
  } else if (OB_FAIL(ObTabletSplitUtil::build_mds_sstable(
        build_mds_arena,
        context_->ls_handle_,
        context_->tablet_handle_,
        dest_tablet_id,
        context_->split_scn_,
      #ifdef OB_BUILD_SHARED_STORAGE
        ss_mds_split_helper,
      #endif
        table_handle))) {
    LOG_WARN("build lost medium mds sstable failed", K(ret), KPC(param_));
  } else if (OB_UNLIKELY(!table_handle.is_valid())) {
    LOG_INFO("no need to fill medium mds sstable", K(ret),
      "src_tablet_id", param_->source_tablet_id_, K(dest_tablet_id));
  } else if (OB_FAIL(batch_sstables_handle.add_table(table_handle))) {
    LOG_WARN("add table failed", K(ret));
  }

  if (OB_SUCC(ret) && !batch_sstables_handle.empty()) {
    if (OB_FAIL(ObTabletSplitMergeTask::update_table_store_with_batch_tables(
          context_->ls_rebuild_seq_,
          context_->ls_handle_,
          context_->tablet_handle_,
          dest_tablet_id,
          batch_sstables_handle,
          compaction::ObMergeType::MDS_MINI_MERGE,
          context_->skipped_split_major_keys_,
          op_id,
          context_->reorg_scn_))) {
      LOG_WARN("update table store with batch tables failed", K(ret), K(batch_sstables_handle));
    }
  }

#ifdef OB_BUILD_SHARED_STORAGE
  int tmp_ret = OB_SUCCESS;
  if (GCTX.is_shared_storage_mode() && OB_TMP_FAIL(ss_mds_split_helper.finish_add_op(
      0/*dest_tablet_index*/,
      OB_SUCC(ret)/*need_finish*/))) {
    LOG_WARN("finish add op failed", K(ret), K(tmp_ret));
  }
#endif
  table_handle.reset();
  batch_sstables_handle.reset();
  build_mds_arena.reset();
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObTabletSplitMergeTask::close_ss_index_builder(
    const ObSSTable &src_table, // source table.
    const int64_t dest_tablet_index, // index at param.dest_tablets_id.
    ObSSTableIndexBuilder *index_builder,
    ObSSTableMergeRes &res)
{
  int ret = OB_SUCCESS;
  ObTabletID dst_tablet_id;
  int64_t sstable_index = -1;
  int64_t index_tree_start_seq = 0;
  int64_t new_root_start_seq = 0;
  share::ObPreWarmerParam pre_warm_param;
  const int64_t parallel_cnt = context_->parallel_cnt_of_each_sstable_;
  const ObSSDataSplitHelper &ss_split_helper = context_->ss_split_helper_;
  if (OB_UNLIKELY(dest_tablet_index >= param_->dest_tablets_id_.count() || nullptr == index_builder)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(dest_tablet_index), "dest_tablets", param_->dest_tablets_id_,
        KP(index_builder));
  } else if (OB_FALSE_IT(dst_tablet_id = param_->dest_tablets_id_.at(dest_tablet_index))) {
  } else if (OB_FAIL(pre_warm_param.init(param_->ls_id_, dst_tablet_id))) {
    LOG_WARN("failed to init pre warm param", K(ret));
  } else if (OB_FAIL(context_->get_index_in_source_sstables(src_table, sstable_index))) {
    LOG_WARN("get index in source sstables failed", K(ret));
  } else if (src_table.is_major_sstable()) {
    const int64_t total_majors_cnt = context_->split_majors_count_;
    if (OB_FAIL(ss_split_helper.generate_major_macro_seq_info(
        sstable_index/*the index in the generated majors*/,
        parallel_cnt/*the parallel cnt in one sstable*/,
        parallel_cnt/*the parallel idx in one sstable*/,
        index_tree_start_seq))) {
      LOG_WARN("get macro seq failed", K(ret));
    } else if (OB_FAIL(index_builder->close_with_macro_seq(
                  res, index_tree_start_seq/*start seq of the cluster n-1/n-2 IndexTree*/,
                  OB_DEFAULT_MACRO_BLOCK_SIZE /*nested_size*/,
                  0 /*nested_offset*/, pre_warm_param))) {
      LOG_WARN("close with seq failed", K(ret), K(index_tree_start_seq));
    } else if (OB_FAIL(ss_split_helper.get_major_macro_seq_by_stage(
        ObGetMacroSeqStage::GET_NEW_ROOT_MACRO_SEQ,
        total_majors_cnt,
        parallel_cnt,
        new_root_start_seq))) {
      LOG_WARN("get new root macro seq failed", K(ret), K(total_majors_cnt), K(parallel_cnt));
    } else {
      res.root_macro_seq_ = new_root_start_seq;
    }
  } else {
    if (OB_FAIL(ss_split_helper.generate_minor_macro_seq_info(
          dest_tablet_index/*index in dest_tables_id*/,
          sstable_index/*the index in the generated minors*/,
          parallel_cnt/*the parallel cnt in one sstable*/,
          parallel_cnt/*the parallel idx in one sstable*/,
          index_tree_start_seq))) {
      LOG_WARN("get macro seq failed", K(ret));
    } else if (OB_FAIL(index_builder->close_with_macro_seq(
                  res, index_tree_start_seq/*start seq of the cluster n-1/n-2 IndexTree*/,
                  OB_DEFAULT_MACRO_BLOCK_SIZE /*nested_size*/,
                  0 /*nested_offset*/, pre_warm_param))) {
      LOG_WARN("close with seq failed", K(ret), K(index_tree_start_seq));
    }
  }
  FLOG_INFO("DEBUG CODE, CHANGE TO TRACE LATER", K(ret),
          K(dest_tablet_index),
          K(sstable_index),
          K(parallel_cnt),
          "end_scn", src_table.get_end_scn(),
          K(index_tree_start_seq),
          K(new_root_start_seq));
  return ret;
}
#endif

int ObTabletSplitMergeTask::build_create_sstable_param(
    const ObSSTable &src_table, // source table.
    const int64_t dest_tablet_index, // index at param.dest_tablets_id_
    ObSSTableIndexBuilder *index_builder,
    const ObIArray<MacroBlockId> &split_point_macros_id,
    ObTabletCreateSSTableParam &create_sstable_param)
{
  int ret = OB_SUCCESS;
  ObSSTableMetaHandle meta_handle;
  meta_handle.reset();
  ObSSTableMergeRes res;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == index_builder || !src_table.is_valid()
      || dest_tablet_index < 0
      || dest_tablet_index >= param_->dest_tablets_id_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), KP(index_builder), K(src_table), K(dest_tablet_index),
        "dest_tablets", param_->dest_tablets_id_);
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (GCTX.is_shared_storage_mode() &&
      OB_FAIL(close_ss_index_builder(src_table, dest_tablet_index, index_builder, res))) {
    LOG_WARN("close ss index builder failed", K(ret), K(dest_tablet_index), K(src_table));
#endif
  } else if (!GCTX.is_shared_storage_mode() &&
      OB_FAIL(index_builder->close(res))) {
    LOG_WARN("close sstable index builder failed", K(ret));
  } else if (OB_FAIL(src_table.get_meta(meta_handle))) {
    LOG_WARN("get sstable meta failed", K(ret));
  } else {
    const ObTabletID &dst_tablet_id = param_->dest_tablets_id_.at(dest_tablet_index);
    const ObSSTableBasicMeta &basic_meta = meta_handle.get_sstable_meta().get_basic_meta();
    if (OB_FAIL(create_sstable_param.init_for_split(dst_tablet_id, src_table.get_key(), basic_meta,
                                                    basic_meta.schema_version_, split_point_macros_id, res))) {
      LOG_WARN("init sstable param fail", K(ret), K(dst_tablet_id), K(src_table.get_key()), K(basic_meta), K(res));
    }
  }
  return ret;
}

int ObTabletSplitMergeTask::check_need_fill_empty_sstable(
    ObLSHandle &ls_handle,
    const bool is_minor_sstable,
    const ObITable::TableKey &table_key,
    const ObTabletID &dst_tablet_id,
    bool &need_fill_empty_sstable,
    SCN &end_scn)
{
  int ret = OB_SUCCESS;
  ObTabletHandle dst_tablet_handle;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_handle;
  ObTabletCreateSSTableParam create_sstable_param;
  need_fill_empty_sstable = false;
  end_scn.reset();
  if (is_minor_sstable) {
    if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle, dst_tablet_id, dst_tablet_handle, ObMDSGetTabletMode::READ_ALL_COMMITED))) {
      LOG_WARN("get tablet failed", K(ret));
    } else if (OB_FAIL(dst_tablet_handle.get_obj()->fetch_table_store(table_store_handle))) {
      LOG_WARN("failed to fetch table store", K(ret));
    } else {
      ObITable *first_dst_table = table_store_handle.get_member()->get_minor_sstables().get_boundary_table(false/*is_last*/);
      const SCN dst_start_scn = nullptr != first_dst_table ? first_dst_table->get_key().get_start_scn()
                                                           : dst_tablet_handle.get_obj()->get_tablet_meta().clog_checkpoint_scn_;
      if (table_key.get_end_scn() < dst_start_scn) {
        need_fill_empty_sstable = true;
        end_scn = dst_start_scn;
      }
    }
  }
  return ret;
}

int ObTabletSplitMergeTask::build_create_empty_sstable_param(
    const ObSSTableBasicMeta &meta,
    const ObITable::TableKey &table_key,
    const ObTabletID &dst_tablet_id,
    const SCN &end_scn,
    ObTabletCreateSSTableParam &create_sstable_param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!meta.is_valid() || !table_key.is_valid() || !dst_tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(meta), K(table_key), K(dst_tablet_id));
  } else if (OB_FAIL(create_sstable_param.init_for_split_empty_minor_sstable(dst_tablet_id,
                     table_key.get_end_scn()/*start_scn*/, end_scn, meta))) {
    LOG_WARN("init sstable param fail", K(ret), K(meta), K(table_key), K(dst_tablet_id), K(end_scn));
  }
  return ret;
}

int ObTabletSplitMergeTask::update_table_store_with_batch_tables(
    const int64_t ls_rebuild_seq,
    const ObLSHandle &ls_handle,
    const ObTabletHandle &src_tablet_handle,
    const ObTabletID &dst_tablet_id,
    const ObTablesHandleArray &tables_handle,
    const compaction::ObMergeType &merge_type,
    const ObIArray<ObITable::TableKey> &skipped_split_major_keys,
    const int64_t op_id,
    const share::SCN &dest_reorg_scn)
{
  int ret = OB_SUCCESS;
  ObBatchUpdateTableStoreParam param;
#ifdef ERRSIM
  if (is_major_merge_type(merge_type)) {
    ret = OB_E(EventTable::EN_AFTER_MINOR_BUT_BEFORE_MAJOR_SPLIT) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      LOG_WARN("errsim error code for split", K(ret));
    }
  }
#endif
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObTabletSplitUtil::build_update_table_store_param(
      dest_reorg_scn,
      ls_rebuild_seq,
      src_tablet_handle.get_obj()->get_snapshot_version(),
      src_tablet_handle.get_obj()->get_multi_version_start(),
      dst_tablet_id, tables_handle, merge_type, skipped_split_major_keys,
      param))) {
    LOG_WARN("build upd param failed", K(ret));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (is_mds_merge(merge_type) && OB_FAIL(DDL_SIM(MTL_ID(), 1/*ddl_task_id*/, SPLIT_UPDATE_MDS_FAILED))) {
    LOG_WARN("ddl sim failed, update mds failed", K(ret));
  } else if (is_minor_merge(merge_type) && OB_FAIL(DDL_SIM(MTL_ID(), 1/*ddl_task_id*/, SPLIT_UPDATE_MINOR_FAILED))) {
    LOG_WARN("ddl sim failed, update minor failed", K(ret));
  } else if (is_major_merge(merge_type) && OB_FAIL(DDL_SIM(MTL_ID(), 1/*ddl_task_id*/, SPLIT_UPDATE_MAJOR_FAILED))) {
    LOG_WARN("ddl sim failed, update major failed", K(ret));
  } else if (is_mds_merge(merge_type) && OB_FAIL(DDL_SIM_WHEN(true/*condition*/, MTL_ID(), 1/*ddl_task_id*/, SPLIT_UPDATE_MDS_SLOW))) {
    LOG_WARN("ddl sim failed, download sstables slow", K(ret));
  } else if (is_minor_merge(merge_type) && OB_FAIL(DDL_SIM_WHEN(true/*condition*/, MTL_ID(), 1/*ddl_task_id*/, SPLIT_UPDATE_MINOR_SLOW))) {
    LOG_WARN("ddl sim failed, download sstables slow", K(ret));
  } else if (is_major_merge(merge_type) && OB_FAIL(DDL_SIM_WHEN(true/*condition*/, MTL_ID(), 1/*ddl_task_id*/, SPLIT_UPDATE_MAJOR_SLOW))) {
    LOG_WARN("ddl sim failed, download sstables slow", K(ret));
  } else if (GCTX.is_shared_storage_mode()) {
    ObArenaAllocator tmp_arena("SplitUpdTablet", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObTabletHandle local_tablet_handle;
    ObTabletHandle ss_tablet_handle;
    ObMetaUpdateReason update_reason = is_mds_merge(merge_type) ?
        ObMetaUpdateReason::TABLET_SPLIT_ADD_MDS_SSTABLE : is_major_merge(merge_type) ?
        ObMetaUpdateReason::TABLET_SPLIT_ADD_MAJOR_SSTABLE : ObMetaUpdateReason::TABLET_SPLIT_ADD_MINOR_SSTABLE;
    const share::ObLSID &ls_id = ls_handle.get_ls()->get_ls_id();
    if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle,
      dst_tablet_id, local_tablet_handle, ObMDSGetTabletMode::READ_ALL_COMMITED))) {
      LOG_WARN("get tablet failed", K(ret));
    } else if (OB_UNLIKELY(!local_tablet_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected err", K(ret), K(ls_id), K(dst_tablet_id), K(local_tablet_handle));
    } else if (dest_reorg_scn != local_tablet_handle.get_obj()->get_reorganization_scn()) {
      ret = OB_TABLET_REORG_SCN_NOT_MATCH;
      LOG_WARN("tablet reorg scn is not same, need retry", K(ret), "local tablet reorg scn", local_tablet_handle.get_obj()->get_reorganization_scn(),
          "dest reorg scn", dest_reorg_scn);
    } else if (OB_FAIL(MTL(ObSSMetaService*)->get_tablet(
        ls_id, dst_tablet_id,
        local_tablet_handle.get_obj()->get_reorganization_scn(),
        tmp_arena, ss_tablet_handle))) {
      LOG_WARN("get shared tablet fail", K(ret));
      if (OB_TABLET_NOT_EXIST == ret) {
        LOG_INFO("tablet meta not exist now, create shared tablet", K(ret), K(dst_tablet_id));
        if (OB_FAIL(MTL(ObSSMetaService*)->create_tablet(ls_id, dst_tablet_id,
            local_tablet_handle.get_obj()->get_reorganization_scn()))) {
          LOG_WARN("create tablet fail", K(ret), K(ls_id), K(dst_tablet_id));
        }
      }
    }
    if (FAILEDx(MTL(ObSSMetaService*)->build_tablet_with_batch_tables(
              ls_id,
              dst_tablet_id,
              dest_reorg_scn,
              update_reason,
              op_id,
              param))) {
      LOG_WARN("update shared tablet meta fail", K(ret), K(ls_id), K(dst_tablet_id), K(param));
    }
#endif
  } else {
    if (OB_FAIL(ls_handle.get_ls()->build_tablet_with_batch_tables(dst_tablet_id, param))) {
      LOG_WARN("failed to update tablet table store", K(ret), K(dst_tablet_id), K(param));
    }
  }
  FLOG_INFO("update batch sstables", K(ret), K(dst_tablet_id), K(param));

  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObMacroEndKey::assign(const ObMacroEndKey &other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(other));
  } else {
    macro_id_ = other.macro_id_;
    end_key_ = other.end_key_; //shallow copy
  }
  return ret;
}

int ObSplitDownloadSSTableTask::init(
    const int64_t ls_rebuild_seq,
    const ObLSID &ls_id,
    const ObTabletID &src_tablet_id,
    const ObIArray<ObTabletID> &dst_tablets_id,
    const bool can_reuse_macro_block,
    const share::SCN &dest_reorg_scn,
    const share::SCN &split_start_scn)
{
  int ret = OB_SUCCESS;
  ObIDag *this_dag = get_dag();
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(ls_rebuild_seq < 0
      || !ls_id.is_valid()
      || !src_tablet_id.is_valid()
      || dst_tablets_id.empty()
      || nullptr == this_dag
      || !is_data_split_dag(this_dag->get_type())
      || !dest_reorg_scn.is_valid()
      || !split_start_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(ls_rebuild_seq), K(ls_id),
      K(src_tablet_id), K(dst_tablets_id), KPC(this_dag), K(dest_reorg_scn), K(split_start_scn));
  } else if (OB_FAIL(dest_tablets_id_.assign(dst_tablets_id))) {
    LOG_WARN("assign failed", K(ret));
  } else {
    ls_rebuild_seq_ = ls_rebuild_seq;
    ls_id_ = ls_id;
    source_tablet_id_ = src_tablet_id;
    can_reuse_macro_block_ = can_reuse_macro_block;
    dest_reorg_scn_ = dest_reorg_scn;
    split_start_scn_ = split_start_scn;
    is_inited_ = true;
  }
  return ret;
}

int ObSplitDownloadSSTableTask::iterate_macros_update_eff_id(
    const ObTabletID &dest_tablet_id,
    ObDualMacroMetaIterator &meta_iter,
    ObIArray<MacroBlockId> &dest_macro_ids,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObDataMacroBlockMeta macro_meta;
  ObMacroBlockDesc data_macro_desc;
  MacroBlockId tmp_macro_id;
  ObDatumRowkey tmp_row_key;
  const int64_t MACRO_BATCH_SIZE = 100;
  ObSSLocalCacheService *local_cache_service = MTL(ObSSLocalCacheService *);
  ObSEArray<blocksstable::MacroBlockId, MACRO_BATCH_SIZE> macro_ids;
  macro_meta.reset();
  data_macro_desc.reuse();
  data_macro_desc.macro_meta_ = &macro_meta;
  int cmp_ret = 0;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(meta_iter.get_next_macro_block(data_macro_desc))) {
      if (OB_ITER_END == ret) {
        if (!macro_ids.empty()) {
          local_cache_service->update_macro_cache_tablet_id(macro_ids, dest_tablet_id);
        }
        ret = OB_SUCCESS;
        // get the last macro end keys
        if (!dest_macro_ids.empty() && dest_macro_ids.at(0) != tmp_macro_id) {
          if (OB_FAIL(dest_macro_ids.push_back(tmp_macro_id))) {
            LOG_WARN("failed to push back", K(ret), K(dest_macro_ids), K(tmp_macro_id));
          }
        }
        break;
      } else {
        LOG_WARN("get data macro meta failed", K(ret));
      }
    } else if (OB_FAIL(macro_ids.push_back(data_macro_desc.macro_block_id_))) {
      LOG_WARN("failed to push back into macro_ids", K(ret));
    } else {
      if (macro_ids.count() == MACRO_BATCH_SIZE) {
        local_cache_service->update_macro_cache_tablet_id(macro_ids, dest_tablet_id);
        macro_ids.reuse();
      }
      tmp_macro_id.reset();
      tmp_macro_id = data_macro_desc.macro_block_id_;
      if (dest_macro_ids.empty()) {
        //get first macro end keys
        if (OB_FAIL(dest_macro_ids.push_back(tmp_macro_id))) {
          LOG_WARN("failed to push back", K(ret), K(dest_macro_ids), K(tmp_macro_id));
        }
      }
    }
  }
  return ret;
}

int ObSplitDownloadSSTableTask::iterate_micros_update_eff_id(const ObTabletID &dest_tablet_id, ObMicroBlockIndexIterator &micro_iter)
{
  int ret = OB_SUCCESS;

  ObMicroIndexInfo micro_index_info;
  ObSSMicroBlockCacheKey micro_cache_key;
  const int64_t MICRO_BATCH_SIZE = 50;
  ObSEArray<ObSSMicroBlockCacheKey, MICRO_BATCH_SIZE> micro_keys;
  ObSSLocalCacheService *local_cache_service = MTL(ObSSLocalCacheService *);
  if (OB_ISNULL(local_cache_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr of local cache service", K(ret));
  }
  while (OB_SUCC(ret)) {
    micro_index_info.reset();
    if (OB_FAIL(micro_iter.get_next(micro_index_info))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get data micro meta failed", K(ret));
      } else {
        ret = OB_SUCCESS;
        if (micro_keys.count() != 0) {
          local_cache_service->update_micro_cache_tablet_id(micro_keys, dest_tablet_id);
        }
        break;
      }
    } else if (OB_ISNULL(micro_index_info.row_header_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null ptr of micro_index_info.row_header_", K(ret));
    } else {
      if (micro_index_info.row_header_->has_valid_logic_micro_id()) {
        micro_cache_key.logic_micro_id_ = micro_index_info.row_header_->get_logic_micro_id();
        micro_cache_key.mode_ = ObSSMicroBlockCacheKeyMode::LOGICAL_KEY_MODE;
        micro_cache_key.micro_crc_ = micro_index_info.get_data_checksum();
      } else {
        micro_cache_key.micro_id_.macro_id_ = micro_index_info.row_header_->get_macro_id();
        micro_cache_key.micro_id_.offset_ = micro_index_info.row_header_->get_block_offset();
        micro_cache_key.micro_id_.size_ = micro_index_info.row_header_->get_block_size();
        micro_cache_key.mode_ = ObSSMicroBlockCacheKeyMode::PHYSICAL_KEY_MODE;
      }
      if (OB_FAIL(micro_keys.push_back(micro_cache_key))) {
        LOG_WARN("failed to push back into micro keys", K(ret));
      } else if (micro_keys.count() == MICRO_BATCH_SIZE) {
        local_cache_service->update_micro_cache_tablet_id(micro_keys, dest_tablet_id);
        micro_keys.reuse();
      }
    }
  }
  return ret;
}

int ObSplitDownloadSSTableTask::is_split_dest_sstable(const ObSSTable &sstable, bool &is_split_dest_sstable)
{
  int ret = OB_SUCCESS;
  const ObSSTableMeta *sstable_meta = nullptr;
  ObSSTableMetaHandle sstable_meta_handle;
  is_split_dest_sstable = false;
  if (OB_FAIL(sstable.get_meta(sstable_meta_handle))) {
    LOG_WARN("failed to get sstable meta handle", K(ret), K(sstable));
  } else if (OB_FAIL(sstable_meta_handle.get_sstable_meta(sstable_meta))) {
    LOG_WARN("failed to get meta", K(ret), K(sstable_meta_handle));
  } else if (OB_ISNULL(sstable_meta)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error of sstable_meta", K(ret), KP(sstable_meta));
  } else {
    is_split_dest_sstable = sstable_meta->is_split_table();
  }
  return ret;
}

int ObSplitDownloadSSTableTask::prewarm(
    const ObTabletHandle &ss_tablet_handle,
    const ObTablesHandleArray &batch_sstables_handle)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObITable *, MAX_SSTABLE_CNT_IN_STORAGE> batch_tables;
  if (OB_UNLIKELY(!ss_tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(ss_tablet_handle));
  } else if (OB_FAIL(batch_sstables_handle.get_tables(batch_tables))) {
    LOG_WARN("get batch sstables failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_tables.count(); i++) {
      ObSSTable *sstable = nullptr;
      if (OB_ISNULL(sstable = static_cast<ObSSTable *>(batch_tables.at(i)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null split sstable", K(ret), K(batch_sstables_handle));
      } else {
        bool stop = false;
        storage::ObSSTableMacroPrewarmer prewarmer(sstable, stop, ObSSMicroCacheAccessType::SPLIT_PART_PREWARM_TYPE);
        if (OB_FAIL(prewarmer.do_prewarm(ss_tablet_handle))) {
          LOG_WARN("failed to do prewarm", K(ret), K(ss_tablet_handle));
        } else if (!sstable->is_mds_sstable() && OB_FAIL(prewarm_for_split(ss_tablet_handle, *sstable))) {
          LOG_WARN("failed to prewarm for split", K(ret), K(ss_tablet_handle));
        }
      }
    }
  }
  return ret;
}

int ObSplitDownloadSSTableTask::prewarm_for_split(const ObTabletHandle &dest_tablet_handle,
                                                  ObSSTable &sstable)
{
  int ret = OB_SUCCESS;
  ObMalloc allocator;
  allocator.set_attr(ObMemAttr(MTL_ID(), "splprewarm"));
  const ObTabletID &dest_tablet_id = dest_tablet_handle.get_obj()->get_tablet_id();
  const ObITableReadInfo *read_info = nullptr;
  bool is_from_rewrite_warm_macro = false;
  ObMicroBlockIndexIterator micro_iter;
  ObSEArray<MacroBlockId, 2> dest_macro_ids; // record the first and last macros of sstable if any
  ObTablet *dest_tablet = nullptr;
  if (OB_ISNULL(dest_tablet = dest_tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to check null ptr", K(ret), K(dest_tablet));
  } else if (OB_FAIL(dest_tablet->get_sstable_read_info(&sstable, read_info))) {
    LOG_WARN("fail to get index read info ", KR(ret), K(sstable));
  } else {
    SMART_VAR(ObDualMacroMetaIterator, meta_iter) {
      ObDatumRange whole_range;
      whole_range.set_whole_range();
      const bool is_small_sstable = sstable.is_small_sstable();
      const bool is_major_sstable = sstable.is_major_sstable();
      if (OB_FAIL(meta_iter.open(sstable, whole_range, *read_info, allocator))) {
        LOG_WARN("open dual macro meta iter failed", K(ret), K(sstable));
      } else if (OB_FAIL(iterate_macros_update_eff_id(dest_tablet_id, meta_iter, dest_macro_ids, allocator))) {
        LOG_WARN("failed to iterate macros and update effective id", K(ret));
      } else if (OB_FAIL(micro_iter.open(sstable, whole_range, dest_tablet->get_rowkey_read_info(), allocator, true))) {
        LOG_WARN("failed to open micro iter", K(ret), K(sstable), K(dest_tablet_id));
      } else if (OB_FAIL(iterate_micros_update_eff_id(dest_tablet_id, micro_iter))) {
        LOG_WARN("failed to iterate micros and update effective id", K(ret), K(dest_tablet_id));
      } else if (OB_FAIL(prewarm_split_point_macro_if_need(dest_tablet->get_tablet_id().id(), sstable, dest_macro_ids))) {
        LOG_WARN("failed to try prewamr split point macro", K(ret), K(sstable), K(dest_macro_ids));
      }
    }
  }
  return ret;
}

int ObSplitDownloadSSTableTask::prewarm_split_point_macro_if_need(const int64_t dest_tablet_id,
                                                                  const ObSSTable &dest_sstable,
                                                                  const ObIArray<MacroBlockId> &dest_macro_ids/*fist and last macro of dest sstable if any*/)
{
  int ret = OB_SUCCESS;
  bool stop = false;
  /*TODO: only prewarm warm macro ly435438*/
  storage::ObSSTableMacroPrewarmer prewarmer(&dest_sstable, stop, ObSSMicroCacheAccessType::SPLIT_PART_PREWARM_TYPE);
  for (int64_t i = 0; i < dest_macro_ids.count(); ++i) {
    if (OB_FAIL(prewarmer.prewarm_single_macro(dest_macro_ids.at(i), dest_tablet_id))) {
      LOG_WARN("failed to prewarm macro", K(ret), K(dest_macro_ids.at(i)));
    }
  }
  return ret;
}

int ObSplitDownloadSSTableTask::get_shared_tablet_versions_iter(
    const ObTabletID &dst_tablet_id,
    const share::SCN &end_version,
    ObSSMetaIterGuard<ObSSTabletIterator> &iter_guard,
    ObSSTabletIterator *&tablet_version_iter)
{
  int ret = OB_SUCCESS;
  tablet_version_iter = nullptr;
  iter_guard.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!dst_tablet_id.is_valid() || !end_version.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(dst_tablet_id), K(end_version));
  } else {
    ObSSMetaReadParam read_param;
    read_param.set_tablet_level_param(ObSSMetaReadParamType::TABLET_KEY,
                                      ObSSMetaReadResultType::READ_WHOLE_ROW,
                                      false, /*try read local*/
                                      ObSSLogMetaType::SSLOG_TABLET_META,
                                      ls_id_,
                                      dst_tablet_id,
                                      dest_reorg_scn_);
    ObSSTabletIterator *tablet_iter = nullptr;
    if (OB_FAIL(MTL(ObSSMetaService*)->get_tablet(read_param,
                                                  ObMetaVersionRange(SCN::min_scn()/*start_version*/, end_version, true/*include_last_version*/),
                                                  iter_guard))) {
      LOG_WARN("get tablet iter fail", K(ret), K(read_param), K(end_version));
    } else if (OB_FAIL(iter_guard.get_iter(tablet_iter))) {
      LOG_WARN("get iter fail", K(ret));
    } else if (OB_ISNULL(tablet_iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet iter is null", K(ret));
    } else {
      tablet_version_iter = tablet_iter;
    }
  }
  return ret;
}

int ObSplitDownloadSSTableTask::get_shared_tablet_for_split_major(
    const ObTabletID &dst_tablet_id,
    share::SCN &target_tablet_version)
{
  int ret = OB_SUCCESS;
  target_tablet_version.reset();
  ObArenaAllocator tmp_arena("SplitIterSSTab", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObSSMetaIterGuard<ObSSTabletIterator> iter_guard;
  ObSSTabletIterator *tablet_iter = nullptr;
  ObTabletHandle tmp_tablet_handle;
  ObSSMetaUpdateMetaInfo update_meta_info;
  ObAtomicExtraInfo extra_info;
  share::SCN iter_tablet_version;
  share::SCN end_version;
  if (OB_UNLIKELY(!dst_tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(dst_tablet_id));
  } else if (OB_FAIL(MTL(ObSSMetaService*)->get_max_committed_meta_scn(ls_id_, end_version))) {
    LOG_WARN("get max committed meta scn failed", K(ret));
  } else if (OB_FAIL(get_shared_tablet_versions_iter(dst_tablet_id, end_version, iter_guard, tablet_iter))) {
    LOG_WARN("get shared tablet versions iter failed", K(ret), K(dst_tablet_id), K(end_version));
  } else {
    while (OB_SUCC(ret)) {
      tmp_tablet_handle.reset();
      tmp_arena.reuse();
      if (OB_FAIL(tablet_iter->get_next(tmp_arena, tmp_tablet_handle, iter_tablet_version, update_meta_info, extra_info))) {
        LOG_WARN("get next tablet fail", K(ret), K(ls_id_), K(dst_tablet_id), K(dest_reorg_scn_));
      } else if (OB_UNLIKELY(!tmp_tablet_handle.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet handle is invalid", K(ret), K(tmp_tablet_handle));
      } else if (OB_UNLIKELY(!tmp_tablet_handle.get_obj()->get_tablet_meta().split_info_.is_data_incomplete())) {
        target_tablet_version = iter_tablet_version;
      } else if (OB_UNLIKELY(!target_tablet_version.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("target tablet version is invalid", K(ret), K(tmp_tablet_handle));
      } else {
        break;
      }
    }
  }
  return ret;
}

int ObSplitDownloadSSTableTask::get_shared_tablet_for_split_mds(
    const ObTabletID &dst_tablet_id,
    const share::SCN &target_major_tablet_version,
    share::SCN &target_tablet_version)
{
  int ret = OB_SUCCESS;
  target_tablet_version.reset();
  ObArenaAllocator tmp_arena("SplitIterSSTab", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());

  ObSSMetaIterGuard<ObSSTabletIterator> iter_guard;
  ObSSTabletIterator *tablet_iter = nullptr;
  ObSSMetaUpdateMetaInfo update_meta_info;
  ObAtomicExtraInfo extra_info;
  share::SCN iter_tablet_version;

  ObTabletHandle tmp_tablet_handle;
  ObTableStoreIterator mds_sstables_iter;
  ObITable *first_table = nullptr;
  ObSSMetaReadParam read_param;
  read_param.set_tablet_level_param(ObSSMetaReadParamType::TABLET_KEY,
                                    ObSSMetaReadResultType::READ_WHOLE_ROW,
                                    true, /*try read local*/
                                    ObSSLogMetaType::SSLOG_TABLET_META,
                                    ls_id_,
                                    dst_tablet_id,
                                    dest_reorg_scn_);
  if (OB_UNLIKELY(!dst_tablet_id.is_valid() || !target_major_tablet_version.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(dst_tablet_id), K(target_major_tablet_version));
  } else if (OB_FAIL(MTL(ObSSMetaService*)->get_tablet(read_param, target_major_tablet_version, tmp_arena, tmp_tablet_handle))) {
    LOG_WARN("get target tablet fail", K(ret), K(target_major_tablet_version));
  } else if (OB_UNLIKELY(!tmp_tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet handl is invalid", K(ret), K(target_major_tablet_version));
  } else if (OB_FAIL(tmp_tablet_handle.get_obj()->get_mds_sstables(mds_sstables_iter))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (mds_sstables_iter.count() == 0) {
    target_tablet_version = target_major_tablet_version;
    LOG_INFO("found target version, no mds sstable", K(ret), K(dst_tablet_id), K(target_major_tablet_version));
  } else if (OB_FAIL(mds_sstables_iter.get_boundary_table(false/*is_last*/, first_table))) {
    LOG_WARN("fail to get boundary table", K(ret));
  } else if (OB_ISNULL(first_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("first table is null", K(ret), K(dst_tablet_id), K(target_major_tablet_version));
  } else if (OB_UNLIKELY(first_table->get_start_scn() > split_start_scn_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("first table start scn is greater than split start scn", K(ret), K(dst_tablet_id), K(target_major_tablet_version));
  } else if (first_table->get_start_scn() == split_start_scn_) {
    target_tablet_version = target_major_tablet_version;
    LOG_INFO("found target version, no mds sstable", K(ret), K(dst_tablet_id), K(target_major_tablet_version));
  } else if (first_table->get_end_scn() == split_start_scn_) {
    target_tablet_version = target_major_tablet_version;
    LOG_INFO("found target version", K(ret), K(dst_tablet_id), K(target_major_tablet_version), KPC(first_table));
  } else {
    if (OB_FAIL(get_shared_tablet_versions_iter(dst_tablet_id, SCN::scn_dec(target_major_tablet_version), iter_guard, tablet_iter))) {
      LOG_WARN("get shared tablet versions iter failed", K(ret), K(dst_tablet_id), K(target_major_tablet_version));
    } else {
      while (OB_SUCC(ret) && !target_tablet_version.is_valid()) {
        first_table = nullptr;
        mds_sstables_iter.reset();
        tmp_tablet_handle.reset();
        tmp_arena.reuse();
        if (OB_FAIL(tablet_iter->get_next(tmp_arena, tmp_tablet_handle, iter_tablet_version, update_meta_info, extra_info))) {
          LOG_WARN("get next tablet fail", K(ret), K(dst_tablet_id));
        } else if (OB_UNLIKELY(!tmp_tablet_handle.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tablet handle is invalid", K(ret), K(dst_tablet_id), K(tmp_tablet_handle));
        } else if (OB_FAIL(tmp_tablet_handle.get_obj()->get_mds_sstables(mds_sstables_iter))) {
          LOG_WARN("fail to get mini mds sstables", K(ret));
        } else if (OB_FAIL(mds_sstables_iter.get_boundary_table(false/*is_last*/, first_table))) {
          LOG_WARN("fail to get boundary table", K(ret));
        } else if (OB_ISNULL(first_table)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("no first table", K(ret), K(dst_tablet_id), K(iter_tablet_version), K(tmp_tablet_handle));
        } else if (first_table->get_end_scn() == split_start_scn_) {
          target_tablet_version = iter_tablet_version;
          LOG_INFO("found target version", K(ret), K(dst_tablet_id), K(target_major_tablet_version), K(target_tablet_version));
        }
      }
    }
  }
  if (OB_SUCC(ret) && !target_tablet_version.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no satisfied shared tablet version", K(ret), K(dst_tablet_id), K(target_major_tablet_version));
  }
  return ret;
}

int ObSplitDownloadSSTableTask::get_shared_tablet_for_split_minor(
    const ObTabletID &dst_tablet_id,
    const share::SCN &target_major_tablet_version,
    share::SCN &target_tablet_version)
{
  int ret = OB_SUCCESS;
  target_tablet_version.reset();
  ObArenaAllocator tmp_arena("SplitIterSSTab", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());

  ObSSMetaIterGuard<ObSSTabletIterator> iter_guard;
  ObSSTabletIterator *tablet_iter = nullptr;
  ObSSMetaUpdateMetaInfo update_meta_info;
  ObAtomicExtraInfo extra_info;
  share::SCN iter_tablet_version;

  ObTabletHandle tmp_tablet_handle;
  ObTableStoreIterator minor_sstables_iter;
  ObITable *first_table = nullptr;
  ObSSMetaReadParam read_param;
  read_param.set_tablet_level_param(ObSSMetaReadParamType::TABLET_KEY,
                                    ObSSMetaReadResultType::READ_WHOLE_ROW,
                                    true, /*try read local*/
                                    ObSSLogMetaType::SSLOG_TABLET_META,
                                    ls_id_,
                                    dst_tablet_id,
                                    dest_reorg_scn_);
  if (OB_FAIL(MTL(ObSSMetaService*)->get_tablet(read_param, target_major_tablet_version, tmp_arena, tmp_tablet_handle))) {
    LOG_WARN("get target tablet fail", K(ret), K(target_major_tablet_version));
  } else if (OB_UNLIKELY(!tmp_tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet handl is invalid", K(ret), K(target_major_tablet_version));
  } else if (OB_FAIL(tmp_tablet_handle.get_obj()->get_mini_minor_sstables(minor_sstables_iter))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (minor_sstables_iter.count() == 0) {
    target_tablet_version = target_major_tablet_version;
    LOG_INFO("found target version, no minor sstable", K(ret), K(dst_tablet_id), K(target_major_tablet_version));
  } else if (OB_FAIL(minor_sstables_iter.get_boundary_table(false/*is_last*/, first_table))) {
    LOG_WARN("fail to get boundary table", K(ret));
  } else if (OB_ISNULL(first_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("first table is null", K(ret), K(dst_tablet_id), K(target_major_tablet_version));
  } else if (OB_UNLIKELY(first_table->get_start_scn() > split_start_scn_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("first table start scn is greater than split start scn", K(ret), K(dst_tablet_id), K(target_major_tablet_version));
  } else if (first_table->get_start_scn() == split_start_scn_ && first_table->get_end_scn() > split_start_scn_) {
    target_tablet_version = target_major_tablet_version;
    LOG_INFO("found target version", K(ret), K(dst_tablet_id), K(target_major_tablet_version), KPC(first_table));
  } else if (first_table->get_end_scn() == split_start_scn_) {
    target_tablet_version = target_major_tablet_version;
    LOG_INFO("found target version", K(ret), K(dst_tablet_id), K(target_major_tablet_version), KPC(first_table));
  } else {
    if (OB_FAIL(get_shared_tablet_versions_iter(dst_tablet_id, SCN::scn_dec(target_major_tablet_version), iter_guard, tablet_iter))) {
      LOG_WARN("get shared tablet versions iter failed", K(ret), K(dst_tablet_id), K(target_major_tablet_version));
    } else {
      while (OB_SUCC(ret) && !target_tablet_version.is_valid()) {
        first_table = nullptr;
        minor_sstables_iter.reset();
        tmp_tablet_handle.reset();
        tmp_arena.reuse();
        if (OB_FAIL(tablet_iter->get_next(tmp_arena, tmp_tablet_handle, iter_tablet_version, update_meta_info, extra_info))) {
          LOG_WARN("get next tablet fail", K(ret), K(dst_tablet_id));
        } else if (OB_UNLIKELY(!tmp_tablet_handle.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tablet handle is invalid", K(ret), K(tmp_tablet_handle));
        } else if (OB_FAIL(tmp_tablet_handle.get_obj()->get_mini_minor_sstables(minor_sstables_iter))) {
          LOG_WARN("fail to get mini mds sstables", K(ret));
        } else {
          bool need_end_loop = false;
          ObTableHandleV2 cur_table_handle;
          ObSSTable *sstable = nullptr;
          while (OB_SUCC(ret) && !need_end_loop) {
            sstable = nullptr;
            cur_table_handle.reset();
            if (OB_FAIL(minor_sstables_iter.get_next(cur_table_handle))) {
              if (OB_UNLIKELY(OB_ITER_END != ret)) {
                LOG_WARN("fail to get next table", K(ret));
              } else {
                need_end_loop = true;
                ret = OB_SUCCESS;
              }
            } else if (OB_FAIL(cur_table_handle.get_sstable(sstable))) {
              LOG_WARN("failed to get sstable from handle", K(ret), K(cur_table_handle));
            } else if (OB_ISNULL(sstable)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected err", K(ret), KPC(sstable));
            } else if (sstable->get_end_scn() == split_start_scn_) {
              need_end_loop = true;
              target_tablet_version = iter_tablet_version;
              LOG_INFO("found target version", K(ret), K(dst_tablet_id), K(split_start_scn_),
                K(target_major_tablet_version), K(target_tablet_version), KPC(sstable));
            } else if (sstable->get_end_scn() > split_start_scn_) {
              need_end_loop = true;
            }
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && !target_tablet_version.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no satisfied shared tablet version", K(ret), K(dst_tablet_id), K(target_major_tablet_version));
  }
  return ret;
}

int ObSplitDownloadSSTableTask::get_specified_shared_tablet_versions(
    const ObTabletID &dst_tablet_id,
    ObIArray<share::SCN> &target_tablet_versions) // order by minor, mds, major.
{
  int ret = OB_SUCCESS;
  share::SCN target_major_tablet_version;
  share::SCN target_mds_tablet_version;
  share::SCN target_minor_tablet_version;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!dst_tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(dst_tablet_id));
  } else if (OB_FAIL(get_shared_tablet_for_split_major(dst_tablet_id, target_major_tablet_version))) {
    LOG_WARN("get_shared_tablet_for_split_major failed", K(ret), K(dst_tablet_id),
      K(target_major_tablet_version));
  } else if (OB_FAIL(get_shared_tablet_for_split_mds(dst_tablet_id, target_major_tablet_version, target_mds_tablet_version))) {
    LOG_WARN("get_shared_tablet_for_split_mds failed", K(ret), K(dst_tablet_id),
      K(target_major_tablet_version), K(target_mds_tablet_version));
  } else if (OB_FAIL(get_shared_tablet_for_split_minor(dst_tablet_id, target_mds_tablet_version, target_minor_tablet_version))) {
    LOG_WARN("get_shared_tablet_for_split_minor failed", K(ret), K(dst_tablet_id),
      K(target_major_tablet_version), K(target_mds_tablet_version), K(target_minor_tablet_version));
  } else if (OB_FAIL(target_tablet_versions.push_back(target_minor_tablet_version))) {
    LOG_WARN("push back failed", K(ret));
  } else if (OB_FAIL(target_tablet_versions.push_back(target_mds_tablet_version))) {
    LOG_WARN("push back failed", K(ret));
  } else if (OB_FAIL(target_tablet_versions.push_back(target_major_tablet_version))) {
    LOG_WARN("push back failed", K(ret));
  }
  return ret;
}

int check_need_block_downloading()
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(BEFORE_SPLIT_DOWNLOAD_SSTABLE);
  ret = OB_E(EventTable::EN_BLOCK_BEFORE_SPLIT_DOWNLOAD_SSTABLE) OB_SUCCESS;
  if (OB_EAGAIN == ret) {
    // In this scenario, we need to block downloading for the followers(z1 & z3).
    common::ObZone self_zone;
    ObString leader_zone_str("z2");
    if (OB_FAIL(SVR_TRACER.get_server_zone(GCTX.self_addr(), self_zone))) { // overwrite ret is expected.
      LOG_WARN("get server zone failed", K(ret));
    } else if (0 != ObCharset::instr(ObCollationType::CS_TYPE_UTF8MB4_GENERAL_CI, self_zone.str().ptr(), self_zone.str().length(),
        leader_zone_str.ptr(), leader_zone_str.length())) {
    } else {
      ret = OB_EAGAIN;
      LOG_INFO("[ERRSIM] set eagain to block followers download", K(ret));
    }
  }
  return ret;
}

int ObSplitDownloadSSTableTask::process()
{
  int ret = OB_SUCCESS;
  ObArenaAllocator arena_alloc("SplitGetTab", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObIDag *tmp_dag = get_dag();
  ObIDataSplitDag *data_split_dag = nullptr;
  ObLSHandle ls_handle;
  ObTabletHandle local_source_tablet_hdl;
  int64_t epoch = 0;
  bool is_sswriter = false;
  if (OB_FAIL(check_need_block_downloading())) {
    LOG_INFO("block downloading", K(ret));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == tmp_dag
      || !is_data_split_dag(tmp_dag->get_type()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), KPC(tmp_dag));
  } else if (OB_ISNULL(data_split_dag = static_cast<ObIDataSplitDag *>(tmp_dag))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KPC(tmp_dag));
  } else if (OB_SUCCESS != data_split_dag->get_complement_data_ret()) {
    LOG_WARN("complement data has already failed",
      "ret", data_split_dag->get_complement_data_ret());
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id_));
  } else if (OB_FAIL(ObTabletSplitUtil::get_tablet(arena_alloc, ls_handle, source_tablet_id_,
      false/*is_shared_mode*/, local_source_tablet_hdl))) {
    LOG_WARN("get tablet failed", K(ret), K(source_tablet_id_));
  } else if (OB_UNLIKELY(!local_source_tablet_hdl.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected tablet", K(ret), K(source_tablet_id_), K(local_source_tablet_hdl));
  } else if (OB_FAIL(ObSSDataSplitHelper::check_at_sswriter_lease(
      ls_id_, source_tablet_id_, epoch, is_sswriter))) {
    LOG_WARN("check executor failed", K(ret));
  } else if (OB_FAIL(DDL_SIM_WHEN(!is_sswriter, MTL_ID(), 1/*ddl_task_id*/, SPLIT_DOWNLOAD_SSTABLE_SLOW))) {
    LOG_WARN("ddl sim failed, download sstables slow", K(ret));
  } else if (OB_FAIL(download_sstables_and_update_local(
      ls_handle,
      local_source_tablet_hdl))) {
    LOG_WARN("download and prewarm sstables failed", K(ret));
  } else if (can_reuse_macro_block_) {
    if (OB_FAIL(DDL_SIM(MTL_ID(), 1/*ddl_task_id*/, SPLIT_UPDATE_SOURCE_TABLET_FAILED))) {
      LOG_WARN("ddl sim failed, update source tablet failed", K(ret), K(ls_id_), K(source_tablet_id_));
    } else if (OB_FAIL(ObSSDataSplitHelper::set_source_tablet_split_status(
        ls_handle, local_source_tablet_hdl, ObSplitTabletInfoStatus::CANT_GC_MACROS))) {
      LOG_WARN("set can not gc data block failed", K(ret));
    }
  }

  if (OB_NOT_NULL(data_split_dag) && OB_SUCCESS == data_split_dag->get_complement_data_ret()) {
    data_split_dag->set_complement_data_ret(ret);
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObSplitDownloadSSTableTask::collect_split_sstables(
    ObArenaAllocator &allocator,
    const share::ObSplitSSTableType &split_sstable_type,
    const ObTableStoreIterator &ss_table_store_iterator,
    ObTablesHandleArray &batch_sstables_handle)
{
  int ret = OB_SUCCESS;
  batch_sstables_handle.reset();
  ObSEArray<ObITable *, MAX_SSTABLE_CNT_IN_STORAGE> sstables_in_table_store;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ObTabletSplitUtil::get_participants(
      split_sstable_type, ss_table_store_iterator,
      false/*is_table_restore*/,
      ObArray<ObITable::TableKey>()/*skip_split_majors*/,
      sstables_in_table_store))) {
    LOG_WARN("get participant sstables failed", K(ret));
  } else if (OB_UNLIKELY(share::ObSplitSSTableType::SPLIT_MAJOR == split_sstable_type
      && sstables_in_table_store.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null majors when downloading", K(ret), K(ss_table_store_iterator));
  } else {
    ObTableHandleV2 table_handle_v2;
    share::SCN sstable_max_end_scn = share::SCN::min_scn();
    for (int64_t j = 0; OB_SUCC(ret) && j < sstables_in_table_store.count(); j++) {
      table_handle_v2.reset();
      ObITable *iter_sstable = sstables_in_table_store.at(j);
      if (OB_ISNULL(iter_sstable)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected err", K(ret), KPC(iter_sstable));
      } else if (iter_sstable->is_major_sstable()) {
        if (OB_FAIL(table_handle_v2.set_sstable(iter_sstable, &allocator))) {
          LOG_WARN("set sstable failed", K(ret));
        } else if (OB_FAIL(batch_sstables_handle.add_table(table_handle_v2))) {
          LOG_WARN("add table failed", K(ret));
        }
      } else {
        const SCN &sstable_start_scn = iter_sstable->get_start_scn();
        const SCN &sstable_end_scn = iter_sstable->get_end_scn();
        if (sstable_start_scn >= split_start_scn_) {
          LOG_TRACE("skip incremental sstable", K(ret), K(split_start_scn_), KPC(iter_sstable));
        } else if (sstable_end_scn > split_start_scn_) {
          ret = OB_ERR_SYS;
          LOG_WARN("a sstable with cross range", K(ret), K(split_start_scn_), KPC(iter_sstable));
        } else if (OB_FAIL(table_handle_v2.set_sstable(iter_sstable, &allocator))) {
          LOG_WARN("set sstable failed", K(ret));
        } else if (OB_FAIL(batch_sstables_handle.add_table(table_handle_v2))) {
          LOG_WARN("add table failed", K(ret));
        } else {
          sstable_max_end_scn = SCN::max(sstable_max_end_scn, sstable_end_scn);
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (share::ObSplitSSTableType::SPLIT_MAJOR == split_sstable_type) {
        if (OB_UNLIKELY(batch_sstables_handle.empty())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected empty majors when downloading", K(ret), K(sstables_in_table_store));
        }
      } else {
        if (OB_UNLIKELY(!batch_sstables_handle.empty() && sstable_max_end_scn != split_start_scn_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected sstable max end scn", K(ret), K(sstable_max_end_scn), K(split_start_scn_), K(batch_sstables_handle));
        }
      }
    }
  }
  return ret;
}

int ObSplitDownloadSSTableTask::download_sstables_and_update_local(
    ObLSHandle &ls_handle,
    const ObTabletHandle &local_source_tablet_hdl)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_arena("SplitUpdSSTa", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!ls_handle.is_valid()
      || !local_source_tablet_hdl.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(ls_handle), K(local_source_tablet_hdl));
  } else {
    ObSEArray<ObTabletID, 1> check_major_exist_tablets;
    const int64_t snapshot_version = local_source_tablet_hdl.get_obj()->get_snapshot_version();
    const int64_t mult_version_start = local_source_tablet_hdl.get_obj()->get_multi_version_start();
    ObSEArray<ObSplitSSTableType, 3> update_sstable_types;
    ObSEArray<share::SCN, 3> target_tablet_versions;
    if (OB_FAIL(update_sstable_types.push_back(share::ObSplitSSTableType::SPLIT_MINOR))) {
      LOG_WARN("push back failed", K(ret));
    } else if (OB_FAIL(update_sstable_types.push_back(share::ObSplitSSTableType::SPLIT_MDS))) {
      LOG_WARN("push back failed", K(ret));
    } else if (OB_FAIL(update_sstable_types.push_back(share::ObSplitSSTableType::SPLIT_MAJOR))) {
      LOG_WARN("push back failed", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < dest_tablets_id_.count(); i++) {
      target_tablet_versions.reset();
      check_major_exist_tablets.reset();
      tmp_arena.reuse();
      const ObTabletID &dst_tablet_id = dest_tablets_id_.at(i);
      bool is_local_data_completed = false;
      if (OB_FAIL(check_major_exist_tablets.push_back(dst_tablet_id))) {
        LOG_WARN("push back failed", K(ret));
      } else if (OB_FAIL(ObTabletSplitUtil::check_dest_data_completed(
          ls_handle,
          check_major_exist_tablets,
          false/*check_remote*/,
          is_local_data_completed))) {
        LOG_WARN("check all major exist failed", K(ret));
      } else if (is_local_data_completed) {
        FLOG_INFO("CHANGE TO TRACE LATER, local data has already completed", K(ret), K(dst_tablet_id));
      } else if (OB_FAIL(get_specified_shared_tablet_versions(dst_tablet_id, target_tablet_versions))) {
        LOG_WARN("get specified shared tablet versions failed", K(ret), K(ls_id_), K(dst_tablet_id), K(dest_reorg_scn_));
      } else {
        ObTabletHandle ss_tablet_handle;
        ObTableStoreIterator table_store_iterator;
        ObBatchUpdateTableStoreParam param;
        ObTablesHandleArray batch_sstables_handle;
        for (int64_t j = 0; OB_SUCC(ret) && j < update_sstable_types.count(); j++) {
          batch_sstables_handle.reset();
          param.reset();
          table_store_iterator.reset();
          ss_tablet_handle.reset();
          const share::SCN &target_tablet_version = target_tablet_versions.at(j); // minor -> mds -> major.
          const share::ObSplitSSTableType &split_sstable_type = update_sstable_types.at(j);
          const ObMergeType merge_type = ObSplitSSTableType::SPLIT_MAJOR == split_sstable_type ?
              MAJOR_MERGE : ObSplitSSTableType::SPLIT_MINOR == split_sstable_type ?
                MINOR_MERGE : MDS_MINI_MERGE;
          ObSSMetaReadParam read_param;
          read_param.set_tablet_level_param(ObSSMetaReadParamType::TABLET_KEY,
                                            ObSSMetaReadResultType::READ_WHOLE_ROW,
                                            true, /*try read local*/
                                            ObSSLogMetaType::SSLOG_TABLET_META,
                                            ls_id_,
                                            dst_tablet_id,
                                            dest_reorg_scn_);
          if (OB_FAIL(MTL(ObSSMetaService*)->get_tablet(read_param, target_tablet_version, tmp_arena, ss_tablet_handle))) {
            LOG_WARN("get target tablet fail", K(ret), K(target_tablet_version));
          } else if (OB_UNLIKELY(!ss_tablet_handle.is_valid())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("tablet handl is invalid", K(ret), K(target_tablet_version));
          } else if (OB_FAIL(ss_tablet_handle.get_obj()->get_all_tables(table_store_iterator))) {
            LOG_WARN("fail to fetch table store", K(ret));
          } else if (OB_FAIL(collect_split_sstables(tmp_arena, split_sstable_type,
                table_store_iterator,
                batch_sstables_handle))) {
            LOG_WARN("collect split sstables failed", K(ret), K(ss_tablet_handle));
          } else if (!batch_sstables_handle.empty()) {
            if (OB_FAIL(prewarm(ss_tablet_handle, batch_sstables_handle))) {
              LOG_WARN("failed to do split prewarm", K(ret));
            } else if (OB_FAIL(ObTabletSplitUtil::build_update_table_store_param(
                dest_reorg_scn_,
                ls_rebuild_seq_,
                snapshot_version/*snapshot_version*/,
                mult_version_start/*mult_version_start*/,
                dst_tablet_id,
                batch_sstables_handle,
                merge_type,
                ObArray<ObITable::TableKey>()/*skipped_split_major_keys*/,
                param))) {
              LOG_WARN("build upd param failed", K(ret), K(param));
            } else if (is_mds_merge(merge_type) && OB_FAIL(DDL_SIM(MTL_ID(), 1/*ddl_task_id*/, SPLIT_DOWNLOAD_MDS_FAILED))) {
              LOG_WARN("ddl sim failed, download mds failed", K(ret), K(source_tablet_id_), K(dst_tablet_id));
            } else if (is_minor_merge(merge_type) && OB_FAIL(DDL_SIM(MTL_ID(), 1/*ddl_task_id*/, SPLIT_DOWNLOAD_MINOR_FAILED))) {
              LOG_WARN("ddl sim failed, download minor failed", K(ret), K(source_tablet_id_), K(dst_tablet_id));
            } else if (is_major_merge(merge_type) && OB_FAIL(DDL_SIM(MTL_ID(), 1/*ddl_task_id*/, SPLIT_DOWNLOAD_MAJOR_FAILED))) {
              LOG_WARN("ddl sim failed, download major failed", K(ret), K(source_tablet_id_), K(dst_tablet_id));
            } else if (OB_FAIL(ls_handle.get_ls()->build_tablet_with_batch_tables(dst_tablet_id, param))) {
              LOG_WARN("failed to update tablet table store", K(ret), K(dst_tablet_id), K(param));
            }
          }
          FLOG_INFO("DEBUG CODE, CHANGE TO TRACE LATER", K(ret), K(source_tablet_id_), K(dst_tablet_id), K(merge_type), K(param));
        }
      }
    }
  }
  return ret;
}
#endif

int ObSplitFinishTask::init()
{
  int ret = OB_SUCCESS;
  is_inited_ = true;
  return ret;
}

int ObSplitFinishTask::process()
{
  int ret = OB_SUCCESS;
  ObDagType::ObDagTypeEnum dag_type;
  ObIDag *this_dag = get_dag();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == this_dag
      || !is_data_split_dag(this_dag->get_type()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), KPC(this_dag));
  } else if (OB_FAIL(static_cast<ObIDataSplitDag *>(this_dag)->report_replica_build_status())) {
    LOG_WARN("report replica build status failed", K(ret),
      "complement_data_ret", static_cast<ObIDataSplitDag *>(this_dag)->get_complement_data_ret());
  }
  ret = OB_SUCCESS;
  return ret;
}


ObRowScan::ObRowScan() : is_inited_(false), row_iter_(nullptr), ctx_(),
    access_ctx_(), rowkey_read_info_(nullptr), access_param_(), allocator_("SplitScanRow")
{}

ObRowScan::~ObRowScan()
{
  if (OB_NOT_NULL(row_iter_)) {
    row_iter_->~ObSSTableRowWholeScanner();
    row_iter_ = nullptr;
  }
  if (nullptr != rowkey_read_info_) {
    rowkey_read_info_->~ObRowkeyReadInfo();
    allocator_.free(rowkey_read_info_);
    rowkey_read_info_ = nullptr;
  }
  allocator_.reset();
}

//construct table access param
int ObRowScan::construct_access_param(
    const ObSplitScanParam &param)
{
  int ret = OB_SUCCESS;
  ObTabletTableIterator table_iter;
  ObLSService *ls_service = nullptr;
  ObLSHandle ls_handle;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(param));
  } else if (OB_FAIL(build_rowkey_read_info(param))) {
    LOG_WARN("build rowkey read info failed", K(ret), K(param));
  } else if (OB_FAIL(access_param_.init_merge_param(
        param.table_id_, param.src_tablet_.get_tablet_meta().tablet_id_, *rowkey_read_info_, false/*is_multi_version_minor_merge*/, false/*is_delete_insert*/))) {
    LOG_WARN("init table access param failed", K(ret), KPC(rowkey_read_info_), K(param));
  }
  LOG_INFO("construct table access param finished", K(ret), K(access_param_));
  return ret;
}

// construct version range and ctx
int ObRowScan::construct_access_ctx(
      const share::ObLSID &ls_id,
      const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  const int64_t snapshot_version = INT64_MAX;
  ObQueryFlag query_flag(ObQueryFlag::Forward,
                         true, /*is daily merge scan*/
                         true, /*is read multiple macro block*/
                         true, /*sys task scan, read one macro block in single io*/
                         false /*is full row scan?*/,
                         false,
                         false);
  query_flag.is_bare_row_scan_ = true; // output mult-version rows without do_compact.
  common::ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = snapshot_version;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 0;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(ls_id));
  } else if (OB_FAIL(ctx_.init_for_read(ls_id,
                                        tablet_id,
                                        INT64_MAX,
                                        -1,
                                        SCN::max_scn()))) {
    LOG_WARN("fail to init store ctx", K(ret), K(ls_id));
  } else if (OB_FAIL(access_ctx_.init(query_flag,
                                      ctx_,
                                      allocator_,
                                      allocator_,
                                      trans_version_range))) {
    LOG_WARN("fail to init accesss ctx", K(ret));
  }
  LOG_INFO("construct access ctx finished", K(ret), K(access_ctx_));
  return ret;
}

int ObRowScan::build_rowkey_read_info(
    const ObSplitScanParam &param)
{
  int ret = OB_SUCCESS;
  int64_t full_stored_col_cnt = 0;
  ObSEArray<share::schema::ObColDesc, 16> cols_desc;
  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(param));
  } else if (OB_FAIL(param.storage_schema_->get_mulit_version_rowkey_column_ids(cols_desc))) {
    LOG_WARN("fail to get rowkey column ids", K(ret), KPC(param.storage_schema_));
  } else if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator_, rowkey_read_info_))) {
    LOG_WARN("fail to allocate and new rowkey read info", K(ret));
  } else if (OB_FAIL(param.storage_schema_->get_store_column_count(full_stored_col_cnt, true/*full col*/))) {
    LOG_WARN("failed to get store column count", K(ret), KPC(param.storage_schema_));
  } else if (OB_FAIL(rowkey_read_info_->init(allocator_,
                                             full_stored_col_cnt,
                                             param.storage_schema_->get_rowkey_column_num(),
                                             param.storage_schema_->is_oracle_mode(),
                                             cols_desc,
                                             false /*is_cg_sstable*/,
                                             false /*use_default_compat_version*/,
                                             false/*is_cs_replica_compat*/))) {
    LOG_WARN("fail to init rowkey read info", K(ret), KPC(param.storage_schema_));
  }
  if (OB_FAIL(ret) && nullptr != rowkey_read_info_) {
    rowkey_read_info_->~ObRowkeyReadInfo();
    allocator_.free(rowkey_read_info_);
    rowkey_read_info_ = nullptr;
  }
  return ret;
}

int ObRowScan::init(
    const ObSplitScanParam &param,
    ObSSTable &sstable)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid() || !sstable.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(param), K(sstable));
  } else if (OB_FAIL(construct_access_param(param))) {
    LOG_WARN("construct access param failed", K(ret), K(param));
  } else if (OB_FAIL(construct_access_ctx(param.src_tablet_.get_tablet_meta().ls_id_, param.src_tablet_.get_tablet_meta().tablet_id_))) {
    LOG_WARN("construct access param failed", K(ret), K(param));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObSSTableRowWholeScanner)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc mem failed", K(ret));
  } else if (FALSE_IT(row_iter_ = new(buf)ObSSTableRowWholeScanner())) {
  } else if (OB_FAIL(row_iter_->init(access_param_.iter_param_,
                                     access_ctx_,
                                     &sstable,
                                     param.query_range_))) {
    LOG_WARN("construct iterator failed", K(ret));
  } else {
    is_inited_ = true;
  }
  if (OB_FAIL(ret)) {
    if (nullptr != row_iter_) {
      row_iter_->~ObSSTableRowWholeScanner();
      row_iter_ = nullptr;
    }
    if (nullptr != buf) {
      allocator_.free(buf);
      buf = nullptr;
    }
  }
  return ret;
}

int ObRowScan::init(
    const ObSplitScanParam &param,
    const blocksstable::ObMacroBlockDesc &macro_desc,
    ObSSTable &sstable)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid() || !macro_desc.is_valid() || !sstable.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(param), K(macro_desc), K(sstable));
  } else if (OB_FAIL(construct_access_param(param))) {
    LOG_WARN("construct access param failed", K(ret), K(param));
  } else if (OB_FAIL(construct_access_ctx(param.src_tablet_.get_tablet_meta().ls_id_, param.src_tablet_.get_tablet_meta().tablet_id_))) {
    LOG_WARN("construct access param failed", K(ret), K(param));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObSSTableRowWholeScanner)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc mem failed", K(ret));
  } else if (FALSE_IT(row_iter_ = new(buf)ObSSTableRowWholeScanner())) {
  } else if (OB_FAIL(row_iter_->open(access_param_.iter_param_,
     access_ctx_, *param.query_range_, macro_desc, sstable))) {
    LOG_WARN("constrtuct stored row iterator failed", K(ret), K(access_param_), K(access_ctx_), K(macro_desc), K(sstable));
  } else {
    is_inited_ = true;
  }
  if (OB_FAIL(ret)) {
    if (nullptr != row_iter_) {
      row_iter_->~ObSSTableRowWholeScanner();
      row_iter_ = nullptr;
    }
    if (nullptr != buf) {
      allocator_.free(buf);
      buf = nullptr;
    }
  }
  return ret;
}

int ObRowScan::get_next_row(const ObDatumRow *&tmp_row)
{
  int ret = OB_SUCCESS;
  tmp_row = nullptr;
  const ObDatumRow *row = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(row_iter_->get_next_row(row))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to get next row", K(ret));
    }
  } else if (OB_UNLIKELY(nullptr == row || !row->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected datum row", K(ret), KPC(row));
  } else {
    tmp_row = row;
  }
  return ret;
}

ObSnapshotRowScan::ObSnapshotRowScan() : is_inited_(false), allocator_("SplitSnapScan"), snapshot_version_(0),
  range_(), read_info_(), write_row_(), out_cols_projector_(), access_param_(), ctx_(), access_ctx_(), get_table_param_(), scan_merge_(nullptr)
{
  reset();
}

ObSnapshotRowScan::~ObSnapshotRowScan()
{
  reset();
}

void ObSnapshotRowScan::reset()
{
  is_inited_ = false;
  if (OB_NOT_NULL(scan_merge_)) {
    scan_merge_->~ObMultipleScanMerge();
    allocator_.free(scan_merge_);
    scan_merge_ = nullptr;
  }
  get_table_param_.reset();
  access_ctx_.reset();
  ctx_.reset();
  access_param_.reset();
  out_cols_projector_.reset();
  write_row_.reset();
  read_info_.reset();
  range_.reset();
  snapshot_version_ = 0;
  allocator_.reset();
}

int ObSnapshotRowScan::init(
    const ObSplitScanParam &param,
    const ObIArray<ObColDesc> &schema_store_col_descs,
    const int64_t schema_column_cnt,
    const int64_t schema_rowkey_cnt,
    const bool is_oracle_mode,
    const ObTabletHandle &tablet_handle,
    const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    const ObTabletID &tablet_id = param.src_tablet_.get_tablet_meta().tablet_id_;
    const ObLSID &ls_id = param.src_tablet_.get_tablet_meta().ls_id_;
    ObQueryFlag query_flag(ObQueryFlag::Forward,
        false, /* daily merge*/
        true,  /* use *optimize */
        true,  /* use whole macro scan*/
        false, /* not full row*/
        false, /* not index_back*/
        false);/* query stat */
    query_flag.disable_cache();
    ObTabletTableIterator table_iter;
    snapshot_version_ = snapshot_version;
    schema_rowkey_cnt_ = schema_rowkey_cnt;
    range_.set_whole_range();
    if (OB_FAIL(table_iter.set_tablet_handle(tablet_handle))) {
      LOG_WARN("failed to set tablet handle", K(ret));
    } else if (OB_FAIL(table_iter.refresh_read_tables_from_tablet(snapshot_version,
                                                                  false/*allow_no_ready_read*/,
                                                                  false/*major_sstable_only*/,
                                                                  false/*need_split_src_table*/,
                                                                  false/*need_split_dst_table*/))) {
      LOG_WARN("failed to get read tables", K(ret), K(param));
    } else if (OB_FAIL(read_info_.init(allocator_,
                                       schema_column_cnt,
                                       schema_rowkey_cnt,
                                       is_oracle_mode,
                                       schema_store_col_descs,
                                       nullptr/*storage_cols_index*/))) {
      LOG_WARN("failed to init read info", K(ret), K(schema_column_cnt), K(schema_rowkey_cnt), K(schema_store_col_descs));
    } else if (OB_FAIL(write_row_.init(allocator_, read_info_.get_columns_desc().count() + storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt()))) {
      LOG_WARN("Fail to init write row", K(ret));
    } else if (OB_FALSE_IT(write_row_.row_flag_.set_flag(ObDmlFlag::DF_INSERT))) {
    } else if (OB_FAIL(construct_access_param(param.table_id_, tablet_id, read_info_))) {
      LOG_WARN("failed to init access param", K(ret), K(param));
    } else if (OB_FAIL(construct_range_ctx(query_flag, ls_id))) {
      LOG_WARN("failed to init access ctx", K(ret));
    } else if (OB_FAIL(construct_multiple_scan_merge(table_iter, range_))) {
      LOG_WARN("failed to init scan merge", K(ret), K(table_iter));
    } else {
      is_inited_ = true;
    }
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObSnapshotRowScan::construct_access_param(
    const uint64_t table_id,
    const common::ObTabletID &tablet_id,
    const ObITableReadInfo &read_info)
{
  int ret = OB_SUCCESS;
  const int64_t column_cnt = read_info.get_columns_desc().count();
  out_cols_projector_.reset();
  if (OB_FAIL(out_cols_projector_.prepare_allocate(column_cnt, 0))) {
    LOG_WARN("failed to prepare allocate", K(ret), K(column_cnt));
  } else {
    for (int64_t i = 0; i < out_cols_projector_.count(); i++) {
      out_cols_projector_[i] = i;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(access_param_.init_merge_param(table_id,
                                                    tablet_id,
                                                    read_info,
                                                    false/*is_multi_version_minor_merge*/,
                                                    false/*is_delete_insert*/))) {
    LOG_WARN("failed to init access param", K(ret));
  } else {
    access_param_.iter_param_.out_cols_project_ = &out_cols_projector_;
  }
  return ret;
}

int ObSnapshotRowScan::construct_range_ctx(
    ObQueryFlag &query_flag,
    const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  common::ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = snapshot_version_;
  trans_version_range.multi_version_start_ = snapshot_version_;
  trans_version_range.base_version_ = 0;
  SCN tmp_scn;
  if (OB_FAIL(tmp_scn.convert_for_tx(snapshot_version_))) {
    LOG_WARN("convert fail", K(ret), K(ls_id), K_(snapshot_version));
  } else if (OB_FAIL(ctx_.init_for_read(ls_id,
                                        access_param_.iter_param_.tablet_id_,
                                        INT64_MAX,
                                        -1,
                                        tmp_scn))) {
    LOG_WARN("fail to init store ctx", K(ret), K(ls_id));
  } else if (OB_FAIL(access_ctx_.init(query_flag, ctx_, allocator_, allocator_, trans_version_range))) {
    LOG_WARN("fail to init accesss ctx", K(ret));
  } else if (OB_NOT_NULL(access_ctx_.lob_locator_helper_)) {
    access_ctx_.lob_locator_helper_->update_lob_locator_ctx(access_param_.iter_param_.table_id_,
                                                            access_param_.iter_param_.tablet_id_.id(),
                                                            0/*tx_id*/);
  }
  return ret;
}

int ObSnapshotRowScan::construct_multiple_scan_merge(
    const ObTabletTableIterator &table_iter,
    const ObDatumRange &range)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_FAIL(get_table_param_.tablet_iter_.assign(table_iter))) {
    LOG_WARN("fail to assign tablet iterator", K(ret));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObMultipleScanMerge)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObMultipleScanMerge", K(ret));
  } else if (FALSE_IT(scan_merge_ = new(buf)ObMultipleScanMerge())) {
  } else if (OB_FAIL(scan_merge_->init(access_param_, access_ctx_, get_table_param_))) {
    LOG_WARN("fail to init scan merge", K(ret), K(access_param_), K(access_ctx_));
  } else if (OB_FAIL(scan_merge_->open(range))) {
    LOG_WARN("fail to open scan merge", K(ret), K(access_param_), K(access_ctx_), K(range));
  } else {
    scan_merge_->disable_padding();
    scan_merge_->disable_fill_virtual_column();
  }
  return ret;
}

int ObSnapshotRowScan::add_extra_rowkey(const ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  const int64_t rowkey_column_count = schema_rowkey_cnt_;
  const int64_t extra_rowkey_cnt = storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  if (OB_UNLIKELY(write_row_.get_capacity() < row.count_ + extra_rowkey_cnt ||
                  row.count_ < rowkey_column_count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected row", K(ret), K(write_row_), K(row.count_), K(rowkey_column_count));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < row.count_; i++) {
      if (i < rowkey_column_count) {
        write_row_.storage_datums_[i] = row.storage_datums_[i];
      } else {
        write_row_.storage_datums_[i + extra_rowkey_cnt] = row.storage_datums_[i];
      }
    }
    write_row_.storage_datums_[rowkey_column_count].set_int(-snapshot_version_);
    write_row_.storage_datums_[rowkey_column_count + 1].set_int(0);
    write_row_.count_ = row.count_ + extra_rowkey_cnt;
  }
  return ret;
}

int ObSnapshotRowScan::get_next_row(const ObDatumRow *&out_row)
{
  int ret = OB_SUCCESS;
  out_row = nullptr;
  ObDatumRow *row = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(scan_merge_->get_next_row(row))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to get next row", K(ret));
    }
  } else if (OB_UNLIKELY(nullptr == row || !row->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected datum row", K(ret), KPC(row));
  } else if (OB_FAIL(add_extra_rowkey(*row))) {
    LOG_WARN("failed to add extra rowkey", K(ret));
  } else {
    out_row = static_cast<const ObDatumRow *>(&write_row_);
  }
  return ret;
}

ObUncommittedRowScan::ObUncommittedRowScan()
  : row_scan_(), row_scan_end_(false), next_row_(nullptr), major_snapshot_version_(OB_INVALID_TIMESTAMP), trans_version_col_idx_(0), row_queue_(), row_queue_allocator_(), row_queue_has_unskippable_row_(false)
{
}

ObUncommittedRowScan::~ObUncommittedRowScan()
{
}

int ObUncommittedRowScan::init(
    const ObSplitScanParam param,
    ObSSTable &src_sstable,
    const int64_t major_snapshot_version,
    const int64_t schema_column_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(row_scan_.init(param, src_sstable))) {
    LOG_WARN("failed to init", K(ret));
  } else {
    row_scan_end_ = false;
    next_row_ = nullptr;
    major_snapshot_version_ = major_snapshot_version;
    trans_version_col_idx_ = ObMultiVersionRowkeyHelpper::get_trans_version_col_store_index(
        row_scan_.get_rowkey_read_info()->get_schema_rowkey_count(), true);
    if (OB_FAIL(row_queue_.init(schema_column_cnt + storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt()))) {
      LOG_WARN("failed to init row queue", K(ret));
    } else {
      row_queue_allocator_.reset();
      row_queue_has_unskippable_row_ = false;
    }
  }
  return ret;
}

int ObUncommittedRowScan::get_next_row(const ObDatumRow *&res_row)
{
  int ret = OB_SUCCESS;
  const ObDatumRow *row = nullptr;
  res_row = nullptr;
  while (OB_SUCC(ret) && !row_queue_.has_next()) {
    if (OB_FAIL(get_next_rowkey_rows())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next rowkey rows", K(ret));
      }
    } else if (!row_queue_has_unskippable_row_) {
      row_queue_reuse();
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(row_queue_.get_next_row(row))) {
    LOG_WARN("failed to get next row", K(ret));
  } else {
    res_row = row;
  }
  return ret;
};

int ObUncommittedRowScan::get_next_rowkey_rows()
{
  int ret = OB_SUCCESS;
  const ObDatumRow *row = nullptr;
  if (OB_UNLIKELY(row_queue_.has_next())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("previous rowkey rows still exist", K(ret));
  }

  // collect rows from next_row_ to row_queue_
  if (OB_SUCC(ret) && OB_NOT_NULL(next_row_)) {
    if (OB_FAIL(row_queue_add(*next_row_))) {
      LOG_WARN("failed to add row", K(ret));
    } else {
      next_row_ = nullptr;
    }
  }

  // collect rows from row_scan_ to row_queue_ or next_row_
  while (OB_SUCC(ret) && !row_scan_end_ && nullptr == next_row_) {
    if (OB_FAIL(row_scan_.get_next_row(row))) {
      if (OB_ITER_END == ret) {
        row_scan_end_ = true;
        ret = OB_SUCCESS;
      }
    } else if (OB_ISNULL(row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid row", K(ret));
    } else if (OB_FAIL(row_queue_add(*row))) {
      if (OB_SIZE_OVERFLOW == ret) {
        next_row_ = row;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to add row", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && !row_queue_.has_next()) {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObUncommittedRowScan::row_queue_add(const ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  bool rowkey_changed = false;
  if (row_queue_.has_next()) {
    const int64_t schema_rowkey_cnt = row_scan_.get_rowkey_read_info()->get_schema_rowkey_count();
    const blocksstable::ObStorageDatumUtils &datum_utils = row_scan_.get_rowkey_read_info()->get_datum_utils();
    const ObDatumRow *last_row_in_queue = row_queue_.get_last();
    ObDatumRowkey cur_key;
    ObDatumRowkey last_key;
    int compare_result = 0;
    if (OB_ISNULL(last_row_in_queue)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected last row is nullptr", K(ret), K(row_queue_));
    } else if (OB_FAIL(last_key.assign(last_row_in_queue->storage_datums_, schema_rowkey_cnt))) {
      LOG_WARN("failed to assign qu rowkey", K(ret));
    } else if (OB_FAIL(cur_key.assign(row.storage_datums_, schema_rowkey_cnt))) {
      LOG_WARN("failed to assign cur key", K(ret));
    } else if (OB_FAIL(cur_key.compare(last_key, datum_utils, compare_result))) {
      LOG_WARN("failed to compare last key", K(ret), K(cur_key), K(last_key));
    } else if (OB_UNLIKELY(compare_result < 0)) {
      ret = OB_ROWKEY_ORDER_ERROR;
      LOG_ERROR("input rowkey is less then last rowkey", K(ret), K(cur_key), K(last_key), K(ret));
    } else if (compare_result > 0) {
      rowkey_changed = true;
    }
  }

  if (OB_SUCC(ret) && !rowkey_changed) {
    bool can_skip = false;
    if (OB_FAIL(row_queue_.add_row(row, row_queue_allocator_))) {
      LOG_WARN("failed to add row", K(ret));
    } else if (OB_FAIL(check_can_skip(row, can_skip))) {
      LOG_WARN("failed to check can skip", K(ret));
    } else if (!can_skip) {
      row_queue_has_unskippable_row_ = true;
    }
  }

  if (OB_SUCC(ret) && rowkey_changed) {
    ret = OB_SIZE_OVERFLOW;
  } else if (OB_SIZE_OVERFLOW == ret) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("change errcode", K(ret), "real_ret", OB_SIZE_OVERFLOW);
  }
  return ret;
}

void ObUncommittedRowScan::row_queue_reuse()
{
  row_queue_.reuse();
  row_queue_allocator_.reuse();
  row_queue_has_unskippable_row_ = false;
  return;
}

int ObUncommittedRowScan::check_can_skip(const ObDatumRow &row, bool &can_skip)
{
  int ret = OB_SUCCESS;
  SCN scn_commit_trans_version = SCN::max_scn();
  int64_t commit_version = OB_INVALID_VERSION;
  if (OB_INVALID_TIMESTAMP == major_snapshot_version_) {
    can_skip = false;
  } else if (row.mvcc_row_flag_.is_uncommitted_row()) {
    SCN major_snapshot_scn;
    storage::ObTxTableGuards &tx_table_guards = row_scan_.get_tx_table_guards();
    bool can_read = false;
    int64_t state = ObTxData::MAX_STATE_CNT;
    const transaction::ObTransID &read_trans_id = row.trans_id_;
    if (OB_FAIL(major_snapshot_scn.convert_for_tx(major_snapshot_version_))) {
      LOG_WARN("failed to convert major snapshot version", K(ret), K(major_snapshot_version_));
    } else if (OB_FAIL(tx_table_guards.get_tx_state_with_scn(
        read_trans_id, major_snapshot_scn, state, scn_commit_trans_version))) {
      LOG_WARN("get transaction status failed", K(ret), K(read_trans_id), K(state));
    } else if (ObTxData::RUNNING == state) {
      can_skip = false;
    } else if (ObTxData::COMMIT == state || ObTxData::ELR_COMMIT == state || ObTxData::ABORT == state) {
      can_skip = true;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected row state", K(ret));
    }
  } else {
    if (OB_UNLIKELY(trans_version_col_idx_ >= row.get_column_count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("trans version column index out of range", K(ret), K(trans_version_col_idx_), K(row));
    } else {
      const int64_t row_commit_version = -(row.storage_datums_[trans_version_col_idx_].get_int());
      can_skip = row_commit_version <= major_snapshot_version_;
    }
  }
  if (OB_SUCC(ret) && can_skip) {
    LOG_DEBUG("skip row", K(row));
  }
  return ret;
}

} //end namespace stroage
} //end namespace oceanbase
