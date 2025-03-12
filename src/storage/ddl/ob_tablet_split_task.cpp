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
#include "storage/ob_partition_range_spliter.h"
#include "storage/tablet/ob_mds_scan_param_helper.h"
#include "storage/tablet/ob_tablet_mds_table_mini_merger.h"
#include "storage/tablet/ob_tablet_medium_info_reader.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/access/ob_multiple_scan_merge.h"

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
      && dest_tablets_id_.count() > 0 && compaction_scn_ > 0 && user_parallelism_ > 0
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

ObTabletSplitCtx::ObTabletSplitCtx()
  : range_allocator_("SplitRangeCtx", OB_MALLOC_NORMAL_BLOCK_SIZE /*8KB*/, MTL_ID()),
    is_inited_(false), complement_data_ret_(OB_SUCCESS), ls_handle_(), tablet_handle_(),
    index_builder_map_(), clipped_schemas_map_(),
    allocator_("SplitCtx", OB_MALLOC_NORMAL_BLOCK_SIZE /*8KB*/, MTL_ID()),
    skipped_split_major_keys_(),
    row_inserted_(0), physical_row_count_(0)
{
}

ObTabletSplitCtx::~ObTabletSplitCtx()
{
  int ret = OB_SUCCESS;
  is_inited_ = false;
  complement_data_ret_ = OB_SUCCESS;
  ls_handle_.reset();
  tablet_handle_.reset();
  DESTROY_BUILT_MAP(allocator_, index_builder_map_, ObSplitSSTableTaskKey, ObSSTableIndexBuilder);
  DESTROY_BUILT_MAP(allocator_, clipped_schemas_map_, int64_t, ObStorageSchema);
  table_store_iterator_.reset();
  data_split_ranges_.reset();
  skipped_split_major_keys_.reset();
  range_allocator_.reset();
  allocator_.reset();
}

bool ObTabletSplitCtx::is_valid() const
{
  return is_inited_ && ls_handle_.is_valid() && tablet_handle_.is_valid();
}

int ObTabletSplitCtx::init(const ObTabletSplitParam &param)
{
  int ret = OB_SUCCESS;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(param));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(param.ls_id_, ls_handle_, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(param));
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle_,
    param.source_tablet_id_, tablet_handle_, ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    LOG_WARN("get tablet failed", K(ret));
  } else if (OB_FAIL(ObTabletSplitUtil::check_satisfy_split_condition(ls_handle_, tablet_handle_, param.dest_tablets_id_, param.compaction_scn_, param.min_split_start_scn_))) {
    if (OB_NEED_RETRY == ret) {
      if (REACH_COUNT_INTERVAL(1000L)) {
        LOG_WARN("wait to satisfy the data split condition", K(ret), K(param));
      }
    } else {
      LOG_WARN("check satisfy split condition failed", K(ret), K(param));
    }
  } else if (OB_FAIL(tablet_handle_.get_obj()->get_all_tables(table_store_iterator_))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_FAIL(ObTabletSplitUtil::check_sstables_skip_data_split(
      ls_handle_, table_store_iterator_, param.dest_tablets_id_, OB_INVALID_VERSION/*lob_major_snapshot*/, skipped_split_major_keys_))) {
    LOG_WARN("check sstables skip data split failed", K(ret));
  } else if (OB_FAIL(ObTabletSplitUtil::convert_rowkey_to_range(range_allocator_, param.parallel_datum_rowkey_list_, data_split_ranges_))) {
    LOG_WARN("convert to range failed", K(ret), K(param));
  }

  if (OB_SUCC(ret)) {
    complement_data_ret_ = OB_SUCCESS;
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
    if (OB_FAIL(clipped_schemas_map_.get_refactored(schema_stored_cols_cnt, target_storage_schema))) {
      void *buf = nullptr;
      target_storage_schema = nullptr;
      ObUpdateCSReplicaSchemaParam update_param;
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("get storage schema failed", K(ret), K(schema_stored_cols_cnt));
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
      } else if (OB_FAIL(clipped_schemas_map_.set_refactored(schema_stored_cols_cnt, target_storage_schema))) {
        LOG_WARN("set refactored failed", K(ret));
      } else {
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

int ObTabletSplitCtx::prepare_index_builder(
    const ObTabletSplitParam &param)
{
  int ret = OB_SUCCESS;
  const int64_t bucket_num = 16;
  ObSEArray<ObITable *, MAX_SSTABLE_CNT_IN_STORAGE> sstables;
  ObTabletSplitMdsUserData split_data;
  const ObStorageSchema *storage_schema = nullptr;
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
  } else if (OB_FAIL(ObTabletSplitUtil::get_participants(param.split_sstable_type_, table_store_iterator_, false, skipped_split_major_keys_, sstables))) {
    LOG_WARN("get participant sstables failed", K(ret));
  } else if (OB_FAIL(tablet_handle_.get_obj()->get_split_data(split_data, ObTabletCommon::DEFAULT_GET_TABLET_DURATION_10_S))) {
    LOG_WARN("failed to get split data", K(ret));
  } else if (OB_FAIL(split_data.get_storage_schema(storage_schema))) {
    LOG_WARN("failed to get storage schema", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < sstables.count(); i++) {
      ObSSTable *sstable = static_cast<ObSSTable *>(sstables.at(i));
      for (int64_t j = 0; OB_SUCC(ret) && j < param.dest_tablets_id_.count(); j++) {
        void *buf = nullptr;
        ObWholeDataStoreDesc data_desc;
        ObSSTableIndexBuilder *sstable_index_builder = nullptr;
        ObSplitSSTableTaskKey key;
        ObITable::TableKey dest_table_key = sstable->get_key();
        dest_table_key.tablet_id_ = param.dest_tablets_id_.at(j);
        key.src_sst_key_ = sstable->get_key();
        key.dest_tablet_id_ = param.dest_tablets_id_.at(j);
        ObTabletHandle tablet_handle;
        const ObMergeType merge_type = sstable->is_major_sstable() ? MAJOR_MERGE : MINOR_MERGE;
        const int64_t snapshot_version = sstable->is_major_sstable() ?
          sstable->get_snapshot_version() : sstable->get_end_scn().get_val_for_tx();
        const ObStorageSchema *clipped_storage_schema = nullptr;
        if (OB_FAIL(get_clipped_storage_schema_on_demand(
            param.source_tablet_id_, *sstable, *storage_schema,
            true/*try_create*/, clipped_storage_schema))) {
          LOG_WARN("get storage schema via sstable failed", K(ret));
        } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle_, param.dest_tablets_id_.at(j), tablet_handle))) {
          LOG_WARN("get tablet failed", K(ret));
        } else if (OB_FAIL(ObTabletDDLUtil::prepare_index_data_desc(*tablet_handle.get_obj(),
            dest_table_key, snapshot_version, param.data_format_version_,
            nullptr/*first_ddl_sstable*/, clipped_storage_schema, data_desc))) {
          LOG_WARN("prepare index data desc failed", K(ret));
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
  : ObIDag(ObDagType::DAG_TYPE_TABLET_SPLIT), is_inited_(false), param_(), context_()
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

int ObTabletSplitDag::create_first_task()
{
  int ret = OB_SUCCESS;
  int64_t task_id = 0;
  ObSEArray<ObITable *, MAX_SSTABLE_CNT_IN_STORAGE> source_sstables;
  ObTabletSplitPrepareTask *prepare_task = nullptr;
  ObTabletSplitMergeTask *merge_task = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
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
  } else if (OB_FAIL(add_task(*merge_task))) {
    LOG_WARN("add task failed", K(ret));
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
      } else if (OB_FAIL(write_task->init(task_id++, param_, context_, source_sstables.at(i)))) {
        LOG_WARN("init write task failed", K(ret));
      } else if (OB_FAIL(prepare_task->add_child(*write_task))) {
        LOG_WARN("add child task failed", K(ret));
      } else if (OB_FAIL(add_task(*write_task))) {
        LOG_WARN("add task failed", K(ret));
      } else if (OB_FAIL(write_task->add_child(*merge_task))) {
        LOG_WARN("add child task failed", K(ret));
      }
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
  }
  FLOG_INFO("create first task finish", K(ret),
    "can_reuse_macro_block", param_.can_reuse_macro_block_, "sstables_count", source_sstables.count(), K(param_), K(context_));
  return ret;
}

int64_t ObTabletSplitDag::hash() const
{
  int ret = OB_SUCCESS;
  int64_t hash_val = 0;
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

int ObTabletSplitDag::report_replica_build_status()
{
  int ret = OB_SUCCESS;
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
    SERVER_EVENT_ADD("ddl", "replica_split_resp",
        "result", context_.complement_data_ret_,
        "tenant_id", param_.tenant_id_,
        "source_tablet_id", param_.source_tablet_id_.id(),
        "svr_addr", GCTX.self_addr(),
        "physical_row_count", context_.physical_row_count_,
        "split_total_rows", context_.row_inserted_,
        *ObCurTraceId::get_trace_id());
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
  } else if (OB_FAIL(ObTabletSplitUtil::check_data_split_finished(param_->ls_id_, param_->dest_tablets_id_, is_data_split_finished))) {
    LOG_WARN("check all major exist failed", K(ret));
  } else if (is_data_split_finished) {
    LOG_INFO("split task has alreay finished", KPC(param_));
  } else if (OB_FAIL(prepare_context())) {
    LOG_WARN("prepare index builder map failed", K(ret), KPC(param_));
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

int ObTabletSplitWriteTask::prepare_context(
    ObTabletSplitMdsUserData &split_data,
    const ObStorageSchema *&clipped_storage_schema)
{
  int ret = OB_SUCCESS;
  split_data.reset();
  clipped_storage_schema = nullptr;
  ObArenaAllocator tmp_arena("TmpInitSplitW");
  int64_t column_cnt = 0;
  const ObStorageSchema *storage_schema = nullptr;
  ObDatumRow tmp_default_row;
  ObArray<ObColDesc> multi_version_cols_desc;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(context_->tablet_handle_.get_obj()->get_split_data(split_data, ObTabletCommon::DEFAULT_GET_TABLET_DURATION_10_S))) {
    LOG_WARN("failed to get split data", K(ret));
  } else if (OB_FAIL(split_data.get_storage_schema(storage_schema))) {
    LOG_WARN("failed to get storage schema", K(ret));
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
  ObTabletSplitMdsUserData split_data;
  const ObStorageSchema *clipped_storage_schema = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_SUCCESS != (context_->complement_data_ret_)) {
    LOG_WARN("complement data has already failed", KPC(context_));
  } else if (OB_FAIL(ObTabletSplitUtil::check_data_split_finished(param_->ls_id_, param_->dest_tablets_id_, is_data_split_finished))) {
    LOG_WARN("check all major exist failed", K(ret));
  } else if (is_data_split_finished) {
    LOG_INFO("split task has alreay finished", KPC(param_));
  } else if (OB_FAIL(prepare_context(split_data/*keep_life_of_split_data_schema*/,
      clipped_storage_schema))) {
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

int ObTabletSplitWriteTask::prepare_macro_block_writer(
    const ObStorageSchema &clipped_storage_schema,
    ObIArray<ObWholeDataStoreDesc> &data_desc_arr,
    ObIArray<ObMacroBlockWriter *> &macro_block_writer_arr)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObMacroDataSeq macro_start_seq(0);
  ObSSTableMetaHandle meta_handle;
  meta_handle.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(param_->ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("get ls failed", K(ret), KPC(param_));
  } else if (OB_FAIL(sstable_->get_meta(meta_handle))) {
    LOG_WARN("get sstable meta failed", K(ret));
  } else if (OB_FAIL(macro_start_seq.set_sstable_seq(meta_handle.get_sstable_meta().get_sstable_seq()))) {
    LOG_WARN("set sstable logical seq failed", K(ret), "sst_meta", meta_handle.get_sstable_meta());
  } else if (OB_FAIL(macro_start_seq.set_parallel_degree(task_id_))) {
    LOG_WARN("set parallel degree failed", K(ret));
  } else {
    ObMacroSeqParam macro_seq_param;
    macro_seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
    macro_seq_param.start_ = macro_start_seq.macro_data_seq_;
    const bool micro_index_clustered = context_->tablet_handle_.get_obj()->get_tablet_meta().micro_index_clustered_;
    for (int64_t i = 0; OB_SUCC(ret) && i < param_->dest_tablets_id_.count(); i++) {
      ObPreWarmerParam pre_warm_param;
      ObSSTablePrivateObjectCleaner *object_cleaner = nullptr;
      void *buf = nullptr;
      ObWholeDataStoreDesc data_desc;
      ObTabletHandle tablet_handle;
      ObMacroBlockWriter *macro_block_writer = nullptr;
      ObSSTableIndexBuilder *sst_idx_builder = nullptr;
      const ObTabletID &dst_tablet_id = param_->dest_tablets_id_.at(i);
      ObSplitSSTableTaskKey task_key(sstable_->get_key(), dst_tablet_id);
      const ObMergeType merge_type = sstable_->is_major_sstable() ? MAJOR_MERGE : MINOR_MERGE;
      const int64_t snapshot_version = sstable_->is_major_sstable() ?
          sstable_->get_snapshot_version() : sstable_->get_end_scn().get_val_for_tx();
      if (OB_FAIL(context_->index_builder_map_.get_refactored(task_key, sst_idx_builder))) {
        LOG_WARN("get refactored failed", K(ret));
      } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle, dst_tablet_id, tablet_handle))) {
        LOG_WARN("get tablet failed", K(ret));
      } else if (OB_FAIL(data_desc.init(true/*is_ddl*/, clipped_storage_schema,
                                        param_->ls_id_,
                                        dst_tablet_id,
                                        merge_type,
                                        snapshot_version,
                                        param_->data_format_version_,
                                        micro_index_clustered,
                                        tablet_handle.get_obj()->get_transfer_seq(),
                                        sstable_->get_end_scn()))) {
        LOG_WARN("fail to init data store desc", K(ret), K(dst_tablet_id), KPC(param_));
      } else if (FALSE_IT(data_desc.get_desc().sstable_index_builder_ = sst_idx_builder)) {
      } else if (FALSE_IT(data_desc.get_static_desc().is_ddl_ = true)) {
      } else if (OB_FAIL(data_desc_arr.push_back(data_desc))) {  // copy_and_assign.
        LOG_WARN("push back data store desc failed", K(ret));
      } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObMacroBlockWriter)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc mem failed", K(ret));
      } else if (FALSE_IT(macro_block_writer = new (buf) ObMacroBlockWriter())) {
      } else if (OB_FAIL(pre_warm_param.init(param_->ls_id_, dst_tablet_id))) {
        LOG_WARN("failed to init pre warm param", K(ret), K(dst_tablet_id), KPC(param_));
      } else if (OB_FAIL(ObSSTablePrivateObjectCleaner::get_cleaner_from_data_store_desc(
                                 data_desc.get_desc(),
                                 object_cleaner))) {
        LOG_WARN("failed to get cleaner from data store desc", K(ret));
      } else if (OB_FAIL(macro_block_writer->open(data_desc_arr.at(i).get_desc(), macro_start_seq.get_parallel_idx(),
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
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(check_and_cast_high_bound(rowkey_read_info_->get_columns_desc(), tablet_bound_arr))) {
      LOG_WARN("failed to check and cast high bound", K(ret), K(tablet_bound_arr), K(rowkey_read_info_->get_columns_desc()));
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
      whole_range.set_whole_range();
      const bool is_small_sstable = sstable_->is_small_sstable();
      if (OB_FAIL(meta_iter.open(
        *sstable_, whole_range, *rowkey_read_info_, allocator_))) {
        LOG_WARN("open dual macro meta iter failed", K(ret), K(*sstable_));
      } else {
        while (OB_SUCC(ret)) {
          ObDataMacroBlockMeta macro_meta;
          ObMacroBlockDesc data_macro_desc;
          data_macro_desc.reset();
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
              LOG_INFO("append row successfully", "tablet_id", tablet_bound_arr.at(dest_tablet_index).first,
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

int ObTabletSplitWriteTask::check_and_cast_high_bound(
    const common::ObIArray<ObColDesc> &col_descs,
    common::ObSArray<TabletBoundPair> &tablet_bound_arr)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_bound_arr.count(); ++i) {
    ObDatumRowkey &drowkey = tablet_bound_arr.at(i).second;
    for (int64_t j = 0; OB_SUCC(ret) && j < drowkey.datum_cnt_; ++j) {
      ObStorageDatum &storage_datum = drowkey.datums_[j];
      if (j >= col_descs.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rowkey size should equal to the size of col_parameters", K(j), K(drowkey.datums_), K(col_descs));
      } else {
        const ObColDesc &col_desc = col_descs.at(j);
        void *buf = nullptr;
        if (col_desc.col_type_.is_timestamp_ltz()) {
          /*need to convert the high bound from timestamp_tz to timestamp_ltz*/
          if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObOTimestampTinyData)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocat memory for storage datum", K(ret));
          } else {
            const ObOTimestampData & ts_data = storage_datum.get_otimestamp_tz();
            ObObj *timestamp_ltz_obj = new (buf) ObObj();
            timestamp_ltz_obj->set_timestamp_ltz(ts_data);
            storage_datum.reuse();
            if (OB_FAIL(storage_datum.from_obj_enhance(*timestamp_ltz_obj))) {
              LOG_WARN("failed to from obj", K(ret), K(*timestamp_ltz_obj));
            }
          }
        }
      }
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
  } else if (OB_FAIL(ObTabletSplitUtil::check_data_split_finished(param_->ls_id_, param_->dest_tablets_id_, is_data_split_finished))) {
    LOG_WARN("check all major exist failed", K(ret));
  } else if (is_data_split_finished) {
    LOG_INFO("split task has alreay finished", KPC(param_));
  } else if (OB_SUCCESS != (context_->complement_data_ret_)) {
    LOG_WARN("complement data has already failed", "ret", context_->complement_data_ret_);
  } else if (share::ObSplitSSTableType::SPLIT_BOTH == param_->split_sstable_type_) {
    if (OB_FAIL(create_sstable(share::ObSplitSSTableType::SPLIT_MINOR))) {
      LOG_WARN("create sstable failed", K(ret));
    } else if (OB_FAIL(create_sstable(share::ObSplitSSTableType::SPLIT_MAJOR))) {
      LOG_WARN("create sstable failed", K(ret));
    }
  } else if (OB_FAIL(create_sstable(param_->split_sstable_type_))) {
    LOG_WARN("create sstable failed", K(ret));
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(context_)) {
    context_->complement_data_ret_ = ret;
    ret = OB_SUCCESS;
  }
  DEBUG_SYNC(AFTER_TABLET_SPLIT_MERGE_TASK);
  if (OB_NOT_NULL(dag) && OB_FAIL(dag->report_replica_build_status())) {
    // do not worry about the error code overrided here, which is useless.
    LOG_WARN("report replica build status failed", K(ret), KPC(context_));
  }
  return ret;
}

int ObTabletSplitMergeTask::create_sstable(
    const share::ObSplitSSTableType &split_sstable_type)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObITable *, MAX_SSTABLE_CNT_IN_STORAGE> participants;
  common::ObArenaAllocator build_mds_arena("SplitBuildMds", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(share::ObSplitSSTableType::SPLIT_MAJOR != split_sstable_type
      && share::ObSplitSSTableType::SPLIT_MINOR != split_sstable_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(split_sstable_type), KPC(param_));
  } else if (OB_FAIL(ObTabletSplitUtil::get_participants(split_sstable_type, context_->table_store_iterator_, false/*is_table_restore*/,
      context_->skipped_split_major_keys_, participants))) {
    LOG_WARN("get participants failed", K(ret));
  } else {
    const int64_t multi_version_start = context_->tablet_handle_.get_obj()->get_multi_version_start();
    const compaction::ObMergeType merge_type = share::ObSplitSSTableType::SPLIT_MINOR == split_sstable_type ?
        compaction::ObMergeType::MINOR_MERGE : compaction::ObMergeType::MAJOR_MERGE;
    for (int64_t i = 0; OB_SUCC(ret) && i < param_->dest_tablets_id_.count(); i++) {
      bool is_data_split_finished = false;
      const ObTabletID &dest_tablet_id = param_->dest_tablets_id_.at(i);
      ObSEArray<ObTabletID, 1> check_major_exist_tablets;
      if (OB_FAIL(check_major_exist_tablets.push_back(dest_tablet_id))) {
        LOG_WARN("push back failed", K(ret));
      } else if (OB_FAIL(ObTabletSplitUtil::check_data_split_finished(param_->ls_id_, check_major_exist_tablets, is_data_split_finished))) {
        LOG_WARN("check major exist failed", K(ret), K(check_major_exist_tablets), KPC(param_));
      } else if (is_data_split_finished) {
        FLOG_INFO("skip to create sstable", K(ret), K(dest_tablet_id));
      } else {
        const int64_t src_table_cnt = participants.count();
        ObTablesHandleArray batch_sstables_handle;
        for (int64_t j = 0; OB_SUCC(ret) && j < src_table_cnt; j++) { // keep destination sstable commit versions incremental.
          const ObSSTable *src_sstable = static_cast<ObSSTable *>(participants.at(j));
          if (OB_ISNULL(src_sstable)) {
            ret = OB_ERR_SYS;
            LOG_WARN("Error sys", K(ret), K(dest_tablet_id), K(participants));
          } else {
            ObSplitSSTableTaskKey key;
            key.dest_tablet_id_ = dest_tablet_id;
            key.src_sst_key_ = src_sstable->get_key();
            ObSSTableIndexBuilder *index_builder = nullptr;
            ObTabletCreateSSTableParam create_sstable_param;
            ObTableHandleV2 table_handle;
            table_handle.reset();
            if (OB_FAIL(context_->index_builder_map_.get_refactored(key, index_builder))) {
              LOG_WARN("get refactored failed", K(ret), K(key));
            } else if (OB_FAIL(build_create_sstable_param(
              *src_sstable, key.dest_tablet_id_, index_builder, create_sstable_param))) {
              LOG_WARN("build create sstable param failed", K(ret));
            } else if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable(create_sstable_param, context_->allocator_, table_handle))) {
              LOG_WARN("create sstable failed", K(ret), K(create_sstable_param));
            } else if (OB_FAIL(batch_sstables_handle.add_table(table_handle))) {
              LOG_WARN("add table failed", K(ret));
            }
          }
          // fill empty minor sstable if scn not continous
          if (OB_SUCC(ret) && j == src_table_cnt - 1 && !is_major_merge_type(merge_type)) {
            bool need_fill_empty_sstable = false;
            SCN end_scn;
            if (OB_FAIL(check_need_fill_empty_sstable(context_->ls_handle_, src_sstable->is_minor_sstable(), src_sstable->get_key(), dest_tablet_id, need_fill_empty_sstable, end_scn))) {
              LOG_WARN("failed to check need fill", K(ret));
            } else if (need_fill_empty_sstable) {
              ObTabletCreateSSTableParam create_sstable_param;
              ObTableHandleV2 table_handle;
              table_handle.reset();
              ObSSTableMetaHandle meta_handle;
              if (OB_FAIL(src_sstable->get_meta(meta_handle))) {
                LOG_WARN("get meta failed", K(ret));
              } else if (OB_FAIL(build_create_empty_sstable_param(meta_handle.get_sstable_meta().get_basic_meta(), src_sstable->get_key(), dest_tablet_id, end_scn, create_sstable_param))) {
                LOG_WARN("failed to build create empty sstable param", K(ret));
              } else if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable(create_sstable_param, context_->allocator_, table_handle))) {
                LOG_WARN("create sstable failed", K(ret), K(create_sstable_param));
              } else if (OB_FAIL(batch_sstables_handle.add_table(table_handle))) {
                LOG_WARN("add table failed", K(ret));
              }
            }
          }
        }
        if (OB_SUCC(ret) && (src_table_cnt > 0 || is_major_merge_type(merge_type))) {
          // empty major result should also need to swap tablet, to update data_split_status and restore status.
          if (OB_FAIL(ObTabletSplitMergeTask::update_table_store_with_batch_tables(
                context_->ls_handle_,
                context_->tablet_handle_,
                dest_tablet_id,
                batch_sstables_handle,
                merge_type,
                param_->can_reuse_macro_block_,
                context_->skipped_split_major_keys_))) {
            LOG_WARN("update table store with batch tables failed", K(ret), K(batch_sstables_handle), K(split_sstable_type));
          }
        }
        if (OB_SUCC(ret) && !is_major_merge_type(merge_type)) {
          // build lost mds sstable after minor merge.
          ObTableHandleV2 table_handle;
          batch_sstables_handle.reset();
          if (OB_FAIL(ObTabletSplitUtil::build_lost_medium_mds_sstable(
                build_mds_arena,
                context_->ls_handle_,
                context_->tablet_handle_,
                dest_tablet_id,
                table_handle))) {
            LOG_WARN("build lost medium mds sstable failed", K(ret), KPC(param_));
          } else if (OB_UNLIKELY(!table_handle.is_valid())) {
            LOG_INFO("no need to fill medium mds sstable", K(ret),
              "src_tablet_id", param_->source_tablet_id_, K(dest_tablet_id));
          } else if (OB_FAIL(batch_sstables_handle.add_table(table_handle))) {
            LOG_WARN("add table failed", K(ret));
          } else if (OB_FAIL(ObTabletSplitMergeTask::update_table_store_with_batch_tables(
                context_->ls_handle_,
                context_->tablet_handle_,
                dest_tablet_id,
                batch_sstables_handle,
                compaction::ObMergeType::MDS_MINI_MERGE,
                param_->can_reuse_macro_block_,
                context_->skipped_split_major_keys_))) {
            LOG_WARN("update table store with batch tables failed", K(ret), K(batch_sstables_handle));
          }
        }
      }
    }
  }
  return ret;
}

int ObTabletSplitMergeTask::build_create_sstable_param(
    const ObSSTable &src_table, // source table.
    const ObTabletID &dst_tablet_id, // dest tablet id.
    ObSSTableIndexBuilder *index_builder,
    ObTabletCreateSSTableParam &create_sstable_param)
{
  int ret = OB_SUCCESS;
  ObSSTableMetaHandle meta_handle;
  meta_handle.reset();
  ObSSTableMergeRes res;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == index_builder || !src_table.is_valid() || !dst_tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), KP(index_builder), K(src_table), K(dst_tablet_id));
  } else if (OB_FAIL(index_builder->close(res))) {
    LOG_WARN("close sstable index builder failed", K(ret));
  } else if (OB_FAIL(src_table.get_meta(meta_handle))) {
    LOG_WARN("get sstable meta failed", K(ret));
  } else {
    const ObSSTableBasicMeta &basic_meta = meta_handle.get_sstable_meta().get_basic_meta();
    if (OB_FAIL(create_sstable_param.init_for_split(dst_tablet_id, src_table.get_key(), basic_meta,
                                      param_->schema_version_, res))) {
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
    const ObLSHandle &ls_handle,
    const ObTabletHandle &src_tablet_handle,
    const ObTabletID &dst_tablet_id,
    const ObTablesHandleArray &tables_handle,
    const compaction::ObMergeType &merge_type,
    const bool can_reuse_macro_block,
    const ObIArray<ObITable::TableKey> &skipped_split_major_keys)
{
  int ret = OB_SUCCESS;
  ObBatchUpdateTableStoreParam param;
  param.reset();
  ObSEArray<ObITable *, MAX_SSTABLE_CNT_IN_STORAGE> batch_tables;
  if (OB_UNLIKELY(!ls_handle.is_valid()
      || !src_tablet_handle.is_valid()
      || !dst_tablet_id.is_valid()
      || !is_valid_merge_type(merge_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(ls_handle), K(src_tablet_handle),
      K(dst_tablet_id), K(merge_type));
  } else if (OB_FAIL(tables_handle.get_tables(batch_tables))) {
    LOG_WARN("get batch sstables failed", K(ret));
  } else if (!is_major_merge_type(merge_type)) { // minor merge or mds mini merge.
    if (OB_FAIL(param.tables_handle_.assign(tables_handle))) {
      LOG_WARN("assign failed", K(ret), K(batch_tables));
    }
  } else {
    // ATTENTION, Meta major sstable should be placed at the end of the array.
    ObTableHandleV2 meta_major_handle;
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_tables.count(); i++) {
      const ObITable *table = batch_tables.at(i);
      ObTableHandleV2 table_handle;
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error", K(ret), K(i), K(batch_tables));
      } else if (OB_UNLIKELY(table->is_meta_major_sstable())) {
        if (OB_UNLIKELY(meta_major_handle.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("more than 1 meta major", K(ret), K(meta_major_handle), K(batch_tables));
        } else if (OB_FAIL(tables_handle.get_table(i, meta_major_handle))) {
          LOG_WARN("get handle failed", K(ret), K(i), K(batch_tables));
        }
      } else if (OB_FAIL(tables_handle.get_table(i, table_handle))) {
        LOG_WARN("get handle failed", K(ret));
      } else if (OB_FAIL(param.tables_handle_.add_table(table_handle))) {
        LOG_WARN("add table failed", K(ret));
      }
    }
    if (OB_SUCC(ret) && meta_major_handle.is_valid()) {
      if (OB_FAIL(param.tables_handle_.add_table(meta_major_handle))) {
        LOG_WARN("add table failed", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && is_major_merge_type(merge_type)) {
    // iterate all major and minors, to determine the dest restore status.
    if (OB_FAIL(check_and_determine_restore_status(ls_handle, dst_tablet_id, param.tables_handle_, param.restore_status_))) {
      LOG_WARN("check and determine restore status failed", K(ret), K(dst_tablet_id));
    } else if (OB_FAIL(param.tablet_split_param_.skip_split_keys_.assign(skipped_split_major_keys))) {
      LOG_WARN("assign failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObLSHandle new_ls_handle;
    const share::ObLSID &ls_id = ls_handle.get_ls()->get_ls_id();
    param.tablet_split_param_.snapshot_version_         = src_tablet_handle.get_obj()->get_tablet_meta().snapshot_version_;
    param.tablet_split_param_.multi_version_start_      = src_tablet_handle.get_obj()->get_multi_version_start();
    param.tablet_split_param_.merge_type_               = merge_type;
    param.rebuild_seq_                                  = ls_handle.get_ls()->get_rebuild_seq(); // old rebuild seq.
    param.release_mds_scn_.set_min();
    if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id, new_ls_handle, ObLSGetMod::DDL_MOD))) {
      LOG_WARN("failed to get log stream", K(ret), K(param));
    } else if (OB_FAIL(new_ls_handle.get_ls()->build_tablet_with_batch_tables(dst_tablet_id, param))) {
      LOG_WARN("failed to update tablet table store", K(ret), K(dst_tablet_id), K(param));
    }
    FLOG_INFO("update batch sstables", K(ret), K(dst_tablet_id), K(batch_tables), K(param));
  }
  return ret;
}

int ObTabletSplitMergeTask::check_and_determine_restore_status(
    const ObLSHandle &ls_handle,
    const ObTabletID &dst_tablet_id,
    const ObTablesHandleArray &major_handles_array,
    ObTabletRestoreStatus::STATUS &restore_status)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  ObTableStoreIterator table_store_iterator;
  ObSEArray<ObITable::TableKey, 1> empty_skipped_keys;

  restore_status = ObTabletRestoreStatus::FULL;
  ObSEArray<ObITable *, MAX_SSTABLE_CNT_IN_STORAGE> old_major_tables_array;
  ObSEArray<ObITable *, MAX_SSTABLE_CNT_IN_STORAGE> new_major_tables_array;
  if (OB_UNLIKELY(!ls_handle.is_valid() || !dst_tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(ls_handle), K(dst_tablet_id));
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle,
        dst_tablet_id, tablet_handle, ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    LOG_WARN("get tablet handle failed", K(ret), K(dst_tablet_id));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(tablet->get_all_sstables(table_store_iterator))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_FAIL(ObTabletSplitUtil::get_participants(ObSplitSSTableType::SPLIT_MAJOR, table_store_iterator, false/*is_table_restore*/,
      empty_skipped_keys, old_major_tables_array))) {
    LOG_WARN("get participant sstables failed", K(ret), K(dst_tablet_id));
  } else if (OB_FAIL(major_handles_array.get_tables(new_major_tables_array))) {
    LOG_WARN("get batch sstables failed", K(ret));
  } else {
    // 1. to check if there is any remote macro block in major tables.
    for (int64_t i = 0; OB_SUCC(ret) && ObTabletRestoreStatus::is_full(restore_status) && i < new_major_tables_array.count(); i++) {
      ObITable *table = new_major_tables_array.at(i);
      ObSSTableMetaHandle sstable_meta_handle;
      if (OB_UNLIKELY(nullptr == table || !table->is_major_sstable())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null table", K(ret), K(dst_tablet_id), KPC(table), K(major_handles_array));
      } else if (OB_FAIL(static_cast<ObSSTable *>(table)->get_meta(sstable_meta_handle))) {
        LOG_WARN("get sstable meta failed", K(ret));
      } else if (table->is_meta_major_sstable()) {
        // a. always replace meta major with the newest one.
        restore_status = sstable_meta_handle.get_sstable_meta().get_basic_meta().table_backup_flag_.has_backup() ?
                  ObTabletRestoreStatus::REMOTE : ObTabletRestoreStatus::FULL;
      } else if (!sstable_meta_handle.get_sstable_meta().get_basic_meta().table_backup_flag_.has_backup()) {
        // b. keep full, always replaces majors with backup with majors without backup.
      } else {
        // c. use the old major's restore status to decide the final restore status if there is an old major, else use status restore.
        ObSSTableMetaHandle old_sstable_meta_handle;
        restore_status = ObTabletRestoreStatus::REMOTE;
        for (int64_t j = 0; OB_SUCC(ret) && !old_sstable_meta_handle.is_valid() && j < old_major_tables_array.count(); j++) {
          const ObITable *old_sstable = old_major_tables_array.at(i);
          if (OB_ISNULL(old_sstable)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected err", K(ret), KPC(old_sstable));
          } else if (old_sstable->get_key() == table->get_key()) {
            if (OB_FAIL(static_cast<const ObSSTable *>(old_sstable)->get_meta(old_sstable_meta_handle))) {
              LOG_WARN("get sstable meta failed", K(ret), KPC(old_sstable));
            } else {
              restore_status = old_sstable_meta_handle.get_sstable_meta().get_basic_meta().table_backup_flag_.has_backup() ?
                  ObTabletRestoreStatus::REMOTE : ObTabletRestoreStatus::FULL;
            }
          }
        }
      }
      LOG_TRACE("with backup macro block sstable is found", K(ret), K(dst_tablet_id), KPC(table));
    }
  }

  if (OB_SUCC(ret) && ObTabletRestoreStatus::is_full(restore_status)) {
    // 2. to check if there is any remote macro block in minor tables.
    ObTabletHandle tablet_handle;
    ObTableStoreIterator table_iter;
    if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle, dst_tablet_id, tablet_handle, ObMDSGetTabletMode::READ_ALL_COMMITED))) {
      LOG_WARN("get tablet failed", K(ret), K(dst_tablet_id));
    } else if (OB_FAIL(tablet_handle.get_obj()->get_all_sstables(table_iter, true/*need_unpack*/))) {
      LOG_WARN("get all sstables failed", K(ret));
    } else {
      while (OB_SUCC(ret) && ObTabletRestoreStatus::is_full(restore_status)) {
        ObITable *table = nullptr;
        ObSSTableMetaHandle sstable_meta_handle;
        if (OB_FAIL(table_iter.get_next(table))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("iterate tables failed", K(ret), K(dst_tablet_id));
          } else {
            ret = OB_SUCCESS;
            break;
          }
        } else if (OB_UNLIKELY(nullptr == table)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("sstable should not be NULL", K(ret), KPC(table), K(table_iter));
        } else if (table->is_major_sstable()) {
          // already checked at the first stage.
        } else if (OB_FAIL(static_cast<ObSSTable *>(table)->get_meta(sstable_meta_handle))) {
          LOG_WARN("get sstable meta failed", K(ret));
        } else {
          restore_status = sstable_meta_handle.get_sstable_meta().get_basic_meta().table_backup_flag_.has_backup() ?
            ObTabletRestoreStatus::REMOTE : restore_status;
          LOG_TRACE("with backup macro block sstable is found", K(ret), K(dst_tablet_id), KPC(table));
        }
      }
    }
  }
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
        param.table_id_, param.src_tablet_.get_tablet_meta().tablet_id_, *rowkey_read_info_, false/*is_multi_version_minor_merge*/))) {
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
        false,  /* use whole macro scan*/
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
                                                    false/*is_multi_version_minor_merge*/))) {
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

// For split util.
// get all sstables that need to split.
int ObTabletSplitUtil::get_participants(
    const share::ObSplitSSTableType &split_sstable_type,
    const ObTableStoreIterator &const_table_store_iter,
    const bool is_table_restore,
    const ObIArray<ObITable::TableKey> &skipped_table_keys,
    ObIArray<ObITable *> &participants)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  participants.reset();
  ObTableStoreIterator table_store_iter;
  if (OB_FAIL(table_store_iter.assign(const_table_store_iter))) {
    LOG_WARN("failed to assign table store iter", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      ObITable *table = nullptr;
      ObSSTableMetaHandle sstable_meta_hdl;
      if (OB_FAIL(table_store_iter.get_next(table))) {
        if (OB_UNLIKELY(OB_ITER_END == ret)) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("get next table failed", K(ret), K(table_store_iter));
        }
      } else if (OB_UNLIKELY(nullptr == table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected err", K(ret), KPC(table));
      } else if (is_table_restore) {
        if (table->is_minor_sstable() || table->is_major_sstable()
            || ObITable::TableType::DDL_DUMP_SSTABLE == table->get_table_type()) {
          if (OB_FAIL(participants.push_back(table))) {
            LOG_WARN("push back major failed", K(ret));
          }
        } else {
          LOG_INFO("skip table", K(ret), KPC(table));
        }
      } else if (is_contain(skipped_table_keys, table->get_key())) {
        FLOG_INFO("no need to split for the table", "table_key", table->get_key(), K(skipped_table_keys));
      } else if (table->is_minor_sstable()) {
        if (share::ObSplitSSTableType::SPLIT_MAJOR == split_sstable_type) {
          // Split with major only.
        } else if (OB_FAIL(participants.push_back(table))) {
          LOG_WARN("push back failed", K(ret));
        }
      } else if (table->is_major_sstable()) {
        if (share::ObSplitSSTableType::SPLIT_MINOR == split_sstable_type) {
          // Split with minor only.
        } else if (OB_FAIL(participants.push_back(table))) {
          LOG_WARN("push back major failed", K(ret));
        }
      } else {
        if (!table->is_sstable()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected err", K(ret), KPC(table));
        } else if (table->is_minor_sstable()) {
          if (share::ObSplitSSTableType::SPLIT_MAJOR == split_sstable_type) {
            // Split with major only.
          } else if (OB_FAIL(participants.push_back(table))) {
            LOG_WARN("push back failed", K(ret));
          }
        } else if (table->is_major_sstable()) {
          if (share::ObSplitSSTableType::SPLIT_MINOR == split_sstable_type) {
            // Split with minor only.
          } else if (OB_FAIL(participants.push_back(table))) {
            LOG_WARN("push back major failed", K(ret));
          }
        } else if (table->is_mds_sstable()) {
          // skip
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected err", K(ret), KPC(table));
        }
      }
    }
  }
  return ret;
}

int ObTabletSplitUtil::split_task_ranges(
    ObIAllocator &allocator,
    const share::ObDDLType ddl_type,
    const share::ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const int64_t user_parallelism,
    const int64_t schema_tablet_size,
    ObIArray<blocksstable::ObDatumRowkey> &parallel_datum_rowkey_list)
{
  int ret = OB_SUCCESS;
  parallel_datum_rowkey_list.reset();
  ObSEArray<ObITable::TableKey, 1> empty_skipped_keys;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObTableStoreIterator table_store_iterator;
  ObSEArray<ObStoreRange, 32> store_ranges;
  ObSEArray<ObITable *, MAX_SSTABLE_CNT_IN_STORAGE> tables;
  const bool is_table_restore = ObDDLType::DDL_TABLE_RESTORE == ddl_type;
  common::ObArenaAllocator tmp_arena("SplitRange", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id));
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle,
    tablet_id, tablet_handle, ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    LOG_WARN("get tablet failed", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_all_sstables(table_store_iterator))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_FAIL(ObTabletSplitUtil::get_participants(
      share::ObSplitSSTableType::SPLIT_BOTH, table_store_iterator, is_table_restore, empty_skipped_keys, tables))) {
    LOG_WARN("get participants failed", K(ret));
  } else if (is_table_restore && tables.empty()) {
    ObDatumRowkey tmp_min_key;
    ObDatumRowkey tmp_max_key;
    tmp_min_key.set_min_rowkey();
    tmp_max_key.set_max_rowkey();
    if (OB_FAIL(parallel_datum_rowkey_list.prepare_allocate(2))) { // min key and max key.
      LOG_WARN("reserve failed", K(ret), K(ls_id), K(tablet_id));
    } else if (OB_FAIL(tmp_min_key.deep_copy(parallel_datum_rowkey_list.at(0), allocator))) {
      LOG_WARN("failed to push min rowkey", K(ret));
    } else if (OB_FAIL(tmp_max_key.deep_copy(parallel_datum_rowkey_list.at(1), allocator))) {
      LOG_WARN("failed to push min rowkey", K(ret));
    }
  } else {
    const ObITableReadInfo &rowkey_read_info = tablet_handle.get_obj()->get_rowkey_read_info();
    ObRangeSplitInfo range_info;
    ObPartitionRangeSpliter range_spliter;
    ObStoreRange whole_range;
    whole_range.set_whole_range();
    if (OB_FAIL(range_spliter.get_range_split_info(tables,
       rowkey_read_info, whole_range, range_info))) {
      LOG_WARN("init range split info failed", K(ret));
    } else if (OB_FALSE_IT(range_info.parallel_target_count_
      = MAX(1, MIN(user_parallelism, (range_info.total_size_ + schema_tablet_size - 1) / schema_tablet_size)))) {
    } else if (OB_FAIL(range_spliter.split_ranges(range_info,
      tmp_arena, false /*for_compaction*/, store_ranges))) {
      LOG_WARN("split ranges failed", K(ret), K(range_info));
    } else if (OB_UNLIKELY(store_ranges.count() <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(ret), K(range_info));
    } else {
      const int64_t rowkey_arr_cnt = store_ranges.count() + 1; // with min and max.
      if (OB_FAIL(parallel_datum_rowkey_list.prepare_allocate(rowkey_arr_cnt))) {
        LOG_WARN("reserve failed", K(ret), K(rowkey_arr_cnt), K(store_ranges));
      } else {
        ObDatumRowkey tmp_key;
        for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_arr_cnt; i++) {
          if (i == 0) {
            tmp_key.set_min_rowkey();
            if (OB_FAIL(tmp_key.deep_copy(parallel_datum_rowkey_list.at(i), allocator))) {
              LOG_WARN("failed to push min rowkey", K(ret));
            }
          } else if (OB_FAIL(tmp_key.from_rowkey(store_ranges.at(i - 1).get_end_key().get_rowkey(), tmp_arena))) {
            LOG_WARN("failed to shallow copy from obj", K(ret));
          } else if (OB_FAIL(tmp_key.deep_copy(parallel_datum_rowkey_list.at(i), allocator))) {
            LOG_WARN("failed to deep copy end key", K(ret), K(i), "src_key", store_ranges.at(i - 1).get_end_key());
          }
        }
      }
    }
    LOG_INFO("prepare task split ranges finished", K(ret), K(user_parallelism), K(schema_tablet_size), K(tablet_id),
        K(parallel_datum_rowkey_list), K(range_info));
  }
  tmp_arena.reset();
  return ret;
}

int ObTabletSplitUtil::convert_rowkey_to_range(
    ObIAllocator &allocator,
    const ObIArray<blocksstable::ObDatumRowkey> &parallel_datum_rowkey_list,
    ObIArray<ObDatumRange> &datum_ranges_array)
{
  int ret = OB_SUCCESS;
  datum_ranges_array.reset();
  ObDatumRange schema_rowkey_range;
  ObDatumRange multi_version_range;
  schema_rowkey_range.set_left_open();
  schema_rowkey_range.set_right_closed();
  for (int64_t i = 0; OB_SUCC(ret) && i < parallel_datum_rowkey_list.count() - 1; i++) {
    schema_rowkey_range.start_key_ = parallel_datum_rowkey_list.at(i); // shallow copy.
    schema_rowkey_range.end_key_ = parallel_datum_rowkey_list.at(i + 1); // shallow copy.
    multi_version_range.reset();
    if (OB_FAIL(schema_rowkey_range.to_multi_version_range(allocator, multi_version_range))) { // deep copy necessary, to hold datum buffer.
      LOG_WARN("failed to convert multi_version range", K(ret), K(schema_rowkey_range));
    } else if (OB_FAIL(datum_ranges_array.push_back(multi_version_range))) { // buffer is kept by the to_multi_version_range.
      LOG_WARN("failed to push back merge range to array", K(ret), K(multi_version_range));
    }
  }
  LOG_INFO("change to datum range array finished", K(ret), K(parallel_datum_rowkey_list), K(datum_ranges_array));
  return ret;
}

// only used for table recovery to build parallel tasks cross tenants.
int ObTabletSplitUtil::convert_datum_rowkey_to_range(
    ObIAllocator &allocator,
    const ObIArray<blocksstable::ObDatumRowkey> &parallel_datum_rowkey_list,
    ObIArray<ObDatumRange> &datum_ranges_array)
{
  int ret = OB_SUCCESS;
  datum_ranges_array.reset();
  ObDatumRange schema_rowkey_range;
  schema_rowkey_range.set_left_open();
  schema_rowkey_range.set_right_closed();
  for (int64_t i = 0; OB_SUCC(ret) && i < parallel_datum_rowkey_list.count() - 1; i++) {
    const ObDatumRowkey &start_key = parallel_datum_rowkey_list.at(i);
    const ObDatumRowkey &end_key = parallel_datum_rowkey_list.at(i + 1);
    if (OB_FAIL(start_key.deep_copy(schema_rowkey_range.start_key_, allocator))) { // deep copy necessary, to hold datum buffer.
      LOG_WARN("failed to deep copy start_key", K(ret), K(start_key));
    } else if (OB_FAIL(end_key.deep_copy(schema_rowkey_range.end_key_, allocator))) {
      LOG_WARN("failed to deep copy end_key", K(ret), K(end_key));
    } else if (OB_FAIL(datum_ranges_array.push_back(schema_rowkey_range))) { // buffer is kept by the schema_rowkey_range.
      LOG_WARN("failed to push back merge range to array", K(ret), K(schema_rowkey_range));
    }
  }
  LOG_INFO("change to datum range array finished", K(ret), K(parallel_datum_rowkey_list), K(datum_ranges_array));
  return ret;
}

// check whether the data tablet split finished.
int ObTabletSplitUtil::check_data_split_finished(
    const share::ObLSID &ls_id,
    const ObIArray<ObTabletID> &check_tablets_id,
    bool &is_finished)
{
  int ret = OB_SUCCESS;
  is_finished = true;
  ObLSHandle ls_handle;
  if (OB_UNLIKELY(!ls_id.is_valid() || check_tablets_id.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(ls_id), K(check_tablets_id));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < check_tablets_id.count() && is_finished; i++) {
      const ObTabletID &tablet_id = check_tablets_id.at(i);
      ObTabletHandle tmp_tablet_handle;
      ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
      if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle, tablet_id,
          tmp_tablet_handle, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
        LOG_WARN("get tablet handle failed", K(ret), K(ls_id), K(tablet_id));
      } else if (OB_UNLIKELY(nullptr == tmp_tablet_handle.get_obj())) {
        ret = OB_ERR_SYS;
        LOG_WARN("tablet handle is null", K(ret), K(ls_id), K(tablet_id));
      } else if (tmp_tablet_handle.get_obj()->get_tablet_meta().split_info_.is_data_incomplete()) {
        is_finished = false;
      } else if (OB_FAIL(tmp_tablet_handle.get_obj()->fetch_table_store(table_store_wrapper))) {
        LOG_WARN("fail to fetch table store", K(ret));
      } else if (OB_ISNULL(table_store_wrapper.get_member()->get_major_sstables().get_boundary_table(false/*first*/))) {
        ret = OB_EAGAIN;
        LOG_WARN("wait for migration to copy sstable finished", K(ret), K(ls_id), K(tablet_id), K(tmp_tablet_handle));
      }
    }
  }
  return ret;
}


int ObTabletSplitUtil::check_satisfy_split_condition(
    const ObLSHandle &ls_handle,
    const ObTabletHandle &source_tablet_handle,
    const ObArray<ObTabletID> &dest_tablets_id,
    const int64_t compaction_scn,
    const share::SCN &min_split_start_scn)
{
  int ret = OB_SUCCESS;
  UNUSED(compaction_scn);
  ObArray<ObTableHandleV2> memtable_handles;
  ObSSTable *latest_major;
  ObTablet *tablet = nullptr;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  bool is_tablet_status_need_to_split = false;
  share::SCN max_decided_scn;
  if (OB_UNLIKELY(!ls_handle.is_valid() || !source_tablet_handle.is_valid()
      || dest_tablets_id.empty() || !min_split_start_scn.is_valid_and_not_min())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(ls_handle), K(source_tablet_handle), K(dest_tablets_id), K(min_split_start_scn));
  } else if (OB_ISNULL(tablet = source_tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), K(source_tablet_handle));
  } else if (OB_FAIL(check_tablet_restore_status(dest_tablets_id, ls_handle, source_tablet_handle, is_tablet_status_need_to_split))) {
    LOG_WARN("failed to check tablet status", K(ret), K(source_tablet_handle));
  } else if (OB_UNLIKELY(!is_tablet_status_need_to_split)) {
    ret = OB_TABLET_STATUS_NO_NEED_TO_SPLIT;
    LOG_WARN("there is no need to split, because of the special restore status of src tablet or des tablets", K(ret), K(source_tablet_handle), K(dest_tablets_id));
  } else if (OB_FAIL(check_tablet_ha_status(ls_handle, source_tablet_handle, dest_tablets_id))) {
    if (OB_NEED_RETRY != ret) {
      LOG_WARN("check tablet ha status failed", K(ls_handle), K(source_tablet_handle), K(dest_tablets_id));
    }
  } else if (OB_FAIL(tablet->get_all_memtables_from_memtable_mgr(memtable_handles))) {
    LOG_WARN("failed to get_memtable_mgr for get all memtable", K(ret), KPC(tablet));
  } else if (!memtable_handles.empty()) {
    ret = OB_NEED_RETRY;
    if (REACH_COUNT_INTERVAL(1000L)) {
      LOG_INFO("should wait memtable dump", K(ret), "tablet_id", tablet->get_tablet_meta().tablet_id_, K(memtable_handles));
    }
  } else if (OB_FAIL(ls_handle.get_ls()->get_max_decided_scn(max_decided_scn))) {
    LOG_WARN("get max decided log ts failed", K(ret), "ls_id", ls_handle.get_ls()->get_ls_id(),
      "source_tablet_id", tablet->get_tablet_meta().tablet_id_);
    if (OB_STATE_NOT_MATCH == ret) {
      ret = OB_NEED_RETRY;
    }
  } else if (SCN::plus(max_decided_scn, 1) < min_split_start_scn) {
    ret = OB_NEED_RETRY;
    if (REACH_COUNT_INTERVAL(1000L)) {
      LOG_INFO("need wait max decided scn reach", K(ret), "ls_id", ls_handle.get_ls()->get_ls_id(),
        "source_tablet_id", tablet->get_tablet_meta().tablet_id_, K(max_decided_scn), K(min_split_start_scn));
    }
  } else if (MTL_TENANT_ROLE_CACHE_IS_RESTORE()) {
    LOG_INFO("dont check compaction in restore progress", K(ret), "tablet_id", tablet->get_tablet_meta().tablet_id_);
  } else {
    const ObMediumCompactionInfoList *medium_list = nullptr;
    ObArenaAllocator tmp_allocator("SplitGetMedium", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()); // for load medium info
    if (OB_FAIL(tablet->read_medium_info_list(tmp_allocator, medium_list))) {
      LOG_WARN("failed to load medium info list", K(ret), K(tablet));
    } else if (medium_list->size() > 0) {
      ret = OB_NEED_RETRY;
      if (REACH_COUNT_INTERVAL(1000L)) {
        LOG_INFO("should wait compact end", K(ret), "tablet_id", tablet->get_tablet_meta().tablet_id_, KPC(medium_list));
      }
    }
  }
  return ret;
}

int ObTabletSplitUtil::get_split_dest_tablets_info(
    const share::ObLSID &ls_id,
    const ObTabletID &source_tablet_id,
    ObIArray<ObTabletID> &dest_tablets_id,
    lib::Worker::CompatMode &compat_mode)
{
  int ret = OB_SUCCESS;
  dest_tablets_id.reset();
  compat_mode = lib::Worker::CompatMode::INVALID;
  ObTabletSplitMdsUserData data;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  if (OB_UNLIKELY(!ls_id.is_valid() || !source_tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(ls_id), K(source_tablet_id));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id));
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle,
                                               source_tablet_id,
                                               tablet_handle,
                                               ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("get tablet handle failed", K(ret), K(ls_id), K(source_tablet_id));
  } else if (OB_FAIL(tablet_handle.get_obj()->ObITabletMdsInterface::get_split_data(data, ObTabletCommon::DEFAULT_GET_TABLET_DURATION_10_S))) {
    LOG_WARN("failed to get split data", K(ret));
  } else if (OB_FAIL(data.get_split_dst_tablet_ids(dest_tablets_id))) {
    LOG_WARN("failed to get split dst tablet ids", K(ret));
  } else if (OB_UNLIKELY(dest_tablets_id.count() < 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected mds data", K(ret), K(ls_id), K(source_tablet_id), K(dest_tablets_id));
  } else {
    compat_mode = tablet_handle.get_obj()->get_tablet_meta().compat_mode_;
  }
  return ret;
}

int ObTabletSplitUtil::check_medium_compaction_info_list_cnt(
    const obrpc::ObCheckMediumCompactionInfoListArg &arg,
    obrpc::ObCheckMediumCompactionInfoListResult &result)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(arg.ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(arg));
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle,
        arg.tablet_id_, tablet_handle, ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    LOG_WARN("get tablet handle failed", K(ret), K(arg));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(arg));
  } else {
    ObSEArray<ObITable::TableKey, 1> empty_skipped_keys;
    common::ObArenaAllocator allocator;
    ObTableStoreIterator table_store_iterator;
    ObSEArray<ObITable *, MAX_SSTABLE_CNT_IN_STORAGE> major_tables;
    const compaction::ObMediumCompactionInfoList *medium_info_list = nullptr;
    if (OB_FAIL(tablet->read_medium_info_list(allocator, medium_info_list))) {
      LOG_WARN("failed to get mediumn info list", K(ret));
    } else if (medium_info_list->size() > 0) {
      // compaction still ongoing
      result.info_list_cnt_ = medium_info_list->size();
      result.primary_compaction_scn_ = -1;
    } else if (OB_FAIL(tablet->get_all_sstables(table_store_iterator))) {
      LOG_WARN("fail to fetch table store", K(ret));
    } else if (OB_FAIL(ObTabletSplitUtil::get_participants(ObSplitSSTableType::SPLIT_MAJOR, table_store_iterator, false/*is_table_restore*/,
        empty_skipped_keys, major_tables))) {
      LOG_WARN("get participant sstables failed", K(ret));
    } else {
      result.info_list_cnt_ = 0;
      result.primary_compaction_scn_ = -1;
      ObSSTable *sstable = nullptr;
      for (int64_t i = 0; OB_SUCC(ret) && i < major_tables.count(); i++) {
        if (OB_ISNULL(sstable = static_cast<ObSSTable *>(major_tables.at(i)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected err", K(ret), KPC(sstable));
        } else {
          const int64_t snapshot_version = sstable->get_snapshot_version();
          result.primary_compaction_scn_ = MAX(result.primary_compaction_scn_, snapshot_version);
        }
      }
      if (OB_SUCC(ret) && -1 == result.primary_compaction_scn_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error", K(ret), K(major_tables));
      }
    }
  }
  LOG_INFO("receive check medium compaction info list", K(ret), K(arg), K(result));
  return ret;
}

int ObTabletSplitUtil::check_tablet_restore_status(
    const ObArray<ObTabletID> &dest_tablets_id,
    const ObLSHandle &ls_handle,
    const ObTabletHandle &source_tablet_handle,
    bool &is_tablet_status_need_to_split)
{
  int ret = OB_SUCCESS;
  is_tablet_status_need_to_split = true;
  ObTablet *source_tablet = nullptr;
  ObTabletRestoreStatus::STATUS source_restore_status = ObTabletRestoreStatus::STATUS::RESTORE_STATUS_MAX;
  ObLS *ls = nullptr;
  if (OB_UNLIKELY(dest_tablets_id.count() <= 0 || !ls_handle.is_valid() || !source_tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dest_tablets_id), K(source_tablet_handle), K(ls_handle));
  } else if (OB_ISNULL(source_tablet = source_tablet_handle.get_obj()) || OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_NULL_CHECK_ERROR;
    LOG_WARN("unexpected nullptr", K(ret), KP(source_tablet), KP(ls));
  } else if (OB_FAIL(source_tablet->get_restore_status(source_restore_status))) {
    LOG_WARN("failed  to get restore status", K(ret), KP(source_tablet));
  } else if (OB_UNLIKELY(ObTabletRestoreStatus::STATUS::UNDEFINED == source_restore_status
      || ObTabletRestoreStatus::STATUS::EMPTY == source_restore_status || ObTabletRestoreStatus::STATUS::REMOTE == source_restore_status)) {
    is_tablet_status_need_to_split = false;
    ObTabletHandle t_handle;
    ObTabletRestoreStatus::STATUS des_restore_status = ObTabletRestoreStatus::STATUS::RESTORE_STATUS_MAX;
    ObArray<ObTabletRestoreStatus::STATUS> des_tablet_status;
    ObTablet *tablet = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < dest_tablets_id.count(); ++i) {
      t_handle.reset();
      const ObTabletID &t_id = dest_tablets_id.at(i);
      if (OB_FAIL(ls->get_tablet(t_id, t_handle))) {
        LOG_WARN("failed to get table", K(ret), K(t_id));
      } else if (OB_ISNULL(tablet = t_handle.get_obj())) {
        ret = OB_NULL_CHECK_ERROR;
        LOG_WARN("unexpected null ptr of tablet", K(ret), KPC(tablet));
      } else if (OB_FAIL(tablet->get_restore_status(des_restore_status))) {
        LOG_WARN("failed to get restore status of tablet", K(ret), K(tablet));
      } else if (OB_FAIL(des_tablet_status.push_back(des_restore_status))) {
        LOG_WARN("failed to push back into des_tablet_status", K(ret), K(i), K(des_tablet_status));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(des_tablet_status.count() != dest_tablets_id.count())) {
       ret = OB_ERR_UNEXPECTED;
       LOG_WARN("the count of des_tablet_status doesn't equal to the count of dest_tablets_id",
           K(ret), K(des_tablet_status.count()), K(dest_tablets_id.count()));
    } else if (((ObTabletRestoreStatus::STATUS::EMPTY == source_restore_status))
        || (ObTabletRestoreStatus::STATUS::REMOTE == source_restore_status)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < des_tablet_status.count() && !is_tablet_status_need_to_split; ++i) {
        ObTabletRestoreStatus::STATUS &des_res_sta = des_tablet_status.at(i);
        if (ObTabletRestoreStatus::STATUS::FULL == des_res_sta) {
          is_tablet_status_need_to_split = true;
        }
      }
    }
    if (OB_SUCC(ret) && !is_tablet_status_need_to_split) {
      LOG_INFO("tablets' resotre status are unexpected:", "src_tablet_id", source_tablet_handle.get_obj()->get_tablet_id(), K(dest_tablets_id),
          K(source_restore_status), "dst_tablet_restore_status", des_tablet_status);
    }
  }
  return ret;
}

int ObTabletSplitUtil::build_lost_medium_mds_sstable(
    common::ObArenaAllocator &allocator,
    const ObLSHandle &ls_handle,
    const ObTabletHandle &source_tablet_handle,
    const ObTabletID &dest_tablet_id,
    ObTableHandleV2 &medium_mds_table_handle)
{
  int ret = OB_SUCCESS;
  medium_mds_table_handle.reset();
  ObTabletHandle dest_tablet_handle;
  if (OB_UNLIKELY(!ls_handle.is_valid() || !source_tablet_handle.is_valid() || !dest_tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(ls_handle), K(source_tablet_handle), K(dest_tablet_id));
  } else if (!MTL_TENANT_ROLE_CACHE_IS_RESTORE()) {
    LOG_INFO("not restore tenant, no medium info lost", "tenant_id", MTL_ID(),
        "source_tablet_id", source_tablet_handle.get_obj()->get_tablet_id(), K(dest_tablet_id));
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle,
      dest_tablet_id, dest_tablet_handle, ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    LOG_WARN("get tablet failed", K(ret), K(dest_tablet_id));
  } else {
    const share::ObLSID &ls_id = ls_handle.get_ls()->get_ls_id();
    const ObTabletID &source_tablet_id = source_tablet_handle.get_obj()->get_tablet_id();
    HEAP_VARS_3 ((compaction::ObTabletMergeDagParam, param),
                 (compaction::ObTabletMergeCtx, tablet_merge_ctx, param, allocator),
                 (ObTabletMediumInfoReader, medium_info_reader)) {
    HEAP_VARS_3 ((ObTableScanParam, scan_param),
                 (ObTabletDumpMediumMds2MiniOperator, op),
                 (ObMdsTableMiniMerger, mds_mini_merger)) {
      if (OB_FAIL(check_and_build_mds_sstable_merge_ctx(ls_handle, dest_tablet_handle, tablet_merge_ctx))) {
        LOG_WARN("prepare medium mds merge ctx failed", K(ret), K(ls_handle), K(dest_tablet_id));
      } else if (tablet_merge_ctx.static_param_.scn_range_.end_scn_.is_base_scn()) { // = 1
        LOG_INFO("no need to build lost mds sstable again", K(ls_id), K(source_tablet_id), K(dest_tablet_id));
      } else if (OB_FAIL(mds_mini_merger.init(tablet_merge_ctx, op))) {
        LOG_WARN("fail to init mds mini merger", K(ret), K(tablet_merge_ctx), K(ls_id), K(dest_tablet_id));
      } else if (OB_FAIL(ObMdsScanParamHelper::build_medium_info_scan_param(
          allocator,
          ls_id,
          source_tablet_id,
          scan_param))) {
        LOG_WARN("fail to build scan param", K(ret), K(ls_id), K(source_tablet_id));
      } else if (OB_FAIL(medium_info_reader.init(*source_tablet_handle.get_obj(), scan_param))) {
        LOG_WARN("failed to init medium info reader", K(ret));
      } else {
        bool has_medium_info = false;
        mds::MdsDumpKV *kv = nullptr;
        common::ObArenaAllocator iter_arena("SplitIterMedium", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
        while (OB_SUCC(ret)) {
          iter_arena.reuse();
          if (OB_FAIL(medium_info_reader.get_next_mds_kv(iter_arena, kv))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("iter medium mds failed", K(ret), K(ls_id), K(source_tablet_id));
            } else {
              ret = OB_SUCCESS;
              break;
            }
          } else if (OB_FAIL(op(*kv))) {
            LOG_WARN("write medium row failed", K(ret));
          } else {
            kv->mds::MdsDumpKV::~MdsDumpKV();
            iter_arena.free(kv);
            kv = nullptr;
            has_medium_info = true;
          }
        }
        if (OB_SUCC(ret)) {
          if (!has_medium_info) {
            LOG_INFO("no need to build lost mds sstable", K(ls_id), K(source_tablet_id), K(dest_tablet_id));
          } else if (OB_FAIL(op.finish())) {
            LOG_WARN("finish failed", K(ret));
          } else if (OB_FAIL(mds_mini_merger.generate_mds_mini_sstable(allocator, medium_mds_table_handle))) {
            LOG_WARN("fail to generate mds mini sstable with mini merger", K(ret), K(mds_mini_merger));
          }
        }
      }
    }
    }
  }
  return ret;
}

int ObTabletSplitUtil::check_and_build_mds_sstable_merge_ctx(
    const ObLSHandle &ls_handle,
    const ObTabletHandle &dest_tablet_handle,
    compaction::ObTabletMergeCtx &tablet_merge_ctx)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = nullptr;
  share::SCN end_scn;
  if (OB_UNLIKELY(!ls_handle.is_valid() || !dest_tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(ls_handle), K(dest_tablet_handle));
  } else if (OB_FAIL(check_and_determine_mds_end_scn(dest_tablet_handle, end_scn))) {
    LOG_WARN("get mds sstable start scn failed", K(ret), K(dest_tablet_handle));
  } else {
    compaction::ObStaticMergeParam &static_param = tablet_merge_ctx.static_param_;
    static_param.ls_handle_             = ls_handle;
    static_param.dag_param_.ls_id_      = ls_handle.get_ls()->get_ls_id();
    static_param.dag_param_.merge_type_ = compaction::ObMergeType::MDS_MINI_MERGE;
    static_param.dag_param_.tablet_id_  = dest_tablet_handle.get_obj()->get_tablet_id();
    static_param.pre_warm_param_.type_  = ObPreWarmerType::MEM_PRE_WARM;
    tablet_merge_ctx.tablet_handle_                  = dest_tablet_handle;
    static_param.scn_range_.start_scn_               = SCN::base_scn(); // 1
    static_param.scn_range_.end_scn_                 = end_scn;
    static_param.version_range_.snapshot_version_    = end_scn.get_val_for_tx();
    static_param.version_range_.multi_version_start_ = dest_tablet_handle.get_obj()->get_multi_version_start();
    static_param.merge_scn_                          = end_scn;
    static_param.create_snapshot_version_            = 0;
    static_param.need_parallel_minor_merge_          = false;
    static_param.tablet_transfer_seq_                  = dest_tablet_handle.get_obj()->get_transfer_seq();
    tablet_merge_ctx.static_desc_.tablet_transfer_seq_ = dest_tablet_handle.get_obj()->get_transfer_seq();

    if (OB_FAIL(tablet_merge_ctx.init_tablet_merge_info())) {
      LOG_WARN("failed to init tablet merge info", K(ret), K(ls_handle), K(dest_tablet_handle), K(tablet_merge_ctx));
    }
  }
  return ret;
}

int ObTabletSplitUtil::check_and_determine_mds_end_scn(
    const ObTabletHandle &dest_tablet_handle,
    share::SCN &end_scn)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  ObITable *first_mds_sstable = nullptr;
  ObTableStoreIterator table_store_iterator;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  if (OB_UNLIKELY(!dest_tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(dest_tablet_handle));
  } else if (OB_ISNULL(tablet = dest_tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be nullptr", K(ret), K(dest_tablet_handle));
  } else if (OB_FAIL(tablet->get_all_sstables(table_store_iterator))) {
    LOG_WARN("get all sstables failed", K(ret));
  } else if (OB_FAIL(tablet->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fetch table store failed", K(ret), KPC(tablet));
  } else if (OB_ISNULL(first_mds_sstable =
      table_store_wrapper.get_member()->get_mds_sstables().get_boundary_table(false/*first*/))) {
    end_scn = tablet->get_mds_checkpoint_scn();
  } else {
    end_scn = first_mds_sstable->get_start_scn();
  }

  return ret;
}

int ObTabletSplitUtil::check_sstables_skip_data_split(
    const ObLSHandle &ls_handle,
    const ObTableStoreIterator &source_table_store_iter,
    const ObIArray<ObTabletID> &dest_tablets_id,
    const int64_t lob_major_snapshot/*OB_INVALID_VERSION for non lob tablets*/,
    ObIArray<ObITable::TableKey> &skipped_split_major_keys)
{
  int ret = OB_SUCCESS;
  skipped_split_major_keys.reset();
  ObSEArray<ObITable::TableKey, 1> empty_skipped_keys;
  ObSEArray<ObITable *, MAX_SSTABLE_CNT_IN_STORAGE> major_sstables;
  if (OB_UNLIKELY(dest_tablets_id.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(dest_tablets_id));
  } else if (OB_FAIL(ObTabletSplitUtil::get_participants(
      share::ObSplitSSTableType::SPLIT_MAJOR, source_table_store_iter, false/*is_table_restore*/, empty_skipped_keys, major_sstables))) {
    LOG_WARN("get all majors failed", K(ret), K(dest_tablets_id));
  } else if (OB_UNLIKELY(major_sstables.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no major in source tablet", K(ret));
  } else {
    const bool is_lob_tablet = OB_INVALID_VERSION != lob_major_snapshot;
    const int64_t checked_table_cnt = is_lob_tablet ? 1 : major_sstables.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < checked_table_cnt; i++) {
      bool need_data_split = false;
      ObITable::TableKey this_key = major_sstables.at(i)->get_key();
      this_key.version_range_.snapshot_version_ = is_lob_tablet ? lob_major_snapshot : this_key.version_range_.snapshot_version_;
      for (int64_t j = 0; OB_SUCC(ret) && !need_data_split && j < dest_tablets_id.count(); j++) {
        ObSSTableWrapper sstable_wrapper;
        ObTabletHandle tmp_tablet_handle;
        const ObTabletID &this_tablet_id = dest_tablets_id.at(j);
        this_key.tablet_id_ = this_tablet_id;
        ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
        if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle, this_tablet_id,
            tmp_tablet_handle, ObMDSGetTabletMode::READ_ALL_COMMITED))) {
          LOG_WARN("get tablet handle failed", K(ret), K(this_tablet_id));
        } else if (OB_UNLIKELY(nullptr == tmp_tablet_handle.get_obj())) {
          ret = OB_ERR_SYS;
          LOG_WARN("tablet handle is null", K(ret), K(this_tablet_id));
        } else if (OB_FAIL(tmp_tablet_handle.get_obj()->fetch_table_store(table_store_wrapper))) {
          LOG_WARN("fail to fetch table store", K(ret));
        } else if (OB_FAIL(table_store_wrapper.get_member()->get_sstable(this_key, sstable_wrapper))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            need_data_split = true;
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to get table from table store", K(ret), K(this_key));
          }
        }
      }
      if (OB_SUCC(ret) && !need_data_split) {
        if (OB_FAIL(skipped_split_major_keys.push_back(this_key))) {
          LOG_WARN("push back failed", K(ret), K(this_key));
        }
      }
    }
  }
  return ret;
}

// For migration, wait data status COMPLETE.
// For restore, wait restore status FULL/REMOTE.
int ObTabletSplitUtil::check_tablet_ha_status(
    const ObLSHandle &ls_handle,
    const ObTabletHandle &source_tablet_handle,
    const ObIArray<ObTabletID> &dest_tablets_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_handle.is_valid() || !source_tablet_handle.is_valid() || dest_tablets_id.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(source_tablet_handle), K(dest_tablets_id));
  } else if (!source_tablet_handle.get_obj()->get_tablet_meta().ha_status_.check_allow_read()) {
    ret = OB_NEED_RETRY;
    if (REACH_COUNT_INTERVAL(1000L)) {
      LOG_INFO("should wait data complete", K(ret),
          "tablet_meta", source_tablet_handle.get_obj()->get_tablet_meta());
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < dest_tablets_id.count(); i++) {
      ObTabletHandle tmp_tablet_handle;
      const ObTabletID &tablet_id = dest_tablets_id.at(i);
      if ((OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle, tablet_id, tmp_tablet_handle, ObMDSGetTabletMode::READ_ALL_COMMITED)))) {
        LOG_WARN("get tablet failed", K(ret), K(tablet_id));
      } else if (OB_UNLIKELY(!tmp_tablet_handle.is_valid())) {
        ret = OB_ERR_SYS;
        LOG_WARN("invalid tablet", K(ret), K(tablet_id), K(tmp_tablet_handle));
      } else if (!tmp_tablet_handle.get_obj()->get_tablet_meta().ha_status_.check_allow_read()) {
        ret = OB_NEED_RETRY;
        if (REACH_COUNT_INTERVAL(1000L)) {
          LOG_INFO("should wait data complete", K(ret),
              "tablet_meta", tmp_tablet_handle.get_obj()->get_tablet_meta());
        }
      }
    }
  }
  return ret;
}

} //end namespace stroage
} //end namespace oceanbase
