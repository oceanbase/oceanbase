/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_tablet_split_task.h"
#include "logservice/ob_log_service.h"
#include "lib/ob_define.h"
#include "share/ob_ddl_sim_point.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"


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
    arena_allocator_("SplitCtx", OB_MALLOC_NORMAL_BLOCK_SIZE /*8KB*/, MTL_ID()),
    lock_(ObLatchIds::TABLET_SPLIT_CONTEXT_LOCK), bucket_lock_(),
    is_inited_(false), result_tables_handle_array_(), mds_storage_schema_(nullptr),
    complement_data_ret_(OB_SUCCESS), ls_handle_(), tablet_handle_(),
    skipped_split_major_keys_(), row_inserted_(0), cg_row_inserted_(0), physical_row_count_(0),
    split_scn_(), reorg_scn_(), ls_rebuild_seq_(-1)
{
  result_tables_handle_array_.set_allocator(&arena_allocator_);
}

ObTabletSplitCtx::~ObTabletSplitCtx()
{
  int ret = OB_SUCCESS;
  is_inited_ = false;
  is_split_finish_with_meta_flag_ = false;
  ls_rebuild_seq_ = -1;
  complement_data_ret_ = OB_SUCCESS;
  ls_handle_.reset();
  tablet_handle_.reset();
  table_store_iterator_.reset();
  data_split_ranges_.reset();
  skipped_split_major_keys_.reset();
  if (nullptr != mds_storage_schema_) {
    mds_storage_schema_->~ObStorageSchema();
    arena_allocator_.free(mds_storage_schema_);
    mds_storage_schema_ = nullptr;
  }
  ObArray<ObSSTableSplitHelper *> remain_helpers;
  for (common::hash::ObHashMap<ObITable::TableKey, ObSSTableSplitHelper *>::iterator iter = sstable_split_helpers_map_.begin();
        iter != sstable_split_helpers_map_.end(); ++iter) {
    destroy_split_object(concurrent_allocator_, iter->second);
  }
  sstable_split_helpers_map_.destroy();
  result_tables_handle_array_.reset();
  concurrent_allocator_.destroy();
  arena_allocator_.reset();
}

bool ObTabletSplitCtx::is_valid() const
{
  return is_inited_ && ls_handle_.is_valid() && tablet_handle_.is_valid();
}

int ObTabletSplitCtx::init(const ObTabletSplitParam &param)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator tmp_arena("GetSplitTab", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObTabletHandle local_tablet_hdl;
  lib::ObMemAttr attr(MTL_ID(), "SplitIdxBuilder");
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(param));
  } else if (OB_FAIL(concurrent_allocator_.init(OB_MALLOC_MIDDLE_BLOCK_SIZE,
      attr.label_, MTL_ID(), 1024L * 1024L * 1024L * 10L/*10GB*/))) {
    LOG_WARN("init alloctor failed", K(ret));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(param.ls_id_, ls_handle_, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(param));
  } else if (OB_FAIL(ObTabletSplitUtil::get_tablet(tmp_arena, ls_handle_,
      param.source_tablet_id_, false/*is_shared_mode*/, local_tablet_hdl,
      ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    LOG_WARN("get local tablet failed", K(ret), K(param));
  } else if (OB_FAIL(ObTabletSplitUtil::check_satisfy_split_condition(ls_handle_, local_tablet_hdl,
      param.dest_tablets_id_, param.compaction_scn_, param.min_split_start_scn_))) {
    if (OB_NEED_RETRY != ret) {
      LOG_WARN("check satisfy split condition failed", K(ret), K(param));
    }
  } else if (OB_FAIL(ObTabletSplitUtil::get_tablet(arena_allocator_, ls_handle_,
      param.source_tablet_id_, GCTX.is_shared_storage_mode()/*is_shared_mode*/,
      tablet_handle_,
      ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    LOG_WARN("get tablet failed", K(ret), "tablet_id", param.source_tablet_id_);
  } else if (OB_FAIL(tablet_handle_.get_obj()->get_all_tables(table_store_iterator_, true/*need_unpack*/))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_FAIL(ObTabletSplitUtil::check_sstables_skip_data_split(
      ls_handle_, table_store_iterator_, param.dest_tablets_id_, OB_INVALID_VERSION/*lob_major_snapshot*/, skipped_split_major_keys_))) {
    LOG_WARN("check sstables skip data split failed", K(ret));
  } else if (OB_FAIL(ObTabletSplitUtil::convert_rowkey_to_range(arena_allocator_, param.parallel_datum_rowkey_list_, data_split_ranges_))) {
    LOG_WARN("convert to range failed", K(ret), K(param));
  } else if (OB_FAIL(ObTabletSplitUtil::check_data_split_finished(param.ls_id_, param.source_tablet_id_,
      param.dest_tablets_id_ , param.can_reuse_macro_block_, is_split_finish_with_meta_flag_))) {
    LOG_WARN("check all tablets major exist failed", K(ret), K(param.ls_id_), K(param.dest_tablets_id_));
  } else {
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
    if (OB_FAIL(bucket_lock_.init(MAX_SSTABLE_CNT_IN_STORAGE/*bucket_nums*/, common::ObLatchIds::TABLET_SPLIT_SSTABLE_HELPERS_LOCK))) {
      LOG_WARN("init bucket lock failed", K(ret));
    } else if (OB_FAIL(sstable_split_helpers_map_.create(MAX_SSTABLE_CNT_IN_STORAGE/*bucket_nums*/, "SplitHelperMap"))) {
      LOG_WARN("create sstable split helpers map failed", K(ret));
    }
    is_inited_ = true;
  }
  return ret;
}

int ObTabletSplitCtx::prepare_schema_and_result_array(
    const ObTabletSplitParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(mds_storage_schema_ != nullptr)) {
    ret = OB_ERR_SYS;
    LOG_WARN("param init twice", K(ret));
  } else if (OB_FAIL(ObTabletSplitUtil::get_storage_schema_from_mds(
      tablet_handle_,
      param.data_format_version_,
      mds_storage_schema_,
      arena_allocator_))) {
    LOG_WARN("prepare mds storage schema failed", K(ret));
  } else if (OB_UNLIKELY(nullptr == mds_storage_schema_
      || !mds_storage_schema_->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr of mds schema", K(ret), KPC(mds_storage_schema_));
  } else if (OB_FAIL(result_tables_handle_array_.init(param.dest_tablets_id_.count()))) {
    LOG_WARN("init result tables handle array failed", K(ret));
  } else {
    ObTablesHandleArray tables_handle_array;
    for (int64_t i = 0; OB_SUCC(ret) && i < param.dest_tablets_id_.count(); i++) {
      if (OB_FAIL(result_tables_handle_array_.push_back(tables_handle_array))) {
        LOG_WARN("push back result tables handle array failed", K(ret));
      }
    }
  }
  return ret;
}

int ObTabletSplitCtx::generate_sstable(
    const int64_t dest_tablet_index,
    const ObTabletCreateSSTableParam &create_sstable_param)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(dest_tablet_index < 0
      || dest_tablet_index >= result_tables_handle_array_.count()
      || !create_sstable_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(dest_tablet_index), K(create_sstable_param), "arr_cnt", result_tables_handle_array_.count());
  } else {
    ObTableHandleV2 table_handle;
    const bool is_co_sstable = create_sstable_param.table_key().is_co_sstable();
    ObSpinLockGuard guard(lock_);
    if (is_co_sstable && OB_FAIL(ObTabletCreateDeleteHelper::create_sstable<ObCOSSTableV2>(
        create_sstable_param,
        arena_allocator_/*arena, the thread safety is guaranteed by the lock*/,
        table_handle))) {
      LOG_WARN("create sstable failed", K(ret), K(create_sstable_param));
    } else if (!is_co_sstable && OB_FAIL(ObTabletCreateDeleteHelper::create_sstable<ObSSTable>(
        create_sstable_param,
        arena_allocator_/*arena, the thread safety is guaranteed by the lock*/,
        table_handle))) {
      LOG_WARN("create sstable failed", K(ret), K(create_sstable_param));
    } else if (OB_FAIL(result_tables_handle_array_.at(dest_tablet_index).add_table(table_handle))) {
      LOG_WARN("add table failed", K(ret));
    }
  }
  return ret;
}

int ObTabletSplitCtx::generate_mds_sstable(
  const int64_t dest_tablet_index,
  ObMdsTableMiniMerger &mds_mini_merger)
{
  int ret = OB_SUCCESS;
  ObTableHandleV2 table_handle;
  ObSpinLockGuard guard(lock_);
  if (OB_UNLIKELY(dest_tablet_index < 0 || dest_tablet_index >= result_tables_handle_array_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(dest_tablet_index), "arr_cnt", result_tables_handle_array_.count());
  } else if (OB_FAIL(mds_mini_merger.generate_mds_mini_sstable(arena_allocator_, table_handle))) {
    LOG_WARN("fail to generate mds mini sstable with mini merger", K(ret), K(mds_mini_merger));
  }
  if (FAILEDx(result_tables_handle_array_.at(dest_tablet_index).add_table(table_handle))) {
    LOG_WARN("add table failed", K(ret));
  }
  return ret;
}

int ObTabletSplitCtx::inner_organize_result_tables(
    const share::ObSplitSSTableType &split_sstable_type,
    const int64_t dest_tablet_index,
    ObTablesHandleArray &cg_tables_handle_array/*to hold cgs' macro ref*/)
{
  int ret = OB_SUCCESS;
  cg_tables_handle_array.reset();
  if (share::ObSplitSSTableType::SPLIT_MAJOR != split_sstable_type) {
    // do nothing.
  } else if (OB_UNLIKELY(dest_tablet_index < 0 || dest_tablet_index >= result_tables_handle_array_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(dest_tablet_index), "arr_cnt", result_tables_handle_array_.count());
  } else {
    ObTableHandleV2 table_handle;
    ObTablesHandleArray organized_tables_handle_array;
    const ObTablesHandleArray &tablet_tables_handle_array = result_tables_handle_array_.at(dest_tablet_index);
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_tables_handle_array.get_count(); i++) {
      ObITable *table = nullptr;
      table_handle.reset();
      if (OB_FAIL(tablet_tables_handle_array.get_table(i/*idx*/, table_handle))) {
        LOG_WARN("get table failed", K(ret));
      } else if (OB_ISNULL(table = table_handle.get_table())) {
        ret = OB_ERR_SYS;
        LOG_WARN("error sys", K(ret), K(i), K(table_handle));
      } else if (!table->is_column_store_sstable()) {
        if (OB_FAIL(organized_tables_handle_array.add_table(table_handle))) {
          LOG_WARN("add table failed", K(ret));
        }
      } else if (table->is_co_sstable()) {
        if (OB_FAIL(organized_tables_handle_array.add_table(table_handle))) {
          LOG_WARN("add table failed", K(ret));
        }
      } else if (table->is_cg_sstable()) {
        if (OB_FAIL(cg_tables_handle_array.add_table(table_handle))) {
          LOG_WARN("add table failed", K(ret));
        }
      } else {
        ret = OB_ERR_SYS;
        LOG_WARN("unexpected table", K(ret), K(dest_tablet_index), KPC(table));
      }
    }
    int cmp_cg_sstables_cnt = 0;
    const bool with_cg_in_result_array = !cg_tables_handle_array.empty();
    for (int64_t i = 0; OB_SUCC(ret) && with_cg_in_result_array && i < organized_tables_handle_array.get_count(); i++) {
      ObTableHandleV2 co_table_handle, cg_table_handle;
      if (OB_FAIL(organized_tables_handle_array.get_table(i/*idx*/, co_table_handle))) {
        LOG_WARN("get table failed", K(ret));
      } else if (!co_table_handle.get_table()->is_co_sstable()
        || static_cast<ObCOSSTableV2 *>(co_table_handle.get_table())->is_cgs_empty_co_table()) {
        // do nothing for row-store/cgs empty co sstable.
      } else {
        ObITable::TableKey co_key = co_table_handle.get_table()->get_key();
        ObSEArray<ObITable *, MAX_SSTABLE_CNT_IN_STORAGE> cg_sstables;

        for (int64_t idx = 0; OB_SUCC(ret) && idx < cg_tables_handle_array.get_count(); ++idx) {
          const ObITable::TableKey &cur_key = cg_tables_handle_array.get_table(idx)->get_key();
          if (cur_key.tablet_id_ != co_key.tablet_id_ ||
              cur_key.scn_range_ != co_key.scn_range_ ||
              cur_key.slice_range_ != co_key.slice_range_ ||
              cur_key.column_group_idx_ == co_key.column_group_idx_) {
            // do nothing
          } else if (OB_FAIL(cg_sstables.push_back(cg_tables_handle_array.get_table(idx)))) {
            LOG_WARN("push back cg sstable failed", K(ret));
          }
        }
        // sort cg sstables by cg idx asc
        if (FAILEDx(ObTableStoreUtil::sort_major_tables(cg_sstables))) {
          LOG_WARN("sort cg sstables failed", K(ret), K(co_key), K(cg_sstables));
        } else if (OB_FAIL(static_cast<ObCOSSTableV2 *>(co_table_handle.get_table())->fill_cg_sstables(cg_sstables))) {
          LOG_WARN("fill cg sstables failed", K(ret));
        } else {
          cmp_cg_sstables_cnt += cg_sstables.count();
        }
      }
    }
    if (OB_SUCC(ret) && with_cg_in_result_array) {
      if (cmp_cg_sstables_cnt != cg_tables_handle_array.get_count()) {
        ret = OB_ERR_SYS;
        LOG_WARN("cg sstables cnt mismatch", K(ret), K(cmp_cg_sstables_cnt), K(cg_tables_handle_array.get_count()));
      } else {
        result_tables_handle_array_.at(dest_tablet_index).reset();
        if (OB_FAIL(result_tables_handle_array_.at(dest_tablet_index).assign(organized_tables_handle_array))) {
          LOG_WARN("assign failed", K(ret));
        }
      }
    }
  }
  return ret;
}


int ObTabletSplitCtx::get_result_tables_handle_array(
    const int64_t dest_tablet_index,
    const share::ObSplitSSTableType &split_sstable_type,
    ObTablesHandleArray &tables_handle_array,
    ObTablesHandleArray &cg_tables_handle_array)
{
  int ret = OB_SUCCESS;
  tables_handle_array.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(dest_tablet_index < 0 || dest_tablet_index >= result_tables_handle_array_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(dest_tablet_index), "arr_cnt", result_tables_handle_array_.count());
  } else if (result_tables_handle_array_.at(dest_tablet_index).empty()) {
    LOG_TRACE("no result sstables", K(ret), K(dest_tablet_index), K(split_sstable_type));
  } else if (OB_FAIL(inner_organize_result_tables(split_sstable_type, dest_tablet_index, cg_tables_handle_array))) {
    // Inner organize operation will put cg sstables into co's cg_sstables array,
    // to avoid double free cg sstables when deconstructing ctx like,
    // 1. deconstruct cg_sstable.
    // 2. deconstruct co_sstable will deconstruct cg_sstable again.
    LOG_WARN("inner organize result tables failed", K(ret));
  } else {
    ObTableHandleV2 table_handle;
    ObITable *table = nullptr;
    const ObTablesHandleArray &tablet_tables_handle_array = result_tables_handle_array_.at(dest_tablet_index);
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_tables_handle_array.get_count(); i++) {
      table = nullptr;
      table_handle.reset();
      if (OB_FAIL(tablet_tables_handle_array.get_table(i/*idx*/, table_handle))) {
        LOG_WARN("get table failed", K(ret));
      } else if (OB_ISNULL(table = table_handle.get_table())) {
        ret = OB_ERR_SYS;
        LOG_WARN("error sys", K(ret), K(i), K(table_handle));
      } else if (split_sstable_type == share::ObSplitSSTableType::SPLIT_MAJOR && table->is_major_sstable()) {
        if (table->is_cg_sstable()) {
          ret = OB_ERR_SYS;
          LOG_WARN("unexpected table after organize", K(ret), KPC(table), K(tablet_tables_handle_array));
        } else if (OB_FAIL(tables_handle_array.add_table(table_handle))) {
          LOG_WARN("add table failed", K(ret));
        }
      } else if (split_sstable_type == share::ObSplitSSTableType::SPLIT_MINOR && table->is_minor_sstable()) {
        if (OB_FAIL(tables_handle_array.add_table(table_handle))) {
          LOG_WARN("add table failed", K(ret));
        }
      } else if (split_sstable_type == share::ObSplitSSTableType::SPLIT_MDS && table->is_mds_sstable()) {
        if (OB_FAIL(tables_handle_array.add_table(table_handle))) {
          LOG_WARN("add table failed", K(ret));
        }
      } else {
        LOG_TRACE("filter this table", K(ret), K(split_sstable_type), KPC(table));
      }
    }
  }
  return ret;
}

int ObTabletSplitCtx::alloc_and_init_helper(
    const ObSSTSplitHelperInitParam &init_param,
    ObSSTableSplitHelper *&sstable_split_helper)
{
  int ret = OB_SUCCESS;
  sstable_split_helper = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!init_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(init_param));
  } else if (init_param.table_key_.is_mds_sstable()) {
    sstable_split_helper = OB_NEWx(ObSpecialSplitWriteHelper, &concurrent_allocator_);
  } else {
    if (init_param.sstable_->is_column_store_sstable()) {
      sstable_split_helper = OB_NEWx(ObColSSTableSplitWriteHelper, &concurrent_allocator_);
    } else {
      sstable_split_helper = OB_NEWx(ObRowSSTableSplitWriteHelper, &concurrent_allocator_);
    }
  }
  if (OB_SUCC(ret)) {
    ObBucketHashWLockGuard guard(bucket_lock_, init_param.table_key_.hash());
    if (OB_ISNULL(sstable_split_helper)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else if (OB_FAIL(sstable_split_helper->init(init_param))) {
      LOG_WARN("init failed", K(ret));
    } else if (OB_FAIL(sstable_split_helpers_map_.set_refactored(init_param.table_key_, sstable_split_helper))) {
      LOG_WARN("set sstable split helper failed", K(ret));
    }
  }
  if (OB_FAIL(ret) && nullptr != sstable_split_helper) {
    destroy_split_object(concurrent_allocator_, sstable_split_helper);
  }
  return ret;
}

int ObTabletSplitCtx::get_sstable_helper(
    const ObITable::TableKey &table_key,
    ObSSTableSplitHelper *&helper)
{
  int ret = OB_SUCCESS;
  helper = nullptr;
  if (OB_UNLIKELY(!table_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(table_key));
  } else {
    ObBucketHashRLockGuard guard(bucket_lock_, table_key.hash());
    if (OB_FAIL(sstable_split_helpers_map_.get_refactored(table_key, helper))) {
      LOG_WARN("get sstable split helper failed", K(ret));
    } else if (OB_ISNULL(helper)) {
      ret = OB_ERR_SYS;
      LOG_WARN("error sys", K(ret), K(table_key));
    }
  }
  return ret;
}

int ObTabletSplitCtx::free_helper(const ObITable::TableKey &table_key)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!table_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(table_key));
  } else {
    ObBucketHashWLockGuard guard(bucket_lock_, table_key.hash());
    ObSSTableSplitHelper *sstable_split_helper = nullptr;
    if (OB_FAIL(sstable_split_helpers_map_.erase_refactored(table_key, &sstable_split_helper))) {
      LOG_WARN("erase sstable split helper failed", K(ret));
    } else {
      destroy_split_object(concurrent_allocator_, sstable_split_helper);
      LOG_TRACE("free sstable split helper", K(ret), K(table_key), K(common::lbt()));
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

int ObTabletSplitDag::calc_total_row_count()
{
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
  ObITask *last_task)
{
  int ret = OB_SUCCESS;
  ObSplitFinishTask *finish_task = nullptr;
  if (OB_FAIL(alloc_task(finish_task))) {
    LOG_WARN("alloc task failed", K(ret));
  } else if (OB_FAIL(finish_task->init())) {
    LOG_WARN("init failed", K(ret));
  } else if (OB_ISNULL(last_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null last", K(ret), KPC(last_task));
  } else if (OB_FAIL(last_task->add_child(*finish_task))) {
    LOG_WARN("add child task failed", K(ret));
  } else if (OB_FAIL(add_task(*finish_task))) {
    LOG_WARN("add task failed", K(ret));
  }
  return ret;
}

int ObTabletSplitDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObTabletSplitPrepareTask *prepare_task = nullptr;
  ObTabletSplitMergeTask *merge_task = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(alloc_task(prepare_task))) {
    LOG_WARN("allocate task failed", K(ret));
  } else if (OB_FAIL(alloc_task(merge_task))) {
    LOG_WARN("alloc task failed", K(ret));
  } else if (OB_UNLIKELY(nullptr == prepare_task
      || nullptr == merge_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr task", K(ret), KP(prepare_task), KP(merge_task));
  } else if (OB_FAIL(prepare_task->init(param_, context_, *merge_task))) {
    LOG_WARN("init prepare task failed", K(ret));
  } else if (OB_FAIL(merge_task->init(param_, context_))) {
    LOG_WARN("init merge task failed", K(ret));
  } else if (OB_FAIL(prepare_task->add_child(*merge_task))) {
    LOG_WARN("add child task failed", K(ret));
  } else if (OB_FAIL(add_task(*merge_task))) {
    LOG_WARN("add task failed", K(ret));
  } else if (OB_FAIL(add_task(*prepare_task))) {
    LOG_WARN("add task failed", K(ret));
  } else if (OB_FAIL(alloc_and_add_common_task(
      merge_task/*last_task*/))) {
    LOG_WARN("alloc and add common failed", K(ret));
  }
  FLOG_INFO("create first task finish", K(ret), K(param_), K(context_));
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
    char split_event_info[common::MAX_ROOTSERVICE_EVENT_VALUE_LENGTH/*512*/];
    snprintf(split_event_info, sizeof(split_event_info),
      "physical_rows_cnt: %ld, split_rows_cnt: %ld", context_.physical_row_count_, context_.row_inserted_);
    report_build_stat("replica_split_resp", context_.complement_data_ret_, split_event_info);
  }
  FLOG_INFO("send tablet split response to RS", K(ret), K(context_), K(arg));
  return ret;
}

void ObTabletSplitDag::report_build_stat(
    const char *event_name,
    const int result,
    const char *event_info) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("dag not init", K(ret));
  } else {
    bool is_split_executor = true;
    char split_basic_info[common::MAX_ROOTSERVICE_EVENT_VALUE_LENGTH/*512*/];
    memset(split_basic_info, 0, sizeof(split_basic_info));
    snprintf(split_basic_info, sizeof(split_basic_info),
      "tenant_id: %ld, ls_id: %ld, src_tablet_id: %ld, dst_tablet_ids: %ld, %ld, can_reuse_macro: %d, is_split_executor: %d",
      MTL_ID(), param_.ls_id_.id(), param_.source_tablet_id_.id(),
      param_.dest_tablets_id_.empty() ? 0 : param_.dest_tablets_id_.at(0).id(),
      param_.dest_tablets_id_.empty() ? 0 : param_.dest_tablets_id_.at(param_.dest_tablets_id_.count() - 1).id(),
      param_.can_reuse_macro_block_,
      is_split_executor);
    SERVER_EVENT_ADD("ddl", event_name,
        "result", result,
        "split_basic_info", split_basic_info,
        "trace_id", *ObCurTraceId::get_trace_id(),
        "event_info", event_info);
  }
}

int ObTabletSplitPrepareTask::init(
    ObTabletSplitParam &param,
    ObTabletSplitCtx &ctx,
    ObITask &tablet_merge_task)
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
    tablet_merge_task_ = &tablet_merge_task;
    is_inited_ = true;
  }
  return ret;
}

int ObTabletSplitPrepareTask::prepare_mds_mock_table_key(ObITable::TableKey &mock_mds_key)
{
  int ret = OB_SUCCESS;
  mock_mds_key.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    mock_mds_key.tablet_id_ = param_->source_tablet_id_;
    mock_mds_key.table_type_ = ObITable::TableType::MDS_MINI_SSTABLE;
    mock_mds_key.scn_range_.start_scn_ = SCN::base_scn();
    mock_mds_key.scn_range_.end_scn_ = context_->split_scn_;
  }
  return ret;
}

int ObTabletSplitPrepareTask::prepare_context()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(context_->prepare_schema_and_result_array(*param_))) {
    LOG_WARN("prepare schema and result array failed", K(ret));
  }
  return ret;
}

int ObTabletSplitPrepareTask::generate_next_tasks()
{
  int ret = OB_SUCCESS;
  ObITable::TableKey mock_mds_key;
  ObSEArray<ObITable *, MAX_SSTABLE_CNT_IN_STORAGE> source_sstables;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(prepare_mds_mock_table_key(mock_mds_key))) {
    LOG_WARN("prepare mds mock table key failed", K(ret));
  } else if (OB_FAIL(ObTabletSplitUtil::get_participants(
      param_->split_sstable_type_, context_->table_store_iterator_, false/*is_table_restore*/,
      context_->skipped_split_major_keys_,
      true/*filter_normal_cg_sstables*/,
      false/*filter_meta_major_sstables*/,
      source_sstables))) {
    LOG_WARN("get all sstables failed", K(ret));
  } else {
    ObSSTableSplitPrepareTask *sstable_prepare_task = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < source_sstables.count() + 1/*mds/empty_minor*/; i++) {
      sstable_prepare_task = nullptr;
      ObITable *sstable = i < source_sstables.count() ? source_sstables.at(i) : nullptr;
      const ObITable::TableKey &table_key = i < source_sstables.count() ? source_sstables.at(i)->get_key() : mock_mds_key;
      if (OB_FAIL(dag_->alloc_task(sstable_prepare_task))) {
        LOG_WARN("alloc task failed", K(ret));
      } else if (OB_ISNULL(sstable_prepare_task)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr task", K(ret));
      } else if (OB_FAIL(sstable_prepare_task->init(*param_, *context_,table_key, sstable, tablet_merge_task_))) {
        LOG_WARN("init write task failed", K(ret));
      } else if (OB_FAIL(sstable_prepare_task->add_child(*tablet_merge_task_, false/*check_child_task_status*/))) {
        LOG_WARN("add child task failed", K(ret));
      } else if (OB_FAIL(add_child(*sstable_prepare_task))) {
        LOG_WARN("add child task failed", K(ret));
      } else if (OB_FAIL(dag_->add_task(*sstable_prepare_task))) {
        LOG_WARN("add task failed", K(ret));
      }
    }
  }
  return ret;
}

int ObTabletSplitPrepareTask::process()
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(BEFORE_TABLET_SPLIT_PREPARE_TASK);
  bool is_data_split_finished = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_SUCCESS != context_->get_complement_data_ret()) {
    LOG_WARN("complement data has already failed", "ret", context_->get_complement_data_ret(), KPC(context_));
  } else if (OB_FAIL(ObTabletSplitUtil::check_dest_data_completed(
      context_->ls_handle_,
      param_->dest_tablets_id_,
      false/*check_remote*/,
      is_data_split_finished))) {
    LOG_WARN("check all major exist failed", K(ret));
  } else if (is_data_split_finished) {
    LOG_INFO("split task has alreay finished", KPC(param_));
  } else if (OB_FAIL(prepare_context())) {
    LOG_WARN("prepare index builder map failed", K(ret), KPC(param_));
  } else if (OB_FAIL(generate_next_tasks())) {
    LOG_WARN("generate sstable split tasks failed", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(static_cast<ObTabletSplitDag *>(dag_)->calc_total_row_count())) { // only calc row count once time for a task
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

  if (OB_FAIL(ret) && OB_NOT_NULL(context_)) {
    context_->set_complement_data_ret(ret);
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObSSTableSplitPrepareTask::init(
    ObTabletSplitParam &param,
    ObTabletSplitCtx &ctx,
    const ObITable::TableKey &table_key,
    storage::ObITable *table,
    ObITask *tablet_merge_task)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid()
    || !ctx.is_valid()
    || !table_key.is_valid()
    || (!table_key.is_mds_sstable() && nullptr == table)
    || nullptr == tablet_merge_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(param), K(ctx), K(table_key), KP(table), KP(tablet_merge_task));
  } else {
    param_ = &param;
    context_ = &ctx;
    table_key_ = table_key;
    sstable_ = static_cast<ObSSTable *>(table);
    tablet_merge_task_ = tablet_merge_task;
    is_inited_ = true;
  }
  return ret;
}

int ObSSTableSplitPrepareTask::generate_common_tasks(
    const ObITable::TableKey &table_key)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!table_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(table_key));
  } else {
    ObSSTableSplitWriteTask *sstable_write_task = nullptr;
    ObSSTableSplitMergeTask *sstable_merge_task = nullptr;
    if (OB_FAIL(dag_->alloc_task(sstable_write_task))) {
      LOG_WARN("alloc task failed", K(ret));
    } else if (OB_FAIL(dag_->alloc_task(sstable_merge_task))) {
      LOG_WARN("alloc task failed", K(ret));
    } else if (OB_ISNULL(sstable_write_task) || OB_ISNULL(sstable_merge_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr task", K(ret), KP(sstable_write_task), KP(sstable_merge_task));
    } else if (OB_FAIL(sstable_write_task->init(0/*task_id*/, *param_, *context_, table_key))) {
      LOG_WARN("init write task failed", K(ret));
    } else if (OB_FAIL(sstable_merge_task->init(*param_, *context_, table_key))) {
      LOG_WARN("init failed", K(ret));
    } else if (OB_FAIL(sstable_merge_task->add_child(*tablet_merge_task_, false/*check_child_task_status*/))) {
      LOG_WARN("add child failed", K(ret));
    } else if (OB_FAIL(sstable_write_task->add_child(*sstable_merge_task))) {
      LOG_WARN("add child failed", K(ret));
    } else if (OB_FAIL(this->add_child(*sstable_write_task))) {
      LOG_WARN("add child failed", K(ret));
    } else if (OB_FAIL(dag_->add_task(*sstable_merge_task))) {
      LOG_WARN("add task failed", K(ret));
    } else if (OB_FAIL(dag_->add_task(*sstable_write_task))) {
      LOG_WARN("add task failed", K(ret));
    }
  }
  return ret;
}

int ObSSTableSplitPrepareTask::generate_tasks_for_packed_sstable(
    const ObSSTableSplitHelper &helper)
{
  int ret = OB_SUCCESS;
  ObArray<ObSSTableWrapper> cg_table_wrappers;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!param_->can_reuse_macro_block_ || nullptr == sstable_ || !sstable_->is_co_sstable())) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys", K(ret), "can_reuse_macro_block", param_->can_reuse_macro_block_, KPC(sstable_));
  } else if (OB_FAIL(static_cast<ObCOSSTableV2 *>(sstable_)->get_all_tables(cg_table_wrappers/*contain co itself*/))) {
    LOG_WARN("get all co sstables failed", K(ret));
  } else {
    const ObColSSTableSplitWriteHelper &co_helper = static_cast<const ObColSSTableSplitWriteHelper &>(helper);
    const ObIArray<ObCSRowId> &end_partkey_rowids = co_helper.get_end_partkey_rowids();
    for (int64_t i = 0; OB_SUCC(ret) && i < cg_table_wrappers.count(); i++) {
      ObSSTable *cg_sstable = nullptr;
      if (OB_FAIL(cg_table_wrappers.at(i).get_loaded_column_store_sstable(cg_sstable))) {
        LOG_WARN("get loaded column store sstable failed", K(ret), K(i), K(cg_table_wrappers));
      } else if (OB_ISNULL(cg_sstable)) {
        ret = OB_ERR_SYS;
        LOG_WARN("unexpected co sstable", K(ret), KPC(cg_sstable), KPC(sstable_));
      } else if (cg_sstable->get_key() == sstable_->get_key()) {
        LOG_TRACE("ignore co itself", K(ret), KPC(cg_sstable), KPC(sstable_));
      } else {
        ObSSTableSplitHelper *cg_helper = nullptr;
        ObColSSTSplitHelperInitParam cg_helper_init_param;
        cg_helper_init_param.param_ = param_;
        cg_helper_init_param.context_ = context_;
        cg_helper_init_param.table_key_ = cg_sstable->get_key();
        cg_helper_init_param.sstable_ = cg_sstable;
        if (OB_UNLIKELY(end_partkey_rowids.empty())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected empty end partkey rowids", K(ret), K(co_helper));
        } else if (OB_FAIL(cg_helper_init_param.end_partkey_rowids_.assign(end_partkey_rowids))) {
          LOG_WARN("assign end partkey rowids failed", K(ret));
        } else if (OB_FAIL(context_->alloc_and_init_helper(cg_helper_init_param, cg_helper))) {
          LOG_WARN("alloc and init helper failed", K(ret));
        } else if (OB_ISNULL(cg_helper)) {
          ret = OB_ERR_SYS;
          LOG_WARN("error sys", K(ret), K(cg_sstable->get_key()));
        } else if (OB_FAIL(generate_common_tasks(cg_sstable->get_key()))) {
          LOG_WARN("generate common tasks failed", K(ret), KPC(cg_sstable));
        }
      }
    }
  }
  return ret;
}

int ObSSTableSplitPrepareTask::generate_next_tasks()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObSSTableSplitHelper *helper = nullptr;
    ObSSTSplitHelperInitParam rs_helper_init_param;
    ObColSSTSplitHelperInitParam cs_helper_init_param;
    ObSSTSplitHelperInitParam *helper_init_param = table_key_.is_column_store_sstable() ? &cs_helper_init_param : &rs_helper_init_param;
    helper_init_param->param_ = param_;
    helper_init_param->context_ = context_;
    helper_init_param->table_key_ = table_key_;
    helper_init_param->sstable_ = sstable_;
    if (OB_FAIL(context_->alloc_and_init_helper(*helper_init_param, helper))) {
      LOG_WARN("alloc and init helper failed", K(ret));
    } else if (OB_ISNULL(helper)) {
      ret = OB_ERR_SYS;
      LOG_WARN("error sys", K(ret), K(table_key_));
    } else if (OB_FAIL(generate_common_tasks(table_key_))) {
      LOG_WARN("generate common tasks failed", K(ret));
    } else if (table_key_.is_column_store_sstable()) {
      if (OB_FAIL(generate_tasks_for_packed_sstable(*helper))) {
        LOG_WARN("generate tasks for packed sstable failed", K(ret));
      }
    }
  }
  return ret;
}

int ObSSTableSplitPrepareTask::process()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(OB_SUCCESS != context_->get_complement_data_ret())) {
    LOG_WARN("complement data has already failed", "ret", context_->get_complement_data_ret(), KPC(context_));
  } else if (OB_FAIL(generate_next_tasks())) {
    LOG_WARN("generate next tasks failed", K(ret));
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(context_)) {
    context_->set_complement_data_ret(ret);
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObSSTableSplitWriteTask::init(
    const int64_t task_idx,
    ObTabletSplitParam &param,
    ObTabletSplitCtx &ctx,
    const ObITable::TableKey &table_key)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(task_idx < 0 || task_idx >= ctx.data_split_ranges_.count()
      || !param.is_valid()
      || !ctx.is_valid()
      || !table_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(task_idx), K(param), K(ctx), K(table_key));
  } else {
    task_idx_ = task_idx;
    param_ = &param;
    context_ = &ctx;
    table_key_ = table_key;
    is_inited_ = true;
  }
  return ret;
}

int ObSSTableSplitWriteTask::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  next_task = nullptr;
  ObSSTableSplitWriteTask *next_write_task = nullptr;
  const int64_t next_task_idx = task_idx_ + 1;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (param_->can_reuse_macro_block_ || next_task_idx >= context_->data_split_ranges_.count()) {
    ret = OB_ITER_END;
    LOG_TRACE("iter end", K(ret), K(next_task_idx), KPC(this));
  } else if (OB_FAIL(dag_->alloc_task(next_write_task))) {
    LOG_WARN("alloc task failed", K(ret));
  } else if (OB_FAIL(next_write_task->init(next_task_idx, *param_, *context_, table_key_))) {
    LOG_WARN("init next write task failed", K(ret));
  } else {
    next_task = next_write_task;
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(context_)) {
    if (OB_ITER_END != ret) {
      context_->set_complement_data_ret(ret);
    }
  }
  return ret;
}

int ObSSTableSplitWriteTask::process()
{
  int ret = OB_SUCCESS;
  ObArenaAllocator arena_allocator("SplitData", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  DEBUG_SYNC(BEFORE_TABLET_SPLIT_WRITE_TASK);
  ObSSTableSplitHelper *helper = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_SUCCESS != context_->get_complement_data_ret()) {
    LOG_WARN("complement data has already failed", "ret", context_->get_complement_data_ret(), KPC(context_));
  } else if (OB_FAIL(context_->get_sstable_helper(table_key_, helper))) {
    LOG_WARN("get sstable helper failed", K(ret));
  } else if (OB_ISNULL(helper)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys", K(ret), K(table_key_));
  } else if (OB_FAIL(helper->split_data(arena_allocator, task_idx_))) {
    LOG_WARN("split data failed", K(ret));
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(context_)) {
    context_->set_complement_data_ret(ret);
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObSSTableSplitMergeTask::init(
    ObTabletSplitParam &param,
    ObTabletSplitCtx &ctx,
    const ObITable::TableKey &table_key)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid()
      || !ctx.is_valid()
      || !table_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(param), K(ctx), K(table_key));
  } else {
    param_ = &param;
    context_ = &ctx;
    table_key_ = table_key;
    is_inited_ = true;
  }
  return ret;
}

int ObSSTableSplitMergeTask::process()
{
  int ret = OB_SUCCESS;
  ObSSTableSplitHelper *helper = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_SUCCESS != context_->get_complement_data_ret()) {
    LOG_WARN("complement data has already failed", "ret", context_->get_complement_data_ret(), KPC(context_));
  } else if (OB_FAIL(context_->get_sstable_helper(table_key_, helper))) {
    LOG_WARN("get sstable helper failed", K(ret));
  } else if (OB_ISNULL(helper)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys", K(ret), K(table_key_));
  } else if (OB_FAIL(helper->generate_sstable())) {
    LOG_WARN("generate sstable failed", K(ret));
  }
  if (OB_NOT_NULL(context_)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(context_->free_helper(table_key_))) {
      LOG_WARN("free helper failed", K(ret), K(tmp_ret), K(table_key_));
    }
    ret = OB_SUCC(ret) ? tmp_ret : ret;
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(context_)) {
    context_->set_complement_data_ret(ret);
    ret = OB_SUCCESS;
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
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_SUCCESS != context_->get_complement_data_ret()) {
    LOG_WARN("complement data has already failed", "ret", context_->get_complement_data_ret(), KPC(context_));
  } else if (OB_FAIL(ObTabletSplitUtil::check_dest_data_completed(
      context_->ls_handle_,
      param_->dest_tablets_id_,
      false/*check_remote*/,
      is_data_split_finished))) {
    LOG_WARN("check all major exist failed", K(ret));
  } else if (is_data_split_finished) {
    LOG_INFO("split task has alreay finished", KPC(param_));
  } else if (OB_FAIL(collect_and_update_sstable(share::ObSplitSSTableType::SPLIT_MINOR))) {
    LOG_WARN("collect and update sstable failed", K(ret));
  } else if (OB_FAIL(collect_and_update_sstable(share::ObSplitSSTableType::SPLIT_MDS))) {
    LOG_WARN("collect and update sstable failed", K(ret));
  } else {
    DEBUG_SYNC(BEFORE_TABLET_SPLIT_MAJOR_SSTABLE);
    if (OB_FAIL(collect_and_update_sstable(share::ObSplitSSTableType::SPLIT_MAJOR))) {
      LOG_WARN("collect and update sstable failed", K(ret));
    }
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(context_)) {
    context_->set_complement_data_ret(ret);
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObTabletSplitMergeTask::check_cg_sstables_checksum(
    const share::ObSplitSSTableType &split_sstable_type,
    const ObTablesHandleArray &batch_sstables_handle)
{
  int ret = OB_SUCCESS;
  common::ObArray<ObColDesc> column_descs;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(batch_sstables_handle.empty() || share::ObSplitSSTableType::SPLIT_MAJOR != split_sstable_type)) {
    // do nothing.
  } else if (OB_FAIL(context_->mds_storage_schema_->get_multi_version_column_descs(column_descs))) {
    LOG_WARN("get multi version column descs failed", K(ret));
  } else {
    ObArray<int64_t> all_column_checksums;
    ObITable *table = nullptr;
    ObTableHandleV2 table_handle;
    common::ObArray<ObSSTableWrapper> cg_tables_wrappers;
    const int64_t mv_stored_column_count = column_descs.count();
    if (OB_FAIL(all_column_checksums.prepare_allocate(mv_stored_column_count))) {
      LOG_WARN("reserve failed", K(ret));
    }
    common::ObArenaAllocator tmp_arena("SplitCkms", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    const ObStorageSchema *clipped_storage_schema = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_sstables_handle.get_count(); i++) { // iterate each co.
      table = nullptr;
      table_handle.reset();
      cg_tables_wrappers.reset();
      clipped_storage_schema = nullptr;
      tmp_arena.reuse();
      for (int64_t j = 0; OB_SUCC(ret) && j < mv_stored_column_count; j++) {
        all_column_checksums.at(j) = 0;
      }
      if (OB_FAIL(batch_sstables_handle.get_table(i/*idx*/, table_handle))) {
        LOG_WARN("get table failed", K(ret));
      } else if (OB_ISNULL(table = table_handle.get_table())) {
        ret = OB_ERR_SYS;
        LOG_WARN("error sys", K(ret), K(i), K(table_handle));
      } else if (!table->is_co_sstable()) {
        // do nothing.
      } else if (OB_FAIL(static_cast<ObCOSSTableV2 *>(table)->get_all_tables(cg_tables_wrappers))) { // contain co itself.
        LOG_WARN("get all tables failed", K(ret));
      } else if (OB_FAIL(ObTabletSplitUtil::get_clipped_storage_schema_on_demand(tmp_arena,
          param_->source_tablet_id_,
          *static_cast<ObSSTable *>(table),
          *context_->mds_storage_schema_,
          clipped_storage_schema))) {
        LOG_WARN("get clipped storage schema failed", K(ret));
      } else if (OB_UNLIKELY(nullptr == clipped_storage_schema || !clipped_storage_schema->is_valid())) {
        ret = OB_ERR_SYS;
        LOG_WARN("sys error to get a null schema", K(ret), KPC(table), KPC(context_->mds_storage_schema_), KPC(clipped_storage_schema));
      } else {
        ObSSTableMetaHandle cg_table_meta_hdl;
        for (int64_t j = 0; OB_SUCC(ret) && j < cg_tables_wrappers.count(); j++) { // iterate each cg.
          uint16_t cg_idx = 0;
          const ObSSTable *cg_sstable = cg_tables_wrappers.at(j).get_sstable();
          const ObStorageColumnGroupSchema *column_group = nullptr;
          cg_table_meta_hdl.reset();
          if (OB_UNLIKELY(nullptr == cg_sstable || !cg_sstable->is_column_store_sstable())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected cg table", K(ret), KPC(cg_sstable));
          } else if (FALSE_IT(cg_idx = cg_sstable->get_column_group_id())) {
          } else if (OB_FAIL(clipped_storage_schema->get_cg_schema_with_column_group_idx(cg_idx, column_group))) {
            LOG_WARN("fail to get column group schema", K(ret), K(cg_idx), K(clipped_storage_schema));
          } else if (OB_UNLIKELY(nullptr == column_group)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected cg schema", K(ret), K(cg_idx), KPC(clipped_storage_schema));
          } else {
            ObStorageColumnGroupSchema mock_row_store_cg;
            const bool is_all_co_sstable = cg_sstable->is_co_sstable() && static_cast<const ObCOSSTableV2 *>(cg_sstable)->is_all_cg_base();
            if (is_all_co_sstable && column_group->is_rowkey_column_group()) {
              column_group = nullptr;
              if (OB_FAIL(clipped_storage_schema->mock_row_store_cg(mock_row_store_cg))) {
                LOG_WARN("mock row store cg failed", K(ret));
              } else {
                column_group = &mock_row_store_cg;
              }
            }
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(cg_sstable->get_meta(cg_table_meta_hdl))) {
              LOG_WARN("fail to get meta", K(ret), KPC(cg_sstable));
            } else if (OB_UNLIKELY(cg_table_meta_hdl.get_sstable_meta().get_col_checksum_cnt() != column_group->get_column_count())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected col_checksum_cnt", K(ret), K(cg_idx),
                  "sstable_cols_ckm_cnt", cg_table_meta_hdl.get_sstable_meta().get_col_checksum_cnt(),
                  KPC(column_group), KPC(clipped_storage_schema));
            } else {
              const int cnt = min(cg_table_meta_hdl.get_sstable_meta().get_col_checksum_cnt(), column_group->get_column_count());
              for (int64_t k = 0; OB_SUCC(ret) && k < cnt; k++) { // iterate each column ckm.
                const int64_t column_idx = column_group->get_column_idx(k);
                if (OB_UNLIKELY(column_idx >= mv_stored_column_count)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("unexpected column idx", K(ret), K(column_idx), K(mv_stored_column_count));
                } else if (all_column_checksums.at(column_idx) == 0) {
                  all_column_checksums.at(column_idx) = cg_table_meta_hdl.get_sstable_meta().get_col_checksum()[k];
                } else if (OB_UNLIKELY(all_column_checksums.at(column_idx) != cg_table_meta_hdl.get_sstable_meta().get_col_checksum()[k])) {
                  ret = OB_CHECKSUM_ERROR;
                  LOG_ERROR("catch split checksum error", K(ret),
                      K(cg_idx),
                      K(column_idx),
                      "ckm1", all_column_checksums.at(column_idx),
                      "ckm2", cg_table_meta_hdl.get_sstable_meta().get_col_checksum()[k],
                      K(all_column_checksums),
                      KPC(cg_sstable),
                      "co_sstable", PC(static_cast<ObCOSSTableV2 *>(table)));
                }
              } // END FOR K.
            }
          }
        } // END FOR J.
      }
    } // END FOR I.
  }
  return ret;
}

int ObTabletSplitMergeTask::collect_and_update_sstable(
    const share::ObSplitSSTableType &split_sstable_type)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(share::ObSplitSSTableType::SPLIT_MAJOR != split_sstable_type
      && share::ObSplitSSTableType::SPLIT_MINOR != split_sstable_type
      && share::ObSplitSSTableType::SPLIT_MDS != split_sstable_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(split_sstable_type), KPC(param_));
  } else {
    ObTablesHandleArray batch_sstables_handle, cg_sstables_handle/*to hold cgs' macro ref before updating table store*/;
    ObSEArray<ObTabletID, 1> check_major_exist_tablets;
    const compaction::ObMergeType merge_type = share::ObSplitSSTableType::SPLIT_MINOR == split_sstable_type ?
        compaction::ObMergeType::MINOR_MERGE : share::ObSplitSSTableType::SPLIT_MDS == split_sstable_type ?
        compaction::ObMergeType::MDS_MINI_MERGE : compaction::ObMergeType::MAJOR_MERGE;
    for (int64_t i = 0; OB_SUCC(ret) && i < param_->dest_tablets_id_.count(); i++) {
      cg_sstables_handle.reset();
      batch_sstables_handle.reset();
      check_major_exist_tablets.reset();
      bool is_data_split_finished = false;
      const int64_t dest_tablet_index = i;
      const ObTabletID &dest_tablet_id = param_->dest_tablets_id_.at(i);
      if (OB_FAIL(check_major_exist_tablets.push_back(dest_tablet_id))) {
        LOG_WARN("push back failed", K(ret));
      } else if (OB_FAIL(ObTabletSplitUtil::check_dest_data_completed(
          context_->ls_handle_,
          check_major_exist_tablets,
          false/*check_remote*/,
          is_data_split_finished))) {
        LOG_WARN("check all major exist failed", K(ret));
      } else if (is_data_split_finished) {
        FLOG_INFO("skip to create sstable", K(ret), K(dest_tablet_id));
      } else if (OB_FAIL(context_->get_result_tables_handle_array(dest_tablet_index, split_sstable_type, batch_sstables_handle, cg_sstables_handle))) {
        LOG_WARN("get result tables handle array failed", K(ret));
      } else if (OB_FALSE_IT(check_cg_sstables_checksum(split_sstable_type, batch_sstables_handle))) {
        // ignore ret_code is expected.
        LOG_WARN("check cg sstables checksum failed", K(ret));
      } else if (batch_sstables_handle.empty() && !is_major_merge_type(merge_type)) {
        // empty major result should also need to swap tablet, to update data_split_status and restore status.
        LOG_TRACE("no need to update table store", K(ret), K(dest_tablet_id), K(split_sstable_type));
      } else {
        if (FAILEDx(ObTabletSplitMergeTask::update_table_store_with_batch_tables(
              context_->ls_rebuild_seq_,
              context_->ls_handle_,
              context_->tablet_handle_,
              dest_tablet_id,
              batch_sstables_handle,
              merge_type,
              context_->skipped_split_major_keys_,
              context_->reorg_scn_))) {
          LOG_WARN("update table store with batch tables failed", K(ret), K(batch_sstables_handle), K(split_sstable_type));
        }
      }
    }
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
  } else {
    if (OB_FAIL(ls_handle.get_ls()->build_tablet_with_batch_tables(dst_tablet_id, param))) {
      LOG_WARN("failed to update tablet table store", K(ret), K(dst_tablet_id), K(param));
    }
  }
  FLOG_INFO("update batch sstables", K(ret), K(dst_tablet_id), K(param));

  return ret;
}


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

} //end namespace stroage
} //end namespace oceanbase
