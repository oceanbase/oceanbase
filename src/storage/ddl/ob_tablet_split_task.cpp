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
    parallel_cnt_of_each_sstable_(-1),
    split_scn_(), reorg_scn_(), ls_rebuild_seq_(-1),
    split_majors_count_(-1), max_major_snapshot_(-1)
  #ifdef OB_BUILD_SHARED_STORAGE
    , ss_minor_split_helper_(), ss_mds_split_helper_(), is_data_split_executor_(false)
  #endif
{
  result_tables_handle_array_.set_allocator(&arena_allocator_);
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
  return is_inited_ && ls_handle_.is_valid() && tablet_handle_.is_valid()
    && parallel_cnt_of_each_sstable_ >= 1;
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
      param.dest_tablets_id_, param.compaction_scn_, param.min_split_start_scn_
#ifdef OB_BUILD_SHARED_STORAGE
      , is_data_split_executor_
#endif
      ))) {
    if (OB_NEED_RETRY != ret) {
      LOG_WARN("check satisfy split condition failed", K(ret), K(param));
    }
  } else if (OB_FAIL(ObTabletSplitUtil::get_tablet(arena_allocator_, ls_handle_,
      param.source_tablet_id_, GCTX.is_shared_storage_mode()/*is_shared_mode*/,
      tablet_handle_,
      ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    LOG_WARN("get tablet failed", K(ret), "tablet_id", param.source_tablet_id_, K(GCTX.is_shared_storage_mode()));
  } else if (OB_FAIL(tablet_handle_.get_obj()->get_all_tables(table_store_iterator_, true/*need_unpack*/))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_FAIL(get_split_majors_infos())) {
    LOG_WARN("get split majors infos failed", K(ret));
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
    parallel_cnt_of_each_sstable_ = param.can_reuse_macro_block_ ?
        1 : data_split_ranges_.count();
    if (OB_FAIL(bucket_lock_.init(MAX_SSTABLE_CNT_IN_STORAGE/*bucket_nums*/, common::ObLatchIds::TABLET_SPLIT_SSTABLE_HELPERS_LOCK))) {
      LOG_WARN("init bucket lock failed", K(ret));
    } else if (OB_FAIL(sstable_split_helpers_map_.create(MAX_SSTABLE_CNT_IN_STORAGE/*bucket_nums*/, "SplitHelperMap"))) {
      LOG_WARN("create sstable split helpers map failed", K(ret));
    }
    is_inited_ = true;
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
      false/*filter_normal_cg_sstables*/,
      major_sstables))) {
    LOG_WARN("get participant sstables failed", K(ret));
  } else if (OB_UNLIKELY(major_sstables.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no major sstables", K(ret), KPC(this));
  } else {
    split_majors_count_ = major_sstables.count(); // including normal cg sstables.
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
    int64_t &sstable_index) const
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
      false/*filter_normal_cg_sstables*/,
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
  const compaction::ObTabletMergeCtx &tablet_merge_ctx,
  const int64_t dest_tablet_index,
  int64_t index_tree_start_seq,
  ObMdsTableMiniMerger &mds_mini_merger)
{
  UNUSED(tablet_merge_ctx); // guarantee mds_mini_merger validity
  int ret = OB_SUCCESS;
  ObTableHandleV2 table_handle;
  ObSpinLockGuard guard(lock_);
  if (OB_UNLIKELY(dest_tablet_index < 0 || dest_tablet_index >= result_tables_handle_array_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(dest_tablet_index), "arr_cnt", result_tables_handle_array_.count());
  } else if (GCTX.is_shared_storage_mode()) {
    if (OB_FAIL(mds_mini_merger.generate_ss_mds_mini_sstable(arena_allocator_, table_handle, index_tree_start_seq))) {
      LOG_WARN("fail to generate ss mds mini sstable with mini merger", K(ret), K(mds_mini_merger));
    }
  } else {
    if (OB_FAIL(mds_mini_merger.generate_mds_mini_sstable(arena_allocator_, table_handle))) {
      LOG_WARN("fail to generate mds mini sstable with mini merger", K(ret), K(mds_mini_merger));
    }
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
        ObITable::TableKey table_key = co_table_handle.get_table()->get_key();
        const uint16_t base_cg_idx = table_key.get_column_group_id();
        ObArray<ObITable *> cg_sstables;
        for (int64_t cg_idx = 0; OB_SUCC(ret) && cg_idx < mds_storage_schema_->get_column_group_count(); cg_idx++) { // sorted by cg_idx.
          cg_table_handle.reset();
          table_key.column_group_idx_ = cg_idx;
          table_key.table_type_ = ObITable::TableType::NORMAL_COLUMN_GROUP_SSTABLE;
          if (cg_idx == base_cg_idx) {
            // do nothing
          } else if (OB_FAIL(cg_tables_handle_array.get_table(table_key, cg_table_handle))) {
            if (OB_ENTRY_NOT_EXIST != ret) {
              LOG_WARN("get table failed", K(ret), K(table_key));
            } else {
              ret = OB_SUCCESS;
            }
          } else if (OB_FAIL(cg_sstables.push_back(cg_table_handle.get_table()))) {
            LOG_WARN("push back cg sstable failed", K(ret));
          }
        }
        if (FAILEDx(static_cast<ObCOSSTableV2 *>(co_table_handle.get_table())->fill_cg_sstables(cg_sstables))) {
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
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (GCTX.is_shared_storage_mode()) {
    if (init_param.sstable_->is_column_store_sstable()) {
      sstable_split_helper = OB_NEWx(ObSSColSSTableSplitWriteHelper, &concurrent_allocator_);
    } else {
      sstable_split_helper = OB_NEWx(ObSSRowSSTableSplitWriteHelper, &concurrent_allocator_);
    }
#endif
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
  if (FAILEDx(add_task(*finish_task))) {
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
      "src_tablet_id", param_.source_tablet_id_);
#endif
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
      merge_task/*last_task*/,
      context_.ls_rebuild_seq_,
      param_.ls_id_,
      param_.source_tablet_id_,
      param_.dest_tablets_id_,
      param_.can_reuse_macro_block_,
      context_.reorg_scn_,
      context_.split_scn_))) {
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
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (GCTX.is_shared_storage_mode()) {
    ObSSDataSplitHelper &ss_minor_split_helper = context_->ss_minor_split_helper_;
    if (OB_FAIL(ss_minor_split_helper.start_add_minor_op(
        param_->ls_id_,
        context_->split_scn_,
        context_->parallel_cnt_of_each_sstable_,
        context_->table_store_iterator_,
        param_->dest_tablets_id_))) {
      LOG_WARN("start add minor op failed", K(ret), KPC(param_));
    } else if (OB_FAIL(ss_minor_split_helper.persist_majors_gc_rely_info(
        param_->ls_id_,
        param_->dest_tablets_id_,
        context_->reorg_scn_,
        context_->split_majors_count_,
        context_->max_major_snapshot_,
        context_->parallel_cnt_of_each_sstable_/*parallel_cnt*/))) {
      LOG_WARN("persist majors gc rely info failed", K(ret), KPC_(param), KPC(context_));
    } else {
      ObSSDataSplitHelper &ss_mds_split_helper = context_->ss_mds_split_helper_;
      if (OB_FAIL(ss_mds_split_helper.start_add_mds_op(
          param_->ls_id_,
          context_->split_scn_,
          1/*parallel_cnt_of_each_sstable*/,
          1/*sstables_cnt*/,
          param_->dest_tablets_id_))) {
        LOG_WARN("start add mds op failed", K(ret), KPC(param_));
      }
    }
#endif
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
      GCTX.is_shared_storage_mode()/*check_remote*/,
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
          cg_table_meta_hdl.reset();
          if (OB_UNLIKELY(nullptr == cg_sstable || !cg_sstable->is_column_store_sstable())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected cg table", K(ret), KPC(cg_sstable));
          } else if (FALSE_IT(cg_idx = cg_sstable->get_column_group_id())) {
          } else if (OB_UNLIKELY(cg_idx < 0 || cg_idx >= clipped_storage_schema->get_column_groups().count())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected cg idx", K(ret), K(cg_idx), KPC(clipped_storage_schema));
          } else {
            ObStorageColumnGroupSchema mock_row_store_cg;
            const ObStorageColumnGroupSchema *column_group = &clipped_storage_schema->get_column_groups().at(cg_idx);
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
  #ifdef OB_BUILD_SHARED_STORAGE
    ObSSDataSplitHelper &ss_split_helper = is_minor_merge_type(merge_type) ?
        context_->ss_minor_split_helper_ : context_->ss_mds_split_helper_;
  #endif
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
          GCTX.is_shared_storage_mode()/*check_remote*/,
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
        int64_t op_id = -1;
      #ifdef OB_BUILD_SHARED_STORAGE
        if (GCTX.is_shared_storage_mode() && !is_major_merge_type(merge_type)) {
          if (OB_FAIL(ss_split_helper.get_op_id(dest_tablet_index, op_id))) {
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
              && OB_TMP_FAIL(ss_split_helper.finish_add_op(
            dest_tablet_index/*dest_tablet_index*/,
            OB_SUCC(ret)/*need_finish*/))) {
          LOG_WARN("finish add op failed", K(ret), K(tmp_ret));
        }
      #endif
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
    ObSSTable *sstable = nullptr;
    ObArray<ObSSTable *> unpacked_sstables;
    ObArray<ObSSTableWrapper> cg_table_wrappers;
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_tables.count(); i++) {
      sstable = nullptr;
      unpacked_sstables.reset();
      cg_table_wrappers.reset();
      if (OB_ISNULL(sstable = static_cast<ObSSTable *>(batch_tables.at(i)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null split sstable", K(ret), K(batch_sstables_handle));
      } else if (!sstable->is_co_sstable()) {
        if (OB_FAIL(unpacked_sstables.push_back(sstable))) {
          LOG_WARN("push back sstable failed", K(ret));
        }
      } else if (OB_FAIL(static_cast<ObCOSSTableV2 *>(sstable)->get_all_tables(cg_table_wrappers))) {
        LOG_WARN("get all co sstables failed", K(ret));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < cg_table_wrappers.count(); j++) {
          ObSSTable *cg_sstable = nullptr;
          if (OB_FAIL(cg_table_wrappers.at(j).get_loaded_column_store_sstable(cg_sstable))) {
            LOG_WARN("get loaded column store sstable failed", K(ret), K(i), K(cg_table_wrappers));
          } else if (OB_UNLIKELY(nullptr == cg_sstable || !cg_sstable->is_column_store_sstable())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected cg table", K(ret), KPC(cg_sstable));
          } else if (OB_FAIL(unpacked_sstables.push_back(cg_sstable))) {
            LOG_WARN("push back cg sstable failed", K(ret));
          }
        }
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < unpacked_sstables.count(); j++) {
        bool stop = false;
        ObSSTable *iter_sstable = unpacked_sstables.at(j);
        storage::ObSSTableMacroPrewarmer prewarmer(iter_sstable, stop, ObSSMicroCacheAccessType::SPLIT_PART_PREWARM_TYPE);
        if (OB_FAIL(prewarmer.do_prewarm(ss_tablet_handle))) {
          LOG_WARN("failed to do prewarm", K(ret), K(ss_tablet_handle));
        } else if (!iter_sstable->is_mds_sstable() && OB_FAIL(prewarm_for_split(ss_tablet_handle, *iter_sstable))) {
          LOG_WARN("failed to prewarm for split", K(ret), K(ss_tablet_handle));
        }
      }
    }
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(get_dag())) {
    static_cast<ObIDataSplitDag *>(get_dag())->report_build_stat("split_prewarm", ret);
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
      } else if (OB_FAIL(micro_iter.open(sstable, whole_range, *read_info, allocator, true))) {
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
  } else if (OB_FAIL(DDL_SIM_WHEN(true/*condition*/, MTL_ID(), 1/*ddl_task_id*/, SPLIT_DOWNLOAD_SSTABLE_SLOW))) {
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
      true/*filter_normal_cg_sstables*/,
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
          } else if (OB_FAIL(ss_tablet_handle.get_obj()->get_all_tables(table_store_iterator, false/*need_unpack*/))) {
            LOG_WARN("fail to fetch table store", K(ret));
          } else if (OB_FAIL(collect_split_sstables(tmp_arena, split_sstable_type,
                table_store_iterator,
                batch_sstables_handle))) {
            LOG_WARN("collect split sstables failed", K(ret), K(ss_tablet_handle));
          } else if (!batch_sstables_handle.empty()) {
            // Try to warm up the cache, but success is not guaranteed.
            prewarm(ss_tablet_handle, batch_sstables_handle);
            if (OB_FAIL(ObTabletSplitUtil::build_update_table_store_param(
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
        if (OB_NOT_NULL(get_dag())) {
          char split_event_info[common::MAX_ROOTSERVICE_EVENT_VALUE_LENGTH/*512*/];
          snprintf(split_event_info, sizeof(split_event_info),
            "tablet_id: %ld, snapshot: %ld, mult_version_start: %ld, tablet_versions: %ld, %ld, %ld",
            dst_tablet_id.id(), snapshot_version, mult_version_start,
            target_tablet_versions.count() > 0 ? target_tablet_versions.at(0).get_val_for_gts() : -1/*minor_version*/,
            target_tablet_versions.count() > 1 ? target_tablet_versions.at(1).get_val_for_gts() : -1/*mds_version*/,
            target_tablet_versions.count() > 2 ? target_tablet_versions.at(2).get_val_for_gts() : -1/*major_version*/);
          static_cast<ObIDataSplitDag *>(get_dag())->report_build_stat("split_download", ret, split_event_info);
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

} //end namespace stroage
} //end namespace oceanbase
