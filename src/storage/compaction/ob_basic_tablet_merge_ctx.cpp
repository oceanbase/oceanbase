//Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "storage/compaction/ob_basic_tablet_merge_ctx.h"
#include "storage/compaction/ob_medium_compaction_func.h"
#include "storage/compaction/ob_compaction_suggestion.h"
#include "storage/compaction/ob_sstable_merge_info_mgr.h"
#include "storage/compaction/ob_partition_merge_policy.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_medium_info_reader.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "storage/ob_tenant_tablet_stat_mgr.h"
#include "storage/blocksstable/ob_data_store_desc.h"
#include "storage/ob_storage_schema_util.h"
#include "storage/ob_gc_upper_trans_helper.h"
#include "ob_medium_list_checker.h"
#include "share/schema/ob_tenant_schema_service.h"
#include "storage/tablet/ob_mds_schema_helper.h"
#include "storage/tablet/ob_mds_scan_param_helper.h"

namespace oceanbase
{
using namespace memtable;
namespace compaction
{
ObStaticMergeParam::ObStaticMergeParam(ObTabletMergeDagParam &dag_param)
  : dag_param_(dag_param),
    is_full_merge_(false),
    is_rebuild_column_store_(false),
    is_schema_changed_(false),
    need_parallel_minor_merge_(true),
    is_tenant_major_merge_(false),
    merge_level_(MICRO_BLOCK_MERGE_LEVEL),
    merge_reason_(ObAdaptiveMergePolicy::AdaptiveMergeReason::NONE),
    co_major_merge_type_(ObCOMajorMergePolicy::INVALID_CO_MAJOR_MERGE_TYPE),
    major_sstable_status_(ObCOMajorSSTableStatus::INVALID_CO_MAJOR_SSTABLE_STATUS),
    sstable_logic_seq_(0),
    ls_handle_(),
    tables_handle_(MTL_ID()),
    progressive_merge_round_(0),
    progressive_merge_num_(0),
    progressive_merge_step_(0),
    concurrent_cnt_(0),
    data_version_(0),
    ls_rebuild_seq_(-1),
    read_base_version_(0),
    create_snapshot_version_(0),
    start_time_(0),
    merge_scn_(),
    version_range_(),
    scn_range_(),
    rowkey_read_info_(nullptr),
    schema_version_(0),
    schema_(nullptr),
    report_(nullptr),
    snapshot_info_(),
    tx_id_(0),
    multi_version_column_descs_(),
    is_backfill_(false)
{
  merge_scn_.set_max();
}

void ObStaticMergeParam::reset()
{
  tables_handle_.reset();
  report_ = nullptr;
  rowkey_read_info_ = nullptr;
  co_major_merge_type_ = ObCOMajorMergePolicy::INVALID_CO_MAJOR_MERGE_TYPE;
  multi_version_column_descs_.reset();
  ls_handle_.reset(); // ls_handle could release before tablet_handle
  tx_id_ = 0;
}

bool ObStaticMergeParam::is_valid() const
{
  bool bret = false;
  if (OB_UNLIKELY(!dag_param_.is_valid())) {
    bret = false;
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "dag param is invalid", K_(dag_param), K(dag_param_.is_valid()));
  } else if (OB_UNLIKELY(!ls_handle_.is_valid() || tables_handle_.empty())) {
    bret = false;
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "ls_handle or tables_handle is invalid", K_(ls_handle), K_(tables_handle));
  } else if (OB_UNLIKELY(is_multi_version_merge(get_merge_type()) && !scn_range_.is_valid())) {
    bret = false;
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "scn range is invalid for multi_version merge", "merge_type", get_merge_type(), K_(scn_range));
  } else if (OB_UNLIKELY(schema_version_ < 0 || nullptr == schema_)) {
    bret = false;
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "schema info is invalid", K_(schema_version), KPC_(schema));
  } else if (OB_UNLIKELY(is_major_merge_type(get_merge_type()) && nullptr == report_)) {
    bret = false;
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "report is empty for major merge type", "merge_type", get_merge_type(), KP_(report));
  } else if (OB_UNLIKELY(multi_version_column_descs_.empty() || create_snapshot_version_ < 0)) {
    bret = false;
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "column desc is empty or create snapshot is invalid", K_(multi_version_column_descs),
      K_(create_snapshot_version));
  } else {
    bret = true;
  }
  return bret;
}

int ObStaticMergeParam::init_static_info(
  const int64_t concurrent_cnt,
  ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  if (is_mds_minor_merge(get_merge_type())) {
    rowkey_read_info_ = ObMdsSchemaHelper::get_instance().get_rowkey_read_info();
  } else {
    rowkey_read_info_ = static_cast<const ObRowkeyReadInfo *>(&(tablet_handle.get_obj()->get_rowkey_read_info()));
  }
  concurrent_cnt_ = concurrent_cnt;
  if (OB_FAIL(init_multi_version_column_descs())) {
    LOG_WARN("failed to init multi_version_column_descs", KR(ret));
  }
  return ret;
}

int ObStaticMergeParam::init_co_major_merge_params()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ObCOMajorMergePolicy::is_valid_major_merge_type(co_major_merge_type_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid major merge type", K(ret), K(major_sstable_status_), K(co_major_merge_type_));
  } else if (ObCOMajorMergePolicy::is_build_column_store_merge(co_major_merge_type_)) {
  } else if (ObCOMajorMergePolicy::is_build_row_store_merge(co_major_merge_type_)) {
    set_full_merge_and_level(true);
  } else if (ObCOMajorMergePolicy::is_rebuild_column_store_merge(co_major_merge_type_)) {
    set_full_merge_and_level(true);
    is_rebuild_column_store_ = true;
  }
  return ret;
}

int ObStaticMergeParam::init_multi_version_column_descs()
{
  int ret = OB_SUCCESS;
  multi_version_column_descs_.set_attr(ObMemAttr(MTL_ID(), "MvColDescs"));
  const bool is_major = is_major_or_meta_merge_type(get_merge_type());
  if (OB_UNLIKELY(!multi_version_column_descs_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("multi_version_column_descs is invalid", K(ret), K(multi_version_column_descs_));
  } else if (is_major) {
    if (OB_FAIL(schema_->get_multi_version_column_descs(multi_version_column_descs_))) {
      LOG_WARN("failed to get_multi_version_column_descs", K(ret), KPC_(schema), K(is_major));
    }
  } else if (OB_FAIL(schema_->get_mulit_version_rowkey_column_ids(multi_version_column_descs_))) {
    LOG_WARN("Failed to get get_multi_version_column_descs", K(ret), KPC_(schema), K(is_major));
  }
  return ret;
}

int ObStaticMergeParam::init_sstable_logic_seq()
{
  int ret = OB_SUCCESS;
  const ObITable *table = nullptr;
  if (is_major_merge_type(get_merge_type()) || is_mini_merge(get_merge_type())) {
    sstable_logic_seq_ = 0;
  } else if (OB_ISNULL(table = tables_handle_.get_table(tables_handle_.get_count() - 1))
      || !table->is_sstable()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table type", K(ret), KPC(table), K(tables_handle_));
  } else {
    const ObSSTable *sstable = static_cast<const ObSSTable *>(table);
    ObSSTableMetaHandle meta_handle;
    if (OB_FAIL(sstable->get_meta(meta_handle))) {
      LOG_WARN("get meta handle fail", K(ret), KPC(sstable));
    } else {
      sstable_logic_seq_ =
        MIN(ObMacroDataSeq::MAX_SSTABLE_SEQ, meta_handle.get_sstable_meta().get_sstable_seq()+ 1);
    }
  }
  return ret;
}

int ObStaticMergeParam::get_basic_info_from_result(
   const ObGetMergeTablesResult &get_merge_table_result)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(get_merge_table_result.handle_.empty() && !get_merge_table_result.update_tablet_directly_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected empty table handle", K(ret), K(tables_handle_));
  } else if (OB_FAIL(tables_handle_.assign(get_merge_table_result.handle_))) {
    LOG_WARN("failed to add tables", K(ret));
  } else if (OB_FAIL(init_sstable_logic_seq())) {
    LOG_WARN("failed to init sstable logic seq", K(ret), K(tables_handle_));
  } else {
    version_range_ = get_merge_table_result.version_range_;
    scn_range_ = get_merge_table_result.scn_range_;
    snapshot_info_ = get_merge_table_result.snapshot_info_;
    is_backfill_ = get_merge_table_result.is_backfill_;
    merge_scn_ = get_merge_table_result.get_merge_scn();

    if (is_major_or_meta_merge_type(get_merge_type())) {
      // for major or meta, need set create_snapshot as last major/meta sstable
      create_snapshot_version_ = tables_handle_.get_table(0)->get_snapshot_version();
    } else {
      create_snapshot_version_ = 0;
    }
    if (is_major_merge_type(get_merge_type())) {
      report_ = GCTX.ob_service_;
    }
  }
  return ret;
}

int ObStaticMergeParam::cal_minor_merge_param(const bool has_compaction_filter)
{
  int ret = OB_SUCCESS;
  //some input param check
  if (OB_UNLIKELY(tables_handle_.empty() || NULL == tables_handle_.get_table(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tables handle is invalid", K(ret), K(tables_handle_));
  } else {
    read_base_version_ = 0;
    if (get_tablet_id().is_ls_inner_tablet() && has_compaction_filter) {
      // full merge has been setted when preparing compaction filter
      set_full_merge_and_level(true/*is_full_merge*/);
    } else {
      set_full_merge_and_level(false/*is_full_merge*/);
    }
    data_version_ = DATA_CURRENT_VERSION;
  }
  return ret;
}

int ObStaticMergeParam::cal_major_merge_param()
{
  int ret = OB_SUCCESS;
  ObSSTable *base_table = nullptr;
  int64_t full_stored_col_cnt = 0;
  ObSSTableMetaHandle sstable_meta_hdl;

  const ObTablesHandleArray &tables_handle = tables_handle_;
  if (OB_UNLIKELY(tables_handle.empty()
      || NULL == (base_table = static_cast<ObSSTable*>(tables_handle.get_table(0)))
      || !base_table->is_major_sstable())) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("base table must be major or meta major", K(ret), K(tables_handle));
  } else if (OB_FAIL(base_table->get_meta(sstable_meta_hdl))) {
    LOG_WARN("fail to get sstable meta", K(ret), KPC(base_table));
  } else if (OB_FAIL(schema_->get_stored_column_count_in_sstable(full_stored_col_cnt))) {
    LOG_WARN("failed to get stored column count in sstable", K(ret), KPC(schema_));
  } else if (OB_UNLIKELY(sstable_meta_hdl.get_sstable_meta().get_column_count() > full_stored_col_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("stored col cnt in curr schema is less than old major sstable", K(ret),
      "col_cnt_in_sstable", sstable_meta_hdl.get_sstable_meta().get_column_count(),
      "col_cnt_in_schema", full_stored_col_cnt,
      K(sstable_meta_hdl), KPC(this));
  } else {
    read_base_version_ = base_table->get_snapshot_version();
    if (1 == schema_->get_progressive_merge_num() || is_rebuild_column_store_) {
      is_full_merge_ = true;
    } else {
      is_full_merge_ = false;
    }

    const ObSSTableBasicMeta &base_meta = sstable_meta_hdl.get_sstable_meta().get_basic_meta();
    const int64_t meta_progressive_merge_round = base_meta.progressive_merge_round_;
    const int64_t schema_progressive_merge_round = schema_->get_progressive_merge_round();
    if (0 == schema_->get_progressive_merge_num()) {
      progressive_merge_num_ = (1 == schema_progressive_merge_round) ? 0 : OB_AUTO_PROGRESSIVE_MERGE_NUM;
    } else {
      progressive_merge_num_ = schema_->get_progressive_merge_num();
    }

    if (is_full_merge_) {
      progressive_merge_round_ = schema_progressive_merge_round;
      progressive_merge_step_ = progressive_merge_num_;
    } else if (meta_progressive_merge_round < schema_progressive_merge_round) { // new progressive merge
      progressive_merge_round_ = schema_progressive_merge_round;
      progressive_merge_step_ = 0;
    } else if (meta_progressive_merge_round == schema_progressive_merge_round) {
      progressive_merge_round_ = meta_progressive_merge_round;
      progressive_merge_step_ = base_meta.progressive_merge_step_;
    }
    // ATTENTION! Critical diagnostic log, DO NOT CHANGE!!!
    FLOG_INFO("Calc progressive param", K_(is_schema_changed), K(progressive_merge_num_),
        K(progressive_merge_round_), K(meta_progressive_merge_round), K(progressive_merge_step_),
        K(is_full_merge_), K(full_stored_col_cnt), K(sstable_meta_hdl.get_sstable_meta().get_column_count()));
  }

  if (OB_SUCC(ret)) {
    if (is_full_merge_ || is_meta_major_merge(get_merge_type()) || (merge_level_ != MACRO_BLOCK_MERGE_LEVEL && is_schema_changed_)) {
      merge_level_ = MACRO_BLOCK_MERGE_LEVEL;
    }
  }
  return ret;
}

bool ObStaticMergeParam::is_build_row_store_from_rowkey_cg() const
{
  return is_build_row_store() && is_rowkey_major_sstable(major_sstable_status_);
}

bool ObStaticMergeParam::is_build_row_store() const
{
  return ObCOMajorMergePolicy::is_build_row_store_merge(co_major_merge_type_);
}

/*
* ObCtxMergeInfoCollector
*/
void ObCtxMergeInfoCollector::prepare(ObBasicTabletMergeCtx &ctx)
{
  int tmp_ret = OB_SUCCESS;
  if (!ctx.get_tablet()->is_row_store()
    || nullptr == ctx.merge_dag_
    || typeid(*ctx.merge_dag_) == typeid(ObTxTableMergeDag)) {
    // not init progress for Tx*Table Mini Merge & columnar store
  } else if (OB_TMP_FAIL(ctx.prepare_merge_progress(merge_progress_))) {
    LOG_WARN_RET(tmp_ret, "failed to init merge progress");
  }
}

void ObCtxMergeInfoCollector::finish(ObTabletMergeInfo &merge_info)
{
  int tmp_ret = OB_SUCCESS;
  if (OB_NOT_NULL(merge_progress_)) {
    if (OB_TMP_FAIL(merge_progress_->update_merge_info(merge_info.get_sstable_merge_info()))) {
      LOG_WARN_RET(tmp_ret, "fail to update update merge info");
    }

    if (OB_TMP_FAIL(merge_progress_->finish_merge_progress())) {
      LOG_WARN_RET(tmp_ret, "fail to update final merge progress");
    }
  }
}

void ObCtxMergeInfoCollector::destroy(compaction::ObCompactionMemoryContext &mem_ctx)
{
  if (OB_NOT_NULL(merge_progress_)) {
    merge_progress_->~ObPartitionMergeProgress();
    mem_ctx.free(merge_progress_);
    merge_progress_ = nullptr;
  }
  if (OB_NOT_NULL(compaction_filter_)) {
    compaction_filter_->~ObICompactionFilter();
    mem_ctx.free(compaction_filter_);
    compaction_filter_ = nullptr;
  }
}

void ObBasicTabletMergeCtx::collect_tnode_dml_stat(const ObTransNodeDMLStat tnode_stat)
{
  if (OB_LIKELY(1 >= get_concurrent_cnt())) {
    // serial mini compaction
    info_collector_.tnode_stat_ = tnode_stat;
  } else {
    // parallel mini compaction
    info_collector_.tnode_stat_.atomic_inc(tnode_stat);
  }
}

int ObBasicTabletMergeCtx::prepare_merge_progress(
  compaction::ObPartitionMergeProgress *&progress,
  ObTabletMergeDag *merge_dag/*nullptr*/,
  const uint32_t start_cg_idx/*0*/,
  const uint32_t end_cg_idx/*0*/)
{
  int ret = OB_SUCCESS;
  progress = nullptr;
  if (get_is_tenant_major_merge()) {
    if (!get_tablet()->is_row_store()) {
      progress = OB_NEWx(ObCOMajorMergeProgress, &(mem_ctx_.get_safe_arena()), mem_ctx_.get_safe_arena());
    } else {
      progress = OB_NEWx(ObPartitionMajorMergeProgress, &(mem_ctx_.get_safe_arena()), mem_ctx_.get_safe_arena());
    }
  } else {
    progress = OB_NEWx(ObPartitionMergeProgress, &(mem_ctx_.get_safe_arena()), mem_ctx_.get_safe_arena());
  }
  if (nullptr != merge_dag_) {
    merge_dag = merge_dag_;
  }
  if (OB_ISNULL(progress)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate merge progress", KR(ret), KP(progress));
  } else if (OB_FAIL(progress->init(this, merge_dag, start_cg_idx, end_cg_idx))) {
    progress->reset();
    LOG_WARN("failed to init merge progress", K(ret));
  } else {
    LOG_INFO("succeed to init merge progress", K(ret), KPC(progress));
  }
  if (OB_FAIL(ret) && nullptr != progress) {
    progress->~ObPartitionMergeProgress();
    mem_ctx_.free(progress);
    progress = nullptr;
  }
  return ret;
}

int ObBasicTabletMergeCtx::build_ctx(bool &finish_flag)
{
  int ret = OB_SUCCESS;
  ObGetMergeTablesResult get_merge_table_result;
  finish_flag = false;
  static_param_.start_time_ = common::ObTimeUtility::fast_current_time();
  #define LOG_PRINT_WRAPPER(str) \
    LOG_WARN(str, KR(ret), KPC(this)); CTX_SET_DIAGNOSE_LOCATION(*this);
  if (OB_FAIL(get_ls_and_tablet())) {
    if (OB_TABLET_NOT_EXIST != ret) {
      LOG_PRINT_WRAPPER("failed to get ls_handle/tablet_handle/rebuild_seq");
    }
  } else if (OB_FAIL(ObTablet::check_transfer_seq_equal(*get_tablet(), get_transfer_seq()))) {
    LOG_WARN("new tablet transfer seq not eq with old transfer seq", K(ret),
        "new_tablet_meta", get_tablet()->get_tablet_meta(),
        "old_transfer_seq", get_transfer_seq());
  } else if (OB_FAIL(get_merge_tables(get_merge_table_result))) {
    if (OB_NO_NEED_MERGE != ret) {
      LOG_PRINT_WRAPPER("failed to get merge tables");
    }
  } else if (get_merge_table_result.update_tablet_directly_) {
    if (OB_UNLIKELY(!is_mini_merge(get_merge_type()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_PRINT_WRAPPER("unexpected merge type to update tablet directly");
    } else if (OB_FAIL(update_tablet_directly(get_merge_table_result))) {
      LOG_PRINT_WRAPPER("failed to update tablet directly");
    } else {
      finish_flag = true;
    }
  } else if (OB_FAIL(try_swap_tablet(get_merge_table_result))) {
    LOG_PRINT_WRAPPER("failed to try swap tablet handle");
  } else if (OB_FAIL(static_param_.get_basic_info_from_result(get_merge_table_result))) {
    LOG_PRINT_WRAPPER("failed to get basic infor from result");
  } else if (FALSE_IT(time_guard_click(ObStorageCompactionTimeGuard::COMPACTION_POLICY))) {
  } else if (OB_FAIL(prepare_schema())) {
    LOG_PRINT_WRAPPER("failed to get schema");
  } else if (OB_FAIL(build_ctx_after_init())) {
    LOG_PRINT_WRAPPER("failed to build ctx after init");
  }
  #undef LOG_PRINT_WRAPPER
  return ret;
}

int ObBasicTabletMergeCtx::build_ctx_after_init()
{
  int ret = OB_SUCCESS;
  #define LOG_PRINT_WRAPPER(str) \
    LOG_WARN(str, KR(ret), KPC(this)); CTX_SET_DIAGNOSE_LOCATION(*this);
  if (OB_FAIL(cal_merge_param())) {
    LOG_PRINT_WRAPPER("failed to cal merge param");
  } else if (OB_FAIL(init_parallel_merge_ctx())) {
    LOG_PRINT_WRAPPER("failed to init parallel merge ctx");
  } else if (FALSE_IT(time_guard_click(ObStorageCompactionTimeGuard::GET_PARALLEL_RANGE))) {
  } else if (OB_FAIL(init_static_param_and_desc())) {
    LOG_PRINT_WRAPPER("failed to init static param and static desc");
  } else if (OB_FAIL(init_read_info())) {
    LOG_PRINT_WRAPPER("failed to init read info");
  } else if (FALSE_IT(info_collector_.prepare(*this))) {
  } else if (OB_FAIL(init_tablet_merge_info())) {
    LOG_PRINT_WRAPPER("failed to int tablet merge info");
  } else if (OB_FAIL(prepare_index_tree())) {
    LOG_PRINT_WRAPPER("failed to init index tree");
  }
  #undef LOG_PRINT_WRAPPER
  return ret;
}

void ObBasicTabletMergeCtx::destroy()
{
  free_schema();
  static_param_.reset(); // clear tables_handle before tablet_handle reset
  info_collector_.destroy(mem_ctx_);
  read_info_.reset();
}

void ObBasicTabletMergeCtx::free_schema()
{
  if (nullptr != static_param_.schema_) {
    static_param_.schema_->~ObStorageSchema();
    static_param_.schema_ = nullptr;
    // TODO(@lixia.yq): ensure that the buffer corresponding to storage schema is always allocated by ObArenaAllocator
    // otherwise there will be memory leak here.
  }
}

ObBasicTabletMergeCtx::ObBasicTabletMergeCtx(
  ObTabletMergeDagParam &param,
  common::ObArenaAllocator &allocator)
  : mem_ctx_(param, allocator),
    static_param_(param),
    static_desc_(),
    tablet_handle_(),
    parallel_merge_ctx_(mem_ctx_.get_allocator()),
    merge_dag_(nullptr),
    info_collector_(),
    read_info_()
{
}

bool ObBasicTabletMergeCtx::is_valid() const
{
  bool bret = false;
  if (OB_UNLIKELY(!static_param_.is_valid())) {
    bret = false;
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "static param is invalid", K(ret), K(static_param_));
  } else if (OB_UNLIKELY(!tablet_handle_.is_valid())) {
    bret = false;
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "tablet handle is invalid", K(ret), K(tablet_handle_));
  } else if (OB_UNLIKELY(!parallel_merge_ctx_.is_valid())) {
    bret = false;
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "parallel_merge_ctx is invalid", K(ret), K(parallel_merge_ctx_));
  } else if (OB_UNLIKELY(!read_info_.is_valid())) {
    bret = false;
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "read_info is invalid", K(ret), K(read_info_));
  } else {
    bret = true;
  }
  return bret;
}

int ObBasicTabletMergeCtx::get_ls_and_tablet()
{
  int ret = OB_SUCCESS;
  ObLSHandle &ls_handle = static_param_.ls_handle_;
  if (OB_FAIL(MTL(ObLSService *)->get_ls(static_param_.get_ls_id(), ls_handle, ObLSGetMod::COMPACT_MODE))) {
    LOG_WARN("failed to get log stream", K(ret), K(static_param_.get_ls_id()));
  } else if (ls_handle.get_ls()->is_offline()) {
    ret = OB_CANCELED;
    LOG_INFO("ls offline, skip merge", K(ret), "param", get_dag_param());
  } else if (FALSE_IT(static_param_.ls_rebuild_seq_ = ls_handle.get_ls()->get_rebuild_seq())) {
  } else if (get_dag_param().need_swap_tablet_flag_) {
    if (OB_FAIL(swap_tablet())) {
      LOG_WARN("failed to swap tablet", K(ret), K(static_param_));
    }
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet_svr()->get_tablet(
          static_param_.get_tablet_id(),
          tablet_handle_,
          0/*timeout_us*/,
          storage::ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    LOG_WARN("failed to get tablet", K(ret), K(static_param_));
  }
  return ret;
}

int ObBasicTabletMergeCtx::get_merge_tables(ObGetMergeTablesResult &get_merge_table_result)
{
  int ret = OB_SUCCESS;
  ObGetMergeTablesParam get_merge_table_param;
  get_merge_table_param.merge_type_ = get_merge_type();
  get_merge_table_param.merge_version_ = get_merge_version();
  get_merge_table_result.error_location_ = &info_collector_.error_location_;
  if (is_valid_merge_type(get_merge_type())
    && OB_FAIL(ObPartitionMergePolicy::get_merge_tables[get_merge_type()](
          get_merge_table_param,
          *get_ls(),
          *get_tablet(),
          get_merge_table_result))) {
    if (OB_NO_NEED_MERGE != ret) {
      LOG_WARN("failed to get merge tables", K(ret), KPC(this), K(get_merge_table_result));
    }
  }
  return ret;
}

int ObBasicTabletMergeCtx::swap_tablet()
{
  int ret = OB_SUCCESS;
  const ObTabletMapKey key(get_ls_id(), get_tablet_id());
  if (OB_FAIL(get_ls()->get_tablet_svr()->get_tablet_without_memtables(
      WashTabletPriority::WTP_LOW, key, mem_ctx_.get_allocator(), tablet_handle_))) {
    LOG_WARN("failed to get alloc tablet handle", K(ret), K(key));
  } else {
    static_param_.rowkey_read_info_ = static_cast<const ObRowkeyReadInfo *>(&(get_tablet()->get_rowkey_read_info()));
    LOG_INFO("success to swap tablet handle", K(ret), K(tablet_handle_),
      "new_rowkey_read_info", static_param_.rowkey_read_info_);
  }
  return ret;
}

bool ObBasicTabletMergeCtx::need_swap_tablet(ObProtectedMemtableMgrHandle &memtable_mgr_handle,
                                             const int64_t row_count,
                                             const int64_t macro_count)
{
  bool bret = memtable_mgr_handle.has_memtable()
    && (row_count >= LARGE_VOLUME_DATA_ROW_COUNT_THREASHOLD
      || macro_count >= LARGE_VOLUME_DATA_MACRO_COUNT_THREASHOLD);
#ifdef ERRSIM
  int ret = OB_E(EventTable::EN_SWAP_TABLET_IN_COMPACTION) OB_SUCCESS;
  if (OB_FAIL(ret)) {
    ret = OB_SUCCESS;
    STORAGE_LOG(INFO, "ERRSIM EN_SWAP_TABLET_IN_COMPACTION");
    bret = true;
  }
#endif
  return bret;
}

int ObBasicTabletMergeCtx::get_storage_schema()
{
  int ret  = OB_SUCCESS;
  ObStorageSchema *schema_on_tablet = nullptr;
  if (OB_FAIL(get_tablet()->load_storage_schema(mem_ctx_.get_allocator(), schema_on_tablet))) {
    LOG_WARN("failed to load storage schema", K(ret), K_(tablet_handle));
  } else {
    static_param_.schema_ = schema_on_tablet;
  }
  return ret;
}

int ObBasicTabletMergeCtx::prepare_schema()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_storage_schema())) {
    LOG_WARN("failed to get storage schema on tablet", KR(ret));
  } else {
    FLOG_INFO("get storage schema to merge", "param", get_dag_param(), KPC_(static_param_.schema));
  }
  return ret;
}

int ObBasicTabletMergeCtx::init_parallel_merge_ctx()
{
  int ret = OB_SUCCESS;
  if (!parallel_merge_ctx_.is_valid() && OB_FAIL(parallel_merge_ctx_.init(*this))) {
    LOG_WARN("Failed to init parallel merge context", K(ret));
  }
  return ret;
}

int ObBasicTabletMergeCtx::get_merge_range(int64_t parallel_idx, ObDatumRange &merge_range)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!parallel_merge_ctx_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected invalid parallel merge ctx", K(ret), K_(parallel_merge_ctx));
  } else if (OB_FAIL(parallel_merge_ctx_.get_merge_range(parallel_idx, merge_range))) {
    LOG_WARN("Failed to get merge range from parallel merge ctx", K(ret));
  }
  return ret;
}

int ObBasicTabletMergeCtx::generate_participant_table_info(PartTableInfo &info) const
{
  int ret = OB_SUCCESS;
  const ObTablesHandleArray &tables_handle = get_tables_handle();
  info.is_major_merge_ = is_major_merge_type(get_merge_type());
  if (info.is_major_merge_) {
    info.table_cnt_ = static_cast<int32_t>(tables_handle.get_count());
    info.snapshot_version_ = tables_handle.get_table(0)->get_snapshot_version();
    if (tables_handle.get_count() > 1) {
      info.start_scn_ = tables_handle.get_table(1)->get_start_scn().get_val_for_tx();
      info.end_scn_ = tables_handle.get_table(tables_handle.get_count() - 1)->get_end_scn().get_val_for_tx();
    }
  } else {
    if (tables_handle.get_count() > 0) {
      info.table_cnt_ = static_cast<int32_t>(tables_handle.get_count());
      info.start_scn_ = tables_handle.get_table(0)->get_start_scn().get_val_for_tx();
      info.end_scn_ = tables_handle.get_table(tables_handle.get_count() - 1)->get_end_scn().get_val_for_tx();
    }
  }
  return ret;
}

int ObBasicTabletMergeCtx::generate_macro_id_list(char *buf, const int64_t buf_len, const ObSSTable *&sstable) const
{
  int ret = OB_SUCCESS;
  ObMacroIdIterator iter;
  ObSSTableMetaHandle sst_meta_hdl;

  if (OB_ISNULL(sstable) || OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(sstable), KP(buf));
  } else if (OB_FAIL(sstable->get_meta(sst_meta_hdl))) {
    LOG_WARN("failed to get sstable meta", K(ret));
  } else if (OB_FAIL(sst_meta_hdl.get_sstable_meta().get_macro_info().get_data_block_iter(iter))) {
    LOG_WARN("fail to get data block iterator", K(ret), KPC(sstable));
  } else {
    MEMSET(buf, '\0', buf_len);
    int pret = 0;
    const int64_t macro_count = sst_meta_hdl.get_sstable_meta().get_data_macro_block_count();
    int64_t remain_len = buf_len;
    if (macro_count < 40) {
      MacroBlockId macro_id;
      for (int64_t i = 0; OB_SUCC(ret) && OB_SUCC(iter.get_next_macro_id(macro_id)); ++i) {
        if (0 == i) {
          pret = snprintf(buf + strlen(buf), remain_len, "%ld", macro_id.second_id());
        } else {
          pret = snprintf(buf + strlen(buf), remain_len, ",%ld", macro_id.second_id());
        }
        if (pret < 0 || pret > remain_len) {
          ret = OB_BUF_NOT_ENOUGH;
        } else {
          remain_len -= pret;
        }
      } // end of for
    }
  }
  return ret;
}

void ObBasicTabletMergeCtx::add_sstable_merge_info(
    ObSSTableMergeInfo &sstable_merge_info,
    const share::ObDagId &dag_id,
    const int64_t hash,
    const ObCompactionTimeGuard &time_guard,
    const ObSSTable *sstable,
    const ObStorageSnapshotInfo *snapshot_info,
    const int64_t start_cg_idx,
    const int64_t end_cg_idx)
{
  int tmp_ret = OB_SUCCESS;
  ObDagWarningInfo warning_info;
  sstable_merge_info.compaction_scn_ = static_param_.get_compaction_scn();
  sstable_merge_info.start_cg_idx_ = start_cg_idx;
  sstable_merge_info.end_cg_idx_ = end_cg_idx;
  sstable_merge_info.dag_id_ = dag_id;
  sstable_merge_info.merge_finish_time_ = common::ObTimeUtility::fast_current_time();
  sstable_merge_info.merge_level_ = static_param_.merge_level_;
  (void)generate_participant_table_info(sstable_merge_info.participant_table_info_);

  if (OB_NOT_NULL(sstable)) {
    (void)generate_macro_id_list(sstable_merge_info.macro_id_list_, sizeof(sstable_merge_info.macro_id_list_), sstable);
  }

  if (OB_NOT_NULL(snapshot_info) && snapshot_info->is_valid()) {
    sstable_merge_info.kept_snapshot_info_ = *snapshot_info;
  }
#define ADD_COMMENT(...) \
  ADD_COMPACTION_INFO_PARAM(sstable_merge_info.comment_, sizeof(sstable_merge_info.comment_), __VA_ARGS__)
  if (ObAdaptiveMergePolicy::AdaptiveMergeReason::NONE != static_param_.merge_reason_) {
    ADD_COMMENT("merge_reason", ObAdaptiveMergePolicy::merge_reason_to_str(static_param_.merge_reason_));
  }
  if (is_major_merge_type(get_merge_type())
      && ObCOMajorMergePolicy::INVALID_CO_MAJOR_MERGE_TYPE != static_param_.co_major_merge_type_) {
    ADD_COMMENT("major", static_param_.major_sstable_status_);
    ADD_COMMENT("co", ObCOMajorMergePolicy::co_major_merge_type_to_str(static_param_.co_major_merge_type_));
  }
  int64_t mem_peak_mb = mem_ctx_.get_total_mem_peak() >> 20;
  if (mem_peak_mb > 0) {
    ADD_COMMENT("cost_mb", mem_peak_mb);
  }
  ADD_COMMENT("time", time_guard);
#undef ADD_COMMENT
  // calc flush macro speed
  uint32_t exe_ts = time_guard.get_specified_cost_time(ObStorageCompactionTimeGuard::EXECUTE);
  if (exe_ts > 0 && sstable_merge_info.new_flush_occupy_size_ > 0) {
    sstable_merge_info.new_flush_data_rate_ = (int)(((float)sstable_merge_info.new_flush_occupy_size_/ 1024) / ((float)exe_ts / 1_s));
  }

  ObInfoParamBuffer info_allocator;
  if (OB_SUCCESS == MTL(ObDagWarningHistoryManager *)->get_with_param(hash, &warning_info, info_allocator)) {
    sstable_merge_info.dag_ret_ = warning_info.dag_ret_;
    sstable_merge_info.task_id_ = warning_info.task_id_;
    sstable_merge_info.retry_cnt_ = warning_info.retry_cnt_;
    sstable_merge_info.error_location_ = warning_info.location_;
    sstable_merge_info.early_create_time_ = warning_info.gmt_create_;
    warning_info.info_param_ = nullptr;
  }

  ObScheduleSuspectInfo ret_info;
  int64_t suspect_info_hash = ObScheduleSuspectInfo::gen_hash(MTL_ID(), hash);
  info_allocator.reuse();
  if (OB_SUCCESS == MTL(compaction::ObScheduleSuspectInfoMgr *)->get_with_param(suspect_info_hash, &ret_info, info_allocator)) {
    sstable_merge_info.suspect_add_time_ = ret_info.add_time_;
    sstable_merge_info.info_param_ = ret_info.info_param_;
    if (OB_TMP_FAIL(MTL(compaction::ObScheduleSuspectInfoMgr *)->delete_info(suspect_info_hash))) {
      LOG_WARN_RET(tmp_ret, "failed to delete old suspect info", K(sstable_merge_info));
    }
  }

  if (OB_TMP_FAIL(MTL(storage::ObTenantSSTableMergeInfoMgr*)->add_sstable_merge_info(sstable_merge_info))) {
    LOG_WARN_RET(tmp_ret, "failed to add sstable merge info", K(sstable_merge_info));
  }
  sstable_merge_info.info_param_ = nullptr;

  // ATTENTION : merge_dag_ is nullptr when tablet is columnar store
  if (!sstable_merge_info.is_fake_) {
    int64_t cost_time = sstable_merge_info.merge_finish_time_ - time_guard.add_time_;
    if (nullptr != merge_dag_) {
      MTL(ObCompactionSuggestionMgr*)->analyze_merge_info(sstable_merge_info, merge_dag_->get_type(), cost_time);
    } else {
      MTL(ObCompactionSuggestionMgr*)->analyze_merge_info(sstable_merge_info, ObDagType::DAG_TYPE_CO_MERGE_BATCH_EXECUTE, cost_time);
    }
  }
}

int ObBasicTabletMergeCtx::init_static_param_and_desc()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(static_param_.init_static_info(get_concurrent_cnt(), tablet_handle_))) {
    LOG_WARN("failed to init basic info", KR(ret));
  } else if (OB_FAIL(static_desc_.init(*get_schema(), get_ls_id(), get_tablet_id(),
                                get_merge_type(), get_snapshot(),
                                static_param_.scn_range_.end_scn_,
                                static_param_.data_version_))) {
    LOG_WARN("failed to init static desc", KR(ret), KPC(this));
  }
  return ret;
}

int ObBasicTabletMergeCtx::init_read_info()
{
  int ret = OB_SUCCESS;
  int64_t schema_stored_col_cnt = 0;
  if (OB_FAIL(get_schema()->get_store_column_count(schema_stored_col_cnt, true/*full_col*/))) {
    LOG_WARN("failed to get storage count", KR(ret), KPC(this));
  } else if (OB_FAIL(read_info_.init(mem_ctx_.get_allocator(), schema_stored_col_cnt, get_schema()->get_rowkey_column_num(),
            get_schema()->is_oracle_mode(), static_param_.multi_version_column_descs_))) {
    LOG_WARN("failed to init read info", KR(ret), KPC(this));
  }
  return ret;
}

ObITable::TableType ObBasicTabletMergeCtx::get_merged_table_type(
    const ObStorageColumnGroupSchema *cg_schema,
    const bool is_main_table) const
{
  ObITable::TableType table_type = ObITable::MAX_TABLE_TYPE;

  if (is_major_or_meta_merge_type(get_merge_type())) { // MAJOR / META MERGE
    const bool is_meta_merge = is_meta_major_merge(get_merge_type());
    if (nullptr == cg_schema) {
      table_type = is_meta_merge
                 ? ObITable::TableType::META_MAJOR_SSTABLE
                 : ObITable::TableType::MAJOR_SSTABLE;
    } else if (cg_schema->is_all_column_group()) {
      table_type = is_meta_merge
                 ? ObITable::TableType::COLUMN_ORIENTED_META_SSTABLE
                 : ObITable::TableType::COLUMN_ORIENTED_SSTABLE;
    } else if (cg_schema->is_rowkey_column_group()) {
      table_type = is_main_table
                 ? (is_meta_merge ? ObITable::TableType::COLUMN_ORIENTED_META_SSTABLE : ObITable::TableType::COLUMN_ORIENTED_SSTABLE)
                 : ObITable::TableType::ROWKEY_COLUMN_GROUP_SSTABLE;
    } else {
      table_type = ObITable::TableType::NORMAL_COLUMN_GROUP_SSTABLE;
    }
  } else if (MINI_MERGE == get_merge_type()) {
    table_type = ObITable::TableType::MINI_SSTABLE;
  } else if (DDL_KV_MERGE == get_merge_type()) {
    table_type = ObITable::TableType::DDL_DUMP_SSTABLE;
  } else if (MDS_MINI_MERGE == get_merge_type()) {
    table_type = ObITable::TableType::MDS_MINI_SSTABLE;
  } else if (MDS_MINOR_MERGE == get_merge_type()) {
    table_type = ObITable::TableType::MDS_MINOR_SSTABLE;
  } else { // MINOR_MERGE || HISTORY_MINOR_MERGE
    table_type = ObITable::TableType::MINOR_SSTABLE;
  }
  return table_type;
}

void ObBasicTabletMergeCtx::after_update_tablet_for_major()
{
  int tmp_ret = OB_SUCCESS;
  if (is_major_merge_type(get_merge_type())) {
    const ObLSID &ls_id = get_ls_id();
    const ObTabletID &tablet_id = get_tablet_id();
    if (OB_NOT_NULL(static_param_.report_)) {
      if (OB_TMP_FAIL(MTL(observer::ObTabletTableUpdater*)->submit_tablet_update_task(ls_id, tablet_id, true/*need_diagnose*/))) {
        LOG_WARN_RET(tmp_ret, "failed to submit tablet update task to report", K(ls_id), K(tablet_id));
      } else if (OB_TMP_FAIL(get_ls()->get_tablet_svr()->update_tablet_report_status(tablet_id))) {
        LOG_WARN_RET(tmp_ret, "failed to update tablet report status", K(ls_id), K(tablet_id));
      }
    }
    if (OB_TMP_FAIL(MTL(ObTenantMediumChecker*)->add_tablet_ls(tablet_id, ls_id, get_merge_version()))) {
      LOG_WARN_RET(tmp_ret, "failed to add tablet ls for check", K(ls_id),
          K(tablet_id), "merge_version", get_merge_version());
    }
  }
}

void ObBasicTabletMergeCtx::build_update_table_store_param(
  const blocksstable::ObSSTable *sstable,
  ObUpdateTableStoreParam &param)
{
  const ObMergeType merge_type = get_merge_type();
  SCN clog_checkpoint_scn = SCN::min_scn();
  if (is_mini_merge(merge_type) && nullptr != sstable) {
    clog_checkpoint_scn = sstable->get_end_scn();
  }
  if (is_meta_major_merge(get_merge_type())) {
    param.multi_version_start_ = tablet_handle_.get_obj()->get_multi_version_start();
    param.snapshot_version_ = tablet_handle_.get_obj()->get_snapshot_version();
  } else {
    param.multi_version_start_ = get_tablet_id().is_ls_inner_tablet() ? 1 : static_param_.version_range_.multi_version_start_;
    param.snapshot_version_ = static_param_.version_range_.snapshot_version_;
  }

  param.sstable_ = sstable;
  param.storage_schema_ = static_param_.schema_;
  param.rebuild_seq_ = get_ls_rebuild_seq();
  param.need_report_ = is_major_merge_type(merge_type);
  param.merge_type_ = get_inner_table_merge_type();
  param.clog_checkpoint_scn_ = clog_checkpoint_scn;
  param.need_check_sstable_ = is_minor_merge(merge_type);
  param.allow_duplicate_sstable_ = false;
  param.need_check_transfer_seq_ = true;
  param.transfer_seq_ = get_tablet()->get_tablet_meta().transfer_info_.transfer_seq_;
  param.update_with_major_flag_ = false;
}

int ObBasicTabletMergeCtx::update_tablet(
  const ObSSTable &sstable,
  ObTabletHandle &new_tablet_handle)
{
  int ret = OB_SUCCESS;
  // means finish current major/medium compaction
  ObArenaAllocator allocator("MdsAlloc", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::MERGE_NORMAL_CTX_ID);
  if (share::is_reserve_mode()) {
    // TODO(@DanLing) use LocalArena later
    allocator.set_ctx_id(ObCtxIds::MERGE_RESERVE_CTX_ID);
  }
  ObUpdateTableStoreParam param;
  (void) build_update_table_store_param(&sstable, param);

  if (OB_FAIL(get_ls()->update_tablet_table_store(
      get_tablet_id(), param, new_tablet_handle))) {
    LOG_WARN("failed to update tablet table store", K(ret), K(param), K(new_tablet_handle));
    CTX_SET_DIAGNOSE_LOCATION(*this);
  } else {
    time_guard_click(ObStorageCompactionTimeGuard::UPDATE_TABLET);
  }
  return ret;
}

int ObBasicTabletMergeCtx::try_set_upper_trans_version(blocksstable::ObSSTable &sstable)
{
  int ret = OB_SUCCESS;
  const ObMergeType merge_type = get_inner_table_merge_type();
  const int64_t rebuild_seq = get_ls_rebuild_seq();
  // update upper_trans_version for param.sstable_, and then update table store
  if (is_mini_merge(merge_type) || is_minor_merge(merge_type)) {
    // upper_trans_version calculated from ls is invalid when ls is rebuilding, use rebuild_seq to prevent concurrency bug.
    int tmp_ret = OB_SUCCESS;
    ObLS *ls = get_ls();
    int64_t new_upper_trans_version = INT64_MAX;
    int64_t new_rebuild_seq = 0;
    bool ls_is_migration = false;

    if (INT64_MAX != sstable.get_upper_trans_version()) {
      // all row committed, has set as max_merged_trans_version
    } else if (OB_TMP_FAIL(ls->check_ls_migration_status(ls_is_migration, new_rebuild_seq))) {
      LOG_WARN("failed to check ls migration status", K(tmp_ret), K(ls_is_migration), K(new_rebuild_seq));
    } else if (ls_is_migration) {
    } else if (rebuild_seq != new_rebuild_seq) {
      ret = OB_EAGAIN;
      LOG_WARN("rebuild seq not same, need retry merge", K(ret), "ls_meta", ls->get_ls_meta(), K(new_rebuild_seq), K(rebuild_seq));
    } else if (OB_TMP_FAIL(ObGCUpperTransHelper::try_get_sstable_upper_trans_version(*ls, sstable, new_upper_trans_version))) {
      LOG_WARN("failed to get new upper_trans_version for sstable", K(tmp_ret), K(sstable));
    } else if (INT64_MAX != new_upper_trans_version
            && OB_TMP_FAIL(sstable.set_upper_trans_version(mem_ctx_.get_allocator(), new_upper_trans_version))) {
      LOG_WARN("failed to set upper trans version", K(tmp_ret), K(sstable));
    } else {
      time_guard_click(ObStorageCompactionTimeGuard::UPDATE_UPPER_TRANS);
    }
  }
  return ret;
}

int ObBasicTabletMergeCtx::update_tablet_after_merge()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  time_guard_click(ObStorageCompactionTimeGuard::EXECUTE);
  ObTabletHandle new_tablet_handle;
  const ObSSTable *sstable = nullptr;
  if (OB_FAIL(create_sstable(sstable))) {
    LOG_WARN("failed to create sstable", KR(ret), "dag_param", get_dag_param());
  } else if (OB_FAIL(update_tablet(*sstable, new_tablet_handle))) {
    LOG_WARN("failed to update tablet", KR(ret), "dag_param", get_dag_param(), KPC(sstable));
  } else {
    mem_ctx_.mem_click();
    (void) after_update_tablet_for_major();  // only works for major_merge
    (void) update_and_analyze_progress();
    if (OB_TMP_FAIL(collect_running_info())) {
      LOG_WARN("fail to collect running info", K(tmp_ret));
    }
  }
  return ret;
}

int ObBasicTabletMergeCtx::build_index_tree(
  ObTabletMergeInfo &merge_info,
  const ObITableReadInfo *index_read_info,
  const storage::ObStorageColumnGroupSchema *cg_schema,
  const uint16_t table_cg_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid merge ctx", K(ret), KPC(this));
  } else if (OB_FAIL(merge_info.prepare_sstable_builder(index_read_info))) {
    LOG_WARN("fail to prepare sstable builder", K(ret));
  } else if (OB_FAIL(merge_info.get_sstable_build_desc().init(
      static_desc_, *get_schema(), cg_schema, table_cg_idx))) {
    LOG_WARN("failed to init index store desc", K(ret), KPC(this));
  } else if (OB_FAIL(merge_info.prepare_index_builder())) {
    LOG_WARN("failed to prepare index builder", K(ret), K(merge_info));
  }
  return ret;
}

int ObBasicTabletMergeCtx::get_schema_info_from_tables(
  const ObTablesHandleArray &merge_tables_handle,
  const int64_t column_cnt_in_schema,
  int64_t &max_column_cnt_in_memtable,
  int64_t &max_schema_version_in_memtable)
{
  int ret = OB_SUCCESS;
  int64_t max_column_cnt_on_recorder = 0;
  max_column_cnt_in_memtable = 0;
  max_schema_version_in_memtable = 0;
  ObITable *table = nullptr;
  memtable::ObMemtable *memtable = nullptr;
  for (int i = merge_tables_handle.get_count() - 1; OB_SUCC(ret) && i >= 0; --i) {
    if (OB_ISNULL(table = merge_tables_handle.get_table(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table in tables_handle is invalid", KR(ret), KPC(table));
    } else if (OB_ISNULL(memtable = static_cast<memtable::ObMemtable *>(table))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table pointer does not point to a ObMemtable object", KR(ret), KPC(table));
    } else if (OB_FAIL(memtable->get_schema_info(column_cnt_in_schema,
        max_schema_version_in_memtable, max_column_cnt_in_memtable))) {
      LOG_WARN("failed to get schema info from memtable", KR(ret), KPC(memtable));
    }
  } // end of for
  if (FAILEDx(tablet_handle_.get_obj()->get_max_column_cnt_on_schema_recorder(max_column_cnt_on_recorder))) {
    LOG_WARN("failed to get max column cnt on schema recorder", KR(ret));
  } else {
    max_column_cnt_in_memtable = MAX(max_column_cnt_in_memtable, max_column_cnt_on_recorder);
  }
  return ret;
}

// TODO(@lixia.yq): input schema_on_tablet is from tablet, if generate new schema from memtable_info, the old one could be freed
int ObBasicTabletMergeCtx::update_storage_schema_by_memtable(
  const ObStorageSchema &schema_on_tablet,
  const ObTablesHandleArray &merge_tables_handle)
{
  int ret = OB_SUCCESS;
  int64_t max_column_cnt_in_memtable = 0;
  int64_t max_schema_version_in_memtable = 0;
  int64_t column_cnt_in_schema = 0;
  bool column_info_simplified = false;
  if (!is_mini_merge(get_merge_type()) || get_tablet_id().is_ls_inner_tablet()) {
    // do nothing
  } else if (OB_FAIL(schema_on_tablet.get_store_column_count(column_cnt_in_schema, true/*full_col*/))) {
    LOG_WARN("failed to get store column count", K(ret), K(column_cnt_in_schema));
  } else if (OB_FAIL(get_schema_info_from_tables(merge_tables_handle, column_cnt_in_schema,
      max_column_cnt_in_memtable, max_schema_version_in_memtable))) {
    LOG_WARN("failed to get schemaFrom tables", K(ret), K(merge_tables_handle), K(column_cnt_in_schema));
  } else if (FALSE_IT(column_info_simplified = max_column_cnt_in_memtable > column_cnt_in_schema)) {
    // can't get new added column info from memtable, need simplify column info
  } else if (column_info_simplified
    || max_schema_version_in_memtable > schema_on_tablet.get_schema_version()) {
    // need alloc new storage schema & set column cnt
    ObStorageSchema *storage_schema = nullptr;
    if (OB_FAIL(ObStorageSchemaUtil::alloc_storage_schema(mem_ctx_.get_allocator(), storage_schema))) {
      LOG_WARN("failed to alloc storage schema", K(ret));
    } else if (OB_FAIL(storage_schema->init(mem_ctx_.get_allocator(), schema_on_tablet, column_info_simplified))) {
      LOG_WARN("failed to init storage schema", K(ret), K(schema_on_tablet));
      ObStorageSchemaUtil::free_storage_schema(mem_ctx_.get_allocator(), storage_schema);
      storage_schema = nullptr;
    } else {
      // only update column cnt by memtable, use schema version on tablet_schema
      storage_schema->column_cnt_ = MAX(storage_schema->column_cnt_, max_column_cnt_in_memtable);
      storage_schema->store_column_cnt_ = MAX(column_cnt_in_schema, max_column_cnt_in_memtable);
      storage_schema->schema_version_ = MAX(max_schema_version_in_memtable, schema_on_tablet.get_schema_version());
      static_param_.schema_ = storage_schema;
    }
  }
  if (OB_SUCC(ret)) {
    // ATTENTION! Critical diagnostic log, DO NOT CHANGE!!!
    FLOG_INFO("get storage schema to merge", "param", get_dag_param(), KPC_(static_param_.schema), K(schema_on_tablet),
      K(max_column_cnt_in_memtable), K(max_schema_version_in_memtable));
  }
  return ret;
}

int ObBasicTabletMergeCtx::get_medium_compaction_info()
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = get_tablet();
  const share::ObLSID &ls_id = tablet->get_ls_id();
  const common::ObTabletID &tablet_id = tablet->get_tablet_id();
  ObArenaAllocator temp_allocator("GetMediumInfo", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()); // for load medium info
  ObMediumCompactionInfoKey medium_info_key(get_merge_version());
  ObMediumCompactionInfo *medium_info = nullptr;

  if (OB_UNLIKELY(tablet->get_multi_version_start() > get_merge_version())) {
    ret = OB_SNAPSHOT_DISCARDED;
    LOG_ERROR("multi version data is discarded, should not execute compaction now", K(ret),
        "param", get_dag_param(), KPC(this));
  } else if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(temp_allocator, medium_info))) {
    LOG_WARN("fail to alloc and new", K(ret));
  } else {
    SMART_VAR(ObTableScanParam, scan_param) {
      storage::ObTabletMediumInfoReader medium_info_reader;
      if (OB_FAIL(ObMdsScanParamHelper::build_medium_info_scan_param(
          temp_allocator,
          ls_id,
          tablet_id,
          scan_param))) {
        LOG_WARN("fail to build scan param", K(ret), K(ls_id), K(tablet_id));
      } else if (OB_FAIL(medium_info_reader.init(*tablet, scan_param))) {
        LOG_WARN("failed to init medium info reader", K(ret), KPC(this));
      } else if (OB_FAIL(medium_info_reader.get_specified_medium_info(temp_allocator, medium_info_key, *medium_info))) {
        LOG_WARN("failed to get specified scn info", K(ret), K(medium_info_key));
      } else if (OB_UNLIKELY(!medium_info->is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("medium info is invalid", KR(ret), K(medium_info));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (medium_info->contain_parallel_range_
      && !parallel_merge_ctx_.is_valid()
      && OB_FAIL(parallel_merge_ctx_.init(*medium_info))) {
    LOG_WARN("failed to init parallel merge ctx", K(ret), KPC(medium_info));
  } else if (OB_FAIL(ObMediumListChecker::check_next_schedule_medium(
      medium_info, get_tables_handle().get_table(0)->get_snapshot_version(), true/*force_check*/))) {
    LOG_WARN("failed to check medium info and last major sstable", KR(ret), K(medium_info), KPC(this));
  } else if (OB_ISNULL(static_param_.schema_)) {
    ObStorageSchema *storage_schema = nullptr;
    if (OB_FAIL(ObStorageSchemaUtil::alloc_storage_schema(mem_ctx_.get_allocator(), storage_schema))) {
      LOG_WARN("failed to alloc storage schema", K(ret));
    } else if (OB_FAIL(storage_schema->init(mem_ctx_.get_allocator(), medium_info->storage_schema_))) {
      LOG_WARN("failed to init storage schema from current medium info", K(ret), K(medium_info));
      ObStorageSchemaUtil::free_storage_schema(mem_ctx_.get_allocator(), storage_schema);
    } else {
      static_param_.schema_ = storage_schema;
    }
  }

  if (OB_SUCC(ret)) {
    static_param_.schema_version_ = medium_info->storage_schema_.schema_version_;
    static_param_.data_version_ = medium_info->data_version_;
    static_param_.is_rebuild_column_store_ = (medium_info->medium_merge_reason_ == ObAdaptiveMergePolicy::REBUILD_COLUMN_GROUP);
    static_param_.is_tenant_major_merge_ = medium_info->is_major_compaction();
    if (medium_info->medium_compat_version_ >= ObMediumCompactionInfo::MEDIUM_COMPAT_VERSION_V4) {
      static_param_.is_schema_changed_ = medium_info->is_schema_changed_;
    }
    static_param_.merge_reason_ = (ObAdaptiveMergePolicy::AdaptiveMergeReason)medium_info->medium_merge_reason_;
    static_param_.co_major_merge_type_ = static_cast<ObCOMajorMergePolicy::ObCOMajorMergeType>(medium_info->co_major_merge_type_);
    FLOG_INFO("get storage schema to merge", "param", get_dag_param(), KPC_(static_param_.schema), KPC(medium_info));
  }

  // always free medium info
  ObTabletObjLoadHelper::free(temp_allocator, medium_info);

  return ret;
}

int ObBasicTabletMergeCtx::swap_tablet(ObGetMergeTablesResult &get_merge_table_result)
{
    int ret = OB_SUCCESS;
  // check need swap tablet when compaction
  if (OB_UNLIKELY(!is_major_or_meta_merge_type(get_merge_type()))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("other merge type not support swap tablet", KR(ret), "param", get_dag_param());
  } else if (OB_UNLIKELY(!get_tablet()->get_tablet_meta().ha_status_.is_data_status_complete())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("ha status is not allowed major", KR(ret), KPC(this));
  } else {
    ObTablesHandleArray &tables_handle = get_merge_table_result.handle_;
    ObProtectedMemtableMgrHandle *protected_handle = NULL;
    int64_t row_count = 0;
    int64_t macro_count = 0;
    const ObSSTable *sstable = nullptr;
    for (int64_t i = 0; i < tables_handle.get_count(); ++i) {
      sstable = static_cast<const ObSSTable*>(tables_handle.get_table(i));
      row_count += sstable->get_row_count();
      macro_count += sstable->get_data_macro_block_count();
    } // end of for
    if (OB_FAIL(get_tablet()->get_protected_memtable_mgr_handle(protected_handle))) {
      LOG_WARN("failed to get_protected_memtable_mgr_handle", K(ret), KPC(get_tablet()));
    } else if (need_swap_tablet(*protected_handle, row_count, macro_count)) {
      tables_handle.reset(); // clear tables array
      if (OB_FAIL(swap_tablet())) {
        LOG_WARN("failed to get alloc tablet handle", KR(ret));
      } else if (OB_FAIL(ObTablet::check_transfer_seq_equal(*get_tablet(), get_transfer_seq()))) {
        LOG_WARN("new tablet transfer seq not eq with old transfer seq", K(ret),
            "new_tablet_meta", get_tablet()->get_tablet_meta(),
            "old_transfer_seq", get_transfer_seq());
      } else if (OB_FAIL(get_merge_tables(get_merge_table_result))) {
        if (OB_NO_NEED_MERGE != ret) {
          LOG_WARN("failed to get merge tables", KR(ret), KPC(this));
        }
      }
    }
  }
  return ret;
}

int ObBasicTabletMergeCtx::get_meta_compaction_info()
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = get_tablet();
  ObMultiVersionSchemaService *schema_service = nullptr;
  int64_t full_stored_col_cnt = 0;
  int64_t schema_version = 0;
  ObStorageSchema *storage_schema = nullptr;

  if (OB_UNLIKELY(!is_meta_major_merge(get_merge_type())
               || nullptr != static_param_.schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected static param", K(ret), K(static_param_), KPC(static_param_.schema_));
  } else if (OB_FAIL(ObStorageSchemaUtil::alloc_storage_schema(mem_ctx_.get_allocator(), storage_schema))) {
    LOG_WARN("failed to alloc storage schema", K(ret));
  } else if (OB_ISNULL(schema_service = MTL(ObTenantSchemaService *)->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get schema service from MTL", K(ret));
  } else if (OB_FAIL(tablet->get_schema_version_from_storage_schema(schema_version))){
    LOG_WARN("failed to get schema version from tablet", KR(ret), KPC(tablet));
  } else if (OB_FAIL(ObMediumCompactionScheduleFunc::get_table_schema_to_merge(
        *schema_service, *tablet, schema_version, ObMediumCompactionInfo::MEDIUM_COMPAT_VERSION_V3, mem_ctx_.get_allocator(), *storage_schema))) {
    if (OB_TABLE_IS_DELETED != ret) {
      LOG_WARN("failed to get table schema", KR(ret), KPC(this));
    }
  } else if (OB_FAIL(storage_schema->get_stored_column_count_in_sstable(full_stored_col_cnt))) {
    LOG_WARN("failed to get stored column count in sstable", K(ret), KPC(storage_schema));
  } else if (OB_UNLIKELY(tablet->get_last_major_column_count() > full_stored_col_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("stored col cnt in curr schema is less than old major sstable", K(ret),
              "col_cnt_in_sstable", tablet->get_last_major_column_count(),
              "col_cnt_in_schema", full_stored_col_cnt, KPC(this));
  } else {
    static_param_.schema_ = storage_schema;
  }

  if (OB_SUCC(ret)) {
    static_param_.schema_version_ = storage_schema->schema_version_;
    static_param_.data_version_ = DATA_CURRENT_VERSION;
    static_param_.is_rebuild_column_store_ = false;
    static_param_.is_schema_changed_ = true; // use MACRO_BLOCK_MERGE_LEVEL
    static_param_.merge_reason_ = ObAdaptiveMergePolicy::TOMBSTONE_SCENE;
    FLOG_INFO("get storage schema to meta merge", "param", get_dag_param(), KPC_(static_param_.schema));
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(storage_schema)) {
    ObStorageSchemaUtil::free_storage_schema(mem_ctx_.get_allocator(), storage_schema);
    static_param_.schema_ = nullptr;
  }
  return ret;
}

} // namespace compaction
} // namespace oceanbase
