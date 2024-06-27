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

#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "storage/compaction/ob_tablet_merge_task.h"
#include "storage/compaction/ob_partition_merger.h"
#include "share/rc/ob_tenant_base.h"
#include "lib/stat/ob_session_stat.h"
#include "storage/tablet/ob_tablet_common.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/compaction/ob_partition_merge_progress.h"
#include "storage/tx_storage/ob_tenant_freezer.h"
#include "ob_tenant_compaction_progress.h"
#include "ob_compaction_diagnose.h"
#include "ob_compaction_suggestion.h"
#include "ob_partition_merge_progress.h"
#include "storage/ddl/ob_ddl_merge_task.h"
#include "storage/compaction/ob_schedule_dag_func.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "share/ob_get_compat_mode.h"
#include "storage/compaction/ob_sstable_merge_info_mgr.h"
#include "storage/compaction/ob_compaction_diagnose.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "storage/compaction/ob_tablet_merge_checker.h"
#include "share/ob_get_compat_mode.h"
#include "share/resource_manager/ob_cgroup_ctrl.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "storage/column_store/ob_column_oriented_sstable.h"
#include "storage/compaction/ob_compaction_dag_ranker.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/multi_data_source/ob_tablet_mds_merge_ctx.h"
#include "storage/compaction/ob_basic_tablet_merge_ctx.h"
#include "storage/compaction/ob_tenant_compaction_progress.h"
#include "storage/tx_storage/ob_tenant_freezer.h"
#include "storage/checkpoint/ob_checkpoint_diagnose.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace storage;
using namespace memtable;
using namespace blocksstable;
namespace compaction
{
ERRSIM_POINT_DEF(SPECIFIED_SERVER_STOP_COMPACTION);
/*
 *  ----------------------------------------------ObMergeParameter--------------------------------------------------
 */
ObMergeParameter::ObMergeParameter(
  const ObStaticMergeParam &static_param)
  : static_param_(static_param),
    merge_range_(),
    merge_rowid_range_(),
    cg_rowkey_read_info_(nullptr),
    trans_state_mgr_(nullptr),
    error_location_(nullptr),
    merge_scn_(),
    allocator_(nullptr)
{
}

bool ObMergeParameter::is_valid() const
{
  return static_param_.is_valid();
}

void ObMergeParameter::reset()
{
  merge_range_.reset();
  merge_rowid_range_.reset();
  trans_state_mgr_ = nullptr;
  error_location_ = nullptr;
  if (nullptr != cg_rowkey_read_info_) {
    cg_rowkey_read_info_->~ObITableReadInfo();
    allocator_->free(cg_rowkey_read_info_);
    cg_rowkey_read_info_ = nullptr;
  }
  allocator_ = nullptr;
}

int ObMergeParameter::init(
    compaction::ObBasicTabletMergeCtx &merge_ctx,
    const int64_t idx,
    ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!merge_ctx.is_valid() || idx < 0 || idx >= static_param_.concurrent_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to assign merge parameter", K(merge_ctx), K(idx), K(ret));
  } else if (OB_FAIL(merge_ctx.get_merge_range(idx, merge_range_))) {
    STORAGE_LOG(WARN, "failed to get merge range from merge context", K(ret));
  } else {
    const ObMergeType merge_type = static_param_.get_merge_type();
    merge_version_range_ = static_param_.version_range_;
    merge_scn_ = merge_ctx.get_merge_scn();
    allocator_ = allocator;
    if (is_major_merge_type(merge_type)) {
      // major merge should only read data between two major freeze points
      // but there will be some minor sstables which across major freeze points
      merge_version_range_.base_version_ = merge_ctx.get_read_base_version();
    } else if (is_meta_major_merge(merge_type)) {
      // meta major merge does not keep multi-version
      merge_version_range_.multi_version_start_ = static_param_.version_range_.snapshot_version_;
    } else if (is_multi_version_merge(merge_type)) {
      // minor compaction always need to read all the data from input table
      // rewrite version to whole version range
      merge_version_range_.snapshot_version_ = MERGE_READ_SNAPSHOT_VERSION;
    }

    if (is_major_or_meta_merge_type(static_param_.get_merge_type()) && !get_schema()->is_row_store()) {
      if (OB_ISNULL(allocator)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected null allocator", K(ret));
      } else if (OB_FAIL(set_merge_rowid_range(allocator))) {
        STORAGE_LOG(WARN, "failed to set merge rowid range", K(ret));
      } else if (OB_ISNULL(cg_rowkey_read_info_ = OB_NEWx(ObCGRowkeyReadInfo, allocator, *static_param_.rowkey_read_info_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "unexpected null rowkey read info", K(ret));
      }
    }

    if (OB_SUCC(ret) & merge_scn_ > static_param_.scn_range_.end_scn_) {
      if (!static_param_.is_backfill_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("merge scn is bigger than scn range but merge type is not backfill, unexpected",
            K(ret), K(merge_scn_), K(static_param_.scn_range_), K(static_param_.get_merge_type()));
      } else {
        FLOG_INFO("set backfill merge scn", K(merge_scn_), K(static_param_.scn_range_), K(static_param_.get_merge_type()));
      }
    }
  }
  if (OB_SUCC(ret)) {
    FLOG_INFO("success to init ObMergeParameter", K(ret), K(idx), KPC(this));
  }
  return ret;
}

int ObMergeParameter::set_merge_rowid_range(ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  ObITable *table = nullptr;
  ObSSTable *sstable = nullptr;
  const ObTablesHandleArray &tables_handle = get_tables_handle();
  if (OB_UNLIKELY(tables_handle.empty() || nullptr == allocator)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected null tables handle", K(ret), K(tables_handle), K(allocator));
  } else if (OB_ISNULL(table = tables_handle.get_table(0)) || !table->is_sstable()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected null table", K(ret), KPC(table));
  } else if (OB_ISNULL(sstable = static_cast<ObSSTable *>(table))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected null table", K(ret), KPC(sstable));
  } else if (OB_FAIL(sstable->get_cs_range(merge_range_, *static_param_.rowkey_read_info_, *allocator, merge_rowid_range_))) {
    STORAGE_LOG(WARN, "failed to get cs range", K(ret), K(merge_range_));
  }

  return ret;
}

const storage::ObTablesHandleArray & ObMergeParameter::get_tables_handle() const
{
  return static_param_.tables_handle_;
}
const ObStorageSchema *ObMergeParameter::get_schema() const
{
  return static_param_.schema_;
}

bool ObMergeParameter::is_full_merge() const
{
  return static_param_.is_full_merge_;
}

int64_t ObMergeParameter::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
  } else {
    J_OBJ_START();
    J_KV(K_(static_param), K_(merge_version_range), K_(merge_range), K_(merge_rowid_range), K_(merge_scn));
    J_OBJ_END();
  }
  return pos;
}

/*
 *  ----------------------------------------------ObCompactionParam--------------------------------------------------
 */
ObCompactionParam::ObCompactionParam()
  : score_(0),
    occupy_size_(0),
    estimate_phy_size_(0),
    replay_interval_(0),
    add_time_(0),
    last_end_scn_(),
    sstable_cnt_(0),
    parallel_dag_cnt_(0),
    parallel_sstable_cnt_(0),
    estimate_concurrent_cnt_(1),
    batch_size_(ObCompactionEstimator::DEFAULT_BATCH_SIZE)
{
}


void ObCompactionParam::estimate_concurrent_count(const compaction::ObMergeType merge_type)
{
  int ret = OB_SUCCESS;

  /*
   * tablet_size is a non-standard syntax used only when creating tables, and is default in most cases.
   * so it's OK to use default val directly.
   */
  const int64_t tablet_size = OB_DEFAULT_TABLET_SIZE;
  estimate_concurrent_cnt_ = 1;

  if (is_mini_merge(merge_type)) {
    estimate_concurrent_cnt_ = MAX((estimate_phy_size_ + tablet_size - 1) / tablet_size, 1);
  } else if (is_minor_merge_type(merge_type)) {
    int64_t avg_sstable_size = MAX(occupy_size_ / parallel_sstable_cnt_, 0);
    estimate_concurrent_cnt_ = MAX((avg_sstable_size + tablet_size - 1) / tablet_size, 1);
  }
}


/*
 *  ----------------------------------------------ObTabletMergeDagParam--------------------------------------------------
 */
ObTabletMergeDagParam::ObTabletMergeDagParam()
  :  skip_get_tablet_(false),
     need_swap_tablet_flag_(false),
     is_reserve_mode_(false),
     merge_type_(INVALID_MERGE_TYPE),
     merge_version_(0),
     transfer_seq_(-1),
     ls_id_(),
     tablet_id_()
{
}

ObTabletMergeDagParam::ObTabletMergeDagParam(
    const compaction::ObMergeType merge_type,
    const share::ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const int64_t transfer_seq)
  :  skip_get_tablet_(false),
     need_swap_tablet_flag_(false),
     is_reserve_mode_(false),
     merge_type_(merge_type),
     merge_version_(0),
     transfer_seq_(transfer_seq),
     ls_id_(ls_id),
     tablet_id_(tablet_id)
{
}

bool ObTabletMergeDagParam::is_valid() const
{
  return ls_id_.is_valid()
         && tablet_id_.is_valid()
         && is_valid_merge_type(merge_type_)
         && (!is_multi_version_merge(merge_type_) || merge_version_ >= 0);
}

ObTabletMergeDag::ObTabletMergeDag(
    const ObDagType::ObDagTypeEnum type)
  : ObIDag(type),
    ObMergeDagHash(),
    is_inited_(false),
    compat_mode_(lib::Worker::CompatMode::INVALID),
    ctx_(nullptr),
    param_(),
    allocator_("MergeDag", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::MERGE_NORMAL_CTX_ID)
{
}

ObTabletMergeDag::~ObTabletMergeDag()
{
  /* To make LocalArena has access to mem ctx when destroyed,
   * We should clear ObIdag::task_list_ at first when dag failed.
   */
  clear_task_list_with_lock();
  if (OB_NOT_NULL(ctx_)) {
    ctx_->~ObBasicTabletMergeCtx();
    allocator_.free(ctx_);
    ctx_ = nullptr;
  }
}

int ObTabletMergeDag::get_tablet_and_compat_mode()
{
  int ret = OB_SUCCESS;
  // can't get tablet_handle now! because this func is called in create dag,
  // the last compaction dag is not finished yet, tablet is in old version
  ObLSHandle tmp_ls_handle;
  ObTabletHandle tmp_tablet_handle;
  if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, tmp_ls_handle, ObLSGetMod::COMPACT_MODE))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id_));
  } else if (OB_FAIL(tmp_ls_handle.get_ls()->get_tablet_svr()->get_tablet(
      tablet_id_, tmp_tablet_handle, 0/*timeout_us*/, storage::ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    LOG_WARN("failed to get tablet", K(ret), K(ls_id_), K(tablet_id_));
  } else if (OB_FAIL(ObTabletMergeChecker::check_need_merge(merge_type_, *tmp_tablet_handle.get_obj()))) {
    if (OB_NO_NEED_MERGE != ret) {
      LOG_WARN("failed to check need merge", K(ret));
    }
  } else if (OB_FAIL(ObTablet::check_transfer_seq_equal(*tmp_tablet_handle.get_obj(), param_.transfer_seq_))) {
    LOG_WARN("tmp tablet transfer seq not eq with old transfer seq", K(ret),
        "tmp_tablet_meta", tmp_tablet_handle.get_obj()->get_tablet_meta(),
        "old_transfer_seq", param_.transfer_seq_);
  } else if (FALSE_IT(compat_mode_ = tmp_tablet_handle.get_obj()->get_tablet_meta().compat_mode_)) {
  } else if (is_mini_merge(merge_type_)) {
    int64_t inc_sstable_cnt = 0;
    bool is_exist = false;
    if (OB_FAIL(MTL(ObTenantDagScheduler *)->check_dag_exist(this, is_exist))) {
      LOG_WARN("failed to check dag exist", K(ret), K_(param));
    } else if (FALSE_IT(inc_sstable_cnt = tmp_tablet_handle.get_obj()->get_minor_table_count() + (is_exist ? 1 : 0))) {
    } else if (ObPartitionMergePolicy::is_sstable_count_not_safe(inc_sstable_cnt)) {
      ret = OB_TOO_MANY_SSTABLE;
      LOG_WARN("Too many sstables in tablet, cannot schdule mini compaction, retry later",
            K(ret), K_(ls_id), K_(tablet_id), K(inc_sstable_cnt), K(tmp_tablet_handle.get_obj()));
      ObPartitionMergePolicy::diagnose_table_count_unsafe(MINI_MERGE, ObDiagnoseTabletType::TYPE_MINI_MERGE,
          *tmp_tablet_handle.get_obj());
    }
  }

  int tmp_ret = OB_SUCCESS;
  if (OB_SUCC(ret) && OB_TMP_FAIL(collect_compaction_param(tmp_tablet_handle))) { // it's OK to use old tablet handle
    LOG_WARN("failed to collect compaction param", K(tmp_ret), K_(param));
  }
  return ret;
}

int64_t ObTabletMergeDag::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;

  if (OB_ISNULL(buf) || buf_len <= 0) {
    // do nothing
  } else {
    databuff_printf(buf, buf_len, pos, "{");
    databuff_printf(buf, buf_len, pos, "ObIDag:");
    pos += ObIDag::to_string(buf + pos, buf_len - pos);

    databuff_print_json_kv_comma(buf, buf_len, pos, "param", param_);
    databuff_print_json_kv_comma(buf, buf_len, pos, "compat_mode", compat_mode_);
    if (nullptr == ctx_) {
      databuff_print_json_kv_comma(buf, buf_len, pos, "ctx", ctx_);
    } else {
      databuff_printf(buf, buf_len, pos, ", ctx:{");
      databuff_print_json_kv(buf, buf_len, pos, "sstable_version_range", ctx_->static_param_.version_range_);
      databuff_print_json_kv_comma(buf, buf_len, pos, "scn_range", ctx_->static_param_.scn_range_);
      databuff_printf(buf, buf_len, pos, "}");
    }
    databuff_printf(buf, buf_len, pos, "}");
  }

  return pos;
}

int ObTabletMergeDag::inner_init(const ObTabletMergeDagParam *param)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else if (OB_UNLIKELY(nullptr == param || !param->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KPC(param));
  } else {
    param_ = *param;
    merge_type_ = param->merge_type_;
    ls_id_ = param->ls_id_;
    tablet_id_ = param->tablet_id_;
    if (param->skip_get_tablet_) {
    } else if (OB_FAIL(get_tablet_and_compat_mode())) {
      LOG_WARN("failed to get tablet and compat mode", K(ret));
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

bool ObTabletMergeDag::operator == (const ObIDag &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObTabletMergeDag &other_merge_dag = static_cast<const ObTabletMergeDag &>(other);
    if (merge_type_ != other_merge_dag.merge_type_
        || ls_id_ != other_merge_dag.ls_id_
        || tablet_id_ != other_merge_dag.tablet_id_) {
      is_same = false;
    }
  }
  return is_same;
}

int64_t ObMergeDagHash::inner_hash() const
{
  int64_t hash_value = 0;
  // make two merge type same
  hash_value = common::murmurhash(&merge_type_, sizeof(merge_type_), hash_value);
  hash_value = common::murmurhash(&ls_id_, sizeof(ls_id_), hash_value);
  hash_value = common::murmurhash(&tablet_id_, sizeof(tablet_id_), hash_value);
  return hash_value;
}

bool ObMergeDagHash::belong_to_same_tablet(const ObMergeDagHash *other) const
{
  bool bret = false;
  if (nullptr != other) {
    bret = ls_id_ == other->ls_id_
        && tablet_id_ == other->tablet_id_;
  }
  return bret;
}

int64_t ObTabletMergeDag::hash() const
{
  return inner_hash();
}

int ObTabletMergeDag::init_by_param(const share::ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  const ObTabletMergeDagParam *merge_param = nullptr;
  if (OB_ISNULL(param)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to init sstable minor merge dag", K(ret), K(param));
  } else if (FALSE_IT(merge_param = static_cast<const ObTabletMergeDagParam *>(param))) {
  } else if (OB_FAIL(ObTabletMergeDag::inner_init(merge_param))) {
    LOG_WARN("failed to init ObTabletMergeDag", K(ret));
  }
  return ret;
}

int ObTabletMergeDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls basic tablet merge dag do not init", K(ret));
  } else {
    const char *merge_type = merge_type_to_str(merge_type_);
    if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                  ls_id_.id(),
                                  static_cast<int64_t>(tablet_id_.id()),
                                  param_.merge_version_,
                                  "merge_type", merge_type))) {
      LOG_WARN("failed to fill info param", K(ret));
    }
  }
  return ret;
}

int ObTabletMergeDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, "ls_id=%ld tablet_id=%ld", ls_id_.id(), tablet_id_.id()))) {
    LOG_WARN("failed to fill dag key", K(ret), K_(ls_id), K_(tablet_id));
  }
  return ret;
}

int ObTabletMergeDag::update_compaction_param(const ObTabletMergeDagParam &param)
{
  int ret = OB_SUCCESS;
  const ObCompactionParam &other = param.compaction_param_;

  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), K(param));
  } else if (!is_mini_merge(param.merge_type_)) {
    // do nothing.
  } else if (other.last_end_scn_ <= param_.compaction_param_.last_end_scn_) {
    // memtable flush repeatedly by checkpoint thread
  } else {
    param_.compaction_param_.last_end_scn_ = other.last_end_scn_;
    param_.compaction_param_.occupy_size_ += other.occupy_size_;
    param_.compaction_param_.sstable_cnt_ = other.sstable_cnt_; // keep with the latest dag
  }
  return ret;
}

int ObTabletMergeDag::collect_compaction_param(const ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), K(tablet_handle));
  } else if (is_mini_merge(merge_type_)) {
    param_.compaction_param_.sstable_cnt_ = tablet_handle.get_obj()->get_minor_table_count();
    param_.compaction_param_.estimate_concurrent_count(MINI_MERGE);
  }
  return ret;
}

void ObTabletMergeDag::fill_compaction_progress(
    compaction::ObTabletCompactionProgress &progress,
    ObBasicTabletMergeCtx &ctx,
    compaction::ObPartitionMergeProgress *input_progress,
    int64_t start_cg_idx, int64_t end_cg_idx)
{
  int tmp_ret = OB_SUCCESS;
  progress.tenant_id_ = MTL_ID();
  progress.merge_type_ = ctx.get_inner_table_merge_type();
  progress.merge_version_ = ctx.get_merge_version();
  progress.status_ = get_dag_status();
  progress.ls_id_ = ctx.get_ls_id().id();
  progress.tablet_id_ = ctx.get_tablet_id().id();
  progress.dag_id_ = get_dag_id();
  progress.create_time_ = add_time_;
  progress.start_time_ = start_time_;
  progress.progressive_merge_round_ = ctx.static_param_.progressive_merge_round_;
  progress.estimated_finish_time_ = ObTimeUtility::fast_current_time() + ObCompactionProgress::EXTRA_TIME;
  progress.start_cg_idx_ = start_cg_idx;
  progress.end_cg_idx_ = end_cg_idx;

  if (OB_NOT_NULL(input_progress)
      && OB_UNLIKELY(OB_SUCCESS != (tmp_ret = input_progress->get_progress_info(progress)))) {
    LOG_WARN_RET(tmp_ret, "failed to get progress info", K(tmp_ret));
  } else {
    LOG_TRACE("success to get progress info", K(tmp_ret), K(input_progress));
  }

  if (DAG_STATUS_FINISH == progress.status_) { // fix merge_progress
    progress.unfinished_data_size_ = 0;
    progress.estimated_finish_time_ = ObTimeUtility::fast_current_time();
  }
}

int ObTabletMergeDag::gene_compaction_info(compaction::ObTabletCompactionProgress &input_progress)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_NOT_NULL(ctx_) && ObIDag::DAG_STATUS_NODE_RUNNING == get_dag_status()) {
    fill_compaction_progress(input_progress, *ctx_, ctx_->info_collector_.merge_progress_);
  } else {
    ret = OB_EAGAIN;
  }
  return ret;
}

void ObTabletMergeDag::fill_diagnose_compaction_progress(
    compaction::ObDiagnoseTabletCompProgress &progress,
    ObBasicTabletMergeCtx *ctx,
    compaction::ObPartitionMergeProgress *input_progress,
    int64_t start_cg_idx, int64_t end_cg_idx)
{
  progress.tenant_id_ = MTL_ID();
  progress.merge_type_ = merge_type_;
  progress.status_ = get_dag_status();
  progress.dag_id_ = get_dag_id();
  progress.create_time_ = add_time_;
  progress.start_time_ = start_time_;
  progress.start_cg_idx_ = start_cg_idx;
  progress.end_cg_idx_ = end_cg_idx;

  if (OB_NOT_NULL(ctx)) {
    progress.merge_type_ = ctx->get_inner_table_merge_type();
    progress.snapshot_version_ = ctx->get_snapshot();
    progress.base_version_ = ctx->static_param_.version_range_.base_version_;

    if (OB_NOT_NULL(input_progress)) {
      int64_t tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(input_progress->get_progress_info(progress))) {
        LOG_WARN_RET(tmp_ret, "failed to get progress info", K(tmp_ret), KPC(ctx), K(progress));
      } else if (OB_TMP_FAIL(input_progress->diagnose_progress(progress))) {
        LOG_WARN_RET(tmp_ret, "failed to diagnose progress info", K(tmp_ret), KPC(ctx), K(progress));
      } else {
        LOG_TRACE("success to diagnose progress", K(tmp_ret), K(progress));
      }
    }
  }
}

int ObTabletMergeDag::diagnose_compaction_info(compaction::ObDiagnoseTabletCompProgress &input_progress)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (DAG_STATUS_NODE_RUNNING == get_dag_status()) { // only diagnose running dag
    compaction::ObPartitionMergeProgress *merge_progress = nullptr;
    if (OB_NOT_NULL(ctx_)) {
      merge_progress = ctx_->info_collector_.merge_progress_;
    }
    fill_diagnose_compaction_progress(input_progress, ctx_, merge_progress);
  }
  return ret;
}

void ObTabletMergeDag::set_dag_error_location()
{
  if (OB_NOT_NULL(ctx_)) {
    error_location_.set(ctx_->info_collector_.error_location_);
  }
}

int ObTabletMergeDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObTabletMergePrepareTask *task = nullptr;
  if (OB_FAIL(create_task(nullptr/*parent*/, task))) {
    STORAGE_LOG(WARN, "fail to alloc prepare task", K(ret));
  }
  return ret;
}

/*
 *  ----------------------------------------------ObTabletMergeExecuteDag--------------------------------------------------
 */

ObTabletMergeExecuteDag::ObTabletMergeExecuteDag()
  : ObTabletMergeDag(ObDagType::DAG_TYPE_MERGE_EXECUTE),
    result_()
{
}

ObTabletMergeExecuteDag::~ObTabletMergeExecuteDag()
{
}

int ObTabletMergeExecuteDag::init_by_param(const share::ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  const ObTabletMergeDagParam *merge_param = nullptr;
  if (OB_ISNULL(param)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to init sstable minor merge dag", K(ret), K(param));
  } else if (FALSE_IT(merge_param = static_cast<const ObTabletMergeDagParam *>(param))) {
  } else if (OB_UNLIKELY(!is_multi_version_merge(merge_param->merge_type_)
      && !is_meta_major_merge(merge_param->merge_type_))) {
    ret = OB_ERR_SYS;
    LOG_ERROR("Unexpected merge type to init minor merge dag", K(ret), KPC(merge_param));
  } else if (OB_FAIL(ObTabletMergeDag::inner_init(merge_param))) {
    LOG_WARN("failed to init ObTabletMergeDag", K(ret));
  }

  return ret;
}

int ObTabletMergeExecuteDag::prepare_init(
    const ObTabletMergeDagParam &param,
    const lib::Worker::CompatMode compat_mode,
    const ObGetMergeTablesResult &result,
    ObLSHandle &ls_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY((!is_minor_merge_type(param.merge_type_)
      && !is_meta_major_merge(param.merge_type_) && !is_mds_minor_merge(param.merge_type_))
      || !result.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("result is invalid", K(ret), K(result));
  } else {
    param_ = param;
    merge_type_ = param.merge_type_;
    ls_id_ = param.ls_id_;
    tablet_id_ = param.tablet_id_;
    compat_mode_ = compat_mode;
    if (OB_FAIL(result_.assign(result))) {
      LOG_WARN("failed to assgin result", K(ret), K(result));
    } else {
      table_key_array_.set_attr(ObMemAttr(MTL_ID(), "TableKeyArr", ObCtxIds::MERGE_NORMAL_CTX_ID));
      for (int64_t i = 0; OB_SUCC(ret) && i < result.handle_.get_count(); ++i) {
        if (OB_FAIL(table_key_array_.push_back(result.handle_.get_table(i)->get_key()))) {
          LOG_WARN("failed to push back key", K(ret));
        }
      }
    }
    if (FAILEDx(create_first_task())) {
      LOG_WARN("failed to create first task", K(ret), K(result));
    } else {
      result_.simplify_handle(); // clear tables handle, get sstable when execute after get tablet_handle
      is_inited_ = true;
    }
  }
  return ret;
}

bool ObTabletMergeExecuteDag::operator == (const ObIDag &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObTabletMergeExecuteDag &other_merge_dag = static_cast<const ObTabletMergeExecuteDag &>(other);
    if (!belong_to_same_tablet(&other_merge_dag)
        || merge_type_ != other_merge_dag.merge_type_) { // different merge type
      is_same = false;
    } else if (is_minor_merge(merge_type_)) {  // range cross -> two dag equal
      const share::ObScnRange &merge_scn_range = get_merge_range();
      const share::ObScnRange &other_merge_scn_range = other_merge_dag.get_merge_range();
      if (merge_scn_range.start_scn_ >= other_merge_scn_range.end_scn_
          || merge_scn_range.end_scn_ <= other_merge_scn_range.start_scn_) {
        is_same = false;
      }
    }
  }
  return is_same;
}

/*
 *  ----------------------------------------------ObTabletMergePrepareTask--------------------------------------------------
 */

ObTabletMergePrepareTask::ObTabletMergePrepareTask()
  : ObITask(ObITask::TASK_TYPE_SSTABLE_MERGE_PREPARE),
    is_inited_(false),
    merge_dag_(NULL)
{
}

ObTabletMergePrepareTask::~ObTabletMergePrepareTask()
{
}

int ObTabletMergePrepareTask::init()
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else if (OB_ISNULL(dag_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("dag must not null", K(ret));
  } else if (OB_UNLIKELY(!is_compaction_dag(dag_->get_type()))) {
    ret = OB_ERR_SYS;
    LOG_ERROR("dag type not match", K(ret), KPC(dag_));
  } else {
    merge_dag_ = static_cast<ObTabletMergeDag *>(dag_);
    if (OB_UNLIKELY(!merge_dag_->get_param().is_valid())) {
      ret = OB_ERR_SYS;
      LOG_WARN("param_ is not valid", K(ret), K(merge_dag_->get_param()));
    } else {
      is_inited_ = true;
    }
  }

  return ret;
}

int ObTabletMergeDag::alloc_merge_ctx()
{
  int ret = OB_SUCCESS;
  const ObMergeType merge_type = param_.merge_type_;
  #define NEW_CTX(CTX_NAME) \
    OB_NEWx(CTX_NAME, &allocator_, param_, allocator_)
  if (OB_NOT_NULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is not null", K(ret), K(ctx_));
  } else if (FALSE_IT(prepare_allocator(merge_type, param_.is_reserve_mode_, allocator_))) {
  } else if (is_mini_merge(merge_type)) {
    ctx_ = NEW_CTX(ObTabletMiniMergeCtx);
  } else if (is_major_or_meta_merge_type(merge_type)) {
    ctx_ = NEW_CTX(ObTabletMajorMergeCtx);
  } else if (is_multi_version_merge(merge_type) && !is_mds_minor_merge(merge_type)) {
    ctx_ = NEW_CTX(ObTabletExeMergeCtx);
  } else if (is_mds_mini_merge(merge_type) || is_backfill_tx_merge(merge_type)) {
    ctx_ = NEW_CTX(ObTabletMergeCtx);
  } else if (is_mds_minor_merge(merge_type)) {
    ctx_ = NEW_CTX(ObTabletMdsMinorMergeCtx);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid merge type", KR(ret), K(merge_type), KP(ctx_));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(ctx_)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate ctx", KR(ret), KP(ctx_));
  } else {
    ctx_->merge_dag_ = this;
    ctx_->init_time_guard(get_add_time());
    ctx_->time_guard_click(ObStorageCompactionTimeGuard::DAG_WAIT_TO_SCHEDULE);
  }
  return ret;
}

int ObTabletMergeDag::prepare_merge_ctx(bool &finish_flag)
{
  int ret = OB_SUCCESS;
  finish_flag = false;
  if (OB_FAIL(alloc_merge_ctx())) {
    LOG_WARN("failed to alloc ctx", KR(ret), K_(param));
  } else if (OB_FAIL(ctx_->build_ctx(finish_flag))) {
    LOG_WARN("failed to build ctx", KR(ret), K_(param), KP_(ctx));
  }
  return ret;
}

int ObTabletMergePrepareTask::process()
{
  int ret = OB_SUCCESS;
  ObTenantStatEstGuard stat_est_guard(MTL_ID());
  ObBasicTabletMergeCtx *ctx = NULL;
  ObTaskController::get().switch_task(share::ObTaskType::DATA_MAINTAIN);
  bool finish_flag = false;

  DEBUG_SYNC(MERGE_PARTITION_TASK);
  DEBUG_SYNC(AFTER_EMPTY_SHELL_TABLET_CREATE);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (is_mini_merge(merge_dag_->get_param().merge_type_)
          && !merge_dag_->get_param().skip_get_tablet_
          && MTL(ObTenantCompactionMemPool *)->acquire_reserve_mem()) {
    merge_dag_->set_reserve_mode();
    SET_RESERVE_MODE();
    FLOG_INFO("Mini Compaction Enter the Reserve Mode", KPC(merge_dag_));
  }
  if (FAILEDx(merge_dag_->prepare_merge_ctx(finish_flag))) {
    LOG_WARN("failed to prepare merge ctx", K(ret), KPC(ctx), KPC(merge_dag_));
  } else if (OB_ISNULL(ctx = merge_dag_->get_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is unexpected null", K(ret), KP(ctx), KPC(merge_dag_));
  } else if (finish_flag) {
    // do nothing
  } else if (OB_FAIL(merge_dag_->generate_merge_task(*ctx, this/*prepare_task*/))) {
    LOG_WARN("failed to generate merge task", K(ret), KPC(ctx), KPC(merge_dag_));
  }

  if (OB_FAIL(ret)) {
    FLOG_WARN("sstable merge failed", K(ret), KPC(ctx), "task", *(static_cast<ObITask *>(this)));
  } else {
    FLOG_INFO("succeed to build merge ctx", "tablet_id", ctx->get_tablet_id(), K(finish_flag), KPC(ctx));
  }
  return ret;
}

int ObTabletMergeDag::generate_merge_task(
    ObBasicTabletMergeCtx &ctx,
    ObITask *prepare_task)
{
  int ret = OB_SUCCESS;
  ObTabletMergeTask *merge_task = NULL;
  ObTabletMergeFinishTask *finish_task = NULL;
  SET_MEM_CTX(ctx.mem_ctx_);

  if (OB_FAIL(create_task(prepare_task/*parent*/, merge_task, 0/*task_idx*/, ctx))) {
    LOG_WARN("fail to create merge task", K(ret), K(ctx));
  } else if (OB_ISNULL(merge_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge task is unexpected null", KR(ret), KP(merge_task));
  } else if (OB_FAIL(create_task(merge_task/*parent*/, finish_task))) {
    LOG_WARN("fail to create finish task", K(ret), K(ctx));
  }
  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(merge_task)) {
      remove_task(*merge_task);
      merge_task = nullptr;
    }
    if (OB_NOT_NULL(finish_task)) {
      remove_task(*finish_task);
      finish_task = nullptr;
    }
  }
  return ret;
}
/*
 *  ----------------------------------------------ObTabletMergeFinishTask--------------------------------------------------
 */

ObTabletMergeFinishTask::ObTabletMergeFinishTask()
  : ObITask(ObITask::TASK_TYPE_SSTABLE_MERGE_FINISH),
    is_inited_(false),
    merge_dag_(NULL)
{
}

ObTabletMergeFinishTask::~ObTabletMergeFinishTask()
{
}

int ObTabletMergeFinishTask::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else if (OB_ISNULL(dag_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("dag must not null", K(ret));
  } else if (!is_compaction_dag(dag_->get_type())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("dag type not match", K(ret), KPC(dag_));
  } else {
    merge_dag_ = static_cast<ObTabletMergeDag *>(dag_);
    if (OB_UNLIKELY(nullptr == merge_dag_->get_ctx() || !merge_dag_->get_ctx()->is_valid())) {
      ret = OB_ERR_SYS;
      LOG_WARN("ctx not valid", K(ret), KPC(merge_dag_->get_ctx()));
    } else {
      is_inited_ = true;
    }
  }

  return ret;
}

int ObTabletMergeFinishTask::report_checkpoint_diagnose_info(ObTabletMergeCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObITable *table = nullptr;
  storage::ObTablesHandleArray &tables_handle = ctx.static_param_.tables_handle_;
  for (int64_t i = 0; i < tables_handle.get_count() && OB_SUCC(ret); i++) {
    if (OB_ISNULL(table = tables_handle.get_table(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table is null", K(ret), K(tables_handle), KP(table));
    } else if (OB_UNLIKELY(!table->is_memtable())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table is not memtable", K(ret), K(tables_handle), KPC(table));
    } else {
      const ObSSTableMergeInfo &sstable_merge_info = ctx.get_merge_info().get_sstable_merge_info();
      if (table->is_data_memtable()) {
        ObMemtable *memtable = nullptr;
        memtable = static_cast<ObMemtable*>(table);
        memtable->report_memtable_diagnose_info(ObMemtable::UpdateMergeInfoForMemtable(
              sstable_merge_info.merge_start_time_, sstable_merge_info.merge_finish_time_,
              sstable_merge_info.occupy_size_, sstable_merge_info.concurrent_cnt_));
      } else {
        ObIMemtable *memtable = nullptr;
        memtable = static_cast<ObIMemtable*>(table);
        REPORT_CHECKPOINT_DIAGNOSE_INFO(update_merge_info_for_checkpoint_unit, memtable)
      }
    }
  }
  return ret;
}

int ObTabletMergeFinishTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTaskController::get().switch_task(share::ObTaskType::DATA_MAINTAIN);
  ObTabletMergeCtx *ctx_ptr = nullptr;
  DEBUG_SYNC(MERGE_PARTITION_FINISH_TASK);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited yet", K(ret));
  } else if (OB_UNLIKELY(nullptr == merge_dag_
      || (nullptr == (ctx_ptr = static_cast<ObTabletMergeCtx *>(merge_dag_->get_ctx()))))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null ctx", K(ret));
  } else if (FALSE_IT(SET_MEM_CTX(ctx_ptr->mem_ctx_))) {
  } else if (OB_FAIL(ctx_ptr->update_tablet_after_merge())) {
    LOG_WARN("failed to update tablet after merge", KR(ret), KPC(ctx_ptr));
  }
  if (OB_FAIL(ret)) {
    FLOG_WARN("sstable merge failed", K(ret), KPC(ctx_ptr), "task", *(static_cast<ObITask *>(this)));
  } else {
    ObITable *sstable = ctx_ptr->merged_table_handle_.get_table();
    // ATTENTION! Critical diagnostic log, DO NOT CHANGE!!!
    FLOG_INFO("sstable merge finish", K(ret), "merge_info", ctx_ptr->get_merge_info(),
        KPC(sstable), "mem_peak", ctx_ptr->mem_ctx_.get_total_mem_peak(), "compat_mode", merge_dag_->get_compat_mode(),
        "time_guard", ctx_ptr->info_collector_.time_guard_);
  }

  if (OB_FAIL(ret)) {
  } else if (is_mini_merge(ctx_ptr->static_param_.get_merge_type())
      && OB_TMP_FAIL(report_checkpoint_diagnose_info(*ctx_ptr))) {
    LOG_WARN("failed to report_checkpoint_diagnose_info", K(tmp_ret), KPC(ctx_ptr));
  }

  return ret;
}
/*
 *  ----------------------------------------------ObTabletMergeTask--------------------------------------------------
 */
ObTabletMergeTask::ObTabletMergeTask()
  : ObITask(ObITask::TASK_TYPE_MACROMERGE),
    allocator_("MergerArena"),
    idx_(0),
    ctx_(nullptr),
    merger_(nullptr),
    is_inited_(false)
{
}

ObTabletMergeTask::~ObTabletMergeTask()
{
  if (OB_NOT_NULL(merger_)) {
    merger_->~ObPartitionMerger();
    merger_ = nullptr;
  }
  allocator_.~ObLocalArena();
}

int ObTabletMergeTask::init(const int64_t idx, ObBasicTabletMergeCtx &ctx)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(idx < 0 || !ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is invalid", K(ret), K(idx), K(ctx));
  } else if (FALSE_IT(allocator_.bind_mem_ctx(ctx.mem_ctx_))) { // maybe not bind mem ctx in constructor
  } else if (FALSE_IT(prepare_allocator(ctx.get_merge_type(), ctx.get_dag_param().is_reserve_mode_,
      allocator_, false/*is_global_mem*/))) {
  } else if (is_major_or_meta_merge_type(ctx.get_merge_type())) {
    merger_ = OB_NEWx(ObPartitionMajorMerger, &allocator_, allocator_, ctx.static_param_);
  } else {
    merger_ = OB_NEWx(ObPartitionMinorMerger, &allocator_, allocator_, ctx.static_param_);
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(merger_)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "Failed to allocate memory for partition merger", K(ret), KP(merger_));
  } else {
    idx_ = idx;
    ctx_ = &ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObTabletMergeTask::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (idx_ + 1 == ctx_->get_concurrent_cnt()) {
    ret = OB_ITER_END;
  } else if (!is_compaction_dag(dag_->get_type()) && !is_ha_backfill_dag(dag_->get_type())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("dag type not match", K(ret), KPC(dag_));
  } else {
    ObTabletMergeTask *merge_task = NULL;
    ObTabletMergeDag *merge_dag = static_cast<ObTabletMergeDag *>(dag_);

    if (OB_FAIL(merge_dag->alloc_task(merge_task))) {
      LOG_WARN("fail to alloc task", K(ret));
    } else if (OB_FAIL(merge_task->init(idx_ + 1, *ctx_))) {
      LOG_WARN("fail to init task", K(ret));
    } else {
      next_task = merge_task;
    }
  }
  return ret;
}

int ObTabletMergeTask::process()
{
  int ret = OB_SUCCESS;
  ObTenantStatEstGuard stat_est_guard(MTL_ID());
  ObTaskController::get().switch_task(share::ObTaskType::DATA_MAINTAIN);

#ifdef ERRSIM
  ret = OB_E(EventTable::EN_COMPACTION_MERGE_TASK) OB_SUCCESS;
  if (OB_FAIL(ret)) {
    STORAGE_LOG(INFO, "ERRSIM EN_COMPACTION_MERGE_TASK", K(ret));
    return ret;
  }
  if (OB_NOT_NULL(ctx_) && ctx_->get_tablet_id().id() > ObTabletID::MIN_USER_TABLET_ID) {
    DEBUG_SYNC(MERGE_TASK_PROCESS);
  }
  ret = SPECIFIED_SERVER_STOP_COMPACTION;
  if (OB_FAIL(ret)) {
    if (-ret == GCTX.server_id_) {
      STORAGE_LOG(INFO, "ERRSIM SPECIFIED_SERVER_STOP_COMPACTION", K(ret));
      return OB_EAGAIN;
    } else {
      ret = OB_SUCCESS;
    }
  }
#endif

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTabletMergeTask is not inited", K(ret));
  } else if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected null merge ctx", K(ret));
  } else if (OB_UNLIKELY(is_major_merge_type(ctx_->get_merge_type())
                         && !MTL(ObTenantTabletScheduler *)->could_major_merge_start())) {
    ret = OB_CANCELED;
    LOG_INFO("Merge has been paused", K(ret));
    CTX_SET_DIAGNOSE_LOCATION(*ctx_);
  } else if (OB_ISNULL(merger_)) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "Unexpected null partition merger", K(ret));
  } else {
    ctx_->mem_ctx_.mem_click();
    if (OB_FAIL(merger_->merge_partition(*ctx_, idx_))) {
      if (is_major_or_meta_merge_type(ctx_->get_merge_type()) && OB_ENCODING_EST_SIZE_OVERFLOW == ret) {
        STORAGE_LOG(WARN, "failed to merge partition with possibly encoding error, "
            "retry with flat row store type", K(ret), KPC(ctx_), K_(idx));
        merger_->reset();
        merger_->force_flat_format();
        if (OB_FAIL(merger_->merge_partition(*ctx_, idx_))) {
          if (OB_ALLOCATE_MEMORY_FAILED == ret || OB_TIMEOUT == ret || OB_IO_ERROR == ret) {
            STORAGE_LOG(WARN, "retry merge partition with flat row store type failed", K(ret));
          } else {
            STORAGE_LOG(ERROR, "retry merge partition with flat row store type failed", K(ret));
          }
        }
      } else {
        STORAGE_LOG(WARN, "failed to merge partition", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      ctx_->mem_ctx_.mem_click();
      FLOG_INFO("merge macro blocks ok", K(idx_), "task", *this);
    }
    merger_->reset();
  }

  if (OB_FAIL(ret)) {
    if (NULL != ctx_) {
      if (OB_CANCELED == ret) {
        STORAGE_LOG(INFO, "merge is canceled", K(ret), "param", ctx_->get_dag_param(), K(idx_));
      } else {
        STORAGE_LOG(WARN, "failed to merge", K(ret), "param", ctx_->get_dag_param(), K(idx_));
      }
    }
  }

  return ret;
}

ObTxTableMergeDag::~ObTxTableMergeDag()
{
  if (param_.is_reserve_mode_) {
    MTL(ObTenantCompactionMemPool *)->release_reserve_mem();
    FLOG_INFO("TxTable Compaction Leave the Reserve Mode", K(param_));
  }
}

ObTabletMiniMergeDag::~ObTabletMiniMergeDag()
{
  if (param_.is_reserve_mode_) {
    MTL(ObTenantCompactionMemPool *)->release_reserve_mem();
    FLOG_INFO("Mini Compaction Leave the Reserve Mode", K(param_));
  }
}

void prepare_allocator(
    const compaction::ObMergeType &merge_type,
    const bool is_reserve_mode,
    common::ObIAllocator &allocator,
    const bool is_global_mem)
{
  const lib::ObLabel label = is_major_merge_type(merge_type)
                           ? (is_global_mem ? "MajorMemCtx" : "MajorMemMerger")
                           : (is_mini_merge(merge_type) ? (is_global_mem ? "MiniMemCtx" : "MiniMemMerger")
                                                        : (is_global_mem ? "MinorMemCtx" : "MinorMemMerger"));


  const int64_t ctx_id = is_reserve_mode && is_mini_merge(merge_type)
                       ? ObCtxIds::MERGE_RESERVE_CTX_ID
                       : ObCtxIds::MERGE_NORMAL_CTX_ID;

  allocator.set_attr(lib::ObMemAttr(MTL_ID(), label, ctx_id));
}


/*
 *  ----------------------------------------ObBatchFreezeTabletsParam--------------------------------------------
 */
ObBatchFreezeTabletsParam::ObBatchFreezeTabletsParam()
  : ls_id_(),
    tablet_pairs_()
{
  tablet_pairs_.set_attr(lib::ObMemAttr(MTL_ID(), "BtFrzTbls", ObCtxIds::MERGE_NORMAL_CTX_ID));
}

int ObBatchFreezeTabletsParam::assign(
    const ObBatchFreezeTabletsParam &other)
{
  int ret = OB_SUCCESS;

  if (this == &other) {
    // do nothing
  } else if (OB_FAIL(tablet_pairs_.assign(other.tablet_pairs_))) {
    LOG_WARN("failed to copy tablet ids", K(ret));
  } else {
    ls_id_ = other.ls_id_;
  }
  return ret;
}

int64_t ObBatchFreezeTabletsParam::get_hash() const
{
  int64_t hash_val = 0;
  hash_val = common::murmurhash(&ls_id_, sizeof(ls_id_), hash_val);
  return hash_val;
}


ObBatchFreezeTabletsDag::ObBatchFreezeTabletsDag()
  : ObIDag(share::ObDagType::DAG_TYPE_BATCH_FREEZE_TABLETS),
    is_inited_(false),
    param_()
{
}

ObBatchFreezeTabletsDag::~ObBatchFreezeTabletsDag()
{
}

int ObBatchFreezeTabletsDag::init_by_param(
    const share::ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  const ObBatchFreezeTabletsParam *init_param = nullptr;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObBatchFreezeTabletsDag has been inited", K(ret), KPC(this));
  } else if (FALSE_IT(init_param = static_cast<const ObBatchFreezeTabletsParam *>(param))) {
  } else if (OB_UNLIKELY(nullptr == init_param || !init_param->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), KPC(init_param));
  } else if (OB_FAIL(param_.assign(*init_param))) {
    LOG_WARN("failed to init param", K(ret), KPC(init_param));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObBatchFreezeTabletsDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObBatchFreezeTabletsTask *task = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("BatchFreezeTabletsDag has not inited", K(ret));
  } else if (OB_FAIL(create_task(nullptr/*parent*/, task))) {
    LOG_WARN("failed to create batch tablet sstable task", K(ret));
  }
  return ret;
}

bool ObBatchFreezeTabletsDag::operator == (const ObIDag &other) const
{
  bool is_same = true;

  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else if (param_.ls_id_ != static_cast<const ObBatchFreezeTabletsDag &>(other).param_.ls_id_) {
    is_same = false;
  }
  return is_same;
}

int64_t ObBatchFreezeTabletsDag::hash() const
{
  return param_.get_hash();
}

int ObBatchFreezeTabletsDag::fill_info_param(
    compaction::ObIBasicInfoParam *&out_param,
    ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBatchFreezeTabletsDag not inited", K(ret));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param,
                                             allocator,
                                             get_type(),
                                             param_.ls_id_.id(),
                                             param_.tablet_pairs_.count()))) {
    LOG_WARN("failed to fill info param", K(ret), K(param_));
  }
  return ret;
}

int ObBatchFreezeTabletsDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, "ls_id=%ld freeze_tablet_cnt=%ld",
      param_.ls_id_.id(), param_.tablet_pairs_.count()))) {
    LOG_WARN("failed to fill dag key", K(ret), K(param_));
  }
  return ret;
}


ObBatchFreezeTabletsTask::ObBatchFreezeTabletsTask()
  : ObITask(ObITask::TASK_TYPE_BATCH_FREEZE_TABLETS),
    is_inited_(false),
    base_dag_(nullptr)
{
}

ObBatchFreezeTabletsTask::~ObBatchFreezeTabletsTask()
{
}

int ObBatchFreezeTabletsTask::init()
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObBatchFreezeTabletsTask init twice", K(ret));
  } else if (OB_ISNULL(dag_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null dag", K(ret));
  } else if (OB_UNLIKELY(ObDagType::ObDagTypeEnum::DAG_TYPE_BATCH_FREEZE_TABLETS != dag_->get_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected dag type", K(ret));
  } else if (FALSE_IT(base_dag_ = static_cast<ObBatchFreezeTabletsDag *>(dag_))) {
  } else if (OB_UNLIKELY(!base_dag_->get_param().is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected not valid param", K(ret), K(base_dag_->get_param()));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObBatchFreezeTabletsTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObBatchFreezeTabletsParam &param = base_dag_->get_param();

  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  int64_t weak_read_ts = 0;
  if (OB_FAIL(MTL(ObLSService *)->get_ls(param.ls_id_, ls_handle, ObLSGetMod::COMPACT_MODE))) {
    LOG_WARN("failed to get log stream", K(ret), K(param));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null ls", K(ret), K(param));
  } else {
    weak_read_ts = ls->get_ls_wrs_handler()->get_ls_weak_read_ts().get_val_for_tx();
  }

  int64_t fail_freeze_cnt = 0;
  int64_t succ_schedule_cnt = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < param.tablet_pairs_.count(); ++i) {
    const ObTabletSchedulePair &cur_pair = param.tablet_pairs_.at(i);
    ObTabletHandle tablet_handle;
    ObTablet *tablet = nullptr;
    const bool is_sync = true;
    const bool need_rewrite_meta = true;

    if (OB_UNLIKELY(!cur_pair.is_valid())) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_WARN_RET(tmp_ret, "get invalid tablet pair", K(cur_pair));
    } else if (cur_pair.schedule_merge_scn_ > weak_read_ts) {
      // no need to force freeze
    } else if (OB_TMP_FAIL(MTL(ObTenantFreezer *)->tablet_freeze(cur_pair.tablet_id_, need_rewrite_meta, is_sync))) {
      LOG_WARN_RET(tmp_ret, "failed to force freeze tablet", K(param), K(cur_pair));
      ++fail_freeze_cnt;
    } else if (!MTL(ObTenantTabletScheduler *)->could_major_merge_start()) {
      // merge is suspended
    } else if (OB_TMP_FAIL(ls->get_tablet_svr()->get_tablet(cur_pair.tablet_id_,
                                                            tablet_handle,
                                                            0 /*timeout_us*/,
                                                            storage::ObMDSGetTabletMode::READ_ALL_COMMITED))) {
      LOG_WARN_RET(tmp_ret, "failed to get tablet", K(param), K(cur_pair));
    } else if (FALSE_IT(tablet = tablet_handle.get_obj())) {
    } else if (OB_UNLIKELY(tablet->get_snapshot_version() < cur_pair.schedule_merge_scn_)) {
      // do nothing
    } else if (!tablet->is_data_complete()) {
      // no need to schedule merge
    } else if (OB_TMP_FAIL(compaction::ObTenantTabletScheduler::schedule_merge_dag(
                   param.ls_id_, *tablet, MEDIUM_MERGE, cur_pair.schedule_merge_scn_))) {
      if (OB_SIZE_OVERFLOW != tmp_ret && OB_EAGAIN != tmp_ret) {
        ret = tmp_ret;
        LOG_WARN_RET(tmp_ret, "failed to schedule medium merge dag", K(param), K(cur_pair));
      }
    } else {
      ++succ_schedule_cnt;
    }

    if (OB_FAIL(share::dag_yield())) {
      LOG_WARN("failed to dag yield", K(ret));
    }
    if (REACH_TENANT_TIME_INTERVAL(5_s)) {
      weak_read_ts = ls->get_ls_wrs_handler()->get_ls_weak_read_ts().get_val_for_tx();
    }
  } // end for

  if (OB_UNLIKELY(fail_freeze_cnt * 2 > param.tablet_pairs_.count())) {
    ret = OB_PARTIAL_FAILED;
  }
  FLOG_INFO("batch freeze tablets finished", KR(ret), K(param), K(fail_freeze_cnt), K(succ_schedule_cnt));

  return ret;
}


} // namespace compaction
} // namespace oceanbase
