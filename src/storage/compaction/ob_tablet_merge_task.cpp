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
#include "ob_tablet_merge_ctx.h"
#include "ob_tablet_merge_task.h"
#include "ob_partition_merger.h"
#include "ob_partition_merge_policy.h"
#include "share/rc/ob_tenant_base.h"
#include "lib/stat/ob_session_stat.h"
#include "storage/blocksstable/ob_index_block_builder.h"
#include "storage/tablet/ob_tablet_common.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "ob_tenant_compaction_progress.h"
#include "ob_compaction_diagnose.h"
#include "ob_compaction_suggestion.h"
#include "ob_partition_merge_progress.h"
#include "ob_tx_table_merge_task.h"
#include "storage/ddl/ob_ddl_merge_task.h"
#include "ob_schedule_dag_func.h"
#include "ob_tenant_tablet_scheduler.h"
#include "share/ob_get_compat_mode.h"
#include "ob_sstable_merge_info_mgr.h"
#include "storage/compaction/ob_compaction_diagnose.h"
#include "storage/ob_tenant_tablet_stat_mgr.h"
#include "storage/access/ob_table_estimator.h"
#include "storage/access/ob_index_sstable_estimator.h"
#include "ob_medium_compaction_func.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "storage/compaction/ob_tablet_merge_checker.h"
#include "share/ob_get_compat_mode.h"
#include "share/ob_tablet_meta_table_compaction_operator.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "src/storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "share/resource_manager/ob_cgroup_ctrl.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace storage;
using namespace memtable;
using namespace blocksstable;
namespace compaction
{

bool is_merge_dag(ObDagType::ObDagTypeEnum dag_type)
{
  return dag_type == ObDagType::DAG_TYPE_MAJOR_MERGE
    || dag_type == ObDagType::DAG_TYPE_MERGE_EXECUTE
    || dag_type == ObDagType::DAG_TYPE_MINI_MERGE
    || dag_type == ObDagType::DAG_TYPE_TX_TABLE_MERGE
    || dag_type == ObDagType::DAG_TYPE_MDS_TABLE_MERGE;
}

/*
 *  ----------------------------------------------ObMergeParameter--------------------------------------------------
 */
ObMergeParameter::ObMergeParameter()
  : ls_id_(),
    tablet_id_(),
    ls_handle_(),
    tables_handle_(nullptr),
    merge_type_(INVALID_MERGE_TYPE),
    merge_level_(MACRO_BLOCK_MERGE_LEVEL),
    merge_schema_(nullptr),
    merge_range_(),
    sstable_logic_seq_(0),
    version_range_(),
    scn_range_(),
    rowkey_read_info_(nullptr),
    is_full_merge_(false),
    trans_state_mgr_(nullptr),
    merge_scn_()
{
}

bool ObMergeParameter::is_valid() const
{
  return (ls_id_.is_valid() && tablet_id_.is_valid())
         && ls_handle_.is_valid()
         && tables_handle_ != nullptr
         && sstable_logic_seq_ >= 0
         && !tables_handle_->empty()
         && nullptr != rowkey_read_info_
         && merge_type_ > INVALID_MERGE_TYPE
         && merge_type_ < MERGE_TYPE_MAX
         && merge_scn_.is_valid();
}

void ObMergeParameter::reset()
{
  ls_id_.reset();
  tablet_id_.reset();
  ls_handle_.reset();
  tables_handle_ = nullptr;
  merge_type_ = INVALID_MERGE_TYPE;
  merge_level_ = MACRO_BLOCK_MERGE_LEVEL;
  merge_schema_ = nullptr;
  sstable_logic_seq_ = 0;
  merge_range_.reset();
  version_range_.reset();
  scn_range_.reset();
  is_full_merge_ = false;
  trans_state_mgr_ = nullptr;
  rowkey_read_info_ = nullptr;
  merge_scn_.reset();
}

int ObMergeParameter::init(compaction::ObTabletMergeCtx &merge_ctx, const int64_t idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!merge_ctx.is_valid() || idx < 0 || idx >= merge_ctx.get_concurrent_cnt())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to assign merge parameter", K(merge_ctx), K(idx), K(ret));
  } else if (OB_FAIL(merge_ctx.get_merge_range(idx, merge_range_))) {
    STORAGE_LOG(WARN, "failed to get merge range from merge context", K(ret));
  } else {
    ls_id_ = merge_ctx.param_.ls_id_;
    tablet_id_ = merge_ctx.param_.tablet_id_;
    ls_handle_ = merge_ctx.ls_handle_;
    tables_handle_ = &merge_ctx.tables_handle_;
    merge_type_ = merge_ctx.param_.merge_type_;
    merge_level_ = merge_ctx.merge_level_;
    merge_schema_ = merge_ctx.get_schema();
    version_range_ = merge_ctx.sstable_version_range_;
    sstable_logic_seq_ = merge_ctx.sstable_logic_seq_;
    if (is_major_merge_type(merge_type_)) {
      // major merge should only read data between two major freeze points
      // but there will be some minor sstables which across major freeze points
      version_range_.base_version_ = MAX(merge_ctx.read_base_version_, version_range_.base_version_);
    } else if (is_meta_major_merge(merge_type_)) {
      // meta major merge does not keep multi-version
      version_range_.multi_version_start_ = version_range_.snapshot_version_;
    } else if (is_multi_version_merge(merge_type_)) {
      // minor compaction always need to read all the data from input table
      // rewrite version to whole version range
      version_range_.snapshot_version_ = MERGE_READ_SNAPSHOT_VERSION;
    }
    scn_range_ = merge_ctx.scn_range_;
    is_full_merge_ = merge_ctx.is_full_merge_;
    rowkey_read_info_ = &(merge_ctx.tablet_handle_.get_obj()->get_rowkey_read_info());
    merge_scn_ = merge_ctx.merge_scn_;

    if (merge_scn_ > scn_range_.end_scn_) {
      if (ObMergeType::BACKFILL_TX_MERGE != merge_type_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("merge scn is bigger than scn range but merge type is not backfill, unexpected",
            K(ret), K(merge_scn_), K(scn_range_), K(merge_type_));
      } else {
        FLOG_INFO("set backfill merge scn", K(merge_scn_), K(scn_range_), K(merge_type_));
      }
    }
  }
  return ret;
}

/*
 *  ----------------------------------------------ObTabletMergeDagParam--------------------------------------------------
 */

ObTabletMergeDagParam::ObTabletMergeDagParam()
  :  for_diagnose_(false),
     is_tenant_major_merge_(false),
     need_swap_tablet_flag_(false),
     merge_type_(INVALID_MERGE_TYPE),
     merge_version_(0),
     ls_id_(),
     tablet_id_(),
     report_(nullptr)
{
}

ObTabletMergeDagParam::ObTabletMergeDagParam(
    const storage::ObMergeType merge_type,
    const share::ObLSID &ls_id,
    const ObTabletID &tablet_id)
  :  for_diagnose_(false),
     is_tenant_major_merge_(false),
     need_swap_tablet_flag_(false),
     merge_type_(merge_type),
     merge_version_(0),
     ls_id_(ls_id),
     tablet_id_(tablet_id),
     report_(nullptr)
{
}

bool ObTabletMergeDagParam::is_valid() const
{
  return ls_id_.is_valid()
         && tablet_id_.is_valid()
         && (merge_type_ > INVALID_MERGE_TYPE && merge_type_ < MERGE_TYPE_MAX)
         && (!is_major_merge_type(merge_type_) || merge_version_ >= 0);
}

int64_t ObTabletMergeDagParam::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV("merge_type", merge_type_to_str(merge_type_),
       K_(merge_version),
       K_(ls_id),
       K_(tablet_id),
       KP(report_),
       K_(for_diagnose),
       K_(is_tenant_major_merge),
       K_(need_swap_tablet_flag));
  J_OBJ_END();
  return pos;
}

ObBasicTabletMergeDag::ObBasicTabletMergeDag(
    const ObDagType::ObDagTypeEnum type)
  : ObIDag(type),
    ObMergeDagHash(),
    is_inited_(false),
    compat_mode_(lib::Worker::CompatMode::INVALID),
    ctx_(nullptr),
    param_(),
    allocator_("MergeDag", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID())
{
}

ObBasicTabletMergeDag::~ObBasicTabletMergeDag()
{
  int report_ret = OB_SUCCESS;
  if (OB_NOT_NULL(ctx_)) {
  //    if (ctx_->partition_guard_.get_pg_partition() != NULL) {
  //      ctx_->partition_guard_.get_pg_partition()->set_merge_status(OB_SUCCESS == get_dag_ret());
  //    }
    ctx_->~ObTabletMergeCtx();
    allocator_.free(ctx_);
    ctx_ = nullptr;
  }
}

// create ObTabletMergeCtx when Dag start running
int ObBasicTabletMergeDag::alloc_merge_ctx()
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_NOT_NULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is not null", K(ret), K(ctx_));
  } else if (NULL == (buf = allocator_.alloc(sizeof(ObTabletMergeCtx)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocator memory", K(ret), "alloc_size", sizeof(ObTabletMergeCtx));
  } else {
    ctx_ = new(buf) ObTabletMergeCtx(param_, allocator_);
    ctx_->merge_dag_ = this;
  }
  return ret;
}

int ObBasicTabletMergeDag::prepare_merge_ctx()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(alloc_merge_ctx())) {
    LOG_WARN("failed to alloc memory for merge ctx", K(ret), KPC(this));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ctx_->ls_handle_, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id_));
  } else {
    int tmp_ret = OB_SUCCESS;
    if (typeid(*this) != typeid(ObTxTableMergeDag)
        && OB_TMP_FAIL(ctx_->init_merge_progress(param_.is_tenant_major_merge_))) {
      LOG_WARN("failed to init merge progress", K(tmp_ret), K_(param));
    }
  }
  return ret;
}
int ObBasicTabletMergeDag::get_tablet_and_compat_mode()
{
  int ret = OB_SUCCESS;
  // can't get tablet_handle now! because this func is called in create dag,
  // the last compaction dag is not finished yet, tablet is in old version
  ObLSHandle tmp_ls_handle;
  ObTabletHandle tmp_tablet_handle;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, tmp_ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id_));
  } else if (OB_FAIL(tmp_ls_handle.get_ls()->get_tablet_svr()->get_tablet(
          tablet_id_,
          tmp_tablet_handle,
          0/*timeout*/,
          ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    LOG_WARN("failed to get tablet", K(ret), K(ls_id_), K(tablet_id_));
  } else if (OB_FAIL(ObTabletMergeChecker::check_need_merge(param_.merge_type_, *tmp_tablet_handle.get_obj()))) {
    if (OB_NO_NEED_MERGE != ret) {
      LOG_WARN("failed to check need merge", K(ret));
    }
  } else if (OB_FAIL(tmp_tablet_handle.get_obj()->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("failed to fetch table store", K(ret), K(ls_id_), K(tablet_id_));
  } else {
    compat_mode_ = tmp_tablet_handle.get_obj()->get_tablet_meta().compat_mode_;
  }

  if (OB_SUCC(ret) && is_mini_merge(merge_type_)) {
    bool is_exist = false;
    if (OB_FAIL(MTL(ObTenantDagScheduler *)->check_dag_exist(this, is_exist))) {
      LOG_WARN("failed to check dag exist", K(ret), K_(param));
    } else {
      const int64_t inc_sstable_cnt = table_store_wrapper.get_member()->get_minor_sstables().count() + (is_exist ? 1 : 0);
      if (ObPartitionMergePolicy::is_sstable_count_not_safe(inc_sstable_cnt)) {
        ret = OB_TOO_MANY_SSTABLE;
        LOG_WARN("Too many sstables in tablet, cannot schdule mini compaction, retry later",
            K(ret), K_(ls_id), K_(tablet_id), K(tmp_tablet_handle.get_obj()),
            "inc_sstable_cnt", table_store_wrapper.get_member()->get_minor_sstables().count());
        ObPartitionMergePolicy::diagnose_table_count_unsafe(MINI_MERGE, *tmp_tablet_handle.get_obj());
      }
    }
  }
  return ret;
}

int64_t ObBasicTabletMergeDag::to_string(char* buf, const int64_t buf_len) const
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
      databuff_print_json_kv(buf, buf_len, pos, "sstable_version_range", ctx_->sstable_version_range_);
      databuff_print_json_kv_comma(buf, buf_len, pos, "scn_range", ctx_->scn_range_);
      databuff_printf(buf, buf_len, pos, "}");
    }
    databuff_printf(buf, buf_len, pos, "}");
  }

  return pos;
}

int ObBasicTabletMergeDag::inner_init(const ObTabletMergeDagParam &param)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret), K(param));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(param));
  } else {
    param_ = param;
    merge_type_ = param.merge_type_;
    ls_id_ = param.ls_id_;
    tablet_id_ = param.tablet_id_;
    if (param.for_diagnose_) {
    } else if (OB_FAIL(get_tablet_and_compat_mode())) {
      LOG_WARN("failed to get tablet and compat mode", K(ret));
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

bool ObBasicTabletMergeDag::operator == (const ObIDag &other) const
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

int64_t ObBasicTabletMergeDag::hash() const
{
  return inner_hash();
}

int ObBasicTabletMergeDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
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

int ObBasicTabletMergeDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, "ls_id=%ld tablet_id=%ld", ls_id_.id(), tablet_id_.id()))) {
    LOG_WARN("failed to fill dag key", K(ret), K(ctx_));
  }
  return ret;
}

/*
 *  ----------------------------------------------ObTabletMergeDag--------------------------------------------------
 */

ObTabletMergeDag::ObTabletMergeDag(const ObDagType::ObDagTypeEnum type)
  : ObBasicTabletMergeDag(type)
{
}

int ObTabletMergeDag::gene_compaction_info(compaction::ObTabletCompactionProgress &input_progress)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_NOT_NULL(ctx_) && ObIDag::DAG_STATUS_NODE_RUNNING == get_dag_status()) {
    input_progress.tenant_id_ = MTL_ID();
    input_progress.merge_type_ = merge_type_;
    input_progress.merge_version_ = ctx_->param_.merge_version_;
    input_progress.status_ = get_dag_status();
    input_progress.ls_id_ = ctx_->param_.ls_id_.id();
    input_progress.tablet_id_ = ctx_->param_.tablet_id_.id();
    input_progress.dag_id_ = get_dag_id();
    input_progress.create_time_ = add_time_;
    input_progress.start_time_ = start_time_;
    input_progress.progressive_merge_round_ = ctx_->progressive_merge_round_;
    input_progress.estimated_finish_time_ = ObTimeUtility::fast_current_time() + ObCompactionProgress::EXTRA_TIME;

    int64_t tmp_ret = OB_SUCCESS;
    if (OB_NOT_NULL(ctx_->merge_progress_)
        && OB_UNLIKELY(OB_SUCCESS != (tmp_ret = ctx_->merge_progress_->get_progress_info(input_progress)))) {
      LOG_WARN("failed to get progress info", K(tmp_ret), K(ctx_));
    } else {
      LOG_INFO("success to get progress info", K(tmp_ret), K(ctx_), K(input_progress));
    }

    if (DAG_STATUS_FINISH == input_progress.status_) { // fix merge_progress
      input_progress.unfinished_data_size_ = 0;
      input_progress.estimated_finish_time_ = ObTimeUtility::fast_current_time();
    }
  } else {
    ret = OB_EAGAIN;
  }
  return ret;
}

int ObTabletMergeDag::diagnose_compaction_info(compaction::ObDiagnoseTabletCompProgress &input_progress)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (DAG_STATUS_NODE_RUNNING == get_dag_status()) { // only diagnose running dag
    input_progress.tenant_id_ = MTL_ID();
    input_progress.merge_type_ = merge_type_;
    input_progress.status_ = get_dag_status();
    input_progress.dag_id_ = get_dag_id();
    input_progress.create_time_ = add_time_;
    input_progress.start_time_ = start_time_;

    if (OB_NOT_NULL(ctx_)) { // ctx_ is not created yet
      input_progress.snapshot_version_ = ctx_->sstable_version_range_.snapshot_version_;
      input_progress.base_version_ = ctx_->sstable_version_range_.base_version_;

      if (OB_NOT_NULL(ctx_->merge_progress_)) {
        int64_t tmp_ret = OB_SUCCESS;
        if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = ctx_->merge_progress_->get_progress_info(input_progress)))) {
          LOG_WARN("failed to get progress info", K(tmp_ret), K(ctx_));
        } else if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = ctx_->merge_progress_->diagnose_progress(input_progress)))) {
          LOG_INFO("success to diagnose progress", K(tmp_ret), K(ctx_), K(input_progress));
        }
      }
    }
  }
  return ret;
}

/*
 *  ----------------------------------------------ObTabletMajorMergeDag--------------------------------------------------
 */

ObTabletMajorMergeDag::ObTabletMajorMergeDag()
  : ObTabletMergeDag(ObDagType::DAG_TYPE_MAJOR_MERGE)
{
}

ObTabletMajorMergeDag::~ObTabletMajorMergeDag()
{
  // TODO dead lock, fix later
  // if (0 == MTL(ObTenantDagScheduler*)->get_dag_count(ObDagType::DAG_TYPE_MAJOR_MERGE)) {
  //   MTL(ObTenantTabletScheduler *)->merge_all();
  // }
}

int ObTabletMajorMergeDag::init_by_param(const ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  const ObTabletMergeDagParam *merge_param = nullptr;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret), K(param));
  } else if (OB_ISNULL(param)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("input param is null", K(ret), K(param));
  } else if (FALSE_IT(merge_param = static_cast<const ObTabletMergeDagParam *>(param))) {
  } else if (OB_UNLIKELY(!is_major_merge_type(merge_param->merge_type_))) {
    ret = OB_ERR_SYS;
    LOG_ERROR("param is invalid or is major merge param not match", K(ret), K(param));
  } else if (OB_FAIL(ObBasicTabletMergeDag::inner_init(*merge_param))) {
    LOG_WARN("failed to init ObTabletMergeDag", K(ret));
  }
  return ret;
}

/*
 *  ----------------------------------------------ObTabletMiniMergeDag--------------------------------------------------
 */

ObTabletMiniMergeDag::ObTabletMiniMergeDag()
  : ObTabletMergeDag(ObDagType::DAG_TYPE_MINI_MERGE)
{
}

ObTabletMiniMergeDag::~ObTabletMiniMergeDag()
{
}

int ObTabletMiniMergeDag::init_by_param(const share::ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  const ObTabletMergeDagParam *merge_param = nullptr;
  if (OB_ISNULL(param)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("input param is null", K(ret), K(param));
  } else if (FALSE_IT(merge_param = static_cast<const ObTabletMergeDagParam *>(param))) {
  } else if (OB_UNLIKELY(!is_mini_merge(merge_param->merge_type_))) {
    ret = OB_ERR_SYS;
    LOG_ERROR("is mini merge param not match", K(ret), K(param));
  } else if (OB_FAIL(ObBasicTabletMergeDag::inner_init(*merge_param))) {
    LOG_WARN("failed to init ObTabletMergeDag", K(ret));
  }
  return ret;
}

/*
 *  ----------------------------------------------ObTabletMergeExecuteDag--------------------------------------------------
 */

ObTabletMergeExecuteDag::ObTabletMergeExecuteDag()
  : ObTabletMergeDag(ObDagType::DAG_TYPE_MERGE_EXECUTE),
    merge_scn_range_()
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
  } else if (OB_FAIL(ObTabletMergeDag::inner_init(*merge_param))) {
    LOG_WARN("failed to init ObTabletMergeDag", K(ret));
  }

  return ret;
}

int ObTabletMergeExecuteDag::direct_init_ctx(
    const ObTabletMergeDagParam &param,
    const lib::Worker::CompatMode compat_mode,
    const ObGetMergeTablesResult &result,
    ObLSHandle &ls_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY((!is_minor_merge_type(result.suggest_merge_type_)
      && !is_meta_major_merge(result.suggest_merge_type_)) || !result.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("result is invalid", K(ret), K(result));
  } else {
    param_ = param;
    merge_type_ = param.merge_type_;
    ls_id_ = param.ls_id_;
    tablet_id_ = param.tablet_id_;
    compat_mode_ = compat_mode;
    merge_scn_range_ = result.scn_range_;
    if (OB_FAIL(alloc_merge_ctx())) {
      LOG_WARN("failed to alloc merge ctx", K(ret));
    } else if (FALSE_IT(ctx_->ls_handle_ = ls_handle)) { // assign ls_handle
    } else if (FALSE_IT(ctx_->rebuild_seq_ = ls_handle.get_ls()->get_rebuild_seq())) {
    } else if (OB_FAIL(create_first_task(result, param.need_swap_tablet_flag_))) {
      LOG_WARN("failed to create first task", K(ret), K(result));
    } else {
      is_inited_ = true;
    }
  }

  return ret;
}
template<class T>
int ObTabletMergeExecuteDag::create_first_task(
    const ObGetMergeTablesResult &result,
    const bool need_swap_tablet_flag)
{
  int ret = OB_SUCCESS;
  T *task = nullptr;
  if (OB_FAIL(alloc_task(task))) {
    STORAGE_LOG(WARN, "fail to alloc task", K(ret));
  } else if (OB_FAIL(task->init(result, *ctx_, need_swap_tablet_flag))) {
    STORAGE_LOG(WARN, "failed to init prepare_task", K(ret));
  } else if (OB_FAIL(add_task(*task))) {
    STORAGE_LOG(WARN, "fail to add task", K(ret), K_(ls_id), K_(tablet_id), K_(ctx));
  }
  return ret;
}

int ObTabletMergeExecuteDag::create_first_task(
  const ObGetMergeTablesResult &result,
  const bool need_swap_tablet_flag)
{
  return create_first_task<ObTabletMergeExecutePrepareTask>(result, need_swap_tablet_flag);
}

ObTabletMergeExecutePrepareTask::ObTabletMergeExecutePrepareTask()
  : ObITask(ObITask::TASK_TYPE_SSTABLE_MERGE_PREPARE),
    is_inited_(false),
    need_swap_tablet_flag_(false),
    ctx_(nullptr),
    table_key_array_()
{}

ObTabletMergeExecutePrepareTask::~ObTabletMergeExecutePrepareTask()
{}

int ObTabletMergeExecutePrepareTask::init(
    const ObGetMergeTablesResult &result,
    ObTabletMergeCtx &ctx,
    const bool need_swap_tablet_flag)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(result_.assign(result))) {
    LOG_WARN("failed to assgin result", K(ret), K(result));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < result.handle_.get_count(); ++i) {
      if (OB_FAIL(table_key_array_.push_back(result.handle_.get_table(i)->get_key()))) {
        LOG_WARN("failed to push back key", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ctx_ = &ctx;
    result_.handle_.reset(); // clear tables handle, get sstable when execute after get tablet_handle
    need_swap_tablet_flag_ = need_swap_tablet_flag;
    is_inited_ = true;
  }
  return ret;
}

int ObTabletMergeExecutePrepareTask::process()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("task is not init", K(ret));
  } else if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is unexpected null", K(ret), K(ctx_));
  } else if (FALSE_IT(ctx_->start_time_ = ObTimeUtility::fast_current_time())) {
  } else if (OB_FAIL(get_tablet_and_result())) {
    LOG_WARN("failed to get tablet and result", K(ret));
  } else if (OB_FAIL(ctx_->get_schema_and_gene_from_result(result_))) {
    LOG_WARN("failed to get schema and generage from result", K(ret), K_(result));
  } else if (OB_FAIL(ctx_->init_merge_info())) {
    LOG_WARN("fail to init merge info", K(ret), K_(result), KPC(ctx_));
  } else if (OB_FAIL(ctx_->prepare_index_tree())) {
    LOG_WARN("fail to prepare sstable index tree", K(ret), KPC(ctx_));
  } else if (OB_FAIL(ObBasicTabletMergeDag::generate_merge_task(
      *static_cast<ObTabletMergeExecuteDag *>(get_dag()), *ctx_, this))) {
    LOG_WARN("Failed to generate_merge_sstable_task", K(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    if (ctx_->param_.tablet_id_.is_special_merge_tablet()) {
      // init compaction filter for minor merge in TxDataTable
      if (OB_FAIL(prepare_compaction_filter())) {
        LOG_WARN("failed to prepare compaction filter", K(ret), K(ctx_->param_));
      }
    } else if (OB_TMP_FAIL(ctx_->init_merge_progress(ctx_->param_.is_tenant_major_merge_))) {
      LOG_WARN("failed to init merge progress", K(tmp_ret), K_(result));
    } else if (OB_TMP_FAIL(ctx_->prepare_merge_progress())) {
      LOG_WARN("failed to init merge progress", K(tmp_ret));
    }
    FLOG_INFO("succeed to init merge ctx", K(ret), KPC(ctx_), K(result_));
  }
  return ret;
}

int ObTabletMergeExecutePrepareTask::get_tablet_and_result()
{
  int ret = OB_SUCCESS;
  if (need_swap_tablet_flag_) {
    const ObTabletMapKey key(ctx_->param_.ls_id_, ctx_->param_.tablet_id_);
    if (OB_FAIL(MTL(ObTenantMetaMemMgr *)->get_tablet_with_allocator(
                        WashTabletPriority::WTP_LOW, key, ctx_->allocator_,
                        ctx_->tablet_handle_, true /*force_alloc_new*/))) {
      LOG_WARN("failed to get alloc tablet handle", K(ret), K(key));
    } else if (OB_FAIL(ctx_->tablet_handle_.get_obj()->clear_memtables_on_table_store())) {
      LOG_WARN("failed to clear memtables on table_store", K(ret), K(ctx_->tablet_handle_));
    }
  } else if (OB_FAIL(ctx_->ls_handle_.get_ls()->get_tablet(
            ctx_->param_.tablet_id_,
            ctx_->tablet_handle_,
            0/*timeout*/,
            storage::ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    LOG_WARN("failed to get tablet", K(ret), K(ctx_->param_));
  } else if (OB_FAIL(ObTabletMergeChecker::check_need_merge(ctx_->param_.merge_type_, *ctx_->tablet_handle_.get_obj()))) {
    if (OB_NO_NEED_MERGE != ret) {
      LOG_WARN("failed to check need merge", K(ret));
    }
  }
  if (FAILEDx(get_result_by_table_key())) {
    if (OB_NO_NEED_MERGE != ret) {
      LOG_WARN("failed to get result by table key", K(ret));
    }
  }
  return ret;
}

int ObTabletMergeExecutePrepareTask::get_result_by_table_key()
{
  int ret = OB_SUCCESS;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  if (OB_FAIL(ctx_->tablet_handle_.get_obj()->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_UNLIKELY(!table_store_wrapper.get_member()->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet store is invalid", K(ret), KPC(table_store_wrapper.get_member()));
  } else {
    ObITable *table = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < table_key_array_.count(); ++i) {
      const ObITable::TableKey &table_key = table_key_array_.at(i);
      if (OB_FAIL(table_store_wrapper.get_member()->get_table(table_key, table))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_NO_NEED_MERGE;
        } else {
          LOG_WARN("failed to get table from new table_store", K(ret));
        }
      } else if (OB_FAIL(result_.handle_.add_sstable(table, table_store_wrapper.get_meta_handle()))) {
        LOG_WARN("failed to add sstable into result", K(ret), KPC(table));
      }
    } // end of for
    if (OB_SUCC(ret) && result_.handle_.get_count() != table_key_array_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected tables from current tablet", K(ret), K(table_key_array_), K(ctx_->tables_handle_));
    }
  }
  return ret;
}

int ObTxTableMergeExecutePrepareTask::prepare_compaction_filter()
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is unexpected null", K(ret), K(ctx_));
  } else if (OB_UNLIKELY(!ctx_->param_.tablet_id_.is_ls_tx_data_tablet())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only tx data tablet can execute minor merge", K(ret), K(ctx_->param_));
  } else if (OB_ISNULL(buf = ctx_->allocator_.alloc(sizeof(ObTransStatusFilter)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(sizeof(ObTransStatusFilter)));
  } else {
    ObTransStatusFilter *compaction_filter = new(buf) ObTransStatusFilter();
    ObTxTableGuard guard;
    share::SCN recycle_scn = share::SCN::min_scn();
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(ctx_->ls_handle_.get_ls()->get_tx_table_guard(guard))) {
      LOG_WARN("failed to get tx table", K(tmp_ret), K_(ctx_->param));
    } else if (OB_UNLIKELY(!guard.is_valid())) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tx table guard is invalid", K(tmp_ret), K_(ctx_->param), K(guard));
    } else if (OB_TMP_FAIL(guard.get_recycle_scn(recycle_scn))) {
      LOG_WARN("failed to get recycle ts", K(tmp_ret), K_(ctx_->param));
    } else if (OB_TMP_FAIL(compaction_filter->init(recycle_scn, ObTxTable::get_filter_col_idx()))) {
      LOG_WARN("failed to get init compaction filter", K(tmp_ret), K_(ctx_->param), K(recycle_scn));
    } else {
      ctx_->compaction_filter_ = compaction_filter;
      FLOG_INFO("success to init compaction filter", K(tmp_ret), K(recycle_scn));
    }

    if (OB_SUCC(ret)) {
      ctx_->progressive_merge_num_ = 0;
      ctx_->is_full_merge_ = true;
      ctx_->merge_level_ = MACRO_BLOCK_MERGE_LEVEL;
      ctx_->read_base_version_ = 0;
    } else if (OB_NOT_NULL(buf)) {
      ctx_->allocator_.free(buf);
      buf = nullptr;
    }
  }
  return ret;
}

int ObTxTableMinorExecuteDag::create_first_task(const ObGetMergeTablesResult &result, const bool need_swap_tablet_flag)
{
  return ObTabletMergeExecuteDag::create_first_task<ObTxTableMergeExecutePrepareTask>(result, need_swap_tablet_flag);
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
        || merge_type_ != other_merge_dag.merge_type_ // different merge type
        || (is_minor_merge(merge_type_)  // different merge range for minor
            && merge_scn_range_ != other_merge_dag.merge_scn_range_)) {
      is_same = false;
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
  } else if (!is_merge_dag(dag_->get_type())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("dag type not match", K(ret), KPC(dag_));
  } else {
    merge_dag_ = static_cast<ObBasicTabletMergeDag *>(dag_);
    if (OB_UNLIKELY(!merge_dag_->get_param().is_valid())) {
      ret = OB_ERR_SYS;
      LOG_WARN("param_ is not valid", K(ret), K(merge_dag_->get_param()));
    } else {
      is_inited_ = true;
    }
  }

  return ret;
}

int ObTabletMergePrepareTask::process()
{
  int ret = OB_SUCCESS;
  ObTenantStatEstGuard stat_est_guard(MTL_ID());
  ObTabletMergeCtx *ctx = NULL;
  ObTaskController::get().switch_task(share::ObTaskType::DATA_MAINTAIN);
  bool skip_rest_operation = false;

  DEBUG_SYNC(MERGE_PARTITION_TASK);
  DEBUG_SYNC(AFTER_EMPTY_SHELL_TABLET_CREATE);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(merge_dag_->prepare_merge_ctx())) {
    LOG_WARN("failed to alloc merge ctx", K(ret));
  } else if (OB_ISNULL(ctx = merge_dag_->get_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is unexpected null", K(ret), KP(ctx), KPC(merge_dag_));
  } else if (FALSE_IT(ctx->start_time_ = ObTimeUtility::fast_current_time())) {
  } else if (OB_UNLIKELY(is_major_merge_type(ctx->param_.merge_type_)
                         && !MTL(ObTenantTabletScheduler *)->could_major_merge_start())) {
    ret = OB_CANCELED;
    LOG_INFO("Merge has been paused", K(ret), K(ctx));
  } else if (ctx->ls_handle_.get_ls()->is_offline()) {
    ret = OB_CANCELED;
    LOG_INFO("ls offline, skip merge", K(ret), K(ctx));
  } else if (FALSE_IT(ctx->time_guard_.click(ObCompactionTimeGuard::DAG_WAIT_TO_SCHEDULE))) {
  } else if (OB_FAIL(check_before_init())) {
    if (OB_CANCELED != ret) {
      LOG_WARN("failed to check before init", K(ret), K(ctx->param_));
    }
  } else if (OB_FAIL(ctx->ls_handle_.get_ls()->get_tablet(
            ctx->param_.tablet_id_,
            ctx->tablet_handle_,
            0/*timeout*/,
            storage::ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    LOG_WARN("failed to get tablet", K(ret), K(ctx->param_));
  } else if (OB_FAIL(ObTabletMergeChecker::check_need_merge(ctx->param_.merge_type_, *ctx->tablet_handle_.get_obj()))) {
    if (OB_NO_NEED_MERGE != ret) {
      LOG_WARN("failed to check need merge", K(ret));
    }
  } else if (FALSE_IT(ctx->rebuild_seq_ = ctx->ls_handle_.get_ls()->get_rebuild_seq())) {
  } else if (OB_FAIL(build_merge_ctx(skip_rest_operation))) {
    if (OB_NO_NEED_MERGE != ret) {
      LOG_WARN("failed to build merge ctx", K(ret), K(ctx->param_));
    }
  }

  if (OB_FAIL(ret) || skip_rest_operation) {
  } else if (is_major_merge_type(ctx->param_.merge_type_)
    && OB_FAIL(ctx->try_swap_tablet_handle())) {
    LOG_WARN("failed to try swap tablet handle", K(ret));
  } else if (OB_FAIL(ObBasicTabletMergeDag::generate_merge_task(
      *merge_dag_, *ctx, this))) {
    LOG_WARN("Failed to generate_merge_sstable_task", K(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(ctx->prepare_merge_progress())) {
      LOG_WARN("failed to init merge progress", K(tmp_ret));
    }
    FLOG_INFO("succeed to init merge ctx", "task", *this);
  }
  if (OB_FAIL(ret)) {
    FLOG_WARN("sstable merge finish", K(ret), K(ctx), "task", *(static_cast<ObITask *>(this)));
  }

  return ret;
}

int ObBasicTabletMergeDag::generate_merge_task(
    ObBasicTabletMergeDag &merge_dag,
    ObTabletMergeCtx &ctx,
    ObITask *prepare_task)
{
  int ret = OB_SUCCESS;
  ObTabletMergeTask *merge_task = NULL;
  ObTabletMergeFinishTask *finish_task = NULL;

  // add macro merge task
  if (OB_FAIL(merge_dag.alloc_task(merge_task))) {
    LOG_WARN("fail to alloc task", K(ret));
  } else if (OB_ISNULL(merge_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("Unexpecte null macro merge task", K(ret), K(ctx));
  } else if (OB_FAIL(merge_task->init(0/*task_idx*/, ctx))) {
    LOG_WARN("fail to init macro merge task", K(ret), K(ctx));
  } else if (OB_NOT_NULL(prepare_task) && OB_FAIL(prepare_task->add_child(*merge_task))) {
    LOG_WARN("fail to add child", K(ret), K(ctx));
  } else if (OB_FAIL(merge_dag.add_task(*merge_task))) {
    LOG_WARN("fail to add task", K(ret), K(ctx));
  }

  // add finish task
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(merge_dag.alloc_task(finish_task))) {
    LOG_WARN("fail to alloc task", K(ret), K(ctx));
  } else if (OB_FAIL(finish_task->init())) {
    LOG_WARN("fail to init main table finish task", K(ret), K(ctx));
  } else if (OB_FAIL(merge_task->add_child(*finish_task))) {
    LOG_WARN("fail to add child", K(ret), K(ctx));
  } else if (OB_FAIL(merge_dag.add_task(*finish_task))) {
    LOG_WARN("fail to add task", K(ret), K(ctx));
  }
  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(merge_task)) {
      merge_dag.remove_task(*merge_task);
      merge_task = nullptr;
    }
    if (OB_NOT_NULL(finish_task)) {
      merge_dag.remove_task(*finish_task);
      finish_task = nullptr;
    }
  }
  return ret;
}

int ObTabletMergePrepareTask::build_merge_ctx(bool &skip_rest_operation)
{
  int ret = OB_SUCCESS;
  skip_rest_operation = false;
  ObTabletMergeCtx *ctx = nullptr;

  // only ctx.param_ is inited, fill other fields here
  if (OB_ISNULL(ctx = merge_dag_->get_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is unexpected null", K(ret), KP(ctx), KPC(merge_dag_));
  } else if (OB_UNLIKELY(!ctx->param_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ctx));
  } else if (OB_FAIL(inner_init_ctx(*ctx, skip_rest_operation))) {
    LOG_WARN("fail to inner init ctx", K(ret), "tablet_id", ctx->param_.tablet_id_, KPC(ctx));
  }

  if (OB_FAIL(ret) || skip_rest_operation) {
  } else if (FALSE_IT(ctx->merge_scn_ = ctx->scn_range_.end_scn_)) {
  } else if (OB_FAIL(ctx->init_merge_info())) {
    LOG_WARN("fail to init merge info", K(ret), "tablet_id", ctx->param_.tablet_id_, KPC(ctx));
  } else if (OB_FAIL(ctx->prepare_index_tree())) {
    LOG_WARN("fail to prepare sstable index tree", K(ret), K(ctx));
  }
  if (OB_SUCC(ret)) {
    FLOG_INFO("succeed to build merge ctx", "tablet_id", ctx->param_.tablet_id_, KPC(ctx), K(skip_rest_operation));
  }

  return ret;
}

int ObTabletMajorPrepareTask::check_before_init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!MTL(ObTenantTabletScheduler *)->could_major_merge_start())) {
    ret = OB_CANCELED;
    LOG_INFO("Merge has been paused", K(ret), KPC(merge_dag_));
  }
  return ret;
}

int ObTabletMajorPrepareTask::inner_init_ctx(ObTabletMergeCtx &ctx, bool &skip_merge_task_flag)
{
  int ret = OB_SUCCESS;
  skip_merge_task_flag = false;
  if (OB_FAIL(ctx.inner_init_for_medium())) {
    LOG_WARN("failed to inner init for major", K(ret));
  }
  return ret;
}

int ObTabletMiniPrepareTask::inner_init_ctx(ObTabletMergeCtx &ctx, bool &skip_merge_task_flag)
{
  int ret = OB_SUCCESS;
  skip_merge_task_flag = false;
  if (OB_FAIL(ctx.inner_init_for_mini(skip_merge_task_flag))) {
    LOG_WARN("failed to inner init for mini", K(ret));
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
  } else if (!is_merge_dag(dag_->get_type())) {
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

int ObTabletMergeFinishTask::create_sstable_after_merge()
{
  int ret = OB_SUCCESS;
  ObTabletMergeCtx *ctx = merge_dag_->get_ctx();
  if (ctx->merged_sstable_.is_valid()) {
    if (OB_UNLIKELY(!is_major_merge_type(ctx->param_.merge_type_))) {
      ret = OB_ERR_SYS;
      LOG_ERROR("Unxpected valid merged table handle with other merge", K(ret), KPC(ctx));
    }
  } else if (OB_FAIL(get_merged_sstable(*ctx))) {
    LOG_WARN("failed to finish_merge_sstable", K(ret));
  }
  return ret;
}

int ObTabletMergeFinishTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTaskController::get().switch_task(share::ObTaskType::DATA_MAINTAIN);
  ObTabletMergeCtx *ctx = nullptr;
  DEBUG_SYNC(MERGE_PARTITION_FINISH_TASK);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited yet", K(ret));
  } else if (OB_ISNULL(ctx = merge_dag_->get_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is unexpected null", K(ret), KP(ctx), KPC(merge_dag_));
  } else {
    ObLSID &ls_id = ctx->param_.ls_id_;
    ObTabletID &tablet_id = ctx->param_.tablet_id_;

    ctx->time_guard_.click(ObCompactionTimeGuard::EXECUTE);
    if (OB_FAIL(create_sstable_after_merge())) {
      LOG_WARN("failed to create sstable after merge", K(ret), K(tablet_id));
    } else if (FALSE_IT(ctx->time_guard_.click(ObCompactionTimeGuard::CREATE_SSTABLE))) {
    } else if (OB_FAIL(add_sstable_for_merge(*ctx))) {
      LOG_WARN("failed to add sstable for merge", K(ret));
    }
    if (OB_SUCC(ret) && is_major_merge_type(ctx->param_.merge_type_) && NULL != ctx->param_.report_) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(ctx->param_.report_->submit_tablet_update_task(MTL_ID(), ctx->param_.ls_id_, tablet_id))) {
        LOG_WARN("failed to submit tablet update task to report", K(tmp_ret), K(MTL_ID()), K(ctx->param_.ls_id_), K(tablet_id));
      } else if (OB_TMP_FAIL(ctx->ls_handle_.get_ls()->get_tablet_svr()->update_tablet_report_status(tablet_id))) {
        LOG_WARN("failed to update tablet report status", K(tmp_ret), K(tablet_id));
      }
    }

    if (OB_SUCC(ret) && OB_NOT_NULL(ctx->merge_progress_)) {
      if (OB_TMP_FAIL(ctx->merge_progress_->update_merge_info(ctx->merge_info_.get_sstable_merge_info()))) {
        STORAGE_LOG(WARN, "fail to update update merge info", K(tmp_ret));
      }

      if (OB_TMP_FAIL(compaction::ObCompactionSuggestionMgr::get_instance().analyze_merge_info(
              ctx->merge_info_,
              *ctx->merge_progress_))) {
        STORAGE_LOG(WARN, "fail to analyze merge info", K(tmp_ret));
      }
      ObSSTableMetaHandle sst_meta_hdl;
      if (OB_TMP_FAIL(ctx->merged_sstable_.get_meta(sst_meta_hdl))) {
        STORAGE_LOG(WARN, "fail to get sstable meta handle", K(tmp_ret));
      } else if (OB_TMP_FAIL(ctx->merge_progress_->finish_merge_progress(
          sst_meta_hdl.get_sstable_meta().get_total_macro_block_count()))) {
        STORAGE_LOG(WARN, "fail to update final merge progress", K(tmp_ret));
      }
    }
  }


  if (NULL != merge_dag_) {
    if (OB_FAIL(ret)) {
      FLOG_WARN("sstable merge finish", K(ret), KPC(ctx), "task", *(static_cast<ObITask *>(this)));
    } else {
      ctx->time_guard_.click(ObCompactionTimeGuard::DAG_FINISH);
      (void)ctx->collect_running_info();
      // ATTENTION! Critical diagnostic log, DO NOT CHANGE!!!
      FLOG_INFO("sstable merge finish", K(ret), "merge_info", ctx->get_merge_info(),
          K(ctx->merged_sstable_), "compat_mode", merge_dag_->get_compat_mode(), K(ctx->time_guard_));
    }
  }

  return ret;
}

int ObTabletMergeFinishTask::get_merged_sstable(ObTabletMergeCtx &ctx)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!ctx.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid argument to get merged sstable", K(ret), K(ctx));
  } else {
    LOG_INFO("create new merged sstable", K(ctx.param_.tablet_id_),
             "snapshot_version", ctx.sstable_version_range_.snapshot_version_,
             K(ctx.param_.merge_type_), K(ctx.create_snapshot_version_),
             "table_mode_flag", ctx.get_schema()->get_table_mode_flag());

    if (OB_FAIL(ctx.merge_info_.create_sstable(ctx))) {
      LOG_WARN("fail to create sstable", K(ret), K(ctx));
    }
  }
  return ret;
}

int ObTabletMergeFinishTask::add_sstable_for_merge(ObTabletMergeCtx &ctx)
{
  int ret = OB_SUCCESS;
  const ObMergeType merge_type = ctx.param_.merge_type_;

  if (OB_UNLIKELY(!ctx.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error of merge ctx", K(ctx));
  } else {
    SCN clog_checkpoint_scn = is_mini_merge(merge_type) ? ctx.merged_sstable_.get_end_scn() : SCN::min_scn();
    // means finish current major/medium compaction
    ObArenaAllocator allocator;
    ObUpdateTableStoreParam param(&(ctx.merged_sstable_),
                                  ctx.sstable_version_range_.snapshot_version_,
                                  ctx.sstable_version_range_.multi_version_start_,
                                  ctx.schema_ctx_.storage_schema_,
                                  ctx.rebuild_seq_,
                                  true/*need_check_transfer_seq*/,
                                  ctx.tablet_handle_.get_obj()->get_tablet_meta().transfer_info_.transfer_seq_,
                                  is_major_merge_type(merge_type)/*need_report*/,
                                  clog_checkpoint_scn,
                                  is_minor_merge(ctx.param_.merge_type_)/*need_check_sstable*/,
                                  false/*allow_duplicate_sstable*/,
                                  ctx.param_.get_merge_type());
    ObTabletHandle new_tablet_handle;
    if (ctx.param_.tablet_id_.is_special_merge_tablet()) {
      param.multi_version_start_ = 1;
    }

    if (OB_FAIL(ctx.ls_handle_.get_ls()->update_tablet_table_store(ctx.param_.tablet_id_, param, new_tablet_handle))) {
      LOG_WARN("failed to update tablet table store", K(ret), K(param));
    } else if (FALSE_IT(ctx.time_guard_.click(ObCompactionTimeGuard::UPDATE_TABLET))) {
    } else if (is_mini_merge(merge_type)) {
      if (OB_FAIL(new_tablet_handle.get_obj()->release_memtables(ctx.scn_range_.end_scn_))) {
        LOG_WARN("failed to release memtable", K(ret), "end_scn", ctx.scn_range_.end_scn_);
      } else {
        ctx.time_guard_.click(ObCompactionTimeGuard::RELEASE_MEMTABLE);
      }
    }

    // get info from inner table and save medium info
    // try schedule minor or major merge after mini
    if (OB_SUCC(ret) && is_mini_merge(merge_type) && new_tablet_handle.is_valid()) {
      int tmp_ret = OB_SUCCESS;
      if (!ctx.param_.tablet_id_.is_special_merge_tablet()) {
        if (OB_TMP_FAIL(try_schedule_compaction_after_mini(ctx, new_tablet_handle))) {
          LOG_WARN("failed to schedule compaction after mini", K(tmp_ret),
              "ls_id", ctx.param_.ls_id_, "tablet_id", ctx.param_.tablet_id_);
        }
      } else if (OB_TMP_FAIL(ObTenantTabletScheduler::schedule_tablet_minor_merge<ObTxTableMinorExecuteDag>(
              ctx.ls_handle_,
              new_tablet_handle))) {
        if (OB_SIZE_OVERFLOW != tmp_ret) {
          LOG_WARN("failed to schedule special tablet minor merge", K(tmp_ret),
              "ls_id", ctx.param_.ls_id_, "tablet_id", ctx.param_.tablet_id_);
        }
      }
      ctx.time_guard_.click(ObCompactionTimeGuard::SCHEDULE_OTHER_COMPACTION);
    }
  }
  return ret;
}

int ObTabletMergeFinishTask::try_report_tablet_stat_after_mini(ObTabletMergeCtx &ctx)
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = ctx.param_.ls_id_;
  const ObTabletID &tablet_id = ctx.param_.tablet_id_;
  const ObTransNodeDMLStat &tnode_stat = ctx.tnode_stat_;
  bool report_succ = false;

  if (tablet_id.is_special_merge_tablet()) {
    // no need report
  } else if (ObTabletStat::MERGE_REPORT_MIN_ROW_CNT >= tnode_stat.get_dml_count()) {
    // insufficient data, skip to report
  } else {
    // always report tablet stat whether _enable_adaptive_compaction is true or not for mini compaction
    ObTabletStat report_stat;
    report_stat.ls_id_ = ls_id.id();
    report_stat.tablet_id_ = tablet_id.id();
    report_stat.merge_cnt_ = 1;
    report_stat.insert_row_cnt_ = tnode_stat.insert_row_count_;
    report_stat.update_row_cnt_ = tnode_stat.update_row_count_;
    report_stat.delete_row_cnt_ = tnode_stat.delete_row_count_;
    if (OB_FAIL(MTL(ObTenantTabletStatMgr *)->report_stat(report_stat, report_succ))) {
      STORAGE_LOG(WARN, "failed to report tablet stat", K(ret));
    }
  }
  FLOG_INFO("try report tablet stat", K(ret), K(ls_id), K(tablet_id), K(tnode_stat), K(report_succ));
  return ret;
}

int ObTabletMergeFinishTask::try_schedule_compaction_after_mini(
    ObTabletMergeCtx &ctx,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObTabletID &tablet_id = ctx.param_.tablet_id_;
  ObLSID ls_id = ctx.param_.ls_id_;

  // report tablet stat
  if (0 == ctx.get_merge_info().get_sstable_merge_info().macro_block_count_) {
    // empty mini compaction, no need to reprot stat
  } else if (OB_TMP_FAIL(try_report_tablet_stat_after_mini(ctx))) {
    LOG_WARN("failed to report table stat after mini compaction", K(tmp_ret), K(ls_id), K(tablet_id));
  }
  if (OB_TMP_FAIL(ObMediumCompactionScheduleFunc::schedule_tablet_medium_merge(
          *ctx.ls_handle_.get_ls(),
          *tablet_handle.get_obj()))) {
    if (OB_SIZE_OVERFLOW != tmp_ret && OB_EAGAIN != tmp_ret) {
      LOG_WARN("failed to schedule tablet medium merge", K(tmp_ret));
    }
  }
  return ret;
}
/*
 *  ----------------------------------------------ObTabletMergeTask--------------------------------------------------
 */

ObTabletMergeTask::ObTabletMergeTask()
  : ObITask(ObITask::TASK_TYPE_MACROMERGE),
    allocator_("MergeTask", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
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
}

int ObTabletMergeTask::init(const int64_t idx, ObTabletMergeCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (idx < 0 || !ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is invalid", K(ret), K(idx), K(ctx));
  } else {
    void *buf = nullptr;
    if (is_major_merge_type(ctx.param_.merge_type_) || is_meta_major_merge(ctx.param_.merge_type_)) {
      if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObPartitionMajorMerger)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "failed to alloc memory for major merger", K(ret));
      } else {
        merger_ = new (buf) ObPartitionMajorMerger();
      }
    } else {
      if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObPartitionMinorMerger)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "failed to alloc memory for minor merger", K(ret));
      } else {
        merger_ = new (buf) ObPartitionMinorMerger();
      }
    }
    if (OB_SUCC(ret)) {
      idx_ = idx;
      ctx_ = &ctx;
      is_inited_ = true;
    }
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
  } else if (!is_merge_dag(dag_->get_type())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("dag type not match", K(ret), KPC(dag_));
  }  else {
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
    STORAGE_LOG(INFO, "ERRSIM EN_COMPACTION_MERGE_TASK");
    return ret;
  }
#endif

  DEBUG_SYNC(MERGE_TASK_PROCESS);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTabletMergeTask is not inited", K(ret));
  } else if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected null merge ctx", K(ret));
  } else if (OB_UNLIKELY(is_major_merge_type(ctx_->param_.merge_type_)
                         && !MTL(ObTenantTabletScheduler *)->could_major_merge_start())) {
    ret = OB_CANCELED;
    LOG_INFO("Merge has been paused", K(ret));
  } else if (OB_ISNULL(merger_)) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "Unexpected null partition merger", K(ret));
  } else {
    if (OB_FAIL(merger_->merge_partition(*ctx_, idx_))) {
      if (is_major_merge_type(ctx_->param_.merge_type_) && OB_ENCODING_EST_SIZE_OVERFLOW == ret) {
        STORAGE_LOG(WARN, "failed to merge partition with possibly encoding error, "
            "retry with flat row store type", K(ret), KPC(ctx_), K_(idx));
        merger_->reset();
        const bool force_flat_format = true;
        if (OB_FAIL(merger_->merge_partition(*ctx_, idx_, force_flat_format))) {
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
      FLOG_INFO("merge macro blocks ok", K(idx_), "task", *this);
    }
    merger_->reset();
  }

  if (OB_FAIL(ret)) {
    if (NULL != ctx_) {
      if (OB_CANCELED == ret) {
        STORAGE_LOG(INFO, "merge is canceled", K(ret), K(ctx_->param_), K(idx_));
      } else {
        STORAGE_LOG(WARN, "failed to merge", K(ret), K(ctx_->param_), K(idx_));
      }
    }
  }

  return ret;
}

} // namespace compaction
} // namespace oceanbase
