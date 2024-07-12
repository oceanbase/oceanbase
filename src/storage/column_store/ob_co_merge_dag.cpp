/**
 * Copyright (c) 2023 OceanBase
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
#include "storage/column_store/ob_co_merge_dag.h"
#include "storage/column_store/ob_column_oriented_merger.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/compaction/ob_tenant_compaction_progress.h"
#include "storage/compaction/ob_compaction_diagnose.h"
#include "storage/compaction/ob_tenant_medium_checker.h"
#include "storage/ob_storage_schema.h"
#include "storage/compaction/ob_sstable_merge_info_mgr.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "lib/utility/ob_tracepoint.h"
#include "storage/compaction/ob_compaction_dag_ranker.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/column_store/ob_co_merge_ctx.h"

namespace oceanbase
{
using namespace share;
using namespace storage;

namespace compaction
{
ObCOMergeDagParam::ObCOMergeDagParam()
  : ObTabletMergeDagParam(),
    start_cg_idx_(0),
    end_cg_idx_(0),
    compat_mode_(lib::Worker::CompatMode::INVALID),
    dag_net_id_()
{
}

ObCOMergeDagParam::~ObCOMergeDagParam()
{
}

bool ObCOMergeDagParam::is_valid() const
{
  return ObTabletMergeDagParam::is_valid() && (0 == end_cg_idx_ || start_cg_idx_ < end_cg_idx_)
      && lib::Worker::CompatMode::INVALID != compat_mode_;
}

/*
 * ObCOMergeDag
 * */
ObCOMergeDag::ObCOMergeDag(const share::ObDagType::ObDagTypeEnum type)
 : ObTabletMergeDag(type)
{
}

int ObCOMergeDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, "%s: ls_id=%ld tablet_id=%ld",
          OB_DAG_TYPES[get_type()].dag_type_str_,
          ls_id_.id(), tablet_id_.id()))) {
    LOG_WARN("failed to fill dag key", K(ret));
  }
  return ret;
}

int ObCOMergeDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("co merge dag do not init", K(ret));
  } else {
    if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                  ls_id_.id(),
                                  static_cast<int64_t>(tablet_id_.id()),
                                  param_.merge_version_))) {
      LOG_WARN("failed to fill info param", K(ret));
    }
  }
  return ret;
}

void ObCOMergeDag::set_dag_error_location()
{
  ObCOTabletMergeCtx *ctx = nullptr;
  ObCOMergeDagNet *dag_net = nullptr;
  if (OB_NOT_NULL(get_dag_net())) {
    dag_net = static_cast<ObCOMergeDagNet*>(get_dag_net());
    ctx = dag_net->get_merge_ctx();
    if (OB_NOT_NULL(ctx)) {
      error_location_.set(ctx->info_collector_.error_location_);
    }
  }
}

/*
 * ObCOMergePrepareDag
 * */
ObCOMergePrepareDag::ObCOMergePrepareDag()
 : ObCOMergeDag(ObDagType::DAG_TYPE_CO_MERGE_PREPARE)
{
}

ObCOMergePrepareDag::~ObCOMergePrepareDag()
{
}

int ObCOMergePrepareDag::init_by_param(const share::ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  const ObCOMergeDagParam *merge_param = nullptr;
  if (OB_ISNULL(param)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(param));
  } else if (FALSE_IT(merge_param = static_cast<const ObCOMergeDagParam*>(param))) {
  } else if (OB_UNLIKELY(!is_major_or_meta_merge_type(merge_param->merge_type_) || !merge_param->is_valid())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("Unexpected merge type to init major merge dag", K(ret), KPC(merge_param));
  } else if (OB_FAIL(ObTabletMergeDag::inner_init(merge_param))) {
    LOG_WARN("failed to init ObTabletMergeDag", K(ret));
  } else {
    compat_mode_ = merge_param->compat_mode_;
    dag_net_id_ = merge_param->dag_net_id_;
  }
  return ret;
}

int ObCOMergePrepareDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObCOMergeDagNet* dag_net = static_cast<ObCOMergeDagNet*>(get_dag_net());
  ObCOMergePrepareTask *task = nullptr;
  if (OB_FAIL(create_task(nullptr/*parent*/, task, dag_net))) {
    STORAGE_LOG(WARN, "fail to create prepare task", K(ret));
  }
  return ret;
}

/*
 * ObCOMergePrepareTask
 * */
ObCOMergePrepareTask::ObCOMergePrepareTask()
 : ObITask(ObITask::TASK_TYPE_SSTABLE_MERGE_PREPARE),
   is_inited_(false),
   dag_net_(nullptr)
{
}

ObCOMergePrepareTask::~ObCOMergePrepareTask()
{
}

int ObCOMergePrepareTask::init(ObCOMergeDagNet *dag_net)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("task is inited", K(ret), KP(dag_net));
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dag net is null", K(ret));
  } else {
    dag_net_ = dag_net;
    is_inited_ = true;
  }
  return ret;
}

int ObCOMergePrepareTask::create_schedule_dag(ObCOTabletMergeCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObGetMergeTablesResult result;
  bool schedule_minor = false;

  if (OB_FAIL(ctx.check_need_schedule_minor(schedule_minor))) {
    LOG_WARN("failed to check need chedule minor", K(ret), K(schedule_minor));
  } else if (schedule_minor) {
    ObTableHandleV2 tmp_table_handle;
    ObMinorExecuteRangeMgr minor_range_mgr;
    if (OB_FAIL(minor_range_mgr.get_merge_ranges(ctx.get_ls_id(), ctx.get_tablet_id()))) {
      LOG_WARN("failed to get merge range", K(ret), "param", ctx.get_dag_param());
    }
    for (int64_t i = 1; OB_SUCC(ret) && i < ctx.get_tables_handle().get_count(); ++i) {
      if (OB_FAIL(ctx.get_tables_handle().get_table(i, tmp_table_handle))) {
        LOG_WARN("failed to get table", K(ret), K(i));
      } else if (minor_range_mgr.in_execute_range(tmp_table_handle.get_table())) {
        // in execute range
        schedule_minor = false; // TODO(lixia.yq) need wait minor dag finish?
        LOG_INFO("table in execute range", K(ret), K(i), K(tmp_table_handle), K(minor_range_mgr.exe_range_array_));
        break;
      } else if (OB_FAIL(ObPartitionMergePolicy::add_table_with_check(result, tmp_table_handle))) {
        LOG_WARN("failed to add table", K(ret), K(i), K(tmp_table_handle));
      }
    } // end of for
  }

  if (OB_FAIL(ret)) {
  } else if (!schedule_minor) {
    /* No need to schedule minor merge, so creating ObCOMergeSCheduleDag is meanless
     * We can directly execute the ObCOMergeScheduleTask to accelerate the processing of co merge
     */
    if (OB_FAIL(dag_net_->create_co_execute_dags(*get_dag()))) {
      LOG_WARN("failed to create co execute dags in prepare task", K(ret), K(ctx)); // cancel dag net in process()
    }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(EventTable::EN_COMPACTION_CO_MERGE_PREPARE_FAILED) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      STORAGE_LOG(INFO, "ERRSIM EN_COMPACTION_CO_MERGE_PREPARE_FAILED", K(ret));
    }
  }
#endif
    dag_net_->update_merge_status(ObCOMergeDagNet::PREPARE_FINISHED);
  } else {
    /* We need to schedule minor merge first to reduce the count of inc sstables
     * Current Dag Path: prepare_dag -> minor_dag -> schedule_dag
     * Optimization: use ObMinorMergeTask instead of the MinorDags && ScheduleDags?
     */

    int tmp_ret = OB_SUCCESS;
    ObCOMergeScheduleDag *schedule_dag = nullptr;
    ObTabletMergeExecuteDag *minor_exe_dag = nullptr;
    ObTablet *tablet = ctx.get_tablet();
    result.version_range_.multi_version_start_ = ctx.get_tablet()->get_multi_version_start();
    result.version_range_.base_version_ = 0;
    result.version_range_.snapshot_version_ = ctx.get_tablet()->get_snapshot_version();
    ObTabletMergeDagParam dag_param(MINOR_MERGE, ctx.get_ls_id(), ctx.get_tablet_id(),
        ctx.get_transfer_seq());
    if (OB_FAIL(MTL(share::ObTenantDagScheduler *)->alloc_dag(minor_exe_dag))) {
      LOG_WARN("failed to alloc dag", K(ret));
    } else if (OB_FAIL(minor_exe_dag->prepare_init(
            dag_param,
            tablet->get_tablet_meta().compat_mode_,
            result,
            ctx.static_param_.ls_handle_))) {
      LOG_WARN("failed to init dag", K(ret), K(result));
    } else if (OB_FAIL(dag_net_->add_dag_into_dag_net(*minor_exe_dag))) {
      LOG_WARN("failed to add dag into dag net", K(ret), K(minor_exe_dag), KPC(dag_net_));
    } else if (OB_FAIL(dag_net_->create_dag<ObCOMergeScheduleDag>(0 /*start_cg_idx*/, 0 /*end_cg_idx*/, schedule_dag, minor_exe_dag/*parent*/))) {
      LOG_WARN("failed to create schedule dag", K(ret));
    } else if (OB_TMP_FAIL(MTL(share::ObTenantDagScheduler *)->add_dag(minor_exe_dag, true/*is_emergency*/))) {
      LOG_WARN("failed to add dag", K(tmp_ret), KPC(minor_exe_dag));
    } else {
      schedule_dag->set_swap_tablet_flag(true/*schedule_minor*/);
      LOG_INFO("success to add minor dag before schedule dag", K(ret), KP(minor_exe_dag), KP(schedule_dag));
    }

    if (OB_SUCCESS != tmp_ret && OB_NOT_NULL(minor_exe_dag)) {
      MTL(share::ObTenantDagScheduler *)->free_dag(*minor_exe_dag);
      minor_exe_dag = nullptr;
    }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(EventTable::EN_COMPACTION_CO_MERGE_PREPARE_MINOR_FAILED) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      STORAGE_LOG(INFO, "ERRSIM EN_COMPACTION_CO_MERGE_PREPARE_MINOR_FAILED", K(ret));
    }
  }
#endif
  }
  return ret;
}

int ObCOMergePrepareTask::process()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("task is not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(NULL == dag_net_ || NULL != dag_net_->get_merge_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected dag net", K(ret), KPC(dag_net_));
  } else if (OB_FAIL(dag_net_->prepare_co_merge_ctx())) {
    LOG_WARN("failed to prepare merge ctx", K(ret), KPC(dag_net_));
  } else if (OB_FAIL(create_schedule_dag(*dag_net_->get_merge_ctx()))) {
    LOG_WARN("failed to schedule minor dag", K(ret));
  }

  if (OB_FAIL(ret)) {
    dag_net_->cancel_dag_net(ret);
  }
  return ret;
}

/*
 * ObCOMergeScheduleDag
 * */
ObCOMergeScheduleDag::ObCOMergeScheduleDag()
 : ObCOMergeDag(ObDagType::DAG_TYPE_CO_MERGE_SCHEDULE),
   swap_tablet_flag_(false)
{
}

ObCOMergeScheduleDag::~ObCOMergeScheduleDag()
{
}

int ObCOMergeScheduleDag::init_by_param(const share::ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  const ObCOMergeDagParam *merge_param = nullptr;
  if (OB_ISNULL(param)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(param));
  } else if (FALSE_IT(merge_param = static_cast<const ObCOMergeDagParam*>(param))) {
  } else if (OB_UNLIKELY(!is_major_or_meta_merge_type(merge_param->merge_type_) || !merge_param->is_valid())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("Unexpected merge type to init major merge dag", K(ret), KPC(merge_param));
  } else if (OB_FAIL(ObTabletMergeDag::inner_init(merge_param))) {
    LOG_WARN("failed to init ObTabletMergeDag", K(ret));
  } else {
    compat_mode_ = merge_param->compat_mode_;
    dag_net_id_ = merge_param->dag_net_id_;
  }
  return ret;
}

int ObCOMergeScheduleDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObCOMergeScheduleTask *schedule_task = nullptr;
  if (OB_FAIL(alloc_task(schedule_task))) {
    STORAGE_LOG(WARN, "fail to alloc task", K(ret));
  } else if (OB_FAIL(schedule_task->init(static_cast<ObCOMergeDagNet*>(get_dag_net())))) {
    STORAGE_LOG(WARN, "failed to init prepare_task", K(ret));
  } else if (OB_FAIL(add_task(*schedule_task))) {
    STORAGE_LOG(WARN, "fail to add task", K(ret), K_(ls_id), K_(tablet_id), K_(ctx));
  }
  if (OB_FAIL(ret) && nullptr != schedule_task) {
    remove_task(*schedule_task);
    schedule_task = nullptr;
  }
  return ret;
}

/*
 * ObCOMergeScheduleTask
 * */
ObCOMergeScheduleTask::ObCOMergeScheduleTask()
 : ObITask(ObITask::TASK_TYPE_SSTABLE_MERGE_PREPARE),
   is_inited_(false),
   dag_net_(nullptr)
{
}

ObCOMergeScheduleTask::~ObCOMergeScheduleTask()
{
}

int ObCOMergeScheduleTask::init(ObCOMergeDagNet *dag_net)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("task is inited", K(ret), KP(dag_net));
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dag net is null", K(ret));
  } else {
    dag_net_ = dag_net;
    is_inited_ = true;
  }
  return ret;
}

int ObCOMergeScheduleTask::process()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("task is not inited", K(ret), K_(is_inited));
  } else if (OB_ISNULL(dag_net_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net is unexpected null", K(ret), KP_(dag_net));
  } else if (FALSE_IT(SET_MEM_CTX(dag_net_->get_merge_ctx()->mem_ctx_))) {
  } else if (static_cast<ObCOMergeScheduleDag *>(get_dag())->get_swap_tablet_flag()
      && OB_FAIL(dag_net_->swap_tablet_after_minor())) {
    LOG_WARN("failed to swap tablet after minor", K(ret));
  } else if (OB_FAIL(dag_net_->create_co_execute_dags(*get_dag()))) {
    LOG_WARN("failed to create execute dags in schedule task", K(ret));
  }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(EventTable::EN_COMPACTION_CO_MERGE_SCHEDULE_FAILED) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      STORAGE_LOG(INFO, "ERRSIM EN_COMPACTION_CO_MERGE_SCHEDULE_FAILED", K(ret));
    }
  }
#endif
  dag_net_->update_merge_status(ObCOMergeDagNet::PREPARE_FINISHED);
  if (OB_FAIL(ret)) {
    dag_net_->cancel_dag_net(ret);
  }
  return ret;
}

/*
 * ObCOMergeBatchExeDag
 */
ObCOMergeBatchExeDag::ObCOMergeBatchExeDag()
 : ObCOMergeDag(ObDagType::DAG_TYPE_CO_MERGE_BATCH_EXECUTE),
   exe_lock_(),
   start_cg_idx_(0),
   end_cg_idx_(0),
   retry_create_task_(false),
   progress_inited_(false),
   time_guard_(),
   merge_progress_(nullptr)
{
}

ObCOMergeBatchExeDag::~ObCOMergeBatchExeDag()
{
  if (OB_NOT_NULL(merge_progress_)) {
    merge_progress_->~ObPartitionMergeProgress();
    merge_progress_ = nullptr;
  }
}

bool ObCOMergeBatchExeDag::operator ==(const ObIDag &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObCOMergeBatchExeDag &other_merge_dag = static_cast<const ObCOMergeBatchExeDag&>(other);
    if (merge_type_ != other_merge_dag.merge_type_
      || ls_id_ != other_merge_dag.ls_id_
      || tablet_id_ != other_merge_dag.tablet_id_
      || start_cg_idx_ != other_merge_dag.start_cg_idx_
      || end_cg_idx_ != other_merge_dag.end_cg_idx_) {
      is_same = false;
    }
  }
  return is_same;
}

int ObCOMergeBatchExeDag::init_by_param(const share::ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  const ObCOMergeDagParam *merge_param = nullptr;
  if (OB_ISNULL(param)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(param));
  } else if (FALSE_IT(merge_param = static_cast<const ObCOMergeDagParam*>(param))) {
  } else if (OB_UNLIKELY(!is_major_or_meta_merge_type(merge_param->merge_type_) || !merge_param->is_valid())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("Unexpected merge type to init major merge dag", K(ret), KPC(merge_param));
  } else if (OB_FAIL(ObTabletMergeDag::inner_init(merge_param))) {
    LOG_WARN("failed to init ObTabletMergeDag", K(ret));
  } else {
    is_inited_ = true;
    compat_mode_ = merge_param->compat_mode_;
    start_cg_idx_ = merge_param->start_cg_idx_;
    end_cg_idx_ = merge_param->end_cg_idx_;
    dag_net_id_ = merge_param->dag_net_id_;
  }
  return ret;
}

int ObCOMergeBatchExeDag::create_first_task()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObCOMergeBatchExeTask *execute_task = nullptr;
  ObCOMergeBatchFinishTask *finish_task = nullptr;
  ObCOMergeDagNet *dag_net = static_cast<ObCOMergeDagNet*>(get_dag_net());
  ObCOTabletMergeCtx *ctx = nullptr;
  if (OB_UNLIKELY(nullptr == dag_net || nullptr == (ctx = dag_net->get_merge_ctx()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag_net or ctx is null", K(ret), KPC(dag_net));
  } else if (OB_UNLIKELY(!ctx->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("co merge ctx is not ready", K(ret), KPC(ctx));
  } else if (OB_FAIL(create_task(nullptr/*parent*/, execute_task, 0/*task_idx*/, *ctx, *dag_net))) {
    LOG_WARN("fail to create merge task", K(ret), KPC(dag_net));
  } else if (OB_ISNULL(execute_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("execute task is unexpected null", KR(ret), KP(execute_task));
  } else if (OB_FAIL(create_task(execute_task/*parent*/, finish_task, *ctx, *dag_net))) {
    LOG_WARN("fail to create finish task", K(ret), KPC(dag_net));
  } else { // fill compaction param
    // the dag_net has been set, and the dag hasn't been added to the scheduler now
    param_.compaction_param_.sstable_cnt_ = ctx->get_tables_handle().get_count();
    param_.compaction_param_.estimate_concurrent_cnt_ = ctx->get_concurrent_cnt();
    param_.compaction_param_.add_time_ = common::ObTimeUtility::fast_current_time();
    param_.compaction_param_.batch_size_ = end_cg_idx_ - start_cg_idx_;
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(execute_task)) {
      remove_task(*execute_task);
      execute_task = nullptr;
    }
    if (OB_NOT_NULL(finish_task)) {
      remove_task(*finish_task);
      finish_task = nullptr;
    }
  }
  return ret;
}

int ObCOMergeBatchExeDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, "ObCOMergeBatchExeDag: ls_id=%ld tablet_id=%ld start_cg_idx=%u end_cg_idx=%u",
          ls_id_.id(), tablet_id_.id(), start_cg_idx_, end_cg_idx_))) {
    LOG_WARN("failed to fill dag key", K(ret));
  }
  return ret;
}

int ObCOMergeBatchExeDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("co merge dag do not init", K(ret));
  } else {
    if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                  ls_id_.id(),
                                  static_cast<int64_t>(tablet_id_.id()),
                                  param_.merge_version_,
                                  static_cast<int64_t>(start_cg_idx_),
                                  static_cast<int64_t>(end_cg_idx_)))) {
      LOG_WARN("failed to fill info param", K(ret));
    }
  }
  return ret;
}

int ObCOMergeBatchExeDag::report_result()
{
  int ret = OB_SUCCESS;
  ObCOMergeDagNet *dag_net = static_cast<ObCOMergeDagNet*>(get_dag_net());
  if (OB_UNLIKELY(nullptr == dag_net || nullptr == dag_net->get_merge_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag_net or ctx is null", K(ret), KPC(dag_net));
  } else if (OB_FAIL(dag_net->dag_report_result(get_start_cg_idx(), get_end_cg_idx(), get_dag_ret()))) {
    LOG_WARN("failed to mark cg schedule status", K(ret), K(start_cg_idx_), K(end_cg_idx_),
        K(dag_ret_));
    ob_abort();
  }
  return ret;
}

int ObCOMergeBatchExeDag::gene_compaction_info(compaction::ObTabletCompactionProgress &input_progress)
{
  int ret = OB_SUCCESS;
  ObCOMergeDagNet *dag_net = static_cast<ObCOMergeDagNet*>(get_dag_net());
  ObCOTabletMergeCtx *ctx = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (nullptr == dag_net || (ctx = dag_net->get_merge_ctx()) == nullptr){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag_net or ctx is null", K(ret), KPC(dag_net));
  } else if (ObIDag::DAG_STATUS_NODE_RUNNING == get_dag_status()) {
    fill_compaction_progress(input_progress, *ctx, merge_progress_, start_cg_idx_, end_cg_idx_);
  } else {
    ret = OB_EAGAIN;
  }
  return ret;
}

int ObCOMergeBatchExeDag::diagnose_compaction_info(compaction::ObDiagnoseTabletCompProgress &input_progress)
{
  int ret = OB_SUCCESS;
  ObCOMergeDagNet *dag_net = static_cast<ObCOMergeDagNet*>(get_dag_net());
  ObCOTabletMergeCtx *ctx = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (nullptr == dag_net || (ctx = dag_net->get_merge_ctx()) == nullptr){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag_net or ctx is null", K(ret), KPC(dag_net));
  } else if (DAG_STATUS_NODE_RUNNING == get_dag_status()) { // only diagnose running dag
    fill_diagnose_compaction_progress(input_progress, ctx, merge_progress_, start_cg_idx_, end_cg_idx_);
  }
  return ret;
}

int ObCOMergeBatchExeDag::inner_reset_status_for_retry()
{
  int ret = OB_SUCCESS;
  ObCOMergeDagNet *dag_net = static_cast<ObCOMergeDagNet*>(get_dag_net());
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(dag_net->get_merge_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge ctx is null", K(ret), KPC(dag_net));
  } else if (FALSE_IT(retry_create_task_ = true)) {
  } else if (FALSE_IT(dag_net->get_merge_ctx()->destroy_merge_info_array(start_cg_idx_, end_cg_idx_, false/*release_mem_flag*/))) {
  } else if (OB_FAIL(create_first_task())) {
    LOG_WARN("failed to create first task", K(ret));
  } else {
    FLOG_INFO("co merge batch exe dag retry", K(ret), KPC(this));
  }
  return ret;
}

int ObCOMergeBatchExeDag::init_merge_progress()
{
  int ret = OB_SUCCESS;
  ObCOMergeDagNet *dag_net = static_cast<ObCOMergeDagNet*>(get_dag_net());
  ObCOTabletMergeCtx *ctx = nullptr;
  if (OB_UNLIKELY(nullptr == dag_net || nullptr == (ctx = dag_net->get_merge_ctx()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag_net or ctx is null", K(ret), KPC(dag_net));
  } else if (!progress_inited_
      && OB_ISNULL(merge_progress_)
      && OB_FAIL(ctx->prepare_merge_progress(merge_progress_, this, start_cg_idx_, end_cg_idx_))) {
    STORAGE_LOG(WARN, "fail to prepare merge_progress", K(ret));
  }
  progress_inited_ = true; // execute only once regardless of any failure
  return ret;
}

int ObCOMergeBatchExeDag::create_sstable_after_merge()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObCOMergeDagNet *dag_net = static_cast<ObCOMergeDagNet*>(get_dag_net());
  ObCOTabletMergeCtx *ctx = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(nullptr == dag_net || nullptr == (ctx = dag_net->get_merge_ctx()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx or dag_net is unexpected null", K(ret), KP_(ctx), KP(dag_net));
  } else if (FALSE_IT(dag_time_guard_click(ObStorageCompactionTimeGuard::EXECUTE))) {
  } else if (FALSE_IT(ctx->mem_ctx_.mem_click())) {
  } else if (OB_FAIL(ctx->create_sstables(start_cg_idx_, end_cg_idx_))) {
    LOG_WARN("failed to create sstables", K(ret), KPC_(ctx), KPC(this));
  } else if (FALSE_IT(dag_time_guard_click(ObStorageCompactionTimeGuard::CREATE_SSTABLE))) {
  } else {
    ctx->mem_ctx_.mem_click();
    (void)ctx->collect_running_info(start_cg_idx_, end_cg_idx_, hash(),
                                        get_dag_id(), get_time_guard());
    if (OB_NOT_NULL(merge_progress_)
      && OB_TMP_FAIL(merge_progress_->finish_merge_progress())) {
      STORAGE_LOG(WARN, "fail to update final merge progress", K(tmp_ret), KPC(this));
    }
  }
  return ret;
}

/*
 * ObCOMergeBatchExeTask
 */
ObCOMergeBatchExeTask::ObCOMergeBatchExeTask()
 : ObITask(ObITask::TASK_TYPE_MACROMERGE),
   is_inited_(false),
   idx_(0),
   allocator_("CoMergeExe"),
   ctx_(nullptr),
   dag_net_(nullptr),
   merger_(nullptr)
{
}

ObCOMergeBatchExeTask::~ObCOMergeBatchExeTask()
{
  if (nullptr != merger_) {
    merger_->~ObMerger();
  }
}

int ObCOMergeBatchExeTask::init(
    const int64_t idx,
    ObCOTabletMergeCtx &ctx,
    ObCOMergeDagNet &dag_net)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("task is inited", K(ret));
  } else if (OB_UNLIKELY(idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("idx is invalid", K(ret), K(idx));
  } else {
    allocator_.bind_mem_ctx(ctx.mem_ctx_);
    idx_ = idx;
    ctx_ = &ctx;
    dag_net_ = &dag_net;
    is_inited_ = true;
  }
  return ret;
}

int ObCOMergeBatchExeTask::process()
{
  int ret = OB_SUCCESS;
  ObCOMergeBatchExeDag *exe_dag = static_cast<ObCOMergeBatchExeDag*>(dag_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("task is not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(nullptr == ctx_ || nullptr == dag_net_ || nullptr != merger_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected exe task", K(ret), K(ctx_), K(dag_net_), KPC(merger_));
  } else if (FALSE_IT(SET_MEM_CTX(ctx_->mem_ctx_))) {
  } else {
    ObSpinLockGuard lock_guard(exe_dag->exe_lock_);
    if (ctx_->is_cg_merge_infos_valid(exe_dag->get_start_cg_idx(), exe_dag->get_end_cg_idx(), true/*check info ready*/)) {
      // do nothing
    } else if (OB_FAIL(ctx_->prepare_index_builder(exe_dag->get_start_cg_idx(),
                                                   exe_dag->get_end_cg_idx(),
                                                   exe_dag->get_retry_create_task()))) {
      STORAGE_LOG(WARN, "failed to prepare index builder ", K(ret), KPC(exe_dag));
    }
  }

  void *buf = nullptr;
#ifdef ERRSIM
  ret = OB_E(EventTable::EN_CO_MREGE_DAG_SCHEDULE_REST) ret;
  if (OB_FAIL(ret)) {
    LOG_INFO("ERRSIM EN_CO_MREGE_DAG_SCHEDULE_REST PROCESS FAILED", K(ret));
  }
#endif
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObCOMerger)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc memory for major merger", K(ret));
  } else {
    merger_ = new (buf) ObCOMerger(allocator_,
                                   ctx_->static_param_,
                                   exe_dag->get_start_cg_idx(),
                                   exe_dag->get_end_cg_idx(),
                                   ctx_->static_param_.is_rebuild_column_store_);
    if (ctx_->static_param_.is_rebuild_column_store_ || ctx_->is_build_row_store_from_rowkey_cg()) {
      FLOG_INFO("rebuild column store data or build row store", K(ret), K(ctx_->get_tablet_id()), K(ctx_->static_param_));
    }

    merge_start();
    if (OB_FAIL(merger_->merge_partition(*ctx_, idx_))) {
      STORAGE_LOG(WARN, "failed to merge partition", K(ret));
    } else {
      FLOG_INFO("merge macro blocks ok", K(idx_), "task", *this, KPC(dag_));
    }
    merger_->reset();
  }
  return ret;
}

void ObCOMergeBatchExeTask::merge_start()
{
  int tmp_ret = OB_SUCCESS;
  ObCOMergeBatchExeDag *execute_dag = static_cast<ObCOMergeBatchExeDag*>(dag_);
  {
    ObSpinLockGuard lock_guard(execute_dag->exe_lock_);
    // execute init_progress only once
    if (OB_TMP_FAIL(execute_dag->init_merge_progress())) {
      LOG_WARN_RET(tmp_ret, "failed to init merge progress");
    }
    // each task has one merger, and all mergers share the progress
    ObCOMerger *co_merger = static_cast<ObCOMerger*>(merger_);
    co_merger->set_merge_progress(execute_dag->get_merge_progress());
    // execute time click init only once
    if (execute_dag->get_time_guard().is_empty()) {
      ctx_->cg_merge_info_array_[execute_dag->get_start_cg_idx()]->get_sstable_merge_info().update_start_time();
      execute_dag->get_time_guard().set_last_click_ts(execute_dag->get_add_time());
      execute_dag->dag_time_guard_click(ObStorageCompactionTimeGuard::DAG_WAIT_TO_SCHEDULE);
    }
  }
}

int ObCOMergeBatchExeTask::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("task is not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(nullptr == ctx_ || nullptr == dag_net_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx or dag_net is unexpected null", K(ret), KP_(ctx), KP_(dag_net));
  } else if (idx_ + 1 == ctx_->get_concurrent_cnt()) {
    ret = OB_ITER_END;
  } else {
    ObCOMergeBatchExeTask *merge_task = NULL;
    ObCOMergeBatchExeDag *merge_dag = static_cast<ObCOMergeBatchExeDag*>(dag_);

    if (OB_FAIL(merge_dag->alloc_task(merge_task))) {
      LOG_WARN("fail to alloc task", K(ret));
    } else if (OB_FAIL(merge_task->init(idx_ + 1, *ctx_, *dag_net_))) {
      LOG_WARN("fail to init task", K(ret));
    } else {
      next_task = merge_task;
    }

    if (OB_FAIL(ret) && nullptr != merge_task) {
      merge_dag->remove_task(*merge_task);
      merge_task = nullptr;
    }
  }
  return ret;
}

ObCOMergeBatchFinishTask::ObCOMergeBatchFinishTask()
 : ObITask(ObITask::TASK_TYPE_MACROMERGE),
   is_inited_(false),
   ctx_(nullptr),
   dag_net_(nullptr)
{
}

ObCOMergeBatchFinishTask::~ObCOMergeBatchFinishTask()
{
}

int ObCOMergeBatchFinishTask::init(
    ObCOTabletMergeCtx &ctx,
    ObCOMergeDagNet &dag_net)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("task is inited", K(ret));
  } else {
    ctx_ = &ctx;
    dag_net_ = &dag_net;
    is_inited_ = true;
  }
  return ret;
}

int ObCOMergeBatchFinishTask::process()
{
  int ret = OB_SUCCESS;
  ObCOMergeBatchExeDag *execute_dag = static_cast<ObCOMergeBatchExeDag*>(get_dag());
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("task is not inited", K(ret), K_(is_inited));
  } else if (FALSE_IT(SET_MEM_CTX(ctx_->mem_ctx_))) {
  } else if (OB_FAIL(execute_dag->create_sstable_after_merge())) {
    LOG_WARN("failed to create sstable after merge", K(ret), KPC(execute_dag));
  } else {
    FLOG_INFO("co batch sstable merge finish", K(ret),
              "start_cg sstable_merge_info", ctx_->cg_merge_info_array_[execute_dag->get_start_cg_idx()]->get_sstable_merge_info(),
              "time_guard", execute_dag->get_time_guard(),
              KPC(dag_), "param", ctx_->get_dag_param(),
              "mem_peak", ctx_->mem_ctx_.get_total_mem_peak());
  }

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(EventTable::EN_COMPACTION_CO_MERGE_EXE_FAILED) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      STORAGE_LOG(INFO, "ERRSIM EN_COMPACTION_CO_MERGE_EXE_FAILED", K(ret));
    }
  }
#endif

  return ret;
}

/*
 * ObCOMergeFinishDag
 * */
ObCOMergeFinishDag::ObCOMergeFinishDag()
 : ObCOMergeDag(ObDagType::DAG_TYPE_CO_MERGE_FINISH)
{
}

ObCOMergeFinishDag::~ObCOMergeFinishDag()
{
}

int ObCOMergeFinishDag::init_by_param(const share::ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  const ObCOMergeDagParam *merge_param = nullptr;
  if (OB_ISNULL(param)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(param));
  } else if (FALSE_IT(merge_param = static_cast<const ObCOMergeDagParam*>(param))) {
  } else if (OB_UNLIKELY(!is_major_or_meta_merge_type(merge_param->merge_type_) || !merge_param->is_valid())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("Unexpected merge type to init major merge dag", K(ret), KPC(merge_param));
  } else if (OB_FAIL(ObTabletMergeDag::inner_init(merge_param))) {
    LOG_WARN("failed to init ObTabletMergeDag", K(ret));
  } else {
    compat_mode_ = merge_param->compat_mode_;
    dag_net_id_ = merge_param->dag_net_id_;
  }
  return ret;
}

int ObCOMergeFinishDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObCOMergeDagNet *dag_net = static_cast<ObCOMergeDagNet*>(get_dag_net());
  ObCOMergeFinishTask *finish_task = nullptr;
  if (OB_FAIL(create_task(nullptr/*parent*/, finish_task, *dag_net->get_merge_ctx(), *dag_net))) {
    STORAGE_LOG(WARN, "fail to alloc task", K(ret));
  }
  return ret;
}

bool ObCOMergeFinishDag::check_can_schedule()
{
  ObCOMergeDagNet *dag_net = static_cast<ObCOMergeDagNet*>(get_dag_net());
  return dag_net->check_merge_finished();
}

ObCOMergeFinishTask::ObCOMergeFinishTask()
  : ObITask(ObITask::TASK_TYPE_SSTABLE_MERGE_PREPARE),
    is_inited_(false),
    ctx_(nullptr),
    dag_net_(nullptr)
{
}

ObCOMergeFinishTask::~ObCOMergeFinishTask()
{
}

int ObCOMergeFinishTask::init(ObCOTabletMergeCtx &ctx, ObCOMergeDagNet &dag_net)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("task is inited", K(ret));
  } else {
    ctx_ = &ctx;
    dag_net_ = &dag_net;
    is_inited_ = true;
  }
  return ret;
}

int ObCOMergeFinishTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool merge_finish = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("task is not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(nullptr == ctx_ || nullptr == dag_net_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx or dag_net is unexpected null", K(ret), KP(ctx_), KP(dag_net_));
  } else if (FALSE_IT(ctx_->time_guard_click(ObStorageCompactionTimeGuard::EXECUTE))) {
  } else if (FALSE_IT(SET_MEM_CTX(ctx_->mem_ctx_))) {
  } else if (FALSE_IT(ctx_->mem_ctx_.mem_click())) {
  } else if (OB_FAIL(ctx_->update_tablet_after_merge())) {
    LOG_WARN("failed to create sstable and update tablet", K(ret));
  } else {
    if (OB_TMP_FAIL(MTL(ObTenantCompactionProgressMgr*)->update_unfinish_tablet(ctx_->get_merge_version()))) {
      LOG_WARN("failed to update unfinish tablet", K(tmp_ret), K(ctx_->get_merge_version()));
    }
    // ATTENTION! Critical diagnostic log, DO NOT CHANGE!!!
    FLOG_INFO("sstable merge finish", K(ret), KPC(dag_), "param", ctx_->get_dag_param(),
              "mem_peak", ctx_->mem_ctx_.get_total_mem_peak(), "time_guard", ctx_->info_collector_.time_guard_);
  }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(EventTable::EN_COMPACTION_CO_MERGE_FINISH_FAILED) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      STORAGE_LOG(INFO, "ERRSIM EN_COMPACTION_CO_MERGE_FINISH_FAILED", K(ret));
    }
  }
#endif
  if (OB_FAIL(ret)) {
    dag_net_->cancel_dag_net(ret);
  }
  return ret;
}

/*
 * ObCOMergeDagNet
 * */
ObCOMergeDagNet::ObCOMergeDagNet()
  : ObIDagNet(ObDagNetType::DAG_NET_TYPE_CO_MAJOR),
    ObMergeDagHash(),
    is_inited_(false),
    finish_added_(false),
    batch_reduced_(false),
    ctx_lock_(),
    merge_batch_size_(ObCOTabletMergeCtx::DEFAULT_CG_MERGE_BATCH_SIZE),
    merge_status_(COMergeStatus::NOT_INIT),
    basic_param_(),
    tmp_allocator_("CoDagNet", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::MERGE_NORMAL_CTX_ID),
    co_merge_ctx_(nullptr),
    finish_dag_(nullptr),
    time_guard_()
{
}

/*
 * ATTENTION: NEVER USE ANY LOG STREEM VARIABLES IN THIS FUNCTION.
 * Destructor will be called when finish dag net.
 * ObCOMergeDagNet is special, it will be check canceled when ls offine in ObDagNetScheduler::check_ls_compaction_dag_exist_with_cancel.
 * But dag_net is only moved into finished dag net list and delaying freed. So if log streem variables used in this function after ls offine, it will be dangerous
 */
ObCOMergeDagNet::~ObCOMergeDagNet()
{
  finish_dag_ = nullptr;
  if (OB_NOT_NULL(co_merge_ctx_)) {
    co_merge_ctx_->~ObCOTabletMergeCtx();
    tmp_allocator_.free(co_merge_ctx_);
    co_merge_ctx_ = nullptr;
  }
  tmp_allocator_.reset();
}

bool ObCOMergeDagNet::is_valid() const
{
  return ObMergeDagHash::is_valid();
}

int ObCOMergeDagNet::init_by_param(const ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  const ObCOMergeDagParam *merge_param = nullptr;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("dag net is inited", K(ret), K(is_inited_));
  } else if (OB_ISNULL(param)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to init dag net", K(ret), K(param));
  } else if (FALSE_IT(merge_param = static_cast<const ObCOMergeDagParam*>(param))) {
  } else if (OB_UNLIKELY(!is_major_or_meta_merge_type(merge_param->merge_type_) || !merge_param->is_valid())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("Unexpected merge type to init major merge dag", K(ret), KPC(merge_param));
  } else {
    basic_param_ = *merge_param;
    merge_type_ = merge_param->merge_type_;
    ls_id_ = merge_param->ls_id_;
    tablet_id_ = merge_param->tablet_id_;
    is_inited_ = true;
  }
  return ret;
}

int ObCOMergeDagNet::start_running()
{
  int ret = OB_SUCCESS;
  ObCOMergePrepareDag *prepare_dag = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("dag net is not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(get_compat_mode())) {
    LOG_WARN("failed to prepare merge ctx", K(ret));
  } else if (OB_FAIL(create_dag<ObCOMergePrepareDag>(0/*start_cg_idx*/, 0/*end_cg_idx*/, prepare_dag))) {
    LOG_WARN("failed to create preapre dag", K(ret));
  }
  return ret;
}

// schedule_rest_dag is called in loop_running_dag_net_list(Timer)
// schedule dag may not finish yet, need wait merge_status >= PREPARE_FINISHED
int ObCOMergeDagNet::schedule_rest_dag()
{
  int ret = OB_SUCCESS;
  // retry schedule rest cg
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("dag net is not inited", K(ret), K_(is_inited));
  } else if (COMergeStatus::CTX_PREPARED > ATOMIC_LOAD(&merge_status_)) {
    // do nothing, the prepare dag has not been scheduled.
  } else if (OB_UNLIKELY(nullptr == co_merge_ctx_ || !co_merge_ctx_->is_schema_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge ctx is null or schema invalid", K(ret), KPC(co_merge_ctx_));
  } else if (!is_cancel() && COMergeStatus::PREPARE_FINISHED <= ATOMIC_LOAD(&merge_status_)) {
#ifdef ERRSIM
    ret = OB_E(EventTable::EN_CO_MREGE_DAG_SCHEDULE_REST) ret;
    if (OB_FAIL(ret)) {
      LOG_INFO("ERRSIM EN_CO_MREGE_DAG_SCHEDULE_REST SCHEDULE FAILED", K(ret));
    }
#endif
    if (FAILEDx(inner_create_and_schedule_dags())) {
      LOG_WARN("failed to create and schedule rest dags", K(ret));
    }
  }
  return ret;
}

/*
 * ATTENTION: NEVER USE ANY LOG STREEM VARIABLES IN THIS FUNCTION.
 * clear_dag_net_ctx() will be called when finish dag net.
 * ObCOMergeDagNet is special, it will be check canceled when ls offine in ObDagNetScheduler::check_ls_compaction_dag_exist_with_cancel.
 * But dag_net is only moved into finished dag net list and delaying freed. So if log streem variables used in this function after ls offine, it will be dangerous
 */
int ObCOMergeDagNet::clear_dag_net_ctx()
{
  return ObIDagNet::clear_dag_net_ctx();
}

#define MARK_CG_SCHEDULE_STATUS(start_cg_idx, end_cg_idx, target_status) \
    for (int64_t i = start_cg_idx; i < end_cg_idx; ++i) { \
      if (ObCOTabletMergeCtx::CG_SCHE_STATUS_FINISHED != co_merge_ctx_->cg_schedule_status_array_[i]) { \
        co_merge_ctx_->cg_schedule_status_array_[i] = target_status; \
      } else { \
        LOG_INFO("unexpect mark status after finish", K(start_cg_idx), K(end_cg_idx), K(target_status)); \
      } \
    }

bool ObCOMergeDagNet::check_merge_finished()
{
  ObMutexGuard guard(ctx_lock_);
  return co_merge_ctx_->all_cg_finish();
}

bool ObCOMergeDagNet::should_force_cancel()
{
  return ObCOMergeDagNet::PREPARE_FINISHED > ATOMIC_LOAD(&merge_status_);
}

void ObCOMergeDagNet::cancel_dag_net(const int error_code)
{
  if (should_force_cancel()
      || !ObCOMergeScheduleDag::can_ignore_warning(error_code)) {
    LOG_WARN_RET(error_code, "cancel co dag net");
    set_cancel();
    // avoid that the canceled dag_net_ keeps an unschedule finish_dag_, which cause the dag_net_ never finish
    (void)deal_with_cancel();
  }
}

int ObCOMergeDagNet::create_co_execute_dags(share::ObIDag &schedule_dag)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("dag net is not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(nullptr == co_merge_ctx_ || !co_merge_ctx_->is_schema_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge ctx is null or schema invalid", K(ret), KPC(co_merge_ctx_));
    SET_DAG_LOCATION(&schedule_dag);
  } else if (OB_FAIL(choose_merge_batch_size(co_merge_ctx_->array_count_))) {
    LOG_WARN("failed to choose merge batch size", K(ret));
  } else if (OB_FAIL(inner_create_and_schedule_dags(&schedule_dag))) {
    LOG_WARN("failed to create and schedule dags", K(ret));
  }

  return ret;
}

int ObCOMergeDagNet::choose_merge_batch_size(const int64_t column_group_cnt)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(column_group_cnt <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected count", K(ret), K(column_group_cnt));
  } else if (column_group_cnt < ObCOTabletMergeCtx::DEFAULT_CG_MERGE_BATCH_SIZE * 2) {
    merge_batch_size_ = column_group_cnt;
  } else {
    int64_t batch_cnt = column_group_cnt / ObCOTabletMergeCtx::DEFAULT_CG_MERGE_BATCH_SIZE;
    merge_batch_size_ = column_group_cnt / batch_cnt;
  }

  return ret;
}

int ObCOMergeDagNet::inner_schedule_finish_dag(ObIDag *parent_dag)
{
  int ret = OB_SUCCESS;
  if (!ATOMIC_LOAD(&finish_added_)) { // avoid to add finish_dag which has finished and been freed
    if (OB_NOT_NULL(parent_dag)) { // schedule_dag or prepare_dag call this func
      if (OB_NOT_NULL(finish_dag_)) {
        if (OB_FAIL(MTL(share::ObTenantDagScheduler*)->add_dag(finish_dag_))) {
          LOG_WARN("failed to add finish dag", K(ret), KPC(finish_dag_));
        } else {
          ATOMIC_SET(&finish_added_, true);
        }
      } // no need to create finish dag when finish_dag is not exist
    } else if (check_merge_finished()) {  // co_dag_net call this func by schedule_rest_dag()
      if (OB_ISNULL(finish_dag_)) {
        if (OB_FAIL(create_dag<ObCOMergeFinishDag>(0, 0, finish_dag_))) { // already add into scheduler
          LOG_WARN("failed to create and add finish dag", K(ret));
        }
      } else if (OB_FAIL(MTL(share::ObTenantDagScheduler*)->add_dag(finish_dag_))) {
        LOG_WARN("failed to add finish dag", K(ret));
      }
      if (OB_SUCC(ret)) {
        ATOMIC_SET(&finish_added_, true);
      } else if (OB_EAGAIN == ret) {
        // when parent_dag try to schedule finish_dag, all exe dags may finished and also try to schedule finish_dag
        ret = OB_SUCCESS; // ignore OB_EAGAIN
      }
    }
  }
  return ret;
}

void ObCOMergeDagNet::try_update_merge_batch_size(const int64_t column_group_cnt)
{
  int tmp_ret = OB_SUCCESS;
  int64_t merge_thread = 0; // default value
  if (OB_TMP_FAIL(MTL(ObTenantDagScheduler *)->get_limit(ObDagPrio::DAG_PRIO_COMPACTION_LOW, merge_thread))) {
    LOG_WARN_RET(tmp_ret, "failed to get major thread limit, use default value");
  }
  merge_thread = (0 >= merge_thread) ? ObCompactionEstimator::DEFAULT_MERGE_THREAD_CNT : merge_thread;

  const int64_t mem_allow_used = lib::get_tenant_memory_remain(MTL_ID()) * ADAPTIVE_PERCENT; // allow use 40% memory for co merge
  int64_t batch_mem_allow_per_thread = MAX(mem_allow_used / merge_thread - ObCompactionEstimator::MAJOR_MEM_PER_THREAD, 0);
  int64_t mem_allow_batch_size = MAX(batch_mem_allow_per_thread / ObCompactionEstimator::CO_MAJOR_CG_BASE_MEM, 1);

  if (mem_allow_batch_size > merge_batch_size_ * 2) {
    merge_batch_size_ = MIN(merge_batch_size_ * 2, column_group_cnt);
  } else if (OB_TMP_FAIL(choose_merge_batch_size(column_group_cnt))) {
    merge_batch_size_ = ObCOTabletMergeCtx::DEFAULT_CG_MERGE_BATCH_SIZE;
    LOG_WARN_RET(tmp_ret, "failed to choose merge batch size, use default batch size", K(column_group_cnt), K(merge_batch_size_));
  }

  FLOG_INFO("[ADAPTIVE_SCHED] update co merge batch size", K(merge_thread),
      K(mem_allow_used), K(batch_mem_allow_per_thread), K(mem_allow_batch_size), K(merge_batch_size_));
}

int ObCOMergeDagNet::inner_create_and_schedule_dags(ObIDag *parent_dag)
{
  int ret = OB_SUCCESS;
  const int64_t max_cg_idx = co_merge_ctx_->get_schema()->get_column_group_count();
  common::ObSEArray<ObCOMergeBatchExeDag *, 8> exe_dag_array;
  ObCOMergeBatchExeDag *dag = nullptr;

  /*
   * When firstly scheduled by PrepareDag(skip minor merge) or ScheduleDag, schedule_dag is not nullptr.
   * If there are too many cgs, batch exe dags can not be created in one round, so we delay creating FinsihDag.
   * If not, we can create FinishDag and add it into dag scheduler in this function.
   * If major merge type is BUILD_ROW_STORE_MERGE, we consider cg number as only 1.
   */
  if (OB_NOT_NULL(parent_dag)
  && (co_merge_ctx_->is_build_row_store() || max_cg_idx < DELAY_SCHEDULE_FINISH_DAG_CG_CNT)) {
    // add into dag_scheduler after parent-child relation generated
    if (OB_FAIL(create_dag<ObCOMergeFinishDag>(0, 0, finish_dag_, parent_dag/*parent*/, false/*add_scheduler_flag*/))) {
      LOG_WARN("failed to create finish dag", K(ret));
    }
  }
  // refine merge_batch_size_ with tenant memory
  if (OB_SUCC(ret) && MTL(ObTenantTabletScheduler *)->enable_adaptive_merge_schedule()) {
    try_update_merge_batch_size(co_merge_ctx_->array_count_);
  }

  int64_t allowed_schedule_dag_count =
      MTL(share::ObTenantDagScheduler*)->allowed_schedule_dag_count(ObDagType::DAG_TYPE_CO_MERGE_BATCH_EXECUTE);
  if (OB_SUCC(ret) && allowed_schedule_dag_count > 0) {
    ObMutexGuard guard(ctx_lock_);
    batch_reduced_ = false;
    if (co_merge_ctx_->is_build_row_store()) {
      if (OB_FAIL(inner_create_row_store_dag(parent_dag, max_cg_idx, exe_dag_array))) {
        LOG_WARN("failed to create co exe dag for row store", K(ret), K(exe_dag_array.count()));
      }
    } else if (OB_FAIL(inner_create_column_store_dag(parent_dag, allowed_schedule_dag_count, max_cg_idx, exe_dag_array))) {
      LOG_WARN("failed to create co exe dag for column store", K(ret), K(allowed_schedule_dag_count), K(exe_dag_array.count()));
    }
  } // ctx_lock_ unlock // avoid lock ctx_lock_ before prio_lock_

  int64_t unscheduled_dag_idx = 0;
  if (OB_SUCC(ret)) {
    // schedule all created exe dag
    if (OB_FAIL(inner_schedule_exe_dags(exe_dag_array, unscheduled_dag_idx))) {
      LOG_WARN("failed to schedule all execute dags", K(ret), K(unscheduled_dag_idx));
    }
  }
  if (OB_FAIL(ret)) {
    inner_free_exe_dags(unscheduled_dag_idx, exe_dag_array, dag);
  } else if (OB_FAIL(inner_schedule_finish_dag(parent_dag))) {
    LOG_WARN("failed to schedule finish dag", K(ret));
  }
  return ret;
}

int ObCOMergeDagNet::inner_create_row_store_dag(
    ObIDag *parent_dag,
    const int64_t &max_cg_idx,
    common::ObIArray<ObCOMergeBatchExeDag *> &exe_dag_array)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("chengkong debug: build row store in this compaction", "co_major_merge_type_",
    ObCOMajorMergePolicy::co_major_merge_type_to_str(co_merge_ctx_->static_param_.co_major_merge_type_));

  ObCOMergeBatchExeDag *dag = nullptr;
  const ObIArray<ObStorageColumnGroupSchema> &cg_schemas = co_merge_ctx_->get_schema()->get_column_groups();
  const int64_t cg_schemas_count = cg_schemas.count();
  int64_t start_cg_idx = OB_INVALID_INDEX_INT64;
  int64_t allowed_schedule_dag_count_place_holder = 1;

  for (int64_t idx = 0; idx < cg_schemas_count; idx++) {
    if (cg_schemas.at(idx).is_all_column_group() || cg_schemas.at(idx).is_rowkey_column_group()) {
      start_cg_idx = idx;
      break;
    }
  }

  if (OB_INVALID_INDEX_INT64 == start_cg_idx) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get all cg or rowkey cg in cg schemas", K(ret));
  } else {
    if (OB_NOT_NULL(parent_dag)) {
      // in schedule state, firstly into inner_create_and_schedule_dags
      MARK_CG_SCHEDULE_STATUS(0, max_cg_idx, ObCOTabletMergeCtx::CG_SCHE_STATUS_FINISHED);
      co_merge_ctx_->cg_schedule_status_array_[start_cg_idx] = ObCOTabletMergeCtx::CG_SCHE_STATUS_IDLE;
      co_merge_ctx_->one_batch_finish(max_cg_idx-1);
    }

    if (ObCOTabletMergeCtx::is_cg_could_schedule(co_merge_ctx_->cg_schedule_status_array_[start_cg_idx])
    && OB_FAIL(inner_create_and_add_exe_dag(start_cg_idx, start_cg_idx+1, max_cg_idx, allowed_schedule_dag_count_place_holder,
            dag, exe_dag_array, false /*add_scheduler_flag*/))) {
      LOG_WARN("failed to create or add cg dag", K(ret), K(start_cg_idx));
    }
  }
  return ret;
}

int ObCOMergeDagNet::inner_create_column_store_dag(
    ObIDag *parent_dag,
    int64_t &allowed_schedule_dag_count,
    const int64_t &max_cg_idx,
    common::ObIArray<ObCOMergeBatchExeDag *> &exe_dag_array)
{
  int ret = OB_SUCCESS;
  ObCOMergeBatchExeDag *dag = nullptr;
  int64_t start_cg_idx = 0;
  int64_t end_cg_idx = 0;

  while (OB_SUCC(ret) && end_cg_idx < max_cg_idx && allowed_schedule_dag_count > 0) {
    start_cg_idx = end_cg_idx;
    if (OB_NOT_NULL(parent_dag)) { // in schedule stage
      end_cg_idx = (start_cg_idx + merge_batch_size_ * 2 > max_cg_idx)
                  ? max_cg_idx
                  : start_cg_idx + merge_batch_size_;
    } else if (ObCOTabletMergeCtx::is_cg_could_schedule(co_merge_ctx_->cg_schedule_status_array_[end_cg_idx])) {
      const int64_t tmp_end_cg_idx = MIN(start_cg_idx + merge_batch_size_, max_cg_idx);
      do { // decide end_cg_idx by cg schedule status
        ++end_cg_idx;
      } while (end_cg_idx < tmp_end_cg_idx
          && ObCOTabletMergeCtx::is_cg_could_schedule(co_merge_ctx_->cg_schedule_status_array_[end_cg_idx]));
    }
    if (start_cg_idx == end_cg_idx) {
      // move to next cg
      ++end_cg_idx;
    } else if (OB_FAIL(inner_create_and_add_exe_dag(start_cg_idx, end_cg_idx, max_cg_idx, allowed_schedule_dag_count,
            dag, exe_dag_array, false /*add_scheduler_flag*/))) {
      LOG_WARN("failed to create or add cg dag", K(ret), K(start_cg_idx), K(end_cg_idx));
    }
  }
  return ret;
}

int ObCOMergeDagNet::swap_tablet_after_minor()
{
  int ret = OB_SUCCESS;
  ObTabletHandle tmp_tablet_handle;
  ObGetMergeTablesResult tmp_result;
  if (OB_FAIL(co_merge_ctx_->get_ls()->get_tablet(
          co_merge_ctx_->get_tablet_id(),
          tmp_tablet_handle,
          ObTabletCommon::DEFAULT_GET_TABLET_NO_WAIT,
          storage::ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    LOG_WARN("failed to get tablet", K(ret));
  } else if (OB_FAIL(ObTablet::check_transfer_seq_equal(*tmp_tablet_handle.get_obj(), co_merge_ctx_->get_transfer_seq()))) {
    LOG_WARN("tmp tablet transfer seq not eq with old transfer seq", K(ret),
        "tmp_tablet_meta", tmp_tablet_handle.get_obj()->get_tablet_meta(),
        "old_transfer_seq", co_merge_ctx_->get_transfer_seq());
  } else if (OB_FAIL(ObPartitionMergePolicy::get_result_by_snapshot(
    *tmp_tablet_handle.get_obj(),
    co_merge_ctx_->get_merge_version(),
    tmp_result))) {
    LOG_WARN("failed to get result by snapshot", K(ret));
  } else if (OB_FAIL(co_merge_ctx_->static_param_.tables_handle_.assign(tmp_result.handle_))) {
    LOG_WARN("failed to assign tables handle", K(ret), K(tmp_result));
  } else {
    co_merge_ctx_->tablet_handle_ = tmp_tablet_handle;
    co_merge_ctx_->static_param_.rowkey_read_info_ =
      static_cast<const ObRowkeyReadInfo *>(&(co_merge_ctx_->get_tablet()->get_rowkey_read_info()));
    LOG_INFO("success to swap tablet after minor", K(ret), K(tmp_result),
      "new_rowkey_read_info", co_merge_ctx_->static_param_.rowkey_read_info_);
  }
  return ret;
}

int ObCOMergeDagNet::inner_create_and_add_exe_dag(
    const int64_t start_cg_idx,
    const int64_t end_cg_idx,
    const int64_t max_cg_idx,
    int64_t &allowed_schedule_dag_count,
    ObCOMergeBatchExeDag *&dag,
    common::ObIArray<ObCOMergeBatchExeDag *> &exe_dag_array,
    const bool add_scheduler_flag/* = true*/)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(end_cg_idx < 0 || end_cg_idx > max_cg_idx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid end_cg_idx", K(ret), K(end_cg_idx), K(max_cg_idx));
  } else if (OB_FAIL(create_dag<ObCOMergeBatchExeDag>(start_cg_idx, end_cg_idx, dag, nullptr, add_scheduler_flag))) {
    MARK_CG_SCHEDULE_STATUS(start_cg_idx, end_cg_idx, ObCOTabletMergeCtx::CG_SCHE_STATUS_FAILED);
  } else {
    MARK_CG_SCHEDULE_STATUS(start_cg_idx, end_cg_idx, ObCOTabletMergeCtx::CG_SCHE_STATUS_CREATED);
    LOG_DEBUG("create dag", K(ret), K(co_merge_ctx_->array_count_), K(start_cg_idx), K(end_cg_idx),
        K(max_cg_idx));
  }

  if (OB_FAIL(ret)) {
    if (OB_EAGAIN != ret && OB_SIZE_OVERFLOW != ret) {
      LOG_WARN("failed to create cg dag", K(ret), K(start_cg_idx), K(end_cg_idx));
    }
  } else if (OB_FAIL(exe_dag_array.push_back(dag))) {
    LOG_WARN("failed to add next execute dag", K(ret));
  } else {
    FLOG_INFO("success to create cg dag", K(ret), K(start_cg_idx), K(end_cg_idx), KP(dag));
    --allowed_schedule_dag_count;
    dag = nullptr; // dag is in exe_dag_array
  }
  return ret;
}

int ObCOMergeDagNet::inner_schedule_exe_dags(
    common::ObIArray<ObCOMergeBatchExeDag *> &dag_array,
    int64_t &unscheduled_dag_idx)
{
  int ret = OB_SUCCESS;
  unscheduled_dag_idx = 0;
  ObCOMergeBatchExeDag *dag = nullptr;
  for (int64_t idx = 0; OB_SUCC(ret) && idx < dag_array.count(); ++idx) {
    dag = dag_array.at(idx);
    const uint32_t start_cg_idx = dag->get_start_cg_idx();
    const uint32_t end_cg_idx = dag->get_end_cg_idx();
    // don't add finish_dag_ as child, cause after add child, free finish_dag_ needs all parent dags
    // check_can_schedule() will guarante to schedule finish_dag after all exe dags finished
    if (OB_FAIL(MTL(share::ObTenantDagScheduler*)->add_dag(dag))) {
      LOG_WARN("failed to add dag into scheduler", K(ret), KP(dag));
    } else {
      unscheduled_dag_idx = idx + 1;
      ObMutexGuard guard(ctx_lock_);
      MARK_CG_SCHEDULE_STATUS(start_cg_idx,
                              end_cg_idx,
                              ObCOTabletMergeCtx::CG_SCHE_STATUS_SCHEDULED);
    }
  }
  return ret;
}

void ObCOMergeDagNet::inner_free_exe_dags(
    const int64_t start_idx,
    common::ObIArray<ObCOMergeBatchExeDag *> &dag_array,
    ObCOMergeBatchExeDag *&dag)
{
  int tmp_ret = OB_SUCCESS;

  ObMutexGuard guard(ctx_lock_);
  for (int64_t idx = start_idx; idx < dag_array.count(); ++idx) {
    inner_free_exe_dag(dag_array.at(idx));
  }
  inner_free_exe_dag(dag);
}

void ObCOMergeDagNet::inner_free_exe_dag(ObCOMergeBatchExeDag *&dag)
{
  int tmp_ret = OB_SUCCESS;

  if (OB_NOT_NULL(dag)) {
    int64_t start_cg_idx = dag->get_start_cg_idx();
    int64_t end_cg_idx = dag->get_end_cg_idx();
    MARK_CG_SCHEDULE_STATUS(start_cg_idx, end_cg_idx, ObCOTabletMergeCtx::CG_SCHE_STATUS_FAILED);
    (void)MTL(share::ObTenantDagScheduler*)->free_dag(*dag);
    dag = nullptr;
    if (OB_NOT_NULL(co_merge_ctx_)) {
      (void) co_merge_ctx_->destroy_merge_info_array(start_cg_idx, end_cg_idx);
    }
  }
}

int ObCOMergeDagNet::dag_report_result(
    const uint32_t start_cg_idx,
    const uint32_t end_cg_idx,
    const int dag_ret)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("dag net is not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(nullptr == co_merge_ctx_ || !co_merge_ctx_->is_schema_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge ctx is null or schema invalid", K(ret), KPC(co_merge_ctx_));
  } else {
    ObMutexGuard guard(ctx_lock_);
    const int64_t max_cg_idx = co_merge_ctx_->get_schema()->get_column_group_count();
    if (OB_UNLIKELY(end_cg_idx < 0 || end_cg_idx > max_cg_idx)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid end_cg_idx", K(ret), K(end_cg_idx), K(max_cg_idx));
    } else if (FALSE_IT(co_merge_ctx_->destroy_merge_info_array(start_cg_idx, end_cg_idx))) {
    } else {
      FLOG_INFO("dag report result", K(ret), K(start_cg_idx), K(end_cg_idx), K(dag_ret), "exe_stat", co_merge_ctx_->exe_stat_);
      if (OB_SUCCESS == dag_ret) {
        MARK_CG_SCHEDULE_STATUS(start_cg_idx, end_cg_idx, ObCOTabletMergeCtx::CG_SCHE_STATUS_FINISHED);
        co_merge_ctx_->one_batch_finish(end_cg_idx - start_cg_idx);
      } else {
        MARK_CG_SCHEDULE_STATUS(start_cg_idx, end_cg_idx, ObCOTabletMergeCtx::CG_SCHE_STATUS_FAILED);
        co_merge_ctx_->one_batch_fail();
        if (OB_ALLOCATE_MEMORY_FAILED == dag_ret) {
          // split cg idx array to schedule
          LOG_INFO("failed for dag_ret = allocate memory", K(ret), K(dag_ret));
          if (!batch_reduced_ && (merge_batch_size_ > 1)) {
            merge_batch_size_ = merge_batch_size_ >> 1;
            batch_reduced_ = true;
          }
        }
      }
    }
    if (OB_SUCC(ret)
      && !is_cancel()
      && (co_merge_ctx_->is_co_dag_net_failed() || ObTabletMergeDag::can_not_retry_warning(dag_ret))) {
      set_cancel();
      LOG_WARN("dag net error count reach ERROR_COUNT_THREASHOLD", "exe_stat", co_merge_ctx_->exe_stat_, K(dag_ret));
    }
  }
  MTL(share::ObTenantDagScheduler *)->set_fast_schedule_dag_net();
  return ret;
}

int ObCOMergeDagNet::get_compat_mode()
{
  int ret = OB_SUCCESS;
  ObLSHandle tmp_ls_handle;
  ObTabletHandle tmp_tablet_handle;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("dag net is not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(merge_batch_size_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge batch size is not valid", K(ret), K(merge_batch_size_));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, tmp_ls_handle, ObLSGetMod::COMPACT_MODE))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id_), K(tablet_id_));
  } else if (OB_FAIL(tmp_ls_handle.get_ls()->get_tablet_svr()->get_tablet(
          tablet_id_,
          tmp_tablet_handle,
          0/*timeout_us*/,
          storage::ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    LOG_WARN("failed to get tablet", K(ret), K(ls_id_), K(tablet_id_));
  } else if (OB_FAIL(ObTablet::check_transfer_seq_equal(*tmp_tablet_handle.get_obj(), basic_param_.transfer_seq_))) {
    LOG_WARN("tmp tablet transfer seq not eq with old transfer seq", K(ret),
        "tmp_tablet_meta", tmp_tablet_handle.get_obj()->get_tablet_meta(),
        "old_transfer_seq", basic_param_.transfer_seq_);
  } else {
    basic_param_.dag_net_id_ = get_dag_id();
    basic_param_.skip_get_tablet_ = true;
    basic_param_.compat_mode_ = tmp_tablet_handle.get_obj()->get_tablet_meta().compat_mode_;
  }
  return ret;
}

int ObCOMergeDagNet::prepare_co_merge_ctx()
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  bool useless_finish_flag = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("dag net is not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(NULL != co_merge_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge ctx is unexpected null", K(ret), KPC(co_merge_ctx_));
  } else if (!MTL(ObTenantTabletScheduler *)->could_major_merge_start()) {
    ret = OB_CANCELED;
    LOG_INFO("Tenant Merge has been paused", K(ret), KPC(this));
  } else if (OB_ISNULL(buf = static_cast<char*>(tmp_allocator_.alloc(sizeof(ObCOTabletMergeCtx))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret), K(ls_id_), K(tablet_id_));
  } else if (FALSE_IT(co_merge_ctx_ = new (buf) ObCOTabletMergeCtx(*this, basic_param_, tmp_allocator_))) { // will set thread local mem ctx
  } else if (FALSE_IT(co_merge_ctx_->init_time_guard(get_add_time()))) {
  } else if (FALSE_IT(co_merge_ctx_->time_guard_click(ObStorageCompactionTimeGuard::DAG_WAIT_TO_SCHEDULE))) {
  } else if (OB_FAIL(co_merge_ctx_->build_ctx(useless_finish_flag))) {
    LOG_WARN("failed to build ctx", KR(ret), KP_(co_merge_ctx));
  } else {
    update_merge_status(COMergeStatus::CTX_PREPARED);
  }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(EventTable::EN_COMPACTION_CO_MERGE_PREPARE_CTX_FAILED) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      STORAGE_LOG(INFO, "ERRSIM EN_COMPACTION_CO_MERGE_PREPARE_CTX_FAILED", K(ret));
    }
  }
#endif
  return ret;
}

bool ObCOMergeDagNet::operator ==(const ObIDagNet &other) const
{
  bool bret = false;
  if (get_type() == other.get_type()) {
    const ObCOMergeDagNet &other_dag_net = static_cast<const ObCOMergeDagNet&>(other);
    if (merge_type_ == other_dag_net.merge_type_
        || ls_id_ == other_dag_net.ls_id_
        || tablet_id_ == other_dag_net.tablet_id_) {
      bret = true;
    }
  }
  return bret;
}

int ObCOMergeDagNet::fill_dag_net_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  const char *merge_type = merge_type_to_str(merge_type_);

  if (OB_FAIL(databuff_printf(buf, buf_len, "COMergeDagNet: ls_id=%ld tablet_id=%ld",
          ls_id_.id(), tablet_id_.id()))) {
    LOG_WARN("failed to fill comment", K(ret));
  }
  return ret;
}

int ObCOMergeDagNet::fill_comment(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  const char *merge_type = merge_type_to_str(merge_type_);

  if (OB_FAIL(databuff_printf(buf, buf_len, "COMergeDagNet: ls_id=%ld tablet_id=%ld",
          ls_id_.id(), tablet_id_.id()))) {
    LOG_WARN("failed to fill comment", K(ret));
  }
  return ret;
}

} // namespace compaction
} // namespace oceanbase
