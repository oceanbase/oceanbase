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
#include "ob_co_merge_dag.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/compaction/ob_tenant_compaction_progress.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "storage/compaction/ob_compaction_dag_ranker.h"
#include "observer/ob_server_event_history_table_operator.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/compaction_v2/ob_ss_major_merge_ctx.h"
#endif
namespace oceanbase
{
using namespace share;
using namespace storage;

namespace compaction
{
ERRSIM_POINT_DEF(EN_COMPACTION_ADD_CO_MREGE_FINISH_TASK_FAILED);
ERRSIM_POINT_DEF(EN_COMPACTION_PERSIST_FAILED);
ERRSIM_POINT_DEF(EN_COMPACTION_PERSIST_GENERATE_NEXT_FAILED);
ERRSIM_POINT_DEF(EN_COMPACTION_REPLAY_FAILED);
ERRSIM_POINT_DEF(EN_COMPACTION_UPDATE_TABLET_FAILED);
ERRSIM_POINT_DEF(EN_COMPACTION_GENERATE_NEXT_FAILED);
ERRSIM_POINT_DEF(EN_COMPACTION_RANDOM_BATCH_SIZE);
ObCOMergeDagParam::ObCOMergeDagParam()
  : ObTabletMergeDagParam(),
    compat_mode_(lib::Worker::CompatMode::INVALID),
    dag_net_id_(),
    range_count_(0),
    cg_count_(0),
    is_using_column_tmp_file_(false)
{
}

bool ObCOMergeDagParam::is_valid() const
{
  return ObTabletMergeDagParam::is_valid() && 0 <= cg_count_ && 0 <= range_count_
      && lib::Worker::CompatMode::INVALID != compat_mode_;
}

int ObCOMergeTask::init(ObCOMergeDagNet &dag_net)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("task is inited", K(ret), K(dag_net));
  } else {
    dag_net_ = &dag_net;
    is_inited_ = true;
  }
  return ret;
}

int ObCOMergeTask::get_ctx_from_dag_net(ObCOMergeDagNet &dag_net, ObCOTabletMergeCtx *&ctx)
{
  int ret = OB_SUCCESS;
  ctx = nullptr;
  if (OB_ISNULL(ctx = static_cast<ObCOTabletMergeCtx*>(dag_net.get_merge_ctx()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is null", K(ret));
  } else if (!ctx->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is not valid", K(ret), KPC(ctx));
  }
  return ret;
}

/*
 * ObCOMergeDag
 * */
ObCOMergeDag::ObCOMergeDag(const share::ObDagType::ObDagTypeEnum type)
 : ObTabletMergeDag(type)
{
}

int ObCOMergeDag::init_by_param(const share::ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  const ObCOMergeDagParam *merge_param = nullptr;
  if (OB_ISNULL(param)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(param));
  } else if (FALSE_IT(merge_param = static_cast<const ObCOMergeDagParam*>(param))) {
  } else if (OB_UNLIKELY(!is_major_or_meta_merge_type(merge_param->merge_type_) || !merge_param->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("Unexpected merge type to init major merge dag", K(ret), KPC(merge_param));
  } else if (OB_FAIL(ObTabletMergeDag::inner_init(merge_param))) {
    LOG_WARN("failed to init ObTabletMergeDag", K(ret));
  } else {
    compat_mode_ = merge_param->compat_mode_;
    dag_net_id_ = merge_param->dag_net_id_;
  }
  return ret;
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
    // TODO more infos
    if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                  ls_id_.id(),
                                  static_cast<int64_t>(tablet_id_.id()),
                                  param_.merge_version_,
                                  "exec_mode", exec_mode_to_str(param_.exec_mode_),
                                  "dag_net_id", dag_net_id_))) {
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
int ObCOMergePrepareDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObCOMergeDagNet* dag_net = static_cast<ObCOMergeDagNet*>(get_dag_net());
  ObCOMergePrepareTask *task = nullptr;
  if (OB_FAIL(create_task(nullptr/*parent*/, task, *dag_net))) {
    STORAGE_LOG(WARN, "fail to create prepare task", K(ret));
  }
  return ret;
}

/*
 * ObCOMergePrepareTask
 * */
int ObCOMergePrepareTask::create_schedule_dag(ObCOTabletMergeCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObGetMergeTablesResult minor_result;
  ObSEArray<ObTableHandleV2, 4> minor_tables;

  if (is_convert_co_major_merge(ctx.get_merge_type())) {
    // convert co major merge only rely on major sstable
  } else if (!MTL(ObTenantTabletScheduler *)->enable_adaptive_merge_schedule()) {
    // don't schedule minor dag if enable_adaptive_merge_schedule=false
  } else if (!is_local_exec_mode(ctx.get_exec_mode())) {
    // don't schedule minor dag in ss mode
  } else if (OB_FAIL(ObPartitionMergePolicy::get_co_major_minor_merge_tables(ctx.get_schema(),
                                                                             ctx.get_merge_version(),
                                                                             ctx.get_major_sstable_count(),
                                                                             ctx.get_tables_handle(),
                                                                             minor_tables))) {
    LOG_WARN("failed to get co major minor merge tables", K(ret));
  } else if (!minor_tables.empty()) {
    ObMinorExecuteRangeMgr minor_range_mgr;
    if (OB_FAIL(minor_range_mgr.get_merge_ranges(ctx.get_ls_id(), ctx.get_tablet_id()))) {
      LOG_WARN("failed to get merge range", K(ret), "param", ctx.get_dag_param());
    }

    for (int64_t idx = 0; OB_SUCC(ret) && idx < minor_tables.count(); ++idx) {
      if (minor_range_mgr.in_execute_range(minor_tables.at(idx).get_table())) {
        minor_result.reset();
        LOG_INFO("table in execute range", K(ret), K(idx), K(minor_tables), K(minor_range_mgr.exe_range_array_));
        break;
      } else if (OB_FAIL(ObPartitionMergePolicy::add_table_with_check(minor_result, minor_tables.at(idx)))) {
        LOG_WARN("failed to add table", K(ret), K(idx), K(minor_tables.at(idx)));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (minor_result.handle_.get_count() > 0) {
    if (OB_FAIL(schedule_minor_exec_dag(ctx, minor_result))) {
      LOG_WARN("failed to schedule minor exec dag", K(ret));
    }
  } else {
    /* No need to schedule minor merge, so creating ObCOMergeSCheduleDag is meanless
     * We can directly execute the ObCOMergeScheduleTask to accelerate the processing of co merge
     */
    if (OB_FAIL(dag_net_->create_co_execute_dag(*get_dag()))) {
      LOG_WARN("failed to create co execute dags in prepare task", K(ret), K(ctx)); // cancel dag net in process()
    }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(EventTable::EN_COMPACTION_CO_MERGE_PREPARE_FAILED) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      SERVER_EVENT_SYNC_ADD("merge_errsim", "co_merge_prepare_failed", "ret_code", ret);
      STORAGE_LOG(INFO, "ERRSIM EN_COMPACTION_CO_MERGE_PREPARE_FAILED", K(ret));
    }
  }
#endif
  }
  return ret;
}

int ObCOMergePrepareTask::schedule_minor_exec_dag(
  ObCOTabletMergeCtx &ctx,
  ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  /* We need to schedule minor merge first to reduce the count of inc sstables
   * Current Dag Path: prepare_dag -> minor_dag -> schedule_dag
   * Optimization: use ObMinorMergeTask instead of the MinorDags && ScheduleDags?
   */
  const int32_t private_transfer_epoch = ctx.static_param_.private_transfer_epoch_;
  ObCOMergeScheduleDag *schedule_dag = nullptr;
  ObTabletMergeExecuteDag *minor_exe_dag = nullptr;
  result.version_range_.multi_version_start_ = ctx.get_tablet()->get_multi_version_start();
  result.version_range_.base_version_ = 0;
  result.version_range_.snapshot_version_ = ctx.get_tablet()->get_snapshot_version();
  result.private_transfer_epoch_ = private_transfer_epoch;

  ObTabletMergeDagParam dag_param(MINOR_MERGE, ctx.get_ls_id(),
                                  ctx.get_tablet_id(), private_transfer_epoch);
  if (OB_FAIL(MTL(share::ObTenantDagScheduler *)->alloc_dag(minor_exe_dag))) {
    LOG_WARN("failed to alloc dag", K(ret));
  } else if (OB_FAIL(ctx.get_tablet()->get_recycle_version(result.version_range_.multi_version_start_, result.version_range_.base_version_))) {
    LOG_WARN("Fail to get table store recycle version", K(ret), K(result.version_range_), KPC(ctx.get_tablet()));
  } else if (OB_FAIL(minor_exe_dag->prepare_init(
              dag_param,
              ctx.get_tablet()->get_tablet_meta().compat_mode_,
              result,
              ctx.static_param_.ls_handle_))) {
    LOG_WARN("failed to init dag", K(ret), K(result));
  } else if (OB_FAIL(dag_net_->add_dag_into_dag_net(*minor_exe_dag))) {
    LOG_WARN("failed to add dag into dag net", K(ret), K(minor_exe_dag), KPC(dag_net_));
  } else if (OB_FAIL(dag_net_->create_dag(schedule_dag, minor_exe_dag/*parent*/))) {
    // will add ObCOMergeScheduleDag into scheduler, but have minor_exe_dag as parent
    // alloc schedule_dag will be destroy in create_dag()
    LOG_WARN("failed to create schedule dag", K(ret));
#ifdef ERRSIM
  } else if (OB_FAIL(ret = OB_E(EventTable::EN_COMPACTION_SCHEDULE_MINOR_FAIL) OB_SUCCESS)) {
    FLOG_INFO("ERRSIM EN_COMPACTION_SCHEDULE_MINOR_FAIL", KR(ret), K(ctx));
    SERVER_EVENT_SYNC_ADD("merge_errsim", "schedule_minor_failure", "tablet_id", ctx.get_tablet_id().id());
#endif
  } else if (OB_FAIL(MTL(share::ObTenantDagScheduler *)->add_dag(minor_exe_dag, true/*is_emergency*/))) {
    if (OB_EAGAIN != ret && OB_SIZE_OVERFLOW != ret) {
      LOG_WARN("failed to add dag", K(ret), KPC(minor_exe_dag));
    }
  } else {
    schedule_dag->set_swap_tablet_flag(true/*schedule_minor*/);
    LOG_INFO("success to add minor dag before schedule dag", K(ret), KP(minor_exe_dag), KP(schedule_dag));
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(schedule_dag)) {
      MTL(share::ObTenantDagScheduler *)->cancel_dag(schedule_dag);
      schedule_dag = nullptr;
    }
    if (OB_NOT_NULL(minor_exe_dag)) {
      MTL(share::ObTenantDagScheduler *)->free_dag(*minor_exe_dag);
      minor_exe_dag = nullptr;
    }
  }
  return ret;
}

int ObCOMergePrepareTask::process()
{
  int ret = OB_SUCCESS;
#ifdef ERRSIM
  if (OB_NOT_NULL(dag_net_) && dag_net_->get_dag_param().tablet_id_.id() > ObTabletID::MIN_USER_TABLET_ID) {
    DEBUG_SYNC(MAJOR_MERGE_PREPARE_TASK_PROCESS);
  }
#endif
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("task is not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(NULL == dag_net_ || NULL != dag_net_->get_merge_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected dag net", K(ret), KPC(dag_net_));
  } else if (FALSE_IT(dag_net_->set_prepare_dag_running_ts())) {
  } else if (OB_FAIL(dag_net_->prepare_co_merge_ctx())) {
    LOG_WARN("failed to prepare merge ctx", K(ret), KPC(dag_net_));
  } else if (OB_FAIL(create_schedule_dag(*dag_net_->get_merge_ctx()))) {
    LOG_WARN("failed to create schedule dag", K(ret));
  }

  if (OB_FAIL(ret)) {
    // TODO(@jingshui): may change ctx->merge_log_storage_ to retry
    dag_net_->cancel_dag_net(ret);
  }
  return ret;
}

/*
 * ObCOMergeScheduleDag
 * */
int ObCOMergeScheduleDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObCOMergeScheduleTask *schedule_task = nullptr;
  if (OB_FAIL(alloc_task(schedule_task))) {
    STORAGE_LOG(WARN, "fail to alloc task", K(ret));
  } else if (OB_FAIL(schedule_task->init(*static_cast<ObCOMergeDagNet*>(get_dag_net())))) {
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
  } else if (OB_FAIL(dag_net_->create_co_execute_dag(*get_dag()))) {
    LOG_WARN("failed to create execute dags in schedule task", K(ret));
  }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(EventTable::EN_COMPACTION_CO_MERGE_SCHEDULE_FAILED) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      SERVER_EVENT_SYNC_ADD("merge_errsim", "co_merge_schedule_failed", "ret_code", ret);
      STORAGE_LOG(INFO, "ERRSIM EN_COMPACTION_CO_MERGE_SCHEDULE_FAILED", K(ret));
    }
  }
#endif
  if (OB_FAIL(ret)) {
    dag_net_->cancel_dag_net(ret);
  }
  return ret;
}

/*
 * ObCOMergeExeDag
 * */
ObCOMergeExeDag::ObCOMergeExeDag()
  : ObCOMergeDag(ObDagType::DAG_TYPE_CO_MERGE_EXECUTE),
    need_reduce_batch_(false),
    need_update_batch_size_(true),
    range_count_(0),
    cg_count_(0),
    merge_batch_size_(DEFAULT_CG_MERGE_BATCH_SIZE),
    range_status_(nullptr),
    cg_merge_status_(nullptr),
    exe_lock_(),
    merge_progress_(nullptr)
{}

ObCOMergeExeDag::~ObCOMergeExeDag()
{
  reset();
}

void ObCOMergeExeDag::reset()
{
  if (OB_NOT_NULL(range_status_)) {
    allocator_.free(range_status_);
    range_status_ = nullptr;
    cg_merge_status_ = nullptr;
  }
}

int ObCOMergeExeDag::init_by_param(const share::ObIDagInitParam *param)
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
  } else if (OB_FAIL(prepare_replay_status(merge_param->range_count_, merge_param->cg_count_))) {
    LOG_WARN("failed to prepare replay status", K(ret));
  } else if (OB_FAIL(init_merge_batch_size(merge_param->is_using_column_tmp_file_))) {
    LOG_WARN("failed to init merge batch size", K(ret));
  } else {
    compat_mode_ = merge_param->compat_mode_;
    dag_net_id_ = merge_param->dag_net_id_;
    is_inited_ = true;
  }
  return ret;
}

int ObCOMergeExeDag::get_dag_net_and_ctx(ObCOMergeDagNet *&dag_net, ObCOTabletMergeCtx *&ctx)
{
  int ret = OB_SUCCESS;
  dag_net = nullptr;
  ctx = nullptr;
  if (OB_ISNULL(dag_net = static_cast<ObCOMergeDagNet*>(get_dag_net()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net is null", K(ret));
  } else if (OB_ISNULL(ctx = static_cast<ObCOTabletMergeCtx*>(dag_net->get_merge_ctx()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is null", K(ret));
  } else if (!ctx->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is not valid", K(ret));
  }
  return ret;
}

int ObCOMergeExeDag::prepare_replay_status(const int64_t range_count, const int64_t cg_count)
{
  int ret = OB_SUCCESS;
  const int64_t alloc_size = range_count * (sizeof(CGMergeStatus) * cg_count + sizeof(RangeMergeStatus));
  void *buf = nullptr;
  if (OB_UNLIKELY(range_count <= 0 || cg_count <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected count", K(ret), K(range_count), K(cg_count));
  } else if (OB_ISNULL(buf = allocator_.alloc(alloc_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret), K(alloc_size));
  } else {
    MEMSET(buf, 0, alloc_size);
    range_status_ = static_cast<RangeMergeStatus*>(buf);
    buf = (void *)(static_cast<char *>(buf) + sizeof(RangeMergeStatus) * range_count);
    cg_merge_status_ = static_cast<CGMergeStatus*>(buf);
    range_count_ = range_count;
    cg_count_ = cg_count;
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObCOMergeExeDag::init_merge_batch_size(const bool is_using_column_tmp_file)
{
  int ret = OB_SUCCESS;
  if (is_using_column_tmp_file) {
    merge_batch_size_ = 1;
  } else if (cg_count_ < ALL_CG_IN_ONE_BATCH_CNT) {
    merge_batch_size_ = cg_count_;
  } else {
    const int64_t batch_cnt = cg_count_ / DEFAULT_CG_MERGE_BATCH_SIZE;
    merge_batch_size_ = cg_count_ / batch_cnt;
  }
  return ret;
}

void ObCOMergeExeDag::try_update_merge_batch_size()
{
  if (need_update_batch_size_) {
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
      merge_batch_size_ = MIN(merge_batch_size_ * 2, cg_count_);
      need_update_batch_size_ = false;
    }
#ifdef ERRSIM
    if (EN_COMPACTION_RANDOM_BATCH_SIZE) {
      merge_batch_size_ = ObRandom::rand(merge_batch_size_ - 1, merge_batch_size_);
      SERVER_EVENT_SYNC_ADD("merge_errsim", "random_merge_batch_size", "merge_batch_size", merge_batch_size_);
    }
#endif
    FLOG_INFO("[ADAPTIVE_SCHED] update co merge batch size", K(merge_thread),
        K(mem_allow_used), K(batch_mem_allow_per_thread), K(mem_allow_batch_size), K(merge_batch_size_));
  }
}

int64_t ObCOMergeExeDag::inner_get_replayed_cg_count() const
{
  int64_t count = 0;
  for (int64_t i = 0; i < range_count_ * cg_count_; ++i) {
    if (CG_REPLAY_FINISH <= cg_merge_status_[i]) {
      ++count;
    }
  }
  return count;
}

int64_t ObCOMergeExeDag::inner_get_persisted_range_count() const
{
  int64_t count = 0;
  for (int64_t i = 0; i < range_count_; ++i) {
    if (RANGE_PERSIST_FINISH == range_status_[i]) {
      ++count;
    }
  }
  return count;
}

bool ObCOMergeExeDag::check_replay_finished()
{
  bool bret = false;
  ObSpinLockGuard lock_guard(exe_lock_);
  bret = inner_check_replay_finished();
  return bret;
}

int ObCOMergeExeDag::get_cg_merge_status(const int64_t range_idx, const int64_t cg_idx, CGMergeStatus &status)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(range_idx < 0 || range_idx >= range_count_ ||
      cg_idx < 0 || cg_idx >= cg_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(range_idx), K(cg_idx), K(range_count_), K(cg_count_));
  } else {
    ObSpinLockGuard lock_guard(exe_lock_);
    status = cg_merge_status_[range_idx * cg_count_ + cg_idx];
  }
  return ret;
}

int ObCOMergeExeDag::set_cg_merge_status(
    const int64_t range_idx,
    const int64_t start_cg_idx,
    const int64_t end_cg_idx,
    const CGMergeStatus status)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(range_idx < 0 || range_idx >= range_count_ ||
      start_cg_idx >= end_cg_idx || end_cg_idx > cg_count_ ||
      status < CGMergeStatus::CG_NEED_REPLAY || status > CGMergeStatus::CG_SSTABLE_CREATED)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(range_idx), K(start_cg_idx), K(end_cg_idx),
                                 K(range_count_), K(cg_count_), K(status));
  } else {
    ObSpinLockGuard lock_guard(exe_lock_);
    inner_set_cg_merge_status(range_idx, start_cg_idx, end_cg_idx, status);
  }
  return ret;
}

int ObCOMergeExeDag::finish_replay(
    const int64_t range_idx,
    const int64_t start_cg_idx,
    const int64_t end_cg_idx,
    bool &all_cg_finished)
{
  int ret = OB_SUCCESS;
  ObCOMergeDagNet *dag_net = nullptr;
  ObCOTabletMergeCtx *ctx = nullptr;
  if (OB_UNLIKELY(range_idx < 0 || range_idx >= range_count_ ||
      start_cg_idx >= end_cg_idx || end_cg_idx > cg_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(range_idx), K(start_cg_idx), K(end_cg_idx), K(range_count_), K(cg_count_));
  } else if (OB_FAIL(get_dag_net_and_ctx(dag_net, ctx))) {
    LOG_WARN("failed to get dag net and ctx", K(ret));
  } else {
    ObCOMergeFinishTask *finish_task = nullptr;
    bool range_replay_finished = false;
    {
      ObSpinLockGuard lock_guard(exe_lock_);
      inner_set_cg_merge_status(range_idx, start_cg_idx, end_cg_idx, CGMergeStatus::CG_REPLAY_FINISH);
      range_replay_finished = inner_check_range_replay_finished(range_idx);
      all_cg_finished = inner_check_replay_finished();
    }
    if (ctx->is_using_row_tmp_file() && range_replay_finished) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(ctx->do_replay_finish(range_idx, 0, 1))) {
        LOG_WARN("failed to do replay finish", K(tmp_ret), K(range_idx));
      }
    }
  }
  return ret;
}

int ObCOMergeExeDag::try_generate_finish_task(ObITask *parent)
{
  int ret = OB_SUCCESS;
  ObCOMergeFinishTask *finish_task = nullptr;
  ObCOMergeDagNet *dag_net = nullptr;
#ifdef ERRSIM
  if (EN_COMPACTION_ADD_CO_MREGE_FINISH_TASK_FAILED) {
    ret = EN_COMPACTION_ADD_CO_MREGE_FINISH_TASK_FAILED;
    LOG_INFO("ERRSIM EN_COMPACTION_ADD_CO_MREGE_FINISH_TASK_FAILED", K(ret));
    SERVER_EVENT_SYNC_ADD("merge_errsim", "after_replay_add_finish_failed", "ret_code", ret);
  }
#endif
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(dag_net = static_cast<ObCOMergeDagNet*>(get_dag_net()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr dag net", K(ret));
  } else if (OB_FAIL(ObIDag::create_task(parent, finish_task, *dag_net))) {
    LOG_WARN("failed to create finish task", K(ret));
  } else {
    LOG_INFO("success to add finish task", K(ret), KPC(finish_task));
  }
  return ret;
}

int ObCOMergeExeDag::set_range_merge_status(const int64_t range_idx, const RangeMergeStatus status)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(range_idx < 0 || range_idx >= range_count_ ||
      status < RangeMergeStatus::RANGE_NEED_PERSIST || status > RangeMergeStatus::RANGE_PERSIST_FINISH)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(range_idx), K(range_count_), K(cg_count_));
  } else {
    ObSpinLockGuard lock_guard(exe_lock_);
    range_status_[range_idx] = status;
  }
  return ret;
}

int ObCOMergeExeDag::get_next_replay_cg_pair(
    const int64_t range_idx,
    ObCOTabletMergeCtx &ctx,
    int64_t &start_cg_idx,
    int64_t &end_cg_idx)
{
  int ret = OB_SUCCESS;
  start_cg_idx = 0;
  end_cg_idx = 0;
  if (0 > range_idx || range_count_ <= range_idx) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(range_idx));
  } else if (!ctx.is_using_column_tmp_file()
      && ObBasicMergeScheduler::get_merge_scheduler()->enable_adaptive_merge_schedule()
      && FALSE_IT(try_update_merge_batch_size())) {
  } else if (ctx.is_build_row_store()) {
    const CGMergeStatus &cg_merge_status = cg_merge_status_[range_idx * cg_count_ + ctx.base_rowkey_cg_idx_];
    inner_set_cg_merge_status(range_idx, 0, ctx.base_rowkey_cg_idx_, CGMergeStatus::CG_SSTABLE_CREATED);
    inner_set_cg_merge_status(range_idx, ctx.base_rowkey_cg_idx_ + 1, cg_count_, CGMergeStatus::CG_SSTABLE_CREATED);
    if (cg_merge_status == CG_NEED_REPLAY) {
      start_cg_idx = ctx.base_rowkey_cg_idx_;
      end_cg_idx = start_cg_idx + 1;
      inner_set_cg_merge_status(range_idx, start_cg_idx, end_cg_idx, CGMergeStatus::CG_IS_REPLAYING);
    } else {
      ret = OB_ITER_END;
    }
  } else {
    bool only_check_batch_size = false;
#ifdef ERRSIM
    if (EN_COMPACTION_RANDOM_BATCH_SIZE) {
      only_check_batch_size = true;
    }
#endif
    while (OB_SUCC(ret) && end_cg_idx < cg_count_ && OB_NOT_NULL(cg_merge_status_)) {
      const CGMergeStatus &cg_merge_status = cg_merge_status_[range_idx * cg_count_ + end_cg_idx];
      if (cg_merge_status == CG_NEED_REPLAY) {
        end_cg_idx++;
        if (!only_check_batch_size && cg_count_ - end_cg_idx < merge_batch_size_) { // the last batch
        } else if (start_cg_idx + merge_batch_size_ <= end_cg_idx) {
          break;
        }
      } else if (start_cg_idx == end_cg_idx) {
        end_cg_idx++;
        start_cg_idx = end_cg_idx;
      } else {
        break;
      }
    }
    if (end_cg_idx == cg_count_ && start_cg_idx == end_cg_idx) {
      ret = OB_ITER_END;
    } else {
      inner_set_cg_merge_status(range_idx, start_cg_idx, end_cg_idx, CGMergeStatus::CG_IS_REPLAYING);
    }
  }
  return ret;
}

int ObCOMergeExeDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObITask *task = nullptr;
  ObCOMergeDagNet *dag_net = nullptr;
  ObCOTabletMergeCtx *ctx = nullptr;
  if (OB_FAIL(get_dag_net_and_ctx(dag_net, ctx))) {
    LOG_WARN("failed to get dag net and ctx", K(ret));
  } else if (OB_FAIL(generate_next_task(*ctx, *dag_net, nullptr, task))) {
    LOG_WARN("failed to generate next task", K(ret));
  } else if (nullptr == task) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr task", K(ret));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("failed to add task", K(ret), KPC(task));
  } else { // fill compaction param
    // the dag_net has been set, and the dag hasn't been added to the scheduler now
    param_.compaction_param_.sstable_cnt_ = ctx->get_tables_handle().get_count();
    param_.compaction_param_.estimate_concurrent_cnt_ = ctx->get_concurrent_cnt();
    param_.compaction_param_.add_time_ = common::ObTimeUtility::fast_current_time();
    // TODO param_.compaction_param_.batch_size_
  }

  if (OB_FAIL(ret) && nullptr != task) {
    delete_task(task);
  }
  return ret;
}

// reset parent/son task relationship and remove task
void ObCOMergeExeDag::delete_task(ObITask *&task)
{
  if (nullptr != task) {
    task->reset_node();
    if (ObITask::TASK_TYPE_CO_MERGE_REPLAY == task->get_type()) {
      // revert cg status
      ObCOMergeLogReplayTask *replay_task = static_cast<ObCOMergeLogReplayTask*>(task);
      set_cg_merge_status(replay_task->range_idx_,
                          replay_task->start_cg_idx_,
                          replay_task->end_cg_idx_,
                          ObCOMergeExeDag::CG_NEED_REPLAY);
    }
    remove_task(*task);
    task = nullptr;
  }
}

int ObCOMergeExeDag::generate_next_task(
    ObCOTabletMergeCtx &ctx,
    ObCOMergeDagNet &dag_net,
    ObITask *parent_task,
    ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  next_task = nullptr;
  int64_t range_idx = 0;
  int64_t start_cg_idx = 0;
  int64_t end_cg_idx = 0;
  bool need_persist_task = false;
  bool need_replay_task = false;
  bool need_finish_task = false;
  ObCOMergeLogPersistTask *persist_task = nullptr;
  ObCOMergeLogReplayTask *replay_task = nullptr;
  ObCOMergeFinishTask *finish_task = nullptr;
#ifdef ERRSIM
  if (EN_COMPACTION_GENERATE_NEXT_FAILED) {
    ret = EN_COMPACTION_GENERATE_NEXT_FAILED;
    LOG_INFO("ERRSIM EN_COMPACTION_GENERATE_NEXT_FAILED", K(ret));
    SERVER_EVENT_SYNC_ADD("merge_errsim", "generate_next_failed", "ret_code", ret);
  }
#endif
  {
    ObSpinLockGuard lock_guard(exe_lock_);
    while (OB_SUCC(ret) && range_idx < ctx.get_concurrent_cnt()) {
      if (RANGE_NEED_PERSIST == range_status_[range_idx] && ctx.is_using_tmp_file()) {
        need_persist_task = true;
        break;
      } else if (OB_FAIL(get_next_replay_cg_pair(range_idx, ctx, start_cg_idx, end_cg_idx))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next replay cg pair", K(ret), K(range_idx));
        } else {
          ret = OB_SUCCESS;
          ++range_idx;
        }
      } else {
        need_replay_task = true;
        break;
      }
    }
    // only when is_build_row_store() + replay directly in persist task + persist task generate next task
    // then the finish task will be the persist task's child
    if (OB_SUCC(ret) && !need_persist_task && !need_replay_task) {
      need_finish_task = inner_check_replay_finished();
    }
  }
  if (OB_FAIL(ret)) {
  } else if (need_persist_task) {
    if (OB_FAIL(ObCOMergeExeDag::create_task(parent_task, persist_task, dag_net, range_idx))) {
      LOG_WARN("failed to create persist task", K(ret));
    } else {
      next_task = persist_task;
    }
  } else if (need_replay_task) {
    if (OB_FAIL(ObCOMergeExeDag::create_task(
        parent_task, replay_task, dag_net, range_idx, start_cg_idx, end_cg_idx))) {
      LOG_WARN("failed to create replay task", K(ret));
    } else {
      next_task = replay_task;
    }
  } else if (need_finish_task) {
#ifdef ERRSIM
    if (EN_COMPACTION_ADD_CO_MREGE_FINISH_TASK_FAILED) {
      ret = EN_COMPACTION_ADD_CO_MREGE_FINISH_TASK_FAILED;
      LOG_INFO("ERRSIM EN_COMPACTION_ADD_CO_MREGE_FINISH_TASK_FAILED", K(ret));
      SERVER_EVENT_SYNC_ADD("merge_errsim", "add_finish_task_failed", "ret_code", ret);
    }
#endif
    if (FAILEDx(ObCOMergeExeDag::create_task(parent_task, finish_task, dag_net))) {
      LOG_WARN("failed to create finish task", K(ret));
    } else {
      next_task = finish_task;
    }
  }
  if (OB_FAIL(ret) && nullptr != next_task) {
    delete_task(next_task);
  } else {
    LOG_INFO("success to generate next task", K(ret), KPC(next_task));
  }
  return ret;
}

int ObCOMergeExeDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, "ObCOMergeExeDag: ls_id=%ld tablet_id=%ld range_count=%ld",
          ls_id_.id(), tablet_id_.id(), range_count_))) {
    LOG_WARN("failed to fill dag key", K(ret));
  }
  return ret;
}

int ObCOMergeExeDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("co merge dag do not init", K(ret));
  } else {
    const int64_t persisted_range_count = inner_get_persisted_range_count();
    const float replayed_progress = ((float)inner_get_replayed_cg_count()) / (range_count_ * cg_count_);
    if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                  ls_id_.id(),
                                  static_cast<int64_t>(tablet_id_.id()),
                                  param_.merge_version_,
                                  range_count_,
                                  persisted_range_count,
                                  "replayed_progress", replayed_progress,
                                  "dag_net_id", dag_net_id_))) {
      LOG_WARN("failed to fill info param", K(ret));
    }
  }
  return ret;
}

int ObCOMergeExeDag::gene_compaction_info(compaction::ObTabletCompactionProgress &progress)
{
  int ret = OB_SUCCESS;
  ObCOMergeDagNet *dag_net = nullptr;
  ObCOTabletMergeCtx *ctx = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(get_dag_net_and_ctx(dag_net, ctx))) {
    LOG_WARN("failed to get dag net and ctx", K(ret));
  } else if (ObIDag::DAG_STATUS_NODE_RUNNING == get_dag_status()) {
    fill_compaction_progress(progress, *ctx, merge_progress_, 0, ctx->array_count_);
  } else {
    ret = OB_EAGAIN;
  }
  return ret;
}

int ObCOMergeExeDag::diagnose_compaction_info(compaction::ObDiagnoseTabletCompProgress &progress)
{
  int ret = OB_SUCCESS;
  ObCOMergeDagNet *dag_net = nullptr;
  ObCOTabletMergeCtx *ctx = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(get_dag_net_and_ctx(dag_net, ctx))) {
    LOG_WARN("failed to get dag net and ctx", K(ret));
  } else if (DAG_STATUS_NODE_RUNNING == get_dag_status()) { // only diagnose running dag
    fill_diagnose_compaction_progress(progress, ctx, merge_progress_, 0, ctx->array_count_);
  }
  return ret;
}

int ObCOMergeExeDag::decide_retry_strategy(const int error_code, ObDagRetryStrategy &retry_status)
{
  int ret = OB_SUCCESS;
  retry_status = DAG_CAN_RETRY;

  if (OB_TRANS_CTX_NOT_EXIST == error_code
      || OB_SERVER_OUTOF_DISK_SPACE == error_code) {
    retry_status = DAG_AND_DAG_NET_SKIP_RETRY;
  }
  return ret;
}

int ObCOMergeExeDag::inner_reset_status_for_retry()
{
  int ret = OB_SUCCESS;
  ObCOMergeDagNet *dag_net = nullptr;
  ObCOTabletMergeCtx *ctx = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(get_dag_net_and_ctx(dag_net, ctx))) {
    LOG_WARN("failed to get dag net and ctx", K(ret));
  } else if (need_reduce_batch_ && merge_batch_size_ > 1) {
    merge_batch_size_ = merge_batch_size_ >> 1;
    need_reduce_batch_ = false;
  }
  if (OB_SUCC(ret)) {
    // reset status and destroy merge info for failed replay cg
    for (int64_t i = 0; i < cg_count_; ++i) {
      bool need_retry_all = false;
      for (int64_t range_idx = 0; range_idx < range_count_; ++range_idx) {
        if (CG_REPLAY_FAILED == cg_merge_status_[range_idx * cg_count_ + i]) {
          need_retry_all = true;
          break;
        } else if (CG_IS_REPLAYING == cg_merge_status_[range_idx * cg_count_ + i]) {
          // if there is no failure, we can reuse merge info, only reset status
          (void)set_cg_merge_status(range_idx, i, i + 1, CG_NEED_REPLAY);
        }
      }
      if (need_retry_all) {
        for (int64_t range_idx = 0; range_idx < range_count_; ++range_idx) {
          (void)set_cg_merge_status(range_idx, i, i + 1, CG_NEED_REPLAY);
        }
        // destroy merge info for failed replay cg
        ctx->destroy_merge_info_array(i, i + 1, true);
      }
    }
    if (OB_FAIL(create_first_task())) {
      LOG_WARN("failed to create first task", K(ret));
    } else {
      FLOG_INFO("co merge exe dag retry", K(ret), KPC(this));
    }
  }
  return ret;
}

/*
 * ObCOMergeExeTask
 * */
int ObCOMergeExeTask::init(ObCOMergeDagNet &dag_net, const int64_t range_idx)
{
  int ret = OB_SUCCESS;
  ObCOTabletMergeCtx *ctx = nullptr;
  if (OB_FAIL(ObCOMergeTask::init(dag_net))) {
    LOG_WARN("failed to init ObCOMergeTask", K(ret));
  } else if (OB_FAIL(get_ctx_from_dag_net(dag_net, ctx))) {
    LOG_WARN("fail to get ctx from dag net", K(ret));
  } else if (0 > range_idx || range_idx > ctx->get_concurrent_cnt()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid range idx", K(ret), K(range_idx), KPC(ctx));
  } else {
    allocator_.bind_mem_ctx(ctx->mem_ctx_);
    range_idx_ = range_idx;
    is_inited_ = true;
  }
  if (OB_FAIL(ret)) {
    is_inited_ = false;
  }
  return ret;
}

/*
 * ObCOMergeLogPersistTask
 * */
ObCOMergeLogPersistTask::~ObCOMergeLogPersistTask()
{
  reset();
}

void ObCOMergeLogPersistTask::reset()
{
  if (nullptr != persister_) {
    persister_->~ObCOMergeLogPersister();
    allocator_.free(persister_);
    persister_ = nullptr;
  }
}

int ObCOMergeLogPersistTask::process()
{
  int ret = OB_SUCCESS;
  ObCOMergeExeDag *exe_dag = static_cast<ObCOMergeExeDag*>(dag_);
  ObCOTabletMergeCtx *ctx = nullptr;
  ObITask *next_task = nullptr;
  void *buf = nullptr;
  const int64_t start_time = ObClockGenerator::getClock();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("task is not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(nullptr == exe_dag || nullptr == dag_net_ || nullptr != persister_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected exe task", K(ret), K(exe_dag), K(dag_net_), KPC(persister_));
  } else if (OB_FAIL(get_ctx_from_dag_net(*dag_net_, ctx))) {
    LOG_WARN("fail to get ctx from dag net", K(ret), K(ctx));
  } else if (FALSE_IT(SET_MEM_CTX(ctx->mem_ctx_))) {
  } else if (ctx->need_replay_base_directly_ &&
      OB_FAIL(ctx->prepare_index_builder(ctx->base_rowkey_cg_idx_, ctx->base_rowkey_cg_idx_ + 1))) {
    LOG_WARN("failed to prepare merge info", K(ret));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObCOMergeLogPersister)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for persister", K(ret));
  } else if (FALSE_IT(persister_ = new (buf) ObCOMergeLogPersister(allocator_))) {
  } else if (OB_FAIL(persister_->init(*ctx, range_idx_))) {
    LOG_WARN("failed to init persister", K(ret));
  } else if (OB_FAIL(persister_->persist_merge_log())) {
    LOG_WARN("failed to persist merge log", K(ret));
  } else if (FALSE_IT(ctx->mem_ctx_.mem_click())) {
#ifdef ERRSIM
  } else if (EN_COMPACTION_PERSIST_FAILED) {
    ret = EN_COMPACTION_PERSIST_FAILED;
    LOG_INFO("ERRSIM EN_COMPACTION_PERSIST_FAILED", K(ret));
    SERVER_EVENT_SYNC_ADD("merge_errsim", "persist task failed", "ret_code", ret);
#endif
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("failed to persist merge log", K(ret), KPC(this));
    if (nullptr != ctx) {
      (void)ctx->destroy_merge_log_mgr(range_idx_);
      if (ctx->need_replay_base_directly_) {
        (void)exe_dag->set_cg_merge_status(range_idx_, ctx->base_rowkey_cg_idx_, ctx->base_rowkey_cg_idx_ + 1, ObCOMergeExeDag::CG_REPLAY_FAILED);
      }
    }
  } else {
    (void)exe_dag->set_range_merge_status(range_idx_, ObCOMergeExeDag::RANGE_PERSIST_FINISH);
    if (ctx->need_replay_base_directly_) {
      (void)exe_dag->set_cg_merge_status(range_idx_, ctx->base_rowkey_cg_idx_, ctx->base_rowkey_cg_idx_ + 1, ObCOMergeExeDag::CG_REPLAY_FINISH);
    }
    ctx->update_execute_time(ObClockGenerator::getClock() - start_time);
    LOG_INFO("succeed to persist merge log", K(ret), KPC(this), K(ctx->need_replay_base_directly_));
    // cg/range status will not revert after this
    // if failed to add next task, dag retry will do it (TODO: add retry logic)
    if (OB_FAIL(exe_dag->generate_next_task(*ctx, *dag_net_, this, next_task))) {
      LOG_WARN("failed to generate next task", K(ret));
    } else if (OB_ISNULL(next_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr next task", K(ret));
    } else if (OB_FAIL(exe_dag->add_task(*next_task))) {
      LOG_WARN("failed to add task", K(ret), KPC(next_task));
#ifdef ERRSIM
    } else if (EN_COMPACTION_PERSIST_GENERATE_NEXT_FAILED) {
      ret = EN_COMPACTION_PERSIST_GENERATE_NEXT_FAILED;
      LOG_INFO("ERRSIM EN_COMPACTION_PERSIST_GENERATE_NEXT_FAILED", K(ret));
      SERVER_EVENT_SYNC_ADD("merge_errsim", "persist generate next failed", "ret_code", ret);
#endif
    }
    if (OB_FAIL(ret) && nullptr != next_task) {
      exe_dag->delete_task(next_task);
    }
  }
  reset();
  return ret;
}

/*
 * ObCOMergeLogReplayTask
 * */
ObCOMergeLogReplayTask::~ObCOMergeLogReplayTask()
{
  reset();
}

void ObCOMergeLogReplayTask::reset()
{
  if (nullptr != replayer_) {
    replayer_->~ObCOMergeLogReplayer();
    allocator_.free(replayer_);
    replayer_ = nullptr;
  }
}

int ObCOMergeLogReplayTask::init(
    ObCOMergeDagNet &dag_net,
    const int64_t range_idx,
    const int64_t start_cg_idx,
    const int64_t end_cg_idx)
{
  int ret = OB_SUCCESS;
  ObCOTabletMergeCtx *ctx = nullptr;
  if (OB_FAIL(ObCOMergeExeTask::init(dag_net, range_idx))) {
    LOG_WARN("failed to init ObCOMergeExeTask", K(ret));
  } else if (OB_FAIL(get_ctx_from_dag_net(dag_net, ctx))) {
    LOG_WARN("fail to get ctx from dag net", K(ret));
  } else if (start_cg_idx >= end_cg_idx || end_cg_idx > ctx->array_count_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(start_cg_idx), K(end_cg_idx), K(ctx));
  } else {
    start_cg_idx_ = start_cg_idx;
    end_cg_idx_ = end_cg_idx;
    is_inited_ = true;
  }
  if (OB_FAIL(ret)) {
    is_inited_ = false;
  }
  return ret;
}

int ObCOMergeLogReplayTask::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  next_task = nullptr;
  ObCOMergeExeDag *exe_dag = static_cast<ObCOMergeExeDag*>(dag_);
  ObCOTabletMergeCtx *ctx = nullptr;
  int64_t start_cg_idx = 0;
  int64_t end_cg_idx = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("task is not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(nullptr == exe_dag || nullptr == dag_net_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx or dag_net is unexpected null", K(ret), K(exe_dag), KP_(dag_net));
  } else if (OB_FAIL(get_ctx_from_dag_net(*dag_net_, ctx))) {
    LOG_WARN("fail to get ctx from dag net", K(ret));
  } else if (OB_FAIL(exe_dag->generate_next_task(*ctx, *dag_net_, nullptr, next_task))) {
    LOG_WARN("failed to get next cg pair", K(ret));
  } else if (nullptr == next_task) {
    ret = OB_ITER_END;
  }
  if (OB_FAIL(ret) && OB_ITER_END != ret) {
    exe_dag->delete_task(next_task);
  }
  return ret;
}

int ObCOMergeLogReplayTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool all_cg_replay_finished = false;
  ObCOMergeExeDag *exe_dag = static_cast<ObCOMergeExeDag*>(dag_);
  ObCOTabletMergeCtx *ctx = nullptr;
  void *buf = nullptr;
  const int64_t start_time = ObClockGenerator::getClock();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("task is not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(nullptr == exe_dag || nullptr == dag_net_ || nullptr != replayer_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected exe task", K(ret), K(exe_dag), K(dag_net_), KPC(replayer_));
  } else if (OB_FAIL(get_ctx_from_dag_net(*dag_net_, ctx))) {
    LOG_WARN("fail to get ctx from dag net", K(ret));
  } else if (FALSE_IT(SET_MEM_CTX(ctx->mem_ctx_))) {
  } else if (OB_FAIL(ctx->prepare_index_builder(start_cg_idx_, end_cg_idx_))) {
    LOG_WARN("failed to prepare merge info", K(ret));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObCOMergeLogReplayer)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for persister", K(ret));
  } else if (FALSE_IT(replayer_ = new (buf) ObCOMergeLogReplayer(allocator_,
      ctx->static_param_, start_cg_idx_, end_cg_idx_,
      ctx->prefer_reuse_macro_block_ ? false : ctx->get_is_rebuild_column_store()))) {
  } else if (OB_FAIL(replayer_->init(*ctx, range_idx_))) {
    LOG_WARN("failed to init replayer", K(ret));
  } else if (OB_FAIL(replayer_->replay_merge_log())) {
    LOG_WARN("failed to replay merge log", K(ret));
  } else if (FALSE_IT(ctx->mem_ctx_.mem_click())) {
#ifdef ERRSIM
  } else if (EN_COMPACTION_REPLAY_FAILED) {
    ret = EN_COMPACTION_REPLAY_FAILED;
    if (start_cg_idx_ <= -ret && end_cg_idx_ > -ret) {
      LOG_INFO("ERRSIM EN_COMPACTION_REPLAY_FAILED", K(ret));
      SERVER_EVENT_SYNC_ADD("merge_errsim", "replay task failed", "ret_code", ret);
    } else {
      ret = OB_SUCCESS;
    }
#endif
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("failed to replay merge log", K(ret), KPC(this));
    if (nullptr != exe_dag) {
      (void)exe_dag->set_cg_merge_status(range_idx_, start_cg_idx_, end_cg_idx_, ObCOMergeExeDag::CG_REPLAY_FAILED);
      if (OB_ALLOCATE_MEMORY_FAILED == ret && end_cg_idx_ - start_cg_idx_ > 1) {
        exe_dag->set_need_reduce_batch();
      }
    }
  } else {
    ctx->update_execute_time(ObClockGenerator::getClock() - start_time);
    LOG_INFO("succeed to replay merge log", K(ret), KPC(this));
    if (ctx->is_using_column_tmp_file() &&
        OB_TMP_FAIL(ctx->do_replay_finish(range_idx_, start_cg_idx_, end_cg_idx_))) { // release tmp file
      LOG_WARN("failed to do replay finish", K(tmp_ret));
    }
    (void)exe_dag->finish_replay(range_idx_, start_cg_idx_, end_cg_idx_, all_cg_replay_finished);
    // cg status will not revert after this
    // if failed to generate finish task, dag retry will do it (TODO: add retry logic)
    if (all_cg_replay_finished && OB_FAIL(exe_dag->try_generate_finish_task(this))) {
      LOG_WARN("failed to generate finish task", K(ret));
    }
  }
  reset();
  return ret;
}

/*
 * ObCOMergeFinishTask
 * */
int ObCOMergeFinishTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObCOMergeExeDag *exe_dag = static_cast<ObCOMergeExeDag*>(dag_);
  ObCOTabletMergeCtx *ctx = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("task is not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(nullptr == exe_dag || nullptr == dag_net_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx or dag_net is unexpected null", K(ret), KP(exe_dag), KP(dag_net_));
  } else if (OB_FAIL(get_ctx_from_dag_net(*dag_net_, ctx))) {
    LOG_WARN("fail to get ctx from dag net", K(ret));
  } else if (!exe_dag->check_replay_finished()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("replay not finished", K(ret));
  } else if (FALSE_IT(ctx->time_guard_click(ObStorageCompactionTimeGuard::EXECUTE))) {
  } else if (FALSE_IT(SET_MEM_CTX(ctx->mem_ctx_))) {
  } else if (FALSE_IT(ctx->mem_ctx_.mem_click())) {
  } else if (OB_FAIL(create_cg_sstables())) {
    LOG_WARN("failed to create sstables", K(ret), KPC(ctx), KPC(this));
  } else if (FALSE_IT(ctx->mem_ctx_.mem_click())) {
  } else if (FALSE_IT(ctx->time_guard_click(ObStorageCompactionTimeGuard::CREATE_SSTABLE))) {
#ifdef ERRSIM
  } else if (EN_COMPACTION_UPDATE_TABLET_FAILED) {
    ret = EN_COMPACTION_UPDATE_TABLET_FAILED;
    LOG_INFO("ERRSIM EN_COMPACTION_UPDATE_TABLET_FAILED", K(ret));
    SERVER_EVENT_SYNC_ADD("merge_errsim", "update tablet failed", "ret_code", ret);
#endif
  } else if (OB_FAIL(ctx->update_tablet_after_merge())) {
    LOG_WARN("failed to create sstable and update tablet", K(ret));
  } else {
    ctx->destroy_merge_info_array(0, ctx->array_count_, true);
    if (OB_TMP_FAIL(MTL(ObTenantCompactionProgressMgr*)->update_unfinish_tablet(ctx->get_merge_version()))) {
      LOG_WARN("failed to update unfinish tablet", K(tmp_ret), K(ctx->get_merge_version()));
    }
    // ATTENTION! Critical diagnostic log, DO NOT CHANGE!!!
    FLOG_INFO("sstable merge finish", K(ret), KPC(dag_), "param", ctx->get_dag_param(), "merge_info", ctx->dag_net_merge_history_,
              "mem_peak", ctx->mem_ctx_.get_total_mem_peak(), "time_guard", ctx->info_collector_.time_guard_);
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to finish co merge", K(ret), KPC(this));
  }
  return ret;
}

int ObCOMergeFinishTask::create_cg_sstables()
{
  int ret = OB_SUCCESS;
  ObCOMergeExeDag *exe_dag = static_cast<ObCOMergeExeDag*>(dag_);
  ObCOTabletMergeCtx *ctx = nullptr;
  ObCOMergeExeDag::CGMergeStatus status;
  if (OB_FAIL(get_ctx_from_dag_net(*dag_net_, ctx))) {
    LOG_WARN("fail to get ctx from dag net", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx->array_count_; ++i) {
      if (OB_FAIL(exe_dag->get_cg_merge_status(0/*range_idx*/, i, status))) {
        LOG_WARN("failed to get cg merge status", K(ret), K(i));
      } else if (ObCOMergeExeDag::CG_REPLAY_FINISH > status) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected cg merge status", K(ret), K(i), K(status));
      } else if (ObCOMergeExeDag::CG_SSTABLE_CREATED == status) {
        // do nothing
      } else if (OB_FAIL(ctx->create_cg_sstable(i))) {
        LOG_WARN("failed to create cg sstable", K(ret), K(i));
      } else {
        (void)exe_dag->set_cg_merge_status(0/*range_idx*/, i, i + 1, ObCOMergeExeDag::CG_SSTABLE_CREATED);
        (void)ctx->collect_cg_running_info(i);
        (void)ctx->destroy_merge_info(i, true/*release_mem_flag*/);
      }
    }
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
    basic_param_(),
    tmp_allocator_("CoDagNet", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::MERGE_NORMAL_CTX_ID),
    co_merge_ctx_(nullptr),
    min_sstable_end_scn_(-1),
    prepare_dag_running_ts_(0)
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
    (void) set_dag_net_id(merge_param->dag_net_id_);
    is_inited_ = true;
  }
  return ret;
}

int ObCOMergeDagNet::start_running()
{
  int ret = OB_SUCCESS;
  ObCOMergePrepareDag *dag = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("dag net is not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(get_compat_mode())) {
    LOG_WARN("failed to prepare merge ctx", K(ret));
  } else if (OB_FAIL(create_dag(dag))) {
    LOG_WARN("failed to create preapre dag", K(ret));
  }
  return ret;
}

/*
 * ATTENTION: NEVER USE ANY LOG STREAM VARIABLES IN THIS FUNCTION.
 * clear_dag_net_ctx() will be called when finish dag net.
 * ObCOMergeDagNet is special, it will be check canceled when ls offine in ObDagNetScheduler::check_ls_compaction_dag_exist_with_cancel.
 * But dag_net is only moved into finished dag net list and delaying freed. So if log streem variables used in this function after ls offine, it will be dangerous
 */
int ObCOMergeDagNet::clear_dag_net_ctx()
{
  return ObIDagNet::clear_dag_net_ctx();
}

void ObCOMergeDagNet::cancel_dag_net(const int error_code, const bool force_cancel)
{
  if (force_cancel || !ObCOMergeScheduleDag::can_ignore_warning(error_code)) {
    LOG_WARN_RET(error_code, "cancel co dag net");
    int tmp_ret = OB_SUCCESS;
    // avoid that the canceled dag_net_ keeps an unschedule finish_dag_, which cause the dag_net_ never finish
    if (OB_TMP_FAIL(set_cancel())) { // co merge dag net do not deal with cancel, so ignore ret
      LOG_WARN_RET(tmp_ret, "failed to set dag net cancel", K(error_code));
    }
  }
}

int ObCOMergeDagNet::swap_tablet_after_minor()
{
  int ret = OB_SUCCESS;
  ObGetMergeTablesResult tmp_result;
  // ATTENTION: must reset tables_handle_ before swap_tablet !!!
  co_merge_ctx_->static_param_.tables_handle_.reset();
  if (OB_FAIL(co_merge_ctx_->swap_tablet())) {
    LOG_WARN("failed to get tablet without memtables", K(ret));
  } else if (OB_FAIL(ObPartitionMergePolicy::get_result_by_snapshot(
    *co_merge_ctx_->get_ls(),
    *co_merge_ctx_->tablet_handle_.get_obj(),
    co_merge_ctx_->get_merge_version(),
    tmp_result,
    false/*need_check_tablet*/))) {
    LOG_WARN("failed to get result by snapshot", K(ret));
  }
#ifdef ERRSIM
  DEBUG_SYNC(MAJOR_MERGE_PREPARE);
#endif
  if (FAILEDx(co_merge_ctx_->static_param_.tables_handle_.assign(tmp_result.handle_))) {
    LOG_WARN("failed to assign tables handle", K(ret), K(tmp_result));
  } else {
    LOG_INFO("success to swap tablet after minor", K(ret), K(tmp_result),
      "new_rowkey_read_info", co_merge_ctx_->static_param_.rowkey_read_info_);
  }
  return ret;
}

int ObCOMergeDagNet::create_co_execute_dag(share::ObIDag &parent_dag)
{
  int ret = OB_SUCCESS;
  ObCOMergeExeDag *dag = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("dag net is not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(nullptr == co_merge_ctx_ || !co_merge_ctx_->is_schema_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge ctx is null or schema invalid", K(ret), KPC(co_merge_ctx_));
    SET_DAG_LOCATION(&parent_dag);
  } else if (OB_FAIL(create_dag(dag,
                                &parent_dag,
                                co_merge_ctx_->get_concurrent_cnt(),
                                co_merge_ctx_->array_count_,
                                co_merge_ctx_->is_using_column_tmp_file()))) {
    LOG_WARN("failed to create co merge exe dag", K(ret), KPC(co_merge_ctx_));
  }
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
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, tmp_ls_handle, ObLSGetMod::COMPACT_MODE))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id_), K(tablet_id_));
  } else if (OB_FAIL(tmp_ls_handle.get_ls()->get_tablet_svr()->get_tablet(
          tablet_id_,
          tmp_tablet_handle,
          0/*timeout_us*/,
          storage::ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    LOG_WARN("failed to get tablet", K(ret), K(ls_id_), K(tablet_id_));
  } else if (OB_FAIL(ObTablet::check_private_transfer_epoch_equal(*tmp_tablet_handle.get_obj(), basic_param_.schedule_private_transfer_epoch_))) {
    LOG_WARN("tmp tablet private transfer epoch not eq with old private transfer epoch", K(ret),
        "tmp_tablet_meta", tmp_tablet_handle.get_obj()->get_tablet_meta(),
        "old_private_transfer_epoch", basic_param_.schedule_private_transfer_epoch_);
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
  bool finish_flag = false;
  #define NEW_CTX(CTX_NAME) \
    OB_NEWx(CTX_NAME, &tmp_allocator_, *this, basic_param_, tmp_allocator_)
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("dag net is not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(NULL != co_merge_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge ctx is unexpected null", K(ret), KPC(co_merge_ctx_));
  } else if (!MERGE_SCHEDULER_PTR->could_major_merge_start()) {
    ret = OB_CANCELED;
    LOG_INFO("Tenant Merge has been paused", K(ret), KPC(this));
  } else if (is_local_exec_mode(basic_param_.exec_mode_)) {
    co_merge_ctx_ = NEW_CTX(ObCOTabletMergeCtx);
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (is_output_exec_mode(basic_param_.exec_mode_)) {
    co_merge_ctx_ = NEW_CTX(ObSSCOTabletMergeCtx);
#endif
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid exec mode", KR(ret), K_(basic_param));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(co_merge_ctx_)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc ctx", KR(ret), K_(co_merge_ctx));
  } else if (FALSE_IT(co_merge_ctx_->init_time_guard(get_add_time()))) {
  } else if (FALSE_IT(co_merge_ctx_->time_guard_click(ObStorageCompactionTimeGuard::DAG_WAIT_TO_SCHEDULE))) {
  } else if (OB_FAIL(co_merge_ctx_->build_ctx(finish_flag))) {
    LOG_WARN("failed to build ctx", KR(ret), "param", co_merge_ctx_->get_dag_param(), KP_(co_merge_ctx));
  } else if (finish_flag) {
    ret = OB_NO_NEED_MERGE;
  } else if (OB_FAIL(co_merge_ctx_->init_merge_flag())) {
    LOG_WARN("failed to init merge flag", K(ret));
  } else if (OB_FAIL(co_merge_ctx_->prepare_two_stage_ctx())) {
    LOG_WARN("failed to prepare two stage ctx", K(ret));
  } else if (OB_FAIL(co_merge_ctx_->check_merge_ctx_valid())) {
    LOG_WARN("invalid merge ctx", KR(ret), KPC_(co_merge_ctx));
  } else if (OB_FAIL(init_min_sstable_end_scn())) {
    LOG_WARN("failed to init min sstable end scn", KR(ret), KPC_(co_merge_ctx));
  } else {
    LOG_INFO("finish prepare co merge ctx", K(ret), KPC_(co_merge_ctx));
  }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(EventTable::EN_COMPACTION_CO_MERGE_PREPARE_CTX_FAILED) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      SERVER_EVENT_SYNC_ADD("merge_errsim", "co_merge_prepare_ctx_failed", "ret_code", ret);
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

  if (OB_FAIL(databuff_printf(buf, buf_len, "COMergeDagNet: ls_id=%ld tablet_id=%ld merge_type=%s",
          ls_id_.id(), tablet_id_.id(), merge_type))) {
    LOG_WARN("failed to fill comment", K(ret));
  }
  return ret;
}

int ObCOMergeDagNet::fill_comment(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (nullptr != co_merge_ctx_) {
    if (OB_FAIL(databuff_printf(buf, buf_len, "replay_base_directly=%d merge_log_storage=%d",
            co_merge_ctx_->need_replay_base_directly_, co_merge_ctx_->merge_log_storage_))) {
      LOG_WARN("failed to fill comment", K(ret));
    }
  }
  return ret;
}

int ObCOMergeDagNet::get_min_sstable_end_scn(SCN &min_end_scn)
{
  int ret = OB_SUCCESS;
  if (min_sstable_end_scn_ > 0) {
    if (OB_FAIL(min_end_scn.convert_for_tx(min_sstable_end_scn_))) {
      LOG_WARN("failed to convert for tx", K(ret), K(min_sstable_end_scn_));
    }
  }
  return ret;
}

int ObCOMergeDagNet::init_min_sstable_end_scn()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(co_merge_ctx_)) {
    // if table_array is empty, means have not get tablet yet
    const ObTablesHandleArray &table_array = co_merge_ctx_->get_tables_handle();
    for (int64_t idx = 0; OB_SUCC(ret) && idx < table_array.get_count(); ++idx) {
      ObSSTable *sstable = nullptr;
      if (OB_UNLIKELY(nullptr == table_array.get_table(idx)
        || nullptr == (sstable = static_cast<ObSSTable *>(table_array.get_table(idx))))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("nullptr in sstable array", KR(ret));
      } else if (sstable->is_multi_version_minor_sstable()) {
        min_sstable_end_scn_ = sstable->get_end_scn().get_val_for_tx();
        break;
      }
    } // for
  }
  return ret;
}

} // namespace compaction
} // namespace oceanbase
