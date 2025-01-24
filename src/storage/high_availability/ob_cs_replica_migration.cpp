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
#include "storage/high_availability/ob_cs_replica_migration.h"
#include "storage/column_store/ob_column_store_replica_util.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"

namespace oceanbase
{
namespace storage
{
ERRSIM_POINT_DEF(EN_ALL_STATE_DETERMINISTIC_FALSE);
ERRSIM_POINT_DEF(EN_DISABLE_WAITING_CONVERT_CO_WHEN_MIGRATION);

/*----------------------------- ObTabletCOConvertCtx -----------------------------*/
ObTabletCOConvertCtx::ObTabletCOConvertCtx()
  : tablet_id_(),
    co_dag_net_id_(),
    status_(Status::MAX_STATUS),
    retry_cnt_(0),
    eagain_cnt_(0),
    is_inited_(false)
{
}

ObTabletCOConvertCtx::~ObTabletCOConvertCtx()
{
  reset();
}

int ObTabletCOConvertCtx::init(
    const ObTabletID &tablet_id,
    const share::ObDagId &co_dag_net_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!tablet_id.is_valid() || !co_dag_net_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet_id ro co dag net id is invalid", K(ret), K(tablet_id), K(co_dag_net_id));
  } else {
    tablet_id_ = tablet_id;
    co_dag_net_id_ = co_dag_net_id;
    status_ = Status::UNKNOWN;
    is_inited_ = true;
  }
  return ret;
}

void ObTabletCOConvertCtx::reset()
{
  tablet_id_.reset();
  co_dag_net_id_.reset();
  status_ = Status::MAX_STATUS;
  retry_cnt_ = 0;
  eagain_cnt_ = 0;
  is_inited_ = false;
}

bool ObTabletCOConvertCtx::is_valid() const
{
  return tablet_id_.is_valid()
      && co_dag_net_id_.is_valid()
      && status_ >= Status::UNKNOWN
      && status_ < Status::MAX_STATUS
      && retry_cnt_ >= 0
      && eagain_cnt_ >= 0
      && is_inited_;
}

void ObTabletCOConvertCtx::set_progressing()
{
  int ret = OB_SUCCESS;
  if (status_ != Status::RETRY_EXHAUSTED) {
    status_ = Status::PROGRESSING;
  }
}

/*----------------------------- ObHATabletGroupCOConvertCtx -----------------------------*/
ObHATabletGroupCOConvertCtx::ObHATabletGroupCOConvertCtx()
  : ObHATabletGroupCtx(TabletGroupCtxType::CS_REPLICA_TYPE),
    finish_migration_cnt_(0),
    finish_check_cnt_(0),
    retry_exhausted_cnt_(0),
    idx_map_(),
    convert_ctxs_()
{
}

ObHATabletGroupCOConvertCtx::~ObHATabletGroupCOConvertCtx()
{
  common::SpinWLockGuard guard(lock_);
  if (idx_map_.created()) {
    idx_map_.destroy();
  }
}

void ObHATabletGroupCOConvertCtx::reuse()
{
  common::SpinWLockGuard guard(lock_);
  inner_reuse();
  ObHATabletGroupCtx::inner_reuse();
}

void ObHATabletGroupCOConvertCtx::inner_reuse()
{
  finish_migration_cnt_ = 0;
  finish_check_cnt_ = 0;
  retry_exhausted_cnt_ = 0;
  if (idx_map_.created()) {
    idx_map_.destroy();
  }
  convert_ctxs_.reuse();
}

int ObHATabletGroupCOConvertCtx::inner_init()
{
  int ret = OB_SUCCESS;
  finish_migration_cnt_ = 0;
  finish_check_cnt_ = 0;
  retry_exhausted_cnt_ = 0;
  const int64_t count = tablet_id_array_.count();
  if (OB_UNLIKELY(idx_map_.created() || !convert_ctxs_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("idx map is created or convert ctxs not empty", K(ret), KPC(this));
  } else if (OB_FAIL(idx_map_.create(TABLET_CONVERT_CTX_MAP_BUCKED_NUM, ObMemAttr(MTL_ID(), "HATGCOCtx")))) {
    LOG_WARN("failed to create tablet convert ctx idx map", K(ret));
  } else if (OB_FAIL(convert_ctxs_.reserve(count))) {
    LOG_WARN("failed to reserve convert ctxs", K(ret), K(count));
  } else {
    for (int64_t idx = 0; OB_SUCC(ret) && idx < count; ++idx) {
      const ObTabletID &tablet_id = tablet_id_array_.at(idx).tablet_id_;
      ObDagId co_dag_net_id;
      co_dag_net_id.init(GCTX.self_addr());
      ObTabletCOConvertCtx co_convert_ctx;
      if (OB_FAIL(co_convert_ctx.init(tablet_id, co_dag_net_id))) {
        LOG_WARN("failed to init co convert ctx", K(ret), K(tablet_id), K(co_dag_net_id));
      } else if (OB_FAIL(convert_ctxs_.push_back(co_convert_ctx))) {
        LOG_WARN("failed to push back co convert ctx", K(ret), K(co_convert_ctx));
      } else if (OB_FAIL(idx_map_.set_refactored(tablet_id, idx))) {
        LOG_WARN("failed to set ctx idx into map", K(ret), K(tablet_id), K(idx));
      }
    }
  }
  return ret;
}

void ObHATabletGroupCOConvertCtx::inc_finish_migration_cnt()
{
  common::SpinWLockGuard guard(lock_);
  finish_migration_cnt_++;
}

bool ObHATabletGroupCOConvertCtx::ready_to_check() const
{
  common::SpinRLockGuard guard(lock_);
  bool bret = false;
  if (finish_migration_cnt_ < convert_ctxs_.count()) {
  } else if (finish_migration_cnt_ > convert_ctxs_.count()) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "invalid finish migration cnt", KPC(this));
  } else {
    int ret = OB_SUCCESS; // ignore ret
    for (int64_t idx = 0; OB_SUCC(ret) && idx < tablet_id_array_.count(); ++idx) {
      const ObTabletID &tablet_id = tablet_id_array_[idx].tablet_id_;
      bool is_exist = true;
      int64_t ctx_idx = 0;
      if (OB_FAIL(inner_get_valid_convert_ctx_idx(tablet_id, ctx_idx))) {
        LOG_WARN("failed to get convert ctx idx", K(ret), K(tablet_id));
      } else if (convert_ctxs_[ctx_idx].is_progressing()) {
        if (OB_FAIL(MTL(ObTenantDagScheduler *)->check_dag_net_exist(convert_ctxs_[ctx_idx].co_dag_net_id_, is_exist))) {
          LOG_WARN("failed to check dag exists", K(ret), K(tablet_id), K(convert_ctxs_[ctx_idx]));
        } else if (!is_exist) {
          bret = true;
          break;
        }
      }
    }
  }
  LOG_TRACE("[CS-Replica] check ready to check", K(bret), KPC(this));
  return bret;
}

bool ObHATabletGroupCOConvertCtx::is_all_state_deterministic() const
{
  common::SpinRLockGuard guard(lock_);
  return convert_ctxs_.count() <= (finish_check_cnt_ + retry_exhausted_cnt_);
}

int ObHATabletGroupCOConvertCtx::set_convert_status(const ObTabletID &tablet_id, const ObTabletCOConvertCtx::Status status)
{
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  common::SpinWLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(inner_get_valid_convert_ctx_idx(tablet_id, idx))) {
    LOG_WARN("failed to get convert ctx idx", K(ret), K(tablet_id));
  } else if (ObTabletCOConvertCtx::Status::FINISHED == status) {
    inner_set_convert_finish(convert_ctxs_[idx]);
  } else if (ObTabletCOConvertCtx::Status::RETRY_EXHAUSTED == status) {
    inner_set_retry_exhausted(convert_ctxs_[idx]);
  } else if (ObTabletCOConvertCtx::Status::PROGRESSING == status) {
    convert_ctxs_[idx].set_progressing();
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid status to set", K(ret), K(tablet_id), K(status), K(convert_ctxs_[idx]));
  }
  return ret;
}

int ObHATabletGroupCOConvertCtx::get_co_dag_net_id(const ObTabletID &tablet_id, share::ObDagId &co_dag_net_id) const
{
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  common::SpinRLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(inner_get_valid_convert_ctx_idx(tablet_id, idx))) {
    LOG_WARN("failed to get convert ctx idx", K(ret), K(tablet_id));
  } else {
    co_dag_net_id = convert_ctxs_[idx].co_dag_net_id_;
  }
  return ret;
}

int ObHATabletGroupCOConvertCtx::check_and_schedule(ObLS &ls)
{
  common::SpinWLockGuard guard(lock_);
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    const int64_t count = tablet_id_array_.count();
    for (int64_t idx = 0; idx < count; ++idx) {
      if (OB_TMP_FAIL(inner_check_and_schedule(ls, tablet_id_array_[idx].tablet_id_))) {
        LOG_WARN("failed to check and schedule", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObHATabletGroupCOConvertCtx::check_need_convert(const ObTablet &tablet, bool &need_convert)
{
  int ret = OB_SUCCESS;
  need_convert = false;
  common::ObArenaAllocator tmp_allocator; // for schema_on_tablet
  ObStorageSchema *schema_on_tablet = nullptr;
  if (0 == tablet.get_last_major_snapshot_version()) {
    // no major, may be doing ddl, do not need to convert
  } else if (OB_FAIL(tablet.load_storage_schema(tmp_allocator, schema_on_tablet))) {
    LOG_WARN("failed to load storage schema", K(ret),K(tablet));
  } else {
    need_convert = ObCSReplicaUtil::check_need_convert_cs_when_migration(tablet, *schema_on_tablet);
  }

  if (OB_NOT_NULL(schema_on_tablet)) {
    schema_on_tablet->~ObStorageSchema();
    tmp_allocator.free(schema_on_tablet);
    schema_on_tablet = nullptr;
  }
  return ret;
}

int ObHATabletGroupCOConvertCtx::update_deleted_data_tablet_status(
    ObHATabletGroupCtx *tablet_group_ctx,
    const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObHATabletGroupCOConvertCtx *group_convert_ctx = nullptr;
  if (OB_UNLIKELY(OB_ISNULL(tablet_group_ctx)
      || !tablet_group_ctx->is_cs_replica_ctx())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet group ctx", K(ret), K(tablet_id), KPC(tablet_group_ctx));
  } else if (FALSE_IT(group_convert_ctx = static_cast<ObHATabletGroupCOConvertCtx *>(tablet_group_ctx))) {
  } else if (OB_FAIL(group_convert_ctx->set_convert_finsih(tablet_id))) {
    LOG_WARN("failed to set convert finish", K(ret), K(tablet_id));
  } else {
    (void) group_convert_ctx->inc_finish_migration_cnt();
  }
  return ret;
}

void ObHATabletGroupCOConvertCtx::inner_set_convert_finish(ObTabletCOConvertCtx &convert_ctx)
{
  convert_ctx.set_finished();
  finish_check_cnt_++;
}

void ObHATabletGroupCOConvertCtx::inner_set_retry_exhausted(ObTabletCOConvertCtx &convert_ctx)
{
  convert_ctx.set_retry_exhausted();
  retry_exhausted_cnt_++;
}

int ObHATabletGroupCOConvertCtx::inner_get_valid_convert_ctx_idx(const ObTabletID &tablet_id, int64_t &idx) const
{
  int ret = OB_SUCCESS;
  idx = 0;
  if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet_id", K(ret), K(tablet_id));
  } else if (OB_FAIL(idx_map_.get_refactored(tablet_id, idx))) {
    LOG_WARN("failed to get convert ctx idx", K(ret), K(tablet_id));
  } else if (OB_UNLIKELY(idx < 0 || idx >= convert_ctxs_.count())) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("convert ctx idx is invalid", K(ret), K(idx), K(tablet_id));
  } else if (OB_UNLIKELY(!convert_ctxs_[idx].is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("convert ctx is null or invalid", K(ret), K(convert_ctxs_[idx]));
  }
  return ret;
}

int ObHATabletGroupCOConvertCtx::inner_check_and_schedule(ObLS &ls, const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  int schedule_ret = OB_SUCCESS;
  int64_t idx = 0;
  const ObLSID &ls_id = ls.get_ls_id();
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  bool need_convert = false;
  bool is_dag_net_exist = false;

  if (OB_FAIL(inner_get_valid_convert_ctx_idx(tablet_id, idx))) {
    LOG_WARN("failed to get convert ctx idx", K(ret), K(ls_id), K(tablet_id));
  } else if (convert_ctxs_[idx].is_finished() || convert_ctxs_[idx].is_retry_exhausted()) {
  } else if (OB_FAIL(ls.get_tablet(tablet_id, tablet_handle))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      LOG_INFO("tablet maybe deleted, skip it", K(ret), K(ls_id), K(tablet_id));
      inner_set_convert_finish(convert_ctxs_[idx]);
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get tablet handle", K(ret), K(ls_id), K(tablet_id));
    }
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be null", K(ret), K(ls_id), K(tablet_id));
  } else if (convert_ctxs_[idx].is_unknown()) {
    // tablet in cs replcia != tablet need convert to column store, need take storage schema into consideration
    if (OB_FAIL(check_need_convert(*tablet, need_convert))) {
      LOG_WARN("failed to check need convert", K(ret), K(ls_id), K(tablet_id));
    } else if (need_convert) {
      if (tablet->get_tablet_meta().ha_status_.is_data_status_complete()) {
        convert_ctxs_[idx].set_progressing();
      }
    } else {
      inner_set_convert_finish(convert_ctxs_[idx]);
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!convert_ctxs_[idx].is_progressing()) {
  } else if (OB_ISNULL(tablet)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("co_convert_ctx is progressing, but tablet is nullptr", K(ret), K(tablet_id), K(ls_id), K(need_convert), K(idx), K(convert_ctxs_[idx]), KPC(this));
  } else if (!tablet->is_row_store()) {
    inner_set_convert_finish(convert_ctxs_[idx]);
    LOG_INFO("[CS-Replica] Finish co merge dag net for switching column store", K(ls_id), K(tablet_id), K(tablet_handle));
  } else if (OB_FAIL(MTL(ObTenantDagScheduler *)->check_dag_net_exist(convert_ctxs_[idx].co_dag_net_id_, is_dag_net_exist))) {
    LOG_WARN("failed to check dag exists", K(ret), K(convert_ctxs_[idx]), K(tablet_id));
  } else if (is_dag_net_exist) {
  } else if (OB_FAIL(compaction::ObTenantTabletScheduler::schedule_convert_co_merge_dag_net(ls_id, *tablet, convert_ctxs_[idx].retry_cnt_, convert_ctxs_[idx].co_dag_net_id_, schedule_ret))) {
    LOG_WARN("failed to schedule convert co merge", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_EAGAIN == schedule_ret && !convert_ctxs_[idx].is_eagain_exhausted()) {
    if (REACH_THREAD_TIME_INTERVAL(10 * 60 * 1000 * 1000L /*10min*/)) {
      LOG_INFO("[CS-Replica] convert co merge is doing now, please wait for a while, or set EN_DISABLE_WAITING_CONVERT_CO_WHEN_MIGRATION tracepoint to skip it",
        K(schedule_ret), K(ls_id), K(tablet_id), K(convert_ctxs_[idx]));
    }
    convert_ctxs_[idx].inc_eagain_cnt();
  } else {
    convert_ctxs_[idx].inc_retry_cnt();
    if (convert_ctxs_[idx].is_retry_exhausted()) {
      inner_set_retry_exhausted(convert_ctxs_[idx]);
#ifdef ERRSIM
      LOG_INFO("[CS-Replica] set tablet co convert retry exhausted", K(ret), K(idx), K(tablet_id));
      DEBUG_SYNC(AFTER_SET_CO_CONVERT_RETRY_EXHUASTED);
#endif
    }
  }

  return ret;
}

/*----------------------------- ObDataTabletsCheckCOConvertDag -----------------------------*/
ObDataTabletsCheckCOConvertDag::ObDataTabletsCheckCOConvertDag()
  : ObMigrationDag(ObDagType::DAG_TYPE_TABLET_CHECK_CONVERT),
    ls_(nullptr),
    first_start_time_(0),
    is_inited_(false)
{
}

ObDataTabletsCheckCOConvertDag::~ObDataTabletsCheckCOConvertDag()
{
}

bool ObDataTabletsCheckCOConvertDag::check_can_schedule()
{
  int ret = OB_SUCCESS;
  bool bret = false;
  ObMigrationCtx *migration_ctx = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(ha_dag_net_ctx_) || OB_UNLIKELY(ObIHADagNetCtx::LS_MIGRATION != ha_dag_net_ctx_->get_dag_net_ctx_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ha dag net ctx is null or unexpected type", K(ret), KPC(ha_dag_net_ctx_));
  } else if (OB_ISNULL(migration_ctx = get_migration_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("migration ctx is null", K(ret));
  } else if (OB_FAIL(inner_check_can_schedule(*migration_ctx, bret))) {
    LOG_WARN("failed to check can schedule", K(ret));
  }
  if (OB_FAIL(ret)) {
    bret = true;
    LOG_INFO("failed to check can schedule, allow dag running", K(ret), KPC_(ha_dag_net_ctx));
  }
  return bret;
}

int ObDataTabletsCheckCOConvertDag::inner_check_can_schedule(
    ObMigrationCtx &migration_ctx,
    bool &can_schedule)
{
  int ret  = OB_SUCCESS;
  bool all_state_deterministic = true; // all finish check or retry exhausted
  ObCheckScheduleReason reason = ObCheckScheduleReason::MAX_NOT_SCHEDULE;
  // time for diagnose. if dag has not be scheduled, start_time_ is 0
  first_start_time_ = (0 == first_start_time_) ? start_time_ : first_start_time_;
  const int64_t current_time = ObTimeUtility::current_time();
  const int64_t wait_one_round_time = (0 == start_time_) ? current_time - add_time_ : current_time - start_time_;
  const int64_t total_wait_time = current_time - first_start_time_;

  if (migration_ctx.is_failed()) {
    // migration dag net failed, no need to check anymore
    can_schedule = true;
    reason = ObCheckScheduleReason::MIGRATION_FAILED;
#ifdef ERRSIM
    LOG_INFO("migration dag net failed, make check dag schedule");
#endif
  } else if (EN_DISABLE_WAITING_CONVERT_CO_WHEN_MIGRATION) {
    can_schedule = true;
    reason = ObCheckScheduleReason::CONVERT_DISABLED;
    FLOG_INFO("[CS-Replica] schedule check convert dag right now since waiting convert is disabled", K(ret), K(reason), K(migration_ctx.tablet_group_mgr_));
  } else {
    const int64_t tablet_group_cnt = migration_ctx.tablet_group_mgr_.get_tablet_group_ctx_count();
    ObHATabletGroupCtx *ctx = nullptr;
    ObHATabletGroupCOConvertCtx *group_convert_ctx = nullptr;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < tablet_group_cnt; ++idx) {
      if (OB_FAIL(migration_ctx.tablet_group_mgr_.get_tablet_group_ctx(idx, ctx))) {
        LOG_WARN("failed to get tablet group ctx", K(ret), K(idx));
      } else if (OB_ISNULL(ctx) || OB_UNLIKELY(!ctx->is_cs_replica_ctx())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ctx is null or invalid type", K(ret), KPC(ctx), K(migration_ctx));
      } else if (FALSE_IT(group_convert_ctx = static_cast<ObHATabletGroupCOConvertCtx *>(ctx))) {
      } else if (group_convert_ctx->is_all_state_deterministic()) {
      } else if (FALSE_IT(all_state_deterministic = false)) {
      } else if (group_convert_ctx->ready_to_check()) {
        can_schedule = true;
        reason = ObCheckScheduleReason::READY_TO_CHECK;
        break;
      }
    }
  }

  if (!can_schedule) {
    if (all_state_deterministic) {
      can_schedule = true;
      reason = ObCheckScheduleReason::ALL_DETERMINISTIC;
    } else if (wait_one_round_time > OB_DATA_TABLETS_NOT_CHECK_CONVERT_THRESHOLD) {
      can_schedule = true;
      reason = ObCheckScheduleReason::WAIT_TIME_EXCEED;
    }
  }

  const int64_t cost_time = ObTimeUtility::current_time() - current_time;
  if (REACH_THREAD_TIME_INTERVAL(OB_DATA_TABLETS_NOT_CHECK_CONVERT_THRESHOLD)) {
    LOG_INFO("[CS-Replica] finish check_can_schedule", K(ret), K(can_schedule), K(reason), K(wait_one_round_time), K(total_wait_time), K(cost_time), K(migration_ctx.tablet_group_mgr_));
  } else {
    LOG_TRACE("[CS-Replica] finish check_can_schedule", K(ret), K(can_schedule), K(reason), K(wait_one_round_time), K(total_wait_time), K(cost_time), K(migration_ctx.tablet_group_mgr_));
  }
  return ret;
}

int ObDataTabletsCheckCOConvertDag::init(
    ObIHADagNetCtx *ha_dag_net_ctx,
    ObLS *ls)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("can not init twice", K(ret));
  } else if (OB_UNLIKELY(OB_ISNULL(ha_dag_net_ctx) || OB_ISNULL(ls))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument to init ObDataTabletsCheckCOConvertDag",
              K(ret), KPC(ha_dag_net_ctx), KPC(ls));
  } else if (OB_FAIL(check_convert_ctx_valid(ha_dag_net_ctx))) {
    LOG_WARN("ha dag net ctx is invalid", K(ret), KPC(ha_dag_net_ctx));
  } else {
    ha_dag_net_ctx_ = ha_dag_net_ctx;
    ls_ = ls;
    is_inited_ = true;
  }
  return ret;
}

int ObDataTabletsCheckCOConvertDag::check_convert_ctx_valid(ObIHADagNetCtx *ha_dag_net_ctx)
{
  int ret = OB_SUCCESS;
  ObMigrationCtx *migration_ctx = nullptr;
  if (OB_UNLIKELY(OB_ISNULL(ha_dag_net_ctx)
               || ObIHADagNetCtx::DagNetCtxType::LS_MIGRATION != ha_dag_net_ctx->get_dag_net_ctx_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ha_dag_net_ctx", K(ret), KPC(ha_dag_net_ctx));
  } else if (OB_ISNULL(migration_ctx = static_cast<ObMigrationCtx *>(ha_dag_net_ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("migration ctx should not be NULL", K(ret), KP(migration_ctx));
  } else {
    const int64_t count = migration_ctx->tablet_group_mgr_.get_tablet_group_ctx_count();
    ObHATabletGroupCtx *ctx = nullptr;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < count; ++idx) {
      if (OB_FAIL(migration_ctx->tablet_group_mgr_.get_tablet_group_ctx(idx, ctx))) {
        LOG_WARN("failed to get tablet group ctx", K(ret), K(idx));
      } else if (OB_ISNULL(ctx) || OB_UNLIKELY(!ctx->is_cs_replica_ctx())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ctx is null or not cs replica ctx", K(ret), KPC(ctx));
      }
    }
  }
  return ret;
}

int ObDataTabletsCheckCOConvertDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObDataTabletsCheckConvertTask *task = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet check convert dag not init", K(ret));
  } else if (OB_FAIL(create_task(nullptr /*parent*/, task, ha_dag_net_ctx_, ls_))) {
    LOG_WARN("failed to create tablet check convert task", K(ret));
  } else {
    LOG_INFO("[CS-Replica] Success to create tablet check convert task", K(ret), KPC(this), KPC(task));
  }
  return ret;
}

int ObDataTabletsCheckCOConvertDag::report_result()
{
  int ret = OB_SUCCESS;
  if (OB_EAGAIN == dag_ret_) {
    // ignore waiting convert co error code, prevent migration dag net retry
  } else if (OB_FAIL(ObStorageHADag::report_result())) {
    LOG_WARN("failed to report result", K(ret), KPC(this));
  }
  return ret;
}

bool ObDataTabletsCheckCOConvertDag::operator == (const ObIDag &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (get_type() != other.get_type() || ObDagType::DAG_TYPE_TABLET_CHECK_CONVERT != other.get_type()) {
    is_same = false;
  } else {
    const ObDataTabletsCheckCOConvertDag &other_dag = static_cast<const ObDataTabletsCheckCOConvertDag &>(other);
    ObMigrationCtx *ctx = get_migration_ctx();
    if (OB_ISNULL(ctx) || OB_ISNULL(other_dag.get_migration_ctx())) {
      is_same = false;
    } else {
      is_same = ctx->arg_.ls_id_ == other_dag.get_migration_ctx()->arg_.ls_id_;
    }
  }
  return is_same;
}

int64_t ObDataTabletsCheckCOConvertDag::hash() const
{
  int64_t hash_value = 0;
  ObMigrationCtx *ctx = nullptr;
  if (IS_NOT_INIT) {
    LOG_ERROR_RET(OB_NOT_INIT, "tablet check convert dag not init");
  } else if (OB_ISNULL(ctx = get_migration_ctx())) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "migration ctx should not be NULL", KP(ctx));
  } else {
    hash_value = common::murmurhash(&ctx->arg_.ls_id_, sizeof(ctx->arg_.ls_id_), hash_value);
    ObDagType::ObDagTypeEnum dag_type = get_type();
    hash_value = common::murmurhash(&dag_type, sizeof(dag_type), hash_value);
  }
  return hash_value;
}

int ObDataTabletsCheckCOConvertDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObMigrationCtx *ctx = nullptr;
  ObCStringHelper helper;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet check convert dag not init", K(ret));
  } else if (OB_ISNULL(ctx = get_migration_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("migration ctx should not be NULL", K(ret), KP(ctx));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
       "ObDataTabletsCheckCOConvertDag: ls_id = %s, migration_type = %s, dag_prio = %s",
       helper.convert(ctx->arg_.ls_id_), ObMigrationOpType::get_str(ctx->arg_.type_),
       ObIDag::get_dag_prio_str(this->get_priority())))) {
    LOG_WARN("failed to fill comment", K(ret), KPC(ctx));
  }
  return ret;
}

/*----------------------------- ObDataTabletsCheckConvertTask -----------------------------*/
ObDataTabletsCheckConvertTask::ObDataTabletsCheckConvertTask()
  : ObITask(ObITask::TASK_TYPE_CHECK_CONVERT_TABLET),
    is_inited_(false),
    ctx_(nullptr),
    ls_(nullptr)
{}

ObDataTabletsCheckConvertTask::~ObDataTabletsCheckConvertTask()
{}

int ObDataTabletsCheckConvertTask::init(
    ObIHADagNetCtx *ha_dag_net_ctx,
    ObLS *ls)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("can not init twice", K(ret));
  } else if (OB_UNLIKELY(OB_ISNULL(ha_dag_net_ctx) || OB_ISNULL(ls)
          || ObIHADagNetCtx::LS_MIGRATION != ha_dag_net_ctx->get_dag_net_ctx_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument to init ObDataTabletsCheckConvertTask",
              K(ret), KPC(ha_dag_net_ctx), KPC(ls));
  } else {
    ctx_ = static_cast<ObMigrationCtx *>(ha_dag_net_ctx);
    ls_ = ls;
    is_inited_ = true;
  }
  return ret;
}

int ObDataTabletsCheckConvertTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool all_state_deterministic = true;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("task not init", K(ret));
  } else if (ctx_->is_failed()) {
    // do nothing
  } else if (OB_ISNULL(ls_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is unexpected null", K(ret), KPC_(ls));
  } else {
    const int64_t count = ctx_->tablet_group_mgr_.get_tablet_group_ctx_count();
    ObHATabletGroupCtx *group_ctx = nullptr;
    ObHATabletGroupCOConvertCtx *group_convert_ctx = nullptr;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < count; ++idx) {
      if (OB_FAIL(ctx_->tablet_group_mgr_.get_tablet_group_ctx(idx, group_ctx))) {
        LOG_WARN("failed to get tablet group ctx", K(ret), K(idx));
      } else if (OB_ISNULL(group_ctx) || OB_UNLIKELY(!group_ctx->is_cs_replica_ctx())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("group_ctx is null or not cs replica ctx", K(ret), KPC(group_ctx));
      } else if (FALSE_IT(group_convert_ctx = static_cast<ObHATabletGroupCOConvertCtx *>(group_ctx))) {
      } else if (OB_TMP_FAIL(group_convert_ctx->check_and_schedule(*ls_))) {
        LOG_WARN("failed to check and schedule", K(tmp_ret), KPC(group_convert_ctx));
      } else if (group_convert_ctx->is_all_state_deterministic()) {
#ifdef ERRSIM
        if (EN_ALL_STATE_DETERMINISTIC_FALSE) {
          all_state_deterministic = false;
          LOG_INFO("ERRSIM EN_ALL_STATE_DETERMINISTIC_FALSE make new round check", K(ret), K(all_state_deterministic));
        }
#endif
      } else {
        all_state_deterministic = false;
      }
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_TMP_FAIL(ObStorageHADagUtils::deal_with_fo(ret, this->get_dag(), true /*alllow_retry*/))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret), KPC_(ctx));
    }
  } else if (ctx_->is_failed()) {
#ifdef ERRSIM
    LOG_INFO("migration dag net failed, make check dag exit");
#endif
  } else if (!all_state_deterministic) {
    if (EN_DISABLE_WAITING_CONVERT_CO_WHEN_MIGRATION) {
      FLOG_INFO("[CS-Replica] stop waiting convert co when migration if there are too many tablets", K(ret));
    } else {
      ret = OB_EAGAIN;
      LOG_WARN("not wait all tablets convert finish, failed this task, make dag retry", K(ret), K(all_state_deterministic), KPC_(ctx));
    }
  }
  LOG_TRACE("[CS-Replica] Finish process check data tablets convert to column store", K(ret), KPC_(ls), KPC_(ctx));
  return ret;
}

} // namespace storage
} // namespace oceanbase
