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
#include "ob_tenant_freeze_info_mgr.h"
#include "share/ob_zone_merge_info.h"
#include "share/ob_global_merge_table_operator.h"
#include "share/ob_zone_merge_table_operator.h"
#include "storage/compaction/ob_compaction_schedule_util.h"
#include "storage/concurrency_control/ob_multi_version_garbage_collector.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx_storage/ob_tenant_freezer.h"
#include "storage/meta_store/ob_server_storage_meta_service.h"
#include "storage/compaction/ob_compaction_schedule_util.h"

namespace oceanbase
{

using namespace common;
using namespace share;
using namespace share::schema;

using common::hash::ObHashSet;

namespace storage
{
const char *ObStorageSnapshotInfo::ObSnapShotTypeStr[] = {
    "UNDO_RETENTION",
    "SNAPSHOT_FOR_TX",
    "MAJOR_FREEZE_TS",
    "MULTI_VERSION_START_ON_TABLET",
    "SNAPSHOT_ON_TABLET",
    "LS_RESERVED",
    "MIN_MEDIUM",
    "SPLIT"
};

ObStorageSnapshotInfo::ObStorageSnapshotInfo()
  : snapshot_type_(SNAPSHOT_MAX),
    snapshot_(0)
{
  STATIC_ASSERT(SNAPSHOT_MAX - share::ObSnapShotType::MAX_SNAPSHOT_TYPE == ARRAYSIZEOF(ObSnapShotTypeStr), "snapshot type len is mismatch");
}

const char * ObStorageSnapshotInfo::get_snapshot_type_str() const
{
  const char * str = nullptr;
  if (OB_UNLIKELY(snapshot_type_ >= SNAPSHOT_MAX)) {
    str = "invalid_snapshot_type";
  } else if (snapshot_type_ < ObSnapShotType::MAX_SNAPSHOT_TYPE) {
    str = ObSnapshotInfo::get_snapshot_type_str((ObSnapShotType)snapshot_type_);
  } else {
    str = ObSnapShotTypeStr[snapshot_type_ - ObSnapShotType::MAX_SNAPSHOT_TYPE];
  }
  return str;
}

void ObStorageSnapshotInfo::update_by_smaller_snapshot(
  const uint64_t input_snapshot_type,
  const int64_t input_snapshot)
{
  if ((input_snapshot_type < SNAPSHOT_MAX && input_snapshot >= 0) // input info is valid
      && (!is_valid() || snapshot_ > input_snapshot)) {
    // assign to smaller snapshot
    snapshot_ = input_snapshot;
    snapshot_type_ = input_snapshot_type;
  }
}

ObTenantFreezeInfoMgr::ObTenantFreezeInfoMgr()
  : reload_task_(*this),
    update_reserved_snapshot_task_(*this),
    freeze_info_mgr_(),
    snapshots_(),
    lock_(),
    cur_idx_(0),
    last_change_ts_(0),
    global_broadcast_scn_(),
    tenant_id_(OB_INVALID_ID),
    tg_id_(-1),
    inited_(false)
{
}

ObTenantFreezeInfoMgr::~ObTenantFreezeInfoMgr()
{
  destroy();
}

int ObTenantFreezeInfoMgr::mtl_init(ObTenantFreezeInfoMgr* &freeze_info_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "failed to get sql proxy from GCTX, cannot init FreezeInfoMgr", K(ret));
  } else if (OB_FAIL(freeze_info_mgr->init(MTL_ID(), *GCTX.sql_proxy_))) {
    STORAGE_LOG(WARN, "failed to init freeze info mgr", K(ret), K(MTL_ID()));
  } else {
    STORAGE_LOG(INFO, "success to init TenantFreezeInfoMgr", K(MTL_ID()));
  }
  return ret;
}

int ObTenantFreezeInfoMgr::init(const uint64_t tenant_id, ObISQLClient &sql_proxy)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get invalid arguments", K(ret), K(tenant_id));
  } else if (OB_FAIL(freeze_info_mgr_.init(tenant_id, *GCTX.sql_proxy_))) {
    STORAGE_LOG(WARN, "fail to init freeze info mgr", K(ret), K(tenant_id));
  } else if (OB_FAIL(reload_task_.init())) {
    STORAGE_LOG(ERROR, "fail to init reload task", K(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::FreInfoReload, tg_id_))) {
    STORAGE_LOG(ERROR, "fail to init timer", K(ret));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    STORAGE_LOG(ERROR, "fail to init timer", K(ret));
  } else {
    tenant_id_ = tenant_id;
    last_change_ts_ = ObTimeUtility::current_time();
    global_broadcast_scn_ = share::SCN::min_scn();
    inited_ = true;
  }
  return ret;
}

int ObTenantFreezeInfoMgr::start()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id_, reload_task_, RELOAD_INTERVAL, true))) {
    STORAGE_LOG(ERROR, "fail to schedule reload task", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id_, update_reserved_snapshot_task_, UPDATE_LS_RESERVED_SNAPSHOT_INTERVAL, true))) {
    STORAGE_LOG(ERROR, "fail to schedule update reserved snapshot task", K(ret));
  }
  return ret;
}

void ObTenantFreezeInfoMgr::wait()
{
  TG_WAIT(tg_id_);
}

void ObTenantFreezeInfoMgr::stop()
{
  TG_STOP(tg_id_);
}

void ObTenantFreezeInfoMgr::destroy()
{
  TG_DESTROY(tg_id_);
}

int64_t ObTenantFreezeInfoMgr::get_latest_frozen_version()
{
  int64_t frozen_version = 0;

  RLockGuard lock_guard(lock_);
  frozen_version = freeze_info_mgr_.get_latest_frozen_scn().get_val_for_tx();
  return frozen_version;
}

int ObTenantFreezeInfoMgr::get_min_dependent_freeze_info(ObFreezeInfo &freeze_info)
{
  int ret = OB_SUCCESS;
  const int64_t abs_timeout_us = common::ObTimeUtility::current_time() + RLOCK_TIMEOUT_US;
  RLockGuardWithTimeout lock_guard(lock_, abs_timeout_us, ret);

  if (OB_FAIL(ret)) {
    STORAGE_LOG(WARN, "get_lock failed", KR(ret));
  } else if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else {
    const int64_t info_cnt = freeze_info_mgr_.get_freeze_info_count();
    int64_t idx = 0;
    if (info_cnt > MIN_DEPENDENT_FREEZE_INFO_GAP) {
      idx = info_cnt - MIN_DEPENDENT_FREEZE_INFO_GAP;
    }

    if (OB_FAIL(freeze_info_mgr_.get_freeze_info_by_idx(idx, freeze_info))) {
      STORAGE_LOG(WARN, "fail to get frozen status", K(ret), K(idx));
    } else {
      LOG_INFO("get min dependent freeze info", K(ret), K(freeze_info)); // diagnose code for issue 45841468
    }
  }
  return ret;
}

int ObTenantFreezeInfoMgr::get_freeze_info_behind_major_snapshot(
    const int64_t major_snapshot_version,
    ObIArray<ObFreezeInfo> &freeze_infos)
{
  int ret = OB_SUCCESS;
  ObSEArray<share::ObFreezeInfo, 8> info_list;
  const int64_t abs_timeout_us = common::ObTimeUtility::current_time() + RLOCK_TIMEOUT_US;
  RLockGuardWithTimeout lock_guard(lock_, abs_timeout_us, ret);

  if (OB_FAIL(ret)) {
    STORAGE_LOG(WARN, "get_lock failed", KR(ret));
  } else if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_UNLIKELY(major_snapshot_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to get freeze info", K(ret), K(major_snapshot_version));
  } else if (OB_FAIL(freeze_info_mgr_.get_freeze_info_by_major_snapshot(major_snapshot_version, freeze_infos, true/*need_all*/))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "failed to get frozen status behind given snapshot version", K(ret), K(major_snapshot_version));
    }
  }
  return ret;
}

int ObTenantFreezeInfoMgr::get_freeze_info_by_snapshot_version(
    const int64_t snapshot_version,
    ObFreezeInfo &freeze_info)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObFreezeInfo, 1> freeze_infos;
  const int64_t abs_timeout_us = common::ObTimeUtility::current_time() + RLOCK_TIMEOUT_US;
  RLockGuardWithTimeout lock_guard(lock_, abs_timeout_us, ret);

  if (OB_FAIL(ret)) {
    STORAGE_LOG(WARN, "get_lock failed", KR(ret));
  } else if (OB_UNLIKELY(snapshot_version <= 0 || INT64_MAX == snapshot_version)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "snapshot version is invalid", K(ret), K(snapshot_version));
  } else if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(freeze_info_mgr_.get_freeze_info_by_major_snapshot(snapshot_version, freeze_infos, false/*need_all*/))) {
    STORAGE_LOG(WARN, "failed to get frozen status by snapshot", K(ret), K(snapshot_version));
  } else {
    freeze_info = freeze_infos.at(0);
  }
  return ret;
}

int ObTenantFreezeInfoMgr::get_freeze_info_behind_snapshot_version(
    const int64_t snapshot_version,
    ObFreezeInfo &freeze_info)
{
  int ret = OB_SUCCESS;
  const int64_t abs_timeout_us = common::ObTimeUtility::current_time() + RLOCK_TIMEOUT_US;
  RLockGuardWithTimeout lock_guard(lock_, abs_timeout_us, ret);
  if (OB_FAIL(ret)) {
    STORAGE_LOG(WARN, "get_lock failed", KR(ret));
  } else if (OB_FAIL(get_freeze_info_compare_with_snapshot_version_(snapshot_version, share::ObFreezeInfoManager::CmpType::GREATER_THAN, freeze_info))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "failed to get freeze info behind snapshot version", KR(ret), K(snapshot_version));
    }
  }
  return ret;
}

int ObTenantFreezeInfoMgr::get_lower_bound_freeze_info_before_snapshot_version(const int64_t snapshot_version, share::ObFreezeInfo &freeze_info)
{
  int ret = OB_SUCCESS;
  const int64_t abs_timeout_us = common::ObTimeUtility::current_time() + RLOCK_TIMEOUT_US;
  RLockGuardWithTimeout lock_guard(lock_, abs_timeout_us, ret);
  if (OB_FAIL(ret)) {
    STORAGE_LOG(WARN, "get_lock failed", KR(ret));
  } else if (OB_FAIL(get_freeze_info_compare_with_snapshot_version_(snapshot_version, share::ObFreezeInfoManager::CmpType::LOWER_BOUND, freeze_info))) {
    STORAGE_LOG(WARN, "failed to get freeze info before snapshot version", KR(ret), K(snapshot_version));
  }
  return ret;
}

int ObTenantFreezeInfoMgr::get_freeze_info_compare_with_snapshot_version_(
    const int64_t snapshot_version,
    const share::ObFreezeInfoManager::CmpType cmp_type,
    ObFreezeInfo &freeze_info)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(snapshot_version <= 0 || INT64_MAX == snapshot_version)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "snapshot version is invalid", K(ret), K(snapshot_version));
  } else if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(freeze_info_mgr_.get_freeze_info_compare_with_major_snapshot(snapshot_version, cmp_type, freeze_info))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "fail to found frozen status compare with major snapshot", K(ret), K(snapshot_version), K(cmp_type));
    }
  }
  return ret;
}

int ObTenantFreezeInfoMgr::get_neighbour_major_freeze(
    const int64_t snapshot_version,
    NeighbourFreezeInfo &info)
{
  int ret = OB_SUCCESS;

  info.reset();
  bool found = false;
  share::ObFreezeInfo prev_frozen_status;
  share::ObFreezeInfo next_frozen_status;
  const int64_t abs_timeout_us = common::ObTimeUtility::current_time() + RLOCK_TIMEOUT_US;
  RLockGuardWithTimeout lock_guard(lock_, abs_timeout_us, ret);

  if (OB_FAIL(ret)) {
    STORAGE_LOG(WARN, "get_lock failed", KR(ret));
  } else if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(freeze_info_mgr_.get_neighbour_frozen_status(snapshot_version, prev_frozen_status, next_frozen_status))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "failed to get neighbour frozen status", K(ret), K(snapshot_version));
    }
  } else {
    info.next = next_frozen_status;
    info.prev = prev_frozen_status;
  }
  return ret;
}

static inline
int is_snapshot_related_to_tablet(
    const ObTabletID &tablet_id,
    const ObSnapshotInfo &snapshot,
    bool &related)
{
  int ret = OB_SUCCESS;
  related = false;
  uint64_t tenant_id = MTL_ID();

  if (!snapshot.is_valid() || !is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(snapshot), K(tenant_id));
  } else if (snapshot.snapshot_type_ == share::SNAPSHOT_FOR_RESTORE_POINT
      || snapshot.snapshot_type_ == share::SNAPSHOT_FOR_BACKUP_POINT) {
    if (snapshot.snapshot_type_ == share::SNAPSHOT_FOR_RESTORE_POINT && tablet_id.is_inner_tablet()) {
      related = false;
    } else if (tenant_id == snapshot.tenant_id_) {
//      TODO (@yanyuan) fix restore point
//      related = true;
//      bool is_complete = false;
//      if (OB_FAIL(ObPartitionService::get_instance().check_restore_point_complete(
//         pkey, snapshot.snapshot_ts_, is_complete))) {
//        STORAGE_LOG(WARN, "failed to check restore point exist", K(ret));
//      } else if (is_complete) {
//        related = false;
//      }
    }
  } else {
    // when tenant_id_ equals to 0, it means need all tenants
    if (0 == snapshot.tenant_id_
        || tenant_id == snapshot.tenant_id_) {
      // when tablet_id_ equals to 0, it means need all tablets in tenant
      if (0 == snapshot.tablet_id_
          || snapshot.tablet_id_ == tablet_id.id()) {
        related = true;
      }
    }
  }
  return ret;
}

int ObTenantFreezeInfoMgr::get_multi_version_duration(int64_t &duration) const
{
  int ret = OB_SUCCESS;

  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (tenant_config.is_valid()) {
    duration = tenant_config->undo_retention;
  } else {
    ret = OB_TENANT_NOT_EXIST;
  }

  return ret;
}

int64_t ObTenantFreezeInfoMgr::get_min_reserved_snapshot_for_tx()
{
  int ret = OB_SUCCESS;
  int64_t snapshot_version = INT64_MAX;
  uint64_t data_version = 0;

  // is_gc_disabled means whether gc using globally reserved snapshot is disabled,
  // and it may be because of disk usage or lost connection to inner table
  bool is_gc_disabled = MTL(concurrency_control::ObMultiVersionGarbageCollector *)->
    is_gc_disabled();

  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));

  if (OB_FAIL(GET_MIN_DATA_VERSION(gen_meta_tenant_id(MTL_ID()),
                                   data_version))) {
    STORAGE_LOG(WARN, "get min data version failed", KR(ret),
                K(gen_meta_tenant_id(MTL_ID())));
    // we disable the gc when fetch min data version failed
    is_gc_disabled = true;
  }

  if (data_version >= DATA_VERSION_4_1_0_0
      && tenant_config->_mvcc_gc_using_min_txn_snapshot
      && !is_gc_disabled) {
    share::SCN snapshot_for_active_tx =
      MTL(concurrency_control::ObMultiVersionGarbageCollector *)->
      get_reserved_snapshot_for_active_txn();
    snapshot_version = snapshot_for_active_tx.get_val_for_tx();
  }

  return snapshot_version;
}

void ObTenantFreezeInfoMgr::check_tenant_in_restore_with_mv_(
    bool &need_check_mview,
    ObSchemaGetterGuard &schema_guard,
    const ObSimpleTenantSchema *&tenant_schema)
{
  need_check_mview = false;
  int ret = OB_SUCCESS;
  if (MTL_TENANT_ROLE_CACHE_IS_RESTORE() || MTL_TENANT_ROLE_CACHE_IS_INVALID()) {
    need_check_mview = true;
  } else if (MTL_TENANT_ROLE_CACHE_IS_PRIMARY()) {
    need_check_mview = false;
  } else {
    // get tenant schema if pointer is nullptr
    const uint64_t tenant_id = MTL_ID();
    if (OB_ISNULL(tenant_schema)) {
      if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
        LOG_WARN("failed to get schema guard", K(ret), K(tenant_id));
      } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
        LOG_WARN("failed to get tenant info", K(ret), K(tenant_id));
      }
    }
    if (OB_FAIL(ret)) {
      need_check_mview = true;
      LOG_WARN("failed to get tenant schema, need check mview", K(ret),
                K(tenant_id), K(need_check_mview), KP(tenant_schema));
    } else if (OB_ISNULL(tenant_schema)) {
      need_check_mview = true;
      LOG_WARN("tenant schema is null, need check mview", K(ret),
                K(tenant_id), K(need_check_mview), KP(tenant_schema));
    } else if (tenant_schema->is_restore()) {
      need_check_mview = true;
    }
  }
}

// get smallest kept snapshot
int ObTenantFreezeInfoMgr::get_min_reserved_snapshot(
    const ObTabletID &tablet_id,
    const int64_t merged_version,
    ObStorageSnapshotInfo &snapshot_info)
{
  int ret = OB_SUCCESS;
  ObFreezeInfo freeze_info;
  int64_t duration = 0;
  bool unused = false;
  snapshot_info.reset();
  const ObSimpleTenantSchema *tenant_schema = nullptr;
  ObSchemaGetterGuard schema_guard;

  const int64_t abs_timeout_us = common::ObTimeUtility::current_time() + RLOCK_TIMEOUT_US;
  RLockGuardWithTimeout lock_guard(lock_, abs_timeout_us, ret);
  ObIArray<ObSnapshotInfo> &snapshots = snapshots_[cur_idx_];
  if (OB_FAIL(ret)) {
    STORAGE_LOG(WARN, "get_lock failed", KR(ret));
  } else if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(get_multi_version_duration(duration))) {
    STORAGE_LOG(WARN, "fail to get multi version duration", K(ret), K(tablet_id));
  } else {
    if (merged_version < 1) {
      freeze_info.frozen_scn_.set_min();
    } else if (OB_FAIL(get_freeze_info_compare_with_snapshot_version_(merged_version, share::ObFreezeInfoManager::CmpType::GREATER_THAN, freeze_info))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("failed to get freeze info behind snapshot", K(ret), K(merged_version));
      } else {
        freeze_info.frozen_scn_.set_max();
        ret = OB_SUCCESS;
      }
    }

    const int64_t snapshot_gc_ts = freeze_info_mgr_.get_snapshot_gc_scn().get_val_for_tx();
    const int64_t snapshot_for_undo_retention = MAX(0, snapshot_gc_ts - duration * 1000L * 1000L * 1000L);
    const int64_t snapshot_for_tx = get_min_reserved_snapshot_for_tx();
    snapshot_info.update_by_smaller_snapshot(ObStorageSnapshotInfo::SNAPSHOT_FOR_UNDO_RETENTION, snapshot_for_undo_retention);
    snapshot_info.update_by_smaller_snapshot(ObStorageSnapshotInfo::SNAPSHOT_FOR_TX, snapshot_for_tx);
    snapshot_info.update_by_smaller_snapshot(ObStorageSnapshotInfo::SNAPSHOT_FOR_MAJOR_FREEZE_TS, freeze_info.frozen_scn_.get_val_for_tx());
    bool exit_loop = false;
    for (int64_t i = 0; i < snapshots.count() && OB_SUCC(ret) && !exit_loop; ++i) {
      bool related = false;
      const ObSnapshotInfo &snapshot = snapshots.at(i);
      if (OB_FAIL(is_snapshot_related_to_tablet(tablet_id, snapshot, related))) {
        STORAGE_LOG(WARN, "fail to check snapshot relation", K(ret), K(tablet_id), K(snapshot));
      } else if (related) {
        snapshot_info.update_by_smaller_snapshot(snapshot.snapshot_type_, snapshot.snapshot_scn_.get_val_for_tx());
        if (ObSnapShotType::SNAPSHOT_FOR_MAJOR_REFRESH_MV == snapshot.snapshot_type_) {
          // if exist mview snapshot type and tenant in restore
          // if tenant is invalid or restore, need check mview snapshot
          // TODO siyu:: use tenant_status_cache
          bool need_check_mview = false;
          IGNORE_RETURN check_tenant_in_restore_with_mv_(need_check_mview, schema_guard, tenant_schema);
          if (need_check_mview) {
            exit_loop = true;
            snapshot_info.update_by_smaller_snapshot(ObSnapShotType::SNAPSHOT_FOR_MAJOR_REFRESH_MV, static_cast<int64_t>(0));
            LOG_INFO("exist new mv in restore", K(ret), K(snapshot_info), K(tablet_id), K(merged_version), K(need_check_mview));
          }
          LOG_INFO("exist new mview when calc multi_version_start", K(ret), K(tablet_id), K(need_check_mview), K(snapshot_info), K(exit_loop));
        }
      }
    }
    LOG_TRACE("check_freeze_info_mgr", K(ret), K(snapshot_info), K(duration), K(snapshot_for_undo_retention),
      K(freeze_info), K(snapshot_gc_ts), K(snapshot_for_tx));
  }
  return ret;
}

int ObTenantFreezeInfoMgr::update_next_snapshots(const ObIArray<ObSnapshotInfo> &snapshots)
{
  int ret = OB_SUCCESS;
  int64_t next_idx = get_next_idx();
  snapshots_[next_idx].reset();
  ObIArray<ObSnapshotInfo> &next_snapshots = snapshots_[next_idx];

  for (int64_t i = 0; OB_SUCC(ret) && i < snapshots.count(); ++i) {
    if (OB_FAIL(next_snapshots.push_back(snapshots.at(i)))) {
      STORAGE_LOG(WARN, "fail to push back snapshot", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    switch_info();
  }
  return ret;
}

int64_t ObTenantFreezeInfoMgr::get_snapshot_gc_ts()
{
  return get_snapshot_gc_scn().get_val_for_tx();
}

share::SCN ObTenantFreezeInfoMgr::get_snapshot_gc_scn()
{
  RLockGuard lock_guard(lock_);
  return freeze_info_mgr_.get_snapshot_gc_scn();
}

ObTenantFreezeInfoMgr::ReloadTask::ReloadTask(ObTenantFreezeInfoMgr &mgr)
  : inited_(false),
    check_tenant_status_(true),
    mgr_(mgr)
{
}

int ObTenantFreezeInfoMgr::ReloadTask::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

int ObTenantFreezeInfoMgr::ReloadTask::refresh_merge_info()
{
  int ret = OB_SUCCESS;

  const uint64_t tenant_id = MTL_ID();
  ObZoneMergeInfo zone_merge_info;
  zone_merge_info.tenant_id_ = tenant_id;
  zone_merge_info.zone_ = GCTX.config_->zone.str();

  ObGlobalMergeInfo global_merge_info;
  global_merge_info.tenant_id_ = tenant_id;

  if (OB_FAIL(ObGlobalMergeTableOperator::load_global_merge_info(*GCTX.sql_proxy_, tenant_id, global_merge_info))) {
    LOG_WARN("failed to load global merge info", KR(ret), K(global_merge_info));
  } else if (OB_FAIL(ObZoneMergeTableOperator::load_zone_merge_info(*GCTX.sql_proxy_, tenant_id, zone_merge_info))) {
    LOG_WARN("fail to load zone merge info", KR(ret), K(zone_merge_info));
  } else {
    // set merged version
    MERGE_SCHEDULER_PTR->set_inner_table_merged_scn(global_merge_info.last_merged_scn_.get_scn().get_val_for_tx());
    mgr_.set_global_broadcast_scn(global_merge_info.global_broadcast_scn_.get_scn());
    if (global_merge_info.suspend_merging_.get_value()) { // suspend_merge
      MERGE_SCHEDULER_PTR->stop_major_merge();
      LOG_INFO("schedule zone to stop major merge", K(tenant_id), K(zone_merge_info), K(global_merge_info));
    } else {
      if (check_tenant_status_) {
        if (is_sys_tenant(tenant_id) || is_meta_tenant(tenant_id)) {
          check_tenant_status_ = false;
        } else if (is_virtual_tenant_id(tenant_id)) { // skip virtual tenant
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tenant is unexpected virtual tenant", KR(ret), K(tenant_id));
        } else {
          const ObTenantRole::Role &role = MTL_GET_TENANT_ROLE_CACHE();
          if (is_primary_tenant(role) || is_standby_tenant(role)) {
            check_tenant_status_ = false;
            LOG_INFO("finish check tenant restore", K(tenant_id), K(role));
          } else if (REACH_THREAD_TIME_INTERVAL(10L * 1000L * 1000L)) {
            LOG_INFO("skip restoring tenant to schedule major merge", K(tenant_id), K(role));
          }
        }
      }
      if (!check_tenant_status_) {
        MERGE_SCHEDULER_PTR->resume_major_merge();
        const int64_t cur_broadcast_version = MERGE_SCHEDULER_PTR->get_frozen_version();
        if (zone_merge_info.broadcast_scn_.get_scn().get_val_for_tx() > cur_broadcast_version) {
          FLOG_INFO("try to schedule merge", K(tenant_id), "zone", zone_merge_info.zone_, "broadcast_scn",
            zone_merge_info.broadcast_scn_, K(cur_broadcast_version));
          if (OB_FAIL(MERGE_SCHEDULER_PTR->schedule_merge(zone_merge_info.broadcast_scn_.get_scn().get_val_for_tx()))) {
            LOG_WARN("fail to schedule merge", K(ret), K(zone_merge_info));
          } else if (OB_FAIL(MTL(ObTenantFreezer*)->update_frozen_scn(zone_merge_info.broadcast_scn_.get_scn().get_val_for_tx()))) {
            LOG_WARN("update frozen scn failed", K(ret), K(zone_merge_info.broadcast_scn_.get_scn()));
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    LOG_TRACE("refresh merge info", K(tenant_id), "zone", zone_merge_info.zone_, "broadcast_scn",
      zone_merge_info.broadcast_scn_);
  }
  return ret;
}

int ObTenantFreezeInfoMgr::try_update_info()
{
  int ret = OB_SUCCESS;

  DEBUG_SYNC(BEFORE_UPDATE_FREEZE_SNAPSHOT_INFO);
  ObSEArray<ObSnapshotInfo, 4> snapshots;
  ObSEArray<ObFreezeInfo, 4> freeze_infos;
  share::SCN new_snapshot_gc_scn;
  share::ObSnapshotTableProxy snapshot_proxy;

  if (OB_FAIL(ObFreezeInfoManager::fetch_new_freeze_info(
        MTL_ID(), share::SCN::base_scn(), *GCTX.sql_proxy_, freeze_infos, new_snapshot_gc_scn))) {
    STORAGE_LOG(WARN, "failed to load updated info", K(ret));
  } else if (OB_FAIL(snapshot_proxy.get_all_snapshots(*GCTX.sql_proxy_, MTL_ID(), snapshots))) {
    STORAGE_LOG(WARN, "failed to get snapshots", K(ret));
  } else if (OB_FAIL(inner_update_info(new_snapshot_gc_scn, freeze_infos, snapshots))) {
    STORAGE_LOG(WARN, "failed to update info", K(ret), K(freeze_infos), K(new_snapshot_gc_scn), K(snapshots));
  }
  return ret;
}

int ObTenantFreezeInfoMgr::inner_update_info(
    const share::SCN &new_snapshot_gc_scn,
    const common::ObIArray<share::ObFreezeInfo> &new_freeze_infos,
    const common::ObIArray<share::ObSnapshotInfo> &new_snapshots)
{
  int ret = OB_SUCCESS;
  bool gc_snapshot_ts_changed = false;
  int64_t snapshot_gc_ts = 0;
  {
    WLockGuard lock_guard(lock_);
    const int64_t old_snapshot_gc_ts = freeze_info_mgr_.get_snapshot_gc_scn().get_val_for_tx();
    snapshot_gc_ts = old_snapshot_gc_ts;
    if (OB_FAIL(freeze_info_mgr_.update_freeze_info(new_freeze_infos, new_snapshot_gc_scn))) {
      STORAGE_LOG(WARN, "failed to reload freeze info mgr", K(ret));
    } else if (OB_FAIL(update_next_snapshots(new_snapshots))) {
      STORAGE_LOG(WARN, "fail to update next snapshots", K(ret));
    } else {
      snapshot_gc_ts = freeze_info_mgr_.get_snapshot_gc_scn().get_val_for_tx();
      gc_snapshot_ts_changed = old_snapshot_gc_ts != snapshot_gc_ts;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (gc_snapshot_ts_changed) {
    last_change_ts_ = ObTimeUtility::current_time();
  } else {
    const int64_t last_not_change_interval_us = ObTimeUtility::current_time() - last_change_ts_;
    if (MAX_GC_SNAPSHOT_TS_REFRESH_TS <= last_not_change_interval_us &&
        (0 != snapshot_gc_ts && 1 != snapshot_gc_ts)) {
      if (REACH_THREAD_TIME_INTERVAL(60L * 1000L * 1000L)) {
        // ignore ret
        STORAGE_LOG(WARN, "snapshot_gc_ts not refresh too long",
                    K(snapshot_gc_ts), K(new_snapshots), K(last_change_ts_),
                    K(last_not_change_interval_us));
      }
    } else if (FLUSH_GC_SNAPSHOT_TS_REFRESH_TS <= last_not_change_interval_us) {
      STORAGE_LOG(WARN, "snapshot_gc_ts not refresh too long",
                  K(snapshot_gc_ts), K(new_snapshots), K(last_change_ts_),
                  K(last_not_change_interval_us));
    }
  }
  STORAGE_LOG(DEBUG, "reload freeze info and snapshots", K(snapshot_gc_ts), K(new_snapshots));

  if (OB_SUCC(ret)) {
    if (REACH_THREAD_TIME_INTERVAL(20 * 1000 * 1000 /*20s*/)) {
      STORAGE_LOG(INFO, "ObTenantFreezeInfoMgr success to update infos",
          K(new_snapshot_gc_scn), K(new_freeze_infos), K(new_snapshots), K(freeze_info_mgr_));
    }
  }
  return ret;
}

void ObTenantFreezeInfoMgr::ReloadTask::runTimerTask()
{
  int tmp_ret = OB_SUCCESS;
  if (!SERVER_STORAGE_META_SERVICE.is_started()) {
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000 /* 10s */)) {
      LOG_WARN_RET(tmp_ret, "slog replay hasn't finished, this task can't start");
    }
  } else {
    if (OB_TMP_FAIL(refresh_merge_info())) {
      LOG_WARN_RET(tmp_ret, "fail to refresh merge info", KR(tmp_ret));
    }
    if (OB_TMP_FAIL(mgr_.try_update_info())) {
      LOG_WARN_RET(tmp_ret, "fail to try update info", KR(tmp_ret));
    }
  }
}

void ObTenantFreezeInfoMgr::UpdateLSResvSnapshotTask::runTimerTask()
{
  int tmp_ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  compaction::ObBasicMergeScheduler *scheduler = nullptr;
  if (OB_ISNULL(scheduler = compaction::ObBasicMergeScheduler::get_merge_scheduler())) {
    // may be during the start phase
  } else if (OB_TMP_FAIL(scheduler->get_min_data_version(compat_version))) {
    LOG_WARN_RET(tmp_ret, "failed to get min data version", KR(tmp_ret));
  } else if (compat_version < DATA_VERSION_4_1_0_0) {
    // do nothing, should not update reserved snapshot
  } else if (OB_TMP_FAIL(mgr_.try_update_reserved_snapshot())) {
    LOG_WARN_RET(tmp_ret, "fail to try reserved snapshot", KR(tmp_ret));
  }
}

int ObTenantFreezeInfoMgr::try_update_reserved_snapshot()
{
  int ret = OB_SUCCESS;
  int64_t duration = 0;
  int64_t reserved_snapshot = 0;
  int64_t cost_ts = ObTimeUtility::fast_current_time();
  {
    RLockGuard lock_guard(lock_);

    if (OB_UNLIKELY(!inited_)) {
      ret = OB_NOT_INIT;
      STORAGE_LOG(WARN, "ObTenantFreezeInfoMgr not init", K(ret));
    } else if (OB_FAIL(get_multi_version_duration(duration))) {
      STORAGE_LOG(WARN, "fail to get multi version duration", K(ret));
    } else {
      int64_t snapshot_gc_ts = freeze_info_mgr_.get_snapshot_gc_scn().get_val_for_tx();
      reserved_snapshot = std::max(0L, snapshot_gc_ts - duration * 1000L * 1000L *1000L);
      LOG_INFO("success to update min reserved snapshot", K(reserved_snapshot), K(duration), K(snapshot_gc_ts));
    }
  } // end of lock

  // loop all ls, try update reserved snapshot
  ObSharedGuard<ObLSIterator> ls_iter_guard;
  ObLS *ls = nullptr;
  if (OB_FAIL(ret) || reserved_snapshot <= 0) {
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls_iter(ls_iter_guard, ObLSGetMod::COMPACT_MODE))) {
    LOG_WARN("failed to get ls iterator", K(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(ls_iter_guard.get_ptr()->get_next(ls))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_ISNULL(ls)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls is null", K(ret), KP(ls));
      } else if (OB_TMP_FAIL(ls->try_sync_reserved_snapshot(reserved_snapshot, true/*update_flag*/))) {
        LOG_WARN("failed to update min reserved snapshot", K(tmp_ret), KPC(ls), K(reserved_snapshot));
      }
    } // end of while
  }
  cost_ts = ObTimeUtility::fast_current_time() - cost_ts;
  STORAGE_LOG(INFO, "update reserved snapshot finished", K(cost_ts), K(reserved_snapshot));
  return ret;
}

} // storage
} // oceanbase
