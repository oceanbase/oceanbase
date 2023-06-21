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
#include "common/storage/ob_freeze_define.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/utility/ob_print_utils.h"
#include "share/ob_global_stat_proxy.h"
#include "share/ob_tenant_mgr.h"
#include "share/ob_thread_mgr.h"
#include "share/rc/ob_tenant_base.h"
#include "share/ob_zone_table_operation.h"
#include "share/schema/ob_tenant_schema_service.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_tenant_schema_service.h"
#include "observer/ob_server_struct.h"
#include "share/schema/ob_tenant_schema_service.h"
#include "share/system_variable/ob_system_variable_alias.h"
#include "share/ob_zone_merge_info.h"
#include "share/ob_global_merge_table_operator.h"
#include "share/ob_zone_merge_table_operator.h"
#include "storage/compaction/ob_server_compaction_event_history.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "storage/concurrency_control/ob_multi_version_garbage_collector.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/slog_ckpt/ob_server_checkpoint_slog_handler.h"

namespace oceanbase
{

using namespace common;
using namespace share;
using namespace share::schema;

using common::hash::ObHashSet;

namespace storage
{

ObTenantFreezeInfoMgr::ObTenantFreezeInfoMgr()
  : reload_task_(*this),
    update_reserved_snapshot_task_(*this),
    info_list_(),
    snapshots_(),
    lock_(),
    snapshot_gc_ts_(0),
    cur_idx_(0),
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
  } else if (OB_FAIL(reload_task_.init(sql_proxy))) {
    STORAGE_LOG(ERROR, "fail to init reload task", K(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::FreInfoReload, tg_id_))) {
    STORAGE_LOG(ERROR, "fail to init timer", K(ret));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    STORAGE_LOG(ERROR, "fail to init timer", K(ret));
  } else {
    tenant_id_ = tenant_id;
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
  ObIArray<FreezeInfo> &info_list = info_list_[cur_idx_];

  if (0 != info_list.count()) {
    frozen_version = info_list.at(info_list.count()-1).freeze_version;
  }
  return frozen_version;
}

int ObTenantFreezeInfoMgr::get_min_dependent_freeze_info(FreezeInfo &freeze_info)
{
  int ret = OB_SUCCESS;
  RLockGuard lock_guard(lock_);
  ObIArray<FreezeInfo> &info_list = info_list_[cur_idx_];
  int64_t idx = 0;
  if (info_list.count() > MIN_DEPENDENT_FREEZE_INFO_GAP) {
    idx = info_list.count() - MIN_DEPENDENT_FREEZE_INFO_GAP;
  }
  ret = get_info_nolock(idx, freeze_info);
  LOG_INFO("get min dependent freeze info", K(ret), K(freeze_info)); // diagnose code for issue 45841468
  return ret;
}

int ObTenantFreezeInfoMgr::get_latest_freeze_info(FreezeInfo &freeze_info)
{
  int ret = OB_SUCCESS;
  RLockGuard lock_guard(lock_);
  ret = get_info_nolock(info_list_[cur_idx_].count() - 1, freeze_info);
  return ret;
}

int ObTenantFreezeInfoMgr::get_info_nolock(const int64_t idx, FreezeInfo &freeze_info)
{
  int ret = OB_SUCCESS;
  ObIArray<FreezeInfo> &info_list = info_list_[cur_idx_];
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (info_list.empty() || idx > info_list.count()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("no freeze info in curr info_list", K(ret), K(cur_idx_), K(info_list_[0]), K(info_list_[1]));
  } else {
    freeze_info = info_list.at(idx);
  }
  return ret;
}


int ObTenantFreezeInfoMgr::get_freeze_info_behind_major_snapshot(
    const int64_t major_snapshot_version,
    ObIArray<FreezeInfo> &freeze_infos)
{
  int ret = OB_SUCCESS;
  RLockGuard lock_guard(lock_);

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_UNLIKELY(major_snapshot_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to get freeze info", K(ret), K(major_snapshot_version));
  } else {
    const ObIArray<FreezeInfo> &info_list = info_list_[cur_idx_];
    int64_t ret_pos = find_pos_in_list_(major_snapshot_version, info_list);
    if (ret_pos < 0 || ret_pos >= info_list.count()) {
      ret = OB_ENTRY_NOT_EXIST;
      STORAGE_LOG(DEBUG, "Freeze info of specified major version not found", K(ret), K(major_snapshot_version));
    } else {
      for (int64_t i = ret_pos; OB_SUCC(ret) && i < info_list.count(); i++) {
        if (OB_FAIL(freeze_infos.push_back(info_list.at(i)))) {
          STORAGE_LOG(WARN, "Failed to push back freeze info", K(ret));
        }
      }
    }
  }
  return ret;
}

int64_t ObTenantFreezeInfoMgr::find_pos_in_list_(
    const int64_t snapshot_version,
    const ObIArray<FreezeInfo> &info_list)
{
  int64_t ret_pos = -1;
  int64_t mid = 0;
  int64_t l = 0;
  int64_t r = info_list.count();

  while (l < r && ret_pos < 0) {
    mid = (l + r) >> 1;
    const FreezeInfo &tmp_info = info_list.at(mid);
    if (snapshot_version < tmp_info.freeze_version) {
      r = mid;
    } else if (snapshot_version > tmp_info.freeze_version) {
      l = mid + 1;
    } else {
      ret_pos = mid;
    }
  }
  return ret_pos;
}

int ObTenantFreezeInfoMgr::get_freeze_info_by_snapshot_version(
    const int64_t snapshot_version,
    FreezeInfo &freeze_info)
{
  int ret = OB_SUCCESS;

  RLockGuard lock_guard(lock_);
  const ObIArray<FreezeInfo> &info_list = info_list_[cur_idx_];

  if (OB_UNLIKELY(snapshot_version <= 0 || INT64_MAX == snapshot_version)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "snapshot version is invalid", K(ret), K(snapshot_version));
  } else if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (info_list.empty()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("no freeze info in curr info_list", K(ret), K(cur_idx_), K(info_list_[0]), K(info_list_[1]));
  } else {
    int64_t ret_pos = find_pos_in_list_(snapshot_version, info_list);
    if (ret_pos < 0 || ret_pos >= info_list.count()) {
      ret = OB_ENTRY_NOT_EXIST;
      STORAGE_LOG(WARN, "can not find the freeze info", K(ret), K(snapshot_version), K(info_list));
    } else {
      freeze_info = info_list.at(ret_pos);
    }
  }
  return ret;
}

int ObTenantFreezeInfoMgr::get_freeze_info_behind_snapshot_version(
    const int64_t snapshot_version,
    FreezeInfo &freeze_info)
{
  RLockGuard lock_guard(lock_);
  return get_freeze_info_behind_snapshot_version_(snapshot_version, freeze_info);
}

int ObTenantFreezeInfoMgr::get_freeze_info_behind_snapshot_version_(
    const int64_t snapshot_version,
    FreezeInfo &freeze_info)
{
  int ret = OB_SUCCESS;

  ObIArray<FreezeInfo> &info_list = info_list_[cur_idx_];

  if (OB_UNLIKELY(snapshot_version <= 0 || INT64_MAX == snapshot_version)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "snapshot version is invalid", K(ret), K(snapshot_version));
  } else if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (info_list.empty()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_INFO("no freeze info in curr info_list", K(ret), K(cur_idx_), K(info_list_[0]), K(info_list_[1]));
  } else {
    bool found = false;
    for (int64_t i = 0; OB_SUCC(ret) && !found && i < info_list.count(); ++i) {
      FreezeInfo &tmp_info = info_list.at(i);
      if (snapshot_version < tmp_info.freeze_version) {
        freeze_info = tmp_info;
        found = true;
      }
    }

    if (OB_SUCC(ret)) {
      LOG_DEBUG("get freeze info", K(ret), K(found), K(snapshot_version), K(freeze_info));
      if (!found) {
        ret = OB_ENTRY_NOT_EXIST;
      }
    }
  }

  return ret;
}

int ObTenantFreezeInfoMgr::inner_get_neighbour_major_freeze(
    const int64_t snapshot_version,
    NeighbourFreezeInfo &info)
{
  int ret = OB_SUCCESS;

  info.reset();

  ObIArray<FreezeInfo> &info_list = info_list_[cur_idx_];
  bool found = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (info_list.empty()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("no freeze info in curr info_list", K(ret), K(cur_idx_), K(info_list_[0]), K(info_list_[1]));
  } else if (snapshot_version >= info_list.at(info_list.count() - 1).freeze_version) {
    // use found = false setting
  } else {
    for (int64_t i = 0; i < info_list.count() && OB_SUCC(ret) && !found; ++i) {
      FreezeInfo &next_info = info_list.at(i);
      if (snapshot_version < next_info.freeze_version) {
        found = true;
        if (0 == i) {
          ret = OB_ENTRY_NOT_EXIST;
          STORAGE_LOG(WARN, "cannot get neighbour major freeze before bootstrap",
                      K(ret), K(snapshot_version), K(next_info));
        } else {
          info.next = next_info;
          info.prev = info_list.at(i- 1);
        }
      }
    }
  }
  if (OB_SUCC(ret) && !found) {
    info.next.freeze_version = INT64_MAX;
    info.prev = info_list.at(info_list.count() - 1);
  }
  return ret;
}

int ObTenantFreezeInfoMgr::get_neighbour_major_freeze(
    const int64_t snapshot_version,
    NeighbourFreezeInfo &info)
{
  RLockGuard lock_guard(lock_);
  return inner_get_neighbour_major_freeze(snapshot_version, info);
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

int ObTenantFreezeInfoMgr::get_min_reserved_snapshot(
    const ObTabletID &tablet_id,
    const int64_t merged_version,
    int64_t &snapshot_version)
{
  int ret = OB_SUCCESS;
  FreezeInfo freeze_info;
  int64_t duration = 0;
  bool unused = false;
  snapshot_version = 0;

  RLockGuard lock_guard(lock_);
  ObIArray<ObSnapshotInfo> &snapshots = snapshots_[cur_idx_];

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(get_multi_version_duration(duration))) {
    STORAGE_LOG(WARN, "fail to get multi version duration", K(ret), K(tablet_id));
  } else {
    if (merged_version < 1) {
      freeze_info.freeze_version = 0;
    } else if (OB_FAIL(get_freeze_info_behind_snapshot_version_(merged_version, freeze_info))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("failed to get freeze info behind snapshot", K(ret), K(merged_version));
      } else {
        freeze_info.freeze_version = INT64_MAX;
        ret = OB_SUCCESS;
      }
    }

    snapshot_version = std::max(0L, snapshot_gc_ts_ - duration * 1000L * 1000L * 1000L);
    snapshot_version = std::min(snapshot_version, get_min_reserved_snapshot_for_tx());
    snapshot_version = std::min(snapshot_version, freeze_info.freeze_version);

    for (int64_t i = 0; i < snapshots.count() && OB_SUCC(ret); ++i) {
      bool related = false;
      const ObSnapshotInfo &snapshot = snapshots.at(i);
      if (OB_FAIL(is_snapshot_related_to_tablet(tablet_id, snapshot, related))) {
        STORAGE_LOG(WARN, "fail to check snapshot relation", K(ret), K(tablet_id), K(snapshot));
      } else if (related) {
        snapshot_version = std::min(snapshot_version, snapshot.snapshot_scn_.get_val_for_tx());
      }
    }
    LOG_DEBUG("get_min_reserved_snapshot", K(ret), K(duration), K(snapshot_version), K(freeze_info),
        K(snapshot_gc_ts_));
  }
  return ret;
}

int ObTenantFreezeInfoMgr::diagnose_min_reserved_snapshot(
    const ObTabletID &tablet_id,
    const int64_t merge_snapshot_version,
    int64_t &snapshot_version,
    common::ObString &snapshot_from_type)
{
  int ret = OB_SUCCESS;
  FreezeInfo freeze_info;
  int64_t duration = 0;
  bool unused = false;

  RLockGuard lock_guard(lock_);
  ObIArray<ObSnapshotInfo> &snapshots = snapshots_[cur_idx_];

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(get_multi_version_duration(duration))) {
    STORAGE_LOG(WARN, "fail to get multi version duration", K(ret), K(tablet_id));
  } else {
    if (merge_snapshot_version < 1) {
      freeze_info.freeze_version = 0;
    } else if (OB_FAIL(get_freeze_info_behind_snapshot_version_(merge_snapshot_version, freeze_info))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("failed to get freeze info behind snapshot", K(ret), K(merge_snapshot_version));
      } else {
        freeze_info.freeze_version = INT64_MAX;
        ret = OB_SUCCESS;
      }
    }

    snapshot_version = std::max(0L, snapshot_gc_ts_ - duration * 1000L * 1000L * 1000L);
    snapshot_from_type = "undo_retention";

    const int64_t snapshot_version_for_tx = get_min_reserved_snapshot_for_tx();
    if (snapshot_version_for_tx < snapshot_version) {
      snapshot_version = snapshot_version_for_tx;
      snapshot_from_type = "snapshot_for_tx";
    }

    if (freeze_info.freeze_version < snapshot_version) {
      snapshot_version = freeze_info.freeze_version;
      snapshot_from_type = "major_freeze_ts";
    }

    for (int64_t i = 0; i < snapshots.count() && OB_SUCC(ret); ++i) {
      bool related = false;
      const ObSnapshotInfo &snapshot = snapshots.at(i);
      if (OB_FAIL(is_snapshot_related_to_tablet(tablet_id, snapshot, related))) {
        STORAGE_LOG(WARN, "fail to check snapshot relation", K(ret), K(tablet_id), K(snapshot));
      } else if (related && snapshot.snapshot_scn_.get_val_for_tx() < snapshot_version) {
        snapshot_version = snapshot.snapshot_scn_.get_val_for_tx();
        snapshot_from_type = snapshot.get_snapshot_type_str();
      }
    }
  }
  return ret;
}

int ObTenantFreezeInfoMgr::get_reserve_points(
    const int64_t tenant_id,
    const share::ObSnapShotType snapshot_type,
    ObIArray<ObSnapshotInfo> &restore_points,
    int64_t &snapshot_gc_ts)
{
  int ret = OB_SUCCESS;
  restore_points.reset();
  if (!is_valid_tenant_id(tenant_id) || tenant_id != tenant_id_) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get invalid tenant id", K(ret), K(tenant_id), K(tenant_id_));
  } else {
    RLockGuard lock_guard(lock_);
    snapshot_gc_ts = snapshot_gc_ts_;
    ObIArray<ObSnapshotInfo> &snapshots = snapshots_[cur_idx_];
    for (int64_t i = 0; i < snapshots.count() && OB_SUCC(ret); ++i) {
      const ObSnapshotInfo &snapshot = snapshots.at(i);
      if (snapshot.snapshot_type_ == snapshot_type
          && tenant_id == snapshot.tenant_id_) {
        if (OB_FAIL(restore_points.push_back(snapshot))) {
          STORAGE_LOG(WARN, "fail to push back snapshot", K(ret), K(snapshot));
        }
      }
    }
  }
  return ret;
}

// int ObTenantFreezeInfoMgr::get_restore_point_min_schema_version(
//     const int64_t tenant_id,
//     int64_t &schema_version)
// {
//   int ret = OB_SUCCESS;
//   schema_version = INT64_MAX;
//   if (!is_valid_tenant_id(tenant_id) || tenant_id != tenant_id_) {
//     ret = OB_INVALID_ARGUMENT;
//     STORAGE_LOG(WARN, "get invalid tenant id", K(ret), K(tenant_id), K(tenant_id_));
//   } else {
//     RLockGuard lock_guard(lock_);
//     ObIArray<ObSnapshotInfo> &snapshots = snapshots_[cur_idx_];
//     for (int64_t i = 0; i < snapshots.count() && OB_SUCC(ret); ++i) {
//       const ObSnapshotInfo &snapshot = snapshots.at(i);
//       if (snapshot.snapshot_type_ == SNAPSHOT_FOR_RESTORE_POINT
//           && tenant_id == snapshot.tenant_id_) {
//         schema_version = std::min(snapshot.schema_version_, schema_version);
//       }
//     }
//   }
//   return ret;
// }

int ObTenantFreezeInfoMgr::get_latest_freeze_version(int64_t &freeze_version)
{
  int ret = OB_SUCCESS;

  RLockGuard lock_guard(lock_);
  ObIArray<FreezeInfo> &info_list = info_list_[cur_idx_];

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else {
    freeze_version = 0;

    if (info_list.count() > 0) {
      freeze_version = info_list.at(info_list.count() - 1).freeze_version;
    }
  }

  return ret;
}

int ObTenantFreezeInfoMgr::prepare_new_info_list(const int64_t min_major_snapshot)
{
  int ret = OB_SUCCESS;

  int64_t next_idx = get_next_idx();
  info_list_[next_idx].reset();
  snapshots_[next_idx].reset();

  for (int64_t i = 0; i < info_list_[cur_idx_].count() && OB_SUCC(ret); ++i) {
    if (INT64_MAX == min_major_snapshot || // no garbage collection is necessary
        // or version is bigger or equal than the smallest major version currently
        info_list_[cur_idx_].at(i).freeze_version >= min_major_snapshot) {
      if (OB_FAIL(info_list_[next_idx].push_back(info_list_[cur_idx_].at(i)))) {
        STORAGE_LOG(WARN, "fail to push back info", K(ret));
      }
    } else {
      STORAGE_LOG(INFO, "update info, gc info list", K(info_list_[cur_idx_].at(i)));
    }
  }

  return ret;
}

int ObTenantFreezeInfoMgr::update_next_info_list(const ObIArray<FreezeInfo> &info_list)
{
  int ret = OB_SUCCESS;

  int64_t next_idx = get_next_idx();
  ObIArray<FreezeInfo> &next_info_list = info_list_[next_idx];

  for (int64_t i = 0; OB_SUCC(ret) && i < info_list.count(); ++i) {
    const FreezeInfo &next = info_list.at(i);
    if (next_info_list.count() > 0) {
      FreezeInfo &prev = next_info_list.at(next_info_list.count() - 1);
      if (OB_UNLIKELY(prev.freeze_version > next.freeze_version)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "freeze version decrease is not allowed", K(ret), K(prev), K(next));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(next_info_list.push_back(next))) {
        STORAGE_LOG(WARN, "failed to push back freeze info", K(ret));
      }
    }
  }

  return ret;
}

int ObTenantFreezeInfoMgr::update_next_snapshots(const ObIArray<ObSnapshotInfo> &snapshots)
{
  int ret = OB_SUCCESS;

  int64_t next_idx = get_next_idx();
  ObIArray<ObSnapshotInfo> &next_snapshots = snapshots_[next_idx];

  for (int64_t i = 0; OB_SUCC(ret) && i < snapshots.count(); ++i) {
    if (OB_FAIL(next_snapshots.push_back(snapshots.at(i)))) {
      STORAGE_LOG(WARN, "fail to push back snapshot", K(ret));
    }
  }
  return ret;
}

int ObTenantFreezeInfoMgr::update_info(
    const int64_t snapshot_gc_ts,
    const ObIArray<FreezeInfo> &info_list,
    const ObIArray<ObSnapshotInfo> &snapshots,
    const int64_t min_major_snapshot,
    bool& gc_snapshot_ts_changed)
{
  int ret = OB_SUCCESS;


  WLockGuard lock_guard(lock_);

  gc_snapshot_ts_changed = snapshot_gc_ts != snapshot_gc_ts_;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_UNLIKELY(snapshot_gc_ts < snapshot_gc_ts_)) {
    STORAGE_LOG(INFO, "update snapshot gc ts is smaller than snapshot_gc_ts cache!", K(snapshot_gc_ts_), K(snapshot_gc_ts));
  } else if (OB_FAIL(prepare_new_info_list(min_major_snapshot))) {
    STORAGE_LOG(WARN, "fail to prepare new info list", K(ret));
  } else if (OB_FAIL(update_next_info_list(info_list))) {
    STORAGE_LOG(WARN, "fail to update next info list", K(ret));
  } else if (OB_FAIL(update_next_snapshots(snapshots))) {
    STORAGE_LOG(WARN, "fail to update next snapshots", K(ret));
  } else {
    switch_info();
    snapshot_gc_ts_ = snapshot_gc_ts;
    if (REACH_TENANT_TIME_INTERVAL(20 * 1000 * 1000 /*20s*/)) {
      STORAGE_LOG(INFO, "ObTenantFreezeInfoMgr success to update infos", K_(snapshot_gc_ts), K(min_major_snapshot),
                  K_(cur_idx), K(info_list_[0]), K(info_list_[1]), K(snapshots_[0]), K(snapshots_[1]));
    }
  }

  return ret;
}

int64_t ObTenantFreezeInfoMgr::get_snapshot_gc_ts()
{
  RLockGuard lock_guard(lock_);
  return snapshot_gc_ts_;
}

ObTenantFreezeInfoMgr::ReloadTask::ReloadTask(ObTenantFreezeInfoMgr &mgr)
  : inited_(false),
    check_tenant_status_(true),
    mgr_(mgr),
    sql_proxy_(NULL),
    snapshot_proxy_(),
    last_change_ts_(0)
{
}

int ObTenantFreezeInfoMgr::ReloadTask::init(ObISQLClient &sql_proxy)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else {
    sql_proxy_ = &sql_proxy;
    last_change_ts_ = ObTimeUtility::current_time();
    inited_ = true;
  }
  return ret;
}

int ObTenantFreezeInfoMgr::ReloadTask::get_global_info(int64_t &snapshot_gc_ts)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  ObGlobalStatProxy stat_proxy(*sql_proxy_, tenant_id);
  SCN snapshot_gc_scn;
  if (OB_FAIL(stat_proxy.get_snapshot_gc_scn(snapshot_gc_scn))) {
    if (OB_TENANT_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "fail to get global info", K(ret), K(tenant_id));
    }
  } else {
    // TODO SCN
    snapshot_gc_ts = snapshot_gc_scn.get_val_for_tx();
  }

  return ret;
}

int ObTenantFreezeInfoMgr::ReloadTask::get_freeze_info(
    int64_t &min_major_version,
    ObIArray<FreezeInfo> &freeze_info)
{
  int ret = OB_SUCCESS;

  int64_t freeze_version = 0;
  SCN freeze_scn = SCN::min_scn();
  SCN min_major_scn = SCN::max_scn();
  min_major_version = INT64_MAX;
  ObSEArray<ObSimpleFrozenStatus, 8> tmp;
  ObFreezeInfoProxy freeze_info_proxy(MTL_ID());

  if (OB_FAIL(mgr_.get_latest_freeze_version(freeze_version))) {
    STORAGE_LOG(WARN, "fail to get major version", K(ret));
  } else if (OB_FAIL(freeze_scn.convert_for_tx(freeze_version))) {
    STORAGE_LOG(WARN, "fail to convert to scn", K(ret), K(freeze_version));
  } else if (OB_FAIL(freeze_info_proxy.get_min_major_available_and_larger_info(
             *sql_proxy_,
             freeze_scn,
             min_major_scn,
             tmp))) {
    STORAGE_LOG(WARN, "fail to get freeze info", K(ret), K(freeze_scn));
  } else if (OB_UNLIKELY(!min_major_scn.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "get unexpected invalid scn", K(ret), K(min_major_scn));
  } else if (FALSE_IT(min_major_version = min_major_scn.get_val_for_tx())) {
  } else {
    for (int64_t i = 0; i < tmp.count() && OB_SUCC(ret); ++i) {
      ObSimpleFrozenStatus &status = tmp.at(i);
      if (OB_FAIL(freeze_info.push_back(
            FreezeInfo(status.frozen_scn_.get_val_for_tx(), status.schema_version_, status.data_version_)))) {
        STORAGE_LOG(WARN, "fail to push back freeze info", K(ret), K(status));
      }
    }
    if (OB_SUCC(ret) && tmp.count() > 0) {
      compaction::ADD_COMPACTION_EVENT(
          MTL_ID(),
          MAJOR_MERGE,
          tmp.at(tmp.count() - 1).frozen_scn_.get_val_for_tx(),
          compaction::ObServerCompactionEvent::GET_FREEZE_INFO,
          ObTimeUtility::fast_current_time(),
          "new_freeze_info_cnt",
          tmp.count(),
          "latest_freeze_scn",
          tmp.at(tmp.count() - 1).frozen_scn_);
    }
  }

  return ret;
}

int ObTenantFreezeInfoMgr::ReloadTask::refresh_merge_info()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is nullptr", KR(ret));
  } else {
    const uint64_t tenant_id = MTL_ID();
    ObZoneMergeInfo zone_merge_info;
    zone_merge_info.tenant_id_ = tenant_id;
    zone_merge_info.zone_ = GCTX.config_->zone.str();

    ObGlobalMergeInfo global_merge_info;
    global_merge_info.tenant_id_ = tenant_id;

    if (OB_FAIL(ObGlobalMergeTableOperator::load_global_merge_info(*sql_proxy_, tenant_id, global_merge_info))) {
      LOG_WARN("failed to load global merge info", KR(ret), K(global_merge_info));
    } else if (OB_FAIL(ObZoneMergeTableOperator::load_zone_merge_info(*sql_proxy_, tenant_id, zone_merge_info))) {
      LOG_WARN("fail to load zone merge info", KR(ret), K(zone_merge_info));
    } else {
      ObTenantTabletScheduler *scheduler = MTL(ObTenantTabletScheduler *);
      scheduler->set_inner_table_merged_scn(global_merge_info.last_merged_scn_.get_scn().get_val_for_tx()); // set merged version
      if (global_merge_info.suspend_merging_.get_value()) { // suspend_merge
        scheduler->stop_major_merge();
        LOG_INFO("schedule zone to stop major merge", K(tenant_id), K(zone_merge_info), K(global_merge_info));
      } else {
        if (check_tenant_status_) {
          bool is_restore = false;
          if (OB_FAIL(ObMultiVersionSchemaService::get_instance().check_tenant_is_restore(nullptr, tenant_id, is_restore))) {
            LOG_WARN("failed to check tenant is restore", K(ret));
          } else if (is_restore) {
            if (REACH_TENANT_TIME_INTERVAL(10L * 1000L * 1000L)) {
              LOG_INFO("skip restoring tenant to schedule major merge", K(tenant_id), K(is_restore));
            }
          } else {
            check_tenant_status_ = false;
            LOG_INFO("finish check tenant restore", K(tenant_id), K(is_restore));
          }
        }
        if (!check_tenant_status_) {
          scheduler->resume_major_merge();
          const int64_t scheduler_frozen_version = scheduler->get_frozen_version();
          if (zone_merge_info.broadcast_scn_.get_scn().get_val_for_tx() > scheduler_frozen_version) {
            FLOG_INFO("try to schedule merge", K(tenant_id), "zone", zone_merge_info.zone_, "broadcast_scn",
              zone_merge_info.broadcast_scn_, K(scheduler_frozen_version));
            if (OB_FAIL(scheduler->schedule_merge(zone_merge_info.broadcast_scn_.get_scn().get_val_for_tx()))) {
              LOG_WARN("fail to schedule merge", K(ret), K(zone_merge_info));
            }
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      LOG_TRACE("refresh merge info", K(tenant_id), "zone", zone_merge_info.zone_, "broadcast_scn",
        zone_merge_info.broadcast_scn_);
    }
  }
  return ret;
}

int ObTenantFreezeInfoMgr::ReloadTask::try_update_info()
{
  int ret = OB_SUCCESS;

  DEBUG_SYNC(BEFORE_UPDATE_FREEZE_SNAPSHOT_INFO);

  if (OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(ERROR, "sql_proxy is NULL", K(ret));
  } else {
    int64_t snapshot_gc_ts = 0;
    // snapshot_gc_ts should be obtained before freeze_info and snapshots
    ObSEArray<FreezeInfo, 4> freeze_info;
    ObSEArray<ObSnapshotInfo, 4> snapshots;
    bool gc_snapshot_ts_changed = false;
    int64_t min_major_snapshot = INT64_MAX;

    if (OB_FAIL(get_global_info(snapshot_gc_ts))) {
      if (OB_TENANT_NOT_EXIST != ret) {
        STORAGE_LOG(WARN, "failed to get global info", K(ret));
      } else {
        STORAGE_LOG(WARN, "tenant not exists, maybe has been removed", K(ret), K(MTL_ID()));
      }
    } else if (OB_FAIL(get_freeze_info(min_major_snapshot,
                                       freeze_info))) {
      STORAGE_LOG(WARN, "failed to get freeze info", K(ret));
    } else if (OB_FAIL(snapshot_proxy_.get_all_snapshots(*sql_proxy_, MTL_ID(), snapshots))) {
      STORAGE_LOG(WARN, "failed to get snapshots", K(ret));
    } else if (OB_FAIL(mgr_.update_info(snapshot_gc_ts,
                                        freeze_info,
                                        snapshots,
                                        min_major_snapshot,
                                        gc_snapshot_ts_changed))) {
      STORAGE_LOG(WARN, "update info failed",
                  K(ret), K(snapshot_gc_ts), K(freeze_info), K(snapshots));
    } else {
      if (gc_snapshot_ts_changed) {
        last_change_ts_ = ObTimeUtility::current_time();
      } else {
        const int64_t last_not_change_interval_us = ObTimeUtility::current_time() - last_change_ts_;
        if (MAX_GC_SNAPSHOT_TS_REFRESH_TS <= last_not_change_interval_us &&
            (0 != snapshot_gc_ts && 1 != snapshot_gc_ts)) {
          if (REACH_TENANT_TIME_INTERVAL(60L * 1000L * 1000L)) {
            STORAGE_LOG(WARN, "snapshot_gc_ts not refresh too long",
                        K(snapshot_gc_ts), K(freeze_info), K(snapshots), K(last_change_ts_),
                        K(last_not_change_interval_us));
          }
        } else if (FLUSH_GC_SNAPSHOT_TS_REFRESH_TS <= last_not_change_interval_us) {
          STORAGE_LOG(WARN, "snapshot_gc_ts not refresh too long",
                      K(snapshot_gc_ts), K(freeze_info), K(snapshots), K(last_change_ts_),
                      K(last_not_change_interval_us));
        }
      }

      STORAGE_LOG(DEBUG, "reload freeze info and snapshots",
                  K(snapshot_gc_ts), K(freeze_info), K(snapshots));
    }
  }
  return ret;
}

void ObTenantFreezeInfoMgr::ReloadTask::runTimerTask()
{
  int tmp_ret = OB_SUCCESS;
  if (!ObServerCheckpointSlogHandler::get_instance().is_started()) {
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000 /* 10s */)) {
      LOG_WARN_RET(tmp_ret, "slog replay hasn't finished, this task can't start");
    }
  } else {
    if (OB_TMP_FAIL(refresh_merge_info())) {
      LOG_WARN_RET(tmp_ret, "fail to refresh merge info", KR(tmp_ret));
    }
    if (OB_TMP_FAIL(try_update_info())) {
      LOG_WARN_RET(tmp_ret, "fail to try update info", KR(tmp_ret));
    }
  }
}

void ObTenantFreezeInfoMgr::UpdateLSResvSnapshotTask::runTimerTask()
{
  int tmp_ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  if (OB_TMP_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), compat_version))) {
    LOG_WARN_RET(tmp_ret, "fail to get data version", K(tmp_ret));
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
      reserved_snapshot = std::max(0L, snapshot_gc_ts_ - duration * 1000L * 1000L *1000L);
      LOG_INFO("success to update min reserved snapshot", K(reserved_snapshot), K(duration), K(snapshot_gc_ts_));
    }
  } // end of lock

  // loop all ls, try update reserved snapshot
  ObSharedGuard<ObLSIterator> ls_iter_guard;
  ObLS *ls = nullptr;
  if (OB_FAIL(ret) || reserved_snapshot <= 0) {
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls_iter(ls_iter_guard, ObLSGetMod::STORAGE_MOD))) {
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
