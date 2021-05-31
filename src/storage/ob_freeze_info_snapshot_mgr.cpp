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
#include "storage/ob_freeze_info_snapshot_mgr.h"

#include "common/storage/ob_freeze_define.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/utility/ob_print_utils.h"
#include "share/ob_global_stat_proxy.h"
#include "share/ob_tenant_mgr.h"
#include "share/ob_thread_mgr.h"
#include "share/ob_zone_table_operation.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/ob_multi_cluster_util.h"
#include "share/backup/ob_backup_info_mgr.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_service.h"
#include "storage/ob_partition_service.h"

namespace oceanbase {

using namespace common;
using namespace share;
using namespace share::schema;

using common::hash::ObHashSet;

namespace storage {

ObFreezeInfoSnapshotMgr::ObFreezeInfoSnapshotMgr()
    : inited_(false),
      tg_id_(-1),
      reload_task_(*this, schema_query_set_),
      schema_query_set_(schema_cache_),
      snapshot_gc_ts_(0),
      gc_snapshot_info_(),
      schema_cache_(schema_query_set_),
      info_list_(),
      snapshots_(),
      backup_snapshot_version_(0),
      delay_delete_snapshot_version_(0),
      cur_idx_(0),
      lock_()
{}

ObFreezeInfoSnapshotMgr::~ObFreezeInfoSnapshotMgr()
{}

int ObFreezeInfoSnapshotMgr::init(ObISQLClient& sql_proxy, bool is_remote)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else if (OB_FAIL(schema_query_set_.init())) {
    STORAGE_LOG(ERROR, "fail to init schema query queue", K(ret));
  } else if (OB_FAIL(reload_task_.init(sql_proxy, is_remote))) {
    STORAGE_LOG(ERROR, "fail to init reload task", K(ret));
  } else if (OB_FAIL(schema_cache_.init(sql_proxy))) {
    STORAGE_LOG(ERROR, "fail to init schema cache", K(ret));
  } else if (OB_FAIL(TG_CREATE(lib::TGDefIDs::FreInfoReload, tg_id_))) {
    STORAGE_LOG(ERROR, "fail to init timer", K(ret));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    STORAGE_LOG(ERROR, "fail to init timer", K(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

int ObFreezeInfoSnapshotMgr::start()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id_, reload_task_, RELOAD_INTERVAL, true))) {
    STORAGE_LOG(ERROR, "fail to schedule task", K(ret));
  }

  return ret;
}

void ObFreezeInfoSnapshotMgr::wait()
{
  TG_WAIT(tg_id_);
}

void ObFreezeInfoSnapshotMgr::stop()
{
  TG_STOP(tg_id_);
}

static inline uint64_t get_tenant_id(const uint64_t table_id)
{
  // system table of user tenant needs to use the schema version of system
  // tenant
  uint64_t tenant_id = OB_SYS_TENANT_ID;
  if (!is_inner_table(table_id)) {
    tenant_id = extract_tenant_id(table_id);
  }
  return tenant_id;
}

static inline bool is_schema_cache_related_table(const uint64_t table_id)
{
  uint64_t pure_id = extract_pure_id(table_id);
  return OB_ALL_CORE_TABLE_TID == pure_id || OB_ALL_FREEZE_SCHEMA_VERSION_TID == pure_id;
}

int64_t ObFreezeInfoSnapshotMgr::get_latest_frozen_timestamp()
{
  int64_t frozen_timestamp = 0;
  RLockGuard lock_guard(lock_);
  ObIArray<FreezeInfoLite>& info_list = info_list_[cur_idx_];

  if (0 != info_list.count()) {
    frozen_timestamp = info_list.at(info_list.count() - 1).freeze_ts;
  }
  return frozen_timestamp;
}

int ObFreezeInfoSnapshotMgr::get_freeze_info_by_major_version(const int64_t major_version, FreezeInfoLite& freeze_info)
{
  int ret = OB_SUCCESS;
  bool unused = false;
  RLockGuard lock_guard(lock_);
  ret = get_freeze_info_by_major_version_(major_version, freeze_info, unused);
  return ret;
}

int ObFreezeInfoSnapshotMgr::get_freeze_info_by_major_version(
    const int64_t major_version, FreezeInfoLite& freeze_info, bool& is_first_major_version)
{
  int ret = OB_SUCCESS;
  is_first_major_version = false;
  RLockGuard lock_guard(lock_);
  ret = get_freeze_info_by_major_version_(major_version, freeze_info, is_first_major_version);
  return ret;
}

int ObFreezeInfoSnapshotMgr::get_freeze_info_by_major_version_(
    const int64_t major_version, FreezeInfoLite& freeze_info, bool& is_first_major_version)
{
  int ret = OB_SUCCESS;
  is_first_major_version = false;

  ObIArray<FreezeInfoLite>& info_list = info_list_[cur_idx_];

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else {
    bool found = false;
    int64_t l = 0;
    int64_t r = info_list.count();

    while (l < r && !found) {
      int64_t mid = (l + r) >> 1;
      FreezeInfoLite& tmp_info = info_list.at(mid);
      if (major_version < tmp_info.freeze_version) {
        r = mid;
      } else if (major_version > tmp_info.freeze_version) {
        l = mid + 1;
      } else {
        found = true;
        freeze_info = tmp_info;
        if (0 == mid) {
          is_first_major_version = true;
        }
      }
    }

    if (!found) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }

  return ret;
}

int ObFreezeInfoSnapshotMgr::get_freeze_info_behind_major_version(
    const int64_t major_version, ObIArray<FreezeInfoLite>& freeze_infos)
{
  int ret = OB_SUCCESS;
  RLockGuard lock_guard(lock_);

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_UNLIKELY(major_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to get freeze info", K(ret), K(major_version));
  } else {
    ObIArray<FreezeInfoLite>& info_list = info_list_[cur_idx_];
    bool found = false;
    int64_t mid = 0;
    int64_t l = 0;
    int64_t r = info_list.count();

    while (l < r && !found) {
      mid = (l + r) >> 1;
      FreezeInfoLite& tmp_info = info_list.at(mid);
      if (major_version < tmp_info.freeze_version) {
        r = mid;
      } else if (major_version > tmp_info.freeze_version) {
        l = mid + 1;
      } else {
        found = true;
      }
    }
    if (found) {
      for (int64_t i = mid; OB_SUCC(ret) && i < info_list.count(); i++) {
        if (OB_FAIL(freeze_infos.push_back(info_list.at(i)))) {
          STORAGE_LOG(WARN, "Failed to push back freeze info", K(ret));
        }
      }
    } else {
      ret = OB_ENTRY_NOT_EXIST;
      STORAGE_LOG(WARN, "Freeze info of specified major version not found", K(ret), K(major_version));
    }
  }
  return ret;
}

int ObFreezeInfoSnapshotMgr::get_freeze_info_by_major_version(
    const uint64_t table_id, const int64_t major_version, FreezeInfo& freeze_info)
{
  uint64_t tenant_id = get_tenant_id(table_id);
  bool async = is_schema_cache_related_table(table_id);
  return get_tenant_freeze_info_by_major_version_(tenant_id, major_version, async, freeze_info);
}

int ObFreezeInfoSnapshotMgr::get_tenant_freeze_info_by_major_version(
    const uint64_t tenant_id, const int64_t major_version, FreezeInfo& freeze_info)
{
  return get_tenant_freeze_info_by_major_version_(tenant_id, major_version, true, freeze_info);
}

int ObFreezeInfoSnapshotMgr::get_tenant_freeze_info_by_major_version_(
    const uint64_t tenant_id, const int64_t major_version, const bool async, FreezeInfo& freeze_info)
{
  int ret = OB_SUCCESS;

  FreezeInfoLite lite;
  if (OB_FAIL(get_freeze_info_by_major_version(major_version, lite))) {
    STORAGE_LOG(WARN, "fail to get lite freeze info", K(ret), K(major_version));
  } else if (OB_FAIL(schema_cache_.get_freeze_schema_version(
                 tenant_id, lite.freeze_version, async, freeze_info.schema_version))) {
    if (OB_EAGAIN == ret) {
      STORAGE_LOG(INFO, "schema not ready, try again later", K(ret), K(tenant_id), K(lite));
    } else {
      STORAGE_LOG(WARN, "fail to get freeze schema version", K(ret), K(tenant_id), K(lite.freeze_version));
    }
  } else {
    freeze_info = lite;
  }

  return ret;
}

int ObFreezeInfoSnapshotMgr::get_freeze_info_by_snapshot_version(
    const int64_t snapshot_version, FreezeInfoLite& freeze_info)
{
  int ret = OB_SUCCESS;

  RLockGuard lock_guard(lock_);
  ObIArray<FreezeInfoLite>& info_list = info_list_[cur_idx_];

  if (snapshot_version <= 0 || INT64_MAX == snapshot_version) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "snapshot version is invalid", K(ret), K(snapshot_version));
  } else if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (info_list.empty()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("not freeze info exists", K(ret));
  } else {
    bool found = false;
    for (int64_t i = 0; OB_SUCC(ret) && !found && i < info_list.count(); ++i) {
      FreezeInfoLite& tmp_info = info_list.at(i);
      if (snapshot_version == tmp_info.freeze_ts) {
        freeze_info = tmp_info;
        found = true;
      }
    }

    if (OB_SUCC(ret)) {
      if (!found) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "can not find the freeze info", K(ret), K(snapshot_version));
      }
    }
  }

  return ret;
}

int ObFreezeInfoSnapshotMgr::get_freeze_info_by_snapshot_version(
    const uint64_t table_id, const int64_t snapshot_version, FreezeInfo& freeze_info)
{
  int ret = OB_SUCCESS;

  FreezeInfoLite lite;
  if (OB_FAIL(get_freeze_info_by_snapshot_version(snapshot_version, lite))) {
    STORAGE_LOG(WARN, "fail to get lite freeze info", K(ret), K(snapshot_version));
  } else if (OB_FAIL(schema_cache_.get_freeze_schema_version(get_tenant_id(table_id),
                 lite.freeze_version,
                 is_schema_cache_related_table(table_id),
                 freeze_info.schema_version))) {
    if (OB_EAGAIN == ret) {
      STORAGE_LOG(INFO, "schema not ready, try again later", K(ret), K(table_id), K(lite));
    } else {
      STORAGE_LOG(WARN, "fail to get freeze schema version", K(ret), K(table_id), K(lite.freeze_version));
    }
  } else {
    freeze_info = lite;
  }

  return ret;
}

int ObFreezeInfoSnapshotMgr::inner_get_neighbour_major_freeze(
    const int64_t snapshot_version, NeighbourFreezeInfoLite& info)
{
  int ret = OB_SUCCESS;

  info.reset();

  ObIArray<FreezeInfoLite>& info_list = info_list_[cur_idx_];

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (info_list.empty()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("not freeze info exists", K(ret));
  } else {
    bool found = false;
    for (int64_t i = 0; i < info_list.count() && OB_SUCC(ret) && !found; ++i) {
      FreezeInfoLite& next_info = info_list.at(i);
      if (snapshot_version < next_info.freeze_ts) {
        found = true;
        if (0 == i) {
          if (!GCTX.is_standby_cluster()) {
            ret = OB_ERR_SYS;
            LOG_ERROR("cannot get neighbour major freeze before bootstrap", K(ret), K(snapshot_version), K(next_info));
          } else {
            ret = OB_ENTRY_NOT_EXIST;
          }
        } else {
          info.next = next_info;
          info.prev = info_list.at(i - 1);
        }
      }
    }

    if (OB_SUCC(ret) && !found) {
      info.next.freeze_version = 0;
      info.next.freeze_ts = snapshot_gc_ts_;
      info.prev = info_list.at(info_list.count() - 1);
    }
  }

  return ret;
}

int ObFreezeInfoSnapshotMgr::get_neighbour_major_freeze(const int64_t snapshot_version, NeighbourFreezeInfoLite& info)
{
  RLockGuard lock_guard(lock_);
  return inner_get_neighbour_major_freeze(snapshot_version, info);
}

int ObFreezeInfoSnapshotMgr::get_neighbour_major_freeze(
    const uint64_t table_id, const int64_t snapshot_version, NeighbourFreezeInfo& info)
{
  int ret = OB_SUCCESS;

  NeighbourFreezeInfoLite lite;

  {
    RLockGuard lock_guard(lock_);
    if (OB_FAIL(inner_get_neighbour_major_freeze(snapshot_version, lite))) {
      STORAGE_LOG(WARN, "fail to get lite neighbour freeze info", K(ret), K(snapshot_version));
    } else if (0 == lite.next.freeze_version) {
      info = lite;
      // obtain the schema_version based on gc_snapshot_ts
      GCSnapshotInfo finfo;
      if (OB_FAIL(gc_snapshot_info_[cur_idx_].get_refactored(get_tenant_id(table_id), finfo))) {
        if (OB_HASH_NOT_EXIST == ret) {
          STORAGE_LOG(WARN, "tenant not exist in gc snapshot hash", K(ret), K(table_id));
        } else {
          STORAGE_LOG(WARN, "fail to get gc_snapshot_info", K(ret));
        }
      } else {
        info.next.freeze_ts = finfo.snapshot_ts;
        info.next.schema_version = finfo.schema_version;
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (lite.next.freeze_version > 0) {
    info = lite;
    if (OB_FAIL(schema_cache_.get_freeze_schema_version(get_tenant_id(table_id),
            lite.next.freeze_version,
            is_schema_cache_related_table(table_id),
            info.next.schema_version))) {
      if (OB_EAGAIN == ret) {
        STORAGE_LOG(INFO, "schema not ready, try again later", K(ret), K(table_id), K(lite.next));
      } else {
        STORAGE_LOG(WARN, "fail to get freeze schema version", K(ret), K(table_id), K(lite.next.freeze_version));
      }
    }
  }

  return ret;
}

static inline int is_snapshot_related_to_table(
    const ObPartitionKey& pkey, const int64_t create_schema_version, const ObSnapshotInfo& snapshot, bool& related)
{
  int ret = OB_SUCCESS;
  related = false;
  const int64_t table_id = pkey.get_table_id();

  if (!snapshot.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(snapshot));
  } else if ((snapshot.snapshot_type_ == share::SNAPSHOT_FOR_RESTORE_POINT && !is_inner_table(table_id)) ||
             snapshot.snapshot_type_ == share::SNAPSHOT_FOR_BACKUP_POINT) {
    if (extract_tenant_id(table_id) == snapshot.tenant_id_) {
      related = true;
      bool is_complete = false;
      if (create_schema_version > snapshot.schema_version_ && !is_inner_table(table_id)) {
        related = false;
      } else if (OB_FAIL(ObPartitionService::get_instance().check_restore_point_complete(
                     pkey, snapshot.snapshot_ts_, is_complete))) {
        STORAGE_LOG(WARN, "failed to check restore point exist", K(ret));
      } else if (is_complete) {
        related = false;
      }
    }
  } else {
    // when tenant_id_ equals to 0, it means need all tenants
    if (0 == snapshot.tenant_id_ || extract_tenant_id(table_id) == snapshot.tenant_id_) {
      // when table_id_ equals to 0, it means need all tables in tenant
      if (0 == snapshot.table_id_ || snapshot.table_id_ == table_id) {
        related = true;
      }
    }
  }
  return ret;
}

int ObFreezeInfoSnapshotMgr::get_multi_version_duration(const uint64_t tenant_id, int64_t& duration) const
{
  int ret = OB_SUCCESS;

  ObMultiVersionSchemaService& schema_service = ObMultiVersionSchemaService::get_instance();
  ObSchemaGetterGuard guard;
  ObString var_name(share::OB_SV_UNDO_RETENTION);
  const ObSysVarSchema* var_schema = NULL;
  ObArenaAllocator allocator(ObModIds::OB_PARTITION_SERVICE);
  ObObj value;

  if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service.get_tenant_full_schema_guard(tenant_id, guard))) {
    STORAGE_LOG(WARN, "get schema guard failed", K(ret), K(tenant_id));
  } else if (OB_FAIL(guard.get_tenant_system_variable(tenant_id, var_name, var_schema))) {
    STORAGE_LOG(WARN, "get tenant system variable failed", K(ret));
  } else if (NULL == var_schema) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unexpected error, var schema is NULL", K(ret));
  } else if (OB_FAIL(var_schema->get_value(&allocator, NULL, value))) {
    STORAGE_LOG(WARN, "get value failed", K(ret));
  } else if (OB_FAIL(value.get_int(duration))) {
    STORAGE_LOG(WARN, "get duration failed", K(ret));
  }

  return ret;
}

int ObFreezeInfoSnapshotMgr::get_min_reserved_snapshot(const ObPartitionKey& pkey, const int64_t merged_version,
    const int64_t create_schema_version, int64_t& snapshot_version, int64_t& backup_snapshot_version)
{
  int ret = OB_SUCCESS;
  FreezeInfoLite freeze_info;
  int64_t duration = 0;
  bool unused = false;
  backup_snapshot_version = 0;
  const int64_t table_id = pkey.get_table_id();

  RLockGuard lock_guard(lock_);
  ObIArray<ObSnapshotInfo>& snapshots = snapshots_[cur_idx_];

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(get_multi_version_duration(get_tenant_id(table_id), duration))) {
    STORAGE_LOG(WARN, "fail to get multi version duration", K(ret), K(table_id));
  } else {
    if (merged_version < 1) {
      freeze_info.freeze_ts = 0;
    } else if (OB_FAIL(get_freeze_info_by_major_version_(merged_version + 1, freeze_info, unused))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("failed to get_freeze_info_by_major_version", K(ret), K(merged_version));
      } else {
        freeze_info.freeze_ts = INT64_MAX;
        ret = OB_SUCCESS;
      }
    }
    snapshot_version = std::max(0L, snapshot_gc_ts_ - duration * 1000L * 1000L);
    snapshot_version = std::min(snapshot_version, freeze_info.freeze_ts);
    backup_snapshot_version = backup_snapshot_version_;
    for (int64_t i = 0; i < snapshots.count() && OB_SUCC(ret); ++i) {
      bool related = false;
      const ObSnapshotInfo& snapshot = snapshots.at(i);
      if (OB_FAIL(is_snapshot_related_to_table(pkey, create_schema_version, snapshot, related))) {
        STORAGE_LOG(WARN, "fail to check snapshot relation", K(ret), K(table_id), K(snapshot));
      } else if (related) {
        snapshot_version = std::min(snapshot_version, snapshot.snapshot_ts_);
      }
    }
  }
  return ret;
}

int ObFreezeInfoSnapshotMgr::get_reserve_points(const int64_t tenant_id, const share::ObSnapShotType snapshot_type,
    ObIArray<ObSnapshotInfo>& restore_points, int64_t& snapshot_gc_ts)
{
  int ret = OB_SUCCESS;
  restore_points.reset();
  RLockGuard lock_guard(lock_);
  snapshot_gc_ts = snapshot_gc_ts_;
  ObIArray<ObSnapshotInfo>& snapshots = snapshots_[cur_idx_];
  for (int64_t i = 0; i < snapshots.count() && OB_SUCC(ret); ++i) {
    const ObSnapshotInfo& snapshot = snapshots.at(i);
    if (snapshot.snapshot_type_ == snapshot_type && tenant_id == snapshot.tenant_id_) {
      if (OB_FAIL(restore_points.push_back(snapshot))) {
        STORAGE_LOG(WARN, "fail to push back snapshot", K(ret), K(snapshot));
      }
    }
  }
  return ret;
}

int ObFreezeInfoSnapshotMgr::get_latest_freeze_version(int64_t& freeze_version)
{
  int ret = OB_SUCCESS;

  RLockGuard lock_guard(lock_);
  ObIArray<FreezeInfoLite>& info_list = info_list_[cur_idx_];

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

int ObFreezeInfoSnapshotMgr::prepare_new_info_list(const int64_t min_major_version)
{
  int ret = OB_SUCCESS;

  int64_t next_idx = get_next_idx();
  info_list_[next_idx].reset();
  snapshots_[next_idx].reset();
  gc_snapshot_info_[next_idx].reuse();

  for (int64_t i = 0; i < info_list_[cur_idx_].count() && OB_SUCC(ret); ++i) {
    if (INT64_MAX == min_major_version ||  // no garbage collection is necessary
                                           // or version is bigger or equal than the smallest major version currently
        info_list_[cur_idx_].at(i).freeze_version >= min_major_version) {
      if (OB_FAIL(info_list_[next_idx].push_back(info_list_[cur_idx_].at(i)))) {
        STORAGE_LOG(WARN, "fail to push back info", K(ret));
      }
    } else {
      STORAGE_LOG(INFO, "update info, gc info list", K(info_list_[cur_idx_].at(i)));
    }
  }

  if (gc_snapshot_info_[cur_idx_].created()) {
    if (!gc_snapshot_info_[next_idx].created()) {
      if (OB_FAIL(gc_snapshot_info_[next_idx].create(1024, ObModIds::OB_FREEZE_INFO_CACHE))) {
        STORAGE_LOG(WARN, "fail to create hash bucket", K(ret));
      }
    }

    for (hash::ObHashMap<uint64_t, GCSnapshotInfo>::iterator it = gc_snapshot_info_[cur_idx_].begin();
         it != gc_snapshot_info_[cur_idx_].end() && OB_SUCC(ret);
         ++it) {
      if (OB_FAIL(gc_snapshot_info_[next_idx].set_refactored(it->first, it->second))) {
        STORAGE_LOG(WARN, "fail to push back into gc_snapshot_info", K(ret));
      }
    }
  }

  return ret;
}

int ObFreezeInfoSnapshotMgr::update_next_gc_schema_version(
    const ObIArray<SchemaPair>& gc_schema_version, const int64_t gc_snapshot_ts)
{
  int ret = OB_SUCCESS;

  int64_t next_idx = get_next_idx();
  hash::ObHashMap<uint64_t, GCSnapshotInfo>& next_gc_snapshot_info = gc_snapshot_info_[next_idx];

  if (!next_gc_snapshot_info.created()) {
    if (OB_FAIL(next_gc_snapshot_info.create(1024, ObModIds::OB_FREEZE_INFO_CACHE))) {
      STORAGE_LOG(WARN, "fail to create hash bucket", K(ret));
    }
  }

  for (int64_t i = 0; i < gc_schema_version.count() && OB_SUCC(ret); ++i) {
    const SchemaPair& pair = gc_schema_version.at(i);
    if (OB_INVALID_VERSION != pair.schema_version) {
      if (OB_FAIL(next_gc_snapshot_info.set_refactored(
              pair.tenant_id, GCSnapshotInfo(gc_snapshot_ts, pair.schema_version), 1 /*overwrite*/))) {
        STORAGE_LOG(WARN, "fail to set schema version", K(ret));
      }
    } else {
      STORAGE_LOG(WARN,
          "receive invalid schema version from rs, may be memory explosion",
          K(pair.tenant_id),
          K(gc_snapshot_ts));
    }
  }

  if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
    STORAGE_LOG(INFO, "update info", "gc schema version", gc_schema_version, K(gc_snapshot_ts));
  }

  return ret;
}

int ObFreezeInfoSnapshotMgr::update_next_info_list(const ObIArray<FreezeInfoLite>& info_list)
{
  int ret = OB_SUCCESS;

  int64_t next_idx = get_next_idx();
  ObIArray<FreezeInfoLite>& next_info_list = info_list_[next_idx];

  for (int64_t i = 0; i < info_list.count() && OB_SUCC(ret); ++i) {
    const FreezeInfoLite& next = info_list.at(i);
    if (next_info_list.count() > 0) {
      FreezeInfoLite& prev = next_info_list.at(next_info_list.count() - 1);
      if (OB_UNLIKELY(prev.freeze_version > next.freeze_version)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "freeze verison decrease is not allowed", K(ret), K(prev), K(next));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(next_info_list.push_back(next))) {
        STORAGE_LOG(WARN, "fail to push back freeze info", K(ret));
      } else {
        STORAGE_LOG(INFO, "update info", "freeze info", next);
      }
    }
  }

  return ret;
}

int ObFreezeInfoSnapshotMgr::update_next_snapshots(const ObIArray<ObSnapshotInfo>& snapshots)
{
  int ret = OB_SUCCESS;

  int64_t next_idx = get_next_idx();
  ObIArray<ObSnapshotInfo>& next_snapshots = snapshots_[next_idx];

  for (int64_t i = 0; i < snapshots.count() && OB_SUCC(ret); ++i) {
    if (OB_FAIL(next_snapshots.push_back(snapshots.at(i)))) {
      STORAGE_LOG(WARN, "fail to push back snapshot", K(ret));
    } else {
      STORAGE_LOG(INFO, "update info", "snapshot", snapshots.at(i));
    }
  }

  return ret;
}

int ObFreezeInfoSnapshotMgr::update_info(const int64_t snapshot_gc_ts, const ObIArray<SchemaPair>& gc_schema_version,
    const ObIArray<FreezeInfoLite>& info_list, const ObIArray<ObSnapshotInfo>& snapshots,
    const int64_t backup_snapshot_version, const int64_t delay_delete_snapshot_version, const int64_t min_major_version,
    bool& gc_snapshot_ts_changed)
{
  int ret = OB_SUCCESS;

  WLockGuard lock_guard(lock_);

  gc_snapshot_ts_changed = snapshot_gc_ts != snapshot_gc_ts_;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_UNLIKELY(snapshot_gc_ts < snapshot_gc_ts_)) {
    STORAGE_LOG(
        INFO, "update snapshot gc ts is smaller than snapshot_gc_ts cache!", K(snapshot_gc_ts_), K(snapshot_gc_ts));
  } else if (OB_FAIL(prepare_new_info_list(min_major_version))) {
    STORAGE_LOG(WARN, "fail to prepare new info list", K(ret));
  } else if (OB_FAIL(update_next_gc_schema_version(gc_schema_version, snapshot_gc_ts))) {
    STORAGE_LOG(WARN, "fail to update next gc schema version", K(ret));
  } else if (OB_FAIL(update_next_info_list(info_list))) {
    STORAGE_LOG(WARN, "fail to update next info list", K(ret));
  } else if (OB_FAIL(update_next_snapshots(snapshots))) {
    STORAGE_LOG(WARN, "fail to update next snapshots", K(ret));
  } else {
    STORAGE_LOG(INFO, "update info commit", K(snapshot_gc_ts_), K(snapshot_gc_ts));
    switch_info();
    snapshot_gc_ts_ = snapshot_gc_ts;
  }

  if (OB_SUCC(ret)) {
    if (backup_snapshot_version_ != backup_snapshot_version) {
      STORAGE_LOG(INFO, "update backup snapshot_version", K(backup_snapshot_version_), K(backup_snapshot_version));
      backup_snapshot_version_ = backup_snapshot_version;
    }
    if (delay_delete_snapshot_version_ != delay_delete_snapshot_version) {
      STORAGE_LOG(INFO,
          "update delay_delete_snapshot_version",
          K(delay_delete_snapshot_version_),
          K(delay_delete_snapshot_version));
      delay_delete_snapshot_version_ = delay_delete_snapshot_version;
    }
  }

  return ret;
}

int64_t ObFreezeInfoSnapshotMgr::get_snapshot_gc_ts()
{
  RLockGuard lock_guard(lock_);
  return snapshot_gc_ts_;
}

ObFreezeInfoSnapshotMgr::SchemaQuerySet::SchemaQuerySet(SchemaCache& schema_cache)
    : schema_querys_(), schema_cache_(schema_cache), lock_(), inited_(false)
{}

int ObFreezeInfoSnapshotMgr::SchemaQuerySet::init()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(schema_querys_.create(128))) {
    STORAGE_LOG(WARN, "fail to init hash set", K(ret));
  } else {
    inited_ = true;
  }

  return ret;
}

int ObFreezeInfoSnapshotMgr::SchemaQuerySet::submit_async_schema_query(
    const uint64_t tenant_id, const int64_t freeze_version)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else {
    WLockGuard guard(lock_);

    SchemaQuery query(tenant_id, freeze_version);
    if (OB_FAIL(schema_querys_.set_refactored(query, 0))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      }
      STORAGE_LOG(WARN, "fail to add new schema query");
    } else {
      STORAGE_LOG(INFO, "submit schema query", K(ret), K(tenant_id), K(freeze_version));
    }
  }

  return ret;
}

int ObFreezeInfoSnapshotMgr::SchemaQuerySet::update_schema_cache()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else {
    SchemaQuery query;

    while (OB_SUCC(pop_schema_query(query))) {
      int tmp_ret = OB_SUCCESS;
      if (OB_UNLIKELY(
              OB_SUCCESS != (tmp_ret = schema_cache_.update_schema_version(query.tenant_id_, query.freeze_version_)))) {
        STORAGE_LOG(WARN, "fail to update schema cache", K(tmp_ret));
      }

      STORAGE_LOG(INFO,
          "update schema cache",
          K(tmp_ret),
          "tenant_id",
          query.tenant_id_,
          "freeze_version",
          query.freeze_version_);
    }

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

int ObFreezeInfoSnapshotMgr::SchemaQuerySet::pop_schema_query(SchemaQuery& query)
{
  int ret = OB_SUCCESS;

  WLockGuard guard(lock_);

  ObHashSet<SchemaQuery>::iterator iter = schema_querys_.begin();

  if (iter != schema_querys_.end()) {
    query = iter->first;
    schema_querys_.erase_refactored(query);
  } else {
    ret = OB_ITER_END;
  }

  return ret;
}

ObFreezeInfoSnapshotMgr::ReloadTask::ReloadTask(ObFreezeInfoSnapshotMgr& mgr, SchemaQuerySet& schema_query_set)
    : inited_(false),
      is_remote_(false),
      mgr_(mgr),
      sql_proxy_(NULL),
      freeze_info_proxy_(),
      snapshot_proxy_(),
      last_change_ts_(0),
      schema_query_set_(schema_query_set)
{}

int ObFreezeInfoSnapshotMgr::ReloadTask::init(ObISQLClient& sql_proxy, bool is_remote)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else {
    sql_proxy_ = &sql_proxy;
    is_remote_ = is_remote;
    last_change_ts_ = ObTimeUtility::current_time();
    inited_ = true;
  }
  return ret;
}

int ObFreezeInfoSnapshotMgr::ReloadTask::get_global_info_compat_below_220(
    int64_t& snapshot_gc_ts, ObIArray<SchemaPair>& gc_schema_version)
{
  int ret = OB_SUCCESS;

  int64_t snapshot_gc_timestamp = 0;
  ObGlobalStatProxy stat_proxy(*sql_proxy_);
  ObSEArray<uint64_t, 64> tenant_ids;

  if (OB_FAIL(ObTenantManager::get_instance().get_all_tenant_id(tenant_ids))) {
    STORAGE_LOG(WARN, "fail to get all tenant id", K(ret));
  } else if (OB_FAIL(stat_proxy.get_snapshot_info(snapshot_gc_ts, snapshot_gc_timestamp))) {
    STORAGE_LOG(WARN, "fail to get global info", K(ret));
  } else {
    for (int64_t i = 0; i < tenant_ids.count() && OB_SUCC(ret); ++i) {
      uint64_t tenant_id = tenant_ids.at(i);
      if (OB_FAIL(gc_schema_version.push_back(SchemaPair(tenant_id, snapshot_gc_timestamp)))) {
        STORAGE_LOG(WARN, "fail to push back schema pair", K(ret));
      }
    }
  }

  return ret;
}

int ObFreezeInfoSnapshotMgr::ReloadTask::get_global_info(
    int64_t& snapshot_gc_ts, ObIArray<SchemaPair>& gc_schema_version)
{
  int ret = OB_SUCCESS;

  ObSEArray<TenantIdAndSchemaVersion, 64> tmp;
  ObGlobalStatProxy stat_proxy(*sql_proxy_);
  bool need_gc_schema_version = (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2230);
  if (OB_FAIL(stat_proxy.get_snapshot_info_v2(*sql_proxy_, 0, need_gc_schema_version, snapshot_gc_ts, tmp))) {
    STORAGE_LOG(WARN, "fail to get global info", K(ret));
  } else {
    for (int64_t i = 0; i < tmp.count() && OB_SUCC(ret); ++i) {
      TenantIdAndSchemaVersion& pair = tmp.at(i);
      if (OB_FAIL(gc_schema_version.push_back(SchemaPair(pair.tenant_id_, pair.schema_version_)))) {
        STORAGE_LOG(WARN, "fail to push back schema pair", K(ret));
      }
    }
  }

  return ret;
}

int ObFreezeInfoSnapshotMgr::ReloadTask::get_freeze_info(
    int64_t& min_major_version, ObIArray<FreezeInfoLite>& freeze_info)
{
  int ret = OB_SUCCESS;

  int64_t freeze_version = 0;
  min_major_version = INT64_MAX;
  ObSEArray<ObSimpleFrozenStatus, 8> tmp;

  if (OB_FAIL(mgr_.get_latest_freeze_version(freeze_version))) {
    STORAGE_LOG(WARN, "fail to get major version", K(ret));
  } else if (OB_FAIL(freeze_info_proxy_.get_min_major_available_and_larger_info(
                 *sql_proxy_, freeze_version, min_major_version, tmp))) {
    STORAGE_LOG(WARN, "fail to get freeze info", K(ret), K(freeze_version));
  } else {
    for (int64_t i = 0; i < tmp.count() && OB_SUCC(ret); ++i) {
      ObSimpleFrozenStatus& status = tmp.at(i);
      if (OB_FAIL(freeze_info.push_back(
              FreezeInfoLite(status.frozen_version_, status.frozen_timestamp_, status.cluster_version_)))) {
        STORAGE_LOG(WARN, "fail to push back freeze info", K(ret), K(status));
      }
    }
  }

  return ret;
}

int ObFreezeInfoSnapshotMgr::ReloadTask::get_backup_snapshot_version(int64_t& backup_snapshot_version)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  const uint64_t cluster_version = GET_MIN_CLUSTER_VERSION();

  if (cluster_version < CLUSTER_VERSION_2250) {
    backup_snapshot_version = 0;
    LOG_INFO("old cluster version, no need get_backup_snapshot_version", K(cluster_version));
  } else if (OB_FAIL(ObTenantBackupInfoOperation::get_backup_snapshot_version(
                 *sql_proxy_, tenant_id, backup_snapshot_version))) {
    LOG_WARN("failed to get backup snapshot_version", K(ret));
  }

  return ret;
}

int ObFreezeInfoSnapshotMgr::get_local_backup_snapshot_version(int64_t& backup_snapshot_version)
{
  int ret = OB_SUCCESS;
  RLockGuard lock_guard(lock_);

  if (0 == snapshot_gc_ts_ || 1 == snapshot_gc_ts_) {
    if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
      TRANS_LOG(INFO, "rs havenot send the snapshot or standby ob is starting", K(snapshot_gc_ts_));
    }
    ret = OB_EAGAIN;
  } else {
    // ATTENTION:
    // 1. snapshot_gc_ts is updated incrementally
    // 2. backup_snapshot_version is bigger than snapshot_gc_ts in rootservice heartbeat
    // 3. it is guaranteed there exists a recover point smaller than snapshot_gc_ts
    //
    // Then, there should exist a recover point smaller than
    // backup_snapshot_version when rootservice heartbeat is receive
    if (0 == backup_snapshot_version_) {
      backup_snapshot_version = snapshot_gc_ts_;
    } else {
      backup_snapshot_version = MIN(backup_snapshot_version_, snapshot_gc_ts_);
    }
  }

  return ret;
}

int ObFreezeInfoSnapshotMgr::ReloadTask::try_update_info()
{
  int ret = OB_SUCCESS;

  if (!is_remote_) {
    DEBUG_SYNC(BEFORE_UPDATE_FREEZE_SNAPSHOT_INFO);
  }
  STORAGE_LOG(INFO, "start reload freeze info and snapshots", K(is_remote_));

  if (OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(ERROR, "sql_proxy is NULL", K(ret));
  } else if (is_remote_ && !GCTX.is_standby_cluster()) {
  } else {
    int64_t snapshot_gc_ts = 0;
    ObSEArray<SchemaPair, 64> gc_schema_version;
    // snapshot_gc_ts should be obtained before freeze_info and snapshots
    ObSEArray<FreezeInfoLite, 4> freeze_info;
    ObSEArray<ObSnapshotInfo, 4> snapshots;
    bool changed = false;
    observer::ObService* ob_service = GCTX.ob_service_;
    int64_t backup_snapshot_version = 0;
    int64_t min_major_version = INT64_MAX;
    int64_t delay_delete_snapshot_version = 0;

    if (OB_FAIL(schema_query_set_.update_schema_cache())) {
      STORAGE_LOG(WARN, "fail to update schema cache", K(ret));
      ret = OB_SUCCESS;  // ignore ret
    }

    if (OB_FAIL(get_global_info(snapshot_gc_ts, gc_schema_version))) {
      STORAGE_LOG(WARN, "failed to get global info", K(ret));
    } else if (OB_FAIL(get_freeze_info(min_major_version, freeze_info))) {
      STORAGE_LOG(WARN, "failed to get freeze info", K(ret));
    } else if (OB_FAIL(snapshot_proxy_.get_all_snapshots(*sql_proxy_, snapshots))) {
      STORAGE_LOG(WARN, "failed to get snapshots", K(ret));
    } else if (OB_FAIL(get_backup_snapshot_version(backup_snapshot_version))) {
      LOG_WARN("failed to get backup snapshot version", K(ret));
    } else if (OB_FAIL(ObBackupInfoMgr::get_instance().get_backup_snapshot_version(delay_delete_snapshot_version))) {
      LOG_WARN("failed to get delay delete snapshot version", K(ret));
    } else if (OB_FAIL(mgr_.update_info(snapshot_gc_ts,
                   gc_schema_version,
                   freeze_info,
                   snapshots,
                   backup_snapshot_version,
                   delay_delete_snapshot_version,
                   min_major_version,
                   changed))) {
      STORAGE_LOG(WARN, "update info failed", K(ret), K(snapshot_gc_ts), K(freeze_info), K(snapshots));
    } else {
      if (changed || ob_service->is_heartbeat_expired()) {
        last_change_ts_ = ObTimeUtility::current_time();
      } else {
        const int64_t last_not_change_interval_us = ObTimeUtility::current_time() - last_change_ts_;
        if (MAX_GC_SNAPSHOT_TS_REFRESH_TS <= last_not_change_interval_us &&
            (0 != snapshot_gc_ts && 1 != snapshot_gc_ts)) {
          if (REACH_TIME_INTERVAL(60L * 1000L * 1000L)) {
            STORAGE_LOG(ERROR,
                "snapshot_gc_ts not refresh too long",
                K(snapshot_gc_ts),
                K(freeze_info),
                K(snapshots),
                K(last_change_ts_),
                K(last_not_change_interval_us));
          }
        } else if (FLUSH_GC_SNAPSHOT_TS_REFRESH_TS <= last_not_change_interval_us) {
          STORAGE_LOG(WARN,
              "snapshot_gc_ts not refresh too long",
              K(snapshot_gc_ts),
              K(freeze_info),
              K(snapshots),
              K(last_change_ts_),
              K(last_not_change_interval_us));
        }
      }

      STORAGE_LOG(DEBUG,
          "reload freeze info and snapshots",
          K(snapshot_gc_ts),
          K(freeze_info),
          K(snapshots),
          K(backup_snapshot_version));
    }
  }
  return ret;
}

void ObFreezeInfoSnapshotMgr::ReloadTask::runTimerTask()
{
  try_update_info();
}

ObFreezeInfoSnapshotMgr::SchemaCache::SchemaCache(SchemaQuerySet& schema_query_set)
    : lock_(),
      head_(NULL),
      tail_(NULL),
      cnt_(0),
      inited_(false),
      sql_proxy_(NULL),
      freeze_info_proxy_(),
      mem_attr_(OB_SERVER_TENANT_ID, ObModIds::OB_FREEZE_INFO_CACHE),
      allocator_(sizeof(schema_node), mem_attr_),
      schema_query_set_(schema_query_set)
{}

ObFreezeInfoSnapshotMgr::SchemaCache::~SchemaCache()
{
  reset();
}

int ObFreezeInfoSnapshotMgr::SchemaCache::init(ObISQLClient& sql_proxy)
{
  int ret = OB_SUCCESS;

  schema_node* p = NULL;

  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
  } else if (NULL == (p = (schema_node*)allocator_.alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc head node", K(ret));
  } else {
    head_ = tail_ = p;
    sql_proxy_ = &sql_proxy;
    inited_ = true;
  }

  return ret;
}

void ObFreezeInfoSnapshotMgr::SchemaCache::reset()
{
  if (!inited_) {
    return;
  }

  SpinLockGuard guard(lock_);

  schema_node* curr = head_;
  schema_node* t = NULL;

  while (curr) {
    t = curr;
    curr = curr->next;

    allocator_.free(t);
  }

  head_ = tail_ = NULL;
  cnt_ = 0;
  inited_ = false;
}

int ObFreezeInfoSnapshotMgr::SchemaCache::get_freeze_schema_version(
    const uint64_t tenant_id, const int64_t freeze_version, const bool async, int64_t& schema_version)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else {
    bool schema_decided = false;
    schema_version = ObFrozenStatus::INVALID_SCHEMA_VERSION;
    if (OB_FAIL(find(tenant_id, freeze_version, schema_version))) {
      STORAGE_LOG(WARN, "fail to find in cache", K(ret), K(tenant_id), K(freeze_version));
    } else if (ObFrozenStatus::INVALID_SCHEMA_VERSION == schema_version || OB_INVALID_VERSION == schema_version) {
      // cache miss
      if (async) {
        ret = OB_EAGAIN;
        STORAGE_LOG(INFO, "async query freeze schema", K(tenant_id), K(freeze_version));

        int tmp_ret = OB_SUCCESS;
        if (OB_UNLIKELY(
                OB_SUCCESS != (tmp_ret = schema_query_set_.submit_async_schema_query(tenant_id, freeze_version)))) {
          STORAGE_LOG(WARN, "fail to submit async query task", K(tmp_ret), K(tenant_id), K(freeze_version));
        }
      } else if (freeze_info_exist(freeze_version)) {
        // only specific tenant
        schema_decided = true;
        if (OB_FAIL(fetch_freeze_schema(tenant_id, freeze_version, schema_version))) {
          STORAGE_LOG(WARN, "fail to fetch freeze schema", K(ret), K(tenant_id), K(freeze_version));
        } else if (ObFrozenStatus::INVALID_SCHEMA_VERSION != schema_version && OB_INVALID_VERSION != schema_version) {
          if (OB_FAIL(update_freeze_schema(tenant_id, freeze_version, schema_version))) {
            STORAGE_LOG(
                WARN, "fail to update freeze schema", K(ret), K(tenant_id), K(freeze_version), K(schema_version));
          }
        }
      } else {
        // all local tenant
        ObSEArray<SchemaPair, 32> freeze_schema;
        if (OB_FAIL(fetch_freeze_schema(tenant_id, freeze_version, schema_version, freeze_schema))) {
          STORAGE_LOG(WARN, "fail to fetch freeze schema", K(ret), K(freeze_version));
        } else if (freeze_schema.count() > 0) {
          schema_decided = true;
          if (OB_FAIL(update_freeze_schema(freeze_version, freeze_schema))) {
            STORAGE_LOG(WARN, "fail to update freeze schema", K(ret), K(freeze_version), K(freeze_schema));
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (ObFrozenStatus::INVALID_SCHEMA_VERSION == schema_version) {
          if (schema_decided) {
            ret = OB_ENTRY_NOT_EXIST;
            STORAGE_LOG(INFO, "tenant schema not exist", K(ret), K(tenant_id), K(freeze_version));
          } else {
            ret = OB_EAGAIN;
          }
        } else if (OB_INVALID_VERSION == schema_version) {
          ret = OB_EAGAIN;
          STORAGE_LOG(WARN,
              "get invalid version(-1) for schema version, may be memory explosion for tenant",
              K(ret),
              K(tenant_id),
              K(freeze_version));
        }
      }
    }
  }

  return ret;
}

int ObFreezeInfoSnapshotMgr::SchemaCache::update_schema_version(const uint64_t tenant_id, const int64_t freeze_version)
{
  int ret = OB_SUCCESS;

  int64_t schema_version = OB_INVALID_VERSION;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (schema_exist(tenant_id, freeze_version)) {
  } else if (OB_FAIL(fetch_freeze_schema(tenant_id, freeze_version, schema_version))) {
    STORAGE_LOG(WARN, "fail to fetch freeze schema", K(ret), K(tenant_id), K(freeze_version));
  } else if (ObFrozenStatus::INVALID_SCHEMA_VERSION != schema_version && OB_INVALID_VERSION != schema_version) {
    if (OB_FAIL(update_freeze_schema(tenant_id, freeze_version, schema_version))) {
      STORAGE_LOG(WARN, "fail to update freeze schema", K(ret), K(tenant_id), K(freeze_version), K(schema_version));
    }
  }

  return ret;
}

void ObFreezeInfoSnapshotMgr::SchemaCache::insert(schema_node* p)
{
  p->next = head_->next;
  p->prev = head_;

  if (head_->next)
    head_->next->prev = p;
  head_->next = p;
  if (!p->next)
    tail_ = p;
}

void ObFreezeInfoSnapshotMgr::SchemaCache::move_forward(schema_node* p)
{
  if (!p->next) {
    tail_ = p->prev;
  } else {
    p->next->prev = p->prev;
  }
  p->prev->next = p->next;

  insert(p);
}

ObFreezeInfoSnapshotMgr::SchemaCache::schema_node* ObFreezeInfoSnapshotMgr::SchemaCache::inner_find(
    const uint64_t tenant_id, const int64_t freeze_version)
{
  schema_node* node = NULL;
  schema_node* prev = head_;
  schema_node* curr = prev->next;

  while (curr) {
    if (curr->tenant_id == tenant_id && curr->freeze_version == freeze_version) {
      node = curr;
      break;
    }

    prev = curr;
    curr = prev->next;
  }

  return node;
}

int ObFreezeInfoSnapshotMgr::SchemaCache::find(
    const uint64_t tenant_id, const int64_t freeze_version, int64_t& schema_version)
{
  int ret = OB_SUCCESS;

  SpinLockGuard guard(lock_);

  schema_node* node = inner_find(tenant_id, freeze_version);
  if (NULL != node) {
    schema_version = node->schema_version;
    move_forward(node);
  }

  return ret;
}

bool ObFreezeInfoSnapshotMgr::SchemaCache::freeze_info_exist(const int64_t freeze_version)
{
  bool bool_ret = false;

  SpinLockGuard guard(lock_);

  schema_node* curr = head_;
  do {
    curr = curr->next;
    if (curr && curr->freeze_version == freeze_version) {
      bool_ret = true;
      break;
    }
  } while (curr);

  return bool_ret;
}

bool ObFreezeInfoSnapshotMgr::SchemaCache::schema_exist(const uint64_t tenant_id, const int64_t freeze_version)
{
  SpinLockGuard guard(lock_);
  return inner_schema_exist(tenant_id, freeze_version);
}

bool ObFreezeInfoSnapshotMgr::SchemaCache::inner_schema_exist(const uint64_t tenant_id, const int64_t freeze_version)
{
  int ret = OB_SUCCESS;
  bool bool_ret = false;

  schema_node* node = inner_find(tenant_id, freeze_version);
  if (NULL != node) {
    bool_ret = true;
  }

  return bool_ret;
}

int ObFreezeInfoSnapshotMgr::SchemaCache::update_freeze_schema(
    const uint64_t tenant_id, const int64_t freeze_version, const int64_t schema_version)
{
  int ret = OB_SUCCESS;

  SpinLockGuard guard(lock_);

  if (inner_schema_exist(tenant_id, freeze_version)) {
    // skip
  } else if (cnt_ < MAX_SCHEMA_ENTRY) {
    schema_node* p = NULL;
    if (NULL == (p = (schema_node*)allocator_.alloc())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to alloc schema node", K(ret));
    } else {
      p->set(tenant_id, freeze_version, schema_version);
      insert(p);
      cnt_++;
    }
  } else {
    schema_node* p = tail_;
    p->set(tenant_id, freeze_version, schema_version);
    move_forward(p);
  }

  if (OB_SUCC(ret)) {
    STORAGE_LOG(INFO, "update freeze schema", K(tenant_id), K(freeze_version), K(schema_version));
  }

  return ret;
}

int ObFreezeInfoSnapshotMgr::SchemaCache::update_freeze_schema(
    const int64_t freeze_version, ObIArray<SchemaPair>& freeze_schema)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; i < freeze_schema.count() && OB_SUCC(ret); ++i) {
    SchemaPair& pair = freeze_schema.at(i);
    if (OB_FAIL(update_freeze_schema(pair.tenant_id, freeze_version, pair.schema_version))) {
      STORAGE_LOG(WARN, "fail to update freeze schema", K(ret), K(freeze_version), K(pair));
    }
  }

  return ret;
}

int ObFreezeInfoSnapshotMgr::SchemaCache::fetch_freeze_schema(
    const uint64_t tenant_id, const int64_t freeze_version, int64_t& schema_version)
{
  int ret = OB_SUCCESS;

  ObFrozenStatus status;
  if (OB_FAIL(freeze_info_proxy_.get_freeze_info_v2(*sql_proxy_, tenant_id, freeze_version, status))) {
    STORAGE_LOG(WARN, "fail to get freeze info", K(ret), K(tenant_id), K(freeze_version));
  } else if (status.schema_version_ != ObFrozenStatus::INVALID_SCHEMA_VERSION) {
    schema_version = status.schema_version_;
  }

  return ret;
}

int ObFreezeInfoSnapshotMgr::SchemaCache::fetch_freeze_schema(const uint64_t tenant_id, const int64_t freeze_version,
    int64_t& schema_version, ObIArray<SchemaPair>& freeze_schema)
{
  int ret = OB_SUCCESS;

  ObSEArray<TenantIdAndSchemaVersion, 64> tmp;
  ObFrozenStatus old_status;
  if (OB_FAIL(freeze_info_proxy_.get_freeze_info_v2(*sql_proxy_, freeze_version, old_status, tmp))) {
    STORAGE_LOG(WARN, "fail to get freeze info", K(ret), K(freeze_version));
  } else if (old_status.schema_version_ > 0) {
    // minor freeze before 2.2.x
    schema_version = old_status.schema_version_;
    if (OB_FAIL(freeze_schema.push_back(SchemaPair(tenant_id, schema_version)))) {
      STORAGE_LOG(WARN, "fail to push back schema pair", K(ret));
    }
  } else if (tmp.count() > 0) {
    for (int64_t i = 0; i < tmp.count() && OB_SUCC(ret); ++i) {
      TenantIdAndSchemaVersion& pair = tmp.at(i);

      if (tenant_id == pair.tenant_id_) {
        schema_version = pair.schema_version_;
      }

      // ignore schema version that remote doesn't know
      if (OB_INVALID_VERSION != pair.schema_version_) {
        if (tenant_id == pair.tenant_id_ || ObTenantManager::get_instance().has_tenant(pair.tenant_id_)) {
          if (OB_FAIL(freeze_schema.push_back(SchemaPair(pair.tenant_id_, pair.schema_version_)))) {
            STORAGE_LOG(WARN, "fail to push back schema pair", K(ret));
          }
        }
      }
    }
  }

  return ret;
}

ObFreezeInfoSnapshotMgr& ObFreezeInfoMgrWrapper::get_instance(const uint64_t table_id)
{
  bool remote_table =
      OB_INVALID_ID != table_id && is_sys_table(table_id) && !ObMultiClusterUtil::is_cluster_private_table(table_id);
  bool use_remote = GCTX.is_standby_cluster() && remote_table;

  if (use_remote) {
    STORAGE_LOG(INFO, "use remote freeze info", K(table_id));
    return remote_mgr_;
  } else {
    return local_mgr_;
  }
}

int ObFreezeInfoMgrWrapper::init(common::ObISQLClient& local_proxy, common::ObISQLClient& remote_proxy)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(local_mgr_.init(local_proxy, false))) {
    STORAGE_LOG(ERROR, "fail to init local mgr", K(ret));
  } else if (OB_FAIL(remote_mgr_.init(remote_proxy, true))) {
    STORAGE_LOG(ERROR, "fail to init remote mgr", K(ret));
  }

  return ret;
}

int ObFreezeInfoMgrWrapper::start()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(local_mgr_.start())) {
    STORAGE_LOG(ERROR, "fail to start local mgr", K(ret));
  } else if (OB_FAIL(remote_mgr_.start())) {
    STORAGE_LOG(ERROR, "fail to start remote mgr", K(ret));
  }

  return ret;
}

void ObFreezeInfoMgrWrapper::wait()
{
  local_mgr_.wait();
  remote_mgr_.wait();
}

void ObFreezeInfoMgrWrapper::stop()
{
  local_mgr_.stop();
  remote_mgr_.stop();
}

ObFreezeInfoSnapshotMgr ObFreezeInfoMgrWrapper::local_mgr_;
ObFreezeInfoSnapshotMgr ObFreezeInfoMgrWrapper::remote_mgr_;

}  // namespace storage
}  // namespace oceanbase
