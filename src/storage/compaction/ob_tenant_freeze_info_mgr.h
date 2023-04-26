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

#ifndef OCEANBASE_STORAGE_TENANT_FREEZE_INFO_MGR_
#define OCEANBASE_STORAGE_TENANT_FREEZE_INFO_MGR_

#include <stdint.h>

#include "lib/allocator/ob_slice_alloc.h"
#include "lib/hash/ob_hashset.h"
#include "lib/lock/ob_tc_rwlock.h"
#include "lib/task/ob_timer.h"
#include "share/ob_freeze_info_proxy.h"
#include "share/ob_snapshot_table_proxy.h"
#include "share/ob_zone_info.h"
#include "share/scn.h"

namespace oceanbase
{

namespace common
{
class ObISQLClient;
}

namespace storage
{

struct ObFrozenStatus;

class ObTenantFreezeInfoMgr
{
public:

  struct FreezeInfo {
    int64_t freeze_version;
    int64_t schema_version;
    int64_t cluster_version;

    FreezeInfo() : freeze_version(-1), schema_version(-1), cluster_version(0) {}
    FreezeInfo(const int64_t &freeze_ver, const int64_t schema_ver, const int64_t cluster_ver)
      : freeze_version(freeze_ver), schema_version(schema_ver), cluster_version(cluster_ver) {}
    FreezeInfo &operator =(const FreezeInfo &o)
    {
      freeze_version = o.freeze_version;
      schema_version = o.schema_version;
      cluster_version = o.cluster_version;
      return *this;
    }
    void reset() { freeze_version = -1; schema_version = -1; cluster_version = 0; }
    TO_STRING_KV(K(freeze_version), K(schema_version), K(cluster_version));
  };

  struct NeighbourFreezeInfo {
    FreezeInfo next;
    FreezeInfo prev;

    NeighbourFreezeInfo &operator =(const NeighbourFreezeInfo &o)
    {
      next = o.next;
      prev = o.prev;
      return *this;
    }
    void reset() { next.reset(); prev.reset(); }
    TO_STRING_KV(K(next), K(prev));
  };

public:
  static int mtl_init(ObTenantFreezeInfoMgr* &freeze_info_mgr);

  int init(const uint64_t tenant_id, common::ObISQLClient &sql_proxy);
  void init_for_test() { inited_ = true; }
  bool is_inited() const { return inited_; }
  int start();
  void wait();
  void stop();
  void destroy();

  int64_t get_latest_frozen_version();

  int get_freeze_info_behind_major_snapshot(const int64_t major_snapshot, common::ObIArray<FreezeInfo> &freeze_infos);
  int get_freeze_info_by_snapshot_version(const int64_t snapshot_version, FreezeInfo &freeze_info);
  // get first freeze info larger than snapshot
  int get_freeze_info_behind_snapshot_version(const int64_t snapshot_version, FreezeInfo &freeze_info);

  int get_neighbour_major_freeze(const int64_t snapshot_version, NeighbourFreezeInfo &info);

  int64_t get_min_reserved_snapshot_for_tx();
  int get_min_reserved_snapshot(
      const ObTabletID &tablet_id,
      const int64_t merged_version,
      int64_t &snapshot_version);
  int diagnose_min_reserved_snapshot(
      const ObTabletID &tablet_id,
      const int64_t merged_version,
      int64_t &snapshot_version,
      common::ObString &snapshot_from_type);
  int get_reserve_points(
      const int64_t tenant_id,
      const share::ObSnapShotType snapshot_type,
      common::ObIArray<share::ObSnapshotInfo> &restore_points,
      int64_t &snapshot_gc_ts);

  int get_min_dependent_freeze_info(FreezeInfo &freeze_info);
  int get_latest_freeze_info(FreezeInfo &freeze_info);

  //  maybe no need any more
  // int get_restore_point_min_schema_version(const int64_t tenant_id, int64_t &schema_version);
  int update_info(
      const int64_t snapshot_gc_ts,
      const common::ObIArray<FreezeInfo> &info_list,
      const common::ObIArray<share::ObSnapshotInfo> &snapshots,
      const int64_t min_major_snapshot,
      bool& changed);

  int64_t get_snapshot_gc_ts();

  ObTenantFreezeInfoMgr(const ObTenantFreezeInfoMgr&) = delete;
  ObTenantFreezeInfoMgr& operator=(const ObTenantFreezeInfoMgr&) = delete;

  ObTenantFreezeInfoMgr();
  virtual ~ObTenantFreezeInfoMgr();
private:
  typedef common::RWLock::RLockGuard RLockGuard;
  typedef common::RWLock::WLockGuard WLockGuard;

  static const int64_t RELOAD_INTERVAL = 3L * 1000L * 1000L;
  static const int64_t UPDATE_LS_RESERVED_SNAPSHOT_INTERVAL = 10L * 1000L * 1000L;
  static const int64_t MAX_GC_SNAPSHOT_TS_REFRESH_TS = 10L * 60L * 1000L * 1000L;
  static const int64_t FLUSH_GC_SNAPSHOT_TS_REFRESH_TS =
      common::MODIFY_GC_SNAPSHOT_INTERVAL + 10L * 1000L * 1000L;
  static const int64_t MIN_DEPENDENT_FREEZE_INFO_GAP = 2;

  int get_latest_freeze_version(int64_t &freeze_version);
  int64_t get_next_idx() { return 1L - cur_idx_; }
  void switch_info() { cur_idx_ = get_next_idx(); }
  int get_info_nolock(const int64_t idx, FreezeInfo &freeze_info);
  int prepare_new_info_list(const int64_t min_major_snapshot);
  virtual int get_multi_version_duration(int64_t &duration) const;
  int inner_get_neighbour_major_freeze(const int64_t snapshot_version,
                                       NeighbourFreezeInfo &info);
  int update_next_info_list(const common::ObIArray<FreezeInfo> &info_list);
  int update_next_snapshots(const common::ObIArray<share::ObSnapshotInfo> &snapshots);
  int64_t find_pos_in_list_(
      const int64_t snapshot_version,
      const ObIArray<FreezeInfo> &info_list);
  int get_freeze_info_behind_snapshot_version_(
      const int64_t snapshot_version,
      FreezeInfo &freeze_info);
  int try_update_reserved_snapshot();
  class ReloadTask : public common::ObTimerTask
  {
  public:
    ReloadTask(ObTenantFreezeInfoMgr &mgr);
    virtual ~ReloadTask() {}
    int init(common::ObISQLClient &sql_proxy);
    virtual void runTimerTask();
    int refresh_merge_info();
    int try_update_info();

  private:
    int get_global_info(int64_t &snapshot_gc_ts);
    int get_freeze_info(int64_t &min_major_snapshot,
                        common::ObIArray<FreezeInfo> &freeze_info);

    bool inited_;
    bool check_tenant_status_;
    ObTenantFreezeInfoMgr &mgr_;
    common::ObISQLClient *sql_proxy_;
    share::ObSnapshotTableProxy snapshot_proxy_;
    int64_t last_change_ts_;
  };

  class UpdateLSResvSnapshotTask : public common::ObTimerTask
  {
  public:
    UpdateLSResvSnapshotTask(ObTenantFreezeInfoMgr &mgr) : mgr_(mgr) {}
    virtual void runTimerTask();
  private:
    ObTenantFreezeInfoMgr &mgr_;
  };

private:
  ReloadTask reload_task_;
  UpdateLSResvSnapshotTask update_reserved_snapshot_task_;
  common::ObSEArray<FreezeInfo, 8> info_list_[2];
  common::ObSEArray<share::ObSnapshotInfo, 8> snapshots_[2]; // snapshots_ maintains multi_version_start for index and others
  common::RWLock lock_;
  int64_t snapshot_gc_ts_;
  int64_t cur_idx_;
  uint64_t tenant_id_;
  int tg_id_;
  bool inited_;
};

#define MTL_CALL_FREEZE_INFO_MGR(func, args...)                                    \
  ({                                                                               \
    int ret = common::OB_SUCCESS;                                                  \
    storage::ObTenantFreezeInfoMgr *mgr = MTL(storage::ObTenantFreezeInfoMgr *);   \
    if (OB_UNLIKELY(NULL == mgr)) {                                                \
      ret = common::OB_ERR_UNEXPECTED;                                             \
      STORAGE_LOG(ERROR, "failed to get tenant freeze info mgr from mtl", K(ret)); \
    } else if (OB_FAIL(mgr->func(args))) {                                         \
      if (OB_ENTRY_NOT_EXIST != ret) {                                             \
        STORAGE_LOG(WARN, "failed to execute func", K(ret));                       \
      }                                                                            \
    }                                                                              \
    ret;                                                                           \
  })

}
}

#endif /* OCEANBASE_STORAGE_TENANT_FREEZE_INFO_MGR_ */
