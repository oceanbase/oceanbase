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
#include "share/ob_freeze_info_manager.h"
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

struct ObStorageSnapshotInfo
{
  enum ObStorageSnapShotType
  {
    SNAPSHOT_FOR_UNDO_RETENTION = share::ObSnapShotType::MAX_SNAPSHOT_TYPE,
    SNAPSHOT_FOR_TX,
    SNAPSHOT_FOR_MAJOR_FREEZE_TS,
    SNAPSHOT_MULTI_VERSION_START_ON_TABLET,
    SNAPSHOT_ON_TABLET,
    SNAPSHOT_FOR_LS_RESERVED,
    SNAPSHOT_FOR_MIN_MEDIUM,
    SNAPSHOT_MAX,
  };
  ObStorageSnapshotInfo();
  ~ObStorageSnapshotInfo() { reset(); }
  OB_INLINE void reset() {
    snapshot_type_ = SNAPSHOT_MAX;
    snapshot_ = 0;
  }
  OB_INLINE bool is_valid() const {
    return snapshot_type_ < SNAPSHOT_MAX && snapshot_ >= 0;
  }
  ObStorageSnapshotInfo &operator =(const ObStorageSnapshotInfo &input)
  {
    snapshot_type_ = input.snapshot_type_;
    snapshot_ = input.snapshot_;
    return *this;
  }
  void update_by_smaller_snapshot(const uint64_t snapshot_type, const int64_t snapshot);
  static const char *ObSnapShotTypeStr[];
  const char *get_snapshot_type_str() const;
  TO_STRING_KV("type", get_snapshot_type_str(), K_(snapshot));
  uint64_t snapshot_type_;
  int64_t snapshot_;
};

struct ObFrozenStatus;

class ObTenantFreezeInfoMgr
{
public:

  struct NeighbourFreezeInfo {
    share::ObFreezeInfo next;
    share::ObFreezeInfo prev;

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

  int get_freeze_info_behind_major_snapshot(const int64_t major_snapshot, common::ObIArray<share::ObFreezeInfo> &freeze_infos);
  int get_freeze_info_by_snapshot_version(const int64_t snapshot_version, share::ObFreezeInfo &freeze_info);
  // get first freeze info larger than snapshot
  int get_freeze_info_behind_snapshot_version(const int64_t snapshot_version, share::ObFreezeInfo &freeze_info);

  int get_neighbour_major_freeze(const int64_t snapshot_version, NeighbourFreezeInfo &info);

  int64_t get_min_reserved_snapshot_for_tx();
  void set_global_broadcast_scn(const share::SCN &global_broadcast_scn) { global_broadcast_scn_ = global_broadcast_scn; }
  int get_min_reserved_snapshot(
      const ObTabletID &tablet_id,
      const int64_t merged_version,
      ObStorageSnapshotInfo &snapshot_info);

  int get_min_dependent_freeze_info(share::ObFreezeInfo &freeze_info);
  int64_t get_snapshot_gc_ts();
  share::SCN get_snapshot_gc_scn();

  ObTenantFreezeInfoMgr(const ObTenantFreezeInfoMgr&) = delete;
  ObTenantFreezeInfoMgr& operator=(const ObTenantFreezeInfoMgr&) = delete;

  ObTenantFreezeInfoMgr();
  virtual ~ObTenantFreezeInfoMgr();
private:
  typedef common::RWLock::RLockGuard RLockGuard;
  typedef common::RWLock::WLockGuard WLockGuard;
  typedef common::RWLock::RLockGuardWithTimeout RLockGuardWithTimeout;

  static const int64_t RELOAD_INTERVAL = 3L * 1000L * 1000L;
  static const int64_t UPDATE_LS_RESERVED_SNAPSHOT_INTERVAL = 10L * 1000L * 1000L;
  static const int64_t MAX_GC_SNAPSHOT_TS_REFRESH_TS = 10L * 60L * 1000L * 1000L;
  static const int64_t FLUSH_GC_SNAPSHOT_TS_REFRESH_TS =
      common::MODIFY_GC_SNAPSHOT_INTERVAL + 10L * 1000L * 1000L;
  static const int64_t MIN_DEPENDENT_FREEZE_INFO_GAP = 2;
  static const int64_t RLOCK_TIMEOUT_US = 2L * 1000L * 1000L; // 2s

  int64_t get_next_idx() { return 1L - cur_idx_; }
  void switch_info() { cur_idx_ = get_next_idx(); }
  virtual int get_multi_version_duration(int64_t &duration) const;
  int update_next_snapshots(const common::ObIArray<share::ObSnapshotInfo> &snapshots);
  int get_freeze_info_behind_snapshot_version_(
      const int64_t snapshot_version,
      share::ObFreezeInfo &freeze_info);
  int try_update_reserved_snapshot();
  int try_update_info();
  int inner_update_info(
      const share::SCN &new_snapshot_gc_scn,
      const common::ObIArray<share::ObFreezeInfo> &new_freeze_infos,
      const common::ObIArray<share::ObSnapshotInfo> &new_snapshots);

  class ReloadTask : public common::ObTimerTask
  {
  public:
    ReloadTask(ObTenantFreezeInfoMgr &mgr);
    virtual ~ReloadTask() {}
    int init();
    virtual void runTimerTask();
    int refresh_merge_info();

  private:
    bool inited_;
    bool check_tenant_status_;
    ObTenantFreezeInfoMgr &mgr_;
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
  share::ObFreezeInfoManager freeze_info_mgr_;
  common::ObSEArray<share::ObSnapshotInfo, 8> snapshots_[2]; // snapshots_ maintains multi_version_start for index and others
  common::RWLock lock_;
  int64_t cur_idx_;
  int64_t last_change_ts_;
  share::SCN global_broadcast_scn_;
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
