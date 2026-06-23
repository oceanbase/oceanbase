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

#pragma once

#include "logservice/ob_log_base_type.h"
#include "rootserver/mview/ob_mlog_maintenance_task.h"
#include "rootserver/mview/ob_mview_maintenance_task.h"
#include "rootserver/mview/ob_mview_refresh_stats_maintenance_task.h"
#include "rootserver/mview/ob_mview_push_refresh_scn_task.h"
#include "rootserver/mview/ob_mview_push_snapshot_task.h"
#include "rootserver/mview/ob_collect_mv_merge_info_task.h"
#include "rootserver/mview/ob_mview_clean_snapshot_task.h"
#include "share/scn.h"
#include "rootserver/mview/ob_replica_safe_check_task.h"
#include "rootserver/mview/ob_mview_update_cache_task.h"
#include "rootserver/mview/ob_mview_mds_op_task.h"
#ifdef OB_BUILD_MV_REFRESH_QUEUEING
#include "rootserver/mview/ob_mview_pending_task_manager.h"
#endif

namespace oceanbase
{
namespace rootserver
{
class ObMViewMaintenanceService : public logservice::ObIReplaySubHandler,
                                  public logservice::ObICheckpointSubHandler,
                                  public logservice::ObIRoleChangeSubHandler
{
public:
  static const int64_t CacheValidInterval = 30 * 1000 * 1000; //30s
  struct MViewRefreshInfo
  {
    uint64_t refresh_scn_;
    MViewRefreshInfo() : refresh_scn_(0)
    {};
  };
  typedef hash::ObHashMap<uint64_t, MViewRefreshInfo> MViewRefreshInfoCache;
  typedef hash::ObHashMap<transaction::ObTransID, ObMViewOpArg> MViewMdsOpMap;
private:
class GetMinMVMdsSnapshotFunctor
{
public:
  GetMinMVMdsSnapshotFunctor(share::SCN &scn):scn_(scn)
  {};
  virtual ~GetMinMVMdsSnapshotFunctor() {};
  int operator()(hash::HashMapPair<transaction::ObTransID, ObMViewOpArg> &mv_mds_kv);
private:
  share::SCN &scn_;
};
class CheckMVMdsExistFunctor
{
public:
  CheckMVMdsExistFunctor(bool &exist,
                         const uint64_t refresh_id,
                         const share::SCN &target_data_sync_scn) :
                        exist_(exist),
                        refresh_id_(refresh_id),
                        target_data_sync_scn_(target_data_sync_scn)
  {};
  virtual ~CheckMVMdsExistFunctor() {};
  int operator()(hash::HashMapPair<transaction::ObTransID, ObMViewOpArg> &mv_mds_kv);
private:
  bool &exist_;
  uint64_t refresh_id_;
  const share::SCN &target_data_sync_scn_;
};
class GetMVMinTargetDataSyncScnFunctor
{
public:
  GetMVMinTargetDataSyncScnFunctor(const uint64_t mview_id,
                                   share::SCN &target_data_sync_scn) :
                                   mview_id_(mview_id),
                                   target_data_sync_scn_(target_data_sync_scn)
  {};
  virtual ~GetMVMinTargetDataSyncScnFunctor() {};
  int operator()(hash::HashMapPair<transaction::ObTransID, ObMViewOpArg> &mv_mds_kv);
private:
  uint64_t mview_id_;
  share::SCN &target_data_sync_scn_;
};

public:
  ObMViewMaintenanceService();
  virtual ~ObMViewMaintenanceService();
  DISABLE_COPY_ASSIGN(ObMViewMaintenanceService);

  // for MTL
  static int mtl_init(ObMViewMaintenanceService *&service);
  int init();
  int start();
  void stop();
  void wait();
  void destroy();

  // for replay, do nothing
  int replay(const void *buffer, const int64_t nbytes, const palf::LSN &lsn,
             const share::SCN &scn) override final
  {
    UNUSED(buffer);
    UNUSED(nbytes);
    UNUSED(lsn);
    UNUSED(scn);
    return OB_SUCCESS;
  }

  // for checkpoint, do nothing
  share::SCN get_rec_scn() override final { return share::SCN::max_scn(); }
  int flush(share::SCN &rec_scn) override final
  {
    UNUSED(rec_scn);
    return OB_SUCCESS;
  }

  // for role change
  void switch_to_follower_forcedly() override final;
  int switch_to_leader() override final;
  int switch_to_follower_gracefully() override final;
  int resume_leader() override final;

  int get_mview_refresh_info(const ObIArray<uint64_t> &src_mview_ids,
                             ObMySQLProxy *sql_proxy,
                             const share::SCN &read_snapshot,
                             ObIArray<uint64_t> &mview_ids,
                             ObIArray<uint64_t> &mview_refresh_scns);
  int get_mview_last_refresh_info(const ObIArray<uint64_t> &src_mview_ids,
                                  ObMySQLProxy *sql_proxy,
                                  const uint64_t tenant_id,
                                  const share::SCN &scn,
                                  ObIArray<uint64_t> &mview_ids,
                                  ObIArray<uint64_t> &last_refresh_scns,
                                  ObIArray<uint64_t> &mview_refresh_modes);
  int update_mview_refresh_info_cache(const ObIArray<uint64_t> &mview_ids,
                                      const ObIArray<uint64_t> &mview_refresh_scns,
                                      const ObIArray<uint64_t> &mview_refresh_modes);
  int fetch_mv_refresh_scns(const ObIArray<uint64_t> &src_mview_ids,
                            const share::SCN &read_snapshot,
                            ObIArray<uint64_t> &mview_ids,
                            ObIArray<uint64_t> &mview_refresh_scns,
                            bool &hit_cache);

  MViewMdsOpMap &get_mview_mds_op() { return mview_mds_map_; }
  const MViewMdsOpMap &get_mview_mds_op() const { return mview_mds_map_; }
  void update_mview_mds_ts(int64_t ts) { mview_mds_timestamp_ = ts; }
  int64_t get_mview_mds_ts() { return mview_mds_timestamp_; }
  int get_min_mview_mds_snapshot(share::SCN &scn);
  int get_min_mview_pending_task_snapshot(share::SCN &scn);
  int check_nested_mview_mds_exists(const uint64_t refresh_id,
                                    const share::SCN &target_data_sync_scn);
  int get_min_target_data_sync_scn(const uint64_t mview_id,
                                   share::SCN &target_data_sync_scn);
  int64_t get_proposal_id() { return proposal_id_; }

#ifdef OB_BUILD_MV_REFRESH_QUEUEING
  ObMViewPendingTaskManager *get_pending_task_manager() { return &pending_task_manager_; }
  const ObMViewPendingTaskManager *get_pending_task_manager() const { return &pending_task_manager_; }
#endif
private:
  int inner_switch_to_leader();
  int inner_switch_to_follower();
  void sys_ls_task_stop_();
  int get_mview_last_refresh_info_sql_(const share::SCN &scn,
                                       const ObIArray<uint64_t> &mview_ids,
                                       const uint64_t tenant_id,
                                       ObSqlString &sql);

private:
  bool is_inited_;
  ObMLogMaintenanceTask mlog_maintenance_task_;
  ObMViewMaintenanceTask mview_maintenance_task_;
  ObMViewRefreshStatsMaintenanceTask mvref_stats_maintenance_task_;
  ObMViewPushRefreshScnTask mview_push_refresh_scn_task_;
  ObMViewPushSnapshotTask mview_push_snapshot_task_;
  ObReplicaSafeCheckTask replica_safe_check_task_;
  ObCollectMvMergeInfoTask collect_mv_merge_info_task_;
  ObMViewCleanSnapshotTask mview_clean_snapshot_task_;
  ObMviewUpdateCacheTask mview_update_cache_task_;
  ObMViewMdsOpTask mview_mds_task_;
  MViewRefreshInfoCache mview_refresh_info_cache_;
  int64_t mview_refresh_info_timestamp_;
  int64_t mview_mds_timestamp_;
  MViewMdsOpMap mview_mds_map_;
  int64_t proposal_id_;
#ifdef OB_BUILD_MV_REFRESH_QUEUEING
  ObMViewPendingTaskManager pending_task_manager_;
#endif
};

} // namespace rootserver
} // namespace oceanbase
