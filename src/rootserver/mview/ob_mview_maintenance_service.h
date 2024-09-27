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

namespace oceanbase
{
namespace rootserver
{
class ObMviewUpdateCacheTask;
struct RefreshInfo
{
  uint64_t refresh_scn_;
  int64_t refresh_ts_;
  int64_t expired_ts_;
  bool hit_cache(const share::SCN &read_snapshot) {
    return expired_ts_ >= read_snapshot.convert_to_ts() &&
           read_snapshot.get_val_for_tx() >= refresh_scn_;
  }
  RefreshInfo() : refresh_scn_(0), refresh_ts_(0), expired_ts_(0)
  {};
};

typedef common::hash::
    ObHashMap<uint64_t, RefreshInfo, common::hash::NoPthreadDefendMode>
    ObMviewRefreshInfoMap;
class ObMViewMaintenanceService : public logservice::ObIReplaySubHandler,
                                  public logservice::ObICheckpointSubHandler,
                                  public logservice::ObIRoleChangeSubHandler
{
public:
  static const int64_t CacheValidInterval = 30 * 1000 * 1000; //30s
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
  static int extract_sql_result(sqlclient::ObMySQLResult *mysql_result,
                                ObIArray<uint64_t> &mview_ids,
                                ObIArray<uint64_t> &last_refresh_scns,
                                ObIArray<uint64_t> &mview_refresh_modes);
  static int update_mview_refresh_info_cache(const ObIArray<uint64_t> &mview_ids,
                                             const ObIArray<uint64_t> &mview_refresh_scns,
                                             const ObIArray<uint64_t> &mview_refresh_modes,
                                             ObMviewRefreshInfoMap &mview_refresh_info_map);
  int fetch_mv_refresh_scns(const ObIArray<uint64_t> &src_mview_ids,
                            const share::SCN &read_snapshot,
                            ObIArray<uint64_t> &mview_ids,
                            ObIArray<uint64_t> &mview_refresh_scns,
                            uint64_t &not_hit_count);
  int64_t get_last_request_ts() const { return ATOMIC_LOAD(&last_request_ts_); }
  void set_last_request_ts(const int64_t last_request_ts) { ATOMIC_STORE(&last_request_ts_, last_request_ts); }
  ObMviewRefreshInfoMap &get_mview_refresh_info_map() { return mview_refresh_info_map_; }
private:
  int inner_switch_to_leader();
  int inner_switch_to_follower();

private:
  ObMLogMaintenanceTask mlog_maintenance_task_;
  ObMViewMaintenanceTask mview_maintenance_task_;
  ObMViewRefreshStatsMaintenanceTask mvref_stats_maintenance_task_;
  ObMViewPushRefreshScnTask mview_push_refresh_scn_task_;
  ObMViewPushSnapshotTask mview_push_snapshot_task_;
  ObReplicaSafeCheckTask replica_safe_check_task_;
  ObCollectMvMergeInfoTask collect_mv_merge_info_task_;
  ObMViewCleanSnapshotTask mview_clean_snapshot_task_;
  ObMviewUpdateCacheTask mview_update_cache_task_;
  ObMviewRefreshInfoMap mview_refresh_info_map_;
  int64_t last_request_ts_;
  bool is_inited_;
};

} // namespace rootserver
} // namespace oceanbase
