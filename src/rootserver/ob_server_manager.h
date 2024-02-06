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

#ifndef OCEANBASE_ROOTSERVER_OB_SERVER_MANAGER_H_
#define OCEANBASE_ROOTSERVER_OB_SERVER_MANAGER_H_

#include "lib/container/ob_array.h"
#include "lib/net/ob_addr.h"
#include "common/ob_zone.h"
#include "share/ob_lease_struct.h"
#include "share/partition_table/ob_iserver_trace.h"
#include "share/ob_rpc_struct.h"
#include "rootserver/ob_rs_reentrant_thread.h"
#include "share/ob_server_status.h"
#include "rootserver/ob_server_table_operator.h"
#include "rootserver/ob_leader_coordinator.h"

namespace oceanbase {
namespace common {
class ObServerConfig;
class ObMySQLProxy;
}  // namespace common
namespace obrpc {
class ObSrvRpcProxy;
}
namespace rootserver {
class ObUnitManager;
class ObZoneManager;
class ObILeaderCoordinator;
class ObFreezeInfoManager;
class ObRebalanceTaskMgr;
class ObIStatusChangeCallback {
public:
  virtual int wakeup_balancer() = 0;
  virtual int wakeup_daily_merger() = 0;
  // FIXME(): make it suitable for different task type, this is just a sample iterface
  virtual int on_server_status_change(const common::ObAddr& server) = 0;
  virtual int on_start_server(const common::ObAddr& server) = 0;
  virtual int on_stop_server(const common::ObAddr& server) = 0;
  virtual int on_offline_server(const common::ObAddr& server) = 0;
};
class ObIServerChangeCallback {
public:
  virtual int on_server_change() = 0;
};
class ObServerManager : public share::ObIServerTrace {
public:
  typedef common::ObIArray<common::ObAddr> ObIServerArray;
  typedef common::ObArray<common::ObAddr> ObServerArray;
  typedef common::ObArray<share::ObServerStatus> ObServerStatusArray;
  typedef common::hash::ObHashMap<common::ObAddr, ObServerSwitchLeaderInfoStat::ServerSwitchLeaderInfo,
      common::hash::NoPthreadDefendMode>
      ServerSwitchLeaderInfoMap;
  friend class FakeServerMgr;
  ObServerManager();
  virtual ~ObServerManager();
  int init(ObIStatusChangeCallback& status_change_callback, ObIServerChangeCallback& server_change_callback,
      common::ObMySQLProxy& proxy, ObUnitManager& unit_mgr, ObZoneManager& zone_mgr,
      ObILeaderCoordinator& leader_coordinator, common::ObServerConfig& config, const common::ObAddr& rs_addr,
      obrpc::ObSrvRpcProxy& rpc_proxy);
  inline bool is_inited() const
  {
    return inited_;
  }
  virtual int add_server(const common::ObAddr& server, const common::ObZone& zone);
  virtual int delete_server(const common::ObIArray<common::ObAddr>& servers, const common::ObZone& zone);
  virtual int end_delete_server(const common::ObAddr& server, const common::ObZone& zone, const bool commit = true);
  virtual int start_server_list(const obrpc::ObServerList& server_list, const common::ObZone& zone);
  virtual int stop_server_list(const obrpc::ObServerList& server_list, const common::ObZone& zone);
  // only add to memory
  int add_server_list(const obrpc::ObServerInfoList& server_list, uint64_t& server_id);

  // server_id is OB_INVALID_ID before build server manager from __all_server
  int receive_hb(
      const share::ObLeaseRequest& lease_request, uint64_t& server_id, bool& to_alive, bool& update_delay_time_flag);

  int expend_server_lease(const common::ObAddr& server, const int64_t new_lease_end);

  // if server not exist or server's status is not serving, return false
  // otherwise, return true
  virtual int check_server_alive(const common::ObAddr& server, bool& is_alive) const;
  virtual int check_server_active(const common::ObAddr& server, bool& is_active) const;
  virtual int check_in_service(const common::ObAddr& addr, bool& service_started) const;
  virtual int check_server_stopped(const common::ObAddr& server, bool& is_stopped) const;
  virtual int check_server_permanent_offline(const common::ObAddr& server, bool& is_offline) const;
  virtual int check_migrate_in_blocked(const common::ObAddr& addr, bool& blocked) const;
  virtual int check_server_valid_for_partition(const common::ObAddr& server, bool& is_valid) const;

  virtual int get_alive_servers(const common::ObZone& zone, ObIServerArray& server_list) const;
  virtual int get_servers_by_status(ObIServerArray& active_server_list, ObIServerArray& inactive_server_list) const;
  virtual int get_servers_by_status(
      const common::ObZone& zone, ObIServerArray& active_server_list, ObIServerArray& inactive_server_list) const;
  virtual int get_alive_server_count(const common::ObZone& zone, int64_t& count) const;
  virtual int get_active_server_count(const common::ObZone& zone, int64_t& count) const;
  virtual int get_active_server_array(const common::ObZone& zone, ObIServerArray& server_list) const;
  virtual int get_servers_of_zone(const common::ObZone& zone, ObServerArray& server_list) const;
  virtual int get_servers_takenover_by_rs(const common::ObZone& zone, ObIServerArray& server_list) const;
  virtual int finish_server_recovery(const common::ObAddr& server);
  void clear_in_recovery_server_takenover_by_rs(const common::ObAddr& server);
  int get_server_count(const ObZone& zone, int64_t& alive_count, int64_t& not_alive_count) const;
  // used by check server thread which set server offline if needed
  int check_servers();
  int try_renew_rs_list();

  int is_server_exist(const common::ObAddr& server, bool& exist) const;
  // get ObServerStatus through server addr, return OB_ENTRY_NOT_EXIST if not exist
  virtual int get_server_status(const common::ObAddr& server, share::ObServerStatus& server_status) const;
  int update_server_status(const share::ObServerStatus& server_status);
  // build ObServerManager from __all_server table
  int load_server_manager();
  int load_server_statuses(const ObServerStatusArray& server_status);
  virtual bool has_build() const;
  virtual int get_all_server_list(common::ObIArray<common::ObAddr>& server_list);
  // get server infos of zone, if zone is empty, get all server_infos
  virtual int get_server_statuses(const common::ObZone& zone, ObServerStatusArray& server_statuses) const;
  virtual int get_server_statuses(const ObServerArray& servers, ObServerStatusArray& server_statuses) const;
  int get_persist_server_statuses(ObServerStatusArray& server_statuses);
  int adjust_server_status(const ObAddr& server, ObRebalanceTaskMgr& rebalance_task_mgr, const bool with_rootserver);
  int get_lease_duration(int64_t& lease_duration_time) const;
  virtual int get_server_zone(const common::ObAddr& addr, common::ObZone& zone) const;
  inline ObIStatusChangeCallback& get_status_change_callback() const;
  inline const common::ObAddr& get_rs_addr() const
  {
    return rs_addr_;
  }
  void reset();

  // set %zone_merged to true if servers in the same zone of %addr merged to %frozen_version
  virtual int update_merged_version(const common::ObAddr& addr, int64_t frozen_version, bool& zone_merged);
  int get_merged_version(const common::ObAddr& addr, int64_t& merged_version) const;

  int block_migrate_in(const common::ObAddr& addr);
  int unblock_migrate_in(const common::ObAddr& addr);

  int64_t to_string(char* buf, const int64_t buf_len) const;

  virtual int set_with_partition(const common::ObAddr& server);
  virtual int clear_with_partiton(const common::ObAddr& server, const int64_t last_hb_time);
  virtual int set_force_stop_hb(const common::ObAddr& server, const bool& force_stop_hb);
  virtual int is_server_stopped(const common::ObAddr& server, bool& is_stopped) const;
  virtual int update_leader_cnt_status(ObServerSwitchLeaderInfoStat::ServerSwitchLeaderInfoMap& info_map);
  virtual int get_server_leader_cnt(const common::ObAddr& server, int64_t& leader_cnt) const;
  int check_other_zone_stopped(const common::ObZone& zone, bool& stopped);
  int have_server_stopped(const common::ObZone& zone, bool& is_stopped) const;
  int get_min_server_version(char min_server_version[OB_SERVER_VERSION_LENGTH]);
  bool have_server_deleting() const;
  int check_all_server_active(bool& all_active) const;

protected:
  int construct_not_empty_server_set(common::hash::ObHashSet<common::ObAddr>& not_empty_server_set);
  // update server status by lease_request;
  int process_report_status_change(const share::ObLeaseRequest& lease_request, share::ObServerStatus& server_status);
  int set_server_status(const share::ObLeaseRequest& lease_request, const int64_t hb_timestamp,
      const bool with_rootserver, share::ObServerStatus& server_status);
  int set_server_delay_time(
      const int64_t server_behind_time, const int64_t round_trip_time, share::ObServerStatus& server_status);
  int reset_existing_rootserver();

  int update_admin_status(
      const common::ObAddr& server, const share::ObServerStatus::ServerAdminStatus status, const bool remove);

  int set_migrate_in_blocked(const common::ObAddr& addr, const bool block);

  int find(const common::ObAddr& server, const share::ObServerStatus*& status) const;
  int find(const common::ObAddr& server, share::ObServerStatus*& status);
  int fetch_new_server_id(uint64_t& server_id);
  int check_server_id_used(const uint64_t server_id, bool& server_id_used);
  int start_or_stop_server(const common::ObAddr& server, const common::ObZone& zone, const bool is_start);
  virtual int start_server(const common::ObAddr& server, const common::ObZone& zone);
  virtual int stop_server(const common::ObAddr& server, const common::ObZone& zone);

protected:
  bool inited_;
  bool has_build_;  // has been loaded from __all_server table

  common::SpinRWLock server_status_rwlock_;  // to protect server_statuses_
  common::SpinRWLock maintaince_lock_;       // avoid maintain operation run concurrently

  ObIStatusChangeCallback* status_change_callback_;
  ObIServerChangeCallback* server_change_callback_;
  common::ObServerConfig* config_;
  ObUnitManager* unit_mgr_;
  ObZoneManager* zone_mgr_;
  ObILeaderCoordinator* leader_coordinator_;
  obrpc::ObSrvRpcProxy* rpc_proxy_;

  ObServerTableOperator st_operator_;
  common::ObAddr rs_addr_;
  ObServerStatusArray server_statuses_;

  DISALLOW_COPY_AND_ASSIGN(ObServerManager);
};

ObIStatusChangeCallback& ObServerManager::get_status_change_callback() const
{
  return *status_change_callback_;
}

class ObHeartbeatChecker : public ObRsReentrantThread {
public:
  ObHeartbeatChecker();
  int init(ObServerManager& server_manager);
  virtual ~ObHeartbeatChecker();

  virtual void run3() override;
  virtual int blocking_run() override
  {
    BLOCKING_RUN_IMPLEMENT();
  }
  int64_t get_schedule_interval() const override;

private:
  static const int64_t CHECK_INTERVAL_US = 100 * 1000L;  // 100ms
  bool inited_;
  ObServerManager* server_manager_;
};

}  // end namespace rootserver
}  // end namespace oceanbase
#endif
