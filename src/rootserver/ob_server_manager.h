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

// server manager is deprecated, please do not use it!!! USE SVR_TRACER INSTEAD!!!
// server manager is deprecated, please do not use it!!! USE SVR_TRACER INSTEAD!!!
// server manager is deprecated, please do not use it!!! USE SVR_TRACER INSTEAD!!!

#ifndef OCEANBASE_ROOTSERVER_OB_SERVER_MANAGER_H_
#define OCEANBASE_ROOTSERVER_OB_SERVER_MANAGER_H_

#include "lib/container/ob_array.h"
#include "lib/net/ob_addr.h"
#include "common/ob_zone.h"
#include "share/ob_lease_struct.h"
#include "share/ob_iserver_trace.h"
#include "share/ob_rpc_struct.h"
#include "rootserver/ob_rs_reentrant_thread.h"
#include "share/ob_server_status.h"
#include "share/ob_server_table_operator.h"

namespace oceanbase
{
namespace common
{
class ObServerConfig;
class ObMySQLProxy;
}
namespace obrpc
{
class ObSrvRpcProxy;
}
namespace rootserver
{
class ObUnitManager;
class ObZoneManager;
class ObFreezeInfoManager;
class ObDRTaskMgr;
class ObIStatusChangeCallback
{
public:
  virtual int wakeup_balancer() = 0;
  virtual int wakeup_daily_merger() = 0;
  //FIXME(jingqian): make it suitable for different task type, this is just a sample iterface
  virtual int on_server_status_change(const common::ObAddr &server) = 0;
  virtual int on_start_server(const common::ObAddr &server) = 0;
  virtual int on_stop_server(const common::ObAddr &server) = 0;
  virtual int on_offline_server(const common::ObAddr &server) = 0;
};
class ObIServerChangeCallback
{
public:
  virtual int on_server_change() = 0;
};
// server manager is deprecated, please do not use it!!! USE SVR_TRACER INSTEAD!!!
// server manager is deprecated, please do not use it!!! USE SVR_TRACER INSTEAD!!!
// server manager is deprecated, please do not use it!!! USE SVR_TRACER INSTEAD!!!
class ObServerManager : public share::ObIServerTrace
{
public:
  typedef common::ObIArray<common::ObAddr> ObIServerArray;
  typedef common::ObArray<common::ObAddr> ObServerArray;
  typedef common::ObArray<share::ObServerStatus> ObServerStatusArray;
  typedef common::ObIArray<share::ObServerStatus> ObServerStatusIArray;
  friend class FakeServerMgr;
  ObServerManager();
  virtual ~ObServerManager();
  int init(ObIStatusChangeCallback &status_change_callback,
           ObIServerChangeCallback &server_change_callback,
           common::ObMySQLProxy &proxy,
           ObUnitManager &unit_mgr,
           ObZoneManager &zone_mgr,
           common::ObServerConfig &config,
           const common::ObAddr &rs_addr,
           obrpc::ObSrvRpcProxy &rpc_proxy);
  inline bool is_inited() const { return inited_; }
  virtual int add_server(const common::ObAddr &server, const common::ObZone &zone);
  virtual int delete_server(const common::ObIArray<common::ObAddr> &servers,
      const common::ObZone &zone);
  virtual int end_delete_server(const common::ObAddr &server, const common::ObZone &zone,
                                const bool commit = true);
  virtual int start_server_list(const obrpc::ObServerList &server_list, const common::ObZone &zone);
  virtual int stop_server_list(const obrpc::ObServerList &server_list, const common::ObZone &zone);

  // server_id is OB_INVALID_ID before build server manager from __all_server
  int receive_hb(const share::ObLeaseRequest &lease_request,
                 uint64_t &server_id,
                 bool &to_alive);
  int get_server_id(
      const common::ObAddr &server,
      uint64_t &server_id) const;

  // if server not exist or server's status is not serving, return false
  // otherwise, return true
  virtual int check_server_alive(const common::ObAddr &server, bool &is_alive) const;
  virtual int check_server_active(const common::ObAddr &server, bool &is_active) const;
  virtual int check_in_service(const common::ObAddr &addr, bool &service_started) const;
  virtual int check_server_stopped(const common::ObAddr &server, bool &is_stopped) const;
  virtual int check_server_permanent_offline(const common::ObAddr &server, bool &is_offline) const;
  virtual int check_migrate_in_blocked(const common::ObAddr &addr, bool &blocked) const;
  virtual int get_servers_of_zone(
      const common::ObZone &zone,
      ObServerArray &server_list) const;
  virtual int get_servers_of_zone(
      const common::ObZone &zone,
      common::ObIArray<common::ObAddr> &server_list,
      common::ObIArray<uint64_t> &server_id_list) const;
  int get_server_count(const ObZone &zone, int64_t &alive_count, int64_t &not_alive_count) const;
  // used by check server thread which set server offline if needed
  int check_servers();
  int try_renew_rs_list();

  int is_server_exist(const common::ObAddr &server, bool &exist) const;
  // get ObServerStatus through server addr, return OB_ENTRY_NOT_EXIST if not exist
  virtual int get_server_status(const common::ObAddr &server,
                        share::ObServerStatus &server_status) const;
  int get_server_resource_info(
      const common::ObAddr &server,
      share::ObServerResourceInfo &resource_info);
  int update_server_status(const share::ObServerStatus &server_status);
  // build ObServerManager from __all_server table
  int load_server_manager();
  int load_server_statuses(const ObServerStatusArray &server_status);
  virtual bool has_build() const;
  virtual int get_server_statuses(const common::ObZone &zone,
      ObServerStatusIArray &server_statuses,
      bool include_permanent_offline = true) const;
  virtual int build_server_resource_info_result(
      const common::ObZone &zone,
      ObIArray<obrpc::ObGetServerResourceInfoResult> &active_servers_resource_info);
  virtual int get_server_statuses(const ObServerArray &servers,
                                  ObServerStatusArray &server_statuses) const;
  int get_persist_server_statuses(ObServerStatusArray &server_statuses);
  int adjust_server_status(
      const ObAddr &server,
      ObDRTaskMgr &disaster_recovery_task_mgr,
      const bool with_rootserver);
  virtual int get_server_zone(const common::ObAddr &addr, common::ObZone &zone) const;
  inline ObIStatusChangeCallback &get_status_change_callback() const;
  inline const common::ObAddr &get_rs_addr() const { return rs_addr_; }
  void reset();

  int64_t to_string(char *buf, const int64_t buf_len) const;
  virtual int is_server_stopped(const common::ObAddr &server, bool &is_stopped) const;
  int get_server_id(const ObZone &zone, const common::ObAddr &server, uint64_t &server_id) const;
  static int try_delete_server_working_dir(
      const common::ObZone &zone,
      const common::ObAddr &server,
      const int64_t svr_seq);
protected:
  int construct_not_empty_server_set(
      common::hash::ObHashSet<common::ObAddr> &not_empty_server_set);
  //update server status by lease_request;
  int process_report_status_change(const share::ObLeaseRequest &lease_request,
                                   share::ObServerStatus &server_status);
  int set_server_status(const share::ObLeaseRequest &lease_request,
                        const int64_t hb_timestamp,
                        const bool with_rootserver,
                        share::ObServerStatus &server_status);
  int reset_existing_rootserver();
  int update_admin_status(const common::ObAddr &server,
      const share::ObServerStatus::ServerAdminStatus status,
      const bool remove);

  int find(const common::ObAddr &server, const share::ObServerStatus *&status) const;
  int find(const common::ObAddr &server, share::ObServerStatus *&status);
  int fetch_new_server_id(uint64_t &server_id);
  int start_or_stop_server(const common::ObAddr &server,
      const common::ObZone &zone, const bool is_start);
  virtual int start_server(const common::ObAddr &server, const common::ObZone &zone);
  virtual int stop_server(const common::ObAddr &server, const common::ObZone &zone);
protected:
  bool inited_;
  bool has_build_;                          // has been loaded from __all_server table

  common::SpinRWLock server_status_rwlock_;  // to protect server_statuses_
  common::SpinRWLock maintaince_lock_; // avoid maintain operation run concurrently

  ObIStatusChangeCallback *status_change_callback_;
  ObIServerChangeCallback *server_change_callback_;
  common::ObServerConfig *config_;
  ObUnitManager *unit_mgr_;
  ObZoneManager *zone_mgr_;
  obrpc::ObSrvRpcProxy *rpc_proxy_;

  share::ObServerTableOperator st_operator_;
  common::ObAddr rs_addr_;
  ObServerStatusArray server_statuses_;

  DISALLOW_COPY_AND_ASSIGN(ObServerManager);
};

ObIStatusChangeCallback &ObServerManager::get_status_change_callback() const
{
  return *status_change_callback_;
}

class ObHeartbeatChecker : public ObRsReentrantThread
{
public:
  ObHeartbeatChecker();
  int init(ObServerManager &server_manager);
  virtual ~ObHeartbeatChecker();

  virtual void run3() override;
  virtual int blocking_run() { BLOCKING_RUN_IMPLEMENT(); }
  int64_t get_schedule_interval() const;
private:
  static const int64_t CHECK_INTERVAL_US = 100 * 1000L;  //100ms
  bool inited_;
  ObServerManager *server_manager_;
};

}//end namespace rootserver
}//end namespace oceanbase
#endif
