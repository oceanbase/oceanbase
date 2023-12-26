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

#ifndef OCEANBASE_LOG_ROUTE_SERVICE_H_
#define OCEANBASE_LOG_ROUTE_SERVICE_H_

#include "common/ob_region.h"               // ObRegion
#include "lib/hash/ob_hashset.h"            // ObHashSet
#include "lib/hash/ob_linear_hash_map.h"    // ObLinearHashMap
#include "lib/mysqlclient/ob_isql_client.h" // ObISQLClient
#include "lib/thread/thread_mgr_interface.h"// TGTaskHandler
#include "logservice/palf/lsn.h"            // LSN
#include "ob_external_server_blacklist.h"   // ObLogSvrBlacklist
#include "ob_log_systable_queryer.h"        // ObLogSysTableQueryer
#include "ob_log_route_struct.h"            // ObLSRouterKey, ObLSRouterValue
#include "ob_log_all_svr_cache.h"           // ObLogAllSvrCache

namespace oceanbase
{
namespace logservice
{
/*
 * LogRouteService is used for OB-CDC and Standby cluster etc to fetch log. Supports multiple policies, such as:
 *
 * 1. Fetch specified Region logs preferentially;
 * 2. Select machines based on multiple replicated type policies;
 * 3. Fetch log to the standby replica preferentially to minimize the impact on the leader.
 *    If logs cannot be obtained from all replica, logs can be obtained from the leader;
 * 4. Complete blacklist mechanism, including internal automatic maintenance of Server blacklist and whitewashing mechanism;
 *    Supports the list of servers for filtering external configurations;
 * 5. Perfect switch server mechanism, detection of serviceable machines, automatic fault switching,
 *    to ensure availability and real-time;
 * 6. Supports filtering encrypts Zone to avoid unnecessary access;
 * 7. Supports filtering inactive servers.
 *    ......
 *
 */
class ObLogRouteService : public lib::TGTaskHandler
{
public:
  ObLogRouteService();
  virtual ~ObLogRouteService();

  // @param [in] proxy             ObMySQLProxy
  // @param [in] prefer_region     Prefer Region to fetch log
  // @param [in] cluster_id        ClusterID
  // @param [in] is_across_cluster Whether SQL queries cross cluster
  //   For Standby Cluster, need to set is_across_cluster=true. So SQL Query will contain cluster_id.
  //   For CDC, need to set is_across_cluster=false. So SQL Query will not contain cluster_id.
  // @param [in] external_server_blacklist  External server blacklist
  // @param [in] background_refresh_time_sec  Background periodic refresh time, in second
  // @param [in] all_server_cache_update_interval_sec AllServer periodic refresh time, in second
  // @param [in] all_zone_cache_update_interval_sec AllZone periodic refresh time, in second
  // @param [in] blacklist_survival_time_sec  The survival time of the Server in the blacklist, in seconds
  // @param [in] blacklist_survival_time_upper_limit_min  Upper threshold of the Server blacklist duration (in minute)
  // @param [in] blacklist_survival_time_penalty_period_min
  //    The server is blacklisted, based on the time of the current server service LS - to decide whether to penalize
  //    the survival time.
  //    When the service time is less than a certain interval, a doubling-live-time policy is adopted (in minutes)
  // @param [in] blacklist_history_overdue_time_min Blacklist history expiration time, used to delete history(in minutes)
  // @param [in] blacklist_history_clear_interval_min  Clear blacklist history period(in minutes)
  //
  // @retval OB_SUCCESS           Success
  // @retval Other return values  Failed
  int init(ObISQLClient *proxy,
      const common::ObRegion &prefer_region,
      const int64_t cluster_id,
      const bool is_across_cluster,
      logfetcher::IObLogErrHandler *err_handler,
      const char *external_server_blacklist = "|",
      const int64_t background_refresh_time_sec = 10,
      const int64_t all_server_cache_update_interval_sec = 5,
      const int64_t all_zone_cache_update_interval_sec = 5,
      const int64_t blacklist_survival_time_sec = 30,
      const int64_t blacklist_survival_time_upper_limit_min = 4,
      const int64_t blacklist_survival_time_penalty_period_min = 1,
      const int64_t blacklist_history_overdue_time_min = 30,
      const int64_t blacklist_history_clear_interval_min = 20,
      const bool is_tenant_mode = false,
      const uint64_t tenant_id = OB_INVALID_TENANT_ID,
      const uint64_t self_tenant_id = OB_SERVER_TENANT_ID);
  int start();
  void stop();
  void wait();
  void destroy();
  virtual void handle(void *task);

public:
  //// Obtain and modify related parameters
  // Background refresh time
  int update_background_refresh_time(const int64_t background_refresh_time_sec);
  int get_background_refresh_time(int64_t &background_refresh_time_sec);

  // Region
  int update_preferred_upstream_log_region(const common::ObRegion &prefer_region);
  int get_preferred_upstream_log_region(common::ObRegion &prefer_region);

  // Cache interval
  int update_cache_update_interval(const int64_t all_server_cache_update_interval_sec,
      const int64_t all_zone_cache_update_interval_sec);
  int get_cache_update_interval(int64_t &all_server_cache_update_interval_sec,
      int64_t &all_zone_cache_update_interval_sec);

  // BlackList
  int update_blacklist_parameter(
      const int64_t blacklist_survival_time_sec,
      const int64_t blacklist_survival_time_upper_limit_min,
      const int64_t blacklist_survival_time_penalty_period_min,
      const int64_t blacklist_history_overdue_time_min,
      const int64_t blacklist_history_clear_interval_min);
  int get_blacklist_parameter(
      int64_t &blacklist_survival_time_sec,
      int64_t &blacklist_survival_time_upper_limit_min,
      int64_t &blacklist_survival_time_penalty_period_min,
      int64_t &blacklist_history_overdue_time_min,
      int64_t &blacklist_history_clear_interval_min);

  // Non-blocking registered: generate an asynchronous task
  //
  // @param [in] tenant_id        tenantID
  // @param [in] ls_id            LS ID
  //
  // @retval OB_SUCCESS           Success
  // @retval OB_EAGAIN            thread queue is already full
  // @retval OB_HASH_EXIST        tenant_ls_id is already registe async task
  // @retval Other return values  Failed
  int registered(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id);

  // Remove the task when LS remove
  //
  // @param [in] tenant_id        tenantID
  // @param [in] ls_id            LS ID
  //
  // @retval OB_SUCCESS           Success
  // @retval Other return values  Failed
  int remove(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id);

  // Return all the LS for the specified tenant
  //
  // @param [in] tenant_id        tenantID
  // @param [out] ls_ids          all LS ID
  //
  // @retval OB_SUCCESS           Success
  // @retval Other return values  Failed
  int get_all_ls(
      const uint64_t tenant_id,
      ObIArray<share::ObLSID> &ls_ids);

  // Get the server synchronously, iterate over the server for the next service log.
  // If the local cache does not exist, the SQL query is triggered and construct struct to insert.
  //
  // 1. If the server has completed one round of iteration (all servers have been iterated over), then OB_ITER_END is returned
  // 2. After returning OB_ITER_END, the list of servers will be iterated over from the beginning next time
  // 3. If no servers are available, return OB_ITER_END
  //
  // @param [in] tenant_id        tenantID
  // @param [in] ls_id            LS ID
  // @param [in] next_lsn         next LSN
  // @param [out] svr             return fetch log server
  //
  // @retval OB_SUCCESS           Success
  // @retval OB_ITER_END          Server list iterations complete one round, need to query server
  // @retval Other return values  Failed, may be network anomaly and so on.
  int next_server(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const palf::LSN &next_lsn,
      common::ObAddr &svr);

  // @param [in] tenant_id        tenantID
  // @param [in] ls_id            LS ID
  // @param [out] svr_array       return fetch log server
  //
  // @retval OB_SUCCESS           Success
  // @retval Other return values  Failed
  int get_server_array_for_locate_start_lsn(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      ObIArray<common::ObAddr> &svr_array);

  // Get the leader synchronously. If the local cache does not exist or the leader information do not get,
  // query the LEADER synchronously
  //
  // @param [in] tenant_id        tenantID
  // @param [in] ls_id            LS ID
  // @param [out] leader          return leader
  //
  // @retval OB_SUCCESS           Success
  // @retval OB_NOT_MASTER        Current has no leader, need retry
  // @retval Other return values  Failed
  int get_leader(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      common::ObAddr &leader);

  // Determine if the server needs to be switched
  //
  // @param [in] tenant_id        tenantID
  // @param [in] ls_id            LS ID
  // @param [in] next_lsn         next LSN
  // @param [in] cur_svr          current fetch log server
  //
  // @retval true                 Need to switch server
  // @retval false                Don't need to switch server
  bool need_switch_server(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const palf::LSN &next_lsn,
      const common::ObAddr &cur_svr);

  // Get available server count
  //
  // @param [in] tenant_id        tenantID
  // @param [in] ls_id            LS ID
  // @param [out] avail_svr_count available server count
  //
  // @retval OB_SUCCESS           Success
  // @retval Other error codes    Fail
  int get_server_count(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      int64_t &avail_svr_count) const;

  // Add server to blacklist
  //
  // @param [in] tenant_id        tenantID
  // @param [in] ls_id            LS ID
  // @param [in] svr              blacklisted sever
  // @param [in] svr_service_time Current server service time
  //
  // @retval OB_SUCCESS           Success
  // @retval Other error codes    Fail
  int add_into_blacklist(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const common::ObAddr &svr,
      const int64_t svr_service_time,
      int64_t &survival_time);

   // External Server blacklist, default is|, means no configuration, support configuration single/multiple servers
   // Single: SEVER_IP1:PORT1
   // Multiple: SEVER_IP1:PORT1|SEVER_IP2:PORT2|SEVER_IP3:PORT3
   // Used to filter server
   //
   // @param [in] server_blacklist External server blacklist string
   //
   // @retval OB_SUCCESS           Success
   // @retval Other error codes    Fail
   int set_external_svr_blacklist(const char *server_blacklist);

  // Launch an asynchronous update task for the server list of LS
  //
  // @param [in] tenant_id        tenantID
  // @param [in] ls_id            LS ID
  //
  // @retval OB_SUCCESS           Success
  // @retval OB_EAGAIN            thread queue is already full
  // @retval Other return values  Failed
   int async_server_query_req(
       const uint64_t tenant_id,
       const share::ObLSID &ls_id);

private:
  int get_ls_svr_list_(const ObLSRouterKey &router_key,
      LSSvrList &svr_list);

  int query_ls_log_info_and_update_(const ObLSRouterKey &router_key,
      LSSvrList &svr_list);

  int query_units_info_and_update_(const ObLSRouterKey &router_key,
      LSSvrList &svr_list);

  int get_ls_router_value_(
      const ObLSRouterKey &router_key,
      ObLSRouterValue *&router_value);
  // @retval OB_SUCCESS           Create ObLSRouterValue and insert success
  // @retval OB_ENTRY_EXIST       Has been exist
  // @retval Other error codes    Fail
  int handle_when_ls_route_info_not_exist_(
      const ObLSRouterKey &router_key,
      ObLSRouterValue *&router_value);
  int update_all_ls_server_list_();
  int update_server_list_(
      const ObLSRouterKey &router_key,
      ObLSRouterValue &router_value);
  // query the GV$OB_UNITS
  int query_units_info_and_update_(
      const ObLSRouterKey &router_key,
      ObLSRouterValue &router_value);
  int update_all_server_and_zone_cache_();
  void free_mem_();

private:
  static const int64_t DEFAULT_LS_ROUTE_KEY_SET_SIZE = 64;
  static const int64_t LS_ROUTER_VALUE_SIZE = sizeof(ObLSRouterValue);
  static const int64_t ASYN_TASK_VALUE_SIZE = sizeof(ObLSRouterAsynTask);
  static const int NWAY = 4;
  typedef common::hash::ObHashSet<ObLSRouterKey> LSRouteKeySet;
  typedef common::ObLinearHashMap<ObLSRouterKey, ObLSRouterValue *> LSRouterMap;

  struct ObLSRouterKeyGetter
  {
    ObLSRouterKeyGetter(ObLogRouteService &log_route_service,
        const int64_t cluster_id,
        const uint64_t tenant_id,
        ObIArray<share::ObLSID> &ls_ids) :
        log_route_service_(log_route_service),
        cluster_id_(cluster_id),
        tenant_id_(tenant_id),
        ls_ids_(&ls_ids) {}
    bool operator()(const ObLSRouterKey &key, ObLSRouterValue *value);

    ObLogRouteService &log_route_service_;
    int64_t cluster_id_;
    uint64_t tenant_id_;
    ObIArray<share::ObLSID> *ls_ids_;
  };

  struct ObLSRouterValueGetter
  {
    ObLSRouterValueGetter(ObLogRouteService &log_route_service,
        ObIArray<ObLSRouterValue *> &router_values) :
        log_route_service_(log_route_service),
        router_values_(&router_values) {}
    bool operator()(const ObLSRouterKey &key, ObLSRouterValue *value);

    ObLogRouteService &log_route_service_;
    ObIArray<ObLSRouterValue *> *router_values_;
  };

  struct ObAllLSRouterKeyGetter
  {
    ObAllLSRouterKeyGetter(): router_keys_() {}
    bool operator()(const ObLSRouterKey &key, ObLSRouterValue *value);

    ObSEArray<ObLSRouterKey, 4> router_keys_;
  };

  struct ObLSRouterValueUpdater
  {
    ObLSRouterValueUpdater(const LSSvrList &svr_list): svr_list_(svr_list) {}
    bool operator()(const ObLSRouterKey &key, ObLSRouterValue *value);
    const LSSvrList &svr_list_;
  };

  class ObLSRouteTimerTask : public common::ObTimerTask
  {
  public:
    explicit ObLSRouteTimerTask(ObLogRouteService &log_route_service);
    virtual ~ObLSRouteTimerTask() {}
    int init(int tg_id);
    void destroy();
    virtual void runTimerTask() override;
    static const int64_t REFRESH_INTERVAL = 5 * _SEC_;
  private:
    bool is_inited_;
    ObLogRouteService &log_route_service_;
  };
  int schedule_ls_timer_task_();

private:
  bool is_inited_;
  int64_t cluster_id_;
  bool is_tenant_mode_;
  int64_t source_tenant_id_;
  uint64_t self_tenant_id_;
  volatile bool is_stopped_ CACHE_ALIGNED;
  LSRouteKeySet ls_route_key_set_;
  LSRouterMap ls_router_map_;
  ObSliceAlloc log_router_allocator_;
  ObSliceAlloc asyn_task_allocator_;
  ObLogSvrBlacklist svr_blacklist_;
  ObLogSysTableQueryer systable_queryer_;
  ObLogAllSvrCache all_svr_cache_;
  ObLSRouteTimerTask ls_route_timer_task_;
  common::ObTimer timer_;
  logfetcher::IObLogErrHandler *err_handler_;
  int timer_id_;
  int tg_id_;
  int64_t background_refresh_time_sec_;
  int64_t blacklist_survival_time_sec_;
  int64_t blacklist_survival_time_upper_limit_min_;
  int64_t blacklist_survival_time_penalty_period_min_;
  int64_t blacklist_history_overdue_time_min_;
  int64_t blacklist_history_clear_interval_min_;

  DISALLOW_COPY_AND_ASSIGN(ObLogRouteService);
};

} // namespace logservice
} // namespace oceanbase

#endif

