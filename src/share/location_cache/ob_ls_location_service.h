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

#ifndef OCEANBASE_SHARE_OB_LS_LOCATION_SERVICE
#define OCEANBASE_SHARE_OB_LS_LOCATION_SERVICE

#include "share/location_cache/ob_location_struct.h"
#include "share/location_cache/ob_location_update_task.h"
#include "share/location_cache/ob_ls_location_map.h"

namespace oceanbase
{
namespace common
{
class ObTimeoutCtx;
class ObMySQLProxy;
}

namespace share
{
class ObLSLocationUpdateTask;
class ObLogstreamLocationFetcher;
class ObLSLocationService;
class ObLSInfo;
class ObLSTableOperator;
typedef observer::ObUniqTaskQueue<ObLSLocationUpdateTask,
    ObLSLocationService> ObLSLocUpdateQueue;
namespace schema
{
class ObMultiVersionSchemaService;
}

class ObLSLocationUpdateQueueSet
{
public:
  ObLSLocationUpdateQueueSet(ObLSLocationService *location_service);
  virtual ~ObLSLocationUpdateQueueSet();
  int init();
  int add_task(const ObLSLocationUpdateTask &task);
  int set_thread_count(const int64_t thread_cnt);
  void stop();
  void wait();
private:
  const int64_t MINI_MODE_UPDATE_THREAD_CNT = 1;
  const int64_t LSL_TASK_QUEUE_SIZE = 100;
  const int64_t USER_TASK_QUEUE_SIZE = 10000;
  const int64_t MINI_MODE_USER_TASK_QUEUE_SIZE = 1000;
  bool inited_;
  ObLSLocationService *location_service_;
  ObLSLocUpdateQueue sys_tenant_queue_;    // Refresh log stream in sys tenant
  ObLSLocUpdateQueue meta_tenant_queue_;   // Refresh log stream in meta tenant
  ObLSLocUpdateQueue user_tenant_queue_;   // Refresh log streams in user tenant. Threads are configurable。
};

// This class is used to process ls location by ObLocationService.
class ObLSLocationService
{
public:
  ObLSLocationService();
  virtual ~ObLSLocationService();
  int init(
      ObLSTableOperator &lst,
      schema::ObMultiVersionSchemaService &schema_service,
      share::ObRsMgr &rs_mgr,
      obrpc::ObSrvRpcProxy &srv_rpc_proxy);
  int start();
  // Get ls location synchronously
  //
  // @param [in] expire_renew_time: The oldest renew time of cache which the user can tolerate.
  //                                Setting it to 0 means that the cache will not be renewed.
  //                                Setting it to INT64_MAX means renewing cache synchronously.
  // @param [out] is_cache_hit: If hit in location cache.
  // @param [out] location: Include all replicas' addresses of a log stream.
  // @return OB_LS_LOCATION_NOT_EXIST if no records in sys table.
  //         OB_GET_LOCATION_TIME_OUT if get location by inner sql timeout.
  int get(
      const int64_t cluster_id,
      const uint64_t tenant_id,
      const ObLSID &ls_id,
      const int64_t expire_renew_time,
      bool &is_cache_hit,
      ObLSLocation &location);
  // Get leader address of a log stream synchronously.
  //
  // @param [in] force_renew: whether to renew location synchronously.
  // @param [out] leader: leader address of the log stream.
  // @return OB_LS_LOCATION_NOT_EXIST if no records in sys table,
  //         OB_LS_LEADER_NOT_EXIST if no leader in location.
  //         OB_GET_LOCATION_TIME_OUT if get location by inner sql timeout.
  int get_leader(
      const int64_t cluster_id,
      const uint64_t tenant_id,
      const ObLSID &ls_id,
      const bool force_renew,
      common::ObAddr &leader);

  // Get leader of ls. If not hit in cache or leader not exist,
  // it will renew location and try to get leader again until abs_retry_timeout.
  //
  // @param [in] cluster_id: target cluster which the ls belongs to
  // @param [in] tenant_id: target tenant which the ls belongs to
  // @param [in] ls_id: target ls
  // @param [out] leader: leader address of the log stream.
  // @param [in] abs_retry_timeout: absolute timestamp for retry timeout.
  //             (default use remain timeout of ObTimeoutCtx or THIS_WORKER)
  // @param [in] retry_interval: interval between each retry. (default 100ms)
  // @return OB_LS_LOCATION_NOT_EXIST if no records in sys table,
  //         OB_LS_LEADER_NOT_EXIST if no leader in location.
  int get_leader_with_retry_until_timeout(
      const int64_t cluster_id,
      const uint64_t tenant_id,
      const ObLSID &ls_id,
      common::ObAddr &leader,
      const int64_t abs_retry_timeout = 0,
      const int64_t retry_interval = 100000/*100ms*/);

  // Nonblock way to get log stream location.
  //
  // @param [out] location: include all replicas' addresses of a log stream
  // @return OB_LS_LOCATION_NOT_EXIST if no records in sys table.
  int nonblock_get(
      const int64_t cluster_id,
      const uint64_t tenant_id,
      const ObLSID &ls_id,
      ObLSLocation &location);
  // Nonblock way to get leader address of the log stream.
  //
  // @param [out] leader: leader address of the log stream
  // @return OB_LS_LOCATION_NOT_EXIST if no records in sys table
  //         OB_LS_LEADER_NOT_EXIST if no leader in location.
  int nonblock_get_leader(
      const int64_t cluster_id,
      const uint64_t tenant_id,
      const ObLSID &ls_id,
      common::ObAddr &leader);
  // Nonblock way to renew location cache. It will trigger a location update task.
  int nonblock_renew(
      const int64_t cluster_id,
      const uint64_t tenant_id,
      const ObLSID &ls_id);
  // Nonblock way to renew location cache for all ls in a tenant.
  int nonblock_renew(
      const int64_t cluster_id,
      const uint64_t tenant_id);
  // renew location cache of ls_ids synchronously
  // @param [in] cluster_id: target cluster which the ls belongs to
  // @param [in] tenant_id: target tenant which the ls belongs to
  // @param [in] ls_ids: target ls_id array(can't have duplicate values)
  // @param [out] ls_locations: locations of ls_ids recorded in inner_table
  //                            (may be less than ls_ids when ls location not exist)
  int batch_renew_ls_locations(
      const int64_t cluster_id,
      const uint64_t tenant_id,
      const common::ObIArray<ObLSID> &ls_ids,
      common::ObIArray<ObLSLocation> &ls_locations);
  // renew all ls location caches for tenant
  int renew_location_for_tenant(
      const int64_t cluster_id,
      const uint64_t tenant_id,
      common::ObIArray<ObLSLocation> &locations);
  // Add update task into async_queue_set.
  int add_update_task(const ObLSLocationUpdateTask &task);
  // Process update tasks.
  int batch_process_tasks(
      const common::ObIArray<ObLSLocationUpdateTask> &tasks,
      bool &stopped);
  // Template interface. Unused.
  int process_barrier(const ObLSLocationUpdateTask &task, bool &stopped);
  void stop();
  void wait();
  int destroy();
  int reload_config();
  // For ObLSLocationTimerTask. Renew all ls location by __all_ls_meta_table.
  int renew_all_ls_locations();
  // For ObLSLocationByRpcTimerTask. Renew all ls location's leader info by rpc.
  int renew_all_ls_locations_by_rpc();
  // Clear dead cache.
  int check_and_clear_dead_cache();
  // Schedule renew all ls timer task.
  int schedule_ls_timer_task();
  // Schedule renew all ls by rpc timer task.
  int schedule_ls_by_rpc_timer_task();
  // Schedule dump ls location cache timer task.
  int schedule_dump_cache_timer_task();
  // Dump all ls locations in cache.
  int dump_cache();
  static bool is_valid_key(
      const int64_t cluster_id,
      const uint64_t tenant_id,
      const ObLSID &ls_id);

private:
  int check_inner_stat_() const;
  int get_from_cache_(
      const int64_t cluster_id,
      const uint64_t tenant_id,
      const ObLSID &ls_id,
      ObLSLocation &location);
  int renew_location_(
      const int64_t cluster_id,
      const uint64_t tenant_id,
      const ObLSID &ls_id,
      ObLSLocation &location);
  int fill_location_(const int64_t cluster_id, const ObLSInfo &ls_info, ObLSLocation &location);
  int update_cache_(
      const int64_t cluster_id,
      const uint64_t tenant_id,
      const ObLSID &ls_id,
      ObLSLocation &location);
  int erase_location_(
      const int64_t cluster_id,
      const uint64_t tenant_id,
      const ObLSID &ls_id);
  int build_tenant_ls_info_hash_(ObTenantLsInfoHashMap &hash);
  int construct_rpc_dests_(common::ObIArray<common::ObAddr> &addrs);
  int detect_ls_leaders_(
      const common::ObIArray<common::ObAddr> &dests,
      common::ObArray<share::ObLSLeaderLocation> &leaders);
  int batch_update_caches_(
      const int64_t cluster_id,
      const common::ObIArray<ObLSInfo> &ls_infos,
      const bool can_erase,
      common::ObIArray<ObLSLocation> &locations);
private:
  static const int64_t OB_LOCATION_CACHE_BUCKET_NUM = 512;
  static const int64_t RENEW_LS_LOCATION_INTERVAL_US = 5 * 1000 * 1000L; // 5s
  static const int64_t RENEW_LS_LOCATION_BY_RPC_INTERVAL_US = 1000 * 1000L; // 1s
  static const int64_t DUMP_CACHE_INTERVAL_US = 10 * 1000 * 1000L; // 10s

  bool inited_;
  bool stopped_;
  ObLSTableOperator *lst_;
  schema::ObMultiVersionSchemaService *schema_service_;
  ObRsMgr *rs_mgr_;
  obrpc::ObSrvRpcProxy *srv_rpc_proxy_;
  ObLSLocationMap inner_cache_;              // Stores location of log streams in all tenants.
  ObLSLocationUpdateQueueSet local_async_queue_set_;
  ObLSLocationUpdateQueueSet remote_async_queue_set_;
  common::ObTimer ls_loc_timer_;            // process ls_loc_timer_task
  common::ObTimer ls_loc_by_rpc_timer_;     // process ls_loc_by_rpc_timer_task
  common::ObTimer dump_log_timer_;          // process dump_cache_timer_task
  ObLSLocationTimerTask ls_loc_timer_task_; // task for renewing ls location cache regularly
  ObLSLocationByRpcTimerTask ls_loc_by_rpc_timer_task_;  // task for renewing ls location cache regularly by rpc
  ObDumpLSLocationCacheTimerTask dump_cache_timer_task_; // task for dumping all content in inner_cache_
  int64_t last_cache_clear_ts_;
  //TODO: need semaphore to limit concurrency
};

} // end namespace share
} // end namespace oceanbase
#endif
