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

#ifndef OCEANBASE_SHARE_OB_ALL_SERVER_TRACER_H_
#define OCEANBASE_SHARE_OB_ALL_SERVER_TRACER_H_

#include "lib/lock/ob_spin_rwlock.h"
#include "lib/hash/ob_hashset.h"
#include "share/ob_iserver_trace.h"
#include "share/ob_server_table_operator.h"
#include "lib/function/ob_function.h"
#include "share/unit/ob_unit_info.h"        // ObTenantServers
#include "share/ob_unit_table_operator.h"   // ObUnitTableOperator

namespace oceanbase
{
namespace share
{
class ObServerTraceMap : public share::ObIServerTrace
{
public:
  ObServerTraceMap();
  virtual ~ObServerTraceMap();
  int init();
  virtual int is_server_exist(const common::ObAddr &server, bool &exist) const;
  virtual int get_server_rpc_port(const common::ObAddr &server, const int64_t sql_port,
                                          int64_t &rpc_port, bool &exist) const;
  virtual int check_server_alive(const common::ObAddr &server, bool &is_alive) const;
  virtual int check_in_service(const common::ObAddr &addr, bool &service_started) const;
  virtual int check_migrate_in_blocked(const common::ObAddr &addr, bool &is_block) const;
  virtual int check_server_permanent_offline(const common::ObAddr &server, bool &is_offline) const;
  virtual int check_server_active(const common::ObAddr &server, bool &is_active) const;
  virtual int check_server_can_migrate_in(const common::ObAddr &server, bool &can_migrate_in) const;
  virtual int is_server_stopped(const common::ObAddr &server, bool &is_stopped) const;
  virtual int get_server_zone(const common::ObAddr &server, common::ObZone &zone) const;
  virtual int get_servers_of_zone(
      const common::ObZone &zone,
      common::ObIArray<common::ObAddr> &servers) const;
  virtual int get_servers_of_zone(
      const common::ObZone &zone,
      common::ObIArray<common::ObAddr> &servers,
      common::ObIArray<uint64_t> &server_id_list) const;
  virtual int get_server_info(const common::ObAddr &server, ObServerInfoInTable &server_info) const;
  virtual int get_servers_info(
      const common::ObZone &zone,
      common::ObIArray<ObServerInfoInTable> &servers_info,
      bool include_permanent_offline) const;
  virtual int get_active_servers_info(
      const common::ObZone &zone,
      common::ObIArray<ObServerInfoInTable> &active_servers_info) const;
  virtual int get_alive_servers(const common::ObZone &zone, common::ObIArray<common::ObAddr> &server_list) const;
  // Atomically get alive_server and renew_time
  virtual int get_alive_servers_with_renew_time(const common::ObZone &zone,
                                                common::ObIArray<common::ObAddr> &server_list,
                                                int64_t &renew_time) const;
  virtual int get_alive_servers_count(const common::ObZone &zone, int64_t &count) const;
  virtual int get_alive_and_not_stopped_servers(const common::ObZone &zone, common::ObIArray<common::ObAddr> &server_list) const;
  virtual int get_servers_by_status(
      const ObZone &zone,
      common::ObIArray<common::ObAddr> &alive_server_list,
      common::ObIArray<common::ObAddr> &not_alive_server_list) const;
  virtual int get_min_server_version(
              char min_server_version_str[OB_SERVER_VERSION_LENGTH],
              uint64_t &min_observer_version);
  bool has_build() const {return has_build_; };
  int refresh(bool allow_broadcast = false);
  int for_each_server_info(const ObFunction<int(const ObServerInfoInTable &server_info)> &functor);
private:
  int find_server_status(const ObAddr &addr, ObServerInfoInTable &status) const;
  int get_rpc_port_status(const ObAddr &addr, const int64_t sql_port,
                          int64_t &rpc_port, ObServerInfoInTable &status) const;
  int find_server_info(const ObAddr &addr, ObServerInfoInTable &server_info) const;
  int broadcast_server_trace_() const;

private:
  static const int64_t DEFAULT_SERVER_COUNT = 2048;
  bool is_inited_;
  bool has_build_;
  mutable common::SpinRWLock lock_;
  int64_t renew_time_;
  common::ObArray<ObServerInfoInTable> server_info_arr_;
  common::ObSEArray<common::ObZone, 4> inactive_zone_list_;
};

// only the machine addresses for user and system tenants will be stored here.
class ObTenantServersCacheMap
{
public:
  ObTenantServersCacheMap();
  ~ObTenantServersCacheMap();
  int init();
  int renew_tenant_map();
  int renew_tenant_map_by_id(const uint64_t tenant_id);
  int get_alive_tenant_servers(
      const uint64_t tenant_id,
      common::ObIArray<common::ObAddr> &alive_servers,
      int64_t &renew_time) const;
private:
  /*
    set new tenant_servers to tenant_map_,
    this operation will compare renew_time,
    and only the latest renew_time will be allowed to be inserted.

    @param[in] tenant_id:        Tenant for servers.
    @param[in] tenant_servers:   Tenant’s machine address
    @return
      - OB_SUCCESS:               successfully
      - OB_ERR_UNEXPECTED         The tenant_id in the input tenant_servers
                                  is for the meta tenant.
      - other:                    other failures
  */
  int set_tenant_servers_cache_(
      const ObTenantServers &tenant_servers);
  int clear_expired_tenent_servers_cache_();
private:
  static const int64_t CLEAR_CACHE_INTERVAL = 60 * 1000 * 1000L; // 60s
  bool is_inited_;
  // store server addresses only for user and system tenants.
  common::hash::ObHashMap<uint64_t, ObTenantServers> tenant_map_;
  share::ObUnitTableOperator ut_operator_;
  int64_t last_cache_clear_ts_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantServersCacheMap);
};

class ObTenantServersCacheUpdater
{
public:
  explicit ObTenantServersCacheUpdater(const ObTenantServers &update_value)
      : update_value_(update_value) {}
  ~ObTenantServersCacheUpdater() {}
  void operator()(common::hash::HashMapPair<
                  uint64_t,
                  ObTenantServers> &entry);
private:
  const ObTenantServers &update_value_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantServersCacheUpdater);
};

class ObAllServerTracer;
class ObServerTraceTask : public common::ObTimerTask
{
public:
  ObServerTraceTask();
  virtual ~ObServerTraceTask();
  int init(ObAllServerTracer *tracer, int tg_id);
  virtual void runTimerTask();
  TO_STRING_KV(KP_(tracer), K_(tg_id));
private:
  int tg_id_;
  ObAllServerTracer *tracer_;
  bool is_inited_;
};

class ObAllServerTracer : public share::ObIServerTrace
{
public:
  static ObAllServerTracer &get_instance();
  int init(int tg_id, ObServerTraceTask &trace_task);
  int for_each_server_info(const ObFunction<int(const ObServerInfoInTable &server_info)> &functor);
  virtual int is_server_exist(const common::ObAddr &server, bool &exist) const;
  virtual int get_server_rpc_port(const common::ObAddr &server, const int64_t sql_port,
                                          int64_t &rpc_port, bool &exist) const;
  virtual int check_server_alive(const common::ObAddr &server, bool &is_alive) const;
  virtual int check_in_service(const common::ObAddr &addr, bool &service_started) const;
  virtual int check_server_permanent_offline(const common::ObAddr &server, bool &is_offline) const;
  virtual int is_server_stopped(const common::ObAddr &server, bool &is_stopped) const;
  virtual int check_migrate_in_blocked(const common::ObAddr &addr, bool &is_block) const;
  virtual int get_server_zone(const common::ObAddr &server, common::ObZone &zone) const;
  // empty zone means that get all servers
  virtual int get_servers_of_zone(
      const common::ObZone &zone,
      common::ObIArray<common::ObAddr> &servers) const;
  // empty zone means that get all servers
  virtual int get_servers_of_zone(
      const common::ObZone &zone,
      common::ObIArray<common::ObAddr> &servers,
      common::ObIArray<uint64_t> &server_id_list) const;
  virtual int get_server_info(
      const common::ObAddr &server,
      ObServerInfoInTable &server_info) const;
  virtual int get_servers_info(
      const common::ObZone &zone,
      common::ObIArray<ObServerInfoInTable> &servers_info,
      bool include_permanent_offline = true) const;
  virtual int get_active_servers_info(
      const common::ObZone &zone,
      common::ObIArray<ObServerInfoInTable> &active_servers_info) const;
  virtual int get_alive_servers(const common::ObZone &zone, common::ObIArray<common::ObAddr> &server_list) const;
  /*
    Atomically get alive_server and renew_time
    @param[in] zone               Retrieve the alive server for the specified zone.
                                  If no specific zone is required,
                                  enter an empty zone to obtain alive servers from all zones.
    @param[out] server_list       The returned alive_server
                                  This may return empty (When the return ret is OB_SUCCESS,
                                  it applies to the following situations):
                                    1. No alive server for the corresponding zone.
                                    2. The server_tracer has not yet refreshed（Cache is empty）
    @param[out] renew_time        The time to get the servers from the table
                                  Effective only if server_list is not empty
    @return
      - other
  */
  virtual int get_alive_servers_with_renew_time(const common::ObZone &zone,
                                                common::ObIArray<common::ObAddr> &server_list,
                                                int64_t &renew_time) const;
  virtual int check_server_active(const common::ObAddr &server, bool &is_active) const;
  /*
    if allow_broadcast is true,
    RPC will be sent to all active machines to refresh their server trace maps
    after the local machine's server trace map is successfully updated.
    allow_broadcast defaults to false.

    @param[in] allow_broadcast          Indicates whether to broadcast to all machines for all_server_tracer refresh;
                                        defaults to false.
    @return
      - OB_NEED_RETRY                   The sys tenant has not yet been built
      - OB_INVALID_ARGUMENT             The dependent table does not contain any information yet.
  */
  virtual int refresh(bool allow_broadcast = false);
  virtual int check_server_can_migrate_in(const common::ObAddr &server, bool &can_migrate_in) const;
  virtual int get_alive_servers_count(const common::ObZone &zone, int64_t &count) const;
  virtual int get_alive_and_not_stopped_servers(const common::ObZone &zone, common::ObIArray<common::ObAddr> &server_list) const;
  virtual int get_servers_by_status(
      const ObZone &zone,
      common::ObIArray<common::ObAddr> &alive_server_list,
      common::ObIArray<common::ObAddr> &not_alive_server_list) const;
  virtual int get_min_server_version(
              char min_server_version_str[OB_SERVER_VERSION_LENGTH],
              uint64_t &min_observer_version);
  bool has_build() const;
  /*
    Refresh the server for all corresponding tenants

    @return
      - OB_NEED_RETRY             The sys has not yet been built
      - other
  */
  int renew_tenant_servers_cache_map();
  /*
    retrieve the address of the alive server corresponding to the tenant
    based on the tenant_id（If a meta tenant is entered,
    it will be converted to the corresponding user tenant）

    @param[in] tenant_id          The tenant_id to be obtained
    @param[out] servers           The machine where the tenant unit is located
                                  This may return empty (When the return ret is OB_SUCCESS,
                                  it applies to the following situations):
                                    1. The tenant does not exist (including being deleted).
                                    2. The tenant was just created and has not yet been refreshed.
                                    3. The tenant's server has been refreshed, but all servers are not alive.
    @param[out] renew_time        The time to get the servers from the table
                                  Effective only if server_list is not empty
    @return
      - other
  */
  int get_alive_tenant_servers(const uint64_t tenant_id,
                               common::ObIArray<common::ObAddr> &servers,
                               int64_t &renew_time) const;
  /*
    Refresh the server of the corresponding tenant
    according to tenant_id（If a meta tenant is entered,
    it will be converted to the corresponding user tenant）

    @param[in] tenant_id          The tenant to refresh
    @return
      - OB_ENTRY_NOT_EXIST        There is no server information
                                  corresponding to the tenant in the table.
                                  (The tenant may have already been deleted,
                                  or the tenant ID does not exist.)
      - other
  */
  int renew_tenant_servers_cache_by_id(const uint64_t tenant_id);
private:
  ObAllServerTracer();
  virtual ~ObAllServerTracer();
private:
  bool is_inited_;
  ObServerTraceMap trace_map_;
  ObTenantServersCacheMap tenant_servers_cache_map_;
};

}  // end namespace share
}  // end namespace oceanbase

#define SVR_TRACER (::oceanbase::share::ObAllServerTracer::get_instance())

#endif  // OCEANBASE_SHARE_OB_ALL_SERVER_TRACER_H_
