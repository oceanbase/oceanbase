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

#ifndef OCEANBASE_SHARE_OB_LOCATION_SERVICE
#define OCEANBASE_SHARE_OB_LOCATION_SERVICE

#include "share/location_cache/ob_ls_location_service.h"
#include "share/location_cache/ob_tablet_ls_service.h"
#include "share/location_cache/ob_vtable_location_service.h"

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
}
namespace share
{
namespace schema
{
class ObMultiVersionSchemaService;
}
// This class provides interfaces to get/renew location
// in block and nonblock ways for log stream and tablet.
class ObLocationService
{
public:
  ObLocationService();
  virtual ~ObLocationService() {}
  // ------------------------- Interfaces for log stream location -------------------------
  // Gets log stream location synchronously.
  //
  // @param [in] cluster_id: target cluster which the ls belongs to
  // @param [in] tenant_id: target tenant which the ls belongs to
  // @param [in] ls_id: target ls
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

  // Gets leader address of a log stream synchronously.
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
  // ------------------------ End interfaces for log stream location ------------------------

  // ------------- Interfaces for tablet to log stream (just for local cluster) -------------
  // Gets the mapping between the tablet and log stream synchronously.
  //
  // @param [in] tenant_id: target tenant which the tablet belongs to
  // @param [in] expire_renew_time: The oldest renew time of cache which the user can tolerate.
  //                                Setting it to 0 means that the cache will not be renewed.
  //                                Setting it to INT64_MAX means renewing cache synchronously.
  // @param [out] is_cache_hit: If hit in location cache.
  // @return OB_MAPPING_BETWEEN_TABLET_AND_LS_NOT_EXIST if no records in sys table.
  //         OB_GET_LOCATION_TIME_OUT if get location by inner sql timeout.
  int get(
      const uint64_t tenant_id,
      const ObTabletID &tablet_id,
      const int64_t expire_renew_time,
      bool &is_cache_hit,
      ObLSID &ls_id);

  // Noblock way to get the mapping between the tablet and log stream.
  //
  // @return OB_MAPPING_BETWEEN_TABLET_AND_LS_NOT_EXIST if no records in sys table.
  int nonblock_get(
      const uint64_t tenant_id,
      const ObTabletID &tablet_id,
      ObLSID &ls_id);

  // Nonblock way to renew the mapping between the tablet and log stream.
  int nonblock_renew(
      const uint64_t tenant_id,
      const ObTabletID &tablet_id);

  // renew location cache according to error_code for tablets
  //
  // @param [in] tenant_id: target tenant which the tablets belong to
  // @param [in] tablet_list: target tablet_id list(there may be duplicate values)
  // @param [in] error_code: the error code form sql which needs retry
  // @param [in] is_nonblock: renew locations synchronously or asynchronously
  // @return OB_GET_LOCATION_TIME_OUT if inner_sql timeout when you block renew
  int batch_renew_tablet_locations(
      const uint64_t tenant_id,
      const ObList<common::ObTabletID, common::ObIAllocator> &tablet_list,
      const int error_code,
      const bool is_nonblock);

  // renew tablet ls mapping and ls location according to error_code
  // implementation is based on batch_renew_tablet_locations
  int renew_tablet_location(
      const uint64_t tenant_id,
      const common::ObTabletID &tablet_id,
      const int error_code,
      const bool is_nonblock);
 // ----------------------- End interfaces for tablet to log stream -----------------------

  // ----------------------- Interfaces for virtual table location -------------------------
  // Gets location for virtual table synchronously.
  //
  // @param [in] tenant_id: Tenant for virtual table.
  // @param [in] table_id: Virtual table id.
  // @param [in] expire_renew_time: The oldest renew time of cache which the user can tolerate.
  //                                Setting it to 0 means that the cache will not be renewed.
  //                                Setting it to INT64_MAX means renewing cache synchronously.
  // @param [out] is_cache_hit: If hit in cache.
  // @param [out] locations: Array of virtual table locations.
  // @return OB_LOCATION_NOT_EXIST if can not find location.
  int vtable_get(
      const uint64_t tenant_id,
      const uint64_t table_id,
      const int64_t expire_renew_time,
      bool &is_cache_hit,
      ObIArray<common::ObAddr> &locations);

  int external_table_get(const uint64_t tenant_id,
                         const uint64_t table_id,
                         ObIArray<common::ObAddr> &locations);

  // Nonblock way to renew the virtual table location
  //
  // @param [in] tenant_id: Tenant for virtual table.
  // @param [in] table_id: Virtual table id.
  int vtable_nonblock_renew(
      const uint64_t tenant_id,
      const uint64_t table_id);
  // --------------------- End interfaces for virtual table location -----------------------

  /* check if the ls exists by querying __all_ls_status and __all_tenant_info
   *
   * @param[in] tenant_id:   target tenant_id
   * @param[in] ls_id:       target ls_id
   * @param[out] state:      EXISTING/DELETED/UNCREATED
   * @return
   *  - OB_SUCCESS:          check successfully
   *  - OB_TENANT_NOT_EXIST: tenant not exist
   *  - OB_INVALID_ARGUMENT: invalid ls_id or tenant_id
   *  - other:               other failures
   */
  static int check_ls_exist(const uint64_t tenant_id, const ObLSID &ls_id, ObLSExistState &state);

  int init(
      ObLSTableOperator &ls_pt,
      schema::ObMultiVersionSchemaService &schema_service,
      common::ObMySQLProxy &sql_proxy,
      ObIAliveServerTracer &server_tracer,
      ObRsMgr &rs_mgr,
      obrpc::ObCommonRpcProxy &rpc_proxy,
      obrpc::ObSrvRpcProxy &srv_rpc_proxy);
  int start();
  void stop();
  void wait();
  int destroy();
  int reload_config();

private:
  enum RenewType {
    DEFAULT_RENEW_BOTH = 0,
    ONLY_RENEW_TABLET_LS_MAPPING = 1,
    ONLY_RENEW_LS_LOCATION = 2
  };
  RenewType gen_renew_type_(const int error) const;

  static int construct_check_ls_exist_sql_(
      const uint64_t tenant_id,
      const ObLSID &ls_id,
      ObSqlString &sql);
private:
  bool inited_;
  bool stopped_;
  ObLSLocationService ls_location_service_;
  ObTabletLSService tablet_ls_service_;
  ObVTableLocationService vtable_location_service_;
};

} // end namespace share
} // end namespace oceanbase
#endif
