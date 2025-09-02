/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_ROOTSERVER_OB_SERVER_ZONE_OP_SERVICE_H
#define OCEANBASE_ROOTSERVER_OB_SERVER_ZONE_OP_SERVICE_H

#include "share/ob_server_table_operator.h"
#include "share/ob_rpc_struct.h"
#ifdef OB_BUILD_TDE_SECURITY
#include "rootserver/ob_rs_master_key_manager.h"
#endif
#ifdef OB_BUILD_SHARED_STORAGE
#include "close_modules/shared_storage/storage/incremental/sslog/ob_sslog_kv_palf_adapter.h"
#endif

namespace oceanbase
{
namespace obrpc
{
class ObSrvRpcProxy;
struct ObRsListArg;
#ifdef OB_BUILD_TDE_SECURITY
struct ObWaitMasterKeyInSyncArg;
#endif
// struct ObAdminServerArg;
class ObAdminStorageArg;
}
namespace share
{
class ObLSTableOperator;
class ObAllServerTracer;
struct ObRootKey;
}
namespace rootserver
{
class ObIServerChangeCallback;
class ObUnitManager;
typedef common::ObSEArray<uint64_t, 128> ServerIDArray;
typedef common::ObSEArray<ObZone, DEFAULT_ZONE_COUNT> ZoneNameArray;
class ObServerZoneOpService
{
public:
  static const char *PALF_KV_SERVER_IDS_INFOS_PREFIX;
  static const char *PALF_KV_ZONE_NAMES_INFOS_PREFIX;
  static const char *PALF_KV_MAX_UNIT_ID_FORMAT_STR;
  static const char *PALF_KV_TENANT_DATA_VERSION_FORMAT_STR;
  static const int64_t MAX_ROW_KEY_LENGTH = 128;
  const static int64_t OB_SERVER_SEND_MASTER_KEY_TIMEOUT = 10 * 1000 * 1000; // 10s
public:
  ObServerZoneOpService();
  virtual ~ObServerZoneOpService();
  int init(
      ObIServerChangeCallback &server_change_callback,
      obrpc::ObSrvRpcProxy &rpc_proxy,
      share::ObLSTableOperator &lst_operator,
      ObUnitManager &unit_manager,
      ObMySQLProxy &sql_proxy
#ifdef OB_BUILD_TDE_SECURITY
      , ObRsMasterKeyManager *master_key_mgr
#endif
      );
#ifdef OB_BUILD_SHARED_STORAGE
  static int delete_zone_from_palf_kv(const ObZone &zone);
  static int insert_zone_in_palf_kv(const ObZone &zone);
  static int store_all_zone_in_palf_kv(const ZoneNameArray &zone_list);
  static int check_new_zone_in_palf_kv(const ObZone& zone, bool &new_zone);
  static int delete_server_id_in_palf_kv(const uint64_t server_id);
  static int get_server_ids_from_palf_kv(ServerIDArray &server_ids);
  static int get_server_infos_from_palf_kv(ServerIDArray &server_ids, ServerIDArray &server_ids_with_ts, uint64_t &max_server_id);
  static int store_server_ids_in_palf_kv(ServerIDArray &server_ids, const uint64_t max_server_id);
  static int generate_new_server_id_from_palf_kv(uint64_t &new_server_id);

  static int store_max_unit_id_in_palf_kv(const uint64_t max_unit_id);
  static int generate_new_unit_id_from_palf_kv(uint64_t &new_unit_id);
  static int store_data_version_in_palf_kv(const uint64_t tenant_id, const uint64_t data_version);
  static int get_data_version_in_palf_kv(const uint64_t tenant_id, uint64_t &data_version);
#endif
  static int calculate_new_candidate_server_id(
      const common::ObIArray<uint64_t> &server_id_in_cluster,
      const uint64_t candidate_server_id,
      uint64_t &new_candidate_server_id);
  static bool check_server_index(
      const uint64_t candidate_server_id,
      const common::ObIArray<uint64_t> &server_id_in_cluster);
  // Add new servers to a specified（optional) zone in the cluster.
  // The servers should be empty and the zone should be active.
  // This operation is successful
  // if the servers' info are inserted into __all_server table successfully.
  //
  // @param[in]  servers	the servers which we want to add
  // @param[in]  zone     the zone in which the servers will be located. If it's empty,
  //                      the zone specified in the servers' local config will be picked
  //
  // @ret OB_SUCCESS 		           add successfully
  // @ret OB_ZONE_NOT_ACTIVE       the specified zone is not active
  // @ret OB_SERVER_ZONE_NOT_MATCH the zone specified in the server's local config is not the same
  //                               as the zone specified in the system command ADD SERVER
  //                               or both are empty
  // @ret OB_ENTRY_EXIST           there exists servers which are already added
  //
  // @ret other error code		     failure
  int add_servers(const ObIArray<ObAddr> &servers,
      const ObZone &zone,
      const bool is_bootstrap = false);
  int construct_rs_list_arg(obrpc::ObRsListArg &rs_list_arg);
  // Try to delete the given servers from the cluster (logically).
  // In this func, we only set their statuses in __all_server table be OB_SERVER_DELETING.
  // Root balancer will detect servers with such statuses
  // and start to migrate units on these servers to other servers.
  // Once a server with status OB_SERVER_DELETING has no units and no records in __all_ls_meta_table,
  // this server will be deleted from __all_server table, which means this server is no longer in the cluster
  // (see related machanism in ObEmptyServerChecker).
  //
  // @param[in]  server	  the server which we try to delete
  // @param[in]  zone     the zone in which the server is located
  //
  // @ret OB_SUCCESS 		               set status be OB_SERVER_DELETING in __all_server table successfully
  // @ret OB_SERVER_ZONE_NOT_MATCH     the arg zone is not the same as the server's zone in __all_server table
  // @ret OB_SERVER_ALREADY_DELETED    the server's status has been OB_SERVER_DELETING already
  // @ret OB_SERVER_NOT_IN_WHITE_LIST  the server is not in the cluster
  // @ret OB_NOT_MASTER                not rs leader, cannot execute the command
  //
  // @ret other error code		 failure
  int delete_servers(
      const ObIArray<common::ObAddr> &servers,
      const common::ObZone &zone);
  // Revoke the delete operation for the given server from the cluster (logically).
  // What we do in this func is to set servers' status be OB_SERVER_ACTIVE
  // or OB_SERVER_INACTIVE in __all_server table
  // and prevent units on this server be migrated to other servers.
  //
  // @param[in]  server  	the server for which we want to revoke the delete operation
  // @param[in]  zone     the zone in which the server is located
  //
  // @ret OB_SUCCESS 		               set status be OB_SERVER_ACTIVE or OB_SERVER_INACTIVE in __all_server table successfully
  // @ret OB_SERVER_ZONE_NOT_MATCH     the arg zone is not the same as the server's zone in __all_server table
  // @ret OB_SERVER_NOT_DELETING       the server's status is not OB_SERVER_DELETING, we cannot cancel delete
  // @ret OB_SERVER_NOT_IN_WHITE_LIST  the server is not in the cluster
  // @ret OB_NOT_MASTER                not rs leader, cannot execute the command

// @ret other error code		 failure
  int cancel_delete_servers(
      const ObIArray<common::ObAddr> &servers,
      const common::ObZone &zone);
  // Delete the given server from the cluster
  // In this func, we delete the server from __all_server table.

  // @param[in]  server	  the server which we want to delete
  // @param[in]  zone     the zone in which the server is located

  // @ret OB_SUCCESS 		               delete the server from __all_server table successfully
  // @ret OB_SERVER_NOT_DELETING       the server's status is not OB_SERVER_DELETING, we cannot remove it
  // @ret OB_SERVER_NOT_IN_WHITE_LIST  the server is not in the cluster
  // @ret OB_NOT_MASTER                not rs leader, cannot execute the command

  // @ret other error code		 failure
  int finish_delete_server(
      const common::ObAddr &server,
      const common::ObZone &zone);
  // stop the given server
  // In this func, we set the server's stop_time be now in __all_server table
  // Stopping server should guarantee that there is no other zone's server is stopped.
  // Isolating server should guarantee that there still exists started server in primary region after isolating
  // In addition, stop server will check majority and log sync.
  //
  // @param[in]  server	  the server which we want to stop
  // @param[in]  zone     the zone in which the server is located
  // @param[in]  is_stop  true if stop, otherwise isolate
  //
  // @ret OB_SUCCESS 		               stop the server successfully
  // @ret OB_INVALID_ARGUMENT          an invalid server
  // @ret OB_SERVER_ZONE_NOT_MATCH     the arg zone is not the same as the server's zone in __all_server table
  // @ret OB_NOT_MASTER                not rs leader, cannot execute the command
  // @ret OB_SERVER_NOT_IN_WHITE_LIST  the server is not in the cluster

  // @ret other error code		 failure
  int stop_servers(
      const ObIArray<ObAddr> &servers,
      const ObZone &zone,
      const obrpc::ObAdminServerArg::AdminServerOp &op);
  // start the given server
  // In this func, we set the server's stop_time be zero in __all_server table
  //
  // @param[in]  server  	the server which we want to start
  // @param[in]  zone     the zone in which the server is located
  // @param[in]  op       op: isolate, stop, force_stop
  //
  // @ret OB_SUCCESS 		                start the server successfully
  // @ret OB_INVALID_ARGUMENT           an invalid server
  // @ret OB_SERVER_ZONE_NOT_MATCH      the arg zone is not the same as the server's zone in __all_server table
  // @ret OB_NOT_MASTER                 not rs leader, cannot execute the command
  // @ret OB_SERVER_NOT_IN_WHITE_LIST   the server is not in the cluster

  // @ret other error code		 failure
  int start_servers(
      const ObIArray<ObAddr> &servers,
      const ObZone &zone);
#ifdef OB_BUILD_TDE_SECURITY
  int handle_master_key_for_adding_server(
      const common::ObAddr &server,
      const common::ObZone &zone,
      obrpc::ObWaitMasterKeyInSyncArg &wms_in_sync_arg);
  int check_master_key_for_adding_server(
      const common::ObAddr &server,
      const common::ObZone &zone,
      obrpc::ObWaitMasterKeyInSyncArg &wms_in_sync_arg);
  int send_master_key_for_adding_server(
      const common::ObAddr &server);
#endif
  int stop_server_precheck(
      const ObIArray<ObAddr> &servers,
      const obrpc::ObAdminServerArg::AdminServerOp &op);
private:
  int check_startup_mode_match_(const share::ObServerMode startup_mode);
  int check_logservice_deployment_mode_(const bool added_server_logservice);
  int zone_checking_for_adding_server_(
      const common::ObZone &rpc_zone,
      ObZone &picked_zone);
  int add_server_(
      const common::ObAddr &server,
      const uint64_t server_id,
      const common::ObZone &zone,
      const int64_t sql_port,
      const share::ObServerInfoInTable::ObBuildVersion &build_version,
      const ObIArray<share::ObZoneStorageTableInfo> &storage_infos);
  int delete_server_(
      const common::ObAddr &server,
      const common::ObZone &zone);
  int check_and_end_delete_server_(
      common::ObMySQLTransaction &trans,
      const common::ObAddr &server,
      const common::ObZone &zone,
      const bool is_cancel,
      share::ObServerInfoInTable &server_info);
  int start_or_stop_server_(
      const common::ObAddr &server,
      const ObZone &zone,
      const obrpc::ObAdminServerArg::AdminServerOp &op);
  int check_and_update_service_epoch_(common::ObMySQLTransaction &trans);
  int fetch_new_server_id_(uint64_t &server_id);
  int fetch_new_server_id_for_sn_(uint64_t &server_id);
  int fetch_new_server_id_for_ss_(uint64_t &server_id);
  int check_server_have_enough_resource_for_delete_server_(
      const ObIArray<common::ObAddr> &servers,
      const common::ObZone &zone);
  int check_zone_and_server_(
    const ObIArray<share::ObServerInfoInTable> &servers_info,
    const ObIArray<ObAddr> &servers,
    bool &is_same_zone,
    bool &is_all_stopped);
  void end_trans_and_on_server_change_(
      int &ret,
      common::ObMySQLTransaction &trans,
      const char *op_print_str,
      const common::ObAddr &server,
      const common::ObZone &zone,
      const int64_t start_time);
  int get_and_check_storage_infos_by_zone_(const ObZone &zone,
      ObIArray<share::ObZoneStorageTableInfo> &result);
  int check_storage_infos_not_changed_(common::ObISQLClient &proxy, const ObZone &zone,
      const ObIArray<share::ObZoneStorageTableInfo> &storage_infos);
  int precheck_server_empty_and_get_zone_(const ObAddr &server,
      const ObTimeoutCtx &timeout,
      const bool is_bootstrap,
      ObZone &zone);
  int prepare_server_for_adding_server_(const ObAddr &server,
      const ObTimeoutCtx &timeout,
      const bool &is_bootstrap,
      ObZone &picked_zone,
      obrpc::ObPrepareServerForAddingServerArg &rpc_arg,
      obrpc::ObPrepareServerForAddingServerResult &rpc_result);
#ifdef OB_BUILD_SHARED_STORAGE
  static int get_server_ids_from_palf_kv_(ServerIDArray &server_ids);
  static int get_zone_names_from_palf_kv_(ZoneNameArray &zone_names);
  static int insert_server_ids_in_palf_kv_(const ServerIDArray &server_ids);
  static int insert_zone_names_in_palf_kv_(const ZoneNameArray &zone_names);
  static int cas_server_ids_in_palf_kv_(
      const ServerIDArray &old_server_ids,
      const ServerIDArray &new_server_ids);
  static int cas_zone_names_in_palf_kv_(
      const ZoneNameArray &old_zone_names,
      const ZoneNameArray &new_zone_names);
  static int store_max_resource_id_in_palf_kv_(
      const common::ObString &row_key,
      const uint64_t max_resource_id);
  static int cas_resource_id_in_palf_kv_(
      const common::ObString &row_key,
      const uint64_t orig_resource_id,
      const uint64_t new_resource_id);
  static int insert_resource_id_in_palf_kv_(
      const common::ObString &row_key,
      const uint64_t resource_id);
  static int get_resource_id_in_palf_kv_(
      const common::ObString &row_key,
      uint64_t &resource_id);
  static int trans_str_to_uint_(
      const ObString &str_val,
      uint64_t &ret_val);
#endif
  bool is_inited_;
  ObIServerChangeCallback *server_change_callback_;
  obrpc::ObSrvRpcProxy *rpc_proxy_;
  ObMySQLProxy *sql_proxy_;
  share::ObLSTableOperator *lst_operator_;
  share::ObServerTableOperator st_operator_;
  ObUnitManager *unit_manager_;
#ifdef OB_BUILD_TDE_SECURITY
  ObRsMasterKeyManager *master_key_mgr_;
#endif

private:
  DISALLOW_COPY_AND_ASSIGN(ObServerZoneOpService);
};
} // rootserver
} // oceanbase

#endif
