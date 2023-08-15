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
}
namespace share
{
class ObLSTableOperator;
class ObAllServerTracer;
}
namespace rootserver
{
class ObIServerChangeCallback;
class ObUnitManager;
class ObServerZoneOpService
{
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
  int add_servers(const ObIArray<ObAddr> &servers, const ObZone &zone, bool is_bootstrap = false);
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
  int master_key_checking_for_adding_server(
      const common::ObAddr &server,
      const common::ObZone &zone,
      obrpc::ObWaitMasterKeyInSyncArg &wms_in_sync_arg);
#endif
  int stop_server_precheck(
      const ObIArray<ObAddr> &servers,
      const obrpc::ObAdminServerArg::AdminServerOp &op);
private:
  int zone_checking_for_adding_server_(
      const common::ObZone &command_zone,
      const common::ObZone &rpc_zone,
      ObZone &picked_zone);
  int add_server_(
      const common::ObAddr &server,
      const uint64_t server_id,
      const common::ObZone &zone,
      const int64_t sql_port,
      const share::ObServerInfoInTable::ObBuildVersion &build_version);
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