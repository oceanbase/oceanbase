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

#define USING_LOG_PREFIX RS

#include "ob_server_zone_op_service.h"

#include "share/ob_zone_table_operation.h"
#include "share/ob_service_epoch_proxy.h"
#include "share/ob_max_id_fetcher.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"  // ObMySQLTransaction
#include "lib/utility/ob_tracepoint.h" // ERRSIM
#include "rootserver/ob_root_service.h" // callback
#include "share/ob_all_server_tracer.h"
#include "share/ob_errno.h"
#include "rootserver/ob_server_manager.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "share/object_storage/ob_zone_storage_table_operation.h"
#endif
#ifdef OB_BUILD_TDE_SECURITY
#include "share/ob_master_key_getter.h"
#endif

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace obrpc;
namespace rootserver
{
ObServerZoneOpService::ObServerZoneOpService()
    : is_inited_(false),
      server_change_callback_(NULL),
      rpc_proxy_(NULL),
      sql_proxy_(NULL),
      lst_operator_(NULL),
      unit_manager_(NULL)
#ifdef OB_BUILD_TDE_SECURITY
      , master_key_mgr_()
#endif
{
}
ObServerZoneOpService::~ObServerZoneOpService()
{
}
int ObServerZoneOpService::init(
    ObIServerChangeCallback &server_change_callback,
    ObSrvRpcProxy &rpc_proxy,
    ObLSTableOperator &lst_operator,
    ObUnitManager &unit_manager,
    ObMySQLProxy &sql_proxy
#ifdef OB_BUILD_TDE_SECURITY
    , ObRsMasterKeyManager *master_key_mgr
#endif
)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("server zone operation service has been inited already", KR(ret), K(is_inited_));
#ifdef OB_BUILD_TDE_SECURITY
  } else if (OB_ISNULL(master_key_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("master key mgr is null", KR(ret), KP(master_key_mgr));
#endif
  } else if (OB_FAIL(st_operator_.init(&sql_proxy))) {
    LOG_WARN("fail to init server table operator", KR(ret));
  } else {
    server_change_callback_ = &server_change_callback;
    rpc_proxy_ = &rpc_proxy;
    sql_proxy_ = &sql_proxy;
    lst_operator_ = &lst_operator;
    unit_manager_ = &unit_manager;
#ifdef OB_BUILD_TDE_SECURITY
    master_key_mgr_ = master_key_mgr;
#endif
    is_inited_ = true;
  }
  return ret;
}
#ifdef OB_BUILD_SHARED_STORAGE
int ObServerZoneOpService::get_and_check_storage_infos_by_zone_(const ObZone& zone,
    ObIArray<share::ObZoneStorageTableInfo> &result)
{
  int ret = OB_SUCCESS;
  if (!GCTX.is_shared_storage_mode()) {
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is NULL", KR(ret), KP(GCTX.root_service_));
  } else {
    if (OB_FAIL(ObStorageInfoOperator::get_ordered_zone_storage_infos_with_sub_op_id(*sql_proxy_,
            zone, result))) {
      LOG_WARN("failed to get all storage infos", KR(ret), K(zone));
    } else if (result.empty()) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("zone storage infos is empty", KR(ret), K(zone));
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Zone storage info not exists. ADD SERVER");
    }
  }
  return ret;
}
int ObServerZoneOpService::check_storage_infos_not_changed_(common::ObISQLClient &proxy,
    const ObZone &zone, const ObIArray<share::ObZoneStorageTableInfo> &storage_infos)
{
  int ret = OB_SUCCESS;
  ObArray<share::ObZoneStorageTableInfo> zone_storage_infos;
  if (!GCTX.is_shared_storage_mode()) {
  } else if (OB_FAIL(ObStorageInfoOperator::get_ordered_zone_storage_infos_with_sub_op_id(proxy,
          zone, zone_storage_infos))) {
    LOG_WARN("failed to get get zone storage infos", KR(ret), K(zone));
  } else if (storage_infos.count() != zone_storage_infos.count()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("zone storage infos changed when adding server", KR(ret), K(zone),
        K(storage_infos), K(zone_storage_infos));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Zone storage changed. ADD SERVER");
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < storage_infos.count(); i++) {
      const ObZoneStorageTableInfo &target = storage_infos.at(i);
      const ObZoneStorageTableInfo &current = zone_storage_infos.at(i);
      if (!target.is_equal(current)) {
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("zone storage infos changed when adding server", KR(ret), K(zone),
            K(storage_infos), K(zone_storage_infos));
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Zone storage changed. ADD SERVER");
      }
    }
  }
  return ret;
}
#endif

#define PRINT_NON_EMPTY_SERVER_ERR_MSG(addr) \
  do {\
      int tmp_ret = OB_SUCCESS; \
      const int64_t ERR_MSG_BUF_LEN = OB_MAX_SERVER_ADDR_SIZE + 100; \
      char non_empty_server_err_msg[ERR_MSG_BUF_LEN] = ""; \
      int64_t pos = 0; \
      if (OB_TMP_FAIL(databuff_print_multi_objs(non_empty_server_err_msg, ERR_MSG_BUF_LEN, pos, \
          "add non-empty server ", addr))) { \
        LOG_WARN("fail to execute databuff_printf", KR(tmp_ret), K(addr)); \
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, "add non-empty server"); \
      } else { \
        LOG_USER_ERROR(OB_OP_NOT_ALLOW, non_empty_server_err_msg); \
      } \
  } while (0)

int ObServerZoneOpService::precheck_server_empty_and_get_zone_(const ObAddr &server,
    const ObTimeoutCtx &ctx,
    const bool is_bootstrap,
    ObZone &picked_zone)
{
  int ret = OB_SUCCESS;
  uint64_t sys_data_version = 0;
  ObCheckServerEmptyArg rpc_arg;
  Bool is_empty;
  uint64_t min_observer_version = GET_MIN_CLUSTER_VERSION();
  int64_t timeout = ctx.get_timeout();
  if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid addr", KR(ret), K(server));
  } else if (is_bootstrap) {
    // when in bootstrap mode, server is check empty and set server_id in prepare_bootstrap
    // no need to check server empty
    // the zone must be provided in SQL, the parser ensures this
    if (picked_zone.is_empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("in bootstrap mode, zone must be provided", KR(ret), K(picked_zone));
    }
  } else if (OB_ISNULL(rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rpc_proxy_ is null", KR(ret), KP(rpc_proxy_));
  } else if (OB_UNLIKELY(timeout <= 0)) {
    ret = OB_TIMEOUT;
    LOG_WARN("ctx time out", KR(ret), K(timeout));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(OB_SYS_TENANT_ID, sys_data_version))) {
    LOG_WARN("failed to get sys tenant data version", KR(ret));
  } else if (OB_FAIL(rpc_arg.init(obrpc::ObCheckServerEmptyArg::ADD_SERVER, sys_data_version,
          OB_INVALID_ID /* server_id */))) {
    LOG_WARN("failed to init ObCheckServerEmptyArg", KR(ret),
        "mode", obrpc::ObCheckServerEmptyArg::ADD_SERVER,
        K(sys_data_version), "server_id", OB_INVALID_ID);
  } else {
    if (min_observer_version >= CLUSTER_VERSION_4_3_3_0) {
      ObCheckServerEmptyResult rpc_result;
      if (OB_FAIL(rpc_proxy_->to(server)
            .timeout(timeout)
            .check_server_empty_with_result(rpc_arg, rpc_result))) {
        // do not rewrite errcode, make rs retry if failed to send rpc
        LOG_WARN("failed to check server empty", KR(ret), K(server), K(timeout), K(rpc_arg));
      } else if (OB_FAIL(zone_checking_for_adding_server_(rpc_result.get_zone(), picked_zone))) {
        LOG_WARN("failed to get picked_zone from rpc result", KR(ret));
      } else {
        is_empty = rpc_result.get_server_empty();
      }
    } else {
      if (OB_FAIL(rpc_proxy_->to(server)
            .timeout(timeout)
            .check_server_empty(rpc_arg, is_empty))) {
        // do not rewrite errcode, make rs retry if failed to send rpc
        LOG_WARN("failed to check server empty", KR(ret), K(server), K(timeout), K(rpc_arg));
      } else {}
    }
    if (OB_SUCC(ret) && !is_empty) {
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("adding non-empty server is not allowed", KR(ret), K(is_bootstrap), K(is_empty));
        PRINT_NON_EMPTY_SERVER_ERR_MSG(server);
    }
  }
  return ret;
}
ERRSIM_POINT_DEF(EN_ADD_SERVER_RPC_FAIL);
int ObServerZoneOpService::prepare_server_for_adding_server_(const ObAddr &server,
      const ObTimeoutCtx &ctx,
      const bool &is_bootstrap,
      ObZone &picked_zone,
      ObPrepareServerForAddingServerArg &rpc_arg,
      ObPrepareServerForAddingServerResult &rpc_result)
{
  int ret = OB_SUCCESS;
  uint64_t server_id = OB_INVALID_ID;
  ObSArray<share::ObZoneStorageTableInfo> zone_storage_infos;
  ObPrepareServerForAddingServerArg::Mode mode = is_bootstrap ?
      ObPrepareServerForAddingServerArg::BOOTSTRAP : ObPrepareServerForAddingServerArg::ADD_SERVER;
  uint64_t sys_tenant_data_version = 0;
#ifdef OB_BUILD_TDE_SECURITY
  // In SS mode, root-key of SYS tenant is sent to server when adding server for encryption of ak/sk.
  ObString root_key_str;
  RootKeyType root_key_type = RootKeyType::INVALID;
#endif
  int64_t timeout = ctx.get_timeout();
  if (!server.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("server is invalid", KR(ret), K(server));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (GCTX.is_shared_storage_mode() && picked_zone.is_empty()) {
    // in shared storage mode, zone is set in check_server_empty_and_get_zone_
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("zone is empty in shared storage mode", KR(ret), K(picked_zone), K(GCTX.is_shared_storage_mode()));
#endif
  } else if (OB_ISNULL(rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rpc_proxy_ is NULL", KR(ret), KP(rpc_proxy_));
  } else if (timeout <= 0) {
    ret = OB_TIMEOUT;
    LOG_WARN("ctx time out", KR(ret), K(timeout));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(OB_SYS_TENANT_ID, sys_tenant_data_version))) {
    LOG_WARN("fail to get sys tenant's min data version", KR(ret));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (GCTX.is_shared_storage_mode()) {
    ObRootKey root_key;
    if (OB_FAIL(get_and_check_storage_infos_by_zone_(picked_zone, zone_storage_infos))) {
      LOG_WARN("failed to get storage infos", KR(ret), K(picked_zone));
    }
#ifdef OB_BUILD_TDE_SECURITY
    if (FAILEDx(ObMasterKeyGetter::instance().get_root_key(OB_SYS_TENANT_ID, root_key))) {
      LOG_WARN("failed to get sys root key", KR(ret));
    } else {
      root_key_str = root_key.key_;
      root_key_type = root_key.key_type_;
    }
#endif
#endif
  }
  if (FAILEDx(fetch_new_server_id_(server_id))) {
    // fetch a new server id and insert the server into __all_server table
    LOG_WARN("fail to fetch new server id", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_server_id(server_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("server id is invalid", KR(ret), K(server_id));
  } else if (OB_FAIL(rpc_arg.init(mode,
          sys_tenant_data_version,
          server_id,
          zone_storage_infos
#ifdef OB_BUILD_TDE_SECURITY
          , root_key_type, root_key_str
#endif
          ))) {
    LOG_WARN("fail to init rpc arg", KR(ret), K(sys_tenant_data_version), K(server_id),
        K(zone_storage_infos)
#ifdef OB_BUILD_TDE_SECURITY
        , K(root_key_type), K(root_key_str)
#endif
        );
  } else if (OB_FAIL(rpc_proxy_->to(server)
      .timeout(timeout)
      .prepare_server_for_adding_server(rpc_arg, rpc_result))
      || OB_FAIL(OB_E(EN_ADD_SERVER_RPC_FAIL) OB_SUCCESS)) {
    // change errcode to avoid retry in add server RPC
    // the retry may increase max_used_server_id which is meaningless
    ret = OB_SERVER_CONNECTION_ERROR;
    LOG_WARN("fail to connect to server and set server_id", KR(ret), K(server));
    ObCStringHelper helper;
    LOG_USER_ERROR(OB_SERVER_CONNECTION_ERROR, helper.convert(server));
    // in bootstrap mode, server_id is set in prepare_bootstrap, the server is not empty here
  } else if (!is_bootstrap && !rpc_result.get_is_server_empty()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("adding non-empty server is not allowed", KR(ret), K(server), K(rpc_result), K(is_bootstrap));
    PRINT_NON_EMPTY_SERVER_ERR_MSG(server);
  } else if (OB_FAIL(check_startup_mode_match_(rpc_result.get_startup_mode()))) {
    LOG_WARN("failed to check_startup_mode_match", KR(ret), K(rpc_result.get_startup_mode()));
  } else if (OB_FAIL(zone_checking_for_adding_server_(rpc_result.get_zone(), picked_zone))) {
    LOG_WARN("failed to get picked_zone from rpc result", KR(ret));
  }
  return ret;
}
#undef PRINT_NON_EMPTY_SERVER_ERR_MSG
int ObServerZoneOpService::add_servers(const ObIArray<ObAddr> &servers,
    const ObZone &zone,
    const bool is_bootstrap)
{
  int ret = OB_SUCCESS;
  ObPrepareServerForAddingServerArg rpc_arg;
  ObPrepareServerForAddingServerResult rpc_result;
  ObZone picked_zone;
  ObTimeoutCtx ctx;
#ifdef OB_BUILD_TDE_SECURITY
  ObWaitMasterKeyInSyncArg wms_in_sync_arg;
  // master key mgr sync
#endif
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(is_inited_));
#ifdef OB_BUILD_TDE_SECURITY
  } else if (OB_ISNULL(master_key_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("master_key_mgr_ is null", KR(ret), KP(master_key_mgr_));
  } else if (OB_FAIL(construct_rs_list_arg(wms_in_sync_arg.rs_list_arg_))) {
    LOG_WARN("fail to construct rs list arg", KR(ret));
#endif
  } else if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
    LOG_WARN("fail to get timeout ctx", KR(ret), K(ctx));
  }
  if (OB_FAIL(ret)) {
  } else {
#ifdef OB_BUILD_TDE_SECURITY
    SpinRLockGuard sync_guard(master_key_mgr_->sync());
#endif
    for (int64_t i = 0; OB_SUCC(ret) && i < servers.count(); ++i) {
      const ObAddr &addr = servers.at(i);
      int64_t timeout = ctx.get_timeout();
      // zone is empty means user did not set zone in add server command
      // zone is not empty means user set zone in add server command
      if (OB_FAIL(picked_zone.assign(zone))) {
        LOG_WARN("failed to init picked_zone", KR(ret));
        // check server empty before get a new server_id
        // avoid server_id increasing when adding non-empty server
      } else if (OB_FAIL(precheck_server_empty_and_get_zone_(addr, ctx, is_bootstrap, picked_zone))) {
        LOG_WARN("failed to check server empty and get zone", KR(ret), K(addr), K(timeout),
            K(zone), K(is_bootstrap));
      } else if (OB_FAIL(prepare_server_for_adding_server_(addr, ctx, is_bootstrap, picked_zone, rpc_arg, rpc_result))) {
        LOG_WARN("failed to set server id", KR(ret), K(addr), K(timeout), K(zone), K(is_bootstrap), K(rpc_arg));
#ifdef OB_BUILD_TDE_SECURITY
      } else if (!is_bootstrap && OB_FAIL(master_key_checking_for_adding_server(addr, picked_zone, wms_in_sync_arg))) {
        LOG_WARN("master key checking for adding server is failed", KR(ret), K(addr), K(picked_zone));
#endif
      } else if (OB_FAIL(add_server_(
          addr,
          rpc_arg.get_server_id(),
          picked_zone,
          rpc_result.get_sql_port(),
          rpc_result.get_build_version(),
          rpc_arg.get_zone_storage_infos()))) {
        LOG_WARN("add_server failed", KR(ret), K(addr), "server_id", rpc_arg.get_server_id(), K(picked_zone), "sql_port",
            rpc_result.get_sql_port(), "build_version", rpc_result.get_build_version());
      } else {}
    }
  }
  return ret;
}
int ObServerZoneOpService::delete_servers(
    const ObIArray<ObAddr> &servers,
    const ObZone &zone)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(is_inited_));
  } else if (OB_ISNULL(GCTX.root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root_service_ is null", KR(ret), KP(GCTX.root_service_));
  } else if (OB_UNLIKELY(servers.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(servers));
  } else if (OB_FAIL(check_server_have_enough_resource_for_delete_server_(servers, zone))) {
    LOG_WARN("not enough resource, cannot delete servers", KR(ret), K(servers), K(zone));
  } else if (OB_FAIL(GCTX.root_service_->check_all_ls_has_leader("delete server"))) {
    LOG_WARN("fail to check whether all ls has leader", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < servers.count(); ++i) {
      if (OB_FAIL(delete_server_(servers.at(i), zone))) {
        LOG_WARN("delete_server failed", "server", servers.at(i), "zone", zone, KR(ret));
      }
    }
  }
  return ret;
}
int ObServerZoneOpService::cancel_delete_servers(
    const ObIArray<ObAddr> &servers,
    const ObZone &zone)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(is_inited_));
  } else if (OB_ISNULL(unit_manager_) || OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit_manager_ or sql_proxy_ or server_change_callback_ is null", KR(ret),
        KP(unit_manager_), KP(sql_proxy_));
  } else {
    ObServerInfoInTable server_info_in_table;
    for (int64_t i = 0; OB_SUCC(ret) && i < servers.count(); ++i) {
      const ObAddr &server = servers.at(i);
      const int64_t now = ObTimeUtility::current_time();
      ObMySQLTransaction trans;
      server_info_in_table.reset();
      if (OB_FAIL(trans.start(sql_proxy_, OB_SYS_TENANT_ID))) {
        LOG_WARN("fail to start trans", KR(ret));
      } else if (OB_FAIL(check_and_end_delete_server_(trans, server, zone, true /* is_cancel */, server_info_in_table))) {
        LOG_WARN("fail to check and end delete server", KR(ret), K(server), K(zone));
      } else if (OB_FAIL(ObServerTableOperator::update_status(
          trans,
          server,
          ObServerStatus::OB_SERVER_DELETING,
          server_info_in_table.is_alive() ? ObServerStatus::OB_SERVER_ACTIVE : ObServerStatus::OB_SERVER_INACTIVE))) {
        LOG_WARN("fail to update status in __all_server table", KR(ret),
            K(server), K(server_info_in_table));
      } else if (OB_FAIL(unit_manager_->cancel_migrate_out_units(server))) {
        LOG_WARN("unit_manager_ cancel_migrate_out_units failed", KR(ret), K(server));
      }
      (void) end_trans_and_on_server_change_(ret, trans, "cancel_delete_server", server, server_info_in_table.get_zone(), now);
    }
  }
  return ret;
}
int ObServerZoneOpService::finish_delete_server(
    const ObAddr &server,
    const ObZone &zone)
{
  int ret = OB_SUCCESS;
  ObServerInfoInTable server_info_in_table;
  const int64_t now = ObTimeUtility::current_time();
  ObMySQLTransaction trans;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(is_inited_));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is null", KR(ret), KP(sql_proxy_));
  } else if (OB_FAIL(trans.start(sql_proxy_, OB_SYS_TENANT_ID))) {
    LOG_WARN("fail to start trans", KR(ret));
  } else if (OB_FAIL(check_and_end_delete_server_(trans, server, zone, false /* is_cancel */, server_info_in_table))) {
    LOG_WARN("fail to check and end delete server", KR(ret), K(server), K(zone));
  } else if (OB_FAIL(ObServerManager::try_delete_server_working_dir(
      server_info_in_table.get_zone(),
      server,
      server_info_in_table.get_server_id()))) {
    LOG_WARN("fail to delete server working dir", KR(ret), K(server_info_in_table));
  } else if (OB_FAIL(st_operator_.remove(server, trans))) {
    LOG_WARN("fail to remove this server from __all_server table", KR(ret), K(server));
  }
  (void) end_trans_and_on_server_change_(ret, trans, "finish_delete_server", server, server_info_in_table.get_zone(), now);
  return ret;
}
int ObServerZoneOpService::stop_servers(
    const ObIArray<ObAddr> &servers,
    const ObZone &zone,
    const obrpc::ObAdminServerArg::AdminServerOp &op)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(is_inited_));
  } else if (OB_FAIL(stop_server_precheck(servers, op))) {
    LOG_WARN("fail to precheck stop server", KR(ret), K(servers), K(zone));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < servers.count(); i++) {
      const ObAddr &server = servers.at(i);
      if (OB_FAIL(start_or_stop_server_(server, zone, op))) {
        LOG_WARN("fail to stop server", KR(ret), K(server), K(zone));
      }
    }
  }
  return ret;
}
int ObServerZoneOpService::start_servers(
    const ObIArray<ObAddr> &servers,
    const ObZone &zone)
{
  int ret = OB_SUCCESS;
  ObCheckServerMachineStatusArg rpc_arg;
  ObCheckServerMachineStatusResult rpc_result;
  ObServerInfoInTable server_info;
  ObTimeoutCtx ctx;
  uint64_t sys_tenant_data_version = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(is_inited_));
  } else if (OB_ISNULL(rpc_proxy_) || OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rpc_proxy_ or GCTX.sql_proxy_ is null", KR(ret), KP(rpc_proxy_), KP(GCTX.sql_proxy_));
  } else if (OB_UNLIKELY(servers.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("servers' count is zero", KR(ret), K(servers));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(OB_SYS_TENANT_ID, sys_tenant_data_version))) {
    LOG_WARN("fail to get sys tenant's min data version", KR(ret));
  } else if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
    LOG_WARN("fail to get timeout ctx", KR(ret), K(ctx));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < servers.count(); ++i) {
      const ObAddr &server = servers.at(i);
      if (OB_FAIL(ObServerTableOperator::get(*GCTX.sql_proxy_, server, server_info))) {
        // make sure the server is in whitelist, then send rpc
        LOG_WARN("fail to get server_info", KR(ret), K(server));
      } else if ((sys_tenant_data_version >= MOCK_DATA_VERSION_4_2_5_0
                  && sys_tenant_data_version < DATA_VERSION_4_3_0_0)
                || sys_tenant_data_version >= DATA_VERSION_4_3_2_0) {
        int64_t timeout = ctx.get_timeout();
        const int64_t ERR_MSG_BUF_LEN = OB_MAX_SERVER_ADDR_SIZE + 150;
        char disk_error_server_err_msg[ERR_MSG_BUF_LEN] = "";
        int64_t pos = 0;
        if (OB_UNLIKELY(timeout <= 0)) {
          ret = OB_TIMEOUT;
          LOG_WARN("ctx time out", KR(ret), K(timeout));
        } else if (OB_FAIL(databuff_print_multi_objs(
            disk_error_server_err_msg,
            ERR_MSG_BUF_LEN,
            pos,
            "The target server ",
            server, " may encounter device failures. Please check GV$OB_SERVERS for more information. START SERVER is"))) {
          LOG_WARN("fail to execute databuff_printf", KR(ret), K(server));
        } else if (OB_FAIL(rpc_arg.init(GCONF.self_addr_, server))) {
          LOG_WARN("fail to init rpc arg", KR(ret), K(GCONF.self_addr_), K(server));
        } else if (OB_FAIL(rpc_proxy_->to(server)
            .timeout(timeout)
            .check_server_machine_status(rpc_arg, rpc_result))) {
          LOG_WARN("fail to check server machine status", KR(ret), K(rpc_arg));
        } else if (OB_UNLIKELY(!rpc_result.is_valid())) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("rpc result is invalid", KR(ret), K(rpc_arg), K(rpc_result));
        } else if (!rpc_result.get_server_health_status().is_healthy()) {
          ret = OB_OP_NOT_ALLOW;
          LOG_WARN("server is not healthy, cannot start it", KR(ret), K(rpc_arg), K(rpc_result));
          LOG_USER_ERROR(OB_OP_NOT_ALLOW, disk_error_server_err_msg);
        }
      }
      if (FAILEDx(start_or_stop_server_(server, zone, ObAdminServerArg::START))) {
        LOG_WARN("fail to start server", KR(ret), K(server), K(zone));
      }
    }
  }
  return ret;
}
#ifdef OB_BUILD_TDE_SECURITY
int ObServerZoneOpService::master_key_checking_for_adding_server(
    const common::ObAddr &server,
    const ObZone &zone,
    obrpc::ObWaitMasterKeyInSyncArg &wms_in_sync_arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(is_inited_));
  } else if (OB_ISNULL(master_key_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("master_key_mgr_ is null", KR(ret), KP(master_key_mgr_));
  } else {
    bool master_key_empty = true;
    share::ObLeaseResponse tmp_lease_response;
    bool encryption = false;
    ObTimeoutCtx ctx;
    if (OB_FAIL(master_key_mgr_->check_master_key_empty(master_key_empty))) {
      LOG_WARN("fail to check whether master key is empty", KR(ret));
    } else if (master_key_empty) {
      LOG_INFO("empty master key, no need to sync master key info");
    } else if (!master_key_empty && zone.is_empty()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "not support to add a server "
      "without a specified zone when the master key is valid");
    } else if (OB_FAIL(ObZoneTableOperation::check_encryption_zone(*sql_proxy_, zone, encryption))) {
      LOG_WARN("fail to check zone encryption", KR(ret), "zone", zone);
    } else if (encryption) {
      LOG_INFO("server in encrypted zone, no need to sync master key info", "zone", zone);
    } else if (OB_FAIL(master_key_mgr_->get_all_tenant_master_key(
            zone, wms_in_sync_arg.tenant_max_key_version_))) {
      LOG_WARN("fail to get all tenant master key", KR(ret));
    } else if (OB_FAIL(OTC_MGR.get_lease_response(tmp_lease_response))) {
      LOG_WARN("fail to get lease response", KR(ret));
    } else if (OB_FAIL(wms_in_sync_arg.tenant_config_version_.assign(
            tmp_lease_response.tenant_config_version_))) {
      LOG_WARN("fail to assign tenant config version", KR(ret));
    } else if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", KR(ret), K(ctx));
    } else {
      int64_t timeout = ctx.get_timeout();
      if (OB_UNLIKELY(timeout <= 0)) {
        ret = OB_TIMEOUT;
        LOG_WARN("ctx time out", KR(ret), K(timeout));
      } else if (OB_FAIL(rpc_proxy_->to(server)
          .timeout(timeout)
          .wait_master_key_in_sync(wms_in_sync_arg))) {
        LOG_WARN("fail to wait master key in sync", KR(ret), K(server));
      } else {}
    }
  }
  return ret;
}
#endif

int ObServerZoneOpService::stop_server_precheck(
    const ObIArray<ObAddr> &servers,
    const obrpc::ObAdminServerArg::AdminServerOp &op)
{
  int ret = OB_SUCCESS;
  ObZone zone;
  bool is_same_zone = false;
  bool is_all_stopped = false;
  ObArray<ObServerInfoInTable> all_servers_info_in_table;
  ObServerInfoInTable server_info;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(is_inited_));
  } else if (OB_UNLIKELY(servers.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("servers' count is zero", KR(ret), K(servers));
  } else if (OB_ISNULL(GCTX.root_service_) || OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.root_service_ or sql_proxy_ is null", KR(ret), KP(GCTX.root_service_), KP(sql_proxy_));
  } else if (OB_FAIL(ObServerTableOperator::get(*sql_proxy_, all_servers_info_in_table))) {
    LOG_WARN("fail to read __all_server table", KR(ret), KP(sql_proxy_));
  } else if (OB_FAIL(check_zone_and_server_(
      all_servers_info_in_table,
      servers,
      is_same_zone,
      is_all_stopped))) {
    LOG_WARN("fail to check zone and server", KR(ret), K(all_servers_info_in_table), K(servers));
  } else if (is_all_stopped) {
    //nothing todo
  } else if (!is_same_zone) {
    ret = OB_STOP_SERVER_IN_MULTIPLE_ZONES;
    LOG_WARN("can not stop servers in multiple zones", KR(ret), K(server_info), K(servers));
  } else if (OB_FAIL((ObRootUtils::find_server_info(all_servers_info_in_table, servers.at(0), server_info)))) {
    LOG_WARN("fail to find server info", KR(ret), K(all_servers_info_in_table), K(servers.at(0)));
  } else {
    const ObZone &zone = server_info.get_zone();
    if (ObAdminServerArg::ISOLATE == op) {
      //"Isolate server" does not need to check the total number and status of replicas; it cannot be restarted later;
      if (OB_FAIL(GCTX.root_service_->check_can_stop(zone, servers, false /*is_stop_zone*/))) {
        LOG_WARN("fail to check can stop", KR(ret), K(zone), K(servers), K(op));
        if (OB_OP_NOT_ALLOW == ret) {
          LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Stop all servers in primary region is");
        }
      }
    } else {
      if (ObRootUtils::have_other_stop_task(zone)) {
        ret = OB_STOP_SERVER_IN_MULTIPLE_ZONES;
        LOG_WARN("can not stop servers in multiple zones", KR(ret), K(zone), K(servers), K(op));
        LOG_USER_ERROR(OB_STOP_SERVER_IN_MULTIPLE_ZONES,
            "cannot stop server or stop zone in multiple zones");
      } else if (OB_FAIL(GCTX.root_service_->check_majority_and_log_in_sync(
          servers,
          ObAdminServerArg::FORCE_STOP == op,/*skip_log_sync_check*/
          "stop server"))) {
        LOG_WARN("fail to check majority and log in-sync", KR(ret), K(zone), K(servers), K(op));
      }
    }
  }
  return ret;
}

int ObServerZoneOpService::check_startup_mode_match_(const share::ObServerMode startup_mode)
{
  int ret = OB_SUCCESS;
  bool match = false;
  if (share::ObServerMode::INVALID_MODE == startup_mode) {
    if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_3_0) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("invalid startup_mode, server's build_version lower than 4.3", KR(ret), K(startup_mode));
    } else {
      // during upgrading, server to add is lower version than 4.4, it must be NORMAL_MODE
      match = !GCTX.is_shared_storage_mode();
    }
  } else {
    match = startup_mode == GCTX.startup_mode_;
  }
  if (OB_SUCC(ret) && !match) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("added server startup mode mot match not allowed", KR(ret),
              "current_mode", GCTX.startup_mode_, "added_server_mode", startup_mode);
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "startup mode not match, add server");
    // TODO(cangming.zl): add case
  }
  return ret;
}

int ObServerZoneOpService::zone_checking_for_adding_server_(
    const ObZone &rpc_zone,
    ObZone &picked_zone)
{
  int ret = OB_SUCCESS;
  // rpc_zone: the zone specified in the server's local config and send to rs via rpc
  // picked_zone: the zone we will use in add_server
  // picked_zone is initialized in add_servers by command_zone
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(is_inited_));
  } else if (OB_UNLIKELY(rpc_zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("rpc_zone cannot be empty. It implies that server's local config zone is empty.",
    KR(ret), K(rpc_zone));
    // when picked_zone is empty, user did not specify zone in command
    // we use zone specified in observer command line
  } else if (picked_zone.is_empty()) {
    if (OB_FAIL(picked_zone.assign(rpc_zone))) {
      LOG_WARN("fail to assign picked_zone", KR(ret), K(rpc_zone));
    }
  } else if (picked_zone != rpc_zone) {
    ret = OB_SERVER_ZONE_NOT_MATCH;
    LOG_WARN("the zone specified in the server's local config is not the same as"
        " the zone specified in the command", KR(ret), K(picked_zone), K(rpc_zone));
  } else {}
  return ret;
}
int ObServerZoneOpService::add_server_(
    const ObAddr &server,
    const uint64_t server_id,
    const ObZone &zone,
    const int64_t sql_port,
    const ObServerInfoInTable::ObBuildVersion &build_version,
    const ObIArray<ObZoneStorageTableInfo> &storage_infos)
{
  int ret = OB_SUCCESS;
  bool is_active = false;
  const int64_t now = ObTimeUtility::current_time();
  ObServerInfoInTable server_info_in_table;
  ObArray<uint64_t> server_id_in_cluster;
  ObMySQLTransaction trans;
  DEBUG_SYNC(BEFORE_ADD_SERVER_TRANS);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!server.is_valid()
      || !is_valid_server_id(server_id)
      || zone.is_empty()
      || sql_port <= 0
      || build_version.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(server), K(server_id), K(zone), K(sql_port), K(build_version));
  } else if (OB_ISNULL(sql_proxy_) || OB_ISNULL(server_change_callback_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ or server_change_callback_ is null", KR(ret),
        KP(sql_proxy_), KP(server_change_callback_));
  } else if (OB_FAIL(trans.start(sql_proxy_, OB_SYS_TENANT_ID))) {
    LOG_WARN("fail to start trans", KR(ret));
  } else if (OB_FAIL(ObServiceEpochProxy::check_and_update_server_zone_op_service_epoch(trans))) {
    LOG_WARN("fail to check and update service epoch", KR(ret));
  } else if (OB_FAIL(ObZoneTableOperation::check_zone_active(trans, zone, is_active))){
    // we do not need to lock the zone info in __all_zone table
    // all server/zone operations are mutually exclusive since we locked the service epoch
    LOG_WARN("fail to check whether the zone is active", KR(ret), K(zone));
  } else if (OB_UNLIKELY(!is_active)) {
    ret = OB_ZONE_NOT_ACTIVE;
    LOG_WARN("the zone is not active", KR(ret), K(zone), K(is_active));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (GCTX.is_shared_storage_mode() &&
    OB_FAIL(check_storage_infos_not_changed_(trans, zone, storage_infos))) {
    LOG_WARN("check zone storage not changed failed", KR(ret), K(zone));
#endif
  } else if (OB_FAIL(ObServerTableOperator::get(trans, server, server_info_in_table))) {
    if (OB_SERVER_NOT_IN_WHITE_LIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get server_info in table", KR(ret), K(server));
    }
  } else {
    ret = OB_ENTRY_EXIST;
    LOG_WARN("server exists", KR(ret), K(server_info_in_table));
  }
  if (FAILEDx(ObServerTableOperator::get_clusters_server_id(trans, server_id_in_cluster))) {
    LOG_WARN("fail to get servers' id in the cluster", KR(ret));
  } else if (OB_UNLIKELY(!check_server_index_(server_id, server_id_in_cluster))) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("server index is outdated due to concurrent operations", KR(ret), K(server_id), K(server_id_in_cluster));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "server index is outdated due to concurrent operations, ADD_SERVER is");
  } else if (OB_FAIL(server_info_in_table.init(
      server,
      server_id,
      zone,
      sql_port,
      false, /* with_rootserver */
      ObServerStatus::OB_SERVER_ACTIVE,
      build_version,
      0, /* stop_time */
      0, /* start_service_time */
      0 /* last_offline_time */))) {
    LOG_WARN("fail to init server info in table", KR(ret), K(server), K(server_id), K(zone),
        K(sql_port), K(build_version), K(now));
  } else if (OB_FAIL(ObServerTableOperator::insert(trans, server_info_in_table))) {
    LOG_WARN("fail to insert server info into __all_server table", KR(ret), K(server_info_in_table));
  }
  (void) end_trans_and_on_server_change_(ret, trans, "add_server", server, zone, now);
  return ret;
}
int ObServerZoneOpService::delete_server_(
    const common::ObAddr &server,
    const ObZone &zone)
{
  int ret = OB_SUCCESS;
  ObServerInfoInTable server_info_in_table;
  const int64_t now = ObTimeUtility::current_time();
  char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  ObMySQLTransaction trans;
  int64_t job_id = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!server.is_valid() || !server.ip_to_string(ip, sizeof(ip)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(server));
  } else if (OB_ISNULL(sql_proxy_) || OB_ISNULL(server_change_callback_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ or server_change_callback_ is null", KR(ret),
        KP(sql_proxy_), KP(server_change_callback_));
  } else if (OB_FAIL(trans.start(sql_proxy_, OB_SYS_TENANT_ID))) {
    LOG_WARN("fail to start trans", KR(ret));
  } else if (OB_FAIL(ObServiceEpochProxy::check_and_update_server_zone_op_service_epoch(trans))) {
    LOG_WARN("fail to check and update service epoch", KR(ret));
  } else if (OB_FAIL(ObServerTableOperator::get(trans, server, server_info_in_table))) {
    LOG_WARN("fail to get server_info in table", KR(ret), K(server));
  } else if (!zone.is_empty() && zone != server_info_in_table.get_zone()) {
    ret = OB_SERVER_ZONE_NOT_MATCH;
    LOG_WARN("zone not matches", KR(ret), K(server), K(zone), K(server_info_in_table));
  } else if (OB_UNLIKELY(server_info_in_table.is_deleting())) {
    ret = OB_SERVER_ALREADY_DELETED;
    LOG_WARN("the server has been deleted", KR(ret), K(server_info_in_table));
  } else if (OB_FAIL(RS_JOB_CREATE_WITH_RET(
      job_id,
      JOB_TYPE_DELETE_SERVER,
      trans,
      "svr_ip", ip,
      "svr_port", server.get_port()))) {
    LOG_WARN("fail to create rs job DELETE_SERVER", KR(ret));
  } else if (OB_FAIL(ObServerTableOperator::update_status(
      trans,
      server,
      server_info_in_table.get_status(),
      ObServerStatus::OB_SERVER_DELETING))) {
    LOG_WARN("fail to update status", KR(ret), K(server), K(server_info_in_table));
  }
  (void) end_trans_and_on_server_change_(ret, trans, "delete_server", server, server_info_in_table.get_zone(), now);
  return ret;
}
int ObServerZoneOpService::check_and_end_delete_server_(
    common::ObMySQLTransaction &trans,
    const common::ObAddr &server,
    const ObZone &zone,
    const bool is_cancel,
    share::ObServerInfoInTable &server_info)
{
  int ret = OB_SUCCESS;
  server_info.reset();
  char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!server.is_valid() || !server.ip_to_string(ip, sizeof(ip)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(server));
  } else if (OB_FAIL(ObServiceEpochProxy::check_and_update_server_zone_op_service_epoch(trans))) {
    LOG_WARN("fail to check and update service epoch", KR(ret));
  } else if (OB_FAIL(ObServerTableOperator::get(trans, server, server_info))) {
    LOG_WARN("fail to get server_info in table", KR(ret), K(server));
  } else if (!zone.is_empty() && zone != server_info.get_zone()) {
    ret = OB_SERVER_ZONE_NOT_MATCH;
    LOG_WARN("zone not matches", KR(ret), K(server), K(zone), K(server_info));
  } else if (OB_UNLIKELY(!server_info.is_deleting())) {
    ret = OB_SERVER_NOT_DELETING;
    LOG_ERROR("server is not in deleting status, cannot be removed from __all_server table",
        KR(ret), K(server_info));
  } else {
    int64_t job_id = 0;
    ret = RS_JOB_FIND(DELETE_SERVER, job_id, trans,
                      "svr_ip", ip, "svr_port", server.get_port());
    if (OB_SUCC(ret)  && job_id > 0) {
      int tmp_ret = is_cancel ? OB_CANCELED : OB_SUCCESS;
      if (OB_FAIL(RS_JOB_COMPLETE(job_id, tmp_ret, trans))) {
        LOG_WARN("fail to all_rootservice_job" , KR(ret), K(server));
      }
    } else {
      LOG_WARN("failed to find job", KR(ret), K(server));
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}
int ObServerZoneOpService::start_or_stop_server_(
    const common::ObAddr &server,
    const ObZone &zone,
    const obrpc::ObAdminServerArg::AdminServerOp &op)
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  ObServerInfoInTable server_info;
  ObMySQLTransaction trans;
  bool is_start = (ObAdminServerArg::START == op);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(server));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is null", KR(ret), KP(sql_proxy_));
  } else if (OB_FAIL(trans.start(sql_proxy_, OB_SYS_TENANT_ID))) {
    LOG_WARN("fail to start trans", KR(ret));
  } else if (OB_FAIL(ObServiceEpochProxy::check_and_update_server_zone_op_service_epoch(trans))) {
    LOG_WARN("fail to check and update service epoch", KR(ret));
  } else if (OB_FAIL(ObServerTableOperator::get(trans, server, server_info))) {
    LOG_WARN("fail to get server_info", KR(ret), K(server));
  } else if (!zone.is_empty() && zone != server_info.get_zone()) {
    ret = OB_SERVER_ZONE_NOT_MATCH;
    LOG_WARN("zone not matches", KR(ret), K(server), K(zone), K(server_info));
  } else if (ObAdminServerArg::STOP == op || ObAdminServerArg::FORCE_STOP == op) {
    // check again, if there exists stopped servers in other zones
    if (ObRootUtils::have_other_stop_task(server_info.get_zone())) {
      ret = OB_STOP_SERVER_IN_MULTIPLE_ZONES;
      LOG_WARN("can not stop servers in multiple zones", KR(ret), K(server_info.get_zone()));
      LOG_USER_ERROR(OB_STOP_SERVER_IN_MULTIPLE_ZONES,
          "cannot stop server or stop zone in multiple zones");
    }
  }
  if (OB_SUCC(ret)) {
      int64_t new_stop_time = is_start ? 0 : now;
      int64_t old_stop_time = server_info.get_stop_time();
      if ((is_start && 0 != old_stop_time) || (!is_start && 0 == old_stop_time)) {
        if (OB_FAIL(ObServerTableOperator::update_stop_time(
          trans,
          server,
          old_stop_time,
          new_stop_time))) {
            LOG_WARN("fail to update stop_time", KR(ret), K(server), K(old_stop_time), K(new_stop_time));
        }
      }
      LOG_INFO("update stop time", KR(ret), K(server_info),
          K(old_stop_time), K(new_stop_time), K(op), K(is_start));
  }
  const char *op_print_str = is_start ? "start_server" : "stop_server";
  (void) end_trans_and_on_server_change_(ret, trans, op_print_str, server, server_info.get_zone(), now);
  return ret;
}

int ObServerZoneOpService::construct_rs_list_arg(ObRsListArg &rs_list_arg)
{
  int ret = OB_SUCCESS;
  ObLSInfo ls_info;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(is_inited_));
  } else if (OB_ISNULL(lst_operator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lst operator is null", KR(ret), KP(lst_operator_));
  } else if (OB_FAIL(lst_operator_->get(
      GCONF.cluster_id,
      OB_SYS_TENANT_ID,
      SYS_LS,
      share::ObLSTable::DEFAULT_MODE,
      ls_info))) {
    LOG_WARN("fail to get ls info", KR(ret));
  } else {
    rs_list_arg.master_rs_ = GCONF.self_addr_;
    FOREACH_CNT_X(replica, ls_info.get_replicas(), OB_SUCC(ret)) {
      if (replica->get_server() == GCONF.self_addr_
          || (replica->is_in_service()
              && ObReplicaTypeCheck::is_paxos_replica_V2(replica->get_replica_type()))) {
        if (OB_FAIL(rs_list_arg.rs_list_.push_back(replica->get_server()))) {
          LOG_WARN("fail to push a server into rs list", KR(ret), K(replica->get_server()));
        }
      }
    }
  }
  return ret;
}
int ObServerZoneOpService::fetch_new_server_id_(uint64_t &server_id)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> server_id_in_cluster;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(is_inited_));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid sql proxy", KR(ret), KP(sql_proxy_));
  } else if (OB_FAIL(ObServerTableOperator::get_clusters_server_id(*sql_proxy_, server_id_in_cluster))) {
    LOG_WARN("fail to get server_ids in the cluster", KR(ret), KP(sql_proxy_));
  } else if (OB_UNLIKELY(server_id_in_cluster.count() >= MAX_SERVER_COUNT)) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("server count reaches the limit", KR(ret), K(server_id_in_cluster.count()));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "server count reaches the limit, ADD_SERVER is");
  } else {
    uint64_t candidate_server_id = OB_INVALID_ID;
    ObMaxIdFetcher id_fetcher(*sql_proxy_);
    if (OB_FAIL(id_fetcher.fetch_new_max_id(
        OB_SYS_TENANT_ID,
        OB_MAX_USED_SERVER_ID_TYPE,
        candidate_server_id))) {
      LOG_WARN("fetch_new_max_id failed", KR(ret));
    } else {
      uint64_t new_candidate_server_id = candidate_server_id;
      while (!check_server_index_(new_candidate_server_id, server_id_in_cluster)) {
        if (new_candidate_server_id % 10 == 0) {
          LOG_INFO("[FETCH NEW SERVER ID] periodical log", K(new_candidate_server_id), K(server_id_in_cluster));
        }
        ++new_candidate_server_id;
      }
      if (new_candidate_server_id != candidate_server_id
          && OB_FAIL(id_fetcher.update_server_max_id(candidate_server_id, new_candidate_server_id))) {
        LOG_WARN("fail to update server max id", KR(ret), K(candidate_server_id), K(new_candidate_server_id),
            K(server_id_in_cluster));
      }
      if (OB_SUCC(ret)) {
        server_id = new_candidate_server_id;
        LOG_INFO("[FETCH NEW SERVER ID] new candidate server id", K(server_id), K(server_id_in_cluster));
      }
    }
  }
  return ret;
}
bool ObServerZoneOpService::check_server_index_(
    const uint64_t candidate_server_id,
    const common::ObIArray<uint64_t> &server_id_in_cluster) const
{
  // server_index = server_id % 4096
  // server_index cannot be zero and must be unique in the cluster
  bool is_good_candidate = true;
  const uint64_t candidate_index = ObShareUtil::compute_server_index(candidate_server_id);
  if (0 == candidate_index) {
    is_good_candidate = false;
  } else {
    for (int64_t i = 0; i < server_id_in_cluster.count() && is_good_candidate; ++i) {
      const uint64_t server_index = ObShareUtil::compute_server_index(server_id_in_cluster.at(i));
      if (candidate_index == server_index) {
        is_good_candidate = false;
      }
    }
  }
  return is_good_candidate;
}
int ObServerZoneOpService::check_server_have_enough_resource_for_delete_server_(
    const ObIArray<ObAddr> &servers,
    const ObZone &zone)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(is_inited_));
  } else if (OB_ISNULL(unit_manager_) || OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit_manager_ or sql_proxy_ is null", KR(ret), KP(unit_manager_), KP(sql_proxy_));
  } else {
    ObServerInfoInTable server_info;
    FOREACH_CNT_X(server, servers, OB_SUCC(ret)) {
      server_info.reset();
      if (OB_FAIL(ObServerTableOperator::get(*sql_proxy_, *server, server_info))) {
        LOG_WARN("fail to get server_info in table", KR(ret), KP(sql_proxy_), KPC(server));
      } else if (!zone.is_empty() && server_info.get_zone() != zone) {
        ret = OB_SERVER_ZONE_NOT_MATCH;
        LOG_WARN("the arg zone is not the same as the server's zone in __all_server table", KR(ret),
            K(zone), K(server_info));
      } else if (OB_FAIL(unit_manager_->check_enough_resource_for_delete_server(
              *server, server_info.get_zone()))) {
        LOG_WARN("fail to check enouch resource", KR(ret), KPC(server), K(server_info));
      }
    }//end for each
  }
  return ret;
}
int ObServerZoneOpService::check_zone_and_server_(
    const ObIArray<share::ObServerInfoInTable> &servers_info,
    const ObIArray<ObAddr> &servers,
    bool &is_same_zone,
    bool &is_all_stopped)
{
  int ret = OB_SUCCESS;
  is_same_zone = true;
  is_all_stopped = true;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(is_inited_));
  } else {
    ObServerInfoInTable server_info;
    ObZone zone;
    for (int64_t i = 0; i < servers.count() && OB_SUCC(ret) && (is_same_zone || is_all_stopped); i++) {
      const ObAddr &server = servers.at(i);
      server_info.reset();
      if (OB_FAIL(ObRootUtils::find_server_info(servers_info, server, server_info))) {
        LOG_WARN("fail to get server info", KR(ret), K(servers_info), K(server));
      } else if (0 == i) {
        if (OB_FAIL(zone.assign(server_info.get_zone()))) {
          LOG_WARN("fail to assign zone", KR(ret), K(server_info.get_zone()));
        }
      } else if (zone != server_info.get_zone()) {
        is_same_zone = false;
        LOG_WARN("server zone not same", K(zone), K(server_info), K(servers));
      }
      if (OB_FAIL(ret)) {
      } else if (!server_info.is_stopped()) {
        is_all_stopped = false;
      }
    }
  }
  return ret;
}
ERRSIM_POINT_DEF(ALL_SERVER_LIST_ERROR);
void ObServerZoneOpService::end_trans_and_on_server_change_(
    int &ret,
    common::ObMySQLTransaction &trans,
    const char *op_print_str,
    const common::ObAddr &server,
    const ObZone &zone,
    const int64_t start_time)
{
  int tmp_ret = OB_SUCCESS;
  LOG_INFO("start execute end_trans_and_on_server_change_", KR(ret),
      K(op_print_str), K(server), K(zone), K(start_time));
  if (OB_UNLIKELY(!trans.is_started())) {
    LOG_WARN("the transaction is not started");
  } else {
    if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
      LOG_WARN("fail to commit the transaction", KR(ret), KR(tmp_ret), K(server), K(zone));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  if (OB_TMP_FAIL(SVR_TRACER.refresh())) {
    LOG_WARN("fail to refresh server tracer", KR(ret), KR(tmp_ret));
  }
  bool no_on_server_change = ALL_SERVER_LIST_ERROR ? true : false;
  if (OB_ISNULL(server_change_callback_)) {
    tmp_ret = OB_ERR_UNEXPECTED;
    LOG_WARN("server_change_callback_ is null", KR(ret), KR(tmp_ret), KP(server_change_callback_));
    ret = OB_SUCC(ret) ? tmp_ret : ret;
  } else if (no_on_server_change) {
  } else if (OB_TMP_FAIL(server_change_callback_->on_server_change())) {
    LOG_WARN("fail to callback on server change", KR(ret), KR(tmp_ret));
  }
  int64_t time_cost = ::oceanbase::common::ObTimeUtility::current_time() - start_time;
  FLOG_INFO(op_print_str, K(server),  K(zone), "time cost", time_cost, KR(ret));
  ROOTSERVICE_EVENT_ADD("server", op_print_str, K(server), K(ret));
}
}
}
