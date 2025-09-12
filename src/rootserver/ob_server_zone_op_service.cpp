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
#include "rootserver/ob_root_service.h" // callback
#include "share/ob_license_utils.h"
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

#define PALF_KV_LOG_INFO(fmt, args...) FLOG_INFO("[PALF_KV_DR] " fmt, ##args)

// format: [id1, timstamp1, id2, timestamp2, .... max_server_id]
const char *ObServerZoneOpService::PALF_KV_SERVER_IDS_INFOS_PREFIX = "SERVER_IDS_INFOS_FOR_REPLACE_SYS:CLUSTER_ID=%ld";
const char *ObServerZoneOpService::PALF_KV_ZONE_NAMES_INFOS_PREFIX = "ZONE_NAMES_INFOS_FOR_REPLACE_SYS:CLUSTER_ID=%ld";
const char *ObServerZoneOpService::PALF_KV_MAX_UNIT_ID_FORMAT_STR = "MAX_UNIT_ID_FOR_REPLACE_SYS:CLUSTER_ID=%ld";
const char *ObServerZoneOpService::PALF_KV_TENANT_DATA_VERSION_FORMAT_STR = "TENANT_DATA_VERSION_FOR_REPLACE_TENANT:CLUSTER_ID=%ld:TENANT_ID=%lu";

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
  } else {
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
          , GET_MIN_CLUSTER_VERSION()
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
  } else if (OB_FAIL(check_logservice_deployment_mode_(rpc_result.get_enable_logservice()))) {
    LOG_WARN("failed to check logservice", KR(ret), K(rpc_result));
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
  } else if (OB_FAIL(ObLicenseUtils::check_add_server_allowed(servers.count()))) {
    LOG_WARN("fail to check add server allowed", KR(ret));
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
      } else if (!is_bootstrap && OB_FAIL(handle_master_key_for_adding_server(addr, picked_zone, wms_in_sync_arg))) {
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
      }
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
#ifdef OB_BUILD_SHARED_STORAGE
  if (OB_FAIL(ret)) {
  } else if (GCTX.is_shared_storage_mode() && OB_FAIL(delete_server_id_in_palf_kv(server_info_in_table.get_server_id()))) {
    LOG_WARN("fail to delete server id in palf kv", KR(ret), K(server_info_in_table));
  }
#endif
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
int ObServerZoneOpService::handle_master_key_for_adding_server(
    const common::ObAddr &server,
    const ObZone &zone,
    obrpc::ObWaitMasterKeyInSyncArg &wms_in_sync_arg)
{
  int ret = OB_SUCCESS;
  if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_4_0_0) {
    if (OB_FAIL(send_master_key_for_adding_server(server))) {
      LOG_WARN("fail to send master key for adding server", KR(ret), K(server));
    }
  } else if (OB_FAIL(check_master_key_for_adding_server(server, zone, wms_in_sync_arg))){
    LOG_WARN("fail to check master key for adding server", KR(ret), K(server), K(zone));
  }
  return ret;
}

int ObServerZoneOpService::check_master_key_for_adding_server(
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

int ObServerZoneOpService::send_master_key_for_adding_server(
    const common::ObAddr &server)
{
  int ret = OB_SUCCESS;
  obrpc::ObSetMasterKeyArg all_master_keys;
  // After version 4.4, we send the master_key stored locally on the RS to the new observer(may not be the latest),
  // the process of adding a server no longer waits observer to update to the latest version.
  // Subsequent RS can then notify other observers of the latest version of the tenant's master_key through heartbeat,
  // the observer can read the inner table to refresh the latest master key.
  int64_t rpc_timeout = ObServerZoneOpService::OB_SERVER_SEND_MASTER_KEY_TIMEOUT + GCONF.rpc_timeout;
  const int64_t start_time = ObTimeUtility::fast_current_time();
  if (OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("srv_rpc_proxy_ is nullptr", KR(ret), KP(GCTX.srv_rpc_proxy_));
  } else if (OB_FAIL(ObMasterKeyGetter::instance().get_all_master_keys(all_master_keys))) {
    LOG_WARN("fail to get all master keys", KR(ret));
  } else if (0 == all_master_keys.master_key_version_array_.count()) {
    LOG_INFO("no tenant master key, skip");
  } else if (OB_FAIL(GCTX.srv_rpc_proxy_->to(server).timeout(rpc_timeout).set_master_key(all_master_keys))) {
    LOG_WARN("fail to execute rpc", KR(ret));
  }
  const int64_t cost = ObTimeUtility::fast_current_time() - start_time;
  LOG_INFO("send master_key for adding server", KR(ret), K(cost), K(server), "count", all_master_keys.master_key_version_array_.count());
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

int ObServerZoneOpService::check_logservice_deployment_mode_(
    const bool added_server_logservice)
{
  // compatibility is guaranteed
  int ret = OB_SUCCESS;
  if (added_server_logservice != GCONF.enable_logservice) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("added server logservice mode not match not allowed",
            KR(ret), "current_mode", GCONF.enable_logservice, "added_server_mode", added_server_logservice);
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "Logservice deployment mode not match, add server");
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
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (GCTX.is_shared_storage_mode() && OB_FAIL(insert_zone_in_palf_kv(zone))) {
    // after confirming the zone exists, we can insert the zone into palf kv to ensure that
    // whenever there is a server in the zone, the zone must also exist in palf kv.
    // and do not check data_version.
    LOG_WARN("fail to insert zone in palf kv", KR(ret), K(zone));
#endif
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
  } else if (OB_UNLIKELY(!check_server_index(server_id, server_id_in_cluster))) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("server index is outdated due to concurrent operations", KR(ret), K(server_id), K(server_id_in_cluster));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "server index is outdated due to concurrent operations, ADD_SERVER is");
  } else if (OB_FAIL(server_info_in_table.init(
      server,
      server_id,
      zone,
      sql_port,
#ifdef OB_ENABLE_STANDALONE_LAUNCH
      true, /* with_rootserver */
#else
      false, /* with_rootserver */
#endif
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

#ifdef OB_BUILD_SHARED_STORAGE

#define KEY_OF_RESOURCE_IN_PALF_KV(format)                                                                                     \
  common::ObString row_key;                                                                                                    \
  const int64_t cluster_id = GCONF.cluster_id;                                                                                 \
  char row_key_buf[ObServerZoneOpService::MAX_ROW_KEY_LENGTH] = {'\0'};                                                        \
  int64_t row_key_len = 0;                                                                                                     \
  if (FAILEDx(databuff_printf(row_key_buf, ObServerZoneOpService::MAX_ROW_KEY_LENGTH, row_key_len, format, cluster_id))) {     \
    LOG_WARN("failed to print rowkey", KR(ret), K(format), K(cluster_id));                                                     \
  } else {                                                                                                                     \
    row_key.assign_ptr(row_key_buf, row_key_len);                                                                              \
  }                                                                                                                            \

#define SERIALIZE_SERVER_OR_ZONE_FOR_PALF_KV(resources, buffer, str_var)                                                       \
  do {                                                                                                                         \
    if (OB_SUCC(ret)) {                                                                                                        \
      int64_t pos = 0;                                                                                                         \
      MEMSET((buffer), 0, MAX_BUFFER_SIZE);                                                                                    \
      if (OB_FAIL((resources).serialize((buffer), MAX_BUFFER_SIZE, pos))) {                                                    \
        LOG_WARN("serialize failed", KR(ret));                                                                                 \
      } else {                                                                                                                 \
        (str_var).assign_ptr((buffer), pos);                                                                                   \
      }                                                                                                                        \
    }                                                                                                                          \
  } while (0)                                                                                                                  \

#define GET_SERVER_OR_ZONE_FUNC_DEFINE(RESOUCE_NAME, RESOURCE_TYPE, KEY_NAME)                                       \
  int ObServerZoneOpService::get_##RESOUCE_NAME##_from_palf_kv_(RESOURCE_TYPE &resources_array)                     \
  {                                                                                                                 \
    int ret = OB_SUCCESS;                                                                                           \
    int64_t pos = 0;                                                                                                \
    sslog::ObSSLogKVPalfAdapter palf_kv_adapter;                                                                    \
    ObTenantMutilAllocator allocator(OB_SYS_TENANT_ID);                                                             \
    common::ObStringBuffer resource_id_str(&allocator);                                                             \
    KEY_OF_RESOURCE_IN_PALF_KV(ObServerZoneOpService::PALF_KV_##KEY_NAME##_INFOS_PREFIX)                            \
    if (FAILEDx(palf_kv_adapter.init(cluster_id, OB_SYS_TENANT_ID))) {                                              \
      LOG_WARN("init palf kv adapter failed", KR(ret), K(cluster_id));                                              \
    } else if (OB_FAIL(palf_kv_adapter.get(row_key, resource_id_str))) {                                            \
      LOG_WARN("get resource from palf kv failed", KR(ret), K(row_key));                                            \
    } else if (OB_FAIL(resources_array.deserialize(resource_id_str.ptr(), resource_id_str.length(), pos))) {        \
      LOG_WARN("deserialize resources array failed", KR(ret), K(resource_id_str));                                  \
    } else if (OB_UNLIKELY(pos != resource_id_str.length())) {                                                      \
      ret = OB_DESERIALIZE_ERROR;                                                                                   \
      LOG_WARN("failed to deserialize", KR(ret), K(pos), "size", resource_id_str.length());                         \
    }                                                                                                               \
    PALF_KV_LOG_INFO("get all resources in palf_kv", KR(ret), K(row_key), K(resources_array));                      \
    return ret;                                                                                                     \
  }                                                                                                                 \

#define INSERT_SERVER_OR_ZONE_FUNC_DEFINE(RESOUCE_NAME, RESOURCE_TYPE, KEY_NAME)                                    \
  int ObServerZoneOpService::insert_##RESOUCE_NAME##_in_palf_kv_(const RESOURCE_TYPE &resources_array)              \
  {                                                                                                                 \
    int ret = OB_SUCCESS;                                                                                           \
    sslog::ObSSLogKVPalfAdapter palf_kv_adapter;                                                                    \
    const int64_t cluster_id = GCONF.cluster_id;                                                                    \
    if (OB_UNLIKELY(0 == resources_array.count())) {                                                                \
      ret = OB_INVALID_ARGUMENT;                                                                                    \
      LOG_WARN("invalid argument", KR(ret), K(resources_array));                                                    \
    } else if (OB_FAIL(palf_kv_adapter.init(cluster_id, OB_SYS_TENANT_ID))) {                                       \
      LOG_WARN("init palf kv adapter failed", KR(ret), K(cluster_id));                                              \
    } else {                                                                                                        \
      SMART_VAR(char[MAX_BUFFER_SIZE], buf) {                                                                       \
        common::ObString resource_ids_str;                                                                          \
        KEY_OF_RESOURCE_IN_PALF_KV(ObServerZoneOpService::PALF_KV_##KEY_NAME##_INFOS_PREFIX)                        \
        SERIALIZE_SERVER_OR_ZONE_FOR_PALF_KV(resources_array, buf, resource_ids_str);                               \
        if (FAILEDx(palf_kv_adapter.put(row_key, resource_ids_str))) {                                              \
          LOG_WARN("insert in palf kv failed", KR(ret), K(row_key), K(resource_ids_str));                           \
        }                                                                                                           \
        PALF_KV_LOG_INFO("insert resources in palf_kv", KR(ret), K(row_key), K(resources_array));                   \
      }                                                                                                             \
    }                                                                                                               \
    return ret;                                                                                                     \
  }                                                                                                                 \

#define CAS_SERVER_OR_ZONE_FUNC_DEFINE(RESOUCE_NAME, RESOURCE_TYPE, KEY_NAME)                                              \
  int ObServerZoneOpService::cas_##RESOUCE_NAME##_in_palf_kv_(                                                             \
      const RESOURCE_TYPE &old_resources,                                                                                  \
      const RESOURCE_TYPE &new_resources)                                                                                  \
  {                                                                                                                        \
    int ret = OB_SUCCESS;                                                                                                  \
    sslog::ObSSLogKVPalfAdapter palf_kv_adapter;                                                                           \
    const int64_t cluster_id = GCONF.cluster_id;                                                                           \
    if (OB_UNLIKELY(0 == old_resources.count() || 0 == new_resources.count())) {                                           \
      ret = OB_INVALID_ARGUMENT;                                                                                           \
      LOG_WARN("invalid argument", KR(ret), K(old_resources), K(new_resources));                                           \
    } else if (OB_FAIL(palf_kv_adapter.init(cluster_id, OB_SYS_TENANT_ID))) {                                              \
      LOG_WARN("init palf kv adapter failed", KR(ret), K(cluster_id));                                                     \
    } else {                                                                                                               \
      SMART_VARS_2((char[MAX_BUFFER_SIZE], old_resources_buf),                                                             \
                   (char[MAX_BUFFER_SIZE], new_resources_buf)) {                                                           \
        bool expected = false;                                                                                             \
        KEY_OF_RESOURCE_IN_PALF_KV(ObServerZoneOpService::PALF_KV_##KEY_NAME##_INFOS_PREFIX)                               \
        common::ObString old_resources_str;                                                                                \
        common::ObString new_resources_str;                                                                                \
        SERIALIZE_SERVER_OR_ZONE_FOR_PALF_KV(old_resources, old_resources_buf, old_resources_str);                         \
        SERIALIZE_SERVER_OR_ZONE_FOR_PALF_KV(new_resources, new_resources_buf, new_resources_str);                         \
        if (FAILEDx(palf_kv_adapter.cas(row_key, old_resources_str, new_resources_str, expected))) {                       \
          LOG_WARN("cas reources in palf kv failed", KR(ret), K(row_key), K(old_resources_str), K(new_resources_str));     \
        } else if (!expected) {                                                                                            \
          ret = OB_EAGAIN;                                                                                                 \
          LOG_WARN("cas reources in palf kv failed", KR(ret), K(row_key), K(old_resources_str), K(new_resources_str));     \
        }                                                                                                                  \
        PALF_KV_LOG_INFO("cas all reources in palf_kv", KR(ret), K(row_key),                                               \
          K(old_resources_str), K(new_resources_str), K(old_resources), K(new_resources));                                 \
      }                                                                                                                    \
    }                                                                                                                      \
    return ret;                                                                                                            \
  }                                                                                                                        \

GET_SERVER_OR_ZONE_FUNC_DEFINE(server_ids, ServerIDArray, SERVER_IDS);
GET_SERVER_OR_ZONE_FUNC_DEFINE(zone_names, ZoneNameArray, ZONE_NAMES);
INSERT_SERVER_OR_ZONE_FUNC_DEFINE(server_ids, ServerIDArray, SERVER_IDS);
INSERT_SERVER_OR_ZONE_FUNC_DEFINE(zone_names, ZoneNameArray, ZONE_NAMES);
CAS_SERVER_OR_ZONE_FUNC_DEFINE(server_ids, ServerIDArray, SERVER_IDS);
CAS_SERVER_OR_ZONE_FUNC_DEFINE(zone_names, ZoneNameArray, ZONE_NAMES);

// for upgrade and bootstrap
int ObServerZoneOpService::store_all_zone_in_palf_kv(const ZoneNameArray &zone_list)
{
  int ret = OB_SUCCESS;
  ZoneNameArray old_zone_names;
  if (!GCTX.is_shared_storage_mode()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported", KR(ret));
  } else if (OB_UNLIKELY(0 == zone_list.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(zone_list));
  } else if (OB_FAIL(get_zone_names_from_palf_kv_(old_zone_names))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      if (OB_FAIL(insert_zone_names_in_palf_kv_(zone_list))) {
        LOG_WARN("insert zone in palf kv failed", KR(ret), K(zone_list));
      }
    } else {
      LOG_WARN("fail to get zone names", KR(ret));
    }
  }
  PALF_KV_LOG_INFO("store zone names in palf kv", KR(ret), K(zone_list), K(old_zone_names));
  return ret;
}

int ObServerZoneOpService::check_new_zone_in_palf_kv(const ObZone& zone, bool &new_zone)
{
  int ret = OB_SUCCESS;
  new_zone = true;
  common::ObSEArray<ObZone, DEFAULT_ZONE_COUNT> zone_list;
  if (OB_UNLIKELY(zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argment", KR(ret), K(zone));
  } else if (OB_FAIL(get_zone_names_from_palf_kv_(zone_list))) {
    LOG_WARN("fail to get zone names", KR(ret));
  } else if (OB_UNLIKELY(0 == zone_list.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected zone names count", KR(ret), K(zone_list));
  } else {
    for (int64_t index = 0; new_zone && index < zone_list.count(); index++) {
      if (zone == zone_list.at(index)) {
        new_zone = false;
      }
    }
    PALF_KV_LOG_INFO("check new zone", KR(ret), K(zone), K(new_zone), K(zone_list));
  }
  return ret;
}

int ObServerZoneOpService::insert_zone_in_palf_kv(const ObZone &zone)
{
  int ret = OB_SUCCESS;
  ZoneNameArray orig_zone_names;
  ZoneNameArray new_zone_names;
  if (!GCTX.is_shared_storage_mode()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported", KR(ret));
  } else if (OB_UNLIKELY(zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(zone));
  } else if (OB_FAIL(get_zone_names_from_palf_kv_(orig_zone_names))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      // when deploying binary hybrid, no zone_name in palf kv
      // ignore it, upload it in upgrade.
      PALF_KV_LOG_INFO("there is no zone list in palf kv, do nothing", K(zone));
    } else {
      LOG_WARN("fail to get zone names", KR(ret));
    }
  } else if (OB_UNLIKELY(0 == orig_zone_names.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected zone names count", KR(ret), K(orig_zone_names));
  } else {
    bool found = false;
    for (int64_t index = 0; !found && index < orig_zone_names.count(); index++) {
      if (zone == orig_zone_names.at(index)) {
        found = true;
      }
    }
    if (found) {
      LOG_INFO("found zone, no need insert", K(zone), K(orig_zone_names));
    } else if (OB_FAIL(new_zone_names.assign(orig_zone_names))) {
      LOG_WARN("zone assign failed", KR(ret), K(orig_zone_names));
    } else if (OB_FAIL(new_zone_names.push_back(zone))) {
      LOG_WARN("push back zone failed", KR(ret), K(zone));
    } else if (OB_FAIL(cas_zone_names_in_palf_kv_(orig_zone_names, new_zone_names))) {
      LOG_WARN("cas zone name failed", KR(ret), K(orig_zone_names), K(new_zone_names));
    }
    PALF_KV_LOG_INFO("insert zone name in palf kv", KR(ret), K(found), K(zone), K(orig_zone_names), K(new_zone_names));
  }
  return ret;
}

int ObServerZoneOpService::delete_zone_from_palf_kv(const ObZone &zone)
{
  int ret = OB_SUCCESS;
  ZoneNameArray orig_zone_names;
  ZoneNameArray new_zone_names;
  if (!GCTX.is_shared_storage_mode()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported", KR(ret));
  } else if (OB_UNLIKELY(zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(zone));
  } else if (OB_FAIL(get_zone_names_from_palf_kv_(orig_zone_names))) {
    // When deleting or adding a zone, there is no data version check when writing to palf kv.
    // In hybrid deployment scenarios, both add and delete zone operations will not report errors here.
    // After upgrading, once the zone list is inserted into palf kv, subsequent add and delete zone operations will start writing to palf kv.
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("not found zone in palf kv", K(zone));
    } else {
      LOG_WARN("get all zone in palf_kv failed", KR(ret));
    }
  } else if (OB_UNLIKELY(0 == orig_zone_names.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected zone names count", KR(ret), K(orig_zone_names));
  } else {
    bool found = false;
    for (int64_t index = 0; OB_SUCC(ret) && index < orig_zone_names.count(); index++) {
      if (zone == orig_zone_names.at(index)) {
        found = true;
      } else if (OB_FAIL(new_zone_names.push_back(orig_zone_names.at(index)))) {
        LOG_WARN("push zone failed", KR(ret), K(orig_zone_names));
      }
    } // end for
    if (OB_FAIL(ret)) {
    } else if (!found) {
      LOG_INFO("not found zone", K(zone), K(orig_zone_names));
    } else if (OB_FAIL(cas_zone_names_in_palf_kv_(orig_zone_names, new_zone_names))) {
      LOG_WARN("cas zone name failed", KR(ret), K(orig_zone_names), K(new_zone_names));
    }
    PALF_KV_LOG_INFO("delete zone in palf kv", KR(ret), K(found), K(zone), K(orig_zone_names), K(new_zone_names));
  }
  return ret;
}

int ObServerZoneOpService::get_server_infos_from_palf_kv(
    ServerIDArray &server_ids,
    ServerIDArray &server_ids_with_ts,
    uint64_t &max_server_id)
{
  int ret = OB_SUCCESS;
  server_ids.reset();
  server_ids_with_ts.reset();
  max_server_id = OB_INVALID_ID;
  if (OB_FAIL(get_server_ids_from_palf_kv_(server_ids_with_ts))) {
    LOG_WARN("get all server_id in palf_kv failed", KR(ret));
  } else if (OB_UNLIKELY(0 == server_ids_with_ts.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected server_id array", KR(ret), K(server_ids_with_ts));
  } else {
    // array[n-1] is max_server_id. array[0, n-1) is all server_id.
    max_server_id = server_ids_with_ts.at(server_ids_with_ts.count() - 1);
    if (OB_FAIL(server_ids_with_ts.remove(server_ids_with_ts.count() - 1))) {
      // remove max server_id at last
      LOG_WARN("remove max_server_id failed", KR(ret), K(server_ids_with_ts));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < server_ids_with_ts.count();) {
        if (OB_FAIL(server_ids.push_back(server_ids_with_ts.at(i)))) {
          LOG_WARN("fail to push back", KR(ret), K(i), K(server_ids_with_ts));
        }
        i += 2;
      } // end for
    }
  }
  PALF_KV_LOG_INFO("get max_server_id and list in palf_kv",
    KR(ret), K(max_server_id), K(server_ids), K(server_ids_with_ts));
  return ret;
}

int ObServerZoneOpService::get_server_ids_from_palf_kv(
    ServerIDArray &server_ids)
{
  int ret = OB_SUCCESS;
  ServerIDArray server_ids_with_ts; // not used
  uint64_t max_server_id = 0; // not used
  if (!GCTX.is_shared_storage_mode()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported", KR(ret));
  } else if (OB_FAIL(get_server_infos_from_palf_kv(server_ids, server_ids_with_ts, max_server_id))) {
    LOG_WARN("get all server_id in palf_kv failed", KR(ret));
  }
  PALF_KV_LOG_INFO("get server ids from palf_kv",
    KR(ret), K(max_server_id), K(server_ids_with_ts), K(server_ids));
  return ret;
}

int ObServerZoneOpService::delete_server_id_in_palf_kv(const uint64_t server_id)
{
  int ret = OB_SUCCESS;
  ServerIDArray orig_server_ids;
  ServerIDArray new_server_ids;
  if (!GCTX.is_shared_storage_mode()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported", KR(ret));
  } else if (OB_FAIL(get_server_ids_from_palf_kv_(orig_server_ids))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("not found server_ids", K(server_id));
    } else {
      LOG_WARN("get all server_id in palf_kv failed", KR(ret));
    }
  } else if (OB_UNLIKELY(0 == orig_server_ids.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected server ids count", KR(ret), K(orig_server_ids));
  } else {
    bool found = false;
    for (int64_t index = 0; OB_SUCC(ret) && index < orig_server_ids.count() - 1;) {
      // orig_server_ids[count() - 1] is max_server_id, except it.
      if (server_id == orig_server_ids.at(index)) {
        found = true;
      } else if (OB_FAIL(new_server_ids.push_back(orig_server_ids.at(index)))) {
        LOG_WARN("push server_id failed", KR(ret), K(orig_server_ids));
      } else if (OB_FAIL(new_server_ids.push_back(orig_server_ids.at(index + 1)))) {
        LOG_WARN("push ts failed", KR(ret), K(orig_server_ids));
      }
      index += 2;
    } // end for
    if (OB_FAIL(ret)) {
    } else if (!found) {
      LOG_INFO("not found server_id", K(server_id), K(new_server_ids));
    } else if (OB_FAIL(new_server_ids.push_back(orig_server_ids.at(orig_server_ids.count() - 1)))) { // push max server_id.
      LOG_WARN("push server_id failed", KR(ret), K(orig_server_ids));
    } else if (OB_FAIL(cas_server_ids_in_palf_kv_(orig_server_ids, new_server_ids))) {
      LOG_WARN("cas server_id failed", KR(ret), K(orig_server_ids), K(new_server_ids));
    }
    PALF_KV_LOG_INFO("delete server id in palf kv", KR(ret), K(found), K(server_id), K(orig_server_ids), K(new_server_ids));
  }
  return ret;
}

// for upgrade and bootstrap.
int ObServerZoneOpService::store_server_ids_in_palf_kv(
    ServerIDArray &server_ids, // only server ids, not including timestamp.
    const uint64_t input_max_server_id)
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = GCONF.cluster_id;
  sslog::ObSSLogKVPalfAdapter palf_kv_adapter;
  ServerIDArray old_server_ids;
  ServerIDArray server_ids_timestamp;
  uint64_t old_max_server_id = OB_INVALID_ID;
  if (!GCTX.is_shared_storage_mode()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == input_max_server_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(input_max_server_id));
  } else if (OB_FAIL(palf_kv_adapter.init(cluster_id, OB_SYS_TENANT_ID))) {
    LOG_WARN("init palf kv adapter failed", KR(ret), K(cluster_id));
  } else if (OB_FAIL(get_server_ids_from_palf_kv_(old_server_ids))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      const int64_t now = common::ObTimeUtility::current_time();
      for (int64_t i = 0; OB_SUCC(ret) && i < server_ids.count(); i++) {
        if (OB_FAIL(server_ids_timestamp.push_back(server_ids.at(i)))) {
          LOG_WARN("fail to push back", KR(ret), K(server_ids));
        } else if (OB_FAIL(server_ids_timestamp.push_back(now))) {
          LOG_WARN("fail to push back", KR(ret), K(now));
        }
      } // end for
      if (FAILEDx(server_ids_timestamp.push_back(input_max_server_id))) {
        LOG_WARN("fail to push back max server id", KR(ret), K(input_max_server_id));
      } else if (OB_FAIL(insert_server_ids_in_palf_kv_(server_ids_timestamp))) {
        LOG_WARN("insert server_id in palf kv failed", KR(ret), K(server_ids_timestamp));
      }
      PALF_KV_LOG_INFO("store server ids in palf kv", KR(ret), K(server_ids_timestamp), K(server_ids), K(input_max_server_id));
    } else {
      LOG_WARN("fail to get server ids info", KR(ret));
    }
  } else {
    PALF_KV_LOG_INFO("server ids has exist in palf kv", KR(ret));
  }
  return ret;
}

int ObServerZoneOpService::generate_new_server_id_from_palf_kv(uint64_t &new_server_id)
{
  int ret = OB_SUCCESS;
  uint64_t orig_max_server_id = OB_INVALID_ID;
  ServerIDArray orig_server_ids;
  ServerIDArray orig_server_ids_with_ts;
  ServerIDArray new_server_ids_with_ts;
  const int64_t now = common::ObTimeUtility::current_time();
  if (!GCTX.is_shared_storage_mode()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported", KR(ret));
  } else if (OB_FAIL(get_server_infos_from_palf_kv(orig_server_ids, orig_server_ids_with_ts, orig_max_server_id))) {
    LOG_WARN("fail to get server zone op service", KR(ret));
  } else if (OB_FAIL(calculate_new_candidate_server_id(orig_server_ids,
                                                       orig_max_server_id + 1,
                                                       new_server_id))) {
    LOG_WARN("fail to get new candidate server id", KR(ret), K(orig_server_ids), K(orig_max_server_id));
  } else if (OB_INVALID_ID == new_server_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new candidate server id is invalid", KR(ret));
  } else if (OB_FAIL(new_server_ids_with_ts.assign(orig_server_ids_with_ts))) {
    LOG_WARN("fail to assign new server ids", KR(ret), K(orig_server_ids_with_ts));
  } else if (OB_FAIL(new_server_ids_with_ts.push_back(new_server_id))) { // new server_id
    LOG_WARN("fail to push back new server id", KR(ret), K(new_server_id));
  } else if (OB_FAIL(new_server_ids_with_ts.push_back(now))) { // new timestamp of server_id
    LOG_WARN("fail to push back new server id", KR(ret), K(now));
  } else if (OB_FAIL(new_server_ids_with_ts.push_back(new_server_id))) { // new max_server_id
    LOG_WARN("fail to push back max server id", KR(ret), K(new_server_id));
  } else if (OB_FAIL(orig_server_ids_with_ts.push_back(orig_max_server_id))) { // restore orig_max_server_id
    LOG_WARN("fail to push back orig max server id", KR(ret), K(orig_max_server_id));
  } else if (OB_FAIL(cas_server_ids_in_palf_kv_(orig_server_ids_with_ts, new_server_ids_with_ts))) {
    LOG_WARN("fail to cas server ids in palf kv", KR(ret), K(orig_server_ids_with_ts), K(new_server_ids_with_ts));
  }
  PALF_KV_LOG_INFO("generate new server id from palf_kv", KR(ret),
    K(new_server_id), K(orig_server_ids), K(orig_server_ids_with_ts), K(new_server_ids_with_ts));
  return ret;
}

int ObServerZoneOpService::store_max_unit_id_in_palf_kv(const uint64_t max_unit_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == max_unit_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(max_unit_id));
  } else if (!GCTX.is_shared_storage_mode()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support in non-shared storage mode", KR(ret), K(max_unit_id));
  } else {
    KEY_OF_RESOURCE_IN_PALF_KV(ObServerZoneOpService::PALF_KV_MAX_UNIT_ID_FORMAT_STR)
    if (FAILEDx(store_max_uint_in_palf_kv_(row_key, max_unit_id))) {
      LOG_WARN("store max_unit_id in palf kv failed", KR(ret), K(row_key), K(max_unit_id));
    }
    PALF_KV_LOG_INFO("store max_unit_id in palf_kv", KR(ret), K(row_key), K(max_unit_id));
  }
  return ret;
}

int ObServerZoneOpService::generate_new_unit_id_from_palf_kv(uint64_t &new_unit_id)
{
  int ret = OB_SUCCESS;
  uint64_t orig_max_unit_id = OB_INVALID_ID;
  KEY_OF_RESOURCE_IN_PALF_KV(ObServerZoneOpService::PALF_KV_MAX_UNIT_ID_FORMAT_STR)
  if (FAILEDx(get_uint_in_palf_kv_(row_key, orig_max_unit_id))) {
    LOG_WARN("fail to get max unit id in palf kv", KR(ret));
  } else if (orig_max_unit_id == OB_INVALID_ID) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orig_max_unit_id is invalid", KR(ret), K(orig_max_unit_id));
  } else if (FALSE_IT(new_unit_id = orig_max_unit_id + 1)) {
  } else if (OB_FAIL(cas_uint_in_palf_kv_(row_key, orig_max_unit_id, new_unit_id))) {
    LOG_WARN("cas uint in palf kv failed", KR(ret), K(row_key), K(orig_max_unit_id), K(new_unit_id));
  }
  PALF_KV_LOG_INFO("generate new unit_id from palf_kv", KR(ret), K(orig_max_unit_id), K(new_unit_id));
  return ret;
}

#define KEY_OF_DATA_VERSION_IN_PALF_KV                                                            \
  common::ObString row_key;                                                                       \
  const int64_t cluster_id = GCONF.cluster_id;                                                    \
  char row_key_buf[ObServerZoneOpService::MAX_ROW_KEY_LENGTH] = {'\0'};                           \
  int64_t pos = 0;                                                                                \
  if (FAILEDx(databuff_printf(row_key_buf,                                                        \
                              ObServerZoneOpService::MAX_ROW_KEY_LENGTH,                          \
                              pos,                                                                \
                              ObServerZoneOpService::PALF_KV_TENANT_DATA_VERSION_FORMAT_STR,      \
                              cluster_id,                                                         \
                              tenant_id))) {                                                      \
    LOG_WARN("failed to print rowkey", KR(ret), K(tenant_id), K(cluster_id));                     \
  } else if (FALSE_IT(row_key.assign_ptr(row_key_buf, pos))) {                                    \
  }                                                                                               \

int ObServerZoneOpService::store_data_version_in_palf_kv(
    const uint64_t tenant_id,
    const uint64_t data_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (!GCTX.is_shared_storage_mode()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support in non-shared storage mode", KR(ret));
  } else {
    KEY_OF_DATA_VERSION_IN_PALF_KV
    if (FAILEDx(store_max_uint_in_palf_kv_(row_key, data_version))) {
      LOG_WARN("store data_version in palf kv failed", KR(ret), K(row_key), KDV(data_version));
    }
    PALF_KV_LOG_INFO("store data_version in palf_kv", KR(ret), K(tenant_id), K(row_key), KDV(data_version));
  }
  return ret;
}

int ObServerZoneOpService::get_data_version_in_palf_kv(
    const uint64_t tenant_id,
    uint64_t &data_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (!GCTX.is_shared_storage_mode()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not in ss mode", KR(ret));
  } else {
    KEY_OF_DATA_VERSION_IN_PALF_KV
    if (FAILEDx(get_uint_in_palf_kv_(row_key, data_version))) {
      LOG_WARN("get resource_id in palf kv failed", KR(ret), K(row_key));
    }
    PALF_KV_LOG_INFO("get data_version in palf_kv", KR(ret), K(tenant_id), K(row_key), KDV(data_version));
  }
  return ret;
}

int ObServerZoneOpService::store_max_uint_in_palf_kv_(
    const common::ObString &row_key,
    const uint64_t max_uint)
{
  int ret = OB_SUCCESS;
  uint64_t orignal_uint = OB_INVALID_ID;
  if (OB_UNLIKELY(OB_INVALID_ID == max_uint || row_key.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(max_uint), K(row_key));
  } else if (OB_FAIL(get_uint_in_palf_kv_(row_key, orignal_uint))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      if (OB_FAIL(insert_uint_in_palf_kv_(row_key, max_uint))) {
        LOG_WARN("store server_id in palf kv failed", KR(ret), K(row_key), K(max_uint));
      }
    } else {
      LOG_WARN("get all resource id in palf_kv failed", KR(ret), K(row_key));
    }
  } else if (max_uint > orignal_uint) {
    LOG_INFO("small uint in palf_kv, need update", K(orignal_uint), K(max_uint));
    if (OB_FAIL(cas_uint_in_palf_kv_(row_key, orignal_uint, max_uint))) {
      LOG_WARN("store uint in palf kv failed", KR(ret), K(row_key), K(orignal_uint), K(max_uint));
    }
  } else {
    FLOG_INFO("orignal_uint in palf_kv is more bigger than max_uint, no need update");
  }
  PALF_KV_LOG_INFO("store max_uint in palf_kv", KR(ret), K(row_key), K(max_uint), K(orignal_uint));
  return ret;
}

#define SERIALIZE_RESOURCE_ID_FOR_PALF_KV(resource_id, buffer, str_var)                                                           \
  do {                                                                                                                            \
    if (OB_SUCC(ret)) {                                                                                                           \
      int64_t pos = 0;                                                                                                            \
      if (OB_FAIL(databuff_printf((buffer), MAX_UINT64_LEN, pos, "%lu", (resource_id)))) {                                        \
        LOG_WARN("failed to print id", KR(ret), K((resource_id)));                                                                \
      } else {                                                                                                                    \
        (str_var).assign_ptr((buffer), pos);                                                                                      \
      }                                                                                                                           \
    }                                                                                                                             \
  } while (0)                                                                                                                     \

int ObServerZoneOpService::cas_uint_in_palf_kv_(
    const common::ObString &row_key,
    const uint64_t orig_uint,
    const uint64_t new_uint)
{
  int ret = OB_SUCCESS;
  sslog::ObSSLogKVPalfAdapter palf_kv_adapter;
  const int64_t cluster_id = GCONF.cluster_id;
  if (OB_UNLIKELY(OB_INVALID_ID == orig_uint || OB_INVALID_ID == new_uint || row_key.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(orig_uint), K(new_uint), K(row_key));
  } else if (OB_FAIL(palf_kv_adapter.init(cluster_id, OB_SYS_TENANT_ID))) {
    LOG_WARN("init palf kv adapter failed", KR(ret), K(cluster_id));
  } else {
    bool expected = false;
    const int64_t MAX_UINT64_LEN = 128;
    common::ObString orig_uint_str;
    common::ObString new_uint_str;
    char orig_uint_char[MAX_UINT64_LEN] = {'\0'};
    char new_uint_char[MAX_UINT64_LEN] = {'\0'};
    SERIALIZE_RESOURCE_ID_FOR_PALF_KV(orig_uint, orig_uint_char, orig_uint_str);
    SERIALIZE_RESOURCE_ID_FOR_PALF_KV(new_uint, new_uint_char, new_uint_str);
    if (FAILEDx(palf_kv_adapter.cas(row_key, orig_uint_str, new_uint_str, expected))) {
      LOG_WARN("cas uint in palf kv failed", KR(ret), K(row_key), K(orig_uint_str), K(new_uint_str));
    } else if (!expected) {
      ret = OB_EAGAIN;
      LOG_WARN("cas uint in palf kv failed", KR(ret), K(row_key), K(orig_uint_str), K(new_uint_str));
    }
    PALF_KV_LOG_INFO("store uint in palf_kv", KR(ret), K(row_key), K(orig_uint), K(new_uint));
  }
  return ret;
}

int ObServerZoneOpService::insert_uint_in_palf_kv_(
    const common::ObString &row_key,
    const uint64_t uint_val)
{
  int ret = OB_SUCCESS;
  sslog::ObSSLogKVPalfAdapter palf_kv_adapter;
  const int64_t cluster_id = GCONF.cluster_id;
  if (OB_UNLIKELY(OB_INVALID_ID == uint_val || row_key.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(uint_val), K(row_key));
  } else if (OB_FAIL(palf_kv_adapter.init(cluster_id, OB_SYS_TENANT_ID))) {
    LOG_WARN("init palf kv adapter failed", KR(ret), K(cluster_id));
  } else {
    common::ObString uint_str;
    const int64_t MAX_UINT64_LEN = 128;
    char uint_char[MAX_UINT64_LEN] = {'\0'};
    SERIALIZE_RESOURCE_ID_FOR_PALF_KV(uint_val, uint_char, uint_str);
    if (FAILEDx(palf_kv_adapter.put(row_key, uint_str))) { // insert
      LOG_WARN("put uint_val into palf kv failed", KR(ret), K(row_key), K(uint_str));
    }
    PALF_KV_LOG_INFO("store uint_val in palf_kv", KR(ret), K(row_key), K(uint_str), K(uint_val));
  }
  return ret;
}

int ObServerZoneOpService::get_uint_in_palf_kv_(
    const common::ObString &row_key,
    uint64_t &uint_val)
{
  int ret = OB_SUCCESS;
  uint_val = OB_INVALID_ID;
  sslog::ObSSLogKVPalfAdapter palf_kv_adapter;
  const int64_t cluster_id = GCONF.cluster_id;
  ObTenantMutilAllocator allocator(OB_SYS_TENANT_ID);
  common::ObStringBuffer uint_str(&allocator);
  if (OB_UNLIKELY(row_key.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(row_key));
  } else if (OB_FAIL(palf_kv_adapter.init(cluster_id, OB_SYS_TENANT_ID))) {
    LOG_WARN("init palf kv adapter failed", KR(ret), K(cluster_id));
  } else if (OB_FAIL(palf_kv_adapter.get(row_key, uint_str))) {
    LOG_WARN("get row key from palf kv failed", KR(ret), K(row_key));
  } else if (OB_FAIL(trans_str_to_uint_(common::ObString(uint_str.length(), uint_str.ptr()), uint_val))) {
    LOG_WARN("trans str to uint failed", KR(ret), K(uint_str));
  }
  PALF_KV_LOG_INFO("get uint_val in palf_kv", KR(ret), K(row_key), K(uint_str), K(uint_val));
  return ret;
}

int ObServerZoneOpService::trans_str_to_uint_(
    const ObString &str_val,
    uint64_t &ret_val)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(str_val.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(str_val));
  } else {
    const int64_t MAX_UINT64_LEN = 128;
    char resource_id_char[MAX_UINT64_LEN] = {'\0'};
    int64_t copy_len = MIN(str_val.length(), MAX_UINT64_LEN);
    MEMCPY(resource_id_char, str_val.ptr(), copy_len);
    char *end_ptr = NULL;
    if (OB_FAIL(ob_strtoull(resource_id_char, end_ptr, ret_val))) {
      LOG_WARN("failed to trans str to uint", K(resource_id_char));
    }
  }
  PALF_KV_LOG_INFO("trans str to uint", KR(ret), K(str_val), K(ret_val));
  return ret;
}

#endif

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
  uint64_t sys_data_version = 0;
  if (GCTX.is_shared_storage_mode()) {
    if (OB_FAIL(GET_MIN_DATA_VERSION(OB_SYS_TENANT_ID, sys_data_version))) {
      LOG_WARN("failed to get sys tenant data version", KR(ret));
    } else if (sys_data_version >= DATA_VERSION_4_4_1_0) {
      if (OB_FAIL(fetch_new_server_id_for_ss_(server_id))) {
        LOG_WARN("fail to fetch new server id", KR(ret));
      }
    } else {
      ret= OB_NOT_SUPPORTED;
      LOG_WARN("not support add server when sys_data_version < 4410 in ss", KR(ret), KDV(sys_data_version));
    }
  } else if (OB_FAIL(fetch_new_server_id_for_sn_(server_id))) {
    LOG_WARN("fail to fetch new server id", KR(ret));
  }
  return ret;
}

int ObServerZoneOpService::fetch_new_server_id_for_ss_(uint64_t &server_id)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_SHARED_STORAGE
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(is_inited_));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid sql proxy", KR(ret), KP(sql_proxy_));
  } else {
    ObMaxIdFetcher id_fetcher(*sql_proxy_);
    uint64_t old_server_id = OB_INVALID_ID;
    if (OB_FAIL(generate_new_server_id_from_palf_kv(server_id))) {
      LOG_WARN("fail to generate new server id", KR(ret));
    } else if (OB_FAIL(id_fetcher.fetch_max_id(*sql_proxy_,
                                               OB_SYS_TENANT_ID,
                                               OB_MAX_USED_SERVER_ID_TYPE,
                                               old_server_id))) {
      LOG_WARN("fetch_new server_id failed", KR(ret));
    } else if (server_id <= old_server_id) {
      LOG_INFO("ald server_id > new server_id, no need update", K(old_server_id), K(server_id));
    } else if (OB_FAIL(id_fetcher.update_server_max_id(old_server_id, server_id))) {
      LOG_WARN("fail to update server max id", KR(ret), K(old_server_id));
    }
  }
#endif
  return ret;
}

int ObServerZoneOpService::fetch_new_server_id_for_sn_(uint64_t &server_id)
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
    uint64_t new_candidate_server_id = OB_INVALID_ID;
    ObMaxIdFetcher id_fetcher(*sql_proxy_);
    if (OB_FAIL(id_fetcher.fetch_new_max_id(
        OB_SYS_TENANT_ID,
        OB_MAX_USED_SERVER_ID_TYPE,
        candidate_server_id))) {
      LOG_WARN("fetch_new_max_id failed", KR(ret));
    } else if (OB_FAIL(calculate_new_candidate_server_id(server_id_in_cluster, candidate_server_id, new_candidate_server_id))) {
      LOG_WARN("fail to get new candidate server id", KR(ret), K(server_id_in_cluster));
    } else if (new_candidate_server_id != candidate_server_id) {
      if (OB_FAIL(id_fetcher.update_server_max_id(candidate_server_id, new_candidate_server_id))) {
        LOG_WARN("fail to update server max id", KR(ret), K(candidate_server_id), K(new_candidate_server_id));
      }
    }
    if (OB_SUCC(ret)) {
      server_id = new_candidate_server_id;
      LOG_INFO("[FETCH NEW SERVER ID] new candidate server id", K(server_id), K(server_id_in_cluster));
    }
  }
  return ret;
}

int ObServerZoneOpService::calculate_new_candidate_server_id(
    const common::ObIArray<uint64_t> &server_id_in_cluster,
    const uint64_t candidate_server_id,
    uint64_t &new_candidate_server_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == candidate_server_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(candidate_server_id));
  } else {
    new_candidate_server_id = candidate_server_id;
    while (!check_server_index(new_candidate_server_id, server_id_in_cluster)) {
      if (new_candidate_server_id % 10 == 0) {
        LOG_INFO("[FETCH NEW SERVER ID] periodical log", K(new_candidate_server_id), K(server_id_in_cluster));
      }
      ++new_candidate_server_id;
    }
  }
  return ret;
}

bool ObServerZoneOpService::check_server_index(
    const uint64_t candidate_server_id,
    const common::ObIArray<uint64_t> &server_id_in_cluster)
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
  bool allow_broadcast = true;
  if (OB_TMP_FAIL(SVR_TRACER.refresh(allow_broadcast))) {
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
