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

#include "ob_service_name_command.h"

#include "share/ob_all_server_tracer.h"
#include "rootserver/ob_rs_async_rpc_proxy.h"
#include "src/rootserver/ob_root_utils.h"
#include "rootserver/ob_tenant_event_def.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::tenant_event;
namespace oceanbase
{
namespace rootserver
{
int ObServiceNameKillSessionFunctor::init(
    const uint64_t tenant_id,
    const share::ObServiceNameString &service_name,
    ObArray<uint64_t> *killed_connection_list)
{
  int ret = OB_SUCCESS;
  if (!is_valid_tenant_id(tenant_id) || !service_name.is_valid() || OB_ISNULL(killed_connection_list)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(tenant_id), K(service_name), K(killed_connection_list));
  } else if (OB_FAIL(service_name_.assign(service_name))) {
    LOG_WARN("fail to assign service_name", KR(ret), K(service_name));
  }
  else {
    tenant_id_ = tenant_id;
    killed_connection_list_ = killed_connection_list;
    killed_connection_list_->reset();
  }
  return ret;
}
bool ObServiceNameKillSessionFunctor::operator()(sql::ObSQLSessionMgr::Key key, sql::ObSQLSessionInfo *sess_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sess_info) || OB_ISNULL(GCTX.session_mgr_) || OB_ISNULL(killed_connection_list_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", KR(ret), KP(sess_info), KP(GCTX.session_mgr_),
        KP(killed_connection_list_));
  } else if (sess_info->get_effective_tenant_id() == tenant_id_
      && sess_info->get_service_name().equal_to(service_name_)) {
    uint64_t sess_id = sess_info->get_sessid();
    if (OB_FAIL(GCTX.session_mgr_->kill_session(*sess_info))) {
      LOG_WARN("fail to kill session", KR(ret), K(sess_id));
    } else if (OB_FAIL(killed_connection_list_->push_back(sess_id))) {
      LOG_WARN("fail to push back", KR(ret), K(sess_id));
    }
  }
  return OB_SUCCESS == ret;
}

int ObServiceNameCommand::create_service(
    const uint64_t tenant_id,
    const ObServiceNameString &service_name_str)
{
  int ret = OB_SUCCESS;
  ObArray<ObAddr> target_servers;
  ObServiceName service_name;
  int64_t epoch = 0;
  ObArray<ObServiceName> all_service_names;
  int64_t begin_ts = ObTimeUtility::current_time();
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !service_name_str.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(tenant_id), K(service_name_str));
  } else if (OB_FAIL(check_and_get_tenants_servers_(tenant_id, true /* include_temp_offline */, target_servers))) {
    LOG_WARN("fail to execute check_and_get_tenants_online_servers_", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObServiceNameProxy::insert_service_name(tenant_id, service_name_str,
      epoch, all_service_names))) {
    // insert service_name into __all_service if the tenant's so_status is NORMAL
    LOG_WARN("fail to insert service_name", KR(ret), K(tenant_id), K(service_name_str));
  } else if (OB_FAIL(extract_service_name_(all_service_names, service_name_str, service_name))) {
    LOG_WARN("fail to execute extract_service_name_", KR(ret), K(all_service_names), K(service_name_str));
    if (OB_SERVICE_NAME_NOT_FOUND == ret) {
      ret = OB_ERR_UNEXPECTED;
    }
  } else if (OB_FAIL(broadcast_refresh_(
      tenant_id,
      service_name.get_service_name_id(),
      ObServiceNameArg::CREATE_SERVICE,
      target_servers,
      epoch,
      all_service_names))) {
    LOG_WARN("fail to broadcast", KR(ret), K(tenant_id), K(service_name), K(target_servers),
        K(epoch), K(all_service_names));
  }
  int64_t end_ts = ObTimeUtility::current_time();
  TENANT_EVENT(tenant_id, SERVICE_NAME, CREATE_SERVICE, end_ts, ret, end_ts - begin_ts,
      service_name_str.ptr(), service_name);
  return ret;
}
int ObServiceNameCommand::delete_service(
    const uint64_t tenant_id,
    const ObServiceNameString &service_name_str)
{
  int ret = OB_SUCCESS;
  ObServiceName service_name;
  int64_t begin_ts = ObTimeUtility::current_time();
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !service_name_str.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(tenant_id), K(service_name_str));
  } else if (OB_FAIL(ObServiceNameProxy::select_service_name(*GCTX.sql_proxy_, tenant_id, service_name_str, service_name))) {
    LOG_WARN("fail to select service_name", KR(ret), K(tenant_id), K(service_name_str));
  } else if (!service_name.is_stopped()) {
    // simple check at first
    // status will be checked again when the service_name is removed from the table
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("service_status is not STOPPED, delete_service is not allowed", KR(ret), K(service_name));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "The service_status is not STOPPED, DELETE SERVICE is");
  } else if (OB_FAIL(ObServiceNameProxy::delete_service_name(service_name))) {
    LOG_WARN("fail to delete service_name", KR(ret), K(tenant_id), K(service_name));
  }
  int64_t end_ts = ObTimeUtility::current_time();
  TENANT_EVENT(tenant_id, SERVICE_NAME, DELETE_SERVICE, end_ts, ret, end_ts - begin_ts, service_name);
  return ret;
}
int ObServiceNameCommand::start_service(
    const uint64_t tenant_id,
    const ObServiceNameString &service_name_str)
{
  int ret = OB_SUCCESS;
  ObArray<ObAddr> target_servers;
  ObServiceName service_name;
  ObServiceName service_name_before;
  int64_t epoch = 0;
  ObArray<ObServiceName> all_service_names;
  int64_t begin_ts = ObTimeUtility::current_time();
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !service_name_str.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(tenant_id), K(service_name_str));
  } else if (OB_FAIL(check_and_get_tenants_servers_(tenant_id, true /* include_temp_offline */, target_servers))) {
    LOG_WARN("fail to execute check_and_get_tenants_servers_", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObServiceNameProxy::select_all_service_names_with_epoch(tenant_id, epoch, all_service_names))) {
    LOG_WARN("fail to select service_name", KR(ret), K(tenant_id), K(service_name_str));
  } else if (OB_FAIL(extract_service_name_(all_service_names, service_name_str, service_name))) {
    LOG_WARN("fail to execute extract_service_name_", KR(ret), K(all_service_names), K(service_name_str));
  } else if (OB_FAIL(service_name_before.assign(service_name))) {
    LOG_WARN("fail to assign service_name_before", KR(ret), K(service_name));
  } else if (!service_name.is_started()) {
    if (OB_FAIL(ObServiceNameProxy::update_service_status(service_name, ObServiceName::STARTED,
      epoch, all_service_names))) {
      LOG_WARN("fail to update service_status", KR(ret), K(tenant_id), K(service_name));
    } else if (OB_FAIL(extract_service_name_(all_service_names, service_name_str, service_name))) {
      LOG_WARN("fail to execute extract_service_name_", KR(ret), K(all_service_names), K(service_name_str));
      if (OB_SERVICE_NAME_NOT_FOUND == ret) {
        ret = OB_ERR_UNEXPECTED;
      }
    }
  }
  if (FAILEDx(broadcast_refresh_(
      tenant_id,
      service_name.get_service_name_id(),
      ObServiceNameArg::START_SERVICE,
      target_servers,
      epoch,
      all_service_names))) {
    LOG_WARN("fail to broadcast", KR(ret), K(tenant_id), K(service_name), K(target_servers),
        K(epoch), K(all_service_names));
  }
  int64_t end_ts = ObTimeUtility::current_time();
  TENANT_EVENT(tenant_id, SERVICE_NAME, START_SERVICE, end_ts, ret, end_ts - begin_ts, service_name_before, service_name);
  return ret;
}
int ObServiceNameCommand::stop_service(
    const uint64_t tenant_id,
    const ObServiceNameString &service_name_str)
{
  int ret = OB_SUCCESS;
  ObArray<ObAddr> tenant_online_servers;
  ObServiceName service_name;
  ObServiceName service_name_before;
  int64_t epoch = 0;
  ObArray<ObServiceName> all_service_names;
  int64_t begin_ts = ObTimeUtility::current_time();
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !service_name_str.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(tenant_id), K(service_name_str));
  } else if (OB_FAIL(ObServiceNameProxy::select_all_service_names_with_epoch(tenant_id, epoch, all_service_names))) {
    LOG_WARN("fail to select service_name", KR(ret), K(tenant_id), K(service_name_str));
  } else if (OB_FAIL(extract_service_name_(all_service_names, service_name_str, service_name))) {
    LOG_WARN("fail to execute extract_service_name_", KR(ret), K(all_service_names), K(service_name_str));
  } else if (OB_FAIL(service_name_before.assign(service_name))) {
    LOG_WARN("fail to assign service_name_before", KR(ret), K(service_name));
  } else if (service_name.is_stopped()) {
    // status has been already stopped, do nothing
  } else if (OB_FAIL(check_and_get_tenants_servers_(tenant_id, false /* include_temp_offline */, tenant_online_servers))) {
    // also ensure the tenant has no units on temp. offline servers
    LOG_WARN("fail to execute check_and_get_tenants_online_servers_", KR(ret), K(tenant_id));
  } else {
    if (service_name.is_started()) {
      if (OB_FAIL(ObServiceNameProxy::update_service_status(service_name, ObServiceName::STOPPING,
        epoch, all_service_names))) {
        LOG_WARN("fail to update service_status", KR(ret), K(tenant_id), K(service_name));
      } else if (OB_FAIL(extract_service_name_(all_service_names, service_name_str, service_name))) {
        LOG_WARN("fail to execute extract_service_name_", KR(ret), K(all_service_names), K(service_name_str));
        if (OB_SERVICE_NAME_NOT_FOUND == ret) {
          ret = OB_ERR_UNEXPECTED;
        }
      }
    }
    if (FAILEDx(broadcast_refresh_(
        tenant_id,
        service_name.get_service_name_id(),
        ObServiceNameArg::STOP_SERVICE,
        tenant_online_servers,
        epoch,
        all_service_names))) {
      LOG_WARN("fail to broadcast", KR(ret), K(tenant_id), K(service_name), K(tenant_online_servers),
          K(epoch), K(all_service_names));
    } else if (OB_FAIL(ObServiceNameProxy::update_service_status(service_name, ObServiceName::STOPPED,
      epoch, all_service_names))) {
      LOG_WARN("fail to update service_status", KR(ret), K(tenant_id), K(service_name));
    } else if (OB_FAIL(extract_service_name_(all_service_names, service_name_str, service_name))) {
      LOG_WARN("fail to execute extract_service_name_", KR(ret), K(all_service_names), K(service_name_str));
      if (OB_SERVICE_NAME_NOT_FOUND == ret) {
        ret = OB_ERR_UNEXPECTED;
      }
    }
  }
  int64_t end_ts = ObTimeUtility::current_time();
  TENANT_EVENT(tenant_id, SERVICE_NAME, STOP_SERVICE, end_ts, ret, end_ts - begin_ts, service_name_before, service_name);
  return ret;
}

int ObServiceNameCommand::kill_local_connections(
    const uint64_t tenant_id,
    const share::ObServiceName &service_name)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> killed_connection_list;
  int64_t begin_ts = ObTimeUtility::current_time();
  ObServiceNameKillSessionFunctor kill_session_functor;
  const ObServiceNameString & service_name_str = service_name.get_service_name_str();
  if (OB_ISNULL(GCTX.session_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.session_mgr_ is null", KR(ret), KP(GCTX.session_mgr_));
  } else if (OB_FAIL(kill_session_functor.init(
      tenant_id,
      service_name_str,
      &killed_connection_list))) {
    LOG_WARN("fail to init kill_session_functor", KR(ret), K(tenant_id), K(service_name_str));
  } else if (OB_FAIL(GCTX.session_mgr_->for_each_session(kill_session_functor))) {
    LOG_WARN("fail to kill local sessions", KR(ret));
  }
  if (killed_connection_list.count() > 0) {
    int64_t end_ts = ObTimeUtility::current_time();
    TENANT_EVENT(tenant_id, SERVICE_NAME, KILL_CONNECTIONS_OF_SERVICE_NAME, end_ts, ret, end_ts - begin_ts,
        service_name, killed_connection_list.count(), killed_connection_list);
  }
  return ret;
}

int ObServiceNameCommand::check_and_get_tenants_servers_(
    const uint64_t tenant_id,
    const bool include_temp_offline,
    common::ObIArray<common::ObAddr> &target_servers)
{
  int ret = OB_SUCCESS;
  ObArray<ObUnit> units;
  target_servers.reset();
  ObUnitTableOperator unit_operator;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(unit_operator.init(*GCTX.sql_proxy_))) {
    LOG_WARN("failed to init unit operator", KR(ret));
  } else if (OB_FAIL(unit_operator.get_units_by_tenant(tenant_id, units))) {
    LOG_WARN("failed to get tenant unit", KR(ret), K(tenant_id));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < units.count(); i++) {
    const ObUnit &unit = units.at(i);
    if (OB_FAIL(server_check_and_push_back_(unit.server_, include_temp_offline, target_servers))) {
      LOG_WARN("fail to execute server_check_and_push_back_", KR(ret), K(unit.server_));
    } else if (unit.migrate_from_server_.is_valid() &&
        OB_FAIL(server_check_and_push_back_(unit.migrate_from_server_, include_temp_offline, target_servers))) {
      LOG_WARN("fail to execute server_check_and_push_back_", KR(ret), K(unit.migrate_from_server_));
    }
  }
  return ret;
}

int ObServiceNameCommand::server_check_and_push_back_(
    const common::ObAddr &server,
    const bool include_temp_offline,
    common::ObIArray<common::ObAddr> &target_servers)
{
  int ret = OB_SUCCESS;
  ObServerInfoInTable server_info;
  if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", KR(ret), K(server));
  } else if (OB_FAIL(SVR_TRACER.get_server_info(server, server_info))) {
    LOG_WARN("fail to execute get_server_info", KR(ret), K(server));
  } else if (server_info.is_permanent_offline()) {
    // skip
  } else if (!include_temp_offline && server_info.is_temporary_offline()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("the tenant has units on temporary offline servers", KR(ret), K(server_info));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "The tenant has units on temporary offline servers, STOP SERVICE is");
  } else if (OB_FAIL(target_servers.push_back(server))) {
    LOG_WARN("fail to push back", KR(ret), K(server), K(server_info));
  }
  return ret;
}

int ObServiceNameCommand::broadcast_refresh_(
    const uint64_t tenant_id,
    const share::ObServiceNameID &target_service_name_id,
    const share::ObServiceNameArg::ObServiceOp &service_op,
    const common::ObIArray<common::ObAddr> &target_servers,
    const int64_t epoch,
    const ObArray<share::ObServiceName> &all_service_names)
{
  // 1. Broadcasting to all servers restricts executing commands related to service_name
  //    when any server is permanently offline.
  // 2. Users are required to ensure that processes on permanently offline servers are terminated.
  // 3. Broadcasts target only online servers, following verification that no servers are temporarily offline.
  int ret = OB_SUCCESS;
  const ObAddr &from_server = GCTX.self_addr();
  ObTimeoutCtx ctx;
  ObArray<int> return_code_array;
  obrpc::ObRefreshServiceNameArg arg;
  share::ObAllTenantInfo tenant_info;
  int64_t ora_rowscn = 0;
  common::ObArray<common::ObAddr> success_servers;
  int64_t begin_ts = ObTimeUtility::current_time();
  if (OB_ISNULL(GCTX.srv_rpc_proxy_) || OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr", KR(ret), KP(GCTX.srv_rpc_proxy_), KP(GCTX.sql_proxy_));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    // the validity of epoch and all_service_names will be checked when we init arg
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(tenant_id));
  } else if (0 == target_servers.count()) {
    LOG_INFO("no target servers, no need to broadcast", KR(ret), K(tenant_id),
        K(all_service_names), K(target_servers));
  } else if (ObServiceNameArg::START_SERVICE == service_op &&
      OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(tenant_id, GCTX.sql_proxy_, false, ora_rowscn, tenant_info))) {
    // When starting the service, it is expected that `service_name` is utilized.
    // However, the ability for users to connect via `service_name` also depends on `tenant_info`,
    // so it's crucial to ensure that `tenant_info` is up-to-date.
    LOG_WARN("fail to load tenant info", KR(ret), K(tenant_id));
  } else if (OB_FAIL(arg.init(tenant_id, epoch, from_server, target_service_name_id,
      all_service_names, service_op, tenant_info, ora_rowscn))) {
    LOG_WARN("failed to init arg", KR(ret), K(tenant_id), K(from_server), K(target_service_name_id),
        K(all_service_names), K(service_op), K(tenant_info), K(ora_rowscn));
  } else {
    const uint64_t group_id = share::OBCG_DBA_COMMAND;
    ObRefreshServiceNameProxy proxy(*GCTX.srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::refresh_service_name);
    int tmp_ret = OB_SUCCESS;
    // Try to send to all target servers, but return failure if at least one fails.
    // The intention is to maximize the number of observers aware of the STARTED status when starting the service.
    for (int64_t i = 0; OB_SUCC(ret) && i < target_servers.count(); i++) {
      const ObAddr &server = target_servers.at(i);
      if (OB_FAIL(ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
        LOG_WARN("fail to get timeout ctx", KR(ret), K(ctx));
      } else if (OB_TMP_FAIL(proxy.call(server, ctx.get_timeout(), GCONF.cluster_id, tenant_id, group_id, arg))) {
        LOG_WARN("failed to send rpc", KR(ret), KR(tmp_ret), K(server), K(ctx), K(tenant_id), K(arg));
      }
    }

    if (OB_TMP_FAIL(proxy.wait_all(return_code_array))) {
      LOG_WARN("wait all batch result failed", KR(ret), KR(tmp_ret));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }
    int first_ret = OB_SUCCESS;
    if (FAILEDx(proxy.check_return_cnt(return_code_array.count()))) {
      LOG_WARN("fail to check return cnt", KR(ret), "return_cnt", return_code_array.count());
    }
    ARRAY_FOREACH_X(proxy.get_results(), idx, cnt, OB_SUCC(ret)) {
      const obrpc::ObRefreshServiceNameRes *result = proxy.get_results().at(idx);
      const ObAddr &dest_addr = proxy.get_dests().at(idx);
      tmp_ret = return_code_array.at(idx);
      if (OB_TENANT_NOT_EXIST == tmp_ret) {
        LOG_WARN("tenant not exist", KR(ret), KR(tmp_ret), K(dest_addr));
        tmp_ret = OB_SUCCESS;
      }
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("fail to send rpc", KR(ret), KR(tmp_ret), K(dest_addr), K(idx));
      } else if (OB_ISNULL(result)) {
        tmp_ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", KR(ret), KR(tmp_ret), KR(first_ret), KP(result));
      } else if (OB_UNLIKELY(!result->is_valid())) {
        tmp_ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid result",
            KR(ret), KR(tmp_ret), KR(first_ret), KPC(result), K(dest_addr));
      } else {
        LOG_INFO("refresh_service_name success", KR(ret), KR(tmp_ret), KR(first_ret), K(dest_addr), K(arg), KPC(result));
        if (OB_FAIL(success_servers.push_back(dest_addr))) {
          LOG_WARN("fail to push back", KR(ret), K(dest_addr));
        }
      }
      first_ret = OB_SUCCESS == first_ret ? tmp_ret : first_ret;
    }
    ret = OB_SUCCESS == ret ? first_ret : ret;

    if (OB_FAIL(ret)) {
      int prev_ret = ret;
      ret = ObServiceNameArg::STOP_SERVICE == service_op ? OB_NEED_RETRY : OB_SERVICE_NOT_FULLY_STARTED;
      LOG_WARN("fail to broadcast to the tenant's all servers", KR(ret), KR(prev_ret),
          K(target_servers), K(success_servers), K(service_op));
    }
    int64_t end_ts = ObTimeUtility::current_time();
    TENANT_EVENT(tenant_id, SERVICE_NAME, BROADCAST_SERVICE_NAME, end_ts, ret, end_ts - begin_ts,
        epoch, target_service_name_id, all_service_names, ObServiceNameArg::service_op_to_str(service_op),
        target_servers, success_servers);
  }
  return ret;
}

int ObServiceNameCommand::extract_service_name_(
    const ObArray<share::ObServiceName> &all_service_names,
    const share::ObServiceNameString &service_name_str,
    share::ObServiceName &service_name)
{
  int ret = OB_SUCCESS;
  bool is_found = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < all_service_names.count() && !is_found; ++i) {
    if (all_service_names.at(i).get_service_name_str().equal_to(service_name_str)) {
      is_found = true;
      if (OB_FAIL(service_name.assign(all_service_names.at(i)))) {
        LOG_WARN("fail to assign service_name", KR(ret), K(all_service_names.at(i)));
      }
    }
  }
  if (OB_SUCC(ret) && !is_found) {
    ret = OB_SERVICE_NAME_NOT_FOUND;
    LOG_WARN("service_name_str is not found", KR(ret), K(service_name_str), K(all_service_names));
  }
  return ret;
}
} // end namespace rootserver
} // end namespace oceanbase