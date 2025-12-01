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

#define USING_LOG_PREFIX RS

#include "ob_admin_switch_replica_role.h"
#include "common/ob_role.h"
#include "lib/allocator/page_arena.h"
#include "lib/net/ob_addr.h"
#include "lib/ob_define.h"
#include "share/ob_server_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/ls/ob_ls_table_operator.h"
#include "share/ls/ob_ls_table.h"
#include "share/ob_all_server_tracer.h"
#include "share/ob_server_table_operator.h"
#include "rootserver/ob_root_utils.h"
#include "logservice/leader_coordinator/table_accessor.h"

namespace oceanbase
{
using namespace obrpc;
namespace rootserver
{

int ObAdminSwitchReplicaRole::handle_switch_replica_role_sql_command(
    const ObAdminSwitchReplicaRoleArg &arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("execute switch replica role request", K(arg));
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg));
  } else if (OB_FAIL(get_tenant_id_by_name_(arg, tenant_id))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_USER_ERROR(OB_ENTRY_NOT_EXIST, "invalid tenant");
    }
    LOG_WARN("fail to convert tenant name to id", K(arg), KR(ret));
  } else if (OB_FAIL(execute_switch_replica_role_(arg, tenant_id))) {
    LOG_WARN("fail to execute switch replica role core", K(arg), KR(ret));
  }
  LOG_INFO("switch leader done", KR(ret), K(arg), K(tenant_id));
  return ret;
}

int ObAdminSwitchReplicaRole::handle_switch_replica_role_obadmin_command(
    const ObAdminSwitchReplicaRoleStr &command_arg_str)
{
  int ret = OB_SUCCESS;
  ObAdminSwitchReplicaRoleArg arg;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  if (OB_UNLIKELY(!command_arg_str.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(command_arg_str));
  } else if (OB_FAIL(parse_params_from_obadmin_command_arg(command_arg_str, arg, tenant_id))) {
    LOG_WARN("fail to parse parameters from command", KR(ret), K(command_arg_str));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg));
  } else if (OB_FAIL(execute_switch_replica_role_(arg, tenant_id))) {
    LOG_WARN("fail to execute switch replica role", KR(ret), K(arg));
  }
  return ret;
}

int ObAdminSwitchReplicaRole::execute_switch_replica_role_(
    const ObAdminSwitchReplicaRoleArg &arg,
    const uint64_t &tenant_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLSInfo ls_info;
  const share::ObLSID ls_id(arg.ls_id_);
  if (OB_ISNULL(GCTX.srv_rpc_proxy_) || OB_ISNULL(GCTX.lst_operator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("srv_rpc_proxy_ or lst_operator_ is NULL", KR(ret), KP(GCTX.srv_rpc_proxy_), KP(GCTX.lst_operator_));
  } else if (OB_UNLIKELY(!arg.is_valid()
                      || !ls_id.is_valid_with_tenant(tenant_id)
                      || !is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id), K(arg));
  } else if (OB_FAIL(check_server_valid_(arg))) {
    LOG_WARN("check server valid state failed", K(arg), KR(ret));
  } else if (OB_FAIL(update_ls_election_reference_info_table_(arg, tenant_id))) {
    LOG_WARN("fail to update ls election reference info", K(arg), KR(ret), K(tenant_id));
  } else if (OB_FAIL(GCTX.lst_operator_->get(GCONF.cluster_id, tenant_id,
                     ls_id, share::ObLSTable::DEFAULT_MODE, ls_info))) {
    LOG_WARN("get ls info from GCTX.lst_operator_ failed", K(arg), KR(ret), K(tenant_id));
  } else if (OB_TMP_FAIL(ObRootUtils::try_notify_switch_ls_leader(GCTX.srv_rpc_proxy_, ls_info,
          obrpc::ObNotifySwitchLeaderArg::SwitchLeaderComment::MANUAL_SWITCH))) {
    LOG_WARN("failed to notify switch ls leader", KR(ret), K(ls_info));
  }
  return ret;
}

int ObAdminSwitchReplicaRole::get_tenant_id_by_name_(
    const ObAdminSwitchReplicaRoleArg &arg,
    uint64_t &tenant_id)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", KR(ret));
  } else if (OB_UNLIKELY(arg.tenant_name_.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant name is empty", KR(ret), K(arg));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("get schema manager failed", KR(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_id(arg.tenant_name_.str(), tenant_id)) || !is_valid_tenant_id(tenant_id)) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("tenant not exist", KR(ret), K(arg));
  }
  return ret;
}

int ObAdminSwitchReplicaRole::check_server_valid_(
    const ObAdminSwitchReplicaRoleArg &arg)
{
  int ret = OB_SUCCESS;
  ObServerInfoInTable server_info;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg));
  } else if (OB_FAIL(SVR_TRACER.get_server_info(arg.server_, server_info))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_USER_ERROR(OB_ENTRY_NOT_EXIST, "server not in cluster");
      LOG_WARN("server not in cluster", KR(ret), K(arg));
    } else {
      LOG_WARN("fail to get server info", KR(ret), K(arg));
    }
  } else if (common::LEADER != arg.role_) {
    // skip check server status
  } else if (!server_info.is_active()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "server not active which is");
    LOG_WARN("server not active", KR(ret), K(arg), K(server_info));
  }
  return ret;
}

int ObAdminSwitchReplicaRole::update_ls_election_reference_info_table_(
    const ObAdminSwitchReplicaRoleArg &arg,
    const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg));
  } else if (arg.role_ == ObRole::LEADER) {
    logservice::coordinator::LsElectionReferenceInfoRow row(tenant_id, share::ObLSID(arg.ls_id_));
    if (OB_FAIL(row.change_manual_leader(arg.server_))) {
      LOG_WARN("fail to change manual leader in __all_ls_election_reference_info", K(ret), K(arg));
    } else {
      LOG_INFO("successfully to change manual leader in __all_ls_election_reference_info", K(ret), K(arg));
    }
  } else if (arg.role_ == ObRole::FOLLOWER) {
    logservice::coordinator::LsElectionReferenceInfoRow row(tenant_id, share::ObLSID(arg.ls_id_));
    if (OB_FAIL(row.add_server_to_blacklist(arg.server_, logservice::coordinator::InsertElectionBlacklistReason::SWITCH_REPLICA))) {
      LOG_WARN("fail to add remove member info in __all_ls_election_reference_info", K(ret), K(arg));
    } else {
      LOG_INFO("successfully to add remove member info in __all_ls_election_reference_info", K(ret), K(arg));
    }
  } else if (arg.role_ == ObRole::INVALID_ROLE) {
    logservice::coordinator::LsElectionReferenceInfoRow row(tenant_id, share::ObLSID(arg.ls_id_));
    if (OB_FAIL(row.change_manual_leader(ObAddr()))) {
      LOG_WARN("fail to change manual leader in __all_ls_election_reference_info", K(ret), K(arg));
    } else if (OB_FAIL(row.delete_server_from_blacklist(arg.server_))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("fail to del remove member info in __all_ls_election_reference_info", K(ret), K(arg));
      } else {
        ret = OB_SUCCESS;
      }
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("successfully to reset server status in __all_ls_election_reference_info", K(ret), K(arg));
    }
  }
  return ret;
}

int ObAdminSwitchReplicaRole::parse_params_from_obadmin_command_arg(
    const ObAdminSwitchReplicaRoleStr &command_arg_str,
    ObAdminSwitchReplicaRoleArg &command_arg,
    uint64_t &tenant_id)
{
  int ret = OB_SUCCESS;
  tenant_id = OB_INVALID_TENANT_ID;
  int64_t ls_id = share::ObLSID::INVALID_LS_ID;
  common::ObAddr target_server;
  common::ObRole role = ObRole::INVALID_ROLE; // invalid role means default mode
  bool is_valid_role = false;
  LOG_INFO("parse params from obadmin command arg", K(command_arg_str));
  ObArenaAllocator allocator("SwitchRole");
  ObString admin_command_before_trim;
  ObString admin_command_after_trim;
  ObArray<ObString> command_params_array;
  if (OB_UNLIKELY(!command_arg_str.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(command_arg_str));
  } else if (OB_FAIL(ob_write_string(allocator, command_arg_str.get_admin_command_str(), admin_command_before_trim))) {
    LOG_WARN("fail to write string", KR(ret), K(command_arg_str));
  } else if (FALSE_IT(admin_command_after_trim = admin_command_before_trim.trim())) {
  } else if (OB_FAIL(split_on(admin_command_after_trim, ',', command_params_array))) {
    LOG_WARN("fail to split string", KR(ret), K(admin_command_after_trim), K(admin_command_before_trim));
  } else if (command_params_array.count() != ObAdminSwitchReplicaRole::OB_ADMIN_SWITCH_REPLICA_ROLE_ARG_COUNT) { // 4
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(command_arg_str), K(command_params_array));
  } else {
    for (int64_t param_index = 0; param_index < command_params_array.count() && OB_SUCC(ret); param_index++) {
      ObString param_name_with_value_str = command_params_array.at(param_index);
      ObArray<ObString> param_name_with_value;
      ObSqlString param_name;
      ObSqlString param_value;
      int64_t pos = 0;
      if (OB_FAIL(split_on(param_name_with_value_str, '=', param_name_with_value))) {
        LOG_WARN("fail to split param name and value", KR(ret), K(param_name_with_value_str));
      } else if (OB_UNLIKELY(2 != param_name_with_value.count())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", KR(ret), K(param_name_with_value));
      } else if (OB_FAIL(param_name.assign(param_name_with_value.at(0).trim()))) {
        LOG_WARN("fail to construct parameter name", KR(ret), K(param_name_with_value));
      } else if (OB_FAIL(param_value.assign(param_name_with_value.at(1).trim()))) {
        LOG_WARN("fail to construct parameter value", KR(ret), K(param_name_with_value));
      } else if (0 == param_name.string().case_compare("tenant_id")) {
        int64_t tmp_tenant_id = OB_INVALID_TENANT_ID;
        if (OB_FAIL(extract_int(param_value.string(), 0, pos, tmp_tenant_id))) {
          LOG_WARN("fail to extract int from string", KR(ret), K(param_value.string()), K(tmp_tenant_id));
        } else {
          tenant_id = static_cast<uint64_t>(tmp_tenant_id);
        }
      } else if (0 == param_name.string().case_compare("ls_id")) {
        if (OB_FAIL(extract_int(param_value.string(), 0, pos, ls_id))) {
          LOG_WARN("fail to extract int from string", KR(ret), K(param_value.string()), K(ls_id));
        }
      } else if (0 == param_name.string().case_compare("server")) {
        if (OB_FAIL(target_server.parse_from_string(param_value.string()))) {
          LOG_WARN("fail to construct server from string", KR(ret), K(param_value));
        }
      } else if (0 == param_name.string().case_compare("role")) {
        if (OB_FAIL(trans_role_str_to_value_(param_value, role))) {
          LOG_WARN("fail to switch role str to value", KR(ret), K(param_value));
        } else {
          is_valid_role = true;
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", KR(ret), K(param_name_with_value_str), K(param_name_with_value));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(share::ObLSID::INVALID_LS_ID == ls_id
                   || OB_INVALID_TENANT_ID == tenant_id
                   || common::ObAddr() == target_server
                   || !is_valid_role)) {
        // ensure all parameters are valid input values
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", KR(ret), K(ls_id), K(tenant_id), K(target_server), K(is_valid_role));
      } else {
        command_arg.ls_id_ = ls_id;
        command_arg.server_ = target_server;
        command_arg.role_ = role;
      }
    }
    LOG_INFO("finish parse parameters from command", KR(ret), K(command_arg_str), K(command_params_array),
              K(tenant_id), K(ls_id), K(target_server), K(role));
  }
  return ret;
}

int ObAdminSwitchReplicaRole::trans_role_str_to_value_(
    const ObSqlString &role_str,
    common::ObRole &role)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(role_str.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(role_str));
  } else if (0 == role_str.string().case_compare("LEADER")) {
    role = ObRole::LEADER;
  } else if (0 == role_str.string().case_compare("FOLLOWER")) {
    role = ObRole::FOLLOWER;
  } else if (0 == role_str.string().case_compare("DEFAULT")) {
    role = ObRole::INVALID_ROLE;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(role_str));
  }
  return ret;
}

} // end namespace rootserver
} // end namespace oceanbase