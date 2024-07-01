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
#include "ob_admin_drtask_util.h"
#include "logservice/ob_log_service.h" // for ObLogService
#include "share/ob_locality_parser.h" // for ObLocalityParser
#include "storage/tx_storage/ob_ls_service.h" // for ObLSService
#include "storage/ls/ob_ls.h" // for ObLS
#include "observer/ob_server_event_history_table_operator.h" // for SERVER_EVENT_ADD

namespace oceanbase
{
namespace rootserver
{
static const char* obadmin_drtask_ret_comment_strs[] = {
  "succeed to send ob_admin command",
  "invalid tenant_id or ls_id in command",
  "expect leader to execute this ob_admin command",
  "fail to send rpc",
  "fail to execute ob_admin command",
  ""/*default max*/
};

const char* ob_admin_drtask_ret_comment_strs(const rootserver::ObAdminDRTaskRetComment ret_comment)
{
  STATIC_ASSERT(ARRAYSIZEOF(obadmin_drtask_ret_comment_strs) == (int64_t)rootserver::ObAdminDRTaskRetComment::MAX_COMMENT + 1,
                "ret_comment string array size mismatch enum ObAdminDRTaskRetComment count");
  const char *str = NULL;
  if (ret_comment >= rootserver::ObAdminDRTaskRetComment::SUCCEED_TO_SEND_COMMAND && ret_comment <= rootserver::ObAdminDRTaskRetComment::MAX_COMMENT) {
    str = obadmin_drtask_ret_comment_strs[static_cast<int64_t>(ret_comment)];
  } else {
    str = obadmin_drtask_ret_comment_strs[static_cast<int64_t>(rootserver::ObAdminDRTaskRetComment::MAX_COMMENT)];
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid ObAdminDRTaskRetComment", K(ret_comment));
  }
  return str;
}

int ObAdminDRTaskUtil::handle_obadmin_command(const ObAdminCommandArg &command_arg)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  FLOG_INFO("begin to handle ob_admin command", K(command_arg));
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  share::ObLSID ls_id;
  ObSqlString result_comment("ResCmmt");
  ObAdminDRTaskRetComment ret_comment = FAIL_TO_EXECUTE_COMMAND;
  int64_t check_begin_time = ObTimeUtility::current_time();

  if (OB_UNLIKELY(!command_arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(command_arg));
  } else if (command_arg.is_remove_task()) {
    if (OB_FAIL(handle_remove_command_(command_arg, tenant_id, ls_id, ret_comment))) {
      LOG_WARN("fail to handle remove command", KR(ret), K(command_arg));
    }
  } else if (command_arg.is_add_task()) {
    if (OB_FAIL(handle_add_command_(command_arg, tenant_id, ls_id, ret_comment))) {
      LOG_WARN("fail to handle add command", KR(ret), K(command_arg));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task type", KR(ret), K(command_arg));
  }

  if (OB_SUCCESS != (tmp_ret = try_construct_result_comment_(ret, ret_comment, result_comment))) {
    LOG_WARN("fail to construct result comment", K(tmp_ret), KR(ret), K(ret_comment));
  }
  SERVER_EVENT_ADD("ob_admin", command_arg.get_type_str(),
                   "tenant_id", tenant_id,
                   "ls_id", ls_id.id(),
                   "arg", command_arg,
                   "result", result_comment,
                   "trace_id", ObCurTraceId::get_trace_id_str(),
                   "comment", command_arg.get_comment());

  int64_t cost = ObTimeUtility::current_time() - check_begin_time;
  FLOG_INFO("finish handle ob_admin command", K(command_arg), K(tenant_id), K(ls_id),
            K(result_comment), K(ret_comment), K(cost));
  return ret;
}

int ObAdminDRTaskUtil::handle_add_command_(
    const ObAdminCommandArg &command_arg,
    uint64_t &tenant_id,
    share::ObLSID &ls_id,
    ObAdminDRTaskRetComment &ret_comment)
{
  int ret = OB_SUCCESS;
  tenant_id = OB_INVALID_TENANT_ID;
  ret_comment = FAIL_TO_EXECUTE_COMMAND;
  ObLSAddReplicaArg arg;

  if (OB_UNLIKELY(!command_arg.is_valid())
      || OB_UNLIKELY(!command_arg.is_add_task())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(command_arg));
  } else if (OB_FAIL(construct_arg_for_add_command_(command_arg, arg, ret_comment))) {
    LOG_WARN("fail to construct arg for add command", KR(ret), K(command_arg),
             K(arg), K(ret_comment));
  } else if (OB_FAIL(execute_task_for_add_command_(command_arg, arg, ret_comment))) {
    LOG_WARN("fail to execute task for add command", KR(ret), K(command_arg), K(arg), K(ret_comment));
  } else {
    tenant_id = arg.tenant_id_;
    ls_id = arg.ls_id_;
    ret_comment = SUCCEED_TO_SEND_COMMAND;
  }
  return ret;
}

int ObAdminDRTaskUtil::construct_arg_for_add_command_(
    const ObAdminCommandArg &command_arg,
    ObLSAddReplicaArg &arg,
    ObAdminDRTaskRetComment &ret_comment)
{
  int ret = OB_SUCCESS;
  ret_comment = FAIL_TO_EXECUTE_COMMAND;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  share::ObLSID ls_id;
  ObReplicaType replica_type = REPLICA_TYPE_FULL;
  common::ObAddr force_data_source_server;
  common::ObAddr leader_server;
  common::ObAddr target_server;
  int64_t orig_paxos_replica_number = 0;
  int64_t new_paxos_replica_number = 0;

  if (OB_UNLIKELY(!command_arg.is_valid())
      || OB_UNLIKELY(!command_arg.is_add_task())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(command_arg));
  // STEP 1: parse parameters from ob_admin command directly
  } else if (OB_FAIL(parse_params_from_obadmin_command_arg(
                         command_arg, tenant_id, ls_id, replica_type, force_data_source_server,
                         target_server, orig_paxos_replica_number, new_paxos_replica_number))) {
    LOG_WARN("fail to parse parameters provided in ob_admin command", KR(ret), K(command_arg));
  } else if (OB_UNLIKELY(!ls_id.is_valid_with_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    ret_comment = ObAdminDRTaskRetComment::TENANT_ID_OR_LS_ID_NOT_VALID;
    LOG_WARN("invalid tenant_id or ls_id", KR(ret), K(command_arg), K(tenant_id), K(ls_id));
  } else if (OB_UNLIKELY(!target_server.is_valid())
             || OB_UNLIKELY(REPLICA_TYPE_FULL != replica_type && REPLICA_TYPE_READONLY != replica_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(replica_type), K(target_server));
  // STEP 2: construct orig_paxos_replica_number and leader_server if not specified by ob_admin command
  } else if (OB_FAIL(construct_default_params_for_add_command_(
                    tenant_id,
                    ls_id,
                    orig_paxos_replica_number,
                    leader_server))) {
    LOG_WARN("fail to fetch ls info and construct related parameters", KR(ret), K(tenant_id),
               K(ls_id), K(orig_paxos_replica_number), K(leader_server));
  }

  if (OB_SUCC(ret)) {
    new_paxos_replica_number = 0 == new_paxos_replica_number
                               ? orig_paxos_replica_number
                               : new_paxos_replica_number;
    ObReplicaMember data_source_member(leader_server, 0/*timstamp*/);
    ObReplicaMember force_data_source_member(force_data_source_server, 0/*timstamp*/);
    ObReplicaMember add_member(target_server, ObTimeUtility::current_time(), replica_type);
    // STEP 3: construct arg
    if (OB_ISNULL(ObCurTraceId::get_trace_id())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret));
    } else if (OB_FAIL(arg.init(
                      *ObCurTraceId::get_trace_id()/*task_id*/,
                      tenant_id,
                      ls_id,
                      add_member,
                      data_source_member,
                      orig_paxos_replica_number,
                      new_paxos_replica_number,
                      false/*is_skip_change_member_list-not used*/,
                      force_data_source_member/*force_data_source*/))) {
      LOG_WARN("fail to init arg", KR(ret), K(tenant_id), K(ls_id), K(add_member), K(data_source_member),
              K(orig_paxos_replica_number), K(new_paxos_replica_number), K(force_data_source_member));
    }
  }
  return ret;
}

int ObAdminDRTaskUtil::construct_default_params_for_add_command_(
    const uint64_t &tenant_id,
    const share::ObLSID &ls_id,
    int64_t &orig_paxos_replica_number,
    common::ObAddr &leader_server)
{
  int ret = OB_SUCCESS;
  share::ObLSInfo ls_info;
  const share::ObLSReplica *leader_replica = nullptr;

  if (OB_UNLIKELY(!ls_id.is_valid_with_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id));
  } else if (OB_ISNULL(GCTX.lst_operator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls table operator", KR(ret));
  } else if (OB_FAIL(GCTX.lst_operator_->get(GCONF.cluster_id, tenant_id, ls_id,
                     share::ObLSTable::COMPOSITE_MODE, ls_info))) {
    LOG_WARN("fail to get ls info", KR(ret), K(tenant_id), K(ls_id), K(ls_info));
  } else if (OB_FAIL(ls_info.find_leader(leader_replica))) {
    LOG_WARN("fail to get ls leader replica", KR(ret), K(ls_info));
  } else if (OB_ISNULL(leader_replica)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid leader replica", KR(ret), K(ls_info));
  } else {
    //   If [orig_paxos_replica_number] not specified in obadmin command,
    //   need to construct from leader_replica, use leader replica as default
    if (0 == orig_paxos_replica_number) {
      orig_paxos_replica_number = leader_replica->get_paxos_replica_number();
    }
    leader_server = leader_replica->get_server();
  }
  return ret;
}

int ObAdminDRTaskUtil::execute_task_for_add_command_(
    const ObAdminCommandArg &command_arg,
    const ObLSAddReplicaArg &arg,
    ObAdminDRTaskRetComment &ret_comment)
{
  int ret = OB_SUCCESS;
  ret_comment = FAIL_TO_EXECUTE_COMMAND;
  const int64_t add_timeout = GCONF.rpc_timeout * 5;

  if (OB_UNLIKELY(!arg.is_valid())
      || OB_UNLIKELY(!command_arg.is_valid())
      || OB_UNLIKELY(!command_arg.is_add_task())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg), K(command_arg));
  } else if (GCTX.self_addr() == arg.dst_.get_server()) {
    // do not need to send rpc, execute locally
    MTL_SWITCH(arg.tenant_id_) {
      if (OB_FAIL(observer::ObService::do_add_ls_replica(arg))) {
        LOG_WARN("fail to execute add replica rpc locally", KR(ret), K(arg));
      }
    }
  } else if (OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("svr rpc proxy is nullptr", KR(ret));
  } else if (OB_FAIL(GCTX.srv_rpc_proxy_->to(arg.dst_.get_server()).by(arg.tenant_id_).timeout(add_timeout).ls_add_replica(arg))) {
    ret_comment = ObAdminDRTaskRetComment::FAILED_TO_SEND_RPC;
    LOG_WARN("fail to execute add replica rpc", KR(ret), K(arg), K(add_timeout));
  }

  if (OB_SUCC(ret)) {
    // local execute or rpc is send, log task start, task finish will be recorded later
    ROOTSERVICE_EVENT_ADD("disaster_recovery", drtasklog::START_ADD_LS_REPLICA_STR,
                          "tenant_id", arg.tenant_id_,
                          "ls_id", arg.ls_id_.id(),
                          "task_id", ObCurTraceId::get_trace_id_str(),
                          "destination", arg.dst_,
                          "comment", command_arg.get_comment());
  }
  return ret;
}

int ObAdminDRTaskUtil::handle_remove_command_(
    const ObAdminCommandArg &command_arg,
    uint64_t &tenant_id,
    share::ObLSID &ls_id,
    ObAdminDRTaskRetComment &ret_comment)
{
  int ret = OB_SUCCESS;
  tenant_id = OB_INVALID_TENANT_ID;
  ret_comment = FAIL_TO_EXECUTE_COMMAND;
  ObReplicaType replica_type = REPLICA_TYPE_FULL;
  common::ObAddr data_source_server;
  common::ObAddr target_server;
  int64_t orig_paxos_replica_number = 0;
  int64_t new_paxos_replica_number = 0;

  if (OB_UNLIKELY(!command_arg.is_valid())
      || OB_UNLIKELY(!command_arg.is_remove_task())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(command_arg));
  // STEP 1: parse parameters from ob_admin command directly
  } else if (OB_FAIL(parse_params_from_obadmin_command_arg(
                         command_arg, tenant_id, ls_id, replica_type, data_source_server,
                         target_server, orig_paxos_replica_number, new_paxos_replica_number))) {
    LOG_WARN("fail to parse parameters provided in ob_admin command", KR(ret), K(command_arg));
  } else if (OB_UNLIKELY(!ls_id.is_valid_with_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    ret_comment = ObAdminDRTaskRetComment::TENANT_ID_OR_LS_ID_NOT_VALID;
    LOG_WARN("invalid tenant_id or ls_id", KR(ret), K(command_arg), K(tenant_id), K(ls_id));
  } else if (OB_UNLIKELY(!target_server.is_valid())
             || OB_UNLIKELY(REPLICA_TYPE_FULL != replica_type && REPLICA_TYPE_READONLY != replica_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(replica_type), K(target_server));
  } else {
    // STEP 2: construct args and execute
    if (REPLICA_TYPE_FULL == replica_type) {
      ObLSDropPaxosReplicaArg remove_paxos_arg;
      if (OB_FAIL(construct_remove_paxos_task_arg_(
                      tenant_id, ls_id, target_server, orig_paxos_replica_number,
                      new_paxos_replica_number, ret_comment, remove_paxos_arg))) {
        LOG_WARN("fail to construct remove paxos task arg", KR(ret), K(tenant_id), K(ls_id),
                 K(target_server), K(orig_paxos_replica_number), K(new_paxos_replica_number),
                 K(ret_comment), K(remove_paxos_arg));
      } else if (OB_FAIL(execute_remove_paxos_task_(command_arg, remove_paxos_arg))) {
        LOG_WARN("fail to execute remove paxos replica task", KR(ret), K(command_arg), K(remove_paxos_arg));
      } else {
        ret_comment = SUCCEED_TO_SEND_COMMAND;
      }
    } else if (REPLICA_TYPE_READONLY == replica_type) {
      ObLSDropNonPaxosReplicaArg remove_nonpaxos_arg;
      if (OB_FAIL(construct_remove_nonpaxos_task_arg_(
                      tenant_id, ls_id, target_server, ret_comment, remove_nonpaxos_arg))) {
        LOG_WARN("fail to construct remove non-paxos replica task arg", KR(ret), K(tenant_id),
                 K(ls_id), K(target_server), K(ret_comment), K(remove_nonpaxos_arg));
      } else if (OB_FAIL(execute_remove_nonpaxos_task_(command_arg, remove_nonpaxos_arg))) {
        LOG_WARN("fail to execute remove nonpaxos replica task", KR(ret), K(command_arg), K(remove_nonpaxos_arg));
      } else {
        ret_comment = SUCCEED_TO_SEND_COMMAND;
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unexpected replica type", KR(ret), K(replica_type), K(tenant_id), K(ls_id), K(command_arg));
    }
  }
  return ret;
}

int ObAdminDRTaskUtil::construct_remove_paxos_task_arg_(
    const uint64_t &tenant_id,
    const share::ObLSID &ls_id,
    const common::ObAddr &target_server,
    int64_t &orig_paxos_replica_number,
    int64_t &new_paxos_replica_number,
    ObAdminDRTaskRetComment &ret_comment,
    ObLSDropPaxosReplicaArg &remove_paxos_arg)
{
  int ret = OB_SUCCESS;
  ret_comment = FAIL_TO_EXECUTE_COMMAND;
  common::ObMember member;
  ObReplicaMember member_to_remove;
  palf::PalfStat palf_stat;

  if (OB_UNLIKELY(!ls_id.is_valid_with_tenant(tenant_id))
      || OB_UNLIKELY(!target_server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id or ls_id", KR(ret), K(tenant_id), K(ls_id), K(target_server));
  } else if (OB_FAIL(get_local_palf_stat_(tenant_id, ls_id, palf_stat, ret_comment))) {
    LOG_WARN("fail to get local palf stat", KR(ret), K(tenant_id), K(ls_id));
  } else if (OB_UNLIKELY(!palf_stat.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(palf_stat));
  } else if (OB_UNLIKELY(!palf_stat.paxos_member_list_.contains(target_server))) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("replica not found in member_list", KR(ret), K(target_server), K(palf_stat));
  } else if (OB_FAIL(palf_stat.paxos_member_list_.get_member_by_addr(target_server, member))) {
    LOG_WARN("fail to get member from paxos_member_list", KR(ret), K(palf_stat), K(target_server));
  } else {
    member_to_remove = ObReplicaMember(member);
    if (OB_FAIL(member_to_remove.set_replica_type(REPLICA_TYPE_FULL))) {
      LOG_WARN("fail to set replica type for member to remove", KR(ret));
    } else {
      //  If [orig_paxos_replica_number] not specified in obadmin command,
      //  use leader replica's info as default
      orig_paxos_replica_number = 0 == orig_paxos_replica_number
                                ? palf_stat.paxos_replica_num_
                                : orig_paxos_replica_number;
      new_paxos_replica_number = 0 == new_paxos_replica_number
                               ? orig_paxos_replica_number
                               : new_paxos_replica_number;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(ObCurTraceId::get_trace_id())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (OB_FAIL(remove_paxos_arg.init(
                         *ObCurTraceId::get_trace_id()/*task_id*/, tenant_id, ls_id,
                         member_to_remove, orig_paxos_replica_number, new_paxos_replica_number))) {
    LOG_WARN("fail to init arg", KR(ret), K(tenant_id), K(ls_id), K(member_to_remove),
             K(orig_paxos_replica_number), K(new_paxos_replica_number));
  }
  return ret;
}

int ObAdminDRTaskUtil::construct_remove_nonpaxos_task_arg_(
    const uint64_t &tenant_id,
    const share::ObLSID &ls_id,
    const common::ObAddr &target_server,
    ObAdminDRTaskRetComment &ret_comment,
    ObLSDropNonPaxosReplicaArg &remove_nonpaxos_arg)
{
  int ret = OB_SUCCESS;
  ret_comment = FAIL_TO_EXECUTE_COMMAND;
  common::ObMember member;
  ObReplicaMember member_to_remove;
  palf::PalfStat palf_stat;

  if (OB_UNLIKELY(!ls_id.is_valid_with_tenant(tenant_id))
      || OB_UNLIKELY(!target_server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id or ls_id", KR(ret), K(tenant_id), K(ls_id), K(target_server));
  } else if (OB_FAIL(get_local_palf_stat_(tenant_id, ls_id, palf_stat, ret_comment))) {
    LOG_WARN("fail to get local palf stat", KR(ret), K(tenant_id), K(ls_id));
  } else if (OB_UNLIKELY(!palf_stat.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(palf_stat));
  } else if (OB_UNLIKELY(!palf_stat.learner_list_.contains(target_server))) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("replica not found in learner_list", KR(ret), K(target_server), K(palf_stat));
  } else if (OB_FAIL(palf_stat.learner_list_.get_learner_by_addr(target_server, member))) {
    LOG_WARN("fail to get member from learner_list", KR(ret), K(palf_stat), K(target_server));
  } else {
    member_to_remove = ObReplicaMember(member);
    if (OB_FAIL(member_to_remove.set_replica_type(REPLICA_TYPE_READONLY))) {
      LOG_WARN("fail to set replica type for member to remove", KR(ret));
    } else if (OB_ISNULL(ObCurTraceId::get_trace_id())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret));
    } else if (OB_FAIL(remove_nonpaxos_arg.init(
                           *ObCurTraceId::get_trace_id()/*task_id*/, tenant_id,
                           ls_id, member_to_remove))) {
      LOG_WARN("fail to init arg", KR(ret), K(tenant_id), K(ls_id), K(member_to_remove));
    }
  }
  return ret;
}

int ObAdminDRTaskUtil::get_local_palf_stat_(
    const uint64_t &tenant_id,
    const share::ObLSID &ls_id,
    palf::PalfStat &palf_stat,
    ObAdminDRTaskRetComment &ret_comment)
{
  int ret = OB_SUCCESS;
  ret_comment = FAIL_TO_EXECUTE_COMMAND;
  palf_stat.reset();

  if (OB_UNLIKELY(!ls_id.is_valid_with_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id or ls_id", KR(ret), K(tenant_id), K(ls_id));
  } else {
    MTL_SWITCH(tenant_id) {
      logservice::ObLogService *log_service = NULL;
      palf::PalfHandleGuard palf_handle_guard;
      if (OB_ISNULL(log_service = MTL(logservice::ObLogService*))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("MTL ObLogService is null", KR(ret), K(tenant_id));
      } else if (OB_FAIL(log_service->open_palf(ls_id, palf_handle_guard))) {
        LOG_WARN("failed to open palf", KR(ret), K(tenant_id), K(ls_id));
      } else if (OB_FAIL(palf_handle_guard.stat(palf_stat))) {
        LOG_WARN("get palf_stat failed", KR(ret), K(tenant_id), K(ls_id));
      } else if (LEADER != palf_stat.role_) {
        ret = OB_STATE_NOT_MATCH;
        ret_comment = ObAdminDRTaskRetComment::SERVER_TO_EXECUTE_COMMAND_NOT_LEADER;
        LOG_WARN("invalid argument, expect self address is leader replica", KR(ret),
                 K(tenant_id), K(ls_id), K(palf_stat));
      }
    }
  }
  return ret;
}

int ObAdminDRTaskUtil::execute_remove_paxos_task_(
    const ObAdminCommandArg &command_arg,
    const ObLSDropPaxosReplicaArg &remove_paxos_arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!command_arg.is_valid())
      || OB_UNLIKELY(!command_arg.is_remove_task())
      || OB_UNLIKELY(!remove_paxos_arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(command_arg), K(remove_paxos_arg));
  } else {
    // do not need to send rpc, just execute locally
    LOG_INFO("start to remove member from member_list", K(remove_paxos_arg));
    MTL_SWITCH(remove_paxos_arg.tenant_id_) {
      if (OB_FAIL(observer::ObService::do_remove_ls_paxos_replica(remove_paxos_arg))) {
        LOG_WARN("fail to execute remove paxos replica rpc locally", KR(ret), K(remove_paxos_arg));
      }
    }
  }
  if (OB_SUCC(ret)) {
    // rpc is send, log task start, task finish will be recorded later
    ROOTSERVICE_EVENT_ADD("disaster_recovery", drtasklog::START_REMOVE_LS_PAXOS_REPLICA_STR,
                          "tenant_id", remove_paxos_arg.tenant_id_,
                          "ls_id", remove_paxos_arg.ls_id_.id(),
                          "task_id", ObCurTraceId::get_trace_id_str(),
                          "remove_server", remove_paxos_arg.remove_member_,
                          "comment", command_arg.get_comment());
  }
  return ret;
}

int ObAdminDRTaskUtil::execute_remove_nonpaxos_task_(
    const ObAdminCommandArg &command_arg,
    const ObLSDropNonPaxosReplicaArg &remove_non_paxos_arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!command_arg.is_valid())
      || OB_UNLIKELY(!command_arg.is_remove_task())
      || OB_UNLIKELY(!remove_non_paxos_arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(command_arg), K(remove_non_paxos_arg));
  } else {
    // do not need to send rpc, just execute locally
    LOG_INFO("start to remove learner from learner_list", K(remove_non_paxos_arg));
    MTL_SWITCH(remove_non_paxos_arg.tenant_id_) {
      if (OB_FAIL(observer::ObService::do_remove_ls_nonpaxos_replica(remove_non_paxos_arg))) {
        LOG_WARN("fail to execute remove non-paxos replica rpc locally", KR(ret), K(remove_non_paxos_arg));
      }
    }
  }
  if (OB_SUCC(ret)) {
    // rpc is send, log task start, task finish will be recorded later
    ROOTSERVICE_EVENT_ADD("disaster_recovery", drtasklog::START_REMOVE_LS_PAXOS_REPLICA_STR,
                          "tenant_id", remove_non_paxos_arg.tenant_id_,
                          "ls_id", remove_non_paxos_arg.ls_id_.id(),
                          "task_id", ObCurTraceId::get_trace_id_str(),
                          "remove_server", remove_non_paxos_arg.remove_member_,
                          "comment", command_arg.get_comment());
  }
  return ret;
}

int ObAdminDRTaskUtil::parse_params_from_obadmin_command_arg(
    const ObAdminCommandArg &command_arg,
    uint64_t &tenant_id,
    share::ObLSID &ls_id,
    ObReplicaType &replica_type,
    common::ObAddr &data_source_server,
    common::ObAddr &target_server,
    int64_t &orig_paxos_replica_number,
    int64_t &new_paxos_replica_number)
{
  int ret = OB_SUCCESS;
  // reset output params
  tenant_id = OB_INVALID_TENANT_ID;
  ls_id.reset();
  replica_type = REPLICA_TYPE_FULL;
  data_source_server.reset();
  target_server.reset();
  orig_paxos_replica_number = 0;
  new_paxos_replica_number = 0;
  // construct items to use
  ObArenaAllocator allocator("ObAdminDRTask");
  ObString admin_command_before_trim;
  ObString admin_command_after_trim;
  ObArray<ObString> command_params_array;
  if (OB_UNLIKELY(!command_arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(command_arg));
  } else if (OB_FAIL(ob_write_string(allocator, command_arg.get_admin_command_str(), admin_command_before_trim))) {
    LOG_WARN("fail to write string", KR(ret), K(command_arg));
  } else if (FALSE_IT(admin_command_after_trim = admin_command_before_trim.trim())) {
  } else if (OB_FAIL(split_on(admin_command_after_trim, ',', command_params_array))) {
    LOG_WARN("fail to split string", KR(ret), K(admin_command_after_trim), K(admin_command_before_trim));
  } else {
    LOG_INFO("start to parse parameters from command", K(command_arg), K(command_params_array));
    ObSqlString data_source_string("DtStr");
    for (int64_t param_index = 0;
         param_index < command_params_array.count() && OB_SUCC(ret);
         param_index++) {
      ObString param_name_with_value_str = command_params_array.at(param_index);
      ObArray<ObString> param_name_with_value;
      ObSqlString param_name("ParamN");
      ObSqlString param_value("ParamV");
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
        int64_t tenant_id_to_set = OB_INVALID_TENANT_ID;
        if (OB_FAIL(extract_int(param_value.string(), 0, pos, tenant_id_to_set))) {
          LOG_WARN("fail to extract int from string", KR(ret), K(param_name_with_value), K(tenant_id_to_set));
        } else {
          tenant_id = tenant_id_to_set;
        }
      } else if (0 == param_name.string().case_compare("ls_id")) {
        int64_t ls_id_to_set;
        if (OB_FAIL(extract_int(param_value.string(), 0, pos, ls_id_to_set))) {
          LOG_WARN("fail to extract int from string", KR(ret), K(param_name_with_value), K(ls_id_to_set));
        } else {
          ls_id = share::ObLSID(ls_id_to_set);
        }
      } else if (0 == param_name.string().case_compare("replica_type")) {
        if (OB_FAIL(share::ObLocalityParser::parse_type(
                        param_value.ptr(),
                        param_value.length(),
                        replica_type))) {
          LOG_WARN("fail to parse replica type", KR(ret), K(param_name_with_value), K(replica_type));
        }
      } else if (0 == param_name.string().case_compare("orig_paxos_replica_number")) {
        if (OB_FAIL(extract_int(param_value.string(), 0, pos, orig_paxos_replica_number))) {
          LOG_WARN("fail to extract int from string", KR(ret), K(param_name_with_value), K(orig_paxos_replica_number));
        }
      } else if (0 == param_name.string().case_compare("new_paxos_replica_number")) {
        if (OB_FAIL(extract_int(param_value.string(), 0, pos, new_paxos_replica_number))) {
          LOG_WARN("fail to extract int from string", KR(ret), K(param_name_with_value), K(new_paxos_replica_number));
        }
      } else if (0 == param_name.string().case_compare("server")) {
        if (OB_FAIL(target_server.parse_from_string(param_value.string()))) {
          LOG_WARN("fail to construct target server from string", KR(ret), K(param_value));
        }
      } else if (0 == param_name.string().case_compare("data_source")) {
        if (OB_FAIL(data_source_server.parse_from_string(param_value.string()))) {
          LOG_WARN("fail to construct data source server from string", KR(ret), K(param_value));
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", KR(ret), K(param_name_with_value_str), K(param_name_with_value));
      }
    }

    if (OB_SUCC(ret)) {
      // if [server] not specified, use local as default
      target_server = target_server.is_valid() ? target_server : GCTX.self_addr();
    }

    LOG_INFO("finish parse parameters from command", KR(ret), K(command_arg), K(command_params_array), K(tenant_id),
             K(ls_id), K(replica_type), K(data_source_server), K(target_server), K(orig_paxos_replica_number),
             K(new_paxos_replica_number));
  }
  return ret;
}

int ObAdminDRTaskUtil::try_construct_result_comment_(
    const int &ret_code,
    const ObAdminDRTaskRetComment &ret_comment,
    ObSqlString &result_comment)
{
  int ret = OB_SUCCESS;
  result_comment.reset();
  if (OB_FAIL(result_comment.assign_fmt("ret:%d, %s; ret_comment:%s;",
                                         ret_code, common::ob_error_name(ret_code),
                                         ob_admin_drtask_ret_comment_strs(ret_comment)))) {
    LOG_WARN("fail to construct result comment", KR(ret), K(ret_code), K(ret_comment));
  }
  return ret;
}
} // end namespace rootserver
} // end namespace oceanbase
