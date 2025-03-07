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

#include "ob_disaster_recovery_task_utils.h"

#include "ob_disaster_recovery_service.h"                              // for ObDRService
#include "ob_disaster_recovery_task_mgr.h"                             // for ObParallelMigrationMode
#include "ob_disaster_recovery_task_table_operator.h"                  // for ObLSReplicaTaskTableOperator
#include "share/ls/ob_ls_table_operator.h"                             // for ObLSTableOperator
#include "share/ls/ob_ls_info.h"                                       // for ObLSInfo
#include "share/ob_all_server_tracer.h"                                // for SVR_TRACER
#include "share/ob_server_table_operator.h"                            // for ObServerInfoInTable
#include "share/ob_srv_rpc_proxy.h"                                    // for ObSrvRpcProxy
#include "share/ob_debug_sync.h"                                       // for DEBUG_SYNC
#include "share/ob_service_epoch_proxy.h"                              // for ObServiceEpochProxy
#include "share/location_cache/ob_location_service.h"                  // for ObLocationService

namespace oceanbase
{
using namespace common;
using namespace lib;
using namespace obrpc;
using namespace share;

namespace rootserver
{

#define FREE_DISASTER_RECOVERY_UTILS_TASK_MEMORY                             \
  for (int64_t i = 0; i < dr_tasks.count(); ++i) {                           \
    ObDRTask *task = dr_tasks.at(i);                                         \
    if (OB_NOT_NULL(task)) {                                                 \
      task->~ObDRTask();                                                     \
    }                                                                        \
  }                                                                          \
  task_alloc.reset();                                                        \
  dr_tasks.reset();                                                          \

#define COMMIT_DISASTER_RECOVERY_UTILS_TRANS                                 \
  if (trans.is_started()) {                                                  \
    int tmp_ret = OB_SUCCESS;                                                \
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {                 \
      LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));              \
      ret = OB_SUCC(ret) ? tmp_ret : ret;                                    \
    }                                                                        \
  }                                                                          \

bool DisasterRecoveryUtils::is_history_table_data_version_match(
    uint64_t tenant_data_version)
{
  return ((tenant_data_version >= DATA_VERSION_4_3_3_0)
       || (tenant_data_version >= MOCK_DATA_VERSION_4_2_3_0 && tenant_data_version < DATA_VERSION_4_3_0_0)
       || (tenant_data_version >= MOCK_DATA_VERSION_4_2_1_8 && tenant_data_version < DATA_VERSION_4_2_2_0));
}

int DisasterRecoveryUtils::get_dr_tasks_count(
    const uint64_t tenant_id,
    int64_t &task_count)
{
  int ret = OB_SUCCESS;
  task_count = 0;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else {
    ObSqlString sql;
    uint64_t sql_tenant_id = gen_meta_tenant_id(tenant_id);
    SMART_VAR(ObISQLClient::ReadResult, res) {
      sqlclient::ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql.assign_fmt("SELECT count(*) AS count FROM %s ", share::OB_ALL_LS_REPLICA_TASK_TNAME))) {
        LOG_WARN("fail to assign sql", KR(ret), K(tenant_id));
      } else if (OB_FAIL(GCTX.sql_proxy_->read(res, sql_tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(sql_tenant_id), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret), K(sql));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("fail to get next result", KR(ret), K(sql));
      } else {
        EXTRACT_INT_FIELD_MYSQL(*result, "count", task_count, int64_t);
      }
    } // end SMART_VAR
  }
  return ret;
}

#define DR_SERVICE_SEND_RPC_TO_META_WITH_RETRY(func_name)                                                     \
  int ret = OB_SUCCESS;                                                                                       \
  ObTimeoutCtx ctx;                                                                                           \
  int tmp_ret = OB_SUCCESS;                                                                                   \
  if (OB_ISNULL(GCTX.location_service_) || OB_ISNULL(GCTX.srv_rpc_proxy_)) {                                  \
    ret = OB_ERR_UNEXPECTED;                                                                                  \
    LOG_WARN("location_service_ or srv_rpc_proxy_ is NULL",                                                   \
              KR(ret), KP(GCTX.location_service_), KP(GCTX.srv_rpc_proxy_));                                  \
  } else if (OB_UNLIKELY(!arg.is_valid())) {                                                                  \
    ret = OB_INVALID_ARGUMENT;                                                                                \
    LOG_WARN("invalid argument", KR(ret), K(arg));                                                            \
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF.internal_sql_execute_timeout))) {        \
    LOG_WARN("failed to set default timeout", KR(ret));                                                       \
  } else {                                                                                                    \
    ObAddr leader;                                                                                            \
    int64_t retry_count = 0;                                                                                  \
    const uint64_t meta_tenant_id = gen_meta_tenant_id(arg.get_tenant_id());                                  \
    while (retry_count++ < MAX_REPORT_RETRY_TIMES) {                                                          \
      if (0 > ctx.get_timeout()) {                                                                            \
        ret = OB_TIMEOUT;                                                                                     \
        LOG_WARN("wait send rpc to meta tenant finished timeout", KR(ret));                                   \
        break;                                                                                                \
      } else if (OB_FAIL(GCTX.location_service_->get_leader(GCONF.cluster_id,                                 \
                        meta_tenant_id, SYS_LS, false/*force_renew*/, leader))) {                             \
        LOG_WARN("failed to get ls leader", KR(ret), K(arg));                                                 \
      } else if (OB_FAIL(GCTX.srv_rpc_proxy_->to(leader).by(meta_tenant_id)                                   \
                                             .timeout(GCONF.rpc_timeout).func_name(arg))) {                   \
        LOG_WARN("fail to send rpc", KR(ret), K(arg));                                                        \
      }                                                                                                       \
      if (OB_SUCC(ret)) {                                                                                     \
        LOG_INFO("send rpc success", K(leader), K(arg));                                                      \
        break;                                                                                                \
      } else if (OB_TMP_FAIL(GCTX.location_service_->nonblock_renew(                                          \
                                GCONF.cluster_id, meta_tenant_id, SYS_LS))) {                                 \
        LOG_WARN("failed to renew location", KR(ret), KR(tmp_ret), K(arg));                                   \
      }                                                                                                       \
      if (OB_FAIL(ret)) {                                                                                     \
        ob_usleep(REPORT_RETRY_INTERVAL_MS);                                                                  \
      }                                                                                                       \
    }                                                                                                         \
  }                                                                                                           \
  return ret;                                                                                                 \

int DisasterRecoveryUtils::wakeup_tenant_dr_service(
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  uint64_t sys_data_version = 0;
  ObNotifyTenantThreadArg arg;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(OB_SYS_TENANT_ID, sys_data_version))) {
    LOG_WARN("fail to get min data version", KR(ret));
  } else if (is_sys_tenant(tenant_id) || sys_data_version < DATA_VERSION_4_3_5_1) {
    // the task to be processed is the sys tenant's or
    // sys tenant's data_version did not push up to DATA_VERSION_4_3_5_1.
    // (sys tenant is responsible for all tenant tasks)
    if (OB_FAIL(wakeup_local_service(OB_SYS_TENANT_ID))) {
      LOG_WARN("fail to wakeup local dr service", KR(ret));
    }
  } else if (OB_FAIL(arg.init(gen_meta_tenant_id(tenant_id), obrpc::ObNotifyTenantThreadArg::DISASTER_RECOVERY_SERVICE))) {
    LOG_WARN("failed to init arg", KR(ret), K(tenant_id));
  } else if (OB_FAIL(DisasterRecoveryUtils::wakeup_tenant_service(arg))) {
    LOG_WARN("fail to wake up", KR(ret), K(arg));
  }
  return ret;
}

int DisasterRecoveryUtils::wakeup_local_service(
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    MTL_SWITCH(tenant_id) {
      ObDRService *dr_service = MTL(ObDRService*);
      if (OB_ISNULL(dr_service)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("dr_service is nullptr", KR(ret), KP(dr_service));
      } else {
        dr_service->wakeup();
      }
    }
  }
  return ret;
}

int DisasterRecoveryUtils::wakeup_tenant_service(
    const obrpc::ObNotifyTenantThreadArg &arg)
{
  DR_SERVICE_SEND_RPC_TO_META_WITH_RETRY(notify_tenant_thread)
}

int DisasterRecoveryUtils::report_to_meta_tenant(
    const obrpc::ObDRTaskReplyResult &arg)
{
  DR_SERVICE_SEND_RPC_TO_META_WITH_RETRY(disaster_recovery_task_reply)
}

// compatibility code
int DisasterRecoveryUtils::report_to_rs(
    const obrpc::ObDRTaskReplyResult &res)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  if (OB_ISNULL(GCTX.rs_mgr_) || OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rs_mgr or storage_rpc_ is NULL", KR(ret), KP(GCTX.rs_mgr_), KP(GCTX.srv_rpc_proxy_));
  } else if (OB_UNLIKELY(!res.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(res));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF.internal_sql_execute_timeout))) {
    LOG_WARN("failed to set default timeout", KR(ret));
  } else {
    ObAddr rs_addr;
    int64_t retry_count = 0;
    while (retry_count++ < MAX_REPORT_RETRY_TIMES) {
      if (0 > ctx.get_timeout()) {
        ret = OB_TIMEOUT;
        LOG_WARN("wait send rpc to rs finished timeout", KR(ret));
        break;
      } else if (OB_FAIL(GCTX.rs_mgr_->get_master_root_server(rs_addr))) {
        LOG_WARN("get master root service failed", KR(ret));
      } else if (OB_FAIL(GCTX.srv_rpc_proxy_->to(rs_addr).disaster_recovery_task_reply(res))) {
        LOG_WARN("failed to post ls diaster recovery res", KR(ret), K(rs_addr), K(res));
      }
      if (OB_SUCC(ret)) {
        LOG_INFO("post task reply rs success", K(rs_addr), K(res));
        break;
      } else if (OB_TMP_FAIL(GCTX.rs_mgr_->renew_master_rootserver())) {
        LOG_WARN("renew master root service failed", KR(ret));
      }
      if (OB_FAIL(ret)) {
        ob_usleep(REPORT_RETRY_INTERVAL_MS); // 100ms
      }
    } // end while
  }
  return ret;
}

int DisasterRecoveryUtils::get_member_list(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    common::ObMemberList &member_list)
{
  int ret = OB_SUCCESS;
  member_list.reset();
  share::ObLSInfo ls_info;
  const share::ObLSReplica *leader_replica = nullptr;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)) || OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id));
  } else if (!ls_id.is_valid_with_tenant(tenant_id)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("check ls_id is_valid_with_tenant failed", KR(ret), K(tenant_id), K(ls_id));
  } else if (OB_ISNULL(GCTX.lst_operator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lst_operator_ is null", KR(ret), KP(GCTX.lst_operator_));
  } else if (OB_FAIL(GCTX.lst_operator_->get(GCONF.cluster_id, tenant_id, ls_id,
                      share::ObLSTable::COMPOSITE_MODE, ls_info))) {
    LOG_WARN("get ls info failed", KR(ret), K(tenant_id), K(ls_id));
  } else if (ls_info.get_replicas().count() == 0) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("ls_info replica count is 0", KR(ret), K(tenant_id), K(ls_id), K(ls_info));
  } else if (OB_FAIL(ls_info.find_leader(leader_replica))) {
    LOG_WARN("fail to find leader", KR(ret), K(ls_info));
  } else if (OB_ISNULL(leader_replica)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("leader replica ptr is null", KR(ret), KP(leader_replica));
  } else {
    // construct member list
    FOREACH_CNT_X(m, leader_replica->get_member_list(), OB_SUCC(ret)) {
      if (OB_ISNULL(m)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid SimpleMember", KR(ret), KP(m));
      } else if (OB_FAIL(member_list.add_member(ObMember(m->get_server(), m->get_timestamp())))) {
        LOG_WARN("fail to add server to member list", KR(ret), KPC(m));
      }
    }
  }
  return ret;
}

int DisasterRecoveryUtils::build_execute_result(
    const int ret_code,
    const ObDRTaskRetComment &ret_comment,
    const int64_t start_time,
    ObSqlString &execute_result)
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  const int64_t elapsed = (OB_SUCCESS == ret_code) ? (now - start_time) : 0;
  execute_result.reset();
  if (OB_FAIL(execute_result.append_fmt("ret:%d, %s; elapsed:%ld;",
                        ret_code, common::ob_error_name(ret_code), elapsed))) {
    LOG_WARN("fail to append to execute_result", KR(ret), K(ret_code), K(elapsed));
  } else if (OB_SUCCESS != ret_code
          && OB_FAIL(execute_result.append_fmt(" comment:%s;", ob_disaster_recovery_task_ret_comment_strs(ret_comment)))) {
    LOG_WARN("fail to append ret comment to execute result", KR(ret), K(ret_comment));
  }
  return ret;
}

int DisasterRecoveryUtils::get_member_in_member_list_(
    const share::ObLSInfo &ls_info,
    const common::ObAddr &server_addr,
    common::ObReplicaMember &replica_member)
{
  int ret = OB_SUCCESS;
  replica_member.reset();
  ObMember member;
  const ObLSReplica *leader_replica = nullptr;
  if (OB_UNLIKELY(!ls_info.is_valid()
               || !server_addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_info), K(server_addr));
  } else if (OB_FAIL(ls_info.find_leader(leader_replica))) {
    LOG_WARN("fail to find leader", KR(ret), K(ls_info));
  } else if (OB_ISNULL(leader_replica)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("leader replica ptr is null", KR(ret), KP(leader_replica));
  } else {
    FOREACH_CNT_X(m, leader_replica->get_member_list(), OB_SUCC(ret)) {
      if (OB_ISNULL(m)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("nullptr", KR(ret), KP(m));
      } else if (server_addr == m->get_server()) {
        if (OB_FAIL(member.assign(ObMember(m->get_server(), m->get_timestamp())))) {
          LOG_WARN("fail to assign", KR(ret), KPC(m));
        }
        break;
      }
    } // end for
  }  // end else
  if (OB_FAIL(ret)) {
  } else if (member.is_valid()) {
    if (OB_FAIL(replica_member.init(member, REPLICA_TYPE_FULL))) {
      LOG_WARN("fail to init remove_member", KR(ret), K(member));
    }
  }
  return ret;
}

int DisasterRecoveryUtils::get_member_in_learner_list_(
    const share::ObLSInfo &ls_info,
    const common::ObAddr &server_addr,
    common::ObReplicaMember &replica_member)
{
  int ret = OB_SUCCESS;
  replica_member.reset();
  ObMember member;
  const ObLSReplica *leader_replica = nullptr;
  if (OB_UNLIKELY(!ls_info.is_valid()
               || !server_addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_info), K(server_addr));
  } else if (OB_FAIL(ls_info.find_leader(leader_replica))) {
    LOG_WARN("fail to find leader", KR(ret));
  } else if (OB_ISNULL(leader_replica)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("leader replica ptr is null", KR(ret), KP(leader_replica));
  } else {
    for (int64_t index = 0; OB_SUCC(ret) && index < leader_replica->get_learner_list().get_member_number(); ++index) {
      ObMember learner;
      if (OB_FAIL(leader_replica->get_learner_list().get_member_by_index(index, learner))) {
        LOG_WARN("fail to get learner by index", KR(ret), K(index));
      } else if (server_addr == learner.get_server()) {
        if (OB_FAIL(member.assign(learner))) {
          LOG_WARN("fail to assign", KR(ret), K(learner));
        }
        break;
      }
    } // end for
  } // end else
  if (OB_FAIL(ret)) {
  } else if (member.is_valid()) {
    common::ObReplicaType replica_type = REPLICA_TYPE_INVALID;
    if (member.is_columnstore()) {
      replica_type = REPLICA_TYPE_COLUMNSTORE;
    } else {
      replica_type = REPLICA_TYPE_READONLY;
    }
    if (OB_FAIL(replica_member.init(member, replica_type))) {
      LOG_WARN("fail to init remove_member", KR(ret), K(member), K(replica_type));
    }
  }
  return ret;
}

int DisasterRecoveryUtils::get_member_by_server(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObAddr &server_addr,
    common::ObReplicaMember &replica_member)
{
  int ret = OB_SUCCESS;
  share::ObLSInfo ls_info;
  const share::ObLSReplica *ls_replica_ptr = nullptr;
  replica_member.reset();
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))
   || OB_UNLIKELY(!ls_id.is_valid_with_tenant(tenant_id))
   || OB_UNLIKELY(!server_addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id), K(server_addr));
  } else if (OB_ISNULL(GCTX.lst_operator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.lst_operator_ is null", KR(ret), KP(GCTX.lst_operator_));
  } else if (OB_FAIL(GCTX.lst_operator_->get(GCONF.cluster_id, tenant_id, ls_id, share::ObLSTable::COMPOSITE_MODE, ls_info))) {
    LOG_WARN("get ls info failed", KR(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(get_member_in_member_list_(ls_info, server_addr, replica_member))) {
    LOG_WARN("get member in member list", KR(ret), K(tenant_id), K(ls_id), K(ls_info));
  } else if (replica_member.is_valid()) { // ok
  } else if (OB_FAIL(get_member_in_learner_list_(ls_info, server_addr, replica_member))) {
    LOG_WARN("get member in member list", KR(ret), K(tenant_id), K(ls_id), K(ls_info));
  } else if (replica_member.is_valid()) { // ok
  } else {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("fail to get member by server", KR(ret), K(tenant_id), K(ls_id), K(server_addr));
  }
  return ret;
}

int DisasterRecoveryUtils::check_service_epoch_exist_or_insert(
    common::ObMySQLTransaction &trans,
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t disaster_recovery_service_epoch = palf::INVALID_PROPOSAL_ID;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObServiceEpochProxy::get_service_epoch(
                          trans,
                          tenant_id,
                          ObServiceEpochProxy::DISASTER_RECOVERY_SERVICE_EPOCH,
                          disaster_recovery_service_epoch))) {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      FLOG_INFO("[DRTASK_NOTICE] insert service epoch", K(tenant_id));
      if (OB_FAIL(ObServiceEpochProxy::insert_service_epoch(
                          trans,
                          tenant_id,
                          ObServiceEpochProxy::DISASTER_RECOVERY_SERVICE_EPOCH,
                          0))) {
        LOG_WARN("fail to load service epoch in table", KR(ret), K(tenant_id));
      }
    } else {
      LOG_WARN("fail to get service epoch in table", KR(ret), K(tenant_id));
    }
  }
  return ret;
}

/*
The use of the __all_service_epoch table has two main functions:
1. Lock when reading and writing internal tables.
2. Preventing dual master problems.

Scenarios for reading and writing inner table:
1. dr worker insert waiting task.
2. manual operation insert waiting task.
3. cancel manual operation delete waiting task.
4. observer delete inprogress task.
5. dr mgr delete waiting task.
6. dr mgr update task from waiting to inprogress.

All the above scenarios require locking.
In the case 1, case 5 and case 6, need to verify the service epoch value to prevent dual-master issues.
In other case, no need to verify service epoch value.
*/

int DisasterRecoveryUtils::lock_service_epoch(
    common::ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    const int64_t service_epoch)
{
  int ret = OB_SUCCESS;
  int64_t disaster_recovery_service_epoch = palf::INVALID_PROPOSAL_ID;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_service_epoch_exist_or_insert(trans, tenant_id))) {
    LOG_WARN("failed to check and insert service epoch", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObServiceEpochProxy::select_service_epoch_for_update(
                         trans,
                         tenant_id,
                         ObServiceEpochProxy::DISASTER_RECOVERY_SERVICE_EPOCH,
                         disaster_recovery_service_epoch))) {
    LOG_WARN("fail to load service epoch in table", KR(ret), K(tenant_id));
  } else if (INVALID_DR_SERVICE_EPOCH_VALUE == service_epoch) {
    // if service_epoch is set to the default value of INVALID_DR_SERVICE_EPOCH_VALUE -1,
    // skip check service epoch value.
    LOG_TRACE("service_epoch is invalid, no need check");
  } else if (service_epoch != disaster_recovery_service_epoch) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("state not match, can not provide service",
              KR(ret), K(tenant_id), K(disaster_recovery_service_epoch), K(service_epoch));
  }
  return ret;
}

int DisasterRecoveryUtils::record_history_and_clean_task(
    ObDRTask &task,
    const int ret_code,
    const ObDRTaskRetComment &ret_comment)
{
  DEBUG_SYNC(BEFORE_DELETE_DRTASK_FROM_INNER_TABLE);
  int ret = OB_SUCCESS;
  uint64_t tenant_data_version = 0;
  ObMySQLTransaction trans;
  ObSqlString execute_result;
  ObLSReplicaTaskTableOperator task_table_operator;
  const uint64_t task_tenant_id = task.get_tenant_id();
  const uint64_t sql_tenant_id = gen_meta_tenant_id(task_tenant_id);
  if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(task));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rpc_proxy is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, sql_tenant_id))) {
    LOG_WARN("failed to start trans", KR(ret), K(sql_tenant_id));
  } else if (OB_FAIL(lock_service_epoch(trans, task_tenant_id, DisasterRecoveryUtils::INVALID_DR_SERVICE_EPOCH_VALUE))) {
    LOG_WARN("failed to lock server epoch", KR(ret), K(task));
  } else if (OB_FAIL(task_table_operator.delete_task(trans, task))) {
    // only when the task is successfully cleared, will the history table be written
    LOG_WARN("delete task failed", KR(ret), K(task));
  } else if (OB_FAIL(DisasterRecoveryUtils::build_execute_result(
                        ret_code, ret_comment, task.get_schedule_time(), execute_result))) {
    LOG_WARN("fail to build execute result", KR(ret), K(task), K(ret_code), K(ret_comment));
  } else if (OB_FAIL(task.set_execute_result(execute_result.ptr()))) {
    LOG_WARN("task set execute result failed", KR(ret), K(task), K(execute_result));
  } else if (OB_FAIL(task.log_execute_result())) {
    // record rs event, never fail
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(gen_meta_tenant_id(task_tenant_id), tenant_data_version))) {
    LOG_WARN("fail to get min data version", KR(ret), K(task));
  } else if (is_history_table_data_version_match(tenant_data_version)) {
    // record task history table
    ObDRLSReplicaTaskStatus task_status(ObDRLSReplicaTaskStatus::COMPLETED);
    if (OB_CANCELED == ret_code) {
      task_status = ObDRLSReplicaTaskStatus::CANCELED;
    } else if (OB_SUCCESS != ret_code) {
      task_status = ObDRLSReplicaTaskStatus::FAILED;
    }
    task.set_task_status(task_status);
    if (OB_FAIL(task_table_operator.insert_task(trans, task, true/*record_history*/))) {
      LOG_WARN("insert task failed", KR(ret), K(task));
    }
  }
  COMMIT_DISASTER_RECOVERY_UTILS_TRANS
  LOG_INFO("[DRTASK_NOTICE] record history and clean task", KR(ret), K(task));
  return ret;
}

int DisasterRecoveryUtils::send_rpc_to_cancel_task(
    const ObDRTask &task)
{
  int ret = OB_SUCCESS;
  ObLSCancelReplicaTaskArg rpc_arg;
  if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(task));
  } else if (OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rpc_proxy_ ptr is null", KR(ret), KP(GCTX.srv_rpc_proxy_));
  } else if (OB_FAIL(rpc_arg.init(task.get_task_id(), task.get_ls_id(), task.get_tenant_id()))) {
    LOG_WARN("fail to init arg", KR(ret), K(task));
  } else if (OB_FAIL(GCTX.srv_rpc_proxy_->to(task.get_dst_server()).by(task.get_tenant_id()).timeout(GCONF.rpc_timeout)
                                .ls_cancel_replica_task(rpc_arg))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_INFO("task not exist", KR(ret), K(task));
      LOG_USER_ERROR(OB_ENTRY_NOT_EXIST, "Task not exist");
    } else {
      LOG_WARN("fail to execute cancel task rpc", KR(ret), K(rpc_arg), K(task));
    }
  }
  return ret;
}

int DisasterRecoveryUtils::clean_task_while_task_finish(
    const share::ObTaskId &task_id,
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const int ret_code)
{
  // while task execute finish in observer
  // clean task in inner table, record rs event and task history table.
  const int64_t start_time = ObTimeUtility::fast_current_time();
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObMySQLTransaction trans;
  ObArray<ObDRTask*> dr_tasks;
  ObLSReplicaTaskTableOperator table_operator;
  const uint64_t sql_tenant_id = gen_meta_tenant_id(tenant_id);
  char task_id_to_set[OB_TRACE_STAT_BUFFER_SIZE] = "";
  common::ObArenaAllocator task_alloc("DRUtils", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  if (OB_UNLIKELY(task_id.is_invalid()
               || !is_valid_tenant_id(tenant_id)
               || !ls_id.is_valid_with_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(task_id), K(tenant_id), K(ls_id));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is nullptr", KR(ret), KP(GCTX.sql_proxy_));
  } else if (false == task_id.to_string(task_id_to_set, sizeof(task_id_to_set))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("convert task id to string failed", KR(ret), K(task_id));
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, sql_tenant_id))) {
    LOG_WARN("failed to start trans", KR(ret), K(sql_tenant_id));
  } else if (OB_FAIL(lock_service_epoch(trans, tenant_id, DisasterRecoveryUtils::INVALID_DR_SERVICE_EPOCH_VALUE))) {
    LOG_WARN("failed to lock server epoch", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sql.append_fmt("SELECT * FROM %s WHERE tenant_id = %ld AND ls_id = %lu AND task_id = '%s'",
                                    share::OB_ALL_LS_REPLICA_TASK_TNAME, tenant_id, ls_id.id(), task_id_to_set))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant_id), K(ls_id), K(task_id_to_set));
  } else if (OB_FAIL(table_operator.load_task_from_inner_table(*GCTX.sql_proxy_, tenant_id, sql, task_alloc, dr_tasks))) {
    LOG_WARN("failed to load task from inner table", KR(ret), K(tenant_id), K(sql));
  }
  COMMIT_DISASTER_RECOVERY_UTILS_TRANS
  if (OB_FAIL(ret)) {
  } else if (is_single_row(dr_tasks.count())) {
    ObDRTask* task_in_array = dr_tasks.at(0);
    if (OB_ISNULL(task_in_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task is nullptr", KR(ret), KP(task_in_array));
    } else if (OB_FAIL(record_history_and_clean_task(*task_in_array, ret_code, ObDRTaskRetComment::RECEIVE_FROM_STORAGE_RPC))) {
      LOG_WARN("failed to remove task and record task history", KR(ret), KP(task_in_array), K(ret_code));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task count is not single", KR(ret), K(task_id), K(tenant_id), K(ls_id), K(ret_code));
  }
  FREE_DISASTER_RECOVERY_UTILS_TASK_MEMORY
  const int64_t cost = ObTimeUtility::fast_current_time() - start_time;
  LOG_INFO("[DRTASK_NOTICE] clean task while task finish", KR(ret), K(task_id), K(cost));
  return ret;
}

int DisasterRecoveryUtils::check_tenant_enable_parallel_migration(
    const uint64_t &tenant_id,
    bool &enable_parallel_migration)
{
  int ret = OB_SUCCESS;
  const char *str = "auto";
  ObParallelMigrationMode mode;
  enable_parallel_migration = false;
  uint64_t tenant_data_version = 0;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    share::ObTenantRole tenant_role;
    if (OB_FAIL(GET_MIN_DATA_VERSION(gen_meta_tenant_id(tenant_id), tenant_data_version))) {
      LOG_WARN("fail to get min data version", KR(ret), K(tenant_id));
    } else if (!((tenant_data_version >= DATA_VERSION_4_3_5_0)
              || (tenant_data_version >= MOCK_DATA_VERSION_4_2_5_0 && tenant_data_version < DATA_VERSION_4_3_0_0))) {
      enable_parallel_migration = false;
    } else if (OB_UNLIKELY(!tenant_config.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant config is invalid", KR(ret), K(tenant_id));
    } else if (FALSE_IT(str = tenant_config->replica_parallel_migration_mode.str())) {
    } else if (OB_FAIL(mode.parse_from_string(str))) {
      LOG_WARN("mode parse failed", KR(ret), K(str));
    } else if (!mode.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("parallel migration mode is invalid", KR(ret), K(mode));
    } else if (mode.is_on_mode()) {
      enable_parallel_migration = true;
    } else if (mode.is_off_mode()) {
      enable_parallel_migration = false;
    } else if (mode.is_auto_mode()) {
      if (!is_user_tenant(tenant_id)) {
        // sys and meta tenant is primary tenant
        enable_parallel_migration = false;
      } else if (OB_FAIL(ObAllTenantInfoProxy::get_tenant_role(GCTX.sql_proxy_, tenant_id, tenant_role))) {
        LOG_WARN("fail to get tenant_role", KR(ret), K(tenant_id));
      } else if (!tenant_role.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant_role is invalid", KR(ret), K(tenant_role));
      } else if (tenant_role.is_primary()) {
        enable_parallel_migration = false;
      } else {
        enable_parallel_migration = true;
        // in auto mode, other tenant(clone restore standby) enable_parallel_migration is true
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("parallel migration mode is invalid", KR(ret), K(mode));
    }
    LOG_INFO("[DRTASK_NOTICE] check tenant enable_parallel_migration over", KR(ret),
              K(tenant_id), K(enable_parallel_migration), K(tenant_role), K(mode));
  }
  return ret;
}

} // end namespace rootserver
} // end namespace oceanbase
