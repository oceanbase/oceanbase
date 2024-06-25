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

#define USING_LOG_PREFIX BALANCE_TRANSFER

#include "rootserver/ob_tenant_transfer_service.h"
#include "observer/ob_server_struct.h" //GCTX
#include "lib/mysqlclient/ob_mysql_proxy.h" // ObISqlClient, SMART_VAR
#include "lib/utility/ob_tracepoint.h" // ERRSIM_POINT_DEF
#include "share/ls/ob_ls_info.h" // MemberList
#include "share/transfer/ob_transfer_task_operator.h" // ObTransferTaskOperator
#include "share/schema/ob_multi_version_schema_service.h"  // ObMultiSchemaService
#include "share/schema/ob_part_mgr_util.h" // ObPartitionSchemaIter
#include "share/tablet/ob_tablet_to_ls_operator.h" // ObTabletToLSOperator
#include "share/ob_rpc_struct.h" // ObStartTransferTaskArg
#include "share/ob_balance_define.h" // ObBalanceTaskID
#include "share/location_cache/ob_location_service.h"   // location_service_
#include "share/ob_srv_rpc_proxy.h" // srv_rpc_proxy_
#include "share/ob_share_util.h" // ObShareUtil
#include "storage/ob_common_id_utils.h" // ObCommonIDUtils
#include "storage/tablelock/ob_table_lock_service.h" // ObTableLockService
#include "observer/ob_inner_sql_connection.h" // ObInnerSQLConnection
#include "storage/ddl/ob_ddl_lock.h" // ObDDLLock
#include "storage/tablelock/ob_lock_inner_connection_util.h"
//#include "storage/high_availability/ob_transfer_struct.h"

namespace oceanbase
{
using namespace share;
using namespace share::schema;
using namespace common;
using namespace transaction;
using namespace transaction::tablelock;
using namespace observer;

namespace rootserver
{
#define TTS_INFO(fmt, args...) FLOG_INFO("[TENANT_TRANSFER] " fmt, ##args)

int ObTenantTransferService::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_FAIL(ObTenantThreadHelper::create(
      "TntTransf",
      lib::TGDefIDs::TenantTransferService,
      *this))) {
    LOG_WARN("failed to create thread", KR(ret));
  } else if (OB_FAIL(ObTenantThreadHelper::start())) {
    LOG_WARN("fail to start thread", KR(ret));
  } else {
    tenant_id_ = MTL_ID();
    sql_proxy_ = GCTX.sql_proxy_;
    is_inited_ = true;
  }
  return ret;
}

void ObTenantTransferService::destroy()
{
  is_inited_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  sql_proxy_ = NULL;
  ObTenantThreadHelper::destroy();
  TTS_INFO("ObTenantTransferService destory");
}

void ObTenantTransferService::do_work()
{
  int ret = OB_SUCCESS;
  bool tenant_schema_is_ready = false;
  if (IS_NOT_INIT || OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!is_user_tenant(tenant_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant transfer service must run on user tenant", KR(ret), K_(tenant_id));
  } else {
    const int64_t thread_count = lib::is_mini_mode() ? MINI_MODE_THREAD_COUNT : THREAD_COUNT;
    const uint64_t thread_idx = get_thread_idx();
    if (OB_UNLIKELY(thread_idx >= thread_count)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected tread_idx", KR(ret), K(thread_idx), K(thread_count), K_(tenant_id));
    } else {
      int tmp_ret = OB_SUCCESS;
      int64_t idle_time_us = IDLE_TIME_US;
      while (!has_set_stop()) {
        int64_t all_tasks_count = 0;
        int64_t thread_task_count = 0;
        idle_time_us = IDLE_TIME_US;
        ObCurTraceId::init(GCONF.self_addr_);
        ObArray<ObTransferTask::TaskStatus> task_status;
        if (OB_FAIL(check_tenant_schema_is_ready_(tenant_schema_is_ready))) {
          LOG_WARN("check tenant schema failed", KR(ret), K_(tenant_id), K(tenant_schema_is_ready));
        } else if (!tenant_schema_is_ready) {
          ret = OB_NEED_WAIT;
          if (REACH_TIME_INTERVAL(10 * 1000 * 1000L)) { // 10s
            LOG_WARN("tenant schema is not ready, need wait", KR(ret), K_(tenant_id));
          }
        } else if (OB_FAIL(ObTransferTaskOperator::get_all_task_status(
            *sql_proxy_,
            tenant_id_,
            task_status))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
            LOG_TRACE("no transfer task", KR(ret), K_(tenant_id));
          } else {
            LOG_WARN("fail to get", KR(ret), K_(tenant_id));
          }
        } else {
          all_tasks_count = task_status.count();
          int ret_fail = OB_SUCCESS;
          ARRAY_FOREACH_X(task_status, idx, cnt, !has_set_stop()) { // ignore ret
            const ObTransferTask::TaskStatus &task_stat = task_status.at(idx);
            if (thread_idx == task_stat.get_task_id().id() % thread_count) {
              ++thread_task_count;
              if (OB_FAIL(process_task_(task_stat))) {
                LOG_WARN("process task failed", KR(ret), K(task_stat), K(thread_idx));
                ret_fail = ret;
                ret = OB_SUCCESS;
              }
            } else {
              LOG_TRACE("task not for this thread", K(task_stat), K(thread_idx));
            }
          } // end ARRAY_FOREACH
          ret = OB_SUCC(ret) ? ret_fail : ret;
        }
        if (OB_FAIL(ret) && OB_NEED_WAIT != ret) {
          idle_time_us = BUSY_IDLE_TIME_US;
        }
        TTS_INFO("finish one round", KR(ret), K(all_tasks_count), K(thread_task_count), K(thread_idx));
        idle(idle_time_us);
      }// end while
    }
  }
}

int ObTenantTransferService::process_task_(const ObTransferTask::TaskStatus &task_stat)
{
  int ret = OB_SUCCESS;
  ObTransferStatus status;
  ObTransferPartList all_part_list;
  ObTransferPartList finished_part_list;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!task_stat.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task_stat", KR(ret), K(task_stat));
  } else if (task_stat.get_status().is_init_status()) {
    if (OB_FAIL(process_init_task_(task_stat.get_task_id()))) {
      LOG_WARN("fail to process init task", KR(ret), K(task_stat));
    }
  } else if (task_stat.get_status().is_finish_status()) {
    ObTransferTask transfer_task;//no used
    if (OB_FAIL(try_clear_transfer_task(
        task_stat.get_task_id(), transfer_task,
        all_part_list,
        finished_part_list))) {
      LOG_WARN("fail to process finish task", KR(ret), K(task_stat));
    }
  } else {
    // do nothing
  }
  return ret;
}

int ObTenantTransferService::process_init_task_(const ObTransferTaskID task_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTransferTask task;
  ObMySQLTransaction trans;
  ObTransferTaskComment result_comment = EMPTY_COMMENT;
  ObArray<ObTabletID> tablet_ids;
  ObTableLockOwnerID lock_owner_id;
  ObTransferPartList not_exist_part_list;
  ObTransferPartList lock_conflict_part_list;
  ObDisplayTabletList table_lock_tablet_list;
  ObTimeoutCtx ctx;
  bool need_wait = false;

  const int64_t start_time = ObTimeUtil::current_time();
  TTS_INFO("start to process init task", K(task_id), K(start_time));
  if (IS_NOT_INIT || OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(! task_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task_id", KR(ret), K(task_id));
  } else if (OB_FAIL(set_transaction_timeout_(ctx))) {
    LOG_WARN("set transaction timeout failed", KR(ret), K(ctx));
  } else if (OB_FAIL(trans.start(sql_proxy_, tenant_id_))) {
    LOG_WARN("trans start failed", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ObTransferTaskOperator::get(
      trans,
      tenant_id_,
      task_id,
      true/*for_update*/,
      task,
      0/*group_id*/))) {
    LOG_WARN("fail to get task", KR(ret), K_(tenant_id), K(task_id), K(task));
  } else if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid init task", KR(ret), K(task));
  } else if (!task.get_status().is_init_status()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("task status is not init", KR(ret), K(task));
  } else if (FALSE_IT(ObCurTraceId::set(task.get_trace_id()))) {
  } else if (OB_FAIL(check_if_need_wait_due_to_last_failure_(*sql_proxy_, task, need_wait))) {
    LOG_WARN("check if need wait due to last failure failed", KR(ret), K(task), K(need_wait));
  } else if (need_wait) {
    result_comment = WAIT_DUE_TO_LAST_FAILURE;
    ret = OB_NEED_RETRY;
    TTS_INFO("last task failed, need to process task later",
        KR(ret), K_(tenant_id), K(task), "result_comment", transfer_task_comment_to_str(result_comment));
  } else if (OB_FAIL(check_ls_member_list_(
      *sql_proxy_,
      task.get_src_ls(),
      task.get_dest_ls(),
      result_comment))) { // can't use trans
    LOG_WARN("fail to check ls member_list", KR(ret), K(task));
  } else if (EMPTY_COMMENT != result_comment) {
    ret = OB_NEED_RETRY;
    TTS_INFO("member_lists of src_ls and dest_ls are not same or there has inacitve server in member_list, need retry",
        KR(ret), K_(tenant_id), K(task), "result_comment", transfer_task_comment_to_str(result_comment));
  } else if (OB_FAIL(lock_table_and_part_(
      trans,
      task.get_src_ls(),
      task.get_part_list(),
      not_exist_part_list,
      lock_conflict_part_list,
      table_lock_tablet_list,
      tablet_ids,
      lock_owner_id))) {
    LOG_WARN("lock table and part failed", KR(ret), K(task), K(tablet_ids),
        K(lock_owner_id), K(not_exist_part_list), K(lock_conflict_part_list), K(table_lock_tablet_list));
  } else if (tablet_ids.empty()) {
    if (OB_UNLIKELY(not_exist_part_list.empty() && lock_conflict_part_list.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("not exist and lock conflict part list can not be empty when tablet_ids are empty",
          KR(ret), K(task), K(tablet_ids));
    } else if (OB_FAIL(ObTransferTaskOperator::finish_task_from_init(
        trans,
        tenant_id_,
        task_id,
        task.get_status(),
        task.get_part_list(),
        not_exist_part_list,
        lock_conflict_part_list,
        ObTransferStatus(ObTransferStatus::COMPLETED),
        OB_SUCCESS,
        ObTransferTaskComment::TASK_COMPLETED_AS_NO_VALID_PARTITION))) {
      LOG_WARN("finish task from init failed", KR(ret), K_(tenant_id),
          K(task), K(not_exist_part_list), K(lock_conflict_part_list));
    }
  } else if (OB_FAIL(generate_tablet_list_(tablet_ids, task.get_tablet_list()))) {
    LOG_WARN("fail to generate tablet list", KR(ret), K(task), K(tablet_ids));
  } else if (OB_FAIL(ObTransferTaskOperator::update_to_start_status(
      trans,
      tenant_id_,
      task.get_task_id(),
      task.get_status(),
      task.get_part_list(),
      not_exist_part_list,
      lock_conflict_part_list,
      table_lock_tablet_list,
      task.get_tablet_list(),
      ObTransferStatus(ObTransferStatus::START),
      lock_owner_id))) {
    LOG_WARN("fail to update task status from init to start", KR(ret), K_(tenant_id), K(task),
        K(not_exist_part_list), K(lock_conflict_part_list), K(table_lock_tablet_list));
  } else {
    // ObTransferEventRecorder::record_advance_transfer_status_event(
    //   tenant_id_, task.get_task_id(), task.get_src_ls(), task.get_dest_ls(),
    //   ObTransferStatus(ObTransferStatus::START), OB_SUCCESS);
    TTS_INFO("process init task success", K_(tenant_id), K(task),
        K(not_exist_part_list), K(lock_conflict_part_list), K(table_lock_tablet_list));
  }
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }

  // update comments for expected error codes
  if (OB_TMP_FAIL(update_comment_for_expected_errors_(ret, task_id, result_comment))) {
    LOG_WARN("update comment for expected errors failed", KR(tmp_ret), KR(ret),
        K_(tenant_id), K(task_id), "result_comment", transfer_task_comment_to_str(result_comment));
  }

  if (OB_SUCC(ret)) {
    DEBUG_SYNC(AFTER_TRANSFER_PROCESS_INIT_TASK_AND_BEFORE_NOTIFY_STORAGE);
  }
  if (OB_FAIL(ret) || task.get_tablet_list().empty()) {
  } else if (OB_FAIL(notify_storage_transfer_service_(task_id, task.get_src_ls()))) {
    LOG_WARN("notify storage transfer service failed", KR(ret), K(task_id), K(task));
  }
  TTS_INFO("process init task finish", KR(ret), K(task_id),
      "cost_time", ObTimeUtil::current_time() - start_time, K(task),
      K(not_exist_part_list), K(lock_conflict_part_list), K(table_lock_tablet_list));
  return ret;
}

int ObTenantTransferService::check_if_need_wait_due_to_last_failure_(
    common::ObISQLClient &sql_proxy,
    const ObTransferTask &task,
    bool &need_wait)
{
  int ret = OB_SUCCESS;
  need_wait = false;
  ObTransferTask last_task;
  int64_t finish_time = OB_INVALID_TIMESTAMP;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task", KR(ret), K(task));
  } else {
    int64_t wait_interval = 60 * 1000 * 1000L; // default 1m
    {
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
      if (tenant_config.is_valid()) {
        wait_interval = tenant_config->_transfer_task_retry_interval;
      }
    } // release guard
    if (OB_FAIL(ret)) {
    } else if (0 == wait_interval) {
      need_wait = false;
    } else if (OB_FAIL(ObTransferTaskOperator::get_last_task_by_balance_task_id(
        sql_proxy,
        tenant_id_,
        task.get_balance_task_id(),
        last_task,
        finish_time))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        need_wait = false;
      } else {
        LOG_WARN("get last task by balance task id failed",
            KR(ret), K(tenant_id_), K(task), K(last_task));
      }
    } else if (OB_UNLIKELY(!last_task.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("last task should be valid", KR(ret), K(task), K(last_task));
    } else if (last_task.get_status().is_failed_status()
        && ObTimeUtil::current_time() - finish_time < wait_interval) {
      need_wait = true;
      LOG_TRACE("last task failed, need wait", KR(ret),
          K(task), K(last_task), K(finish_time), K(wait_interval));
    }
  }
  return ret;
}

ERRSIM_POINT_DEF(EN_TENANT_TRANSFER_CHECK_LS_MEMBER_LIST_NOT_SAME);

// 1.check leader member_lists of src_ls and dest_ls are same
// 2.if member_lists are same, check that all servers in member_list are acitve
int ObTenantTransferService::check_ls_member_list_(
    common::ObISQLClient &sql_proxy,
    const ObLSID &src_ls,
    const ObLSID &dest_ls,
    ObTransferTaskComment &result_comment)
{
  int ret = OB_SUCCESS;
  result_comment = EMPTY_COMMENT;
  bool all_members_are_active = false;
  ObLSReplica::MemberList src_ls_member_list;
  ObLSReplica::MemberList dest_ls_member_list;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(get_member_lists_by_inner_sql_(
      sql_proxy,
      src_ls,
      dest_ls,
      src_ls_member_list,
      dest_ls_member_list))) {
    LOG_WARN("get member list by inner sql failed", KR(ret), K(src_ls), K(dest_ls));
  } else if (!ObLSReplica::servers_in_member_list_are_same(
      src_ls_member_list,
      dest_ls_member_list)) {
    // result 1: member_lists are not same
    result_comment = WAIT_FOR_MEMBER_LIST;
    LOG_WARN("member_list of src_ls and dest_ls are not same", KR(ret), K_(tenant_id), K(src_ls),
        K(dest_ls), K(src_ls_member_list), K(dest_ls_member_list), K(result_comment));
  } else if (OB_FAIL(ObLSReplica::check_all_servers_in_member_list_are_active(
      src_ls_member_list,
      all_members_are_active))) {
    LOG_WARN("check all servers in member list are active failed",
        KR(ret), K(src_ls_member_list), K(all_members_are_active));
  } else if (!all_members_are_active) {
    // result 2: member_lists are same, but server in member_list is inactive
    result_comment = INACTIVE_SERVER_IN_MEMBER_LIST;
    LOG_WARN("member_list has inactive server", KR(ret), K(src_ls),
        K(src_ls_member_list), K(all_members_are_active), K(result_comment));
  } else {
    // result 3: member_lists are same && all members are active
    result_comment = EMPTY_COMMENT;
    TTS_INFO("member_lists of src_ls and dest_ls are same and all members are acitve",
        KR(ret), K_(tenant_id), K(src_ls), K(dest_ls), K(all_members_are_active),
        K(src_ls_member_list), K(dest_ls_member_list), K(result_comment));
  }
  // just for debug
  if (OB_FAIL(ret)) {
  } else if (OB_IN_STOP_STATE == EN_TENANT_TRANSFER_CHECK_LS_MEMBER_LIST_NOT_SAME) {
    result_comment = INACTIVE_SERVER_IN_MEMBER_LIST;
    TTS_INFO("errsim tenant transfer check ls member list with inactive server", K(result_comment));
  } else if (OB_STATE_NOT_MATCH == EN_TENANT_TRANSFER_CHECK_LS_MEMBER_LIST_NOT_SAME) {
    result_comment = WAIT_FOR_MEMBER_LIST;
    TTS_INFO("errsim tenant transfer check ls member list not same", K(result_comment));
  }
  return ret;
}

// get ls leader member list of src_ls and dest_ls
int ObTenantTransferService::get_member_lists_by_inner_sql_(
    common::ObISQLClient &sql_proxy,
    const ObLSID &src_ls,
    const ObLSID &dest_ls,
    ObLSReplica::MemberList &src_ls_member_list,
    ObLSReplica::MemberList &dest_ls_member_list)
{
  int ret = OB_SUCCESS;
  src_ls_member_list.reset();
  dest_ls_member_list.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!src_ls.is_valid() || !dest_ls.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(src_ls), K(dest_ls));
  } else {
    SMART_VAR(ObISQLClient::ReadResult, result) {
      ObSqlString sql;
      common::sqlclient::ObMySQLResult *res = NULL;
      if (OB_FAIL(sql.assign_fmt(
          "SELECT PAXOS_MEMBER_LIST FROM %s WHERE TENANT_ID = %lu AND ROLE = 'LEADER'"
          " AND LS_ID IN (%ld, %ld) ORDER BY FIELD(LS_ID, %ld, %ld)",
          OB_GV_OB_LOG_STAT_TNAME,
          tenant_id_,
          src_ls.id(),
          dest_ls.id(),
          src_ls.id(),
          dest_ls.id()))) {
        LOG_WARN("fail to assign sql", KR(ret), K_(tenant_id), K(src_ls), K(dest_ls));
      } else if (OB_FAIL(sql_proxy.read(result, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(sql));
      } else if (OB_ISNULL(res = result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret), K(sql));
      } else if (OB_FAIL(construct_ls_member_list_(*res, src_ls_member_list))) {
        LOG_WARN("construct src ls member list failed", KR(ret), K_(tenant_id), K(src_ls));
      } else if (OB_FAIL(construct_ls_member_list_(*res, dest_ls_member_list))) {
        LOG_WARN("construct dest ls member list failed", KR(ret), K_(tenant_id), K(dest_ls));
      }
      // double check sql result
      if (OB_FAIL(ret)) {
        if (OB_UNLIKELY(OB_ITER_END == ret)) { // read less than two rows
          ret = OB_LEADER_NOT_EXIST;
          LOG_WARN("leader of src_ls or dest_ls not found", KR(ret), K_(tenant_id), K(src_ls),
              K(dest_ls), K(src_ls_member_list), K(dest_ls_member_list));
        } else {
          LOG_WARN("get ls member_list from inner table failed", KR(ret), K_(tenant_id),
              K(src_ls), K(dest_ls), K(src_ls_member_list), K(dest_ls_member_list));
        }
      } else if (OB_SUCC(res->next())) { // make sure read only two rows
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("read too much ls from inner table", KR(ret), K_(tenant_id),
            K(src_ls), K(dest_ls), K(src_ls_member_list), K(dest_ls_member_list), K(sql));
      } else if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("next failed", KR(ret), K_(tenant_id), K(src_ls), K(dest_ls),
            K(sql), K(src_ls_member_list), K(dest_ls_member_list));
      } else {
        ret = OB_SUCCESS;
      }
    } // end SMART_VAR
  }
  return ret;
}

int ObTenantTransferService::construct_ls_member_list_(
    sqlclient::ObMySQLResult &res,
    ObLSReplica::MemberList &ls_member_list)
{
  int ret = OB_SUCCESS;
  ls_member_list.reset();
  ObString ls_member_list_str;
  if (OB_FAIL(res.next())) {
    LOG_WARN("next failed", KR(ret));
  } else if (OB_FAIL(res.get_varchar("PAXOS_MEMBER_LIST", ls_member_list_str))) {
    LOG_WARN("fail to get PAXOS_MEMBER_LIST", KR(ret));
  } else if (OB_FAIL(ObLSReplica::text2member_list(
      to_cstring(ls_member_list_str),
      ls_member_list))) {
    LOG_WARN("text2member_list failed", KR(ret), K(ls_member_list_str));
  }
  return ret;
}

ERRSIM_POINT_DEF(EN_TENANT_TRANSFER_ALL_LIST_EMPTY);

int ObTenantTransferService::lock_table_and_part_(
    ObMySQLTransaction &trans,
    const share::ObLSID &src_ls,
    share::ObTransferPartList &part_list,
    share::ObTransferPartList &not_exist_part_list,
    share::ObTransferPartList &lock_conflict_part_list,
    share::ObDisplayTabletList &table_lock_tablet_list,
    ObIArray<ObTabletID> &tablet_ids,
    ObTableLockOwnerID &lock_owner_id)
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(BEFORE_TRANSFER_LOCK_TABLE_AND_PART);
  tablet_ids.reset();
  lock_owner_id.reset();
  ObTransferPartList ordered_part_list;
  ObArenaAllocator allocator;
  const int64_t start_time = ObTimeUtility::current_time();
  TTS_INFO("lock table and part start", KR(ret), K(src_ls), K(part_list), K(start_time));
  if (IS_NOT_INIT || OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(part_list.empty() || !src_ls.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid part_list", KR(ret), K(part_list), K(src_ls));
  } else if (OB_ISNULL(MTL(ObTableLockService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mtl ObTableLockService is null", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(MTL(ObTableLockService*)->generate_owner_id(lock_owner_id))) {
    LOG_WARN("generate owner id failed", KR(ret), K_(tenant_id), K(lock_owner_id));
  } else if (OB_FAIL(ordered_part_list.assign(part_list))) {
    LOG_WARN("assign failed", KR(ret), K(part_list), K(ordered_part_list));
  } else {
    allocator.set_tenant_id(tenant_id_);
    part_list.reset();
    ObSimpleTableSchemaV2 *table_schema = NULL;
    ObTransferPartInfo::Compare cmp;
    lib::ob_sort(ordered_part_list.begin(), ordered_part_list.end(), cmp);

    ARRAY_FOREACH(ordered_part_list, idx) {
      ObLSID ls_id;
      ObTabletID tablet_id;
      int64_t part_idx = OB_INVALID_INDEX;
      int64_t subpart_idx = OB_INVALID_INDEX;
      const ObTransferPartInfo &part_info = ordered_part_list.at(idx);
      const ObObjectID table_id = part_info.table_id();
      const ObObjectID part_object_id = part_info.part_object_id();
      bool is_not_exist = false;
      bool is_lock_conflict = false;
      if (OB_NOT_NULL(table_schema) && table_schema->get_table_id() == table_id) {
        // use previous table_schema
      } else if (OB_FAIL(get_latest_table_schema_(allocator, table_id, table_schema))) {
        if (OB_TABLE_NOT_EXIST == ret) {
          is_not_exist = true;
          ret = OB_SUCCESS;
          TTS_INFO("discard part_info because table does not exist",
              KR(ret), K(table_id), K_(tenant_id), K(is_not_exist));
        } else {
          LOG_WARN("get latest table schema failed", KR(ret), K_(tenant_id), K(table_id));
        }
      }

      if (OB_FAIL(ret) || OB_ISNULL(table_schema) || is_not_exist || is_lock_conflict) {
        // skip
      } else if (OB_FAIL(add_in_trans_lock_and_refresh_schema_(
          trans,
          src_ls,
          part_info,
          allocator,
          table_schema,
          tablet_id,
          part_idx,
          subpart_idx))) {
        if (OB_TRY_LOCK_ROW_CONFLICT == ret || OB_ERR_EXCLUSIVE_LOCK_CONFLICT == ret) {
          is_lock_conflict = true;
          TTS_INFO("lock conflict when adding in_trans lock",
              KR(ret), K(part_info), K(tablet_id), K(part_idx), K(subpart_idx), K(is_not_exist));
          ret = OB_SUCCESS;
        } else if (OB_ENTRY_NOT_EXIST == ret
            || OB_TABLE_NOT_EXIST == ret /*|| OB_TRY_LOCK_PART_NOT_EXIST == ret*/) {
          is_not_exist = true;
          TTS_INFO("part_info not exist when adding in_trans lock",
              KR(ret), K(part_info), K(tablet_id), K(part_idx), K(subpart_idx), K(is_not_exist));
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("add in trans lock and refresh schema failed", KR(ret),
              K(part_info), K(tablet_id), K(part_idx), K(subpart_idx));
        }
      } else if (ObTabletToLSTableOperator::get_ls_by_tablet(
          *sql_proxy_,
          tenant_id_,
          tablet_id,
          ls_id)) { // double check to make sure tablet exists on src_ls
        if (OB_ENTRY_NOT_EXIST == ret) {
          is_not_exist = true;
          TTS_INFO("discard part_info because tablet not exists",
              KR(ret), K(part_info), K(tablet_id), K(part_idx), K(subpart_idx), K(is_not_exist));
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("get ls by tablet failed", KR(ret), K_(tenant_id), K(tablet_id), K(ls_id));
        }
      } else if (ls_id != src_ls) {
        is_not_exist = true;
        TTS_INFO("discard part_info because tablet is not exist on src_ls",
            KR(ret), K_(tenant_id), K(tablet_id), K(part_info), K(ls_id), K(src_ls));
        ret = OB_SUCCESS;
      }

      if (OB_FAIL(ret) || is_not_exist || is_lock_conflict) {
        // skip
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema should not be null", KR(ret), K(part_info), K(tablet_id));
      } else if (OB_FAIL(add_out_trans_lock_(trans, lock_owner_id, *table_schema, part_info, tablet_id))) {
        LOG_WARN("add out trans table and online ddl lock failed",
            KR(ret), K(lock_owner_id), K(part_info), K(tablet_id));
      } else if (OB_FAIL(part_list.push_back(part_info))) { // add to part_list after lock successfully
        LOG_WARN("push back failed", KR(ret), K_(tenant_id), K(part_info), K(part_list));
      } else if (OB_FAIL(record_need_move_table_lock_tablet_(*table_schema, tablet_id, table_lock_tablet_list))) {
        LOG_WARN("record need move table lock tablet failed", KR(ret), K(tablet_id), K(table_lock_tablet_list));
      } else if (OB_FAIL(tablet_ids.push_back(tablet_id))) {
        LOG_WARN("push back failed", KR(ret), K(tablet_id), K(tablet_ids), K(part_info));
      } else if (OB_FAIL(generate_related_tablet_ids_(*table_schema, part_idx, subpart_idx, tablet_ids))) {
        LOG_WARN("generate related tablet_ids failed", KR(ret),
            "table_id", table_schema->get_table_id(), K(part_idx), K(subpart_idx), K(tablet_ids));
      }

      if (OB_FAIL(ret)) {
      } else if (is_not_exist) {
        if (OB_FAIL(not_exist_part_list.push_back(part_info))) {
          LOG_WARN("push back failed", KR(ret), K(part_info), K(not_exist_part_list));
        }
      } else if (is_lock_conflict) {
        if (OB_FAIL(lock_conflict_part_list.push_back(part_info))) {
          LOG_WARN("push back failed", KR(ret), K(part_info), K(lock_conflict_part_list));
        }
      }

      // Try to limit the number of tablet_list to _transfer_task_tablet_count_threshold.
      // This is not a precise limit. In the worst case, there will be
      // _transfer_task_tablet_count_threshold + 128(max index number) + 2(lob tablet number) tablets in tablet_list.
      if (OB_SUCC(ret) && tablet_ids.count() >= get_tablet_count_threshold_()) {
        break;
      }
    } // end ARRAY_FOREACH
  }
  // errsim
  if (OB_SUCC(ret) && EN_TENANT_TRANSFER_ALL_LIST_EMPTY) {
    part_list.reset();
    not_exist_part_list.reset();
    tablet_ids.reset();
  }
  TTS_INFO("lock table and part finish", KR(ret), "cost_time", ObTimeUtility::current_time() - start_time,
      K(part_list), K(not_exist_part_list), K(lock_conflict_part_list), K(tablet_ids), K(lock_owner_id));
  return ret;
}

int ObTenantTransferService::add_table_lock_(
    ObMySQLTransaction &trans,
    share::schema::ObSimpleTableSchemaV2 &table_schema,
    const share::ObTransferPartInfo &part_info,
    const bool is_out_trans,
    const ObTableLockOwnerID &lock_owner_id)
{
  int ret = OB_SUCCESS;
  ObInnerSQLConnection *conn = NULL;
  if (OB_UNLIKELY(!part_info.is_valid()
      || (table_schema.get_table_id() != part_info.table_id())
      || (is_out_trans && (!lock_owner_id.is_valid())))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(part_info),
        "table_id of schema", table_schema.get_table_id(), K(lock_owner_id));
  } else if (OB_ISNULL(conn = dynamic_cast<ObInnerSQLConnection *>(trans.get_connection()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("connection is null", KR(ret), K_(tenant_id), K(part_info));
  } else if (table_schema.is_offline_ddl_table()) {
    ret = OB_TRY_LOCK_ROW_CONFLICT;
    TTS_INFO("treat offline ddl hidden table as locked table, do not transfer", KR(ret), K(part_info));
  } else if (table_schema.is_global_index_table()
      || PARTITION_LEVEL_ZERO == table_schema.get_part_level()) {
    ObLockTableRequest table_lock_arg;
    table_lock_arg.lock_mode_ = ROW_SHARE;
    table_lock_arg.timeout_us_ = 0; // try lock
    if (table_schema.is_global_index_table()) {
      // lock primary table for global index table
      table_lock_arg.table_id_ = table_schema.get_data_table_id();
    } else {
      table_lock_arg.table_id_ = part_info.table_id();
    }
    if (is_out_trans) {
      table_lock_arg.owner_id_ = lock_owner_id;
      table_lock_arg.op_type_ = OUT_TRANS_LOCK;
    } else {
      table_lock_arg.op_type_ = IN_TRANS_COMMON_LOCK;
    }
    if (OB_FAIL(ObInnerConnectionLockUtil::lock_table(tenant_id_, table_lock_arg, conn))) {
      LOG_WARN("lock table failed", KR(ret), K(table_lock_arg));
    }
  } else {
    ObLockPartitionRequest part_lock_arg;
    part_lock_arg.lock_mode_ = ROW_SHARE;
    part_lock_arg.timeout_us_ = 0; // try lock
    part_lock_arg.table_id_ = part_info.table_id();
    part_lock_arg.part_object_id_ = part_info.part_object_id();
    if (is_out_trans) {
      part_lock_arg.owner_id_ = lock_owner_id;
      part_lock_arg.op_type_ = OUT_TRANS_LOCK;
    } else {
      part_lock_arg.op_type_ = IN_TRANS_COMMON_LOCK;
    }
    if (PARTITION_LEVEL_ONE == table_schema.get_part_level()) {
      if (OB_FAIL(ObInnerConnectionLockUtil::lock_partition(tenant_id_, part_lock_arg, conn))) {
        LOG_WARN("lock partition failed", KR(ret), K(part_lock_arg));
      }
    } else if (PARTITION_LEVEL_TWO == table_schema.get_part_level()) {
      if (OB_FAIL(ObInnerConnectionLockUtil::lock_subpartition(tenant_id_, part_lock_arg, conn))) {
        LOG_WARN("lock subpartition failed", KR(ret), K(part_lock_arg));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid part level", KR(ret), K(part_info), "part_level", table_schema.get_part_level());
    }
  }
  return ret;
}

int ObTenantTransferService::add_in_trans_lock_and_refresh_schema_(
    ObMySQLTransaction &trans,
    const share::ObLSID &src_ls,
    const share::ObTransferPartInfo &part_info,
    common::ObIAllocator &allocator,
    ObSimpleTableSchemaV2 *&table_schema,
    ObTabletID &tablet_id,
    int64_t &part_idx,
    int64_t &subpart_idx)
{
  int ret = OB_SUCCESS;
  const uint64_t table_id = part_info.table_id();
  const bool is_out_trans = false;
  const ObTableLockOwnerID invalid_owner_id;
  ObSimpleTableSchemaV2 *new_table_schema = NULL;
  const int64_t start_time = ObTimeUtility::current_time();
  TTS_INFO("add in trans lock and refresh schema start", K(start_time), K(part_info));
  if (IS_NOT_INIT || OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!part_info.is_valid() || !src_ls.is_valid()) || OB_ISNULL(table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(part_info), K(src_ls), K(table_schema));
  } else if (OB_FAIL(add_table_lock_(trans, *table_schema, part_info, is_out_trans, invalid_owner_id))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret
        && OB_ERR_EXCLUSIVE_LOCK_CONFLICT != ret
        && OB_TABLE_NOT_EXIST != ret
        && OB_ENTRY_NOT_EXIST != ret/*&& OB_TRY_LOCK_PART_NOT_EXIST != ret*/) {
      LOG_WARN("add in trans table lock failed",
          KR(ret), K(part_info), K(is_out_trans), K(invalid_owner_id));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (table_schema->is_global_index_table()) {
    DEBUG_SYNC(AFTER_TRANSFER_LOCK_TABLE_FOR_GLOBAL_INDEX);
  } else {
    DEBUG_SYNC(AFTER_TRANSFER_LOCK_TABLE_FOR_NORMAL_TABLE);
  }

  TTS_INFO("add in trans table lock finish", KR(ret),
      "cost_time", ObTimeUtility::current_time() - start_time, K(part_info));
  // After adding table lock, refresh schema to detect the concurrent online ddl
  if (FAILEDx(get_latest_table_schema_(allocator, table_id, new_table_schema))) {
    if (OB_TABLE_NOT_EXIST == ret) {
      // With no table lock on the global index table, we lock primary table for it.
      // So global index table may be deleted after adding table lock.
      if (table_schema->is_global_index_table()) {
        TTS_INFO("global index table not exist", KR(ret), K(part_info));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table cannot not exist", KR(ret), K(table_id), K_(tenant_id));
      }
    } else {
      LOG_WARN("get latest table schema failed", KR(ret), K(table_id));
    }
  } else if (OB_FAIL(get_tablet_and_partition_idx_by_object_id_(
      *new_table_schema,
      part_info.part_object_id(),
      tablet_id,
      part_idx,
      subpart_idx))) {
    // tablet may be deleted by online ddl (e.g. partition split) after adding table lock
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get tablet and partition idx by object_id failed",
          KR(ret), K(part_info), K(tablet_id), K(part_idx), K(subpart_idx));
    }
  } else if (OB_FAIL(ObOnlineDDLLock::lock_for_transfer_in_trans(
      tenant_id_,
      part_info.table_id(),
      tablet_id,
      0,/*try lock*/
      trans))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret || OB_ERR_EXCLUSIVE_LOCK_CONFLICT != ret) {
      LOG_WARN("lock for transfer in trans failed", KR(ret),
          K_(tenant_id), "table_id", part_info.table_id(), K(tablet_id));
    }
  } else {
    table_schema = new_table_schema;
  }
  // Double check for online ddl on global index table.
  // Global index table may be deleted after refreshing table schema and before adding online ddl lock.
  // Online ddl lock can not check tablet exist, so it may lock successfully while tablet not exists.
  if (OB_SUCC(ret) && table_schema->is_global_index_table()) {
    if (OB_FAIL(get_latest_table_schema_(allocator, table_id, new_table_schema))) {
      if (OB_TABLE_NOT_EXIST == ret) {
        TTS_INFO("global index table not exist", KR(ret), K(table_id), K(part_info));
      } else {
        LOG_WARN("get latest table schema failed", KR(ret), K(table_id), K(part_info));
      }
    }
  }
  TTS_INFO("add in trans lock and refresh schema finish", KR(ret),
      "cost_time", ObTimeUtility::current_time() - start_time,
      K(part_info), K(tablet_id), K(part_idx), K(subpart_idx));
  return ret;
}

int ObTenantTransferService::add_out_trans_lock_(
    ObMySQLTransaction &trans,
    const ObTableLockOwnerID &lock_owner_id,
    share::schema::ObSimpleTableSchemaV2 &table_schema,
    const share::ObTransferPartInfo &part_info,
    const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  const int64_t timeout_us = 0; // try lock
  const bool is_out_trans = true;
  const int64_t start_time = ObTimeUtility::current_time();
  if (OB_UNLIKELY(!lock_owner_id.is_valid() || !part_info.is_valid() || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(lock_owner_id), K(part_info), K(tablet_id));
  } else if (OB_FAIL(add_table_lock_(trans, table_schema, part_info, is_out_trans, lock_owner_id))) {
    LOG_WARN("add out trans table lock failed", KR(ret), K(part_info), K(is_out_trans), K(lock_owner_id));
  } else if (OB_FAIL(ObOnlineDDLLock::lock_for_transfer(
      tenant_id_,
      part_info.table_id(),
      tablet_id,
      lock_owner_id,
      timeout_us,
      trans))) {
    LOG_WARN("lock for transfer failed", KR(ret), K_(tenant_id),
        "table_id", part_info.table_id(), K(tablet_id), K(timeout_us));
  }
  TTS_INFO("add out trans lock finish", "cost_time", ObTimeUtility::current_time() - start_time,
      K(lock_owner_id), K(part_info), K(tablet_id));
  return ret;
}

// record tablet_id which adds part table lock successfully
// table lock on this tablet will be moved from src_ls to dest_ls in the transfer process
int ObTenantTransferService::record_need_move_table_lock_tablet_(
    share::schema::ObSimpleTableSchemaV2 &table_schema,
    const ObTabletID &tablet_id,
    share::ObDisplayTabletList &table_lock_tablet_list)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet_id", KR(ret), K(tablet_id));
  } else if (table_schema.is_global_index_table()) {
    // no table lock on global index table because we lock primay table for it
  } else if (PARTITION_LEVEL_ONE == table_schema.get_part_level()
      || PARTITION_LEVEL_TWO == table_schema.get_part_level()) {
    if (OB_FAIL(table_lock_tablet_list.push_back(ObDisplayTabletID(tablet_id)))) {
      LOG_WARN("push back failed", KR(ret), K(tablet_id));
    }
  } else if (PARTITION_LEVEL_ZERO == table_schema.get_part_level()) {
    // PARTITION_LEVEL_ZERO tables only need ROW_SHARE locks at the table level, but do not need ROW_SHARE locks at the Tablet level.
    // Therefore, it is not need to be moved during Tablet Transfer.
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected part level", KR(ret), K(table_schema));
  }
  return ret;
}

int ObTenantTransferService::generate_related_tablet_ids_(
    share::schema::ObSimpleTableSchemaV2 &table_schema,
    const int64_t part_idx,
    const int64_t subpart_idx,
    common::ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  ObArray<ObSimpleTableSchemaV2 *> related_table_schemas;
  ObArenaAllocator allocator;
  const uint64_t table_id = table_schema.get_table_id();
  const int64_t start_time = ObTimeUtility::current_time();
  if (IS_NOT_INIT || OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (FALSE_IT(allocator.set_tenant_id(tenant_id_))) {
  } else if (table_schema.is_global_index_table()) {
    // skip get related tables
  } else if (OB_UNLIKELY(! need_balance_table(table_schema))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("only get related tables for user table and other need balance table", KR(ret),
        K(table_id), K(table_schema));
  } else if (OB_FAIL(get_related_table_schemas_(*sql_proxy_, table_schema, allocator, related_table_schemas))) {
    LOG_WARN("fail to get related table schemas", KR(ret), K_(tenant_id), K(table_id));
  } else {
    ARRAY_FOREACH(related_table_schemas, idx) {
      ObSimpleTableSchemaV2 *related_table_schema = related_table_schemas.at(idx);
      ObTabletID related_tablet_id;
      if (OB_ISNULL(related_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("related table schema is null", KR(ret), "primary_table", table_id);
      } else if (OB_FAIL(get_tablet_by_partition_idx_(
          *related_table_schema,
          part_idx,
          subpart_idx,
          related_tablet_id))) {
        LOG_WARN("fail to get tablet by partition_idx", KR(ret), "related_table_id",
            related_table_schema->get_table_id(), K(part_idx), K(subpart_idx));
      } else if (OB_FAIL(tablet_ids.push_back(related_tablet_id))) {
        LOG_WARN("fail to push back", KR(ret), K(related_tablet_id), K(tablet_ids));
      }
    }
    TTS_INFO("get related tablet_ids", KR(ret), K(table_id), "related_table_count",
        related_table_schemas.count(), K(part_idx), K(subpart_idx), K(tablet_ids),
        "cost_time", ObTimeUtility::current_time() - start_time);
  }
  return ret;
}

int ObTenantTransferService::generate_tablet_list_(
    const ObIArray<ObTabletID> &tablet_ids,
    ObTransferTabletList &tablet_list)
{
  int ret = OB_SUCCESS;
  tablet_list.reset();
  ObArray<ObTabletToLSInfo> tablet_to_ls_infos;
  const int64_t start_time = ObTimeUtility::current_time();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(ObTabletToLSTableOperator::batch_get(
      *sql_proxy_,
      tenant_id_,
      tablet_ids,
      tablet_to_ls_infos))) {
    LOG_WARN("batch get failed", KR(ret), K_(tenant_id), K(tablet_ids), K(tablet_to_ls_infos));
  } else if (OB_UNLIKELY(tablet_to_ls_infos.count() != tablet_ids.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("count not match", KR(ret), "tablet_ids count", tablet_ids.count(),
        "tablet_to_ls_infos count", tablet_to_ls_infos.count(), K(tablet_ids), K(tablet_to_ls_infos));
  } else if (OB_FAIL(tablet_list.reserve(tablet_to_ls_infos.count()))) {
    LOG_WARN("reserve failed", KR(ret), "count", tablet_to_ls_infos.count());
  } else {
    ARRAY_FOREACH(tablet_to_ls_infos, idx) {
      const ObTabletToLSInfo &tablet_to_ls_info = tablet_to_ls_infos.at(idx);
      ObTransferTabletInfo transfer_tablet;
      if (OB_FAIL(transfer_tablet.init(
          tablet_to_ls_info.get_tablet_id(),
          tablet_to_ls_info.get_transfer_seq()))) {
        LOG_WARN("init failed", KR(ret), K(tablet_to_ls_info));
      } else if (OB_FAIL(tablet_list.push_back(transfer_tablet))) {
        LOG_WARN("fail to push_back", KR(ret), K(transfer_tablet), K(tablet_list));
      }
    }
    TTS_INFO("generate tablet_list finish", KR(ret),
        "cost_time", ObTimeUtility::current_time() - start_time,
        "tablet_ids count", tablet_ids.count(), K(tablet_ids),
        "tablet_list count", tablet_list.count(), K(tablet_list));
  }
  return ret;
}

int ObTenantTransferService::get_related_table_schemas_(
    common::ObISQLClient &sql_proxy,
    ObSimpleTableSchemaV2 &table_schema,
    ObArenaAllocator &allocator,
    ObArray<ObSimpleTableSchemaV2 *> &related_table_schemas)
{
  int ret = OB_SUCCESS;
  related_table_schemas.reset();
  const int64_t schema_version = INT64_MAX - 1; // get newest schema
  ObRefreshSchemaStatus schema_status;
  schema_status.tenant_id_ = tenant_id_;
  ObArray<uint64_t> related_table_ids;
  ObSchemaService *schema_service = NULL;
  const uint64_t primary_table_id = table_schema.get_table_id();
  ObArray<ObAuxTableMetaInfo> related_infos;
  if (OB_ISNULL(GCTX.schema_service_)
      || OB_ISNULL(schema_service = GCTX.schema_service_->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.schema_service_ is null", KR(ret));
  } else if (OB_FAIL(schema_service->fetch_aux_tables(
      schema_status,
      tenant_id_,
      table_schema.get_table_id(),
      schema_version,
      sql_proxy,
      related_infos))) {
    LOG_WARN("fail to fetch_aux_tables", KR(ret), K_(tenant_id),
        K(primary_table_id), K(schema_status), K(related_table_ids), K(schema_version));
  } else {
    TTS_INFO("get related table infos", K(primary_table_id), K(related_infos));
  }
  ARRAY_FOREACH(related_infos, idx) {
    const ObAuxTableMetaInfo &info = related_infos.at(idx);
    const uint64_t related_table_id = info.table_id_;
    if (is_related_table(info.table_type_, info.index_type_)) {
      if (OB_FAIL(related_table_ids.push_back(related_table_id))) {
        LOG_WARN("push back failed", KR(ret), K(related_table_id));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (related_table_ids.empty()) {
    // skip
  } else if (OB_FAIL(batch_get_latest_table_schemas_(
      allocator,
      related_table_ids,
      related_table_schemas))) {
    LOG_WARN("fail to batch get latest table schemas", KR(ret), K(related_table_ids));
  } else {
    TTS_INFO("get related table schema", K(primary_table_id),
        K(related_infos), "schema count", related_table_schemas.count());
  }
  return ret;
}

int ObTenantTransferService::get_tablet_and_partition_idx_by_object_id_(
    ObSimpleTableSchemaV2 &table_schema,
    const ObObjectID &part_object_id,
    ObTabletID &tablet_id,
    int64_t &part_idx,
    int64_t &subpart_idx)
{
  int ret = OB_SUCCESS;
  part_idx = OB_INVALID_INDEX;
  subpart_idx = OB_INVALID_INDEX;
  if (OB_UNLIKELY(OB_INVALID_ID == part_object_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(part_object_id));
  } else if (PARTITION_LEVEL_ZERO == table_schema.get_part_level()) {
    tablet_id = table_schema.get_tablet_id();
    part_idx = OB_INVALID_INDEX;
    subpart_idx = OB_INVALID_INDEX;
  } else {
    ObCheckPartitionMode check_partition_mode = CHECK_PARTITION_MODE_NORMAL;
    ObPartitionSchemaIter iter(table_schema, check_partition_mode);
    ObPartitionSchemaIter::Info info;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter.next_partition_info(info))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("switch the src partition info failed", KR(ret));
        }
      } else if (info.object_id_ == part_object_id) {
        tablet_id = info.tablet_id_;
        part_idx = info.part_idx_;
        subpart_idx = info.subpart_idx_;
        break;
      }
    }
    if (OB_UNLIKELY(OB_ITER_END == ret)
        || (OB_SUCC(ret) && OB_UNLIKELY(!tablet_id.is_valid()))) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("object id not found", KR(ret), "table_id",
          table_schema.get_table_id(), K(part_object_id));
    }
  }
  return ret;
}

int ObTenantTransferService::get_tablet_by_partition_idx_(
    ObSimpleTableSchemaV2 &table_schema,
    const int64_t part_idx,
    const int64_t subpart_idx,
    ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObBasePartition *base_part = NULL;
  if (PARTITION_LEVEL_ZERO == table_schema.get_part_level()) {
    tablet_id = table_schema.get_tablet_id();
  } else if (OB_FAIL(table_schema.get_part_by_idx(part_idx, subpart_idx, base_part))) {
    LOG_WARN("fail to get part by idx", KR(ret), K(part_idx), K(subpart_idx));
  } else if (OB_ISNULL(base_part) || OB_UNLIKELY(!base_part->get_tablet_id().is_valid())) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("tablet not found by part_idx", KR(ret), "table_id",
        table_schema.get_table_id(), K(part_idx), K(subpart_idx), K(tablet_id));
  } else {
    tablet_id = base_part->get_tablet_id();
  }
  return ret;
}

int ObTenantTransferService::check_tenant_schema_is_ready_(bool &is_ready)
{
  int ret = OB_SUCCESS;
  schema::ObMultiVersionSchemaService *schema_service = GCTX.schema_service_;
  schema::ObSchemaGetterGuard guard;
  const ObSimpleTenantSchema *tenant_schema = NULL;
  is_ready = false;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", KR(ret));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(guard.get_tenant_info(tenant_id_, tenant_schema))) {
    LOG_WARN("fail to get tenant schema", KR(ret), K_(tenant_id));
  } else if (OB_ISNULL(tenant_schema)) {
    is_ready = false;
  } else if (tenant_schema->is_normal()) {
    is_ready = true;
  }
  return ret;
}

int ObTenantTransferService::generate_transfer_task(
    ObMySQLTransaction &trans,
    const ObLSID &src_ls,
    const ObLSID &dest_ls,
    const ObTransferPartList &part_list,
    const ObBalanceTaskID balance_task_id,
    ObTransferTask &task)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT || OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KP(sql_proxy_));
  } else if (OB_UNLIKELY(!src_ls.is_valid()
      || !dest_ls.is_valid()
      || part_list.empty()
      || !balance_task_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(src_ls), K(dest_ls), K(part_list), K(balance_task_id));
  } else {
    task.reset();
    ObTransferTaskID task_id;
    ObCurTraceId::TraceId trace_id;
    trace_id.init(GCONF.self_addr_);
    ObTransferStatus status(ObTransferStatus::INIT);
    ObTransferPartList transfer_part_list;
    const int64_t part_count = min(get_tablet_count_threshold_(), part_list.count());
    uint64_t data_version = 0;
    if (OB_FAIL(transfer_part_list.reserve(part_count))) {
      LOG_WARN("reserve failed", KR(ret), K(part_count));
    } else if (OB_FAIL(ObShareUtil::fetch_current_data_version(*sql_proxy_, tenant_id_, data_version))) { // can not use trans
      LOG_WARN("fetch current data version failed", KR(ret), K(tenant_id_));
    } else if (OB_FAIL(ObTransferTaskOperator::generate_transfer_task_id(trans, tenant_id_, task_id))) {
      LOG_WARN("fail to generate transfer task id", KR(ret), K_(tenant_id));
    } else {
      // process from the back of part_list makes it easier to remove when task is done
      for (int64_t i = part_list.count() - 1; OB_SUCC(ret) && (i >= part_list.count() - part_count); --i) {
        if (OB_FAIL(transfer_part_list.push_back(part_list.at(i)))) {
          LOG_WARN("push back failed", KR(ret), K(i), K(part_list), K(transfer_part_list));
        }
      }
      if (FAILEDx(task.init(
          task_id,
          src_ls,
          dest_ls,
          transfer_part_list,
          status,
          trace_id,
          balance_task_id,
          data_version))) {
        LOG_WARN("init transfer task failed", KR(ret), K(task_id), K(src_ls),
          K(dest_ls), K(transfer_part_list), K(status), K(trace_id),
          K(balance_task_id), K(data_version));
      } else if (OB_FAIL(ObTransferTaskOperator::insert(trans, tenant_id_, task))) {
        LOG_WARN("insert failed", KR(ret), K_(tenant_id), K(task));
      }
    }
  }
  return ret;
}

int ObTenantTransferService::try_cancel_transfer_task(
    const ObTransferTaskID task_id)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObTransferTask task;
  int64_t create_time = OB_INVALID_TIMESTAMP;
  int64_t finish_time = OB_INVALID_TIMESTAMP;
  bool task_exist = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(! task_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task_id", KR(ret), K(task_id));
  } else if (OB_FAIL(trans.start(sql_proxy_, tenant_id_))) {
    LOG_WARN("trans start failed", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ObTransferTaskOperator::get(
      trans,
      tenant_id_,
      task_id,
      true/*for_update*/,
      task,
      0/*group_id*/))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to get task", KR(ret), K_(tenant_id), K(task_id), K(task));
    } else {
      ret = OB_SUCCESS;
      task_exist = false;
    }
  } else if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task", KR(ret), K(task));
  } else if (FALSE_IT(ObCurTraceId::set(task.get_trace_id()))) {
  } else if (task.get_status().is_canceled_status()) {
    // task is already canceled
  } else if (!task.get_status().is_init_status()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("task status is not init, can't cancel", KR(ret), K(task));
  } else if (OB_FAIL(ObTransferTaskOperator::finish_task_from_init(
      trans,
      tenant_id_,
      task_id,
      task.get_status(),
      task.get_part_list(),
      task.get_not_exist_part_list(),
      task.get_lock_conflict_part_list(),
      ObTransferStatus(ObTransferStatus::CANCELED),
      OB_CANCELED,
      ObTransferTaskComment::TASK_CANCELED))) {
    LOG_WARN("finish task from init failed", KR(ret), K_(tenant_id), K(task));
  }
  task.reset();
  if (OB_FAIL(ret) || !task_exist) {
  } else if (OB_FAIL(ObTransferTaskOperator::get_task_with_time(
      trans,
      tenant_id_,
      task_id,
      true/*for_update*/,
      task,
      create_time,
      finish_time))) {
    LOG_WARN("get task with time failed", KR(ret),
        K_(tenant_id), K(task_id), K(task), K(create_time), K(finish_time));
  } else if (OB_UNLIKELY(!task.get_status().is_canceled_status())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("task status is not canceled", KR(ret), K_(tenant_id), K(task_id), K(task));
  } else if (OB_FAIL(ObTransferTaskOperator::remove(trans, tenant_id_, task_id))) {
    LOG_WARN("remove task failed", KR(ret), K_(tenant_id), K(task_id), K(task));
  } else if (OB_FAIL(ObTransferTaskOperator::insert_history(
      trans,
      tenant_id_,
      task,
      create_time,
      finish_time))) {
    LOG_WARN("insert history failed", KR(ret),
        K_(tenant_id), K(task), K(create_time), K(finish_time));
  }

  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  TTS_INFO("cancel task finished", KR(ret), K(task_id), K(task_exist), K(task));
  return ret;
}

int ObTenantTransferService::try_clear_transfer_task(
    const ObTransferTaskID task_id,
    ObTransferTask &task,
    share::ObTransferPartList &all_part_list,
    share::ObTransferPartList &finished_part_list)
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(BEFORE_PROCESS_BALANCE_TASK_TRANSFER_END);
  if (OB_FAIL(unlock_and_clear_task_(task_id, task))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      int64_t create_time = OB_INVALID_TIMESTAMP;
      int64_t finish_time = OB_INVALID_TIMESTAMP;
      if (OB_FAIL(ObTransferTaskOperator::get_history_task(
          *sql_proxy_,
          tenant_id_,
          task_id,
          task,
          create_time,
          finish_time))) {
        LOG_WARN("get history task status failed", KR(ret), K_(tenant_id), K(task_id), K(task));
      } else {
        TTS_INFO("task is already cleared", KR(ret), K(task_id), K(task));
      }
    } else if (OB_NEED_RETRY == ret) {
      // skip
    } else {
      LOG_WARN("clear transfer task failed", KR(ret), K(task_id), K(task));
    }
  }

  if (FAILEDx(fill_finished_task_info_(task, finished_part_list, all_part_list))) {
    LOG_WARN("fill finished task info failed", KR(ret), K(task));
  } else {
    TTS_INFO("clear transfer task successfully", KR(ret), K(task_id),
        K(all_part_list), K(finished_part_list), K(task));
  }
  return ret;
}

int ObTenantTransferService::unlock_and_clear_task_(
    const ObTransferTaskID task_id,
    ObTransferTask &task)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  task.reset();
  int64_t create_time = OB_INVALID_TIMESTAMP;
  int64_t finish_time = OB_INVALID_TIMESTAMP;
  ObTimeoutCtx ctx;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!task_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task_id", KR(ret), K(task_id));
  } else if (OB_FAIL(set_transaction_timeout_(ctx))) {
    LOG_WARN("set transaction timeout failed", KR(ret), K(ctx));
  } else if (OB_FAIL(trans.start(sql_proxy_, tenant_id_))) {
    LOG_WARN("trans start failed", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ObTransferTaskOperator::get_task_with_time(
      trans,
      tenant_id_,
      task_id,
      true/*for_update*/,
      task,
      create_time,
      finish_time))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to get task", KR(ret), K_(tenant_id), K(task_id), K(task));
    }
  } else if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task", KR(ret), K(task));
  } else if (FALSE_IT(ObCurTraceId::set(task.get_trace_id()))) {
  } else if (!task.get_status().is_finish_status()) {
    ret = OB_NEED_RETRY;
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000L)) { // 10s
      TTS_INFO("task is not in finish status, can't clear", KR(ret), K(task));
    }
  } else if (OB_UNLIKELY(task.get_status().is_canceled_status())) {
    // if transfer task is canceled, it will be deleted at the same time
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("canceled status transfer task should not exist", KR(ret), K(task));
  } else if (OB_FAIL(unlock_table_and_part_(
      trans,
      task.get_part_list(),
      task.get_table_lock_owner_id()))) {
    LOG_WARN("unlock table and part failed", KR(ret), K(task));
  } else {
    if (OB_FAIL(ObTransferTaskOperator::remove(trans, tenant_id_, task_id))) {
      LOG_WARN("remove task failed", KR(ret), K_(tenant_id), K(task_id), K(task));
    } else if (OB_FAIL(ObTransferTaskOperator::insert_history(
        trans,
        tenant_id_,
        task,
        create_time,
        finish_time))) {
      LOG_WARN("insert history failed", KR(ret),
          K_(tenant_id), K(task), K(create_time), K(finish_time));
    }
  }
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }

  if (OB_SUCC(ret)) {
    TTS_INFO("clear transfer task success", KR(ret), K(task_id), K(task));
  }
  return ret;
}

int ObTenantTransferService::unlock_table_and_part_(
    ObMySQLTransaction &trans,
    const share::ObTransferPartList &part_list,
    const ObTableLockOwnerID &lock_owner_id)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_service = NULL;
  ObArenaAllocator allocator;
  ObTransferPartList ordered_part_list;
  if (IS_NOT_INIT || OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (part_list.empty()) {
    TTS_INFO("part list is empty, no need to unlock table and part",
        KR(ret), K(part_list), K(lock_owner_id));
  } else if (OB_ISNULL(GCTX.schema_service_)
      || OB_ISNULL(schema_service = GCTX.schema_service_->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.schema_service_ is null", KR(ret));
  } else if (OB_UNLIKELY(!lock_owner_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(part_list), K(lock_owner_id));
  } else if (OB_FAIL(ordered_part_list.assign(part_list))) {
    LOG_WARN("assign failed", KR(ret), K(part_list), K(lock_owner_id));
  } else if (OB_ISNULL(MTL(ObTableLockService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mtl ObTableLockService is null", KR(ret), K_(tenant_id));
  } else {
    allocator.set_tenant_id(tenant_id_);
    ObSimpleTableSchemaV2 *table_schema = NULL;
    const int64_t timeout_us = GCONF.internal_sql_execute_timeout;
    ObTransferPartInfo::Compare cmp;
    lib::ob_sort(ordered_part_list.begin(), ordered_part_list.end(), cmp);

    ARRAY_FOREACH(ordered_part_list, idx) {
      ObTabletID tablet_id;
      int64_t part_idx = OB_INVALID_INDEX;
      int64_t subpart_idx = OB_INVALID_INDEX;
      const ObTransferPartInfo &part_info = ordered_part_list.at(idx);
      const ObObjectID table_id = part_info.table_id();
      const ObObjectID part_object_id = part_info.part_object_id();
      if (OB_NOT_NULL(table_schema) && table_schema->get_table_id() == table_id) {
        // use previous table_schema
      } else if (OB_FAIL(get_latest_table_schema_(allocator, table_id, table_schema))) {
        if (OB_TABLE_NOT_EXIST == ret) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table cannot not exist", KR(ret), K(table_id), K_(tenant_id));
        } else {
          LOG_WARN("get table schema failed", KR(ret), K(table_id), K_(tenant_id));
        }
      }

      if (FAILEDx(get_tablet_and_partition_idx_by_object_id_(
          *table_schema,
          part_info.part_object_id(),
          tablet_id,
          part_idx,
          subpart_idx))) {
        LOG_WARN("get tablet and partition idx by object_id failed",
            KR(ret), K(part_info), K(tablet_id), K(part_idx));
      } else if (OB_FAIL(ObOnlineDDLLock::unlock_for_transfer(
          tenant_id_,
          table_schema->get_table_id(),
          tablet_id,
          lock_owner_id,
          timeout_us,
          trans))) {
        LOG_WARN("unlock online ddl lock for transfer failed", KR(ret), K_(tenant_id),
            "table_id", table_schema->get_table_id(), K(tablet_id), K(lock_owner_id), K(timeout_us));
      }
      if (FAILEDx(unlock_table_lock_(trans, *table_schema, part_info, lock_owner_id, timeout_us))) {
        LOG_WARN("unlock table lock failed", KR(ret), K(part_info), K(lock_owner_id), K(timeout_us));
      }
    } // end ARRAY_FOREACH
  }

  return ret;
}

int ObTenantTransferService::unlock_table_lock_(
    ObMySQLTransaction &trans,
    share::schema::ObSimpleTableSchemaV2 &table_schema,
    const share::ObTransferPartInfo &part_info,
    const ObTableLockOwnerID &lock_owner_id,
    const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  ObInnerSQLConnection *conn = NULL;
  if (OB_UNLIKELY(!part_info.is_valid()
      || (table_schema.get_table_id() != part_info.table_id())
      || (!lock_owner_id.is_valid())
      || (timeout_us < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(part_info),
        "table_id of schema", table_schema.get_table_id(), K(lock_owner_id), K(timeout_us));
  } else if (OB_ISNULL(conn = dynamic_cast<ObInnerSQLConnection *>(trans.get_connection()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("connection is null", KR(ret), K_(tenant_id), K(part_info));
 } else if (table_schema.is_global_index_table()
      || PARTITION_LEVEL_ZERO == table_schema.get_part_level()) {
    ObUnLockTableRequest unlock_table_arg;
    unlock_table_arg.lock_mode_ = ROW_SHARE;
    unlock_table_arg.timeout_us_ = timeout_us;
    unlock_table_arg.owner_id_ = lock_owner_id;
    unlock_table_arg.op_type_ = OUT_TRANS_UNLOCK;
    if (table_schema.is_global_index_table()) {
      unlock_table_arg.table_id_ = table_schema.get_data_table_id();
    } else {
      unlock_table_arg.table_id_ = part_info.table_id();
    }
    if (OB_FAIL(ObInnerConnectionLockUtil::unlock_table(tenant_id_, unlock_table_arg, conn))) {
      LOG_WARN("unlock table failed", KR(ret), K(unlock_table_arg));
    }
  } else {
    ObUnLockPartitionRequest unlock_part_arg;
    unlock_part_arg.lock_mode_ = ROW_SHARE;
    unlock_part_arg.timeout_us_ = timeout_us;
    unlock_part_arg.table_id_ = part_info.table_id();
    unlock_part_arg.part_object_id_ = part_info.part_object_id();
    unlock_part_arg.owner_id_ = lock_owner_id;
    unlock_part_arg.op_type_ = OUT_TRANS_UNLOCK;

    if (PARTITION_LEVEL_ONE == table_schema.get_part_level()) {
      if (OB_FAIL(ObInnerConnectionLockUtil::unlock_partition(tenant_id_, unlock_part_arg, conn))) {
        LOG_WARN("unlock partition failed", KR(ret), K(unlock_part_arg));
      }
    } else if (PARTITION_LEVEL_TWO == table_schema.get_part_level()) {
      if (OB_FAIL(ObInnerConnectionLockUtil::unlock_subpartition(tenant_id_, unlock_part_arg, conn))) {
        LOG_WARN("unlock subpartition failed", KR(ret), K(unlock_part_arg));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid part level", KR(ret), K(part_info), "part_level", table_schema.get_part_level());
    }
  }
  return ret;
}

int ObTenantTransferService::notify_storage_transfer_service_(
    const ObTransferTaskID task_id,
    const ObLSID &src_ls)
{
  int ret = OB_SUCCESS;
  obrpc::ObStartTransferTaskArg arg;
  ObAddr leader_addr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(! task_id.is_valid() || !src_ls.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(task_id), K(src_ls));
  } else if (OB_FAIL(arg.init(tenant_id_, task_id, src_ls))) {
    LOG_WARN("init ObStartTransferTaskArg failed", KR(ret), K(task_id), K(src_ls));
  } else if (OB_ISNULL(GCTX.location_service_) || OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX has null ptr", KR(ret), K(task_id), K_(tenant_id));
  } else {
    const int64_t RETRY_CNT_LIMIT = 10;
    int64_t retry_cnt = 0;
    do {
      if (OB_FAIL(ret)) {
        ob_usleep(1_s);
        ret = OB_SUCCESS;
      }
      if (FAILEDx(GCTX.location_service_->get_leader_with_retry_until_timeout(
          GCONF.cluster_id,
          tenant_id_,
          src_ls,
          leader_addr))) { // default 1s timeout
        LOG_WARN("get leader failed", KR(ret), K(task_id), "cluster_id", GCONF.cluster_id.get_value(),
            K_(tenant_id), K(src_ls), K(leader_addr));
      } else if (OB_FAIL(GCTX.srv_rpc_proxy_->to(leader_addr)
                                              .by(tenant_id_)
                                              .group_id(share::OBCG_TRANSFER)
                                              .start_transfer_task(arg))) {
        LOG_WARN("send rpc failed", KR(ret), K(task_id), K(src_ls), K(leader_addr), K(arg), K(retry_cnt));
      }
    } while (OB_FAIL(ret) && ++retry_cnt <= RETRY_CNT_LIMIT);
    TTS_INFO("send rpc to storage finished", KR(ret),
        K(task_id), K(src_ls), K(leader_addr), K(arg), K(retry_cnt));
  }
  return ret;
}

// all_part_list = part_list + not_exist_part_list + lock_conflict_part_list
// finished_part_list = not_exist_part_list + part_list (COMPLETED)
// finished_part_list = not_exist_part_list             (FAILED)
int ObTenantTransferService::fill_finished_task_info_(
    const share::ObTransferTask &task,
    share::ObTransferPartList &finished_part_list,
    share::ObTransferPartList &all_part_list)
{
  int ret = OB_SUCCESS;
  finished_part_list.reset();
  all_part_list.reset();
  const int64_t all_part_list_count = task.get_part_list().count()
      + task.get_not_exist_part_list().count() + task.get_lock_conflict_part_list().count();
  const int64_t finished_part_list_count = task.get_not_exist_part_list().count()
      + (task.get_status().is_completed_status() ? task.get_part_list().count() : 0);
  if (OB_UNLIKELY(!task.is_valid() || !task.get_status().is_finish_status())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task", KR(ret), K(task));
  } else if (OB_FAIL(all_part_list.reserve(all_part_list_count))) {
    LOG_WARN("reserve failed", KR(ret), K(all_part_list_count), K(task));
  } else if (OB_FAIL(finished_part_list.reserve(finished_part_list_count))) {
    LOG_WARN("reserve failed", KR(ret), K(finished_part_list_count), K(task));
  } else if (OB_FAIL(common::append(all_part_list, task.get_part_list()))) {
    LOG_WARN("append failed", KR(ret), K(all_part_list), K(task));
  } else if (OB_FAIL(common::append(all_part_list, task.get_not_exist_part_list()))) {
    LOG_WARN("append failed", KR(ret), K(all_part_list), K(task));
  } else if (OB_FAIL(common::append(all_part_list, task.get_lock_conflict_part_list()))) {
    LOG_WARN("append failed", KR(ret), K(all_part_list), K(task));
  } else if (OB_FAIL(common::append(finished_part_list, task.get_not_exist_part_list()))) {
    LOG_WARN("append failed", KR(ret), K(finished_part_list), K(task));
  } else if (task.get_status().is_completed_status()) {
    if (OB_FAIL(common::append(finished_part_list, task.get_part_list()))) {
      LOG_WARN("append failed", KR(ret), K(finished_part_list), K(task));
    }
  }
  return ret;
}

int ObTenantTransferService::get_latest_table_schema_(
    common::ObIAllocator &allocator,
    const ObObjectID &table_id,
    ObSimpleTableSchemaV2 *&table_schema)
{
  int ret = OB_SUCCESS;
  table_schema = NULL;
  ObSEArray<ObObjectID, 1> table_ids;
  ObSEArray<ObSimpleTableSchemaV2 *, 1> table_schemas;
  if (IS_NOT_INIT || OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table_id", KR(ret), K(table_id));
  } else if (OB_FAIL(ObSchemaUtils::get_latest_table_schema(
      *sql_proxy_,
      allocator,
      tenant_id_,
      table_id,
      table_schema))) {
    LOG_WARN("fail to get latest table schema", KR(ret), K_(tenant_id), K(table_id));
  }
  return ret;
}

// the count of table_schemas may be smaller than table_ids
int ObTenantTransferService::batch_get_latest_table_schemas_(
    common::ObIAllocator &allocator,
    const common::ObIArray<ObObjectID> &table_ids,
    common::ObIArray<ObSimpleTableSchemaV2 *> &table_schemas)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT || OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(table_ids.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table_ids", KR(ret), K(table_ids));
  } else if (OB_FAIL(ObSchemaUtils::batch_get_latest_table_schemas(
      *sql_proxy_,
      allocator,
      tenant_id_,
      table_ids,
      table_schemas))) {
    LOG_WARN("fail to get latest table schema", KR(ret), K_(tenant_id), K(table_ids));
  }
  return ret;
}

int ObTenantTransferService::set_transaction_timeout_(common::ObTimeoutCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    int64_t tx_timeout = 0;
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
    tx_timeout = tenant_config.is_valid()
        ? tenant_config->_transfer_process_lock_tx_timeout
        : 100 * 1000 * 1000L; // 100s
    if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, tx_timeout))) {
      LOG_WARN("set default timeout ctx failed", KR(ret), K(ctx), K(tx_timeout));
    }
  }
  return ret;
}

//       err        -->        comment
// OB_TRANS_TIMEOUT      TRANSACTION_TIMEOUT
// OB_TIMEOUT            TRANSACTION_TIMEOUT
// OB_NEED_RETRY         WAIT_FOR_MEMBER_LIST/INACTIVE_SERVER_IN_MEMBER_LIST
int ObTenantTransferService::update_comment_for_expected_errors_(
    const int err,
    const ObTransferTaskID &task_id,
    const ObTransferTaskComment &result_comment)
{
  int ret = OB_SUCCESS;
  ObTransferTaskComment actual_comment = EMPTY_COMMENT;
  ObTimeoutCtx ctx;
  if (IS_NOT_INIT || OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_SUCCESS == err) {
    // skip
  } else if (OB_UNLIKELY(!task_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task_id", KR(ret), K(task_id));
  } else if (OB_TRANS_TIMEOUT == err || OB_TIMEOUT == err) {
    actual_comment = TRANSACTION_TIMEOUT;
  } else if (OB_NEED_RETRY == err) {
    if (result_comment < EMPTY_COMMENT || result_comment >= MAX_COMMENT) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected comment with err", KR(ret), K(err), K(result_comment));
    } else {
      actual_comment = result_comment;
    }
  }
  if (OB_FAIL(ret) || EMPTY_COMMENT == actual_comment) {
    // do nothing
  } else if (OB_FAIL(ctx.set_timeout(GCONF.internal_sql_execute_timeout))) { // overwrite timeout
    LOG_WARN("set default timeout ctx failed", KR(ret), K(ctx), K_(tenant_id), K(task_id));
  } else if (OB_FAIL(ObTransferTaskOperator::update_comment(
      *sql_proxy_,
      tenant_id_,
      task_id,
      actual_comment))) {
    LOG_WARN("update comment failed", KR(ret), K_(tenant_id), K(task_id), K(actual_comment));
  }
  return ret;
}

int64_t ObTenantTransferService::get_tablet_count_threshold_() const
{
  const int64_t DEFAULT_TABLET_COUNT_THRESHOLD = 100;
  int64_t tablet_count_threshold = DEFAULT_TABLET_COUNT_THRESHOLD;
  if (is_valid_tenant_id(tenant_id_)) {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
    tablet_count_threshold = tenant_config.is_valid()
        ? tenant_config->_transfer_task_tablet_count_threshold
        : DEFAULT_TABLET_COUNT_THRESHOLD;
  }
  return tablet_count_threshold;
}

#undef TTS_INFO
} // end namespace rootserver
} // end namespace oceanbase
