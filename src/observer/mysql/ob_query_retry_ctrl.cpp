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

#define USING_LOG_PREFIX SERVER

#include "observer/mysql/ob_query_retry_ctrl.h"
#include "sql/ob_sql_context.h"
#include "sql/resolver/ob_stmt.h"
#include "storage/transaction/ob_trans_define.h"
#include "observer/mysql/ob_mysql_result_set.h"
#include "observer/ob_server_struct.h"
#include "observer/mysql/obmp_query.h"

namespace oceanbase {
using namespace common;
using namespace sql;
using namespace share::schema;
using namespace oceanbase::transaction;

namespace observer {

ObQueryRetryCtrl::ObQueryRetryCtrl()
    : curr_query_tenant_local_schema_version_(0),
      curr_query_tenant_global_schema_version_(0),
      curr_query_sys_local_schema_version_(0),
      curr_query_sys_global_schema_version_(0),
      retry_times_(0),
      retry_type_(RETRY_TYPE_NONE),
      retry_err_code_(OB_SUCCESS),
      in_async_execute_(false)
{}

ObQueryRetryCtrl::~ObQueryRetryCtrl()
{}

void ObQueryRetryCtrl::test_and_save_retry_state(const ObGlobalContext& gctx, const ObSqlCtx& ctx, ObResultSet& result,
    int err, int& client_ret, bool force_local_retry)
{
  int ret = OB_SUCCESS;
  client_ret = err;
  retry_type_ = RETRY_TYPE_NONE;
  retry_err_code_ = OB_SUCCESS;
  ObSQLSessionInfo* session = result.get_exec_context().get_my_session();
  bool expected_stmt = (ObStmt::is_dml_stmt(result.get_stmt_type()) ||
                        ObStmt::is_ddl_stmt(result.get_stmt_type(), result.has_global_variable()) ||
                        ObStmt::is_dcl_stmt(result.get_stmt_type()));
  const ObMultiStmtItem& multi_stmt_item = ctx.multi_stmt_item_;
  if (OB_ISNULL(session)) {
    client_ret = err;  // OOM
    retry_type_ = RETRY_TYPE_NONE;
    LOG_WARN("session is NULL, do not need retry", K(ret), K(err), K(client_ret));
  } else if (session->is_terminate(ret)) {
    LOG_WARN("execution was terminated", K(ret));
    client_ret = ret;  // session terminated
  } else if (result.is_pl_stmt(result.get_stmt_type()) && !session->get_pl_can_retry()) {
    LOG_WARN("current pl can not retry, commit may have occurred", K(ret), K(err), K(result.get_stmt_type()));
    client_ret = err;
    retry_type_ = RETRY_TYPE_NONE;
  } else if (THIS_WORKER.is_timeout()) {
    if (OB_ERR_INSUFFICIENT_PX_WORKER == err) {
      client_ret = OB_ERR_INSUFFICIENT_PX_WORKER;
    } else if (is_distributed_not_supported_err(err)) {
      client_ret = err;
    } else if ((result.get_exec_context().need_change_timeout_ret() &&
                   is_distributed_not_supported_err(session->get_retry_info().get_last_query_retry_err()))) {
      client_ret = session->get_retry_info().get_last_query_retry_err();
      log_distributed_not_supported_user_error(client_ret);
    } else {
      client_ret = OB_TIMEOUT;
    }
    if (is_try_lock_row_err(session->get_retry_info().get_last_query_retry_err())) {
      client_ret = OB_ERR_EXCLUSIVE_LOCK_CONFLICT;
    }
    retry_type_ = RETRY_TYPE_NONE;
    LOG_WARN("this worker is timeout, do not need retry",
        K(client_ret),
        K(err),
        K(retry_type_),
        K(THIS_WORKER.get_timeout_ts()),
        K(result.get_stmt_type()),
        K(result.get_exec_context().need_change_timeout_ret()),
        K(session->get_retry_info().get_last_query_retry_err()));
    if (session->get_retry_info().is_rpc_timeout() || is_transaction_rpc_timeout_err(err)) {
      int err1 = result.refresh_location_cache(true);
      if (OB_SUCCESS != err1) {
        LOG_WARN("fail to nonblock refresh location cache", K(err), K(err1));
      }
      LOG_WARN("sql rpc timeout, or trans rpc timeout, maybe location is changed, "
               "refresh location cache non blockly",
          K(client_ret),
          K(err),
          K(retry_type_),
          K(session->get_retry_info().is_rpc_timeout()));
    }
  } else if (ObStmt::is_ddl_stmt(result.get_stmt_type(), result.has_global_variable())) {
    if (OB_EAGAIN == err || OB_SNAPSHOT_DISCARDED == err || OB_ERR_PARALLEL_DDL_CONFLICT == err) {
      force_local_retry ? (void)(retry_type_ = RETRY_TYPE_LOCAL) : try_packet_retry(multi_stmt_item);
      LOG_WARN("retry for ddl", K(client_ret), K_(retry_type), K(err));
    } else {
      client_ret = err;
      retry_type_ = RETRY_TYPE_NONE;
      LOG_WARN(
          "ddl, and errno is not OB_EAGAIN or OB_SNAPSHOT_DISCARDED or OB_ERR_PARALLEL_DDL_CONFLICT, do not need retry",
          K(client_ret),
          K(err),
          K(retry_type_));
    }
  } else if (is_schema_error(err)) {

    if (NULL != gctx.schema_service_) {
      ObSchemaGetterGuard schema_guard;
      int64_t local_tenant_version_latest = 0;
      int64_t local_sys_version_latest = 0;
      if (OB_FAIL(gctx.schema_service_->get_tenant_schema_guard(session->get_effective_tenant_id(), schema_guard))) {
        client_ret = ret;
        retry_type_ = RETRY_TYPE_NONE;
        LOG_WARN("get schema guard failed", K(ret), K(client_ret), K(retry_type_));
      } else if (OB_FAIL(schema_guard.get_schema_version(
                     session->get_effective_tenant_id(), local_tenant_version_latest))) {
        LOG_WARN("fail get tenant schema version", K(ret));
      } else if (OB_FAIL(schema_guard.get_schema_version(OB_SYS_TENANT_ID, local_sys_version_latest))) {
        LOG_WARN("fail get sys schema version", K(ret));
      } else {
        int64_t local_tenant_version_start = get_tenant_local_schema_version();
        int64_t global_tenant_version_start = get_tenant_global_schema_version();
        int64_t local_sys_version_start = get_sys_local_schema_version();
        int64_t global_sys_version_start = get_sys_global_schema_version();
        if ((OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH == err) ||                  // (c1)
            (OB_SCHEMA_EAGAIN == err) ||                                   // (c5)
            (OB_SCHEMA_NOT_UPTODATE == err) ||                             // (c4)
            (global_tenant_version_start > local_tenant_version_start) ||  // (c2)
            (global_sys_version_start > local_sys_version_start) ||        // (c2)
            (local_tenant_version_latest > local_tenant_version_start) ||  // (c3)
            (local_sys_version_latest > local_sys_version_start)           // (c3)
        ) {
          if (retry_times_ < ObQueryRetryCtrl::MAX_SCHEMA_ERROR_LOCAL_RETRY_TIMES) {
            retry_type_ = RETRY_TYPE_LOCAL;
          } else {
            force_local_retry ? (void)(retry_type_ = RETRY_TYPE_LOCAL) : try_packet_retry(multi_stmt_item);
          }
          if (RETRY_TYPE_LOCAL == retry_type_) {
            sleep_before_local_retry(ObQueryRetryCtrl::RETRY_SLEEP_TYPE_LINEAR,
                WAIT_LOCAL_SCHEMA_REFRESHED_US,
                retry_times_,
                THIS_WORKER.get_timeout_ts());
          }
          LOG_WARN("schema error, need retry",
              K(client_ret),
              K(err),
              K(retry_type_),
              K(local_tenant_version_start),
              K(global_tenant_version_start),
              K(local_tenant_version_latest),
              K(local_sys_version_start),
              K(global_sys_version_start),
              K(local_sys_version_latest));
        } else {
          client_ret = err;
          retry_type_ = RETRY_TYPE_NONE;
          LOG_WARN("schema error, but do not need retry",
              K(client_ret),
              K(err),
              K(retry_type_),
              K(local_tenant_version_start),
              K(global_tenant_version_start),
              K(local_tenant_version_latest),
              K(local_sys_version_start),
              K(global_sys_version_start),
              K(local_sys_version_latest));
        }
      }
    } else {
      client_ret = OB_INVALID_ARGUMENT;
      retry_type_ = RETRY_TYPE_NONE;
      LOG_WARN("schema error, but schema service is NULL, do nothing", K(client_ret), K(err), K(retry_type_));
    }
  } else if (ObStmt::is_dml_write_stmt(result.get_stmt_type()) && is_server_down_error(err)) {
    int refresh_err = result.refresh_location_cache(true);  // non blocking
    if (OB_SUCCESS != refresh_err) {
      LOG_WARN("fail to nonblock refresh location cache", K_(retry_type), K(err), K(refresh_err));
    }
    bool autocommit = session->get_local_autocommit();
    ObPhyPlanType plan_type = result.get_physical_plan()->get_plan_type();
    bool in_transaction = session->is_in_transaction();
    if (ObSqlTransUtil::is_remote_trans(autocommit, in_transaction, plan_type)) {
      client_ret = err;
      retry_type_ = RETRY_TYPE_NONE;
      LOG_WARN("server down error, the write dml is remote, don't retry",
          K(autocommit),
          K(plan_type),
          K(in_transaction),
          K(client_ret),
          K(err),
          K(retry_type_));
    } else {
      force_local_retry ? (void)(retry_type_ = RETRY_TYPE_LOCAL) : try_packet_retry(multi_stmt_item);
      LOG_WARN("server down error, but the write dml can retry",
          K(autocommit),
          K(plan_type),
          K(in_transaction),
          K(retry_type_));

      if (retry_type_ == RETRY_TYPE_LOCAL) {
        sleep_before_local_retry(ObQueryRetryCtrl::RETRY_SLEEP_TYPE_LINEAR,
            WAIT_RETRY_WRITE_DML_US,
            retry_times_,
            THIS_WORKER.get_timeout_ts());
      }
    }
  } else if (expected_stmt &&
             (is_master_changed_error(err) || is_server_down_error(err) || is_partition_change_error(err) ||
                 is_server_status_error(err) || is_unit_migrate(err) || is_transaction_rpc_timeout_err(err) ||
                 is_has_no_readable_replica_err(err) || is_select_dup_follow_replic_err(err) ||
                 is_trans_stmt_need_retry_error(err))) {
    int err3 = OB_SUCCESS;
    if (is_location_leader_not_exist_error(err) || is_has_no_readable_replica_err(err)) {
      // session->get_retry_info_for_update().reset();
      session->get_retry_info_for_update().clear();
    }
    force_local_retry ? (void)(retry_type_ = RETRY_TYPE_LOCAL) : try_packet_retry(multi_stmt_item);
    if (RETRY_TYPE_LOCAL == retry_type_) {
      sleep_before_local_retry(ObQueryRetryCtrl::RETRY_SLEEP_TYPE_INDEX,
          WAIT_NEW_MASTER_ELECTED_US,
          retry_times_,
          THIS_WORKER.get_timeout_ts());
      bool is_direct_local_plan_retry =
          result.get_exec_context().get_direct_local_plan() && (OB_NOT_MASTER == err || OB_PARTITION_NOT_EXIST == err);
      if (!is_direct_local_plan_retry) {
        err3 = result.refresh_location_cache(false);
      }
      if (OB_SUCCESS != err3) {
        if (is_master_changed_error(err3) || is_server_down_error(err3) || is_partition_change_error(err3) ||
            is_server_status_error(err3) || is_unit_migrate(err3) || is_transaction_rpc_timeout_err(err3) ||
            is_has_no_readable_replica_err(err) || is_process_timeout_error(err3) ||
            is_get_location_timeout_error(err3) || is_snapshot_discarded_err(err3) ||
            is_select_dup_follow_replic_err(err3) || is_trans_stmt_need_retry_error(err3)) {
          retry_type_ = RETRY_TYPE_LOCAL;
          LOG_WARN("inner table location status changed, and fail to block refresh, "
                   "retry in local thread",
              K(client_ret),
              K(err),
              K(err3),
              K(retry_type_));
        } else {
          client_ret = err3;
          retry_type_ = RETRY_TYPE_NONE;
          LOG_WARN("fail to block refresh location cache, abort", K(client_ret), K(err), K(err3), K(retry_type_));
        }
      } else {
        retry_type_ = RETRY_TYPE_LOCAL;
        LOG_WARN("partition change or not master or no response, retry in local thread",
            K(client_ret),
            K(err),
            K(retry_type_));
      }
    } else {
      bool is_direct_local_plan_retry =
          result.get_exec_context().get_direct_local_plan() && (OB_NOT_MASTER == err || OB_PARTITION_NOT_EXIST == err);
      if (!is_direct_local_plan_retry) {
        err3 = result.refresh_location_cache(true);
        if (OB_SUCCESS != err3) {
          LOG_WARN("fail to nonblock refresh location cache", K(err), K(err3));
        }
      }
      LOG_INFO("partition change or not master or no response, reutrn it to packet queue to retry",
          K(client_ret),
          K(err),
          K(retry_type_),
          K(is_direct_local_plan_retry));
    }
  } else if (expected_stmt && is_get_location_timeout_error(err)) {
    force_local_retry ? (void)(retry_type_ = RETRY_TYPE_LOCAL) : try_packet_retry(multi_stmt_item);
    if (RETRY_TYPE_LOCAL == retry_type_) {
      sleep_before_local_retry(ObQueryRetryCtrl::RETRY_SLEEP_TYPE_INDEX,
          WAIT_NEW_MASTER_ELECTED_US,
          retry_times_,
          THIS_WORKER.get_timeout_ts());
      LOG_WARN("get location timeout, retry in local thread", K(client_ret), K(err), K(retry_type_), K(retry_times_));
    } else {
      LOG_WARN("get location timeout, return it to packet queue to retry",
          K(client_ret),
          K(err),
          K(retry_type_),
          K(retry_times_));
    }
  } else if (expected_stmt && is_data_not_readable_err(err)) {
    if (retry_times_ < ObQueryRetryCtrl::MAX_DATA_NOT_READABLE_ERROR_LOCAL_RETRY_TIMES) {
      retry_type_ = RETRY_TYPE_LOCAL;
    } else {
      force_local_retry ? (void)(retry_type_ = RETRY_TYPE_LOCAL) : try_packet_retry(multi_stmt_item);
    }
    LOG_DEBUG("has read not readable data, retry", K(client_ret), K(err), K(retry_type_), K(retry_times_));
  } else if (is_distributed_not_supported_err(err)) {
    int err4 = result.refresh_location_cache(true);
    if (OB_SUCCESS != err4) {
      LOG_WARN("fail to nonblock refresh location cache", K(err), K(err4));
    }
    force_local_retry ? (void)(retry_type_ = RETRY_TYPE_LOCAL) : try_packet_retry(multi_stmt_item);
    if (RETRY_TYPE_LOCAL == retry_type_) {
      sleep_before_local_retry(ObQueryRetryCtrl::RETRY_SLEEP_TYPE_INDEX,
          WAIT_NEW_MASTER_ELECTED_US,
          retry_times_,
          THIS_WORKER.get_timeout_ts());
      LOG_WARN("distributed strong read, maybe leader is switched or location cache is old, "
               "retry in local thread",
          K(client_ret),
          K(err),
          K(retry_type_),
          K(retry_times_),
          K(result.get_stmt_type()));
    } else {
      LOG_WARN("distributed strong read, maybe leader is switched or location cache is old, "
               "return it to packet queue to retry",
          K(client_ret),
          K(err),
          K(retry_type_),
          K(retry_times_),
          K(result.get_stmt_type()));
    }
  } else if (is_try_lock_row_err(err)) {
    // sql which in pl will local retry first. see ObInnerSQLConnection::process_retry.
    // sql which not in pl use the same strategy to avoid never getting the lock.
    if (force_local_retry || (retry_times_ <= 0 && !result.is_pl_stmt(result.get_stmt_type()))) {
      retry_type_ = RETRY_TYPE_LOCAL;
    } else {
      try_packet_retry(multi_stmt_item);
      if (RETRY_TYPE_LOCAL == retry_type_ && !multi_stmt_item.is_part_of_multi_stmt()) {
        // rewrite err
        client_ret = OB_ERR_EXCLUSIVE_LOCK_CONFLICT;
        retry_type_ = RETRY_TYPE_NONE;
        LOG_WARN("can not retry local", K(err), K(client_ret), K_(retry_type));
      }
    }
  } else if ((is_transaction_set_violation_err(err) && is_isolation_RR_or_SE(session->get_tx_isolation())) ||
             is_transaction_cannot_serialize_err(err)) {
    client_ret = OB_TRANS_CANNOT_SERIALIZE;
    retry_type_ = RETRY_TYPE_NONE;
    LOG_WARN("transaction cannot serialize", K(client_ret), K(err), K(retry_type_));
  } else if (is_snapshot_discarded_err(err) && is_isolation_RR_or_SE(session->get_tx_isolation())) {
    client_ret = err;
    retry_type_ = RETRY_TYPE_NONE;
    LOG_WARN("snapshot discarded in serializable isolation should not retry", K(client_ret), K(err), K(retry_type_));
  } else if (is_transaction_set_violation_err(err) || is_snapshot_discarded_err(err)) {
    force_local_retry ? (void)(retry_type_ = RETRY_TYPE_LOCAL) : try_packet_retry(multi_stmt_item);
    if (RETRY_TYPE_LOCAL == retry_type_) {
      sleep_before_local_retry(ObQueryRetryCtrl::RETRY_SLEEP_TYPE_LINEAR,
          WAIT_LOCAL_SCHEMA_REFRESHED_US,
          retry_times_,
          THIS_WORKER.get_timeout_ts());
      LOG_WARN("transaction set violation error, retry in local thread",
          K(client_ret),
          K(err),
          K(retry_type_),
          K(retry_times_));
    }
    //  } else if (is_transaction_cannot_serialize_err(err)) {
    //    client_ret = err;
    //    retry_type_ = RETRY_TYPE_NONE;
    //    LOG_WARN("transaction cannot serialize",
    //             K(client_ret), K(err), K(retry_type_));
  } else if (is_scheduler_thread_not_enough_err(err)) {
    if (force_local_retry) {
      retry_type_ = RETRY_TYPE_LOCAL;
    } else {
      try_packet_retry(multi_stmt_item);
      if (RETRY_TYPE_LOCAL == retry_type_) {
        client_ret = err;
        retry_type_ = RETRY_TYPE_NONE;
        LOG_WARN("can not retry local", K(err), K(client_ret), K_(retry_type));
      }
    }
  } else if (is_partition_splitting(err)) {
    force_local_retry ? (void)(retry_type_ = RETRY_TYPE_LOCAL) : try_packet_retry(multi_stmt_item);
    if (RETRY_TYPE_LOCAL == retry_type_) {
      sleep_before_local_retry(ObQueryRetryCtrl::RETRY_SLEEP_TYPE_LINEAR,
          WAIT_LOCAL_SCHEMA_REFRESHED_US,
          retry_times_,
          THIS_WORKER.get_timeout_ts());
    }
  } else if (is_gts_not_ready_err(err)) {
    force_local_retry ? (void)(retry_type_ = RETRY_TYPE_LOCAL) : try_packet_retry(multi_stmt_item);
    if (RETRY_TYPE_LOCAL == retry_type_) {
      sleep_before_local_retry(ObQueryRetryCtrl::RETRY_SLEEP_TYPE_LINEAR,
          WAIT_LOCAL_SCHEMA_REFRESHED_US,
          retry_times_,
          THIS_WORKER.get_timeout_ts());
    }
  } else if (is_weak_read_service_ready_err(err)) {
    LOG_WARN("get weak read cluster version, not ready", K(err), K(retry_type_), K(retry_times_));
    force_local_retry ? (void)(retry_type_ = RETRY_TYPE_LOCAL) : try_packet_retry(multi_stmt_item);
    if (RETRY_TYPE_LOCAL == retry_type_) {
      sleep_before_local_retry(ObQueryRetryCtrl::RETRY_SLEEP_TYPE_LINEAR,
          WAIT_LOCAL_SCHEMA_REFRESHED_US,
          retry_times_,
          THIS_WORKER.get_timeout_ts());
    }
  } else if (is_bushy_tree_not_suport_err(err)) {
    retry_type_ = RETRY_TYPE_LOCAL;
  } else if (is_px_need_retry(err)) {
    retry_type_ = RETRY_TYPE_LOCAL;
  } else if (is_static_engine_retry(err)) {
    retry_type_ = RETRY_TYPE_LOCAL;
    session->set_use_static_typing_engine(false);
  } else {
    if (is_timeout_err(err) && result.get_exec_context().need_change_timeout_ret() &&
        is_distributed_not_supported_err(session->get_retry_info().get_last_query_retry_err())) {
      client_ret = session->get_retry_info().get_last_query_retry_err();
      log_distributed_not_supported_user_error(client_ret);
    } else {
      client_ret = err;
    }

    if (is_timeout_err(err) && is_try_lock_row_err(session->get_retry_info().get_last_query_retry_err())) {
      client_ret = OB_ERR_EXCLUSIVE_LOCK_CONFLICT;
    }
    // client_ret = err;
    retry_type_ = RETRY_TYPE_NONE;
    LOG_WARN("do not need retry",
        K(client_ret),
        K(err),
        K(expected_stmt),
        K(THIS_WORKER.get_timeout_ts()),
        K(retry_type_),
        K(result.get_stmt_type()),
        K(result.get_exec_context().need_change_timeout_ret()),
        K(session->get_retry_info().get_last_query_retry_err()));
  }
  if (RETRY_TYPE_NONE != retry_type_ && !THIS_WORKER.need_retry() && THIS_WORKER.is_timeout()) {
    client_ret = OB_TIMEOUT;
    retry_type_ = RETRY_TYPE_NONE;
    LOG_WARN("this worker is timeout, do not need retry, reset retry_type_ to RETRY_TYPE_NONE",
        K(client_ret),
        K(retry_type_),
        K(THIS_WORKER.get_timeout_ts()));
  }

  retry_times_++;

  if (OB_TRY_LOCK_ROW_CONFLICT != client_ret) {
    LOG_INFO("check if need retry", K(client_ret), K(err), K(retry_type_), K_(retry_times), K(multi_stmt_item));
  }

  if (RETRY_TYPE_NONE != retry_type_) {
    session->get_retry_info_for_update().set_last_query_retry_err(err);
    if (OB_UNLIKELY(err != client_ret)) {
      LOG_ERROR("when need retry, client_ret must be equal to err", K(client_ret), K(err), K(retry_type_));
    }
  }

  if (OB_UNLIKELY(OB_SUCCESS == client_ret)) {
    LOG_ERROR("no mater need retry or not need retry, client_ret should not be OB_SUCCESS",
        K(client_ret),
        K(err),
        K(retry_type_));
  }
  if (RETRY_TYPE_NONE != retry_type_) {
    retry_err_code_ = client_ret;
  }
}

void ObQueryRetryCtrl::try_packet_retry(const ObMultiStmtItem& multi_stmt_item)
{
  if (multi_stmt_item.is_part_of_multi_stmt() && multi_stmt_item.get_seq_num() > 0) {
    retry_type_ = RETRY_TYPE_LOCAL;
  } else {
    retry_type_ = RETRY_TYPE_PACKET;
    if (!in_async_execute_ && !THIS_WORKER.set_retry_flag()) {
      retry_type_ = RETRY_TYPE_LOCAL;
      LOG_WARN("fail to set retry flag, force to do local retry");
    }
  }
}

void ObQueryRetryCtrl::log_distributed_not_supported_user_error(int err)
{
  int ret = err;
  switch (ret) {
    case OB_ERR_DISTRIBUTED_NOT_SUPPORTED: {
      LOG_USER_ERROR(OB_ERR_DISTRIBUTED_NOT_SUPPORTED, "strong consistency across distributed node");
      break;
    }
    default: {
      break;
    }
  }
}

void ObQueryRetryCtrl::sleep_before_local_retry(ObQueryRetryCtrl::RetrySleepType retry_sleep_type,
    int64_t base_sleep_us, int64_t retry_times, int64_t timeout_timestamp)
{
  int ret = OB_SUCCESS;
  int64_t sleep_us = 0;
  int64_t remain_us = timeout_timestamp - ObTimeUtility::current_time();
  switch (retry_sleep_type) {
    case ObQueryRetryCtrl::RETRY_SLEEP_TYPE_LINEAR: {
      sleep_us = base_sleep_us * linear_timeout_factor(retry_times);
      break;
    }
    case ObQueryRetryCtrl::RETRY_SLEEP_TYPE_INDEX: {
      sleep_us = base_sleep_us * index_timeout_factor(retry_times);
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected retry sleep type",
          K(ret),
          K(base_sleep_us),
          K(retry_sleep_type),
          K(retry_times),
          K(timeout_timestamp));
      break;
    }
  }
  if (OB_SUCC(ret)) {
    if (sleep_us > remain_us) {
      sleep_us = remain_us;
    }
    if (sleep_us > 0) {
      LOG_INFO("will sleep",
          K(sleep_us),
          K(remain_us),
          K(base_sleep_us),
          K(retry_sleep_type),
          K(retry_times),
          K(timeout_timestamp));
      usleep(static_cast<uint32_t>(sleep_us));
    } else {
      LOG_INFO("already timeout, do not need sleep",
          K(sleep_us),
          K(remain_us),
          K(base_sleep_us),
          K(retry_sleep_type),
          K(retry_times),
          K(timeout_timestamp));
    }
  }
}

bool ObQueryRetryCtrl::is_isolation_RR_or_SE(int32_t isolation)
{
  return (isolation == ObTransIsolation::REPEATABLE_READ || isolation == ObTransIsolation::SERIALIZABLE);
}

}  // namespace observer
}  // namespace oceanbase
