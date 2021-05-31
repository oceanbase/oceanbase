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

#define USING_LOG_PREFIX SQL_EXE

#include "sql/executor/ob_executor_rpc_processor.h"

#include "lib/stat/ob_session_stat.h"
#include "lib/utility/ob_tracepoint.h"
#include "storage/ob_partition_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "sql/executor/ob_executor_rpc_impl.h"
#include "sql/executor/ob_distributed_task_runner.h"
#include "sql/executor/ob_distributed_scheduler.h"
#include "sql/executor/ob_interm_result.h"
#include "sql/executor/ob_interm_result_manager.h"
#include "sql/executor/ob_task_runner_notifier_service.h"
#include "sql/engine/cmd/ob_kill_executor.h"
#include "sql/ob_sql_trans_util.h"
#include "sql/ob_end_trans_callback.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "sql/monitor/ob_exec_stat_collector.h"
#include "observer/mysql/ob_mysql_request_manager.h"
#include "observer/ob_server.h"
#include "lib/stat/ob_session_stat.h"
#include "sql/ob_sql.h"
#include "sql/engine/px/ob_granule_pump.h"
#include "sql/engine/table/ob_multi_part_table_scan.h"
#include "sql/engine/table/ob_multi_part_table_scan_op.h"
#include "sql/executor/ob_mini_task_executor.h"
#include "sql/ob_sql_mock_schema_utils.h"
#include "share/scheduler/ob_dag_scheduler.h"
#include "sql/executor/ob_bkgd_dist_task.h"
#include "rootserver/ob_root_service.h"
#include "storage/ob_partition_service.h"

namespace oceanbase {
using namespace share;
using namespace common;
using namespace observer;
using namespace storage;
using namespace transaction;
namespace sql {

ObWorkerSessionGuard::ObWorkerSessionGuard(ObSQLSessionInfo* session)
{
  THIS_WORKER.set_session(session);
  if (nullptr != session) {
    session->set_thread_id(GETTID());
  }
}

ObWorkerSessionGuard::~ObWorkerSessionGuard()
{
  THIS_WORKER.set_session(NULL);
}

int ObDistExecuteBaseP::init(ObTask& task)
{
  int ret = OB_SUCCESS;
  task.set_deserialize_param(exec_ctx_, phy_plan_);
  return ret;
}

int ObDistExecuteBaseP::param_preprocess(ObTask& task)
{
  bool table_version_equal = false;
  int ret = OB_SUCCESS;
  process_timestamp_ = ObTimeUtility::current_time();
  ObSQLSessionInfo* session_info = exec_ctx_.get_my_session();
  ObTaskExecutorCtx& executor_ctx = exec_ctx_.get_task_exec_ctx();
  int64_t tenant_local_version = -1;
  int64_t sys_local_version = -1;
  int64_t sys_schema_version = executor_ctx.get_query_tenant_begin_schema_version();
  int64_t tenant_schema_version = executor_ctx.get_query_sys_begin_schema_version();

  LOG_DEBUG("task submit", K(task), "ob_task_id", task.get_ob_task_id());
  if (OB_ISNULL(gctx_.schema_service_) || OB_ISNULL(gctx_.sql_engine_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("schema service or sql engine is NULL", K(ret), K(gctx_.schema_service_), K(gctx_.sql_engine_));
  } else if (OB_FAIL(gctx_.schema_service_->get_tenant_refreshed_schema_version(
                 session_info->get_effective_tenant_id(), tenant_local_version))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      tenant_local_version = OB_INVALID_VERSION;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get tenant refreshed schema version", K(ret), K(session_info->get_effective_tenant_id()));
    }
  } else if (OB_FAIL(gctx_.schema_service_->get_tenant_refreshed_schema_version(OB_SYS_TENANT_ID, sys_local_version))) {
    LOG_WARN("fail to get systenant refresh schema version", K(ret));
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (sys_schema_version > sys_local_version) {
    if (OB_FAIL(gctx_.schema_service_->async_refresh_schema(OB_SYS_TENANT_ID, sys_schema_version))) {
      LOG_WARN("fail to push back effective_tenant_id", K(ret), K(sys_schema_version), K(sys_local_version));
    }
  }

  if (OB_SUCC(ret) && tenant_schema_version != tenant_local_version) {
    if (OB_FAIL(
            gctx_.schema_service_->get_tenant_schema_guard(session_info->get_effective_tenant_id(), schema_guard_))) {
      LOG_WARN("fail to get schema guard",
          K(ret),
          K(tenant_local_version),
          K(sys_local_version),
          K(tenant_schema_version),
          K(sys_schema_version));
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_SUCC(sql::ObSQLUtils::check_table_version(
                   table_version_equal, task.get_des_phy_plan().get_dependency_table(), schema_guard_))) {
      if (!table_version_equal) {
        if (tenant_schema_version > tenant_local_version) {
          if (OB_FAIL(gctx_.schema_service_->async_refresh_schema(
                  session_info->get_effective_tenant_id(), tenant_schema_version))) {
            LOG_WARN("fail to push back effective_tenant_id",
                K(ret),
                K(session_info->get_effective_tenant_id()),
                K(tenant_schema_version),
                K(tenant_local_version));
          }
        }
      }
    } else {
      ret = OB_SUCCESS;
      if (tenant_schema_version > tenant_local_version) {
        if (OB_FAIL(gctx_.schema_service_->async_refresh_schema(
                session_info->get_effective_tenant_id(), tenant_schema_version))) {
          LOG_WARN("fail to push back effective_tenant_id",
              K(ret),
              K(session_info->get_effective_tenant_id()),
              K(tenant_schema_version),
              K(tenant_local_version));
        }
      }
    }
  }

  if (OB_SUCC(ret) && !table_version_equal) {
    if (OB_FAIL(gctx_.schema_service_->get_tenant_schema_guard(
            session_info->get_effective_tenant_id(), schema_guard_, tenant_schema_version, sys_schema_version))) {
      LOG_WARN("fail to get schema guard", K(ret), K(tenant_schema_version), K(sys_schema_version));
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else {
    ObVirtualTableCtx vt_ctx;
    vt_ctx.vt_iter_factory_ = &vt_iter_factory_;
    vt_ctx.session_ = exec_ctx_.get_my_session();
    vt_ctx.schema_guard_ = &schema_guard_;
    vt_ctx.partition_table_operator_ = gctx_.pt_operator_;
    sql_ctx_.session_info_ = exec_ctx_.get_my_session();
    sql_ctx_.schema_guard_ = &schema_guard_;
    exec_ctx_.set_addr(gctx_.self_addr_);
    exec_ctx_.set_sql_proxy(gctx_.sql_proxy_);
    exec_ctx_.set_virtual_table_ctx(vt_ctx);
    exec_ctx_.set_session_mgr(gctx_.session_mgr_);
    exec_ctx_.set_plan_cache_manager(gctx_.sql_engine_->get_plan_cache_manager());
    exec_ctx_.set_sql_ctx(&sql_ctx_);
    if (OB_ISNULL(gctx_.executor_rpc_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("executor rpc is NULL", K(ret));
    } else {
      executor_ctx.set_partition_service(gctx_.par_ser_);
      executor_ctx.set_vt_partition_service(gctx_.vt_par_ser_);
      executor_ctx.set_task_executor_rpc(*gctx_.executor_rpc_);
      executor_ctx.schema_service_ = gctx_.schema_service_;
      partition_location_cache_.init(gctx_.location_cache_, gctx_.self_addr_, &schema_guard_);
      executor_ctx.set_partition_location_cache(&partition_location_cache_);
    }
  }

  return E(EventTable::EN_5) ret;
}

int ObDistExecuteBaseP::execute_dist_plan(ObTask& task, ObTaskCompleteEvent& task_event)
{
  NG_TRACE(exec_dist_plan_begin);
  int ret = OB_SUCCESS;
  ObExecRecord exec_record;
  ObExecTimestamp exec_timestamp;
  exec_timestamp.exec_type_ = RpcProcessor;
  int err_code = OB_SUCCESS;
  int sp_ret = OB_SUCCESS;
  int ep_ret = OB_SUCCESS;
  ObTaskLocation task_loc;
  ObDistributedTaskRunner task_runner;
  common::ObPartitionArray participants;
  ObSEArray<ObSliceEvent, 1> slice_events;
  ObExecutorRpcImpl* rpc = gctx_.executor_rpc_;
  LOG_DEBUG("begin to execute async sql task", K(task));
  LOG_DEBUG("async task physical plan", "phy_plan", task.get_des_phy_plan());
  ObSQLSessionInfo* session = exec_ctx_.get_my_session();
  ObPhysicalPlanCtx* plan_ctx = exec_ctx_.get_physical_plan_ctx();
  const bool enable_perf_event = lib::is_diagnose_info_enabled();
  const bool enable_sql_audit = GCONF.enable_sql_audit && session->get_local_ob_enable_sql_audit();
  if (enable_perf_event) {
    exec_start_timestamp_ = ObTimeUtility::current_time();
    task_event.set_task_recv_done(exec_start_timestamp_);
  }
  int64_t execution_id = 0;
  ObWaitEventDesc max_wait_desc;
  ObWaitEventStat total_wait_desc;
  {
    ObMaxWaitGuard max_wait_guard(enable_perf_event ? &max_wait_desc : NULL);
    ObTotalWaitGuard total_wait_guard(enable_perf_event ? &total_wait_desc : NULL);
    if (enable_sql_audit) {
      exec_record.record_start();
    }
    if (OB_ISNULL(rpc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("rpc is NULL", K(ret), K(rpc));
    } else if (OB_ISNULL(session) || OB_ISNULL(plan_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("session or plan ctx is NULL", K(ret), K(session), K(plan_ctx), K(task));
    } else if (OB_ISNULL(gctx_.sql_engine_)) {
      LOG_WARN("fail to get sql engine", K(ret), K(gctx_.sql_engine_));
    } else {
      ObTaskRunnerNotifier run_notifier(session, exec_ctx_.get_session_mgr());
      ObTaskRunnerNotifierService::Guard notifier_guard(task.get_ob_task_id(), &run_notifier);
      ObWorkerSessionGuard worker_session_guard(session);
      ObSQLSessionInfo::LockGuard lock_guard(session->get_query_lock());
      session->set_peer_addr(task.get_ctrl_server());
      execution_id = gctx_.sql_engine_->get_execution_id();
      NG_TRACE_EXT(execute_async_task, OB_ID(task), task, OB_ID(stmt_type), task.get_des_phy_plan().get_stmt_type());
      trans_state_.clear_start_participant_executed();
      if (OB_SUCCESS != (sp_ret = get_participants(participants, task))) {
        LOG_WARN("fail to get participants", K(ret), K(sp_ret));
      } else {
        if (OB_LIKELY(task.get_des_phy_plan().is_need_trans()) && participants.count() > 0) {
          if (true == trans_state_.is_start_participant_executed()) {
            sp_ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("start_participant is executed", K(ret), K(sp_ret));
          } else if (OB_UNLIKELY(OB_SUCCESS !=
                                 (sp_ret = ObSqlTransControl::start_participant(exec_ctx_, participants, true)))) {
            LOG_WARN("fail to begin trans", K(ret), K(sp_ret));
          }
          trans_state_.set_start_participant_executed(OB_LIKELY(OB_SUCCESS == sp_ret));
        }
        // NG_TRACE(task_runner_exec_begin);
        ObSQLMockSchemaGuard mock_schema_guard;
        if (OB_LIKELY(OB_SUCCESS == sp_ret)) {
          if (OB_FAIL(ObSQLMockSchemaUtils::prepare_mocked_schemas(phy_plan_.get_mock_rowid_tables()))) {
            LOG_WARN("fail to prepare_mocked_schemas", K(ret), K(phy_plan_.get_mock_rowid_tables()));
          } else if (OB_SUCCESS != (err_code = task_runner.execute(exec_ctx_, phy_plan_, slice_events))) {
            LOG_WARN("fail to execute phy plan", K(err_code), K(task));
          }
        }
        // NG_TRACE(task_runner_exec_end);
        bool is_rollback = (OB_SUCCESS != sp_ret || OB_SUCCESS != err_code);
        if (OB_LIKELY(task.get_des_phy_plan().is_need_trans()) && participants.count() > 0 &&
            trans_state_.is_start_participant_executed() && trans_state_.is_start_participant_success() &&
            OB_SUCCESS != (ep_ret = ObSqlTransControl::end_participant(exec_ctx_, is_rollback, participants))) {
          LOG_WARN("fail to end trans", K(ret), K(ep_ret));
        }
      }
      err_code = (OB_SUCCESS != sp_ret) ? sp_ret : (OB_SUCCESS != err_code) ? err_code : ep_ret;
      if (OB_UNLIKELY(OB_SUCCESS != err_code)) {
        if (is_schema_error(err_code)) {
          err_code = OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH;
        }
      }
      if (enable_perf_event) {
        NG_TRACE_EXT(process_ret, OB_ID(process_ret), err_code);
        exec_end_timestamp_ = ObTimeUtility::current_time();
        record_exec_timestamp(true, exec_timestamp);
      }

      if (enable_sql_audit) {
        if (OB_ISNULL(session)) {
          LOG_WARN("invalid argument", K(ret), K(session));
        } else {
          ObAuditRecordData& audit_record = session->get_audit_record();
          audit_record.seq_ = 0;  // don't use now
          audit_record.status_ = (OB_SUCCESS == ret || common::OB_ITER_END == ret) ? obmysql::REQUEST_SUCC : ret;
          audit_record.client_addr_ = task.get_ctrl_server();
          audit_record.user_client_addr_ = task.get_ctrl_server();
          audit_record.user_group_ = THIS_WORKER.get_group_id();
          audit_record.execution_id_ = execution_id;
          audit_record.affected_rows_ = 0;
          audit_record.return_rows_ = 0;

          exec_record.max_wait_event_ = max_wait_desc;
          exec_record.wait_time_end_ = total_wait_desc.time_waited_;
          exec_record.wait_count_end_ = total_wait_desc.total_waits_;

          audit_record.plan_type_ = phy_plan_.get_plan_type();

          audit_record.is_executor_rpc_ = true;
          audit_record.is_inner_sql_ = session->is_inner();
          audit_record.is_hit_plan_cache_ = true;
          audit_record.is_multi_stmt_ = false;

          audit_record.exec_timestamp_ = exec_timestamp;
          audit_record.exec_record_ = exec_record;

          audit_record.update_stage_stat();

          ObSQLUtils::handle_audit_record(false, EXECUTE_DIST, *session, exec_ctx_);
        }
      }

      /**
       * some new participant may be introduced in task execution if there was any cascade
       * sql statement execution, so reply result even the err_code is OB_ERR_INTERRUPTED.
       */
      task_loc.set_ob_task_id(task.get_ob_task_id());
      task_loc.set_server(task.get_runner_server());
      if (OB_FAIL(task_event.init(task_loc, err_code))) {
        LOG_WARN("fail to init task event", K(ret), K(task_loc), K(err_code));
      } else if (OB_FAIL(task_event.merge_trans_result(session->get_trans_result()))) {
        LOG_WARN("merge trans result failed", K(ret));
      } else if (OB_FAIL(task_event.merge_implicit_cursors(plan_ctx->get_implicit_cursor_infos()))) {
        LOG_WARN("merge implicit cursors failed", K(ret));
      } else {
        LOG_DEBUG("execute_dist_plan",
            "cur_stmt",
            session->get_current_query_string(),
            K(task_event.get_trans_result()),
            K(session->get_trans_result()));
        for (int64_t i = 0; OB_SUCC(ret) && i < slice_events.count(); i++) {
          if (OB_FAIL(task_event.add_slice_event(slice_events.at(i)))) {
            LOG_WARN("fail to add sliece event", K(ret), K(i));
          }
        }
        if (OB_SUCC(ret)) {
          task_event.set_result_send_begin(ObTimeUtility::current_time());
          if (sync_) {
            ObExecutorRpcCtx rpc_ctx(session->get_effective_tenant_id(),
                plan_ctx->get_timeout_timestamp(),
                exec_ctx_.get_task_exec_ctx().get_min_cluster_version(),
                NULL,
                exec_ctx_.get_my_session(),
                plan_ctx->is_plain_select_stmt());
            if (OB_FAIL(rpc->task_complete(rpc_ctx, task_event, task.get_ctrl_server()))) {
              LOG_WARN("fail notify ctrl server task execute status",
                  K(ret),
                  K(task_event),
                  "ctrl_server",
                  task.get_ctrl_server());
            }
          } else {
          }
        }
      }
      //      }
    }
  }

  // vt_iter_factory_.reuse();
  phy_plan_.destroy();

  NG_TRACE(exec_dist_plan_end);
  if (exec_end_timestamp_ - exec_start_timestamp_ >= ObServerConfig::get_instance().trace_log_slow_query_watermark) {
    // slow mini task, print trace info
    FORCE_PRINT_TRACE(THE_TRACE, "[slow distributed task]");
  }
  return ret;
}

int ObDistExecuteBaseP::get_participants(ObPartitionIArray& participants, const ObTask& task)
{
  int ret = OB_SUCCESS;
  participants.reset();
  const ObPartitionIArray& pkeys = task.get_partition_keys();
  if (pkeys.count() > 0) {
    for (int64_t i = 0; OB_SUCC(ret) && i < pkeys.count(); ++i) {
      const ObPartitionKey& pkey = pkeys.at(i);
      if (is_virtual_table(pkey.get_table_id())) {
        // skip virtual table
      } else if (OB_FAIL(ObSqlTransControl::append_participant_to_array_distinctly(participants, pkey))) {
        LOG_WARN("fail to push pkey into participants", K(ret), K(pkey));
      }
    }
  } else {
    if (OB_FAIL(
            ObTaskExecutorCtxUtil::extract_server_participants(exec_ctx_, task.get_runner_server(), participants))) {
      LOG_WARN("fail extract participants", K(ret), K(task.get_runner_server()));
    }
  }
  return ret;
}

int ObRpcDistExecuteP::init()
{
  return ObDistExecuteBaseP::init(arg_);
}

int ObRpcDistExecuteP::before_process()
{
  int ret = OB_SUCCESS;
  get_exec_ctx().show_session();
  if (OB_FAIL(param_preprocess(arg_))) {
    LOG_WARN("Failed to pre process", K(ret));
  }
  return ret;
}

int ObRpcDistExecuteP::process()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObRpcDistExecuteP::after_process()
{
  int ret = OB_SUCCESS;
  ObTaskCompleteEvent task_event;
  if (OB_FAIL(execute_dist_plan(arg_, task_event))) {
    LOG_WARN("Failed to execute distribute plan", K(ret));
  }
  get_exec_ctx().hide_session();
  return ret;
}

void ObRpcDistExecuteP::cleanup()
{
  get_exec_ctx().cleanup_session();
  obrpc::ObRpcProcessor<obrpc::ObExecutorRpcProxy::ObRpc<obrpc::OB_DIST_EXECUTE>>::cleanup();
  return;
}

int ObRpcAPDistExecuteP::init()
{
  return ObDistExecuteBaseP::init(arg_);
}

int ObRpcAPDistExecuteP::before_process()
{
  int ret = OB_SUCCESS;
  get_exec_ctx().show_session();
  if (OB_FAIL(param_preprocess(arg_))) {
    LOG_WARN("Failed to pre process", K(ret));
  }
  return ret;
}

int ObRpcAPDistExecuteP::process()
{
  int ret = OB_SUCCESS;
  ObTaskCompleteEvent& task_event = result_;
  ObTask& task = arg_;
  ObExecContext* exec_ctx = NULL;
  ObSQLSessionInfo* session = NULL;
  if (OB_ISNULL(exec_ctx = task.get_exec_context())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("exec ctx is NULL", K(ret), K(task));
  } else if (OB_ISNULL(session = exec_ctx->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("session is NULL", K(ret), K(task));
  } else {
  }

  if (OB_SUCC(ret)) {

    ObSessionStatEstGuard stat_est_guard(session->get_effective_tenant_id(), session->get_sessid());
  }

  if (OB_FAIL(execute_dist_plan(task, task_event))) {
    LOG_WARN("Failed to execute distribute plan", K(ret));
  }
  return ret;
}

int ObRpcAPDistExecuteP::after_process()
{
  return OB_SUCCESS;
}

int ObRpcAPDistExecuteP::before_response()
{
  ObTaskCompleteEvent& task_event = result_;
  if (!task_event.is_valid()) {
    /*
     * error may happen before process(), like deserialize() or before_process()
     * failed, but now the task location and err_code_ in task_event will be set
     * in process() only, so we need set them here, otherwise the distributed
     * scheduler can not call merge_trans_result(), then the wait_all_task() of
     * transactio result collector will wait until timeout.
     */
    ObTaskLocation task_loc;
    ObTask& task = arg_;
    int err_code = (OB_SUCCESS != before_process_ret_ ? before_process_ret_ : process_ret_);
    task_loc.set_ob_task_id(task.get_ob_task_id());
    task_loc.set_server(task.get_runner_server());
    task_event.init(task_loc, err_code);
  }
  get_exec_ctx().hide_session();
  return OB_SUCCESS;
}

void ObRpcAPDistExecuteP::cleanup()
{
  get_exec_ctx().cleanup_session();
  obrpc::ObRpcProcessor<obrpc::ObExecutorRpcProxy::ObRpc<obrpc::OB_AP_DIST_EXECUTE>>::cleanup();
  return;
}

void ObRpcAPDistExecuteCB::on_timeout()
{
  int ret = OB_SUCCESS;
  int t_ret = OB_TIMEOUT;
  ObDistributedSchedulerManager* sched_mgr = ObDistributedSchedulerManager::get_instance();
  if (OB_ISNULL(sched_mgr)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail get ObDistributedSchedulerManager instance", K(ret));
  } else if (FALSE_IT(ObRpcAPMiniDistExecuteCB::deal_with_rpc_timeout_err(timeout_ts_, t_ret))) {
  } else if (OB_FAIL(sched_mgr->signal_schedule_error(task_loc_.get_execution_id(), t_ret, task_loc_.get_server()))) {
    LOG_WARN("fail to signal scheduler", K(ret), K_(trace_id), K_(task_loc));
  }
  free_my_memory();
  LOG_WARN("ObRpcAPDistExecuteCB add complete event to scheduler timeout, maybe network error",
      K(ret),
      K(trace_id_),
      K(task_loc_));
}

int ObRpcAPDistExecuteCB::process()
{
  int ret = OB_SUCCESS;
  ObDistributedSchedulerManager* sched_mgr = ObDistributedSchedulerManager::get_instance();
  ObTaskCompleteEvent& task_event = result_;
  if (OB_ISNULL(sched_mgr)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail get ObDistributedSchedulerManager instance", K(ret));
  } else {
    /*
     * task_event may have no valid task_loc when rpc execute failed very early, such as
     * deserialize failed.
     */
    if (!task_event.is_valid()) {
      task_event.init(task_loc_, rcode_.rcode_);
    }
    int merge_ret = OB_SUCCESS;
    if (OB_SUCCESS != (merge_ret = sched_mgr->merge_trans_result(task_event))) {
      task_event.rewrite_err_code(ret);
      LOG_WARN("fail to merge trans result", K(merge_ret));
    }
    int signal_ret = OB_SUCCESS;
    ObString null_str;
    if (OB_ERR_INTERRUPTED == task_event.get_err_code()) {
      LOG_INFO("Task been interrupted", K(task_event));
    } else if (OB_SUCCESS != (signal_ret = sched_mgr->collect_extent_info(task_event))) {
      LOG_WARN("fail to signal scheduler", K(signal_ret), K(task_event));
    } else if (FALSE_IT(task_event.set_extend_info(null_str))) {
    } else if (OB_SUCCESS != (signal_ret = sched_mgr->signal_scheduler(task_event))) {
      LOG_WARN("fail to signal scheduler", K(signal_ret), K(task_event));
    }
    ret = (merge_ret != OB_SUCCESS) ? merge_ret : signal_ret;
  }
  free_my_memory();
  return ret;
}

ObRpcAPMiniDistExecuteCB::ObRpcAPMiniDistExecuteCB(ObAPMiniTaskMgr* ap_mini_task_mgr, const ObTaskID& task_id,
    const ObCurTraceId::TraceId& trace_id, const ObAddr& dist_server, int64_t timeout_ts)
    : ap_mini_task_mgr_(ap_mini_task_mgr), task_id_(task_id), dist_server_(dist_server), timeout_ts_(timeout_ts)
{
  trace_id_.set(trace_id);
  if (OB_LIKELY(ap_mini_task_mgr_ != NULL)) {
    ap_mini_task_mgr_->inc_ref_count();
  }
}

void ObRpcAPMiniDistExecuteCB::free_my_memory()
{
  result_.reset();
  if (OB_LIKELY(ap_mini_task_mgr_ != NULL)) {
    ObAPMiniTaskMgr::free(ap_mini_task_mgr_);
    ap_mini_task_mgr_ = NULL;
  }
}

void ObRpcAPMiniDistExecuteCB::deal_with_rpc_timeout_err(const int64_t timeout_ts, int& err)
{
  if (OB_TIMEOUT == err) {
    int64_t cur_timestamp = ::oceanbase::common::ObTimeUtility::current_time();
    if (timeout_ts - cur_timestamp > 0) {
      LOG_DEBUG("rpc return OB_TIMEOUT, but it is actually not timeout, "
                "change error code to OB_CONNECT_ERROR",
          K(err),
          K(timeout_ts),
          K(cur_timestamp));
      err = OB_RPC_CONNECT_ERROR;
    } else {
      LOG_DEBUG("rpc return OB_TIMEOUT, and it is actually timeout, "
                "do not change error code",
          K(err),
          K(timeout_ts),
          K(cur_timestamp));
    }
  }
}

void ObRpcAPMiniDistExecuteCB::on_timeout()
{
  if (ap_mini_task_mgr_ != NULL) {
    int ret = OB_TIMEOUT;
    deal_with_rpc_timeout_err(timeout_ts_, ret);
    ap_mini_task_mgr_->set_mgr_rcode(ret);
    (void)ap_mini_task_mgr_->atomic_push_mgr_rcode_addr(dist_server_);
  }
  free_my_memory();
  LOG_WARN("ObRpcAPMiniDistExecuteCB add complete event to scheduler timeout, maybe network error",
      K_(trace_id),
      K_(task_id),
      K(dist_server_));
}

int ObRpcAPMiniDistExecuteCB::process()
{
  int ret = OB_SUCCESS;
  const ObMiniTaskResult& task_result = result_;
  uint64_t task_id = task_id_.get_task_id();
  if (OB_ISNULL(ap_mini_task_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ap_mini_task_mgr is null");
  } else if (OB_FAIL(ap_mini_task_mgr_->merge_trans_result(task_id_, task_result))) {
    LOG_WARN("merge trans result failed", K(ret), K_(dist_server), K_(task_id), K_(trace_id), K(task_result));
  } else if (OB_ERR_INTERRUPTED == task_result.get_task_result().get_err_code()) {
    LOG_INFO("Task been interrupted", K_(dist_server), K_(trace_id), K(task_result));
  } else if (OB_FAIL(ap_mini_task_mgr_->save_task_result(dist_server_, task_id, rcode_.rcode_, task_result))) {
    LOG_WARN("save task result failed", K(ret), K_(dist_server), K_(trace_id), K_(task_id), K_(rcode));
  } else { /*do nothing*/
  }
  free_my_memory();
  return ret;
}

int ObPingSqlTaskBaseP::try_forbid_task(const ObPingSqlTask& ping_task, bool& forbid_succ)
{
  int ret = OB_SUCCESS;
  ObPartitionService* ps = gctx_.par_ser_;
  OV(OB_NOT_NULL(ps));
  OZ(ps->mark_trans_forbidden_sql_no(ping_task.trans_id_, ping_task.part_keys_, ping_task.sql_no_, forbid_succ));
  return ret;
}

int ObPingSqlTaskBaseP::try_kill_task(const ObPingSqlTask& ping_task, bool& is_running)
{
  int ret = OB_SUCCESS;
  OZ(ObTaskRunnerNotifierService::kill_task_runner(ping_task.task_id_, &is_running));
  return ret;
}

int ObRpcAPPingSqlTaskP::process()
{
  int ret = OB_SUCCESS;
  const ObPingSqlTask& ping_task = arg_;
  ObTaskStatus cur_status = static_cast<ObTaskStatus>(ping_task.cur_status_);
  int64_t ret_status = cur_status;
  switch (cur_status) {
    case TS_SENT: {
      bool forbid_succ = true;
      OZ(try_forbid_task(ping_task, forbid_succ), ping_task);
      if (forbid_succ) {
        OX(ret_status = TS_FORBIDDEN);
        OX(LOG_INFO("TRC_sent_to_forbid", K(ret), K(ping_task.task_id_), K(ret_status)));
      } else {
        OX(ret_status = TS_RUNNING);
        OX(LOG_INFO("TRC_sent_to_running", K(ret), K(ping_task.task_id_), K(ret_status)));
      }
      if (OB_FAIL(ret) || forbid_succ) {
        break;
      }
    }
    /*no break*/
    case TS_RUNNING: {
      bool is_running = false;
      OZ(try_kill_task(ping_task, is_running), ping_task);
      if (!is_running) {
        OX(ret_status = TS_FINISHED);
        OX(LOG_INFO("TRC_running_to_finish", K(ret), K(ping_task.task_id_), K(ret_status)));
      } else {
        OX(LOG_INFO("TRC_keep_running", K(ret), K(ping_task.task_id_), K(ret_status)));
      }
      break;
    }
    default:
      break;
  }
  result_.err_code_ = ret;
  result_.ret_status_ = ret_status;
  return ret;
}

ObRpcAPPingSqlTaskCB::ObRpcAPPingSqlTaskCB(const ObTaskID& task_id)
    : task_id_(task_id), dist_task_mgr_(NULL), mini_task_mgr_(NULL)
{}

int ObRpcAPPingSqlTaskCB::set_dist_task_mgr(ObDistributedSchedulerManager* dist_task_mgr)
{
  int ret = OB_SUCCESS;
  OV(task_id_.is_dist_task_type(), OB_ERR_UNEXPECTED, task_id_);
  OV(OB_ISNULL(dist_task_mgr_));
  OV(OB_NOT_NULL(dist_task_mgr));
  OX(dist_task_mgr_ = dist_task_mgr);
  return ret;
}

int ObRpcAPPingSqlTaskCB::set_mini_task_mgr(ObAPMiniTaskMgr* mini_task_mgr)
{
  int ret = OB_SUCCESS;
  OV(task_id_.is_mini_task_type(), OB_ERR_UNEXPECTED, task_id_);
  OV(OB_ISNULL(mini_task_mgr_));
  OV(OB_NOT_NULL(mini_task_mgr));
  OX(mini_task_mgr_ = mini_task_mgr);
  OX(mini_task_mgr_->inc_ref_count());
  return ret;
}

int ObRpcAPPingSqlTaskCB::process()
{
  int ret = OB_SUCCESS;
  ObTaskStatus ret_status = static_cast<ObTaskStatus>(result_.ret_status_);
  switch (task_id_.get_task_type()) {
    case ET_DIST_TASK:
      OV(OB_NOT_NULL(dist_task_mgr_));
      OZ(dist_task_mgr_->set_task_status(task_id_, ret_status));
      break;
    case ET_MINI_TASK:
      OV(OB_NOT_NULL(mini_task_mgr_));
      OZ(mini_task_mgr_->set_task_status(task_id_, ret_status));
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid task type", K(task_id_));
      break;
  }
  // no matter succ or not, free memory.
  free_my_memory();
  return ret;
}

void ObRpcAPPingSqlTaskCB::free_my_memory()
{
  if (task_id_.is_mini_task_type() && mini_task_mgr_ != NULL) {
    ObAPMiniTaskMgr::free(mini_task_mgr_);
    mini_task_mgr_ = NULL;
  }
}

int ObRpcTaskCompleteP::preprocess_arg()
{
  int ret = OB_SUCCESS;
  ObTaskCompleteEvent& task_event = arg_;
  ObDistributedSchedulerManager* sched_mgr = ObDistributedSchedulerManager::get_instance();
  if (OB_ISNULL(sched_mgr)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail get ObDistributedSchedulerManager instance", K(ret));
  } else if (OB_FAIL(sched_mgr->collect_extent_info(task_event))) {
    LOG_WARN("fail to signal scheduler", K(ret), K(arg_));
  } else {
    ObString null_str;
    task_event.set_extend_info(null_str);
  }

  // never main the return code;
  ret = OB_SUCCESS;

  return ret;
}

int ObRpcTaskCompleteP::process()
{
  int ret = OB_SUCCESS;
  ObDistributedSchedulerManager* sched_mgr = ObDistributedSchedulerManager::get_instance();
  if (OB_ISNULL(sched_mgr)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail get ObDistributedSchedulerManager instance", K(ret));
  } else if (OB_FAIL(sched_mgr->merge_trans_result(arg_))) {
    LOG_WARN("merge trans result failed", K(ret));
  } else if (OB_FAIL(sched_mgr->signal_scheduler(arg_))) {
    LOG_WARN("fail to signal scheduler", K(ret), K(arg_));
  } else {
  }
  return ret;
}

int ObRpcTaskNotifyFetchP::process()
{
  return OB_SUCCESS;
}
int ObRpcTaskNotifyFetchP::preprocess_arg()
{
  return OB_SUCCESS;
}

int ObRpcTaskFetchResultP::init()
{
  int ret = OB_SUCCESS;
  ObScanner& scanner = result_;
  if (OB_FAIL(scanner.init())) {
    LOG_WARN("fail to init result", K(ret));
  }
  return ret;
}

int ObRpcTaskFetchResultP::process()
{
  int ret = OB_SUCCESS;
  ObIntermResultManager* mgr = ObIntermResultManager::get_instance();
  if (OB_ISNULL(mgr)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail alloc", "mgr", mgr, K(ret));
  } else {
    ObIntermResultInfo ir_info;
    ObIntermResultIterator iter;
    ir_info.init(arg_);  // arg_ type is ObSliceID
    result_.reset();

    ret = E(EventTable::EN_5) ret;
    if (OB_FAIL(ret)) {
      LOG_WARN("triggered error injection logic", K(ret));
    } else if (OB_FAIL(mgr->get_result(ir_info, iter))) {
      LOG_WARN("fail get result from IRM", K(ret), K(ir_info));
    } else if (OB_FAIL(this->sync_send_result(iter))) {
      LOG_WARN("fail sync send result to fetcher", K(ret), K(ir_info));
    } else {
    }
  }
  return ret;
}

int ObRpcTaskFetchResultP::sync_send_result(ObIntermResultIterator& iter)
{
  int ret = OB_SUCCESS;
  int64_t scanner_cnt = 0;
  ObScanner& scanner = result_;
  common::ObArenaAllocator allocator(ObModIds::OB_SQL_EXECUTOR);
  ObIntermResult* interm_result = iter.get_interm_result();
  if (OB_ISNULL(interm_result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("interm result is NULL", K(ret));
  } else {
    if ((scanner_cnt = iter.get_scanner_count()) <= 0) {
      // ret = OB_ERR_UNEXPECTED;
      // LOG_INFO("no data", K(scanner_cnt), K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < scanner_cnt; ++i) {
      if (OB_FAIL(iter.get_next_scanner(scanner))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail get scanner", K(i), K(scanner_cnt), K(ret));
        } else {
        }
      } else if (i < scanner_cnt - 1) {
        if (OB_FAIL(flush(THIS_WORKER.get_timeout_remain()))) {
          LOG_WARN("fail flush scanner to peer", K(ret));
        } else {
          scanner.reuse();
        }
      }
    }

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      scanner.set_found_rows(interm_result->get_found_rows());
      NG_TRACE_EXT(found_rows, OB_ID(found_rows), interm_result->get_found_rows());
    }
    // set last_insert_id
    if (OB_SUCC(ret)) {
      scanner.set_last_insert_id_session(interm_result->get_last_insert_id_session());
      if (!interm_result->is_result_accurate()) {
        scanner.set_is_result_accurate(interm_result->is_result_accurate());
      }
      scanner.set_affected_rows(interm_result->get_affected_rows());
      NG_TRACE_EXT(last_insert_id, OB_ID(last_insert_id), scanner.get_last_insert_id_session());
      scanner.set_row_matched_count(interm_result->get_matched_rows());
      scanner.set_row_duplicated_count(interm_result->get_duplicated_rows());
    }
  }
  return ret;
}

int ObRpcTaskFetchIntermResultP::process()
{
  int ret = OB_SUCCESS;
  ObIntermResultManager* mgr = ObIntermResultManager::get_instance();
  if (OB_ISNULL(mgr)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail alloc", "mgr", mgr, K(ret));
  } else {
    ObIntermResultInfo ir_info;
    ObIntermResultIterator iter;
    ir_info.init(arg_);
    result_.reset();

    ret = E(EventTable::EN_5) ret;
    if (OB_FAIL(ret)) {
      LOG_WARN("triggered error injection logic", K(ret));
    } else if (OB_FAIL(mgr->get_result(ir_info, iter))) {
      LOG_WARN("fail get result from IRM", K(ret), K(ir_info));
    } else if (OB_FAIL(this->sync_send_result(iter))) {
      LOG_WARN("fail sync send result to fetcher", K(ret), K(ir_info));
    } else {
    }
  }
  return ret;
}

int ObRpcTaskFetchIntermResultP::sync_send_result(ObIntermResultIterator& iter)
{
  int ret = OB_SUCCESS;
  int64_t scanner_cnt = 0;
  ObIntermResultItem& ir_item = result_;
  ObIIntermResultItem* cur_ir_item = NULL;
  common::ObArenaAllocator allocator(ObModIds::OB_SQL_EXECUTOR);
  ObIntermResult* interm_result = iter.get_interm_result();
  if (OB_ISNULL(interm_result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("interm result is NULL", K(ret));
  } else {
    if ((scanner_cnt = iter.get_scanner_count()) <= 0) {}
    for (int64_t i = 0; OB_SUCC(ret) && i < scanner_cnt; ++i) {
      if (OB_FAIL(iter.get_next_interm_result_item(cur_ir_item))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail get interm result item", K(i), K(scanner_cnt), K(ret));
        } else {
        }
      } else if (OB_ISNULL(cur_ir_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("succ to get next ir item, but ir item is NULL", K(ret), K(i));
      } else {
        if (cur_ir_item->in_memory()) {
          if (OB_FAIL(ir_item.assign(*static_cast<ObIntermResultItem*>(cur_ir_item)))) {
            LOG_WARN("fail to deep copy current interm result item to send", K(ret), K(i), "item", *cur_ir_item);
          }
        } else {
          if (OB_FAIL(ir_item.from_disk_ir_item((*static_cast<ObDiskIntermResultItem*>(cur_ir_item))))) {
            LOG_WARN("fail to copy disk interm result item to send", K(ret), K(i), "item", *cur_ir_item);
          }
        }

        if (OB_FAIL(ret)) {
        } else if (i < scanner_cnt - 1) {
          if (OB_FAIL(flush(THIS_WORKER.get_timeout_remain()))) {
            LOG_WARN("fail flush interm result item to peer", K(ret));
          } else {
            ir_item.reset();
          }
        }
      }
    }

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObRpcTaskKillP::process()
{
  ObTaskID& ob_task_id = arg_;
  return ObTaskRunnerNotifierService::kill_task_runner(ob_task_id);
}
int ObRpcTaskKillP::preprocess_arg()
{
  return OB_SUCCESS;
}
int ObRpcCloseResultP::preprocess_arg()
{
  return OB_SUCCESS;
}
int ObRpcCloseResultP::process()
{
  int ret = OB_SUCCESS;
  ObIntermResultManager* mgr = ObIntermResultManager::get_instance();
  if (OB_ISNULL(mgr)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail alloc", "mgr", mgr, K(ret));
  } else {
    ObIntermResultInfo ir_info;
    ir_info.init(arg_);  // arg_ type is ObSliceID
    if (OB_FAIL(mgr->delete_result(ir_info))) {
      if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
        LOG_WARN("fail delete result from IRM", K(ret), K(ir_info));
      }
    }
  }
  return ret;
}

int ObMiniTaskBaseP::init_task(ObMiniTask& task)
{
  int ret = OB_SUCCESS;
  task.set_deserialize_param(exec_ctx_, phy_plan_);
  return ret;
}

int ObMiniTaskBaseP::prepare_task_env(ObMiniTask& task)
{
  bool table_version_equal = false;
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* session_info = exec_ctx_.get_my_session();
  ObTaskExecutorCtx& executor_ctx = exec_ctx_.get_task_exec_ctx();
  int64_t tenant_local_version = -1;
  int64_t sys_local_version = -1;
  int64_t sys_schema_version = executor_ctx.get_query_tenant_begin_schema_version();
  int64_t tenant_schema_version = executor_ctx.get_query_sys_begin_schema_version();
  process_timestamp_ = ObTimeUtility::current_time();

  if (OB_ISNULL(gctx_.schema_service_) || OB_ISNULL(gctx_.sql_engine_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("schema service or sql engine is NULL", K(ret), K(gctx_.schema_service_), K(gctx_.sql_engine_));
  } else if (OB_FAIL(gctx_.schema_service_->get_tenant_refreshed_schema_version(
                 session_info->get_effective_tenant_id(), tenant_local_version))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      tenant_local_version = OB_INVALID_VERSION;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get tenant refreshed schema version", K(ret), K(session_info->get_effective_tenant_id()));
    }
  } else if (OB_FAIL(gctx_.schema_service_->get_tenant_refreshed_schema_version(OB_SYS_TENANT_ID, sys_local_version))) {
    LOG_WARN("fail to get systenant refresh schema version", K(ret));
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (sys_schema_version > sys_local_version) {
    if (OB_FAIL(gctx_.schema_service_->async_refresh_schema(OB_SYS_TENANT_ID, sys_schema_version))) {
      LOG_WARN("fail to push back effective_tenant_id", K(ret), K(sys_schema_version), K(sys_local_version));
    }
  }

  if (OB_SUCC(ret) && tenant_schema_version != tenant_local_version) {
    if (OB_FAIL(
            gctx_.schema_service_->get_tenant_schema_guard(session_info->get_effective_tenant_id(), schema_guard_))) {
      LOG_WARN("fail to get schema guard",
          K(ret),
          K(tenant_local_version),
          K(sys_local_version),
          K(tenant_schema_version),
          K(sys_schema_version));
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_SUCC(ret) &&
               OB_SUCC(sql::ObSQLUtils::check_table_version(
                   table_version_equal, task.get_des_phy_plan().get_dependency_table(), schema_guard_))) {
      if (!table_version_equal) {
        if (tenant_schema_version > tenant_local_version) {
          if (OB_FAIL(gctx_.schema_service_->async_refresh_schema(
                  session_info->get_effective_tenant_id(), tenant_schema_version))) {
            LOG_WARN("fail to push back effective_tenant_id",
                K(ret),
                K(session_info->get_effective_tenant_id()),
                K(tenant_schema_version),
                K(tenant_local_version));
          }
        }
      }
    } else {
      ret = OB_SUCCESS;
      LOG_INFO("fail to check table_schema_version", K(ret));
      if (tenant_schema_version > tenant_local_version) {
        if (OB_FAIL(gctx_.schema_service_->async_refresh_schema(
                session_info->get_effective_tenant_id(), tenant_schema_version))) {
          LOG_WARN("fail to push back effective_tenant_id",
              K(ret),
              K(session_info->get_effective_tenant_id()),
              K(tenant_schema_version),
              K(tenant_local_version));
        }
      }
    }
  }

  if (OB_SUCC(ret) && !table_version_equal) {
    if (OB_FAIL(gctx_.schema_service_->get_tenant_schema_guard(
            session_info->get_effective_tenant_id(), schema_guard_, tenant_schema_version, sys_schema_version))) {
      LOG_WARN("fail to get schema guard", K(ret), K(tenant_schema_version), K(sys_schema_version));
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    ObVirtualTableCtx vt_ctx;
    vt_ctx.vt_iter_factory_ = &vt_iter_factory_;
    vt_ctx.session_ = exec_ctx_.get_my_session();
    vt_ctx.schema_guard_ = &schema_guard_;
    vt_ctx.partition_table_operator_ = gctx_.pt_operator_;
    sql_ctx_.session_info_ = exec_ctx_.get_my_session();
    sql_ctx_.schema_guard_ = &schema_guard_;
    exec_ctx_.set_addr(gctx_.self_addr_);
    exec_ctx_.set_plan_cache_manager(gctx_.sql_engine_->get_plan_cache_manager());
    exec_ctx_.set_sql_proxy(gctx_.sql_proxy_);
    exec_ctx_.set_virtual_table_ctx(vt_ctx);
    exec_ctx_.set_session_mgr(gctx_.session_mgr_);
    exec_ctx_.set_sql_ctx(&sql_ctx_);
    if (OB_ISNULL(gctx_.executor_rpc_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("executor rpc is NULL", K(ret));
    } else {
      executor_ctx.set_partition_service(gctx_.par_ser_);
      executor_ctx.set_vt_partition_service(gctx_.vt_par_ser_);
      executor_ctx.schema_service_ = gctx_.schema_service_;
      executor_ctx.set_task_executor_rpc(*gctx_.executor_rpc_);
    }
  }

  return ret;
}

int ObMiniTaskBaseP::execute_subplan(const ObOpSpec& root_spec, ObScanner& scanner)
{
  int ret = OB_SUCCESS;
  ObOperator* root_op = nullptr;
  LOG_DEBUG("execute subplan", K(ret), K(root_spec));
  if (OB_FAIL(root_spec.create_operator(exec_ctx_, root_op))) {
    LOG_WARN("create operator from spec failed", K(ret));
  } else {
    if (OB_FAIL(root_op->open())) {
      LOG_WARN("fail open task", K(ret));
    } else if (OB_FAIL(sync_send_result(exec_ctx_, *root_op, scanner))) {
      LOG_WARN("fail send extend query result to client", K(ret));
    }
    int close_ret = OB_SUCCESS;
    if (OB_SUCCESS != (close_ret = root_op->close())) {
      LOG_WARN("fail close task", K(close_ret));
    }
    ret = (OB_SUCCESS == ret) ? close_ret : ret;
  }
  return ret;
}

int ObMiniTaskBaseP::execute_mini_plan(ObMiniTask& task, ObMiniTaskResult& result)
{
  int ret = OB_SUCCESS;
  NG_TRACE(exec_mini_plan_begin);
  int close_ret = OB_SUCCESS;
  bool is_static_engine = phy_plan_.is_new_engine();
  ObExecRecord exec_record;
  ObExecTimestamp exec_timestamp;
  exec_timestamp.exec_type_ = RpcProcessor;
  int64_t execution_id = 0;
  result.reuse();
  ObExecContext* exec_ctx = NULL;
  ObSQLSessionInfo* session = NULL;
  const bool enable_perf_event = lib::is_diagnose_info_enabled();
  bool enable_sql_audit = GCONF.enable_sql_audit;
  LOG_DEBUG("remote execute mini plan", K(task));
  if (OB_ISNULL(exec_ctx = task.get_exec_context())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("exec ctx is NULL", K(ret), K(task));
  } else if (OB_ISNULL(session = exec_ctx->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("session is NULL", K(ret), K(task));
  } else if (OB_ISNULL(gctx_.sql_engine_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("sql engine is NULL", K(ret), K(gctx_.sql_engine_));
  } else {
    execution_id = gctx_.sql_engine_->get_execution_id();
    enable_sql_audit = enable_sql_audit && session->get_local_ob_enable_sql_audit();
  }
  if (OB_SUCC(ret)) {

    ObSessionStatEstGuard stat_est_guard(session->get_effective_tenant_id(), session->get_sessid());
  }
  ObPhyOperator* root_op = task.get_root_op();
  ObOpSpec* root_spec = task.get_root_spec();
  const ObPhyOperator* extend_root = task.get_extend_root();
  const ObOpSpec* extend_root_spec = task.get_extend_root_spec();
  ObPhysicalPlanCtx* plan_ctx = NULL;
  ObWaitEventDesc max_wait_desc;
  ObWaitEventStat total_wait_desc;
  {
    ObMaxWaitGuard max_wait_guard(enable_perf_event ? &max_wait_desc : NULL);
    ObTotalWaitGuard total_wait_guard(enable_perf_event ? &total_wait_desc : NULL);
    ObPartitionLeaderArray participants;
    if (enable_sql_audit) {
      exec_record.record_start();
    }

    ObTaskRunnerNotifier run_notifier(session, exec_ctx_.get_session_mgr());
    ObTaskRunnerNotifierService::Guard notifier_guard(task.get_ob_task_id(), &run_notifier);
    ObWorkerSessionGuard worker_session_guard(session);
    ObSQLSessionInfo::LockGuard lock_guard(session->get_query_lock());
    session->set_peer_addr(task.get_ctrl_server());

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(plan_ctx = GET_PHY_PLAN_CTX(exec_ctx_))) {
      LOG_ERROR("plan_ctx is NULL");
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(ObTaskExecutorCtxUtil::get_participants(exec_ctx_, task, participants))) {
      LOG_WARN("get participants failed", K(ret));
    } else {
      if (trans_state_.is_start_participant_executed()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("start_participant is executed", K(ret));
      } else if (OB_FAIL(ObSqlTransControl::start_participant(exec_ctx_, participants.get_partitions(), true))) {
        LOG_WARN("fail to start participant", K(ret));
      }
      trans_state_.set_start_participant_executed(OB_SUCC(ret));
    }
    if (enable_perf_event) {
      exec_start_timestamp_ = ObTimeUtility::current_time();
    }

    ObSQLMockSchemaGuard mock_schema_guard;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObSQLMockSchemaUtils::prepare_mocked_schemas(phy_plan_.get_mock_rowid_tables()))) {
        LOG_WARN("fail to prepare_mocked_schemas", K(ret), K(phy_plan_.get_mock_rowid_tables()));
      }
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (is_static_engine) {
      if (OB_FAIL(exec_ctx_.init_eval_ctx())) {
        LOG_WARN("init eval ctx failed", K(ret));
      } else {
        if (extend_root_spec != NULL) {
          if (OB_FAIL(execute_subplan(*extend_root_spec, result.get_extend_result()))) {
            LOG_WARN("fail to execute subplan", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_ISNULL(root_spec)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", K(ret));
        } else if (OB_FAIL(execute_subplan(*root_spec, result.get_task_result()))) {
          LOG_WARN("fail to execute subplan", K(ret));
        }
      }
    } else {
      if (OB_SUCC(ret) && extend_root != NULL) {
        if (OB_FAIL(extend_root->open(exec_ctx_))) {
          LOG_WARN("fail open task", K(ret));
        } else if (OB_FAIL(sync_send_result(exec_ctx_, *extend_root, result.get_extend_result()))) {
          LOG_WARN("fail send extend query result to client", K(ret));
        }
        if (OB_SUCCESS != (close_ret = extend_root->close(exec_ctx_))) {
          LOG_WARN("fail close task", K(close_ret));
        }
        ret = (OB_SUCCESS == ret) ? close_ret : ret;
      }
      if (OB_SUCC(ret)) {
        if (OB_ISNULL(root_op)) {
          LOG_ERROR("op is NULL");
          ret = OB_ERR_UNEXPECTED;
        } else if (OB_FAIL(root_op->open(exec_ctx_))) {
          LOG_WARN("fail open task", K(ret));
        } else if (OB_FAIL(sync_send_result(exec_ctx_, *root_op, result.get_task_result()))) {
          LOG_WARN("fail send op result to client", K(ret));
        }
        if (OB_SUCCESS != (close_ret = root_op->close(exec_ctx_))) {
          LOG_WARN("fail close task", K(close_ret));
        }
        ret = (OB_SUCCESS == ret) ? close_ret : ret;
      }
    }
    NG_TRACE_EXT(process_ret, OB_ID(process_ret), ret);

    if (OB_FAIL(ret)) {
      is_rollback_ = true;
    } else {
      is_rollback_ = false;
    }
    int end_ret = OB_SUCCESS;
    if (trans_state_.is_start_participant_executed() && trans_state_.is_start_participant_success()) {
      if (OB_SUCCESS !=
          (end_ret = ObSqlTransControl::end_participant(exec_ctx_, is_rollback_, participants.get_partitions()))) {
        LOG_WARN("fail to end participant", K(ret), K(end_ret), K_(is_rollback), K(participants), K(exec_ctx_));
      }
      trans_state_.clear_start_participant_executed();
    }
    ret = (OB_SUCCESS == ret) ? end_ret : ret;

    if (OB_FAIL(ret)) {
      if (is_schema_error(ret)) {
        ret = OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH;
      }
    }
    if (enable_perf_event) {
      exec_end_timestamp_ = ObTimeUtility::current_time();
      if (enable_sql_audit) {
        exec_record.record_end();
      }
      record_exec_timestamp(true, exec_timestamp);
    }

    if (enable_sql_audit) {
      if (OB_ISNULL(session)) {
        LOG_WARN("invalid argument", K(ret), K(session));
      } else {
        ObAuditRecordData& audit_record = session->get_audit_record();
        audit_record.seq_ = 0;  // don't use now
        audit_record.status_ = (OB_SUCCESS == ret || common::OB_ITER_END == ret) ? obmysql::REQUEST_SUCC : ret;
        audit_record.execution_id_ = execution_id;
        audit_record.client_addr_ = task.get_ctrl_server();
        audit_record.user_client_addr_ = task.get_ctrl_server();
        audit_record.user_group_ = THIS_WORKER.get_group_id();
        audit_record.affected_rows_ = plan_ctx->get_affected_rows();
        audit_record.return_rows_ = plan_ctx->get_found_rows();

        exec_record.max_wait_event_ = max_wait_desc;
        exec_record.wait_time_end_ = total_wait_desc.time_waited_;
        exec_record.wait_count_end_ = total_wait_desc.total_waits_;

        audit_record.plan_type_ = phy_plan_.get_plan_type();
        audit_record.is_executor_rpc_ = true;
        audit_record.is_inner_sql_ = session->is_inner();
        audit_record.is_hit_plan_cache_ = true;
        audit_record.is_multi_stmt_ = false;

        audit_record.exec_timestamp_ = exec_timestamp;
        audit_record.exec_record_ = exec_record;

        audit_record.update_stage_stat();

        ObSQLUtils::handle_audit_record(false, EXECUTE_DIST, *session, exec_ctx_);
      }
      LOG_DEBUG("execute mini task",
          K(ret),
          K(participants),
          K(max_wait_desc),
          K(total_wait_desc),
          K(task),
          K(plan_ctx->get_affected_rows()),
          K(plan_ctx->get_found_rows()),
          K(phy_plan_.get_plan_type()),
          K_(exec_start_timestamp),
          K_(exec_end_timestamp));
    }
  }

  /**
   * call merge_result() no matter ret is OB_SUCCESS or not, otherwise only part of
   * transaction will be rollbacked if any previous operation failed.
   */
  if (OB_NOT_NULL(session)) {
    int merge_ret = result.get_task_result().get_trans_result().merge_result(session->get_trans_result());
    if (OB_SUCCESS != merge_ret) {
      LOG_WARN("merge trans result failed",
          K(merge_ret),
          "scanner_trans_result",
          result.get_task_result().get_trans_result(),
          "session_trans_result",
          session->get_trans_result());
      ret = (OB_SUCCESS == ret) ? merge_ret : ret;
    } else {
      LOG_DEBUG("execute_mini_plan trans_result_info",
          "cur_stmt",
          session->get_current_query_string(),
          "scanner_trans_result",
          result.get_task_result().get_trans_result(),
          "session_trans_result",
          session->get_trans_result());
    }
  }

  // return a scanner no matter success or fail, set err_code to scanner
  if (OB_FAIL(ret)) {
    result.get_task_result().set_err_code(ret);
    result.get_task_result().store_err_msg(ob_get_tsi_err_msg(ret));
    LOG_WARN("fail to execute task", K(ret), K(task));
    ret = OB_SUCCESS;
  }
  NG_TRACE(exec_mini_plan_end);
  if (exec_end_timestamp_ - exec_start_timestamp_ >= ObServerConfig::get_instance().trace_log_slow_query_watermark) {
    // slow mini task, print trace info
    FORCE_PRINT_TRACE(THE_TRACE, "[slow mini task]");
  }
  // vt_iter_factory_.reuse();
  phy_plan_.destroy();

  return ret;
}

int ObMiniTaskBaseP::sync_send_result(ObExecContext& exec_ctx, ObOperator& op, ObScanner& scanner)
{
  int ret = OB_SUCCESS;
  int last_row_used = true;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  int64_t total_row_cnt = 0;
  ObSQLSessionInfo* session = exec_ctx.get_my_session();
  if (OB_ISNULL(plan_ctx = GET_PHY_PLAN_CTX(exec_ctx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("plan_ctx is NULL");
  } else if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("session info is NULL");
  } else {
    if (OB_FAIL(op.get_next_row())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to get next row", K(ret));
      }
    }
    while (OB_SUCC(ret) && OB_SUCC(exec_ctx.check_status())) {
      scanner.set_tenant_id(exec_ctx.get_my_session()->get_effective_tenant_id());
      bool added = false;
      if (OB_FAIL(scanner.try_add_row(op.get_spec().output_, exec_ctx.get_eval_ctx(), added))) {
        LOG_WARN("fail add row to scanner", K(ret), K(op.get_spec().output_));
      } else if (!added) {
        ret = OB_OVERSIZE_NEED_RETRY;
      } else {
        last_row_used = true;
        total_row_cnt++;
      }
      if (OB_SUCC(ret)) {
        if (last_row_used) {
          if (OB_FAIL(op.get_next_row())) {
            if (OB_ITER_END != ret) {
              LOG_WARN("fail to get next row", K(ret));
            }
          }
        }
      }
    }
  }

  if (PHY_MULTI_PART_TABLE_SCAN == op.get_spec().type_ && (OB_OVERSIZE_NEED_RETRY == ret)) {
    ObMultiPartTableScanOp& multi_part_scan = reinterpret_cast<ObMultiPartTableScanOp&>(op);
    int64_t range_count = 0;
    multi_part_scan.get_used_range_count(range_count);
    scanner.set_found_rows(range_count);
    if (0 >= range_count) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected state", K(ret), K(range_count), K(scanner.get_row_count()));
    }
    LOG_TRACE("Set found rows", K(ret), K(total_row_cnt), K(range_count), K(scanner.get_row_count()));
  }

  if (OB_ITER_END == ret) {
    if (OB_ISNULL(plan_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("plan ctx is NULL", K(ret));
    } else if (OB_FAIL(scanner.set_session_var_map(session))) {
      LOG_WARN("set user var to scanner failed");
    } else {
      plan_ctx->calc_last_insert_id_session();
      ret = ObTaskExecutorCtxUtil::merge_task_result_meta(scanner, *plan_ctx);
    }
  } else {
  }

  return ret;
}

int ObMiniTaskBaseP::sync_send_result(ObExecContext& exec_ctx, const ObPhyOperator& op, ObScanner& scanner)
{
  int ret = OB_SUCCESS;
  const ObNewRow* row = NULL;
  int last_row_used = true;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  int64_t total_row_cnt = 0;
  ObSQLSessionInfo* session = exec_ctx.get_my_session();
  if (OB_ISNULL(plan_ctx = GET_PHY_PLAN_CTX(exec_ctx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("plan_ctx is NULL");
  } else if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("session info is NULL");
  } else {
    if (OB_FAIL(op.get_next_row(exec_ctx, row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to get next row", K(ret));
      } else {
      }
    } else {
    }
    while (OB_SUCC(ret) && OB_SUCC(exec_ctx.check_status())) {
      scanner.set_tenant_id(exec_ctx.get_my_session()->get_effective_tenant_id());
      if (OB_ISNULL(row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("row is NULL", K(ret));
      } else if (OB_FAIL(scanner.add_row(*row))) {
        if (OB_UNLIKELY(OB_SIZE_OVERFLOW != ret)) {
          LOG_WARN("fail add row to scanner", K(ret), K(*row));
        } else {
          ret = OB_OVERSIZE_NEED_RETRY;
        }
      } else {
        last_row_used = true;
        total_row_cnt++;
      }
      if (OB_SUCC(ret)) {
        if (last_row_used) {
          if (OB_FAIL(op.get_next_row(exec_ctx, row))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("fail to get next row", K(ret), K(op));
            }
          }
        }
      }
    }
  }

  if (PHY_MULTI_PART_TABLE_SCAN == op.get_type() && (OB_OVERSIZE_NEED_RETRY == ret)) {
    const ObMultiPartTableScan& multi_part_scan = static_cast<const ObMultiPartTableScan&>(op);
    int64_t range_count = 0;
    multi_part_scan.get_used_range_count(exec_ctx, range_count);
    scanner.set_found_rows(range_count);
    if (0 >= range_count) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected state", K(ret), K(range_count), K(scanner.get_row_count()));
    }
    LOG_TRACE("Set found rows", K(ret), K(total_row_cnt), K(range_count), K(scanner.get_row_count()));
  }

  if (OB_ITER_END == ret) {
    if (OB_ISNULL(plan_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("plan ctx is NULL", K(ret));
    } else if (OB_FAIL(scanner.set_session_var_map(session))) {
      LOG_WARN("set user var to scanner failed");
    } else {
      plan_ctx->calc_last_insert_id_session();
      ret = ObTaskExecutorCtxUtil::merge_task_result_meta(scanner, *plan_ctx);
    }
  } else {
  }
  return ret;
}

int ObRpcMiniTaskExecuteP::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_task(arg_))) {
    LOG_WARN("init task failed", K(ret));
  } else if (OB_FAIL(result_.init())) {
    LOG_WARN("init result failed", K(ret));
  }
  return ret;
}

int ObRpcMiniTaskExecuteP::before_process()
{
  exec_ctx_.show_session();
  return prepare_task_env(arg_);
}

int ObRpcMiniTaskExecuteP::process()
{
  return execute_mini_plan(arg_, result_);
}

int ObRpcMiniTaskExecuteP::before_response()
{
  exec_ctx_.hide_session();
  return OB_SUCCESS;
}

void ObRpcMiniTaskExecuteP::cleanup()
{
  exec_ctx_.cleanup_session();
  obrpc::ObRpcProcessor<obrpc::ObExecutorRpcProxy::ObRpc<obrpc::OB_MINI_TASK_EXECUTE>>::cleanup();
  return;
}

int ObRpcAPMiniDistExecuteP::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_task(arg_))) {
    LOG_WARN("init task failed", K(ret));
  } else if (OB_FAIL(result_.init())) {
    LOG_WARN("init result failed", K(ret));
  }
  return ret;
}

int ObRpcAPMiniDistExecuteP::before_process()
{
  exec_ctx_.show_session();
  return prepare_task_env(arg_);
}

int ObRpcAPMiniDistExecuteP::process()
{
  return execute_mini_plan(arg_, result_);
}

int ObRpcAPMiniDistExecuteP::before_response()
{
  exec_ctx_.hide_session();
  return OB_SUCCESS;
}

void ObRpcAPMiniDistExecuteP::cleanup()
{
  exec_ctx_.cleanup_session();
  obrpc::ObRpcProcessor<obrpc::ObExecutorRpcProxy::ObRpc<obrpc::OB_AP_MINI_DIST_EXECUTE>>::cleanup();
  return;
}

int ObRpcBKGDDistExecuteP::preprocess_arg()
{
  return OB_SUCCESS;
}

int ObRpcBKGDDistExecuteP::process()
{
  int ret = OB_SUCCESS;
  LOG_INFO("receive background task", K(arg_.task_id_));
  if (!arg_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg_));
  } else {
    ObBKGDDistTaskDag* dag = NULL;
    ObBKGDDistTask* task = NULL;
    if (OB_FAIL(ObDagScheduler::get_instance().alloc_dag(dag))) {
      LOG_WARN("alloc dag failed", K(ret));
    } else if (OB_ISNULL(dag)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dag is NULL", K(ret));
    } else if (OB_FAIL(dag->init(arg_.tenant_id_, arg_.task_id_, arg_.scheduler_id_))) {
      LOG_WARN("dag init failed", K(ret));
    } else if (OB_FAIL(dag->alloc_task(task))) {
      LOG_WARN("alloc task failed", K(ret));
    } else if (OB_ISNULL(task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task is NULL", K(ret));
    } else if (OB_FAIL(task->init(arg_.return_addr_, arg_.serialized_task_, THIS_WORKER.get_timeout_ts()))) {
      LOG_WARN("task init failed", K(ret));
    } else if (OB_FAIL(dag->add_task(*task))) {
      LOG_WARN("add task to dag failed", K(ret));
    } else if (OB_FAIL(ObDagScheduler::get_instance().add_dag(dag))) {
      LOG_WARN("add dag failed", K(ret), K(arg_.task_id_));
    }
    if (OB_FAIL(ret) && NULL != dag) {
      ObDagScheduler::get_instance().free_dag(*dag);
      dag = NULL;
    }
  }
  return ret;
}

int ObCheckBuildIndexTaskExistP::preprocess_arg()
{
  return OB_SUCCESS;
}

int ObCheckBuildIndexTaskExistP::process()
{
  int ret = OB_SUCCESS;
  LOG_TRACE("receive check build index task exist", K(arg_));
  if (!arg_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg_));
  } else {
    bool exist = false;
    ObBKGDDistTaskDag dag;
    if (OB_FAIL(dag.init(arg_.tenant_id_, arg_.task_id_, arg_.scheduler_id_))) {
      LOG_WARN("init dag failed", K(ret));
    } else if (OB_FAIL(ObDagScheduler::get_instance().check_dag_exist(&dag, exist))) {
      LOG_WARN("check dag exist failed", K(ret));
    } else {
      result_ = exist;
    }
  }
  return ret;
}

int ObRpcBKGDTaskCompleteP::notify_error(const ObTaskID& task_id, const uint64_t scheduler_id, const int return_code)
{
  int ret = OB_SUCCESS;
  ObAddr addr;
  ObDistributedSchedulerManager* sched_mgr = ObDistributedSchedulerManager::get_instance();
  if (OB_ISNULL(sched_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schedule manager is NULL", K(ret));
  } else if (OB_FAIL(sched_mgr->signal_schedule_error(task_id.get_execution_id(), return_code, addr, scheduler_id))) {
    LOG_WARN("signal schedule error failed", K(ret));
  }
  return ret;
}

int ObRpcBKGDTaskCompleteP::process()
{
  int ret = OB_SUCCESS;
  LOG_INFO("receive background task complete", K(ret), K(arg_.task_id_), K(arg_.scheduler_id_), K(arg_.return_code_));
  ObDistributedSchedulerManager* sched_mgr = ObDistributedSchedulerManager::get_instance();
  if (OB_ISNULL(GCTX.root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root service is NULL", K(ret));
  } else {
    // ignore notify RS error.
    int tmp_ret = GCTX.root_service_->sql_bkgd_task_execute_over(arg_);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("notify RS background task execute over failed", K(tmp_ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(sched_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schedule manager is NULL", K(ret));
  } else if (OB_FAIL(sched_mgr->merge_trans_result(arg_.event_))) {
    LOG_WARN("merge trans result failed", K(ret));
  } else {
    if (OB_SUCCESS != arg_.return_code_) {
      if (OB_FAIL(notify_error(arg_.task_id_, arg_.scheduler_id_, arg_.return_code_))) {
        LOG_WARN("signal schedule error failed", K(ret), K(arg_));
      }
    } else {
      if (OB_FAIL(sched_mgr->collect_extent_info(arg_.event_))) {
        LOG_WARN("collect extend info failed", K(ret));
      } else {
        ObString empty_str;
        arg_.event_.set_extend_info(empty_str);
        if (OB_FAIL(sched_mgr->signal_scheduler(arg_.event_, arg_.scheduler_id_))) {
          LOG_WARN("signal scheduler failed", K(ret), K(arg_));
        }
      }
    }
  }
  return ret;
}

int ObFetchIntermResultItemP::preprocess_arg()
{
  return OB_SUCCESS;
}

int ObFetchIntermResultItemP::process()
{
  int ret = OB_SUCCESS;
  LOG_TRACE("receive fetch interm result request", K(arg_));
  ObIntermResultManager* mgr = ObIntermResultManager::get_instance();
  if (OB_ISNULL(mgr) || !arg_.slice_id_.is_valid() || arg_.index_ < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg_), KP(mgr));
  } else {
    ObIntermResultInfo ir_info;
    ir_info.init(arg_.slice_id_);
    if (OB_FAIL(mgr->get_result_item(ir_info, arg_.index_, result_.result_item_, result_.total_item_cnt_))) {
      LOG_WARN("get result item failed", K(ret), K(arg_));
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
