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

#define USING_LOG_PREFIX OBLOG

#include "ob_log_sys_ls_task_handler.h"

#include "ob_log_ddl_parser.h"          // IObLogDdlParser
#include "ob_log_sequencer1.h"          // IObLogSequencer
#include "ob_log_committer.h"           // IObLogCommitter
#include "ob_log_instance.h"            // IObLogErrHandler, TCTX
#include "ob_log_schema_getter.h"       // IObLogSchemaGetter
#include "ob_log_tenant_mgr.h"          // IObLogTenantMgr
#include "ob_log_config.h"              // TCONF
#include "ob_log_trace_id.h"            // ObLogTraceIdGuard

#define _STAT(level, fmt, args...) _OBLOG_LOG(level, "[STAT] [SYS_LS_HANDLER] " fmt, ##args)
#define STAT(level, fmt, args...) OBLOG_LOG(level, "[STAT] [SYS_LS_HANDLER] " fmt, ##args)
#define _ISTAT(fmt, args...) _STAT(INFO, fmt, ##args)
#define ISTAT(fmt, args...) STAT(INFO, fmt, ##args)
#define _DSTAT(fmt, args...) _STAT(DEBUG, fmt, ##args)

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;

namespace libobcdc
{

int64_t ObLogSysLsTaskHandler::g_sys_ls_task_op_timeout_msec = ObLogConfig::default_sys_ls_task_op_timeout_msec * _MSEC_;

////////////////////////////// ObLogSysLsTaskHandler::TaskQueue //////////////////////////////

ObLogSysLsTaskHandler::TaskQueue::TaskQueue() :
    queue_()
{}

ObLogSysLsTaskHandler::TaskQueue::~TaskQueue()
{
}

// The requirement task must be a DDL transactional task, as the code that maintains the progress depends on
int ObLogSysLsTaskHandler::TaskQueue::push(PartTransTask *task)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid task", KR(ret), K(task));
  } else {
    queue_.push(task);
  }

  return ret;
}

void ObLogSysLsTaskHandler::TaskQueue::pop()
{
  (void)queue_.pop();
}

int64_t ObLogSysLsTaskHandler::TaskQueue::size() const
{
  return queue_.size();
}

// This function is only the observer, only read the top element
// It must be guaranteed that the person calling this function is the only consumer, i.e. no one else will pop elements during the call to this function
int ObLogSysLsTaskHandler::TaskQueue::next_ready_to_handle(
    const int64_t timeout,
    PartTransTask *&top_task,
    common::ObCond &cond)
{
  int ret = OB_SUCCESS;
  int64_t cur_time = get_timestamp();
  int64_t end_time = cur_time + timeout;

  while (NULL == queue_.top() && OB_SUCCESS == ret) {
    int64_t wait_time = end_time - cur_time;

    if (wait_time <= 0) {
      ret = OB_TIMEOUT;
    } else {
      cond.timedwait(wait_time);
      cur_time = get_timestamp();
    }
  }

  if (OB_SUCCESS == ret) {
    if (NULL == queue_.top()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid error, top task is NULL", KR(ret), K(queue_.top()));
    } else {
      PartTransTask *task = queue_.top();
      int64_t wait_time = end_time - cur_time;

      // DDL task have to wait for parse complete, other types of tasks are considered ready
      if (task->is_ddl_trans() && OB_FAIL(task->wait_formatted(wait_time, cond))) {
        if (OB_TIMEOUT != ret) {
          LOG_ERROR("task wait_formatted fail", KR(ret), K(wait_time), KPC(task));
        }
      } else {
        top_task = task;
      }
    }
  }

  return ret;
}

///////////////////////////////// ObLogSysLsTaskHandler /////////////////////////////////

ObLogSysLsTaskHandler::ObLogSysLsTaskHandler() :
    is_inited_(false),
    ddl_parser_(NULL),
    sequencer_(NULL),
    err_handler_(NULL),
    schema_getter_(NULL),
    ddl_processor_(NULL),
    handle_pid_(0),
    stop_flag_(true),
    sys_ls_fetch_queue_(),
    wait_formatted_cond_()
{}

ObLogSysLsTaskHandler::~ObLogSysLsTaskHandler()
{
  destroy();
}

int ObLogSysLsTaskHandler::init(
    IObLogDdlParser *ddl_parser,
    ObLogDDLProcessor *ddl_processor,
    IObLogSequencer *sequencer,
    IObLogErrHandler *err_handler,
    IObLogSchemaGetter *schema_getter,
    const bool skip_reversed_schema_version)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("ObLogSysLsTaskHandler init twice", KR(ret), K(is_inited_));
  } else if (OB_ISNULL(ddl_parser_ = ddl_parser)
      || OB_ISNULL(ddl_processor_ = ddl_processor)
      || OB_ISNULL(sequencer_ = sequencer)
      || OB_ISNULL(err_handler_ = err_handler)
      || (OB_ISNULL(schema_getter_ = schema_getter) && is_online_refresh_mode(TCTX.refresh_mode_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(ddl_parser), K(sequencer), K(err_handler), K(schema_getter));
  } else {
    handle_pid_ = 0;
    stop_flag_ = true;
    is_inited_ = true;

    LOG_INFO("ObLogSysLsTaskHandler init succ");
  }

  return ret;
}

void ObLogSysLsTaskHandler::destroy()
{
  stop();

  is_inited_ = false;
  ddl_parser_ = NULL;
  ddl_processor_ = NULL;
  sequencer_ = NULL;
  err_handler_ = NULL;
  schema_getter_ = NULL;
  handle_pid_ = 0;
  stop_flag_ = true;
}

int ObLogSysLsTaskHandler::start()
{
  int ret = OB_SUCCESS;
  int pthread_ret = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not init", KR(ret), K(is_inited_));
  } else if (stop_flag_) {
    stop_flag_ = false;

    if (0 != (pthread_ret = pthread_create(&handle_pid_, NULL, handle_thread_func_, this))){
      LOG_ERROR("create DDL handle thread fail", K(pthread_ret), KERRNOMSG(pthread_ret));
      ret = OB_ERR_UNEXPECTED;
    } else {
      LOG_INFO("start ObLogSysLsTaskHandler thread succ");
    }

    if (OB_FAIL(ret)) {
      stop_flag_ = true;
    }
  }

  return ret;
}

void ObLogSysLsTaskHandler::stop()
{
  if (is_inited_) {
    stop_flag_ = true;

    if (0 != handle_pid_) {
      int pthread_ret = pthread_join(handle_pid_, NULL);

      if (0 != pthread_ret) {
        LOG_ERROR_RET(OB_ERR_SYS, "join DDL handle thread fail", K(handle_pid_), K(pthread_ret),
            KERRNOMSG(pthread_ret));
      } else {
        LOG_INFO("stop ObLogSysLsTaskHandler thread succ");
      }

      handle_pid_ = 0;
    }
  }
}

int ObLogSysLsTaskHandler::push(PartTransTask *task, const int64_t timeout)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ddl_parser_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("invalid DDL parser", KR(ret), K(ddl_parser_));
  } else if (OB_UNLIKELY(! task->is_ddl_trans()
      && ! task->is_ls_op_trans()
      && ! task->is_sys_ls_heartbeat()
      && !task->is_sys_ls_offline_task())) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("task is not DDL trans, or LS Table, or HEARTBEAT, or OFFLINE task, not supported", KR(ret), KPC(task));
  }
  // DDL task have to push to the DDL parser first, because the task will retry after the task push DDL parser times out.
  // that is, the same task may be pushed multiple times.To avoid the same task being added to the queue more than once, the DDL parser is pushed first
  else if (task->is_ddl_trans() && OB_FAIL(ddl_parser_->push(*task, timeout))) {
    if (OB_IN_STOP_STATE != ret && OB_TIMEOUT != ret) {
      LOG_ERROR("push task into DDL parser fail", KR(ret), K(task));
    }
  }
  // Add the task to the Fetch queue without timeout failure, ensuring that it will only be pushed once in the Parser
  else if (OB_FAIL(sys_ls_fetch_queue_.push(task))) {
    LOG_ERROR("push task into fetch queue fail", KR(ret), KPC(task));
  } else {
    // success
  }

  return ret;
}

int ObLogSysLsTaskHandler::get_progress(
    uint64_t &sys_min_progress_tenant_id,
    int64_t &sys_ls_min_progress,
    palf::LSN &sys_ls_min_handle_log_lsn)
{
  int ret = OB_SUCCESS;
  IObLogTenantMgr *tenant_mgr = TCTX.tenant_mgr_;
  sys_ls_min_progress = OB_INVALID_TIMESTAMP;
  sys_min_progress_tenant_id = OB_INVALID_TENANT_ID;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogSysLsTaskHandler not init", KR(ret), K(is_inited_));
  } else if (OB_ISNULL(tenant_mgr)) {
    LOG_ERROR("tenant_mgr_ is NULL", K(tenant_mgr));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(tenant_mgr->get_sys_ls_progress(
      sys_min_progress_tenant_id,
      sys_ls_min_progress,
      sys_ls_min_handle_log_lsn))) {
    if (OB_EMPTY_RESULT != ret) {
      LOG_ERROR("get_sys_ls_progress fail", KR(ret), K(sys_min_progress_tenant_id),
          K(sys_ls_min_progress), K(sys_ls_min_handle_log_lsn));
    }
  } else {
    // success
  }

  return ret;
}

int64_t ObLogSysLsTaskHandler::get_part_trans_task_count() const
{
  return sys_ls_fetch_queue_.size();
}

void ObLogSysLsTaskHandler::configure(const ObLogConfig &config)
{
  int64_t sys_ls_task_op_timeout_msec = config.sys_ls_task_op_timeout_msec;

  ATOMIC_STORE(&g_sys_ls_task_op_timeout_msec, sys_ls_task_op_timeout_msec * _MSEC_);
  LOG_INFO("[CONFIG]", K(sys_ls_task_op_timeout_msec));
}

void *ObLogSysLsTaskHandler::handle_thread_func_(void *arg)
{
  if (NULL != arg) {
    ObLogSysLsTaskHandler *sys_task_handler = static_cast<ObLogSysLsTaskHandler *>(arg);
    sys_task_handler->handle_task_routine();
  }

  return NULL;
}

int ObLogSysLsTaskHandler::next_task_(PartTransTask *&task)
{
  int ret = OB_SUCCESS;

  // Get the next task to be processed
  RETRY_FUNC(stop_flag_, sys_ls_fetch_queue_, next_ready_to_handle, ATOMIC_LOAD(&g_sys_ls_task_op_timeout_msec), task,
      wait_formatted_cond_);

  // Unconditionally pop out
  sys_ls_fetch_queue_.pop();

  if (OB_SUCCESS != ret) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("task_queue next_ready_to_handle fail", KR(ret));
    }
  } else if (OB_ISNULL(task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid task", KR(ret), K(task));
  }

  return ret;
}

int ObLogSysLsTaskHandler::handle_task_(PartTransTask &task,
    const uint64_t ddl_tenant_id,
    ObLogTenant *tenant,
    const bool is_tenant_served)
{
  int ret = OB_SUCCESS;
  ObLogTraceIdGuard trace_guard;

  if (OB_UNLIKELY(! task.is_ddl_trans()
      && ! task.is_ls_op_trans()
      && ! task.is_sys_ls_heartbeat()
      && ! task.is_sys_ls_offline_task())) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("task is not DDL trans task, or LS Table, or HEARTBEAT, or OFFLINE task, not supported", KR(ret), K(task));
  } else if (task.is_sys_ls_offline_task()) {
    // OFFLINE tasks for DDL partitions
    if (OB_FAIL(handle_sys_ls_offline_task_(task))) {
      LOG_ERROR("handle_sys_ls_offline_task_ fail", KR(ret), K(task));
    }
  } else if (OB_ISNULL(tenant)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid tenant", KR(ret), KPC(tenant), K(ddl_tenant_id), K(is_tenant_served), K(task));
  }
  // An error is reported if the tenant is not in the service state.
  // The current implementation assumes that the tenant is in service during all DDL processing under the tenant, and that the tenant is taken offline by the DDL offline task
  else if (OB_UNLIKELY(! tenant->is_serving())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tenant state is not serving, unexpected", KR(ret), KPC(tenant), K(task),
        K(ddl_tenant_id), K(is_tenant_served));
  } else {
    const bool is_using_online_schema = is_online_refresh_mode(TCTX.refresh_mode_);
    // The following handles DDL transaction tasks and DDL heartbeat tasks
    // NOTICE: handle_ddl_trans before sequencer when using online_schmea, otherwise(using data_dict) handle_ddl_trans in sequencer.
    if (task.is_ddl_trans() && is_using_online_schema && OB_FAIL(ddl_processor_->handle_ddl_trans(task, *tenant, stop_flag_))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("ddl_processor_ handle_ddl_trans fail", KR(ret), K(task), K(ddl_tenant_id), K(tenant),
            K(is_tenant_served));
      }
    }
    // Both DDL transactions and heartbeats update DDL information
    else if (OB_FAIL(update_sys_ls_info_(task, *tenant))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("update_sys_ls_info_ fail", KR(ret), K(task), KPC(tenant));
      }
    }
  }

  return ret;
}

int ObLogSysLsTaskHandler::dispatch_task_(
    PartTransTask *task,
    ObLogTenant *tenant,
    const bool is_tenant_served)
{
  int ret = OB_SUCCESS;
  IObLogCommitter *trans_committer = TCTX.committer_;

  if (OB_ISNULL(sequencer_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("invalid committer", KR(ret), K(sequencer_));
  } else if (OB_ISNULL(task) || (is_tenant_served && OB_ISNULL(tenant)) || (! is_tenant_served && NULL != tenant)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid tenant", KR(ret), K(is_tenant_served), K(tenant));
  } else {
    if (task->is_ddl_trans()) {
      if (OB_FAIL(sequencer_->push(task, stop_flag_))) {
        LOG_ERROR("sequencer_ push fail", KR(ret), KPC(task));
      }
    } else {
      if (OB_ISNULL(trans_committer)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("trans_committer is NULL", KR(ret));
      } else {
        const int64_t task_count = 1;
        RETRY_FUNC(stop_flag_, (*trans_committer), push, task, task_count, DATA_OP_TIMEOUT);
      }
    }

    if (OB_SUCCESS != ret) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("push into committer fail", KR(ret), KPC(task), KPC(tenant), K(is_tenant_served));
      }
    } else {
      task = NULL;
    }
  }

  return ret;
}

void ObLogSysLsTaskHandler::handle_task_routine()
{
  int ret = OB_SUCCESS;
  ObLogTraceIdGuard trace_guard;

  while (! stop_flag_ && OB_SUCCESS == ret) {
    PartTransTask *task = NULL;

    // Iterate for the next task
    if (OB_FAIL(next_task_(task))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("next_task_ fail", KR(ret));
      }
    } else {
      ObLogTenantGuard guard;
      ObLogTenant *tenant = NULL;
      bool is_tenant_served = false;
      const uint64_t ddl_tenant_id = task->get_tenant_id();

      // First obtain tenant information, the tenant may not serve
      if (OB_FAIL(get_tenant_(*task,
          ddl_tenant_id,
          guard,
          tenant,
          is_tenant_served))) {
        LOG_ERROR("get_tenant_ fail", KR(ret), KPC(task));
      }
      else if (OB_FAIL(handle_task_(*task,
          ddl_tenant_id,
          tenant,
          is_tenant_served))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("handle DDL task fail", KR(ret), KPC(task),
              K(ddl_tenant_id), KPC(tenant), K(is_tenant_served));
        }
      }
      // Final distribution of tasks
      else if (OB_FAIL(dispatch_task_(task, tenant, is_tenant_served))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("dispatch task fail", KR(ret), KPC(task), KPC(tenant),
              K(is_tenant_served));
        }
      }
    }
  }

  if (stop_flag_) {
    ret = OB_IN_STOP_STATE;
  }

  if (OB_SUCCESS != ret && OB_IN_STOP_STATE != ret && NULL != err_handler_) {
    err_handler_->handle_error(ret, "DDL handle thread exits, err=%d", ret);
    stop_flag_ = true;
  }
}

int ObLogSysLsTaskHandler::handle_sys_ls_offline_task_(const PartTransTask &task)
{
  int ret = OB_SUCCESS;
  IObLogTenantMgr *tenant_mgr = TCTX.tenant_mgr_;
  // DDL __all_ddl_operation table offline tasks, must be schema split mode
  // If a offline task is received, it means that all DDLs of the tenant have been processed and it is safe to delete the tenant
  const uint64_t ddl_tenant_id = task.get_tenant_id();

  ISTAT("[SYS_LS] [OFFLINE_TASK] begin drop tenant", K(ddl_tenant_id),
      "tls_id", task.get_tls_id());

  if (OB_ISNULL(tenant_mgr)) {
    LOG_ERROR("invalid tenant mgr", K(tenant_mgr));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(tenant_mgr->drop_tenant(ddl_tenant_id, "DDL_OFFLINE_TASK"))) {
    LOG_ERROR("tenant mgr drop tenant fail", KR(ret), K(ddl_tenant_id), K(task));
  } else {
    ISTAT("[SYS_LS] [OFFLINE_TASK] drop tenant succ", K(ddl_tenant_id),
        "pkey", task.get_tls_id());
  }

  return ret;
}

int ObLogSysLsTaskHandler::get_tenant_(
    PartTransTask &task,
    const uint64_t ddl_tenant_id,
    ObLogTenantGuard &guard,
    ObLogTenant *&tenant,
    bool &is_tenant_served)
{
  int ret = OB_SUCCESS;
  // Default setting for tenant non-service
  tenant = NULL;
  is_tenant_served = false;

  // Determine the tenant ID to which the task belongs
  if (OB_FAIL(TCTX.get_tenant_guard(ddl_tenant_id, guard))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // not serve ddl if tenant not exist
      // In split mode, the tenant cannot not exist, and if it does not exist, then the tenant is deleted in advance, which must have a bug
      LOG_ERROR("tenant not exist when handle DDL task under schema split mode, unexpected",
          KR(ret), K(ddl_tenant_id), K(task));
      ret = OB_ERR_UNEXPECTED;
    } else {
      LOG_ERROR("get_tenant fail", KR(ret), K(ddl_tenant_id), K(guard));
    }
  } else if (OB_ISNULL(guard.get_tenant())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get tenant fail, tenant is NULL", KR(ret), K(ddl_tenant_id));
  } else {
    tenant = guard.get_tenant();
    is_tenant_served = true;
  }

  if (! task.is_sys_ls_heartbeat()) {
    ISTAT("[DDL] detect tenant DDL",
        K(ddl_tenant_id),
        K(is_tenant_served),
        "tenant_state", NULL == tenant ? "NONE" : ObLogTenant::print_state(tenant->get_tenant_state()),
        "task_type", PartTransTask::print_task_type(task.get_type()),
        "schema_version", task.get_local_schema_version(),
        "log_lsn", task.get_commit_log_lsn(),
        "commit_ts", NTS_TO_STR(task.get_commit_ts()),
        "delay", NTS_TO_DELAY(task.get_commit_ts()));
  }

  return ret;
}

int ObLogSysLsTaskHandler::update_sys_ls_info_(
    PartTransTask &task,
    ObLogTenant &tenant)
{
  int ret = OB_SUCCESS;

  // Update DDL information whenever a tenant is served, regardless of whether the task type is a DDL task or a heartbeat task
  if (OB_FAIL(tenant.update_sys_ls_info(task))) {
    LOG_ERROR("update tenant ddl info fail", KR(ret), K(tenant), K(task));
  } else {
  }

  return ret;
}

}
}

#undef _STAT
#undef STAT
#undef _ISTAT
#undef ISTAT
#undef _DSTAT
