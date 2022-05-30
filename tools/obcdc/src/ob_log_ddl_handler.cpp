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

#include "ob_log_ddl_handler.h"

#include "ob_log_ddl_parser.h"          // IObLogDdlParser
#include "ob_log_sequencer1.h"          // IObLogSequencer
#include "ob_log_committer.h"           // IObLogCommitter
#include "ob_log_instance.h"            // IObLogErrHandler, TCTX
#include "ob_log_part_trans_task.h"     // PartTransTask
#include "ob_log_schema_getter.h"       // IObLogSchemaGetter
#include "ob_log_tenant_mgr.h"          // IObLogTenantMgr
#include "ob_log_table_matcher.h"       // IObLogTableMatcher
#include "ob_log_config.h"              // TCONF
#include "share/ob_cluster_version.h"   // GET_MIN_CLUSTER_VERSION

#define _STAT(level, fmt, args...) _OBLOG_LOG(level, "[STAT] [DDL_HANDLER] " fmt, ##args)
#define STAT(level, fmt, args...) OBLOG_LOG(level, "[STAT] [DDL_HANDLER] " fmt, ##args)
#define _ISTAT(fmt, args...) _STAT(INFO, fmt, ##args)
#define ISTAT(fmt, args...) STAT(INFO, fmt, ##args)
#define _DSTAT(fmt, args...) _STAT(DEBUG, fmt, ##args)

#define IGNORE_SCHEMA_ERROR(ret, args...) \
    if (OB_TENANT_HAS_BEEN_DROPPED == ret) { \
      LOG_WARN("ignore DDL on schema error, tenant may be dropped in future", ##args); \
      ret = OB_SUCCESS; \
    }

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;

namespace liboblog
{

////////////////////////////// ObLogDDLHandler::TaskQueue //////////////////////////////

ObLogDDLHandler::TaskQueue::TaskQueue() :
    queue_()
{}

ObLogDDLHandler::TaskQueue::~TaskQueue()
{
}

// The requirement task must be a DDL transactional task, as the code that maintains the progress depends on
int ObLogDDLHandler::TaskQueue::push(PartTransTask *task)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(task)) {
    LOG_ERROR("invalid task", K(task));
    ret = OB_INVALID_ARGUMENT;
  } else {
    queue_.push(task);
  }
  return ret;
}

void ObLogDDLHandler::TaskQueue::pop()
{
  (void)queue_.pop();
}

int64_t ObLogDDLHandler::TaskQueue::size() const
{
  return queue_.size();
}

// This function is only the observer, only read the top element
// It must be guaranteed that the person calling this function is the only consumer, i.e. no one else will pop elements during the call to this function
int ObLogDDLHandler::TaskQueue::next_ready_to_handle(const int64_t timeout,
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
      LOG_ERROR("invalid error, top task is NULL", K(queue_.top()));
      ret = OB_ERR_UNEXPECTED;
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

///////////////////////////////// ObLogDDLHandler /////////////////////////////////

ObLogDDLHandler::ObLogDDLHandler() :
    inited_(false),
    ddl_parser_(NULL),
    sequencer_(NULL),
    err_handler_(NULL),
    schema_getter_(NULL),
    skip_reversed_schema_version_(false),
    handle_pid_(0),
    stop_flag_(true),
    ddl_fetch_queue_(),
    wait_formatted_cond_()
{}

ObLogDDLHandler::~ObLogDDLHandler()
{
  destroy();
}

int ObLogDDLHandler::init(IObLogDdlParser *ddl_parser,
    IObLogSequencer *sequencer,
    IObLogErrHandler *err_handler,
    IObLogSchemaGetter *schema_getter,
    const bool skip_reversed_schema_version)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("init twice", K(inited_));
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(ddl_parser_ = ddl_parser)
      || OB_ISNULL(sequencer_ = sequencer)
      || OB_ISNULL(err_handler_ = err_handler)
      || OB_ISNULL(schema_getter_ = schema_getter)) {
    LOG_ERROR("invalid argument", K(ddl_parser), K(sequencer), K(err_handler), K(schema_getter));
    ret = OB_INVALID_ARGUMENT;
  } else {
    skip_reversed_schema_version_ = skip_reversed_schema_version;
    handle_pid_ = 0;
    stop_flag_ = true;
    inited_ = true;
  }

  return ret;
}

void ObLogDDLHandler::destroy()
{
  stop();

  inited_ = false;
  ddl_parser_ = NULL;
  sequencer_ = NULL;
  err_handler_ = NULL;
  schema_getter_ = NULL;
  skip_reversed_schema_version_ = false;
  handle_pid_ = 0;
  stop_flag_ = true;
}

int ObLogDDLHandler::start()
{
  int ret = OB_SUCCESS;
  int pthread_ret = 0;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init", K(inited_));
    ret = OB_NOT_INIT;
  } else if (stop_flag_) {
    stop_flag_ = false;

    if (0 != (pthread_ret = pthread_create(&handle_pid_, NULL, handle_thread_func_, this))){
      LOG_ERROR("create DDL handle thread fail", K(pthread_ret), KERRNOMSG(pthread_ret));
      ret = OB_ERR_UNEXPECTED;
    } else {
      LOG_INFO("start DDL handle thread succ");
    }

    if (OB_FAIL(ret)) {
      stop_flag_ = true;
    }
  }

  return ret;
}

void ObLogDDLHandler::stop()
{
  if (inited_) {
    stop_flag_ = true;

    if (0 != handle_pid_) {
      int pthread_ret = pthread_join(handle_pid_, NULL);

      if (0 != pthread_ret) {
        LOG_ERROR("join DDL handle thread fail", K(handle_pid_), K(pthread_ret),
            KERRNOMSG(pthread_ret));
      } else {
        LOG_INFO("stop DDL handle thread succ");
      }

      handle_pid_ = 0;
    }
  }
}

int ObLogDDLHandler::push(PartTransTask *task, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ddl_parser_)) {
    LOG_ERROR("invalid DDL parser", K(ddl_parser_));
    ret = OB_NOT_INIT;
  }
  // Only DDL partitions are supported, as well as heartbeats for DDL partitions
  else if (OB_UNLIKELY(! task->is_ddl_trans()
      && ! task->is_ddl_part_heartbeat()
      && !task->is_ddl_offline_task())) {
    LOG_ERROR("task is not DDL trans, or HEARTBEAT, or OFFLINE task, not supported", KPC(task));
    ret = OB_NOT_SUPPORTED;
  }
  // DDL task have to push to the DDL parser first, because the task will retry after the task push DDL parser times out.
  // that is, the same task may be pushed multiple times.To avoid the same task being added to the queue more than once, the DDL parser is pushed first
  else if (task->is_ddl_trans() && OB_FAIL(ddl_parser_->push(*task, timeout))) {
    if (OB_IN_STOP_STATE != ret && OB_TIMEOUT != ret) {
      LOG_ERROR("push task into DDL parser fail", KR(ret), K(task));
    }
  }
  // Add the task to the Fetch queue without timeout failure, ensuring that it will only be pushed once in the Parser
  else if (OB_FAIL(ddl_fetch_queue_.push(task))) {
    LOG_ERROR("push DDL task into fetch queue fail", KR(ret), KPC(task));
  } else {
    // success
  }
  return ret;
}

int ObLogDDLHandler::get_progress(uint64_t &ddl_min_progress_tenant_id,
    int64_t &ddl_min_progress,
    uint64_t &ddl_min_handle_log_id)
{
  int ret = OB_SUCCESS;
  IObLogTenantMgr *tenant_mgr = TCTX.tenant_mgr_;

  ddl_min_progress = OB_INVALID_TIMESTAMP;
  ddl_min_handle_log_id = OB_INVALID_ID;
  ddl_min_progress_tenant_id = OB_INVALID_TENANT_ID;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogDDLHandler not init", K(inited_));
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(tenant_mgr)) {
    LOG_ERROR("tenant_mgr_ is NULL", K(tenant_mgr));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(tenant_mgr->get_ddl_progress(ddl_min_progress_tenant_id, ddl_min_progress,
      ddl_min_handle_log_id))) {
    LOG_ERROR("get_ddl_progress fail", KR(ret), K(ddl_min_progress_tenant_id),
        K(ddl_min_progress), K(ddl_min_handle_log_id));
  } else {
    // success
  }

  return ret;
}

int64_t ObLogDDLHandler::get_part_trans_task_count() const
{
  return ddl_fetch_queue_.size();
}

void *ObLogDDLHandler::handle_thread_func_(void *arg)
{
  if (NULL != arg) {
    ObLogDDLHandler *ddl_handler = static_cast<ObLogDDLHandler *>(arg);
    ddl_handler->handle_ddl_routine();
  }

  return NULL;
}

int ObLogDDLHandler::next_task_(PartTransTask *&task)
{
  int ret = OB_SUCCESS;
  // Get the next task to be processed
  RETRY_FUNC(stop_flag_, ddl_fetch_queue_, next_ready_to_handle, DATA_OP_TIMEOUT, task,
      wait_formatted_cond_);

  // Unconditionally pop out
  ddl_fetch_queue_.pop();

  if (OB_SUCCESS != ret) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("task_queue next_ready_to_handle fail", KR(ret));
    }
  } else if (OB_ISNULL(task)) {
    LOG_ERROR("invalid task", K(task));
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

int ObLogDDLHandler::handle_task_(PartTransTask &task,
    bool &is_schema_split_mode,
    const uint64_t ddl_tenant_id,
    ObLogTenant *tenant,
    const bool is_tenant_served)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! task.is_ddl_trans()
      && ! task.is_ddl_part_heartbeat()
      && ! task.is_ddl_offline_task())) {
    LOG_ERROR("task is not DDL trans task, or HEARTBEAT, or OFFLINE task, not supported", K(task));
    ret = OB_NOT_SUPPORTED;
  } else if (task.is_ddl_offline_task()) {
    // OFFLINE tasks for DDL partitions
    if (OB_FAIL(handle_ddl_offline_task_(task))) {
      LOG_ERROR("handle_ddl_offline_task_ fail", KR(ret), K(task));
    }
  } else if (! is_tenant_served) {
    // Unserviced tenant DDL, ignore it
    ISTAT("[DDL] tenent is not served, ignore DDL task", K(ddl_tenant_id), K(is_schema_split_mode),
        K(task), KPC(tenant));
    // Mark all binlog records as invalid
    mark_all_binlog_records_invalid_(task);
  } else if (OB_ISNULL(tenant)) {
    LOG_ERROR("invalid tenant", KPC(tenant), K(ddl_tenant_id), K(is_tenant_served), K(task));
    ret = OB_ERR_UNEXPECTED;
  }
  // An error is reported if the tenant is not in the service state.
  // The current implementation assumes that the tenant is in service during all DDL processing under the tenant, and that the tenant is taken offline by the DDL offline task
  else if (OB_UNLIKELY(! tenant->is_serving())) {
    LOG_ERROR("tenant state is not serving, unexpected", KPC(tenant), K(task),
        K(is_schema_split_mode), K(ddl_tenant_id), K(is_tenant_served));
    ret = OB_ERR_UNEXPECTED;
  } else {
    // The following handles DDL transaction tasks and DDL heartbeat tasks
    if (task.is_ddl_trans() && OB_FAIL(handle_ddl_trans_(task, is_schema_split_mode, *tenant))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("handle_ddl_trans_ fail", KR(ret), K(task), K(ddl_tenant_id), K(tenant),
            K(is_schema_split_mode), K(is_tenant_served));
      }
    }
    // Both DDL transactions and heartbeats update DDL information
    else if (OB_FAIL(update_ddl_info_(task, is_schema_split_mode, *tenant))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("update_ddl_info_ fail", KR(ret), K(task), K(is_schema_split_mode), KPC(tenant));
      }
    }
  }
  return ret;
}

int ObLogDDLHandler::dispatch_task_(PartTransTask *task, ObLogTenant *tenant,
    const bool is_tenant_served)
{
  int ret = OB_SUCCESS;
  IObLogCommitter *trans_committer = TCTX.committer_;

  if (OB_ISNULL(sequencer_)) {
    LOG_ERROR("invalid committer", K(sequencer_));
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(task) || (is_tenant_served && OB_ISNULL(tenant)) || (! is_tenant_served && NULL != tenant)) {
    LOG_ERROR("invalid tenant", K(is_tenant_served), K(tenant));
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (task->is_ddl_trans()) {
      if (OB_FAIL(sequencer_->push(task, stop_flag_))) {
        LOG_ERROR("sequencer_ push fail", KR(ret), KPC(task));
      }
    } else {
      if (OB_ISNULL(trans_committer)) {
        LOG_ERROR("trans_committer is NULL");
        ret = OB_ERR_UNEXPECTED;
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

void ObLogDDLHandler::handle_ddl_routine()
{
  int ret = OB_SUCCESS;
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
      uint64_t ddl_tenant_id = OB_INVALID_ID;
      bool is_schema_split_mode = TCTX.is_schema_split_mode_;

      // First obtain tenant information, the tenant may not serve
      if (OB_FAIL(get_tenant_(*task,
          is_schema_split_mode,
          ddl_tenant_id,
          guard,
          tenant,
          is_tenant_served))) {
        LOG_ERROR("get_tenant_ fail", KR(ret), KPC(task), K(is_schema_split_mode));
      }
      // Then process the task
      else if (OB_FAIL(handle_task_(*task,
          is_schema_split_mode,
          ddl_tenant_id,
          tenant,
          is_tenant_served))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("handle DDL task fail", KR(ret), KPC(task), K(is_schema_split_mode),
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

int ObLogDDLHandler::handle_ddl_offline_task_(const PartTransTask &task)
{
  int ret = OB_SUCCESS;
  IObLogTenantMgr *tenant_mgr = TCTX.tenant_mgr_;
  // DDL __all_ddl_operation table offline tasks, must be schema split mode
  // If a offline task is received, it means that all DDLs of the tenant have been processed and it is safe to delete the tenant
  uint64_t ddl_tenant_id = task.get_tenant_id();

  ISTAT("[DDL] [DDL_OFFLINE_TASK] begin drop tenant", K(ddl_tenant_id),
      "pkey", task.get_partition());

  if (OB_ISNULL(tenant_mgr)) {
    LOG_ERROR("invalid tenant mgr", K(tenant_mgr));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(tenant_mgr->drop_tenant(ddl_tenant_id, "DDL_OFFLINE_TASK"))) {
    LOG_ERROR("tenant mgr drop tenant fail", KR(ret), K(ddl_tenant_id), K(task));
  } else {
    ISTAT("[DDL] [DDL_OFFLINE_TASK] drop tenant succ", K(ddl_tenant_id),
        "pkey", task.get_partition());
  }

  return ret;
}

int64_t ObLogDDLHandler::decide_ddl_tenant_id_for_schema_non_split_mode_(const PartTransTask &task) const
{
  uint64_t ddl_tenant_id = OB_INVALID_TENANT_ID;
  uint64_t task_op_tenant_id = OB_INVALID_TENANT_ID;
  int64_t stmt_index = 0;
  const int64_t stmt_num = task.get_stmt_num();

  // Iterate through each statement of the DDL to determine the tenant ID corresponding to the DDL in non-split mode
  IStmtTask *stmt_task = task.get_stmt_list().head_;
  while (NULL != stmt_task && OB_INVALID_TENANT_ID == ddl_tenant_id) {
    DdlStmtTask *ddl_stmt = dynamic_cast<DdlStmtTask *>(stmt_task);
    if (OB_NOT_NULL(ddl_stmt)) {
      const uint64_t op_tenant_id = ddl_stmt->get_op_tenant_id();
      const uint64_t exec_tenant_id = ddl_stmt->get_exec_tenant_id();
      const int64_t op_type = ddl_stmt->get_operation_type();
      const int64_t op_schema_version = ddl_stmt->get_op_schema_version();

      // Record op_tenant_id, whichever is the first
      if (OB_INVALID_TENANT_ID == task_op_tenant_id) {
        task_op_tenant_id = op_tenant_id;
      }

      // ALTER_TENANT takes precedence over exec_tenant_id, if it is not allowed, then set it to SYS
      if (OB_DDL_ALTER_TENANT == op_type) {
        ddl_tenant_id = exec_tenant_id;

        // If exec_tenant_id is buggy and neither equal to SYS nor op_tenant_id, then error is reported and set to SYS
        if (op_tenant_id != exec_tenant_id && OB_SYS_TENANT_ID != exec_tenant_id) {
          ddl_tenant_id = OB_SYS_TENANT_ID;
          LOG_ERROR("ALTER_TENANT DDL exec_tenant_id is different with op_tenant_id and SYS",
              K(op_tenant_id), K(exec_tenant_id), KPC(ddl_stmt), K(task));
        }
      }
      // In case of tenant creation and tenant deletion DDL, the tenant ID is forced to be set to SYS
      else if (OB_DDL_ADD_TENANT == op_type
          || OB_DDL_ADD_TENANT_END == op_type
          || OB_DDL_ADD_TENANT_START == op_type
          || OB_DDL_DEL_TENANT == op_type
          || OB_DDL_DEL_TENANT_START == op_type
          || OB_DDL_DEL_TENANT_END == op_type) {
        ddl_tenant_id = OB_SYS_TENANT_ID;
      }

      ISTAT("[DDL] [DECIDE_TENANT_ID] SCAN_DDL_STMT",
          K(stmt_index), K(stmt_num), K(ddl_tenant_id),
          K(op_tenant_id), K(exec_tenant_id), K(op_schema_version),
          K(op_type), "op_type", ObSchemaOperation::type_str((ObSchemaOperationType)op_type),
          "ddl_stmt", ddl_stmt->get_ddl_stmt_str());
    }

    stmt_index++;
    // next statement
    stmt_task = stmt_task->get_next();
  }

  // If not set tenant_id during the scan, set to op_tenant_id
  if (OB_INVALID_TENANT_ID == ddl_tenant_id) {
    ddl_tenant_id = task_op_tenant_id;

    // If it still doesn't work, set it to SYS
    if (OB_INVALID_TENANT_ID == ddl_tenant_id) {
      ddl_tenant_id = OB_SYS_TENANT_ID;
    }
  }

  ISTAT("[DDL] [DECIDE_TENANT_ID] DONE", K(ddl_tenant_id), "scan_stmt_num", stmt_index, K(stmt_num),
      K(task_op_tenant_id), "schema_version", task.get_local_schema_version());
  return ddl_tenant_id;
}

int ObLogDDLHandler::decide_ddl_tenant_id_(const PartTransTask &task,
    const bool is_schema_split_mode,
    uint64_t &ddl_tenant_id)
{
  int ret = OB_SUCCESS;
  ddl_tenant_id = OB_INVALID_TENANT_ID;

  // DDL partition heartbeat and DDL offline tasks, whether in split mode or not, use the tenant ID of the DDL partition
  if (task.is_ddl_part_heartbeat() || task.is_ddl_offline_task()) {
    ddl_tenant_id = task.get_tenant_id();
  } else if (task.is_ddl_trans()) {
    // For DDL partitioned tasks, split mode uses the tenant ID of the DDL partition, and non-split mode uses the executor tenant
    if (is_schema_split_mode) {
      ddl_tenant_id = task.get_tenant_id();
    } else {
      ddl_tenant_id = decide_ddl_tenant_id_for_schema_non_split_mode_(task);
    }
  } else {
    LOG_ERROR("unknown DDL task, cannot decide DDL tenant id", K(task), K(is_schema_split_mode));
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

int ObLogDDLHandler::get_tenant_(
    PartTransTask &task,
    const bool is_schema_split_mode,
    uint64_t &ddl_tenant_id,
    ObLogTenantGuard &guard,
    ObLogTenant *&tenant,
    bool &is_tenant_served)
{
  int ret = OB_SUCCESS;

  // Default setting for tenant non-service
  tenant = NULL;
  is_tenant_served = false;

  // Determine the tenant ID to which the task belongs
  if (OB_FAIL(decide_ddl_tenant_id_(task, is_schema_split_mode, ddl_tenant_id))) {
    LOG_ERROR("decide ddl tenant id fail", KR(ret), K(task), K(is_schema_split_mode));
  } else if (OB_FAIL(TCTX.get_tenant_guard(ddl_tenant_id, guard))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // not serve ddl if tenant not exist
      if (is_schema_split_mode) {
        // In split mode, the tenant cannot not exist, and if it does not exist, then the tenant is deleted in advance, which must have a bug
        LOG_ERROR("tenant not exist when handle DDL task under schema split mode, unexpected",
            KR(ret), K(ddl_tenant_id), K(is_schema_split_mode), K(task));
        ret = OB_ERR_UNEXPECTED;
      } else {
        // schema non-split mode, the sys tenant will pull the DDL of all tenants and will encounter the DDL of the unserviced tenant
        ret = OB_SUCCESS;
        is_tenant_served = false;
      }
    } else {
      LOG_ERROR("get_tenant fail", KR(ret), K(ddl_tenant_id), K(guard));
    }
  } else if (OB_ISNULL(guard.get_tenant())) {
    LOG_ERROR("get tenant fail, tenant is NULL", K(ddl_tenant_id));
    ret = OB_ERR_UNEXPECTED;
  } else {
    tenant = guard.get_tenant();
    is_tenant_served = true;
  }

  if (! task.is_ddl_part_heartbeat()) {
    ISTAT("[DDL] detect tenant DDL",
        K(ddl_tenant_id),
        K(is_tenant_served),
        "tenant_state", NULL == tenant ? "NONE" : ObLogTenant::print_state(tenant->get_tenant_state()),
        "task_type", PartTransTask::print_task_type(task.get_type()),
        K(is_schema_split_mode),
        "schema_version", task.get_local_schema_version(),
        "log_id", task.get_prepare_log_id(),
        "tstamp", TS_TO_STR(task.get_timestamp()),
        "delay", TS_TO_DELAY(task.get_timestamp()));
  }

  return ret;
}

struct DDLInfoUpdater
{
  int err_code_;
  uint64_t host_ddl_tenant_id_;
  PartTransTask &host_ddl_task_;

  DDLInfoUpdater(PartTransTask &task, const uint64_t tenant_id) :
      err_code_(OB_SUCCESS),
      host_ddl_tenant_id_(tenant_id),
      host_ddl_task_(task)
  {}

  bool operator()(const TenantID &tid, ObLogTenant *tenant)
  {
    int ret = OB_SUCCESS;
    // For non-host tenants, execute the update_ddl_info() action
    if (tid.tenant_id_ != host_ddl_tenant_id_ && NULL != tenant) {
      // Tenants that are not in service are filtered internally
      if (OB_FAIL(tenant->update_ddl_info(host_ddl_task_))) {
        LOG_ERROR("update_ddl_info fail", KR(ret), K(tid), K(host_ddl_task_), KPC(tenant));
      }
    }
    err_code_ = ret;
    return OB_SUCCESS == ret;
  }
};

int ObLogDDLHandler::update_ddl_info_(PartTransTask &task,
    const bool is_schema_split_mode,
    ObLogTenant &tenant)
{
  int ret = OB_SUCCESS;
  // Update DDL information whenever a tenant is served, regardless of whether the task type is a DDL task or a heartbeat task
  if (OB_FAIL(tenant.update_ddl_info(task))) {
    LOG_ERROR("update tenant ddl info fail", KR(ret), K(tenant), K(task));
  } else {
    // If it is schema non-split mode, update DDL information for all tenants
    // Since this tenant has already been updated, filter this tenant here
    //
    // The DDL of tenant split will dynamically change is_schema_split_mode, i.e. the tenant split DDL does not need to update the state for each tenant
    if (! is_schema_split_mode) {
      uint64_t ddl_tenant_id = tenant.get_tenant_id();
      DDLInfoUpdater updater(task, ddl_tenant_id);

      // Update DDL Info for all tenants
      if (OB_FAIL(for_each_tenant_(updater))) {
        LOG_ERROR("update ddl info for all tenant fail", KR(ret), K(task),
            K(is_schema_split_mode), K(tenant));
      }
    }
  }
  return ret;
}

template <typename Func>
int ObLogDDLHandler::for_each_tenant_(Func &func)
{
  int ret = OB_SUCCESS;
  IObLogTenantMgr *tenant_mgr = TCTX.tenant_mgr_;
  if (OB_ISNULL(tenant_mgr)) {
    LOG_ERROR("invalid tenant mgr", K(tenant_mgr));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(dynamic_cast<ObLogTenantMgr*>(tenant_mgr)->for_each_tenant(func))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("for each tenant fail", KR(ret), K(func.err_code_));
    }
  } else if (OB_UNLIKELY(OB_SUCCESS != func.err_code_)) {
    // Error during scanning of all tenants
    ret = func.err_code_;
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("for each tenant fail", KR(ret), K(func.err_code_));
    }
  } else {
    // success
  }
  return ret;
}

void ObLogDDLHandler::mark_stmt_binlog_record_invalid_(DdlStmtTask &stmt_task)
{
  if (NULL != stmt_task.get_binlog_record()) {
    stmt_task.get_binlog_record()->set_is_valid(false);
  }
}

void ObLogDDLHandler::mark_all_binlog_records_invalid_(PartTransTask &task)
{
  DdlStmtTask *stmt_task = static_cast<DdlStmtTask *>(task.get_stmt_list().head_);
  while (NULL != stmt_task) {
    mark_stmt_binlog_record_invalid_(*stmt_task);
    stmt_task = static_cast<DdlStmtTask *>(stmt_task->get_next());
  }
}

int ObLogDDLHandler::get_old_schema_version_(const uint64_t tenant_id,
    PartTransTask &task,
    const int64_t tenant_ddl_cur_schema_version,
    int64_t &old_schema_version)
{
  int ret = OB_SUCCESS;
  int64_t ddl_schema_version = task.get_local_schema_version();

  // 1. use tenant_ddl_cur_schema_version as old_schema_version by default
  // 2. Special case: when the schema version is reversed and skip_reversed_schema_version_ = true,
  // then to ensure that the corresponding schema is obtained, take the suitable schema version[official version] as old_schema_version
  //
  // e.g.: cur_schema_version=104, reversed ddl_schema_version=90(drop database operation),
  // then according to schema_version =88 can guarantee to get the corresponding database schema
  old_schema_version = tenant_ddl_cur_schema_version;

  if (OB_UNLIKELY(ddl_schema_version <= tenant_ddl_cur_schema_version)) {
    // 遇到Schema版本反转情况，如果skip_reversed_schema_version_=true忽略，否则报错退出
    LOG_ERROR("DDL schema version is reversed", K(ddl_schema_version), K(tenant_ddl_cur_schema_version),
        K(task));

    if (skip_reversed_schema_version_) {
      if (OB_FAIL(get_schema_version_by_timestamp_util_succ_(tenant_id, ddl_schema_version, old_schema_version))) {
        if (OB_TENANT_HAS_BEEN_DROPPED == ret) {
          // do nothing
        } else if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("get_schema_version_by_timestamp_util_succ_ fail", KR(ret), K(tenant_id), K(ddl_schema_version),
              K(old_schema_version));
        }
      } else if (OB_UNLIKELY(OB_INVALID_TIMESTAMP == old_schema_version)) {
        LOG_ERROR("old_schema_version is not valid", K(old_schema_version));
        ret = OB_ERR_UNEXPECTED;
      } else {
        LOG_WARN("ignore DDL schema version is reversed, "
            "set old schema version as suitable ddl_schema_version",
            K(skip_reversed_schema_version_), K(old_schema_version),
          K(ddl_schema_version), K(tenant_ddl_cur_schema_version));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
    }
  }

  return ret;
}

// @retval OB_SUCCESS                   success
// @retval OB_TENANT_HAS_BEEN_DROPPED   tenant has been dropped
// @retval OB_IN_STOP_STATE             exit
// @retval other error code             fail
int ObLogDDLHandler::get_schema_version_by_timestamp_util_succ_(const uint64_t tenant_id,
    const int64_t ddl_schema_version,
    int64_t &old_schema_version)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(schema_getter_)) {
    LOG_ERROR("schema_getter_ is NULL", K(schema_getter_));
    ret = OB_ERR_UNEXPECTED;
  } else {
    RETRY_FUNC(stop_flag_, (*schema_getter_), get_schema_version_by_timestamp, tenant_id, ddl_schema_version -1,
        old_schema_version, DATA_OP_TIMEOUT);
  }

  return ret;
}

int ObLogDDLHandler::filter_ddl_stmt_(ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    IObLogTenantMgr &tenant_mgr,
    bool &chosen,
    const bool only_filter_by_tenant /* = false */)
{
  int ret = OB_SUCCESS;
  ObSchemaOperationType op_type =
      static_cast<ObSchemaOperationType>(ddl_stmt.get_operation_type());

  chosen = false;

  // Note that.
  // 0.1 Push up schema version number based on op_type and tenant, only_filter_by_tenant=false
  // 0.2 Filter ddl output to committer based on tenant, only_filter_by_tenant=true
  //
  // 1. Do not filter add tenant statement because new tenants may be added (located in whitelist)
  // OB_DDL_ADD_TENANT corresponds to the version before schema splitting
  // OB_DDL_ADD_TENANT_START records ddl_stmt, which corresponds to the version after schema split, and outputs only ddl_stmt_str
  // OB_DDL_ADD_TENANT_END
  // OB_DDL_FINISH_SCHEMA_SPLIT does not filter by default
  //
  // 2. do not filter tenant del tenant statements that are in the whitelist
  // OB_DDL_DEL_TENANT
  // OB_DDL_DEL_TENANT_START
  // OB_DDL_DEL_TENANT_END
  // 3. If a tenant is created after the start bit, the start moment will add all tenants at that time (located in the whitelist)
  // 4. filter outline
  if (only_filter_by_tenant) {
    if (OB_FAIL(tenant_mgr.filter_ddl_stmt(tenant.get_tenant_id(), chosen))) {
      LOG_ERROR("filter ddl stmt fail", KR(ret), K(tenant.get_tenant_id()), K(chosen));
    }
  } else {
    if (OB_DDL_ADD_TENANT == op_type
        || OB_DDL_ADD_TENANT_START == op_type
        || OB_DDL_ADD_TENANT_END == op_type
        || OB_DDL_FINISH_SCHEMA_SPLIT == op_type) {
      chosen = true;
    } else if (OB_DDL_DEL_TENANT == op_type
        || OB_DDL_DEL_TENANT_START == op_type
        || OB_DDL_DEL_TENANT_END == op_type) {
      chosen = true;
    } else if (OB_DDL_CREATE_OUTLINE == op_type
        || OB_DDL_REPLACE_OUTLINE == op_type
        || OB_DDL_DROP_OUTLINE == op_type
        || OB_DDL_ALTER_OUTLINE== op_type) {
      chosen = false;
    }
    // filter based on tenant that ddl belongs to
    else if (OB_FAIL(tenant_mgr.filter_ddl_stmt(tenant.get_tenant_id(), chosen))) {
      LOG_ERROR("filter ddl stmt fail", KR(ret), K(tenant.get_tenant_id()), K(chosen));
    } else {
      // succ
    }
  }

  if (OB_SUCCESS == ret && ! chosen) {
    _ISTAT("[DDL] [FILTER_DDL_STMT] TENANT_ID=%lu OP_TYPE=%s(%d) SCHEMA_VERSION=%ld "
        "SCHEMA_DELAY=%.3lf(sec) CUR_SCHEMA_VERSION=%ld OP_TABLE_ID=%ld OP_TENANT_ID=%ld "
        "EXEC_TENANT_ID=%lu OP_DB_ID=%ld OP_TG_ID=%ld DDL_STMT=[%s] ONLY_FILTER_BY_TENANT=%d",
        tenant.get_tenant_id(),
        ObSchemaOperation::type_str(op_type), op_type,
        ddl_stmt.get_op_schema_version(),
        get_delay_sec(ddl_stmt.get_op_schema_version()),
        tenant.get_schema_version(),
        ddl_stmt.get_op_table_id(),
        ddl_stmt.get_op_tenant_id(),
        ddl_stmt.get_exec_tenant_id(),
        ddl_stmt.get_op_database_id(),
        ddl_stmt.get_op_tablegroup_id(),
        to_cstring(ddl_stmt.get_ddl_stmt_str()),
        only_filter_by_tenant);
  }

  return ret;
}

int ObLogDDLHandler::handle_ddl_trans_(PartTransTask &task,
    bool &is_schema_split_mode,
    ObLogTenant &tenant)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! task.is_ddl_trans())) {
    LOG_ERROR("invalid ddl task which is not DDL trans", K(task));
    ret = OB_INVALID_ARGUMENT;
  }
  // Iterate through all DDL statements
  else if (OB_FAIL(handle_tenant_ddl_task_(task, is_schema_split_mode, tenant))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("handle_tenant_ddl_task_ fail", KR(ret), K(task), K(is_schema_split_mode), K(tenant));
    }
  } else {
    // If non-split mode, issue Virtual DDL Task for all tenants, except itself.
    // Ensure that in non-split mode, all tenant schemas are refreshed to the latest version, and optimize
    // individual tenant schema refreshes, which can affect the refresh speed if individual tenants use different versions of schema
    //
    // Upgrade process processing: update is_schema_split_mode when SCHEMA_SPLIT_FINISH DDL, that is, tenant split DDL does not need to issue VirtualDDLTask
    if (! is_schema_split_mode) {
    }
  }
  return ret;
}

int ObLogDDLHandler::handle_tenant_ddl_task_(PartTransTask &task,
    bool &is_schema_split_mode,
    ObLogTenant &tenant)
{
  int ret = OB_SUCCESS;
  IObLogTenantMgr *tenant_mgr = TCTX.tenant_mgr_;
  int64_t ddl_part_tstamp = task.get_timestamp();
  int64_t ddl_schema_version = task.get_local_schema_version();
  const int64_t checkpoint_seq = task.get_checkpoint_seq();
  int64_t old_schema_version = OB_INVALID_TIMESTAMP;
  int64_t new_schema_version = ddl_schema_version;    // Adopt ddl schema version as new schema version
  const uint64_t ddl_tenant_id = tenant.get_tenant_id();  // The tenant ID to which the DDL belongs
  const int64_t start_schema_version = tenant.get_start_schema_version();
  const int64_t tenant_ddl_cur_schema_version = tenant.get_schema_version();

  _ISTAT("[DDL] [HANDLE_TRANS] IS_SCHEMA_SPLIT_MODE=%d TENANT_ID=%ld STMT_COUNT=%ld CHECKPOINT_SEQ=%ld "
      "SCHEMA_VERSION=%ld CUR_SCHEMA_VERSION=%ld LOG_DELAY=%.3lf(sec) SCHEMA_DELAY=%.3lf(sec)",
      is_schema_split_mode, ddl_tenant_id, task.get_stmt_num(), checkpoint_seq, ddl_schema_version,
      tenant_ddl_cur_schema_version, get_delay_sec(ddl_part_tstamp),
      get_delay_sec(ddl_schema_version));

  if (OB_ISNULL(tenant_mgr)) {
    LOG_ERROR("tenant_mgr is NULL", K(tenant_mgr));
    ret = OB_ERR_UNEXPECTED;
  }
  // Ignore DDL operations that are smaller than the start Schema version
  else if (OB_UNLIKELY(ddl_schema_version <= start_schema_version)) {
    LOG_WARN("ignore DDL task whose schema version is not greater than start schema version",
        K(ddl_schema_version), K(start_schema_version), K(task));
    // Mark all binlog records as invalid
    mark_all_binlog_records_invalid_(task);
  }
  // Calculate the old_schema_version
  else if (OB_FAIL(get_old_schema_version_(ddl_tenant_id, task, tenant_ddl_cur_schema_version, old_schema_version))) {
    if (OB_TENANT_HAS_BEEN_DROPPED == ret) {
      // Tenant does not exist, or schema fetching failure, ignore this DDL statement
      LOG_WARN("get old schema version fail, tenant may be dropped, ignore",
          KR(ret), K(ddl_tenant_id), K(task), K(tenant_ddl_cur_schema_version), K(old_schema_version));
      // Set all records to be invalid
      mark_all_binlog_records_invalid_(task);
      ret = OB_SUCCESS;
    } else if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("get_old_schema_version_ fail", KR(ret), K(ddl_tenant_id), K(task), K(tenant_ddl_cur_schema_version),
          K(old_schema_version));
    }
  } else {
    // Iterate through each statement of the DDL
    IStmtTask *stmt_task = task.get_stmt_list().head_;
    bool only_filter_by_tenant = true;
    while (NULL != stmt_task && OB_SUCCESS == ret) {
      bool stmt_is_chosen = false;
      DdlStmtTask *ddl_stmt = dynamic_cast<DdlStmtTask *>(stmt_task);

      if (OB_UNLIKELY(! stmt_task->is_ddl_stmt()) || OB_ISNULL(ddl_stmt)) {
        LOG_ERROR("invalid DDL statement", KPC(stmt_task), K(ddl_stmt));
        ret = OB_ERR_UNEXPECTED;
      }
      // filter ddl stmt
      else if (OB_FAIL(filter_ddl_stmt_(tenant, *ddl_stmt, *tenant_mgr, stmt_is_chosen))) {
        LOG_ERROR("filter_ddl_stmt_ fail", KR(ret), KPC(ddl_stmt), K(tenant), K(stmt_is_chosen));
      } else if (! stmt_is_chosen) {
        // If the DDL statement is filtered, mark the binlog record as invalid
        mark_stmt_binlog_record_invalid_(*ddl_stmt);
      } else {
        // statements are not filtered, processing DDL statements
        if (OB_FAIL(handle_ddl_stmt_(tenant, task, *ddl_stmt, old_schema_version, new_schema_version,
            is_schema_split_mode))) {
          if (OB_IN_STOP_STATE != ret) {
            LOG_ERROR("handle_ddl_stmt_ fail", KR(ret), K(tenant), K(task), K(ddl_stmt),
                K(old_schema_version), K(new_schema_version));
          }
        }
        // The first filter_ddl_stmt_() will let go of some of the DDLs of the non-service tenants, and here it should be filtered again based on the tenant ID
        // Ensure that only the DDLs of whitelisted tenants are output
        else if (OB_FAIL(filter_ddl_stmt_(tenant, *ddl_stmt, *tenant_mgr, stmt_is_chosen, only_filter_by_tenant))) {
          LOG_ERROR("filter_ddl_stmt fail", KR(ret), KPC(ddl_stmt), K(tenant), K(stmt_is_chosen));
        } else if (! stmt_is_chosen) {
          // If the DDL statement is filtered, mark the binlog record as invalid
          mark_stmt_binlog_record_invalid_(*ddl_stmt);
        }
      }

      if (OB_SUCCESS == ret) {
        stmt_task = stmt_task->get_next();
      }
    }
  }

  return ret;
}

int ObLogDDLHandler::handle_ddl_stmt_(ObLogTenant &tenant,
    PartTransTask &task,
    DdlStmtTask &ddl_stmt,
    const int64_t old_schema_version,
    const int64_t new_schema_version,
    bool &is_schema_split_mode)
{
  int ret = OB_SUCCESS;
  ObSchemaOperationType op_type = (ObSchemaOperationType)ddl_stmt.get_operation_type();
  const int64_t checkpoint_seq = ddl_stmt.get_host().get_checkpoint_seq();

  _ISTAT("[DDL] [HANDLE_STMT] TENANT_ID=%lu OP_TYPE=%s(%d) OP_TABLE_ID=%ld SCHEMA_VERSION=%ld "
      "SCHEMA_DELAY=%.3lf(sec) CUR_SCHEMA_VERSION=%ld EXEC_TENANT_ID=%ld OP_TENANT_ID=%ld "
      "OP_TABLE_ID=%ld OP_DB_ID=%ld OP_TG_ID=%ld DDL_STMT=[%s] CHECKPOINT_SEQ=%ld TRANS_ID=%s",
      tenant.get_tenant_id(), ObSchemaOperation::type_str(op_type), op_type,
      ddl_stmt.get_op_table_id(),
      ddl_stmt.get_op_schema_version(),
      get_delay_sec(ddl_stmt.get_op_schema_version()),
      tenant.get_schema_version(),
      ddl_stmt.get_exec_tenant_id(),
      ddl_stmt.get_op_tenant_id(),
      ddl_stmt.get_op_table_id(),
      ddl_stmt.get_op_database_id(),
      ddl_stmt.get_op_tablegroup_id(),
      to_cstring(ddl_stmt.get_ddl_stmt_str()),
      checkpoint_seq,
      task.get_trans_id_str().ptr());

  switch (op_type) {
    case OB_DDL_DROP_TABLE : {
      ret = handle_ddl_stmt_drop_table_(tenant, ddl_stmt, old_schema_version, new_schema_version);
      break;
    }
    case OB_DDL_ALTER_TABLE : {
      ret = handle_ddl_stmt_alter_table_(tenant, ddl_stmt, old_schema_version, new_schema_version, "alter_table");
      break;
    }
    case OB_DDL_CREATE_TABLE : {
      ret = handle_ddl_stmt_create_table_(tenant, ddl_stmt, new_schema_version);
      break;
    }
    case OB_DDL_TABLE_RENAME : {
      ret = handle_ddl_stmt_rename_table_(tenant, ddl_stmt, old_schema_version, new_schema_version);
      break;
    }
    case OB_DDL_ADD_TABLEGROUP : {
      ret = handle_ddl_stmt_add_tablegroup_partition_(tenant, ddl_stmt, new_schema_version);
      break;
    }
    case OB_DDL_DEL_TABLEGROUP : {
      ret = handle_ddl_stmt_drop_tablegroup_partition_(tenant, ddl_stmt, old_schema_version, new_schema_version);
      break;
    }
    case OB_DDL_PARTITIONED_TABLEGROUP_TABLE : {
      ret = handle_ddl_stmt_split_tablegroup_partition_(tenant, ddl_stmt, new_schema_version);
      break;
    }
    case OB_DDL_SPLIT_TABLEGROUP_PARTITION : {
      ret = handle_ddl_stmt_split_tablegroup_partition_(tenant, ddl_stmt, new_schema_version);
      break;
    }
    case OB_DDL_ALTER_TABLEGROUP_ADD_TABLE :  {
      ret = handle_ddl_stmt_change_tablegroup_(tenant, ddl_stmt, old_schema_version, new_schema_version);
      break;
    }
    case OB_DDL_ALTER_TABLEGROUP_PARTITION : {
      // 1. tablegroup partitions are dynamically added and removed
      // 2. tablegroup splitting
      ret = handle_ddl_stmt_alter_tablegroup_partition_(tenant, ddl_stmt, old_schema_version, new_schema_version);
      break;
    }
    case OB_DDL_ADD_SUB_PARTITION : {
      ret = handle_ddl_stmt_alter_table_(tenant, ddl_stmt, old_schema_version, new_schema_version, "add_sub_partition");
      break;
    }
    case OB_DDL_DROP_SUB_PARTITION : {
      ret = handle_ddl_stmt_alter_table_(tenant, ddl_stmt, old_schema_version, new_schema_version, "drop_sub_partition");
      break;
    }
    case OB_DDL_TRUNCATE_TABLE_DROP : {
      ret = handle_ddl_stmt_truncate_table_drop_(tenant, ddl_stmt, old_schema_version, new_schema_version);
      break;
    }
    case OB_DDL_TRUNCATE_DROP_TABLE_TO_RECYCLEBIN : {
      ret = handle_ddl_stmt_truncate_drop_table_to_recyclebin_(tenant, ddl_stmt, old_schema_version,
          new_schema_version);
      break;
    }
    case OB_DDL_TRUNCATE_TABLE_CREATE : {
      ret = handle_ddl_stmt_truncate_table_create_(tenant, ddl_stmt, new_schema_version);
      break;
    }
    case OB_DDL_TRUNCATE_PARTITION: {
      ret = handle_ddl_stmt_alter_table_(tenant, ddl_stmt, old_schema_version, new_schema_version, "truncate_partition");
      break;
    }
    case OB_DDL_TRUNCATE_SUB_PARTITION: {
      ret = handle_ddl_stmt_alter_table_(tenant, ddl_stmt, old_schema_version, new_schema_version, "truncate_sub_partition");
      break;
    }
    case OB_DDL_DROP_TABLE_TO_RECYCLEBIN : {
      ret = handle_ddl_stmt_drop_table_to_recyclebin_(tenant, ddl_stmt, old_schema_version,
          new_schema_version);
      break;
    }
    case OB_DDL_ADD_TENANT : {
      ret = handle_ddl_stmt_add_tenant_(tenant, ddl_stmt, new_schema_version);
      break;
    }
    case OB_DDL_DEL_TENANT : {
      ret = handle_ddl_stmt_drop_tenant_(tenant, ddl_stmt, old_schema_version, new_schema_version);
      break;
    }
    case OB_DDL_ALTER_TENANT : {
      ret = handle_ddl_stmt_alter_tenant_(tenant, ddl_stmt, old_schema_version, new_schema_version);
      break;
    }
    case OB_DDL_ADD_TENANT_END: {
      ret = handle_ddl_stmt_add_tenant_(tenant, ddl_stmt, new_schema_version);
      break;
    }
    case OB_DDL_DEL_TENANT_START: {
      ret = handle_ddl_stmt_drop_tenant_(tenant, ddl_stmt, old_schema_version, new_schema_version, is_schema_split_mode,
          true/*is_del_tenant_start_op*/);
      break;
    }
    case OB_DDL_DEL_TENANT_END: {
      ret = handle_ddl_stmt_drop_tenant_(tenant, ddl_stmt, old_schema_version, new_schema_version, is_schema_split_mode,
          false/*is_del_tenant_start_op*/);
      break;
    }
    case OB_DDL_RENAME_TENANT: {
      ret = handle_ddl_stmt_rename_tenant_(tenant, ddl_stmt, old_schema_version, new_schema_version);
      break;
    }
    case OB_DDL_DROP_TENANT_TO_RECYCLEBIN: {
      ret = handle_ddl_stmt_drop_tenant_to_recyclebin_(tenant, ddl_stmt, old_schema_version,
          new_schema_version);
      break;
    }
    case OB_DDL_ALTER_DATABASE : {
      ret = handle_ddl_stmt_alter_database_(tenant, ddl_stmt, old_schema_version, new_schema_version);
      break;
    }
    case OB_DDL_DEL_DATABASE : {
      ret = handle_ddl_stmt_drop_database_(tenant, ddl_stmt, old_schema_version, new_schema_version);
      break;
    }
    case OB_DDL_RENAME_DATABASE : {
      ret = handle_ddl_stmt_rename_database_(tenant, ddl_stmt, old_schema_version, new_schema_version);
      break;
    }
    case OB_DDL_DROP_DATABASE_TO_RECYCLEBIN : {
      ret = handle_ddl_stmt_drop_database_to_recyclebin_(tenant, ddl_stmt, old_schema_version,
          new_schema_version);
      break;
    }
    case OB_DDL_CREATE_GLOBAL_INDEX: {
      // add global index
      ret = handle_ddl_stmt_create_index_(tenant, ddl_stmt, new_schema_version);
      break;
    }
    case OB_DDL_DROP_GLOBAL_INDEX: {
      // delete global index
      ret = handle_ddl_stmt_drop_index_(tenant, ddl_stmt, old_schema_version, new_schema_version);
      break;
    }
    case OB_DDL_CREATE_INDEX: {
      // add unique index to TableIDCache
      ret = handle_ddl_stmt_create_index_(tenant, ddl_stmt, new_schema_version);
      break;
    }
    case OB_DDL_DROP_INDEX : {
      // delete unique index from TableIDCache
      ret = handle_ddl_stmt_drop_index_(tenant, ddl_stmt, old_schema_version, new_schema_version);
      break;
    }
    case OB_DDL_DROP_INDEX_TO_RECYCLEBIN : {
      ret = handle_ddl_stmt_drop_index_to_recyclebin_(tenant, ddl_stmt, old_schema_version, new_schema_version);
      break;
    }
    // Modify the number of table partitions and start the partition split
    // Note: OB_DDL_FINISH_SPLIT represents the end of the split, but the end of the split does not write the DDL, so it will not be processed here
    // Non-partitioned table -> Partitioned table
    case OB_DDL_PARTITIONED_TABLE : {
      ret = handle_ddl_stmt_split_begin_(tenant, ddl_stmt, new_schema_version);
      break;
    }
    // Partition Table Split
    case OB_DDL_SPLIT_PARTITION: {
      ret = handle_ddl_stmt_split_begin_(tenant, ddl_stmt, new_schema_version);
      break;
    }
    case OB_DDL_FINISH_SCHEMA_SPLIT: {
      ret = handle_ddl_stmt_finish_schema_split_(tenant, ddl_stmt, new_schema_version,
          is_schema_split_mode);
      break;
    }

    default: {
      // Other DDL types, by default, are output directly and not processed
      // new version of schema parsing is used by default
      ret = handle_ddl_stmt_direct_output_(tenant, ddl_stmt, new_schema_version);
      break;
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("handle ddl statement fail", KR(ret), K(op_type), K(ddl_stmt));
    }
  }

  return ret;
}

int ObLogDDLHandler::handle_ddl_stmt_direct_output_(ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  ObSchemaOperationType op_type =
      static_cast<ObSchemaOperationType>(ddl_stmt.get_operation_type());
  _ISTAT("[DDL] [HANDLE_STMT] [DIRECT_OUTPUT] TENANT_ID=%ld OP_TYPE=%s(%d) DDL_STMT=[%s]",
      tenant.get_tenant_id(), ObSchemaOperation::type_str(op_type), op_type,
      to_cstring(ddl_stmt.get_ddl_stmt_str()));

  if (OB_FAIL(commit_ddl_stmt_(tenant, ddl_stmt, schema_version))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("commit_ddl_stmt_ fail", KR(ret), K(tenant), K(ddl_stmt), K(schema_version));
    }
  } else {
    // succ
  }

  return ret;
}

int ObLogDDLHandler::commit_ddl_stmt_(ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t schema_version,
    const char *tenant_name /* = NULL */,
    const char *db_name /* = NULL */,
    const bool filter_ddl_stmt /* = false */)
{
  int ret = OB_SUCCESS;
  ObLogSchemaGuard schema_guard;
  ObLogBR *br = ddl_stmt.get_binlog_record();
  ILogRecord *br_data = NULL;
  ObSchemaOperationType op_type = (ObSchemaOperationType)ddl_stmt.get_operation_type();
  const char *op_type_str = ObSchemaOperation::type_str(op_type);
  /// Need to get schema when the tenant name is empty
  /// Allow DB name to be empty
  bool need_get_schema = (NULL == tenant_name);

  // The tenant to which this DDL statement belongs is the one that follows
  uint64_t ddl_tenant_id = tenant.get_tenant_id();
  const int64_t checkpoint_seq = ddl_stmt.get_host().get_checkpoint_seq();

  if (ddl_stmt.get_ddl_stmt_str().empty()) {
    // Ignore empty DDL statements
    ISTAT("[DDL] [FILTER_DDL_STMT] ignore empty DDL",
        "schema_version", ddl_stmt.get_op_schema_version(), K(op_type_str),
        K(ddl_tenant_id),
        "op_tenant_id", ddl_stmt.get_op_tenant_id(),
        "exec_tenant_id", ddl_stmt.get_exec_tenant_id(),
        "op_database_id", ddl_stmt.get_op_database_id(),
        "op_table_id", ddl_stmt.get_op_table_id());

    // Set binlog record invalid
    mark_stmt_binlog_record_invalid_(ddl_stmt);
  } else if (filter_ddl_stmt) {
    // 过滤指定的DDL语句
    ISTAT("[DDL] [FILTER_DDL_STMT] ignore DDL",
        "schema_version", ddl_stmt.get_op_schema_version(), K(op_type_str),
        K(ddl_tenant_id),
        "op_tenant_id", ddl_stmt.get_op_tenant_id(),
        "exec_tenant_id", ddl_stmt.get_exec_tenant_id(),
        "op_database_id", ddl_stmt.get_op_database_id(),
        "op_table_id", ddl_stmt.get_op_table_id(),
        "ddl_stmt_str", ddl_stmt.get_ddl_stmt_str());

    // Set binlog record invalid
    mark_stmt_binlog_record_invalid_(ddl_stmt);
  } else if (OB_ISNULL(br) || OB_ISNULL(br_data = br->get_data())) {
    LOG_ERROR("invalid binlog record", K(br), K(br_data), K(ddl_stmt));
    ret = OB_ERR_UNEXPECTED;
  }
  // get tenant schmea and db schema
  else if (need_get_schema
      && OB_FAIL(get_schemas_for_ddl_stmt_(ddl_tenant_id, ddl_stmt, schema_version, schema_guard,
      tenant_name, db_name))) {
    if (OB_TENANT_HAS_BEEN_DROPPED == ret) {
      // Tenant does not exist, or schema fetching failure, ignore this DDL statement
      LOG_WARN("get schemas for ddl stmt fail, tenant may be dropped, ignore DDL statement",
          KR(ret), K(ddl_tenant_id), K(schema_version), K(ddl_stmt));
      // Set all binlog record invalid
      mark_stmt_binlog_record_invalid_(ddl_stmt);
      ret = OB_SUCCESS;
    } else if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("get_schemas_for_ddl_stmt_ fail", KR(ret), K(ddl_stmt), K(schema_version),
          K(ddl_tenant_id));
    }
  }
  // set db name for binlog record
  else if (OB_FAIL(set_binlog_record_db_name_(*br_data, op_type, tenant_name, db_name))) {
    LOG_ERROR("set_binlog_record_db_name_ fail", KR(ret), K(op_type), K(tenant_name), K(db_name));
  } else {
    // handle done
    _ISTAT("[DDL] [HANDLE_DONE] TENANT_ID=%lu DB_NAME=%s OP_TYPE=%s(%d) SCHEMA_VERSION=%ld "
        "OP_TENANT_ID=%lu EXEC_TENANT_ID=%lu OP_DB_ID=%lu OP_TABLE_ID=%lu DDL_STMT=[%s] CHECKPOINT_SEQ=%ld",
        ddl_tenant_id, br_data->dbname(), op_type_str, op_type, ddl_stmt.get_op_schema_version(),
        ddl_stmt.get_op_tenant_id(), ddl_stmt.get_exec_tenant_id(), ddl_stmt.get_op_database_id(),
        ddl_stmt.get_op_table_id(),
        to_cstring(ddl_stmt.get_ddl_stmt_str()),
        checkpoint_seq);
  }

  return ret;
}

// @retval OB_SUCCESS                   success
// @retval OB_TENANT_HAS_BEEN_DROPPED   tenant has been dropped
// @retval OB_IN_STOP_STATE             exit
// @retval other error code             fail
int ObLogDDLHandler::get_lazy_schema_guard_(const uint64_t tenant_id,
    const int64_t version,
    ObLogSchemaGuard &schema_guard)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(schema_getter_)) {
    LOG_ERROR("schema getter is invalid", K(schema_getter_));
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(version < 0)) {
    LOG_ERROR("invalid version", K(version));
    ret = OB_INVALID_ARGUMENT;
  } else {
    RETRY_FUNC(stop_flag_, (*schema_getter_), get_lazy_schema_guard, tenant_id, version,
        DATA_OP_TIMEOUT, schema_guard);
  }

  return ret;
}

// The database_id determines the DDL BinlogRcord output database_name, database_id determines the policy:
// 1. when using new schema, directly use the database id that comes with DDL stmt
// 2. When using old schema, if the table id is invalid, use the database_id in DDL directly; otherwise,
// refresh the table_schema based on the table_id, and then determine the databse id according to the table schema.
// In some cases, the database ids in the old and new schema are not the same, for example,
// if you drop a table to the recycle bin, the DDL database id is "__recyclebin" database id, not the original database id
//
//
// @retval OB_SUCCESS                   success
// @retval OB_TENANT_HAS_BEEN_DROPPED   tenant has been dropped
// @retval OB_IN_STOP_STATE             exit
// @retval other error code             fail
int ObLogDDLHandler::decide_ddl_stmt_database_id_(DdlStmtTask &ddl_stmt,
    const int64_t schema_version,
    ObLogSchemaGuard &schema_guard,
    uint64_t &db_id)
{
  int ret = OB_SUCCESS;
  const bool is_use_new_schema_version_mode = is_use_new_schema_version(ddl_stmt, schema_version);

  if (is_use_new_schema_version_mode) {
    db_id = ddl_stmt.get_op_database_id();
  } else {
    uint64_t table_id = ddl_stmt.get_op_table_id();

    // If the table id is invalid, the db_id is used in the DDL.
    if (OB_INVALID_ID == table_id || 0 == table_id) {
      db_id = ddl_stmt.get_op_database_id();
    } else {
      const ObSimpleTableSchemaV2 *tb_schema = NULL;

      // Retry to get the table schema until it succeeds or exit
      RETRY_FUNC(stop_flag_, schema_guard, get_table_schema, table_id, tb_schema, DATA_OP_TIMEOUT);

      if (OB_FAIL(ret)) {
        if (OB_IN_STOP_STATE != ret) {
          // OB_TENANT_HAS_BEEN_DROPPED means tenant has been droped, dealed by caller
          LOG_ERROR("get_table_schema fail", KR(ret), K(table_id), K(schema_version), K(ddl_stmt));
        }
      }
      // If the schema of the table is empty, the database id is invalid
      else if (NULL == tb_schema) {
        LOG_WARN("table schema is NULL. set database name NULL",
            K(table_id), K(schema_version), "ddl_stmt", ddl_stmt.get_ddl_stmt_str());

        db_id = OB_INVALID_ID;
      } else {
        db_id = tb_schema->get_database_id();
      }
    }
  }

  return ret;
}

// @retval OB_SUCCESS                   success
// @retval OB_TENANT_HAS_BEEN_DROPPED   tenant has been dropped
// @retval OB_IN_STOP_STATE             exit
// @retval other error code             fail
int ObLogDDLHandler::get_schemas_for_ddl_stmt_(const uint64_t ddl_tenant_id,
    DdlStmtTask &ddl_stmt,
    const int64_t schema_version,
    ObLogSchemaGuard &schema_guard,
    const char *&tenant_name,
    const char *&db_name)
{
  int ret = OB_SUCCESS;
  uint64_t db_id = OB_INVALID_ID;
  TenantSchemaInfo tenant_schema_info;
  DBSchemaInfo db_schema_info;

  // Get schema guard based on tenant_id and version number
  if (OB_FAIL(get_lazy_schema_guard_(ddl_tenant_id, schema_version, schema_guard))) {
    if (OB_IN_STOP_STATE != ret) {
      // OB_TENANT_HAS_BEEN_DROPPED indicates that the tenant may have been deleted
      LOG_WARN("get_lazy_schema_guard_ fail", KR(ret), K(ddl_tenant_id), K(schema_version));
    }
  }
  // decide database id
  else if (OB_FAIL(decide_ddl_stmt_database_id_(ddl_stmt, schema_version, schema_guard, db_id))) {
    // OB_TENANT_HAS_BEEN_DROPPED indicates that the tenant may have been deleted
    if (OB_IN_STOP_STATE != ret) {
      LOG_WARN("decide_ddl_stmt_database_id_ fail", KR(ret), K(ddl_stmt), K(schema_version));
    }
  } else {
    // Require ddl_tenant_id to match the tenant to which db_id belongs
    // If it does not match, print ERROR log
    if (OB_INVALID_ID != db_id  && 0 != db_id && ddl_tenant_id != extract_tenant_id(db_id)) {
      LOG_ERROR("DDL database id does not match ddl_tenant_id", K(db_id), K(ddl_tenant_id),
          K(extract_tenant_id(db_id)), K(ddl_stmt));
    }

    // Retry to get tenant schema until success or exit
    RETRY_FUNC(stop_flag_, schema_guard, get_tenant_schema_info, ddl_tenant_id, tenant_schema_info,
        DATA_OP_TIMEOUT);

    if (OB_FAIL(ret)) {
      if (OB_IN_STOP_STATE != ret) {
        // OB_TENANT_HAS_BEEN_DROPPED indicates that the tenant may have been deleted
        LOG_WARN("get_tenant_schema_info fail", KR(ret), K(ddl_tenant_id), K(tenant_schema_info),
            K(schema_version), K(ddl_stmt));
      }
    } else {
      // set tenant name
      tenant_name = tenant_schema_info.name_;

      // FIXME: Currently there are two invalid values: 0 and OB_INVALID_ID, it is recommended that the observer is unified
      if (OB_INVALID_ID == db_id || 0 == db_id) {
        // If db_id is invalid, the corresponding database name is not retrieved
        db_name = NULL;
      } else {
        // Retry to get the database schema until it succeeds or exit
        RETRY_FUNC(stop_flag_, schema_guard, get_database_schema_info, db_id, db_schema_info,
            DATA_OP_TIMEOUT);

        if (OB_FAIL(ret)) {
          if (OB_TENANT_HAS_BEEN_DROPPED == ret) {
            // OB_TENANT_HAS_BEEN_DROPPED indicates that the tenant may have been deleted
            // DB does not exist and is considered normal
            LOG_WARN("get database schema fail, set database name NULL", KR(ret),
                K(tenant_schema_info), K(db_id), K(schema_version),
                "ddl_stmt", ddl_stmt.get_ddl_stmt_str());
            db_name = NULL;
            ret = OB_SUCCESS;
          } else if (OB_IN_STOP_STATE != ret) {
            LOG_WARN("get_database_schema_info fail", KR(ret), K(db_id), K(schema_version));
          }
        } else {
          db_name = db_schema_info.name_;
        }
      }
    }
  }

  if (OB_SUCCESS == ret) {
    if (ddl_tenant_id != ddl_stmt.get_op_tenant_id()) {
      LOG_INFO("[DDL] [NOTICE] DDL stmt belong to different tenant with operated tenant",
          K(ddl_tenant_id), K(ddl_stmt));
    }
  }

  return ret;
}

bool ObLogDDLHandler::is_use_new_schema_version(DdlStmtTask &ddl_stmt,
    const int64_t schema_version)
{
  const int64_t part_local_ddl_schema_version = ddl_stmt.get_host().get_local_schema_version();

  return schema_version == part_local_ddl_schema_version;
}

int ObLogDDLHandler::set_binlog_record_db_name_(ILogRecord &br_data,
    const int64_t ddl_operation_type,
    const char * const tenant_name,
    const char * const db_name)
{
  int ret = OB_SUCCESS;

  // allow db_name empty
  if (OB_ISNULL(tenant_name)) {
    LOG_ERROR("invalid argument", K(tenant_name));
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObSchemaOperationType op_type =
        static_cast<ObSchemaOperationType>(ddl_operation_type);

    // If a DDL only operates on a tenant, the database is invalid
    std::string db_name_str = tenant_name;
    // For create database DDL statement, ILogRecord db information only records tenant information, no database information is recorded.
    if (NULL != db_name && OB_DDL_ADD_DATABASE != op_type && OB_DDL_FLASHBACK_DATABASE != op_type) {
      db_name_str.append(".");
      db_name_str.append(db_name);
    }

    br_data.setDbname(db_name_str.c_str());
  }

  return ret;
}

int ObLogDDLHandler::handle_ddl_stmt_drop_table_(ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t old_schema_version,
    const int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  ObLogSchemaGuard old_schema_guard;
  const char *tenant_name = NULL;
  const char *db_name = NULL;
  bool is_table_should_ignore_in_committer = false;

  RETRY_FUNC(stop_flag_, tenant.get_part_mgr(), drop_table,
      ddl_stmt.get_op_table_id(),
      old_schema_version,
      new_schema_version,
      is_table_should_ignore_in_committer,
      old_schema_guard,
      tenant_name,
      db_name,
      DATA_OP_TIMEOUT);

  // If the schema error is encountered, it means that the tenant may be deleted in the future, so the schema of table,
  // database, tenant or table group cannot be obtained, in this case, the DDL will be ignored.
  IGNORE_SCHEMA_ERROR(ret, "schema_version", old_schema_version, K(ddl_stmt), K(is_table_should_ignore_in_committer));

  if (OB_SUCC(ret)) {
    // Delete table using old_schema_version parsing
    if (OB_FAIL(commit_ddl_stmt_(tenant, ddl_stmt, old_schema_version, tenant_name, db_name,
        is_table_should_ignore_in_committer))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("commit_ddl_stmt_ fail", KR(ret), K(ddl_stmt), K(tenant), K(old_schema_version),
            K(tenant_name), K(db_name), K(is_table_should_ignore_in_committer));
      }
    } else {
      // succ
    }
  }

  return ret;
}

int ObLogDDLHandler::handle_ddl_stmt_drop_table_to_recyclebin_(ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t old_schema_version,
    const int64_t new_schema_version)
{
  int ret = OB_SUCCESS;

  // TODO: After the Table is put into the Recycle Bin, modify the places related to the Table name in PartMgr; in addition, support displaying the table into different states
  UNUSED(new_schema_version);

  // Parsing with older schema versions
  ret = handle_ddl_stmt_direct_output_(tenant, ddl_stmt, old_schema_version);

  return ret;
}

int ObLogDDLHandler::handle_ddl_stmt_alter_table_(ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t old_schema_version,
    const int64_t new_schema_version,
    const char *event)
{
  int ret = OB_SUCCESS;
  ObLogSchemaGuard old_schema_guard;
  ObLogSchemaGuard new_schema_guard;
  const char *old_tenant_name = NULL;
  const char *old_db_name = NULL;

  // TODO：Support table renaming, refiltering  based on filtering rules
  int64_t prepare_log_timestamp = ddl_stmt.get_host().get_timestamp();
  int64_t start_serve_timestamp = get_start_serve_timestamp_(new_schema_version,
      prepare_log_timestamp);

  // Atopt new_schema_version
  RETRY_FUNC(stop_flag_, tenant.get_part_mgr(), alter_table,
      ddl_stmt.get_op_table_id(),
      old_schema_version,
      new_schema_version,
      start_serve_timestamp,
      old_schema_guard,
      new_schema_guard,
      old_tenant_name,
      old_db_name,
      event,
      DATA_OP_TIMEOUT);

  // If the schema error is encountered, it means that the tenant may be deleted in the future, so the schema of table,
  // database, tenant or table group cannot be obtained, in this case, the DDL will be ignored.
  IGNORE_SCHEMA_ERROR(ret, "schema_version", new_schema_version, K(ddl_stmt));

  if (OB_SUCC(ret)) {
    // Set tenant, database and table name with old schema
    if (OB_FAIL(commit_ddl_stmt_(tenant, ddl_stmt, old_schema_version, old_tenant_name, old_db_name))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("commit_ddl_stmt_ fail", KR(ret), K(tenant), K(ddl_stmt), K(old_schema_version),
            K(old_tenant_name), K(old_db_name));
      }
    }
  }

  return ret;
}

int ObLogDDLHandler::handle_ddl_stmt_create_table_(ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  bool is_create_table = true;
  bool is_table_should_ignore_in_committer = false;
  int64_t prepare_log_timestamp = ddl_stmt.get_host().get_timestamp();
  int64_t start_serve_tstamp = get_start_serve_timestamp_(new_schema_version,
      prepare_log_timestamp);
  ObLogSchemaGuard schema_guard;
  const char *tenant_name = NULL;
  const char *db_name = NULL;

  RETRY_FUNC(stop_flag_, tenant.get_part_mgr(), add_table,
      ddl_stmt.get_op_table_id(),
      new_schema_version,
      start_serve_tstamp,
      is_create_table,
      is_table_should_ignore_in_committer,
      schema_guard,
      tenant_name,
      db_name,
      DATA_OP_TIMEOUT);

  // If the schema error is encountered, it means that the tenant may be deleted in the future, so the schema of table,
  // database, tenant or table group cannot be obtained, in this case, the DDL will be ignored.
  IGNORE_SCHEMA_ERROR(ret, "schema_version", new_schema_version, K(ddl_stmt), K(is_table_should_ignore_in_committer));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(commit_ddl_stmt_(tenant, ddl_stmt, new_schema_version, tenant_name, db_name,
        is_table_should_ignore_in_committer))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("commit_ddl_stmt_ fail", KR(ret), K(tenant), K(ddl_stmt), K(tenant_name),
            K(db_name), K(is_table_should_ignore_in_committer));
      }
    } else {}
  }

  return ret;
}

int ObLogDDLHandler::handle_ddl_stmt_rename_table_(ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t old_schema_version,
    const int64_t new_schema_version)
{
  int ret = OB_SUCCESS;

  // TODO：support table rename

  UNUSED(new_schema_version);
  // Parsing with older schema versions
  ret = handle_ddl_stmt_direct_output_(tenant, ddl_stmt, old_schema_version);

  return ret;
}

int ObLogDDLHandler::handle_ddl_stmt_create_index_(ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  int64_t prepare_log_timestamp = ddl_stmt.get_host().get_timestamp();
  int64_t start_serve_tstamp = get_start_serve_timestamp_(new_schema_version,
      prepare_log_timestamp);
  ObLogSchemaGuard schema_guard;
  const char *tenant_name = NULL;
  const char *db_name = NULL;

  RETRY_FUNC(stop_flag_, tenant.get_part_mgr(), add_index_table,
      ddl_stmt.get_op_table_id(),
      new_schema_version,
      start_serve_tstamp,
      schema_guard,
      tenant_name,
      db_name,
      DATA_OP_TIMEOUT);

  // If the schema error is encountered, it means that the tenant may be deleted in the future, so the schema of table,
  // database, tenant or table group cannot be obtained, in this case, the DDL will be ignored.
  IGNORE_SCHEMA_ERROR(ret, "schema_version", new_schema_version, K(ddl_stmt));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(commit_ddl_stmt_(tenant, ddl_stmt, new_schema_version, tenant_name, db_name))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("commit_ddl_stmt_ fail", KR(ret), K(tenant), K(ddl_stmt), K(tenant_name),
            K(db_name));
      }
    } else {}
  }

  return ret;
}

int ObLogDDLHandler::handle_ddl_stmt_drop_index_(ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t old_schema_version,
    const int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  ObLogSchemaGuard old_schema_guard;
  const char *tenant_name = NULL;
  const char *db_name = NULL;

  RETRY_FUNC(stop_flag_, tenant.get_part_mgr(), drop_index_table,
      ddl_stmt.get_op_table_id(),
      old_schema_version,
      new_schema_version,
      old_schema_guard,
      tenant_name,
      db_name,
      DATA_OP_TIMEOUT);

  // If the schema error is encountered, it means that the tenant may be deleted in the future, so the schema of table,
  // database, tenant or table group cannot be obtained, in this case, the DDL will be ignored.
  IGNORE_SCHEMA_ERROR(ret, K(old_schema_version), K(new_schema_version), K(ddl_stmt));

  if (OB_SUCC(ret)) {
    // drop index uses old_schema_version parsing
    // drop index DDL, __all_ddl_operation table_id is the table_id of the index table, in order to ensure
    // that the table_schema is available, use the old_schema version to ensure that the database information is available in BinlogRecord
    if (OB_FAIL(commit_ddl_stmt_(tenant, ddl_stmt, old_schema_version, tenant_name, db_name))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("commit_ddl_stmt_ fail", KR(ret), K(tenant), K(ddl_stmt), K(tenant_name),
            K(db_name));
      }
    } else {
      // succ
    }
  }

  return OB_SUCCESS;
}

int ObLogDDLHandler::handle_ddl_stmt_add_tablegroup_partition_(ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  int64_t prepare_log_timestamp = ddl_stmt.get_host().get_timestamp();
  int64_t start_serve_timestamp = get_start_serve_timestamp_(new_schema_version,
      prepare_log_timestamp);
  ObLogSchemaGuard schema_guard;
  // TableGroup has no DB Name
  const char *tenant_name = NULL;

  // Adopt new version of schema version
  RETRY_FUNC(stop_flag_, tenant.get_part_mgr(), add_tablegroup_partition,
      ddl_stmt.get_op_tablegroup_id(),
      new_schema_version,
      start_serve_timestamp,
      schema_guard,
      tenant_name,
      DATA_OP_TIMEOUT);

  // If the schema error is encountered, it means that the tenant may be deleted in the future, so the schema of table,
  // database, tenant or table group cannot be obtained, in this case, the DDL will be ignored.
  IGNORE_SCHEMA_ERROR(ret, K(new_schema_version), K(ddl_stmt));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(commit_ddl_stmt_(tenant, ddl_stmt, new_schema_version, tenant_name))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("commit_ddl_stmt_ fail", KR(ret), K(tenant), K(ddl_stmt), K(tenant_name));
      }
    } else {}
  }

  return ret;
}

int ObLogDDLHandler::handle_ddl_stmt_drop_tablegroup_partition_(ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t old_schema_version,
    const int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  ObLogSchemaGuard schema_guard;
  const char *tenant_name = NULL;

  // Adopt new version of schema version
  RETRY_FUNC(stop_flag_, tenant.get_part_mgr(), drop_tablegroup_partition,
      ddl_stmt.get_op_tablegroup_id(),
      old_schema_version,
      new_schema_version,
      schema_guard,
      tenant_name,
      DATA_OP_TIMEOUT);

  // If the schema error is encountered, it means that the tenant may be deleted in the future, so the schema of table,
  // database, tenant or table group cannot be obtained, in this case, the DDL will be ignored.
  IGNORE_SCHEMA_ERROR(ret, K(new_schema_version), K(ddl_stmt));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(commit_ddl_stmt_(tenant, ddl_stmt, new_schema_version, tenant_name))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("commit_ddl_stmt_ fail", KR(ret), K(tenant), K(ddl_stmt), K(tenant_name));
      }
    } else {}
  }

  return ret;
}

int ObLogDDLHandler::handle_ddl_stmt_split_tablegroup_partition_(ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  ObLogSchemaGuard schema_guard;
  int64_t prepare_log_timestamp = ddl_stmt.get_host().get_timestamp();
  int64_t start_serve_timestamp = get_start_serve_timestamp_(new_schema_version,
      prepare_log_timestamp);
  const char *tenant_name = NULL;

  // 采用新版本schema version
  RETRY_FUNC(stop_flag_, tenant.get_part_mgr(), split_tablegroup_partition,
      ddl_stmt.get_op_tablegroup_id(),
      new_schema_version,
      start_serve_timestamp,
      schema_guard,
      tenant_name,
      DATA_OP_TIMEOUT);

  // If the schema error is encountered, it means that the tenant may be deleted in the future, so the schema of table,
  // database, tenant or table group cannot be obtained, in this case, the DDL will be ignored.
  IGNORE_SCHEMA_ERROR(ret, K(new_schema_version), K(ddl_stmt));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(commit_ddl_stmt_(tenant, ddl_stmt, new_schema_version, tenant_name))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("commit_ddl_stmt_ fail", KR(ret), K(tenant), K(ddl_stmt), K(tenant_name));
      }
    } else {}
  }

  return ret;
}

int ObLogDDLHandler::handle_ddl_stmt_change_tablegroup_(ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t old_schema_version,
    const int64_t new_schema_version)
{
  int ret = OB_SUCCESS;

  // TODO：support change TableGroup
  UNUSED(old_schema_version);

  // 采用新/老版本schema都可以解析
  // can be resloved by new/old schema version
  ret = handle_ddl_stmt_direct_output_(tenant, ddl_stmt, new_schema_version);

  return ret;
}

int ObLogDDLHandler::handle_ddl_stmt_alter_tablegroup_partition_(ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t old_schema_version,
    const int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  int64_t prepare_log_timestamp = ddl_stmt.get_host().get_timestamp();
  int64_t start_serve_timestamp = get_start_serve_timestamp_(new_schema_version,
      prepare_log_timestamp);
  ObLogSchemaGuard old_schema_guard;
  ObLogSchemaGuard new_schema_guard;
  const char *tenant_name = NULL;

  // 采用新版本schema version
  RETRY_FUNC(stop_flag_, tenant.get_part_mgr(), alter_tablegroup_partition,
      ddl_stmt.get_op_tablegroup_id(),
      old_schema_version,
      new_schema_version,
      start_serve_timestamp,
      old_schema_guard,
      new_schema_guard,
      tenant_name,
      DATA_OP_TIMEOUT);

  // If the schema error is encountered, it means that the tenant may be deleted in the future, so the schema of table,
  // database, tenant or table group cannot be obtained, in this case, the DDL will be ignored.
  IGNORE_SCHEMA_ERROR(ret, K(old_schema_version), K(new_schema_version), K(ddl_stmt));

  if (OB_SUCC(ret)) {
    // Set tenant, database and table name with old schema
    if (OB_FAIL(commit_ddl_stmt_(tenant, ddl_stmt, old_schema_version, tenant_name))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("commit_ddl_stmt_ fail", KR(ret), K(ddl_stmt),
            "schema_version", old_schema_version);
      }
    }
  }

  return ret;
}

int ObLogDDLHandler::handle_ddl_stmt_truncate_table_drop_(ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t old_schema_version,
    const int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  ObLogSchemaGuard old_schema_guard;
  const char *tenant_name = NULL;
  const char *db_name = NULL;
  bool is_table_should_ignore_in_committer = false;

  _ISTAT("[DDL] [TRUNCATE_DROP] TENANT_ID=%lu TABLE_ID=%ld SCHEMA_VERSION=(OLD=%ld,NEW=%ld) DDL_STMT=[%s]",
      tenant.get_tenant_id(),
      ddl_stmt.get_op_table_id(),
      old_schema_version,
      new_schema_version,
      to_cstring(ddl_stmt.get_ddl_stmt_str()));

  RETRY_FUNC(stop_flag_, tenant.get_part_mgr(), drop_table,
      ddl_stmt.get_op_table_id(),
      old_schema_version,
      new_schema_version,
      is_table_should_ignore_in_committer,
      old_schema_guard,
      tenant_name,
      db_name,
      DATA_OP_TIMEOUT);

  // If the schema error is encountered, it means that the tenant may be deleted in the future, so the schema of table,
  // database, tenant or table group cannot be obtained, in this case, the DDL will be ignored.
  IGNORE_SCHEMA_ERROR(ret, K(old_schema_version), K(new_schema_version), K(ddl_stmt), K(is_table_should_ignore_in_committer));

  if (OB_SUCC(ret)) {
    // TRUNCATE DROP operation don't need output
    mark_stmt_binlog_record_invalid_(ddl_stmt);
  }

  return ret;
}

int ObLogDDLHandler::handle_ddl_stmt_truncate_drop_table_to_recyclebin_(ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t old_schema_version,
    const int64_t new_schema_version)
{
  int ret = OB_SUCCESS;

  _ISTAT("[DDL] [TRUNCATE_DROP_TABLE_TO_RECYCLEBIN] TENANT_ID=%lu TABLE_ID=%ld "
      "SCHEMA_VERSION=(OLD=%ld,NEW=%ld) DDL_STMT=[%s]",
      tenant.get_tenant_id(),
      ddl_stmt.get_op_table_id(),
      old_schema_version,
      new_schema_version,
      to_cstring(ddl_stmt.get_ddl_stmt_str()));

  if (OB_SUCC(ret)) {
    // OB_DDL_TRUNCATE_DROP_TABLE_TO_RECYCLEBIN operation does not need to output DDL
    // Set binlog record to be invalid
    mark_stmt_binlog_record_invalid_(ddl_stmt);
  }

  return ret;
}

int ObLogDDLHandler::handle_ddl_stmt_truncate_table_create_(ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  bool is_create_table = true;
  bool is_table_should_ignore_in_committer = false;
  int64_t prepare_log_timestamp = ddl_stmt.get_host().get_timestamp();
  int64_t start_serve_tstamp = get_start_serve_timestamp_(new_schema_version,
      prepare_log_timestamp);

  ObLogSchemaGuard schema_guard;
  const char *tenant_name = NULL;
  const char *db_name = NULL;

  _ISTAT("[DDL] [TRUNCATE_CREATE] TENANT_ID=%lu TABLE_ID=%ld SCHEMA_VERSION=%ld START_TSTAMP=%ld DDL_STMT=[%s]",
      tenant.get_tenant_id(),
      ddl_stmt.get_op_table_id(),
      new_schema_version,
      start_serve_tstamp,
      to_cstring(ddl_stmt.get_ddl_stmt_str()));

  RETRY_FUNC(stop_flag_, tenant.get_part_mgr(), add_table,
      ddl_stmt.get_op_table_id(),
      new_schema_version,
      start_serve_tstamp,
      is_create_table,
      is_table_should_ignore_in_committer,
      schema_guard,
      tenant_name,
      db_name,
      DATA_OP_TIMEOUT);

  // If the schema error is encountered, it means that the tenant may be deleted in the future, so the schema of table,
  // database, tenant or table group cannot be obtained, in this case, the DDL will be ignored.
  IGNORE_SCHEMA_ERROR(ret, K(new_schema_version), K(start_serve_tstamp), K(is_create_table), K(ddl_stmt), K(is_table_should_ignore_in_committer));

  if (OB_SUCC(ret)) {
    // Adopt new version of schema parsing
    if (OB_FAIL(commit_ddl_stmt_(tenant, ddl_stmt, new_schema_version, tenant_name, db_name,
        is_table_should_ignore_in_committer))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("commit_ddl_stmt_ fail", KR(ret), K(tenant), K(ddl_stmt), K(tenant_name),
            K(db_name), K(is_table_should_ignore_in_committer));
      }
    } else {}
  }

  return ret;
}

int ObLogDDLHandler::handle_ddl_stmt_drop_index_to_recyclebin_(ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t old_schema_version,
    const int64_t new_schema_version)
{
  int ret = OB_SUCCESS;

  UNUSED(new_schema_version);

  // Use old version schema parsing
  // Ensure that the binlog record DB information is correct
  ret = handle_ddl_stmt_direct_output_(tenant, ddl_stmt, old_schema_version);

  return ret;
}

int ObLogDDLHandler::handle_ddl_stmt_add_tenant_(ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  bool tenant_is_chosen = false;
  bool is_new_created_tenant = true;
  bool is_new_tenant_by_restore = false;
  uint64_t target_tenant_id = ddl_stmt.get_op_tenant_id();
  int64_t prepare_log_timestamp = ddl_stmt.get_host().get_timestamp();
  int64_t start_serve_tstamp = get_start_serve_timestamp_(new_schema_version,
      prepare_log_timestamp);
  IObLogTenantMgr *tenant_mgr = TCTX.tenant_mgr_;
  ObLogSchemaGuard schema_guard;
  const char *tenant_name = NULL;
  int64_t valid_schema_version = new_schema_version;
  if (OB_ISNULL(tenant_mgr)) {
    LOG_ERROR("invalid tenant mgr", K(tenant_mgr));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(parse_tenant_ddl_stmt_for_restore_(ddl_stmt, valid_schema_version, start_serve_tstamp, is_new_tenant_by_restore))) {
    LOG_ERROR("parse_tenant_ddl_stmt_for_restore_ failed", KR(ret), K(ddl_stmt), K(valid_schema_version), K(start_serve_tstamp), K(is_new_tenant_by_restore));
  } else {
    RETRY_FUNC(stop_flag_, (*tenant_mgr), add_tenant,
        target_tenant_id,
        is_new_created_tenant,
        is_new_tenant_by_restore,
        start_serve_tstamp,
        valid_schema_version,
        schema_guard,
        tenant_name,
        DATA_OP_TIMEOUT,
        tenant_is_chosen);

  // If the schema error is encountered, it means that the tenant may be deleted in the future, so the schema of table,
  // database, tenant or table group cannot be obtained, in this case, the DDL will be ignored.
    IGNORE_SCHEMA_ERROR(ret, K(valid_schema_version), K(start_serve_tstamp),
        K(target_tenant_id), K(ddl_stmt));

    if (OB_SUCC(ret)) {
      bool filter_ddl_stmt = false;
      // Filter tenants that are not on the whitelist
      if (! tenant_is_chosen) {
        filter_ddl_stmt = true;
      }

      // DB name is empty
      if (OB_FAIL(commit_ddl_stmt_(tenant, ddl_stmt, new_schema_version, tenant_name, NULL, filter_ddl_stmt))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("commit_ddl_stmt_ fail", KR(ret), K(tenant), K(ddl_stmt), K(filter_ddl_stmt),
              K(tenant_name));
        }
      } else {}
    }
  }

  return ret;
}

int ObLogDDLHandler::handle_ddl_stmt_drop_tenant_(ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t old_schema_version,
    const int64_t new_schema_version,
    const bool is_schema_split_mode /* = false */,
    const bool is_del_tenant_start_op /* = false */)
{
  int ret = OB_SUCCESS;
  uint64_t target_tenant_id = ddl_stmt.get_op_tenant_id();
  IObLogTenantMgr *tenant_mgr = TCTX.tenant_mgr_;
  const int64_t prepare_log_timestamp = ddl_stmt.get_host().get_timestamp();

  if (OB_ISNULL(tenant_mgr)) {
    LOG_ERROR("invalid tenant mgr", K(tenant_mgr));
    ret = OB_ERR_UNEXPECTED;
  } else {
    ISTAT("[DDL] begin to handle drop tenant DDL stmt", K(is_schema_split_mode),
        K(is_del_tenant_start_op), K(ddl_stmt), K(old_schema_version), K(new_schema_version));

    if (! is_schema_split_mode) {
      // For non-split mode, drop tenant DDL marks the end of this tenant's DDL flow, so the drop_tenant() interface is called directly to delete the tenant
      // In split mode, it is triggered by DDL OFFLINE Task, see handle_ddl_offline_task_() for details
      ret = tenant_mgr->drop_tenant(target_tenant_id, "DROP_TENANT_DDL");
    } else if (is_del_tenant_start_op) {
      // DROP TENANT START for split mode, marking the start of tenant deletion
      ret = tenant_mgr->drop_tenant_start(target_tenant_id, prepare_log_timestamp);
    } else {
      // DROP TENANT END for schema split mode, marking the end of tenant deletion
      ret = tenant_mgr->drop_tenant_end(target_tenant_id, prepare_log_timestamp);
    }

  // If the schema error is encountered, it means that the tenant may be deleted in the future, so the schema of table,
  // database, tenant or table group cannot be obtained, in this case, the DDL will be ignored.
    IGNORE_SCHEMA_ERROR(ret, K(target_tenant_id), K(old_schema_version), K(new_schema_version),
        K(is_schema_split_mode), K(is_del_tenant_start_op), K(ddl_stmt));

    if (OB_SUCC(ret)) {
      if (OB_FAIL(commit_ddl_stmt_(tenant, ddl_stmt, old_schema_version))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("commit_ddl_stmt_ fail", KR(ret), K(tenant), K(ddl_stmt));
        }
      } else {}
    }
  }

  return ret;
}

int ObLogDDLHandler::handle_ddl_stmt_alter_tenant_(ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t old_schema_version,
    const int64_t new_schema_version)
{
  int ret = OB_SUCCESS;

  UNUSED(new_schema_version);

  // TODO: support for changing tenant names
  // Adopt new version of schema parsing
  // Currently OB does not support changing the tenant name, the code needs to be tested later

  ret = handle_ddl_stmt_direct_output_(tenant, ddl_stmt, old_schema_version);

  return ret;
}

// Background: For data consumption chain, a large number of tenant split deployment method is used, i.e. for tenant tt1,
// tenant whitelist is used to start single or multiple liboblog for synchronization
// When the tenant name changes, it will cause the tenant whitelist  expire
// Support: liboblog processes a DDL to rename tenant, with the error OB_NOT_SUPPORTED; liboblog consumers need to start a new instance of the new tenant
int ObLogDDLHandler::handle_ddl_stmt_rename_tenant_(ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t old_schema_version,
    const int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  const char *tenant_name = NULL;
  bool tenant_is_chosen = false;
  const uint64_t target_tenant_id = ddl_stmt.get_op_tenant_id();
  IObLogTenantMgr *tenant_mgr = TCTX.tenant_mgr_;

  if (OB_ISNULL(tenant_mgr)) {
    LOG_ERROR("invalid tenant mgr", K(tenant_mgr));
    ret = OB_ERR_UNEXPECTED;
  } else {
    RETRY_FUNC(stop_flag_, (*tenant_mgr), alter_tenant_name,
        target_tenant_id,
        old_schema_version,
        new_schema_version,
        DATA_OP_TIMEOUT,
        tenant_name,
        tenant_is_chosen);

  // If the schema error is encountered, it means that the tenant may be deleted in the future, so the schema of table,
  // database, tenant or table group cannot be obtained, in this case, the DDL will be ignored.
    IGNORE_SCHEMA_ERROR(ret, K(new_schema_version), K(target_tenant_id), K(ddl_stmt));

    if (OB_SUCC(ret)) {
      bool filter_ddl_stmt = false;
      // Filter tenants that are not on the whitelist
      if (! tenant_is_chosen) {
        filter_ddl_stmt = true;
      }

      // DB name is empty
      if (OB_FAIL(commit_ddl_stmt_(tenant, ddl_stmt, new_schema_version, tenant_name, NULL, filter_ddl_stmt))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("commit_ddl_stmt_ fail", KR(ret), K(tenant), K(ddl_stmt), K(filter_ddl_stmt),
              K(tenant_name));
        }
      } else {}
    }
  }

  return ret;
}

int ObLogDDLHandler::handle_ddl_stmt_drop_tenant_to_recyclebin_(
    ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t old_schema_version,
    const int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  UNUSED(new_schema_version);
  ret = handle_ddl_stmt_direct_output_(tenant, ddl_stmt, old_schema_version);

  return ret;
}

int ObLogDDLHandler::handle_ddl_stmt_alter_database_(ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t old_schema_version,
    const int64_t new_schema_version)
{
  int ret = OB_SUCCESS;

  UNUSED(new_schema_version);

  // TODO: support for changing database names
  // Use old version schema
  ret = handle_ddl_stmt_direct_output_(tenant, ddl_stmt, old_schema_version);

  return ret;
}

int ObLogDDLHandler::handle_ddl_stmt_drop_database_(ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t old_schema_version,
    const int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  // don't need to handle drop database
  UNUSED(new_schema_version);
  ret = handle_ddl_stmt_direct_output_(tenant, ddl_stmt, old_schema_version);
  return ret;
}

int ObLogDDLHandler::handle_ddl_stmt_drop_database_to_recyclebin_(ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t old_schema_version,
    const int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  UNUSED(new_schema_version);
  ret = handle_ddl_stmt_direct_output_(tenant, ddl_stmt, old_schema_version);

  return ret;
}

int ObLogDDLHandler::handle_ddl_stmt_rename_database_(ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t old_schema_version,
    const int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  UNUSED(new_schema_version);
  ret = handle_ddl_stmt_direct_output_(tenant, ddl_stmt, old_schema_version);

  return ret;
}

int ObLogDDLHandler::handle_ddl_stmt_split_begin_(ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  int64_t prepare_log_timestamp = ddl_stmt.get_host().get_timestamp();
  int64_t start_serve_timestamp = get_start_serve_timestamp_(new_schema_version,
      prepare_log_timestamp);
  ObLogSchemaGuard new_schema_guard;
  const char *tenant_name = NULL;
  const char *db_name = NULL;

  RETRY_FUNC(stop_flag_, tenant.get_part_mgr(), split_table,
      ddl_stmt.get_op_table_id(),
      new_schema_version,
      start_serve_timestamp,
      new_schema_guard,
      tenant_name,
      db_name,
      DATA_OP_TIMEOUT);

  // If the schema error is encountered, it means that the tenant may be deleted in the future, so the schema of table,
  // database, tenant or table group cannot be obtained, in this case, the DDL will be ignored.
  IGNORE_SCHEMA_ERROR(ret, K(new_schema_version), K(start_serve_timestamp), K(ddl_stmt));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(commit_ddl_stmt_(tenant, ddl_stmt, new_schema_version, tenant_name, db_name))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("commit_ddl_stmt_ fail", KR(ret), K(tenant),K(ddl_stmt), K(tenant_name), K(db_name));
      }
    } else {}
  }

  return ret;
}

int ObLogDDLHandler::handle_ddl_stmt_finish_schema_split_(ObLogTenant &tenant,
    DdlStmtTask &ddl_stmt,
    const int64_t new_schema_version,
    bool &is_schema_split_mode)
{
  int ret = OB_SUCCESS;
  _ISTAT("[CHANGE_SCHEMA_SPLIT_MODE] [HANDLE_DDL_STMT_FINISH_SCHEMA_SPLIT] SCHEMA_VERSION=%ld IS_SCHEMA_SPLIT_MODE=%d",
      new_schema_version, TCTX.is_schema_split_mode_);

  // enable schema split mode
  TCTX.enable_schema_split_mode();

  // enable schema split mode
  is_schema_split_mode = true;

  int64_t prepare_log_timestamp = ddl_stmt.get_host().get_timestamp();
  int64_t start_serve_timestamp = get_start_serve_timestamp_(new_schema_version,
      prepare_log_timestamp);
  IObLogTenantMgr *tenant_mgr = TCTX.tenant_mgr_;
  const int64_t split_schema_version = ddl_stmt.get_op_schema_version();

  if (OB_ISNULL(tenant_mgr)) {
    LOG_ERROR("invalid tenant mgr", K(tenant_mgr));
    ret = OB_ERR_UNEXPECTED;
  } else {
    RETRY_FUNC(stop_flag_, (*tenant_mgr), handle_schema_split_finish,
        tenant.get_tenant_id(),
        split_schema_version,
        start_serve_timestamp,
        DATA_OP_TIMEOUT);

  // If the schema error is encountered, it means that the tenant may be deleted in the future, so the schema of table,
  // database, tenant or table group cannot be obtained, in this case, the DDL will be ignored.
    IGNORE_SCHEMA_ERROR(ret, K(split_schema_version), K(start_serve_timestamp), K(ddl_stmt));

    if (OB_SUCC(ret)) {
      if (OB_FAIL(commit_ddl_stmt_(tenant, ddl_stmt, new_schema_version))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("commit_ddl_stmt_ fail", KR(ret), K(tenant), K(ddl_stmt));
        }
      } else {}
    }
  }

  return ret;
}

int ObLogDDLHandler::parse_tenant_ddl_stmt_for_restore_(DdlStmtTask &ddl_stmt, int64_t &schema_version,
                                                      int64_t &tenant_gts_value, bool &is_create_tenant_by_restore_ddl)
{
  int ret = OB_SUCCESS;
  is_create_tenant_by_restore_ddl = false;
  ObString ddl_stmt_str = ddl_stmt.get_ddl_stmt_str();
  int64_t len = ddl_stmt_str.length();
  char ddl_stmt_buf[len + 1];
  MEMSET(ddl_stmt_buf, '\0', len + 1);
  MEMCPY(ddl_stmt_buf, ddl_stmt_str.ptr(), len);
  const char *pair_delimiter = "; ";
  const char *kv_delimiter = "=";
  const char *key_gts = "tenant_gts";
  const char *key_version = "schema_version";
  const char *value_gts = NULL;
  const char *value_version = NULL;

//schema_version=1617073037190088; tenant_gts=1617073037222488
  const bool is_restore_tenant_ddl = !ddl_stmt_str.empty()
      && (NULL != strstr(ddl_stmt_buf, pair_delimiter)) && (NULL != strstr(ddl_stmt_buf, kv_delimiter))
      && (NULL != strstr(ddl_stmt_buf, key_gts)) && (NULL != strstr(ddl_stmt_buf, key_version));

  if (is_restore_tenant_ddl) {
    const bool skip_dirty_data = (TCONF.skip_dirty_data != 0);
    int64_t tenant_schema_version_from_ddl = 0;
    int64_t tenant_gts_from_ddl = 0;
    ObLogKVCollection kv_c;

    //ddl for physical backup restore contains str like: schema_version=%ld; tenant_gts=%ld
    if (OB_FAIL(ret) || OB_FAIL(kv_c.init(kv_delimiter, pair_delimiter))) {
      LOG_ERROR("init key-value str fail", KR(ret), K(ddl_stmt_str));
    } else if (OB_FAIL(kv_c.deserialize(ddl_stmt_buf))) {
      LOG_ERROR("deserialize kv string fail", KR(ret), K(ddl_stmt_str));
    } else if (OB_UNLIKELY(!kv_c.is_valid())) {
      LOG_ERROR("key-value collection built by ddl_stmt is not valid", K(ddl_stmt_str), K(kv_c));
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(kv_c.get_value_of_key(key_gts, value_gts))) {
      LOG_ERROR("failed to get tenant gts value", KR(ret), K(ddl_stmt_str), K(kv_c), K(key_gts));
    } else if (OB_FAIL(kv_c.get_value_of_key(key_version, value_version))) {
      LOG_ERROR("failed to get tenant gts value", KR(ret), K(ddl_stmt_str), K(kv_c), K(key_version));
    } else if (OB_FAIL(c_str_to_int(value_version, tenant_schema_version_from_ddl))) {
      LOG_ERROR("failed to get value of tenant schema version", KR(ret), K(value_version), K(tenant_schema_version_from_ddl), K(ddl_stmt));
    } else if (OB_FAIL(c_str_to_int(value_gts, tenant_gts_from_ddl))) {
      LOG_ERROR("failed to get value of tenant schema version", KR(ret), K(value_gts), K(tenant_gts_value), K(ddl_stmt));
    } else {
      is_create_tenant_by_restore_ddl = true;
      schema_version = tenant_schema_version_from_ddl > schema_version ? tenant_schema_version_from_ddl : schema_version;
      tenant_gts_value = tenant_gts_from_ddl;
    }
    if (OB_SUCC(ret)) {
      mark_stmt_binlog_record_invalid_(ddl_stmt);
      LOG_INFO("mark create_tenant_end_ddl invalid for restore tenant", KR(ret), K(is_create_tenant_by_restore_ddl),
        K(schema_version), K(tenant_gts_value), K(ddl_stmt));
    } else if (skip_dirty_data) {
      LOG_WARN("parse_tenant_ddl_stmt_for_restore_ fail!", KR(ret), K(is_create_tenant_by_restore_ddl), K(schema_version), K(tenant_gts_value),
          K(value_gts), K(value_version), K(ddl_stmt), K(kv_c));
      ret = OB_SUCCESS;
    } else {
      LOG_ERROR("parse_tenant_ddl_stmt_for_restore_ fail!", KR(ret), K(is_create_tenant_by_restore_ddl), K(schema_version), K(tenant_gts_value),
          K(value_gts), K(value_version), K(ddl_stmt), K(kv_c));
    }
  } else {
    LOG_INFO("parse_tenant_ddl_stmt_for_restore_ passby", K(is_create_tenant_by_restore_ddl), K(schema_version), K(tenant_gts_value), K(ddl_stmt));
  }
  return ret;
}

int64_t ObLogDDLHandler::get_start_serve_timestamp_(const int64_t new_schema_version,
    const int64_t prepare_log_timestamp)
{
  // The table start timestamp selects the maximum value of the Schema version and prepare log timestamp of table  __all_ddl_operation
  // The purpose is to avoid heartbeat timestamp fallback
  return std::max(new_schema_version, prepare_log_timestamp);
}

}
}
