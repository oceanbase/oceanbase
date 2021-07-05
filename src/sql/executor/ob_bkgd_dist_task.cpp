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

#include "ob_bkgd_dist_task.h"
#include "sql/executor/ob_executor_rpc_processor.h"
#include "lib/time/ob_time_utility.h"
#include "lib/stat/ob_session_stat.h"
#include "sql/executor/ob_determinate_task_transmit.h"
#include "sql/engine/dml/ob_table_append_local_sort_data.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "observer/ob_server_struct.h"
#include "lib/utility/ob_tracepoint.h"

namespace oceanbase {
namespace sql {

using namespace common;
using namespace share;

ObBKGDDistTaskDag::ObBKGDDistTaskDag()
    : ObIDag(DAG_TYPE_SQL_BUILD_INDEX, DAG_PRIO_CREATE_INDEX), tenant_id_(OB_INVALID_ID), scheduler_id_(0)

{}

ObBKGDDistTaskDag::~ObBKGDDistTaskDag()
{}

int ObBKGDDistTaskDag::init(const uint64_t tenant_id, const ObTaskID& task_id, const uint64_t scheduler_id)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ARGUMENT == tenant_id || !task_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(task_id));
  } else {
    tenant_id_ = tenant_id;
    task_id_ = task_id;
    scheduler_id_ = scheduler_id;
  }
  return ret;
}

bool ObBKGDDistTaskDag::operator==(const ObIDag& other) const
{
  bool equal = false;
  if (this == &other) {
    equal = true;
  } else {
    if (get_type() == other.get_type()) {
      const ObBKGDDistTaskDag& o = static_cast<const ObBKGDDistTaskDag&>(other);
      equal = (tenant_id_ == o.tenant_id_ && task_id_ == o.task_id_);
    }
  }
  return equal;
}

int64_t ObBKGDDistTaskDag::hash() const
{
  // task_id_ is unique, %tenant_id_ is not used
  return task_id_.hash();
}

int ObBKGDDistTaskDag::fill_comment(char* buf, const int64_t len) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(
          databuff_printf(buf, len, "build index task, tenant_id=%lu, task_id=%s", tenant_id_, to_cstring(task_id_)))) {
    LOG_WARN("data buffer print failed", K(ret));
  }
  return ret;
}

int64_t ObBKGDDistTaskDag::get_compat_mode() const
{
  int ret = OB_SUCCESS;
  ObWorker::CompatMode compat_mode = ObWorker::CompatMode::MYSQL;
  FETCH_ENTITY(TENANT_SPACE, tenant_id_)
  {
    compat_mode = THIS_WORKER.get_compatibility_mode();
  }
  return static_cast<int64_t>(compat_mode);
}

ObBKGDDistTask::ObBKGDDistTask()
    : ObITask(TASK_TYPE_SQL_BUILD_INDEX),
      abs_timeout_us_(0),
      create_time_us_(0),
      task_allocator_(ObModIds::OB_CS_BUILD_INDEX)
{}

ObBKGDDistTask::~ObBKGDDistTask()
{
  if (!serialized_task_.empty()) {
    task_allocator_.free(serialized_task_.ptr());
  }
}

class ObBKGDDistTask::ObDistTaskProcessor : public ObDistExecuteBaseP {
public:
  ObDistTaskProcessor(ObBKGDDistTask& task) : ObDistExecuteBaseP(GCTX, false /* do not send result */), task_(task)
  {}

  virtual void record_exec_timestamp(bool is_first, ObExecTimestamp& exec_timestamp)
  {
    UNUSED(is_first);
    exec_timestamp.rpc_send_ts_ = task_.create_time_us_;
    exec_timestamp.receive_ts_ = task_.create_time_us_;
    exec_timestamp.enter_queue_ts_ = task_.create_time_us_;
    exec_timestamp.run_ts_ = get_process_timestamp();
    exec_timestamp.before_process_ts_ = get_process_timestamp();
    exec_timestamp.single_process_ts_ = get_single_process_timestamp();
    exec_timestamp.process_executor_ts_ = get_exec_start_timestamp();
    exec_timestamp.executor_end_ts_ = get_exec_end_timestamp();
  }

  int process_task(ObTask& task, ObTaskCompleteEvent& event)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(param_preprocess(task))) {
      LOG_WARN("param pre-process failed", K(ret));
    } else if (OB_FAIL(execute_dist_plan(task, event))) {
      LOG_WARN("execute distribute plan failed", K(ret), K(task));
    }
    return ret;
  }

private:
  ObBKGDDistTask& task_;
};

int ObBKGDDistTask::init(const common::ObAddr& addr, const common::ObString& task, const int64_t abs_timeout_us)
{
  int ret = OB_SUCCESS;
  ObBKGDDistTaskDag* dag = static_cast<ObBKGDDistTaskDag*>(get_dag());
  if (!addr.is_valid() || task.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (abs_timeout_us < ObTimeUtility::current_time()) {
    ret = OB_TIMEOUT;
    LOG_WARN("task already timeout", K(ret));
  } else if (OB_ISNULL(dag)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag is NULL", K(ret));
  } else if (OB_FAIL(ob_write_string(task_allocator_, task, serialized_task_))) {
    LOG_WARN("copy string failed", K(ret));
  } else {
    return_addr_ = addr;
    abs_timeout_us_ = abs_timeout_us;
    create_time_us_ = ObTimeUtility::current_time();
    if (NULL != ObCurTraceId::get_trace_id()) {
      trace_id_ = *ObCurTraceId::get_trace_id();
    }
  }
  return ret;
}

int ObBKGDDistTask::get_index_tid(const ObTask& task, uint64_t& tid) const
{
  int ret = OB_SUCCESS;
  // Only too build task execute in background:
  //
  // 1. scan index data, get index table id from transmit
  //
  //   PHY_DETERMINATE_TASK_TRANSMIT
  //     PHY_UK_ROW_TRANSFORM (for uniq index)
  //       PHY_TABLE_SCAN_WITH_CHECKSUM
  //
  // 2. build index table macro, get index table id from PHY_APPEND_LOCAL_SORT_DATA
  //
  //   PHY_DETERMINATE_TASK_TRANSMIT
  //     PHY_APPEND_LOCAL_SORT_DATA
  //       PHY_SORT
  //         PHY_TASK_ORDER_RECEIVE
  //
  ObPhyOperator* root = task.get_des_phy_plan().get_main_query();
  ObPhyOperator* child = NULL;
  if (NULL == root || PHY_DETERMINATE_TASK_TRANSMIT != root->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("main query is NULL or unexpected type", K(ret), KP(root));
  } else {
    const ObDeterminateTaskTransmit* transmit = static_cast<ObDeterminateTaskTransmit*>(root);
    if (NULL == (child = transmit->get_child(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("transmit has no child", K(ret));
    } else if (PHY_APPEND_LOCAL_SORT_DATA == child->get_type() || PHY_UK_ROW_TRANSFORM == child->get_type() ||
               PHY_TABLE_SCAN_WITH_CHECKSUM == child->get_type()) {
      if (PHY_APPEND_LOCAL_SORT_DATA == child->get_type()) {
        tid = static_cast<const ObTableAppendLocalSortData*>(child)->get_table_id();
      } else {
        tid = transmit->get_repartition_table_id();
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected operator type", K(ret), K(child->get_type()));
    }
  }
  return ret;
}

class ObExtraIndexBuildCheck : public ObIExtraStatusCheck {
public:
  ObExtraIndexBuildCheck(const uint64_t index_tid) : index_tid_(index_tid), last_check_time_(0)
  {}

  const char* name() const override
  {
    return "index build check";
  }
  int check() const override
  {
    int ret = OB_SUCCESS;
    const int64_t CHECK_INTERVAL = 1000000;  // 1 second
    int64_t cur_time = ObTimeUtil::fast_current_time();
    if (cur_time - last_check_time_ > CHECK_INTERVAL) {
      ret = do_check();
      if (OB_SUCC(ret)) {
        last_check_time_ = cur_time;
      }
    }
    return ret;
  }

  int do_check() const
  {
    int ret = OB_SUCCESS;
    schema::ObSchemaGetterGuard schema_guard;
    const schema::ObTableSchema* table_schema = NULL;
    if (NULL == GCTX.schema_service_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL schema service", K(ret));
    } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(extract_tenant_id(index_tid_), schema_guard))) {
      if (OB_TENANT_NOT_EXIST == ret) {
        ret = OB_CANCELED;
        LOG_INFO("tenant not exist", K(ret), K(index_tid_));
      } else {
        LOG_WARN("get tenant schema guard failed", K(ret));
      }
    } else if (OB_FAIL(schema_guard.get_table_schema(index_tid_, table_schema))) {
      LOG_WARN("get table schema failed", K(ret));
    } else if (NULL == table_schema) {
      ret = OB_CANCELED;
      LOG_INFO("index table not exist", K(ret), K(index_tid_));
    } else {
      if (table_schema->is_final_invalid_index()) {
        ret = OB_CANCELED;
        LOG_INFO("index table is in final status or droped",
            K(ret),
            K(index_tid_),
            "index_status",
            table_schema->get_index_status(),
            "is_drop_index",
            table_schema->is_drop_index());
      }
    }
    return ret;
  }

private:
  uint64_t index_tid_;
  mutable int64_t last_check_time_;
};

int ObBKGDDistTask::process()
{
  int ret = OB_SUCCESS;
  if (NULL != ObCurTraceId::get_trace_id()) {
    *ObCurTraceId::get_trace_id() = trace_id_;
  }
  const int64_t worker_abs_timeout_bak = THIS_WORKER.get_timeout_ts();
  ObBKGDDistTaskDag* dag = static_cast<ObBKGDDistTaskDag*>(get_dag());
  if (OB_ISNULL(dag)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag is NULL", K(ret));
  } else if (OB_ISNULL(GCTX.executor_rpc_) || OB_ISNULL(GCTX.executor_rpc_->get_proxy())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rpc proxy is NULL", K(ret));
  } else {
    // setup memory context for plan deserialize
    lib::ContextParam param;
    param.set_mem_attr(dag->get_tenant_id(), ObModIds::OB_REQ_DESERIALIZATION, ObCtxIds::DEFAULT_CTX_ID);
    FETCH_ENTITY(TENANT_SPACE, dag->get_tenant_id())
    {
      CREATE_WITH_TEMP_CONTEXT(param)
      {
        ObTask task;
        ObDistTaskProcessor processor(*this);
        int64_t pos = 0;
        ObPhysicalPlanCtx* plan_ctx = NULL;
        ObSQLSessionInfo* session = NULL;
        LOG_INFO("begin process background build index task", K(ret), K(dag->get_task_id()));
        ObBKGDTaskCompleteArg res;
        res.task_id_ = dag->get_task_id();
        res.scheduler_id_ = dag->get_scheduler_id();
        uint64_t index_tid = OB_INVALID_ID;
        if (OB_FAIL(processor.init(task))) {
          LOG_WARN("processor init failed", K(ret));
        } else if (OB_FAIL(task.deserialize(serialized_task_.ptr(), serialized_task_.length(), pos))) {
          LOG_WARN("task deserialize failed", K(ret));
        } else if (OB_ISNULL(plan_ctx = GET_PHY_PLAN_CTX(processor.get_exec_ctx()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("plan execute context is NULL", K(ret));
        } else if (OB_ISNULL(session = processor.get_exec_ctx().get_my_session())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("session is NULL", K(ret));
        } else if (OB_FAIL(get_index_tid(task, index_tid))) {
          LOG_WARN("get index table tid failed", K(ret));
        } else {
          ObExtraIndexBuildCheck index_status_checker(index_tid);
          ObExtraIndexBuildCheck::Guard index_check_guard(*session, index_status_checker);
          share::CompatModeGuard compat_mode_guard(ORACLE_MODE == session->get_compatibility_mode()
                                                       ? share::ObWorker::CompatMode::ORACLE
                                                       : share::ObWorker::CompatMode::MYSQL);
          THIS_WORKER.set_timeout_ts(plan_ctx->get_timeout_timestamp());
          if (OB_FAIL(index_status_checker.check())) {
            LOG_WARN("check index status failed", K(ret));
          } else if (OB_FAIL(session->store_query_string(
                         ObString::make_string("SQL DISTRIBUTE BACKGROUND PLAN EXECUTING")))) {
            LOG_WARN("store session query string failed", K(ret));
          } else if (OB_FAIL(processor.process_task(task, res.event_))) {
            LOG_WARN("process task failed", K(ret));
          }
        }

        if (!res.event_.is_valid()) {
          // init task event if not valid.
          ObTaskLocation task_loc;
          task_loc.set_ob_task_id(res.task_id_);
          task_loc.set_server(GCONF.self_addr_);
          int tmp_ret = res.event_.init(task_loc, ret);
          if (OB_SUCCESS != tmp_ret) {
            LOG_WARN("init task event failed", K(ret));
            ret = OB_SUCCESS == ret ? tmp_ret : ret;
          }
        }

        res.return_code_ = ret;
        ret = OB_SUCCESS;
        ret = E(EventTable::EN_BKGD_TASK_REPORT_COMPLETE) ret;
        const int64_t timeout_us = abs_timeout_us_ - ObTimeUtility::current_time();
        if (OB_FAIL(ret)) {
          LOG_INFO("do not report background task complete", K(res.task_id_));
        } else if (OB_FAIL(GCTX.executor_rpc_->get_proxy()
                               ->to(return_addr_)
                               .by(OB_SYS_TENANT_ID)  // always send as system tenant for background task
                               .as(OB_SYS_TENANT_ID)
                               .timeout(timeout_us)
                               .bkgd_task_complete(res))) {
          LOG_WARN("send task complete message failed", K(ret));
        } else {
          LOG_INFO("send background task complete message success", K(ret), K(res.task_id_), K(res.return_code_));
        }
      }
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("change tenant context fail when execute background task", K(ret), K(dag->get_tenant_id()));
    }
  }

  THIS_WORKER.set_timeout_ts(worker_abs_timeout_bak);
  ObCurTraceId::init(GCTX.self_addr_);

  return ret;
}

int ObSchedBKGDDistTask::init(const uint64_t tenant_id, const int64_t abs_timeout_us, const ObTaskID& task_id,
    uint64_t scheduler_id, const common::ObPartitionKey& pkey, const common::ObAddr& dest,
    const common::ObString& serialized_task)
{
  int ret = OB_SUCCESS;
  ObSchedBKGDDistTask t;
  t.tenant_id_ = tenant_id;
  t.abs_timeout_us_ = abs_timeout_us;
  t.task_id_ = task_id;
  t.scheduler_id_ = scheduler_id;
  t.pkey_ = pkey;
  t.dest_ = dest;
  t.serialized_task_ = serialized_task;
  if (OB_FAIL(assign(t))) {
    LOG_WARN("assign failed", K(ret));
  }
  t.serialized_task_.reset();
  return ret;
}

int ObSchedBKGDDistTask::init_execute_over_task(const ObTaskID& task_id)
{
  int ret = OB_SUCCESS;
  task_id_ = task_id;
  return ret;
}

int ObSchedBKGDDistTask::assign(const ObSchedBKGDDistTask& o)
{
  int ret = OB_SUCCESS;
  if (this != &o) {
    tenant_id_ = o.tenant_id_;
    abs_timeout_us_ = o.abs_timeout_us_;
    task_id_ = o.task_id_;
    scheduler_id_ = o.scheduler_id_;
    pkey_ = o.pkey_;
    dest_ = o.dest_;
    if (NULL != serialized_task_.ptr()) {
      ob_free(serialized_task_.ptr());
      serialized_task_.reset();
    }
    if (o.serialized_task_.length() > 0) {
      ObMemAttr attr;
      attr.label_ = ObModIds::OB_SQL_EXECUTOR_BKGD_TASK;
      void* buf = ob_malloc(o.serialized_task_.length(), attr);
      if (NULL == buf) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else {
        MEMCPY(buf, o.serialized_task_.ptr(), o.serialized_task_.length());
        serialized_task_.assign(static_cast<char*>(buf), o.serialized_task_.length());
      }
    }
  }
  return ret;
}

void ObSchedBKGDDistTask::destroy()
{
  if (NULL != serialized_task_.ptr()) {
    ob_free(serialized_task_.ptr());
    serialized_task_.reset();
  }
}

void ObSchedBKGDDistTask::to_schedule_pkey(common::ObPartitionKey& pkey) const
{
  pkey.init(task_id_.get_execution_id(), task_id_.get_job_id(), task_id_.get_task_id());
}

}  // end namespace sql
}  // end namespace oceanbase
