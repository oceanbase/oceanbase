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

#ifndef OCEANBASE_SQL_EXECUTOR_OB_EXECUTOR_RPC_PROCESSOR_
#define OCEANBASE_SQL_EXECUTOR_OB_EXECUTOR_RPC_PROCESSOR_

#include "observer/virtual_table/ob_virtual_table_iterator_factory.h"
#include "observer/ob_server_struct.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "sql/executor/ob_executor_rpc_proxy.h"
#include "sql/monitor/ob_phy_plan_monitor_info.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_des_exec_context.h"
#include "sql/ob_sql_trans_control.h"
#include "share/schema/ob_schema_getter_guard.h"

#define OB_DEFINE_SQL_PROCESSOR(cls, pcode, pname) \
  class pname : public obrpc::ObRpcProcessor<obrpc::Ob##cls##RpcProxy::ObRpc<pcode>>

#define OB_DEFINE_SQL_TASK_PROCESSOR(cls, pcode, pname)                                 \
  OB_DEFINE_SQL_PROCESSOR(cls, obrpc::pcode, pname)                                     \
  {                                                                                     \
  public:                                                                               \
    pname(const observer::ObGlobalContext& gctx) : gctx_(gctx) exec_ctx_(), phy_plan_() \
    {}                                                                                  \
    virturl ~pname()                                                                    \
    {}                                                                                  \
    virtual int init();                                                                 \
                                                                                        \
  protected:                                                                            \
    virtual int process();                                                              \
                                                                                        \
  private:                                                                              \
    const observer::ObGlobalContext& gctx_;                                             \
    sql::ObExecContext& exec_ctx_;                                                      \
    sql::ObPhysicalPlan& phy_plan_;                                                     \
  }

#define OB_DEFINE_SQL_CMD_PROCESSOR(cls, pcode, pname)         \
  OB_DEFINE_SQL_PROCESSOR(cls, obrpc::pcode, pname)            \
  {                                                            \
  public:                                                      \
    pname(const observer::ObGlobalContext& gctx) : gctx_(gctx) \
    {}                                                         \
    virtual ~pname()                                           \
    {}                                                         \
                                                               \
  protected:                                                   \
    int process();                                             \
    int preprocess_arg();                                      \
                                                               \
  private:                                                     \
    const observer::ObGlobalContext& gctx_;                    \
    common::ObArenaAllocator alloc_;                           \
  }

namespace oceanbase {
namespace observer {
class ObGlobalContext;
}
namespace share {
namespace schema {}
}  // namespace share
namespace sql {
class ObExecContext;
class ObPhysicalPlan;
class ObIntermResultManager;
class ObIntermResultIterator;

class ObWorkerSessionGuard {
public:
  ObWorkerSessionGuard(ObSQLSessionInfo* session);
  ~ObWorkerSessionGuard();
};

class ObDistExecuteBaseP {
public:
  ObDistExecuteBaseP(const observer::ObGlobalContext& gctx, bool sync)
      : gctx_(gctx),
        exec_ctx_(gctx.session_mgr_),
        vt_iter_factory_(*gctx_.vt_iter_creator_),
        phy_plan_(),
        sql_ctx_(),
        trans_state_(),
        exec_record_(),
        process_timestamp_(0),
        exec_start_timestamp_(0),
        exec_end_timestamp_(0),
        sync_(sync),
        partition_location_cache_()
  {}
  virtual ~ObDistExecuteBaseP()
  {}
  virtual int init(ObTask& task);
  int64_t get_exec_start_timestamp() const
  {
    return exec_start_timestamp_;
  }
  int64_t get_exec_end_timestamp() const
  {
    return exec_end_timestamp_;
  }
  int64_t get_process_timestamp() const
  {
    return process_timestamp_;
  }
  int64_t get_single_process_timestamp() const
  {
    return exec_start_timestamp_;
  }
  ObDesExecContext& get_exec_ctx()
  {
    return exec_ctx_;
  }

protected:
  virtual int param_preprocess(ObTask& task);
  virtual int execute_dist_plan(ObTask& task, ObTaskCompleteEvent& task_event);
  virtual void record_exec_timestamp(bool is_first, ObExecTimestamp& exec_timestamp) = 0;

private:
  int get_participants(common::ObPartitionIArray& participants, const ObTask& task);

private:
  const observer::ObGlobalContext& gctx_;
  sql::ObDesExecContext exec_ctx_;
  observer::ObVirtualTableIteratorFactory vt_iter_factory_;
  sql::ObPhysicalPlan phy_plan_;
  sql::ObSqlCtx sql_ctx_;
  share::schema::ObSchemaGetterGuard schema_guard_;
  TransState trans_state_;
  ObExecRecord exec_record_;
  int64_t process_timestamp_;
  int64_t exec_start_timestamp_;
  int64_t exec_end_timestamp_;
  bool sync_;
  /* partition cache for global index lookup*/
  ObSqlPartitionLocationCache partition_location_cache_;
};

// This would not used after cluster version upgrade to 1.3.0
// Remain this class, only for compatibility
class ObRpcDistExecuteP : public ObDistExecuteBaseP,
                          public obrpc::ObRpcProcessor<obrpc::ObExecutorRpcProxy::ObRpc<obrpc::OB_DIST_EXECUTE>> {
public:
  ObRpcDistExecuteP(const observer::ObGlobalContext& gctx) : ObDistExecuteBaseP(gctx, true)
  {
    set_preserve_recv_data();
  }
  virtual ~ObRpcDistExecuteP()
  {}
  virtual int init();
  virtual void record_exec_timestamp(bool is_first, ObExecTimestamp& exec_timestamp)
  {
    ObExecStatUtils::record_exec_timestamp(*this, is_first, exec_timestamp);
  }

protected:
  virtual int before_process();
  virtual int process();
  virtual int after_process();
  virtual void cleanup();
};

class ObRpcAPDistExecuteP : public ObDistExecuteBaseP,
                            public obrpc::ObRpcProcessor<obrpc::ObExecutorRpcProxy::ObRpc<obrpc::OB_AP_DIST_EXECUTE>> {
public:
  ObRpcAPDistExecuteP(const observer::ObGlobalContext& gctx) : ObDistExecuteBaseP(gctx, false)
  {
    set_preserve_recv_data();
  }
  virtual ~ObRpcAPDistExecuteP()
  {}
  virtual int init();
  virtual void record_exec_timestamp(bool is_first, ObExecTimestamp& exec_timestamp)
  {
    ObExecStatUtils::record_exec_timestamp(*this, is_first, exec_timestamp);
  }

protected:
  virtual int before_process();
  virtual int process();
  virtual int after_process();
  virtual int before_response();
  virtual void cleanup();
};

class ObRpcAPDistExecuteCB : public obrpc::ObExecutorRpcProxy::AsyncCB<obrpc::OB_AP_DIST_EXECUTE> {
public:
  ObRpcAPDistExecuteCB(const common::ObAddr& server, const ObTaskID& ob_task_id, const ObCurTraceId::TraceId& trace_id,
      int64_t timeout_ts)
      : task_loc_(server, ob_task_id), timeout_ts_(timeout_ts)
  {
    trace_id_.set(trace_id);
  }
  virtual ~ObRpcAPDistExecuteCB()
  {}

public:
  virtual int process();
  virtual void on_invalid()
  {
    free_my_memory();
  }
  virtual void on_timeout();
  rpc::frame::ObReqTransport::AsyncCB* clone(const rpc::frame::SPAlloc& alloc) const
  {
    void* buf = alloc(sizeof(*this));
    rpc::frame::ObReqTransport::AsyncCB* newcb = NULL;
    if (NULL != buf) {
      newcb =
          new (buf) ObRpcAPDistExecuteCB(task_loc_.get_server(), task_loc_.get_ob_task_id(), trace_id_, timeout_ts_);
    }
    return newcb;
  }
  void set_args(const ObTask& arg)
  {
    UNUSED(arg);
  }

private:
  void free_my_memory()
  {
    result_.reset();
  }

private:
  ObTaskLocation task_loc_;
  common::ObCurTraceId::TraceId trace_id_;
  int64_t timeout_ts_;
  DISALLOW_COPY_AND_ASSIGN(ObRpcAPDistExecuteCB);
};

class ObMiniTaskBaseP {
public:
  ObMiniTaskBaseP(const observer::ObGlobalContext& gctx)
      : gctx_(gctx),
        exec_ctx_(gctx.session_mgr_),
        vt_iter_factory_(*gctx_.vt_iter_creator_),
        phy_plan_(),
        sql_ctx_(),
        trans_state_(),
        is_rollback_(false),
        process_timestamp_(0),
        exec_start_timestamp_(0),
        exec_end_timestamp_(0)
  {}
  virtual ~ObMiniTaskBaseP()
  {}
  int init_task(ObMiniTask& task);
  int64_t get_exec_start_timestamp() const
  {
    return exec_start_timestamp_;
  }
  int64_t get_exec_end_timestamp() const
  {
    return exec_end_timestamp_;
  }
  int64_t get_process_timestamp() const
  {
    return process_timestamp_;
  }
  int64_t get_single_process_timestamp() const
  {
    return exec_start_timestamp_;
  }

protected:
  int prepare_task_env(ObMiniTask& task);
  int execute_subplan(const ObOpSpec& root_spec, ObScanner& scanner);
  int execute_mini_plan(ObMiniTask& task, ObMiniTaskResult& result);
  int sync_send_result(ObExecContext& exec_ctx, const ObPhyOperator& op, common::ObScanner& scanner);
  int sync_send_result(ObExecContext& exec_ctx, ObOperator& op, ObScanner& scanner);
  virtual void record_exec_timestamp(bool is_first, ObExecTimestamp& exec_timestamp) = 0;

protected:
  const observer::ObGlobalContext& gctx_;
  sql::ObDesExecContext exec_ctx_;
  observer::ObVirtualTableIteratorFactory vt_iter_factory_;
  sql::ObPhysicalPlan phy_plan_;
  sql::ObSqlCtx sql_ctx_;
  share::schema::ObSchemaGetterGuard schema_guard_;
  TransState trans_state_;
  bool is_rollback_;
  int64_t process_timestamp_;
  int64_t exec_start_timestamp_;
  int64_t exec_end_timestamp_;
};

class ObRpcMiniTaskExecuteP
    : public ObMiniTaskBaseP,
      public obrpc::ObRpcProcessor<obrpc::ObExecutorRpcProxy::ObRpc<obrpc::OB_MINI_TASK_EXECUTE>> {
public:
  ObRpcMiniTaskExecuteP(const observer::ObGlobalContext& gctx) : ObMiniTaskBaseP(gctx)
  {
    set_preserve_recv_data();
  }
  virtual int init();

protected:
  virtual void record_exec_timestamp(bool is_first, ObExecTimestamp& exec_timestamp)
  {
    ObExecStatUtils::record_exec_timestamp(*this, is_first, exec_timestamp);
  }

protected:
  virtual int before_process();
  virtual int process();
  virtual int before_response();
  virtual int after_process()
  {
    return common::OB_SUCCESS;
  }
  virtual void cleanup();
};

class ObRpcAPMiniDistExecuteP
    : public ObMiniTaskBaseP,
      public obrpc::ObRpcProcessor<obrpc::ObExecutorRpcProxy::ObRpc<obrpc::OB_AP_MINI_DIST_EXECUTE>> {
public:
  ObRpcAPMiniDistExecuteP(const observer::ObGlobalContext& gctx) : ObMiniTaskBaseP(gctx)
  {
    set_preserve_recv_data();
  }
  virtual int init();

protected:
  virtual void record_exec_timestamp(bool is_first, ObExecTimestamp& exec_timestamp)
  {
    ObExecStatUtils::record_exec_timestamp(*this, is_first, exec_timestamp);
  }

protected:
  virtual int before_process();
  virtual int process();
  virtual int before_response();
  virtual int after_process()
  {
    return common::OB_SUCCESS;
  }
  virtual void cleanup();
};

class ObRpcAPMiniDistExecuteCB : public obrpc::ObExecutorRpcProxy::AsyncCB<obrpc::OB_AP_MINI_DIST_EXECUTE> {
public:
  ObRpcAPMiniDistExecuteCB(ObAPMiniTaskMgr* ap_mini_task_mgr, const ObTaskID& task_id,
      const ObCurTraceId::TraceId& trace_id, const ObAddr& dist_server_, int64_t timeout_ts);
  virtual ~ObRpcAPMiniDistExecuteCB()
  {
    free_my_memory();
  }

public:
  virtual int process();
  virtual void on_invalid()
  {
    free_my_memory();
  }
  virtual void on_timeout();
  rpc::frame::ObReqTransport::AsyncCB* clone(const rpc::frame::SPAlloc& alloc) const
  {
    void* buf = alloc(sizeof(*this));
    rpc::frame::ObReqTransport::AsyncCB* newcb = NULL;
    if (NULL != buf) {
      newcb = new (buf) ObRpcAPMiniDistExecuteCB(ap_mini_task_mgr_, task_id_, trace_id_, dist_server_, timeout_ts_);
    }
    return newcb;
  }
  void set_args(const ObMiniTask& arg)
  {
    UNUSED(arg);
  }

public:
  static void deal_with_rpc_timeout_err(const int64_t timeout_ts, int& err);

private:
  void free_my_memory();

private:
  ObAPMiniTaskMgr* ap_mini_task_mgr_;
  ObTaskID task_id_;
  common::ObCurTraceId::TraceId trace_id_;
  const ObAddr dist_server_;
  int64_t timeout_ts_;
  DISALLOW_COPY_AND_ASSIGN(ObRpcAPMiniDistExecuteCB);
};

class ObPingSqlTaskBaseP {
public:
  ObPingSqlTaskBaseP(const observer::ObGlobalContext& gctx) : gctx_(gctx)
  {}
  virtual ~ObPingSqlTaskBaseP()
  {}

protected:
  int try_forbid_task(const ObPingSqlTask& ping_task, bool& forbid_succ);
  int try_kill_task(const ObPingSqlTask& ping_task, bool& is_running);

protected:
  const observer::ObGlobalContext& gctx_;
};

class ObRpcAPPingSqlTaskP : public ObPingSqlTaskBaseP,
                            public obrpc::ObRpcProcessor<obrpc::ObExecutorRpcProxy::ObRpc<obrpc::OB_AP_PING_SQL_TASK>> {
public:
  ObRpcAPPingSqlTaskP(const observer::ObGlobalContext& gctx) : ObPingSqlTaskBaseP(gctx)
  {}
  virtual ~ObRpcAPPingSqlTaskP()
  {}

protected:
  virtual int process();
};

class ObDistributedSchedulerManager;
class ObRpcAPPingSqlTaskCB : public obrpc::ObExecutorRpcProxy::AsyncCB<obrpc::OB_AP_PING_SQL_TASK> {
public:
  ObRpcAPPingSqlTaskCB(const ObTaskID& task_id);
  virtual ~ObRpcAPPingSqlTaskCB()
  {
    free_my_memory();
  }

public:
  int set_dist_task_mgr(ObDistributedSchedulerManager* dist_task_mgr);
  int set_mini_task_mgr(ObAPMiniTaskMgr* mini_task_mgr);
  virtual int process();
  virtual void on_invalid()
  {
    free_my_memory();
  }
  virtual void on_timeout()
  {
    free_my_memory();
  }
  rpc::frame::ObReqTransport::AsyncCB* clone(const rpc::frame::SPAlloc& alloc) const
  {
    void* buf = alloc(sizeof(*this));
    ObRpcAPPingSqlTaskCB* newcb = NULL;
    if (NULL != buf) {
      newcb = new (buf) ObRpcAPPingSqlTaskCB(task_id_);
    }
    if (NULL != newcb) {
      switch (task_id_.get_task_type()) {
        case ET_DIST_TASK:
          newcb->set_dist_task_mgr(dist_task_mgr_);
          break;
        case ET_MINI_TASK:
          newcb->set_mini_task_mgr(mini_task_mgr_);
          break;
        default:
          break;
      }
    }
    return newcb;
  }
  void set_args(const ObPingSqlTask& arg)
  {
    UNUSED(arg);
  }

protected:
  void free_my_memory();

protected:
  ObTaskID task_id_;
  ObDistributedSchedulerManager* dist_task_mgr_;
  ObAPMiniTaskMgr* mini_task_mgr_;
  DISALLOW_COPY_AND_ASSIGN(ObRpcAPPingSqlTaskCB);
};

class ObRpcTaskFetchResultP
    : public obrpc::ObRpcProcessor<obrpc::ObExecutorRpcProxy::ObRpc<obrpc::OB_TASK_FETCH_RESULT>> {
public:
  ObRpcTaskFetchResultP(const observer::ObGlobalContext& gctx) : gctx_(gctx)
  {
    set_preserve_recv_data();
  }
  virtual ~ObRpcTaskFetchResultP()
  {}
  virtual int init();

protected:
  virtual int process();

private:
  int sync_send_result(ObIntermResultIterator& iter);

private:
  const observer::ObGlobalContext& gctx_;
};

class ObRpcTaskFetchIntermResultP
    : public obrpc::ObRpcProcessor<obrpc::ObExecutorRpcProxy::ObRpc<obrpc::OB_TASK_FETCH_INTERM_RESULT>> {
public:
  ObRpcTaskFetchIntermResultP(const observer::ObGlobalContext& gctx) : gctx_(gctx)
  {
    set_preserve_recv_data();
  }
  virtual ~ObRpcTaskFetchIntermResultP()
  {}
  // virtual int init();
protected:
  virtual int process();

private:
  int sync_send_result(ObIntermResultIterator& iter);

private:
  const observer::ObGlobalContext& gctx_;
};

class ObRpcBKGDTaskCompleteP
    : public obrpc::ObRpcProcessor<obrpc::ObExecutorRpcProxy::ObRpc<obrpc::OB_BKGD_TASK_COMPLETE>> {
public:
  ObRpcBKGDTaskCompleteP(const observer::ObGlobalContext&)
  {
    set_preserve_recv_data();
  }
  virtual int process();

  static int notify_error(const ObTaskID& task_id, const uint64_t scheduler_id, const int return_code);
};

}  // namespace sql

namespace sql {
OB_DEFINE_SQL_CMD_PROCESSOR(Executor, OB_TASK_COMPLETE, ObRpcTaskCompleteP);
OB_DEFINE_SQL_CMD_PROCESSOR(Executor, OB_TASK_NOTIFY_FETCH, ObRpcTaskNotifyFetchP);
OB_DEFINE_SQL_CMD_PROCESSOR(Executor, OB_TASK_KILL, ObRpcTaskKillP);
OB_DEFINE_SQL_CMD_PROCESSOR(Executor, OB_CLOSE_RESULT, ObRpcCloseResultP);
OB_DEFINE_SQL_CMD_PROCESSOR(Executor, OB_BKGD_DIST_EXECUTE, ObRpcBKGDDistExecuteP);
OB_DEFINE_SQL_CMD_PROCESSOR(Executor, OB_FETCH_INTERM_RESULT_ITEM, ObFetchIntermResultItemP);
OB_DEFINE_SQL_CMD_PROCESSOR(Executor, OB_CHECK_BUILD_INDEX_TASK_EXIST, ObCheckBuildIndexTaskExistP);
}  // namespace sql

}  // namespace oceanbase
#endif /* OCEANBASE_SQL_EXECUTOR_OB_EXECUTOR_RPC_PROCESSOR_ */
