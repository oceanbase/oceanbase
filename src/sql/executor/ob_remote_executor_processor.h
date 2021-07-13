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

#ifndef OBDEV_SRC_SQL_EXECUTOR_OB_REMOTE_EXECUTOR_PROCESSOR_H_
#define OBDEV_SRC_SQL_EXECUTOR_OB_REMOTE_EXECUTOR_PROCESSOR_H_
#include "sql/executor/ob_executor_rpc_processor.h"
namespace observer {
class ObGlobalContext;
}
namespace oceanbase {
namespace sql {
template <typename T>
class ObRemoteBaseExecuteP : public obrpc::ObRpcProcessor<T> {
public:
  ObRemoteBaseExecuteP(const observer::ObGlobalContext& gctx, bool is_execute_remote_plan = false)
      : obrpc::ObRpcProcessor<T>(),
        gctx_(gctx),
        exec_ctx_(CURRENT_CONTEXT.get_arena_allocator(), gctx.session_mgr_),
        vt_iter_factory_(*gctx_.vt_iter_creator_),
        sql_ctx_(),
        trans_state_(),
        exec_errcode_(common::OB_SUCCESS),
        process_timestamp_(0),
        exec_start_timestamp_(0),
        exec_end_timestamp_(0),
        has_send_result_(false),
        is_execute_remote_plan_(is_execute_remote_plan)
  {
    obrpc::ObRpcProcessor<T>::set_preserve_recv_data();
  }
  virtual ~ObRemoteBaseExecuteP()
  {}
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
  bool is_execute_remote_plan() const
  {
    return is_execute_remote_plan_;
  }

protected:
  int base_init();
  int base_before_process(
      int64_t tenant_schema_version, int64_t sys_schema_version, const DependenyTableStore& dependency_tables);
  int auto_start_phy_trans(ObPartitionLeaderArray& leader_parts);
  int auto_end_phy_trans(bool rollback, const common::ObPartitionArray& participants);
  int execute_remote_plan(ObExecContext& exec_ctx, const ObPhysicalPlan& plan);
  int execute_with_sql(ObRemoteTask& task);
  int sync_send_result(ObExecContext& exec_ctx, const ObPhysicalPlan& plan, common::ObScanner& scanner);
  virtual int send_result_to_controller(ObExecContext& exec_ctx, const ObPhysicalPlan& plan) = 0;
  virtual void record_exec_timestamp(bool is_first, ObExecTimestamp& exec_timestamp)
  {
    ObExecStatUtils::record_exec_timestamp(*this, is_first, exec_timestamp);
  }
  void record_sql_audit_and_plan_stat(const ObPhysicalPlan* plan, ObSQLSessionInfo* session, ObExecRecord exec_record,
      ObExecTimestamp exec_timestamp, ObWaitEventDesc& max_wait_desc, ObWaitEventStat& total_wait_desc);
  int base_before_response(common::ObScanner& scanner);
  int base_after_process();
  void base_cleanup();
  virtual void clean_result_buffer() = 0;
  bool query_can_retry_in_remote(int& last_err, int& err, ObSQLSessionInfo& session, int64_t& retry_times);

protected:
  const observer::ObGlobalContext& gctx_;
  sql::ObDesExecContext exec_ctx_;
  ObSqlPartitionLocationCache partition_location_cache_;
  observer::ObVirtualTableIteratorFactory vt_iter_factory_;
  sql::ObSqlCtx sql_ctx_;
  share::schema::ObSchemaGetterGuard schema_guard_;
  /*
   * Used to record whether the transaction statement has been executed,
   * and then determine whether the corresponding end statement needs to be executed
   */
  TransState trans_state_;
  int exec_errcode_;
  int64_t process_timestamp_;
  int64_t exec_start_timestamp_;
  int64_t exec_end_timestamp_;
  bool has_send_result_;
  bool is_execute_remote_plan_;  // only execute remote physical_plan not sql_string
};
/* Handle remote single partition situation (REMOTE) */
class ObRpcRemoteExecuteP : public ObRemoteBaseExecuteP<obrpc::ObExecutorRpcProxy::ObRpc<obrpc::OB_REMOTE_EXECUTE> > {
public:
  ObRpcRemoteExecuteP(const observer::ObGlobalContext& gctx) : ObRemoteBaseExecuteP(gctx, true)
  {}
  virtual ~ObRpcRemoteExecuteP()
  {}
  virtual int init();

protected:
  virtual int send_result_to_controller(ObExecContext& exec_ctx, const ObPhysicalPlan& plan) override;
  virtual int before_process();
  virtual int process();
  virtual int before_response();
  virtual int after_process();
  virtual void cleanup();
  virtual void clean_result_buffer() override;

private:
  int get_participants(common::ObPartitionLeaderArray& pla);

private:
  ObPhysicalPlan phy_plan_;
};

class ObRpcRemoteSyncExecuteP
    : public ObRemoteBaseExecuteP<obrpc::ObExecutorRpcProxy::ObRpc<obrpc::OB_REMOTE_SYNC_EXECUTE> > {
public:
  ObRpcRemoteSyncExecuteP(const observer::ObGlobalContext& gctx) : ObRemoteBaseExecuteP(gctx)
  {}
  virtual ~ObRpcRemoteSyncExecuteP()
  {}
  virtual int init();

protected:
  virtual int send_result_to_controller(ObExecContext& exec_ctx, const ObPhysicalPlan& plan) override;
  virtual int before_process();
  virtual int process();
  virtual int before_response();
  virtual int after_process();
  virtual void cleanup();
  virtual void clean_result_buffer() override;
};

class ObRpcRemoteASyncExecuteP
    : public ObRemoteBaseExecuteP<obrpc::ObExecutorRpcProxy::ObRpc<obrpc::OB_REMOTE_ASYNC_EXECUTE> > {
public:
  ObRpcRemoteASyncExecuteP(const observer::ObGlobalContext& gctx)
      : ObRemoteBaseExecuteP(gctx), remote_result_(), is_from_batch_(false)
  {}
  virtual ~ObRpcRemoteASyncExecuteP()
  {}
  virtual int init();
  ObRemoteTask& get_arg()
  {
    return arg_;
  }
  virtual int before_process();
  virtual int process();
  virtual int before_response();
  virtual int after_process();
  virtual void cleanup();
  void set_from_batch()
  {
    is_from_batch_ = true;
  }

protected:
  virtual int send_result_to_controller(ObExecContext& exec_ctx, const ObPhysicalPlan& plan) override;
  int send_remote_result(ObRemoteResult& remote_result);
  virtual void clean_result_buffer() override;

private:
  ObRemoteResult remote_result_;
  bool is_from_batch_;
};

class ObRpcRemotePostResultP
    : public obrpc::ObRpcProcessor<obrpc::ObExecutorRpcProxy::ObRpc<obrpc::OB_REMOTE_POST_RESULT> > {
public:
  ObRpcRemotePostResultP(const observer::ObGlobalContext& gctx) : gctx_(gctx), is_from_batch_(false)
  {
    set_preserve_recv_data();
  }
  virtual ~ObRpcRemotePostResultP()
  {}
  void set_from_batch()
  {
    is_from_batch_ = true;
  }
  virtual int init();
  ObRemoteResult& get_arg()
  {
    return arg_;
  }
  virtual int before_process()
  {
    int ret = common::OB_SUCCESS;
    if (!is_from_batch_) {
      ret = obrpc::ObRpcProcessor<obrpc::ObExecutorRpcProxy::ObRpc<obrpc::OB_REMOTE_POST_RESULT> >::before_process();
    }
    return ret;
  }
  virtual int process();
  virtual int before_response()
  {
    int ret = common::OB_SUCCESS;
    if (!is_from_batch_) {
      ret = obrpc::ObRpcProcessor<obrpc::ObExecutorRpcProxy::ObRpc<obrpc::OB_REMOTE_POST_RESULT> >::before_response();
    }
    return ret;
  }
  virtual int after_process()
  {
    int ret = common::OB_SUCCESS;
    if (!is_from_batch_) {
      ret = obrpc::ObRpcProcessor<obrpc::ObExecutorRpcProxy::ObRpc<obrpc::OB_REMOTE_POST_RESULT> >::after_process();
    }
    return ret;
  }
  virtual void cleanup()
  {
    if (!is_from_batch_) {
      obrpc::ObRpcProcessor<obrpc::ObExecutorRpcProxy::ObRpc<obrpc::OB_REMOTE_POST_RESULT> >::cleanup();
    }
  }

private:
  const observer::ObGlobalContext& gctx_;
  bool is_from_batch_;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OBDEV_SRC_SQL_EXECUTOR_OB_REMOTE_EXECUTOR_PROCESSOR_H_ */
