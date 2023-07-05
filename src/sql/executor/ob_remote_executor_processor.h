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
#include "sql/plan_cache/ob_cache_object_factory.h"

namespace oceanbase
{
namespace observer
{
struct ObGlobalContext;
}
namespace sql
{
template <typename T>
class ObRemoteBaseExecuteP : public obrpc::ObRpcProcessor<T>
{
public:
  ObRemoteBaseExecuteP(const observer::ObGlobalContext &gctx, bool is_execute_remote_plan = false)
    : obrpc::ObRpcProcessor<T>(),
      gctx_(gctx),
      rt_guard_(),
      guard_(MAX_HANDLE),
      exec_ctx_(CURRENT_CONTEXT->get_arena_allocator(), gctx.session_mgr_),
      vt_iter_factory_(*gctx_.vt_iter_creator_),
      schema_guard_(share::schema::ObSchemaMgrItem::MOD_REMOTE_EXE),
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
  virtual ~ObRemoteBaseExecuteP() { }
  int64_t get_exec_start_timestamp() const { return exec_start_timestamp_; }
  int64_t get_exec_end_timestamp() const { return exec_end_timestamp_; }
  int64_t get_process_timestamp() const { return process_timestamp_; }
  int64_t get_single_process_timestamp() const { return exec_start_timestamp_; }
  bool is_execute_remote_plan() const { return is_execute_remote_plan_; }
protected:
  int base_init();
  int base_before_process(int64_t tenant_schema_version, int64_t sys_schema_version,
      const DependenyTableStore &dependency_tables);
  int auto_start_phy_trans();
  int auto_end_phy_trans(bool rollback);
  int execute_remote_plan(ObExecContext &exec_ctx, const ObPhysicalPlan &plan);
  int execute_with_sql(ObRemoteTask &task);
  int sync_send_result(ObExecContext &exec_ctx,
                       const ObPhysicalPlan &plan,
                       common::ObScanner &scanner);
  virtual int send_result_to_controller(ObExecContext &exec_ctx, const ObPhysicalPlan &plan) = 0;
  virtual void record_exec_timestamp(bool is_first, ObExecTimestamp &exec_timestamp)
  { ObExecStatUtils::record_exec_timestamp(*this, is_first, exec_timestamp); }
  void record_sql_audit_and_plan_stat(
                        const ObPhysicalPlan *plan,
                        ObSQLSessionInfo *session);
  int base_before_response(common::ObScanner &scanner);
  int base_after_process();
  void base_cleanup();
  virtual void clean_result_buffer() = 0;
  bool query_can_retry_in_remote(int &last_err,
                                 int &err,
                                 ObSQLSessionInfo &session,
                                 int64_t &retry_times);

protected:
  const observer::ObGlobalContext &gctx_;
  // request time info guard
  observer::ObReqTimeGuard rt_guard_;
  sql::ObCacheObjGuard guard_;
  sql::ObDesExecContext exec_ctx_;
  observer::ObVirtualTableIteratorFactory vt_iter_factory_;
  share::schema::ObSchemaGetterGuard schema_guard_;
  /*
   * 用于记录事务语句是否执行过，然后判断对应的end语句是否需执行
   */
  TransState trans_state_;
  int exec_errcode_;
  int64_t process_timestamp_;
  int64_t exec_start_timestamp_;
  int64_t exec_end_timestamp_;
  bool has_send_result_;
  bool is_execute_remote_plan_; // only execute remote physical_plan not sql_string
};
/* 处理远程单partition情况(REMOTE) */
class ObRpcRemoteExecuteP : public ObRemoteBaseExecuteP<
    obrpc::ObExecutorRpcProxy::ObRpc<obrpc::OB_REMOTE_EXECUTE> >
{
public:
  ObRpcRemoteExecuteP(const observer::ObGlobalContext &gctx)
    : ObRemoteBaseExecuteP(gctx, true)
  {
  }
  virtual ~ObRpcRemoteExecuteP() {}
  virtual int init();
protected:
  virtual int send_result_to_controller(ObExecContext &exec_ctx,
                                        const ObPhysicalPlan &plan) override;
  virtual int before_process();
  virtual int process();
  virtual int before_response(int error_code);
  virtual int after_process(int error_code);
  virtual void cleanup();
  virtual void clean_result_buffer() override;
private:
  ObPhysicalPlan phy_plan_;
};

class ObRpcRemoteSyncExecuteP : public ObRemoteBaseExecuteP<
  obrpc::ObExecutorRpcProxy::ObRpc<obrpc::OB_REMOTE_SYNC_EXECUTE> >
{
public:
  ObRpcRemoteSyncExecuteP(const observer::ObGlobalContext &gctx)
    : ObRemoteBaseExecuteP(gctx)
  {
  }
  virtual ~ObRpcRemoteSyncExecuteP() {}
  virtual int init();
protected:
  virtual int send_result_to_controller(ObExecContext &exec_ctx,
                                        const ObPhysicalPlan &plan) override;
  virtual int before_process();
  virtual int process();
  virtual int before_response(int error_code);
  virtual int after_process(int error_code);
  virtual void cleanup();
  virtual void clean_result_buffer() override;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OBDEV_SRC_SQL_EXECUTOR_OB_REMOTE_EXECUTOR_PROCESSOR_H_ */
