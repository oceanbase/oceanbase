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

#ifndef OCEANBASE_SQL_EXECUTOR_OB_DISTRIBUTED_TASK_EXECUTOR_
#define OCEANBASE_SQL_EXECUTOR_OB_DISTRIBUTED_TASK_EXECUTOR_

#include "sql/executor/ob_task_executor.h"

namespace oceanbase {
namespace sql {
class ObExecContext;
class ObJob;
class ObTask;
class ObExecutorRpcImpl;
class ObExecutorRpcCtx;
class ObTransResultCollector;
class ObDistributedTaskExecutor : public ObTaskExecutor {
public:
  explicit ObDistributedTaskExecutor(const uint64_t scheduler_id);
  virtual ~ObDistributedTaskExecutor();

  virtual int execute(ObExecContext& query_ctx, ObJob* job, ObTaskInfo* task_info);
  virtual int kill(ObExecContext& ctx, ObJob* job, ObTaskInfo* task_info);
  virtual void reset()
  {
    ObTaskExecutor::reset();
  }
  int close_result(ObExecContext& ctx, const ObTaskInfo* task_info);
  void set_trans_result(ObTransResultCollector* trans_result)
  {
    trans_result_ = trans_result;
  }

private:
  int send_close_result_rpc(ObExecContext& ctx, const ObTaskInfo* task_info);
  int build_task(ObExecContext& query_ctx, ObJob& job, ObTaskInfo& task_info, ObTask& task);
  int task_dispatch(
      ObExecContext& exec_ctx, ObExecutorRpcImpl& rpc, ObExecutorRpcCtx& rpc_ctx, ObTask& task, ObTaskInfo& task_info);

private:
  uint64_t scheduler_id_;
  ObTransResultCollector* trans_result_;
  DISALLOW_COPY_AND_ASSIGN(ObDistributedTaskExecutor);
};

}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_EXECUTOR_OB_DISTRIBUTED_TASK_EXECUTOR_ */
