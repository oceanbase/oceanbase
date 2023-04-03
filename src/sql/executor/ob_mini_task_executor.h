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

#ifndef DEV_SRC_SQL_EXECUTOR_OB_MINI_TASK_EXECUTOR_H_
#define DEV_SRC_SQL_EXECUTOR_OB_MINI_TASK_EXECUTOR_H_
#include "sql/engine/ob_exec_context.h"
#include "sql/executor/ob_trans_result_collector.h"
#include "lib/allocator/ob_safe_arena.h"
namespace oceanbase
{
namespace sql
{
class ObSQLSessionInfo;
class ObMiniTaskExecutor
{
public:
  explicit ObMiniTaskExecutor(common::ObIAllocator &allocator)
  {
    UNUSED(allocator);
  }
  virtual ~ObMiniTaskExecutor() {}
  void destroy() { }
  int init(ObSQLSessionInfo &session, ObExecutorRpcImpl *exec_rpc)
  {
    UNUSED(session);
    UNUSED(exec_rpc);
    return common::OB_NOT_SUPPORTED;
  }

  int wait_all_task(int64_t timeout)
  {
    UNUSED(timeout);
    return common::OB_NOT_SUPPORTED;
  }
};

class ObDMLMiniTaskExecutor : public ObMiniTaskExecutor
{
public:
  explicit ObDMLMiniTaskExecutor(common::ObIAllocator &allocator)
    : ObMiniTaskExecutor(allocator)
  {}
  virtual ~ObDMLMiniTaskExecutor()  {}
  int execute(ObExecContext &ctx,
              const ObMiniJob &mini_job,
              common::ObIArray<ObTaskInfo*> &task_list,
              bool table_first,
              ObMiniTaskResult &task_result)
  {
              UNUSED(ctx);
              UNUSED(mini_job);
              UNUSED(task_list);
              UNUSED(table_first);
              UNUSED(task_result);
              return common::OB_NOT_SUPPORTED;
  }
  //同步执行，无论是在本地还是在远端
  int mini_task_execute(ObExecContext &ctx,
                        const ObMiniJob &mini_job,
                        ObTaskInfo &task_info,
                        ObMiniTaskResult &task_result)
  {
    UNUSED(ctx);
    UNUSED(mini_job);
    UNUSED(task_info);
    UNUSED(task_result);
    return common::OB_NOT_SUPPORTED;
  }
  int mini_task_submit(ObExecContext &ctx,
                       const ObMiniJob &mini_job,
                       common::ObIArray<ObTaskInfo*> &task_info_list,
                       int64_t start_idx,
                       ObMiniTaskResult &task_result)
  {
    UNUSED(ctx);
    UNUSED(mini_job);
    UNUSED(task_info_list);
    UNUSED(start_idx);
    UNUSED(task_result);
    return common::OB_NOT_SUPPORTED;
  }
  int build_mini_task_op_input(ObExecContext &ctx,
                               ObTaskInfo &task_info,
                               const ObMiniJob &mini_job)
  {
    UNUSED(ctx);
    UNUSED(task_info);
    UNUSED(mini_job);
    return common::OB_NOT_SUPPORTED;
  }
  int build_mini_task_op_input(ObExecContext &ctx,
                               ObTaskInfo &task_info,
                               const ObOpSpec &root_spec)
  {
    UNUSED(ctx);
    UNUSED(task_info);
    UNUSED(root_spec);
    return common::OB_NOT_SUPPORTED;
  }
  int build_mini_task(ObExecContext &ctx,
                      const ObMiniJob &mini_job,
                      ObTaskInfo &task_info,
                      ObMiniTask &task)
  {
    UNUSED(ctx);
    UNUSED(mini_job);
    UNUSED(task_info);
    UNUSED(task);
    return common::OB_NOT_SUPPORTED;
  }
};

class ObLookupMiniTaskExecutor : public ObMiniTaskExecutor
{
public:
  explicit ObLookupMiniTaskExecutor(common::ObIAllocator &allocator)
  : ObMiniTaskExecutor(allocator)
  {}
  virtual ~ObLookupMiniTaskExecutor() {}
  int execute(ObExecContext &ctx,
              common::ObIArray<ObMiniTask> &task_list,
              common::ObIArray<ObTaskInfo*> &task_info_list,
              ObMiniTaskRetryInfo &retry_info,
              ObMiniTaskResult &task_result)
  {
    UNUSED(ctx);
    UNUSED(task_list);
    UNUSED(task_info_list);
    UNUSED(retry_info);
    UNUSED(task_result);
    return common::OB_NOT_SUPPORTED;
  }
  int execute_one_task(ObExecContext &ctx,
                       ObMiniTask &task,
                       ObTaskInfo *task_info,
                       int64_t &ap_task_cnt,
                       ObMiniTaskRetryInfo &retry_info)
  {
    UNUSED(ctx);
    UNUSED(task);
    UNUSED(task_info);
    UNUSED(ap_task_cnt);
    UNUSED(retry_info);
    return common::OB_NOT_SUPPORTED;
  }
  int fill_lookup_task_op_input(ObExecContext &ctx,
                                ObTaskInfo *task_info,
                                const ObOpSpec &root_spec,
                                const bool retry_execution)
  {
    UNUSED(ctx);
    UNUSED(task_info);
    UNUSED(root_spec);
    UNUSED(retry_execution);
    return common::OB_NOT_SUPPORTED;
  }
  int retry_overflow_task(ObExecContext &ctx,
                          common::ObIArray<ObMiniTask> &task_list,
                          common::ObIArray<ObTaskInfo*> &task_info_list,
                          ObMiniTaskRetryInfo &retry_info,
                          ObMiniTaskResult &task_result)
  {
    UNUSED(ctx);
    UNUSED(task_list);
    UNUSED(task_info_list);
    UNUSED(retry_info);
    UNUSED(task_result);
    return common::OB_NOT_SUPPORTED;
  }
};


}  // namespace sql
}  // namespace oceanbase
#endif /* DEV_SRC_SQL_EXECUTOR_OB_MINI_TASK_EXECUTOR_H_ */
