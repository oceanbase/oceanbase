/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_CLONE_EXECUTOR_
#define OCEANBASE_SQL_OB_CLONE_EXECUTOR_

namespace oceanbase
{
namespace sql
{
class ObExecContext;
class ObCloneTenantStmt;

class ObCloneTenantExecutor
{
public:
  ObCloneTenantExecutor() {}
  virtual ~ObCloneTenantExecutor() {}
  int execute(ObExecContext &ctx, ObCloneTenantStmt &stmt);
  int wait_clone_tenant_finished_(ObExecContext &ctx,
                                  const int64_t job_id);
private:
  DISALLOW_COPY_AND_ASSIGN(ObCloneTenantExecutor);
};

} //end namespace sql
} //end namespace oceanbase


#endif //OCEANBASE_SQL_OB_CLONE_EXECUTOR_
