/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_RESTORE_EXECUTOR_
#define OCEANBASE_SQL_OB_RESTORE_EXECUTOR_

namespace oceanbase
{
namespace sql
{
class ObExecContext;
class ObPhysicalRestoreTenantStmt;

class ObPhysicalRestoreTenantExecutor
{
public:
  ObPhysicalRestoreTenantExecutor();
  virtual ~ObPhysicalRestoreTenantExecutor();
  int execute(ObExecContext &ctx, ObPhysicalRestoreTenantStmt &stmt);
private:
  int sync_wait_tenant_created_(ObExecContext &ctx, const ObString &tenant_name, const int64_t job_id);
  int physical_restore_preview(ObExecContext &ctx, ObPhysicalRestoreTenantStmt &stmt);
};
} //end namespace sql
} //end namespace oceanbase


#endif //OCEANBASE_SQL_OB_RESTORE_EXECUTOR_
