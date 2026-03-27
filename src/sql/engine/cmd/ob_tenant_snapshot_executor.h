/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef __OB_SQL_TENANT_SNAPSHOT_EXECUTOR_H__
#define __OB_SQL_TENANT_SNAPSHOT_EXECUTOR_H__

#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
namespace share
{
class ObTenantSnapshotID;
}

namespace sql
{
class ObCreateTenantSnapshotStmt;
class ObDropTenantSnapshotStmt;

class ObCreateTenantSnapshotExecutor
{
public:
  ObCreateTenantSnapshotExecutor() {}
  virtual ~ObCreateTenantSnapshotExecutor() {}
  int execute(ObExecContext &ctx, ObCreateTenantSnapshotStmt &stmt);
private:
  int wait_create_finish_(const uint64_t tenant_id,
                          const share::ObTenantSnapshotID &tenant_snapshot_id,
                          ObExecContext &ctx);
  DISALLOW_COPY_AND_ASSIGN(ObCreateTenantSnapshotExecutor);
};

class ObDropTenantSnapshotExecutor
{
public:
  ObDropTenantSnapshotExecutor() {}
  virtual ~ObDropTenantSnapshotExecutor() {}
  int execute(ObExecContext &ctx, ObDropTenantSnapshotStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObDropTenantSnapshotExecutor);
};

} //end namespace sql
} //end namespace oceanbase

#endif //__OB_SQL_TENANT_SNAPSHOT_EXECUTOR_H__
