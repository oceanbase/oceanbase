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
