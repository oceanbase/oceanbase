/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_TENANT_SNAPSHOT_STMT_H_
#define OCEANBASE_SQL_OB_TENANT_SNAPSHOT_STMT_H_

#include "sql/resolver/cmd/ob_cmd_stmt.h"

namespace oceanbase
{
namespace sql
{

class ObCreateTenantSnapshotStmt : public ObCMDStmt
{
public:
  explicit ObCreateTenantSnapshotStmt(common::ObIAllocator *name_pool)
    : ObCMDStmt(name_pool, stmt::T_CREATE_TENANT_SNAPSHOT),
      tenant_name_(),
      tenant_snapshot_name_()
      {}
  ObCreateTenantSnapshotStmt()
    : ObCMDStmt(stmt::T_CREATE_TENANT_SNAPSHOT),
      tenant_name_(),
      tenant_snapshot_name_()
      {}
  virtual ~ObCreateTenantSnapshotStmt() {}
  const common::ObString get_tenant_name() const { return tenant_name_.str(); }
  const common::ObString get_tenant_snapshot_name() const { return tenant_snapshot_name_.str(); }

  int set_tenant_name(const common::ObString &tenant_name)
  {
    return tenant_name_.assign(tenant_name);
  }
  int set_tenant_snapshot_name(const common::ObString &tenant_snapshot_name)
  {
    return tenant_snapshot_name_.assign(tenant_snapshot_name);
  }
private:
  common::ObFixedLengthString<common::OB_MAX_TENANT_NAME_LENGTH + 1> tenant_name_;
  common::ObFixedLengthString<common::OB_MAX_TENANT_SNAPSHOT_NAME_LENGTH + 1> tenant_snapshot_name_;
  DISALLOW_COPY_AND_ASSIGN(ObCreateTenantSnapshotStmt);
};

class ObDropTenantSnapshotStmt : public ObCMDStmt
{
public:
  explicit ObDropTenantSnapshotStmt(common::ObIAllocator *name_pool)
    : ObCMDStmt(name_pool, stmt::T_DROP_TENANT_SNAPSHOT),
      tenant_name_(),
      tenant_snapshot_name_()
      {}
  ObDropTenantSnapshotStmt()
    : ObCMDStmt(stmt::T_DROP_TENANT_SNAPSHOT),
      tenant_name_(),
      tenant_snapshot_name_()
      {}
  virtual ~ObDropTenantSnapshotStmt() {}
  const common::ObString get_tenant_name() const { return tenant_name_.str(); }
  const common::ObString get_tenant_snapshot_name() const { return tenant_snapshot_name_.str(); }

  int set_tenant_name(const common::ObString &tenant_name)
  {
    return tenant_name_.assign(tenant_name);
  }
  int set_tenant_snapshot_name(const common::ObString &tenant_snapshot_name)
  {
    return tenant_snapshot_name_.assign(tenant_snapshot_name);
  }
private:
  common::ObFixedLengthString<common::OB_MAX_TENANT_NAME_LENGTH + 1> tenant_name_;
  common::ObFixedLengthString<common::OB_MAX_TENANT_SNAPSHOT_NAME_LENGTH + 1> tenant_snapshot_name_;
  DISALLOW_COPY_AND_ASSIGN(ObDropTenantSnapshotStmt);
};

} /* sql */
} /* oceanbase */

#endif //OCEANBASE_SQL_OB_TENANT_SNAPSHOT_STMT_H_
