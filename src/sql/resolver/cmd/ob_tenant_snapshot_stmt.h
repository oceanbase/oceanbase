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
