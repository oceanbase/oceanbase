/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SRC_SQL_RESOLVER_CMD_OB_SYS_DISPATCH_CALL_STMT_H_
#define OCEANBASE_SRC_SQL_RESOLVER_CMD_OB_SYS_DISPATCH_CALL_STMT_H_

#include "sql/resolver/cmd/ob_cmd_stmt.h"

namespace oceanbase
{

using namespace common;

namespace sql
{

class ObSysDispatchCallStmt final : public ObCMDStmt
{
public:
  explicit ObSysDispatchCallStmt() : ObCMDStmt(stmt::T_SYS_DISPATCH_CALL) {}
  virtual ~ObSysDispatchCallStmt() {}
  DISABLE_COPY_ASSIGN(ObSysDispatchCallStmt);

  void set_call_stmt(const ObString &call_stmt) { call_stmt_ = call_stmt; }
  const ObString &get_call_stmt() const { return call_stmt_; }
  void set_designated_tenant_id(const uint64_t tenant_id) { designated_tenant_id_ = tenant_id; }
  uint64_t get_designated_tenant_id() const { return designated_tenant_id_; }
  void set_designated_tenant_name(const ObString &tenant_name)
  {
    designated_tenant_name_ = tenant_name;
  }
  const ObString &get_designated_tenant_name() const { return designated_tenant_name_; }
  void set_tenant_compat_mode(const ObCompatibilityMode mode) { tenant_compat_mode_ = mode; }
  ObCompatibilityMode get_tenant_compat_mode() const { return tenant_compat_mode_; }

  TO_STRING_KV(N_STMT_TYPE,
               ((int)stmt_type_),
               K_(designated_tenant_id),
               K_(tenant_compat_mode),
               K_(call_stmt));

private:
  ObString call_stmt_;
  uint64_t designated_tenant_id_;
  ObString designated_tenant_name_;
  ObCompatibilityMode tenant_compat_mode_;
};

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_SRC_SQL_RESOLVER_CMD_OB_SYS_DISPATCH_CALL_STMT_H_
