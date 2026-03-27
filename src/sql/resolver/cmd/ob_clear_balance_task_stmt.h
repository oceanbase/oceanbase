/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESOLVER_CMD_CLEAR_BALANCE_TASK_H
#define OCEANBASE_SQL_RESOLVER_CMD_CLEAR_BALANCE_TASK_H
#include "sql/resolver/cmd/ob_system_cmd_stmt.h"
#include "share/ob_rpc_struct.h"
namespace oceanbase
{
namespace sql
{
class ObClearBalanceTaskStmt : public ObSystemCmdStmt
{
public:
  ObClearBalanceTaskStmt() : ObSystemCmdStmt(stmt::T_CLEAR_BALANCE_TASK),
    arg_()
  {
  }
  explicit ObClearBalanceTaskStmt(common::ObIAllocator *name_pool)
      : ObSystemCmdStmt(name_pool, stmt::T_CLEAR_BALANCE_TASK), arg_()
  {}

  virtual ~ObClearBalanceTaskStmt() {}

  common::ObIArray<uint64_t> &get_tenant_ids() { return arg_.tenant_ids_; }
  common::ObIArray<ObZone> &get_zone_names() { return arg_.zone_names_; }
  void set_type(const obrpc::ObAdminClearBalanceTaskArg::TaskType type) { arg_.type_ = type; }
  const obrpc::ObAdminClearBalanceTaskArg &get_rpc_arg() const { return arg_; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObClearBalanceTaskStmt);
  obrpc::ObAdminClearBalanceTaskArg arg_;
};
} //namespace sql
} //namespace oceanbase
#endif


