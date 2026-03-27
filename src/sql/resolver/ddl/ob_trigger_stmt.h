/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESOLVER_DDL_OB_TRIGGER_STMT_
#define OCEANBASE_SQL_RESOLVER_DDL_OB_TRIGGER_STMT_

#include "share/ob_rpc_struct.h"
#include "sql/resolver/ddl/ob_ddl_stmt.h"

namespace oceanbase
{
namespace sql
{

class ObCreateTriggerStmt : public ObDDLStmt
{
public:
  explicit ObCreateTriggerStmt(common::ObIAllocator *name_pool)
    : ObDDLStmt(name_pool, stmt::T_CREATE_TRIGGER),
      trigger_arg_()
  {}
  explicit ObCreateTriggerStmt()
    : ObDDLStmt(stmt::T_CREATE_TRIGGER),
      trigger_arg_()
  {}
  virtual ~ObCreateTriggerStmt()
  {}
  virtual obrpc::ObDDLArg &get_ddl_arg() { return trigger_arg_; }
  obrpc::ObCreateTriggerArg &get_trigger_arg() { return trigger_arg_; }
  const obrpc::ObCreateTriggerArg &get_trigger_arg() const { return trigger_arg_; }
private:
  obrpc::ObCreateTriggerArg trigger_arg_;
  DISALLOW_COPY_AND_ASSIGN(ObCreateTriggerStmt);
};

class ObDropTriggerStmt : public ObDDLStmt
{
public:
  explicit ObDropTriggerStmt(common::ObIAllocator *name_pool)
    : ObDDLStmt(name_pool, stmt::T_DROP_TRIGGER),
      trigger_table_name_(),
      is_exist(true),
      trigger_arg_()
  {}
  explicit ObDropTriggerStmt()
    : ObDDLStmt(stmt::T_DROP_TRIGGER),
      trigger_table_name_(),
      is_exist(true),
      trigger_arg_()
  {}
  virtual ~ObDropTriggerStmt()
  {}
  virtual obrpc::ObDDLArg &get_ddl_arg() { return trigger_arg_; }
  obrpc::ObDropTriggerArg &get_trigger_arg() { return trigger_arg_; }
  const obrpc::ObDropTriggerArg &get_trigger_arg() const { return trigger_arg_; }
  common::ObString trigger_table_name_;
  bool is_exist;
private:
  obrpc::ObDropTriggerArg trigger_arg_;
  DISALLOW_COPY_AND_ASSIGN(ObDropTriggerStmt);
};

class ObAlterTriggerStmt: public ObDDLStmt
{
public:
  explicit ObAlterTriggerStmt(common::ObIAllocator *name_pool)
      :
      ObDDLStmt(name_pool, stmt::T_ALTER_TRIGGER), trigger_arg_()
  {}
  explicit ObAlterTriggerStmt()
      :
      ObDDLStmt(stmt::T_ALTER_TRIGGER), trigger_arg_()
  {}
  virtual ~ObAlterTriggerStmt()
  {}
  virtual obrpc::ObDDLArg& get_ddl_arg()
  {
    return trigger_arg_;
  }
  obrpc::ObAlterTriggerArg& get_trigger_arg()
  {
    return trigger_arg_;
  }
private:
  obrpc::ObAlterTriggerArg trigger_arg_;DISALLOW_COPY_AND_ASSIGN(ObAlterTriggerStmt);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_RESOLVER_DDL_OB_TRIGGER_STMT_

