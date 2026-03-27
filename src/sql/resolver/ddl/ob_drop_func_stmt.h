/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_DROP_FUNC_STMT_H
#define _OB_DROP_FUNC_STMT_H 1

namespace oceanbase
{
namespace sql
{

class ObDropFuncStmt : public ObDDLStmt
{
public:
  ObDropFuncStmt() :
      ObDDLStmt(stmt::T_DROP_FUNC),
      drop_func_arg_()
  {}
  ~ObDropFuncStmt() { }
  void set_func_name(const common::ObString &func_name) { drop_func_arg_.name_ = func_name; }
  void set_tenant_id(uint64_t tenant_id) { drop_func_arg_.tenant_id_ = tenant_id; }
  obrpc::ObDropUserDefinedFunctionArg &get_drop_func_arg() { return drop_func_arg_; }
  obrpc::ObDDLArg &get_ddl_arg() { return drop_func_arg_; }
  TO_STRING_KV(K_(drop_func_arg));
private:
  obrpc::ObDropUserDefinedFunctionArg drop_func_arg_;
  DISALLOW_COPY_AND_ASSIGN(ObDropFuncStmt);

};

}
}

#endif /* _OB_DROP_FUNC_STMT_H */


