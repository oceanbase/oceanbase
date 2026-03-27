/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_CREATE_FUNC_STMT_H
#define _OB_CREATE_FUNC_STMT_H 1

#include "lib/string/ob_string.h"
#include "sql/resolver/ddl/ob_ddl_stmt.h"

namespace oceanbase
{
namespace sql
{

class ObCreateFuncStmt : public ObDDLStmt
{
public:
  ObCreateFuncStmt() :
      ObDDLStmt(stmt::T_CREATE_FUNC)
  {}
  ~ObCreateFuncStmt() {}

  obrpc::ObCreateUserDefinedFunctionArg &get_create_func_arg() { return create_func_arg_; }

  virtual obrpc::ObDDLArg &get_ddl_arg() { return create_func_arg_; }

private:
  obrpc::ObCreateUserDefinedFunctionArg create_func_arg_;
  DISALLOW_COPY_AND_ASSIGN(ObCreateFuncStmt);
};

}
}

#endif /* _OB_CREATE_FUNC_STMT_H */


