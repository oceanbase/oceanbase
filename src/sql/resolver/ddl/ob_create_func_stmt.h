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


