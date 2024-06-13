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

#ifndef OCEANBASE_SQL_OB_DROP_OUTLINE_STMT_H_
#define OCEANBASE_SQL_OB_DROP_OUTLINE_STMT_H_

#include "lib/string/ob_string.h"
#include "sql/resolver/ddl/ob_ddl_stmt.h"
namespace oceanbase
{
namespace sql
{
class ObDropOutlineStmt : public ObDDLStmt
{
public:
  ObDropOutlineStmt() :
      ObDDLStmt(stmt::T_DROP_OUTLINE),
      drop_outline_arg_()
  {}
  ~ObDropOutlineStmt() { }
  void set_database_name(const common::ObString &database_name) { drop_outline_arg_.db_name_ = database_name; }
  void set_outline_name(const common::ObString &outline_name) { drop_outline_arg_.outline_name_ = outline_name; }
  void set_tenant_id(uint64_t tenant_id) { drop_outline_arg_.tenant_id_ = tenant_id; }
  obrpc::ObDropOutlineArg &get_drop_outline_arg() { return drop_outline_arg_; }
  const obrpc::ObDropOutlineArg &get_drop_outline_arg() const { return drop_outline_arg_; }
  virtual obrpc::ObDDLArg &get_ddl_arg() { return drop_outline_arg_; }
  TO_STRING_KV(K_(drop_outline_arg));
private:
  obrpc::ObDropOutlineArg drop_outline_arg_;
  DISALLOW_COPY_AND_ASSIGN(ObDropOutlineStmt);
};
}//namespace sql
}//namespace oceanbase
#endif //OCEANBASE_SQL_OB_DROP_OUTLINE_STMT_H_
