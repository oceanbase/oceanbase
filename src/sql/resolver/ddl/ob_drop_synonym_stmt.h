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

#ifndef OCEANBASE_SQL_OB_DROP_SYNONYM_STMT_H_
#define OCEANBASE_SQL_OB_DROP_SYNONYM_STMT_H_

#include "lib/string/ob_string.h"
#include "sql/resolver/ddl/ob_ddl_stmt.h"
namespace oceanbase
{
namespace sql
{
class ObDropSynonymStmt : public ObDDLStmt
{
public:
  ObDropSynonymStmt() :
      ObDDLStmt(stmt::T_DROP_SYNONYM),
      drop_synonym_arg_()
  {}
  ~ObDropSynonymStmt() { }
  void set_database_name(const common::ObString &database_name) { drop_synonym_arg_.db_name_ = database_name; }
  const common::ObString &get_database_name() const { return drop_synonym_arg_.db_name_; }
  void set_synonym_name(const common::ObString &synonym_name) { drop_synonym_arg_.synonym_name_ = synonym_name; }
  void set_tenant_id(uint64_t tenant_id) { drop_synonym_arg_.tenant_id_ = tenant_id; }
  void set_force(bool is_force) { drop_synonym_arg_.is_force_ = is_force; }
  obrpc::ObDropSynonymArg &get_drop_synonym_arg() { return drop_synonym_arg_; }
  virtual obrpc::ObDDLArg &get_ddl_arg() { return drop_synonym_arg_; }
  TO_STRING_KV(K_(drop_synonym_arg));
private:
  obrpc::ObDropSynonymArg drop_synonym_arg_;
  DISALLOW_COPY_AND_ASSIGN(ObDropSynonymStmt);
};
}//namespace sql
}//namespace oceanbase
#endif //OCEANBASE_SQL_OB_DROP_SYNONYM_STMT_H_
