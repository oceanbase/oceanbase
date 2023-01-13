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

#ifndef OCEANBASE_SQL_RESOLVER_DDL_CREATE_SYNONYM_STMT_H_
#define OCEANBASE_SQL_RESOLVER_DDL_CREATE_SYNONYM_STMT_H_
#include "share/ob_define.h"
#include "sql/resolver/ddl/ob_ddl_stmt.h"
#include "sql/resolver/ob_stmt_resolver.h"

namespace oceanbase
{
namespace sql
{
class ObCreateSynonymStmt : public ObDDLStmt
{
public:
  const static int OB_DEFAULT_ARRAY_SIZE = 16;
  explicit ObCreateSynonymStmt(common::ObIAllocator *name_pool);
  ObCreateSynonymStmt();
  virtual ~ObCreateSynonymStmt();
  void set_or_replace(const bool b);
  bool is_or_replace() const {return create_synonym_arg_.or_replace_;}
  int64_t get_database_id() const { return create_synonym_arg_.synonym_info_.get_database_id();};
  void set_database_id(const uint64_t database_id);

  void set_object_database_id(const uint64_t database_id);
  void set_tenant_id(const uint64_t tenant_id ) {create_synonym_arg_.synonym_info_.set_tenant_id(tenant_id);}
  obrpc::ObCreateSynonymArg &get_create_synonym_arg();

  int set_synonym_name(common::ObString &string) {return create_synonym_arg_.synonym_info_.set_synonym_name(string);}
  const common::ObString &get_synonym_name() const { return  create_synonym_arg_.synonym_info_.get_synonym_name_str(); }

  int set_object_name(common::ObString &string) { return create_synonym_arg_.synonym_info_.set_object_name(string); }
  const common::ObString &get_object_name() const { return  create_synonym_arg_.synonym_info_.get_object_name_str(); }

  void set_database_name(const common::ObString &db_name) { create_synonym_arg_.db_name_ = db_name;  }
  const common::ObString &get_database_name() const { return create_synonym_arg_.db_name_; }
  common::ObString& get_db_name() {return create_synonym_arg_.db_name_;}

  void set_object_database_name(const common::ObString &db_name) { create_synonym_arg_.obj_db_name_ = db_name;  }
  const common::ObString &get_object_database_name() const { return create_synonym_arg_.obj_db_name_; }
  void set_dependency_info(const ObDependencyInfo &dep) { create_synonym_arg_.dependency_info_ = dep; }
  const ObDependencyInfo &get_dependency_info() const { return create_synonym_arg_.dependency_info_; }

  virtual obrpc::ObDDLArg &get_ddl_arg() { return create_synonym_arg_; }
  TO_STRING_KV(K_(create_synonym_arg));
private:
  //  int set_table_id(ObStmtResolver &ctx, const uint64_t table_id);
private:
  obrpc::ObCreateSynonymArg create_synonym_arg_;
};


inline obrpc::ObCreateSynonymArg &ObCreateSynonymStmt::get_create_synonym_arg()
{
  return create_synonym_arg_;
}

inline void ObCreateSynonymStmt::set_database_id(const uint64_t database_id)
{
  create_synonym_arg_.synonym_info_.set_database_id(database_id);
}

inline void ObCreateSynonymStmt::set_object_database_id(const uint64_t database_id)
{
  create_synonym_arg_.synonym_info_.set_object_database_id(database_id);
}

inline void ObCreateSynonymStmt::set_or_replace(bool b)
{
  create_synonym_arg_.or_replace_ = b;
}

}
}

#endif //OCEANBASE_SQL_RESOLVER_DDL_CREATE_SYNONYM_STMT_H_
