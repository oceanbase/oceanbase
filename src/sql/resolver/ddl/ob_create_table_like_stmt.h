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

#ifndef OCEANBASE_SQL_OB_CREATE_TABLE_LIKE_TABLE_STMT_
#define OCEANBASE_SQL_OB_CREATE_TABLE_LIKE_TABLE_STMT_

#include "sql/resolver/ddl/ob_ddl_stmt.h"
#include "sql/resolver/ob_stmt_resolver.h"
#include "share/schema/ob_schema_service.h"

namespace oceanbase
{
namespace sql
{

class ObCreateTableLikeStmt : public ObDDLStmt
{
public:
  explicit ObCreateTableLikeStmt(common::ObIAllocator *name_pool);
  ObCreateTableLikeStmt();
  virtual ~ObCreateTableLikeStmt();

  void set_origin_db_name(const common::ObString &db_name);
  const common::ObString &get_origin_db_name() const
  { return create_table_like_arg_.origin_db_name_; }

  void set_origin_table_name(const common::ObString &table_name);
  const common::ObString &get_origin_table_name() const
  { return create_table_like_arg_.origin_table_name_; }

  void set_new_db_name(const common::ObString &db_name);
  const common::ObString &get_new_db_name() const
  { return create_table_like_arg_.new_db_name_; }

  void set_new_table_name(const common::ObString &table_name);
  const common::ObString &get_new_table_name() const
  { return create_table_like_arg_.new_table_name_; }

  void set_tenant_id(const uint64_t tenant_id);
  uint64_t get_tenant_id() const
  { return create_table_like_arg_.tenant_id_; }

  void set_table_type(const share::schema::ObTableType table_type)
  { create_table_like_arg_.table_type_ = table_type; }

  share::schema::ObTableType get_table_type() const
  { return create_table_like_arg_.table_type_; }

  int set_create_host(common::ObIAllocator &allocator_, const common::ObString create_host)
  {
    return ob_write_string(allocator_, create_host, create_table_like_arg_.create_host_);
  }

  const common::ObString &get_create_host() const
  { return create_table_like_arg_.create_host_; }

  void set_if_not_exist(const bool if_not_exist);

  void set_define_user_id(const uint64_t user_id)
  { create_table_like_arg_.define_user_id_ = user_id; }
  virtual bool cause_implicit_commit() const { return share::schema::TMP_TABLE != create_table_like_arg_.table_type_; }

  inline const obrpc::ObCreateTableLikeArg &get_create_table_like_arg() const;
  virtual obrpc::ObDDLArg &get_ddl_arg() { return create_table_like_arg_; }
  TO_STRING_KV(K_(stmt_type),K_(create_table_like_arg));
private:
  obrpc::ObCreateTableLikeArg create_table_like_arg_;
};

inline const obrpc::ObCreateTableLikeArg &ObCreateTableLikeStmt::get_create_table_like_arg() const
{
  return create_table_like_arg_;
}

inline void ObCreateTableLikeStmt::set_origin_db_name(const common::ObString &database_name)
{
  create_table_like_arg_.origin_db_name_ = database_name;
}

inline void ObCreateTableLikeStmt::set_origin_table_name(const common::ObString &table_name)
{
  create_table_like_arg_.origin_table_name_ = table_name;
}

inline void ObCreateTableLikeStmt::set_new_db_name(const common::ObString &database_name)
{
  create_table_like_arg_.new_db_name_ = database_name;
}

inline void ObCreateTableLikeStmt::set_new_table_name(const common::ObString &table_name)
{
  create_table_like_arg_.new_table_name_ = table_name;
}

inline void ObCreateTableLikeStmt::set_tenant_id(const uint64_t tenant_id)
{
  create_table_like_arg_.tenant_id_ = tenant_id;
}

inline void ObCreateTableLikeStmt::set_if_not_exist(const bool if_not_exist)
{
  create_table_like_arg_.if_not_exist_ = if_not_exist;
}

} // namespace sql
} // namespace oceanbase
#endif //OCEANBASE_SQL_OB_CREATE_TABLE_LIKE_TABLE_STMT_
