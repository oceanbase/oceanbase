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

#ifndef OCEANBASE_SQL_RESOLVER_DROP_INDEX_STMT_
#define OCEANBASE_SQL_RESOLVER_DROP_INDEX_STMT_

#include "lib/string/ob_string.h"
#include "lib/allocator/ob_allocator.h"
#include "sql/resolver/ddl/ob_ddl_stmt.h"
#include "sql/parser/parse_node.h"

namespace oceanbase
{
namespace sql
{
class ObDropIndexStmt : public ObDDLStmt
{
public:
  explicit ObDropIndexStmt(common::ObIAllocator *name_pool);
  ObDropIndexStmt();
  virtual ~ObDropIndexStmt();

  void set_name_pool(common::ObIAllocator *name_pool);
  void set_index_name(const common::ObString &index_name);
  void set_table_name(const common::ObString &table_name);
  void set_tenant_id(const uint64_t tenant_id);
  void set_database_name(const common::ObString &db_name);
  uint64_t get_tenant_id() const { return drop_index_arg_.tenant_id_; };
  uint64_t get_table_id() const { return table_id_; };
  void set_table_id(const uint64_t table_id);

  inline const common::ObString &get_database_name() const;
  inline const common::ObString &get_table_name() const;
  inline common::ObString &get_table_name();
  obrpc::ObDropIndexArg &get_drop_index_arg();
  const obrpc::ObDropIndexArg &get_drop_index_arg() const;
  virtual bool cause_implicit_commit() const { return true; }
  virtual obrpc::ObDDLArg &get_ddl_arg() { return drop_index_arg_; }
  TO_STRING_KV(K_(stmt_type), K_(drop_index_arg));
protected:
  common::ObIAllocator *name_pool_;

private:
  obrpc::ObDropIndexArg drop_index_arg_;
  uint64_t table_id_;
  DISALLOW_COPY_AND_ASSIGN(ObDropIndexStmt);
};

inline void ObDropIndexStmt::set_name_pool(common::ObIAllocator *name_pool)
{
  name_pool_ = name_pool;
}

inline const obrpc::ObDropIndexArg &ObDropIndexStmt::get_drop_index_arg() const
{
  return drop_index_arg_;
}

inline obrpc::ObDropIndexArg &ObDropIndexStmt::get_drop_index_arg()
{
  return drop_index_arg_;
}

inline void ObDropIndexStmt::set_index_name(const common::ObString &index_name)
{
  drop_index_arg_.index_name_ = index_name;
}

inline void ObDropIndexStmt::set_database_name(const common::ObString &db_name)
{
  drop_index_arg_.database_name_ = db_name;
}

inline void ObDropIndexStmt::set_table_name(const common::ObString &table_name)
{
  drop_index_arg_.table_name_ = table_name;
}

inline const common::ObString &ObDropIndexStmt::get_database_name() const
{
  return drop_index_arg_.database_name_;
}

inline const common::ObString &ObDropIndexStmt::get_table_name() const
{
  return drop_index_arg_.table_name_;
}

inline common::ObString &ObDropIndexStmt::get_table_name()
{
  return drop_index_arg_.table_name_;
}

inline void ObDropIndexStmt::set_tenant_id(const uint64_t tenant_id)
{
  drop_index_arg_.tenant_id_ = tenant_id;
}
inline void ObDropIndexStmt::set_table_id(const uint64_t table_id)
{
  table_id_ = table_id;
}

}//end of namespace sql
}//end of namespace oceanbase
#endif //OCEANBASE_SQL_RESOLVER_DROP_INDEX_STMT_
