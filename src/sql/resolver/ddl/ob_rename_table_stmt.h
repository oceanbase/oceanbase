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

#ifndef OCEANBASE_SQL_OB_RENAME_TABLE_STMT_
#define OCEANBASE_SQL_OB_RENAME_TABLE_STMT_

#include "sql/resolver/ddl/ob_ddl_stmt.h"
#include "sql/resolver/ob_stmt_resolver.h"

namespace oceanbase
{
namespace sql
{

class ObRenameTableStmt : public ObDDLStmt
{
public:
  explicit ObRenameTableStmt(common::ObIAllocator *name_pool);
  ObRenameTableStmt();
  virtual ~ObRenameTableStmt();
  obrpc::ObRenameTableArg& get_rename_table_arg(){ return rename_table_arg_; }
  const obrpc::ObRenameTableArg& get_rename_table_arg() const { return rename_table_arg_; }
  int get_rename_table_table_ids(common::ObIArray<share::schema::ObObjectStruct> &object_ids) const;
  int add_rename_table_item(const obrpc::ObRenameTableItem &rename_table_item);
  inline void set_tenant_id(const uint64_t tenant_id);
  uint64_t get_tenant_id() const { return rename_table_arg_.tenant_id_; }
  virtual obrpc::ObDDLArg &get_ddl_arg() { return rename_table_arg_; }
  TO_STRING_KV(K_(stmt_type), K_(rename_table_arg));
private:
  obrpc::ObRenameTableArg rename_table_arg_;
  DISALLOW_COPY_AND_ASSIGN(ObRenameTableStmt);
};

inline void ObRenameTableStmt::set_tenant_id(const uint64_t tenant_id)
{
  rename_table_arg_.tenant_id_ = tenant_id;
}


} // namespace sql
} // namespace oceanbase


#endif //OCEANBASE_SQL_OB_RENAME_TABLE_STMT_

