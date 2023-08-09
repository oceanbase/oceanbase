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

#ifndef OCEANBASE_SQL_OB_TRUNCATE_TABLE_STMT_
#define OCEANBASE_SQL_OB_TRUNCATE_TABLE_STMT_

#include "share/schema/ob_schema_service.h"
#include "sql/resolver/ddl/ob_ddl_stmt.h"
#include "sql/resolver/ob_stmt_resolver.h"

namespace oceanbase
{
namespace sql
{

class ObTruncateTableStmt : public ObDDLStmt
{
public:
  explicit ObTruncateTableStmt(common::ObIAllocator *name_pool);
  ObTruncateTableStmt();
  virtual ~ObTruncateTableStmt();
  void set_database_name(const common::ObString &db_name);
  void set_table_name(const common::ObString &table_name);
  void set_tenant_id(const uint64_t tenant_id);
  uint64_t get_tenant_id() const { return truncate_table_arg_.tenant_id_; }
  const common::ObString& get_database_name() const { return truncate_table_arg_.database_name_; }
  const common::ObString& get_table_name() const { return truncate_table_arg_.table_name_; }

  inline const obrpc::ObTruncateTableArg &get_truncate_table_arg() const;
  obrpc::ObTruncateTableArg &get_truncate_table_arg() { return truncate_table_arg_; }
  virtual obrpc::ObDDLArg &get_ddl_arg() { return truncate_table_arg_; }
  void set_truncate_oracle_temp_table() { is_truncate_oracle_temp_table_ = true; }
  bool is_truncate_oracle_temp_table() { return is_truncate_oracle_temp_table_; }
  void set_oracle_temp_table_type(share::schema::ObTableType oracle_temp_table_type) { oracle_temp_table_type_ = oracle_temp_table_type; }
  share::schema::ObTableType get_oracle_temp_table_type() { return oracle_temp_table_type_; }
  TO_STRING_KV(K_(stmt_type),K_(truncate_table_arg));
private:
  obrpc::ObTruncateTableArg truncate_table_arg_;
  bool is_truncate_oracle_temp_table_;
  share::schema::ObTableType oracle_temp_table_type_;
  DISALLOW_COPY_AND_ASSIGN(ObTruncateTableStmt);
};

inline void ObTruncateTableStmt::set_tenant_id(const uint64_t tenant_id)
{
  truncate_table_arg_.tenant_id_ = tenant_id;
}

inline const obrpc::ObTruncateTableArg &ObTruncateTableStmt::get_truncate_table_arg() const
{
  return truncate_table_arg_;
}

} // namespace sql
} // namespace oceanbase
#endif //OCEANBASE_SQL_OB_TRUNCATE_TABLE_STMT_
