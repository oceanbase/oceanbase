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

#ifndef OCEANBASE_SQL_OB_ALTER_DATABASE_STMT_
#define OCEANBASE_SQL_OB_ALTER_DATABASE_STMT_

#include "share/ob_rpc_struct.h"
#include "sql/resolver/ddl/ob_ddl_stmt.h"
namespace oceanbase
{
namespace sql
{
class ObAlterDatabaseStmt : public ObDDLStmt
{
public:
  ObAlterDatabaseStmt();
  explicit ObAlterDatabaseStmt(common::ObIAllocator *name_pool);
  virtual ~ObAlterDatabaseStmt();
  void set_tenant_id(const uint64_t tenant_id);
  void set_database_id(const uint64_t database_id);
  int set_database_name(const common::ObString &database_name);
  void set_collation_type(const common::ObCollationType type);
  void set_charset_type(const common::ObCharsetType type);
  common::ObCharsetType get_charset_type() const;
  void add_zone(const common::ObString &zone);
  int set_primary_zone(const common::ObString &zone);
  void set_read_only(const bool read_only);
  int set_default_tablegroup_name(const common::ObString &tablegroup_name);
  void set_alter_option_set(const common::ObBitSet<> &alter_option_set);
  common::ObBitSet<> &get_alter_option_set() { return alter_database_arg_.alter_option_bitset_; }
  obrpc::ObAlterDatabaseArg &get_alter_database_arg();
  bool only_alter_primary_zone() const
  { return alter_database_arg_.only_alter_primary_zone(); }
  virtual bool cause_implicit_commit() const { return true; }

  inline const common::ObString &get_database_name() const;
  virtual obrpc::ObDDLArg &get_ddl_arg() { return alter_database_arg_; }
  common::ObCollationType get_collation_type() {
    return alter_database_arg_.database_schema_.get_collation_type();
  }

  TO_STRING_KV(K_(alter_database_arg));
private:
  obrpc::ObAlterDatabaseArg alter_database_arg_;
};

inline const common::ObString &ObAlterDatabaseStmt::get_database_name() const
{
  return alter_database_arg_.database_schema_.get_database_name_str();
}

}//oceanbase sql
}//oceanbase namespace
#endif //OCEANBASE_SQL_OB_ALTER_DATABASE_STMT_
