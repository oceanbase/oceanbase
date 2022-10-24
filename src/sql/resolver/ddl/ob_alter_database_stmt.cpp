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

#define USING_LOG_PREFIX SQL_RESV

#include "sql/resolver/ddl/ob_alter_database_stmt.h"

using namespace oceanbase::common;
using namespace oceanbase::share::schema;

namespace oceanbase
{
namespace sql
{
ObAlterDatabaseStmt::ObAlterDatabaseStmt()
    : ObDDLStmt(stmt::T_ALTER_DATABASE),
    alter_database_arg_()
  {
  }

ObAlterDatabaseStmt::ObAlterDatabaseStmt(common::ObIAllocator *name_pool)
    : ObDDLStmt(name_pool, stmt::T_ALTER_DATABASE),
    alter_database_arg_()
  {
  }

ObAlterDatabaseStmt::~ObAlterDatabaseStmt()
{
}

void ObAlterDatabaseStmt::add_zone(const common::ObString &zone)
{
  // not supported
}

int ObAlterDatabaseStmt::set_primary_zone(const common::ObString &zone)
{
  return OB_SUCCESS; // not supported
}

void ObAlterDatabaseStmt::set_read_only(const bool read_only)
{
  alter_database_arg_.database_schema_.set_read_only(read_only);
}

int ObAlterDatabaseStmt::set_default_tablegroup_name(const common::ObString &tablegroup_name)
{
  return alter_database_arg_.database_schema_.set_default_tablegroup_name(tablegroup_name);
}

void ObAlterDatabaseStmt::set_tenant_id(const uint64_t tenant_id)
{
  alter_database_arg_.database_schema_.set_tenant_id(tenant_id);
}

void ObAlterDatabaseStmt::set_database_id(const uint64_t database_id)
{
  //alter database will not set database_id
  UNUSED(database_id);
}

int ObAlterDatabaseStmt::set_database_name(const ObString &database_name)
{
  return alter_database_arg_.database_schema_.set_database_name(database_name);
}

void ObAlterDatabaseStmt::set_collation_type(const common::ObCollationType type)
{
  alter_database_arg_.database_schema_.set_collation_type(type);
}

void ObAlterDatabaseStmt::set_charset_type(const common::ObCharsetType type)
{
  alter_database_arg_.database_schema_.set_charset_type(type);
}

void ObAlterDatabaseStmt::set_alter_option_set(const ObBitSet<> &alter_option_set)
{
  //copy
  alter_database_arg_.alter_option_bitset_ = alter_option_set;
}

common::ObCharsetType ObAlterDatabaseStmt::get_charset_type() const
{
  return alter_database_arg_.database_schema_.get_charset_type();
}

obrpc::ObAlterDatabaseArg& ObAlterDatabaseStmt::get_alter_database_arg()
{
  return alter_database_arg_;
}
}//namespace sql
}//namespace oceanbase
