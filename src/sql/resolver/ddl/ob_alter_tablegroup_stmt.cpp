/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "sql/resolver/ddl/ob_alter_tablegroup_stmt.h"

namespace oceanbase
{

using namespace share::schema;

namespace sql
{

ObAlterTablegroupStmt::ObAlterTablegroupStmt(common::ObIAllocator *name_pool)
    : ObTablegroupStmt(name_pool, stmt::T_ALTER_TABLEGROUP)
{
}

ObAlterTablegroupStmt::ObAlterTablegroupStmt()
    : ObTablegroupStmt(stmt::T_ALTER_TABLEGROUP)
{
}

ObAlterTablegroupStmt::~ObAlterTablegroupStmt()
{
}


int ObAlterTablegroupStmt::add_table_item(const obrpc::ObTableItem &table_item)
{
  return alter_tablegroup_arg_.table_items_.push_back(table_item);
}

int ObAlterTablegroupStmt::set_primary_zone(const common::ObString &zone)
{
  return OB_SUCCESS; // ignore this (not support in 4.0)
}
int ObAlterTablegroupStmt::set_locality(const common::ObString &locality)
{
  return OB_SUCCESS; // ignore this (not support in 4.0)
}

int ObAlterTablegroupStmt::set_tablegroup_sharding(const common::ObString &sharding)
{
  return alter_tablegroup_arg_.alter_tablegroup_schema_.set_sharding(sharding);
}

int ObAlterTablegroupStmt::set_tablegroup_scope(const common::ObString &scope)
{
  return alter_tablegroup_arg_.alter_tablegroup_schema_.set_scope(scope);
}

} //namespace sql
} //namespace oceanbase


