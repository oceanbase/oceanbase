/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "sql/resolver/ddl/ob_truncate_table_stmt.h"

namespace oceanbase
{

using namespace share::schema;

namespace sql
{

ObTruncateTableStmt::ObTruncateTableStmt(common::ObIAllocator *name_pool)
    : ObDDLStmt(name_pool, stmt::T_TRUNCATE_TABLE),
      is_truncate_oracle_temp_table_(false),
      oracle_temp_table_type_(share::schema::MAX_TABLE_TYPE)
{
}

ObTruncateTableStmt::ObTruncateTableStmt()
    : ObDDLStmt(stmt::T_TRUNCATE_TABLE),
      is_truncate_oracle_temp_table_(false),
      oracle_temp_table_type_(share::schema::MAX_TABLE_TYPE)
{
}

ObTruncateTableStmt::~ObTruncateTableStmt()
{
}

void ObTruncateTableStmt::set_database_name(const common::ObString &database_name)
{
  truncate_table_arg_.database_name_ = database_name;
}

void ObTruncateTableStmt::set_table_name(const common::ObString &table_name)
{
  truncate_table_arg_.table_name_ = table_name;
}

} //namespace sql
} //namespace oceanbase
