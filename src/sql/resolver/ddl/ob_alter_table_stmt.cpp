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

#include "sql/resolver/ddl/ob_alter_table_stmt.h"

namespace oceanbase {
using namespace common;
namespace sql {

ObAlterTableStmt::ObAlterTableStmt(common::ObIAllocator* name_pool)
    : ObTableStmt(name_pool, stmt::T_ALTER_TABLE), is_comment_table_(false), is_alter_system_(false)
{}

ObAlterTableStmt::ObAlterTableStmt()
    : ObTableStmt(stmt::T_ALTER_TABLE), is_comment_table_(false), is_alter_system_(false)
{}

ObAlterTableStmt::~ObAlterTableStmt()
{}

int ObAlterTableStmt::add_column(const share::schema::AlterColumnSchema& column_schema)
{
  int ret = OB_SUCCESS;
  share::schema::AlterTableSchema& alter_table_schema = get_alter_table_arg().alter_table_schema_;
  if (OB_FAIL(alter_table_schema.add_alter_column(column_schema))) {
    SQL_RESV_LOG(WARN, "failed to add column schema to alter table schema", K(ret));
  }
  return ret;
}

int ObAlterTableStmt::add_index_arg(obrpc::ObIndexArg* index_arg)
{
  int ret = OB_SUCCESS;
  if (index_arg == NULL) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "index arg should not be null!", K(ret));
  } else if (OB_FAIL(alter_table_arg_.index_arg_list_.push_back(index_arg))) {
    SQL_RESV_LOG(WARN, "failed to add index arg to alter table arg!", K(ret));
  }
  return ret;
}

int ObAlterTableStmt::set_database_name(const ObString& db_name)
{
  return alter_table_arg_.alter_table_schema_.set_database_name(db_name);
}

int ObAlterTableStmt::set_origin_database_name(const ObString& origin_db_name)
{
  return alter_table_arg_.alter_table_schema_.set_origin_database_name(origin_db_name);
}

int ObAlterTableStmt::set_table_name(const ObString& table_name)
{
  return alter_table_arg_.alter_table_schema_.set_table_name(table_name);
}

int ObAlterTableStmt::set_origin_table_name(const ObString& origin_table_name)
{
  return alter_table_arg_.alter_table_schema_.set_origin_table_name(origin_table_name);
}

void ObAlterTableStmt::set_table_id(const uint64_t table_id)
{
  alter_table_arg_.alter_table_schema_.set_table_id(table_id);
}

}  // namespace sql
}  // namespace oceanbase
