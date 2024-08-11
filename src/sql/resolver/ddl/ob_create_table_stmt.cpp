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

#include "sql/resolver/ddl/ob_create_table_stmt.h"
#include "common/object/ob_obj_type.h"
#include "share/schema/ob_column_schema.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::share::schema;

namespace oceanbase
{
namespace sql
{

ObCreateTableStmt::ObCreateTableStmt(ObIAllocator *name_pool)
    : ObTableStmt(name_pool, stmt::T_CREATE_TABLE),
      create_table_arg_(),
      is_view_stmt_(false),
      view_need_privs_(),
      sub_select_stmt_(NULL),
      view_define_(NULL),
      insert_mode_(0)
{
}

ObCreateTableStmt::ObCreateTableStmt()
    : ObTableStmt(stmt::T_CREATE_TABLE),
      create_table_arg_(),
      is_view_stmt_(false),
      view_need_privs_(),
      sub_select_stmt_(NULL),
      view_define_(NULL),
      insert_mode_(0)
{
}

ObCreateTableStmt::~ObCreateTableStmt()
{
}

int ObCreateTableStmt::set_table_id(ObStmtResolver &ctx, const uint64_t table_id)
{
  UNUSED(ctx);
  int ret = OB_SUCCESS;
  create_table_arg_.schema_.set_table_id(table_id);
  return ret;
}

int ObCreateTableStmt::invalidate_backup_table_id()
{
  int ret = OB_SUCCESS;
  create_table_arg_.schema_.set_table_id(OB_INVALID_ID);
  return ret;
}

int ObCreateTableStmt::add_column_schema(const ObColumnSchemaV2 &column)
{
  return create_table_arg_.schema_.add_column(column);
}

const ObColumnSchemaV2 *ObCreateTableStmt::get_column_schema(const ObString &column_name) const
{
  return create_table_arg_.schema_.get_column_schema(column_name);
}

int ObCreateTableStmt::get_first_stmt(ObString &first_stmt)
{
  int ret = OB_SUCCESS;

  if (EXTERNAL_TABLE == get_table_type()) {
    first_stmt = get_masked_sql();
  } else {
    if (OB_FAIL(ObStmt::get_first_stmt(first_stmt))) {
      LOG_WARN("fail to get first stmt", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(get_query_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("query ctx is null", K(ret));
    } else if (OB_FAIL(ObCharset::charset_convert(allocator_,
                                                  first_stmt,
                                                  get_query_ctx()->get_sql_stmt_coll_type(),
                                                  ObCharset::get_system_collation(),
                                                  first_stmt,
                                                  ObCharset::REPLACE_UNKNOWN_CHARACTER_ON_SAME_CHARSET))) {
      LOG_WARN("fail to convert charset", K(ret), K(first_stmt),
               "stmt collation type", get_query_ctx()->get_sql_stmt_coll_type());
    }
  }

  return ret;
}

} // namespace sql
} // namespace oceanbase
