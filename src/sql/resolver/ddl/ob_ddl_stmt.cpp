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

#include "sql/resolver/ddl/ob_ddl_stmt.h"
#include "sql/ob_sql_context.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{
int ObDDLStmt::get_first_stmt(ObString &first_stmt)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObStmt::get_first_stmt(first_stmt))) {
    LOG_WARN("fail to get first stmt", K(ret));
  } else if (OB_ISNULL(get_query_ctx())) {
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

  return ret;
}
}
}
