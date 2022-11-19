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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_subquery_not_equal.h"
#include "common/row/ob_row.h"
#include "common/object/ob_obj_compare.h"
#include "sql/engine/expr/ob_expr_not_equal.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprSubQueryNotEqual::ObExprSubQueryNotEqual(ObIAllocator &alloc)
  : ObSubQueryRelationalExpr(alloc, T_OP_SQ_NE, N_SQ_NOT_EQUAL, 2, NOT_ROW_DIMENSION,
                             INTERNAL_IN_MYSQL_MODE, INTERNAL_IN_ORACLE_MODE)
{
}

ObExprSubQueryNotEqual::~ObExprSubQueryNotEqual()
{
}


}  // namespace sql
}  // namespace oceanbase
