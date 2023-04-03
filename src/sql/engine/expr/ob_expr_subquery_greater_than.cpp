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
#include "sql/engine/expr/ob_expr_subquery_greater_than.h"
#include "common/row/ob_row.h"
#include "common/object/ob_obj_compare.h"
#include "sql/engine/expr/ob_expr_greater_than.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprSubQueryGreaterThan::ObExprSubQueryGreaterThan(ObIAllocator &alloc)
  : ObSubQueryRelationalExpr(alloc, T_OP_SQ_GT, N_SQ_GREATER_THAN, 2, NOT_ROW_DIMENSION,
                             INTERNAL_IN_MYSQL_MODE, INTERNAL_IN_ORACLE_MODE)
{
}

ObExprSubQueryGreaterThan::~ObExprSubQueryGreaterThan()
{
}

}  // namespace sql
}  // namespace oceanbase
