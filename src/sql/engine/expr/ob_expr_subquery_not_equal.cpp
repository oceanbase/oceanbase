/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_subquery_not_equal.h"

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
