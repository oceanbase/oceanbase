/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_subquery_equal.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprSubQueryEqual::ObExprSubQueryEqual(ObIAllocator &alloc)
  : ObSubQueryRelationalExpr(alloc, T_OP_SQ_EQ, N_SQ_EQUAL, 2, NOT_ROW_DIMENSION,
                             INTERNAL_IN_MYSQL_MODE, INTERNAL_IN_ORACLE_MODE)
{
}

ObExprSubQueryEqual::~ObExprSubQueryEqual()
{
}


}  // namespace sql
}  // namespace oceanbase
