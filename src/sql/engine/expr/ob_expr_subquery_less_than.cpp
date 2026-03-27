/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_subquery_less_than.h"

namespace oceanbase {
using namespace common;
namespace sql {
;


ObExprSubQueryLessThan::ObExprSubQueryLessThan(ObIAllocator &alloc) :
  ObSubQueryRelationalExpr(alloc, T_OP_SQ_LT, N_SQ_LESS_THAN, 2, NOT_ROW_DIMENSION,
                           INTERNAL_IN_MYSQL_MODE, INTERNAL_IN_ORACLE_MODE)
{
}

ObExprSubQueryLessThan::~ObExprSubQueryLessThan()
{
}


} // namespace sql
} // namespace oceanbase
