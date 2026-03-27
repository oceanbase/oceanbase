/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_less_than.h"

namespace oceanbase
{
using namespace common;

namespace sql
{

ObExprLessThan::ObExprLessThan(ObIAllocator &alloc)
    : ObRelationalExprOperator(alloc, T_OP_LT, N_LESS_THAN, 2)
{
}

}
}
