/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_less_equal.h"

namespace oceanbase
{
using namespace common;

namespace sql
{

ObExprLessEqual::ObExprLessEqual(ObIAllocator &alloc)
    : ObRelationalExprOperator(alloc, T_OP_LE, N_LESS_EQUAL, 2)
{
}

}
}
