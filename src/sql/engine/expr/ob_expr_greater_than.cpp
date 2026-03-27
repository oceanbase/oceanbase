/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_greater_than.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprGreaterThan::ObExprGreaterThan(ObIAllocator &alloc)
    : ObRelationalExprOperator(alloc, T_OP_GT, N_GREATER_THAN, 2)
{
}

}
}

