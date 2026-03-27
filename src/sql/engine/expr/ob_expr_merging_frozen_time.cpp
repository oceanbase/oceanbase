/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "sql/engine/expr/ob_expr_merging_frozen_time.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprMergingFrozenTime::ObExprMergingFrozenTime(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_MERGING_FROZEN_TIME,
                         N_MERGING_FROZEN_TIME,
                         0,
                         VALID_FOR_GENERATED_COL,
                         NOT_ROW_DIMENSION,
                         INTERNAL_IN_MYSQL_MODE)
{
}

ObExprMergingFrozenTime::~ObExprMergingFrozenTime()
{
}

int ObExprMergingFrozenTime::calc_result_type0(ObExprResType &type, ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  type.set_timestamp();
  type.set_scale(MAX_SCALE_FOR_TEMPORAL);
  return OB_SUCCESS;
}

}
}
