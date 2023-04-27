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

#include "sql/engine/expr/ob_expr_merging_frozen_time.h"
#include "lib/ob_name_def.h"

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
