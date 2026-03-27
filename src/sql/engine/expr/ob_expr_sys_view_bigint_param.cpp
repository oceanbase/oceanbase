/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_sys_view_bigint_param.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{

ObExprSysViewBigintParam::ObExprSysViewBigintParam(ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_SYS_VIEW_BIGINT_PARAM,
                       N_SYS_VIEW_BIGINT_PARAM, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                       INTERNAL_IN_MYSQL_MODE)
{
}

ObExprSysViewBigintParam::~ObExprSysViewBigintParam()
{
}

int ObExprSysViewBigintParam::calc_result_type1(ObExprResType &type,
                                                ObExprResType &type1,
                                                ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  UNUSED(type1);
  int ret = OB_SUCCESS;
  if (NOT_ROW_DIMENSION != row_dimension_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row dimension must be NOT_ROW_DIMENSION", K(ret), K(row_dimension_));
  } else {
    //keep enumset as origin
    type.set_int();
    type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].scale_);
    type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].precision_);
  }
  return ret;
}

}/* ns sql*/
}/* ns oceanbase */
