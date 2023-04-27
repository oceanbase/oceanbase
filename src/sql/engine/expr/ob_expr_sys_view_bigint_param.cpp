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
#include "sql/engine/expr/ob_expr_sys_view_bigint_param.h"
#include "share/object/ob_obj_cast.h"
#include "sql/session/ob_sql_session_info.h"

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
