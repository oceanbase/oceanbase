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
#include "sql/engine/expr/ob_expr_not_in.h"
#include "sql/engine/expr/ob_expr_in.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase {
using namespace common;
namespace sql {
// ObExprNotIn::ObExprNotIn(ObIAllocator &alloc)
//     :ObVectorExprOperator(alloc, T_OP_NOT_IN, N_NOT_IN, 2, 1) {};
//
// int ObExprNotIn::calc_result_typeN(ObExprResType &type,
//                                   ObExprResType *types,
//                                   int64_t param_num,
//                                   common::ObExprTypeCtx &type_ctx) const
//{
//  int ret = ObVectorExprOperator::calc_result_typeN(type, types, param_num, type_ctx);
//  type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
//  type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
//  return ret;
//}
//
// int ObExprNotIn::calc_resultN(common::ObObj &result, const common::ObObj *objs,
//                              int64_t param_num, ObExprCtx &expr_ctx) const
//{
//  int ret = OB_SUCCESS;
//  int64_t bv = 0;
//  EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
//  const ObIArray<ObExprCalcType> &cmp_types = result_type_.get_row_calc_cmp_types();
//  if (OB_FAIL(ObExprIn::calc(result,
//                             objs,
//                             cmp_types,
//                             param_num,
//                             real_param_num_,
//                             row_dimension_,
//                             cast_ctx))) {
//    LOG_WARN("not in op failed", K(ret));
//  } else if (OB_UNLIKELY(result.is_null())) {
//    //do nothing
//  } else if (OB_FAIL(result.get_int(bv))) {
//    LOG_WARN("get int failed", K(ret), K(result));
//  } else {
//    result.set_int(!bv);
//  }
//  return ret;
//}

}
}  // namespace oceanbase
