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

#include "lib/oblog/ob_log.h"
#include "sql/engine/expr/ob_expr_master_pos_wait.h"
#include "objit/common/ob_item_type.h"
#include "sql/session/ob_sql_session_info.h"
#include <math.h>
#include "lib/oblog/ob_log.h"

namespace oceanbase
{
using namespace common;
using namespace common::number;
namespace sql
{

ObExprMasterPosWait::ObExprMasterPosWait(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_MASTER_POS_WAIT, N_MASTER_POS_WAIT, MORE_THAN_TWO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprMasterPosWait::~ObExprMasterPosWait()
{
}

int ObExprMasterPosWait::calc_result_typeN(ObExprResType &type, 
                                ObExprResType *types, 
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;

  if (!is_mysql_mode()) {
    ret = OB_ERR_FUNCTION_UNKNOWN;
    LOG_WARN("interval expr only exists in mysql mode", K(ret));
  } else if (param_num < 2 || param_num > 4) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(types), K(param_num), K(ret));
  } 
  else {
    for (int64_t i = 0; i < param_num && OB_SUCC(ret); i++) {
      LOG_DEBUG("info for param_type", K(types[i].get_type()));
      if((i == 0 || i == 3)) {
        types[i].set_type(ObCharType);
      }
      else if(i == 1 || i == 2) {
          types[i].set_type(ObIntType);
      }
    }
    if(OB_SUCC(ret)){
      type.set_int();
      type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].scale_);
      type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].precision_);
    }
  }
  return ret;
}

int ObExprMasterPosWait::eval_master_pos_wait(const ObExpr &expr, ObEvalCtx &ctx,
    ObDatum &expr_datum){
        int ret = OB_SUCCESS;
        ObDatum *log_pos = NULL;
        if (expr.arg_cnt_ > 2 && OB_FAIL(expr.args_[2]->eval(ctx, log_pos))) {
            LOG_WARN("eval arg failed", K(ret), K(expr));
        } else if (log_pos != NULL && log_pos->get_int()<0) {
          ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
          LOG_WARN("log_pos should greater than 0", K(ret));
        } else{
          expr_datum.set_null();
          LOG_USER_WARN(OB_NOT_SUPPORTED, "MASTER_POS_WAIT() just msocks the syntax of MySQL without supporting specific realization");
        }
        return ret;
    }

int ObExprMasterPosWait::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprMasterPosWait::eval_master_pos_wait;
  return ret;
}

} /* sql */
} /* oceanbase */
