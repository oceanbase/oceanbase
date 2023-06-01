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
#include "sql/engine/expr/ob_expr_rownum.h"
#include "sql/engine/basic/ob_count_op.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/code_generator/ob_static_engine_expr_cg.h"
#include "sql/engine/expr/ob_expr_util.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

OB_SERIALIZE_MEMBER((ObExprRowNum, ObFuncExprOperator), operator_id_);

ObExprRowNum::ObExprRowNum(ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_ROWNUM, "rownum", 0, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION),
    operator_id_(OB_INVALID_ID)
{
}

ObExprRowNum::~ObExprRowNum()
{
}

int ObExprRowNum::calc_result_type0(ObExprResType &type, ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  type.set_number();
  type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObNumberType].scale_);
  type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObNumberType].precision_);
  return OB_SUCCESS;
}

int ObExprRowNum::cg_expr(ObExprCGCtx &op_cg_ctx,
                          const ObRawExpr &raw_expr,
                          ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  rt_expr.extra_ = operator_id_;
  rt_expr.eval_func_ = &rownum_eval;
  return ret;
}

int ObExprRowNum::rownum_eval(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  uint64_t operator_id = expr.extra_;
  ObOperatorKit *kit = ctx.exec_ctx_.get_operator_kit(operator_id);
  if (OB_UNLIKELY(OB_INVALID_ID == operator_id)) {
    ret = OB_ERR_CBY_PSEUDO_COLUMN_NOT_ALLOWED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Usage of rownum is");
  } else if (OB_ISNULL(kit) || OB_ISNULL(kit->op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operator is NULL", K(ret), K(operator_id), KP(kit));
  } else if (OB_UNLIKELY(PHY_COUNT != kit->op_->get_spec().type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("is not count operator", K(ret), K(operator_id), "spec", kit->op_->get_spec());
  } else {
    char local_buff[number::ObNumber::MAX_BYTE_LEN];
    ObDataBuffer local_alloc(local_buff, number::ObNumber::MAX_BYTE_LEN);
    number::ObNumber num;
    ObCountOp *count_op = static_cast<ObCountOp *>(kit->op_);
    if (OB_FAIL(num.from(count_op->get_cur_rownum(), local_alloc))) {
      LOG_WARN("failed to convert int to number", K(ret));
    } else {
      expr_datum.set_number(num);
    }
  }
  return ret;
}

} /* namespace sql */
} /* namespace oceanbase */
