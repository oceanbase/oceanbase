/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX COMMON

#include "sql/engine/expr/ob_expr_vec_vid.h"
#include "sql/engine/ob_exec_context.h"
#include "share/ob_tablet_autoincrement_service.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
ObExprVecVid::ObExprVecVid(ObIAllocator &allocator)
  : ObFuncExprOperator(allocator, T_FUN_SYS_VEC_VID, N_VEC_VID, ZERO_OR_ONE, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
  need_charset_convert_ = false;
}

int ObExprVecVid::calc_result_typeN(ObExprResType &type,
                                       ObExprResType *types,
                                       int64_t param_num,
                                       ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSEDx(param_num, types);
  type.set_int();
  return ret;
}

int ObExprVecVid::calc_resultN(ObObj &result,
                                  const ObObj *objs_array,
                                  int64_t param_num,
                                  ObExprCtx &expr_ctx) const
{
  return OB_NOT_SUPPORTED;
}

int ObExprVecVid::cg_expr(
    ObExprCGCtx &cg_ctx,
    const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(rt_expr.arg_cnt_ != 1 && rt_expr.arg_cnt_ != 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param count", K(rt_expr.arg_cnt_), K(rt_expr.args_), K(rt_expr.type_));
  } else if (OB_UNLIKELY(rt_expr.arg_cnt_ == 1 && OB_ISNULL(rt_expr.args_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, rt_expr.args_ is nullptr", K(rt_expr.arg_cnt_), K(rt_expr.args_), K(rt_expr.type_));
  } else {
    rt_expr.eval_func_ = generate_vec_id;
  }
  return ret;
}

/*static*/ int ObExprVecVid::generate_vec_id(
    const ObExpr &raw_ctx,
    ObEvalCtx &eval_ctx,
    ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  if (raw_ctx.arg_cnt_ == 0) {
    LOG_DEBUG("[vec index debug]succeed to genearte empty vid", KP(&raw_ctx), K(raw_ctx), K(expr_datum), K(eval_ctx));
  } else if (OB_UNLIKELY(1 != raw_ctx.arg_cnt_) || OB_ISNULL(raw_ctx.args_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(raw_ctx), KP(raw_ctx.args_));
  } else {
    ObExpr *calc_part_id_expr = raw_ctx.args_[0];
    ObObjectID partition_id = OB_INVALID_ID;
    ObTabletID tablet_id;
    if (OB_FAIL(ObExprCalcPartitionBase::calc_part_and_tablet_id(calc_part_id_expr, eval_ctx, partition_id, tablet_id))) {
      LOG_WARN("calc part and tablet id by expr failed", K(ret));
    } else {
      share::ObTabletAutoincrementService &auto_inc = share::ObTabletAutoincrementService::get_instance();
      uint64_t seq_id = 0;
      if (OB_FAIL(auto_inc.get_autoinc_seq(MTL_ID(), tablet_id, seq_id))) {
        LOG_WARN("fail to get tablet autoinc seq", K(ret), K(tablet_id));
      } else {
        expr_datum.set_int(seq_id);
        FLOG_INFO("succeed to genearte vector id", K(tablet_id), K(seq_id));
      }
    }
  }
  return ret;
}


}  // namespace sql
}  // namespace oceanbase
