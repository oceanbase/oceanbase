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

#include "sql/engine/expr/ob_expr_tmp_file_close.h"
#include "storage/tmp_file/ob_tmp_file_manager.h"

namespace oceanbase
{
namespace sql
{

using namespace oceanbase::common;

ObExprTmpFileClose::ObExprTmpFileClose(ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_TMP_FILE_CLOSE, N_TMP_FILE_CLOSE, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION) {}

ObExprTmpFileClose::~ObExprTmpFileClose() {}

int ObExprTmpFileClose::calc_result_type1(ObExprResType &type,
                                          ObExprResType &type1,
                                          ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);

  if (ob_is_numeric_type(type1.get_type())) {
    type1.set_calc_meta(type1.get_obj_meta());
    type1.set_calc_accuracy(type1.get_accuracy());
  } else {
    const ObObjType &calc_type = ObIntType;
    type1.set_calc_type(calc_type);
    const ObAccuracy &calc_acc = ObAccuracy::DDL_DEFAULT_ACCURACY2[0][calc_type];
    type1.set_calc_accuracy(calc_acc);
  }

  type.set_type(ObIntType);
  const ObAccuracy &res_acc = ObAccuracy::DDL_DEFAULT_ACCURACY2[0][ObIntType];
  type.set_accuracy(res_acc);
  ObExprOperator::calc_result_flag1(type, type1);

  return ret;
}

int ObExprTmpFileClose::cg_expr(ObExprCGCtx &expr_cg_ctx,
                                const ObRawExpr &raw_expr,
                                ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);

  if (OB_UNLIKELY(1 != rt_expr.arg_cnt_) || OB_ISNULL(rt_expr.args_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arg cnt is invalid or args_ is NULL", K(ret), K(rt_expr));
  } else {
    rt_expr.eval_func_ = ObExprTmpFileClose::eval_tmp_file_close;
  }

  return ret;
}

int ObExprTmpFileClose::eval_tmp_file_close(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *param_datum = NULL;
  int64_t fd = -1;

  if (OB_FAIL(expr.args_[0]->eval(ctx, param_datum))) {
    LOG_WARN("eval arg 0 failed", KR(ret));
  } else if (param_datum->is_null()) {
    res_datum.set_null();
  } else if (OB_FAIL(validate_input_param(*param_datum, fd))) {
    LOG_WARN("validate input param failed", KR(ret));
  } else if (OB_FAIL(close_temp_file(fd))) {
    LOG_WARN("close temp file failed", KR(ret));
    res_datum.set_int(ret);
  } else {
    res_datum.set_int(0); // return 0 if remove file success or file not exist
  }
  return ret;
}

int ObExprTmpFileClose::validate_input_param(const ObDatum &param_datum, int64_t &fd)
{
  int ret = OB_SUCCESS;

  if (param_datum.is_null()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("input param is null", KR(ret));
  } else {
    fd = param_datum.get_int();
    if (fd < 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid file descriptor", KR(ret), K(fd));
    }
  }

  return ret;
}

int ObExprTmpFileClose::close_temp_file(int64_t fd)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.remove(MTL_ID(), fd))) {
    LOG_WARN("close tmp file failed", KR(ret), K(fd));
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase