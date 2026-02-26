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

#include "sql/engine/expr/ob_expr_tmp_file_open.h"
#include "storage/tmp_file/ob_tmp_file_manager.h"

namespace oceanbase
{
namespace sql
{

using namespace oceanbase::common;

ObExprTmpFileOpen::ObExprTmpFileOpen(ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_TMP_FILE_OPEN, N_TMP_FILE_OPEN, ZERO_OR_ONE, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION) {}

ObExprTmpFileOpen::~ObExprTmpFileOpen() {}

int ObExprTmpFileOpen::calc_result_typeN(ObExprResType &type,
                                         ObExprResType *types,
                                         int64_t param_num,
                                         ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(types);
  UNUSED(type_ctx);

  if (param_num > 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tmp_file_open() does not accept any arguments", K(param_num));
  } else {
    type.set_type(ObIntType);
    const ObAccuracy &res_acc = ObAccuracy::DDL_DEFAULT_ACCURACY2[0][ObIntType];
    type.set_accuracy(res_acc);
  }

  return ret;
}

int ObExprTmpFileOpen::cg_expr(ObExprCGCtx &expr_cg_ctx,
                               const ObRawExpr &raw_expr,
                               ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);

  if (OB_UNLIKELY(0 != rt_expr.arg_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arg cnt is invalid", K(ret), K(rt_expr));
  } else {
    rt_expr.eval_func_ = ObExprTmpFileOpen::eval_tmp_file_open;
  }

  return ret;
}

int ObExprTmpFileOpen::eval_tmp_file_open(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);

  int64_t fd = tmp_file::ObTmpFileGlobal::INVALID_TMP_FILE_FD;
  if (OB_FAIL(create_temp_file(fd))) {
    LOG_WARN("create temp file failed", KR(ret));
  } else {
    res_datum.set_int(fd);
    LOG_INFO("created temp file", K(fd));
  }

  return ret;
}

int ObExprTmpFileOpen::create_temp_file(int64_t &fd)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.open(MTL_ID(), fd, 0, "tmp_file_open"))) {
    LOG_WARN("open tmp file failed", KR(ret));
  } else {
    LOG_INFO("open tmp file success", K(fd));
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase