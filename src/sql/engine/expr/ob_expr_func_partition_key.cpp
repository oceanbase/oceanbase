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
#include "sql/engine/expr/ob_expr_func_partition_key.h"

namespace oceanbase {
using namespace common;
namespace sql {
ObExprFuncPartOldKey::ObExprFuncPartOldKey(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_PART_KEY_V1, N_PART_KEY_V1, MORE_THAN_ZERO, NOT_ROW_DIMENSION)
{}

ObExprFuncPartOldKey::~ObExprFuncPartOldKey()
{}

int ObExprFuncPartOldKey::calc_result_typeN(
    ObExprResType& type, ObExprResType* types_stack, int64_t param_num, ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  UNUSED(types_stack);
  UNUSED(param_num);
  type.set_int();
  type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].precision_);
  type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].scale_);
  return OB_SUCCESS;
}

int ObExprFuncPartOldKey::calc_resultN(
    ObObj& result, const ObObj* objs_stack, int64_t param_num, ObExprCtx& expr_ctx) const
{
  UNUSED(expr_ctx);
  uint64_t hash_code = 0;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(objs_stack)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("objs_stack is null", K(ret));
  } else if (OB_UNLIKELY(param_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(param_num));
  } else {
    for (int64_t i = 0; i < param_num; ++i) {
      hash_code = objs_stack[i].hash_v1(hash_code);
    }
    int64_t result_num = static_cast<int64_t>(hash_code);
    result_num = result_num < 0 ? -result_num : result_num;
    result.set_int(result_num);
  }
  return ret;
}

int ObExprFuncPartOldKey::cg_expr(ObExprCGCtx&, const ObRawExpr&, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  rt_expr.eval_func_ = eval_part_old_key;
  return ret;
}

int ObExprFuncPartOldKey::eval_part_old_key(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  uint64_t hash_val = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; i++) {
    ObDatum* datum = NULL;
    ObObj obj;
    if (OB_FAIL(expr.args_[i]->eval(ctx, datum))) {
      LOG_WARN("evaluate parameter failed", K(ret));
    } else if (OB_FAIL(datum->to_obj(obj, expr.args_[i]->obj_meta_, expr.args_[i]->obj_datum_map_))) {
      LOG_WARN("convert datum to obj failed", K(ret));
    } else {
      hash_val = obj.hash_v1(hash_val);
    }
  }

  if (OB_SUCC(ret)) {
    expr_datum.set_int(std::abs(static_cast<int64_t>(hash_val)));
  }
  return ret;
}

////////////////////////////////////////////////////////////////
ObExprFuncPartKey::ObExprFuncPartKey(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_PART_KEY_V2, N_PART_KEY_V2, MORE_THAN_ZERO, NOT_ROW_DIMENSION)
{}

ObExprFuncPartKey::~ObExprFuncPartKey()
{}

int ObExprFuncPartKey::calc_result_typeN(
    ObExprResType& type, ObExprResType* types_stack, int64_t param_num, ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  UNUSED(types_stack);
  UNUSED(param_num);
  type.set_int();
  return OB_SUCCESS;
}

int ObExprFuncPartKey::calc_resultN(
    ObObj& result, const ObObj* objs_stack, int64_t param_num, ObExprCtx& expr_ctx) const
{
  UNUSED(expr_ctx);
  uint64_t hash_code = 0;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(objs_stack)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("objs_stack is null", K(ret));
  } else if (OB_UNLIKELY(param_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(param_num));
  } else {
    for (int64_t i = 0; i < param_num; ++i) {
      hash_code = objs_stack[i].hash(hash_code);
    }
    int64_t result_num = static_cast<int64_t>(hash_code);
    result_num = result_num < 0 ? -result_num : result_num;
    result.set_int(result_num);
  }
  return ret;
}

int ObExprFuncPartKey::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_partition_key;
  return ret;
}

int ObExprFuncPartKey::calc_partition_key(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("eval param value failed", K(ret));
  } else {
    uint64_t hash_value = 0;
    for (int i = 0; i < expr.arg_cnt_; i++) {
      ObDatum& param_datum = expr.locate_param_datum(ctx, i);
      ObExprHashFuncType hash_func = expr.args_[i]->basic_funcs_->default_hash_;
      hash_value = hash_func(param_datum, hash_value);
    }
    int64_t result_num = static_cast<int64_t>(hash_value);
    result_num = result_num < 0 ? -result_num : result_num;
    expr_datum.set_int(result_num);
  }
  return ret;
}

///////////////////////////////////////////////////////////////////////////

ObExprFuncPartNewKey::ObExprFuncPartNewKey(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_PART_KEY_V3, N_PART_KEY_V3, MORE_THAN_ZERO, NOT_ROW_DIMENSION)
{}

ObExprFuncPartNewKey::~ObExprFuncPartNewKey()
{}

int ObExprFuncPartNewKey::calc_result_typeN(
    ObExprResType& type, ObExprResType* types_stack, int64_t param_num, ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  UNUSED(types_stack);
  UNUSED(param_num);
  type.set_int();
  return OB_SUCCESS;
}

int ObExprFuncPartNewKey::calc_resultN(
    ObObj& result, const ObObj* objs_stack, int64_t param_num, ObExprCtx& expr_ctx) const
{
  UNUSED(expr_ctx);
  uint64_t hash_code = 0;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(objs_stack)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("objs_stack is null", K(ret));
  } else if (OB_UNLIKELY(param_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(param_num));
  } else {
    for (int64_t i = 0; i < param_num; ++i) {
      hash_code = objs_stack[i].hash_murmur(hash_code);
    }
    int64_t result_num = static_cast<int64_t>(hash_code);
    result_num = result_num < 0 ? -result_num : result_num;
    result.set_int(result_num);
  }
  return ret;
}

int ObExprFuncPartNewKey::cg_expr(ObExprCGCtx&, const ObRawExpr&, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  rt_expr.eval_func_ = calc_new_partition_key;
  return ret;
}

int ObExprFuncPartNewKey::calc_new_partition_key(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("eval param value failed", K(ret));
  } else {
    uint64_t hash_value = 0;
    for (int i = 0; i < expr.arg_cnt_; i++) {
      ObDatum& param_datum = expr.locate_param_datum(ctx, i);
      ObExprHashFuncType hash_func = expr.args_[i]->basic_funcs_->murmur_hash_;
      hash_value = hash_func(param_datum, hash_value);
    }
    int64_t result_num = static_cast<int64_t>(hash_value);
    result_num = result_num < 0 ? -result_num : result_num;
    expr_datum.set_int(result_num);
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
