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

#include "sql/engine/expr/ob_expr_greatest.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_less_than.h"
//#include "sql/engine/expr/ob_expr_promotion_util.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/oblog/ob_log.h"
#include "share/object/ob_obj_cast.h"

namespace oceanbase {
using namespace common;
namespace sql {

ObExprBaseGreatest::ObExprBaseGreatest(ObIAllocator& alloc, int32_t param_num,
    ObExprOperatorType type /* T_FUN_SYS_GREATEST */, const char* name /* N_GREATEST */)
    : ObExprBaseLeastGreatest(alloc, type, name, param_num)
{}

ObExprBaseGreatest::~ObExprBaseGreatest()
{}

int ObExprBaseGreatest::calc_result_typeN(
    ObExprResType& type, ObExprResType* types_stack, int64_t param_num, common::ObExprTypeCtx& type_ctx) const
{
  UNUSED(type);
  UNUSED(types_stack);
  UNUSED(param_num);
  UNUSED(type_ctx);

  return OB_SUCCESS;
}

int ObExprBaseGreatest::calc_resultN(
    ObObj& result, const ObObj* objs_stack, int64_t param_num, ObExprCtx& expr_ctx) const
{
  return ObMinMaxExprOperator::calc_(result, objs_stack, param_num, result_type_, expr_ctx, CO_GT, need_cast_);
}

int ObExprBaseGreatest::calc(
    ObObj& result, const ObObj* objs_stack, int64_t param_num, const ObExprResType& result_type, ObExprCtx& expr_ctx)
{
  return ObMinMaxExprOperator::calc_(result, objs_stack, param_num, result_type, expr_ctx, CO_GT, true);
}

ObExprGreatestMySQL::ObExprGreatestMySQL(ObIAllocator& alloc) : ObExprBaseGreatest(alloc, MORE_THAN_ONE)
{}

ObExprGreatestMySQL::~ObExprGreatestMySQL()
{}

/* Greatest behavior in MySQL:
  *. cached_field_type is derived using standard logic agg_field_type() as the result column type.
  *. The calculation process of collation: If the parameter contains a numeric type,
     it is binary, otherwise it is calculated by the rules
  *. The calculation process is to find an intermediate result according to get_cmp_type(),
     and then all values are transferred to the intermediate result for comparison operations
     In other words, the comparison process has nothing to do with cached_field_type. The logic of get_calc_type() is:
     only when the parameters are STRING, The return type is STRING, otherwise the return type is numeric.
     When returning a numeric type, all parameters are converted to numeric types and then compared.
 */
int ObExprGreatestMySQL::calc_result_typeN(
    ObExprResType& type, ObExprResType* types, int64_t param_num, ObExprTypeCtx& type_ctx) const
{
  return calc_result_typeN_mysql(type, types, param_num, type_ctx);
}

ObExprGreatestMySQLInner::ObExprGreatestMySQLInner(ObIAllocator& alloc)
    : ObExprBaseGreatest(alloc, MORE_THAN_ONE, T_FUN_SYS_GREATEST_INNER, "")
{}

ObExprGreatestMySQLInner::~ObExprGreatestMySQLInner()
{}

int ObExprGreatestMySQLInner::calc_result_typeN(
    ObExprResType& type, ObExprResType* types, int64_t param_num, ObExprTypeCtx& type_ctx) const
{
  return calc_result_typeN_mysql(type, types, param_num, type_ctx);
}

ObExprOracleGreatest::ObExprOracleGreatest(ObIAllocator& alloc) : ObExprBaseGreatest(alloc, MORE_THAN_ZERO)
{}

ObExprOracleGreatest::~ObExprOracleGreatest()
{}

/** Oracle greatest behavior:
 * 1. oracle allow greatest with only 1 param. MySQL does not allow 1
 * 2. oracle return type is same with first param. MySQL will check all param and calc one type to
 *    return
 */
int ObExprOracleGreatest::calc_result_typeN(
    ObExprResType& type, ObExprResType* types, int64_t param_num, ObExprTypeCtx& type_ctx) const
{
  return calc_result_typeN_oracle(type, types, param_num, type_ctx);
}

int ObExprBaseGreatest::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  bool all_same_type = true;
  const uint32_t param_num = rt_expr.arg_cnt_;
  if (OB_UNLIKELY(is_oracle_mode() && param_num < 1) || OB_UNLIKELY(!is_oracle_mode() && param_num < 2) ||
      OB_ISNULL(rt_expr.args_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("args_ is null or too few arguments", K(ret), K(rt_expr.args_), K(param_num));
  } else {
    if (!is_oracle_mode()) {
      const uint32_t real_param_num = param_num / 3;
      for (int64_t i = 0; OB_SUCC(ret) && i < real_param_num; i++) {
        if (OB_FAIL(ObStaticEngineExprCG::replace_var_rt_expr(
                rt_expr.args_[i], rt_expr.args_[i + real_param_num], &rt_expr, i + real_param_num))) {
          LOG_WARN("replace var rt expr failed", K(ret));
        } else if (OB_FAIL(ObStaticEngineExprCG::replace_var_rt_expr(
                       rt_expr.args_[i], rt_expr.args_[i + 2 * real_param_num], &rt_expr, i + 2 * real_param_num))) {
          LOG_WARN("replace var rt expr failed", K(ret));
        }
      }
    }
    for (int i = 0; OB_SUCC(ret) && i < param_num; ++i) {
      if (OB_ISNULL(rt_expr.args_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child of expr is null", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      rt_expr.eval_func_ = ObExprBaseGreatest::calc_greatest;
    }
  }
  return ret;
}

// same type params
int ObExprBaseGreatest::calc_greatest(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  return ObExprBaseLeastGreatest::calc(expr, ctx, expr_datum, false);
}

}  // namespace sql
}  // namespace oceanbase
