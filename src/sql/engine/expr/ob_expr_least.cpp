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

#include "sql/engine/expr/ob_expr_least.h"
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

ObExprBaseLeastGreatest::ObExprBaseLeastGreatest(
    ObIAllocator& alloc, ObExprOperatorType type, const char* name, int32_t param_num)
    : ObMinMaxExprOperator(alloc, type, name, param_num, NOT_ROW_DIMENSION)
{}
ObExprBaseLeastGreatest::~ObExprBaseLeastGreatest()
{}

int ObExprBaseLeastGreatest::calc_result_typeN_oracle(
    ObExprResType& type, ObExprResType* types, int64_t param_num, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo* session = static_cast<const ObSQLSessionInfo*>(type_ctx.get_session());
  ObExprOperator::calc_result_flagN(type, types, param_num);
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null");
  } else if (OB_ISNULL(types) || OB_UNLIKELY(param_num < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("types is null or param_num is wrong", K(types), K(param_num), K(ret));
  } else {
    if (OB_ISNULL(types) || OB_UNLIKELY(param_num < 1)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("types is null or param_num is wrong", K(types), K(param_num), K(ret));
    } else {
      ObExprResType& first_type = types[0];
      type = first_type;
      if (ObIntTC == first_type.get_type_class() || ObUIntTC == first_type.get_type_class() ||
          ObNumberTC == first_type.get_type_class()) {
        type.set_type(ObNumberType);
        type.set_calc_type(ObNumberType);
        type.set_scale(ORA_NUMBER_SCALE_UNKNOWN_YET);
        type.set_precision(PRECISION_UNKNOWN_YET);
      } else if (ObLongTextType == type.get_type()) {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_WARN("lob type parameter not expected", K(ret));
      } else {
        type.set_type(first_type.get_type());
        type.set_calc_type(first_type.get_type());
      }

      if (ObStringTC == type.get_type_class()) {
        int64_t max_length = 0;
        int64_t all_char = 0;
        for (int64_t i = 0; OB_SUCC(ret) && i < param_num; i++) {
          int64_t item_length = 0;
          if (ObStringTC == types[i].get_type_class() || ObLongTextType == types[i].get_type()) {
            item_length = types[i].get_length();
            if (LS_CHAR == types[i].get_length_semantics()) {
              item_length = item_length * 4;
              all_char++;
            }
          } else if (ObNumberTC == types[i].get_type_class() || ObIntTC == types[i].get_type_class() ||
                     ObUIntTC == types[i].get_type_class()) {
            item_length = number::ObNumber::MAX_PRECISION - number::ObNumber::MIN_SCALE;
          } else if (ObOTimestampTC == types[i].get_type_class() || ObFloatTC == types[i].get_type_class() ||
                     ObDoubleTC == types[i].get_type_class() || ObNullTC == types[i].get_type_class()) {
            item_length = 40;
          } else if (ObDateTimeTC == types[i].get_type_class()) {
            item_length = 19;
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("unsupported type", K(ret), K(types[i]), K(types[i].get_type_class()));
          }

          if (OB_SUCC(ret)) {
            max_length = MAX(max_length, item_length);
          }
        }
        if (OB_SUCC(ret)) {
          if (all_char == param_num) {
            type.set_length(static_cast<ObLength>(max_length / 4));
            type.set_length_semantics(LS_CHAR);
          } else {
            type.set_length(static_cast<ObLength>(max_length));
          }
        }
      }
    }
  }
  if (ObNullType != types[0].get_type()) {
    for (int64_t i = 1; OB_SUCC(ret) && i < param_num; i++) {
      OZ(ObObjCaster::can_cast_in_oracle_mode(types[0].get_type_class(), types[i].get_type_class()));
    }
  }

  if (OB_SUCC(ret) && session->use_static_typing_engine()) {
    for (int i = 0; i < param_num; i++) {
      types[i].set_calc_meta(type.get_calc_meta());
    }
  }
  return ret;
}

int ObExprBaseLeastGreatest::calc_result_typeN_mysql(
    ObExprResType& type, ObExprResType* types, int64_t param_num, common::ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo* session = static_cast<const ObSQLSessionInfo*>(type_ctx.get_session());
  bool use_static_engine = session->use_static_typing_engine();
  if (use_static_engine && param_num % 3 != 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument count", K(ret), K(param_num));
  } else if (!use_static_engine) {
    ObExprOperator::calc_result_flagN(type, types, param_num);
    const ObLengthSemantics default_length_semantics =
        (OB_NOT_NULL(type_ctx.get_session()) ? type_ctx.get_session()->get_actual_nls_length_semantics() : LS_BYTE);
    if (OB_FAIL(calc_result_meta_for_comparison(
            type, types, param_num, type_ctx.get_coll_type(), default_length_semantics))) {
      LOG_WARN("calc result meta for comparison failed");
    } else {
      for (int64_t i = 0; i < param_num; i++) {
        types[i].set_calc_meta(type.get_calc_meta());
      }
    }
  } else {
    int64_t real_param_num = param_num / 3;
    ObExprOperator::calc_result_flagN(type, types, real_param_num);
    // don't cast parameter is all parameters are IntTC or UIntTC.
    bool all_integer = true;
    bool big_int_result = false;
    for (int i = 0; i < real_param_num && all_integer; ++i) {
      ObObjType type = types[i].get_type();
      if (!ob_is_integer_type(type)) {
        all_integer = false;
      } else if (ObIntType == type || ObUInt64Type == type || ObUInt32Type == type) {
        big_int_result = true;
      }
    }
    if (all_integer) {
      if (big_int_result) {
        type.set_type(ObIntType);
      } else {
        type.set_type(ObInt32Type);
      }
    } else {
      const ObLengthSemantics default_length_semantics =
          (OB_NOT_NULL(type_ctx.get_session()) ? type_ctx.get_session()->get_actual_nls_length_semantics() : LS_BYTE);
      if (OB_FAIL(calc_result_meta_for_comparison(
              type, types, real_param_num, type_ctx.get_coll_type(), default_length_semantics))) {
        LOG_WARN("calc result meta for comparison failed");
      }
    }
    if (!all_integer) {
      // compatible with MySQL. compare type and result type may be different.
      // resolver makes two copies of parameters. First for comparison and second for output result.
      for (int64_t i = 0; i < real_param_num; i++) {
        types[i + real_param_num].set_calc_meta(type.get_calc_meta());
      }
      for (int64_t i = 0; i < real_param_num; i++) {
        types[i + real_param_num * 2].set_calc_meta(type.get_obj_meta());
      }
    }
  }
  return ret;
}

int ObExprBaseLeastGreatest::calc(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum, bool least)
{
  int ret = OB_SUCCESS;
  uint32_t param_num = expr.arg_cnt_;
  bool has_null = false;
  int64_t cmp_param_start = 0;
  int64_t cmp_param_end = param_num - 1;
  int64_t cmp_res_offset = 0;
  if (!lib::is_oracle_mode()) {
    cmp_param_start = param_num / 3;
    cmp_param_end = cmp_param_start * 2 - 1;
    cmp_res_offset = cmp_param_start;
  }
  // evaluate parameters if need. no need to continue to evaluate if null value found.
  // create table t(c1 int, c2 varchar(10));
  // insert into t values(null, 'a');
  // select least(c2, c1) from t; mysql reports warning, oracle reports an error
  // select least(c1, c2) from t; mysql reports no warning, oracle output null.
  for (int i = cmp_param_start; OB_SUCC(ret) && !has_null && i <= cmp_param_end; ++i) {
    ObDatum* tmp_datum = NULL;
    if (OB_FAIL(expr.args_[i]->eval(ctx, tmp_datum))) {
      LOG_WARN("eval param value failed", K(ret), K(i));
    } else {
      if (tmp_datum->is_null()) {
        has_null = true;
        expr_datum.set_null();
      }
    }
  }

  if (!has_null && OB_SUCC(ret)) {
    bool all_integer = !lib::is_oracle_mode();
    for (int i = cmp_param_start; all_integer && i <= cmp_param_end; i++) {
      if (!ob_is_integer_type(expr.args_[i]->datum_meta_.type_)) {
        all_integer = false;
      }
    }
    // compare all params.
    if (all_integer) {
      int64_t minmax_int = expr.locate_param_datum(ctx, cmp_param_start).get_int();
      for (int i = cmp_param_start + 1; i <= cmp_param_end; ++i) {
        int64_t cur_int = expr.locate_param_datum(ctx, i).get_int();
        if ((!least && minmax_int < cur_int) || (least && minmax_int > cur_int)) {
          minmax_int = cur_int;
        }
      }
      expr_datum.set_int(minmax_int);
    } else {
      int res_idx = cmp_param_start;
      ObDatum* minmax_param = static_cast<ObDatum*>(&expr.locate_param_datum(ctx, res_idx));
      for (int i = cmp_param_start + 1; OB_SUCC(ret) && i <= cmp_param_end; ++i) {
        CK(expr.args_[res_idx]->datum_meta_.type_ == expr.args_[i]->datum_meta_.type_);
        ObDatumCmpFuncType cmp_func = expr.args_[res_idx]->basic_funcs_->null_first_cmp_;
        ObDatum* cur_param = static_cast<ObDatum*>(&expr.locate_param_datum(ctx, i));
        int cmp_res = cmp_func(*minmax_param, *cur_param);
        if ((!least && cmp_res < 0) || (least && cmp_res > 0)) {
          res_idx = i;
          minmax_param = static_cast<ObDatum*>(&expr.locate_param_datum(ctx, res_idx));
        }
      }
      // ok, we got the least / greatest param.
      if (OB_SUCC(ret)) {
        res_idx += cmp_res_offset;
        const ObObjType result_type = expr.datum_meta_.type_;
        const ObCollationType result_cs_type = expr.datum_meta_.cs_type_;
        const ObObjType param_type = expr.args_[res_idx]->datum_meta_.type_;
        const ObCollationType param_cs_type = expr.args_[res_idx]->datum_meta_.cs_type_;
        ObDatum* res_datum = nullptr;
        bool result_string_type = ob_is_string_or_enumset_type(result_type) || ob_is_text_tc(result_type);
        if (OB_LIKELY(result_type == param_type && (!result_string_type || result_cs_type == param_cs_type))) {
          // collation type is compared in string comparision
          if (OB_FAIL(expr.args_[res_idx]->eval(ctx, res_datum))) {
            LOG_WARN("eval param value failed", K(ret), K(res_idx));
          } else {
            ObDatum& dst_datum = static_cast<ObDatum&>(expr_datum);
            dst_datum = *res_datum;
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected result type", K(result_type), K(param_type), K(result_cs_type), K(param_cs_type));
        }
      }
    }
  }
  return ret;
}

ObExprBaseLeast::ObExprBaseLeast(ObIAllocator& alloc, int32_t param_num, ObExprOperatorType type /* T_FUN_SYS_LEAST */,
    const char* name /* N_LEAST*/)
    : ObExprBaseLeastGreatest(alloc, type, name, param_num)
{}

ObExprBaseLeast::~ObExprBaseLeast()
{}

int ObExprBaseLeast::calc_result_typeN(
    ObExprResType& type, ObExprResType* types_stack, int64_t param_num, common::ObExprTypeCtx& type_ctx) const
{
  UNUSED(type);
  UNUSED(types_stack);
  UNUSED(param_num);
  UNUSED(type_ctx);

  return OB_SUCCESS;
}

int ObExprBaseLeast::calc_resultN(ObObj& result, const ObObj* objs_stack, int64_t param_num, ObExprCtx& expr_ctx) const
{
  return ObMinMaxExprOperator::calc_(result, objs_stack, param_num, result_type_, expr_ctx, CO_LT, need_cast_);
}

int ObExprBaseLeast::calc(
    ObObj& result, const ObObj* objs_stack, int64_t param_num, const ObExprResType& expected_type, ObExprCtx& expr_ctx)
{
  return ObMinMaxExprOperator::calc_(result, objs_stack, param_num, expected_type, expr_ctx, CO_LT, true);
}

ObExprLeastMySQL::ObExprLeastMySQL(ObIAllocator& alloc) : ObExprBaseLeast(alloc, MORE_THAN_ONE)
{}

ObExprLeastMySQL::~ObExprLeastMySQL()
{}

int ObExprLeastMySQL::calc_result_typeN(
    ObExprResType& type, ObExprResType* types, int64_t param_num, ObExprTypeCtx& type_ctx) const
{
  return calc_result_typeN_mysql(type, types, param_num, type_ctx);
}

ObExprLeastMySQLInner::ObExprLeastMySQLInner(ObIAllocator& alloc)
    : ObExprBaseLeast(alloc, MORE_THAN_ONE, T_FUN_SYS_LEAST_INNER, "")
{}

ObExprLeastMySQLInner::~ObExprLeastMySQLInner()
{}

int ObExprLeastMySQLInner::calc_result_typeN(
    ObExprResType& type, ObExprResType* types, int64_t param_num, ObExprTypeCtx& type_ctx) const
{
  return calc_result_typeN_mysql(type, types, param_num, type_ctx);
}

ObExprOracleLeast::ObExprOracleLeast(ObIAllocator& alloc) : ObExprBaseLeast(alloc, MORE_THAN_ZERO)
{}

ObExprOracleLeast::~ObExprOracleLeast()
{}

/**
 * Oracle, result type of greatest and least is same as first parameter.
 */
int ObExprOracleLeast::calc_result_typeN(
    ObExprResType& type, ObExprResType* types, int64_t param_num, ObExprTypeCtx& type_ctx) const
{
  return calc_result_typeN_oracle(type, types, param_num, type_ctx);
}

int ObExprBaseLeast::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  bool all_same_type = true;
  const uint32_t param_num = rt_expr.arg_cnt_;
  bool is_oracle_mode = lib::is_oracle_mode();
  if (OB_UNLIKELY(is_oracle_mode && param_num < 1) ||
      OB_UNLIKELY(!is_oracle_mode && (param_num < 2 || param_num % 3 != 0)) || OB_ISNULL(rt_expr.args_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("args_ is null or too few arguments", K(ret), K(rt_expr.args_), K(param_num));
  } else {
    if (!is_oracle_mode) {
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
      } else if (OB_ISNULL(rt_expr.args_[i]->basic_funcs_) ||
                 OB_ISNULL(rt_expr.args_[i]->basic_funcs_->null_first_cmp_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("basic func of or cmp func is null", K(ret), K(rt_expr.args_[i]->basic_funcs_));
      }
    }
    if (OB_SUCC(ret)) {
      rt_expr.eval_func_ = ObExprBaseLeast::calc_least;
    }
  }
  return ret;
}

// same type params
int ObExprBaseLeast::calc_least(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  return ObExprBaseLeastGreatest::calc(expr, ctx, expr_datum, true);
}

}  // namespace sql
}  // namespace oceanbase
