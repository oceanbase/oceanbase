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

#include "sql/engine/expr/ob_expr_split_part.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
#define SPLIT_PART_FUN_FORMAT_DISPATCH(START_PART_ARG_VEC_TYEP,                \
                                       END_PART_ARG_VEC_TYPE)                  \
  ret = is_oracle_mode                                                         \
            ? calc_split_part_expr_dispatch<true, START_PART_ARG_VEC_TYEP,     \
                                            END_PART_ARG_VEC_TYPE>(            \
                  expr, ctx, cs_type, skip, bound, eval_flags, res_vec,        \
                  str_vec, delimiter_vec,                                      \
                  static_cast<START_PART_ARG_VEC_TYEP *>(start_part_vec),      \
                  static_cast<END_PART_ARG_VEC_TYPE *>(end_part_vec))          \
            : calc_split_part_expr_dispatch<false, START_PART_ARG_VEC_TYEP,    \
                                            END_PART_ARG_VEC_TYPE>(            \
                  expr, ctx, cs_type, skip, bound, eval_flags, res_vec,        \
                  str_vec, delimiter_vec,                                      \
                  static_cast<START_PART_ARG_VEC_TYEP *>(start_part_vec),      \
                  static_cast<END_PART_ARG_VEC_TYPE *>(end_part_vec));

ObExprSplitPart::ObExprSplitPart(ObIAllocator &alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_SPLIT_PART, N_SPLIT_PART, MORE_THAN_TWO, VALID_FOR_GENERATED_COL)
{
  need_charset_convert_ = false;
}
ObExprSplitPart::~ObExprSplitPart()
{
}
int ObExprSplitPart::calc_result_typeN(ObExprResType &type,
                                      ObExprResType *types,
                                      int64_t param_num,
                                      ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (param_num != 3 && param_num != 4) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("the param number of split_part should be 2 or 4", K(ret), K(param_num));
  } else if (ObJsonType == types[0].get_type()) {
    ObString func_name("SPLIT_PART");
    ret = OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE;
    LOG_WARN("The first argument type is incorrect", K(ret), K(types[0].get_type()));
    LOG_USER_ERROR(OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE, func_name.length(), func_name.ptr());
  } else if (lib::is_mysql_mode()) {
    if (ObTextType == types[0].get_type()
        || ObMediumTextType == types[0].get_type()
        || ObLongTextType == types[0].get_type()) {
      type.set_type(types[0].get_type());
      type.set_length(types[0].get_length());
    } else {
      type.set_varchar();
    }
    if (OB_FAIL(aggregate_charsets_for_string_result(type, types, 1, type_ctx))) {
      LOG_WARN("aggregate_charsets_for_string_result failed", K(ret));
    } else {
      types[0].set_calc_meta(type);
      types[1].set_calc_type(ObVarcharType);
      for (int i = 0; OB_SUCC(ret) && i < 2; i++) {
        types[i].set_calc_collation_type(type.get_collation_type());
        types[i].set_calc_collation_level(type.get_collation_level());
      }
      types[2].set_calc_type(ObIntType);
      if (param_num == 4) {
        types[3].set_calc_type(ObIntType);
      }
    }
  } else {
    ObLengthSemantics length_semantic = type.get_length_semantics();
    ObSEArray<ObExprResType*, 1, ObNullAllocator> params;
    OZ (params.push_back(&types[0]));
    OZ (aggregate_string_type_and_charset_oracle(*type_ctx.get_session(), params, type));
    types[0].set_calc_meta(type);
    types[1].set_calc_type(ObVarcharType);
    for (int i = 0; OB_SUCC(ret) && i < 2; i++) {
      types[i].set_calc_collation_type(type.get_collation_type());
      types[i].set_calc_collation_level(type.get_collation_level());
      types[i].set_calc_length_semantics(length_semantic);
    }
    types[2].set_calc_type(ObIntType);
    if (param_num == 4) {
      types[3].set_calc_type(ObIntType);
    }
  }
  return ret;
}

int ObExprSplitPart::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_split_part_expr;
  rt_expr.eval_vector_func_ = calc_split_part_expr_vec;
  return ret;
}

int ObExprSplitPart::calc_split_part_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                        ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *delimiter_datum = NULL;
  ObDatum *str_datum = NULL;
  ObDatum *start_part_datum = NULL;
  ObDatum *end_part_datum = NULL;
  bool is_oracle_mode = lib::is_oracle_mode();
  if (OB_UNLIKELY(3 != expr.arg_cnt_ && 4 != expr.arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg cnt", K(ret), K(expr.arg_cnt_));
  } else if (4 == expr.arg_cnt_ &&
             OB_FAIL(expr.eval_param_value(ctx, str_datum, delimiter_datum,
                                           start_part_datum, end_part_datum))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (3 == expr.arg_cnt_) {
    if (OB_FAIL(expr.eval_param_value(ctx, str_datum, delimiter_datum, start_part_datum))) {
      LOG_WARN("eval arg failed", K(ret));
    } else {
      end_part_datum = start_part_datum;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (str_datum->is_null() ||
             delimiter_datum->is_null() ||
             start_part_datum->is_null() ||
             end_part_datum->is_null()) {
    res.set_null();
  } else {
    int64_t start_part = start_part_datum->get_int() == 0 ? 1 : start_part_datum->get_int();
    int64_t end_part = 3 == expr.arg_cnt_ ? start_part : end_part_datum->get_int();
    const ObString delimiter = delimiter_datum->get_string();
    ObCollationType calc_cs_type = expr.args_[0]->datum_meta_.cs_type_;
    if (!ob_is_text_tc(expr.args_[0]->datum_meta_.type_)) {
      const ObString &str = str_datum->get_string();
      bool null_res = false;
      ObString res_str;
      ret = is_oracle_mode
                      ? calc_split_part<true>(calc_cs_type, str, delimiter,
                                              start_part, end_part, null_res, res_str)
                      : calc_split_part<false>(calc_cs_type, str, delimiter,
                                                start_part, end_part, null_res, res_str);
      if (OB_FAIL(ret)) {
        LOG_WARN("clac split part fialed", K(ret));
      } else if (is_oracle_mode && null_res) {
        res.set_null();
      } else {
        res.set_string(res_str);
      }
    } else {
      ObEvalCtx::TempAllocGuard alloc_guard(ctx);
      ObIAllocator &tmp_alloc = alloc_guard.get_allocator();
      ObString str;
      ObTextStringDatumResult output_result(expr.datum_meta_.type_, &expr, &ctx, &res);
      if (OB_FAIL(ObTextStringHelper::get_string(expr, tmp_alloc, 0, str_datum, str))) {
        LOG_WARN("get full text string failed ", K(ret));
      } else {
        bool null_res = false;
        ObString res_str;
        ret = is_oracle_mode
                      ? calc_split_part<true>(calc_cs_type, str, delimiter,
                                              start_part, end_part, null_res, res_str)
                      : calc_split_part<false>(calc_cs_type, str, delimiter,
                                                start_part, end_part, null_res, res_str);
        if (OB_FAIL(ret)) {
          LOG_WARN("clac split part fialed", K(ret));
        } else if (is_oracle_mode && null_res) {
          output_result.set_result_null();
        } else {
          if (OB_FAIL(output_result.init(res_str.length()))) {
            LOG_WARN("init TextString result failed", K(ret));
          } else {
            output_result.append(res_str);
            output_result.set_result();
          }
        }
      }
    }
  }
  return ret;
}

template <bool is_oracle_mode, typename StartPartVecType, typename EndPartVecType>
int ObExprSplitPart::calc_split_part_expr_dispatch(const ObExpr &expr,
                                                    ObEvalCtx &ctx,
                                                    ObCollationType cs_type,
                                                    const ObBitVector &skip,
                                                    const EvalBound &bound,
                                                    ObBitVector &eval_flags,
                                                    ObIVector *res_vec,
                                                    ObIVector *str_vec,
                                                    ObIVector *delimiter_vec,
                                                    StartPartVecType *start_part_vec,
                                                    EndPartVecType *end_part_vec)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard alloc_guard(ctx);
  ObIAllocator &tmp_alloc = alloc_guard.get_allocator();
  for (int64_t i = bound.start(); OB_SUCC(ret) && i < bound.end(); i++) {
    if (skip.at(i) || eval_flags.at(i)) {
      continue;
    } else if (str_vec->is_null(i) ||
             delimiter_vec->is_null(i) ||
             start_part_vec->is_null(i) ||
             end_part_vec->is_null(i)) {
      res_vec->set_null(i);
    } else {
      int64_t start_part = start_part_vec->get_int(i) == 0 ? 1 : start_part_vec->get_int(i);
      int64_t end_part = 3 == expr.arg_cnt_ ? start_part : end_part_vec->get_int(i);
      if (!ob_is_text_tc(expr.args_[0]->datum_meta_.type_)) {
        bool null_res = false;
        ObString res_str;
        if (OB_FAIL(calc_split_part<is_oracle_mode>(cs_type, str_vec->get_string(i),
                                    delimiter_vec->get_string(i),
                                    start_part,
                                    end_part,
                                    null_res,
                                    res_str))) {
          LOG_WARN("clac split part fialed", K(ret));
        } else if (is_oracle_mode && null_res) {
          res_vec->set_null(i);
        } else {
          res_vec->set_string(i, res_str);
        }
      } else {
        ObString str;
        ObTextStringDatumResult output_result(expr.datum_meta_.type_, &expr, &ctx, res_vec, i);
        if (OB_FAIL(ObTextStringHelper::get_string<ObVectorBase>(
                expr, tmp_alloc, 0, i, static_cast<ObVectorBase *>(str_vec),
                str))) {
          LOG_WARN("get full text string failed ", K(ret));
        } else {
          bool null_res = false;
          ObString res_str;
          if (OB_FAIL(calc_split_part<is_oracle_mode>(cs_type, str,
                                      delimiter_vec->get_string(i),
                                      start_part,
                                      end_part,
                                      null_res,
                                      res_str))) {
            LOG_WARN("clac split part fialed", K(ret));
          } else if (is_oracle_mode && null_res) {
            output_result.set_result_null();
          } else {
            if (OB_FAIL(output_result.init_with_batch_idx(res_str.length(), i))) {
              LOG_WARN("init TextString result failed", K(ret));
            } else {
              output_result.append(res_str);
              output_result.set_result();
            }
          }
        }
      }
    }
    eval_flags.set(i);
  }
  return ret;
}

int ObExprSplitPart::calc_split_part_expr_vec(const ObExpr &expr,
                                              ObEvalCtx &ctx,
                                              const ObBitVector &skip,
                                              const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = lib::is_oracle_mode();
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("calc 1th arg failed", K(ret), K(bound));
  } else if (OB_FAIL(expr.args_[1]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("calc 2th arg failed", K(ret), K(bound));
  } else if (OB_FAIL(expr.args_[2]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("calc 3th arg failed", K(ret), K(bound));
  } else if (4 == expr.arg_cnt_ && OB_FAIL(expr.args_[3]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("calc 4th arg failed", K(ret), K(bound));
  } else {
    ObCollationType cs_type = expr.args_[0]->datum_meta_.cs_type_;
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    ObIVector *res_vec = expr.get_vector(ctx);
    ObIVector *str_vec = expr.args_[0]->get_vector(ctx);
    ObIVector *delimiter_vec = expr.args_[1]->get_vector(ctx);
    ObIVector *start_part_vec = expr.args_[2]->get_vector(ctx);
    ObIVector *end_part_vec = 4 == expr.arg_cnt_ ? expr.args_[3]->get_vector(ctx) : start_part_vec;
    VectorFormat start_idx_format = expr.args_[2]->get_format(ctx);
    VectorFormat end_idx_format = 4 == expr.arg_cnt_ ? expr.args_[3]->get_format(ctx) : start_idx_format;
    if (start_idx_format == VEC_FIXED && end_idx_format == VEC_FIXED) {
      SPLIT_PART_FUN_FORMAT_DISPATCH(
          ObFixedLengthFormat<RTCType<VEC_TC_INTEGER>>,
          ObFixedLengthFormat<RTCType<VEC_TC_INTEGER>>);
    } else if (start_idx_format == VEC_FIXED &&
               end_idx_format == VEC_UNIFORM_CONST) {
      SPLIT_PART_FUN_FORMAT_DISPATCH(
          ObFixedLengthFormat<RTCType<VEC_TC_INTEGER>>, ObUniformFormat<true>);
    } else if (start_idx_format == VEC_FIXED && end_idx_format == VEC_UNIFORM) {
      SPLIT_PART_FUN_FORMAT_DISPATCH(
          ObFixedLengthFormat<RTCType<VEC_TC_INTEGER>>, ObUniformFormat<false>);
    } else if (start_idx_format == VEC_UNIFORM_CONST &&
               end_idx_format == VEC_FIXED) {
      SPLIT_PART_FUN_FORMAT_DISPATCH(
          ObUniformFormat<true>, ObFixedLengthFormat<RTCType<VEC_TC_INTEGER>>);
    } else if (start_idx_format == VEC_UNIFORM_CONST &&
               end_idx_format == VEC_UNIFORM_CONST) {
      SPLIT_PART_FUN_FORMAT_DISPATCH(ObUniformFormat<true>,
                                     ObUniformFormat<true>);
    } else if (start_idx_format == VEC_UNIFORM_CONST &&
               end_idx_format == VEC_UNIFORM) {
      SPLIT_PART_FUN_FORMAT_DISPATCH(ObUniformFormat<true>,
                                     ObUniformFormat<false>);
    } else if (start_idx_format == VEC_UNIFORM && end_idx_format == VEC_FIXED) {
      SPLIT_PART_FUN_FORMAT_DISPATCH(
          ObUniformFormat<false>, ObFixedLengthFormat<RTCType<VEC_TC_INTEGER>>);
    } else if (start_idx_format == VEC_UNIFORM &&
               end_idx_format == VEC_UNIFORM_CONST) {
      SPLIT_PART_FUN_FORMAT_DISPATCH(ObUniformFormat<false>,
                                     ObUniformFormat<true>);
    } else if (start_idx_format == VEC_UNIFORM &&
               end_idx_format == VEC_UNIFORM) {
      SPLIT_PART_FUN_FORMAT_DISPATCH(ObUniformFormat<false>,
                                     ObUniformFormat<false>);
    } else {
      SPLIT_PART_FUN_FORMAT_DISPATCH(ObVectorBase, ObVectorBase);
    }
  }
  return ret;
}
DEF_SET_LOCAL_SESSION_VARS(ObExprSplitPart, raw_expr) {
  int ret = OB_SUCCESS;
  SET_LOCAL_SYSVAR_CAPACITY(1);
  EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_COLLATION_CONNECTION);
  return ret;
}

}
}
