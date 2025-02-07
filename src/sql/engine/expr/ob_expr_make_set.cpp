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
#include "sql/engine/expr/ob_expr_make_set.h"
#include "sql/engine/expr/ob_expr_concat_ws.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

ObExprMakeSet::ObExprMakeSet(ObIAllocator &alloc)
: ObStringExprOperator(alloc, T_FUN_SYS_MAKE_SET, "make_set", MORE_THAN_ONE, VALID_FOR_GENERATED_COL)
{
  need_charset_convert_ = false;
}

ObExprMakeSet::~ObExprMakeSet()
{
}

static bool enable_return_longtext()
{
  const uint64_t min_cluster_version = GET_MIN_CLUSTER_VERSION();
  // [4.2.5.2, 4.3.0) || [4.3.5.1, )
  return (min_cluster_version >= CLUSTER_VERSION_4_3_5_1)
    || (MOCK_CLUSTER_VERSION_4_2_5_2 <= min_cluster_version && min_cluster_version < CLUSTER_VERSION_4_3_0_0);
}

int ObExprMakeSet::calc_result_typeN(ObExprResType &type,
                                     ObExprResType *types,
                                     int64_t param_num,
                                     ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if(OB_UNLIKELY(param_num <= 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument number, param should not less than 2", K(ret), K(param_num));
  } else {
    // set expected type of parameter
    ObLength max_len = 0;
    type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_STRING_INTEGER_TRUNC);
    types[0].set_calc_type(ObIntType);

    bool has_text = false;
    for (int64_t i = 1; !has_text && i < param_num; ++i) {
      if (ObTinyTextType != types[i].get_type() && types[i].is_lob()) {
        has_text = true;
      }
    }
    if (has_text && enable_return_longtext()) {
      type.set_type(ObLongTextType);
    } else {
      type.set_varchar();
    }
    for (int64_t i = 1; i < param_num; ++i) {
      types[i].set_calc_type(type.get_type());
      max_len += types[i].get_length();
    }
    // set expected type of results
    type.set_length(max_len);
    if OB_FAIL(aggregate_charsets_for_string_result(type, &types[1], param_num - 1, type_ctx)) {
      LOG_WARN("aggregate charset for string result failed", K(ret));
    } else {
      for (int64_t i = 1; i < param_num; i++) {
        types[i].set_calc_collation_type(type.get_collation_type());
        types[i].set_calc_collation_level(type.get_collation_level());
      }
    }
  }
  return ret;
}

int ObExprMakeSet::calc_make_set_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                        ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *input_bits_dat = NULL;
  if (OB_UNLIKELY(expr.arg_cnt_ < 2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(expr.arg_cnt_));
  } else if (OB_FAIL(expr.eval_param_value(ctx, input_bits_dat))) {
    LOG_WARN("eval param failed", K(ret));
  } else if (input_bits_dat->is_null()) {
    res.set_null();
  } else if (ob_is_text_tc(expr.datum_meta_.type_)) {
    if (OB_FAIL(calc_text(expr, ctx, *input_bits_dat, res))) {
      LOG_WARN("calc concat text ws failed", K(ret));
    }
  } else {
    // compute input bits representation
    uint64_t input_bits = static_cast<uint64_t>(input_bits_dat->get_int());
    if (expr.arg_cnt_ <= 64) {
      input_bits &= ((ulonglong) 1 << (expr.arg_cnt_ - 1)) - 1;
    }
    ObSEArray<ObString, 32> words;
    for (int64_t pos = 1, temp_input_bits = input_bits; OB_SUCC(ret) && temp_input_bits > 0;
         temp_input_bits >>= 1, ++pos) {
      const ObDatum &dat = expr.locate_param_datum(ctx, pos);
      if (((temp_input_bits & 1) > 0) && (!dat.is_null())) {
        if (OB_FAIL(words.push_back(dat.get_string()))) {
          LOG_WARN("push back word failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObString res_str;
      if (0 == words.count()) {
        res.set_string(res_str);
      } else {
        const ObString sep_str = ObCharsetUtils::get_const_str(expr.datum_meta_.cs_type_, ',');
        ObExprStrResAlloc res_alloc(expr, ctx);
        if (sep_str.empty()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get empty separator string", K(ret), K(expr.datum_meta_));
        } else if (OB_FAIL(ObExprConcatWs::calc(sep_str, words, res_alloc, res_str))) {
          LOG_WARN("calc concat ws failed", K(ret), K(sep_str));
        } else {
          res.set_string(res_str);
        }
      }
    }
  }
  return ret;
}

int ObExprMakeSet::calc_text(const ObExpr &expr, ObEvalCtx &ctx, const ObDatum &input_bits_dat, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  const ObString sep_str = ObCharsetUtils::get_const_str(expr.datum_meta_.cs_type_, ',');
  ObSEArray<ObExpr*, 32> words;

  // compute input bits representation
  uint64_t input_bits = static_cast<uint64_t>(input_bits_dat.get_int());
  if (expr.arg_cnt_ <= 64) {
    input_bits &= ((ulonglong) 1 << (expr.arg_cnt_ - 1)) - 1;
  }
  for (int64_t pos = 1, temp_input_bits = input_bits; OB_SUCC(ret) && temp_input_bits > 0;
        temp_input_bits >>= 1, ++pos) {
    const ObDatum &dat = expr.locate_param_datum(ctx, pos);
    if (((temp_input_bits & 1) > 0) && (!dat.is_null())) {
      if (OB_FAIL(words.push_back(expr.args_[pos]))) {
        LOG_WARN("push back word failed", K(ret), K(pos), K(input_bits), K(temp_input_bits));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObExprConcatWs::calc_text(expr, ctx, sep_str, words, temp_allocator, res))) {
    LOG_WARN("calc_text fail", K(ret), K(expr), K(words), K(sep_str));
  }
  return ret;
}

int ObExprMakeSet::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_make_set_expr;
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprMakeSet, raw_expr) {
  int ret = OB_SUCCESS;
  SET_LOCAL_SYSVAR_CAPACITY(1);
  EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_COLLATION_CONNECTION);
  return ret;
}

} /* namespace sql */
} /* namespace oceanbase */
