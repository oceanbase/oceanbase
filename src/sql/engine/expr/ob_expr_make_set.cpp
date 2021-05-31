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

namespace oceanbase {
namespace sql {

ObExprMakeSet::ObExprMakeSet(ObIAllocator& alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_MAKE_SET, "make_set", MORE_THAN_ONE)
{
  need_charset_convert_ = false;
}

ObExprMakeSet::~ObExprMakeSet()
{}

int ObExprMakeSet::calc_result_typeN(
    ObExprResType& type, ObExprResType* types, int64_t param_num, ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num <= 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument number, param should not less than 2", K(ret), K(param_num));
  } else {
    // set expected type of parameter
    ObLength max_len = 0;
    types[0].set_calc_type(ObIntType);
    for (int64_t i = 1; i < param_num; ++i) {
      types[i].set_calc_type(ObVarcharType);
      max_len += types[i].get_length();
    }
    // set expected type of results
    type.set_varchar();
    type.set_length(max_len);
    if OB_FAIL (aggregate_charsets_for_string_result(type, &types[1], param_num - 1, type_ctx.get_coll_type())) {
      LOG_WARN("aggregate charset for string result failed", K(ret));
    } else {
      for (int64_t i = 1; i < param_num; i++) {
        types[i].set_calc_collation_type(type.get_collation_type());
      }
    }
  }
  return ret;
}

int ObExprMakeSet::calc_resultN(ObObj& result, const ObObj* objs, int64_t param_num, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  const ObString sep_str = ObCharsetUtils::get_const_str(result_type_.get_collation_type(), ',');
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("varchar buffer not init", K(ret));
  } else if (OB_ISNULL(objs) || OB_UNLIKELY(param_num < 2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(param_num), K(objs));
  } else if (sep_str.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get empty separator string", K(ret), K(result_type_));
  } else if (objs[0].is_null()) {
    result.set_null();
  } else {
    // compute input bits representation
    TYPE_CHECK(objs[0], ObIntType);
    uint64_t input_bits = static_cast<uint64_t>(objs[0].get_int());
    if (param_num <= 64) {
      input_bits &= ((ulonglong)1 << (param_num - 1)) - 1;
    }
    // compute number of valid input string, including separator
    int valid_input_num = 1;
    for (int64_t pos = 1, temp_input_bits = input_bits; temp_input_bits > 0; temp_input_bits >>= 1, ++pos) {
      if (((temp_input_bits & 1) > 0) && (!objs[pos].is_null())) {
        ++valid_input_num;
      }
    }
    // compute result
    int64_t alloc_size = valid_input_num * sizeof(ObObj);
    ObObj* valid_input = static_cast<ObObj*>(expr_ctx.calc_buf_->alloc(alloc_size));
    if (valid_input == NULL) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc make_set memory failed", K(alloc_size));
    } else {
      valid_input[0].set_varchar(sep_str);  // first input is separator
      int cur_pos = 1;
      for (int64_t pos = 1, temp_input_bits = input_bits; temp_input_bits > 0; temp_input_bits >>= 1, ++pos) {
        if (((temp_input_bits & 1) > 0) && (!objs[pos].is_null())) {
          valid_input[cur_pos] = objs[pos];
          ++cur_pos;
        }
      }
      if (OB_FAIL(ObExprConcatWs::calc(result, sep_str, valid_input, valid_input_num, expr_ctx))) {
        LOG_WARN("fail to calc function make_set", K(ret), K(result));
      } else if (!result.is_null()) {
        result.set_collation(result_type_);
      } else { /*do nothing.*/
      }
    }
  }
  return ret;
}

int ObExprMakeSet::calc_make_set_expr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res)
{
  int ret = OB_SUCCESS;
  ObDatum* input_bits_dat = NULL;
  if (OB_UNLIKELY(expr.arg_cnt_ < 2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(expr.arg_cnt_));
  } else if (OB_FAIL(expr.eval_param_value(ctx, input_bits_dat))) {
    LOG_WARN("eval param failed", K(ret));
  } else if (input_bits_dat->is_null()) {
    res.set_null();
  } else {
    // compute input bits representation
    uint64_t input_bits = static_cast<uint64_t>(input_bits_dat->get_int());
    if (expr.arg_cnt_ <= 64) {
      input_bits &= ((ulonglong)1 << (expr.arg_cnt_ - 1)) - 1;
    }
    ObIAllocator& calc_alloc = ctx.get_reset_tmp_alloc();
    ObSEArray<ObString, 32, ObIAllocator> words(OB_MALLOC_MIDDLE_BLOCK_SIZE, calc_alloc);
    for (int64_t pos = 1, temp_input_bits = input_bits; OB_SUCC(ret) && temp_input_bits > 0;
         temp_input_bits >>= 1, ++pos) {
      const ObDatum& dat = expr.locate_param_datum(ctx, pos);
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

int ObExprMakeSet::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_make_set_expr;
  return ret;
}

} /* namespace sql */
} /* namespace oceanbase */
