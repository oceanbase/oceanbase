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
#include "sql/engine/expr/ob_expr_instrb.h"
#include "sql/engine/expr/ob_expr_instr.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "lib/oblog/ob_log.h"
#include "share/object/ob_obj_cast.h"
#include "sql/parser/ob_item_type.h"
#include "sql/session/ob_sql_session_info.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
namespace oceanbase {
namespace sql {

ObExprInstrb::ObExprInstrb(ObIAllocator& alloc)
    : ObLocationExprOperator(alloc, T_FUN_SYS_INSTRB, N_INSTRB, MORE_THAN_ONE, NOT_ROW_DIMENSION)
{}

ObExprInstrb::~ObExprInstrb()
{}

int ObExprInstrb::calc_result_type2(
    ObExprResType& type, ObExprResType& type1, ObExprResType& type2, common::ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo* session = type_ctx.get_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else {
    ObExprOperator::calc_result_flag2(type, type1, type2);
    type.set_number();
    type.set_precision(PRECISION_UNKNOWN_YET);
    type.set_scale(NUMBER_SCALE_UNKNOWN_YET);

    ObSEArray<ObExprResType*, 2, ObNullAllocator> params;
    OZ(params.push_back(&type1));
    ObExprResType tmp_type;
    OZ(aggregate_string_type_and_charset_oracle(*session, params, tmp_type));
    OZ(params.push_back(&type2));
    OZ(deduce_string_param_calc_type_and_charset(*session, tmp_type, params));
  }
  return ret;
}

int ObExprInstrb::calc_result_typeN(
    ObExprResType& type, ObExprResType* type_array, int64_t param_num, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num < 2 || param_num > 4)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("param num is invalid", K(ret), K(param_num));
  } else if (OB_ISNULL(type_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type array is null", K(ret));
  } else if (OB_FAIL(calc_result_type2(type, type_array[0], type_array[1], type_ctx))) {
    LOG_WARN("fail calc result type", K(param_num), K(ret));
  } else {
    if (3 == param_num) {
      type_array[2].set_calc_type(ObNumberType);  // position
    } else if (4 == param_num) {
      type_array[2].set_calc_type(ObNumberType);  // position
      type_array[3].set_calc_type(ObNumberType);  // occurrence
    }
  }
  return ret;
}

int ObExprInstrb::calc_resultN(
    ObObj& result, const common::ObObj* param_array, int64_t param_num, common::ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param_array is null", K(ret));
  } else if (OB_UNLIKELY(param_num < 2 || param_num > 4)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid param count", K(param_num), K(ret));
  } else if (param_array[0].is_null() || param_array[1].is_null()) {
    result.set_null();
  } else if (param_array[0].is_clob() && (0 == param_array[0].get_string().length())) {
    result.set_lob_value(ObLongTextType, param_array[0].get_string().ptr(), param_array[0].get_string().length());
    result.set_meta_type(result_type_);
  } else if (2 == param_num) {
    ObObj position(static_cast<int64_t>(1));
    ObObj occurrence(static_cast<int64_t>(1));
    ret = calc(result, param_array[0], param_array[1], position, occurrence, expr_ctx);
  } else if (3 == param_num) {
    ObObj occurrence(static_cast<int64_t>(1));
    ret = calc(result, param_array[0], param_array[1], param_array[2], occurrence, expr_ctx);
  } else {
    ret = calc(result, param_array[0], param_array[1], param_array[2], param_array[3], expr_ctx);
  }
  return ret;
}

int ObExprInstrb::calc(ObObj& result, const ObObj& haystack, const ObObj& needle, const ObObj& position,
    const ObObj& occurrence, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  int64_t pos_val = -1;
  int64_t occ_val = -1;
  int64_t ret_idx = -1;
  number::ObNumber ret_num;

  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("expr_ctx.calc_buf_ is null", K(ret));
  } else if (OB_UNLIKELY(haystack.is_null_oracle() || needle.is_null_oracle() || position.is_null_oracle() ||
                         occurrence.is_null_oracle())) {
    result.set_null();
  } else if (OB_UNLIKELY(!is_type_valid(haystack.get_type()) || !is_type_valid(needle.get_type()) ||
                         !is_type_valid(position.get_type()) || !is_type_valid(occurrence.get_type()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param is not castable", K(haystack), K(needle), K(position), K(occurrence), K(ret));
  } else if (OB_FAIL(ObExprUtil::get_trunc_int64(position, expr_ctx, pos_val))) {
    LOG_WARN("get position failed", K(position), K(ret));
  } else if (OB_FAIL(ObExprUtil::get_trunc_int64(occurrence, expr_ctx, occ_val))) {
    LOG_WARN("get occurrence failed", K(occurrence), K(ret));
  } else if (OB_UNLIKELY(0 >= occ_val)) {
    ret = OB_ERR_ARGUMENT_OUT_OF_RANGE;
    LOG_USER_ERROR(OB_ERR_ARGUMENT_OUT_OF_RANGE, occ_val);
  } else if (OB_UNLIKELY(0 == pos_val)) {
    ret_num.set_zero();
    result.set_number(ret_num);
  } else {
    const ObString& haystack_str = haystack.get_string();
    const ObString& needle_str = needle.get_string();

    if (OB_SUCC(ret)) {
      if (OB_FAIL(search(haystack_str, needle_str, pos_val, occ_val, ret_idx))) {
        LOG_WARN("search needle in haystack failed", K(ret), K(haystack_str), K(needle_str));
      } else if (OB_FAIL(ret_num.from(ret_idx, *(expr_ctx.calc_buf_)))) {
        LOG_WARN("int64_t to ObNumber failed", K(ret), K(ret_idx));
      } else {
        result.set_number(ret_num);
      }
    }
  }
  return ret;
}

int ObExprInstrb::search(const ObString& haystack, const ObString& needle, int64_t start, int64_t occ, int64_t& ret_idx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(haystack.ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", K(haystack));
  } else if (OB_ISNULL(needle.ptr())) {
    ret = OB_CLOB_ONLY_SUPPORT_WITH_MULTIBYTE_FUN;
    LOG_WARN("ptr is null", K(needle));
  } else if (haystack.length() < needle.length()) {
    ret_idx = 0;
  } else {
    if (0 < start) {
      start = start - 1;
      if (start > haystack.length() || start + needle.length() > haystack.length()) {
        ret_idx = 0;
      } else if (OB_FAIL(ObExprUtil::kmp(
                     needle.ptr(), needle.length(), haystack.ptr() + start, haystack.length() - start, occ, ret_idx))) {
        LOG_WARN("ObExprInstrb kmp failed", K(occ), K(start), K(needle), K(haystack));
      } else if (-1 < ret_idx) {
        ret_idx = ret_idx + 1 + start;
      } else {
        ret_idx = 0;
      }
    } else {  // start < 0
      occ = -occ;
      int64_t compare_len = min(haystack.length(), haystack.length() + start + needle.length());
      if ((-start) > haystack.length()) {
        ret_idx = 0;
      } else {
        if (OB_FAIL(
                ObExprUtil::kmp_reverse(needle.ptr(), needle.length(), haystack.ptr(), compare_len, occ, ret_idx))) {
          LOG_WARN("ObExprInstrb kmp_reverse failed", K(occ), K(compare_len), K(needle), K(haystack));
        } else if (-1 < ret_idx) {
          ret_idx = ret_idx + 1;
        } else {
          ret_idx = 0;
        }
      }
    }
  }
  return ret;
}

int calc_instrb_expr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  bool is_null = false;
  ObDatum* haystack = NULL;
  ObDatum* needle = NULL;
  int64_t pos_int = 0;
  int64_t occ_int = 0;
  number::ObNumber res_nmb;
  ObCollationType calc_cs_type = CS_TYPE_INVALID;
  if (OB_FAIL(ObExprOracleInstr::calc_oracle_instr_arg(
          expr, ctx, is_null, haystack, needle, pos_int, occ_int, calc_cs_type))) {
    LOG_WARN("calc_instrb_arg failed", K(ret));
  } else if (is_null) {
    res_datum.set_null();
  } else if (0 == haystack->get_string().length()) {
    number::ObNumber res_nmb;
    ObNumStackOnceAlloc tmp_alloc;
    if (OB_FAIL(res_nmb.from((uint64_t)(0), tmp_alloc))) {
      LOG_WARN("get number from int failed", K(ret));
    } else {
      res_datum.set_number(res_nmb);
    }
  } else if (OB_ISNULL(haystack) || OB_ISNULL(needle)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("haystack or needle is NULL", K(ret), KP(haystack), KP(needle));
  } else if (0 >= occ_int) {
    ret = OB_ERR_ARGUMENT_OUT_OF_RANGE;
    LOG_USER_ERROR(OB_ERR_ARGUMENT_OUT_OF_RANGE, occ_int);
  } else if (OB_UNLIKELY(0 == pos_int)) {
    res_nmb.set_zero();
    res_datum.set_number(res_nmb);
  } else {
    const ObString& haystack_str = haystack->get_string();
    const ObString& needle_str = needle->get_string();
    int64_t ret_idx = -1;
    if (OB_SUCC(ret)) {
      ObNumStackOnceAlloc tmp_alloc;
      if (OB_FAIL(ObExprInstrb::search(haystack_str, needle_str, pos_int, occ_int, ret_idx))) {
        LOG_WARN("search needle in haystack failed", K(ret), K(haystack_str), K(needle_str));
      } else if (OB_FAIL(res_nmb.from(ret_idx, tmp_alloc))) {
        LOG_WARN("int64_t to ObNumber failed", K(ret), K(ret_idx));
      } else {
        res_datum.set_number(res_nmb);
      }
    }
  }
  return ret;
}

int ObExprInstrb::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_instrb_expr;
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
