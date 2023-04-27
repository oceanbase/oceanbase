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
#include "sql/engine/expr/ob_expr_find_in_set.h"
#include "lib/charset/ob_charset.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

ObExprFindInSet::ObExprFindInSet(ObIAllocator &alloc)
: ObFuncExprOperator(alloc, T_FUN_SYS_FIND_IN_SET, "find_in_set", 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprFindInSet::~ObExprFindInSet()
{
}

int ObExprFindInSet::calc_result_type2(ObExprResType &type,
																			 ObExprResType &type1,
																			 ObExprResType &type2,
																			 ObExprTypeCtx &type_ctx) const
{
	int ret = OB_SUCCESS;
	if (OB_LIKELY(NOT_ROW_DIMENSION == row_dimension_)) {
		type.set_uint64();
		type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObUInt64Type].precision_);
		type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObUInt64Type].scale_);
    type.set_calc_type(ObVarcharType);
		ObExprOperator::calc_result_flag2(type, type1, type2);
    ObObjMeta coll_types[2];
    coll_types[0] = type1.get_obj_meta();
    coll_types[1] = type2.get_obj_meta();
    if (OB_FAIL(aggregate_charsets_for_comparison(type.get_calc_meta(),
                 coll_types, 2, type_ctx.get_coll_type()))) {
      LOG_WARN("failed to aggregate_charsets_for_comparison", K(ret));
    } else {
      type1.set_calc_type(ObVarcharType);
      type1.set_calc_collation_type(type.get_calc_collation_type());
      type2.set_calc_type(ObVarcharType);
      type2.set_calc_collation_type(type.get_calc_collation_type());
    }
	} else {
		ret = OB_ERR_INVALID_TYPE_FOR_OP;
	}
	return ret;
}

int search(const ObString &str, const ObString &str_list, const ObCollationType &cs_type,
           uint64_t &res_pos)
{
  int ret = OB_SUCCESS;
  const char* first_ptr = str.ptr();
  int64_t first_length = str.length();
  // if first input string contains ',', return 0
  if (ObCharset::locate(cs_type, first_ptr, first_length, ",", 1, 1) != 0) {
    res_pos = 0;
  } else {
    int64_t str_list_pos = 0;
    int64_t comma_pos = 0;
    int64_t elem_idx = 1;

    ObString comma_str = ObCharsetUtils::get_const_str(cs_type, ',');

    while (str_list_pos < str_list.length()) {
      int64_t comma_pos = ObCharset::instrb(cs_type, str_list.ptr() + str_list_pos, str_list.length() - str_list_pos,
                                            comma_str.ptr(), comma_str.length());
      const char* elem_ptr = str_list.ptr() + str_list_pos;
      int64_t elem_length = (comma_pos >=0) ? comma_pos : str_list.length() - str_list_pos;
      if (0 != ObCharset::strcmp(cs_type, elem_ptr, elem_length, str.ptr(), str.length())) {
        //not match
        str_list_pos += elem_length + ((comma_pos >= 0) ? comma_str.length() : 0);
        elem_idx++;
      } else {
        break;
      }
    }

    if (str_list_pos < str_list.length()) {
      res_pos = elem_idx;
    } else {
      res_pos = 0;
    }

	}
	return ret;
}

int ObExprFindInSet::calc_find_in_set_expr(const ObExpr &expr, ObEvalCtx &ctx, 
                                           ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  // find_in_set(str, strlist)
  ObDatum *str = NULL;
  ObDatum *strlist = NULL;
  if (OB_UNLIKELY(2 != expr.arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected arg cnt", K(ret));
  } else if (OB_FAIL(expr.eval_param_value(ctx, str, strlist))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (str->is_null() || strlist->is_null()) {
    res_datum.set_null();
  } else {
    const ObCollationType &cs_type = expr.args_[0]->datum_meta_.cs_type_;
    uint64_t res_pos = 0;
    if (OB_UNLIKELY(expr.args_[0]->datum_meta_.cs_type_ != expr.args_[1]->datum_meta_.cs_type_ ||
                    !ObCharset::is_valid_collation(static_cast<int64_t>(cs_type)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid cs_type", K(ret), K(cs_type));
    } else if (OB_FAIL(search(str->get_string(), strlist->get_string(), cs_type, res_pos))) {
      LOG_WARN("search str in str list failed", K(ret));
    } else {
      res_datum.set_uint(res_pos);
    }
  }
  return ret;
}

int ObExprFindInSet::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_find_in_set_expr;
  return ret;
}

} /* namespace sql */
} /* namespace oceanbase */
