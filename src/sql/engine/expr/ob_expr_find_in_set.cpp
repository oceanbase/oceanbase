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

namespace oceanbase {
namespace sql {

ObExprFindInSet::ObExprFindInSet(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_FIND_IN_SET, "find_in_set", 2, NOT_ROW_DIMENSION)
{}

ObExprFindInSet::~ObExprFindInSet()
{}

int ObExprFindInSet::calc_result_type2(
    ObExprResType& type, ObExprResType& type1, ObExprResType& type2, ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  type1.set_calc_type(ObVarcharType);
  type1.set_calc_collation_type(ObCharset::get_system_collation());
  type2.set_calc_type(ObVarcharType);
  type2.set_calc_collation_type(ObCharset::get_system_collation());
  if (OB_LIKELY(NOT_ROW_DIMENSION == row_dimension_)) {
    type.set_uint64();
    type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObUInt64Type].precision_);
    type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObUInt64Type].scale_);
    ObExprOperator::calc_result_flag2(type, type1, type2);
  } else {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
  }
  return ret;
}

int search(const ObString& str, const ObString& str_list, const ObCollationType& cs_type, uint64_t& res_pos);
int ObExprFindInSet::calc_result2(ObObj& result, const ObObj& obj1, const ObObj& obj2, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("varchar buffer not init", K(ret));
  } else if (OB_UNLIKELY(obj1.is_null() || obj2.is_null())) {
    result.set_null();
  } else {
    TYPE_CHECK(obj1, ObVarcharType);
    TYPE_CHECK(obj2, ObVarcharType);
    const ObCollationType& cs_type = obj1.get_collation_type();
    uint64_t res_pos = 0;
    if (OB_UNLIKELY(obj1.get_collation_type() != obj2.get_collation_type() ||
                    !ObCharset::is_valid_collation(static_cast<int64_t>(cs_type)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid cs_type", K(ret), K(cs_type), K(obj1), K(obj2));
    } else if (OB_FAIL(search(obj1.get_string(), obj2.get_string(), cs_type, res_pos))) {
      LOG_WARN("search str in str list failed", K(ret), K(obj1), K(obj2));
    } else {
      result.set_uint64(res_pos);
    }
  }
  return ret;
}

int search(const ObString& str, const ObString& str_list, const ObCollationType& cs_type, uint64_t& res_pos)
{
  int ret = OB_SUCCESS;
  const char* first_ptr = str.ptr();
  int64_t first_length = str.length();
  // if first input string contains ',', return 0
  if (ObCharset::locate(cs_type, first_ptr, first_length, ",", 1, 1) != 0) {
    res_pos = 0;
  } else {
    bool is_found = false;
    res_pos = 1;
    uint32_t pre_separtor_pos = 0;
    uint32_t cur_separtor_pos = 0;
    uint32_t pre_sep_pos_byte = 0;
    uint32_t cur_sep_pos_byte = 0;
    const char* second_ptr = str_list.ptr();
    int64_t second_length = str_list.length();
    while ((!is_found) && (cur_separtor_pos = ObCharset::locate(
                               cs_type, second_ptr, second_length, ",", 1, cur_separtor_pos + 1)) != 0) {
      cur_sep_pos_byte = ObCharset::charpos(cs_type, second_ptr, second_length, cur_separtor_pos);
      if (ObCharset::strcmp(cs_type,
              first_ptr,
              first_length,
              second_ptr + pre_sep_pos_byte,
              cur_sep_pos_byte - pre_sep_pos_byte - 1) == 0) {
        is_found = true;
      } else {
        pre_separtor_pos = cur_separtor_pos;
        pre_sep_pos_byte = cur_sep_pos_byte;
        ++res_pos;
      }
      LOG_DEBUG("find_in_set debug",
          K(ret),
          K(pre_sep_pos_byte),
          K(cur_separtor_pos),
          K(pre_sep_pos_byte),
          K(cur_separtor_pos),
          K(is_found),
          K(res_pos));
    }
    if (!is_found) {
      // match the last substring extracted from strlist
      if (ObCharset::strcmp(
              cs_type, first_ptr, first_length, second_ptr + pre_sep_pos_byte, second_length - pre_sep_pos_byte) == 0) {
        // do nothing
      } else {
        res_pos = 0;
      }
    }
  }
  return ret;
}

int ObExprFindInSet::calc_find_in_set_expr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  // find_in_set(str, strlist)
  ObDatum* str = NULL;
  ObDatum* strlist = NULL;
  if (OB_UNLIKELY(2 != expr.arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected arg cnt", K(ret));
  } else if (OB_FAIL(expr.eval_param_value(ctx, str, strlist))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (str->is_null() || strlist->is_null()) {
    res_datum.set_null();
  } else {
    const ObCollationType& cs_type = expr.args_[0]->datum_meta_.cs_type_;
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

int ObExprFindInSet::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_find_in_set_expr;
  return ret;
}

} /* namespace sql */
} /* namespace oceanbase */
