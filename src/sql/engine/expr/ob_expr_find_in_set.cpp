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
#include "sql/ob_sql_utils.h"
#include "sql/engine/ob_exec_context.h"

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
      comma_pos = ObCharset::instrb(cs_type,
                                    str_list.ptr() + str_list_pos,
                                    str_list.length() - str_list_pos,
                                    comma_str.ptr(),
                                    comma_str.length());
      const char* elem_ptr = str_list.ptr() + str_list_pos;
      int64_t elem_length = (comma_pos >= 0) ? comma_pos : (str_list.length() - str_list_pos);
      if (0 != ObCharset::strcmp(cs_type, elem_ptr, elem_length, str.ptr(), str.length())) {
        //not match
        str_list_pos += elem_length + ((comma_pos >= 0) ? comma_str.length() : 0);
        elem_idx++;
      } else {
        break;
      }
    }

    if (str_list_pos < str_list.length()
        || (str_list_pos == str_list.length() && 0 == str.length() && comma_pos > 0)) {
      res_pos = elem_idx;
    } else {
      res_pos = 0;
    }

	}
	return ret;
}

int add_to_hashmap(ObExprFindIntCachedValue &cached_value, ObString &sort_key, int64_t elem_idx)
{
  int ret = OB_SUCCESS;
  uint64_t value = 0;
  if (OB_SUCC(cached_value.get_hashmap().get_refactored(sort_key, value))) {
    //do nothing
  } else if (OB_LIKELY(OB_HASH_NOT_EXIST == ret)) {
    ret = OB_SUCCESS;
    OZ (cached_value.get_hashmap().set_refactored(sort_key, elem_idx));
  } else {
    LOG_WARN("unexpected error", K(ret));
  }
  return ret;
}

int gen_sortkey(ObCollationType cs_type, ObIAllocator &allocator, ObString &elem, ObString &sort_key) {
  int ret = OB_SUCCESS;
  if (elem.empty()) {
    sort_key = ObString();
  } else if (ObCharset::is_bin_sort(cs_type)) {
    sort_key = elem;
  } else {
    bool is_valid_character = false;
    const ObCharsetInfo *cs = ObCharset::get_charset(cs_type);
    size_t buf_len = cs->coll->strnxfrmlen(cs, elem.length()) * cs->mbmaxlen;
    ObArrayWrap<char> buffer;

    OZ (buffer.allocate_array(allocator, buf_len));
    if (OB_SUCC(ret)) {
      size_t sort_key_len = ObCharset::sortkey(cs_type, elem.ptr(), elem.length(),
                                               buffer.get_data(), buf_len, is_valid_character);
      if (OB_UNLIKELY(!is_valid_character)) {
        ret = OB_ERR_INCORRECT_STRING_VALUE;
      } else {
        sort_key.assign_ptr(buffer.get_data(), sort_key_len);
      }
    }
  }
  return ret;
}

int build_hashmap(ObEvalCtx &ctx,
                  ObExprFindIntCachedValue &cached_value,
                  const ObString &str_list,
                  const ObCollationType &cs_type) {
  int ret = OB_SUCCESS;
  int64_t str_list_pos = 0;
  int64_t comma_pos = 0;
  ObString comma_str = ObCharsetUtils::get_const_str(cs_type, ',');
  ObMemAttr mem_attr(MTL_ID(), "HashMap");
  ObArray<ObString> sortkeys;

  OZ (sortkeys.reserve(16));

  while (OB_SUCC(ret) && str_list_pos < str_list.length()) {
    comma_pos = ObCharset::instrb(cs_type,
                                  str_list.ptr() + str_list_pos,
                                  str_list.length() - str_list_pos,
                                  comma_str.ptr(),
                                  comma_str.length());
    const char* elem_ptr = str_list.ptr() + str_list_pos;
    int64_t elem_length = (comma_pos >= 0) ? comma_pos : (str_list.length() - str_list_pos);
    ObString sort_key;
    ObString elem(elem_length, elem_ptr);
    OZ (gen_sortkey(cs_type, ctx.exec_ctx_.get_allocator(), elem, sort_key));
    OZ (sortkeys.push_back(sort_key));
    if (OB_SUCC(ret)) {
      str_list_pos += elem_length + ((comma_pos >= 0) ? comma_str.length() : 0);
    }
  }

  if (OB_SUCC(ret) && comma_pos > 0) {
    OZ (sortkeys.push_back(ObString()));
  }

  if (OB_UNLIKELY(OB_ERR_INCORRECT_STRING_VALUE == ret)) {
    ret = OB_SUCCESS;
    ObString temp_str;
    bool is_null = false;
    ObSQLUtils::check_well_formed_str(str_list, cs_type, temp_str, is_null, true); //generate warning
  }

  OZ (cached_value.get_hashmap().create(MAX(16, sortkeys.count() * 2), mem_attr, mem_attr));
  for (int i = 0; OB_SUCC(ret) && i < sortkeys.count(); i++) {
    OZ (add_to_hashmap(cached_value, sortkeys.at(i), i + 1));
  }


  return ret;
}

int search_with_const_set(const ObExpr &expr,
                          ObEvalCtx &ctx,
                          const ObString &str,
                          const ObString &str_list,
                          const ObCollationType &cs_type,
                          uint64_t &res_pos)
{
  int ret = OB_SUCCESS;
  const char* first_ptr = str.ptr();
  int64_t first_length = str.length();
  // if first input string contains ',', return 0
  if (ObCharset::locate(cs_type, first_ptr, first_length, ",", 1, 1) != 0) {
    res_pos = 0;
  } else {
    auto rt_ctx_id = static_cast<uint64_t>(expr.expr_ctx_id_);
    ObExprFindIntCachedValue *cached_value = NULL;
    if (NULL == (cached_value = static_cast<ObExprFindIntCachedValue *>
                 (ctx.exec_ctx_.get_expr_op_ctx(rt_ctx_id)))) {
      if (OB_FAIL(ctx.exec_ctx_.create_expr_op_ctx(rt_ctx_id, cached_value))) {
        LOG_WARN("failed to create operator ctx", K(ret));
      } else {
        OZ (build_hashmap(ctx, *cached_value, str_list, cs_type));
      }
    }
    if (OB_SUCC(ret)) {
      ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
      common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
      ObString sort_key;
      ObString input = str;
      if (OB_FAIL(gen_sortkey(cs_type, temp_allocator, input, sort_key))) {
        res_pos = 0;
        if (OB_ERR_INCORRECT_STRING_VALUE == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get sort key", K(ret));
        }
      } else if (OB_FAIL(cached_value->get_hashmap().get_refactored(sort_key, res_pos))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          res_pos = 0;
        } else {
          LOG_WARN("fail to get from hash map", K(ret));
        }
      }
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
    } else {
      if (expr.args_[1]->is_static_const_) {
        ret = search_with_const_set(expr, ctx, str->get_string(), strlist->get_string(), cs_type, res_pos);
      } else {
        ret = search(str->get_string(), strlist->get_string(), cs_type, res_pos);
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("search str in str list failed", K(ret), K(expr.args_[1]->is_static_const_));
      } else {
        res_datum.set_uint(res_pos);
      }
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
