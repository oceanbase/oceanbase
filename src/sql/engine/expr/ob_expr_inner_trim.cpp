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
#include "sql/engine/expr/ob_expr_inner_trim.h"
#include "sql/engine/expr/ob_expr_trim.h"
#include <string.h>
#include "share/object/ob_obj_cast.h"
#include "sql/parser/ob_item_type.h"
#include "sql/session/ob_sql_session_info.h"
//#include "sql/engine/expr/ob_expr_promotion_util.h"

namespace oceanbase {
using namespace common;
namespace sql {

ObExprInnerTrim::ObExprInnerTrim(ObIAllocator& alloc) : ObStringExprOperator(alloc, T_FUN_INNER_TRIM, N_INNER_TRIM, 3)
{
  need_charset_convert_ = false;
}

ObExprInnerTrim::~ObExprInnerTrim()
{}

int ObExprInnerTrim::calc_result3(
    ObObj& result, const ObObj& trim_type, const ObObj& trim_pattern, const ObObj& text, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("varchar buffer not init", K(ret), K(expr_ctx.calc_buf_));
  } else if (OB_UNLIKELY(trim_type.is_null()) || OB_UNLIKELY(trim_pattern.is_null()) || OB_UNLIKELY(text.is_null())) {
    result.set_null();
  } else {
    TYPE_CHECK(trim_type, ObIntType);
    TYPE_CHECK(trim_pattern, ObVarcharType);
    TYPE_CHECK(text, ObVarcharType);
    int64_t trim_type_val = trim_type.get_int();
    ObString pattern_val = trim_pattern.get_string();
    ObString text_val = text.get_string();
    ret = ObExprTrim::calc(result, trim_type_val, pattern_val, text_val, get_result_type().get_type(), expr_ctx);
    if (OB_LIKELY(OB_SUCCESS == ret && !result.is_null())) {
      if (share::is_oracle_mode()) {
        if (OB_NOT_NULL(expr_ctx.my_session_)) {
          result.set_collation_type(expr_ctx.my_session_->get_nls_collation());
        }
        if (OB_FAIL(convert_result_collation(result_type_, result, expr_ctx.calc_buf_))) {
          LOG_WARN("fail to convert result collation", K(ret));
        }
      }
      result.set_collation(result_type_);
    }
  }
  return ret;
}

inline int ObExprInnerTrim::calc_result_type3(ObExprResType& type, ObExprResType& trim_type,
    ObExprResType& trim_pattern, ObExprResType& text, common::ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  CK(NULL != type_ctx.get_session());
  // old engine, keep old logic.
  if (OB_SUCC(ret) && !type_ctx.get_session()->use_static_typing_engine()) {
    // via text.calc_type_ to tell framework convert text to varchar
    // in trim, text must be varchar; otherwise, frameworks will automatically convert it
    bool has_nstring = text.is_nstring() || trim_pattern.is_nstring();
    text.set_calc_type(ObVarcharType);
    trim_pattern.set_calc_type(ObVarcharType);
    trim_type.set_calc_type(ObIntType);

    if (has_nstring) {
      type.set_nvarchar2();
    } else {
      type.set_varchar();
    }
    type.set_length(text.get_length());

    ObSEArray<ObExprResType, 2> types;
    if (OB_FAIL(types.push_back(text))) {
      LOG_WARN("push_back failed", K(ret), K(text));
    } else if (OB_FAIL(types.push_back(trim_pattern))) {
      LOG_WARN("push_back failed", K(ret), K(trim_pattern));
    } else {
      // note that we pass text as the first item
      if (OB_FAIL(
              aggregate_charsets_for_string_result_with_comparison(type, &types.at(0), 2, type_ctx.get_coll_type()))) {
        LOG_WARN("aggregate charset fro string failed", K(ret));
      } else {
        text.set_calc_collation_type(type.get_collation_type());
        trim_pattern.set_calc_collation_type(type.get_collation_type());
      }
    }
  }

  // static typing engine
  if (OB_SUCC(ret) && type_ctx.get_session()->use_static_typing_engine()) {
    // %trim_type, %trim_pattern, %text are adjacent elements in the array
    CK(&trim_type + 1 == &trim_pattern);
    CK(&trim_type + 2 == &text);
    OZ(ObExprTrim::deduce_result_type(type, &trim_type, 3, type_ctx));
  }
  LOG_DEBUG("inner trim", K(type), K(text), K(trim_pattern));

  return ret;
}

int ObExprInnerTrim::cg_expr(ObExprCGCtx&, const ObRawExpr&, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  CK(3 == rt_expr.arg_cnt_);
  // inner trim seems has no difference with trim, set the trim evaluate function directly.
  rt_expr.eval_func_ = &ObExprTrim::eval_trim;
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
