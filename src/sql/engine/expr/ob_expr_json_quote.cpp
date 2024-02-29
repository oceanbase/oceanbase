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
 * This file is for implementation of func json_quote
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_json_quote.h"
#include "sql/engine/expr/ob_expr_json_func_helper.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprJsonQuote::ObExprJsonQuote(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc,
      T_FUN_SYS_JSON_QUOTE,
      N_JSON_QUOTE, 
      1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprJsonQuote::~ObExprJsonQuote()
{
}

int ObExprJsonQuote::calc_result_type1(ObExprResType &type,
                                       ObExprResType &type1,
                                       common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  INIT_SUCC(ret);
  type.set_type(ObLongTextType);
  type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  type.set_collation_level(CS_LEVEL_IMPLICIT);
  type.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY[ObLongTextType]);

  if (type1.get_type() == ObNullType || type1.get_type() == ObDoubleType
      || type1.get_type() == ObIntType) {
    // do nothing
  } else if (ob_is_string_type(type1.get_type()) && type1.get_collation_type() != CS_TYPE_BINARY) {
    if (type1.get_charset_type() != CHARSET_UTF8MB4) {
      type1.set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    }
  }
  
  return ret;
}


int ObExprJsonQuote::calc(ObEvalCtx &ctx, ObIAllocator &temp_allocator, const ObDatum &data,
                          ObDatumMeta meta, bool has_lob_header, ObJsonBuffer &j_buf, bool &is_null)
{
  INIT_SUCC(ret);
  ObObjType type = meta.type_;
  ObCollationType cs_type = meta.cs_type_;
  if (type == ObIntType || type == ObDoubleType) {
    // special for mathematical function, consistent with mysql
    if (data.is_null()) {
      is_null = true;
    } else {
      ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
      LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_ARGUMENT);
    }
  } else if (type == ObNullType || data.is_null()) {
    is_null = true;
  } else if (!ob_is_string_type(type)) {
    LOG_WARN("invalid type", K(type));
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_ARGUMENT);
  } else if (OB_FAIL(ObJsonExprHelper::ensure_collation(type, cs_type))) {
    LOG_WARN("fail to ensure collation", K(ret), K(type), K(cs_type));
  } else { // string type
    ObString json_val = data.get_string();
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, data, meta, has_lob_header, json_val))) {
      LOG_WARN("fail to get real data.", K(ret), K(json_val));
    } else if (json_val.length() == 0) {
      if (OB_FAIL(j_buf.append("\"\"", 2))) {
        LOG_WARN("failed: jbuf append", K(ret));        
      }
    } else if (OB_FAIL(ObJsonPathUtil::double_quote(json_val, &j_buf))) {
      LOG_WARN("failed: add double quote", K(ret), K(json_val));
    }
  }

  return ret;
}

int ObExprJsonQuote::eval_json_quote(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  ObJsonBuffer j_buf(&temp_allocator);
  ObExpr *arg = expr.args_[0];
  ObDatum* json_datum = NULL;
  bool is_null = false;

  if (OB_FAIL(arg->eval(ctx, json_datum))) {
    LOG_WARN("failed: eval json args datum.", K(ret));
  } else if (OB_FAIL(calc(ctx, temp_allocator, *json_datum, arg->datum_meta_,
                          arg->obj_meta_.has_lob_header(), j_buf, is_null))) {
    LOG_WARN("fail to calc json quote result in new engine", K(ret), K(arg->datum_meta_));
  } else if (is_null) {
    res.set_null();
  } else if (OB_FAIL(ObJsonExprHelper::pack_json_str_res(expr, ctx, res, j_buf))) {
    LOG_WARN("fail to pack json result", K(ret));
  }

  return ret;
}

int ObExprJsonQuote::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                             ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_json_quote;
  return OB_SUCCESS;
}

}
}
