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

// This file is for implementation of func json_quote
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
      1, NOT_ROW_DIMENSION)
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
  type.set_length(OB_MAX_LONGTEXT_LENGTH);

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


template <typename T>
int ObExprJsonQuote::calc(const T &data, ObObjType type, ObCollationType cs_type, 
                          ObJsonBuffer &j_buf, bool &is_null)
{
  INIT_SUCC(ret);

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
    size_t len = json_val.length();
    if (len == 0) {
      if (OB_FAIL(j_buf.append("\"\"", 2))) {
        LOG_WARN("failed: jbuf append", K(ret));        
      }
    } else if (OB_FAIL(ObJsonPathUtil::double_quote(json_val, &j_buf))) {
      LOG_WARN("failed: add double quote", K(ret), K(json_val));
    }
  }

  return ret;
}

// for old sql engine
int ObExprJsonQuote::calc_result1(common::ObObj &result, const common::ObObj &obj,
                                  common::ObExprCtx &expr_ctx) const
{ 
  INIT_SUCC(ret);
  ObIAllocator *allocator = expr_ctx.calc_buf_;
  ObJsonBuffer j_buf(allocator);
  bool is_null= false;

  if (OB_ISNULL(allocator)) { // check allocator
    ret = OB_NOT_INIT;
    LOG_WARN("allcator not init", K(ret));
  } else if (OB_FAIL(calc(obj, obj.get_type(), obj.get_collation_type(), j_buf, is_null))) {
    LOG_WARN("fail to calc json quote result in old engine", K(ret), K(obj.get_type()));
  } else if (is_null) {
    result.set_null();
  } else {
    char *buf = static_cast<char*>(allocator->alloc(j_buf.length()));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory for json quote result", K(ret), K(j_buf.length()));
    } else {
      MEMCPY(buf, j_buf.ptr(), j_buf.length());
      result.set_collation_type(result_type_.get_collation_type());
      result.set_string(ObLongTextType, buf, j_buf.length());
    }
  }

  return ret;
}

int ObExprJsonQuote::eval_json_quote(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  common::ObArenaAllocator &temp_allocator = ctx.get_reset_tmp_alloc();
  ObJsonBuffer j_buf(&temp_allocator);
  ObExpr *arg = expr.args_[0];
  ObObjType type = arg->datum_meta_.type_;
  ObCollationType cs_type = arg->datum_meta_.cs_type_;
  ObDatum* json_datum = NULL;
  bool is_null = false;

  if (OB_FAIL(arg->eval(ctx, json_datum))) {
    LOG_WARN("failed: eval json args datum.", K(ret));
  } else if (OB_FAIL(calc(*json_datum, type, cs_type, j_buf, is_null))) {
    LOG_WARN("fail to calc json quote result in new engine", K(ret), K(type));
  } else if (is_null) {
    res.set_null();
  } else {
    char *buf = expr.get_str_res_mem(ctx, j_buf.length());
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory for json quote result", K(ret), K(j_buf.length()));
    } else {
      MEMCPY(buf, j_buf.ptr(), j_buf.length());
      res.set_string(buf, j_buf.length());
    }
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
