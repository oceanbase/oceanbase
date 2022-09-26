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

// This file is for implement of func json_unquote
#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_json_unquote.h"
#include "sql/engine/expr/ob_expr_json_func_helper.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql 
{

ObExprJsonUnquote::ObExprJsonUnquote(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc,T_FUN_SYS_JSON_UNQUOTE,
      N_JSON_UNQUOTE, 1, NOT_ROW_DIMENSION)
{
}

ObExprJsonUnquote::~ObExprJsonUnquote()
{
}

int ObExprJsonUnquote::calc_result_type1(ObExprResType &type,
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
  } else if (type1.get_type() == ObJsonType) {
    type1.set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
  } else if (ob_is_string_type(type1.get_type()) && type1.get_collation_type() != CS_TYPE_BINARY) {
    if (type1.get_charset_type() != CHARSET_UTF8MB4) {
      type1.set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    }
  }

  return ret;
}

template <typename T>
int ObExprJsonUnquote::calc(const T &data, ObObjType type, ObCollationType cs_type,
                            ObIAllocator *allocator, ObJsonBuffer &j_buf, bool &is_null)
{
  INIT_SUCC(ret);
  ObIJsonBase *j_base = NULL;

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
  } else if (type != ObJsonType && !ob_is_string_type(type)) {
    LOG_WARN("invalid type", K(type));
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_ARGUMENT);
  } else if (OB_FAIL(ObJsonExprHelper::ensure_collation(type, cs_type))) {
    LOG_WARN("fail to ensure collation", K(ret), K(type), K(cs_type));
  } else {
    ObIJsonBase *j_base = NULL;
    ObString j_str = data.get_string();
    size_t len = j_str.length();
    ObJsonInType j_in_type = ObJsonExprHelper::get_json_internal_type(type);
    if (ob_is_string_type(type) && (len < 2 || j_str[0] != '"' || j_str[len - 1] != '"')) {
      if (OB_FAIL(j_buf.append(j_str))) {
        LOG_WARN("failed: copy original string", K(ret), K(j_str));
      }
    } else if (OB_FAIL(ObJsonBaseFactory::get_json_base(allocator, j_str, j_in_type,
        j_in_type, j_base))) {
      LOG_WARN("failed: get json base", K(ret), K(type));
      if (OB_ERR_INVALID_JSON_TEXT) {
        ret = OB_ERR_INVALID_JSON_TEXT_IN_PARAM;
      }
      if (ret == OB_ERR_INVALID_JSON_TEXT_IN_PARAM) {
        LOG_USER_ERROR(OB_ERR_INVALID_JSON_TEXT_IN_PARAM);
      }
    } else if (OB_FAIL(j_base->print(j_buf, false))) {
      LOG_WARN("failed: print json string", K(ret));
    }
  }

  return ret;
}

int ObExprJsonUnquote::calc_result1(common::ObObj &result, const common::ObObj &obj,
                                    common::ObExprCtx &expr_ctx) const
{
  INIT_SUCC(ret);
  ObIAllocator *allocator = expr_ctx.calc_buf_;
  ObJsonBuffer j_buf(allocator);
  bool is_null = false; 

  if (result_type_.get_collation_type() != CS_TYPE_UTF8MB4_BIN) {
    ret = OB_ERR_INVALID_JSON_CHARSET;
    LOG_WARN("invalid out put charset", K(ret), K(result_type_));
  } else if (OB_ISNULL(allocator)) { // check allocator
    ret = OB_NOT_INIT;
    LOG_WARN("allcator not init", K(ret));
  } else if (OB_FAIL(calc(obj, obj.get_type(), obj.get_collation_type(), allocator, j_buf, is_null))) {
    LOG_WARN("fail to calc json unquote result in old engine", K(ret), K(obj.get_type()));
  } else if (is_null) {
    result.set_null();
  } else {
    // allocate length() + 1; as for alloc(0) will return null result
    char *buf = static_cast<char*>(allocator->alloc(j_buf.length() + 1));
    if (buf) {
      MEMCPY(buf, j_buf.ptr(), j_buf.length());
      result.set_collation_type(result_type_.get_collation_type());
      result.set_string(ObLongTextType, buf, j_buf.length());
    } else {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory for json unquote result", K(ret), K(j_buf.length()));
    }
  }
  return ret;
}

int ObExprJsonUnquote::eval_json_unquote(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  ObExpr *arg = expr.args_[0];
  ObDatum* json_datum = NULL;
  common::ObArenaAllocator &temp_allocator = ctx.get_reset_tmp_alloc();
  ObObjType val_type = arg->datum_meta_.type_;
  ObCollationType cs_type = arg->datum_meta_.cs_type_;
  ObJsonBuffer j_buf(&temp_allocator);
  bool is_null = false;

  if (OB_FAIL(arg->eval(ctx, json_datum))) {
    ret = OB_ERR_INVALID_DATATYPE;
    LOG_WARN("error, eval json args datum failed", K(ret));
  } else if (OB_FAIL(calc(*json_datum, val_type, cs_type, &temp_allocator, j_buf, is_null))) {
    LOG_WARN("fail to calc json unquote result in new engine", K(ret), K(val_type));
  } else if (is_null) {
    res.set_null();
  } else {
    char *buf = expr.get_str_res_mem(ctx, j_buf.length());
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory for json unquote result", K(ret), K(j_buf.length()));
    } else {
      MEMCPY(buf, j_buf.ptr(), j_buf.length());
      res.set_string(buf, j_buf.length());
    }
  }
  return ret;
}

int ObExprJsonUnquote::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                               ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_json_unquote;
  return OB_SUCCESS;
}

}
}
