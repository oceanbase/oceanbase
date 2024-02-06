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
 * This file is for implement of func json_unquote
 */

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
      N_JSON_UNQUOTE, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
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

int ObExprJsonUnquote::calc(ObEvalCtx &ctx, const ObDatum &data, ObDatumMeta meta, bool has_lob_header,
                            ObIAllocator *allocator, ObJsonBuffer &j_buf, bool &is_null)
{
  INIT_SUCC(ret);
  ObIJsonBase *j_base = NULL;
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
  } else if (type != ObJsonType && !ob_is_string_type(type)) {
    LOG_WARN("invalid type", K(type));
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_ARGUMENT);
  } else if (OB_FAIL(ObJsonExprHelper::ensure_collation(type, cs_type))) {
    LOG_WARN("fail to ensure collation", K(ret), K(type), K(cs_type));
  } else {
    ObIJsonBase *j_base = NULL;
    ObString j_str = data.get_string();
    ObJsonInType j_in_type = ObJsonExprHelper::get_json_internal_type(type);
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(*allocator, data, meta, has_lob_header, j_str))) {
      LOG_WARN("fail to get real data.", K(ret), K(j_str));
    } else if (ob_is_string_type(type) && (j_str.length() < 2 || j_str[0] != '"' || j_str[j_str.length() - 1] != '"')) {
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

int ObExprJsonUnquote::eval_json_unquote(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  ObExpr *arg = expr.args_[0];
  ObDatum* json_datum = NULL;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  ObJsonBuffer j_buf(&temp_allocator);
  bool is_null = false;

  if (OB_FAIL(arg->eval(ctx, json_datum))) {
    ret = OB_ERR_INVALID_DATATYPE;
    LOG_WARN("error, eval json args datum failed", K(ret));
  } else if (OB_FAIL(calc(ctx, *json_datum, arg->datum_meta_, arg->obj_meta_.has_lob_header(), &temp_allocator, j_buf, is_null))) {
    LOG_WARN("fail to calc json unquote result in new engine", K(ret), K(arg->datum_meta_));
  } else if (is_null) {
    res.set_null();
  } else if (OB_FAIL(ObJsonExprHelper::pack_json_str_res(expr, ctx, res, j_buf))) {
    LOG_WARN("fail to pack json result", K(ret));
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
