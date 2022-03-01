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

// This file is for implementation of func json_pretty
#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_json_pretty.h"
#include "sql/engine/expr/ob_expr_json_func_helper.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprJsonPretty::ObExprJsonPretty(ObIAllocator &alloc) 
      :ObFuncExprOperator(alloc,
      T_FUN_SYS_JSON_PRETTY,
      N_JSON_PRETTY, 
      1, NOT_ROW_DIMENSION)
{
}

ObExprJsonPretty::~ObExprJsonPretty()
{
}

int ObExprJsonPretty::calc_result_type1(ObExprResType &type,
                                        ObExprResType &type1,
                                        common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx); // type_ctx session, collation, raw expr, maybe should use type_ctx in oracle mode to judge character set
  INIT_SUCC(ret);

  type.set_type(ObLongTextType);
  type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  type.set_collation_level(CS_LEVEL_IMPLICIT);

  if (OB_FAIL(ObJsonExprHelper::is_valid_for_json(type1, 1, N_JSON_PRETTY))) {
    LOG_WARN("wrong type for json doc.", K(ret), K(type1.get_type()));
  }

  return ret;
}

template <typename T>
int ObExprJsonPretty::calc(const T &data, ObObjType type, ObCollationType cs_type,
                           ObIAllocator *allocator, ObJsonBuffer &j_buf, bool &is_null)
{
  INIT_SUCC(ret);
  ObIJsonBase *j_base = NULL;
  ObJsonInType j_in_type = ObJsonExprHelper::get_json_internal_type(type);
  common::ObString j_str = data.get_string(); // json text or json binary

  if (OB_ISNULL(allocator)) { // check allocator
    ret = OB_NOT_INIT;
    LOG_WARN("allcator is null", K(ret));
  } else if (data.is_null() || type == ObNullType) {
    is_null = true;
  } else if (type != ObJsonType && !ob_is_string_type(type)) {
    ret = OB_ERR_INVALID_TYPE_FOR_JSON;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_JSON, 1, "json_pretty");
  } else if (OB_FAIL(ObJsonExprHelper::ensure_collation(type, cs_type))) {
    LOG_WARN("fail to ensure collation", K(ret), K(type), K(cs_type));
  } else if (OB_FAIL(ObJsonBaseFactory::get_json_base(allocator, j_str, j_in_type,
      j_in_type, j_base))) {
    if (ret == OB_ERR_INVALID_JSON_TEXT) {
      ret = OB_ERR_INVALID_JSON_TEXT_IN_PARAM;
    }
    LOG_WARN("fail to get json base", K(ret), K(type), K(j_str), K(j_in_type));
  } else if (OB_FAIL(j_base->print(j_buf, true, true))) {
    LOG_WARN("fail to print json", K(ret), K(type), K(j_str), K(j_in_type));
  }

  return ret;
}

// for old sql engine
int ObExprJsonPretty::calc_result1(common::ObObj &result, const common::ObObj &obj,
                                   common::ObExprCtx &expr_ctx) const
{
  INIT_SUCC(ret);
  ObIAllocator *allocator = expr_ctx.calc_buf_;
  ObObjType type = obj.get_type();
  ObCollationType cs_type = obj.get_collation_type();
  ObJsonBuffer j_buf(allocator);
  bool is_null = false;

  if (OB_FAIL(calc(obj, type, cs_type, allocator, j_buf, is_null))) {
    LOG_WARN("fail to calc json pretty result", K(ret), K(obj), K(type), K(cs_type));
  } else if (is_null) {
    result.set_null();
  } else {
    char *buf = static_cast<char*>(allocator->alloc(j_buf.length()));
    if (buf) {
      MEMMOVE(buf, j_buf.ptr(), j_buf.length());
      result.set_string(ObLongTextType, buf, j_buf.length());
      result.set_collation_type(result_type_.get_collation_type());
    } else {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory for result", K(ret), K(j_buf));
    }
  }

  return ret;
}

// for new sql engine
int ObExprJsonPretty::eval_json_pretty(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  common::ObArenaAllocator &tmp_allocator = ctx.get_reset_tmp_alloc();
  ObExpr *arg = expr.args_[0];
  ObDatum *j_datum = NULL;
  ObObjType type = arg->datum_meta_.type_;
  ObCollationType cs_type = arg->datum_meta_.cs_type_;
  ObJsonBuffer j_buf(&tmp_allocator);
  bool is_null = false;

  if (OB_FAIL(arg->eval(ctx, j_datum))) {
    ret = OB_ERR_INVALID_DATATYPE;
    LOG_WARN("error, eval json args datum failed", K(ret));
  } else if (OB_FAIL(calc(*j_datum, type, cs_type, &tmp_allocator, j_buf, is_null))) {
    LOG_WARN("fail to calc json pretty result", K(ret), K(j_datum), K(type), K(cs_type));
  } else if (is_null) {
    res.set_null();
  } else {
    char *buf = expr.get_str_res_mem(ctx, j_buf.length());
    if (buf) {
      MEMMOVE(buf, j_buf.ptr(), j_buf.length());
      res.set_string(buf, j_buf.length());        
    } else {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory for json pretty result", K(ret), K(j_buf));
    }
  }

  return ret;
}

int ObExprJsonPretty::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_json_pretty;
  return OB_SUCCESS;
}

}
}
