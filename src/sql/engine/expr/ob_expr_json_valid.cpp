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

// This file contains implementation for json_valid.
#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_json_valid.h"
#include "sql/engine/expr/ob_expr_json_func_helper.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "share/object/ob_obj_cast.h"
#include "sql/parser/ob_item_type.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/json_type/ob_json_tree.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprJsonValid::ObExprJsonValid(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_JSON_VALID, N_JSON_VALID, 1, NOT_ROW_DIMENSION)
{
}

ObExprJsonValid::~ObExprJsonValid()
{
}

int ObExprJsonValid::calc_result_type1(ObExprResType &type,
                                       ObExprResType &type1,
                                       common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx); 
  INIT_SUCC(ret);

  // set result type to int32
  type.set_int32();
  type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].scale_);
  type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);

  ObObjType in_type = type1.get_type();
  if (in_type == ObNullType) {
  } else if (in_type == ObJsonType) {
    // do nothing
  } else if (ob_is_string_type(in_type)) {
    if (type1.get_collation_type() == CS_TYPE_BINARY) {
      type1.set_calc_collation_type(CS_TYPE_BINARY);
    } else {
      if (type1.get_charset_type() != CHARSET_UTF8MB4) {
        type1.set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
      }
    }
  } else {
    // other type is invalid, handle in ObExprJsonValid::eval_json_valid
  }
  
  return ret;
}

template <typename T>
int ObExprJsonValid::calc(const T &data, ObObjType type, ObCollationType cs_type,
                          ObIAllocator *allocator, T &res)
{
  INIT_SUCC(ret);
  bool is_null = false;
  bool is_empty_text = false;
  bool is_invalid = false;

  if (!ObJsonExprHelper::is_convertible_to_json(type)) {
    is_invalid = true;
  } else if (type == ObNullType || data.is_null()) {
    is_null = true;
  } else if (ob_is_string_type(type) && cs_type == CS_TYPE_BINARY) {
    is_invalid = true;
  } else {
    common::ObString j_str = data.get_string();
    if (OB_UNLIKELY(j_str == "")) {
      if (type == ObJsonType) {
        is_null = true;
      } else {
        is_invalid = true;
      }
    } else if (type == ObJsonType) { // json bin
      ObIJsonBase *j_bin = NULL;
      if (OB_FAIL(ObJsonBaseFactory::get_json_base(allocator, j_str, ObJsonInType::JSON_BIN,
          ObJsonInType::JSON_BIN, j_bin))) {
        LOG_WARN("fail to get json base", K(ret), K(type), K(j_str));
      }
    } else { // json tree
      if (OB_FAIL(ObJsonParser::check_json_syntax(j_str, allocator))) {
        LOG_WARN("fail to check json syntax", K(ret), K(type), K(j_str));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(is_null)) {
      res.set_null();
    } else {
      res.set_int(static_cast<int64_t>(!is_invalid));
    }
  } else if (ret == OB_ERR_INVALID_JSON_TEXT) {
    res.set_int(0);
    ret = OB_SUCCESS;
  } else if (ret == OB_ERR_JSON_OUT_OF_DEPTH) {
    LOG_USER_ERROR(OB_ERR_JSON_OUT_OF_DEPTH);
  }

  return ret;
}

// for old sql engine
int ObExprJsonValid::calc_result1(common::ObObj &result, const common::ObObj &obj,
                                  common::ObExprCtx &expr_ctx) const
{
  INIT_SUCC(ret);
  ObCollationType cs_type = obj.get_collation_type();
  ObObjType in_type =  input_types_.at(0).get_calc_type();

  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("varchar buffer not init", K(ret));
  } else if (OB_FAIL(calc(obj, in_type, cs_type, expr_ctx.calc_buf_, result))) {
    LOG_WARN("fail to calc json valid result", K(ret), K(in_type));
  }

  return ret;
}

// for new sql engine
int ObExprJsonValid::eval_json_valid(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  ObDatum *datum = NULL;
  ObExpr *arg = expr.args_[0];
  ObCollationType cs_type = arg->datum_meta_.cs_type_;

  if (OB_FAIL(arg->eval(ctx, datum))) {
    LOG_WARN("eval json arg failed", K(ret));
  } else {
    common::ObIAllocator &tmp_allocator = ctx.get_reset_tmp_alloc();
    if (OB_FAIL(calc(*datum, arg->datum_meta_.type_, cs_type, &tmp_allocator, res))) {
      LOG_WARN("fail to calc json valid result", K(ret), K(arg->datum_meta_.type_));
    }
  } 

  return ret;
}

int ObExprJsonValid::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                             ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_json_valid;
  return OB_SUCCESS;
}


}
}