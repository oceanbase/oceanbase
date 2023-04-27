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
 * This file contains implementation for json_valid.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_is_json.h"
#include "sql/engine/expr/ob_expr_json_func_helper.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "share/object/ob_obj_cast.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "objit/common/ob_item_type.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/json_type/ob_json_tree.h"
#include "lib/hash/ob_hashset.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprIsJson::ObExprIsJson(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_IS_JSON, N_IS_JSON, MORE_THAN_ONE, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprIsJson::~ObExprIsJson()
{
}

int ObExprIsJson::calc_result_typeN(ObExprResType& type,
                                    ObExprResType* types_stack,
                                    int64_t param_num,
                                    ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  INIT_SUCC(ret);
  if (OB_UNLIKELY(param_num != 5)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid argument number", K(ret), K(param_num));
  } else {
    // set result type to int32
    type.set_int32();
    type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].scale_);
    type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);

    ObObjType in_type = types_stack[0].get_type();
    if (in_type == ObNullType) {
    } else if (in_type == ObJsonType) {
      // do nothing
    } else if (ob_is_string_type(in_type)) {
      if (types_stack[0].get_collation_type() == CS_TYPE_BINARY) {
        types_stack[0].set_calc_collation_type(CS_TYPE_BINARY);
      } else {
        if (types_stack[0].get_charset_type() != CHARSET_UTF8MB4) {
          types_stack[0].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
        }
      }
    }
  }

  for (uint32_t i = 1; i < param_num && OB_SUCC(ret); i++) {
    if (types_stack[i].get_type() != ObIntType &&
        types_stack[i].get_type() != ObNumberType) {
      ret = OB_ERR_INVALID_TYPE_FOR_JSON;
      LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_JSON, i + 1, N_IS_JSON);
    }
  }

  return ret;
}

int ObExprIsJson::check_is_json(const ObExpr &expr, ObEvalCtx &ctx,
                                const ObDatum &data, ObObjType type,
                                ObCollationType cs_type, ObArenaAllocator &allocator,
                                uint8_t strict_opt, uint8_t scalar_opt, uint8_t unique_opt,
                                bool check_for_is_json, ObDatum &res)
{
  INIT_SUCC(ret);

  bool is_null = false;
  bool is_invalid = false;
  bool is_scalar = true;
  ObExpr *json_arg = expr.args_[0];

  if (type == ObNullType || data.is_null()) {
    is_null = true;
  } else {
    common::ObString j_str;
    if (OB_FAIL(ObJsonExprHelper::get_json_or_str_data(json_arg, ctx, allocator, j_str, is_null))) {
      LOG_WARN("fail to get real data.", K(ret), K(j_str));
    } else if (is_null) {
    } else if (OB_UNLIKELY(j_str == "")) {
      if (type == ObJsonType) {
        is_null = true;
      } else {
        is_invalid = true;
      }
    } else if (type == ObJsonType) {
      // do nothing
    } else { // json tree
      uint32_t parse_flag = 0;
      ADD_FLAG_IF_NEED(strict_opt != OB_JSON_MODE_STRICT, parse_flag, ObJsonParser::JSN_RELAXED_FLAG);
      ADD_FLAG_IF_NEED(unique_opt == OB_JSON_MODE_UNIQUE_KEYS, parse_flag, ObJsonParser::JSN_UNIQUE_FLAG);

      if (OB_FAIL(ObJsonParser::check_json_syntax(j_str, &allocator, parse_flag))) {
        LOG_WARN("fail to check json syntax", K(ret), K(type), K(j_str));
      }
    }
  }

  bool is_valid_json = !is_invalid;
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(is_null)) {
      res.set_null();
    } else {
      is_invalid = check_for_is_json ? is_invalid : !is_invalid;
      res.set_int(static_cast<int64_t>(!is_invalid));
    }
  } else if (ret == OB_ERR_INVALID_JSON_TEXT) {
    res.set_int(check_for_is_json ? 0 : 1);
    is_valid_json = false;
    ret = OB_SUCCESS;
  } else if (ret == OB_ERR_JSON_OUT_OF_DEPTH) {
    LOG_USER_ERROR(OB_ERR_JSON_OUT_OF_DEPTH);
  } else if (ret == OB_ERR_DUPLICATE_KEY && unique_opt == OB_JSON_MODE_UNIQUE_KEYS) {
    res.set_int(check_for_is_json ? 0 : 1);
    is_valid_json = false;
    ret = OB_SUCCESS;
  }

  // if disallow
  if (OB_SUCC(ret) && is_valid_json && (scalar_opt == OB_IS_JSON_MODE_UNDEFAULT)) {
    if (is_null) {
      res.set_int(check_for_is_json ? 0 : 1);
    } else {
      bool is_null_json = false;
      ObIJsonBase *json_data = NULL;
      if (OB_FAIL(ObJsonExprHelper::get_json_doc(expr, ctx, allocator, 0,
                                                json_data, is_null_json))) {
        LOG_WARN("get_json_doc failed", K(ret));
      } else {
        // 如果is_json && scalar
        if (json_data->json_type() != ObJsonNodeType::J_OBJECT
            && json_data->json_type() != ObJsonNodeType::J_ARRAY) {
          res.set_int(check_for_is_json ? 0 : 1);
        } // end of check scalar
      } // end of get json doc
    }// end of J_NULL
  }

  return ret;
}

int ObExprIsJson::get_is_json_option(const ObExpr &expr, ObEvalCtx &ctx, int64_t idx, uint8_t& is_json_mode)
{
  INIT_SUCC(ret);
  is_json_mode = 0;
  ObExpr *json_arg = expr.args_[idx];
  ObDatum *json_datum = NULL;

  if (OB_ISNULL(json_arg)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("null param", K(idx), K(ret));
  } else {
    ObObjType val_type = json_arg->datum_meta_.type_;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(json_arg->eval(ctx, json_datum))) {
      LOG_WARN("eval json arg failed", K(ret));
    } else if (val_type == ObIntType) {
      int64_t option = json_datum->get_int();
      if (option < OB_IS_JSON_MODE_DEFAULT ||
          option > OB_IS_JSON_MODE_MAX) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("input option type error", K(option), K(ret));
      } else {
        is_json_mode = static_cast<uint8_t>(option);
        if (is_json_mode == OB_IS_JSON_MODE_MAX) is_json_mode = OB_IS_JSON_MODE_DEFAULT;
      }
    } else if (val_type == ObNumberType) {
      const uint32_t *option = json_datum->get_number_digits();
      if (*option < OB_IS_JSON_MODE_DEFAULT ||
          *option > OB_IS_JSON_MODE_MAX) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("input option type error", K(option), K(ret));
      } else {
        is_json_mode = static_cast<uint8_t>(*option);
        if (is_json_mode == OB_IS_JSON_MODE_MAX) is_json_mode = OB_IS_JSON_MODE_DEFAULT;
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("input type error", K(val_type), K(ret));
    }
  }

  return ret;
}

int ObExprIsJson::eval_is_json(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  ObExpr *json_arg = expr.args_[1];
  ObDatum *json_datum = NULL;
  ObObjType val_type = json_arg->datum_meta_.type_;
  uint8_t strict_opt = 0;
  uint8_t scalar_opt = 0;
  uint8_t unique_opt = 0;


  // check if is json or not
  bool check_for_is_json = false;
  if (OB_FAIL(json_arg->eval(ctx, json_datum))) {
    LOG_WARN("eval json arg failed", K(ret));
  } else if (val_type == ObIntType) {
    int64_t option = json_datum->get_int();
    if (option == 0) {
      check_for_is_json = false;
    } else if (option == 1) {
      check_for_is_json = true;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("input option error", K(option), K(ret));
    }
  } else if (val_type == ObNumberType) {
    const uint32_t *option = json_datum->get_number_digits();
    if (*option == 0) {
      check_for_is_json = false;
    } else if (*option == 1) {
      check_for_is_json = true;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("input option error", K(*option), K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("input type error", K(val_type), K(ret));
  }

  // check option for is_json
  // get strict_opt
  if (OB_SUCC(ret)) {
    if (OB_FAIL(get_is_json_option(expr, ctx, 2, strict_opt))) {
      LOG_WARN("input strict option error", K(ret));
    }
  }
  // get scalar_opt
  if (OB_SUCC(ret)) {
    if (OB_FAIL(get_is_json_option(expr, ctx, 3, scalar_opt))) {
      LOG_WARN("input scalar option error", K(ret));
    }
  }
  // get unique_opt
  if (OB_SUCC(ret)) {
    if (OB_FAIL(get_is_json_option(expr, ctx, 4, unique_opt))) {
      LOG_WARN("input unique option error", K(ret));
    }
  }

  // check json_doc is json or not
  json_arg = expr.args_[0];
  ObCollationType cs_type = json_arg->datum_meta_.cs_type_;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(json_arg->eval(ctx, json_datum))) {
    LOG_WARN("eval json arg failed", K(ret));
  } else {
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
    if (OB_FAIL(check_is_json(expr, ctx, *json_datum,
                              json_arg->datum_meta_.type_,
                              cs_type, temp_allocator,
                              strict_opt, scalar_opt, unique_opt,
                              check_for_is_json, res))) {
      LOG_WARN("fail to do checks whether is json", K(ret), K(json_arg->datum_meta_.type_));
    }
  }

  return ret;
}

int ObExprIsJson::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                          ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_is_json;
  return OB_SUCCESS;
}


}
}