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
 * This file is for func json_exists.
 */
#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_json_exists.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "share/object/ob_obj_cast.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/object/ob_obj_cast_util.h"
#include "share/object/ob_obj_cast.h"
#include "sql/engine/expr/ob_expr_cast.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "lib/oblog/ob_log_module.h"
#include "ob_expr_json_func_helper.h"

namespace oceanbase
{
namespace sql
{
ObExprJsonExists::ObExprJsonExists(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_JSON_EXISTS, N_JSON_EXISTS, MORE_THAN_TWO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprJsonExists::~ObExprJsonExists()
{
}

int ObExprJsonExists::calc_result_typeN(ObExprResType& type,
                                        ObExprResType* types_stack,
                                        int64_t param_num,
                                        common::ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  INIT_SUCC(ret);
  if (OB_UNLIKELY(param_num < 5)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid param number", K(ret), K(param_num));
  } else {
    // set the result type to bool
    type.set_int32();
    type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
    type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].scale_);

    // parser json doc
    ObObjType doc_type = types_stack[0].get_type();
    if (types_stack[0].get_type() == ObNullType) {
    } else if (!ObJsonExprHelper::is_convertible_to_json(doc_type)) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, ob_obj_type_str(types_stack[0].get_type()), "JSON");
    } else if (ob_is_string_type(doc_type)) {
      if (types_stack[0].get_collation_type() == CS_TYPE_BINARY) {
        // suport string type with binary charset
        types_stack[0].set_calc_collation_type(CS_TYPE_BINARY);
      } else if (types_stack[0].get_charset_type() != CHARSET_UTF8MB4) {
        types_stack[0].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
      }
    } else if (doc_type == ObJsonType) {
      // do nothing
    } else {
      types_stack[0].set_calc_type(ObVarcharType);
      types_stack[0].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    }

    // json path : 1
    if (OB_SUCC(ret)) {
      if (types_stack[1].get_type() == ObNullType) {
        ret = OB_ERR_PATH_EXPRESSION_NOT_LITERAL;
        LOG_USER_ERROR(OB_ERR_PATH_EXPRESSION_NOT_LITERAL);
      } else if (ob_is_string_type(types_stack[1].get_type())) {
        if (types_stack[1].get_charset_type() != CHARSET_UTF8MB4) {
          types_stack[1].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
        }
      } else {
        types_stack[1].set_calc_type(ObVarcharType);
        types_stack[1].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
      }
    }

    // passing_clause : might be NULL(idx: 2) or even idx: [2, param_num-2)
    if (OB_SUCC(ret)) {
      if (param_num == 5) {
        if (types_stack[2].get_type() != ObNullType) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("input option error", K(types_stack[2].get_type()), K(ret));
        }
      } else if (param_num % 2 == 0 && param_num >= 6) {
        for (int i = 2; i < param_num - 2; ++i) {
          if (ob_is_string_type(types_stack[i].get_type())) {
            if (types_stack[i].get_charset_type() != CHARSET_UTF8MB4) {
              types_stack[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
            }
          } else if (i % 2 == 1) {
            types_stack[i].set_calc_type(ObVarcharType);
            types_stack[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("<passing clause> param num error", K(param_num));
      }
    }
  }

  // on error : param_num-2
  if (OB_SUCC(ret)) {
    if (types_stack[param_num - 2].get_type() == ObNullType) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("<error type> param type is unexpected", K(types_stack[param_num - 2].get_type()));
    } else if (types_stack[param_num - 2].get_type() != ObIntType) {
      types_stack[param_num - 2].set_calc_type(ObIntType);
    }
  }

  // on empty : param_num-1
  if (OB_SUCC(ret)) {
    if (types_stack[param_num - 1].get_type() == ObNullType) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("<error type> param type is unexpected", K(types_stack[param_num - 1].get_type()));
    } else if (types_stack[param_num - 1].get_type() != ObIntType) {
      types_stack[param_num - 1].set_calc_type(ObIntType);
    }
  }
  return ret;
}

int ObExprJsonExists::get_path(const ObExpr &expr, ObEvalCtx &ctx,
                              ObJsonPath* &j_path, common::ObArenaAllocator &allocator,
                              ObJsonPathCache &ctx_cache, ObJsonPathCache* &path_cache)
{
  INIT_SUCC(ret);
  ObExpr *json_arg = nullptr;
  ObDatum *json_datum = nullptr;
  ObObjType type;

  json_arg = expr.args_[1];
  type = json_arg->datum_meta_.type_;
  if (OB_FAIL(json_arg->eval(ctx, json_datum))) {
    LOG_WARN("eval json arg failed", K(ret));
  } else if (type == ObNullType || json_datum->is_null()) {
    // path is null will return error
    // ORA-40442: JSON path expression syntax error
    ret = OB_ERR_JSON_PATH_SYNTAX_ERROR;
    LOG_WARN("JSON path expression syntax error ('')", K(ret));
  } else if (!ob_is_string_type(type)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("input type error", K(type));
  } else {
    ObString j_path_text;
    bool is_null = false;
    if (OB_FAIL(ObJsonExprHelper::get_json_or_str_data(json_arg, ctx, allocator, j_path_text, is_null))) {
      LOG_WARN("fail to get real data.", K(ret), K(j_path_text));
    } else if (is_null || j_path_text.length() == 0) {
      ret = OB_ERR_JSON_PATH_SYNTAX_ERROR;
      LOG_WARN("JSON path expression syntax error ('')", K(ret));
    } else {
      path_cache = ObJsonExprHelper::get_path_cache_ctx(expr.expr_ctx_id_, &ctx.exec_ctx_);
      path_cache = ((path_cache != NULL) ? path_cache : &ctx_cache);

      if (OB_FAIL(ObJsonExprHelper::find_and_add_cache(path_cache, j_path, j_path_text, 1, true))) {
        ret = OB_ERR_JSON_PATH_EXPRESSION_SYNTAX_ERROR;
        LOG_USER_ERROR(OB_ERR_JSON_PATH_EXPRESSION_SYNTAX_ERROR, j_path_text.length(), j_path_text.ptr());
      }
    }
  }

  return ret;
}

int ObExprJsonExists::get_var_data(const ObExpr &expr, ObEvalCtx &ctx, common::ObArenaAllocator &allocator,
                                    uint16_t index, ObIJsonBase*& j_base)
{
  INIT_SUCC(ret);
  ObDatum *json_datum = NULL;
  ObExpr *json_arg = expr.args_[index];
  ObObjType val_type = json_arg->datum_meta_.type_;
  ObCollationType cs_type = json_arg->datum_meta_.cs_type_;
  ObScale dec_scale = json_arg->datum_meta_.scale_;

  if (OB_UNLIKELY(OB_FAIL(json_arg->eval(ctx, json_datum)))) {
    LOG_WARN("eval json arg failed", K(ret));
  } else if (OB_ISNULL(json_datum)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("eval json arg failed", K(ret));
  } else if (json_datum->is_null()) {
    if (ob_is_string_type(val_type)) {
      ObJsonString* tmp_ans = static_cast<ObJsonString*> (allocator.alloc(sizeof(ObJsonString)));
      if (OB_ISNULL(tmp_ans)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate row buffer failed at ObJsonDecimal", K(ret));
      } else {
        tmp_ans = new (tmp_ans) ObJsonString("", 0);
        j_base = tmp_ans;
      }
    } else {
      ObJsonNull* tmp_ans = static_cast<ObJsonNull*> (allocator.alloc(sizeof(ObJsonNull)));
      if (OB_ISNULL(tmp_ans)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate row buffer failed at ObJsonDecimal", K(ret));
      } else {
        tmp_ans = new (tmp_ans) ObJsonNull();
        j_base = tmp_ans;
      }
    }
  } else if (ObJsonExprHelper::is_convertible_to_json(val_type)) {
    ObCollationType cs_type = json_arg->datum_meta_.cs_type_;
    if (OB_FAIL(ObJsonExprHelper::transform_convertible_2jsonBase(*json_datum, val_type,
                                                                  &allocator, cs_type,
                                                                  j_base, ObConv2JsonParam(true,
                                                                  json_arg->obj_meta_.has_lob_header(),
                                                                  false,
                                                                  true,
                                                                  false)))) {
      LOG_WARN("failed: parse value to jsonBase", K(ret), K(val_type));
    }
  } else {
    ObBasicSessionInfo *session = ctx.exec_ctx_.get_my_session();
    ObScale scale = json_arg->datum_meta_.scale_;
    scale = (val_type == ObBitType) ? json_arg->datum_meta_.length_semantics_ : scale;
    if (OB_ISNULL(session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is NULL", K(ret));
    } else if (OB_FAIL(ObJsonExprHelper::transform_scalar_2jsonBase(*json_datum, val_type,
                                                                    &allocator, scale,
                                                                    session->get_timezone_info(),
                                                                    session,
                                                                    j_base, true))) {
      LOG_WARN("failed: parse value to jsonBase", K(ret), K(val_type));
    }
  }
  return ret;
}

int ObExprJsonExists::get_passing(const ObExpr &expr, ObEvalCtx &ctx, PassingMap &pass_map,
                                  uint32_t param_num, ObArenaAllocator& temp_allocator)
{
  INIT_SUCC(ret);
  ObExpr *json_arg = nullptr;
  ObDatum *json_datum = nullptr;
  ObObjType type;
  for (uint32_t i = 2; i < param_num - 3 && OB_SUCC(ret); i += 2) {
    json_arg = expr.args_[i];
    type = json_arg->datum_meta_.type_;
    ObIJsonBase *json_data = nullptr;

    // get json_value, value could be null
    if (type == ObNullType) {
      ret = OB_ERR_INVALID_VARIABLE_IN_JSON_PATH;
      LOG_USER_ERROR(OB_ERR_INVALID_VARIABLE_IN_JSON_PATH);
    } else if (OB_FAIL(get_var_data(expr, ctx, temp_allocator, i, json_data))) {
      LOG_WARN("fail to get json val", K(ret));
    } else {
    // get keyname, keyname can't be null
      json_arg = expr.args_[i + 1];
      type = json_arg->datum_meta_.type_;
      json_datum = nullptr;
      if (OB_FAIL(json_arg->eval(ctx, json_datum))) {
        LOG_WARN("eval json arg failed", K(ret));
      } else if (type == ObNullType || json_datum->is_null()) {
        ret = OB_ERR_JSON_ILLEGAL_ZERO_LENGTH_IDENTIFIER_ERROR;
        LOG_USER_ERROR(OB_ERR_JSON_ILLEGAL_ZERO_LENGTH_IDENTIFIER_ERROR);
      } else if (!ob_is_string_type(type)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("input type error", K(type));
      }

      if (OB_SUCC(ret)) {
        ObString keyname = json_datum->get_string();
        if (keyname.length() == 0) {
          ret = OB_ERR_JSON_ILLEGAL_ZERO_LENGTH_IDENTIFIER_ERROR;
          LOG_USER_ERROR(OB_ERR_JSON_ILLEGAL_ZERO_LENGTH_IDENTIFIER_ERROR);
        } else {
          // In Oracle, if there are two identical keynames, the value of the latter will overwrite the former
          if (OB_FAIL(pass_map.set_refactored(keyname, json_data, 1))) {
            LOG_WARN("fail to set k-v for passing", K(i));
          }
        }
      }
    }
  }
  return ret;
}

int ObExprJsonExists::get_error_or_empty(const ObExpr &expr, ObEvalCtx &ctx, uint32_t idx, uint8_t &result)
{
  INIT_SUCC(ret);
  ObExpr *json_arg = expr.args_[idx];
  ObObjType val_type = json_arg->datum_meta_.type_;
  ObDatum *json_datum = NULL;

  val_type = json_arg->datum_meta_.type_;
  if (OB_FAIL(json_arg->eval(ctx, json_datum))) {
    LOG_WARN("eval json arg failed", K(ret));
  } else if (val_type == ObIntType) {
    int64_t option = json_datum->get_int();
    if (option < OB_JSON_FALSE_ON_ERROR ||
        option > OB_JSON_DEFAULT_ON_ERROR) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("input option type error", K(option), K(ret));
    } else {
      result = static_cast<uint8_t>(option);
      if (result == OB_JSON_DEFAULT_ON_ERROR) result = 0;
    }
  } else if (val_type == ObNumberType) {
    const uint32_t *option = json_datum->get_number_digits();
    if (*option < OB_JSON_FALSE_ON_ERROR ||
        *option > OB_JSON_DEFAULT_ON_ERROR) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("input option type error", K(option), K(ret));
    } else {
      result = static_cast<uint8_t>(*option);
      if (result == OB_JSON_DEFAULT_ON_ERROR) result = 0;
    }
  }
  return ret;
}

int ObExprJsonExists::get_empty_option(int8_t option_on_empty, bool& res_val)
{
  INIT_SUCC(ret);
  switch (option_on_empty) {
    // The on empty clause has two problems
    // 1. The on empty clause of json_exists, its track diagram is inconsistent with the document description:
    // a. The on empty clause in the document includes: NULL ON EMPTY, ERROR ON EMPTY, DEFAULT literal ON EMPTY
    // b. The on empty clause in the track diagram includes: ERROR ON EMPTY, FALSE ON EMPTY, TRUE ON EMPTY
    // c. During the actual test, it is found that inputting NULL ON EMPTY will report a syntax error, and the modification clause cannot be recognized, so implement it according to the track diagram
    // 2. According to the documentation, the on error of json_exists mainly checks whether the json data is wrong, and returns the response result.
    // But the on empty clause cannot find the correct documentation for the corresponding clause due to reason 1.
    // According to: whether the query result is empty / whether the path expression is empty / whether the json data is empty These three situations have been tried
    // It turns out that error/false/true on empty behaves exactly the same in three cases:
    // a. The query result or json data is empty, and all three on empty clauses return false
    // b. The path expression is empty, and the three on empty clauses all return the error code when the path is empty
    // Therefore, when implementing, follow the behavior of oracle, without distinguishing the three clauses, as long as there is no true result, it will return false
    case OB_JSON_ERROR_ON_EMPTY:
    case OB_JSON_FALSE_ON_EMPTY:
    case OB_JSON_TRUE_ON_EMPTY:
    case OB_JSON_DEFAULT_ON_ERROR: {
      res_val = false;
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("on_empty_option type error", K(option_on_empty), K(ret));
    }
  }
  return ret;
}

int ObExprJsonExists::get_error_option(int8_t option_on_error, bool& res_val)
{
  INIT_SUCC(ret);
  switch (option_on_error) {
    case OB_JSON_ERROR_ON_ERROR: {
      ret = OB_ERR_JSON_SYNTAX_ERROR;
      LOG_USER_ERROR(OB_ERR_JSON_SYNTAX_ERROR);
      break;
    }
    case OB_JSON_FALSE_ON_ERROR: {
      ret = OB_SUCCESS;
      res_val = false;
      break;
    }
    case OB_JSON_TRUE_ON_ERROR: {
      ret = OB_SUCCESS;
      res_val = true;
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("on_empty_option type error", K(option_on_error), K(ret));
    }
  }
  return ret;
}

int ObExprJsonExists::set_result(ObDatum &res, ObJsonSeekResult& hit,
                                const uint8_t option_on_error, const uint8_t option_on_empty,
                                const bool is_cover_by_error, const bool is_null_json)
{
  INIT_SUCC(ret);
  bool res_val = false;
  if (is_null_json) {
  } else if (is_cover_by_error) {
    if (OB_FAIL(get_error_option(option_on_error, res_val))) {
      LOG_WARN("fail to get error option", K(ret));
    }
  } else {
    if (hit.size() == 0) {
      if (OB_FAIL(get_empty_option(option_on_empty, res_val))) {
        LOG_WARN("fail to get error option", K(ret));
      }
    } else {
      res_val = true;
    }
  }
  if (OB_SUCC(ret)) {
    res.set_bool(res_val);
  }
  return ret;
}

int ObExprJsonExists::eval_json_exists(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  ObIJsonBase *json_data = NULL;
  bool is_null_json = false;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  ObJsonPathCache ctx_cache(&temp_allocator);
  ObJsonPathCache* path_cache = nullptr;
  ObJsonPath* j_path = nullptr;
  uint32_t param_num = expr.arg_cnt_;
  bool has_passing = true;
  PassingMap pass_map;
  bool is_cover_by_error = false;
  uint8_t option_on_error = 0;
  uint8_t option_on_empty = 0;
  ObJsonSeekResult hit;

  // get json
  // No error is reported when the json data is empty, no error is reported anyway, and the result is false
  // When error on error, json parses an error and reports an error, otherwise no error is reported (false on error by default)
  // ORA-40441: JSON syntax error
  if (OB_FAIL(ObJsonExprHelper::get_json_doc(expr, ctx, temp_allocator, 0,
                                             json_data, is_null_json))) {
    if (ret == OB_ERR_JSON_SYNTAX_ERROR) {
      is_cover_by_error = true;
    }
    LOG_WARN("get_json_doc failed", K(ret));
  }

  // get path
  if (OB_SUCC(ret) && !is_null_json && !is_cover_by_error) {
    if (OB_FAIL(get_path(expr, ctx, j_path, temp_allocator, ctx_cache, path_cache))) {
      LOG_WARN("json_exists fail to get path", K(ret));
    } else if (OB_NOT_NULL(j_path) && j_path->is_last_func()) {
      ret = OB_ERR_JSON_PATH_EXPRESSION_SYNTAX_ERROR;
      LOG_WARN("last path node can't be item function", K(ret));
    }
  }

  // get passing
  if (OB_SUCC(ret) && !is_null_json && !is_cover_by_error) {
    if (param_num == 5) {
      has_passing = false;
    } else {
      if (OB_FAIL(pass_map.create( (param_num - 4) / 2, "json_sql_var"))) {
        LOG_WARN("fail to create hash_map for passing clause", K(ret));
      } else {
        if (OB_FAIL(get_passing(expr, ctx, pass_map, param_num, temp_allocator))) {
          LOG_WARN("fail to get hash_map for passing clause", K(ret));
        }
      }
    }
  }

  // get on_error && on_empty
  // if json_data is null, return false whatever, even true on error
  // if json_data parse fail, result design by on error clause
  if (OB_SUCC(ret) || is_cover_by_error) {
    if (is_null_json) {
      option_on_error = 0;
      option_on_empty = 0;
    } else {
      // Prevent get_error_or_empty from causing error codes to be swallowed
      int tmp_ret = ret;
      if (OB_FAIL(get_error_or_empty(expr, ctx, param_num - 2, option_on_error))) {
        LOG_WARN("fail to get option_on_error for json_exists", K(ret));
      } else {
        if (OB_FAIL(get_error_or_empty(expr, ctx, param_num - 1, option_on_empty))) {
          LOG_WARN("fail to get option_on_empty for json_exists", K(ret));
        }
      }
      ret = tmp_ret;
    }
  }

  // seek
  if (OB_SUCC(ret) && !is_null_json && !is_cover_by_error) {
    if (has_passing) {
      if (OB_FAIL(json_data->seek(*j_path, j_path->path_node_cnt(), true, true, hit, &pass_map))) {
        if (ret == OB_HASH_NOT_EXIST) {
          ret = OB_ERR_NO_VALUE_IN_PASSING;
          LOG_USER_ERROR(OB_ERR_NO_VALUE_IN_PASSING);
        } else if (ret == OB_ERR_JSON_PATH_EXPRESSION_SYNTAX_ERROR) {
          is_cover_by_error = false;
        }
        LOG_WARN("failed: json seek.", K(ret));
      }
    } else {
      if (OB_FAIL(json_data->seek(*j_path, j_path->path_node_cnt(), true, true, hit))) {
        if (ret == OB_ERR_JSON_PATH_EXPRESSION_SYNTAX_ERROR) is_cover_by_error = false;
        LOG_WARN("failed: json seek.", K(ret));
      }
    }
  }

  // set result
  if (OB_SUCC(ret) || is_cover_by_error) {
    if (OB_FAIL(set_result(res, hit, option_on_error, option_on_empty, is_cover_by_error, is_null_json))) {
      LOG_WARN("json_exists failed to set result.", K(ret));
    }
  }
  return ret;
}

int ObExprJsonExists::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  INIT_SUCC(ret);
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_json_exists;
  return OB_SUCCESS;
  return ret;
}

}
}