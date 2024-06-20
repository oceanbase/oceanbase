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
 * This file is for func json_value.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_json_value.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "share/object/ob_obj_cast.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/object/ob_obj_cast_util.h"
#include "share/object/ob_obj_cast.h"
#include "share/ob_json_access_utils.h"
#include "sql/engine/expr/ob_expr_cast.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "lib/oblog/ob_log_module.h"
#include "ob_expr_json_func_helper.h"
#include "lib/charset/ob_charset.h"
#include "sql/engine/expr/ob_expr_json_utils.h"

// from sql_parser_base.h
#define DEFAULT_STR_LENGTH -1

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprJsonValue::ObExprJsonValue(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_JSON_VALUE, N_JSON_VALUE, MORE_THAN_TWO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprJsonValue::~ObExprJsonValue()
{
}

int ObExprJsonValue::calc_result_typeN(ObExprResType& type,
                                    ObExprResType* types_stack,
                                    int64_t param_num,
                                    ObExprTypeCtx& type_ctx) const
{
  INIT_SUCC(ret);

  if (OB_UNLIKELY(param_num < 11)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid param number", K(ret), K(param_num));
  } else {
    bool is_oracle_mode = lib::is_oracle_mode();
    //type.set_json();
    // json doc : 0
    bool is_json_input = false;
    if (OB_FAIL(ObExprJsonValue::calc_input_type(types_stack[0], is_json_input))) {
      LOG_WARN("fail to calc input type", K(ret));
    }

    // json path : 1
    if (OB_SUCC(ret)) {
      if (types_stack[JSN_VAL_PATH].get_type() == ObNullType) {
        if (lib::is_oracle_mode()) {
          ret = OB_ERR_PATH_EXPRESSION_NOT_LITERAL;
          LOG_USER_ERROR(OB_ERR_PATH_EXPRESSION_NOT_LITERAL);
        }
        // do nothing
      } else if (ob_is_string_type(types_stack[JSN_VAL_PATH].get_type())) {
        if (types_stack[JSN_VAL_PATH].get_charset_type() != CHARSET_UTF8MB4) {
          types_stack[JSN_VAL_PATH].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
        }
      } else {
        types_stack[JSN_VAL_PATH].set_calc_type(ObLongTextType);
        types_stack[JSN_VAL_PATH].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
      }
    }
    // returning type : 2
    ObExprResType dst_type;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObJsonExprHelper::get_cast_type(types_stack[JSN_VAL_RET], dst_type, type_ctx))) {
        LOG_WARN("get cast dest type failed", K(ret));
      } else if (OB_FAIL(ObJsonExprHelper::set_dest_type(types_stack[JSN_VAL_DOC], type, dst_type, type_ctx))) {
        LOG_WARN("set dest type failed", K(ret));
      } else {
        type.set_calc_collation_type(type.get_collation_type());
      }
    }
    // truncate 3
    if (OB_SUCC(ret)) {
      if (types_stack[JSN_VAL_TRUNC].get_type() == ObNullType) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param type is unexpected", K(types_stack[JSN_VAL_TRUNC].get_type()), K(JSN_VAL_TRUNC));
      } else if (types_stack[JSN_VAL_TRUNC].get_type() != ObIntType) {
        types_stack[JSN_VAL_TRUNC].set_calc_type(ObIntType);
      }
    }
    // ascii 4
    if (OB_SUCC(ret) && is_oracle_mode) {
      if (OB_FAIL(ObJsonExprHelper::parse_asc_option(types_stack[JSN_VAL_ASCII], types_stack[JSN_VAL_DOC], type, type_ctx))) {
        LOG_WARN("fail to parse asc option.", K(ret));
      }
    }
    // empty : 5, 6
    if (OB_SUCC(ret)) {
      if (OB_FAIL(calc_empty_error_type(types_stack, JSN_VAL_EMPTY, dst_type, type_ctx))) {
        LOG_WARN("fail to parse empty value", K(ret));
      }
    }
    // error : 7, 8
    if (OB_SUCC(ret)) {
      if (OB_FAIL(calc_empty_error_type(types_stack, JSN_VAL_ERROR, dst_type, type_ctx))) {
        LOG_WARN("fail to parse empty value", K(ret));
      }
    }
    // mismatch : 9,
    if (OB_SUCC(ret)) {
      for (size_t i = JSN_VAL_MISMATCH; OB_SUCC(ret) && i < param_num; i++) {
        if (types_stack[i].get_type() == ObNullType) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("<empty type> param type is unexpected", K(types_stack[i].get_type()), K(ret), K(i));
        } else if (types_stack[i].get_type() != ObIntType) {
          types_stack[i].set_calc_type(ObIntType);
        }
      }
    }
  }
  type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_CONST_TO_DECIMAL_INT_EQ);
  return ret;
}

int ObExprJsonValue::calc_input_type(ObExprResType& types_stack, bool &is_json_input)
{
  INIT_SUCC(ret);
  bool is_oracle_mode = lib::is_oracle_mode();
  ObObjType doc_type = types_stack.get_type();
  if (types_stack.get_type() == ObNullType) {
  } else if (!ObJsonExprHelper::is_convertible_to_json(doc_type)) {
    if (is_oracle_mode) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, ob_obj_type_str(types_stack.get_type()), "JSON");
    } else {
      ret = OB_ERR_INVALID_TYPE_FOR_JSON;
      LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_JSON, 1, "json_value");
      LOG_WARN("Invalid type for json doc", K(doc_type), K(ret));
    }
  } else if (ob_is_string_type(doc_type)) {
    if (is_oracle_mode) {
      if (types_stack.get_collation_type() == CS_TYPE_BINARY) {
        types_stack.set_calc_collation_type(CS_TYPE_BINARY);
      } else if (types_stack.get_charset_type() != CHARSET_UTF8MB4) {
        types_stack.set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
      }
    } else {
      if (types_stack.get_collation_type() == CS_TYPE_BINARY) {
      // unsuport string type with binary charset
        ret = OB_ERR_INVALID_JSON_CHARSET;
        LOG_WARN("Unsupport for string type with binary charset input.", K(ret), K(doc_type));
      } else if (types_stack.get_charset_type() != CHARSET_UTF8MB4) {
        types_stack.set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
      }
    }
  } else if (doc_type == ObJsonType) {
    is_json_input = true;
    // do nothing
  } else {
    types_stack.set_calc_type(ObLongTextType);
    types_stack.set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
  }
  return ret;
}

int ObExprJsonValue::calc_empty_error_type(ObExprResType* types_stack, uint8_t pos, ObExprResType &dst_type, ObExprTypeCtx& type_ctx)
{
  INIT_SUCC(ret);
  ObExprResType temp_type;
  if (types_stack[pos].get_type() == ObNullType) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("<empty type> param type is unexpected", K(types_stack[pos].get_type()));
  } else if (types_stack[pos].get_type() != ObIntType) {
    types_stack[pos].set_calc_type(ObIntType);
  } else if (types_stack[pos + 1].get_type() == ObNullType) {
    // do nothing
  } else if (lib::is_oracle_mode() && OB_FAIL(check_default_value(types_stack, pos, dst_type))) { // check default value valid in oracle mode
    LOG_WARN("fail to get empty value", K(ret));
  } else if (OB_FAIL(ObJsonExprHelper::set_dest_type(types_stack[pos + 1], temp_type, dst_type, type_ctx))) {
    LOG_WARN("set dest type failed", K(ret));
  } else {
    types_stack[pos + 1].set_calc_type(temp_type.get_type());
    types_stack[pos + 1].set_calc_collation_type(temp_type.get_collation_type());
    types_stack[pos + 1].set_calc_collation_level(temp_type.get_collation_level());
    types_stack[pos + 1].set_calc_accuracy(temp_type.get_accuracy());
  }
  return ret;
}

int ObExprJsonValue::check_default_value(ObExprResType* types_stack, int8_t pos, ObExprResType &dst_type)
{
  INIT_SUCC(ret);
  int8_t type = 0;
  ObObjType val_type = types_stack[pos + 1].get_type();
  if (OB_FAIL(ObJsonExprHelper::get_expr_option_value(types_stack[pos], type))) {
    LOG_WARN("fail to get option", K(ret));
  } else if (type == JSN_VALUE_DEFAULT) {
    ObString default_val(types_stack[pos + 1].get_param().get_string().length(), types_stack[pos + 1].get_param().get_string().ptr());
    if (val_type == ObCharType || val_type == ObNumberType || val_type == ObDecimalIntType) {
      if (OB_FAIL(ObJsonExprHelper::pre_default_value_check(dst_type.get_type(), default_val, val_type, dst_type.get_length()))) {
        LOG_WARN("default value pre check fail", K(ret), K(default_val));
      }
    }
  }
  return ret;
}

int ObExprJsonValue::eval_json_value(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  bool is_cover_by_error = true;
  bool is_null_result = false;
  uint8_t is_type_mismatch = 0;
  ObDatum *return_val = NULL;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  ObJsonBin st_json(&temp_allocator);
  ObIJsonBase *j_base = &st_json;
  ObJsonSeekResult hits;
  ObJsonBin res_json(&temp_allocator);
  hits.res_point_ = &res_json;
  ObJsonParamCacheCtx ctx_cache(&temp_allocator);
  ObJsonParamCacheCtx* param_ctx = NULL;
  /**
  * get content point，
  */
  param_ctx = ObJsonExprHelper::get_param_cache_ctx(expr.expr_ctx_id_, &ctx.exec_ctx_);
  if (OB_ISNULL(param_ctx)) {
    param_ctx = &ctx_cache;
  }
  // init flag
  if (param_ctx->is_first_exec_ && OB_FAIL(init_ctx_var(expr, param_ctx))) {
    is_cover_by_error = false;
    LOG_WARN("fail to init param ctx", K(ret));
  } else if (param_ctx->is_first_exec_
              && OB_FAIL(ObExprJsonValue::get_clause_param_value(expr, ctx, &param_ctx->json_param_,
                                          is_cover_by_error))) { // get param value & check param valid
    LOG_WARN("fail to get param value", K(ret));
  } else if (OB_FAIL(ObJsonUtil::get_json_doc(expr.args_[JSN_VAL_DOC], ctx,
                    temp_allocator, j_base, is_null_result,
                    is_cover_by_error))) { // parse json doc
    LOG_WARN("fail to parse json doc", K(ret));
  } else if (OB_ISNULL(param_ctx->json_param_.json_path_)
             && OB_FAIL(ObJsonUtil::get_json_path(expr.args_[JSN_VAL_PATH],
                            ctx, is_null_result, param_ctx, temp_allocator,
                            is_cover_by_error))) { // parse json path
    LOG_WARN("fail to get json path", K(ret));
  }
  // parse empty error default value
  if ((OB_SUCC(ret) && !is_null_result) || is_cover_by_error) {
    int temp_ret = get_default_empty_error_value(expr, &param_ctx->json_param_, ctx);
    if (temp_ret != OB_SUCCESS) {
      is_cover_by_error = false;
      ret = temp_ret;
    }
  }
  if (OB_SUCC(ret) && !is_null_result && OB_FAIL(doc_do_seek(hits,
                                  is_null_result, &param_ctx->json_param_, j_base,
                                  expr, ctx, is_cover_by_error,
                                  return_val, is_type_mismatch))) { // do seek
    LOG_WARN("doc do seek fail", K(ret));
  }

  // fill output and deal error case
  if (OB_FAIL(ret)) {
    if (is_cover_by_error && !try_set_error_val(expr, ctx, res, ret, &param_ctx->json_param_, is_type_mismatch)) {
      LOG_WARN("set error val fail", K(ret));
    }
    LOG_WARN("json_values failed", K(ret));
  } else {
    ret = set_result(expr, &param_ctx->json_param_, ctx, is_null_result, is_cover_by_error, is_type_mismatch,
                    res, return_val, &temp_allocator, hits);
  }
  if (OB_SUCC(ret)) {
    param_ctx->is_first_exec_ = false;
  }
  return ret;
}

int ObExprJsonValue::extract_plan_cache_param(const ObExprJsonQueryParamInfo *info, ObJsonExprParam& json_param)
{
  INIT_SUCC(ret);
  json_param.truncate_ = info->truncate_;
  json_param.empty_type_ = info->empty_type_;
  json_param.error_type_ = info->error_type_;
  json_param.ascii_type_ = info->ascii_type_;
  json_param.json_path_ = info->j_path_;
  json_param.is_init_from_cache_ = true;
  for (int i = 0; OB_SUCC(ret) && i < info->on_mismatch_.count(); i++) {
    if (OB_FAIL(json_param.on_mismatch_.push_back(info->on_mismatch_.at(i)))) {
      LOG_WARN("fail to push node to mismatch type", K(ret));
    } else if (OB_FAIL(json_param.on_mismatch_type_.push_back(info->on_mismatch_type_.at(i)))) {
      LOG_WARN("fail to push node to mismatch type", K(ret));
    }
  }
  return ret;
}

int ObExprJsonValue::eval_ora_json_value(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  bool is_cover_by_error = true;
  bool is_null_result = false;
  uint8_t is_type_mismatch = 0;
  ObDatum *return_val = NULL;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  ObJsonBin st_json(&temp_allocator);
  ObIJsonBase *j_base = &st_json;
  ObJsonSeekResult hits;
  ObJsonBin res_json(&temp_allocator);
  hits.res_point_ = &res_json;
  ObJsonParamCacheCtx ctx_cache(&temp_allocator);
  ObJsonParamCacheCtx* param_ctx = NULL;
  param_ctx = ObJsonExprHelper::get_param_cache_ctx(expr.expr_ctx_id_, &ctx.exec_ctx_);
  if (OB_ISNULL(param_ctx)) {
    param_ctx = &ctx_cache;
  }
  // init flag
  if (param_ctx->is_first_exec_ && OB_FAIL(init_ctx_var(expr, param_ctx))) {
    is_cover_by_error = false;
    LOG_WARN("fail to init param ctx", K(ret));
  } else if (OB_ISNULL(param_ctx->json_param_.json_path_) // parse json path
             && OB_FAIL(ObJsonUtil::get_json_path(expr.args_[JSN_VAL_PATH], ctx,
                is_null_result, param_ctx, temp_allocator, is_cover_by_error))) {
    LOG_WARN("fail to get json path", K(ret));
  } else if (param_ctx->is_first_exec_
             && OB_FAIL(ObExprJsonValue::get_clause_param_value(expr, ctx,
                        &param_ctx->json_param_, is_cover_by_error))) { // get param value & check param valid
    LOG_WARN("fail to get param value", K(ret));
  } else if (param_ctx->json_param_.dst_type_ == ObRawType) {
    ret = OB_ERR_UNIMPLEMENT_JSON_FEATURE;
    LOG_WARN("Unimplement json returning type", K(ret));
  } else if (OB_FAIL(ObJsonUtil::get_json_doc(expr.args_[JSN_VAL_DOC], ctx,
            temp_allocator, j_base, is_null_result,
            is_cover_by_error, true))) {
    LOG_WARN("fail to get json doc", K(ret));
  } else if (!is_null_result
              && OB_FAIL(doc_do_seek(hits, is_null_result, &param_ctx->json_param_,
                              j_base, expr, ctx, is_cover_by_error,
                              return_val, is_type_mismatch))) { //  do seek
    if (ret == OB_ERR_JSON_PATH_EXPRESSION_SYNTAX_ERROR) {
      is_cover_by_error = false;
    }
    LOG_WARN("doc do seek fail", K(ret));
  }

  // fill output and deal error case
  if (OB_FAIL(ret)) {
    if (is_cover_by_error && !try_set_error_val(expr, ctx, res, ret, &param_ctx->json_param_, is_type_mismatch)) {
      LOG_WARN("set error val fail", K(ret));
    }
    LOG_WARN("json_values failed", K(ret));
  } else {
    ret = set_result(expr, &param_ctx->json_param_, ctx, is_null_result, is_cover_by_error, is_type_mismatch,
                    res, return_val, &temp_allocator, hits);
  }
  if (OB_SUCC(ret)) {
    param_ctx->is_first_exec_ = false;
  }
  return ret;
}

int ObExprJsonValue::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  INIT_SUCC(ret);
  ObIAllocator &alloc = *expr_cg_ctx.allocator_;
  ObExprJsonQueryParamInfo* info
          = OB_NEWx(ObExprJsonQueryParamInfo, (&alloc), alloc, T_FUN_SYS_JSON_VALUE);
  if (OB_ISNULL(info)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else if (OB_FAIL(info->init_jsn_val_expr_param(alloc, expr_cg_ctx, &raw_expr))) {
    ret = OB_SUCCESS; // not use plan cache
  } else {
    rt_expr.extra_info_ = info;
  }
  if (lib::is_oracle_mode()) {
    rt_expr.eval_func_ = eval_ora_json_value;
  } else {
    rt_expr.eval_func_ = eval_json_value;
  }
  return ret;
}

int ObExprJsonQueryParamInfo::get_int_val_from_raw(ObIAllocator &alloc, ObExecContext *exec_ctx, const ObRawExpr* raw_expr, ObObj &const_data)
{
  INIT_SUCC(ret);
  const_data.reset();
  bool got_data = false;
  if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(exec_ctx,
                                                        raw_expr,
                                                        const_data,
                                                        got_data,
                                                        alloc))) {
    LOG_WARN("failed to calc offset expr", K(ret));
  } else if (!got_data || const_data.is_null()
              || !ob_is_integer_type(const_data.get_type())) {
    ret = OB_ERR_INVALID_INPUT_ARGUMENT;
    LOG_WARN("fail to get int value", K(ret));
  }
  return ret;
}

int ObExprJsonQueryParamInfo::init_mismatch_array(const ObRawExpr* raw_expr,
                                                  ObExecContext *exec_ctx)
{
  INIT_SUCC(ret);
  uint32_t pos = -1;
  ObArray<int8_t> val;
  ObArray<int8_t> type;
  ObObj const_data;
  for(uint32_t i = JSN_VAL_MISMATCH; OB_SUCC(ret)
                    && i < raw_expr->get_param_count(); i++) {
    if (OB_FAIL(get_int_val_from_raw(allocator_, exec_ctx, raw_expr->get_param_expr(i), const_data))) {
      LOG_WARN("failed to calc offset expr", K(ret));
    } else {
      int64_t option_type = const_data.get_int();
      if (OB_FAIL(ObJsonUtil::set_mismatch_val(val, type, option_type, pos))) {
        LOG_WARN("fail to eval mismatch value", K(ret));
      }
    }
  }
  pos = val.size();
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(on_mismatch_.init(pos))) {
    LOG_WARN("fail to init type array", K(ret));
  } else if (OB_FAIL(on_mismatch_type_.init(pos))) {
    LOG_WARN("fail to init type array", K(ret));
  } else {
    for (uint32_t i = 0; OB_SUCC(ret) && i < pos; i++) {
      if (OB_FAIL(on_mismatch_.push_back(val.at(i)))) {
        LOG_WARN("fail to init type array", K(ret));
      } else if (OB_FAIL(on_mismatch_type_.push_back(type.at(i)))) {
        LOG_WARN("fail to init type array", K(ret));
      }
    }
  }
  return ret;
}

int ObExprJsonQueryParamInfo::init_jsn_val_expr_param(ObIAllocator &alloc, ObExprCGCtx &op_cg_ctx, const ObRawExpr* raw_expr)
{
  INIT_SUCC(ret);
  ObExecContext *exec_ctx = op_cg_ctx.session_->get_cur_exec_ctx();
  const ObRawExpr *path = raw_expr->get_param_expr(JSN_VAL_PATH);
  ObObj const_data;
  ObArray<int8_t> param_vec;
  uint32_t pos = -1;
  // parse clause node
  for (int64_t i = JSN_VAL_TRUNC; OB_SUCC(ret) && i <= JSN_VAL_ERROR_DEF; i ++) {
    if (i == JSN_VAL_EMPTY_DEF
        || i == JSN_VAL_ERROR_DEF) {
    } else if (OB_FAIL(get_int_val_from_raw(alloc, exec_ctx, raw_expr->get_param_expr(i), const_data))) {
      LOG_WARN("failed to calc offset expr", K(ret));
    } else if (OB_FAIL(param_vec.push_back(const_data.get_tinyint()))) {
      LOG_WARN("fail to push val into array", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    truncate_ = param_vec[JSN_VAL_TRUNC_OPT];
    ascii_type_ = param_vec[JSN_VAL_ASCII_OPT];
    empty_type_ = param_vec[JSN_VAL_EMPTY_OPT];
    error_type_ = param_vec[JSN_VAL_ERROR_OPT];
  }
  // parse mismatch 1. init array 2. push_back node
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(init_mismatch_array(raw_expr, exec_ctx))) {
    LOG_WARN("fail to eval mismatch array", K(ret));
  } else if (OB_FAIL(ObJsonUtil::init_json_path(alloc, op_cg_ctx, path, *this))) { // parse json path
    LOG_WARN("fail to init path from str", K(ret));
  }
  return ret;
}

int ObExprJsonValue::set_result(const ObExpr &expr,
                                ObJsonExprParam* json_param,
                                ObEvalCtx &ctx,
                                bool &is_null_result,
                                bool &is_cover_by_error,
                                uint8_t &is_type_mismatch,
                                ObDatum &res,
                                ObDatum *return_val,
                                ObIAllocator *allocator,
                                ObJsonSeekResult &hits)
{
  INIT_SUCC(ret);
  if (is_null_result) {
    res.set_null();
  } else {
    if (return_val != NULL) {
      res.set_datum(*return_val);
    } else {
      ObCollationType in_coll_type = expr.args_[0]->datum_meta_.cs_type_;
      ObCollationType dst_coll_type = expr.datum_meta_.cs_type_;
      ObJsonCastParam cast_param(json_param->dst_type_, in_coll_type, dst_coll_type, json_param->ascii_type_);
      cast_param.rt_expr_ = &expr;
      ret = ObJsonUtil::cast_to_res(allocator, ctx, hits[0],
          json_param->accuracy_, cast_param, res, is_type_mismatch);
      if (OB_FAIL(ret)) {
        try_set_error_val(expr, ctx, res, ret, json_param, is_type_mismatch);
      } else if (OB_FAIL(ObJsonUtil::set_lob_datum(allocator, expr, ctx, json_param->dst_type_, json_param->ascii_type_,res))) {
        LOG_WARN("fail to set lob datum from string val", K(ret));
      }
    }
  }
  return ret;
}

int ObExprJsonValue::get_default_empty_error_value(const ObExpr &expr,
                                                  ObJsonExprParam* json_param,
                                                  ObEvalCtx &ctx)
{
  INIT_SUCC(ret);
  // parse empty option
  if (json_param->empty_type_ == JSN_VALUE_DEFAULT) {
    if (!json_param->is_empty_default_const_ || OB_ISNULL(json_param->empty_val_)) {
      if (OB_FAIL(get_default_value(expr.args_[JSN_VAL_EMPTY + 1], ctx,
               json_param->accuracy_, &json_param->empty_val_))) {
        LOG_WARN("failed to get empty datum", K(ret));
      }
    }
  }
  // parse error option
  if (lib::is_mysql_mode() && OB_SUCC(ret) && json_param->error_type_ == JSN_VALUE_DEFAULT) { // always get error option for return default value on error
    if (!json_param->is_error_default_const_ || OB_ISNULL(json_param->error_val_)) {
      if (OB_FAIL(get_default_value(expr.args_[JSN_VAL_ERROR + 1], ctx,
               json_param->accuracy_, &json_param->error_val_))) {
        LOG_WARN("failed to get empty datum", K(ret));
      }
    }
  }
  return ret;
}

int ObExprJsonValue::init_ctx_var(const ObExpr &expr, ObJsonParamCacheCtx* param_ctx)
{
  INIT_SUCC(ret);
  // init json path flag
  param_ctx->is_json_path_const_ = expr.args_[JSN_VAL_PATH]->is_const_expr();
  // init empty default value flag
  param_ctx->json_param_.is_empty_default_const_ = expr.args_[JSN_VAL_EMPTY_DEF]->is_const_expr();
  // init error default value flag
  param_ctx->json_param_.is_error_default_const_ = expr.args_[JSN_VAL_ERROR_DEF]->is_const_expr();
  // extract value from paln cache
  const ObExprJsonQueryParamInfo *info
                  = static_cast<ObExprJsonQueryParamInfo *>(expr.extra_info_);
  if (OB_NOT_NULL(info)
      && OB_FAIL(extract_plan_cache_param(info, param_ctx->json_param_))) {
    LOG_WARN("fail to extract param from plan cache", K(ret));
  }
  return ret;
}

int ObExprJsonValue::check_param_valid(const ObExpr &expr, ObJsonExprParam* json_param,
                                      ObJsonPath *j_path, bool &is_cover_by_error)
{
  INIT_SUCC(ret);
  // binary can not use with ascii
  if ((expr.datum_meta_.cs_type_ == CS_TYPE_BINARY || !(ob_is_string_tc(json_param->dst_type_) || ob_is_text_tc(json_param->dst_type_)))
      && json_param->ascii_type_ > 0) {
    is_cover_by_error = false;
    ret = OB_ERR_NON_TEXT_RET_NOTSUPPORT;
    LOG_WARN("ASCII or PRETTY not supported for non-textual return data type", K(ret));
  }

  int8_t JSON_VALUE_EXPR = 0;
  ObExpr *json_arg_ret = expr.args_[JSN_VAL_RET];
  ObObjType val_type = json_arg_ret->datum_meta_.type_;
  if (OB_SUCC(ret) && val_type != ObNullType && j_path->is_last_func()
      && OB_FAIL( ObJsonExprHelper::check_item_func_with_return(j_path->get_last_node_type(),
                  json_param->dst_type_, expr.datum_meta_.cs_type_, JSON_VALUE_EXPR))) {
    is_cover_by_error = false;
    LOG_WARN("check item func with return type fail", K(ret));
  }
  return ret;
}

int ObExprJsonValue::get_clause_param_value(const ObExpr &expr, ObEvalCtx &ctx,
                                            ObJsonExprParam* json_param,
                                            bool &is_cover_by_error)
{
  INIT_SUCC(ret);
  ObArray<int8_t> param_vec;
  int8_t val = 0;
  // parse return node acc
  if (OB_SUCC(ret)) {
    ret = ObJsonUtil::get_accuracy(expr, ctx, json_param->accuracy_, json_param->dst_type_, is_cover_by_error);
  } else if (is_cover_by_error) { // when need error option, should do get accuracy
    ObJsonUtil::get_accuracy(expr, ctx, json_param->accuracy_, json_param->dst_type_, is_cover_by_error);
  }

  // truncate 3, ascii 4, empty_type 5, empty_val 6, error_type 7, error_val 8
  for (size_t i = JSN_VAL_TRUNC; OB_SUCC(ret) && !json_param->is_init_from_cache_
                                              && i <= JSN_VAL_ERROR_DEF; i ++) {
    if (i == JSN_VAL_EMPTY_DEF
        || i == JSN_VAL_ERROR_DEF) {
    } else if (OB_FAIL(ObJsonExprHelper::get_clause_opt(expr.args_[i], ctx, val))) {
      is_cover_by_error = false;
      LOG_WARN("fail to get clause option", K(ret));
    } else if (OB_FAIL(param_vec.push_back(val))) {
      is_cover_by_error = false;
      LOG_WARN("fail to push val into array", K(ret));
    }
  }
  if (json_param->is_init_from_cache_) {
  } else if (OB_FAIL(ret) && is_cover_by_error) {
    ret = ObJsonExprHelper::get_clause_opt(expr.args_[JSN_VAL_ERROR], ctx, json_param->error_type_);
    if (OB_FAIL(ret)) {
      is_cover_by_error = false;
    }
  } else if (OB_SUCC(ret) && param_vec.size() == 4) {
    json_param->truncate_ = param_vec[JSN_VAL_TRUNC_OPT];
    json_param->ascii_type_ = param_vec[JSN_VAL_ASCII_OPT];
    json_param->empty_type_ = param_vec[JSN_VAL_EMPTY_OPT];
    json_param->error_type_ = param_vec[JSN_VAL_ERROR_OPT];
  } else if (OB_SUCC(ret)) { // should use prior branch
    is_cover_by_error = false;
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get param value", K(ret));
  }

  // parser mismatch  TODO: type cast need complete, take type cast error from all error  ORA_JV_TYPE_CAST
  if (OB_SUCC(ret) && lib::is_oracle_mode()) {
    if (!json_param->is_init_from_cache_
        && OB_FAIL(get_on_mismatch(expr, ctx, JSN_VAL_MISMATCH, is_cover_by_error, json_param->accuracy_, json_param->on_mismatch_, json_param->on_mismatch_type_))) {
      LOG_WARN("failed to get mismatch option.", K(ret), K(json_param->on_mismatch_.count()), K(json_param->on_mismatch_type_.count()));
    } else if (OB_FAIL(ObExprJsonValue::check_param_valid(expr, json_param,
                                      json_param->json_path_, is_cover_by_error))) {
      LOG_WARN("fail to check param_valid", K(ret));
    }
  }

  return ret;
}

template<typename Obj>
int ObExprJsonValue::check_default_val_accuracy(const ObAccuracy &accuracy,
                                                const ObObjType &type,
                                                const Obj *obj)
{
  INIT_SUCC(ret);
  ObObjTypeClass tc = ob_obj_type_class(type);

  switch (tc) {
    case ObNumberTC: {
      number::ObNumber temp(obj->get_number());
      ret = ObJsonUtil::number_range_check(accuracy, NULL, temp, true);
      LOG_WARN("number range is invalid for json_value", K(ret));
      break;
    }
    case ObDecimalIntTC : {
      ObNumStackOnceAlloc tmp_alloc;
      number::ObNumber temp;
      if (OB_FAIL(wide::to_number(obj->get_decimal_int(), obj->get_int_bytes(),
                                  accuracy.scale_, tmp_alloc, temp))) {
        LOG_WARN("to_number failed", K(ret));
      } else if (OB_FAIL(ObJsonUtil::number_range_check(accuracy, NULL, temp, true))) {
        LOG_WARN("number range is invalid for json_value", K(ret));
      }
      break;
    }
    case ObDateTC: {
      int32_t val = obj->get_date();
      if (val == ObTimeConverter::ZERO_DATE) {
        // check zero date for scale over mode
        ret = OB_INVALID_DATE_VALUE;
        LOG_WARN("Zero date is invalid for json_value", K(ret));
      }
      break;
    }
    case ObDateTimeTC: {
      int64_t val = obj->get_datetime();
      ret = ObJsonUtil::datetime_scale_check(accuracy, val, true);
      break;
    }
    case ObTimeTC: {
      int64_t val = obj->get_time();
      ret = ObJsonUtil::time_scale_check(accuracy, val, true);
      break;
    }
    case ObStringTC :
    case ObTextTC : {
      ObString val = obj->get_string();
      const int32_t str_len_char = static_cast<int32_t>(ObCharset::strlen_char(CS_TYPE_UTF8MB4_BIN,
          val.ptr(), val.length()));
      const ObLength max_accuracy_len = (lib::is_oracle_mode() && tc == ObTextTC) ? OB_MAX_LONGTEXT_LENGTH : accuracy.get_length();
      if (OB_SUCC(ret)) {
        if (max_accuracy_len == DEFAULT_STR_LENGTH) { // default string len
        } else if (max_accuracy_len <= 0 || str_len_char > max_accuracy_len) {
          if (lib::is_mysql_mode()) {
            ret = OB_OPERATE_OVERFLOW;
            LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "STRING", "json_value");
          } else {
            ret = OB_ERR_VALUE_EXCEEDED_MAX;
            LOG_USER_ERROR(OB_ERR_VALUE_EXCEEDED_MAX, str_len_char, max_accuracy_len);
          }
        }
      }
      break;
    }
    default:
      break;
  }

  return ret;
}

int ObExprJsonValue::doc_do_seek(ObJsonSeekResult &hits, bool &is_null_result, ObJsonExprParam* json_param,
                                ObIJsonBase *j_base, const ObExpr &expr, ObEvalCtx &ctx, bool &is_cover_by_error,
                                ObDatum *&return_val,
                                uint8_t &is_type_mismatch)
{
  INIT_SUCC(ret);
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  if (OB_SUCC(ret) && !is_null_result) {
    if (OB_FAIL(j_base->seek(*json_param->json_path_, json_param->json_path_->path_node_cnt(), true, false, hits))) {
      if (ret == OB_ERR_JSON_PATH_EXPRESSION_SYNTAX_ERROR) {
        is_cover_by_error = false;
      } else if (ret == OB_ERR_DOUBLE_TRUNCATED) {
        is_type_mismatch = true;
        ret = OB_INVALID_NUMERIC;
      }
      LOG_WARN("json seek failed", K(ret));
    } else if (lib::is_oracle_mode() && hits.size() == 1) {
      ObIJsonBase* data = hits[0];
      if (OB_FAIL(deal_item_method_in_seek(data, is_null_result, json_param->json_path_,
                        &temp_allocator, is_type_mismatch))) {
        LOG_WARN("fail to deal item method and special case", K(ret));
      } else {
        hits.set_node(0, data);
      }
    } else if (hits.size() == 0) {
      // get empty clause
      if (lib::is_oracle_mode() && OB_FAIL(get_default_empty_error_value(expr, json_param, ctx))) {
        if (ret == OB_ERR_VALUE_LARGER_THAN_ALLOWED) {
          is_cover_by_error = false;
        }
        LOG_WARN("fail to get empty clause", K(ret));
      } else if (OB_FAIL(get_empty_option(return_val, is_cover_by_error,
                                    json_param->empty_type_,
                                    json_param->empty_val_,
                                    is_null_result))) {
        LOG_WARN("fail to get empty option", K(ret));
      }
    } else if (hits.size() > 1) {
      // return val decide by error option
      ret = OB_ERR_MULTIPLE_JSON_VALUES;
      LOG_USER_ERROR(OB_ERR_MULTIPLE_JSON_VALUES, "json_value");
      LOG_WARN("json value seek result more than one.", K(hits.size()));
    } else if (hits[0]->json_type() == ObJsonNodeType::J_NULL) {
      is_null_result = true;
    }
  }
  return ret;
}

int ObExprJsonValue::get_empty_option(ObDatum *&empty_res,
                                      bool &is_cover_by_error,
                                      int8_t empty_type,
                                      ObDatum *empty_datum,
                                      bool &is_null_result)
{
  INIT_SUCC(ret);
  switch (empty_type) {
    case JSN_VALUE_ERROR: {
      is_cover_by_error = false;
      if (lib::is_oracle_mode()) {
        ret = OB_ERR_JSON_VALUE_NO_VALUE;
        LOG_USER_ERROR(OB_ERR_JSON_VALUE_NO_VALUE);
      } else {
        ret = OB_ERR_MISSING_JSON_VALUE;
        LOG_USER_ERROR(OB_ERR_MISSING_JSON_VALUE, "json_value");
      }
      LOG_WARN("json value seek result empty.", K(ret));
      break;
    }
    case JSN_VALUE_DEFAULT: {
      empty_res = empty_datum;
      break;
    }
    case JSN_VALUE_NULL: {
      is_null_result = true;
      break;
    }
    case JSN_VALUE_IMPLICIT: {
      if (lib::is_oracle_mode()) {
        ret = OB_ERR_JSON_VALUE_NO_VALUE;
        LOG_USER_ERROR(OB_ERR_JSON_VALUE_NO_VALUE);
        LOG_WARN("json value seek result empty.", K(ret));
        is_cover_by_error = true;
      } else {
        is_null_result = true;
      }
      break;
    }
    default: // empty_type from get_default_value has done range check, do nothing for default
      break;
  }
  return ret;
}

void ObExprJsonValue::get_error_option(int8_t error_type,
                    bool &is_null, bool &has_default_val)
{
	switch (error_type) {
	case JSN_VALUE_DEFAULT : {
      has_default_val = true;
      break;
    }
    case JSN_VALUE_NULL :
    case JSN_VALUE_IMPLICIT: {
      is_null = true;
      break;
    }
    case JSN_VALUE_ERROR : {
      break;
    }
    default: {
      break;
    }
  }
}

/*
template<>
void ObExprJsonValue::wrapper_set_error_result(
  const ObExpr &expr,
  ObEvalCtx &ctx,
  ObObj &res, int &ret, uint8_t &error_type,
  ObDatum *&error_val, ObVector<uint8_t> &mismatch_val,
  ObVector<uint8_t> &mismatch_type,
  uint8_t &is_type_cast,
  const ObAccuracy &accuracy,
  ObObjType dst_type,
  ObObjMeta& meta)
{
  bool has_lob_header = is_lob_storage(dst_type);
  ObTextStringObObjResult text_result(dst_type, nullptr, &res, has_lob_header);
  if (!try_set_error_val<ObObj>(expr, ctx, res, ret, error_type, error_val, mismatch_val, mismatch_type, is_type_cast, accuracy, dst_type, meta)) {
    text_result.set_result();
  }
}

template<>
void ObExprJsonValue::wrapper_set_error_result(
  const ObExpr &expr,
  ObEvalCtx &ctx,
  ObDatum &res, int &ret, uint8_t &error_type,
  ObDatum *&error_val, ObVector<uint8_t> &mismatch_val,
  ObVector<uint8_t> &mismatch_type,
  uint8_t &is_type_cast,
  const ObAccuracy &accuracy,
  ObObjType dst_type,
  ObObjMeta& meta)
{
  ObTextStringDatumResult text_result(expr.datum_meta_.type_, &expr, &ctx, &res);
  if (!try_set_error_val<ObDatum>(expr, ctx, res, ret, error_type, error_val, mismatch_val, mismatch_type, is_type_cast, accuracy, dst_type, meta)) {
    text_result.set_result();
  }
}

template<>
int ObExprJsonValue::wrapper_text_string_result(common::ObIAllocator *allocator,
                                                const ObExpr &expr,
                                                ObEvalCtx &ctx,
                                                ObString& result_value,
                                                uint8_t error_type,
                                                ObDatum *error_val,
                                                ObAccuracy &accuracy,
                                                ObObjType dst_type,
                                                ObDatum &res,
                                                ObVector<uint8_t> &mismatch_val,
                                                ObVector<uint8_t> &mismatch_type,
                                                uint8_t &is_type_cast,
                                                uint8_t ascii_type,
                                                ObObjMeta& res_meta)
{
  int ret = OB_SUCCESS;
  ObTextStringDatumResult text_result(expr.datum_meta_.type_, &expr, &ctx, &res);

  if (dst_type == ObJsonType) {
    if (OB_FAIL(text_result.init(result_value.length()))) {
      LOG_WARN("init lob result failed");
    } else if (OB_FAIL(text_result.append(result_value))) {
      LOG_WARN("failed to append realdata", K(ret), K(result_value), K(text_result));
    }
  } else {
    if (ascii_type == 0) {
      if (OB_FAIL(text_result.init(result_value.length()))) {
        LOG_WARN("init lob result failed");
      } else if (OB_FAIL(text_result.append(result_value))) {
        LOG_WARN("failed to append realdata", K(ret), K(result_value), K(text_result));
      }
    } else {
      char *buf = NULL;
      int64_t buf_len = result_value.length() * ObCharset::MAX_MB_LEN * 2;
      int64_t reserve_len = 0;
      int32_t length = 0;

      if (OB_FAIL(text_result.init(buf_len))) {
        LOG_WARN("init lob result failed");
      } else if (OB_FAIL(text_result.get_reserved_buffer(buf, reserve_len))) {
        LOG_WARN("fail to get reserved buffer", K(ret));
      } else if (reserve_len != buf_len) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get reserve len is invalid", K(ret), K(reserve_len), K(buf_len));
      } else if (OB_FAIL(ObJsonExprHelper::calc_asciistr_in_expr(result_value, expr.args_[0]->datum_meta_.cs_type_,
                                                                  expr.datum_meta_.cs_type_,
                                                                  buf, reserve_len, length))) {
        LOG_WARN("fail to calc unistr", K(ret));
      } else if (OB_FAIL(text_result.lseek(length, 0))) {
        LOG_WARN("text_result lseek failed", K(ret), K(text_result), K(length));
      }
    }
  }

  if (!try_set_error_val<ObDatum>(expr, ctx, res, ret, error_type, error_val, mismatch_val, mismatch_type, is_type_cast, accuracy, dst_type, res_meta)) {
    // old engine set same alloctor for wrapper, so we can use val without copy
    text_result.set_result();
  }

  return ret;
}

template<>
int ObExprJsonValue::wrapper_text_string_result(common::ObIAllocator *allocator,
                                                const ObExpr &expr,
                                                ObEvalCtx &ctx,
                                                ObString& result_value,
                                                uint8_t error_type,
                                                ObDatum *error_val,
                                                ObAccuracy &accuracy,
                                                ObObjType dst_type,
                                                ObObj &res,
                                                ObVector<uint8_t> &mismatch_val,
                                                ObVector<uint8_t> &mismatch_type,
                                                uint8_t &is_type_cast,
                                                uint8_t ascii_type,
                                                ObObjMeta& res_meta)
{
  int ret = OB_SUCCESS;
  bool has_lob_header = is_lob_storage(dst_type);
  ObTextStringObObjResult text_result(dst_type, nullptr, &res, has_lob_header);
  if (dst_type == ObJsonType) {
    if (OB_FAIL(text_result.init(result_value.length(), allocator))) {
      LOG_WARN("init lob result failed");
    } else if (OB_FAIL(text_result.append(result_value))) {
      LOG_WARN("failed to append realdata", K(ret), K(result_value), K(text_result));
    } else {
      text_result.set_result();
    }
  } else {
    if (ascii_type == 0) {
      if (OB_FAIL(text_result.init(result_value.length(), allocator))) {
        LOG_WARN("init lob result failed");
      } else if (OB_FAIL(text_result.append(result_value))) {
        LOG_WARN("failed to append realdata", K(ret), K(result_value), K(text_result));
      } else {
        text_result.set_result();
      }
    } else {
      char *buf = NULL;
      int64_t buf_len = result_value.length() * ObCharset::MAX_MB_LEN * 2;
      int64_t reserve_len = 0;
      int32_t length = 0;

      if (OB_FAIL(text_result.init(buf_len, allocator))) {
        LOG_WARN("init lob result failed");
      } else if (OB_FAIL(text_result.get_reserved_buffer(buf, reserve_len))) {
        LOG_WARN("fail to get reserved buffer", K(ret));
      } else if (reserve_len != buf_len) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get reserve len is invalid", K(ret), K(reserve_len), K(buf_len));
      } else if (OB_FAIL(ObJsonExprHelper::calc_asciistr_in_expr(result_value, expr.args_[0]->datum_meta_.cs_type_,
                                                                  expr.datum_meta_.cs_type_,
                                                                  buf, reserve_len, length))) {
        LOG_WARN("fail to calc unistr", K(ret));
      } else if (OB_FAIL(text_result.lseek(length, 0))) {
        LOG_WARN("text_result lseek failed", K(ret), K(text_result), K(length));
      }
    }
  }

  if (!try_set_error_val<ObObj>(expr, ctx, res, ret, error_type, error_val, mismatch_val, mismatch_type, is_type_cast, accuracy, dst_type, res_meta)) {
    // old engine set same alloctor for wrapper, so we can use val without copy
    text_result.set_result();
  }

  return ret;
}
*/

bool ObExprJsonValue::try_set_error_val(const ObExpr &expr,
                                        ObEvalCtx &ctx,
                                        ObDatum &res, int &ret,
                                        ObJsonExprParam* json_param,
                                        uint8_t &is_type_mismatch)
{
  bool has_set_res = true;
  bool is_null_res = false;
  bool set_default_val = false;
  int temp_ret = OB_SUCCESS;

  if (OB_FAIL(ret)) {
    if (lib::is_oracle_mode() && json_param->error_type_ == JSN_VALUE_DEFAULT) {
      if (!json_param->is_error_default_const_ || OB_ISNULL(json_param->error_val_)) {
        temp_ret = get_default_value(expr.args_[JSN_VAL_ERROR + 1], ctx,
                     json_param->accuracy_, &json_param->error_val_);
      }
    }
    if (temp_ret != OB_SUCCESS) {
      ret = temp_ret;
      LOG_WARN("failed to get error option.", K(temp_ret));
    } else {
      get_error_option(json_param->error_type_, is_null_res, set_default_val);
      if (lib::is_oracle_mode() && is_type_mismatch == 1) {
        get_mismatch_option(json_param->on_mismatch_,
                            json_param->on_mismatch_type_,
                            is_null_res,
                            set_default_val);
      }
    }
    if (temp_ret != OB_SUCCESS) {
    } else if (is_null_res) {
      res.set_null();
      ret = OB_SUCCESS;
    } else if (set_default_val) {
      if (OB_ISNULL(json_param->error_val_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get error val", K(ret));
      } else {
        res.set_datum(*json_param->error_val_);
        ret = OB_SUCCESS;
      }
    }
  } else {
    has_set_res = false;
  }
  return has_set_res;
}

void ObExprJsonValue::get_mismatch_option(ObIArray<int8_t> &mismatch_val,
                                         ObIArray<int8_t> &mismatch_type,
                                         bool &is_null_res,
                                         bool &set_default_val)
{
  bool mismatch_error = true;
  for(size_t i = 0; i < mismatch_val.count(); i++) {  // 目前不支持UDT，因此只考虑第一个参数中的 error 和 null。
    if (mismatch_val.at(i) == OB_JSON_ON_MISMATCH_ERROR) {
      mismatch_error = false;
    } else if (mismatch_val.at(i) == OB_JSON_ON_MISMATCH_NULL || mismatch_val.at(i) == OB_JSON_ON_MISMATCH_IGNORE) {
      is_null_res = true;
    }
  }
  if (mismatch_error) {
    if (is_null_res) {
      set_default_val = false;
    }
  } else {
    is_null_res = false;
    set_default_val = false;
  }
}

int ObExprJsonValue::get_on_mismatch(const ObExpr &expr,
                             ObEvalCtx &ctx,
                             uint8_t index,
                             bool &is_cover_by_error,
                             const ObAccuracy &accuracy,
                             ObIArray<int8_t> &val,
                             ObIArray<int8_t> &type)
{
  INIT_SUCC(ret);

  ObExpr *json_arg = NULL;
  ObObjType val_type;
  ObDatum *json_datum = NULL;

  uint32_t expr_count = expr.arg_cnt_;
  uint32_t pos = -1;

  for(uint32_t i = index; OB_SUCC(ret) && i < expr_count; i++) {
    json_arg = expr.args_[i];
    val_type = json_arg->datum_meta_.type_;
    if (OB_FAIL(json_arg->eval(ctx, json_datum))) {
      is_cover_by_error = false;
      LOG_WARN("eval json arg failed", K(ret));
    } else {
      int64_t option_type = json_datum->get_int();
      if (OB_FAIL(ObJsonUtil::set_mismatch_val(val, type, option_type, pos))) {
        LOG_WARN("fail to set mismatch value", K(ret));
      }
    }
  }
  return ret;
}

int ObExprJsonValue::get_default_value(ObExpr *expr,
                                      ObEvalCtx &ctx,
                                      const ObAccuracy &accuracy,
                                      ObDatum **default_value)
{
  INIT_SUCC(ret);
  ObObjType val_type = expr->datum_meta_.type_;
  ObDatum *json_datum = NULL;
  if (lib::is_mysql_mode()) {
    expr->extra_ &= ~CM_WARN_ON_FAIL; // make cast return error when fail
    expr->extra_ &= ~CM_NO_RANGE_CHECK; // make cast check range
    expr->extra_ &= ~CM_STRING_INTEGER_TRUNC; // make cast check range when string to uint
    expr->extra_ |= CM_ERROR_ON_SCALE_OVER; // make cast check presion and scale
    expr->extra_ |= CM_EXPLICIT_CAST; // make cast json fail return error
  }
  if (OB_FAIL(expr->eval(ctx, json_datum))) {
    LOG_WARN("eval json arg failed", K(ret));
  } else if (val_type == ObNullType || json_datum->is_null()) {
  } else if (OB_FAIL(check_default_val_accuracy<ObDatum>(accuracy, val_type, json_datum))) {
    LOG_WARN("failed check default value", K(ret));
  } else {
    *default_value = json_datum;
  }
  if (ret == OB_OPERATE_OVERFLOW) {
    if (val_type >= ObDateTimeType && val_type <= ObYearType) {
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "TIME DEFAULT", "json_value");
    } else if (val_type == ObNumberType || val_type == ObUNumberType || val_type == ObDecimalIntType) {
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "DECIMAL DEFAULT", "json_value");
    }
  }
  return ret;
}

int ObExprJsonValue::deal_item_method_in_seek(ObIJsonBase*& in,
                                              bool &is_null_result,
                                              ObJsonPath *j_path,common::ObIAllocator *allocator,
                                              uint8_t &is_type_mismatch)
{
  INIT_SUCC(ret);

  if (in->json_type() == ObJsonNodeType::J_OBJECT
      || in->json_type() == ObJsonNodeType::J_ARRAY) {
    ret = OB_ERR_JSON_VALUE_NO_SCALAR;
  } else if (j_path->is_last_func()) {
    ObJsonUtil::ObItemMethodValid eval_func_ = ObJsonUtil::get_item_method_cast_res_func(j_path, in);
    if (OB_ISNULL(eval_func_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("eval func can not be null", K(ret));
    } else if (OB_FAIL(((ObJsonUtil::ObItemMethodValid)(eval_func_))(in, is_null_result, allocator, is_type_mismatch))) {
      LOG_WARN("fail to deal item method and seek result", K(ret));
    }
  }
  if (OB_SUCC(ret) && in->json_type() == ObJsonNodeType::J_NULL) {
    is_null_result = true;
  }

  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprJsonValue, raw_expr) {
  int ret = OB_SUCCESS;
  SET_LOCAL_SYSVAR_CAPACITY(1);
  EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_COLLATION_CONNECTION);
  return ret;
}
}
}
