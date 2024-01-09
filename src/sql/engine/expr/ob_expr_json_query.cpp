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
 * This file is for func json_query.
 */

#define USING_LOG_PREFIX SQL_ENG

#include "ob_expr_json_query.h"
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
#include "ob_expr_json_value.h"
#include "lib/xml/ob_binary_aggregate.h"
#include "ob_expr_json_utils.h"
// from sql_parser_base.h
#define DEFAULT_STR_LENGTH -1
#define VARCHAR2_DEFAULT_LEN 4000

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprJsonQuery::ObExprJsonQuery(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_JSON_QUERY, N_JSON_QUERY, MORE_THAN_TWO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprJsonQuery::~ObExprJsonQuery()
{
}

int ObExprJsonQuery::calc_result_typeN(ObExprResType& type,
                                    ObExprResType* types_stack,
                                    int64_t param_num,
                                    ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  INIT_SUCC(ret);
  common::ObArenaAllocator allocator;
  if (OB_UNLIKELY(param_num != 11)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid param number", K(ret), K(param_num));
  } else {
    bool is_json_input = false;
    if (OB_FAIL(ObExprJsonValue::calc_input_type(types_stack[JSN_QUE_DOC], is_json_input))) {
      LOG_WARN("fail to calc input type", K(ret));
    } else if (types_stack[JSN_QUE_PATH].get_type() == ObNullType) { // json path : 1
      ret = OB_ERR_PATH_EXPRESSION_NOT_LITERAL;
      LOG_USER_ERROR(OB_ERR_PATH_EXPRESSION_NOT_LITERAL);
    } else if (ob_is_string_type(types_stack[JSN_QUE_PATH].get_type())) {
      if (types_stack[JSN_QUE_PATH].get_charset_type() != CHARSET_UTF8MB4) {
        types_stack[JSN_QUE_PATH].set_calc_collation_type(types_stack[JSN_QUE_PATH].get_collation_type());
      }
    } else {
      types_stack[JSN_QUE_PATH].set_calc_type(ObLongTextType);
      types_stack[JSN_QUE_PATH].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    }
    // returning type : 2
    ObExprResType dst_type;
    if (OB_SUCC(ret) && OB_FAIL(calc_returning_type(type, types_stack, type_ctx,
                                  dst_type, &allocator, is_json_input))) {
      LOG_WARN("fail to calc returning type", K(ret));
    }
    // truncate 3  , scalars  4, pretty  5, ascii  6, wrapper 7, error 8, empty 9, mismatch 10
    for (int64_t i = JSN_QUE_TRUNC; i < param_num && OB_SUCC(ret); ++i) {
      if (types_stack[i].get_type() == ObNullType) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param type is unexpected", K(types_stack[i].get_type()), K(i));
      } else if (types_stack[i].get_type() != ObIntType) {
        types_stack[i].set_calc_type(ObIntType);
      }
    }
    // ASCII clause
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObJsonExprHelper::parse_asc_option(types_stack[JSN_QUE_ASCII], types_stack[JSN_QUE_DOC], type, type_ctx))) {
        LOG_WARN("fail to parse asc option.", K(ret));
      }
    }
  }
  return ret;
}

int ObExprJsonQuery::calc_returning_type(ObExprResType& type,
                                        ObExprResType* types_stack,
                                        ObExprTypeCtx& type_ctx,
                                        ObExprResType& dst_type,
                                        common::ObIAllocator *allocator,
                                        bool is_json_input)
{
  INIT_SUCC(ret);
  if (types_stack[JSN_QUE_RET].get_type() == ObNullType) {
    ObString j_path_text(types_stack[JSN_QUE_PATH].get_param().get_string().length(), types_stack[JSN_QUE_PATH].get_param().get_string().ptr());
    ObJsonPath j_path(j_path_text, allocator);

    if (j_path_text.length() == 0) {
      dst_type.set_type(ObObjType::ObVarcharType);
      dst_type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
      dst_type.set_full_length(VARCHAR2_DEFAULT_LEN, 1);
    } else if (OB_FAIL(ObJsonExprHelper::convert_string_collation_type(
                                                types_stack[JSN_QUE_PATH].get_collation_type(),
                                                CS_TYPE_UTF8MB4_BIN,
                                                allocator,
                                                j_path_text,
                                                j_path_text))) {
      LOG_WARN("convert string memory failed", K(ret), K(j_path_text));
    } else if (OB_FAIL(j_path.parse_path())) {
      ret = OB_ERR_JSON_PATH_EXPRESSION_SYNTAX_ERROR;
      LOG_USER_ERROR(OB_ERR_JSON_PATH_EXPRESSION_SYNTAX_ERROR, j_path_text.length(), j_path_text.ptr());
    } else if (is_json_input && !j_path.is_last_func()) {
      dst_type.set_type(ObObjType::ObJsonType);
      dst_type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    } else {
      dst_type.set_type(ObObjType::ObVarcharType);
      dst_type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
      dst_type.set_full_length(VARCHAR2_DEFAULT_LEN, 1);
    }
  } else if (OB_FAIL(ObJsonExprHelper::get_cast_type(types_stack[JSN_QUE_RET], dst_type))) {
    LOG_WARN("get cast dest type failed", K(ret));
  } else if (dst_type.get_type() != ObVarcharType
            && dst_type.get_type() != ObLongTextType
            && dst_type.get_type() != ObJsonType) {
    ret = OB_ERR_INVALID_DATA_TYPE_RETURNING;
    LOG_USER_ERROR(OB_ERR_INVALID_DATA_TYPE_RETURNING);
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObJsonExprHelper::set_dest_type(types_stack[JSN_QUE_DOC], type, dst_type, type_ctx))) {
      LOG_WARN("set dest type failed", K(ret));
    } else {
      type.set_calc_collation_type(type.get_collation_type());
    }
  }
  return ret;
}

int ObExprJsonQuery::extract_plan_cache_param(const ObExprJsonQueryParamInfo *info, ObJsonExprParam& json_param)
{
  INIT_SUCC(ret);
  json_param.truncate_ = info->truncate_;
  json_param.empty_type_ = info->empty_type_;
  json_param.error_type_ = info->error_type_;
  json_param.ascii_type_ = info->ascii_type_;
  json_param.json_path_ = info->j_path_;
  json_param.is_init_from_cache_ = true;
  json_param.scalars_type_ = info->scalars_type_;
  json_param.pretty_type_ = info->pretty_type_;
  json_param.wrapper_ = info->wrapper_;

  if (OB_FAIL(json_param.on_mismatch_.push_back(info->on_mismatch_.at(0)))) {
    LOG_WARN("fail to push node to mismatch type", K(ret));
  }
  return ret;
}

int ObExprJsonQuery::eval_json_query(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  ObDatum *json_datum = NULL;
  ObExpr *json_arg = expr.args_[JSN_QUE_PATH];
  ObObjType type = json_arg->datum_meta_.type_;
  bool is_cover_by_error = true;
  bool is_null_result = false;
  uint8_t is_type_mismatch = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  ObJsonBin st_json(&temp_allocator);
  ObIJsonBase *j_base = &st_json;
  ObIJsonBase *jb_empty = NULL;
  int64_t dst_len = OB_MAX_TEXT_LENGTH;
  int8_t use_wrapper = 0;
  bool is_json_arr = false;
  bool is_json_obj = false;
  ObJsonSeekResult hits;
  ObJsonBin res_json(&temp_allocator);
  hits.res_point_ = &res_json;

  // get context first
  ObJsonParamCacheCtx ctx_cache(&temp_allocator);
  ObJsonParamCacheCtx* param_ctx = ObJsonExprHelper::get_param_cache_ctx(expr.expr_ctx_id_, &ctx.exec_ctx_);
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(ObMultiModeExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session()), "JSONModule"));
  if (OB_ISNULL(param_ctx)) {
    param_ctx = &ctx_cache;
  }

  // add version protection, as lower version has handle input in a defference way
  if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_2_2_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("json query raw expr number has change in 4.2.2 version", K(ret));
  } else if (param_ctx->is_first_exec_ && OB_FAIL(init_ctx_var(param_ctx, expr))) {
    is_cover_by_error = false;
    LOG_WARN("fail to init param ctx", K(ret));
  } else if (OB_ISNULL(param_ctx->json_param_.json_path_)
              && OB_FAIL(ObJsonUtil::get_json_path(expr.args_[JSN_QUE_PATH], ctx, // parse json path
                                        is_null_result, param_ctx,
                                        temp_allocator, is_cover_by_error))) { // ctx_cache->path_cache_
    LOG_WARN("get_json_path failed", K(ret));
  } else if (param_ctx->is_first_exec_
            && OB_FAIL(get_clause_param_value(expr, ctx, &param_ctx->json_param_, dst_len,
                                              is_cover_by_error))) {
                       // get clause param value, set into param_ctx
    LOG_WARN("fail to parse clause value", K(ret));
  } else if (OB_FAIL(ObJsonUtil::get_json_doc(expr.args_[JSN_QUE_DOC], ctx, temp_allocator,
                                              j_base, is_null_result,
                                              is_cover_by_error,  true))) { // parse json doc
    LOG_WARN("get_json_doc failed", K(ret));
  } else if (param_ctx->json_param_.json_path_ == nullptr) {//  do seek
    is_cover_by_error = false;
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("json path parse fail", K(ret));
  } else if (!is_null_result
              && OB_FAIL(ObExprJsonQuery::doc_do_seek(j_base, &param_ctx->json_param_, hits, use_wrapper,
                                is_cover_by_error, is_null_result,
                                is_json_arr, is_json_obj))) {
    LOG_WARN("fail to seek result", K(ret));
  }

  // fill output
  if (OB_FAIL(ret)) {
    if (is_cover_by_error) {
      if (!try_set_error_val(&temp_allocator, ctx, &param_ctx->json_param_, expr, res, ret)) {
        LOG_WARN("set error val fail", K(ret));
      }
    }
    LOG_WARN("json_query failed", K(ret));
  } else if (is_null_result) {
    res.set_null();
  } else if (param_ctx->json_param_.on_mismatch_[0] == JSN_QUERY_MISMATCH_DOT
              && hits.size() == 1
              && param_ctx->json_param_.dst_type_ != ObJsonType) { // dot notation
    ObCollationType in_coll_type = expr.args_[JSN_QUE_DOC]->datum_meta_.cs_type_;
    ObCollationType dst_coll_type = expr.datum_meta_.cs_type_;
    param_ctx->json_param_.error_type_ = JSN_QUERY_NULL;
    ObJsonCastParam cast_param(param_ctx->json_param_.dst_type_, in_coll_type, dst_coll_type, 0);
    ret = ObJsonUtil::cast_to_res(&temp_allocator, ctx, hits[0],
                  param_ctx->json_param_.accuracy_, cast_param, res, is_type_mismatch);
    if (OB_FAIL(ret)) {
      try_set_error_val(&temp_allocator, ctx, &param_ctx->json_param_, expr, res, ret);
    } else if (OB_FAIL(ObJsonUtil::set_lob_datum(&temp_allocator, expr, ctx, param_ctx->json_param_.dst_type_, 0, res))) {
      LOG_WARN("fail to set lob datum from string val", K(ret));
    }
  } else if (use_wrapper == 1) {
    size_t hit_size = hits.size();
    ObJsonArray j_arr_res(&temp_allocator);
    ObIJsonBase *jb_res = NULL;
    jb_res = &j_arr_res;
    // adaptive json binary append
    if (OB_NOT_NULL(param_ctx->json_param_.json_path_) && param_ctx->json_param_.json_path_->is_last_func()) {
      if (OB_FAIL(append_node_into_res(jb_res, param_ctx->json_param_.json_path_,
                                      hits, &temp_allocator))) {
        LOG_WARN("fail to tree apeend node", K(ret));
      }
    } else if (append_binary_node_into_res(jb_res, param_ctx->json_param_.json_path_,
                                      hits, &temp_allocator)) {
      LOG_WARN("fail to apeend binary node", K(ret));
    }

    if (try_set_error_val(&temp_allocator, ctx, &param_ctx->json_param_, expr, res, ret)) {
    } else if (OB_FAIL(set_result(&param_ctx->json_param_, jb_res, &temp_allocator,
                                ctx, expr, res))) {
      LOG_WARN("result set fail", K(ret));
    }
  } else if (is_json_arr) {
    ObJsonArray j_arr_var(&temp_allocator);
    jb_empty = &j_arr_var;
    ret = set_result(&param_ctx->json_param_, jb_empty, &temp_allocator, ctx, expr, res);
  } else if (is_json_obj) {
    ObJsonObject j_obj_var(&temp_allocator);
    jb_empty = &j_obj_var;
    ret = set_result(&param_ctx->json_param_, jb_empty, &temp_allocator, ctx, expr, res);
  } else {
    ret = set_result(&param_ctx->json_param_, hits[0], &temp_allocator, ctx, expr, res);
  }
  if (OB_SUCC(ret)) {
    param_ctx->is_first_exec_ = false;
  }
  return ret;
}

int ObExprJsonQuery::init_ctx_var(ObJsonParamCacheCtx*& param_ctx, const ObExpr &expr)
{
  INIT_SUCC(ret);
  // init json path flag
  param_ctx->is_json_path_const_ = expr.args_[JSN_QUE_PATH]->is_const_expr();
  const ObExprJsonQueryParamInfo *info
                  = static_cast<ObExprJsonQueryParamInfo *>(expr.extra_info_);
  if (OB_NOT_NULL(info)
      && OB_FAIL(extract_plan_cache_param(info, param_ctx->json_param_))) {
    LOG_WARN("fail to extract param from plan cache", K(ret));
  }
  return ret;
}

int ObExprJsonQuery::append_node_into_res(ObIJsonBase*& jb_res,
                                          ObJsonPath* j_path,
                                          ObJsonSeekResult &hits,
                                          common::ObIAllocator *allocator)
{
  INIT_SUCC(ret);
  size_t hit_size = hits.size();
  ObJsonNode *j_node = NULL;
  ObIJsonBase *jb_node = NULL;
  for (size_t i = 0; OB_SUCC(ret) && i < hit_size; i++) {
    bool is_null_res = false;
    if (OB_FAIL(deal_item_method_special_case(j_path, hits, is_null_res, i, true))) {
      LOG_WARN("fail to deal item method special case", K(ret));
    } else if (is_null_res) {
      void* buf = NULL;
      buf = allocator->alloc(sizeof(ObJsonNull));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        jb_node = (ObJsonNull*)new(buf)ObJsonNull(true);
      }
    } else if (OB_FAIL(ObJsonBaseFactory::transform(allocator, hits[i], ObJsonInType::JSON_TREE, jb_node))) { // to tree
      LOG_WARN("fail to transform to tree", K(ret), K(i), K(*(hits[i])));
    }
    if (OB_SUCC(ret)) {
      j_node = static_cast<ObJsonNode *>(jb_node);
      if (OB_ISNULL(j_node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("json node input is null", K(ret), K(i), K(is_null_res), K(hits[i]));
      } else if (OB_FAIL(jb_res->array_append(j_node->clone(allocator)))) {
        LOG_WARN("result array append failed", K(ret), K(i), K(*j_node));
      }
    }
  }
  return ret;
}

int ObExprJsonQuery::append_binary_node_into_res(ObIJsonBase*& jb_res,
                                                 ObJsonPath* j_path,
                                                 ObJsonSeekResult &hits,
                                                 common::ObIAllocator *allocator)
{
  INIT_SUCC(ret);
  size_t hit_size = hits.size();
  ObJsonBin *j_node = NULL;
  ObIJsonBase *jb_node = NULL;
  ObStringBuffer value(allocator);
  ObBinAggSerializer bin_agg(allocator, AGG_JSON, static_cast<uint8_t>(ObJsonNodeType::J_ARRAY));
  for (size_t i = 0; OB_SUCC(ret) && i < hit_size; i++) {
    bool is_null_res = false;
    if (OB_FAIL(deal_item_method_special_case(j_path, hits, is_null_res, i, true))) {
      LOG_WARN("fail to deal item method special case", K(ret));
    } else if (is_null_res) {
      void* buf = NULL;
      buf = allocator->alloc(sizeof(ObJsonNull));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        jb_node = (ObJsonNull*)new(buf)ObJsonNull(true);
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObJsonBaseFactory::transform(allocator, is_null_res ? jb_node : hits[i], ObJsonInType::JSON_BIN, jb_node))) { // to binary
      LOG_WARN("fail to transform to tree", K(ret), K(i), K(*(hits[i])));
    } else {
      j_node = static_cast<ObJsonBin *>(jb_node);
      ObString key;
      if (OB_ISNULL(j_node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("json node input is null", K(ret), K(i), K(is_null_res), K(hits[i]));
      } else if (OB_FAIL(bin_agg.append_key_and_value(key, value, j_node))) {
        LOG_WARN("failed to append key and value", K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(bin_agg.serialize())) {
    LOG_WARN("failed to serialize bin agg.", K(ret));
  } else if (OB_FAIL(ObJsonBaseFactory::get_json_base(allocator, bin_agg.get_buffer()->string(), ObJsonInType::JSON_BIN, ObJsonInType::JSON_BIN, jb_res, ObJsonParser::JSN_RELAXED_FLAG))) {
    LOG_WARN("failed to get json base.", K(ret));
  }
  return ret;
}

int ObExprJsonQuery::check_params_valid(const ObExpr &expr,
                                        ObJsonExprParam* json_param,
                                        bool &is_cover_by_error)
{
  INIT_SUCC(ret);
  int8_t JSON_QUERY_EXPR = 1;
  ObExpr* json_arg = expr.args_[JSN_QUE_DOC];
  ObObjType val_type = json_arg->datum_meta_.type_;
  ObExpr *json_arg_ret = expr.args_[JSN_QUE_RET];
  ObObjType ret_type = json_arg_ret->datum_meta_.type_;
  // check conflict between item method and returning type.
  if (!(val_type == ObJsonType && ret_type == ObNullType)
      && json_param->json_path_->is_last_func()
      && OB_FAIL( ObJsonExprHelper::check_item_func_with_return(json_param->json_path_->get_last_node_type(),
                  json_param->dst_type_, expr.datum_meta_.cs_type_, JSON_QUERY_EXPR))) {
    is_cover_by_error = false;
    LOG_WARN("check item func with return type fail", K(ret));
  } else if (json_param->dst_type_ != ObVarcharType
              && json_param->dst_type_ != ObLongTextType
              && json_param->dst_type_ != ObJsonType) {
    is_cover_by_error = false;
    ret = OB_ERR_INVALID_DATA_TYPE_RETURNING;
    LOG_USER_ERROR(OB_ERR_INVALID_DATA_TYPE_RETURNING);
  } else if (OB_FAIL(check_item_method_valid_with_wrapper(json_param->json_path_, json_param->wrapper_))) {
    is_cover_by_error = false;
    LOG_WARN("fail to check item method with wrapper", K(ret));
  } else if ((expr.datum_meta_.cs_type_ == CS_TYPE_BINARY || json_param->dst_type_ == ObJsonType) && (json_param->pretty_type_ > 0 || json_param->ascii_type_ > 0)) {
    is_cover_by_error = false;
    ret = OB_ERR_NON_TEXT_RET_NOTSUPPORT;
    LOG_WARN("ASCII or PRETTY not supported for non-textual return data type", K(ret));
  }
  return ret;
}

int ObExprJsonQuery::get_clause_param_value(const ObExpr &expr,
                                            ObEvalCtx &ctx,
                                            ObJsonExprParam* json_param,
                                            int64_t &dst_len,
                                            bool &is_cover_by_error)
{
  INIT_SUCC(ret);
  ObArray<int8_t> param_vec;
  int8_t val = 0;
  // returning type
  ObExpr* json_arg = expr.args_[JSN_QUE_DOC];
  ObObjType type = json_arg->datum_meta_.type_;
  ObExpr *json_arg_ret = expr.args_[JSN_QUE_RET];
  ObObjType val_type = json_arg_ret->datum_meta_.type_;
  if (val_type == ObNullType) {
    if (ob_is_string_type(type) || json_param->json_path_->is_last_func()) {
      json_param->dst_type_ = ObVarcharType;
      json_param->accuracy_.set_full_length(VARCHAR2_DEFAULT_LEN, 1, lib::is_oracle_mode());
    } else {
      json_param->dst_type_ = ObJsonType;
      json_param->accuracy_.set_length(0);
    }
  } else {
    ret = ObJsonUtil::get_accuracy(expr, ctx, json_param->accuracy_, json_param->dst_type_, is_cover_by_error);
  }
  // truncate 3, scalars 4, pretty 5, ascii 6, wrapper 7, error 8, empty 9, mismatch 10
  for (size_t i = JSN_QUE_TRUNC; OB_SUCC(ret) && i <= JSN_QUE_MISMATCH; i ++) {
    if (OB_FAIL(ObJsonExprHelper::get_clause_opt(expr.args_[i], ctx, val))) {
      LOG_WARN("fail to get clause option", K(ret));
    } else if (OB_FAIL(param_vec.push_back(val))) {
      LOG_WARN("fail to push val into array", K(ret));
    }
  }
  if (OB_FAIL(ret) && is_cover_by_error) {
    is_cover_by_error = false;
    ret = ObJsonExprHelper::get_clause_opt(expr.args_[JSN_QUE_ERROR], ctx, json_param->error_type_);
  } else if (OB_FAIL(ret)) {
  } else if (param_vec.size() == 8) {
    json_param->truncate_ = param_vec[JSN_QUE_TRUNC_OPT];
    json_param->scalars_type_ = param_vec[JSN_QUE_SCALAR_OPT];
    json_param->pretty_type_ = param_vec[JSN_QUE_PRETTY_OPT];
    json_param->ascii_type_ = param_vec[JSN_QUE_ASCII_OPT];
    json_param->wrapper_ = param_vec[JSN_QUE_WRAPPER_OPT];
    json_param->error_type_ = param_vec[JSN_QUE_ERROR_OPT];
    json_param->empty_type_ = param_vec[JSN_QUE_EMPTY_OPT];
    json_param->on_mismatch_.push_back(param_vec[JSN_QUE_MISMATCH_OPT]);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get param value", K(ret));
  }
  // mismatch      // if mismatch_type == 3  from dot notation
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(json_param->on_mismatch_type_.push_back(JsnValueMisMatch::OB_JSON_TYPE_IMPLICIT))) {
    LOG_WARN("push back failed", K(ret));
  } else if (OB_FAIL(check_params_valid(expr, json_param, is_cover_by_error))) {
    LOG_WARN("fail to check clause", K(ret));
  }
  return ret;
}

int ObExprJsonQuery::doc_do_seek(ObIJsonBase* j_base,
                                  ObJsonExprParam *json_param,
                                  ObJsonSeekResult &hits,
                                  int8_t &use_wrapper,
                                  bool &is_cover_by_error,
                                  bool &is_null_result,
                                  bool& is_json_arr,
                                  bool& is_json_obj)
{
  INIT_SUCC(ret);
  if (OB_FAIL(j_base->seek(*json_param->json_path_, json_param->json_path_->path_node_cnt(), true, false, hits))) {
    if (ret == OB_ERR_JSON_PATH_EXPRESSION_SYNTAX_ERROR) {
      is_cover_by_error = false;
    } else if (ret == OB_ERR_DOUBLE_TRUNCATED) {
      ret = OB_ERR_CONVERSION_FAIL;
    }
    LOG_WARN("json seek failed", K(ret));
  } else if (hits.size() == 1) {
    if (json_param->on_mismatch_[0] == JSN_QUERY_MISMATCH_DOT) {
      if (hits[0]->json_type() == ObJsonNodeType::J_NULL && hits[0]->is_real_json_null(hits[0]) && json_param->dst_type_ != ObJsonType) {
        is_null_result = true;
      }
    } else {
      if (OB_FAIL(get_single_obj_wrapper(json_param->wrapper_, use_wrapper, hits[0]->json_type(), json_param->scalars_type_))) {
        is_cover_by_error = true;
        LOG_WARN("error occur in wrapper type");
      } else if (use_wrapper == 1) { // do nothing
      } else if (OB_FAIL(deal_item_method_special_case(json_param->json_path_, hits, is_null_result,
                                                     0, false))) {
        LOG_WARN("fail to deal special case", K(ret));
      }
    }
  } else if (hits.size() == 0) {
    if (OB_SUCC(ret) && OB_FAIL(get_empty_option(is_cover_by_error,
                                  json_param->empty_type_,
                                  is_null_result, is_json_arr, is_json_obj))) {
      LOG_WARN("get empty type", K(ret));
    } else if (is_json_arr || is_json_obj) {
      use_wrapper = 0;
    }
  } else if (hits.size() > 1) {
    // return val decide by wrapper option
    if (OB_FAIL(get_multi_scalars_wrapper_type(json_param->wrapper_, use_wrapper))) {
      is_cover_by_error = true;
      LOG_WARN("error occur in wrapper type", K(ret), K(hits.size()));
    }
  }
  return ret;
}

int ObExprJsonQuery::deal_item_method_special_case(ObJsonPath* j_path,
                                                   ObJsonSeekResult &hits,
                                                   bool &is_null_result,
                                                   size_t pos,
                                                   bool use_wrapper)
{
  INIT_SUCC(ret);
  if (hits[pos]->json_type() == ObJsonNodeType::J_NULL && !hits[pos]->is_real_json_null(hits[pos])) {
    is_null_result = true;
  } else if (!use_wrapper && j_path->is_last_func() && j_path->path_node_cnt() == 1) { // do nothing
  } else if (j_path->get_last_node_type() == JPN_LENGTH && !(hits[pos]->json_type() == ObJsonNodeType::J_UINT
            && ((ObJsonUint *)hits[pos])->get_is_string_length())) { // distinct uint and length()
    is_null_result = true;
  } else if (ObJsonUtil::get_query_item_method_null_option(j_path, hits[pos]) == 1) {
    is_null_result = true;
  }
  return ret;
}

int ObExprJsonQuery::check_item_method_valid_with_wrapper(ObJsonPath *j_path, int8_t wrapper_type)
{
  INIT_SUCC(ret);
  if (OB_SUCC(ret) && j_path->is_last_func()
      && ObJsonUtil::is_number_item_method(j_path)
      && (wrapper_type == JSN_QUERY_WITHOUT_WRAPPER
          || wrapper_type == JSN_QUERY_WITHOUT_ARRAY_WRAPPER
          || wrapper_type == JSN_QUERY_WRAPPER_IMPLICIT)) {
    ret = OB_ERR_WITHOUT_ARR_WRAPPER;  // result cannot be returned without array wrapper
    LOG_WARN("result cannot be returned without array wrapper.", K(ret), K(j_path->get_last_node_type()), K(wrapper_type));
  }
  return ret;
}

int ObExprJsonQuery::set_result(ObJsonExprParam* json_param,
                                ObIJsonBase *jb_res,
                                common::ObIAllocator *allocator,
                                ObEvalCtx &ctx,
                                const ObExpr &expr,
                                ObDatum &res) {
  INIT_SUCC(ret);
  uint8_t is_type_mismatch = 0;
  ObCollationType in_coll_type = expr.args_[0]->datum_meta_.cs_type_;
  ObCollationType dst_coll_type = expr.datum_meta_.cs_type_;
  ObJsonCastParam cast_param(json_param->dst_type_, in_coll_type, dst_coll_type, json_param->ascii_type_);
  cast_param.is_quote_ = true;
  cast_param.is_trunc_ = json_param->truncate_;
  cast_param.is_pretty_ = json_param->pretty_type_;
  cast_param.rt_expr_ = &expr;
  if (OB_FAIL(ObJsonUtil::cast_to_res(allocator, ctx, jb_res,
          json_param->accuracy_, cast_param, res, is_type_mismatch))) {
    if (ret == OB_OPERATE_OVERFLOW) {
      if (!try_set_error_val(allocator, ctx, json_param, expr, res, ret)) {
        LOG_WARN("set error val fail", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(ObJsonUtil::set_lob_datum(allocator, expr, ctx, json_param->dst_type_, json_param->ascii_type_, res))) {
    LOG_WARN("fail to set lob datum", K(ret));
  }
  return ret;
}

int ObExprJsonQuery::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  INIT_SUCC(ret);
  ObIAllocator &alloc = *expr_cg_ctx.allocator_;
  ObExprJsonQueryParamInfo* info
          = OB_NEWx(ObExprJsonQueryParamInfo, (&alloc), alloc, T_FUN_SYS_JSON_QUERY);
  if (OB_ISNULL(info)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else if (OB_FAIL(info->init_jsn_query_expr_param(alloc, expr_cg_ctx, &raw_expr))) {
    ret = OB_SUCCESS;  // not use plan cache
  } else {
    rt_expr.extra_info_ = info;
  }
  rt_expr.eval_func_ = eval_json_query;
  return ret;
}

int ObExprJsonQueryParamInfo::init_jsn_query_expr_param(ObIAllocator &alloc, ObExprCGCtx &op_cg_ctx, const ObRawExpr* raw_expr)
{
  INIT_SUCC(ret);
  ObExecContext *exec_ctx = op_cg_ctx.session_->get_cur_exec_ctx();
  const ObRawExpr *path = raw_expr->get_param_expr(JSN_QUE_PATH);
  ObObj const_data;
  ObArray<int8_t> param_vec;
  uint32_t pos = -1;
  // parse clause node
  // truncate 3, scalars 4, pretty 5, ascii 6, wrapper 7, error 8, empty 9, mismatch 10
  for (int64_t i = JSN_QUE_TRUNC; OB_SUCC(ret) && i <= JSN_QUE_MISMATCH; i ++) {
    if (OB_FAIL(get_int_val_from_raw(alloc, exec_ctx, raw_expr->get_param_expr(i), const_data))) {
      LOG_WARN("failed to calc offset expr", K(ret));
    } else if (OB_FAIL(param_vec.push_back(const_data.get_tinyint()))) {
      LOG_WARN("fail to push val into array", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    truncate_ = param_vec[JSN_QUE_TRUNC_OPT];
    scalars_type_ = param_vec[JSN_QUE_SCALAR_OPT];
    pretty_type_ = param_vec[JSN_QUE_PRETTY_OPT];
    ascii_type_ = param_vec[JSN_QUE_ASCII_OPT];
    wrapper_ = param_vec[JSN_QUE_WRAPPER_OPT];
    error_type_ = param_vec[JSN_QUE_ERROR_OPT];
    empty_type_ = param_vec[JSN_QUE_EMPTY_OPT];
  }
  // parse mismatch 1. init array 2. push_back node
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(on_mismatch_.init(1))) { // mismatch size == 1
    LOG_WARN("fail to init mismatch array", K(ret));
  } else if (OB_FAIL(on_mismatch_.push_back(param_vec[JSN_QUE_MISMATCH_OPT]))) {
    LOG_WARN("fail to push node into mismatch array", K(ret));
  } else if (OB_FAIL(ObJsonUtil::init_json_path(alloc, op_cg_ctx, path, *this))) {  // init json path
    LOG_WARN("fail to init path from str", K(ret));
  }
  return ret;
}

int ObExprJsonQuery::get_empty_option(bool &is_cover_by_error, int8_t empty_type,
                                      bool &is_null_result, bool &is_json_arr,
                                      bool &is_json_obj)
{
  INIT_SUCC(ret);
  switch (empty_type) {
    case JSN_QUERY_IMPLICIT: {
	is_cover_by_error = true;
      ret = OB_ERR_JSON_VALUE_NO_VALUE;
      LOG_USER_ERROR(OB_ERR_JSON_VALUE_NO_VALUE);
      LOG_WARN("json value seek result empty.", K(ret));
      break;
    }
    case JSN_QUERY_ERROR: {
      is_cover_by_error = false;
      ret = OB_ERR_JSON_VALUE_NO_VALUE;
      LOG_USER_ERROR(OB_ERR_JSON_VALUE_NO_VALUE);
      LOG_WARN("json value seek result empty.", K(ret));
      break;
    }
    case JSN_QUERY_EMPTY_OBJECT: {
      is_json_obj = true;
      break;
    }
    case JSN_QUERY_NULL: {
      is_null_result = true;
      break;
    }
    case JSN_QUERY_EMPTY:
    case JSN_QUERY_EMPTY_ARRAY: {
      is_json_arr = true;
      break;
    }
    default: // empty_type from get_on_empty_or_error has done range check, do nothing for default
      break;
  }
  return ret;
}

int ObExprJsonQuery::get_single_obj_wrapper(int8_t wrapper_type, int8_t &use_wrapper, ObJsonNodeType in_type, int8_t scalars_type)
{
  INIT_SUCC(ret);
  switch (wrapper_type) {
    case JSN_QUERY_WITHOUT_WRAPPER:
    case JSN_QUERY_WITHOUT_ARRAY_WRAPPER:
    case JSN_QUERY_WRAPPER_IMPLICIT: {
      if ((in_type != ObJsonNodeType::J_OBJECT &&  in_type != ObJsonNodeType::J_ARRAY
          && scalars_type == JSN_QUERY_SCALARS_DISALLOW)) {
        ret = OB_ERR_WITHOUT_ARR_WRAPPER;  // result cannot be returned without array wrapper
        LOG_USER_ERROR(OB_ERR_WITHOUT_ARR_WRAPPER);
        LOG_WARN("result cannot be returned without array wrapper.", K(ret));
      }
      break;
    }
    case JSN_QUERY_WITH_WRAPPER:
    case JSN_QUERY_WITH_ARRAY_WRAPPER:
    case JSN_QUERY_WITH_UNCONDITIONAL_WRAPPER:
    case JSN_QUERY_WITH_UNCONDITIONAL_ARRAY_WRAPPER: {
      use_wrapper = 1;
      break;
    }
    case JSN_QUERY_WITH_CONDITIONAL_WRAPPER:
    case JSN_QUERY_WITH_CONDITIONAL_ARRAY_WRAPPER: {
      if (in_type != ObJsonNodeType::J_OBJECT &&  in_type != ObJsonNodeType::J_ARRAY && scalars_type == JSN_QUERY_SCALARS_DISALLOW ) {
        use_wrapper = 1;
      }
      break;
    }
    default:  // error_type from get_on_empty_or_error has done range check, do nothing for default
      break;
  }
  return ret;
}

int ObExprJsonQuery::get_multi_scalars_wrapper_type(int8_t wrapper_type, int8_t &use_wrapper)
{
  INIT_SUCC(ret);
  switch (wrapper_type) {
    case JSN_QUERY_WITHOUT_WRAPPER:
    case JSN_QUERY_WITHOUT_ARRAY_WRAPPER:
    case JSN_QUERY_WRAPPER_IMPLICIT: {
      ret = OB_ERR_WITHOUT_ARR_WRAPPER;  // result cannot be returned without array wrapper
      LOG_USER_ERROR(OB_ERR_WITHOUT_ARR_WRAPPER);
      LOG_WARN("result cannot be returned without array wrapper.", K(ret), K(wrapper_type));
      break;
    }
    case JSN_QUERY_WITH_WRAPPER:
    case JSN_QUERY_WITH_ARRAY_WRAPPER:
    case JSN_QUERY_WITH_UNCONDITIONAL_WRAPPER:
    case JSN_QUERY_WITH_UNCONDITIONAL_ARRAY_WRAPPER: {
      use_wrapper = 1;
      break;
    }
    case JSN_QUERY_WITH_CONDITIONAL_WRAPPER:
    case JSN_QUERY_WITH_CONDITIONAL_ARRAY_WRAPPER: {
      use_wrapper = 1;
      break;
    }
    default:  // error_type from get_on_empty_or_error has done range check, do nothing for default
      break;
  }
  return ret;
}

int ObExprJsonQuery::get_error_option(int8_t &error_type, ObIJsonBase *&error_val, ObIJsonBase *jb_arr, ObIJsonBase *jb_obj, bool &is_null) {
  INIT_SUCC(ret);
	if (error_type == JSN_QUERY_EMPTY || error_type == JSN_QUERY_EMPTY_ARRAY) {
    error_val = jb_arr;
    is_null = false;
  } else if (error_type == JSN_QUERY_EMPTY_OBJECT) {
    error_val = jb_obj;
    is_null = false;
  } else if (error_type == JSN_QUERY_NULL || error_type == JSN_QUERY_IMPLICIT) {
    is_null = true;
  }
  return ret;
}

int ObExprJsonQuery::get_mismatch_option(int8_t &mismatch_type, int &ret) {
  int t_ret = OB_SUCCESS;
  if (mismatch_type == JSN_QUERY_MISMATCH_ERROR) {
    t_ret = ret;
  }
  return t_ret;
}

bool ObExprJsonQuery::try_set_error_val(common::ObIAllocator *allocator,
                                        ObEvalCtx &ctx,
                                        ObJsonExprParam* json_param,
                                        const ObExpr &expr,
                                        ObDatum &res,
                                        int &ret)
{
  bool has_set_res = true;
  bool mismatch_error = true;
  bool is_null = false;
  ObIJsonBase* j_base = NULL;
  ObJsonArray j_arr_res(allocator);
  ObIJsonBase *jb_arr = NULL;
  jb_arr = &j_arr_res;
  ObJsonObject j_obj_res(allocator);
  ObIJsonBase *jb_obj = NULL;
  jb_obj = &j_obj_res;

  if (OB_FAIL(ret)) {
    if (json_param->error_type_ == JSN_QUERY_ERROR) {
    } else if (OB_FAIL(get_error_option(json_param->error_type_, j_base, jb_arr, jb_obj, is_null))) {
      LOG_WARN("fail to get error clause", K(ret));
    } else if (is_null) {
      res.set_null();
    } else if (OB_FAIL(set_result(json_param, j_base, allocator, ctx, expr, res))) {
      LOG_WARN("result set fail", K(ret));
    }
  } else {
    has_set_res = false;
  }
  return has_set_res;
}

} // sql
} // oceanbase
