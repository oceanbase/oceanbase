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
#include "sql/engine/expr/ob_expr_cast.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "lib/oblog/ob_log_module.h"
#include "ob_expr_json_func_helper.h"
#include "ob_expr_json_value.h"
// from sql_parser_base.h
#define DEFAULT_STR_LENGTH -1

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

#define GET_SESSION()                                           \
  ObBasicSessionInfo *session = ctx.exec_ctx_.get_my_session(); \
  if (OB_ISNULL(session)) {                                     \
    ret = OB_ERR_UNEXPECTED;                                    \
    LOG_WARN("session is NULL", K(ret));                        \
  } else


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
    bool input_judge_json_type = false;
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
      input_judge_json_type = true;
      // do nothing
      // types_stack[0].set_calc_type(ObJsonType);
      // types_stack[0].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    } else {
      types_stack[0].set_calc_type(ObLongTextType);
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
        types_stack[1].set_calc_type(ObLongTextType);
        types_stack[1].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
      }
    }
    // returning type : 2     判断default
    ObExprResType dst_type;
    if (OB_SUCC(ret)) {
      if (types_stack[2].get_type() == ObNullType) {
        ObString j_path_text(types_stack[1].get_param().get_string().length(), types_stack[1].get_param().get_string().ptr());
        ObJsonPath j_path(j_path_text, &allocator);
        if (j_path_text.length() == 0) {
        } else if (OB_FAIL(j_path.parse_path())) {
          ret = OB_ERR_JSON_PATH_EXPRESSION_SYNTAX_ERROR;
          LOG_USER_ERROR(OB_ERR_JSON_PATH_EXPRESSION_SYNTAX_ERROR, j_path_text.length(), j_path_text.ptr());
        }
        if (OB_FAIL(ret)) {
        } else if (input_judge_json_type && !j_path.is_last_func()) {
          dst_type.set_type(ObObjType::ObJsonType);
          dst_type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
        } else {
          dst_type.set_type(ObObjType::ObVarcharType);
          dst_type.set_collation_type(CS_TYPE_INVALID);
          dst_type.set_full_length(4000, 1);
        }
      } else if (OB_FAIL(ObJsonExprHelper::get_cast_type(types_stack[2], dst_type))) {
        LOG_WARN("get cast dest type failed", K(ret));
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(ObJsonExprHelper::set_dest_type(types_stack[0], type, dst_type, type_ctx))) {
          LOG_WARN("set dest type failed", K(ret));
        } else {
          type.set_calc_collation_type(type.get_collation_type());
        }
      }
    }
    // truncate 3  , scalars  4, pretty  5, ascii  6, wrapper 7, error 8, empty 9, mismatch 10
    for (int64_t i = 3; i < param_num && OB_SUCC(ret); ++i) {
      if (types_stack[i].get_type() == ObNullType) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param type is unexpected", K(types_stack[i].get_type()), K(i));
      } else if (types_stack[i].get_type() != ObIntType) {
        types_stack[i].set_calc_type(ObIntType);
      }
    }

    // ASCII clause
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObJsonExprHelper::parse_asc_option(types_stack[6], types_stack[0], type, type_ctx))) {
        LOG_WARN("fail to parse asc option.", K(ret));
      }
    }
  }
  return ret;
}

int ObExprJsonQuery::eval_json_query(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  ObDatum *json_datum = NULL;
  ObExpr *json_arg = expr.args_[1];
  ObObjType type = json_arg->datum_meta_.type_;
  bool is_cover_by_error = true;
  bool is_null_result = false;
  bool is_null_json_obj = false;
  bool is_null_json_array = false;
  uint8_t is_type_cast = 0;
  int8_t JSON_QUERY_EXPR = 1;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  ObIJsonBase *j_base = NULL;

  // parse json path
  ObJsonPath *j_path = NULL;
  if (OB_FAIL(get_ora_json_path(expr, ctx, temp_allocator, j_path, 1, is_null_result, is_cover_by_error, json_datum))) {
    LOG_WARN("get_json_path failed", K(ret));
  }

  // parse pretty ascii scalars
  uint8_t pretty_type = OB_JSON_PRE_ASC_EMPTY;
  uint8_t ascii_type = OB_JSON_PRE_ASC_EMPTY;
  uint8_t scalars_type = OB_JSON_SCALARS_IMPLICIT;
  if (OB_SUCC(ret) && !is_null_result) {
    ret = get_clause_pre_asc_sca_opt(expr, ctx, is_cover_by_error, pretty_type, ascii_type, scalars_type);
  }

  // parse return node acc
  ObAccuracy accuracy;
  ObObjType dst_type;
  json_arg = expr.args_[0];
  type = json_arg->datum_meta_.type_;
  ObExpr *json_arg_ret = expr.args_[2];
  ObObjType val_type = json_arg_ret->datum_meta_.type_;
  int32_t dst_len = OB_MAX_TEXT_LENGTH;
  if (OB_SUCC(ret) && val_type == ObNullType) {
    if (expr.args_[0]->datum_meta_.type_ != ObJsonType && j_path->is_last_func()
        && OB_FAIL(ObJsonExprHelper::check_item_func_with_return(j_path->get_last_node_type(),
                    ObVarcharType, expr.datum_meta_.cs_type_, JSON_QUERY_EXPR))) {
      is_cover_by_error = false;
      LOG_WARN("check item func with return type fail", K(ret));
    }
  } else if (OB_SUCC(ret) && !is_null_result) {
    ret = get_dest_type(expr, dst_len, ctx, dst_type, is_cover_by_error);
  } else if (is_cover_by_error) { // when need error option, should do get accuracy
    get_dest_type(expr, dst_len, ctx, dst_type, is_cover_by_error);
  }

  if (OB_SUCC(ret) && val_type != ObNullType && j_path->is_last_func()
      && OB_FAIL( ObJsonExprHelper::check_item_func_with_return(j_path->get_last_node_type(),
                  dst_type, expr.datum_meta_.cs_type_, JSON_QUERY_EXPR))) {
    is_cover_by_error = false;
    LOG_WARN("check item func with return type fail", K(ret));
  }

  if (OB_SUCC(ret) && val_type == ObNullType) {
    if (ob_is_string_type(type) || j_path->is_last_func()) {
      dst_type = ObVarcharType;
      accuracy.set_full_length(4000, 1, lib::is_oracle_mode());
    } else {
      dst_type = ObJsonType;
      accuracy.set_length(0);
    }
  }

  if (OB_SUCC(ret) && dst_type != ObVarcharType && dst_type != ObLongTextType && dst_type != ObJsonType) {
    is_cover_by_error = false;
    ret = OB_ERR_INVALID_DATA_TYPE_RETURNING;
    LOG_USER_ERROR(OB_ERR_INVALID_DATA_TYPE_RETURNING);
  }

  if ((expr.datum_meta_.cs_type_ == CS_TYPE_BINARY || dst_type == ObJsonType) && (pretty_type > 0 || ascii_type > 0)) {
    is_cover_by_error = false;
    ret = OB_ERR_NON_TEXT_RET_NOTSUPPORT;
    LOG_WARN("ASCII or PRETTY not supported for non-textual return data type", K(ret));
  }

   // parse json doc
  if ((OB_SUCC(ret) || is_cover_by_error) && OB_FAIL(get_ora_json_doc(expr, ctx, temp_allocator, 0, j_base, dst_type, is_null_result, is_cover_by_error))) {
    LOG_WARN("get_json_doc failed", K(ret));
  }

  // parse error option
  uint8_t error_type = OB_JSON_ON_RESPONSE_IMPLICIT;
  ObDatum *error_val = NULL;
  if (OB_SUCC(ret) && !is_null_result) {
    ret = get_clause_opt(expr, ctx, 8, is_cover_by_error, error_type, OB_JSON_ON_RESPONSE_COUNT);
  } else if (is_cover_by_error) { // always get error option on error
    int temp_ret = get_clause_opt(expr, ctx, 8, is_cover_by_error, error_type, OB_JSON_ON_RESPONSE_COUNT);
    if (temp_ret != OB_SUCCESS) {
      ret = temp_ret;
      LOG_WARN("failed to get error option.", K(temp_ret));
    }
  }

  // parse wrapper
  uint8_t wrapper_type = OB_WRAPPER_IMPLICIT;
  if (OB_SUCC(ret)) {
    ret = get_clause_opt(expr, ctx, 7, is_cover_by_error, wrapper_type, OB_WRAPPER_COUNT);
  }

  if (OB_SUCC(ret) && j_path->get_last_node_type() > JPN_BEGIN_FUNC_FLAG
      && j_path->get_last_node_type() < JPN_END_FUNC_FLAG
      && (j_path->get_last_node_type() == JPN_NUMBER
      || j_path->get_last_node_type() == JPN_NUM_ONLY
      || j_path->get_last_node_type() == JPN_LENGTH
      || j_path->get_last_node_type() == JPN_TYPE
      || j_path->get_last_node_type() == JPN_SIZE )
      && (wrapper_type == OB_WITHOUT_WRAPPER || wrapper_type == OB_WITHOUT_ARRAY_WRAPPER
      || wrapper_type == OB_WRAPPER_IMPLICIT)) {
    is_cover_by_error = false;
    ret = OB_ERR_WITHOUT_ARR_WRAPPER;  // result cannot be returned without array wrapper
    LOG_WARN("result cannot be returned without array wrapper.", K(ret), K(j_path->get_last_node_type()), K(wrapper_type));
  }

  // mismatch      // if mismatch_type == 3  from dot notation
  uint8_t mismatch_type = OB_JSON_ON_MISMATCH_IMPLICIT;
  uint8_t mismatch_val = 7;
  if (OB_SUCC(ret) && !is_null_result) {
    if (OB_FAIL(get_clause_opt(expr, ctx, 10, is_cover_by_error, mismatch_type, OB_JSON_ON_MISMATCH_COUNT))) {
      LOG_WARN("failed to get mismatch option.", K(ret), K(mismatch_type));
    }
  }

  uint8_t is_truncate = 0;
  if (OB_SUCC(ret) && !is_null_result) {
    if (OB_FAIL(get_clause_opt(expr, ctx, 3, is_cover_by_error, is_truncate, 2))) {
      LOG_WARN("failed to get mismatch option.", K(ret), K(mismatch_type));
    }
  }

  //  do seek
  //  chose wrapper
  int use_wrapper = 0;
  ObJsonBaseVector hits;
  if (json_datum == nullptr) {
    ret = ret = OB_ERR_UNEXPECTED;;
    LOG_WARN("json path parse fail", K(ret));
  } else if (OB_SUCC(ret) && !is_null_result) {

    if (OB_FAIL(j_base->seek(*j_path, j_path->path_node_cnt(), true, false, hits))) {
      if (ret == OB_ERR_JSON_PATH_EXPRESSION_SYNTAX_ERROR) {
        is_cover_by_error = false;
      } else if (ret == OB_ERR_DOUBLE_TRUNCATED) {
        ret = OB_ERR_CONVERSION_FAIL;
      }
      LOG_WARN("json seek failed", K(json_datum->get_string()), K(ret));
    } else if (hits.size() == 1) {
      if (mismatch_type == OB_JSON_ON_MISMATCH_DOT) {
        if (hits[0]->json_type() == ObJsonNodeType::J_NULL && hits[0]->is_real_json_null(hits[0]) && dst_type != ObJsonType) {
          is_null_result = true;
        }

      } else {
        if (OB_FAIL(get_single_obj_wrapper(wrapper_type, use_wrapper, hits[0]->json_type(), scalars_type))) {
          is_cover_by_error = true;
          LOG_WARN("error occur in wrapper type");
        } else if (use_wrapper == 0 && hits[0]->json_type() == ObJsonNodeType::J_NULL && !hits[0]->is_real_json_null(hits[0])) {
          is_null_result = true;
        } else if (use_wrapper == 0 && j_path->is_last_func() && j_path->path_node_cnt() == 1) {
                      // do nothing
        } else if (use_wrapper == 0 && j_path->get_last_node_type() == JPN_BOOLEAN
                  && (hits[0]->is_json_number(hits[0]->json_type()) || hits[0]->json_type() == ObJsonNodeType::J_NULL)) {
          is_null_result = true;
        } else if (use_wrapper == 0 && (j_path->get_last_node_type() == JPN_DATE || j_path->get_last_node_type() == JPN_TIMESTAMP)
                    && !hits[0]->is_json_date(hits[0]->json_type())) {
          is_null_result = true;
        } else if (use_wrapper == 0 && j_path->get_last_node_type() == JPN_DOUBLE
                    && !hits[0]->is_json_number(hits[0]->json_type()) && hits[0]->json_type() != ObJsonNodeType::J_NULL) {
          is_null_result = true;
        } else if (use_wrapper == 0 && (j_path->get_last_node_type() == JPN_STRING || j_path->get_last_node_type() == JPN_STR_ONLY)
                    && (hits[0]->json_type() == ObJsonNodeType::J_OBJECT || hits[0]->json_type() == ObJsonNodeType::J_ARRAY)) {
          is_null_result = true;
        } else if (use_wrapper == 0 && (j_path->get_last_node_type() == JPN_UPPER || j_path->get_last_node_type() == JPN_LOWER)
                    && (hits[0]->json_type() == ObJsonNodeType::J_OBJECT || hits[0]->json_type() == ObJsonNodeType::J_ARRAY)) {
          is_null_result = true;
        } else if (use_wrapper == 0 && (j_path->get_last_node_type() == JPN_NUMBER || j_path->get_last_node_type() == JPN_NUM_ONLY
                  || j_path->get_last_node_type() == JPN_DOUBLE)
                  && (!hits[0]->is_json_number(hits[0]->json_type()) && hits[0]->json_type() != ObJsonNodeType::J_NULL)) {
          is_null_result = true;
        } else if (use_wrapper == 0 && j_path->get_last_node_type() == JPN_LENGTH && !(hits[0]->json_type() == ObJsonNodeType::J_UINT
                  && ((ObJsonUint *)hits[0])->get_is_string_length())) {
          is_null_result = true;
        } else if (use_wrapper == 0 && (j_path->get_last_node_type() == JPN_DATE || j_path->get_last_node_type() == JPN_TIMESTAMP)
                  && !hits[0]->is_json_date(hits[0]->json_type())) {
          is_null_result = true;
        }
      }
    } else if (hits.size() == 0) {
        // parse empty option
      uint8_t empty_type = OB_JSON_ON_RESPONSE_IMPLICIT;
      if (OB_SUCC(ret) && !is_null_result) {
        ret = get_clause_opt(expr, ctx, 9, is_cover_by_error, empty_type, OB_JSON_ON_RESPONSE_COUNT);
      }
      if (OB_SUCC(ret) && OB_FAIL(get_empty_option(hits, is_cover_by_error, empty_type, is_null_result, is_null_json_obj, is_null_json_array))) {
        LOG_WARN("get empty type", K(ret));
      }
    } else if (hits.size() > 1) {
      // return val decide by wrapper option
      if (OB_FAIL(get_multi_scalars_wrapper_type(wrapper_type, use_wrapper, hits, scalars_type))) {
        is_cover_by_error = true;
        LOG_WARN("error occur in wrapper type");
      }
    }
  }

  // fill output
  if (OB_UNLIKELY(OB_FAIL(ret))) {
    if (is_cover_by_error) {
      if (!try_set_error_val(&temp_allocator, ctx, expr, res, ret, error_type, mismatch_type, dst_type)) {
        LOG_WARN("set error val fail", K(ret));
      }
    }
    LOG_WARN("json_query failed", K(ret));
  } else if (is_null_result) {
    res.set_null();
  } else if (mismatch_type == OB_JSON_ON_MISMATCH_DOT && hits.size() == 1 && dst_type != ObJsonType) {
    ObVector<uint8_t> mismatch_val_tmp;
    ObVector<uint8_t> mismatch_type_tmp;    //OB_JSON_TYPE_IMPLICIT
    ObCollationType in_coll_type = expr.args_[0]->datum_meta_.cs_type_;
    ObCollationType dst_coll_type = expr.datum_meta_.cs_type_;
    ret = ObExprJsonValue::cast_to_res(&temp_allocator, expr, ctx, hits[0], OB_JSON_ON_RESPONSE_NULL, error_val,
                  accuracy, dst_type, in_coll_type, dst_coll_type, res, mismatch_val_tmp, mismatch_type_tmp, is_type_cast, ascii_type, is_truncate);
  } else {
    if (is_null_json_obj) {
      ObJsonObject j_node_null(&temp_allocator);
      ObIJsonBase *jb_res = NULL;
      jb_res = &j_node_null;
      if (OB_FAIL(set_result(dst_type, dst_len, jb_res, &temp_allocator, ctx, expr, res, error_type, ascii_type, pretty_type, is_truncate))) {
        LOG_WARN("result set fail", K(ret));
      }
    } else if (use_wrapper == 1 || is_null_json_array) {
      int32_t hit_size = hits.size();
      ObJsonArray j_arr_res(&temp_allocator);
      ObIJsonBase *jb_res = NULL;
      ObJsonNode *j_node = NULL;
      ObIJsonBase *jb_node = NULL;
      jb_res = &j_arr_res;
      if (is_null_json_array) {
      } else {
        for (int32_t i = 0; OB_SUCC(ret) && i < hit_size; i++) {
          bool is_null_res = false;
          if (hits[i]->json_type() ==  ObJsonNodeType::J_NULL
              && !(hits[i]->is_real_json_null(hits[i]))) {
            is_null_res = true;
          } else if (j_path->get_last_node_type() == JPN_BOOLEAN
                      && (hits[i]->is_json_number(hits[i]->json_type()) || hits[i]->json_type() == ObJsonNodeType::J_NULL)) {
            is_null_res = true;
          } else if (j_path->get_last_node_type() == JPN_LENGTH && !(hits[i]->json_type() == ObJsonNodeType::J_UINT
                      && ((ObJsonUint *)hits[i])->get_is_string_length())) {
            is_null_res = true;
          } else if ((j_path->get_last_node_type() == JPN_STRING || j_path->get_last_node_type() == JPN_STR_ONLY)
                      && (hits[i]->json_type() == ObJsonNodeType::J_OBJECT || hits[i]->json_type() == ObJsonNodeType::J_ARRAY)) {
            is_null_res = true;
          } else if ((j_path->get_last_node_type() == JPN_UPPER || j_path->get_last_node_type() == JPN_LOWER)
                      && (hits[i]->json_type() == ObJsonNodeType::J_OBJECT || hits[i]->json_type() == ObJsonNodeType::J_ARRAY)) {
            is_null_res = true;
          } else if ((j_path->get_last_node_type() == JPN_DATE || j_path->get_last_node_type() == JPN_TIMESTAMP)
                      && !hits[i]->is_json_date(hits[i]->json_type())) {
            is_null_res = true;
          } else if ((j_path->get_last_node_type() == JPN_NUMBER || j_path->get_last_node_type() == JPN_NUM_ONLY
                      || j_path->get_last_node_type() == JPN_DOUBLE )
                      && !hits[i]->is_json_number(hits[i]->json_type()) && hits[i]->json_type() != ObJsonNodeType::J_NULL) {
            is_null_res = true;
          } else if ((hits[i]->json_type() == ObJsonNodeType::J_OBJECT || hits[i]->json_type() == ObJsonNodeType::J_ARRAY)
                     && j_path->is_last_func() && j_path->path_node_cnt() == 1) {
                      // do nothing
          }
          if (is_null_res) {
            void* buf = NULL;
            buf = temp_allocator.alloc(sizeof(ObJsonNull));
            if (OB_ISNULL(buf)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
            } else {
              jb_node = (ObJsonNull*)new(buf)ObJsonNull(true);
            }
          } else if (OB_FAIL(ObJsonBaseFactory::transform(&temp_allocator, hits[i], ObJsonInType::JSON_TREE, jb_node))) { // to tree
            LOG_WARN("fail to transform to tree", K(ret), K(i), K(*(hits[i])));
          }
          if (OB_SUCC(ret)) {
            j_node = static_cast<ObJsonNode *>(jb_node);
            if (OB_ISNULL(j_node)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("json node input is null", K(ret), K(i), K(is_null_res), K(hits[i]));
            } else if (OB_FAIL(jb_res->array_append(j_node->clone(&temp_allocator)))) {
              LOG_WARN("result array append failed", K(ret), K(i), K(*j_node));
            }
          }
        }
      }
      if (!is_null_json_array && try_set_error_val(&temp_allocator, ctx, expr, res, ret, error_type, mismatch_type, dst_type)) {
      } else if (OB_FAIL(set_result(dst_type,dst_len, jb_res, &temp_allocator, ctx, expr, res, error_type, ascii_type, pretty_type, is_truncate))) {
        LOG_WARN("result set fail", K(ret));
      }
    } else {
      ret = set_result(dst_type, dst_len, hits[0], &temp_allocator, ctx, expr, res, error_type, ascii_type, pretty_type, is_truncate);
    }
  }
  return ret;
}

int ObExprJsonQuery::set_result(ObObjType dst_type,
                                int32_t dst_len,
                                ObIJsonBase *jb_res,
                                common::ObIAllocator *allocator,
                                ObEvalCtx &ctx,
                                const ObExpr &expr,
                                ObDatum &res,
                                uint8_t error_type,
                                uint8_t ascii_type,
                                uint8_t pretty_type,
                                uint8_t is_truncate) {
  INIT_SUCC(ret);
  if (dst_type == ObVarcharType || dst_type == ObLongTextType) {
    ObJsonBuffer jbuf(allocator);
    ObString res_string;
    if (OB_FAIL(jb_res->print(jbuf, true, pretty_type > 0))) {
      LOG_WARN("json binary to string failed", K(ret));
    } else if (jbuf.empty()) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory for result failed", K(ret));
    } else {
      res_string.assign_ptr(jbuf.ptr(), jbuf.length());
    }
    if (OB_SUCC(ret)) {
      uint64_t length = res_string.length();
      if (dst_type == ObVarcharType && length  > dst_len) {
        if (is_truncate) {
          res_string.assign_ptr(res_string.ptr(), dst_len);
          if (OB_FAIL(ObJsonExprHelper::pack_json_str_res(expr, ctx, res, res_string))) {
            LOG_WARN("fail to pack json result", K(ret));
          }
        } else {
          char res_ptr[OB_MAX_DECIMAL_PRECISION] = {0};
          if (OB_ISNULL(ObCharset::lltostr(dst_len, res_ptr, 10, 1))) {
            LOG_WARN("dst_len fail to string.", K(ret));
          }
          ret = OB_OPERATE_OVERFLOW;
          LOG_USER_ERROR(OB_OPERATE_OVERFLOW, res_ptr, "json_query");
          if (!try_set_error_val(allocator, ctx, expr, res, ret, error_type, OB_JSON_ON_MISMATCH_IMPLICIT, dst_type)) {
            LOG_WARN("set error val fail", K(ret));
          }
        }
      } else {
        ObTextStringDatumResult text_result(expr.datum_meta_.type_, &expr, &ctx, &res);
        if (ascii_type == 0) {
          if (OB_FAIL(text_result.init(res_string.length()))) {
            LOG_WARN("init lob result failed");
          } else if (OB_FAIL(text_result.append(res_string))) {
            LOG_WARN("failed to append realdata", K(ret), K(res_string), K(text_result));
          }
        } else {
          char *buf = NULL;
          int64_t buf_len = res_string.length() * ObCharset::MAX_MB_LEN * 2;
          int32_t length = 0;
          int64_t reserve_len = 0;

          if (OB_FAIL(text_result.init(buf_len))) {
            LOG_WARN("init lob result failed");
          } else if (OB_FAIL(text_result.get_reserved_buffer(buf, reserve_len))) {
            LOG_WARN("fail to get reserved buffer", K(ret));
          } else if (reserve_len != buf_len) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get reserve len is invalid", K(ret), K(reserve_len), K(buf_len));
          } else if (OB_FAIL(ObJsonExprHelper::calc_asciistr_in_expr(res_string, expr.args_[0]->datum_meta_.cs_type_,
                                                    expr.datum_meta_.cs_type_,
                                                    buf, buf_len, length))) {
            LOG_WARN("fail to calc unistr", K(ret));
          } else if (OB_FAIL(text_result.lseek(length, 0))) {
            LOG_WARN("text_result lseek failed", K(ret), K(text_result), K(length));
          }
        }
        if (OB_SUCC(ret)) {
          text_result.set_result();
        }
      }
    }
  } else if (ob_is_json(dst_type)) {
    ObString raw_str;
    ObIJsonBase *jb_res_bin = NULL;
    if (OB_FAIL(ret)) {
      LOG_WARN("json extarct get results failed", K(ret));
    } else if (OB_FAIL(ObJsonBaseFactory::transform(allocator, jb_res, ObJsonInType::JSON_BIN, jb_res_bin))) { // to BIN
      LOG_WARN("fail to transform to tree", K(ret));
    } else if (OB_FAIL(jb_res_bin->get_raw_binary(raw_str, allocator))) {
      LOG_WARN("json extarct get result binary failed", K(ret));
    } else if (OB_FAIL(ObJsonExprHelper::pack_json_str_res(expr, ctx, res, raw_str))) {
      LOG_WARN("fail to pack json result", K(ret));
    }
  } else {
    ret = OB_ERR_INVALID_DATA_TYPE_RETURNING;
    LOG_USER_ERROR(OB_ERR_INVALID_DATA_TYPE_RETURNING);
  }
  return ret;
}

int ObExprJsonQuery::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_json_query;
  return OB_SUCCESS;
}

int ObExprJsonQuery::get_clause_pre_asc_sca_opt(const ObExpr &expr, ObEvalCtx &ctx, bool &is_cover_by_error, uint8_t &pretty_type, uint8_t &ascii_type, uint8_t &scalars_type)
{
  INIT_SUCC(ret);
    // parse pretty
  if (OB_SUCC(ret)) {
    ret = get_clause_opt(expr, ctx, 5, is_cover_by_error, pretty_type, OB_JSON_PRE_ASC_COUNT);
  }
  // parse ascii
  if (OB_SUCC(ret)) {
    ret = get_clause_opt(expr, ctx, 6, is_cover_by_error, ascii_type, OB_JSON_PRE_ASC_COUNT);
  }
  // parse scalars
  if (OB_SUCC(ret)) {
    ret = get_clause_opt(expr, ctx, 4, is_cover_by_error, scalars_type, OB_JSON_SCALARS_COUNT);
  }
  return ret;
}

int ObExprJsonQuery::get_ora_json_path(const ObExpr &expr, ObEvalCtx &ctx,
                                      common::ObArenaAllocator &allocator, ObJsonPath*& j_path,
                                      uint16_t index, bool &is_null, bool &is_cover_by_error,
                                      ObDatum*& json_datum)
{
  INIT_SUCC(ret);
  ObExpr *json_arg = expr.args_[index];
  ObObjType type = json_arg->datum_meta_.type_;
  if (OB_SUCC(ret) && !is_null) {
    if (OB_FAIL(json_arg->eval(ctx, json_datum))) {
      is_cover_by_error = false;
      LOG_WARN("eval json arg failed", K(ret));
    } else if (type == ObNullType || json_datum->is_null()) {
      is_null = true;
    } else if (!ob_is_string_type(type)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("input type error", K(type));
    }
    ObString j_path_text = json_datum->get_string();

    if (OB_FAIL(ObJsonExprHelper::get_json_or_str_data(json_arg, ctx, allocator, j_path_text, is_null))) {
      LOG_WARN("fail to get real data.", K(ret), K(j_path_text));
    } else if (j_path_text.length() == 0) {
      is_null = true;
    }
    ObJsonPathCache ctx_cache(&allocator);
    ObJsonPathCache* path_cache = ObJsonExprHelper::get_path_cache_ctx(expr.expr_ctx_id_, &ctx.exec_ctx_);
    path_cache = ((path_cache != NULL) ? path_cache : &ctx_cache);

    if (OB_FAIL(ObJsonExprHelper::find_and_add_cache(path_cache, j_path, j_path_text, 1, true))) {
      is_cover_by_error = false;
      ret = OB_ERR_JSON_PATH_EXPRESSION_SYNTAX_ERROR;
      LOG_USER_ERROR(OB_ERR_JSON_PATH_EXPRESSION_SYNTAX_ERROR, j_path_text.length(), j_path_text.ptr());
    }
  }
  return ret;
}

int ObExprJsonQuery::get_ora_json_doc(const ObExpr &expr, ObEvalCtx &ctx,
                                      common::ObArenaAllocator &allocator,
                                      uint16_t index, ObIJsonBase*& j_base,
                                      ObObjType dst_type,
                                      bool &is_null, bool &is_cover_by_error)
{
  INIT_SUCC(ret);
  ObDatum *json_datum = NULL;
  ObExpr *json_arg = expr.args_[index];
  ObObjType type = json_arg->datum_meta_.type_;
  ObCollationType cs_type = json_arg->datum_meta_.cs_type_;
  ObJsonInType j_in_type;
  if (OB_SUCC(ret) && OB_FAIL(json_arg->eval(ctx, json_datum))) {
    LOG_WARN("eval json arg failed", K(ret));
    is_cover_by_error = false;
  } else if (type == ObNullType || json_datum->is_null()) {
    is_null = true;
  } else if (type != ObJsonType && !ob_is_string_type(type)) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, ob_obj_type_str(dst_type), ob_obj_type_str(type));
  } else {
    ObString j_str = json_datum->get_string();
    j_in_type = ObJsonExprHelper::get_json_internal_type(type);
    uint32_t parse_flag = ObJsonParser::JSN_RELAXED_FLAG;
    if (OB_FAIL(ObJsonExprHelper::get_json_or_str_data(json_arg, ctx, allocator, j_str, is_null))) {
      LOG_WARN("fail to get real data.", K(ret), K(j_str));
    } else if (j_str.length() == 0) { // maybe input json doc is null type
      is_null = true;
    } else if (OB_FAIL(ObJsonBaseFactory::get_json_base(&allocator, j_str, j_in_type,
        j_in_type, j_base, parse_flag))) {
      LOG_WARN("fail to get json base.", K(ret), K(type), K(j_str), K(j_in_type));
      if (ret == OB_ERR_JSON_OUT_OF_DEPTH) {
        is_cover_by_error = false;
      }
      ret = OB_ERR_JSON_SYNTAX_ERROR;
    }
  }
  return ret;
}

int ObExprJsonQuery::get_empty_option(ObJsonBaseVector &hits, bool &is_cover_by_error, int8_t empty_type,
                    bool &is_null_result, bool &is_null_json_obj, bool &is_null_json_array)
{
  INIT_SUCC(ret);
  switch (empty_type) {
    case OB_JSON_ON_RESPONSE_IMPLICIT: {
      ret = OB_ERR_JSON_VALUE_NO_VALUE;
      LOG_USER_ERROR(OB_ERR_JSON_VALUE_NO_VALUE);
      LOG_WARN("json value seek result empty.", K(hits.size()));
      break;
    }
    case OB_JSON_ON_RESPONSE_ERROR: {
      is_cover_by_error = false;
      ret = OB_ERR_JSON_VALUE_NO_VALUE;
      LOG_USER_ERROR(OB_ERR_JSON_VALUE_NO_VALUE);
      LOG_WARN("json value seek result empty.", K(hits.size()));
      break;
    }
    case OB_JSON_ON_RESPONSE_EMPTY_OBJECT: {
      is_null_json_obj = true;
      break;
    }
    case OB_JSON_ON_RESPONSE_NULL: {
      is_null_result = true;
      break;
    }
    case OB_JSON_ON_RESPONSE_EMPTY:
    case OB_JSON_ON_RESPONSE_EMPTY_ARRAY: {
      is_null_json_array = true;   // set_json_array
      break;
    }
    default: // empty_type from get_on_empty_or_error has done range check, do nothing for default
      break;
  }
  return ret;
}

int ObExprJsonQuery::get_single_obj_wrapper(uint8_t wrapper_type, int &use_wrapper, ObJsonNodeType in_type, uint8_t scalars_type)
{
  INIT_SUCC(ret);
  switch (wrapper_type) {
    case OB_WITHOUT_WRAPPER:
    case OB_WITHOUT_ARRAY_WRAPPER:
    case OB_WRAPPER_IMPLICIT: {
      if ((in_type != ObJsonNodeType::J_OBJECT &&  in_type != ObJsonNodeType::J_ARRAY
          && scalars_type == OB_JSON_SCALARS_DISALLOW)) {
        ret = OB_ERR_WITHOUT_ARR_WRAPPER;  // result cannot be returned without array wrapper
        LOG_USER_ERROR(OB_ERR_WITHOUT_ARR_WRAPPER);
        LOG_WARN("result cannot be returned without array wrapper.", K(ret));
      }
      break;
    }
    case OB_WITH_WRAPPER:
    case OB_WITH_ARRAY_WRAPPER:
    case OB_WITH_UNCONDITIONAL_WRAPPER:
    case OB_WITH_UNCONDITIONAL_ARRAY_WRAPPER: {
      use_wrapper = 1;
      break;
    }
    case OB_WITH_CONDITIONAL_WRAPPER:
    case OB_WITH_CONDITIONAL_ARRAY_WRAPPER: {
      if (in_type != ObJsonNodeType::J_OBJECT &&  in_type != ObJsonNodeType::J_ARRAY && scalars_type == OB_JSON_SCALARS_DISALLOW ) {
        use_wrapper = 1;
      }
      break;
    }
    default:  // error_type from get_on_empty_or_error has done range check, do nothing for default
      break;
  }
  return ret;
}

int ObExprJsonQuery::get_multi_scalars_wrapper_type(uint8_t wrapper_type, int &use_wrapper, ObJsonBaseVector &hits, uint8_t scalars_type)
{
  INIT_SUCC(ret);
  switch (wrapper_type) {
    case OB_WITHOUT_WRAPPER:
    case OB_WITHOUT_ARRAY_WRAPPER:
    case OB_WRAPPER_IMPLICIT: {
      ret = OB_ERR_WITHOUT_ARR_WRAPPER;  // result cannot be returned without array wrapper
      LOG_USER_ERROR(OB_ERR_WITHOUT_ARR_WRAPPER);
      LOG_WARN("result cannot be returned without array wrapper.", K(ret), K(hits.size()));
      break;
    }
    case OB_WITH_WRAPPER:
    case OB_WITH_ARRAY_WRAPPER:
    case OB_WITH_UNCONDITIONAL_WRAPPER:
    case OB_WITH_UNCONDITIONAL_ARRAY_WRAPPER: {
      use_wrapper = 1;
      break;
    }
    case OB_WITH_CONDITIONAL_WRAPPER:
    case OB_WITH_CONDITIONAL_ARRAY_WRAPPER: {
      use_wrapper = 1;
      break;
    }
    default:  // error_type from get_on_empty_or_error has done range check, do nothing for default
      break;
  }
  return ret;
}

bool ObExprJsonQuery::try_set_error_val(common::ObIAllocator *allocator,
                                        ObEvalCtx &ctx,
                                        const ObExpr &expr,
                                        ObDatum &res,
                                        int &ret,
                                        uint8_t error_type,
                                        uint8_t mismatch_type,
                                        ObObjType dst_type)
{
  bool has_set_res = true;
  bool mismatch_error = true;

  if (OB_FAIL(ret)) {
    if (error_type == OB_JSON_ON_RESPONSE_EMPTY_ARRAY || error_type == OB_JSON_ON_RESPONSE_EMPTY) {
      ret = OB_SUCCESS;
      ObJsonArray j_arr_res(allocator);
      ObIJsonBase *jb_res = NULL;
      jb_res = &j_arr_res;
      if (OB_FAIL(set_result(dst_type, OB_MAX_TEXT_LENGTH, jb_res, allocator, ctx, expr, res, error_type, 0, 0))) {
        LOG_WARN("result set fail", K(ret));
      }
    } else if (error_type == OB_JSON_ON_RESPONSE_EMPTY_OBJECT) {
      ret = OB_SUCCESS;
      ObJsonObject j_node_null(allocator);
      ObIJsonBase *jb_res = NULL;
      jb_res = &j_node_null;
      if (OB_FAIL(set_result(dst_type, OB_MAX_TEXT_LENGTH, jb_res, allocator, ctx, expr, res, error_type, 0, 0))) {
        LOG_WARN("result set fail", K(ret));
      }
    } else if (error_type == OB_JSON_ON_RESPONSE_NULL || error_type == OB_JSON_ON_RESPONSE_IMPLICIT) {
      res.set_null();
      ret = OB_SUCCESS;
    }
  } else {
    has_set_res = false;
  }

  return has_set_res;
}

int ObExprJsonQuery::get_clause_opt(const ObExpr &expr,
                                    ObEvalCtx &ctx,
                                    uint8_t index,
                                    bool &is_cover_by_error,
                                    uint8_t &type,
                                    uint8_t size_para)
{
  INIT_SUCC(ret);
  ObExpr *json_arg = expr.args_[index];
  ObObjType val_type = json_arg->datum_meta_.type_;
  ObDatum *json_datum = NULL;
  if (OB_FAIL(json_arg->eval(ctx, json_datum))) {
    is_cover_by_error = false;
    LOG_WARN("eval json arg failed", K(ret));
  } else if (val_type != ObIntType) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("input type error", K(val_type));
  } else {
    int64_t option_type = json_datum->get_int();
    if (option_type < 0 ||
        option_type >= size_para) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("input option type error", K(option_type));
    } else {
      type = static_cast<uint8_t>(option_type);
    }
  }
  return ret;
}

int ObExprJsonQuery::get_dest_type(const ObExpr &expr,
                                  int32_t &dst_len,
                                  ObEvalCtx& ctx,
                                  ObObjType &dest_type,
                                  bool &is_cover_by_error)
{
  INIT_SUCC(ret);
  ParseNode node;
  ObDatum *dst_type_dat = NULL;
  if (OB_ISNULL(expr.args_) || OB_ISNULL(expr.args_[2])) {
    ret = OB_ERR_UNEXPECTED;
    is_cover_by_error = false;
    LOG_WARN("unexpected expr", K(ret), K(expr.arg_cnt_), KP(expr.args_));
  } else if (OB_FAIL(expr.args_[2]->eval(ctx, dst_type_dat))) {
    is_cover_by_error = false;
    LOG_WARN("eval dst type datum failed", K(ret));
  } else {
    node.value_ = dst_type_dat->get_int();
    dest_type = static_cast<ObObjType>(node.int16_values_[0]);
    dst_len = node.int32_values_[OB_NODE_CAST_C_LEN_IDX];
  }
  return ret;
}


} // sql
} // oceanbase