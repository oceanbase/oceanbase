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

// This file is for func json_value.
#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_json_value.h"
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
#include "sql/engine/ob_exec_context.h"

// from sql_parser_base.h
#define DEFAULT_STR_LENGTH -1

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

#define CAST_FAIL(stmt) \
  (OB_UNLIKELY((OB_SUCCESS != (ret = get_cast_ret((stmt))))))

#define GET_SESSION()                                           \
  ObBasicSessionInfo *session = ctx.exec_ctx_.get_my_session(); \
  if (OB_ISNULL(session)) {                                     \
    ret = OB_ERR_UNEXPECTED;                                    \
    LOG_WARN("session is NULL", K(ret));                        \
  } else

ObExprJsonValue::ObExprJsonValue(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_JSON_VALUE, N_JSON_VALUE, MORE_THAN_TWO, NOT_ROW_DIMENSION)
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

  if (OB_UNLIKELY(param_num != 7)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid param number", K(ret), K(param_num));
  } else {
    //type.set_json();
    // json doc : 0
    ObObjType doc_type = types_stack[0].get_type();
    if (types_stack[0].get_type() == ObNullType) {
    } else if (!ObJsonExprHelper::is_convertible_to_json(doc_type)) {
      ret = OB_ERR_INVALID_TYPE_FOR_JSON;
      LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_JSON, 1, "json_value");
      LOG_WARN("Invalid type for json doc", K(doc_type));
    } else if (ob_is_string_type(doc_type)) {
      if (types_stack[0].get_collation_type() == CS_TYPE_BINARY) {
        // unsuport string type with binary charset
        ret = OB_ERR_INVALID_JSON_CHARSET;
        LOG_WARN("Unsupport for string type with binary charset input.", K(ret), K(doc_type));
      } else if (types_stack[0].get_charset_type() != CHARSET_UTF8MB4) {
        types_stack[0].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
      }
    } else if (doc_type == ObJsonType) {
      // do nothing
    } else {
      types_stack[0].set_calc_type(ObLongTextType);
      types_stack[0].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    }

    // json path : 1
    if (OB_SUCC(ret)) {
      if (types_stack[1].get_type() == ObNullType) {
        // do nothing
      } else if (ob_is_string_type(types_stack[1].get_type())) {
        if (types_stack[1].get_charset_type() != CHARSET_UTF8MB4) {
          types_stack[1].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
        }
      } else {
        types_stack[1].set_calc_type(ObLongTextType);
        types_stack[1].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
      }
    }
    // returning type : 2
    ObExprResType dst_type;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(get_cast_type(types_stack[2], dst_type))) {
        LOG_WARN("get cast dest type failed", K(ret));
      } else if (OB_FAIL(set_dest_type(types_stack[0], type, dst_type, type_ctx))) {
        LOG_WARN("set dest type failed", K(ret));
      } else {
        type.set_calc_collation_type(type.get_collation_type());
      }
    }

    // empty : 3, 4
    if (OB_SUCC(ret)) {
      ObExprResType temp_type;
      if (types_stack[3].get_type() == ObNullType) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("<empty type> param type is unexpected", K(types_stack[3].get_type()));
      } else if (types_stack[3].get_type() != ObIntType) {
        types_stack[3].set_calc_type(ObIntType);
      } else if (types_stack[4].get_type() == ObNullType) {
        // do nothing
      } else if (OB_FAIL(set_dest_type(types_stack[4], temp_type, dst_type, type_ctx))) {
        LOG_WARN("set dest type failed", K(ret));
      } else {
        types_stack[4].set_calc_type(temp_type.get_type());
        types_stack[4].set_calc_collation_type(temp_type.get_collation_type());
        types_stack[4].set_calc_collation_level(temp_type.get_collation_level());
        types_stack[4].set_calc_accuracy(temp_type.get_accuracy());
      }
    }

    // error : 5, 6
    if (OB_SUCC(ret)) {
      ObExprResType temp_type;
      if (types_stack[5].get_type() == ObNullType) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("<error type> param type is unexpected", K(types_stack[5].get_type()));
      } else if (types_stack[5].get_type() != ObIntType) {
        types_stack[5].set_calc_type(ObIntType);
      } else if (types_stack[6].get_type() == ObNullType) {
        // do nothing
      } else if (OB_FAIL(set_dest_type(types_stack[6], temp_type, dst_type, type_ctx))) {
        LOG_WARN("set dest type failed", K(ret));
      } else {
        types_stack[6].set_calc_type(temp_type.get_type());
        types_stack[6].set_calc_collation_type(temp_type.get_collation_type());
        types_stack[6].set_calc_collation_level(temp_type.get_collation_level());
        types_stack[6].set_calc_accuracy(temp_type.get_accuracy());
      }
    }
  }

  return ret;
}

int ObExprJsonValue::calc_resultN(ObObj& result,
                                  const ObObj *params,
                                  int64_t params_count,
                                  ObExprCtx& expr_ctx) const
{
  INIT_SUCC(ret);
  ObIAllocator *allocator = expr_ctx.calc_buf_;
  bool is_cover_by_error = true;
  bool is_null_result = false;
  ObIJsonBase *j_base = NULL;

  if (OB_ISNULL(params) || OB_UNLIKELY(params_count != 7)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(params), K(params_count));
  } else if (OB_ISNULL(allocator)) { // check allocator
    ret = OB_NOT_INIT;
    LOG_WARN("varchar buffer not init", K(ret));
  } else if (OB_FAIL(param_eval(expr_ctx, params[0], 0))) {
    LOG_WARN("failed: eval json doc", K(ret));
  } else {
    ObObjType type = params[0].get_type();
    if (type == ObNullType || params[0].is_null()) {
      is_null_result = true;
    } else if (type != ObJsonType && !ob_is_string_type(type)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("input type error", K(type));
    } else {
      ObString j_str = params[0].get_string();
      ObJsonInType j_in_type = ObJsonExprHelper::get_json_internal_type(type);
      if (j_str.length() == 0) { // maybe input json doc is null type
        is_null_result = true;
      } else if (OB_FAIL(ObJsonBaseFactory::get_json_base(allocator, j_str, j_in_type,
          j_in_type, j_base))) {
        LOG_WARN("fail to get json base.", K(ret), K(type), K(j_str), K(j_in_type));
        if (ret == OB_ERR_JSON_OUT_OF_DEPTH) {
          is_cover_by_error = false;
        }
      }
    }
  }

  // parse return node acc
  ObAccuracy accuracy;
  ObObjType dst_type;
  if (is_cover_by_error) { // adapt mysql, Parse return node even if parse json doc wrong.
    int32_t temp_ret = param_eval(expr_ctx, params[2], 2);
    if (temp_ret != OB_SUCCESS) {
      ret = (ret == OB_SUCCESS) ? temp_ret : ret;
      is_cover_by_error = false;
      LOG_WARN("eval json arg failed", K(temp_ret));
    } else {
      temp_ret = get_accuracy_internal(accuracy, dst_type, params[2].get_int(),
          result_type_.get_length_semantics());
      if (temp_ret != OB_SUCCESS) {
        ret = (ret == OB_SUCCESS) ? temp_ret : ret;
        is_cover_by_error = false;
        LOG_WARN("get accuracy failed", K(temp_ret));
      }
    }
  }

  // parse empty option
  ObObj empty_val;
  uint8_t empty_type = OB_JSON_ON_RESPONSE_IMPLICIT;
  if (OB_SUCC(ret) && !is_null_result) {
    ret = get_on_empty_or_error_old(params, expr_ctx, dst_type, 3, is_cover_by_error,
                                    accuracy, empty_type, &empty_val);
  }

  // parse error option
  ObObj error_val;
  uint8_t error_type = OB_JSON_ON_RESPONSE_IMPLICIT;
  if (OB_SUCC(ret) && !is_null_result) {
    ret = get_on_empty_or_error_old(params, expr_ctx, dst_type, 5, is_cover_by_error,
                                    accuracy, error_type, &error_val);
  } else if (is_cover_by_error) { // always get error option for return default value on error
    int temp_ret = get_on_empty_or_error_old(params, expr_ctx, dst_type, 5, is_cover_by_error,
        accuracy, error_type, &error_val);
    if (temp_ret != OB_SUCCESS) {
      LOG_WARN("failed to get on empty or error.", K(ret), K(dst_type));
    }
  }

  // parse json path and do seek
  ObObj *return_val = NULL;
  ObJsonBaseVector hits;
  if (OB_SUCC(ret) && !is_null_result) {
    if (OB_FAIL(param_eval(expr_ctx, params[1], 1))) {
      is_cover_by_error = false;
      LOG_WARN("eval json arg failed", K(ret));
    } else {
      ObObjType type = params[1].get_type();
      if (type == ObNullType || params[1].is_null()) {
        is_null_result = true;
      } else if (!ob_is_string_type(type)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("input type error", K(type));
      } else {
        // Old engine not support op_ctx. use local cache.
        ObJsonPathCache ctx_cache(allocator);
        ObJsonPathCache *path_cache = &ctx_cache;
        ObString j_path_text = params[1].get_string();
        ObJsonPath *j_path;
        if (j_path_text.length() == 0) {
          is_null_result = true;
        } else if (OB_FAIL(ObJsonExprHelper::find_and_add_cache(path_cache, j_path,
            j_path_text, 1, true))) {
          is_cover_by_error = false;
          LOG_WARN("parse text to path failed", K(j_path_text), K(ret));
        } else if (OB_FAIL(j_base->seek(*j_path, j_path->path_node_cnt(), true, false, hits))) {
          LOG_WARN("json seek failed", K(j_path_text), K(ret));
        } else if (hits.size() == 0) {
          switch (empty_type) {
            case OB_JSON_ON_RESPONSE_ERROR: {
              is_cover_by_error = false;
              ret = OB_ERR_MISSING_JSON_VALUE;
              LOG_USER_ERROR(OB_ERR_MISSING_JSON_VALUE, "json_value");
              LOG_WARN("json value seek result empty.", K(hits.size()));
              break;
            }
            case OB_JSON_ON_RESPONSE_DEFAULT: {
              return_val = &empty_val;
              break;
            }
            case OB_JSON_ON_RESPONSE_NULL:
            case OB_JSON_ON_RESPONSE_IMPLICIT: {
              is_null_result = true;
              break;
            }
            default:
              break;
          }
        } else if (hits.size() > 1) {
          // return val decide by error option
          switch (error_type) {
            case OB_JSON_ON_RESPONSE_ERROR: {
              ret = OB_ERR_MULTIPLE_JSON_VALUES;
              LOG_USER_ERROR(OB_ERR_MULTIPLE_JSON_VALUES, "json_value");
              LOG_WARN("json value seek result more than one.", K(hits.size()));
              break;
            }
            case OB_JSON_ON_RESPONSE_DEFAULT: {
              return_val = &error_val;
              break;
            }
            case OB_JSON_ON_RESPONSE_NULL:
            case OB_JSON_ON_RESPONSE_IMPLICIT: {
              is_null_result = true;
              break;
            }
            default:
              break;
          }
        } else if (hits[0]->json_type() == ObJsonNodeType::J_NULL) {
          is_null_result = true;
        }
      }
    }
  }

  // fill output
  if (OB_UNLIKELY(OB_FAIL(ret))) {
    if (is_cover_by_error) {
      try_set_error_val<ObObj>(result, ret, error_type, &error_val);
    }
    LOG_WARN("json_values failed", K(ret));
  } else if (is_null_result){
    result.set_null();
  } else {
    if (return_val != NULL) {
      set_val(result, return_val);
    } else {
      ObCollationType in_coll_type = params[0].get_collation_type();
      ObCollationType dst_coll_type = result_type_.get_collation_type();
      ObCollationLevel dst_coll_level = result_type_.get_collation_level();
      ret = cast_to_res(allocator, expr_ctx, hits[0], error_type, &error_val,
          accuracy, dst_type, in_coll_type, dst_coll_type, dst_coll_level, result);
    }
  }

  return ret;
}

int ObExprJsonValue::eval_json_value(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  ObDatum *json_datum = NULL;
  ObExpr *json_arg = expr.args_[0];
  ObObjType type = json_arg->datum_meta_.type_;
  bool is_cover_by_error = true;
  bool is_null_result = false;
  ObDatum *return_val = NULL;
  common::ObArenaAllocator &temp_allocator = ctx.get_reset_tmp_alloc();
  ObIJsonBase *j_base = NULL;

  // parse json doc
  if (OB_FAIL(json_arg->eval(ctx, json_datum))) {
    LOG_WARN("eval json arg failed", K(ret));
    is_cover_by_error = false;
  } else if (type == ObNullType || json_datum->is_null()) {
    is_null_result = true;
  } else if (type != ObJsonType && !ob_is_string_type(type)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("input type error", K(type));
  } else {
    ObString j_str = json_datum->get_string();
    ObJsonInType j_in_type = ObJsonExprHelper::get_json_internal_type(type);
    if (j_str.length() == 0) { // maybe input json doc is null type
      is_null_result = true;
    } else if (OB_FAIL(ObJsonBaseFactory::get_json_base(&temp_allocator, j_str, j_in_type,
        j_in_type, j_base))) {
      LOG_WARN("fail to get json base.", K(ret), K(type), K(j_str), K(j_in_type));
      if (ret == OB_ERR_JSON_OUT_OF_DEPTH) {
        is_cover_by_error = false;
      }
    }
  }
  
  // parse return node acc
  ObAccuracy accuracy;
  ObObjType dst_type;
  if (OB_SUCC(ret) && !is_null_result) {
    ret = get_accuracy(expr, ctx, accuracy, dst_type, is_cover_by_error);
  } else if (is_cover_by_error) { // when need error option, should do get accuracy
    get_accuracy(expr, ctx, accuracy, dst_type, is_cover_by_error);
  }

  // parse empty option
  ObDatum *empty_datum = NULL;
  uint8_t empty_type = OB_JSON_ON_RESPONSE_IMPLICIT;
  if (OB_SUCC(ret) && !is_null_result) {
    ret = get_on_empty_or_error(expr, ctx, 3, is_cover_by_error, accuracy, empty_type, &empty_datum);
  }

  // parse error option
  ObDatum *error_val = NULL;
  uint8_t error_type = OB_JSON_ON_RESPONSE_IMPLICIT;
  if (OB_SUCC(ret) && !is_null_result) {
    ret = get_on_empty_or_error(expr, ctx, 5, is_cover_by_error, accuracy, error_type, &error_val);
  } else if (is_cover_by_error) { // always get error option for return default value on error
    int temp_ret = get_on_empty_or_error(expr, ctx, 5, is_cover_by_error, accuracy,
        error_type, &error_val);
    if (temp_ret != OB_SUCCESS) {
      LOG_WARN("failed to get error option.", K(temp_ret));
    }
  }

  // parse json path and do seek
  ObJsonBaseVector hits;
  if (OB_SUCC(ret) && !is_null_result) {
    json_arg = expr.args_[1];
    type = json_arg->datum_meta_.type_;
    if (OB_FAIL(json_arg->eval(ctx, json_datum))) {
      is_cover_by_error = false;
      LOG_WARN("eval json arg failed", K(ret));
    } else if (type == ObNullType || json_datum->is_null()) {
      is_null_result = true;
    } else if (!ob_is_string_type(type)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("input type error", K(type));
    } else {
      ObString j_path_text = json_datum->get_string();
      ObJsonPath *j_path;
      if (j_path_text.length() == 0) {
        is_null_result = true;
      }
      
      ObJsonPathCache ctx_cache(&temp_allocator);
      ObJsonPathCache* path_cache = ObJsonExprHelper::get_path_cache_ctx(expr.expr_ctx_id_, &ctx.exec_ctx_);
      path_cache = ((path_cache != NULL) ? path_cache : &ctx_cache);

      if (OB_FAIL(ObJsonExprHelper::find_and_add_cache(path_cache, j_path, j_path_text, 1, true))) {
        is_cover_by_error = false;
        LOG_WARN("parse text to path failed", K(json_datum->get_string()), K(ret));
      } else if (OB_FAIL(j_base->seek(*j_path, j_path->path_node_cnt(), true, false, hits))) {
        LOG_WARN("json seek failed", K(json_datum->get_string()), K(ret));
      } else if (hits.size() == 0) {
        switch (empty_type) {
          case OB_JSON_ON_RESPONSE_ERROR: {
            is_cover_by_error = false;
            ret = OB_ERR_MISSING_JSON_VALUE;
            LOG_USER_ERROR(OB_ERR_MISSING_JSON_VALUE, "json_value");
            LOG_WARN("json value seek result empty.", K(hits.size()));
            break;
          }
          case OB_JSON_ON_RESPONSE_DEFAULT: {
            return_val = empty_datum;
            break;
          }
          case OB_JSON_ON_RESPONSE_NULL:
          case OB_JSON_ON_RESPONSE_IMPLICIT: {
            is_null_result = true;
            break;
          }
          default: // empty_type from get_on_empty_or_error has done range check, do nothing for default
            break;
        }
      } else if (hits.size() > 1) {
        // return val decide by error option
        switch (error_type) {
          case OB_JSON_ON_RESPONSE_ERROR: {
            ret = OB_ERR_MULTIPLE_JSON_VALUES;
            LOG_USER_ERROR(OB_ERR_MULTIPLE_JSON_VALUES, "json_value");
            LOG_WARN("json value seek result more than one.", K(hits.size()));
            break;
          }
          case OB_JSON_ON_RESPONSE_DEFAULT: {
            return_val = error_val;
            break;
          }
          case OB_JSON_ON_RESPONSE_NULL:
          case OB_JSON_ON_RESPONSE_IMPLICIT: {
            is_null_result = true;
            break;
          }
          default:  // error_type from get_on_empty_or_error has done range check, do nothing for default 
            break;
        }
      } else if (hits[0]->json_type() == ObJsonNodeType::J_NULL) {
        is_null_result = true;
      }
    }
  }

  // fill output
  if (OB_UNLIKELY(OB_FAIL(ret))) {
    if (is_cover_by_error) {
      try_set_error_val<ObDatum>(res, ret, error_type, error_val);
    }
    LOG_WARN("json_values failed", K(ret));
  } else if (is_null_result){
    res.set_null();
  } else {
    if (return_val != NULL) {
      res.set_datum(*return_val);
    } else {
      ObCollationType in_coll_type = expr.args_[0]->datum_meta_.cs_type_;
      ObCollationType dst_coll_type = expr.datum_meta_.cs_type_;
      ret = cast_to_res(&temp_allocator, expr, ctx, hits[0], error_type, error_val,
          accuracy, dst_type, in_coll_type, dst_coll_type, res);
    }
  }

  return ret;
}

int ObExprJsonValue::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_json_value;
  return OB_SUCCESS;
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
      ret = number_range_check(accuracy, NULL, temp, true);
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
      ret = datetime_scale_check(accuracy, val, true);
      break;
    }
    case ObTimeTC: {
      int64_t val = obj->get_time();
      ret = time_scale_check(accuracy, val, true);
      break;
    }
    default:
      break;
  }

  return ret;
}

int ObExprJsonValue::get_accuracy_internal(ObAccuracy &accuracy,
                                          ObObjType &dest_type,
                                          const int64_t value,
                                          const ObLengthSemantics &length_semantics)
{
  INIT_SUCC(ret);
  ParseNode node;
  node.value_ = value;
  dest_type = static_cast<ObObjType>(node.int16_values_[0]);

  if (ObFloatType == dest_type) {
    // boundaries already checked in calc result type
    if (node.int16_values_[OB_NODE_CAST_N_PREC_IDX] > OB_MAX_FLOAT_PRECISION) {
      dest_type = ObDoubleType;
    }
  }
  ObObjTypeClass dest_tc = ob_obj_type_class(dest_type);
  if (ObStringTC == dest_tc) {
    // parser will abort all negative number
    // if length < 0 means DEFAULT_STR_LENGTH or OUT_OF_STR_LEN.
    accuracy.set_full_length(node.int32_values_[1], length_semantics,
                              lib::is_oracle_mode());
  } else if (ObRawTC == dest_tc) {
    accuracy.set_length(node.int32_values_[1]);
  } else if(ObTextTC == dest_tc || ObJsonTC == dest_tc) {
    accuracy.set_length(node.int32_values_[1] < 0 ?
        ObAccuracy::DDL_DEFAULT_ACCURACY[dest_type].get_length() : node.int32_values_[1]);
  } else if (ObIntervalTC == dest_tc) {
    if (OB_UNLIKELY(!ObIntervalScaleUtil::scale_check(node.int16_values_[3]) ||
                    !ObIntervalScaleUtil::scale_check(node.int16_values_[2]))) {
      ret = OB_ERR_DATETIME_INTERVAL_PRECISION_OUT_OF_RANGE;
      LOG_WARN("Invalid scale.", K(ret), K(node.int16_values_[3]), K(node.int16_values_[2]));
    } else {
      ObScale scale = (dest_type == ObIntervalYMType) ?
        ObIntervalScaleUtil::interval_ym_scale_to_ob_scale(
            static_cast<int8_t>(node.int16_values_[3]))
        : ObIntervalScaleUtil::interval_ds_scale_to_ob_scale(
            static_cast<int8_t>(node.int16_values_[2]),
            static_cast<int8_t>(node.int16_values_[3]));
      accuracy.set_scale(scale);
    }
  } else {
    const ObAccuracy &def_acc =
      ObAccuracy::DDL_DEFAULT_ACCURACY2[lib::is_oracle_mode()][dest_type];
    if (ObNumberType == dest_type && 0 == node.int16_values_[2]) {
      accuracy.set_precision(def_acc.get_precision());
    } else {
      accuracy.set_precision(node.int16_values_[2]);
    }
    accuracy.set_scale(node.int16_values_[3]);
    if (lib::is_oracle_mode() && ObDoubleType == dest_type) {
      accuracy.set_accuracy(def_acc.get_precision());
    }
  }

  return ret;
}

int ObExprJsonValue::get_accuracy(const ObExpr &expr,
                                  ObEvalCtx &ctx,
                                  ObAccuracy &accuracy,
                                  ObObjType &dest_type,
                                  bool &is_cover_by_error)
{
  INIT_SUCC(ret);
  ObDatum *dst_type_dat = NULL;

  if (OB_ISNULL(expr.args_) || OB_ISNULL(expr.args_[2])) {
    ret = OB_ERR_UNEXPECTED;
    is_cover_by_error = false;
    LOG_WARN("unexpected expr", K(ret), K(expr.arg_cnt_), KP(expr.args_));
  } else if (OB_FAIL(expr.args_[2]->eval(ctx, dst_type_dat))) {
    is_cover_by_error = false;
    LOG_WARN("eval dst type datum failed", K(ret));
  } else {
    ret = get_accuracy_internal(accuracy,
                                dest_type,
                                dst_type_dat->get_int(),
                                expr.datum_meta_.length_semantics_);
  }

  return ret;
}

int ObExprJsonValue::number_range_check(const ObAccuracy &accuracy,
                                        ObIAllocator *allocator,
                                        number::ObNumber &val,
                                        bool strict)
{
  INIT_SUCC(ret);
  ObPrecision precision = accuracy.get_precision();
  ObScale scale = accuracy.get_scale();
  const number::ObNumber *min_check_num = NULL;
  const number::ObNumber *max_check_num = NULL;
  const number::ObNumber *min_num_mysql = NULL;
  const number::ObNumber *max_num_mysql = NULL;
  bool is_finish = false;

  if (OB_UNLIKELY(precision < scale)) {
    ret = OB_ERR_M_BIGGER_THAN_D;
    LOG_WARN("Invalid accuracy.", K(ret), K(scale), K(precision));
  } else if (number::ObNumber::MAX_PRECISION >= precision
      && precision >= OB_MIN_DECIMAL_PRECISION
      && number::ObNumber::MAX_SCALE >= scale
      && scale >= 0) {
    min_check_num = &(ObNumberConstValue::MYSQL_CHECK_MIN[precision][scale]);
    max_check_num = &(ObNumberConstValue::MYSQL_CHECK_MAX[precision][scale]);
    min_num_mysql = &(ObNumberConstValue::MYSQL_MIN[precision][scale]);
    max_num_mysql = &(ObNumberConstValue::MYSQL_MAX[precision][scale]);
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(precision), K(scale));
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(min_check_num) || OB_ISNULL(max_check_num)
        || (!lib::is_oracle_mode()
          && (OB_ISNULL(min_num_mysql) || OB_ISNULL(max_num_mysql)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("min_num or max_num is null", K(ret), KPC(min_check_num), KPC(max_check_num));
    } else if (val <= *min_check_num) {
      if (lib::is_oracle_mode()) {
        ret = OB_ERR_VALUE_LARGER_THAN_ALLOWED;
      } else {
        ret = OB_DATA_OUT_OF_RANGE;
      }
      LOG_WARN("val is out of min range check.", K(val), K(*min_check_num));
      is_finish = true;
    } else if (val >= *max_check_num) {
      if (lib::is_oracle_mode()) {
        ret = OB_ERR_VALUE_LARGER_THAN_ALLOWED;
      } else {
        ret = OB_DATA_OUT_OF_RANGE;
      }
      LOG_WARN("val is out of max range check.", K(val), K(*max_check_num));
      is_finish = true;
    } else {
      ObNumStackOnceAlloc tmp_alloc;
      number::ObNumber num;
      if (OB_FAIL(num.from(val, tmp_alloc))) {
      } else if (OB_FAIL(num.round(scale))) {
        LOG_WARN("num.round failed", K(ret), K(scale));
      } else {
        if (strict) {
          if (num.compare(val) != 0) {
            ret = OB_OPERATE_OVERFLOW;
            LOG_WARN("input value is out of range.", K(scale), K(val));
          } else {
            is_finish = true;
          }
        } else {
          if (OB_ISNULL(allocator)) {
            ret = OB_ERR_NULL_VALUE;
            LOG_WARN("allocator is null", K(ret));
          } else if (OB_FAIL(val.deep_copy_v3(num, *allocator))) {
            LOG_WARN("val.deep_copy_v3 failed", K(ret), K(num));
          } else {
            is_finish = true;
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && !is_finish) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected situation, res is not set", K(ret));
  }
  LOG_DEBUG("number_range_check_v2 done", K(ret), K(is_finish), K(accuracy), K(val),
            KPC(min_check_num), KPC(max_check_num));

  return ret;
}

int ObExprJsonValue::datetime_scale_check(const ObAccuracy &accuracy,
                                          int64_t &value,
                                          bool strict)
{
  INIT_SUCC(ret);
  ObScale scale = accuracy.get_scale();

  if (OB_UNLIKELY(scale > MAX_SCALE_FOR_TEMPORAL)) {
    ret = OB_ERR_TOO_BIG_PRECISION;
    LOG_USER_ERROR(OB_ERR_TOO_BIG_PRECISION, scale, "CAST",
        static_cast<int64_t>(MAX_SCALE_FOR_TEMPORAL));
  } else if (OB_UNLIKELY(0 <= scale && scale < MAX_SCALE_FOR_TEMPORAL)) {
    // first check zero
    if (strict &&
        (value == ObTimeConverter::ZERO_DATE ||
        value == ObTimeConverter::ZERO_DATETIME)) {
      ret = OB_INVALID_DATE_VALUE;
      LOG_WARN("Zero datetime is invalid in json_value.", K(value));
    } else {
      int64_t temp_value = value;
      ObTimeConverter::round_datetime(scale, temp_value);
      if (strict && temp_value != value) {
        ret = OB_OPERATE_OVERFLOW;
        LOG_WARN("Invalid input value.", K(value), K(scale));
      } else if (ObTimeConverter::is_valid_datetime(temp_value)) {
        value = temp_value;
      } else {
        ret = OB_ERR_NULL_VALUE; // set null for res
        LOG_DEBUG("Invalid datetime val, return set_null", K(temp_value));
      }
    }
  }

  return ret;
}

int ObExprJsonValue::time_scale_check(const ObAccuracy &accuracy, int64_t &value, bool strict)
{
  INIT_SUCC(ret);
  ObScale scale = accuracy.get_scale();

  if (OB_LIKELY(0 <= scale && scale < MAX_SCALE_FOR_TEMPORAL)) {
    int64_t temp_value = value;
    ObTimeConverter::round_datetime(scale, temp_value);
    if (strict && temp_value != value) { // round success
      ret = OB_OPERATE_OVERFLOW;
      LOG_WARN("Invalid input value.", K(value), K(scale));
    } else {
      value = temp_value;
    }
  }

  return ret;
}

int ObExprJsonValue::get_cast_ret(int ret)
{
  // compatibility for old ob
  if (OB_UNLIKELY(OB_ERR_UNEXPECTED_TZ_TRANSITION == ret) ||
      OB_UNLIKELY(OB_ERR_UNKNOWN_TIME_ZONE == ret)) {
    ret = OB_INVALID_DATE_VALUE;
  }

  return ret;
}

int ObExprJsonValue::cast_to_int(ObIJsonBase *j_base, ObObjType dst_type, int64_t &val)
{
  INIT_SUCC(ret);

  if (OB_ISNULL(j_base)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("json base is null", K(ret));
  } else if (CAST_FAIL(j_base->to_int(val, true))) {
    ret = OB_OPERATE_OVERFLOW;
    LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "SIGNED", "json_value");
    LOG_WARN("cast to int failed", K(ret), K(*j_base));
  } else if (dst_type < ObIntType &&
    CAST_FAIL(int_range_check(dst_type, val, val))) {
    ret = OB_OPERATE_OVERFLOW;
    LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "SIGNED", "json_value");
  }

  return ret;
}

int ObExprJsonValue::cast_to_uint(ObIJsonBase *j_base, ObObjType dst_type, uint64_t &val)
{
  INIT_SUCC(ret);

  if (OB_ISNULL(j_base)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("json base is null", K(ret));
  } else if (CAST_FAIL(j_base->to_uint(val, true, true))) {
    LOG_WARN("cast to uint failed", K(ret), K(*j_base));
    if (ret == OB_OPERATE_OVERFLOW) {
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "UNSIGNED", "json_value");
    }
  } else if (dst_type < ObUInt64Type &&
    CAST_FAIL(uint_upper_check(dst_type, val))) {
    LOG_WARN("uint_upper_check failed", K(ret));
  }

  return ret;
}

int ObExprJsonValue::cast_to_datetime(ObIJsonBase *j_base,
                                      common::ObAccuracy &accuracy,
                                      int64_t &val)
{
  INIT_SUCC(ret);

  if (OB_ISNULL(j_base)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("json base is null", K(ret));
  } else if (CAST_FAIL(j_base->to_datetime(val))) {
    LOG_WARN("wrapper to datetime failed.", K(ret), K(*j_base));
  } else if (CAST_FAIL(datetime_scale_check(accuracy, val))) {
    LOG_WARN("datetime_scale_check failed.", K(ret));
  }

  return ret;
}

int ObExprJsonValue::cast_to_otimstamp(ObIJsonBase *j_base,
                                      const ObBasicSessionInfo *session,
                                      common::ObAccuracy &accuracy,
                                      ObObjType dst_type,
                                      ObOTimestampData &out_val)
{
  INIT_SUCC(ret);
  int64_t val;

  if (OB_ISNULL(j_base)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("json base is null", K(ret));
  } else if (CAST_FAIL(j_base->to_datetime(val))) {
    LOG_WARN("wrapper to datetime failed.", K(ret), K(*j_base));
  } else {
    if (session != NULL) {
      const ObTimeZoneInfo *tz_info = session->get_timezone_info();
      ObScale scale = accuracy.get_scale();
      if (OB_FAIL(ObTimeConverter::odate_to_otimestamp(val, tz_info, dst_type, out_val))) {
        LOG_WARN("fail to timestamp_to_timestamp_tz", K(ret), K(val), K(dst_type));
      } else if (OB_UNLIKELY(0 <= scale && scale < MAX_SCALE_FOR_ORACLE_TEMPORAL)) {
        ObOTimestampData ot_data = ObTimeConverter::round_otimestamp(scale, out_val);
        if (ObTimeConverter::is_valid_otimestamp(ot_data.time_us_,
            static_cast<int32_t>(ot_data.time_ctx_.tail_nsec_))) {
          out_val = ot_data;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid otimestamp, set it null ", K(ot_data), K(scale), "orig_date", out_val);
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is NULL", K(ret));
    }
  }

  return ret;
}

int ObExprJsonValue::cast_to_date(ObIJsonBase *j_base, int32_t &val)
{
  INIT_SUCC(ret);

  if (OB_ISNULL(j_base)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("json base is null", K(ret));
  } else if (CAST_FAIL(j_base->to_date(val))) {
    LOG_WARN("wrapper to date failed.", K(ret), K(*j_base));
    ret = OB_OPERATE_OVERFLOW;
    LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "DATE", "json_value");
  }

  return ret;
}

int ObExprJsonValue::cast_to_time(ObIJsonBase *j_base,
                                  common::ObAccuracy &accuracy,
                                  int64_t &val)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(j_base)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("json base is null", K(ret));
  } else if (CAST_FAIL(j_base->to_time(val))) {
    LOG_WARN("wrapper to time failed.", K(ret), K(*j_base));
    ret = OB_OPERATE_OVERFLOW;
    LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "TIME", "json_value");
  } else if (CAST_FAIL(time_scale_check(accuracy, val))) {
    LOG_WARN("time_scale_check failed.", K(ret));
  }

  return ret;
}

int ObExprJsonValue::cast_to_year(ObIJsonBase *j_base, uint8_t &val)
{
  INIT_SUCC(ret);
  // Compatible with mysql. 
  // There is no year type in json binary, it is store as a full int. 
  // For example, 1901 is stored as 1901, not 01.
  // in mysql 8.0, json is converted to int first, then converted to year.
  // However, json value returning as different behavior to cast expr.
  int64_t int_val;
  const uint16 min_year = 1901;
  const uint16 max_year = 2155;

  if (OB_ISNULL(j_base)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("json base is null", K(ret));
  } else if (CAST_FAIL(j_base->to_int(int_val))) {
    LOG_WARN("wrapper to year failed.", K(ret), K(*j_base));
  } else if (0 != int_val && (int_val < min_year || int_val > max_year)) {
    // different with cast, if 0 < int val < 100, do not add base year
    LOG_DEBUG("int out of year range", K(int_val));
    ret = OB_DATA_OUT_OF_RANGE;
  } else if(CAST_FAIL(ObTimeConverter::int_to_year(int_val, val))) {
    LOG_WARN("int to year failed.", K(ret), K(int_val));
  }

  return ret;
}

int ObExprJsonValue::cast_to_float(ObIJsonBase *j_base, ObObjType dst_type, float &val)
{
  INIT_SUCC(ret);
  double tmp_val = 0;

  if (OB_ISNULL(j_base)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("json base is null", K(ret));
  } else if (CAST_FAIL(j_base->to_double(tmp_val))) {
    LOG_WARN("wrapper to date failed.", K(ret), K(*j_base));
  } else {
    val = static_cast<float>(tmp_val);
    if (CAST_FAIL(real_range_check(dst_type, tmp_val, val))) {
      LOG_WARN("real_range_check failed", K(ret), K(tmp_val));
    }
  }

  return ret;
}

int ObExprJsonValue::cast_to_double(ObIJsonBase *j_base, ObObjType dst_type, double &val)
{
  INIT_SUCC(ret);

  if (OB_ISNULL(j_base)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("json base is null", K(ret));
  } else if (CAST_FAIL(j_base->to_double(val))) {
    LOG_WARN("wrapper to date failed.", K(ret), K(*j_base));
  } else if (ObUDoubleType == dst_type && CAST_FAIL(numeric_negative_check(val))) {
    LOG_WARN("numeric_negative_check failed", K(ret), K(val));
  }

  return ret;
}

int ObExprJsonValue::cast_to_number(common::ObIAllocator *allocator,
                                    ObIJsonBase *j_base,
                                    common::ObAccuracy &accuracy,
                                    ObObjType dst_type,
                                    number::ObNumber &val)
{
  INIT_SUCC(ret);

  if (OB_ISNULL(j_base)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("json base is null", K(ret));
  } else if (CAST_FAIL(j_base->to_number(allocator, val))) {
    LOG_WARN("fail to cast json as decimal", K(ret));
  } else if (ObUNumberType == dst_type && CAST_FAIL(numeric_negative_check(val))) {
    LOG_WARN("numeric_negative_check failed", K(ret), K(val));
  } else if (CAST_FAIL(number_range_check(accuracy, allocator, val))) {
    LOG_WARN("number_range_check failed", K(ret), K(val));
  }

  return ret;
}

int ObExprJsonValue::cast_to_string(common::ObIAllocator *allocator,
                                    ObIJsonBase *j_base,
                                    ObCollationType in_cs_type,
                                    ObCollationType dst_cs_type,
                                    common::ObAccuracy &accuracy,
                                    ObObjType dst_type,
                                    ObString &val)
{
  INIT_SUCC(ret);

  if (OB_ISNULL(j_base)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("json base is null", K(ret));
  } else if (OB_ISNULL(allocator)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("allocator is null", K(ret));
  } else {
    ObJsonBuffer j_buf(allocator);
    if (CAST_FAIL(j_base->print(j_buf, false))) {
      LOG_WARN("fail to_string as json", K(ret));
    } else {
      ObObjType in_type = ObLongTextType;
      ObString temp_str_val(j_buf.length(), j_buf.ptr());
      bool is_need_string_string_convert = ((CS_TYPE_BINARY == dst_cs_type) || 
                                            (ObCharset::charset_type_by_coll(in_cs_type) != 
                                            ObCharset::charset_type_by_coll(dst_cs_type)));
      if (is_need_string_string_convert) {
        if (CS_TYPE_BINARY != in_cs_type
            && CS_TYPE_BINARY != dst_cs_type
            && (ObCharset::charset_type_by_coll(in_cs_type) !=
            ObCharset::charset_type_by_coll(dst_cs_type))) {
          char *buf = NULL;
          const int64_t factor = 2;
          int64_t buf_len = temp_str_val.length() * factor;
          uint32_t result_len = 0;
          buf = reinterpret_cast<char*>(allocator->alloc(buf_len));
          if (OB_ISNULL(buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("alloc memory failed", K(ret));
          } else if (OB_FAIL(ObCharset::charset_convert(in_cs_type, temp_str_val.ptr(),
                                                        temp_str_val.length(), dst_cs_type, buf,
                                                        buf_len, result_len))) {
            LOG_WARN("charset convert failed", K(ret));
          } else {
            val.assign_ptr(buf, result_len);
          }
        } else {
          if (CS_TYPE_BINARY == in_cs_type || CS_TYPE_BINARY == dst_cs_type) {
            // just copy string when in_cs_type or out_cs_type is binary
            const ObCharsetInfo *cs = NULL;
            int64_t align_offset = 0;
            if (CS_TYPE_BINARY == in_cs_type && lib::is_mysql_mode()
                && (NULL != (cs = ObCharset::get_charset(dst_cs_type)))) {
              if (cs->mbminlen > 0 && temp_str_val.length() % cs->mbminlen != 0) {
                align_offset = cs->mbminlen - temp_str_val.length() % cs->mbminlen;
              }
            }
            int64_t len = align_offset + temp_str_val.length();
            char *buf = reinterpret_cast<char*>(allocator->alloc(len));
            if (OB_ISNULL(buf)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("allocate memory failed", K(ret));
            } else {
              MEMMOVE(buf + align_offset, temp_str_val.ptr(), len - align_offset);
              MEMSET(buf, 0, align_offset);
              val.assign_ptr(buf, len);
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("same charset should not be here, just use cast_eval_arg", K(ret),
                K(in_type), K(dst_type), K(in_cs_type), K(dst_cs_type));
          }
        }
      } else {
        val.assign_ptr(temp_str_val.ptr(), temp_str_val.length());
      }
      // do str length check
      const int32_t str_len_char = static_cast<int32_t>(ObCharset::strlen_char(dst_cs_type,
          val.ptr(), val.length()));
      const ObLength max_accuracy_len = accuracy.get_length();
      if (OB_SUCC(ret)) {
        if (max_accuracy_len == DEFAULT_STR_LENGTH) { // default string len
        } else if (max_accuracy_len <= 0 || str_len_char > max_accuracy_len) {
          ret = OB_OPERATE_OVERFLOW;
          LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "STRING", "json_value");
        }
      }
    }
  }
  
  return ret;
}

int ObExprJsonValue::cast_to_bit(ObIJsonBase *j_base, uint64_t &val)
{
  INIT_SUCC(ret);

  if (OB_ISNULL(j_base)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("json base is null", K(ret));
  } else if (CAST_FAIL(j_base->to_bit(val))) {
    LOG_WARN("fail get bit from json", K(ret));
  }

  return ret;
}

int ObExprJsonValue::cast_to_json(common::ObIAllocator *allocator,
                                  ObIJsonBase *j_base, ObString &val)
{
  INIT_SUCC(ret);

  if (OB_ISNULL(j_base)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("json base is null", K(ret));
  } else if (CAST_FAIL(j_base->get_raw_binary(val, allocator))) {
    LOG_WARN("failed to get raw binary", K(ret));
  }

  return ret;
}

int ObExprJsonValue::cast_to_res(common::ObIAllocator *allocator,
                                 ObExprCtx& expr_ctx,
                                 ObIJsonBase *j_base,
                                 uint8_t error_type,
                                 ObObj *error_val,
                                 common::ObAccuracy &accuracy,
                                 ObObjType dst_type,
                                 common::ObCollationType in_coll_type,
                                 common::ObCollationType dst_coll_type,
                                 common::ObCollationLevel dst_coll_level,
                                 ObObj &res)
{
  INIT_SUCC(ret);

  switch (dst_type) {
    case ObNullType : {
      res.set_null();
      break;
    }
    case ObTinyIntType:
    case ObSmallIntType:
    case ObMediumIntType:
    case ObInt32Type:
    case ObIntType: {
      int64_t val;
      ret = cast_to_int(j_base, dst_type, val);
      if (!try_set_error_val<ObObj>(res, ret, error_type, error_val)) {
        res.set_int(dst_type, val);
      }
      break;
    }
    case ObUTinyIntType:
    case ObUSmallIntType:
    case ObUMediumIntType:
    case ObUInt32Type:
    case ObUInt64Type: {
      uint64_t val;
      ret = cast_to_uint(j_base, dst_type, val);
      if (!try_set_error_val<ObObj>(res, ret, error_type, error_val)) {
        res.set_uint(dst_type, val);
      }
      break;
    }
    case ObDateTimeType: {
      int64_t val;
      ret = cast_to_datetime(j_base, accuracy, val);
      if (ret == OB_ERR_NULL_VALUE) {
        res.set_null();
      } else if (!try_set_error_val<ObObj>(res, ret, error_type, error_val)) {
        res.set_datetime(dst_type, val);
      }
      break;
    }
    case ObTimestampType: {
      const ObBasicSessionInfo *session = expr_ctx.my_session_;
      ObOTimestampData val;
      ret = cast_to_otimstamp(j_base, session, accuracy, dst_type, val);
      if (!try_set_error_val<ObObj>(res, ret, error_type, error_val)) {
        res.set_otimestamp_value(dst_type, val);
      }
      break;
    }
    case ObDateType: {
      int32_t val;
      ret = cast_to_date(j_base, val);
      if (!try_set_error_val<ObObj>(res, ret, error_type, error_val)) {
        res.set_date(val);
      }
      break;
    }
    case ObTimeType: {
      int64_t val;
      ret = cast_to_time(j_base, accuracy, val);
      if (!try_set_error_val<ObObj>(res, ret, error_type, error_val)) {
        res.set_time(val);
      }
      break;
    }
    case ObYearType: {
      uint8_t val;
      ret = cast_to_year(j_base, val);
      if (!try_set_error_val<ObObj>(res, ret, error_type, error_val)) {
        res.set_year(val);
      }
      break;
    }
    case ObFloatType:
    case ObUFloatType: {
      float out_val;
      ret = cast_to_float(j_base, dst_type, out_val);
      if (!try_set_error_val<ObObj>(res, ret, error_type, error_val)) {
        res.set_float(dst_type, out_val);
      }
      break;
    }
    case ObDoubleType:
    case ObUDoubleType: {
      double out_val;
      ret = cast_to_double(j_base, dst_type, out_val);
      if (!try_set_error_val<ObObj>(res, ret, error_type, error_val)) {
        res.set_double(dst_type, out_val);
      }
      break;
    }
    case ObUNumberType:
    case ObNumberType: {
      number::ObNumber out_val;
      ret = cast_to_number(allocator, j_base, accuracy, dst_type, out_val);
      if (!try_set_error_val<ObObj>(res, ret, error_type, error_val)) {
        res.set_number(dst_type, out_val);
      }
      break;
    }
    case ObVarcharType:
    case ObCharType:
    case ObTinyTextType:
    case ObTextType :
    case ObMediumTextType:
    case ObHexStringType:
    case ObLongTextType: {
      ObString val;
      ret = cast_to_string(allocator, j_base, in_coll_type, dst_coll_type, accuracy, dst_type, val);
      if (!try_set_error_val<ObObj>(res, ret, error_type, error_val)) {
        // old engine set same alloctor for wrapper, so we can use val without copy
        res.set_string(dst_type, val);
        res.set_collation_type(dst_coll_type);
        res.set_collation_level(dst_coll_level);
      }
      break;
    }
    case ObBitType: {
      uint64_t out_val;
      ret = cast_to_bit(j_base, out_val);
      if (!try_set_error_val<ObObj>(res, ret, error_type, error_val)) {
        res.set_bit(out_val);
      }
      break;
    }
    case ObJsonType: {
      ObString out_val;
      ret = cast_to_json(allocator, j_base, out_val);
      if (OB_SUCC(ret)) {
        char *buf = reinterpret_cast<char *>(expr_ctx.calc_buf_->alloc(out_val.length()));
        if (OB_UNLIKELY(buf == NULL)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory for json array result", K(ret), K(out_val.length()));
        } else {
          MEMCPY(buf, out_val.ptr(), out_val.length());
          out_val.assign_ptr(buf, out_val.length());
        }
      }
      if (!try_set_error_val<ObObj>(res, ret, error_type, error_val)) {
        res.set_string(dst_type, out_val);
        res.set_collation_type(dst_coll_type);
        res.set_collation_level(dst_coll_level);
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected dst_type", K(dst_type));
      try_set_error_val<ObObj>(res, ret, error_type, error_val);
      break;
    }
  }
  LOG_DEBUG("finish cast_to_res.", K(ret), K(dst_type), K(error_type));

  return ret;
}

int ObExprJsonValue::cast_to_res(common::ObIAllocator *allocator,
                                 const ObExpr &expr,
                                 ObEvalCtx &ctx,
                                 ObIJsonBase *j_base,
                                 uint8_t error_type,
                                 ObDatum *error_val,
                                 ObAccuracy &accuracy,
                                 ObObjType dst_type,
                                 ObCollationType in_coll_type,
                                 ObCollationType dst_coll_type,
                                 ObDatum &res)
{
  INIT_SUCC(ret);

  switch (dst_type) {
    case ObNullType : {
      res.set_null();
      break;
    }
    case ObTinyIntType:
    case ObSmallIntType:
    case ObMediumIntType:
    case ObInt32Type:
    case ObIntType: {
      int64_t val;
      ret = cast_to_int(j_base, dst_type, val);
      if (!try_set_error_val<ObDatum>(res, ret, error_type, error_val)) {
        res.set_int(val);
      }
      break;
    }
    case ObUTinyIntType:
    case ObUSmallIntType:
    case ObUMediumIntType:
    case ObUInt32Type:
    case ObUInt64Type: {
      uint64_t val;
      ret = cast_to_uint(j_base, dst_type, val);
      if (!try_set_error_val<ObDatum>(res, ret, error_type, error_val)) {
        res.set_uint(val);
      }
      break;
    }
    case ObDateTimeType: {
      int64_t val;
      ret = cast_to_datetime(j_base, accuracy, val);
      if (ret == OB_ERR_NULL_VALUE) {
        res.set_null();
      } else if (!try_set_error_val<ObDatum>(res, ret, error_type, error_val)) {
        res.set_datetime(val);
      }
      break;
    }
    case ObTimestampType: {
      ObOTimestampData val;
      GET_SESSION()
      {
        ret = cast_to_otimstamp(j_base, session, accuracy, dst_type, val);
      }
      if (!try_set_error_val<ObDatum>(res, ret, error_type, error_val)) {
        res.set_otimestamp_tiny(val);
      }
      break;
    }
    case ObDateType: {
      int32_t val;
      ret = cast_to_date(j_base, val);
      if (!try_set_error_val<ObDatum>(res, ret, error_type, error_val)) {
        res.set_date(val);
      }
      break;
    }
    case ObTimeType: {
      int64_t val;
      ret = cast_to_time(j_base, accuracy, val);
      if (!try_set_error_val<ObDatum>(res, ret, error_type, error_val)) {
        res.set_time(val);
      }
      break;
    }
    case ObYearType: {
      uint8_t val;
      ret = cast_to_year(j_base, val);
      if (!try_set_error_val<ObDatum>(res, ret, error_type, error_val)) {
        res.set_year(val);
      }
      break;
    }
    case ObFloatType:
    case ObUFloatType: {
      float out_val;
      ret = cast_to_float(j_base, dst_type, out_val);
      if (!try_set_error_val<ObDatum>(res, ret, error_type, error_val)) {
        res.set_float(out_val);
      }
      break;
    }
    case ObDoubleType:
    case ObUDoubleType: {
      double out_val;
      ret = cast_to_double(j_base, dst_type, out_val);
      if (!try_set_error_val<ObDatum>(res, ret, error_type, error_val)) {
        res.set_double(out_val);
      }
      break;
    }
    case ObUNumberType:
    case ObNumberType: {
      number::ObNumber out_val;
      ret = cast_to_number(allocator, j_base, accuracy, dst_type, out_val);
      if (!try_set_error_val<ObDatum>(res, ret, error_type, error_val)) {
        res.set_number(out_val);
      }
      break;
    }
    case ObVarcharType:
    case ObCharType:
    case ObTinyTextType:
    case ObTextType :
    case ObMediumTextType:
    case ObHexStringType:
    case ObLongTextType: {
      ObString val;
      ret = cast_to_string(allocator, j_base, in_coll_type, dst_coll_type, accuracy, dst_type, val);
      if (OB_SUCC(ret)) {
        char *buf = expr.get_str_res_mem(ctx, val.length());
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret));
        } else {
          MEMCPY(buf, val.ptr(), val.length());
          val.assign_ptr(buf, val.length());
        }
      }
      if (!try_set_error_val<ObDatum>(res, ret, error_type, error_val)) {
        // old engine set same alloctor for wrapper, so we can use val without copy
        res.set_string(val);
      }
      break;
    }
    case ObBitType: {
      uint64_t out_val;
      ret = cast_to_bit(j_base, out_val);
      if (!try_set_error_val<ObDatum>(res, ret, error_type, error_val)) {
        res.set_bit(out_val);
      }
      break;
    }
    case ObJsonType: {
      ObString out_val;
      ret = cast_to_json(allocator, j_base, out_val);
      if (OB_SUCC(ret)) {
        char *buf = expr.get_str_res_mem(ctx, out_val.length());
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret));
        } else {
          MEMCPY(buf, out_val.ptr(), out_val.length());
          out_val.assign_ptr(buf, out_val.length());
        }
      }
      if (!try_set_error_val<ObDatum>(res, ret, error_type, error_val)) {
        res.set_string(out_val);
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected dst_type", K(dst_type));
      try_set_error_val<ObDatum>(res, ret, error_type, error_val);
      break;
    }
  }
  LOG_DEBUG("finish cast_to_res.", K(ret), K(dst_type), K(error_type));

  return ret;
}

template<typename Obj>
bool ObExprJsonValue::try_set_error_val(Obj &res, int &ret, uint8_t error_type, Obj *error_val)
{
  bool has_set_res = true;

  if (OB_FAIL(ret)) {
    if (error_type == OB_JSON_ON_RESPONSE_DEFAULT) {
      set_val(res, error_val);
      ret = OB_SUCCESS;
    } else if (error_type == OB_JSON_ON_RESPONSE_NULL ||
      error_type == OB_JSON_ON_RESPONSE_IMPLICIT) {
      res.set_null();
      ret = OB_SUCCESS;
    }
  } else {
    has_set_res = false;
  }

  return has_set_res;
}

int ObExprJsonValue::get_on_empty_or_error_old(const ObObj *params,
                                              ObExprCtx& expr_ctx,
                                              const ObObjType dst_type,
                                              uint8_t index,
                                              bool &is_cover_by_error,
                                              const ObAccuracy &accuracy,
                                              uint8_t &type,
                                              ObObj *default_value) const
{
  INIT_SUCC(ret);

  if (OB_FAIL(param_eval(expr_ctx, params[index], index))) {
    is_cover_by_error = false;
    LOG_WARN("eval json arg failed", K(ret));
  } else {
    ObObjType val_type = params[index].get_type();
    if (val_type != ObIntType) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("input type error", K(val_type));
    } else {
      int64_t option_type = params[index].get_int();
      if (option_type < OB_JSON_ON_RESPONSE_ERROR ||
          option_type > OB_JSON_ON_RESPONSE_IMPLICIT) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("input option type error", K(option_type));
      } else {
        type = static_cast<uint8_t>(option_type);
      }
    }
  }

  if (OB_SUCC(ret)) {
    common::ObCastMode cast_mode = expr_ctx.cast_mode_;
    expr_ctx.cast_mode_ &= ~CM_WARN_ON_FAIL; // make cast return error when fail
    expr_ctx.cast_mode_ &= ~CM_NO_RANGE_CHECK; // make cast check range
    expr_ctx.cast_mode_ &= ~CM_STRING_INTEGER_TRUNC; // make cast check range when string to uint
    expr_ctx.cast_mode_ |= CM_ERROR_ON_SCALE_OVER; // make cast check presion and scale
    expr_ctx.cast_mode_ |= CM_EXPLICIT_CAST; // make cast json fail return error
    if (OB_FAIL(param_eval(expr_ctx, params[index + 1], index + 1))) {
      is_cover_by_error = false;
      LOG_WARN("eval json arg failed", K(ret));
    } else if (params[index + 1].get_type() == ObNullType || params[index + 1].is_null()) {
    } else if (OB_FAIL(check_default_val_accuracy<ObObj>(accuracy, dst_type, &(params[index + 1])))) {
      is_cover_by_error = false;
    } else {
      *default_value = params[index + 1];
    }
    if (ret == OB_OPERATE_OVERFLOW) {
      if (dst_type >= ObDateTimeType && dst_type <= ObYearType) {
        LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "TIME DEFAULT", "json_value");
      } else if (dst_type == ObNumberType || dst_type == ObUNumberType) {
        LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "DECIMAL DEFAULT", "json_value");
      }
    }
    expr_ctx.cast_mode_ = cast_mode; // recover cast mode in old engine for cast mode used in global at expr
  }

  return ret;
}

int ObExprJsonValue::get_on_empty_or_error(const ObExpr &expr,
                                           ObEvalCtx &ctx,
                                           uint8_t index,
                                           bool &is_cover_by_error,
                                           const ObAccuracy &accuracy,
                                           uint8_t &type,
                                           ObDatum **default_value)
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
    if (option_type < OB_JSON_ON_RESPONSE_ERROR ||
        option_type > OB_JSON_ON_RESPONSE_IMPLICIT) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("input option type error", K(option_type));
    } else {
      type = static_cast<uint8_t>(option_type);
    }
  }

  if (OB_SUCC(ret)) {
    json_arg = expr.args_[index + 1];
    val_type = json_arg->datum_meta_.type_;
    json_arg->extra_ &= ~CM_WARN_ON_FAIL; // make cast return error when fail
    json_arg->extra_ &= ~CM_NO_RANGE_CHECK; // make cast check range
    json_arg->extra_ &= ~CM_STRING_INTEGER_TRUNC; // make cast check range when string to uint
    json_arg->extra_ |= CM_ERROR_ON_SCALE_OVER; // make cast check presion and scale
    json_arg->extra_ |= CM_EXPLICIT_CAST; // make cast json fail return error
    if (OB_FAIL(json_arg->eval(ctx, json_datum))) {
      is_cover_by_error = false;
      LOG_WARN("eval json arg failed", K(ret));
    } else if (val_type == ObNullType || json_datum->is_null()) {
    } else if (OB_FAIL(check_default_val_accuracy<ObDatum>(accuracy, val_type, json_datum))) {
      is_cover_by_error = false;
    } else {
      *default_value = json_datum;
    }
    if (ret == OB_OPERATE_OVERFLOW) {
      if (val_type >= ObDateTimeType && val_type <= ObYearType) {
        LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "TIME DEFAULT", "json_value");
      } else if (val_type == ObNumberType || val_type == ObUNumberType) {
        LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "DECIMAL DEFAULT", "json_value");
      }
    }
  }

  return ret;
}

int ObExprJsonValue::get_cast_type(const ObExprResType param_type2, ObExprResType &dst_type) const
{
  INIT_SUCC(ret);

  if (!param_type2.is_int() && !param_type2.get_param().is_int()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cast param type is unexpected", K(param_type2));
  } else {
    const ObObj &param = param_type2.get_param();
    ParseNode parse_node;
    parse_node.value_ = param.get_int();
    ObObjType obj_type = static_cast<ObObjType>(parse_node.int16_values_[OB_NODE_CAST_TYPE_IDX]);
    dst_type.set_collation_type(static_cast<ObCollationType>(parse_node.int16_values_[OB_NODE_CAST_COLL_IDX]));
    dst_type.set_type(obj_type);
    if (ob_is_string_type(obj_type) || ob_is_lob_locator(obj_type)) {
      // cast(x as char(10)) or cast(x as binary(10))
      dst_type.set_full_length(parse_node.int32_values_[OB_NODE_CAST_C_LEN_IDX],
                               param_type2.get_accuracy().get_length_semantics());
    } else if (ob_is_raw(obj_type)) {
      dst_type.set_length(parse_node.int32_values_[OB_NODE_CAST_C_LEN_IDX]);
    } else if (ObFloatType == dst_type.get_type()) {
      // Compatible with mysql. If the precision p is not specified, produces a result of type FLOAT. 
      // If p is provided and 0 <= < p <= 24, the result is of type FLOAT. If 25 <= p <= 53, 
      // the result is of type DOUBLE. If p < 0 or p > 53, an error is returned
      // however, ob use -1 as default precision, so it is a valid value
      ObPrecision float_precision = parse_node.int16_values_[OB_NODE_CAST_N_PREC_IDX];
      if (float_precision < -1 || float_precision > OB_MAX_DOUBLE_FLOAT_PRECISION) {
        ret = OB_ERR_TOO_BIG_PRECISION;
        LOG_USER_ERROR(OB_ERR_TOO_BIG_PRECISION, float_precision, "CAST", OB_MAX_DOUBLE_FLOAT_PRECISION);
      } else if (float_precision <= OB_MAX_FLOAT_PRECISION) {
        dst_type.set_type(ObFloatType);
      } else {
        dst_type.set_type(ObDoubleType);
      }
      dst_type.set_precision(-1);
      dst_type.set_scale(parse_node.int16_values_[OB_NODE_CAST_N_SCALE_IDX]);
    } else if (lib::is_mysql_mode() && ObJsonType == dst_type.get_type()) {
      dst_type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    } else {
      dst_type.set_precision(parse_node.int16_values_[OB_NODE_CAST_N_PREC_IDX]);
      dst_type.set_scale(parse_node.int16_values_[OB_NODE_CAST_N_SCALE_IDX]);
    }
    LOG_DEBUG("get_cast_type", K(dst_type), K(param_type2));
  }

  return ret;
}

int ObExprJsonValue::set_dest_type(ObExprResType &type1,
                                   ObExprResType &type,
                                   ObExprResType &dst_type,
                                   ObExprTypeCtx &type_ctx) const
{
  INIT_SUCC(ret);
  const sql::ObSQLSessionInfo *session = type_ctx.get_session();

  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is NULL", K(ret), KP(session));
  } else {
    // always cast to user requested type
    if (!lib::is_oracle_mode() &&
        ObCharType == dst_type.get_type()) {
      // cast(x as binary(10)), in parser,binary->T_CHAR+bianry, but, result type should be varchar, so set it.
      type.set_type(ObVarcharType);
    } else {
      type.set_type(dst_type.get_type());
      type.set_collation_type(dst_type.get_collation_type());
    }
    int16_t scale = dst_type.get_scale();
    if (!lib::is_oracle_mode()
        && (ObTimeType == dst_type.get_type() || ObDateTimeType == dst_type.get_type())
        && scale > MAX_SCALE_FOR_TEMPORAL) {
      ret = OB_ERR_TOO_BIG_PRECISION;
      LOG_USER_ERROR(OB_ERR_TOO_BIG_PRECISION, scale, "CAST", OB_MAX_DATETIME_PRECISION);
    }
    if (OB_SUCC(ret)) {
      ObCompatibilityMode compatibility_mode = get_compatibility_mode();
      ObCollationType collation_connection = type_ctx.get_coll_type();
      ObCollationType collation_nation = session->get_nls_collation_nation();
      int32_t length = 0;
      if (ob_is_string_type(dst_type.get_type()) || ob_is_json(dst_type.get_type())) {
        type.set_collation_level(CS_LEVEL_IMPLICIT);
        int32_t len = dst_type.get_length();
        int16_t length_semantics = ((dst_type.is_string_type())
            ? dst_type.get_length_semantics()
            : (OB_NOT_NULL(type_ctx.get_session())
                ? type_ctx.get_session()->get_actual_nls_length_semantics()
                : LS_BYTE));
        if (len > 0) { // cast(1 as char(10))
          type.set_full_length(len, length_semantics);
        } else if (OB_FAIL(get_cast_string_len(type1, dst_type, type_ctx, len, length_semantics,
                                               collation_connection))) { // cast (1 as char)
          LOG_WARN("fail to get cast string length", K(ret));
        } else {
          type.set_full_length(len, length_semantics);
        }
        if (CS_TYPE_INVALID != dst_type.get_collation_type()) {
          // cast as binary
          type.set_collation_type(dst_type.get_collation_type());
        } else {
          // use collation of current session
          type.set_collation_type(ob_is_nstring_type(dst_type.get_type()) ?
                                  collation_nation : collation_connection);
        }
      } else {
        type.set_length(length);
        if (ObNumberTC == dst_type.get_type_class() && 0 == dst_type.get_precision()) {
          // MySql:cast (1 as decimal(0)) = cast(1 as decimal)
          // Oracle: cast(1.4 as number) = cast(1.4 as number(-1, -1))
          type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY2[compatibility_mode][ObNumberType].get_precision());
        } else if (ObIntTC == dst_type.get_type_class() || ObUIntTC == dst_type.get_type_class()) {
          // for int or uint , the precision = len
          int32_t len = 0;
          int16_t length_semantics = LS_BYTE;//unused
          if (OB_FAIL(get_cast_inttc_len(type1, dst_type, type_ctx, len, length_semantics, collation_connection))) {
            LOG_WARN("fail to get cast inttc length", K(ret));
          } else {
            len = len > OB_LITERAL_MAX_INT_LEN ? OB_LITERAL_MAX_INT_LEN : len;
            type.set_precision(static_cast<int16_t>(len));
          }
        } else if (ORACLE_MODE == compatibility_mode && ObDoubleType == dst_type.get_type()) {
          ObAccuracy acc = ObAccuracy::DDL_DEFAULT_ACCURACY2[compatibility_mode][dst_type.get_type()];
          type.set_accuracy(acc);
        } else {
          type.set_precision(dst_type.get_precision());
        }
        type.set_scale(dst_type.get_scale());
      }
    }
  }

  return ret;
}

int ObExprJsonValue::get_cast_string_len(ObExprResType &type1,
                                         ObExprResType &type2,
                                         ObExprTypeCtx &type_ctx,
                                         int32_t &res_len,
                                         int16_t &length_semantics,
                                         ObCollationType conn) const
{
  INIT_SUCC(ret);
  const ObObj &val = type1.get_param();

  if (!type1.is_literal()) { // column
    res_len = CAST_STRING_DEFUALT_LENGTH[type1.get_type()];
    int16_t prec = type1.get_accuracy().get_precision();
    int16_t scale = type1.get_accuracy().get_scale();
    switch(type1.get_type()) {
    case ObTinyIntType:
    case ObSmallIntType:
    case ObMediumIntType:
    case ObInt32Type:
    case ObIntType:
    case ObUTinyIntType:
    case ObUSmallIntType:
    case ObUMediumIntType:
    case ObUInt32Type:
    case ObUInt64Type: {
        int32_t prec = static_cast<int32_t>(type1.get_accuracy().get_precision());
        res_len = prec > res_len ? prec : res_len;
        break;
      }
    case ObNumberType:
    case ObUNumberType: {
        if (lib::is_oracle_mode()) {
          if (0 < prec) {
            if (0 < scale) {
              res_len =  prec + 2;
            } else if (0 == scale){
              res_len = prec + 1;
            } else {
              res_len = prec - scale;
            }
          }
        } else {
          if (0 < prec) {
            if (0 < scale) {
              res_len =  prec + 2;
            } else {
              res_len = prec + 1;
            }
          }
        }
        break;
      }
    case ObTimestampTZType:
    case ObTimestampLTZType:
    case ObTimestampNanoType:
    case ObDateTimeType:
    case ObTimestampType: {
        if (scale > 0) {
          res_len += scale + 1;
        }
        break;
      }
    case ObTimeType: {
        if (scale > 0) {
          res_len += scale + 1;
        }
        break;
      }
    case ObTinyTextType:
    case ObTextType:
    case ObMediumTextType:
    case ObLongTextType:
    case ObVarcharType:
    case ObCharType:
    case ObHexStringType:
    case ObRawType:
    case ObNVarchar2Type:
    case ObNCharType: {
        res_len = type1.get_length();
        length_semantics = type1.get_length_semantics();
        break;
      }
    default: {
        break;
      }
    }
  } else if (type1.is_null()) {
    res_len = 0;//compatible with mysql;
  } else if (OB_ISNULL(type_ctx.get_session())) {
    // calc type don't set ret, just print the log. by design.
    LOG_WARN("my_session is null");
  } else { // literal
    ObArenaAllocator oballocator(ObModIds::BLOCK_ALLOC);
    ObCastMode cast_mode = CM_NONE;
    ObCollationType cast_coll_type = (CS_TYPE_INVALID != type2.get_collation_type())
        ? type2.get_collation_type()
        : conn;
    const ObDataTypeCastParams dtc_params =
          ObBasicSessionInfo::create_dtc_params(type_ctx.get_session());
    ObCastCtx cast_ctx(&oballocator,
                       &dtc_params,
                       0,
                       cast_mode,
                       cast_coll_type);
    ObString val_str;
    EXPR_GET_VARCHAR_V2(val, val_str);
    if (OB_SUCC(ret) && NULL != val_str.ptr()) {
      int32_t len_byte = val_str.length();
      res_len = len_byte;
      length_semantics = LS_CHAR;
      if (NULL != val_str.ptr()) {
        int32_t trunc_len_byte = static_cast<int32_t>(ObCharset::strlen_byte_no_sp(cast_coll_type,
            val_str.ptr(), len_byte));
        res_len = static_cast<int32_t>(ObCharset::strlen_char(cast_coll_type,
            val_str.ptr(), trunc_len_byte));
      }
      if (type1.is_numeric_type() && !type1.is_integer_type()) {
        res_len += 1;
      }
    }
  }

  return ret;
}

int ObExprJsonValue::get_cast_inttc_len(ObExprResType &type1,
                                        ObExprResType &type2,
                                        ObExprTypeCtx &type_ctx,
                                        int32_t &res_len,
                                        int16_t &length_semantics,
                                        ObCollationType conn) const
{
  INIT_SUCC(ret);

  if (type1.is_literal()) { // literal
    if (ObStringTC == type1.get_type_class()) {
      res_len = type1.get_accuracy().get_length();
      length_semantics = type1.get_length_semantics();
    } else if (OB_FAIL(ObField::get_field_mb_length(type1.get_type(),
        type1.get_accuracy(), type1.get_collation_type(), res_len))) {
      LOG_WARN("failed to get filed mb length");
    }
  } else {
    res_len = CAST_STRING_DEFUALT_LENGTH[type1.get_type()];
    ObObjTypeClass tc1 = type1.get_type_class();
    int16_t scale = type1.get_accuracy().get_scale();
    if (ObDoubleTC == tc1) {
      res_len -= 1;
    } else if (ObDateTimeTC == tc1 && scale > 0) {
      res_len += scale - 1;
    } else if (OB_FAIL(get_cast_string_len(type1, type2, type_ctx, res_len, length_semantics, conn))) {
      LOG_WARN("fail to get cast string length", K(ret));
    } else {
      // do nothing
    }
  }

  return ret;
}

}
}