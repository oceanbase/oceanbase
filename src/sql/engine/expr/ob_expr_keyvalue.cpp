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

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/expr/ob_expr_keyvalue.h"
#include "lib/oblog/ob_log.h"
#include "objit/common/ob_item_type.h"
#include "sql/session/ob_sql_session_info.h"
#include "storage/ob_storage_util.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
ObExprKeyValue::ObExprKeyValue(ObIAllocator &alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_KEYVALUE, N_KEYVALUE, MORE_THAN_ONE, VALID_FOR_GENERATED_COL)
{
  need_charset_convert_ = false;
}
ObExprKeyValue::~ObExprKeyValue()
{
}
int ObExprKeyValue::calc_result_typeN(ObExprResType &type,
                                      ObExprResType *types,
                                      int64_t param_num,
                                      ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (param_num != 2 && param_num != 4) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("the param number of keyvalue should be 2 or 4", K(ret), K(param_num));
  } else if (ObJsonType == types[0].get_type()) {
    ObString func_name("KEYVALUE");
    ret = OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE;
    LOG_WARN("The first argument type is incorrect", K(ret), K(types[0].get_type()));
    LOG_USER_ERROR(OB_ERR_WRONG_FUNC_ARGUMENTS_TYPE, func_name.length(), func_name.ptr());
  } else if (lib::is_mysql_mode()) {
    if (ObTextType == types[0].get_type()
        || ObMediumTextType == types[0].get_type()
        || ObLongTextType == types[0].get_type()) {
      type.set_type(types[0].get_type());
      type.set_length(types[0].get_length());
    } else {
      type.set_varchar();
    }
    if (OB_FAIL(aggregate_charsets_for_string_result(type, types, 1, type_ctx))) {
      LOG_WARN("aggregate_charsets_for_string_result failed", K(ret));
    } else {
      types[0].set_calc_meta(type);
      for (int64_t i = 1; i < param_num; i++) {
        types[i].set_calc_type(ObVarcharType);
        types[i].set_calc_collation_type(type.get_collation_type());
        types[i].set_calc_collation_level(type.get_collation_level());
      }
    }
  } else {
    ObLengthSemantics length_semantic = type.get_length_semantics();
    ObSEArray<ObExprResType*, 1, ObNullAllocator> params;
    OZ (params.push_back(&types[0]));
    OZ (aggregate_string_type_and_charset_oracle(*type_ctx.get_session(), params, type));
    types[0].set_calc_meta(type);
    types[0].set_calc_collation_type(type.get_collation_type());
    types[0].set_calc_collation_level(type.get_collation_level());
    types[0].set_calc_length_semantics(length_semantic);
    for (int64_t i = 1; OB_SUCC(ret) && i < param_num; i++) {
      types[i].set_calc_type(ObVarcharType);
      types[i].set_calc_collation_type(type.get_collation_type());
      types[i].set_calc_collation_level(type.get_collation_level());
      types[i].set_calc_length_semantics(length_semantic);
    }
  }
  return ret;
}

int ObExprKeyValue::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_key_value_expr;
  rt_expr.eval_vector_func_ = calc_key_value_expr_vector;
  return ret;
}

OB_INLINE void split_and_get_first_item(ObCollationType cs_type,
                                        const ObString &str,
                                        const ObString &delimiter,
                                        ObString &res)
{
  const char *res_ptr = NULL;
  int32_t res_len = 0;
  if (str.empty()) {
  } else if (delimiter.empty() || delimiter.length() > str.length()) {
    res_ptr = str.ptr();
    res_len = str.length();
  } else {
    int64_t idx = 0;
    idx = ObCharset::instrb(cs_type, str.ptr(), str.length(),
                                  delimiter.ptr(), delimiter.length());
    if (idx == -1) {
      res_ptr = str.ptr();
      res_len = str.length();
    } else if (idx == 0) {
      int64_t offset = delimiter.length();
      while(idx == 0 && offset < str.length()) {
        idx = ObCharset::instrb(cs_type, str.ptr() + offset, str.length() - offset,
                                  delimiter.ptr(), delimiter.length());
        if (idx == -1) {
          res_ptr = str.ptr() + offset;
          res_len = str.length() - offset;
        } else if (idx == 0) {
          offset += delimiter.length();
        } else {
          res_ptr = str.ptr() + offset;
          res_len = idx;
        }
      }
    } else {
      res_ptr = str.ptr();
      res_len = idx;
    }
  }
  res.assign_ptr(res_ptr, res_len);
}

OB_INLINE bool get_first_matched_value(ObCollationType cs_type,
                                       ObString dict_str,
                                       const ObString &item_delim,
                                       const ObString &key_delim,
                                       const ObString &key,
                                       ObString &value)
{
  bool matched = false;
  ObString one_item;
  const char *key_ptr = key.ptr();
  const int32_t key_len = key.length();
  const char *key_delim_ptr = key_delim.ptr();
  const int32_t key_delim_len = key_delim.length();
  const int32_t item_delim_len = item_delim.length();
  if (!key_delim.empty() && !item_delim.empty() && !key.empty()) {
    while (!dict_str.empty()) {
      split_and_get_first_item(cs_type, dict_str, item_delim, one_item);
      if (one_item.empty()) {
      } else {
        if (one_item.length() < key_len + key_delim_len) {
        } else if (0 == ObCharset::strcmp(cs_type, one_item.ptr(),
                                  key_len,
                                  key_ptr,
                                  key_len) &&
            0 == ObCharset::strcmp(cs_type, one_item.ptr() + key_len,
                            key_delim_len,
                            key_delim_ptr,
                            key_delim_len)) {
          value.assign_ptr(one_item.ptr() + key_len + key_delim_len,
                            one_item.length() - key_len - key_delim_len);
          matched = true;
          break;
        }
      }
      dict_str += one_item.length() + item_delim_len;
    }
  }
  return matched;
}

int ObExprKeyValue::calc_key_value_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                        ObDatum &res)
{
  int ret = OB_SUCCESS;
  bool null_res = false;
  ObArenaAllocator tmp_alloc;
  ObString item_delim;
  ObString key_delim;
  ObDatum *dict_str_datum = NULL;
  ObDatum *key_str_datum = NULL;
  bool is_oracle_mode = lib::is_oracle_mode();
  ObCollationType cs_type = expr.args_[0]->datum_meta_.cs_type_;
  if (expr.arg_cnt_ == 2) {
    if (OB_FAIL(expr.eval_param_value(ctx, dict_str_datum, key_str_datum))) {
      LOG_WARN("eval args failed", K(ret));
    } else if (dict_str_datum->is_null() ||
               key_str_datum->is_null() ||
               dict_str_datum->get_string().empty() ||
               key_str_datum->get_string().empty()) {
      null_res = true;
    } else {
      // create default delimiter
      OZ(ObCharset::charset_convert(tmp_alloc, ";", CS_TYPE_UTF8MB4_BIN, cs_type, item_delim));
      OZ(ObCharset::charset_convert(tmp_alloc, ":", CS_TYPE_UTF8MB4_BIN, cs_type, key_delim));
    }
  } else if (expr.arg_cnt_ == 4) {
    ObDatum *item_delim_datum = NULL;
    ObDatum *key_delim_datum = NULL;
    if (OB_FAIL(expr.eval_param_value(ctx, dict_str_datum, item_delim_datum,
                                      key_delim_datum, key_str_datum))) {
      LOG_WARN("eval args failed", K(ret));
    } else if (dict_str_datum->is_null() || item_delim_datum->is_null() ||
               key_delim_datum->is_null() || key_str_datum->is_null() ||
               dict_str_datum->get_string().empty() ||
               item_delim_datum->get_string().empty() ||
               key_delim_datum->get_string().empty() ||
               key_str_datum->get_string().empty()) {
      null_res = true;
    } else {
      item_delim = item_delim_datum->get_string();
      key_delim = key_delim_datum->get_string();
      if (0 == ObCharset::strcmp(cs_type, item_delim, key_delim)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "KEYVALUE function. The split1 and split2 cannot be the same.");
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (null_res) {
    res.set_null();
  } else {
    ObString value;
    if (!ob_is_text_tc(expr.args_[0]->datum_meta_.type_)) {
      if (get_first_matched_value(cs_type, dict_str_datum->get_string(),
                                item_delim, key_delim,
                                key_str_datum->get_string(), value)) {
        if (is_oracle_mode && value.empty()) {
          res.set_null();
        } else {
          res.set_string(value);
        }
      } else {
        res.set_null();
      }
    } else {
      ObEvalCtx::TempAllocGuard alloc_guard(ctx);
      ObIAllocator &tmp_alloc = alloc_guard.get_allocator();
      ObString dict_str;
      ObTextStringDatumResult output_result(expr.datum_meta_.type_, &expr, &ctx, &res);
      if (OB_FAIL(ObTextStringHelper::get_string(expr, tmp_alloc, 0, dict_str_datum, dict_str))) {
        LOG_WARN("get full text string failed ", K(ret));
      } else {
        if (get_first_matched_value(cs_type, dict_str,
                                item_delim, key_delim,
                                key_str_datum->get_string(), value)) {
          if (is_oracle_mode && value.empty()) {
            output_result.set_result_null();
          } else {
            if (OB_FAIL(output_result.init(value.length()))) {
                  LOG_WARN("init TextString result failed", K(ret));
            } else {
              output_result.append(value);
              output_result.set_result();
            }
          }
        } else {
          output_result.set_result_null();
        }
      }
    }
  }
  return ret;
}

int ObExprKeyValue::calc_key_value_expr_vector(const ObExpr &expr,
                                              ObEvalCtx &ctx,
                                              const ObBitVector &skip,
                                              const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard alloc_guard(ctx);
  ObIAllocator &tmp_alloc = alloc_guard.get_allocator();
  ObString default_item_delim;
  ObString default_key_delim;
  bool is_oracle_mode = lib::is_oracle_mode();
  ObCollationType cs_type = expr.args_[0]->datum_meta_.cs_type_;
  if (OB_FAIL(expr.eval_vector_param_value(ctx, skip, bound))) {
    LOG_WARN("eval args failed", K(ret));
  } else {
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    ObIVector *dict_str_vec = expr.args_[0]->get_vector(ctx);
    ObIVector *item_delim_vec = expr.arg_cnt_ == 4 ? expr.args_[1]->get_vector(ctx) : NULL;
    ObIVector *key_delim_vec = expr.arg_cnt_ == 4 ? expr.args_[2]->get_vector(ctx) : NULL;
    ObIVector *key_str_vec = expr.args_[expr.arg_cnt_ - 1]->get_vector(ctx);
    ObIVector *value_str_vec = expr.get_vector(ctx);
    ObString value;
    if (expr.arg_cnt_ == 2) {
      // create default delimiter
      OZ(ObCharset::charset_convert(tmp_alloc, ";", CS_TYPE_UTF8MB4_BIN, cs_type, default_item_delim));
      OZ(ObCharset::charset_convert(tmp_alloc, ":", CS_TYPE_UTF8MB4_BIN, cs_type, default_key_delim));
    }
    for (int64_t i = bound.start(); OB_SUCC(ret) && i < bound.end(); ++i) {
      if (skip.at(i) || eval_flags.at(i)) {
        continue;
      } else if (dict_str_vec->is_null(i) ||
                 dict_str_vec->get_string(i).empty() ||
                 key_str_vec->is_null(i) ||
                 key_str_vec->get_string(i).empty() ||
                 (expr.arg_cnt_ == 4 &&
                  (item_delim_vec->is_null(i) ||
                   item_delim_vec->get_string(i).empty())) ||
                 (expr.arg_cnt_ == 4 &&
                  (key_delim_vec->is_null(i) ||
                   key_delim_vec->get_string(i).empty()))) {
        value_str_vec->set_null(i);
      } else if (expr.arg_cnt_ == 4 &&
                 0 == ObCharset::strcmp(cs_type, item_delim_vec->get_string(i),
                                        key_delim_vec->get_string(i))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "KEYVALUE function. The split1 and split2 cannot be the same.");
      } else {
        if (!ob_is_text_tc(expr.args_[0]->datum_meta_.type_)) {
          if (get_first_matched_value(
                        cs_type, dict_str_vec->get_string(i),
                        expr.arg_cnt_ == 4 ? item_delim_vec->get_string(i)
                                            : default_item_delim,
                        expr.arg_cnt_ == 4 ? key_delim_vec->get_string(i)
                                            : default_key_delim,
                        key_str_vec->get_string(i), value)) {
            if (is_oracle_mode && value.empty()) {
              value_str_vec->set_null(i);
            } else {
              value_str_vec->set_string(i, value);
            }
          } else {
            value_str_vec->set_null(i);
          }
        } else {
          ObString dict_str;
          ObTextStringDatumResult output_result(expr.datum_meta_.type_, &expr, &ctx, value_str_vec, i);
          if (OB_FAIL(ObTextStringHelper::get_string<ObVectorBase>(
                  expr, tmp_alloc, 0, i,
                  static_cast<ObVectorBase *>(dict_str_vec), dict_str))) {
            LOG_WARN("get full text string failed ", K(ret));
          } else {
            if (get_first_matched_value(
                    cs_type, dict_str,
                    expr.arg_cnt_ == 4 ? item_delim_vec->get_string(i)
                                       : default_item_delim,
                    expr.arg_cnt_ == 4 ? key_delim_vec->get_string(i)
                                       : default_key_delim,
                    key_str_vec->get_string(i), value)) {
              if (is_oracle_mode && value.empty()) {
                output_result.set_result_null();
              } else {
                if (OB_FAIL(output_result.init_with_batch_idx(value.length(), i))) {
                  LOG_WARN("init TextString result failed", K(ret));
                } else {
                  output_result.append(value);
                  output_result.set_result();
                }
              }
            } else {
              output_result.set_result_null();
            }
          }
        }
      }
      eval_flags.set(i);
    }
  }
  return ret;
}
DEF_SET_LOCAL_SESSION_VARS(ObExprKeyValue, raw_expr) {
  int ret = OB_SUCCESS;
  SET_LOCAL_SYSVAR_CAPACITY(1);
  EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_COLLATION_CONNECTION);
  return ret;
}

}
}
