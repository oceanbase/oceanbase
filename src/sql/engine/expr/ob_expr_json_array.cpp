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
 * This file is for implementation of func json_array
*/


#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_json_array.h"
#include "share/ob_json_access_utils.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_json_func_helper.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprJsonArray::ObExprJsonArray(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc,
      T_FUN_SYS_JSON_ARRAY,
      N_JSON_ARRAY, 
      PARAM_NUM_UNKNOWN,
      VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprJsonArray::~ObExprJsonArray()
{
}

int ObExprJsonArray::calc_result_typeN(ObExprResType& type,
                                       ObExprResType* types_stack,
                                       int64_t param_num,
                                       ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  INIT_SUCC(ret);
  if (lib::is_mysql_mode()) {
    if (OB_UNLIKELY(param_num < 0)) {
      ret = OB_ERR_PARAM_SIZE;
      ObString name("json_array");
      LOG_USER_ERROR(OB_ERR_PARAM_SIZE, name.length(), name.ptr());
    } else {
      // param_num >= 0
      type.set_json();
      type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObJsonType]).get_length());
      for (int64_t i = 0; i < param_num; i++) {
        if (ob_is_string_type(types_stack[i].get_type())) {
          if (types_stack[i].get_type() == ObVarcharType && types_stack[i].get_collation_type() == CS_TYPE_BINARY) {
            types_stack[i].set_calc_type(ObHexStringType);
            types_stack[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
          } else if (types_stack[i].get_charset_type() != CHARSET_UTF8MB4) {
            types_stack[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
          }
        } else if (types_stack[i].get_type() == ObJsonType) {
          types_stack[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
        }
      }
    }
  } else {
    if (OB_UNLIKELY(param_num < 4)) {
      ret = OB_ERR_PARAM_SIZE;
      ObString name("json_array");
      LOG_USER_ERROR(OB_ERR_PARAM_SIZE, name.length(), name.ptr());
    } else {
      int64_t ele_idx = param_num - 3;
      for (int64_t i = 0; i < ele_idx && OB_SUCC(ret); i += 2) {
        if (types_stack[i].get_type() == ObNullType) {
        } else if (ob_is_string_type(types_stack[i].get_type())) {
          if (types_stack[i].get_collation_type() == CS_TYPE_BINARY) {
            types_stack[i].set_calc_collation_type(CS_TYPE_BINARY);
          } else if (types_stack[i].get_charset_type() != CHARSET_UTF8MB4) {
            types_stack[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
          }
          //types_stack[i].set_calc_collation_type(types_stack[i].get_collation_type());
        } else if (types_stack[i].get_type() == ObJsonType) {
          types_stack[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
        } else {
          types_stack[i].set_calc_type(types_stack[i].get_type());
          types_stack[i].set_calc_collation_type(types_stack[i].get_collation_type());
        }

        if (OB_FAIL(ret)) {
        } else if (!ob_is_integer_type(types_stack[i + 1].get_type())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to calc type param type should be int type", K(types_stack[i + 1].get_type()));
        } else {
          types_stack[i + 1].set_calc_collation_type(types_stack[i + 1].get_collation_type());
          types_stack[i + 1].set_calc_type(types_stack[i + 1].get_type());
        }
      }

      // returning type : 2
      if (OB_SUCC(ret)) {
        ObExprResType dst_type;
        dst_type.set_type(ObJsonType);
        dst_type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
        int16_t length_semantics = (OB_NOT_NULL(type_ctx.get_session())
                ? type_ctx.get_session()->get_actual_nls_length_semantics() : LS_BYTE);
        dst_type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObJsonType]).get_length());

        dst_type.set_collation_level(CS_LEVEL_IMPLICIT);
        if (ele_idx > 0 && OB_FAIL(ObJsonExprHelper::parse_res_type(types_stack[0], types_stack[param_num - 2], dst_type, type_ctx))) {
          LOG_WARN("get cast dest type failed", K(ret));
        } else if (OB_FAIL(ObJsonExprHelper::set_dest_type(types_stack[0], type, dst_type, type_ctx))) {
          LOG_WARN("set dest type failed", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        if (!ob_is_integer_type(types_stack[param_num - 3].get_type())
            || !ob_is_integer_type(types_stack[param_num - 1].get_type())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to calc type param type should be int type",
                   K(types_stack[param_num - 1].get_type()), K(types_stack[param_num - 3].get_type()));
        } else {
          types_stack[param_num - 3].set_calc_collation_type(types_stack[param_num - 3].get_collation_type());
          types_stack[param_num - 3].set_calc_type(types_stack[param_num - 3].get_type());

          types_stack[param_num - 1].set_calc_collation_type(types_stack[param_num - 1].get_collation_type());
          types_stack[param_num - 1].set_calc_type(types_stack[param_num - 1].get_type());
        }
      }
    }
  }

  return ret;
}

// for new sql engine
int ObExprJsonArray::eval_ora_json_array(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  ObDatum *json_datum = NULL;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();

  uint32_t max_val_idx = expr.arg_cnt_ - 3;
  int64_t opt_array[OPT_MAX_ID] = {0};
  int64_t& opt_strict = opt_array[OPT_STRICT_ID];

  for (size_t i = max_val_idx; OB_SUCC(ret) && i < expr.arg_cnt_; i++) {
    ObDatum *opt_datum = NULL;
    ObExpr *opt_expr = expr.args_[i];
    ObObjType val_type = opt_expr->datum_meta_.type_;
    if (OB_UNLIKELY(OB_FAIL(opt_expr->eval(ctx, opt_datum)))) {
      LOG_WARN("eval json arg failed", K(ret));
    } else if (val_type == ObNullType || opt_datum->is_null()) {
    } else if (!ob_is_integer_type(val_type)) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("input type error", K(val_type));
    } else {
      opt_array[i-max_val_idx] = opt_datum->get_int();
    }
  }

  ObObjType dst_type;
  int32_t dst_len = OB_MAX_TEXT_LENGTH;
  int64_t& opt_res_type = opt_array[OPT_TYPE_ID];
  if (OB_FAIL(ret)) {
  } else if (opt_res_type == 0) {
    dst_type = ObJsonType;
  } else if (OB_FAIL(ObJsonExprHelper::eval_and_check_res_type(opt_res_type, dst_type, dst_len))) {
    LOG_WARN("fail to check returning type", K(ret));
  }

  int64_t& opt_null = opt_array[OPT_NULL_ID];
  bool is_strict = (opt_strict > 0 || (dst_type == ObJsonType && opt_res_type > 0));
  bool is_null_absent = opt_null > 0 ;
  ObJsonArray j_arr(&temp_allocator);

  for (uint32_t i = 0; OB_SUCC(ret) && i < max_val_idx; i += 2) {
    // [expr][format-json]: i -> expr, i + i -> format json
    ObDatum *opt_format = NULL;
    ObExpr *opt_expr = expr.args_[i + 1];
    ObObjType val_type = opt_expr->datum_meta_.type_;
    bool is_format_json = false;
    ObIJsonBase* j_val = nullptr;
    if (OB_UNLIKELY(OB_FAIL(opt_expr->eval(ctx, opt_format)))) {
      LOG_WARN("eval json arg failed", K(ret));
    } else if (val_type == ObNullType || opt_format->is_null()) {
    } else if (!ob_is_integer_type(val_type)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("input type error", K(val_type));
    } else {
      is_format_json = (opt_format->get_int() > 0);
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObJsonExprHelper::eval_oracle_json_val(
                expr.args_[i], ctx, &temp_allocator, j_val, is_format_json, is_strict, false, is_null_absent))) {
      LOG_WARN("failed to get json value node.", K(ret), K(val_type));
    } else if (OB_ISNULL(j_val)) {
    } else if (OB_FAIL(j_arr.append(static_cast<ObJsonNode*>(j_val)))) {
      LOG_WARN("failed to append in array.", K(ret), K(i));
    }
  }

  if (OB_SUCC(ret)) {
    ObJsonBuffer string_buffer(&temp_allocator);
    ObString res_string;

    if (ObJsonParser::is_json_doc_over_depth(j_arr.depth())) {
      ret = OB_ERR_JSON_OUT_OF_DEPTH;
      LOG_WARN("current json over depth", K(ret), K(j_arr.depth()));
    } else if (dst_type == ObJsonType) {
      if (OB_FAIL(ObJsonWrapper::get_raw_binary(&j_arr, res_string, &temp_allocator))) {
        LOG_WARN("failed: get json raw binary", K(ret));
      }
    } else {
      if (OB_FAIL(string_buffer.reserve(j_arr.get_serialize_size()))) {
        LOG_WARN("fail to reserve string.", K(ret), K(j_arr.get_serialize_size()));
      } else if (OB_FAIL(j_arr.print(string_buffer, true, false))) {
        LOG_WARN("failed: get json string text", K(ret));
      } else {
        ObCollationType in_cs_type = CS_TYPE_UTF8MB4_BIN;
        ObCollationType dst_cs_type = expr.obj_meta_.get_collation_type();
        ObString temp_str = string_buffer.string();

        if (OB_FAIL(ObJsonExprHelper::convert_string_collation_type(in_cs_type,
                                                                    dst_cs_type,
                                                                    &temp_allocator,
                                                                    temp_str,
                                                                    res_string))) {
          LOG_WARN("fail to convert string result", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      uint64_t length = res_string.length();
      if (dst_type == ObVarcharType && length  > dst_len) {
        char res_ptr[OB_MAX_DECIMAL_PRECISION] = {0};
        if (OB_ISNULL(ObCharset::lltostr(dst_len, res_ptr, 10, 1))) {
          LOG_WARN("dst_len fail to string.", K(ret));
        }
        ret = OB_OPERATE_OVERFLOW;
        LOG_USER_ERROR(OB_OPERATE_OVERFLOW, res_ptr, N_JSON_ARRAY);
      } else if (OB_FAIL(ObJsonExprHelper::pack_json_str_res(expr, ctx, res, res_string))) {
        LOG_WARN("fail to pack ressult.", K(ret));
      }
    }
  }

  return ret;
}

// for new sql engine
int ObExprJsonArray::eval_json_array(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  ObDatum *json_datum = NULL;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  ObJsonArray j_arr(&temp_allocator);
  ObIJsonBase *j_base = &j_arr;

  if (expr.datum_meta_.cs_type_ != CS_TYPE_UTF8MB4_BIN) {
    ret = OB_ERR_INVALID_JSON_CHARSET;
    LOG_WARN("invalid out put charset", K(ret), K(expr.datum_meta_.cs_type_));
  }

  for (uint32_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; i++) {
    ObIJsonBase *j_val;
    if (OB_FAIL(ObJsonExprHelper::get_json_val(expr, ctx, &temp_allocator, i, j_val))) {
      LOG_WARN("failed: get_json_val failed", K(ret));
    } else if (OB_FAIL(j_base->array_append(j_val))) {
      LOG_WARN("failed: json array append json value", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObString raw_bin;
    if (ObJsonParser::is_json_doc_over_depth(j_arr.depth())) {
      ret = OB_ERR_JSON_OUT_OF_DEPTH;
      LOG_WARN("current json over depth", K(ret), K(j_arr.depth()));
    } else if (OB_FAIL(ObJsonWrapper::get_raw_binary(j_base, raw_bin, &temp_allocator))) {
      LOG_WARN("failed: json get binary", K(ret));
    } else if (OB_FAIL(ObJsonExprHelper::pack_json_str_res(expr, ctx, res, raw_bin))) {
      LOG_WARN("fail to pack json result", K(ret));
    }
  }

  return ret;
}

int ObExprJsonArray::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  if (lib::is_oracle_mode()) {
    rt_expr.eval_func_ = eval_ora_json_array;
  } else {
    rt_expr.eval_func_ = eval_json_array;
  }

  return OB_SUCCESS;
}

}
}
