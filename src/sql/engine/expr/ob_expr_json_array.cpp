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
      NOT_ROW_DIMENSION)
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
          if (types_stack[i].get_charset_type() != CHARSET_UTF8MB4) {
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
        if (OB_FAIL(ObJsonExprHelper::get_cast_type(types_stack[param_num - 2], dst_type))) {
          LOG_WARN("get cast dest type failed", K(ret));
        } else if (OB_FAIL(ObJsonExprHelper::set_dest_type(types_stack[0], type, dst_type, type_ctx))) {
          LOG_WARN("set dest type failed", K(ret));
        } else {
          type.set_calc_collation_type(type.get_collation_type());
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
  if (OB_SUCC(ret) && OB_FAIL(ObJsonExprHelper::eval_and_check_res_type(opt_res_type, dst_type, dst_len))) {
    LOG_WARN("fail to check returning type", K(ret));
  }

  int64_t& opt_null = opt_array[OPT_NULL_ID];
  ObJsonBuffer jsn_buf(&temp_allocator);
  if (OB_SUCC(ret) && OB_FAIL(jsn_buf.append("["))) {
    LOG_WARN("fail to append curly brace", K(ret));
  }

  for (uint32_t i = 0; OB_SUCC(ret) && i < max_val_idx; i += 2) {
    // [expr][format-json]: i -> expr, i + i -> format json
    ObDatum *opt_format = NULL;
    ObExpr *opt_expr = expr.args_[i + 1];
    ObObjType val_type = opt_expr->datum_meta_.type_;
    bool is_format_json = false;
    if (OB_UNLIKELY(OB_FAIL(opt_expr->eval(ctx, opt_format)))) {
      LOG_WARN("eval json arg failed", K(ret));
    } else if (val_type == ObNullType || opt_format->is_null()) {
    } else if (!ob_is_integer_type(val_type)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("input type error", K(val_type));
    } else {
      is_format_json = (opt_format->get_int() > 0);
    }

    ObExpr *val_expr = expr.args_[i];
    val_type = val_expr->datum_meta_.type_;
    ObCollationType cs_type = val_expr->datum_meta_.cs_type_;
    ObScale scale = val_expr->datum_meta_.scale_;
    ObDatum* j_datum = nullptr;
    bool can_only_be_null = false;
    if (OB_SUCC(ret) && is_format_json
       && (!ObJsonExprHelper::is_convertible_to_json(val_type) || val_type == ObJsonType)) {
      if (val_type == ObObjType::ObNumberType) {
        can_only_be_null = true;
      } else {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "CHAR", ob_obj_type_str(val_type));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(val_expr->eval(ctx, j_datum))) {
      LOG_WARN("eval json arg failed", K(ret), K(val_type));
    } else if (can_only_be_null && !j_datum->is_null()) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "CHAR", ob_obj_type_str(val_type));
    } else if (j_datum->is_null()) {
      if (opt_null == 0) {
        if (OB_FAIL(jsn_buf.append("null"))) {
          LOG_WARN("failed to append null", K(ret));
        }
      } else {
        continue;
      }
    } else if (ObJsonExprHelper::is_convertible_to_json(val_type) || ob_is_raw(val_type)) {
      if (OB_FAIL(ObJsonExprHelper::transform_convertible_2String(*val_expr, ctx, *j_datum,
                                                                  val_type,
                                                                  cs_type,
                                                                  jsn_buf,
                                                                  val_expr->obj_meta_.has_lob_header(),
                                                                  is_format_json,
                                                                  opt_strict == 1, i))) {
        LOG_WARN("failed to transform to string", K(ret), K(val_type));
      }
    } else if (OB_FAIL(ObJsonExprHelper::transform_scalar_2String(ctx, *j_datum,
                                                                  val_type,
                                                                  scale,
                                                                  ctx.exec_ctx_.get_my_session()->get_timezone_info(),
                                                                  jsn_buf))) {
      LOG_WARN("fail to parse value to json base.", K(ret), K(val_type));
    }

    if (OB_SUCC(ret) && i + 2 < max_val_idx && OB_FAIL(jsn_buf.append(","))) {
      LOG_WARN("failed to append comma", K(ret));
    }
  }

  // 添加右括号前删除多余的','
  if (OB_SUCC(ret) && jsn_buf.back() == ',') {
    jsn_buf.set_length(jsn_buf.length() - 1);
  }
  if (OB_SUCC(ret) && jsn_buf.append("]")) {
    LOG_WARN("failed to append brace", K(ret), K(jsn_buf.length()));
  }

  if (OB_SUCC(ret)) {
    ObString j_string;
    j_string.assign_ptr(jsn_buf.ptr(), jsn_buf.length());

    ObString res_string;
    ObJsonNode* j_base = nullptr;

    uint32_t parse_flag = ObJsonParser::JSN_STRICT_FLAG;
    ADD_FLAG_IF_NEED(opt_strict != 1, parse_flag, ObJsonParser::JSN_RELAXED_FLAG);

    if (dst_type == ObJsonType &&
        OB_FAIL(ObJsonParser::get_tree(&temp_allocator,
                                       j_string,
                                       j_base,
                                       parse_flag))) {
      LOG_WARN("fail to get json tree.", K(ret));
    } else if (dst_type == ObJsonType) {
      if (OB_FAIL(j_base->get_raw_binary(res_string, &temp_allocator))) {
        LOG_WARN("failed: get json raw binary", K(ret));
      }
    } else {
      res_string.assign_ptr(j_string.ptr(), j_string.length());
    }

    if (OB_SUCC(ret)) {
      uint64_t length = res_string.length();
      if (dst_type == ObVarcharType && length  > dst_len) {
        char res_ptr[OB_MAX_DECIMAL_PRECISION] = {0};
        if (OB_ISNULL(ObCharset::lltostr(dst_len, res_ptr, 10, 1))) {
          LOG_WARN("dst_len fail to string.", K(ret));
        }
        ret = OB_OPERATE_OVERFLOW;
        LOG_USER_ERROR(OB_OPERATE_OVERFLOW, res_ptr, "json_array");
      } else if (ObJsonExprHelper::pack_json_str_res(expr, ctx, res, res_string)) {
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
      ret = OB_ERR_INVALID_JSON_TEXT_IN_PARAM;
      LOG_WARN("failed: get_json_val failed", K(ret));
    } else if (OB_FAIL(j_base->array_append(j_val))) {
      LOG_WARN("failed: json array append json value", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObString raw_bin;
    if (OB_FAIL(j_base->get_raw_binary(raw_bin, &temp_allocator))) {
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
