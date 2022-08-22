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

// This file is for implementation of func json_array
#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_json_array.h"
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

  return ret;
}

// for old sql engine
int ObExprJsonArray::calc_resultN(ObObj &result,
                                  const ObObj *objs,
                                  int64_t param_num,
                                  ObExprCtx &expr_ctx) const
{
  INIT_SUCC(ret);
  ObIAllocator *allocator = expr_ctx.calc_buf_;

  if (result_type_.get_collation_type() != CS_TYPE_UTF8MB4_BIN) {
    ret = OB_ERR_INVALID_JSON_CHARSET;
    LOG_WARN("invalid out put charset", K(ret), K(result_type_));
  } else if (OB_ISNULL(objs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(objs), K(param_num));
  } else if (OB_ISNULL(allocator)) { // check allocator
    ret = OB_NOT_INIT;
    LOG_WARN("varchar buffer not init", K(ret));
  } else {
    ObJsonArray j_arr(allocator);
    ObIJsonBase *j_base = &j_arr;
    for (int64_t i = 0; i < param_num && OB_SUCC(ret); ++i) {
      bool is_bool = false;
      ObIJsonBase *j_val;

      if (OB_FAIL(get_param_is_boolean(expr_ctx, objs[i], is_bool))) {
        LOG_WARN("failed: get_param_is_boolean", K(ret));
      } else if (OB_FAIL(ObJsonExprHelper::get_json_val(objs[i], expr_ctx, is_bool,
          allocator, j_val))) {
        ret = OB_ERR_INVALID_JSON_TEXT_IN_PARAM;
        LOG_USER_ERROR(OB_ERR_INVALID_JSON_TEXT_IN_PARAM);
      } else if (OB_FAIL(j_base->array_append(j_val))) {
        LOG_WARN("failed: json array append json value", K(ret), K(objs[i].get_type()));
      }
    }

    // set result(json bin)
    if (OB_SUCC(ret)) {
      ObString raw_bin;
      if (OB_FAIL(j_base->get_raw_binary(raw_bin, allocator))) {
        LOG_WARN("failed: get json raw binary", K(ret));
      } else {
        result.set_collation_type(CS_TYPE_UTF8MB4_BIN);
        result.set_string(ObJsonType, raw_bin.ptr(), raw_bin.length());
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
  common::ObArenaAllocator &temp_allocator = ctx.get_reset_tmp_alloc();
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
    } else {
      uint64_t length = raw_bin.length();
      char *buf = expr.get_str_res_mem(ctx, length);
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed: alloc memory for json array result", K(ret), K(length));
      } else {
        MEMMOVE(buf, raw_bin.ptr(), length);
        res.set_string(buf, length);
      }
    }
  }

  return ret;
}

int ObExprJsonArray::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_json_array;
  return OB_SUCCESS;
}

}
}
