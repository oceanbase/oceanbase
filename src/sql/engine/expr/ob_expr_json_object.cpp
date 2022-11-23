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
#include "ob_expr_json_object.h"
#include "sql/engine/expr/ob_expr_json_func_helper.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprJsonObject::ObExprJsonObject(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_JSON_OBJECT, N_JSON_OBJECT, OCCUR_AS_PAIR, NOT_ROW_DIMENSION)
{
}

ObExprJsonObject::~ObExprJsonObject()
{
}

int ObExprJsonObject::calc_result_typeN(ObExprResType& type,
                                        ObExprResType* types_stack,
                                        int64_t param_num,
                                        ObExprTypeCtx& type_ctx) const
{
  INIT_SUCC(ret);
  ObSQLSessionInfo *session = const_cast<ObSQLSessionInfo *>(type_ctx.get_session());
  ObExecContext *exec_ctx = nullptr;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (OB_ISNULL(exec_ctx = session->get_cur_exec_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec context is NULL", K(ret));
  } else if (OB_UNLIKELY(param_num < 0 || param_num % 2 != 0)) {
    ret = OB_ERR_PARAM_SIZE;
    const ObString name = "json_object";
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, name.length(), name.ptr());
  } else if (exec_ctx->is_ps_prepare_stage()) {
    // the ps prepare stage does not do type deduction, and directly gives a default type.
    type.set_json();
    type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObJsonType]).get_length());
  } else {
    type.set_json();
    type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObJsonType]).get_length());
    for (int64_t i = 0; OB_SUCC(ret) && i < param_num; i += 2) {
      if ((types_stack[i].get_type() == ObNullType)) {
        ret = OB_ERR_JSON_DOCUMENT_NULL_KEY;
        LOG_USER_ERROR(OB_ERR_JSON_DOCUMENT_NULL_KEY);
      } else if (ob_is_string_type(types_stack[i].get_type())) {
        if (types_stack[i].get_charset_type() == CHARSET_BINARY) {
          ret = OB_ERR_INVALID_JSON_CHARSET;
          LOG_USER_ERROR(OB_ERR_INVALID_JSON_CHARSET);
        } else if (types_stack[i].get_charset_type() != CHARSET_UTF8MB4) {
          types_stack[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
        }
      } else {
        types_stack[i].set_calc_type(ObLongTextType);
        types_stack[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
      }

      if (OB_SUCC(ret)) {
        if (ob_is_string_type(types_stack[i+1].get_type())) {
          if (types_stack[i+1].get_charset_type() != CHARSET_UTF8MB4) {
            types_stack[i+1].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
          }
        } else if (types_stack[i+1].get_type() == ObJsonType) {
          types_stack[i+1].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
        }
      }
    }
  }

  return ret;
}

// for new sql engine
int ObExprJsonObject::eval_json_object(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  ObJsonObject j_obj(&temp_allocator);
  ObIJsonBase *j_base = &j_obj;

  if (expr.datum_meta_.cs_type_ != CS_TYPE_UTF8MB4_BIN) {
    ret = OB_ERR_INVALID_JSON_CHARSET;
    LOG_WARN("invalid out put charset", K(ret), K(expr.datum_meta_.cs_type_));
  }

  for (int32 i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; i += 2) {
    ObExpr *arg = expr.args_[i];
    ObDatum *json_datum = NULL;  
    if (OB_FAIL(arg->eval(ctx, json_datum))) {
      LOG_WARN("failed: eval json args datum failed", K(ret));
    } else if (json_datum->is_null()) {
      ret = OB_ERR_JSON_DOCUMENT_NULL_KEY;
      LOG_USER_ERROR(OB_ERR_JSON_DOCUMENT_NULL_KEY);
      LOG_WARN("failed:json key is null", K(ret));
    } else {
      ObString key = json_datum->get_string();
      ObIJsonBase *j_val = NULL;
      if (OB_FAIL(ObJsonExprHelper::get_json_val(expr, ctx, &temp_allocator, i+1, j_val))) {
        ret = OB_ERR_INVALID_JSON_TEXT_IN_PARAM;
        LOG_USER_ERROR(OB_ERR_INVALID_JSON_TEXT_IN_PARAM);
      } else if (OB_FAIL(j_base->object_add(key, j_val))) {
        if (ret == OB_ERR_JSON_DOCUMENT_NULL_KEY) {
          LOG_USER_ERROR(OB_ERR_JSON_DOCUMENT_NULL_KEY);
        }
        LOG_WARN("failed: append json object kv", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObString raw_bin;
    if (OB_FAIL(j_base->get_raw_binary(raw_bin, &temp_allocator))) {
      LOG_WARN("failed: get json raw binary", K(ret));
    } else {
      uint64_t length = raw_bin.length();
      char *buf = expr.get_str_res_mem(ctx, length);
      if (buf) {
        MEMCPY(buf, raw_bin.ptr(), length);
        res.set_string(buf, length);
      } else {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed: alloc memory for json object result.", K(ret), K(length));
      }
    }
  }

  return ret;
}

int ObExprJsonObject::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_json_object;
  return OB_SUCCESS;
}

}
}
