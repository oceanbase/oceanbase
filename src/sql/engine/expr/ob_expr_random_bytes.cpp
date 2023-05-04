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

#include "sql/engine/expr/ob_expr_random_bytes.h"

#include <openssl/rand.h>

namespace oceanbase
{
using namespace common;

namespace sql
{

ObExprRandomBytes::ObExprRandomBytes(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_RANDOM_BYTES, N_RANDOM_BYTES, 1, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprRandomBytes::~ObExprRandomBytes()
{
}

int ObExprRandomBytes::calc_result_type1(ObExprResType &type, 
                                         ObExprResType &len,
                                         ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  len.set_calc_type(ObInt32Type);
  static const int64_t MAX_RAND_BYTES = 1024;
  type.set_varbinary();
  type.set_collation_type(CS_TYPE_BINARY);
  type.set_collation_level(CS_LEVEL_COERCIBLE);
  type.set_length(MAX_RAND_BYTES);
  return ret;
}

int ObExprRandomBytes::calc_result1(ObObj &result, const ObObj &len,
                                    ObExprCtx &expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (len.is_null()) {
    result.set_null();
  } else if (len.is_int32()) {
    ObString::obstr_size_t length = len.get_int32(); 
    if (length >= 1 && length <= 1024) {
      char *buf = (char *)expr_ctx.calc_buf_->alloc(length);
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret), K(length));
      } else {
        RAND_bytes((unsigned char*)buf, length);
        ObString rand_str(length, length, buf);
        result.set_binary(rand_str);
      }
    } else {
      ret = OB_ERR_ARGUMENT_OUT_OF_RANGE;
      LOG_WARN("length value is out of range in random_bytes", 
                K(ret), K(length));
    }
  } else {
    ret = OB_OBJ_TYPE_ERROR;
    LOG_WARN("invalid argument type for random_bytes", 
             K(ret));
  }
  return ret;
}

int ObExprRandomBytes::generate_random_bytes(const ObExpr &expr, ObEvalCtx &ctx, 
                                             ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  if (ObInt32Type == expr.args_[0]->datum_meta_.type_) {
    ObDatum *num_datum = NULL;
    if (OB_FAIL(expr.args_[0]->eval(ctx, num_datum))) {
      LOG_WARN("eval param value failed", K(ret));
    } else if (num_datum->is_null()) {
      expr_datum.set_null();
    } else {
      ObString::obstr_size_t length = num_datum->get_int32();
      if (length >= 1 && length <= 1024) {
        char *buf = (char *)expr.get_str_res_mem(ctx, length);
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret), K(length));
        } else {
          RAND_bytes((unsigned char*)buf, length);
          ObString rand_str(length, length, buf);
          expr_datum.set_string(rand_str);
        }
      } else { 
        ret = OB_ERR_ARGUMENT_OUT_OF_RANGE;
        LOG_WARN("length value is out of range in random_bytes", 
                  K(ret), K(length));
      }
    }
  } else {
    ret  = OB_OBJ_TYPE_ERROR;
    LOG_WARN("invalid argument type for random_bytes", 
                K(ret));
  }
  
  return ret;
}

int ObExprRandomBytes::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                               ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  if (rt_expr.arg_cnt_ != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Incorrect parameter count in the call to native function random_bytes", 
              K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of random_bytes expr is null", K(ret), K(rt_expr.args_));
  } else {
    rt_expr.eval_func_ = ObExprRandomBytes::generate_random_bytes;
  }
  return ret;
}

} // sql
} // oceanbase