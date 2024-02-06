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

#define USING_LOG_PREFIX  SQL_ENG

#include "sql/engine/expr/ob_expr_md5.h"
#include <openssl/md5.h>
#include "share/object/ob_obj_cast.h"
//#include "sql/engine/expr/ob_expr_promotion_util.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprMd5::ObExprMd5(ObIAllocator &alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_MD5, N_MD5, 1, VALID_FOR_GENERATED_COL)
{
}

ObExprMd5::~ObExprMd5()
{
}

inline int ObExprMd5::calc_result_type1(ObExprResType &type, ObExprResType &str, ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  static const int64_t MD5_RES_BIT_LENGTH = 32;
  type.set_varchar();
  type.set_length(MD5_RES_BIT_LENGTH);
  type.set_collation_type(get_default_collation_type(type.get_type(), *type_ctx.get_session()));
  type.set_collation_level(CS_LEVEL_COERCIBLE);

  // str参数总是应该被转换成字符串
  str.set_calc_type(ObVarcharType);

  return ret;
}

int ObExprMd5::calc_md5(ObObj &result, const ObString &str, common::ObIAllocator *allocator, ObCollationType col_type) const
{
  int ret = OB_SUCCESS;
  UNUSED(col_type);
  char *md5_sum_buf = NULL;
  char *md5_str_buf = NULL;
  ObString::obstr_size_t md5_sum_len = MD5_LENGTH;
  // convert md5 sum to hexadecimal string, we need double bytes.
  // an extra byte for '\0' at the end of md5 str.
  ObString::obstr_size_t md5_str_len = MD5_LENGTH * 2 + 1;
  if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator null pointer",K(allocator));
  } else if (OB_ISNULL(md5_sum_buf = static_cast<char*>(allocator->alloc(md5_sum_len))) ||
             OB_ISNULL(md5_str_buf = static_cast<char*>(allocator->alloc(md5_str_len)))) {
    result.set_null();
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory failed",
             K(ret), K(md5_sum_buf), K(md5_str_buf), K(md5_sum_len), K(md5_str_len));
  } else {
    unsigned char *res = MD5(reinterpret_cast<const unsigned char *>(str.ptr()),
                             str.length(),
                             reinterpret_cast<unsigned char *>(md5_sum_buf));
    if (OB_ISNULL(res)) {
      // MD5() in openssl always return an pointer not NULL, so we need not check return value.
      // see:
      // http://www.openssl.org/docs/crypto/md5.html#DESCRIPTION
      // http://www.openssl.org/docs/crypto/md5.html#RETURN_VALUES
      // Even so, we HAVE TO check it here. You know it.
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("md5 res null pointer", K(ret), K(res));
    } else if (OB_FAIL(to_hex_cstr(md5_sum_buf, md5_sum_len, md5_str_buf, md5_str_len))) {
      result.set_null();
      LOG_WARN("to hex cstr error", K(ret));
    } else {
      size_t tmp_len = ObCharset::casedn(CS_TYPE_UTF8MB4_BIN,
                                         md5_str_buf,
                                         md5_str_len,
                                         md5_str_buf,
                                         md5_str_len);
      ObString::obstr_size_t len = static_cast<ObString::obstr_size_t>(tmp_len);
      ObString md5_str(len, len - 1/* do not contain \0 in the result */, md5_str_buf);
      result.set_varbinary(md5_str);
    }
  }
  return ret;
}

int ObExprMd5::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (rt_expr.arg_cnt_ != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("md5 expr should have one param", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of md5 expr is null", K(ret), K(rt_expr.args_));
  } else {
    CK(ObVarcharType == rt_expr.args_[0]->datum_meta_.type_);
    rt_expr.eval_func_ = ObExprMd5::calc_md5;
  }
  return ret;
}

int ObExprMd5::calc_md5(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *param_datum = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, param_datum))) {
    LOG_WARN("eval param value failed");
  } else if (OB_UNLIKELY(param_datum->is_null())) {
    expr_datum.set_null();
  } else {
    ObString raw_str = param_datum->get_string();
    ObString::obstr_size_t md5_raw_res_len = MD5_LENGTH;
    // convert md5 sum to hexadecimal string, we need double bytes.
    // an extra byte for '\0' at the end of md5 str.
    ObString::obstr_size_t md5_hex_res_len = MD5_LENGTH * 2 + 1;
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    char *md5_raw_res_buf = static_cast<char*>(alloc_guard.get_allocator().alloc(md5_raw_res_len));
    char *md5_hex_res_buf = expr.get_str_res_mem(ctx, md5_hex_res_len);
    if (OB_ISNULL(md5_raw_res_buf) || OB_ISNULL(md5_hex_res_buf)) {
      expr_datum.set_null();
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc memory failed",
            K(ret), K(md5_raw_res_buf), K(md5_hex_res_buf), K(md5_raw_res_len), K(md5_hex_res_len));
    } else {
      unsigned char *res = MD5(reinterpret_cast<const unsigned char *>(raw_str.ptr()),
                              raw_str.length(),
                              reinterpret_cast<unsigned char *>(md5_raw_res_buf));
      if (OB_ISNULL(res)) {
        // MD5() in openssl always return an pointer not NULL, so we need not check return value.
        // Even so, we HAVE TO check it here. You know it.
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("md5 res null pointer", K(ret), K(res));
      } else if (OB_FAIL(to_hex_cstr(md5_raw_res_buf, md5_raw_res_len,
                                    md5_hex_res_buf, md5_hex_res_len))) {
        expr_datum.set_null();
        LOG_WARN("to hex cstr error", K(ret));
      } else {
        size_t tmp_len = ObCharset::casedn(CS_TYPE_UTF8MB4_BIN,
                                          md5_hex_res_buf,
                                          md5_hex_res_len,
                                          md5_hex_res_buf,
                                          md5_hex_res_len);
        ObString::obstr_size_t len = static_cast<ObString::obstr_size_t>(tmp_len);
        ObString md5_str(len, len - 1/* do not contain \0 in the result */, md5_hex_res_buf);
        expr_datum.set_string(md5_str);
      }
    }
  }
  return ret;
}

}
}

