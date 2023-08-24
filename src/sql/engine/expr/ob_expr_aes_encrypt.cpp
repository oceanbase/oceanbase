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

#define USING_LOG_PREFIX SQL_EXE
#include "ob_expr_aes_encrypt.h"
#include "share/object/ob_obj_cast.h"
#include "share/ob_encryption_util.h"
#include "ob_expr_extract.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::share;
using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

int get_encryption_value(int64_t &encryption, ObSQLSessionInfo *session)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (OB_FAIL(session->get_sys_variable(SYS_VAR_BLOCK_ENCRYPTION_MODE, encryption))) {
    LOG_WARN("fail to get block encryption variable", K(ret));
  } else {
    ++encryption; //0为invalid_mdoe,所以需要+1
  }
  return ret;
}

ObExprAesEncrypt::ObExprAesEncrypt(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc,
                         T_FUN_AES_ENCRYPT,
                         N_AES_ENCRYPT,
                         TWO_OR_THREE,
                         NOT_VALID_FOR_GENERATED_COL,
                         NOT_ROW_DIMENSION)
{
}
ObExprAesEncrypt::~ObExprAesEncrypt() {}

int ObExprAesEncrypt::calc_result_typeN(ObExprResType& type,
                                        ObExprResType* types_stack,
                                        int64_t param_num,
                                        ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(types_stack)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null types",K(ret));
  } else if (OB_UNLIKELY(param_num > 3 || param_num < 2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param num is not correct", K(param_num));
  } else {
    for (int i = 0; i < param_num; ++i) {
      types_stack[i].set_calc_type(common::ObVarcharType);
      types_stack[i].set_calc_collation_type(types_stack[i].get_collation_type());
      types_stack[i].set_calc_collation_level(types_stack[i].get_collation_level());
    }
    type.set_varbinary();
    type.set_length((types_stack[0].get_length() * 3 / ObBlockCipher::OB_CIPHER_BLOCK_LENGTH + 1) *
                     ObBlockCipher::OB_CIPHER_BLOCK_LENGTH);
    type.set_collation_level(CS_LEVEL_COERCIBLE);
  }
  return ret;
}

int ObExprAesEncrypt::eval_aes_encrypt(const ObExpr &expr, ObEvalCtx &ctx,
                                       ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *src = NULL;
  ObDatum *key = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, src, key))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (src->is_null() || key->is_null()) {
    res.set_null();
  } else {
    int64_t encryption = 0;
    OZ(get_encryption_value(encryption, ctx.exec_ctx_.get_my_session()));
    CK(2 == expr.arg_cnt_ || 3 == expr.arg_cnt_);
    if (OB_SUCC(ret)) {
      const ObString &src_str = expr.locate_param_datum(ctx, 0).get_string();
      const ObString &key_str = expr.locate_param_datum(ctx, 1).get_string();
      int64_t out_len = 0;
      ObEvalCtx::TempAllocGuard alloc_guard(ctx);
      ObIAllocator &calc_alloc = alloc_guard.get_allocator();
      ObCipherOpMode opmode = static_cast<ObCipherOpMode>(encryption);
      int buf_length = (src_str.length() / ObBlockCipher::OB_CIPHER_BLOCK_LENGTH + 1) * ObBlockCipher::OB_CIPHER_BLOCK_LENGTH;
      char *buf = static_cast<char *>(calc_alloc.alloc(buf_length));
      bool is_ecb = opmode <= ObCipherOpMode::ob_aes_256_ecb &&
                    opmode >= ObCipherOpMode::ob_aes_128_ecb;
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory failed", K(ret));
      } else if (!is_ecb && 2 == expr.arg_cnt_) {
        ret = OB_ERR_PARAM_SIZE;
        LOG_WARN("param num error", K(ret), K(expr.arg_cnt_), K(encryption));
      } else if (3 == expr.arg_cnt_ && is_ecb) {
        // just user warn, not set ret error.
        LOG_USER_WARN(OB_ERR_INVALID_INPUT_STRING, "iv");
      }
      if (OB_SUCC(ret)) {
        if (2 == expr.arg_cnt_ || is_ecb) {
          OZ(ObBlockCipher::encrypt(key_str.ptr(), key_str.length(), src_str.ptr(),
                                    src_str.length(), buf_length, NULL, 0, NULL, 0, 0, opmode, buf,
                                    out_len, NULL));
        } else {
          ObString iv_str = expr.locate_param_datum(ctx, 2).get_string();
          OV(iv_str.length() >= ObBlockCipher::OB_DEFAULT_IV_LENGTH, OB_ERR_AES_IV_LENGTH);
          OX(iv_str.assign(iv_str.ptr(), ObBlockCipher::OB_DEFAULT_IV_LENGTH));
          OZ(ObBlockCipher::encrypt(key_str.ptr(), key_str.length(), src_str.ptr(), src_str.length(),
                                    buf_length, iv_str.ptr(), iv_str.length(), NULL, 0, 0, opmode,
                                    buf, out_len, NULL));
        }
      }
      if (OB_SUCC(ret)) {
        ObExprStrResAlloc res_alloc(expr, ctx);
        char *res_buf = static_cast<char*>(res_alloc.alloc(out_len));
        OV(OB_NOT_NULL(res_buf), OB_ALLOCATE_MEMORY_FAILED);
        OX(MEMCPY(res_buf, buf, out_len));
        OX(res.set_string(res_buf, out_len));
      }
    }
  }

  return ret;
}

int ObExprAesEncrypt::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_aes_encrypt;
  return ret;
}

//---------------------------------------分割线
ObExprAesDecrypt::ObExprAesDecrypt(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc,
                         T_FUN_AES_DECRYPT,
                         N_AES_DECRYPT,
                         TWO_OR_THREE,
                         NOT_VALID_FOR_GENERATED_COL,
                         NOT_ROW_DIMENSION)
{
}
ObExprAesDecrypt::~ObExprAesDecrypt() {}

int ObExprAesDecrypt::calc_result_typeN(ObExprResType& type,
                                        ObExprResType* types_stack,
                                        int64_t param_num,
                                        ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(types_stack)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null types",K(ret));
  } else if (OB_UNLIKELY(param_num > 3 || param_num < 2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param num is not correct", K(param_num));
  } else {
    for (int i = 0; i < param_num; ++i) {
      types_stack[i].set_calc_type(common::ObVarcharType);
    }
    type.set_varbinary();
    type.set_length(types_stack[0].get_length() * 3);
    type.set_collation_level(CS_LEVEL_COERCIBLE);
  }
  return ret;
}

int ObExprAesDecrypt::eval_aes_decrypt(const ObExpr &expr, ObEvalCtx &ctx,
                                       ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *src = NULL;
  ObDatum *key = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, src, key))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (src->is_null() || key->is_null()) {
    res.set_null();
  } else {
    int64_t encryption = 0;
    OZ(get_encryption_value(encryption, ctx.exec_ctx_.get_my_session()));
    CK(2 == expr.arg_cnt_ || 3 == expr.arg_cnt_);
    bool is_null = false;
    if (OB_SUCC(ret)) {
      const ObString &src_str = expr.locate_param_datum(ctx, 0).get_string();
      const ObString &key_str = expr.locate_param_datum(ctx, 1).get_string();
      int64_t out_len = 0;
      ObEvalCtx::TempAllocGuard alloc_guard(ctx);
      ObIAllocator &calc_alloc = alloc_guard.get_allocator();
      ObCipherOpMode opmode = static_cast<ObCipherOpMode>(encryption);
      const int64_t buf_len = src_str.length() + 1;
      char *buf = static_cast<char *>(calc_alloc.alloc(buf_len));
      bool is_ecb = opmode <= ObCipherOpMode::ob_aes_256_ecb &&
                    opmode >= ObCipherOpMode::ob_aes_128_ecb;
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc mem failed", K(ret));
      } else if (!is_ecb && 2 == expr.arg_cnt_) {
        ret = OB_ERR_PARAM_SIZE;
        LOG_WARN("param num error", K(ret), K(expr.arg_cnt_), K(encryption));
      } else if (3 == expr.arg_cnt_ && is_ecb) {
        // just user warn, not set ret error.
        LOG_USER_WARN(OB_ERR_INVALID_INPUT_STRING, "iv");
      }
      if (OB_SUCC(ret)) {
        if (2 == expr.arg_cnt_ || is_ecb) {
          OZ(ObBlockCipher::decrypt(key_str.ptr(), key_str.length(), src_str.ptr(),
                                    src_str.length(), src_str.length(), NULL, 0, NULL, 0, NULL, 0,
                                    opmode, buf, out_len));
        } else {
          ObString iv_str = expr.locate_param_datum(ctx, 2).get_string();
          OV(iv_str.length() >= ObBlockCipher::OB_DEFAULT_IV_LENGTH, OB_ERR_AES_IV_LENGTH);
          OX(iv_str.assign(iv_str.ptr(), ObBlockCipher::OB_DEFAULT_IV_LENGTH));
          OZ(ObBlockCipher::decrypt(key_str.ptr(), key_str.length(), src_str.ptr(), src_str.length(),
                                    src_str.length(), iv_str.ptr(), iv_str.length(), NULL, 0, NULL, 0,
                                    opmode, buf, out_len));
        }
        if (OB_ERR_AES_DECRYPT == ret) {
          //按照mysql兼容的做法,如果解密失败,则将结果设置为null
          is_null = true;
          ret = OB_SUCCESS;
        }
      }
      if (OB_SUCC(ret)) {
        if (is_null) {
          res.set_null();
        } else {
          ObExprStrResAlloc res_alloc(expr, ctx);
          char *res_buf = static_cast<char*>(res_alloc.alloc(out_len));
          OV(OB_NOT_NULL(res_buf), OB_ALLOCATE_MEMORY_FAILED);
          OX(MEMCPY(res_buf, buf, out_len));
          OX(res.set_string(res_buf, out_len));
        }
      }
    }
  }
  return ret;
}

int ObExprAesDecrypt::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_aes_decrypt;
  return ret;
}

}
}
