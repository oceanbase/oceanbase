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
#include "ob_expr_symmetric_encrypt.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::share;
using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
ObExprBaseEncrypt::ObExprBaseEncrypt(ObIAllocator& alloc, ObItemType func_type, const char* name)
  : ObFuncExprOperator(alloc,
                       func_type,
                       name,
                       TWO_OR_THREE,
                       NOT_VALID_FOR_GENERATED_COL,
                       NOT_ROW_DIMENSION) { }

ObExprBaseEncrypt::~ObExprBaseEncrypt() { }

int ObExprBaseEncrypt::calc_result_typeN(ObExprResType& type,
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

int ObExprBaseEncrypt::eval_encrypt(const ObExpr &expr,
                                    ObEvalCtx &ctx,
                                    const ObCipherOpMode op_mode,
                                    const ObString &func_name,
                                    ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *src = NULL;
  ObDatum *key = NULL;
  bool is_ecb = true;
  if (OB_UNLIKELY(2 != expr.arg_cnt_ && 3 != expr.arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret), K(expr.arg_cnt_));
  } else if (OB_FAIL(expr.eval_param_value(ctx, src, key))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (OB_ISNULL(src) || OB_ISNULL(key)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("got null ptr", K(ret));
  } else if (src->is_null() || key->is_null()) {
    res.set_null();
  } else if (FALSE_IT(is_ecb = ObEncryptionUtil::is_ecb_mode(op_mode))) {
  } else if (!is_ecb && 3 != expr.arg_cnt_) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, func_name.length(), func_name.ptr());
  } else {
    if (is_ecb && 3 == expr.arg_cnt_) {
      LOG_USER_WARN(OB_ERR_INVALID_INPUT_STRING, "iv"); // just user warn, not set ret error.
    }
    const ObString &src_str = expr.locate_param_datum(ctx, 0).get_string();
    const ObString &key_str = expr.locate_param_datum(ctx, 1).get_string();
    const int64_t buf_length = (src_str.length() / ObBlockCipher::OB_CIPHER_BLOCK_LENGTH + 1) *
                               ObBlockCipher::OB_CIPHER_BLOCK_LENGTH;
    char *buf = NULL;
    int64_t out_len = 0;
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    ObIAllocator &calc_alloc = alloc_guard.get_allocator();
    if (OB_ISNULL(buf = static_cast<char *>(calc_alloc.alloc(buf_length)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(ret), K(buf_length));
    } else if (is_ecb) {
      if (OB_FAIL(ObBlockCipher::encrypt(key_str.ptr(), key_str.length(),
                                         src_str.ptr(), src_str.length(),
                                         buf_length, NULL, 0, NULL, 0, 0, op_mode,
                                         buf, out_len, NULL))) {
        LOG_WARN("failed to encrypt", K(ret));
      }
    } else if (!is_ecb) {
      ObString iv_str = expr.locate_param_datum(ctx, 2).get_string();
      if (OB_UNLIKELY(iv_str.length() < ObBlockCipher::OB_DEFAULT_IV_LENGTH)) {
        ret = OB_ERR_AES_IV_LENGTH;
        LOG_USER_ERROR(OB_ERR_AES_IV_LENGTH);
      } else if (FALSE_IT(iv_str.assign(iv_str.ptr(), ObBlockCipher::OB_DEFAULT_IV_LENGTH))) {
      } else if (OB_FAIL(ObBlockCipher::encrypt(key_str.ptr(), key_str.length(),
                                                src_str.ptr(), src_str.length(),
                                                buf_length, iv_str.ptr(), iv_str.length(), NULL, 0,
                                                0, op_mode, buf, out_len, NULL))) {
        LOG_WARN("failed to encrypt", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ObExprStrResAlloc res_alloc(expr, ctx);
      char *res_buf = NULL;
      if (OB_ISNULL(res_buf = static_cast<char*>(res_alloc.alloc(out_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory failed", K(ret), K(out_len));
      } else {
        MEMCPY(res_buf, buf, out_len);
        res.set_string(res_buf, out_len);
      }
    }
  }
  return ret;
}

ObExprBaseDecrypt::ObExprBaseDecrypt(ObIAllocator& alloc, ObItemType func_type, const char *name)
    : ObFuncExprOperator(alloc,
                         func_type,
                         name,
                         TWO_OR_THREE,
                         NOT_VALID_FOR_GENERATED_COL,
                         NOT_ROW_DIMENSION) { }

ObExprBaseDecrypt::~ObExprBaseDecrypt() { }

int ObExprBaseDecrypt::calc_result_typeN(ObExprResType& type,
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

int ObExprBaseDecrypt::eval_decrypt(const ObExpr &expr,
                                    ObEvalCtx &ctx,
                                    const ObCipherOpMode op_mode,
                                    const ObString &func_name,
                                    ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *src = NULL;
  ObDatum *key = NULL;
  bool is_null = false;
  bool is_ecb = true;
  if (OB_FAIL(expr.eval_param_value(ctx, src, key))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (src->is_null() || key->is_null()) {
    res.set_null();
  } else if (OB_UNLIKELY(2 != expr.arg_cnt_ && 3 != expr.arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret), K(expr.arg_cnt_));
  } else if (FALSE_IT(is_ecb= ObEncryptionUtil::is_ecb_mode(op_mode))) {
  } else if (!is_ecb && 3 != expr.arg_cnt_) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, func_name.length(), func_name.ptr());
  } else {
    if (is_ecb && 3 == expr.arg_cnt_) {
      LOG_USER_WARN(OB_ERR_INVALID_INPUT_STRING, "iv");
    }
    const ObString &src_str = expr.locate_param_datum(ctx, 0).get_string();
    const ObString &key_str = expr.locate_param_datum(ctx, 1).get_string();
    const int64_t buf_len = src_str.length() + 1;
    char *buf = NULL;
    int64_t out_len = 0;
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    ObIAllocator &calc_alloc = alloc_guard.get_allocator();
    if (OB_ISNULL(buf = static_cast<char *>(calc_alloc.alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc mem failed", K(ret), K(buf_len));
    } else if (is_ecb) {
      if (OB_FAIL(ObBlockCipher::decrypt(key_str.ptr(), key_str.length(),
                                         src_str.ptr(), src_str.length(), buf_len, NULL, 0, NULL, 0,
                                         NULL, 0, op_mode, buf, out_len))) {
        LOG_WARN("failed to decrypt", K(ret));
      }
    } else {
      ObString iv_str = expr.locate_param_datum(ctx, 2).get_string();
      if (OB_UNLIKELY(iv_str.length() < ObBlockCipher::OB_DEFAULT_IV_LENGTH)) {
        ret = OB_ERR_AES_IV_LENGTH;
        LOG_USER_ERROR(OB_ERR_AES_IV_LENGTH);
      } else if (FALSE_IT(iv_str.assign(iv_str.ptr(), ObBlockCipher::OB_DEFAULT_IV_LENGTH))) {
      } else if (OB_FAIL(ObBlockCipher::decrypt(key_str.ptr(), key_str.length(),
                                                src_str.ptr(), src_str.length(), buf_len,
                                                iv_str.ptr(), iv_str.length(), NULL, 0, NULL, 0,
                                                op_mode, buf, out_len))) {
        LOG_WARN("failed to decrypt", K(ret));
      }
    }
    if (OB_ERR_AES_DECRYPT == ret) {
      is_null = true;
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret)) {
      ObExprStrResAlloc res_alloc(expr, ctx);
      char *res_buf = NULL;
      if (is_null) {
        res.set_null();
      } else if (OB_ISNULL(res_buf = static_cast<char*>(res_alloc.alloc(out_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory failed", K(ret), K(out_len));
      } else {
        MEMCPY(res_buf, buf, out_len);
        res.set_string(res_buf, out_len);
      }
    }
  }
  return ret;
}

ObExprAesEncrypt::ObExprAesEncrypt(ObIAllocator& alloc)
  : ObExprBaseEncrypt(alloc, T_FUN_AES_ENCRYPT, N_AES_ENCRYPT) { }

ObExprAesEncrypt::~ObExprAesEncrypt() { }

int ObExprAesEncrypt::cg_expr(ObExprCGCtx &expr_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_aes_encrypt;
  return ret;
}

int ObExprAesEncrypt::eval_aes_encrypt(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObCipherOpMode op_mode = ObCipherOpMode::ob_invalid_mode;
  ObString func_name(strlen(N_AES_ENCRYPT), N_AES_ENCRYPT);
  if (OB_FAIL(ObEncryptionUtil::get_cipher_op_mode(op_mode, ctx.exec_ctx_.get_my_session()))) {
    LOG_WARN("fail to get cipher mode", K(ret));
  } else if (!ObEncryptionUtil::is_aes_encryption(op_mode)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "using aes_encrypt with not aes block_encryption_mode");
  } else if (OB_FAIL(eval_encrypt(expr, ctx, op_mode, func_name, res))) {
    LOG_WARN("failed to eval aes encrypt", K(ret));
  } else { /* do nothing */ }
  return ret;
}

ObExprAesDecrypt::ObExprAesDecrypt(ObIAllocator& alloc)
  : ObExprBaseDecrypt(alloc, T_FUN_AES_DECRYPT, N_AES_DECRYPT) { }

ObExprAesDecrypt::~ObExprAesDecrypt() { }

int ObExprAesDecrypt::cg_expr(ObExprCGCtx &expr_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_aes_decrypt;
  return ret;
}

int ObExprAesDecrypt::eval_aes_decrypt(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObCipherOpMode op_mode = ObCipherOpMode::ob_invalid_mode;
  ObString func_name(strlen(N_AES_DECRYPT), N_AES_DECRYPT);
  if (OB_FAIL(ObEncryptionUtil::get_cipher_op_mode(op_mode, ctx.exec_ctx_.get_my_session()))) {
    LOG_WARN("fail to get cipher mode", K(ret));
  } else if (!ObEncryptionUtil::is_aes_encryption(op_mode)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "using aes_decrypt with not aes block_encryption_mode");
  } else if (OB_FAIL(eval_decrypt(expr, ctx, op_mode, func_name, res))) {
    LOG_WARN("failed to eval aes decrypt", K(ret));
  } else { /* do nothing */ }
  return ret;
}

ObExprSm4Encrypt::ObExprSm4Encrypt(ObIAllocator& alloc)
  : ObExprBaseEncrypt(alloc, T_FUN_SYS_SM4_ENCRYPT, N_SM4_ENCRYPT) { }

ObExprSm4Encrypt::~ObExprSm4Encrypt() { }

int ObExprSm4Encrypt::cg_expr(ObExprCGCtx &expr_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_sm4_encrypt;
  return ret;
}

int ObExprSm4Encrypt::eval_sm4_encrypt(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObCipherOpMode op_mode = ObCipherOpMode::ob_invalid_mode;
  ObString func_name(strlen(N_SM4_ENCRYPT), N_SM4_ENCRYPT);
#ifdef OB_USE_BABASSL
  if (OB_FAIL(ObEncryptionUtil::get_cipher_op_mode(op_mode, ctx.exec_ctx_.get_my_session()))) {
    LOG_WARN("fail to get cipher mode", K(ret));
  } else if (!ObEncryptionUtil::is_sm4_encryption(op_mode)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "using sm4_encrypt with not sm4 block_encryption_mode");
  } else if (OB_FAIL(eval_encrypt(expr, ctx, op_mode, func_name, res))) {
    LOG_WARN("failed to eval sm4 encrypt", K(ret));
  } else { /* do nothing */ }
#else
  ret = OB_NOT_SUPPORTED;
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "Function sm4_encrypt");
  // we use openssl 1.1.1 in opensource mode, which has no sm4 encryption until in 3.0 version
#endif
  return ret;
}

ObExprSm4Decrypt::ObExprSm4Decrypt(ObIAllocator& alloc)
  : ObExprBaseDecrypt(alloc, T_FUN_SYS_SM4_DECRYPT, N_SM4_DECRYPT) { }

ObExprSm4Decrypt::~ObExprSm4Decrypt() { }

int ObExprSm4Decrypt::cg_expr(ObExprCGCtx &expr_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_sm4_decrypt;
  return ret;
}

int ObExprSm4Decrypt::eval_sm4_decrypt(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObCipherOpMode op_mode = ObCipherOpMode::ob_invalid_mode;
  ObString func_name(strlen(N_SM4_DECRYPT), N_SM4_DECRYPT);
#ifdef OB_USE_BABASSL
  if (OB_FAIL(ObEncryptionUtil::get_cipher_op_mode(op_mode, ctx.exec_ctx_.get_my_session()))) {
    LOG_WARN("fail to get cipher mode", K(ret));
  } else if (!ObEncryptionUtil::is_sm4_encryption(op_mode)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "using sm4_decrypt with not sm4 block_encryption_mode");
  } else if (OB_FAIL(eval_decrypt(expr, ctx, op_mode, func_name, res))) {
    LOG_WARN("failed to eval aes decrypt", K(ret));
  } else { /* do nothing */ }
#else
  ret = OB_NOT_SUPPORTED;
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "Function sm4_decrypt");
  // we use openssl 1.1.1 in opensource mode, which has no sm4 encryption until in 3.0 version
#endif
  return ret;
}

}
}
