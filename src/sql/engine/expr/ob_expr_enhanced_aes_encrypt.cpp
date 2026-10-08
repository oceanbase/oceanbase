/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_EXE
#include "ob_expr_enhanced_aes_encrypt.h"
#include "src/sql/engine/expr/ob_expr_symmetric_encrypt.h"
#include "src/sql/engine/ob_exec_context.h"
#include "src/sql/resolver/expr/ob_raw_expr.h"
#include "src/sql/session/ob_sql_session_info.h"
#include "src/share/ob_encryption_util.h"
#ifdef OB_BUILD_TDE_SECURITY
#include "close_modules/tde_security/share/ob_master_key_getter.h"
#endif

using namespace oceanbase::share;
using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

ObExprEnhancedAes::ObExprEnhancedAes(ObIAllocator &alloc, ObExprOperatorType type, const char *name)
  : ObFuncExprOperator(alloc, type, name, ONE_OR_TWO, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                       false, INTERNAL_IN_ORACLE_MODE)
{}

int ObExprEnhancedAes::calc_result_typeN(ObExprResType &type,
                                         ObExprResType *types,
                                         int64_t param_num,
                                         common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(types)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null arg", K(ret));
  } else if (OB_UNLIKELY(1 != param_num && 2 != param_num)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param num is not correct", K(param_num));
  } else {
    for (int i = 0; i < param_num; ++i) {
      types[i].set_calc_type(common::ObVarcharType);
      types[i].set_calc_collation_type(types[i].get_collation_type());
      types[i].set_calc_collation_level(types[i].get_collation_level());
    }
    type.set_varbinary();
    type.set_collation_level(CS_LEVEL_COERCIBLE);
    type.set_length(0); // must be set properly by subclass
  }
  return ret;
}

ObExprEnhancedAesEncrypt::ObExprEnhancedAesEncrypt(ObIAllocator &alloc)
  : ObExprEnhancedAes(alloc, T_FUN_SYS_ENHANCED_AES_ENCRYPT, N_ENHANCED_AES_ENCRYPT)
{}

#ifdef OB_BUILD_TDE_SECURITY
int ObExprEnhancedAes::prepare_eval_param(const ObExpr &expr,
                                          ObEvalCtx &ctx,
                                          const ObString &func_name,
                                          ObCipherOpMode &op_mode,
                                          bool &is_for_sensitive_rule,
                                          bool &is_ecb)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  op_mode = static_cast<ObCipherOpMode>(expr.extra_);
  is_for_sensitive_rule = ObCipherOpMode::ob_invalid_mode != op_mode;
  is_ecb = false;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null session", K(ret));
  } else if (T_FUN_SYS_ENHANCED_AES_ENCRYPT == expr.type_ && lib::is_oracle_mode() && !is_for_sensitive_rule) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("oracle mode does not support enhanced aes encrypt", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "enhanced aes encrypt");
  } else if (!is_for_sensitive_rule && OB_FAIL(ObEncryptionUtil::get_cipher_op_mode(op_mode, session))) {
    LOG_WARN("failed to get cipher op mode", K(ret));
  } else if (FALSE_IT(is_ecb = ObEncryptionUtil::is_ecb_mode(op_mode))) {
  } else if (OB_UNLIKELY(!is_for_sensitive_rule && !is_ecb && 2 != expr.arg_cnt_)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, func_name.length(), func_name.ptr());
  } else if (OB_UNLIKELY(1 != expr.arg_cnt_ && 2 != expr.arg_cnt_)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, func_name.length(), func_name.ptr());
  }
  return ret;
}

int ObExprEnhancedAes::normalize_iv(const ObExpr &expr,
                                    const bool is_for_sensitive_rule,
                                    const bool is_ecb,
                                    const ObString &input_iv,
                                    ObString &iv_str)
{
  int ret = OB_SUCCESS;
  iv_str.reset();
  if (is_ecb) {
    if (OB_UNLIKELY(2 == expr.arg_cnt_)) {
      LOG_USER_WARN(OB_ERR_INVALID_INPUT_STRING, "iv");
    }
  } else if (is_for_sensitive_rule) {
    // the iv is generated while encrypting sensitive data
  } else if (OB_UNLIKELY(input_iv.length() < ObBlockCipher::OB_DEFAULT_IV_LENGTH)) {
    ret = OB_ERR_AES_IV_LENGTH;
    LOG_USER_ERROR(OB_ERR_AES_IV_LENGTH);
  } else {
    iv_str.assign_ptr(input_iv.ptr(), ObBlockCipher::OB_DEFAULT_IV_LENGTH);
  }
  return ret;
}

int ObExprEnhancedAes::eval_param(const ObExpr &expr,
                                  ObEvalCtx &ctx,
                                  const ObString &func_name,
                                  ObCipherOpMode &op_mode,
                                  ObDatum *&src,
                                  ObString &iv_str)
{
  int ret = OB_SUCCESS;
  bool is_ecb = false;
  bool is_for_sensitive_rule = false;
  if (OB_FAIL(prepare_eval_param(expr, ctx, func_name, op_mode, is_for_sensitive_rule, is_ecb))) {
    LOG_WARN("failed to prepare eval param", K(ret));
  } else if (OB_FAIL(expr.eval_param_value(ctx, src))) {
    LOG_WARN("failed to eval param value", K(ret));
  } else if (OB_ISNULL(src)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null src", K(ret));
  } else {
    ObString input_iv;
    if (!is_for_sensitive_rule && !is_ecb) {
      input_iv = expr.locate_param_datum(ctx, 1).get_string();
    }
    if (OB_FAIL(normalize_iv(expr, is_for_sensitive_rule, is_ecb, input_iv, iv_str))) {
      LOG_WARN("failed to normalize iv", K(ret));
    }
  }
  return ret;
}

int ObExprEnhancedAesEncrypt::encrypt_one_cell(const ObExpr &expr,
                                               ObEvalCtx &ctx,
                                               const ObCipherOpMode op_mode,
                                               const ObString &src_str,
                                               const ObString &iv_str,
                                               const ObString &master_key,
                                               const uint64_t master_key_id,
                                               ObString &result)
{
  int ret = OB_SUCCESS;
  const bool is_for_sensitive_rule = ObCipherOpMode::ob_invalid_mode != static_cast<ObCipherOpMode>(expr.extra_);
  const int64_t encrypt_buf_len = is_for_sensitive_rule
                                    ? MAX(1, ObEncryptionUtil::encrypted_length(op_mode, src_str.length()))
                                    : MAX(1, ObBlockCipher::get_ciphertext_length(op_mode, src_str.length()));
  // Sensitive-rule output already contains IV/tag; explicit output reserves a key-id prefix.
  const int64_t result_buf_len = encrypt_buf_len + (is_for_sensitive_rule ? 0 : KEY_ID_LENGTH);
  char *buf = expr.get_str_res_mem(ctx, result_buf_len);
  int64_t enc_len = 0;
  result.reset();
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate result memory", K(ret), K(result_buf_len));
  } else if (is_for_sensitive_rule) {
    if (OB_FAIL(ObEncryptionUtil::encrypt_data(master_key.ptr(),
                                               master_key.length(),
                                               op_mode,
                                               src_str.ptr(),
                                               src_str.length(),
                                               buf,
                                               result_buf_len,
                                               enc_len))) {
      LOG_WARN("failed to encrypt", K(ret));
    } else {
      result.assign_ptr(buf, enc_len);
    }
  } else if (OB_FAIL(ObBlockCipher::encrypt(master_key.ptr(),
                                            master_key.length(),
                                            src_str.ptr(),
                                            src_str.length(),
                                            encrypt_buf_len,
                                            iv_str.ptr(),
                                            iv_str.length(),
                                            NULL,
                                            0,
                                            0,
                                            op_mode,
                                            buf + KEY_ID_LENGTH,
                                            enc_len,
                                            NULL))) {
    LOG_WARN("failed to encrypt", K(ret));
  } else {
    MEMCPY(buf, &master_key_id, KEY_ID_LENGTH);
    result.assign_ptr(buf, KEY_ID_LENGTH + enc_len);
  }
  return ret;
}

int ObExprEnhancedAesEncrypt::eval_aes_encrypt(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  ObString func_name(strlen(N_ENHANCED_AES_ENCRYPT), N_ENHANCED_AES_ENCRYPT);
  // params
  ObDatum *src = NULL;
  ObString iv_str;
  ObCipherOpMode op_mode = ObCipherOpMode::ob_invalid_mode;
  // master key
  int64_t master_key_len = 0;
  uint64_t master_key_id = 0;
  char master_key[OB_MAX_MASTER_KEY_LENGTH];
  if (OB_FAIL(eval_param(expr, ctx, func_name, op_mode, src, iv_str))) {
    LOG_WARN("failed to eval params", K(ret));
  } else if (src->is_null()) {
    res.set_null();
  } else if (OB_FAIL(ObMasterKeyGetter::get_active_master_key(session->get_effective_tenant_id(),
                                                              master_key,
                                                              OB_MAX_MASTER_KEY_LENGTH,
                                                              master_key_len,
                                                              master_key_id))) {
    LOG_WARN("failed to get active master key", K(ret));
  } else {
    const ObString &src_str = expr.locate_param_datum(ctx, 0).get_string();
    const ObString master_key_str(static_cast<int32_t>(master_key_len), master_key);
    ObString result;
    if (OB_FAIL(encrypt_one_cell(expr, ctx, op_mode, src_str, iv_str, master_key_str, master_key_id, result))) {
      LOG_WARN("failed to encrypt one cell", K(ret));
    } else {
      res.set_string(result);
    }
  }
  return ret;
}

int ObExprEnhancedAesEncrypt::eval_aes_encrypt_vector(const ObExpr &expr,
                                                      ObEvalCtx &ctx,
                                                      const ObBitVector &skip,
                                                      const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  ObString func_name(strlen(N_ENHANCED_AES_ENCRYPT), N_ENHANCED_AES_ENCRYPT);
  bool is_for_sensitive_rule = false;
  ObCipherOpMode op_mode = ObCipherOpMode::ob_invalid_mode;
  bool is_ecb = false;
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  char master_key[OB_MAX_MASTER_KEY_LENGTH];
  int64_t master_key_len = 0;
  uint64_t master_key_id = 0;
  if (OB_UNLIKELY(skip.accumulate_bit_cnt(bound) == bound.range_size())) {
  } else if (OB_FAIL(prepare_eval_param(expr, ctx, func_name, op_mode, is_for_sensitive_rule, is_ecb))) {
    LOG_WARN("failed to prepare eval param", K(ret));
  } else if (OB_FAIL(expr.eval_vector_param_value(ctx, skip, bound))) {
    LOG_WARN("failed to eval params in vector", K(ret));
  } else if (OB_FAIL(ObMasterKeyGetter::get_active_master_key(session->get_effective_tenant_id(),
                                                              master_key,
                                                              OB_MAX_MASTER_KEY_LENGTH,
                                                              master_key_len,
                                                              master_key_id))) {
    LOG_WARN("failed to get active master key", K(ret));
  } else {
    ObIVector *src_vec = expr.args_[0]->get_vector(ctx);
    ObIVector *iv_vec = 2 == expr.arg_cnt_ ? expr.args_[1]->get_vector(ctx) : NULL;
    ObIVector *res_vec = expr.get_vector(ctx);
    const ObString master_key_str(static_cast<int32_t>(master_key_len), master_key);
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
    batch_info_guard.set_batch_size(bound.batch_size());
    for (int64_t i = bound.start(); OB_SUCC(ret) && i < bound.end(); ++i) {
      if (skip.at(i) || eval_flags.at(i)) {
        continue;
      }
      batch_info_guard.set_batch_idx(i);
      ObString input_iv;
      ObString iv_str;
      if (!is_for_sensitive_rule && !is_ecb) {
        input_iv = iv_vec->get_string(i);
      }
      if (OB_FAIL(normalize_iv(expr, is_for_sensitive_rule, is_ecb, input_iv, iv_str))) {
        LOG_WARN("failed to normalize iv", K(ret), K(i));
      } else if (src_vec->is_null(i)) {
        res_vec->set_null(i);
      } else {
        const ObString src_str = src_vec->get_string(i);
        ObString result;
        if (OB_FAIL(encrypt_one_cell(expr, ctx, op_mode, src_str, iv_str, master_key_str, master_key_id, result))) {
          LOG_WARN("failed to encrypt one cell", K(ret), K(i));
        } else {
          res_vec->set_string(i, result);
        }
      }
    }
  }
  return ret;
}

int ObExprEnhancedAesEncrypt::calc_result_typeN(ObExprResType &type,
                                                ObExprResType *types,
                                                int64_t param_num,
                                                common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObCipherOpMode mode = ob_invalid_mode;
  bool is_for_sensitive_rule = false;
  if (NULL != type_ctx.get_raw_expr()) {
    // the encryption mode is specified by a sensitive rule
    uint64_t encryption_mode = type_ctx.get_raw_expr()->get_encryption_mode();
    if (encryption_mode > 0 && encryption_mode < ObCipherOpMode::ob_max_mode) {
      mode = static_cast<ObCipherOpMode>(encryption_mode);
      is_for_sensitive_rule = true;
    }
  }
  if (mode == ob_invalid_mode
      && OB_FAIL(ObEncryptionUtil::get_cipher_op_mode(mode, type_ctx.get_session()))) {
    LOG_WARN("failed to get cipher op mode", K(ret));
  } else if (OB_FAIL(ObExprEnhancedAes::calc_result_typeN(type, types, param_num, type_ctx))) {
    LOG_WARN("failed to calc aes encryption result type", K(ret));
  } else if (is_for_sensitive_rule) {
    type.set_length(MAX(1, ObEncryptionUtil::encrypted_length(mode, types[0].get_length())));
  } else {
    // need extra KEY_ID_LENGTH space to store master key id
    type.set_length(MAX(1, ObBlockCipher::get_ciphertext_length(mode, types[0].get_length()))
                    + KEY_ID_LENGTH);
  }
  return ret;
}
#else
int ObExprEnhancedAes::prepare_eval_param(const ObExpr &expr,
                                          ObEvalCtx &ctx,
                                          const ObString &func_name,
                                          ObCipherOpMode &op_mode,
                                          bool &is_for_sensitive_rule,
                                          bool &is_ecb)
{
  int ret = OB_NOT_SUPPORTED;
  LOG_WARN("feature not supported", K(ret));
  return ret;
}

int ObExprEnhancedAes::normalize_iv(const ObExpr &expr,
                                    const bool is_for_sensitive_rule,
                                    const bool is_ecb,
                                    const ObString &input_iv,
                                    ObString &iv_str)
{
  int ret = OB_NOT_SUPPORTED;
  LOG_WARN("feature not supported", K(ret));
  return ret;
}

int ObExprEnhancedAes::eval_param(const ObExpr &expr,
                                  ObEvalCtx &ctx,
                                  const ObString &func_name,
                                  ObCipherOpMode &op_mode,
                                  ObDatum *&src,
                                  ObString &iv_str)
{
  int ret = OB_NOT_SUPPORTED;
  LOG_WARN("feature not supported", K(ret));
  return ret;
}

int ObExprEnhancedAesEncrypt::eval_aes_encrypt(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  UNUSED(expr);
  UNUSED(ctx);
  UNUSED(res);
  int ret = OB_NOT_SUPPORTED;
  LOG_WARN("feature not supported", K(ret));
  return ret;
}

int ObExprEnhancedAesEncrypt::eval_aes_encrypt_vector(const ObExpr &expr,
                                                      ObEvalCtx &ctx,
                                                      const ObBitVector &skip,
                                                      const EvalBound &bound)
{
  UNUSED(expr);
  UNUSED(ctx);
  UNUSED(skip);
  UNUSED(bound);
  int ret = OB_NOT_SUPPORTED;
  LOG_WARN("feature not supported", K(ret));
  return ret;
}

int ObExprEnhancedAesEncrypt::calc_result_typeN(ObExprResType &type,
                                                ObExprResType *types,
                                                int64_t param_num,
                                                common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type);
  UNUSED(types);
  UNUSED(param_num);
  UNUSED(type_ctx);
  int ret = OB_NOT_SUPPORTED;
  LOG_WARN("feature not supported", K(ret));
  return ret;
}
#endif

int ObExprEnhancedAesEncrypt::cg_expr(ObExprCGCtx &expr_cg_ctx,
                                      const ObRawExpr &raw_expr,
                                      ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  int ret = OB_SUCCESS;
  rt_expr.eval_func_ = eval_aes_encrypt;
  rt_expr.eval_vector_func_ = eval_aes_encrypt_vector;
  // if the encryption mode is set, it means this ENHANCED_AES_ENCRYPT expr
  // was implicitly added by a sensitive rule
  rt_expr.extra_ = raw_expr.get_encryption_mode();
  return ret;
}

ObExprEnhancedAesDecrypt::ObExprEnhancedAesDecrypt(ObIAllocator &alloc)
  : ObExprEnhancedAes(alloc, T_FUN_SYS_ENHANCED_AES_DECRYPT, N_ENHANCED_AES_DECRYPT)
{}

#ifdef OB_BUILD_TDE_SECURITY
int ObExprEnhancedAesDecrypt::eval_aes_decrypt(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObString func_name(strlen(N_ENHANCED_AES_DECRYPT), N_ENHANCED_AES_DECRYPT);
  ObCipherOpMode op_mode = ObCipherOpMode::ob_invalid_mode;
  ObDatum *src = NULL;
  ObString iv_str;
  ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  char master_key[OB_MAX_MASTER_KEY_LENGTH];
  int64_t master_key_len = 0;
  uint64_t master_key_id = 0;
  ObString src_str;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null session", K(ret));
  } else if (OB_FAIL(eval_param(expr, ctx, func_name, op_mode, src, iv_str))) {
    LOG_WARN("failed to eval params", K(ret));
  } else if (src->is_null()) {
    res.set_null();
  } else if (FALSE_IT(src_str = expr.locate_param_datum(ctx, 0).get_string())) {
  } else if (OB_UNLIKELY(src_str.length() < KEY_ID_LENGTH)) {
    res.set_null();
    ret = OB_ERR_INVALID_INPUT_STRING;
    LOG_WARN("input string is too short", K(ret), K(src_str));
  } else if (FALSE_IT(MEMCPY(&master_key_id, src_str.ptr(), KEY_ID_LENGTH))){
  } else if (OB_FAIL(ObMasterKeyGetter::get_master_key(session->get_effective_tenant_id(),
                                                       master_key_id,
                                                       master_key,
                                                       OB_MAX_MASTER_KEY_LENGTH,
                                                       master_key_len))) {
    LOG_WARN("failed to get master key", K(ret), K(master_key_id));
  } else {
    src_str.assign(src_str.ptr() + KEY_ID_LENGTH, src_str.length() - KEY_ID_LENGTH);
    char *buf = NULL;
    const int64_t buf_len = src_str.length() + 1;
    int64_t dec_len = 0;
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    if (OB_ISNULL(buf = static_cast<char *>(alloc_guard.get_allocator().alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret), K(buf_len));
    } else if (OB_FAIL(ObBlockCipher::decrypt(master_key, master_key_len,
                                              src_str.ptr(), src_str.length(), src_str.length(),
                                              iv_str.ptr(), iv_str.length(),
                                              NULL, 0, NULL, 0,
                                              op_mode, buf, dec_len))) {
      LOG_WARN("failed to decrypt", K(ret));
      if (OB_ERR_AES_DECRYPT == ret) {
        ret = OB_SUCCESS; // according to mysql, return null if decryption failed
        res.set_null();
      }
    } else {
      ObExprStrResAlloc res_alloc(expr, ctx);
      char *res_buf = static_cast<char*>(res_alloc.alloc(dec_len));
      if (OB_ISNULL(res_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("failed to allocate memory", K(ret));
      } else {
        MEMCPY(res_buf, buf, dec_len);
        res.set_string(res_buf, dec_len);
      }
    }
  }
  return ret;
}
#else
int ObExprEnhancedAesDecrypt::eval_aes_decrypt(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  UNUSED(expr);
  UNUSED(ctx);
  UNUSED(res);
  int ret = OB_NOT_SUPPORTED;
  LOG_WARN("feature not supported", K(ret));
  return ret;
}
#endif

int ObExprEnhancedAesDecrypt::calc_result_typeN(ObExprResType &type,
                                                ObExprResType *types,
                                                int64_t param_num,
                                                common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObCipherOpMode mode = ob_invalid_mode;
  if (OB_FAIL(ObEncryptionUtil::get_cipher_op_mode(mode, type_ctx.get_session()))) {
    LOG_WARN("failed to get cipher op mode", K(ret));
  } else if (OB_FAIL(ObExprEnhancedAes::calc_result_typeN(type, types, param_num, type_ctx))) {
    LOG_WARN("failed to calc aes decryption result type", K(ret));
  } else {
    type.set_length(ObBlockCipher::get_max_plaintext_length(mode, types[0].get_length()));
  }
  return ret;
}

int ObExprEnhancedAesDecrypt::cg_expr(ObExprCGCtx &expr_cg_ctx,
                                      const ObRawExpr &raw_expr,
                                      ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  rt_expr.eval_func_ = eval_aes_decrypt;
  return ret;
}

}
}  // namespace oceanbase
