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
  : ObFuncExprOperator(alloc, type, name, ONE_OR_TWO, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
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

int ObExprEnhancedAes::eval_param(const ObExpr &expr, 
                                  ObEvalCtx &ctx, 
                                  const ObString &func_name, 
                                  ObCipherOpMode &op_mode, 
                                  ObDatum *&src, 
                                  ObString &iv_str)
{
  int ret = OB_SUCCESS;
  bool is_ecb = false;
  ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  op_mode = static_cast<ObCipherOpMode>(expr.extra_);
  bool is_for_sensitive_rule = (ObCipherOpMode::ob_invalid_mode != op_mode);
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null session", K(ret));
  } else if (!is_for_sensitive_rule && OB_FAIL(ObEncryptionUtil::get_cipher_op_mode(op_mode, session))) {
    LOG_WARN("failed to get cipher op mode", K(ret));
  } else if (FALSE_IT(is_ecb = ObEncryptionUtil::is_ecb_mode(op_mode))) {
  } else if (OB_UNLIKELY(!is_for_sensitive_rule && !is_ecb && 2 != expr.arg_cnt_)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, func_name.length(), func_name.ptr());
  } else if (OB_UNLIKELY(1 != expr.arg_cnt_ && 2 != expr.arg_cnt_)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, func_name.length(), func_name.ptr());
  } else if (OB_FAIL(expr.eval_param_value(ctx, src))) {
    LOG_WARN("failed to eval param value", K(ret));
  } else if (OB_ISNULL(src)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null src", K(ret));
  } else if (is_ecb) {
    if (OB_UNLIKELY(2 == expr.arg_cnt_)) {
      LOG_USER_WARN(OB_ERR_INVALID_INPUT_STRING, "iv"); // warn user but not set error
    }
  } else if (is_for_sensitive_rule) {
    // for sensitive rule, the iv will be generated in eval_aes_encrypt
  } else if (FALSE_IT(iv_str = expr.locate_param_datum(ctx, 1).get_string())){
  } else if (OB_UNLIKELY(iv_str.length() < ObBlockCipher::OB_DEFAULT_IV_LENGTH)) {
    ret = OB_ERR_AES_IV_LENGTH;
    LOG_USER_ERROR(OB_ERR_AES_IV_LENGTH);
  } else {
    iv_str.assign(iv_str.ptr(), ObBlockCipher::OB_DEFAULT_IV_LENGTH);
  }
  return ret;
}

#ifdef OB_BUILD_TDE_SECURITY
int ObExprEnhancedAesEncrypt::eval_aes_encrypt(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  ObString func_name(strlen(N_ENHANCED_AES_ENCRYPT), N_ENHANCED_AES_ENCRYPT);
  bool is_for_sensitive_rule = (ObCipherOpMode::ob_invalid_mode != static_cast<ObCipherOpMode>(expr.extra_));
  // params
  ObDatum *src = NULL;
  ObString iv_str;
  ObCipherOpMode op_mode = ObCipherOpMode::ob_invalid_mode;
  // master key
  int64_t master_key_len = 0;
  uint64_t master_key_id = 0;
  char master_key[OB_MAX_MASTER_KEY_LENGTH];
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null session", K(ret));
  } else if (OB_FAIL(eval_param(expr, ctx, func_name, op_mode, src, iv_str))) {
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
    char *buf = NULL; // stores the ciphertext
    int64_t buf_len = 0;
    int64_t enc_len = 0;
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    if (!is_for_sensitive_rule) {
      // not sensitive rule scenario, encrypt data with ObBlockCipher::encrypt
      // - the master key id will be stored in front of the ciphertext for decryption
      // - the iv will not be stored in the ciphertext, the user should provide it for decryption
      // - DOES NOT support gcm algorithm for now
      ObExprStrResAlloc res_alloc(expr, ctx);
      char *res_buf = NULL;
      // MAX(1, ...) is to avoid the case that the ciphertext length is 0
      buf_len = MAX(1, ObBlockCipher::get_ciphertext_length(op_mode, src_str.length()));
      if (OB_ISNULL(buf = static_cast<char *>(alloc_guard.get_allocator().alloc(buf_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret), K(buf_len));
      } else if (OB_FAIL(ObBlockCipher::encrypt(master_key, master_key_len, 
                                                src_str.ptr(), src_str.length(), buf_len, 
                                                iv_str.ptr(), iv_str.length(), 
                                                NULL, 0, 0, 
                                                op_mode, buf, enc_len, NULL))) {
        LOG_WARN("failed to encrypt", K(ret));
      // store master key id ahead of ciphertext
      } else if (OB_ISNULL(res_buf = static_cast<char *>(res_alloc.alloc(KEY_ID_LENGTH + enc_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret), K(enc_len));
      } else {
        MEMCPY(res_buf, &master_key_id, KEY_ID_LENGTH);
        MEMCPY(res_buf + KEY_ID_LENGTH, buf, enc_len);
        res.set_string(res_buf, enc_len + KEY_ID_LENGTH);
      }   
    } else {
      // sensitive rule scenario, encrypt data with ObEncryptionUtil::encrypt_data
      // - the master key id will NOT be stored in the ciphertext
      // - the iv (and tag str if exists) will be stored in the ciphertext
      // - support gcm algorithm
      buf_len = MAX(1, ObEncryptionUtil::encrypted_length(op_mode, src_str.length()));
      if (OB_ISNULL(buf = static_cast<char *>(alloc_guard.get_allocator().alloc(buf_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret), K(buf_len));
      } else if (OB_FAIL(ObEncryptionUtil::encrypt_data(master_key, master_key_len, op_mode, 
                                                        src_str.ptr(), src_str.length(),
                                                        buf, buf_len, enc_len))) {
        LOG_WARN("failed to encrypt", K(ret));
      } else {
        res.set_string(buf, enc_len);
      }
    }
  }
  return ret;
}
#else
int ObExprEnhancedAesEncrypt::eval_aes_encrypt(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  UNUSED(expr);
  UNUSED(ctx);
  UNUSED(res);
  int ret = OB_NOT_SUPPORTED;
  LOG_WARN("feature not supported", K(ret));
  return ret;
}
#endif

int ObExprEnhancedAesEncrypt::calc_result_typeN(ObExprResType &type,
                                                ObExprResType *types,
                                                int64_t param_num,
                                                common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObCipherOpMode mode = ob_invalid_mode;
  if (NULL != type_ctx.get_raw_expr()) {
    // the encryption mode is specified by a sensitive rule
    uint64_t encryption_mode = type_ctx.get_raw_expr()->get_encryption_mode();
    if (encryption_mode > 0 && encryption_mode < ObCipherOpMode::ob_max_mode) {
      mode = static_cast<ObCipherOpMode>(encryption_mode);
    }
  }
  if (mode == ob_invalid_mode 
      && OB_FAIL(ObEncryptionUtil::get_cipher_op_mode(mode, type_ctx.get_session()))) {
    LOG_WARN("failed to get cipher op mode", K(ret));
  } else if (OB_FAIL(ObExprEnhancedAes::calc_result_typeN(type, types, param_num, type_ctx))) {
    LOG_WARN("failed to calc aes encryption result type", K(ret));
  } else {
    // need extra KEY_ID_LENGTH space to store master key id
    type.set_length(MAX(1, ObBlockCipher::get_ciphertext_length(mode, types[0].get_length()))
                    + KEY_ID_LENGTH);
  }
  return ret;
}

int ObExprEnhancedAesEncrypt::cg_expr(ObExprCGCtx &expr_cg_ctx, 
                                      const ObRawExpr &raw_expr,
                                      ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  rt_expr.eval_func_ = eval_aes_encrypt;
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
}