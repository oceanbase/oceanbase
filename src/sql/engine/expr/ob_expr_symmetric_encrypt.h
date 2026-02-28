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

#ifndef SRC_SQL_ENGINE_EXPR_OB_EXPR_SYMMETRIC_ENCRYPT_H_
#define SRC_SQL_ENGINE_EXPR_OB_EXPR_SYMMETRIC_ENCRYPT_H_
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprBaseEncrypt : public ObFuncExprOperator
{
public:
  ObExprBaseEncrypt();
  explicit ObExprBaseEncrypt(common::ObIAllocator& alloc, ObItemType func_type, const char* name);
  virtual ~ObExprBaseEncrypt();
  virtual int calc_result_typeN(ObExprResType& type,
                                ObExprResType* types,
                                int64_t param_num,
                                common::ObExprTypeCtx& type_ctx) const override;
  static int eval_encrypt(const ObExpr &expr,
                          ObEvalCtx &ctx,
                          const share::ObCipherOpMode op_mode,
                          const ObString &func_name,
                          ObDatum &res);
};

class ObExprBaseDecrypt : public ObFuncExprOperator
{
public:
  ObExprBaseDecrypt();
  explicit ObExprBaseDecrypt(common::ObIAllocator& alloc, ObItemType func_type, const char* name);
  virtual ~ObExprBaseDecrypt();
  virtual int calc_result_typeN(ObExprResType& type,
                                ObExprResType* types,
                                int64_t param_num,
                                common::ObExprTypeCtx& type_ctx) const override;
  static int eval_decrypt(const ObExpr &expr,
                          ObEvalCtx &ctx,
                          const share::ObCipherOpMode op_mode,
                          const ObString &func_name,
                          ObDatum &res);
  template <typename SrcVec, typename KeyVec, typename IvVec, typename ResVec>
  static int eval_decrypt_vector(VECTOR_EVAL_FUNC_ARG_DECL,
                                const share::ObCipherOpMode op_mode,
                                const ObString &func_name);
private:
  OB_INLINE static int eval_decrypt_inner(const ObString &src_str,
                                          const ObString &key_str,
                                          const ObString &iv_str,
                                          const share::ObCipherOpMode op_mode,
                                          ObIAllocator &alloc,
                                          char *&buf,
                                          int64_t &out_len,
                                          bool &is_null_result)
  {
    int ret = OB_SUCCESS;
    const int64_t buf_len = src_str.length() + 1;
    is_null_result = false;
    buf = NULL;
    if (OB_ISNULL(buf = static_cast<char *>(alloc.alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_ENG_LOG(WARN, "alloc mem failed", K(ret), K(buf_len));
    } else if (OB_FAIL(share::ObBlockCipher::decrypt(key_str.ptr(), key_str.length(),
                                                     src_str.ptr(), src_str.length(), buf_len,
                                                     iv_str.ptr(), iv_str.length(), NULL, 0, NULL, 0,
                                                     op_mode, buf, out_len))) {
      SQL_ENG_LOG(WARN, "failed to decrypt", K(ret));
    }
    if (OB_ERR_AES_DECRYPT == ret) {
      is_null_result = true;
      ret = OB_SUCCESS;
    }
    return ret;
  }
};

class ObExprAesEncrypt : public ObExprBaseEncrypt
{
public:
  ObExprAesEncrypt();
  explicit ObExprAesEncrypt(common::ObIAllocator& alloc);
  virtual ~ObExprAesEncrypt();
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_aes_encrypt(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprAesEncrypt);
};

class ObExprAesDecrypt : public ObExprBaseDecrypt
{
public:
  ObExprAesDecrypt();
  explicit ObExprAesDecrypt(common::ObIAllocator& alloc);
  virtual ~ObExprAesDecrypt();
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_aes_decrypt(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_aes_decrypt_vector(VECTOR_EVAL_FUNC_ARG_DECL);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprAesDecrypt);
};

class ObExprSm4Encrypt : public ObExprBaseEncrypt
{
public:
  ObExprSm4Encrypt();
  explicit ObExprSm4Encrypt(common::ObIAllocator& alloc);
  virtual ~ObExprSm4Encrypt();
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_sm4_encrypt(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprSm4Encrypt);
};

class ObExprSm4Decrypt : public ObExprBaseDecrypt
{
public:
  ObExprSm4Decrypt();
  explicit ObExprSm4Decrypt(common::ObIAllocator& alloc);
  virtual ~ObExprSm4Decrypt();
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_sm4_decrypt(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprSm4Decrypt);
};

}
}





#endif
