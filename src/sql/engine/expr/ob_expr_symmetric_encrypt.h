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
