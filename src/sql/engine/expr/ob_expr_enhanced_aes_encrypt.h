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

#ifndef SRC_SQL_ENGINE_EXPR_OB_EXPR_ENHANCED_AES_ENCRYPT_H_
#define SRC_SQL_ENGINE_EXPR_OB_EXPR_ENHANCED_AES_ENCRYPT_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "share/ob_encryption_util.h"
namespace oceanbase
{
namespace sql
{

class ObExprEnhancedAes : public ObFuncExprOperator
{
public:
  ObExprEnhancedAes(common::ObIAllocator &alloc,
                    ObExprOperatorType type,
                    const char *name);
  virtual ~ObExprEnhancedAes() {}
  static int eval_param(const ObExpr &expr, 
                        ObEvalCtx &ctx, 
                        const ObString &func_name, 
                        share::ObCipherOpMode &op_mode, 
                        ObDatum *&src, 
                        ObString &iv_str);
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const override;
protected:
  static const int64_t KEY_ID_LENGTH = sizeof(uint64_t);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprEnhancedAes);
};

class ObExprEnhancedAesEncrypt : public ObExprEnhancedAes
{
public:
  explicit ObExprEnhancedAesEncrypt(common::ObIAllocator &alloc);
  virtual ~ObExprEnhancedAesEncrypt() {}
  static int eval_aes_encrypt(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const override;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, 
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprEnhancedAesEncrypt);
};

class ObExprEnhancedAesDecrypt : public ObExprEnhancedAes
{
public:
  explicit ObExprEnhancedAesDecrypt(common::ObIAllocator &alloc);
  virtual ~ObExprEnhancedAesDecrypt() {}
  static int eval_aes_decrypt(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const override;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, 
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprEnhancedAesDecrypt);
};

}
}

#endif