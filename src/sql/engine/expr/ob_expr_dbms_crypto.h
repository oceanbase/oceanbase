/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef SRC_SQL_ENGINE_EXPR_OB_DBMS_CRYPTO_H_
#define SRC_SQL_ENGINE_EXPR_OB_DBMS_CRYPTO_H_
#include "sql/engine/expr/ob_expr_operator.h"
namespace oceanbase
{
namespace sql
{
class ObExprDbmsCryptoEncrypt : public ObFuncExprOperator 
{
public:
  ObExprDbmsCryptoEncrypt();
  explicit ObExprDbmsCryptoEncrypt(common::ObIAllocator& alloc);
  virtual ~ObExprDbmsCryptoEncrypt();
  virtual int calc_result_typeN(ObExprResType& type,
                                ObExprResType* types,
                                int64_t param_num, 
                                common::ObExprTypeCtx& type_ctx)
                                const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprDbmsCryptoEncrypt);
};

class ObExprDbmsCryptoDecrypt : public ObFuncExprOperator
{
public:
  ObExprDbmsCryptoDecrypt();
  explicit ObExprDbmsCryptoDecrypt(common::ObIAllocator& alloc);
  virtual ~ObExprDbmsCryptoDecrypt();
  virtual int calc_result_typeN(ObExprResType& type,
                                ObExprResType* types,
                                int64_t param_num,
                                common::ObExprTypeCtx& type_ctx)
                                const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprDbmsCryptoDecrypt);
};

}
}





#endif
