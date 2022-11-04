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
