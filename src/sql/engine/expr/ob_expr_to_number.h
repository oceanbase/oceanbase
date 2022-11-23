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

#ifndef SQL_ENGINE_EXPR_OB_EXPR_TO_NUMBER_BASE_H_
#define SQL_ENGINE_EXPR_OB_EXPR_TO_NUMBER_BASE_H_

#include "share/object/ob_obj_cast.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
namespace sql
{

class ObExprToNumberBase: public ObFuncExprOperator
{
public:
  explicit  ObExprToNumberBase(common::ObIAllocator &alloc,
                               const ObExprOperatorType type,
                               const char *name);
  virtual ~ObExprToNumberBase();

  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprToNumberBase);
};

class ObExprToNumber: public ObExprToNumberBase
{
public:
  explicit  ObExprToNumber(common::ObIAllocator &alloc);
  virtual ~ObExprToNumber();

  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const override;
  // for engine 3.0
  static int calc_tonumber_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
  // for static engine batch
  static int calc_tonumber_expr_batch(
      const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const int64_t batch_size);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprToNumber);
};

class ObExprToBinaryFloat: public ObExprToNumberBase
{
public:
  explicit  ObExprToBinaryFloat(common::ObIAllocator &alloc);
  virtual ~ObExprToBinaryFloat();

  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const override;
  static int calc_to_binaryfloat_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                      ObDatum &res_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprToBinaryFloat);
};


class ObExprToBinaryDouble: public ObExprToNumberBase
{
public:
  explicit  ObExprToBinaryDouble(common::ObIAllocator &alloc);
  virtual ~ObExprToBinaryDouble();

  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const override;
  static int calc_to_binarydouble_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                       ObDatum &res_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprToBinaryDouble);
};

} /* namespace sql */
} /* namespace oceanbase */

#endif /* SQL_ENGINE_EXPR_OB_EXPR_TO_NUMBER_BASE_H_ */
