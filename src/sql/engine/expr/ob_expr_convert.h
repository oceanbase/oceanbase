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

#include "sql/engine/expr/ob_expr_operator.h"
#ifndef SRC_SQL_ENGINE_EXPR_OB_EXPR_CONVERT_H_
#define SRC_SQL_ENGINE_EXPR_OB_EXPR_CONVERT_H_

namespace oceanbase
{
namespace sql
{
class ObExprConvert : public ObFuncExprOperator
{
public:
  explicit  ObExprConvert(common::ObIAllocator &alloc);
  virtual ~ObExprConvert();
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const;

  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                           ObExpr &rt_expr) const override;
  virtual common::ObCastMode get_cast_mode() const override { return CM_CHARSET_CONVERT_IGNORE_ERR; }; 
  DECLARE_SET_LOCAL_SESSION_VARS;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprConvert);
};
class ObExprConvertOracle : public ObStringExprOperator
{
public:
  explicit ObExprConvertOracle(common::ObIAllocator &alloc);
  virtual ~ObExprConvertOracle();
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types_array,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const;
  static int calc_convert_oracle_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprConvertOracle);
};
}
}

#endif /* SRC_SQL_ENGINE_EXPR_OB_EXPR_CONVERT_H_ */
