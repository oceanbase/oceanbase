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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_FUNC_CEIL_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_FUNC_CEIL_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{

class ObExprCeilFloor : public ObFuncExprOperator
{
  static const int16_t MAX_LIMIT_WITH_SCALE = 17;
  static const int16_t MAX_LIMIT_WITHOUT_SCALE = 18;
public:
  ObExprCeilFloor(common::ObIAllocator &alloc,
                  ObExprOperatorType type,
                  const char *name,
                  int32_t param_num,
                  int32_t dimension = NOT_ROW_DIMENSION);

  virtual ~ObExprCeilFloor();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                        ObExpr &rt_expr) const override;

  static int ceil_floor_decint(
      const bool is_floor, const ObDatum *arg_datum,
      const ObDatumMeta &in_meta, const ObDatumMeta &out_meta,
      ObDatum &res_datum);

  template <typename LeftVec, typename ResVec>
  static int inner_calc_ceil_floor_vector(const ObExpr &expr,
                                         int intput_type,
                                         ObEvalCtx &ctx,
                                         const ObBitVector &skip,
                                         const EvalBound &bound);

  static int calc_ceil_floor_vector(const ObExpr &expr,
                           ObEvalCtx &ctx,
                           const ObBitVector &skip,
                           const EvalBound &bound);

  template <typename LeftVec, typename ResVec, bool IS_FLOOR>
  static int ceil_floor_decint_vector(const ObDatumMeta &in_meta,
                                              const ObDatumMeta &out_meta,
                                              LeftVec *left_vec,
                                              ResVec *res_vec,
                                              const int64_t &idx);

  static int inner_calc_ceil_floor_fixed_vector(const ObExpr &expr,
                                                 ObEvalCtx &ctx,
                                                 const ObBitVector &skip,
                                                 const EvalBound &bound);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprCeilFloor);
};

class ObExprFuncCeil : public ObExprCeilFloor
{
public:
  explicit  ObExprFuncCeil(common::ObIAllocator &alloc);
  virtual ~ObExprFuncCeil();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprFuncCeil);
};

class ObExprFuncCeiling : public ObExprCeilFloor
{
public:
  explicit  ObExprFuncCeiling(common::ObIAllocator &alloc);
  virtual ~ObExprFuncCeiling();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprFuncCeiling);
};

class ObExprFuncFloor : public ObExprCeilFloor
{
public:
  explicit  ObExprFuncFloor(common::ObIAllocator &alloc);
  virtual ~ObExprFuncFloor();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprFuncFloor);
};
} // namespace sql
} // namespace oceanbase
#endif // OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_FUNC_CEIL_
