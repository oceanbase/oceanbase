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
 * This file contains implementation for map_keys.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_MAP_KEYS_
#define OCEANBASE_SQL_OB_EXPR_MAP_KEYS_

#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/geo/ob_geo_utils.h"
#include "lib/udt/ob_array_utils.h"

namespace oceanbase
{
namespace sql
{

class ObExprMapComponents : public ObFuncExprOperator
{
public:
  explicit ObExprMapComponents(common::ObIAllocator &alloc, ObExprOperatorType type, 
                                const char *name, int32_t param_num, int32_t dimension);
  virtual ~ObExprMapComponents();
  static int calc_map_components_result_type(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx,
                                bool is_key = true);
  static int eval_map_components(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res, bool is_key = true);
  static int eval_map_components_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                          const ObBitVector &skip, const EvalBound &bound, bool is_key = true);
  static  int get_map_components_arr(ObIAllocator &tmp_allocator,
                                ObEvalCtx &ctx,
                                ObString &map_blob, 
                                ObIArrayType *&arr_res, 
                                uint16_t &res_subschema_id,
                                uint16_t &subschema_id,
                                bool is_key = true);
  static int get_map_components_arr_vector(ObIAllocator &tmp_allocator, 
                                  ObEvalCtx &ctx, 
                                  ObExpr &param_expr,
                                  const uint16_t subschema_id, 
                                  int64_t row_idx, 
                                  ObIArrayType *&arr_res,
                                  bool is_key = true);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprMapComponents);
};


class ObExprMapKeys : public ObExprMapComponents
{
public:
  explicit ObExprMapKeys(common::ObIAllocator &alloc);
  virtual ~ObExprMapKeys();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx)
                                const override;
  static int eval_map_keys(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_map_keys_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                          const ObBitVector &skip, const EvalBound &bound);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprMapKeys);
};

class ObExprMapValues : public ObExprMapComponents
{
public:
  explicit ObExprMapValues(common::ObIAllocator &alloc);
  virtual ~ObExprMapValues();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx)
                                const override;
  static int eval_map_values(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_map_values_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                          const ObBitVector &skip, const EvalBound &bound);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprMapValues);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_MAP_KEYS_