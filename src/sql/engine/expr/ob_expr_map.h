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
 * This file contains implementation for map.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_MAP
#define OCEANBASE_SQL_OB_EXPR_MAP

#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/geo/ob_geo_utils.h"
#include "lib/udt/ob_array_utils.h"


namespace oceanbase
{
namespace sql
{
class ObExprMap : public ObFuncExprOperator
{
public:
  explicit ObExprMap(common::ObIAllocator &alloc);
  explicit ObExprMap(common::ObIAllocator &alloc, ObExprOperatorType type, 
                                const char *name, int32_t param_num, int32_t dimension);
  virtual ~ObExprMap();
  virtual int calc_result_typeN(ObExprResType& type,
                                ObExprResType* types,
                                int64_t param_num, 
                                common::ObExprTypeCtx& type_ctx)
                                const override;
  static int eval_map(const ObExpr &expr,
                        ObEvalCtx &ctx,
                        ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  static int deduce_element_type(ObExecContext *exec_ctx, ObExprResType* types_stack,
                                 int64_t param_num, uint16_t &key_subid, uint16_t &value_subid);
  static int construct_key_array(ObEvalCtx &ctx, const ObExpr &expr,
                                 const ObObjType elem_type, ObIArrayType *&full_key_arr,
                                 uint32_t *&idx_arr, uint32_t &idx_count);

  DISALLOW_COPY_AND_ASSIGN(ObExprMap);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_MAP