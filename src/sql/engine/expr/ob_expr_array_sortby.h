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
 * This file contains implementation for array_sortby.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_ARRAY_SORTBY
#define OCEANBASE_SQL_OB_EXPR_ARRAY_SORTBY

#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_array_map.h"
#include "lib/udt/ob_array_type.h"

namespace oceanbase
{
namespace sql
{
class ObExprArraySortby : public ObExprArrayMapCommon
{
public:
  explicit ObExprArraySortby(common::ObIAllocator &alloc);
  explicit ObExprArraySortby(common::ObIAllocator &alloc, ObExprOperatorType type,
                           const char *name, int32_t param_num, int32_t dimension);
  virtual ~ObExprArraySortby();

  virtual int calc_result_typeN(ObExprResType& type,
                                ObExprResType* types,
                                int64_t param_num,
                                common::ObExprTypeCtx& type_ctx) const override;
  static int eval_array_sortby(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int index_sort(common::ObArenaAllocator &allocator, ObIArrayType *lambda_arr, uint32_t *&sort_idx);
  static int fill_array_by_index(ObIArrayType *src_arr, uint32_t *sort_idx, ObIArrayType *res_arr);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprArraySortby);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_ARRAY_SORTBY