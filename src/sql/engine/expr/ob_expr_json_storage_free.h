/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 * This file contains implementation for json_storage_free.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_JSON_STORAGE_FREE_H_
#define OCEANBASE_SQL_OB_EXPR_JSON_STORAGE_FREE_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_multi_mode_func_helper.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
class ObExprJsonStorageFree : public ObFuncExprOperator
{
public:
  explicit ObExprJsonStorageFree(common::ObIAllocator &alloc);
  virtual ~ObExprJsonStorageFree();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx)
                                const override;

  static int calc(ObEvalCtx &ctx, const ObDatum &data, ObDatumMeta meta, bool has_lob_header,
                  MultimodeAlloctor *allocator, ObDatum &res);
  static int eval_json_storage_free(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprJsonStorageFree);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_JSON_STORAGE_FREE_H_