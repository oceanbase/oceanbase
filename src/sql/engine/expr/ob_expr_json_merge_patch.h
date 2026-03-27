/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 * This file is for define of func json_merge_patch
 */

#ifndef OCEANBASE_SQL_OB_EXPR_JSON_MERGE_PATCH_H_
#define OCEANBASE_SQL_OB_EXPR_JSON_MERGE_PATCH_H_

#include "sql/engine/expr/ob_expr_operator.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
class ObExprJsonMergePatch : public ObFuncExprOperator
{
public:
  enum {
    OPT_RES_TYPE_ID,
    OPT_PRETTY_ID,
    OPT_ASCII_ID,
    OPT_TRUNC_ID,
    OPT_ERROR_ID,
    OPT_MAX_ID
  };
  explicit ObExprJsonMergePatch(common::ObIAllocator &alloc);
  virtual ~ObExprJsonMergePatch();
  virtual int calc_result_typeN(ObExprResType& type,
                                ObExprResType* types,
                                int64_t param_num, 
                                common::ObExprTypeCtx& type_ctx)
                                const override;

  static int eval_json_merge_patch(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_ora_json_merge_patch(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                    ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprJsonMergePatch);
};

} // sql
} // oceanbase
#endif 