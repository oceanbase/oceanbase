/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 * This file contains implementation for json_keys.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_JSON_KEYS_H_
#define OCEANBASE_SQL_OB_EXPR_JSON_KEYS_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/json_type/ob_json_tree.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
class 
ObExprJsonKeys : public ObFuncExprOperator
{
public:
  explicit ObExprJsonKeys(common::ObIAllocator &alloc);
  virtual ~ObExprJsonKeys();
  virtual int calc_result_typeN(ObExprResType& type,
                                ObExprResType* types,
                                int64_t param_num, 
                                common::ObExprTypeCtx& type_ctx)
                                const override;
  static int eval_json_keys(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  virtual bool need_rt_ctx() const override { return true; }
private:
  static int get_keys_from_wrapper(ObIJsonBase *json_doc, 
                                   ObIAllocator *allocator,
                                   ObString &str);
  DISALLOW_COPY_AND_ASSIGN(ObExprJsonKeys);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_JSON_KEYS_H_