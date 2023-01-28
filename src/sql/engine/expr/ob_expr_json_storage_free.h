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
 * This file contains implementation for json_storage_free.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_JSON_STORAGE_FREE_H_
#define OCEANBASE_SQL_OB_EXPR_JSON_STORAGE_FREE_H_

#include "sql/engine/expr/ob_expr_operator.h"

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
                  ObIAllocator *allocator, ObDatum &res);
  static int eval_json_storage_free(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprJsonStorageFree);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_JSON_STORAGE_FREE_H_