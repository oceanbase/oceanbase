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
 * This file contains implementation for json_length.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_JSON_LENGTH_H_
#define OCEANBASE_SQL_OB_EXPR_JSON_LENGTH_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/json_type/ob_json_path.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
class ObExprJsonLength : public ObFuncExprOperator
{
public:
  explicit ObExprJsonLength(common::ObIAllocator &alloc);
  virtual ~ObExprJsonLength();
  virtual int calc_result_typeN(ObExprResType& type,
                                ObExprResType* types_stack,
                                int64_t param_num,
                                ObExprTypeCtx& type_ctx) const override;

  static int calc(ObEvalCtx &ctx, const ObDatum &data1, ObDatumMeta meta1, bool has_lob_header1,
                  const ObDatum *data2, ObDatumMeta meta2, bool has_lob_header2,
                  ObIAllocator *allocator, ObDatum &res,
                  ObJsonPathCache* path_cache);
  static int eval_json_length(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  virtual bool need_rt_ctx() const override { return true; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprJsonLength);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_JSON_LENGTH_H_