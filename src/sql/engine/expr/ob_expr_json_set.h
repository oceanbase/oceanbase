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
 * This file is for implementation of func json_set
 */

#ifndef OCEANBASE_SQL_OB_EXPR_JSON_SET_H_
#define OCEANBASE_SQL_OB_EXPR_JSON_SET_H_

#include "sql/engine/expr/ob_expr_operator.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

class ObExprJsonSet : public ObFuncExprOperator
{
public:
  explicit ObExprJsonSet(common::ObIAllocator &alloc);
  virtual ~ObExprJsonSet();
  virtual int calc_result_typeN(ObExprResType& type,
                               ObExprResType* types,
                               int64_t param_num,
                               common::ObExprTypeCtx& type_ctx)
                               const override;
  static int set_value(ObJsonBaseVector &hit, ObIJsonBase *&json_doc, ObIJsonBase* json_val,
                       ObJsonPath *json_path, ObIAllocator *allocator);
  static int eval_json_set(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                     ObExpr &rt_expr) const override;
  virtual bool need_rt_ctx() const override { return true; }
private:
    DISALLOW_COPY_AND_ASSIGN(ObExprJsonSet);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_JSON_SET_H_