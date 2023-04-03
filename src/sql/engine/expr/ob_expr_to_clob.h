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

#ifndef _OCEANBASE_SQL_OB_EXPR_TO_CLOB_H_
#define _OCEANBASE_SQL_OB_EXPR_TO_CLOB_H_
#include "sql/engine/expr/ob_expr_oracle_to_char.h"

namespace oceanbase
{
namespace sql
{

// https://docs.oracle.com/cd/E11882_01/server.112/e41084/functions202.htm#SQLRF06131
class ObExprToClob : public ObExprToCharCommon
{
public:
  explicit ObExprToClob(common::ObIAllocator &alloc);
  virtual ~ObExprToClob();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &text,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const;
  static int calc_to_clob_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprToClob);
};

}
}

#endif // OB_EXPR_TO_CLOB_H_
