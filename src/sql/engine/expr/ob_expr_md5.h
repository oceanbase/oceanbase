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

#ifndef _OB_EXPR_MD5_H_
#define _OB_EXPR_MD5_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprMd5: public ObStringExprOperator
{
public:
  ObExprMd5();
  explicit  ObExprMd5(common::ObIAllocator &alloc);
  virtual ~ObExprMd5();

public:
    virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &str,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_md5(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  DECLARE_SET_LOCAL_SESSION_VARS;
private:
  int calc_md5(common::ObObj &result,
               const common::ObString &str,
               common::ObIAllocator *allocator,
               common::ObCollationType col_type) const;
  static const common::ObString::obstr_size_t MD5_LENGTH = 16;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprMd5);
};
} /* namespace sql */
} /* namespace oceanbase */

#endif /* _OB_EXPR_MD5_H_ */
