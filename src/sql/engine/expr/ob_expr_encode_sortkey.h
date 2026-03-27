/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_ENCODE_SORTKEY_H_
#define OCEANBASE_SQL_ENGINE_EXPR_ENCODE_SORTKEY_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_i_expr_extra_info.h"
#include "sql/ob_sql_define.h"
#include "share/ob_order_perserving_encoder.h"
namespace oceanbase
{
namespace sql
{

class ObExprEncodeSortkey : public ObExprOperator {
  class ObExprEncodeCtx : public ObExprOperatorCtx {
  public:
    ObExprEncodeCtx() : ObExprOperatorCtx(), max_len_(0), params_(NULL)
    {}
    virtual ~ObExprEncodeCtx()
    {}

  public:
    uint64_t max_len_;
    share::ObEncParam *params_;
  };

public:
  explicit ObExprEncodeSortkey(common::ObIAllocator &alloc);
  virtual ~ObExprEncodeSortkey();
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;
  static int eval_encode_sortkey(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
  static int eval_encode_sortkey_batch(const ObExpr &expr,
                                       ObEvalCtx &ctx,
                                       const ObBitVector &skip,
                                       const int64_t batch_size);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  virtual bool need_rt_ctx() const override
  {
    return true;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprEncodeSortkey);
};
}  // namespace sql
}  // namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_EXPR_CHAR_H_ */
