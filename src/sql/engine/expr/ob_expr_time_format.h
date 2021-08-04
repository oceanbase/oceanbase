// Copyright 2021 Alibaba Inc. All Rights Reserved.
// Author:
//     shanting <dachuan.sdc@antgroup.com>


#ifndef OCEANBASE_SQL_OB_EXPR_TIME_FORMAT_H_
#define OCEANBASE_SQL_OB_EXPR_TIME_FORMAT_H_

#include "lib/timezone/ob_time_convert.h"
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprTimeFormat : public ObStringExprOperator
{
public:
  explicit  ObExprTimeFormat(common::ObIAllocator &alloc);
  virtual ~ObExprTimeFormat();
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &date,
                                ObExprResType &format,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int calc_result2(common::ObObj &result,
                           const common::ObObj &date,
                           const common::ObObj &format,
                           common::ObExprCtx &expr_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int time_to_str_format(const int64_t &time_value, const common::ObString &format,
                                char *buf, int64_t buf_len, int64_t &pos, bool &res_null);
  static int calc_time_format(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprTimeFormat);

  static const int64_t OB_TEMPORAL_BUF_SIZE_RATIO = 30;
};

} //sql
} //oceanbase

#endif //OCEANBASE_SQL_OB_EXPR_TIME_FORMAT_H_
