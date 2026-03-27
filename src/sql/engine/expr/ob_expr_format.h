/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_FORMAT_H_
#define OCEANBASE_SQL_ENGINE_EXPR_FORMAT_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{

class ObLocale {
 public:
  uint decimal_point_;
  uint thousand_sep_;
  const char *grouping_;
  ObLocale() : decimal_point_('.'), /* decimal point en_US */
               thousand_sep_(','),  /* thousands_sep en_US */
               grouping_("\x03\x03") { } /* grouping en_US */
  ObLocale(uint decimal_point_par, uint thousand_sep_par, const char *grouping_par)
      : decimal_point_(decimal_point_par),
        thousand_sep_(thousand_sep_par),
        grouping_(grouping_par) { }
};

class ObExprFormat : public ObFuncExprOperator
{
public:
  explicit ObExprFormat(common::ObIAllocator &alloc);
  virtual ~ObExprFormat();
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_format_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
private:
  int get_origin_param_type(ObExprResType &ori_type) const;
  static int64_t get_format_scale(int64_t scale);
  static int convert_num_to_str(const ObDatumMeta &x_meta,
                                const ObDatum &x_datum,
                                char *buf,
                                int64_t buf_len,
                                int64_t scale,
                                common::ObString &num_str);
  static int build_format_str(char *buf,
                              const ObLocale &locale,
                              int64_t scale,
                              common::ObString &num_str);
  int calc_result_type(ObExprResType &type, ObExprResType *types) const;
  DISALLOW_COPY_AND_ASSIGN(ObExprFormat);
};
}
}

#endif /* OCEANBASE_SQL_ENGINE_EXPR_CHAR_H_ */
