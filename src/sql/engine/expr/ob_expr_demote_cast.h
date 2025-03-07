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

#ifndef _OB_EXPR_DEMOTE_CAST_
#define _OB_EXPR_DEMOTE_CAST_

#include "lib/wide_integer/ob_wide_integer.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/resolver/expr/ob_raw_expr_type_demotion.h"

namespace oceanbase
{
namespace sql
{

class ObExprDemoteCastBase : public ObFuncExprOperator
{
public:
  struct TypeDemotionRes
  {
  public:
    explicit TypeDemotionRes(ObDatum &val)
      : val_(val), rp_(ObRawExprTypeDemotion::RangePlacement::RP_OUTSIDE) {}
    inline void set_outside() { rp_ = ObRawExprTypeDemotion::RangePlacement::RP_OUTSIDE; }
    inline void set_inside() { rp_ = ObRawExprTypeDemotion::RangePlacement::RP_INSIDE; }
    inline bool is_outside() const
    { return ObRawExprTypeDemotion::RangePlacement::RP_OUTSIDE == rp_; }

    TO_STRING_KV(K_(val), K_(rp));

    ObDatum &val_; // demoted value from constant
    ObRawExprTypeDemotion::RangePlacement rp_; // range placement of constant in the field
  };

public:
  explicit ObExprDemoteCastBase(common::ObIAllocator &alloc, ObExprOperatorType type,
                                const char *name);
  virtual ~ObExprDemoteCastBase() {};
  static int get_column_res_type(const ObExprResType &param_type, ObExprResType &column_res_type);
  static int get_column_datum_meta(const int64_t val, ObDatumMeta &column_datum_meta);
  int set_calc_type_for_const_param(const ObExprResType &column_type,
                                    ObExprResType &value_type,
                                    common::ObExprTypeCtx &type_ctx) const;
  static int demote_cast(const ObExpr &expr, ObEvalCtx &ctx, TypeDemotionRes &res);
private:
  static int demote_field_constant(const ObDatum &constant,
                                   const ObDatumMeta &src_meta,
                                   const ObDatumMeta &dst_meta,
                                   TypeDemotionRes &res);
  static int demote_int_field_constant(const ObDatum &constant,
                                       const ObDatumMeta &src_meta,
                                       const ObDatumMeta &dst_meta,
                                       TypeDemotionRes &res);
  static int demote_decimal_field_constant(const ObDatum &constant,
                                           const ObDatumMeta &src_meta,
                                           const ObDatumMeta &dst_meta,
                                           TypeDemotionRes &res);
  static int demote_year_field_constant(const ObDatum &constant,
                                        const ObDatumMeta &src_meta,
                                        const ObDatumMeta &dst_meta,
                                        TypeDemotionRes &res);
  static int demote_date_field_constant(const ObDatum &constant,
                                        const ObDatumMeta &src_meta,
                                        const ObDatumMeta &dst_meta,
                                        TypeDemotionRes &res);
  static int demote_datetime_timestamp_field_constant(const ObDatum &constant,
                                                      const ObDatumMeta &src_meta,
                                                      const ObDatumMeta &dst_meta,
                                                      TypeDemotionRes &res);
  static int demote_time_field_constant(const ObDatum &constant,
                                        const ObDatumMeta &src_meta,
                                        const ObDatumMeta &dst_meta,
                                        TypeDemotionRes &res);
  static int demote_null_constant(const ObExpr *constant_expr,
                                  const ObDatumMeta &column_meta,
                                  TypeDemotionRes &res);
private:
  //disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprDemoteCastBase);
};

class ObExprDemoteCast : public ObExprDemoteCastBase
{
public:
  explicit ObExprDemoteCast(common::ObIAllocator &alloc);
  virtual ~ObExprDemoteCast() {};
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const override;

  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  static int eval_demoted_val(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &demoted_val);
private:
  //disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprDemoteCast);
};

class ObExprRangePlacement : public ObExprDemoteCastBase
{
  // The result type of this expression is integer, so there are only 8 bytes as the reserved
  // buffer of the expression. During the analysis process, one datum of the same column type will
  // be required. the extra buffer needs to be supplemented by expr. Currently, the maximum buffer
  // size is sizeof(int512).
  static const int64_t MAX_TYPE_BUF_SIZE = sizeof(int512_t);
public:
  explicit ObExprRangePlacement(common::ObIAllocator &alloc);
  virtual ~ObExprRangePlacement() {};
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const override;

  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  static int eval_range_placement(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &range_placement);
private:
  //disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprRangePlacement);
};

}
}
#endif  /* _OB_EXPR_DEMOTE_CAST_ */
