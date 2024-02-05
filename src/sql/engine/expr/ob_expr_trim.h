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

#ifndef _OB_SQL_EXPR_TRIM_H_
#define _OB_SQL_EXPR_TRIM_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace common
{
  class ObExprTypeCtx;
  struct ObExprCtx;
} // common
namespace sql
{
class ObExprTrim : public ObStringExprOperator
{
public:
  enum TrimType { TYPE_LRTRIM = 0, TYPE_LTRIM = 1, TYPE_RTRIM = 2 };

  explicit  ObExprTrim(common::ObIAllocator &alloc);
  explicit  ObExprTrim(common::ObIAllocator &alloc,
                       ObExprOperatorType type,
                       const char *name,
                       int32_t param_num);

  virtual ~ObExprTrim();
  static int trim(common::ObString &result,
                  const int64_t trim_type,
                  const common::ObString &trim_pattern,
                  const common::ObString &text);

  static  int trim2(common::ObString &result,
                  const int64_t trim_type,
                  const common::ObString &trim_pattern,
                  const common::ObString &text,
                  const common::ObCollationType &cs_type,
                  const common::ObFixedArray<size_t, common::ObIAllocator> &,
                  const common::ObFixedArray<size_t, common::ObIAllocator> &);

  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const
  {
    return deduce_result_type(type, types, param_num, type_ctx);
  }

  static int deduce_result_type(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx);


  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  static int eval_trim(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);

  // fill ' ' to %buf with specified charset.
  static int fill_default_pattern(char *buf, const int64_t in_len,
                                  common::ObCollationType cs_type, int64_t &out_len);
  DECLARE_SET_LOCAL_SESSION_VARS;
private:
  // helper func
  static int lrtrim(const common::ObString src,
                    const common::ObString pattern,
                    int32_t &start, int32_t &end);
  static int ltrim(const common::ObString src,
                   const common::ObString pattern,
                   int32_t &start);
  static int rtrim(const common::ObString src, const common::ObString pattern, int32_t &end);

  static int lrtrim2(const common::ObString src,
                    const common::ObString pattern,
                    int32_t &start, int32_t &end);
  static int ltrim2(const common::ObString src,
                   const common::ObString pattern,
                   int32_t &start,
                   const common::ObCollationType &cs_type,
                   const common::ObFixedArray<size_t, common::ObIAllocator> &,
                   const common::ObFixedArray<size_t, common::ObIAllocator> &);
  static int rtrim2(const common::ObString src,
                    const common::ObString pattern,
                    int32_t &end,
                    const common::ObCollationType &cs_type,
                    const common::ObFixedArray<size_t, common::ObIAllocator> &,
                    const common::ObFixedArray<size_t, common::ObIAllocator> &);

  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprTrim);

};

// Ltrim can use ObExprTrim's calc
class ObExprLtrim : public ObExprTrim
{
public:
  explicit  ObExprLtrim(common::ObIAllocator &alloc);
  explicit  ObExprLtrim(common::ObIAllocator &alloc,
                        ObExprOperatorType type,
                        const char *name,
                        int32_t param_num);
  virtual ~ObExprLtrim();

  virtual int calc_result_type1(ObExprResType &res_type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;

  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const
  {
    return deduce_result_type(type, types, param_num, type_ctx);
  }

  static int deduce_result_type(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx);

  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;


private:
  DISALLOW_COPY_AND_ASSIGN(ObExprLtrim);

};
// Rtrim can use ObExprTrim's calc
class ObExprRtrim : public ObExprLtrim
{
public:
  explicit  ObExprRtrim(common::ObIAllocator &alloc);
  virtual ~ObExprRtrim();

  virtual int calc_result_type1(ObExprResType &res_type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;

  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const
  {
    return ObExprLtrim::deduce_result_type(type, types, param_num, type_ctx);
  }

  static int deduce_result_type(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx);

  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const
  {
    return ObExprLtrim::cg_expr(op_cg_ctx, raw_expr, rt_expr);
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprRtrim);

};
} // sql
} // oceanbase
#endif /* _OB_SQL_EXPR_TRIM_H_ */
