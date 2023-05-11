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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_INITCAP_
#define OCEANBASE_SQL_ENGINE_EXPR_INITCAP_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprInitcapCommon : public ObStringExprOperator
{
public:
  ObExprInitcapCommon(common::ObIAllocator &alloc,
                ObExprOperatorType type, const char *name, int32_t param_num)
                :ObStringExprOperator(alloc, type, name, param_num, VALID_FOR_GENERATED_COL)
  {}
  virtual ~ObExprInitcapCommon() {}
  static int initcap_string(const common::ObString &text,
                                const common::ObCollationType cs_type,
                                common::ObIAllocator *allocator,
                                common::ObString &res_str,
                                bool &has_first_letter);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprInitcapCommon);
};

class ObExprInitcap : public ObExprInitcapCommon
{
public:
  explicit  ObExprInitcap(common::ObIAllocator &alloc);
  virtual ~ObExprInitcap();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &text,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                           ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprInitcap);
};

class ObExprNlsInitCap : public ObExprInitcapCommon
{
public:
  explicit  ObExprNlsInitCap(common::ObIAllocator &alloc);
  virtual ~ObExprNlsInitCap() {}
  virtual int calc_result_typeN(ObExprResType &type,
                              ObExprResType *texts,
                              int64_t param_num,
                              common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_nls_initcap_expr(const ObExpr &expr,
                                   ObEvalCtx &ctx,
                                   ObDatum &expr_datum);
  static int calc_nls_initcap_batch(const ObExpr &expr,
                                    ObEvalCtx &ctx,
                                    const ObBitVector &skip,
                                    const int64_t batch_size);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprNlsInitCap);
};

}
}
#endif /* OCEANBASE_SQL_ENGINE_EXPR_INITCAP_ */
