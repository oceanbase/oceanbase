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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_LOWER_
#define OCEANBASE_SQL_ENGINE_EXPR_LOWER_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprLowerUpper : public ObStringExprOperator
{
public:
  static const char SEPARATOR_IN_NLS_SORT_PARAM = '=';
  ObExprLowerUpper(common::ObIAllocator &alloc,
                   ObExprOperatorType type, const char *name, int32_t param_num);
  virtual ~ObExprLowerUpper() {}
  // For lower/upper of mysql and oracle
  virtual int calc_result_type1(ObExprResType &type, ObExprResType &text,
                                common::ObExprTypeCtx &type_ctx) const;
  // For oracle only nls_lower/nls_upper
  virtual int calc_result_typeN(ObExprResType &type,
                              ObExprResType *texts,
                              int64_t param_num,
                              common::ObExprTypeCtx &type_ctx) const;
  static int calc_common(const ObExpr &expr, ObEvalCtx &ctx,
                         ObDatum &expr_datum, bool lower, common::ObCollationType cs_type);
  static int calc_nls_common(const ObExpr &expr, ObEvalCtx &ctx,
                             ObDatum &expr_datum, bool lower);
  int cg_expr_common(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const;
  int cg_expr_nls_common(ObExprCGCtx &op_cg_ctx,
                         const ObRawExpr &raw_expr,
                         ObExpr &rt_expr) const;
protected:
  virtual int calc(const common::ObCollationType cs_type, char *src, int32_t src_len,
                   char *dest, int32_t det_len, int32_t &out_len) const = 0;
  virtual int32_t get_case_mutiply(const common::ObCollationType cs_type) const = 0;
private:
  int calc(common::ObObj &result,
           const common::ObString &text,
           common::ObCollationType cs_type,
           common::ObIAllocator &calc_buf) const;
  DISALLOW_COPY_AND_ASSIGN(ObExprLowerUpper);
};

class ObExprLower : public ObExprLowerUpper
{
public:
  explicit  ObExprLower(common::ObIAllocator &alloc);
  virtual ~ObExprLower() {}
  virtual int calc(const common::ObCollationType cs_type, char *src, int32_t src_len,
                   char *dest, int32_t det_len, int32_t &out_len) const;
  virtual int32_t get_case_mutiply(const common::ObCollationType cs_type) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_lower(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprLower);
};

class ObExprUpper : public ObExprLowerUpper
{
public:
  explicit  ObExprUpper(common::ObIAllocator &alloc);
  virtual ~ObExprUpper() {}
  virtual int calc(const common::ObCollationType cs_type, char *src, int32_t src_len,
                   char *dest, int32_t det_len, int32_t &out_len) const;
  virtual int32_t get_case_mutiply(const common::ObCollationType cs_type) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_upper(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprUpper);
};

class ObExprNlsLower : public ObExprLowerUpper
{
public:
  explicit  ObExprNlsLower(common::ObIAllocator &alloc);
  virtual ~ObExprNlsLower() {}
  virtual int calc(const common::ObCollationType cs_type, char *src, int32_t src_len,
                   char *dest, int32_t det_len, int32_t &out_len) const;
  virtual int32_t get_case_mutiply(const common::ObCollationType cs_type) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_lower(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprNlsLower);
};

class ObExprNlsUpper : public ObExprLowerUpper
{
public:
  explicit  ObExprNlsUpper(common::ObIAllocator &alloc);
  virtual ~ObExprNlsUpper() {}
  virtual int calc(const common::ObCollationType cs_type, char *src, int32_t src_len,
                   char *dest, int32_t det_len, int32_t &out_len) const;
  virtual int32_t get_case_mutiply(const common::ObCollationType cs_type) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_upper(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprNlsUpper);
};

}
}
#endif /* OCEANBASE_SQL_ENGINE_EXPR_LOWER_ */
