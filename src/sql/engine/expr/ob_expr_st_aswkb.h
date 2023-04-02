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
 * This file contains implementation for st_aswkb/_st_asewkb/st_asbinary expr.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_ST_ASWKB_
#define OCEANBASE_SQL_OB_EXPR_ST_ASWKB_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{

class ObExprGeomWkb : public ObFuncExprOperator
{
public:
  explicit ObExprGeomWkb(common::ObIAllocator &alloc,
                         ObExprOperatorType type,
                         const char *name,
                         int32_t param_num,
                         int32_t dimension);
  virtual ~ObExprGeomWkb();
  virtual int calc_result_typeN(ObExprResType& type,
                                ObExprResType* types_stack,
                                int64_t param_num,
                                common::ObExprTypeCtx& type_ctx) const override;
  int eval_geom_wkb(const ObExpr &expr,
                    ObEvalCtx &ctx,
                    ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override = 0;
  virtual const char *get_func_name() const = 0;
  virtual ObItemType get_expr_type() const = 0;
private:
  bool is_blank_string(const common::ObCollationType coll_type,
                       const common::ObString &str) const;
  DISALLOW_COPY_AND_ASSIGN(ObExprGeomWkb);
};

class ObExprSTAsWkb : public ObExprGeomWkb
{
public:
  explicit ObExprSTAsWkb(common::ObIAllocator &alloc);
  virtual ~ObExprSTAsWkb();
  static int eval_st_aswkb(const ObExpr &expr,
                           ObEvalCtx &ctx,
                           ObDatum &res);
  int cg_expr(ObExprCGCtx &expr_cg_ctx,
              const ObRawExpr &raw_expr,
              ObExpr &rt_expr) const override;
  const char *get_func_name() const override { return N_ST_ASWKB; }
  ObItemType get_expr_type() const override { return T_FUN_SYS_ST_ASWKB; };
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprSTAsWkb);
};

class ObExprSTAsBinary : public ObExprGeomWkb
{
public:
  explicit ObExprSTAsBinary(common::ObIAllocator &alloc);
  virtual ~ObExprSTAsBinary();
  static int eval_st_asbinary(const ObExpr &expr,
                              ObEvalCtx &ctx,
                              ObDatum &res);
  int cg_expr(ObExprCGCtx &expr_cg_ctx,
              const ObRawExpr &raw_expr,
              ObExpr &rt_expr) const override;
  const char *get_func_name() const override { return N_ST_ASBINARY; }
  ObItemType get_expr_type() const override { return T_FUN_SYS_ST_ASBINARY; };
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprSTAsBinary);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_ST_ASWKB_