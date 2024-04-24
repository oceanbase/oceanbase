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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_VECTOR_
#define OCEANBASE_SQL_ENGINE_EXPR_VECTOR_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{

class ObExprVectorL2Distance : public ObVectorTypeExprOperator
{
public:
    explicit ObExprVectorL2Distance(common::ObIAllocator &alloc);
    virtual ~ObExprVectorL2Distance() {};

    virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const override;

    static int calc_result(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
private:
    DISALLOW_COPY_AND_ASSIGN(ObExprVectorL2Distance);
};

class ObExprVectorCosineDistance : public ObVectorTypeExprOperator
{
public:
    explicit ObExprVectorCosineDistance(common::ObIAllocator &alloc);
    virtual ~ObExprVectorCosineDistance() {};

    virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const override;

    static int calc_result(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
private:
    DISALLOW_COPY_AND_ASSIGN(ObExprVectorCosineDistance);
};

class ObExprVectorIpDistance : public ObVectorTypeExprOperator
{
public:
    explicit ObExprVectorIpDistance(common::ObIAllocator &alloc);
    virtual ~ObExprVectorIpDistance() {};

    virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const override;

    static int calc_result(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
private:
    DISALLOW_COPY_AND_ASSIGN(ObExprVectorIpDistance);
};

}
}

#endif