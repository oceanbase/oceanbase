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

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/expr/ob_expr_vector.h"
#include "lib/oblog/ob_log.h"
#include "lib/vector/ob_vector_l2_distance.h"
#include "share/object/ob_obj_cast.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{

ObExprVectorL2Distance::ObExprVectorL2Distance(ObIAllocator &alloc) :
    ObVectorTypeExprOperator(alloc, T_OP_VECTOR_L2_DISTANCE, N_VECTOR_L2_DISTANCE, 2, NOT_ROW_DIMENSION) {}

int ObExprVectorL2Distance::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                                    ObExpr &rt_expr) const
{
    int ret = OB_SUCCESS;
    rt_expr.eval_func_ = ObExprVectorL2Distance::calc_result;
    return ret;
}

int ObExprVectorL2Distance::calc_result(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
    int ret = OB_SUCCESS;
    ObDatum *l = NULL;
    ObDatum *r = NULL;
    expr.args_[0]->max_length_ = -1;
    expr.args_[1]->max_length_ = -1;
    if (OB_FAIL(expr.args_[0]->eval(ctx, l))) {
        LOG_WARN("left eval failed", K(ret));
    } else if (OB_FAIL(expr.args_[1]->eval(ctx, r))) {
        LOG_WARN("right eval failed", K(ret));
    } else if (OB_UNLIKELY(l->is_null() || r->is_null())) {
        res_datum.set_null();
    } else {
        double distance = 0.0;
        ObTypeVector lvec = l->get_vector();
        ObTypeVector rvec = r->get_vector();
        if (OB_UNLIKELY(lvec.dims() != rvec.dims())) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("vector length mismatch", K(ret), K(lvec.dims()), K(rvec.dims()));
        } else if (OB_FAIL(ObVectorL2Distance::l2_distance_func(lvec.ptr(), rvec.ptr(), lvec.dims(), distance))) {
            LOG_WARN("failed to cal distance", K(ret), K(lvec), K(rvec));
        // } else if (ctx.with_order_ && OB_FAIL(ObVectorL2Distance::l2_square_func(lvec.ptr(), rvec.ptr(), lvec.count(), distance))) {
        //     LOG_WARN("failed to cal distance", K(ret), K(lvec), K(rvec));
        } else {
            res_datum.set_double(distance);
        }
    }

    return ret;
}

ObExprVectorCosineDistance::ObExprVectorCosineDistance(ObIAllocator &alloc) :
    ObVectorTypeExprOperator(alloc, T_OP_VECTOR_COSINE_DISTANCE, N_VECTOR_COS_DISTANCE, 2, NOT_ROW_DIMENSION) {}

int ObExprVectorCosineDistance::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                                    ObExpr &rt_expr) const
{
    int ret = OB_SUCCESS;
    rt_expr.eval_func_ = ObExprVectorCosineDistance::calc_result;
    return ret;
}

int ObExprVectorCosineDistance::calc_result(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
    int ret = OB_SUCCESS;
    ObDatum *l = NULL;
    ObDatum *r = NULL;
    expr.args_[0]->max_length_ = -1;
    expr.args_[1]->max_length_ = -1;
    if (OB_FAIL(expr.args_[0]->eval(ctx, l))) {
        LOG_WARN("left eval failed", K(ret));
    } else if (OB_FAIL(expr.args_[1]->eval(ctx, r))) {
        LOG_WARN("right eval failed", K(ret));
    } else if (OB_UNLIKELY(l->is_null() || r->is_null())) {
        res_datum.set_null();
    } else {
        ObTypeVector lvec = l->get_vector();
        ObTypeVector rvec = r->get_vector();
        int64_t len = rvec.dims();
        if (OB_UNLIKELY(lvec.dims() != len)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("vector length mismatch", K(ret), K(lvec.dims()), K(rvec.dims()));
        }
        double distance = 0.0;
        if (OB_SUCC(ret) && OB_FAIL(ObTypeVector::cosine_distance(lvec, rvec, distance))) {
            LOG_WARN("fail to calc distance", K(ret));
        } else {
            res_datum.set_double(distance);
        }
    }
    return ret;
}

ObExprVectorIpDistance::ObExprVectorIpDistance(ObIAllocator &alloc) :
    ObVectorTypeExprOperator(alloc, T_OP_VECTOR_INNER_PRODUCT, N_VECTOR_INNER_PRODUCT, 2, NOT_ROW_DIMENSION) {}

int ObExprVectorIpDistance::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                                    ObExpr &rt_expr) const
{
    int ret = OB_SUCCESS;
    rt_expr.eval_func_ = ObExprVectorIpDistance::calc_result;
    return ret;
}

int ObExprVectorIpDistance::calc_result(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
    int ret = OB_SUCCESS;
    ObDatum *l = NULL;
    ObDatum *r = NULL;
    expr.args_[0]->max_length_ = -1;
    expr.args_[1]->max_length_ = -1;
    if (OB_FAIL(expr.args_[0]->eval(ctx, l))) {
        LOG_WARN("left eval failed", K(ret));
    } else if (OB_FAIL(expr.args_[1]->eval(ctx, r))) {
        LOG_WARN("right eval failed", K(ret));
    } else if (OB_UNLIKELY(l->is_null() || r->is_null())) {
        res_datum.set_null();
    } else {
        ObTypeVector lvec = l->get_vector();
        ObTypeVector rvec = r->get_vector();
        int64_t len = rvec.dims();
        if (OB_UNLIKELY(lvec.dims() != len)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("vector length mismatch", K(ret), K(lvec.dims()), K(rvec.dims()));
        }
        double distance = 0.0;
        if (OB_SUCC(ret) && OB_FAIL(ObTypeVector::ip_distance(lvec, rvec, distance))) {
            LOG_WARN("fail to calc distance", K(ret));
        } else {
            res_datum.set_double(distance);
        }
    }
    return ret;
}

}
}