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

#include "sql/engine/expr/ob_expr_ivfflat_vec_cluster_id.h"
#include "sql/engine/ob_exec_context.h"
#include "src/share/vector_index/ob_tenant_ivfflat_center_cache.h"
#include "lib/vector/ob_vector.h"

namespace oceanbase
{
using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace sql
{

OB_SERIALIZE_MEMBER(ScanTableId,
                    ref_table_id_);

int ScanTableId::deep_copy(common::ObIAllocator &allocator,
                           const ObExprOperatorType type,
                           ObIExprExtraInfo *&copied_info) const
{
    int ret = OB_SUCCESS;
    if (OB_FAIL(ObExprExtraInfoFactory::alloc(allocator, type,
                                                copied_info))) {
        LOG_WARN("failed to alloc extra info", K(ret));
    } else {
        ScanTableId *base_info = static_cast<ScanTableId*>(copied_info);
        base_info->ref_table_id_ = ref_table_id_;
    }
    return ret;
}

ObExprIvfflatVecClusterId::ObExprIvfflatVecClusterId(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_CALC_IVFFLAT_VECTOR_CLUSTER_ID, N_IVFFLAT_VECTOR_CLUSTER_ID, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprIvfflatVecClusterId::~ObExprIvfflatVecClusterId()
{
}

int ObExprIvfflatVecClusterId::calc_result_type2(ObExprResType &type,
                                                 ObExprResType &vec,
                                                 ObExprResType &ivfflat_index_table_id_type,
                                                 ObExprTypeCtx &type_ctx) const
{
    UNUSED(type_ctx);

    int ret = OB_SUCCESS;
    if (ObVectorType != vec.get_type() || !ob_is_int_tc(ivfflat_index_table_id_type.get_type())) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("ivfflat_vec_cluster_id args types are not supported", K(ret), K(vec.get_type()), K(ivfflat_index_table_id_type.get_type()));
    } else if (vec.is_null() || ivfflat_index_table_id_type.is_null()) {
        type.set_null();
    } else {
        if (ObVectorType != vec.get_type()) {
            ret = OB_INVALID_ARGUMENT_NUM;
            LOG_WARN("the param is not a vector", K(vec), K(ret));
        } else {
            type.set_int();
            type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].precision_);
            type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
        }
    }
    return ret;
}

int ObExprIvfflatVecClusterId::eval_ivfflat_vec_cluster_id(const ObExpr &expr, ObEvalCtx &ctx, 
                                                           ObDatum &res)
{
    int ret = OB_SUCCESS;
    int64_t base_table_id = reinterpret_cast<ScanTableId*>(expr.extra_info_)->ref_table_id_;
    ObTenantIvfflatCenterCache *ivfflat_center_cache = nullptr;
    ObTableIvfflatCenters *centers = nullptr;
    uint64_t tablet_id = ctx.exec_ctx_.get_extra_info();
    int64_t ivfflat_index_table_id = -1;
    int64_t partition_idx = -1;
    ObDatum *arg0 = NULL;
    ObDatum *arg1 = NULL;

    if (OB_UNLIKELY(ObObjTypeClass::ObVectorTC != ob_obj_type_class(expr.args_[0]->datum_meta_.type_))) {
        res.set_null();
    } else if (OB_UNLIKELY(!ob_is_int_tc(expr.args_[1]->datum_meta_.type_))) {
        res.set_null();
    } else if (OB_FAIL(expr.eval_param_value(ctx, arg0, arg1))) {
        LOG_WARN("calc param value failed", K(ret));
    } else if (OB_FALSE_IT(ivfflat_index_table_id = arg1->get_int())) {
    } else if (OB_ISNULL(ivfflat_center_cache = MTL(ObTenantIvfflatCenterCache *))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("MTL ObTenantIvfflatCenterCache is null", KR(ret));
    } else if (arg0->is_null()) {
        res.set_null();
    } else if (OB_INVALID_ID == tablet_id && OB_FALSE_IT(partition_idx = 0)) {
    } else if (OB_INVALID_ID != tablet_id && OB_FAIL(ObTenantIvfflatCenterCache::get_partition_index_with_tablet_id(MTL_ID(), base_table_id, tablet_id, partition_idx))) {
        if (OB_OP_NOT_ALLOW == ret) {
            partition_idx = -1;
            ret = OB_SUCCESS;
        } else {
            LOG_WARN("fail to get partition index", K(ret), K(base_table_id), K(tablet_id));
        }
    }
    
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ivfflat_center_cache->get(ivfflat_index_table_id, partition_idx, centers))) {
        LOG_WARN("fail to get ivfflat center cache", K(ret), K(ivfflat_index_table_id), K(partition_idx));
    } else if (OB_ISNULL(centers)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get ivfflat center cache", K(ret), K(ivfflat_index_table_id), K(partition_idx));
    } else {
        uint64_t cluster_id = 0;
        const ObTypeVector &arg_vec = arg0->get_vector();
        double min_cluster_dist = DBL_MAX;
        double cluster_dist = 0;
        for (int64_t i = 0; OB_SUCC(ret) && i < centers->count(); ++i) {
            const ObTypeVector& center = centers->at(i);
            switch (centers->get_dis_type()) {
            case ObVectorDistanceType::L2: {
                arg_vec.cal_l2_distance(center, cluster_dist);
                break;
            }
            case ObVectorDistanceType::INNER_PRODUCT:
            case ObVectorDistanceType::COSINE: {
                arg_vec.cal_angular_distance(center, cluster_dist);
                break;
            }
            default: {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("invalid vector distance type", K(ret), K(centers->get_dis_type()));
                break;
            }
            }
            if (OB_SUCC(ret) && cluster_dist < min_cluster_dist) {
                cluster_id = i;
                min_cluster_dist = cluster_dist;
            }
        }
        if (OB_SUCC(ret)) {
            res.set_uint(cluster_id);
        }
    }
    return ret;
}

int ObExprIvfflatVecClusterId::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                                       ObExpr &rt_expr) const
{
    int ret = OB_SUCCESS;
    // UNUSED(expr_cg_ctx);
    // UNUSED(raw_expr);
    rt_expr.eval_func_ = eval_ivfflat_vec_cluster_id;
    ObTableID ref_table_id = reinterpret_cast<ObTableID>(raw_expr.get_extra());
    ScanTableId *table_id_info = NULL;
    void* buf = expr_cg_ctx.allocator_->alloc(sizeof(ScanTableId));
    if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", K(ret));
    } else {
        table_id_info = new(buf) ScanTableId(*(expr_cg_ctx.allocator_), get_type());
        table_id_info->ref_table_id_ = ref_table_id;
        rt_expr.extra_info_ = table_id_info;
    }
    return ret;
}


}
}

