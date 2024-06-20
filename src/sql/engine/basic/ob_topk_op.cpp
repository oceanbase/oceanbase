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
#include "sql/engine/basic/ob_topk_op.h"
#include "sql/engine/basic/ob_limit_op.h"
#include "sql/engine/sort/ob_sort_op.h"
#include "sql/engine/basic/ob_material_op.h"
#include "sql/engine/aggregate/ob_hash_groupby_op.h"
#include "sql/engine/basic/ob_material_vec_op.h"
#include "sql/engine/aggregate/ob_hash_groupby_vec_op.h"
#include "sql/engine/basic/ob_monitoring_dump_op.h"

namespace oceanbase
{
namespace sql
{
using namespace oceanbase::common;

ObTopKSpec::ObTopKSpec(ObIAllocator &alloc, const ObPhyOperatorType type)
  : ObOpSpec(alloc, type), minimum_row_count_(-1), topk_precision_(-1),
    org_limit_(NULL), org_offset_(NULL) {}

bool ObTopKSpec::is_valid() const
{
  return OB_NOT_NULL(org_limit_) && OB_NOT_NULL(child_);
}

OB_SERIALIZE_MEMBER((ObTopKSpec, ObOpSpec), minimum_row_count_, topk_precision_,
                    org_limit_, org_offset_);

ObTopKOp::ObTopKOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
  : ObOperator(exec_ctx, spec, input), topk_final_count_(-1), output_count_(0) {}

int ObTopKOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (!MY_SPEC.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("topk operator is invalid", K(ret));
  }
  return ret;
}

int ObTopKOp::inner_rescan()
{
  output_count_ = 0;
  return ObOperator::inner_rescan();
}

int ObTopKOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (0 == output_count_ || output_count_ < topk_final_count_) {
    if (OB_FAIL(child_->get_next_row())) {
      if (OB_ITER_END == ret) {
        LOG_WARN("child get next row", K(ret), K(output_count_), K(topk_final_count_));
      }
    } else {
      if (0 == output_count_) {
        if (OB_FAIL(get_topk_final_count())) {
          LOG_WARN("get topk count failed", K(ret));
        } else if (OB_UNLIKELY(0 == topk_final_count_)) {
          ret = OB_ITER_END;
        }
      }
      if (OB_SUCC(ret)) {
        clear_evaluated_flag();
        ++output_count_;
      }
    }
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObTopKOp::get_topk_final_count()
{
  int ret = OB_SUCCESS;
  int64_t limit = -1;
  int64_t offset = 0;
  bool is_null_value = false;
  ObPhysicalPlanCtx *plan_ctx = ctx_.get_physical_plan_ctx();
  if (OB_ISNULL(child_) || OB_ISNULL(plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child_ or plan_ctx is NULL", K(ret), KP(child_), KP(plan_ctx));
  } else if (OB_FAIL(ObLimitOp::get_int_val(MY_SPEC.org_limit_, eval_ctx_,
                                            limit, is_null_value))) {
    LOG_WARN("get limit values failed", K(ret));
  } else if (!is_null_value && OB_FAIL(ObLimitOp::get_int_val(MY_SPEC.org_offset_, eval_ctx_,
                                                              offset, is_null_value))) {
    LOG_WARN("get offset values failed", K(ret));
  } else {
    //revise limit, offset because rownum < -1 is rewritten as limit -1
    limit = (is_null_value || limit < 0) ? 0 : limit;
    offset = (is_null_value || offset < 0) ? 0 : offset;
    topk_final_count_ = std::max(MY_SPEC.minimum_row_count_, limit + offset);
    int64_t row_count = 0;
    ObPhyOperatorType op_type = child_->get_spec().get_type();
    switch (op_type) {
      case PHY_SORT: {
        ObSortOp *sort_op = static_cast<ObSortOp *>(child_);
        row_count = sort_op->get_sort_row_count();
        break;
      }
      case PHY_MATERIAL: {
        ObMaterialOp *mtrl_op = static_cast<ObMaterialOp *>(child_);
        if (OB_FAIL(mtrl_op->get_material_row_count(row_count))) {
          LOG_WARN("get material row count failed", K(ret));
        }
        break;
      }
      case PHY_HASH_GROUP_BY: {
        ObHashGroupByOp *gby_op = static_cast<ObHashGroupByOp *>(child_);
        row_count = gby_op->get_hash_groupby_row_count();
        break;
      }
      case PHY_VEC_HASH_GROUP_BY: {
        ObHashGroupByVecOp *vec_gby_op = static_cast<ObHashGroupByVecOp *>(child_);
        row_count = vec_gby_op->get_hash_groupby_row_count();
        break;
      }
      case PHY_VEC_MATERIAL: {
        ObMaterialVecOp *mtrl_op = static_cast<ObMaterialVecOp *>(child_);
        if (OB_FAIL(mtrl_op->get_material_row_count(row_count))) {
          LOG_WARN("get material row count failed", K(ret));
        }
        break;
      }
      case PHY_MONITORING_DUMP: {
        ObMonitoringDumpOp *monitor_op = static_cast<ObMonitoringDumpOp *>(child_);
        row_count = monitor_op->get_monitored_row_count();
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid child_ for topk operator", K(ret), K(op_type));
        break;
      }
    }

    if (OB_SUCC(ret)) {
      topk_final_count_ = std::max(topk_final_count_,
          static_cast<int64_t>(row_count * MY_SPEC.topk_precision_ / 100));
      if (topk_final_count_ >= row_count) {
        plan_ctx->set_is_result_accurate(true);
      } else {
        plan_ctx->set_is_result_accurate(false);
      }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
