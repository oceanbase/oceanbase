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

#define USING_LOG_PREFIX SQL
#include "ob_groupby_vec_op.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/aggregate/ob_groupby_op.h"
#include "sql/engine/basic/ob_compact_row.h"

namespace oceanbase
{
namespace sql
{

using namespace oceanbase::common;
using namespace oceanbase::share::schema;

int ObGroupByVecOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_open())) {
    LOG_WARN("failed to inner_open", K(ret));
  } else if (OB_FAIL(aggr_processor_.init())) {
    LOG_WARN("failed to init", K(ret));
  } else {
    LOG_DEBUG("finish inner_open");
  }
  return ret;
}

int ObGroupByVecOp::calculate_3stage_agg_info(char *aggr_row, const RowMeta &row_meta,
                                              const int64_t batch_idx, int32_t &start_agg_id,
                                              int32_t &end_agg_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(aggr_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid row", K(ret));
  } else if (OB_FAIL(calculate_3stage_agg_info(aggregate::Processor::get_groupby_stored_row(
                         row_meta,
                         aggr_row), row_meta, batch_idx, start_agg_id, end_agg_id))) {
    LOG_WARN("failed to calc 3 stage info", K(ret));
  }
  return ret;
}

int ObGroupByVecOp::calculate_3stage_agg_info(const ObCompactRow &row, const RowMeta &row_meta,
                                              const int64_t batch_idx, int32_t &start_agg_id,
                                              int32_t &end_agg_id)
{
  int ret = OB_SUCCESS;
  ObGroupBySpec *op_spec = static_cast<ObGroupBySpec*>(const_cast<ObOpSpec*>(&spec_));
  CK(OB_NOT_NULL(op_spec));
  start_agg_id = 0;
  end_agg_id = op_spec->aggr_infos_.count();
  if (OB_FAIL(ret)) {
  } else if (op_spec->aggr_stage_ == ObThreeStageAggrStage::NONE_STAGE) {
    // do nothing
  } else {
    if (OB_ISNULL(op_spec->aggr_code_expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: aggr_code_expr is null in three stage aggregation", K(ret));
    } else if (OB_UNLIKELY(ObThreeStageAggrStage::FIRST_STAGE != op_spec->aggr_stage_
               && 0 == op_spec->dist_aggr_group_idxes_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status:  distinct aggregation group is 0", K(ret),
               K(op_spec->aggr_stage_), K(op_spec->dist_aggr_group_idxes_.count()));
    } else {
      int64_t aggr_code_idx = op_spec->aggr_code_idx_;
      ObThreeStageAggrStage aggr_stage = op_spec->aggr_stage_;
      ObExpr *aggr_code_expr = op_spec->aggr_code_expr_;
      int64_t distinct_aggr_count = op_spec->dist_aggr_group_idxes_.count();
      int64_t aggr_code = distinct_aggr_count;
      if (ObThreeStageAggrStage::THIRD_STAGE == aggr_stage) {
        if (OB_ISNULL(aggr_code_expr->get_vector(eval_ctx_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid null aggr code vector", K(ret));
        } else {
          aggr_code = aggr_code_expr->get_vector(eval_ctx_)->get_int(batch_idx);
          if (aggr_code < 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid aggr code", K(aggr_code), K(ret), K(get_spec().id_));
          } else if (aggr_code < distinct_aggr_count) {
            start_agg_id = (aggr_code == 0 ? 0 : op_spec->dist_aggr_group_idxes_.at(aggr_code - 1));
            end_agg_id = op_spec->dist_aggr_group_idxes_.at(aggr_code);
          } else {
            start_agg_id = op_spec->dist_aggr_group_idxes_.at(distinct_aggr_count - 1);
          }
        }
      } else if (OB_INVALID_INDEX_INT64 == aggr_code_idx) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status: aggr_code_idx is invalid", K(ret));
      } else if (ObThreeStageAggrStage::SECOND_STAGE == aggr_stage) {
        aggr_code =
          *reinterpret_cast<const int64_t *>(row.get_cell_payload(row_meta, aggr_code_idx));
      }
      if (OB_FAIL(ret) || aggr_stage == ObThreeStageAggrStage::THIRD_STAGE) {
      } else if (aggr_code < distinct_aggr_count) {
        if (ObThreeStageAggrStage::FIRST_STAGE == aggr_stage) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected status: first stage need to calc", K(ret));
        } else {
          // distinct aggregate function on first stage and second stage
          start_agg_id = (0 == aggr_code ? 0 : op_spec->dist_aggr_group_idxes_.at(aggr_code - 1));
          end_agg_id = op_spec->dist_aggr_group_idxes_.at(aggr_code);
        }
      } else if (aggr_code != distinct_aggr_count) {
         ret = OB_ERR_UNEXPECTED;
         LOG_WARN("unexpected status: aggr_code is invalid", K(ret), K(aggr_code),
                  K(distinct_aggr_count), K(aggr_stage));
      } else {
        start_agg_id = ObThreeStageAggrStage::SECOND_STAGE != aggr_stage ? 0 : aggr_code;
        start_agg_id =
          (0 == start_agg_id ? 0 : op_spec->dist_aggr_group_idxes_.at(start_agg_id - 1));
      }
    }
  }
  LOG_DEBUG("stage info calculated", K(ret), K(start_agg_id), K(end_agg_id),
            K(op_spec->aggr_stage_));
  return ret;
}

int ObGroupByVecOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  aggr_processor_.reuse();
  if (OB_FAIL(ObOperator::inner_rescan())) {
    LOG_WARN("failed to rescan", K(ret));
  } else if (OB_FAIL(aggr_processor_.init())) {
    LOG_WARN("failed to init", K(ret));
  } else {
    LOG_DEBUG("finish rescan");
  }
  return ret;
}

int ObGroupByVecOp::inner_switch_iterator()
{
  int ret = OB_SUCCESS;
  aggr_processor_.reuse();
  if (OB_FAIL(ObOperator::inner_switch_iterator())) {
    LOG_WARN("failed to switch_iterator", K(ret));
  } else if (OB_FAIL(aggr_processor_.init())) {
    LOG_WARN("failed to init", K(ret));
  } else {
    LOG_DEBUG("finish switch_iterator");
  }
  return ret;
}

int ObGroupByVecOp::inner_close()
{
  int ret = OB_SUCCESS;
  aggr_processor_.reuse();
  if (OB_FAIL(ObOperator::inner_close())) {
    LOG_WARN("failed to inner_close", K(ret));
  } else {
    LOG_DEBUG("finish inner_close");
  }
  return ret;
}

void ObGroupByVecOp::destroy()
{
  aggr_processor_.destroy();
  ObOperator::destroy();
}

} // end sql
} // end namespace