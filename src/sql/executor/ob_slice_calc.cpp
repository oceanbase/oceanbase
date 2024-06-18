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

#define USING_LOG_PREFIX SQL_EXE

#include "sql/engine/px/ob_px_util.h"
#include "sql/executor/ob_slice_calc.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_calc_partition_id.h"
#include "sql/engine/px/exchange/ob_transmit_op.h"
#include "share/schema/ob_table_schema.h"
#include "common/row/ob_row.h"
#include "lib/ob_define.h"
#include "share/schema/ob_part_mgr_util.h"
#include "sql/engine/px/ob_px_sqc_handler.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

#define DEFINE_GET_SLICE_FUNCTION(type) \
template int ObSliceIdxCalc::get_slice_indexes<ObSliceIdxCalc::type, true>(const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx, SliceIdxArray &slice_idx_array, ObBitVector *skip);  \
template int ObSliceIdxCalc::get_slice_indexes<ObSliceIdxCalc::type, false>(const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx, SliceIdxArray &slice_idx_array, ObBitVector *skip); \
template int ObSliceIdxCalc::get_slice_idx_batch<ObSliceIdxCalc::type, true>(const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx, ObBitVector &skip, const int64_t batch_size, int64_t *&indexes); \
template int ObSliceIdxCalc::get_slice_idx_batch<ObSliceIdxCalc::type, false>(const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx, ObBitVector &skip, const int64_t batch_size, int64_t *&indexes);

DEFINE_GET_SLICE_FUNCTION(ALL_TO_ONE);
DEFINE_GET_SLICE_FUNCTION(SM_REPART_RANDOM);
DEFINE_GET_SLICE_FUNCTION(SM_REPART_HASH);
DEFINE_GET_SLICE_FUNCTION(SM_REPART_RANGE);
DEFINE_GET_SLICE_FUNCTION(AFFINITY_REPART);
DEFINE_GET_SLICE_FUNCTION(NULL_AWARE_AFFINITY_REPART);
DEFINE_GET_SLICE_FUNCTION(SM_BROADCAST);
DEFINE_GET_SLICE_FUNCTION(BC2HOST);
DEFINE_GET_SLICE_FUNCTION(RANDOM);
DEFINE_GET_SLICE_FUNCTION(BROADCAST);
DEFINE_GET_SLICE_FUNCTION(RANGE);
DEFINE_GET_SLICE_FUNCTION(HASH);
DEFINE_GET_SLICE_FUNCTION(NULL_AWARE_HASH);
DEFINE_GET_SLICE_FUNCTION(HYBRID_HASH_BROADCAST);
DEFINE_GET_SLICE_FUNCTION(HYBRID_HASH_RANDOM);
DEFINE_GET_SLICE_FUNCTION(WF_HYBRID);

#define CALL_GET_SLICE_INNER(TYPE, CALC, FUNC, args...) \
  case TYPE : { \
    ret = static_cast<CALC *>(this)->FUNC<USE_VEC>(args); \
    break;  \
  }

template <ObSliceIdxCalc::SliceCalcType CALC_TYPE, bool USE_VEC>
int ObSliceIdxCalc::get_slice_indexes(
  const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx, SliceIdxArray &slice_idx_array, ObBitVector *skip)
{
  int ret = OB_SUCCESS;
  switch(CALC_TYPE) {
    CALL_GET_SLICE_INNER(ALL_TO_ONE, ObAllToOneSliceIdxCalc, get_slice_indexes_inner, exprs, eval_ctx, slice_idx_array, skip)
    CALL_GET_SLICE_INNER(SM_REPART_RANDOM, ObSlaveMapPkeyRandomIdxCalc, get_slice_indexes_inner, exprs, eval_ctx, slice_idx_array, skip)
    CALL_GET_SLICE_INNER(SM_REPART_HASH, ObSlaveMapPkeyHashIdxCalc, get_slice_indexes_inner, exprs, eval_ctx, slice_idx_array, skip)
    CALL_GET_SLICE_INNER(SM_REPART_RANGE, ObSlaveMapPkeyRangeIdxCalc, get_slice_indexes_inner, exprs, eval_ctx, slice_idx_array, skip)
    CALL_GET_SLICE_INNER(AFFINITY_REPART, ObAffinitizedRepartSliceIdxCalc, get_slice_indexes_inner, exprs, eval_ctx, slice_idx_array, skip)
    CALL_GET_SLICE_INNER(NULL_AWARE_AFFINITY_REPART, ObNullAwareAffinitizedRepartSliceIdxCalc, get_slice_indexes_inner, exprs, eval_ctx, slice_idx_array, skip)
    CALL_GET_SLICE_INNER(SM_BROADCAST, ObSlaveMapBcastIdxCalc, get_slice_indexes_inner, exprs, eval_ctx, slice_idx_array, skip)
    CALL_GET_SLICE_INNER(BC2HOST, ObBc2HostSliceIdCalc, get_slice_indexes_inner, exprs, eval_ctx, slice_idx_array, skip)
    CALL_GET_SLICE_INNER(RANDOM, ObRandomSliceIdCalc, get_slice_indexes_inner, exprs, eval_ctx, slice_idx_array, skip)
    CALL_GET_SLICE_INNER(BROADCAST, ObBroadcastSliceIdCalc, get_slice_indexes_inner, exprs, eval_ctx, slice_idx_array, skip)
    CALL_GET_SLICE_INNER(RANGE, ObRangeSliceIdCalc, get_slice_indexes_inner, exprs, eval_ctx, slice_idx_array, skip)
    CALL_GET_SLICE_INNER(HASH, ObHashSliceIdCalc, get_slice_indexes_inner, exprs, eval_ctx, slice_idx_array, skip)
    CALL_GET_SLICE_INNER(NULL_AWARE_HASH, ObNullAwareHashSliceIdCalc, get_slice_indexes_inner, exprs, eval_ctx, slice_idx_array, skip)
    CALL_GET_SLICE_INNER(HYBRID_HASH_BROADCAST, ObHybridHashBroadcastSliceIdCalc, get_slice_indexes_inner, exprs, eval_ctx, slice_idx_array, skip)
    CALL_GET_SLICE_INNER(HYBRID_HASH_RANDOM, ObHybridHashRandomSliceIdCalc, get_slice_indexes_inner, exprs, eval_ctx, slice_idx_array, skip)
    CALL_GET_SLICE_INNER(WF_HYBRID, ObWfHybridDistSliceIdCalc, get_slice_indexes_inner, exprs, eval_ctx, slice_idx_array, skip)
    default : {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected slice calc type", K(CALC_TYPE));
    }
  }
  return ret;
}

template <ObSliceIdxCalc::SliceCalcType CALC_TYPE, bool USE_VEC>
int ObSliceIdxCalc::get_slice_idx_batch(const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx,
                              ObBitVector &skip, const int64_t batch_size,
                              int64_t *&indexes)
{
  int ret = OB_SUCCESS;
  switch(CALC_TYPE) {
    CALL_GET_SLICE_INNER(ALL_TO_ONE, ObAllToOneSliceIdxCalc, get_slice_idx_batch_inner, exprs, eval_ctx, skip, batch_size, indexes)
    CALL_GET_SLICE_INNER(AFFINITY_REPART, ObAffinitizedRepartSliceIdxCalc, get_slice_idx_batch_inner, exprs, eval_ctx, skip, batch_size, indexes)
    CALL_GET_SLICE_INNER(RANDOM, ObRandomSliceIdCalc, get_slice_idx_batch_inner, exprs, eval_ctx, skip, batch_size, indexes)
    CALL_GET_SLICE_INNER(RANGE, ObRangeSliceIdCalc, get_slice_idx_batch_inner, exprs, eval_ctx, skip, batch_size, indexes)
    CALL_GET_SLICE_INNER(HASH, ObHashSliceIdCalc, get_slice_idx_batch_inner, exprs, eval_ctx, skip, batch_size, indexes)
    CALL_GET_SLICE_INNER(HYBRID_HASH_RANDOM, ObHybridHashRandomSliceIdCalc, get_slice_idx_batch_inner, exprs, eval_ctx, skip, batch_size, indexes)
    default : {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected slice calc type", K(CALC_TYPE));
    }
  }
  return ret;
}

int ObSliceIdxCalc::get_previous_row_tablet_id(ObObj &tablet_id)
{
  int ret = OB_NOT_SUPPORTED;
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "get previous tablet id");
  UNUSED(tablet_id);
  return ret;
}

int ObSliceIdxCalc::setup_slice_index(SliceIdxArray &slice_idx_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(slice_idx_array.count() != 1)) {
    slice_idx_array.reuse();
    if (OB_FAIL(slice_idx_array.push_back(0))) {
      LOG_WARN("array push back failed", K(ret));
    }
  }
  return ret;
}

int ObSliceIdxCalc::setup_slice_indexes(ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (NULL == slice_indexes_) {
    slice_indexes_ = static_cast<int64_t *>(alloc_.alloc(
            sizeof(*slice_indexes_)
            * ctx.max_batch_size_));
    if (NULL == slice_indexes_) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    }
  }
  return ret;
}

int ObSliceIdxCalc::setup_tablet_ids(ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tablet_ids_)) {
    tablet_ids_ = static_cast<int64_t *> (alloc_.alloc(sizeof(*tablet_ids_)
                                             * ctx.max_batch_size_));
    if (OB_ISNULL(tablet_ids_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    }
  }
  return ret;
}

template <>
int ObSliceIdxCalc::calc_for_null_aware<false>(const ObExpr &expr, const int64_t task_cnt,
                                        ObEvalCtx &eval_ctx, SliceIdxArray &slice_idx_array,
                                        bool &processed, ObBitVector *skip)
{
  int ret = OB_SUCCESS;
  processed = false;
  ObDatum *dis_key = nullptr;
  if (is_first_row_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < task_cnt; ++i) {
      if (OB_FAIL(slice_idx_array.push_back(i))) {
        LOG_WARN("failed to push back i", K(ret));
      }
    }
    is_first_row_ = false;
    processed = true;
  } else if (OB_FAIL(expr.eval(eval_ctx, dis_key))) {
    LOG_WARN("failed to eval repartition expr", K(ret));
  } else if (dis_key->is_null()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < task_cnt; ++i) {
      if (OB_FAIL(slice_idx_array.push_back(i))) {
        LOG_WARN("failed to push back i", K(ret));
      }
    }
    processed = true;
  }
  return ret;
}

template <>
int ObSliceIdxCalc::calc_for_null_aware<true>(const ObExpr &expr, const int64_t task_cnt,
                                        ObEvalCtx &eval_ctx, SliceIdxArray &slice_idx_array,
                                        bool &processed, ObBitVector *skip)
{
  int ret = OB_SUCCESS;
  processed = false;
  ObDatum *dis_key = nullptr;
  if (is_first_row_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < task_cnt; ++i) {
      if (OB_FAIL(slice_idx_array.push_back(i))) {
        LOG_WARN("failed to push back i", K(ret));
      }
    }
    is_first_row_ = false;
    processed = true;
  } else if (OB_ISNULL(skip)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("skip is null", K(ret));
  } else {
    const bool all_rows_active = false;
    const int64_t batch_idx = eval_ctx.get_batch_idx();
    EvalBound eval_bound(eval_ctx.get_batch_size(), batch_idx, batch_idx + 1, all_rows_active);
    if (OB_FAIL(expr.eval_vector(eval_ctx, *skip, eval_bound))) {
      LOG_WARN("eval batch failed", K(ret));
    } else {
      ObIVector *vec = expr.get_vector(eval_ctx);
      if (vec->is_null(batch_idx)) {
        for (int64_t i = 0; OB_SUCC(ret) && i < task_cnt; ++i) {
          if (OB_FAIL(slice_idx_array.push_back(i))) {
            LOG_WARN("failed to push back i", K(ret));
          }
        }
        processed = true;
      }
    }
  }
  return ret;
}

//TODO yishen
int ObRepartSliceIdxCalc::get_part_id_by_one_level_sub_ch_map(int64_t &part_id)
{
  int ret = OB_SUCCESS;
  if (px_repart_ch_map_.size() > 0) {
    ObTabletID tablet_id(px_repart_ch_map_.begin()->first);
    int64_t subpart_id = OB_INVALID_ID;
    if (OB_FAIL(table_schema_.get_part_id_by_tablet(tablet_id, part_id, subpart_id))) {
      LOG_WARN("fail to get part id by tablet", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ch map", K(ret));
  }
  return ret;
}

int ObRepartSliceIdxCalc::get_sub_part_id_by_one_level_first_ch_map(
    const int64_t part_id, int64_t &tablet_id)
{
  int ret = OB_SUCCESS;
  if (part2tablet_id_map_.size() > 0) {
    if (OB_FAIL(part2tablet_id_map_.get_refactored(part_id, tablet_id))) {
      LOG_WARN("fail to get tablet id", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ch map", K(ret));
  }
  return ret;
}

template <>
int ObRepartSliceIdxCalc::get_tablet_id<false>(ObEvalCtx &eval_ctx, int64_t &tablet_id, ObBitVector *skip)
{
  int ret = OB_SUCCESS;
  ObDatum *tablet_id_datum = NULL;
  if (OB_ISNULL(calc_part_id_expr_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(calc_part_id_expr_), K(ret));
  } else if (OB_FAIL(calc_part_id_expr_->eval(eval_ctx, tablet_id_datum))) {
    LOG_WARN("fail to calc part id", K(ret), K(*calc_part_id_expr_));
  } else if (ObExprCalcPartitionId::NONE_PARTITION_ID ==
                             (tablet_id = tablet_id_datum->get_int())) {
    // Usually, calc_part_id_expr_ returns tablet_id and set tablet_id_datum = 0
    // if no partition match(refer to ObExprCalcPartitionBase::calc_partition_id).
    // In this condition, it will not go to this branch because NONE_PARTITION_ID equals to -1.
    // But when repart_type_ equals to OB_REPARTITION_ONE_SIDE_ONE_LEVEL_FIRST(means value of sub part key is fixed),
    // calc_part_id_expr_ returns partition_id of first part and set tablet_id_datum = -1.
  } else if (OB_REPARTITION_ONE_SIDE_ONE_LEVEL_FIRST == repart_type_) {
    int64_t part_id = tablet_id;
    if (OB_FAIL(get_sub_part_id_by_one_level_first_ch_map(part_id, tablet_id))) {
      LOG_WARN("fail to get part id by ch map", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    tablet_id_ = tablet_id;
  }
  LOG_DEBUG("repart partition id", K(tablet_id));

  return ret;
}

template <>
int ObRepartSliceIdxCalc::get_tablet_id<true>(ObEvalCtx &eval_ctx, int64_t &tablet_id, ObBitVector *skip)
{
  int ret = OB_SUCCESS;
  bool all_rows_active = false;
  int64_t batch_idx = eval_ctx.get_batch_idx();
  int64_t batch_size = eval_ctx.get_batch_size();
  EvalBound eval_bound(batch_size, batch_idx, batch_idx + 1, all_rows_active);
  if (OB_ISNULL(calc_part_id_expr_) || OB_ISNULL(skip)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(calc_part_id_expr_));
  } else if (OB_FAIL(calc_part_id_expr_->eval_vector(eval_ctx, *skip, eval_bound))) {
    LOG_WARN("eval failed", K(ret));
  } else {
    ObIVector *vec = calc_part_id_expr_->get_vector(eval_ctx);
    tablet_id = vec->get_int(batch_idx);
  }
  if (OB_FAIL(ret)) {
  } else if (ObExprCalcPartitionId::NONE_PARTITION_ID == tablet_id) {
    // Usually, calc_part_id_expr_ returns tablet_id and set tablet_id_datum = 0
    // if no partition match(refer to ObExprCalcPartitionBase::calc_partition_id).
    // In this condition, it will not go to this branch because NONE_PARTITION_ID equals to -1.
    // But when repart_type_ equals to OB_REPARTITION_ONE_SIDE_ONE_LEVEL_FIRST(means value of sub part key is fixed),
    // calc_part_id_expr_ returns partition_id of first part and set tablet_id_datum = -1.
  } else if (OB_REPARTITION_ONE_SIDE_ONE_LEVEL_FIRST == repart_type_) {
    int64_t part_id = tablet_id;
    if (OB_FAIL(get_sub_part_id_by_one_level_first_ch_map(part_id, tablet_id))) {
      LOG_WARN("fail to get part id by ch map", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    tablet_id_ = tablet_id;
  }
  LOG_DEBUG("repart partition id", K(tablet_id));

  return ret;
}

template <>
int ObRepartSliceIdxCalc::get_tablet_ids<false>(ObEvalCtx &eval_ctx, ObBitVector &skip,
                                         const int64_t batch_size, int64_t *&tablet_ids)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(calc_part_id_expr_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid calc part id expr", K(ret));
  } else if (OB_FAIL(calc_part_id_expr_->eval_batch(eval_ctx, skip, batch_size))) {
    LOG_WARN("failed to eval batch", K(ret));
  } else if (OB_FAIL(ObSliceIdxCalc::setup_tablet_ids(eval_ctx))) {
    LOG_WARN("failed to setup partition id", K(ret));
  } else {
    ObDatumVector tablet_id_datums = calc_part_id_expr_->locate_expr_datumvector(eval_ctx);
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
      if (skip.at(i)) {
        continue;
      }
      if (ObExprCalcPartitionId::NONE_PARTITION_ID ==
          (ObSliceIdxCalc::tablet_ids_[i] = tablet_id_datums.at(i)->get_int())) {
        // do nothing
      } else if (OB_REPARTITION_ONE_SIDE_ONE_LEVEL_FIRST == repart_type_) {
        int64_t part_id = ObSliceIdxCalc::tablet_ids_[i];
        if (OB_FAIL(get_sub_part_id_by_one_level_first_ch_map(part_id, ObSliceIdxCalc::tablet_ids_[i]))) {
          LOG_WARN("fail to get part id by ch map", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        tablet_id_ = tablet_ids[i];
      }
    }
    if (OB_SUCC(ret)) {
      tablet_ids = ObSliceIdxCalc::tablet_ids_;
    }
  }
  return ret;
}

template <>
int ObRepartSliceIdxCalc::get_tablet_ids<true>(ObEvalCtx &eval_ctx, ObBitVector &skip,
                                         const int64_t batch_size, int64_t *&tablet_ids)
{
  int ret = OB_SUCCESS;
  const bool all_rows_active = false;
  EvalBound eval_bound(batch_size, all_rows_active);
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(calc_part_id_expr_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid calc part id expr", K(ret));
  } else if (OB_FAIL(calc_part_id_expr_->eval_vector(eval_ctx, skip, eval_bound))) {
    LOG_WARN("failed to eval batch", K(ret));
  } else if (OB_FAIL(ObSliceIdxCalc::setup_tablet_ids(eval_ctx))) {
    LOG_WARN("failed to setup partition id", K(ret));
  } else {
    ObIVector *vec = calc_part_id_expr_->get_vector(eval_ctx);
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
      if (skip.at(i)) {
        continue;
      }
      //todo shanting. opt. avoid virtual func cost
      if (ObExprCalcPartitionId::NONE_PARTITION_ID ==
          (ObSliceIdxCalc::tablet_ids_[i] = vec->get_int(i))) {
        // do nothing
      } else if (OB_REPARTITION_ONE_SIDE_ONE_LEVEL_FIRST == repart_type_) {
        int64_t part_id = ObSliceIdxCalc::tablet_ids_[i];
        if (OB_FAIL(get_sub_part_id_by_one_level_first_ch_map(part_id, ObSliceIdxCalc::tablet_ids_[i]))) {
          LOG_WARN("fail to get part id by ch map", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        tablet_id_ = tablet_ids_[i];
      }
    }
    if (OB_SUCC(ret)) {
      tablet_ids = ObSliceIdxCalc::tablet_ids_;
    }
  }
  return ret;
}

int ObRepartSliceIdxCalc::get_previous_row_tablet_id(ObObj &tablet_id)
{
  int ret = OB_SUCCESS;
  tablet_id.set_int(tablet_id_);
  return ret;
}

int ObSlaveMapRepartIdxCalcBase::init(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObRepartSliceIdxCalc::init(tenant_id))) {
    LOG_WARN("fail init base", K(ret));
  }
  // 在pkey random情况下，一个partition是可以被其所在的SQC上的所有worker处理的，
  // 所以一个tablet_id可能会对应多个task idx，
  // 形成 tablet_id -> task_idx_list的映射关系
  // 例如：SQC1有3 worker(task1,task2,task3)，2个partition(p0,p1);
  // SQC2有2 worker(task4，task5)，1个partition(p2);
  // 就会形成：
  // p0 : [task1,task2,task3]
  // p1 : [task1,task2,task3]
  // p2 : [task4,task5]
  const ObPxPartChMapTMArray &part_ch_array = part_ch_info_.part_ch_array_;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(part_to_task_array_map_.create(max(1, part_ch_array.count()),
                                               ObModIds::OB_SQL_PX,
                                               ObModIds::OB_HASH_NODE,
                                               tenant_id))) {
      LOG_WARN("fail create part to task array map", "count", part_ch_array.count(), K(ret));
    } else {
      // In ObRepartSliceIdxCalc::init(), the support_vectorized_calc_ has been set to true.
      support_vectorized_calc_ = false;
    }
  }

  // TODO: jiangting.lk
  // 这里可以再进行优化，节约空间，将相同SQC的partition使用同一个task idx array
  ARRAY_FOREACH_X(part_ch_array, idx, cnt, OB_SUCC(ret)) {
    int64_t tablet_id = part_ch_array.at(idx).first_;
    int64_t task_idx = part_ch_array.at(idx).second_;
    LOG_DEBUG("get one channel relationship", K(idx), K(cnt), "key", tablet_id, "val", task_idx);

    if (OB_ISNULL(part_to_task_array_map_.get(tablet_id))) {
      TaskIdxArray task_idx_array;
      if (OB_FAIL(part_to_task_array_map_.set_refactored(tablet_id, task_idx_array))) {
        LOG_WARN("push partition id and task idx array to map failed", K(ret), K(tablet_id));
      } else {
        LOG_TRACE("add task idx array successfully", K(ret), K(tablet_id));
      }
    }

    const TaskIdxArray *task_idx_array = part_to_task_array_map_.get(tablet_id);
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(task_idx_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task idx list is null", K(ret), K(tablet_id));
    } else if (OB_FAIL(const_cast<TaskIdxArray *>(task_idx_array)->push_back(task_idx))) {
      LOG_WARN("failed push back task idx to task idx array",
          K(ret), K(task_idx), K(tablet_id));
    } else {
      LOG_TRACE("push task idx to task idx array",
        K(tablet_id), K(task_idx), K(*task_idx_array), K(task_idx_array->count()));
    }
  }
  return ret;
}

int ObSlaveMapRepartIdxCalcBase::destroy()
{
  int ret = OB_SUCCESS;
  if (part_to_task_array_map_.created()) {
    ret = part_to_task_array_map_.destroy();
  }
  int tmp_ret = ObRepartSliceIdxCalc::destroy();
  ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
  return ret;
}

int ObSlaveMapPkeyRandomIdxCalc::init(uint64_t tenant_id)
{
  return ObSlaveMapRepartIdxCalcBase::init(tenant_id);
}

int ObSlaveMapPkeyRandomIdxCalc::destroy()
{
  return ObSlaveMapRepartIdxCalcBase::destroy();
}

template <bool USE_VEC>
int ObSlaveMapPkeyRandomIdxCalc::get_slice_indexes_inner(const ObIArray<ObExpr*> &exprs,
                                                  ObEvalCtx &eval_ctx,
                                                  SliceIdxArray &slice_idx_array,
                                                  ObBitVector *skip)
{
  int ret = OB_SUCCESS;
  UNUSED(exprs);
  // 计算过程：
  // 1. 通过row计算出对应的partition id
  // 2. 通过partition id，找到对应的task array
  // 3. random的方式从task array中选择task idx作为slice idx
  int64_t tablet_id = OB_INVALID_INDEX;
  if (OB_FAIL(setup_slice_index(slice_idx_array))) {
    LOG_WARN("set slice index failed", K(ret));
  } else if (part_ch_info_.part_ch_array_.size() <= 0) {
    // 表示没有 partition到task idx的映射
    ret = OB_NOT_INIT;
    LOG_WARN("the size of part task channel map is zero", K(ret));
  } else if (OB_FAIL(ObRepartSliceIdxCalc::get_tablet_id<USE_VEC>(eval_ctx, tablet_id, skip))) {
    LOG_WARN("failed to get partition id", K(ret));
  } else if (OB_FAIL(get_task_idx_by_tablet_id(tablet_id, slice_idx_array.at(0)))) {
    if (OB_HASH_NOT_EXIST == ret) {
      if (tablet_id <= 0) {
        // tablet_id <= means this row matches no partition
        ret = OB_NO_PARTITION_FOR_GIVEN_VALUE;
      } else {
        // there are two scenarios tablet_id > 0.
        // 1. insert into t partition (p0) select * from t partition (p1).
        //    tablet_id equals to tablet id of p1 but the map only contains tablet id of p0.
        // 2. insert into t and truncate t concurrently. truncate t will make t maps to a new group of tablets.
        // It's hard to distinct these two scenarios, so we report OB_SCHEMA_ERROR
        //    and record error msg of OB_NO_PARTITION_FOR_GIVEN_VALUE.
        // As a result, if schema has changed, this query will be retried.
        // Otherwise, error msg of OB_NO_PARTITION_FOR_GIVEN_VALUE will be reported to the client.
        ret = OB_SCHEMA_ERROR;
        LOG_USER_ERROR(OB_NO_PARTITION_FOR_GIVEN_VALUE);
      }
      LOG_WARN("can't get the right partition", K(ret), K(tablet_id), K(slice_idx_array.at(0)), K(repart_type_));
    }
  }
  return ret;
}

template <bool USE_VEC>
int ObSlaveMapPkeyRandomIdxCalc::get_slice_idx_batch_inner(const ObIArray<ObExpr*> &, ObEvalCtx &eval_ctx,
                                                           ObBitVector &skip, const int64_t batch_size,
                                                           int64_t *&indexes)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(setup_slice_indexes(eval_ctx))) {
    LOG_WARN("setup slice indexes failed", K(ret));
  } else if (part_ch_info_.part_ch_array_.size() <= 0) {
    // 表示没有 partition到task idx的映射
    ret = OB_NOT_INIT;
    LOG_WARN("the size of part task channel map is zero", K(ret));
  } else if (OB_FAIL(ObRepartSliceIdxCalc::get_tablet_ids<USE_VEC>(eval_ctx, skip,
                                                                   batch_size, tablet_ids_))) {
    LOG_WARN("get tablet ids failed", K(ret));
  } else {
    for (int64_t i = 0; i < batch_size && OB_SUCC(ret); i++) {
      if (OB_FAIL(get_task_idx_by_tablet_id(tablet_ids_[i], slice_indexes_[i]))) {
        if (OB_HASH_NOT_EXIST == ret) {
          if (tablet_ids_[i] <= 0) {
            ret = OB_NO_PARTITION_FOR_GIVEN_VALUE;
          } else {
            ret = OB_SCHEMA_ERROR;
            LOG_USER_ERROR(OB_NO_PARTITION_FOR_GIVEN_VALUE);
          }
          LOG_WARN("can't get the right partition", K(ret), K(tablet_ids_[i]), K(repart_type_));
        }
      }
    }
  }
  indexes = slice_indexes_;
  return ret;
}

int ObSlaveMapPkeyRandomIdxCalc::get_task_idx_by_tablet_id(int64_t tablet_id,
                                                          int64_t &task_idx)
{
  int ret = OB_SUCCESS;
  if (ObSlaveMapRepartIdxCalcBase::part_to_task_array_map_.size() <= 0) {
    ret = OB_NOT_INIT;
    LOG_WARN("part to task array is not inited", K(ret));
  } else {
    const ObSlaveMapRepartIdxCalcBase::TaskIdxArray *task_idx_array = ObSlaveMapRepartIdxCalcBase::part_to_task_array_map_.get(tablet_id);
    if (OB_ISNULL(task_idx_array)) {
      ret = OB_HASH_NOT_EXIST; // convert to hash error
      LOG_WARN("the task idx array is null", K(ret), K(tablet_id));
    } else if (task_idx_array->count() <= 0){
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the size of task idx array is zero", K(ret));
    } else {
      // random的方式从 task idx array中找到结果
      static const int64_t min = 0;
      static const int64_t max = INT64_MAX;
      int64_t rand_idx = common::ObRandom::rand(min, max) % task_idx_array->count();
      task_idx = task_idx_array->at(rand_idx);
      LOG_TRACE("get task_idx/slice_idx by random way", K(tablet_id), K(task_idx));
    }
  }
  return ret;
}

template <bool USE_VEC>
int ObAffinitizedRepartSliceIdxCalc::get_slice_indexes_inner(const ObIArray<ObExpr*> &exprs,
                                                       ObEvalCtx &eval_ctx,
                                                       SliceIdxArray &slice_idx_array,
                                                       ObBitVector *skip)
{
  int ret = OB_SUCCESS;
  UNUSED(exprs);
  int64_t tablet_id = OB_INVALID_INDEX;
  if (OB_FAIL(setup_slice_index(slice_idx_array))) {
    LOG_WARN("set slice index failed", K(ret));
  } else if (task_count_ <= 0) {
    ret = OB_NOT_INIT;
    LOG_WARN("task_count not inited", K_(task_count), K(ret));
  } else if (px_repart_ch_map_.size() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid map size, affinity map should not be empty!", K_(task_count), K(ret));
  } else if (OB_FAIL(ObRepartSliceIdxCalc::get_tablet_id<USE_VEC>(eval_ctx, tablet_id, skip))) {
    LOG_WARN("fail to get partition id", K(ret));
  } else if (OB_FAIL(px_repart_ch_map_.get_refactored(tablet_id, slice_idx_array.at(0)))) {
    if (OB_HASH_NOT_EXIST == ret && unmatch_row_dist_method_ == ObPQDistributeMethod::DROP) {
      slice_idx_array.at(0) = ObSliceIdxCalc::DEFAULT_CHANNEL_IDX_TO_DROP_ROW;
      ret = OB_SUCCESS;
    } else if (OB_HASH_NOT_EXIST == ret && unmatch_row_dist_method_ == ObPQDistributeMethod::RANDOM) {
      slice_idx_array.at(0) = round_robin_idx_;
      round_robin_idx_++;
      round_robin_idx_ = round_robin_idx_ % task_count_;
      ret = OB_SUCCESS;
    } else if (OB_HASH_NOT_EXIST == ret && unmatch_row_dist_method_ == ObPQDistributeMethod::HASH) {
      ObHashSliceIdCalc slice_id_calc(exec_ctx_.get_allocator(), task_count_,
                                      ObNullDistributeMethod::NONE, hash_dist_exprs_, hash_funcs_);
      int64_t task_idx = 0;
      if (OB_ISNULL(hash_dist_exprs_) || OB_UNLIKELY(0 == hash_dist_exprs_->count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("hash dist exprs is null", K(ret));
      } else if (OB_FAIL(slice_id_calc.get_slice_indexes_inner<USE_VEC>(*hash_dist_exprs_,
                                                                      eval_ctx, slice_idx_array,
                                                                      skip))) {
        LOG_WARN("hash slice calc get slice idx failed", K(ret));
      }
    } else {
      LOG_WARN("fail get affinitized taskid", K(ret), K(tablet_id),
          K_(task_count), K_(unmatch_row_dist_method));
    }
  }

  return ret;
}

template <bool USE_VEC>
int ObAffinitizedRepartSliceIdxCalc::get_slice_idx_batch_inner(const ObIArray<ObExpr*> &exprs,
                                                       ObEvalCtx &eval_ctx,
                                                       ObBitVector &skip,
                                                       const int64_t batch_size,
                                                       int64_t *&indexes)
{
  int ret = OB_SUCCESS;
  int64_t tablet_id = OB_INVALID_INDEX;
  if (task_count_ <= 0) {
    ret = OB_NOT_INIT;
    LOG_WARN("task_count not inited", K_(task_count), K(ret));
  } else if (px_repart_ch_map_.size() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid map size, affinity map should not be empty!", K_(task_count), K(ret));
  } else if (OB_FAIL(setup_slice_indexes(eval_ctx))) {
    LOG_WARN("failed to set up slice indexes", K(ret));
  } else if (OB_FAIL(setup_tablet_ids(eval_ctx))) {
    LOG_WARN("failed to setup partition ids", K(ret));
  } else if (OB_FAIL(ObRepartSliceIdxCalc::get_tablet_ids<USE_VEC>(eval_ctx, skip,
                                                             batch_size, tablet_ids_))) {
    LOG_WARN("fail to get partition id", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
      if (skip.at(i)) {
        continue;
      }
      if (OB_FAIL(px_repart_ch_map_.get_refactored(tablet_ids_[i], slice_indexes_[i]))) {
        if (OB_HASH_NOT_EXIST == ret && unmatch_row_dist_method_ == ObPQDistributeMethod::DROP) {
          slice_indexes_[i] = ObSliceIdxCalc::DEFAULT_CHANNEL_IDX_TO_DROP_ROW;
          ret = OB_SUCCESS;
        } else if (OB_HASH_NOT_EXIST == ret
                   && unmatch_row_dist_method_ == ObPQDistributeMethod::RANDOM) {
          slice_indexes_[i] = round_robin_idx_;
          round_robin_idx_++;
          round_robin_idx_ = round_robin_idx_ % task_count_;
          ret = OB_SUCCESS;
        } else if (OB_HASH_NOT_EXIST == ret
                   && unmatch_row_dist_method_ == ObPQDistributeMethod::HASH) {
          ObHashSliceIdCalc slice_id_calc(exec_ctx_.get_allocator(), task_count_,
                                          null_row_dist_method_,
                                          hash_dist_exprs_, hash_funcs_);
          if (OB_ISNULL(hash_dist_exprs_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("hash dist exprs is null", K(ret));
          } else if (OB_FAIL(slice_id_calc.calc_slice_idx<USE_VEC>(eval_ctx, task_count_,
                                                          slice_indexes_[i], &skip))) {
            LOG_WARN("hash slice calc get slice idx failed", K(ret));
          }
        } else {
          LOG_WARN("fail get affinitized taskid", K(ret), K(tablet_id),
              K_(task_count), K_(unmatch_row_dist_method));
        }
      }
    }
    if (OB_SUCC(ret)) {
      indexes = slice_indexes_;
    }
  }
  return ret;
}

int ObRepartSliceIdxCalc::init(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (px_repart_ch_map_.created()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("this map has been init twice", K(ret));
  } else if (OB_FAIL(build_repart_ch_map(px_repart_ch_map_, tenant_id))) {
    LOG_WARN("failed to build affi hash map", K(ret));
  } else if (OB_FAIL(setup_one_side_one_level_info())) {
    LOG_WARN("fail to build one side on level map", K(ret));
  } else {
    support_vectorized_calc_ = true;
  }
  return ret;
}

int ObRepartSliceIdxCalc::setup_one_side_one_level_info()
{
  int ret = OB_SUCCESS;
  CalcPartitionBaseInfo *calc_part_info = NULL;
  calc_part_info = reinterpret_cast<CalcPartitionBaseInfo *>(calc_part_id_expr_->extra_info_);
  CK(OB_NOT_NULL(calc_part_info));
  CK(px_repart_ch_map_.size() > 0);
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(calc_part_id_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected calc part id expr", K(ret));
  } else if (OB_REPARTITION_ONE_SIDE_ONE_LEVEL_FIRST == repart_type_) {
    calc_part_info->partition_id_calc_type_ = CALC_IGNORE_SUB_PART;
    if (OB_FAIL(build_part2tablet_id_map())) {
      LOG_WARN("fail to build part2tablet id map", K(ret));
    }
  } else if (OB_REPARTITION_ONE_SIDE_ONE_LEVEL_SUB == repart_type_) {
    calc_part_info->partition_id_calc_type_ = CALC_IGNORE_FIRST_PART;
    int64_t first_part_id = OB_INVALID_ID;
    if (OB_FAIL(get_part_id_by_one_level_sub_ch_map(first_part_id))) {
      LOG_WARN("fail to get part id by ch map", K(ret));
    } else {
      calc_part_info->first_part_id_ = first_part_id;
    }
  }
  return ret;
}

int ObRepartSliceIdxCalc::build_repart_ch_map(ObPxPartChMap &affinity_map, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  const ObPxPartChMapTMArray &part_ch_array = part_ch_info_.part_ch_array_;
  if (OB_FAIL(affinity_map.create(max(1, part_ch_array.count()),
                                  ObModIds::OB_SQL_PX,
                                  ObModIds::OB_HASH_NODE,
                                  tenant_id))) {
    LOG_WARN("fail create hashmap", "count", part_ch_array.count(), K(ret));
  }

  int64_t tablet_id = common::OB_INVALID_INDEX_INT64;
  // 将 partition idx => channel idx 的映射 array 转化成 hash map，提高查询效率
  ARRAY_FOREACH_X(part_ch_array, idx, cnt, OB_SUCC(ret)) {
    LOG_DEBUG("map build", K(idx), K(cnt), "key", part_ch_array.at(idx).first_, "val",
        part_ch_array.at(idx).second_);
    if (tablet_id != part_ch_array.at(idx).first_) {
      tablet_id = part_ch_array.at(idx).first_;
      if (OB_FAIL(affinity_map.set_refactored(part_ch_array.at(idx).first_,
                                              part_ch_array.at(idx).second_))) {
        LOG_WARN("fail add item to hash map",
                 K(idx), K(cnt), "key", part_ch_array.at(idx).first_, "val",
                 part_ch_array.at(idx).second_, K(ret));
      }
    } else {
      // skip, same partition id may take more than one entry in part_ch_array.
      // (e.g slave mapping pkey hash)
    }
  }
  return ret;
}

int ObRepartSliceIdxCalc::build_part2tablet_id_map()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(part2tablet_id_map_.create(max(1, px_repart_ch_map_.size()), "PxTabletMap"))) {
    LOG_WARN("fail create hashmap", K(ret));
  } else {
    int64_t part_id = OB_INVALID_ID;
    int64_t subpart_id = OB_INVALID_ID;
    ObTabletID tablet_id;
    for (auto iter = px_repart_ch_map_.begin(); OB_SUCC(ret) && iter != px_repart_ch_map_.end(); ++iter) {
      tablet_id = ObTabletID(iter->first);
      if (OB_FAIL(table_schema_.get_part_id_by_tablet(tablet_id, part_id, subpart_id))) {
        LOG_WARN("get part id by tablet id", K(tablet_id), K(part_id));
      } else if (OB_FAIL(part2tablet_id_map_.set_refactored(part_id, tablet_id.id()))) {
        LOG_WARN("fail to set part id", K(part_id), K(tablet_id));
      }
    }
  }
  return ret;
}

template <bool USE_VEC>
int ObSlaveMapBcastIdxCalc::get_slice_indexes_inner(const ObIArray<ObExpr*> &exprs,
    ObEvalCtx &eval_ctx, SliceIdxArray &slice_idx_array, ObBitVector *skip)
{
  UNUSED(exprs);
  int ret = OB_SUCCESS;
  int64_t tablet_id = common::OB_INVALID_INDEX;
  slice_idx_array.reuse();
  if (OB_FAIL(get_tablet_id<USE_VEC>(eval_ctx, tablet_id, skip))) {
    LOG_WARN("failed to get_tablet_id", K(ret));
  } else if (OB_INVALID_INDEX == tablet_id) {
    int64_t drop_idx = ObSliceIdxCalc::DEFAULT_CHANNEL_IDX_TO_DROP_ROW;
    if (OB_FAIL(slice_idx_array.push_back(drop_idx))) {
      LOG_WARN("failed to push back slice idx", K(ret));
    }
  } else {
    const ObPxPartChMapTMArray &part_ch_array = part_ch_info_.part_ch_array_;
    ARRAY_FOREACH (part_ch_array, idx) {
      if (tablet_id == part_ch_array.at(idx).first_) {
        if (OB_FAIL(slice_idx_array.push_back(part_ch_array.at(idx).second_))) {
          LOG_WARN("failed to push back slice idx", K(ret));
        }
        LOG_DEBUG("find slice id to trans", K(tablet_id), K(part_ch_array.at(idx).second_));
      }
    }
  }
  return ret;
}

template <bool USE_VEC>
int ObAllToOneSliceIdxCalc::get_slice_indexes_inner(const ObIArray<ObExpr*> &exprs,
                                                  ObEvalCtx &eval_ctx,
                                                  SliceIdxArray &slice_idx_array,
                                                  ObBitVector *skip)
{
  UNUSED(exprs);
  UNUSED(eval_ctx);
  UNUSED(skip);
  int ret = OB_SUCCESS;
  if (OB_FAIL(setup_slice_index(slice_idx_array))) {
    LOG_WARN("set slice index failed", K(ret));
  } else {
    slice_idx_array.at(0) = 0;
  }
  return ret;
}

template <bool USE_VEC>
int ObAllToOneSliceIdxCalc::get_slice_idx_batch_inner(const ObIArray<ObExpr*> &,
                                              ObEvalCtx &eval_ctx,
                                              ObBitVector &,
                                              const int64_t,
                                              int64_t *&indexes)
{
  int ret = OB_SUCCESS;
  if (NULL == slice_indexes_) {
    if (OB_FAIL(setup_slice_indexes(eval_ctx))) {
      LOG_WARN("setup slice indexes failed", K(ret));
    } else {
      const int64_t size = sizeof(*slice_indexes_)
          * eval_ctx.max_batch_size_;
      MEMSET(slice_indexes_, 0, size);
    }
  }
  indexes = slice_indexes_;
  return ret;
}

template <bool USE_VEC>
int ObBc2HostSliceIdCalc::get_slice_indexes_inner(
  const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx, SliceIdxArray &slice_idx_array,
  ObBitVector *skip)
{
  UNUSED(exprs);
  UNUSED(eval_ctx);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(slice_idx_array.count() != host_idx_.count())) {
    slice_idx_array.reuse();
    FOREACH_CNT_X(it, host_idx_, OB_SUCC(ret)) {
      UNUSED(it);
      if (OB_FAIL(slice_idx_array.push_back(0))) {
        LOG_WARN("array push back failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; i < host_idx_.count(); i++) {
      auto &hi = host_idx_.at(i);
      int64_t idx = hi.begin_ + hi.idx_ % (hi.end_ - hi.begin_);
      slice_idx_array.at(i) = channel_idx_.at(idx);
      hi.idx_++;
    }
  }
  return ret;
}

template <bool USE_VEC>
int ObRandomSliceIdCalc::get_slice_indexes_inner(
  const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx, SliceIdxArray &slice_idx_array,
  ObBitVector *skip)
{
  UNUSED(exprs);
  UNUSED(eval_ctx);
  int ret = OB_SUCCESS;
  if (OB_FAIL(setup_slice_index(slice_idx_array))) {
    LOG_WARN("set slice index failed", K(ret));
  } else if (slice_cnt_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid slice count", K(ret), K(slice_cnt_));
  } else {
    slice_idx_array.at(0) = idx_ % slice_cnt_;
    idx_++;
  }
  return ret;
}

template <bool USE_VEC>
int ObRandomSliceIdCalc::get_slice_idx_batch_inner(const ObIArray<ObExpr*> &,
                                              ObEvalCtx &eval_ctx,
                                              ObBitVector &skip,
                                              const int64_t batch_size,
                                              int64_t *&indexes)
{
  int ret = OB_SUCCESS;
  if (slice_cnt_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid slice count", K(ret), K(slice_cnt_));
  } else if (OB_FAIL(setup_slice_indexes(eval_ctx))) {
    LOG_WARN("setup slice indexes failed", K(ret));
  } else {
    for (int64_t i = 0; i < batch_size; i++) {
      slice_indexes_[i] = idx_ % slice_cnt_;
      idx_ += skip.at(i) ? 0 : 1;
    }
  }
  indexes = slice_indexes_;
  return ret;
}

template <bool USE_VEC>
int ObBroadcastSliceIdCalc::get_slice_indexes_inner(
  const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx, SliceIdxArray &slice_idx_array,
  ObBitVector *skip)
{
  UNUSED(exprs);
  UNUSED(eval_ctx);
  int ret = OB_SUCCESS;
  slice_idx_array.reuse();
  for (int64_t i = 0; OB_SUCC(ret) && i < slice_cnt_; ++i) {
    if (OB_FAIL(slice_idx_array.push_back(i))) {
      LOG_WARN("failed to push back i", K(ret));
    }
  }
  return ret;
}

template<>
int ObHashSliceIdCalc::calc_slice_idx<false>(ObEvalCtx &eval_ctx, int64_t slice_size,
                                          int64_t &slice_idx, ObBitVector *skip)
{
  int ret = OB_SUCCESS;
  uint64_t hash_val = SLICE_CALC_HASH_SEED;
  ObDatum *datum = nullptr;
  bool found_null = false;
  if (OB_ISNULL(hash_dist_exprs_) || OB_ISNULL(hash_funcs_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("hash func and expr not init", K(ret));
  } else if (n_keys_ > hash_dist_exprs_->count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: n_keys is invalid", K(ret),
             K(n_keys_), K(hash_dist_exprs_->count()));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < n_keys_; ++i) {
    const ObExpr* dist_expr = hash_dist_exprs_->at(i);
    if (OB_FAIL(dist_expr->eval(eval_ctx, datum))) {
      LOG_WARN("failed to eval datum", K(ret));
    } else if (datum->is_null() && ObNullDistributeMethod::DROP == null_row_dist_method_) {
      slice_idx = ObSliceIdxCalc::DEFAULT_CHANNEL_IDX_TO_DROP_ROW;
      found_null = true;
      break;
    } else if (datum->is_null() && ObNullDistributeMethod::RANDOM == null_row_dist_method_) {
      slice_idx = round_robin_idx_ % slice_size;
      round_robin_idx_++;
      found_null = true;
      break;
    } else if (OB_FAIL(hash_funcs_->at(i).hash_func_(*datum, hash_val, hash_val))) {
      LOG_WARN("failed to do hash", K(ret));
    }
  }
  if (OB_SUCC(ret) && !found_null) {
    slice_idx = hash_val % slice_size;
  }
  return ret;
}

template<>
int ObHashSliceIdCalc::calc_slice_idx<true>(ObEvalCtx &eval_ctx, int64_t slice_size,
                                          int64_t &slice_idx, ObBitVector *skip)
{
  int ret = OB_SUCCESS;
  uint64_t hash_val = SLICE_CALC_HASH_SEED;
  ObDatum *datum = nullptr;
  bool found_null = false;
  if (OB_ISNULL(hash_dist_exprs_) || OB_ISNULL(hash_funcs_) || OB_ISNULL(skip)) {
    ret = OB_NOT_INIT;
    LOG_WARN("hash func and expr not init", K(ret), K(hash_dist_exprs_), K(hash_funcs_));
  } else if (n_keys_ > hash_dist_exprs_->count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: n_keys is invalid", K(ret),
             K(n_keys_), K(hash_dist_exprs_->count()));
  }
  bool all_rows_active = false;
  int64_t batch_idx = eval_ctx.get_batch_idx();
  int64_t batch_size = eval_ctx.get_batch_size();
  EvalBound eval_bound(batch_size, batch_idx, batch_idx + 1, all_rows_active);
  for (int64_t i = 0; OB_SUCC(ret) && i < n_keys_; ++i) {
    const ObExpr* dist_expr = hash_dist_exprs_->at(i);
    LOG_DEBUG("[VEC2.0 PX]calc hash slice idx", KPC(dist_expr));
    if (OB_FAIL(dist_expr->eval_vector(eval_ctx, *skip, eval_bound))) {
      LOG_WARN("eval vector failed", K(ret));
    } else {
      ObIVector *vec = dist_expr->get_vector(eval_ctx);
      bool is_null = vec->is_null(batch_idx);
      if (is_null && ObNullDistributeMethod::DROP == null_row_dist_method_) {
        slice_idx = ObSliceIdxCalc::DEFAULT_CHANNEL_IDX_TO_DROP_ROW;
        found_null = true;
        break;
      } else if (is_null && ObNullDistributeMethod::RANDOM == null_row_dist_method_) {
        slice_idx = round_robin_idx_ % slice_size;
        round_robin_idx_++;
        found_null = true;
        break;
      } else if (OB_FAIL(vec->murmur_hash_v3_for_one_row(*dist_expr, hash_val, batch_idx,
                                                         batch_size, hash_val))) {
        LOG_WARN("failed to cal hash");
      } else {
        LOG_DEBUG("[VEC2.0 PX] calc hash slice value", K(i), K(ret), K(null_row_dist_method_),
                  K(hash_val), K(hash_funcs_->at(i)).hash_func_);
      }
    }
  }
  if (OB_SUCC(ret) && !found_null) {
    slice_idx = hash_val % slice_size;
  }
  return ret;
}

template <>
int ObHashSliceIdCalc::calc_hash_value<false>(ObEvalCtx &eval_ctx, uint64_t &hash_val,
                                              ObBitVector *skip)
{
  int ret = OB_SUCCESS;
  ObDatum *datum = nullptr;
  if (OB_ISNULL(hash_dist_exprs_) || OB_ISNULL(hash_funcs_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("hash func and expr not init", K(ret));
  } else if (n_keys_ > hash_dist_exprs_->count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: n_keys is invalid", K(ret),
      K(n_keys_), K(hash_dist_exprs_->count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < n_keys_; ++i) {
    const ObExpr* dist_expr = hash_dist_exprs_->at(i);
    if (OB_FAIL(dist_expr->eval(eval_ctx, datum))) {
      LOG_WARN("failed to eval datum", K(ret));
    } else if (OB_FAIL(hash_funcs_->at(i).hash_func_(*datum, hash_val, hash_val))) {
      LOG_WARN("failed to do hash", K(ret));
    }
  }
  return ret;
}

template <>
int ObHashSliceIdCalc::calc_hash_value<true>(ObEvalCtx &eval_ctx, uint64_t &hash_val,
                                            ObBitVector *skip)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(hash_dist_exprs_) || OB_ISNULL(hash_funcs_) || OB_ISNULL(skip)) {
    ret = OB_NOT_INIT;
    LOG_WARN("hash func and expr not init", K(ret), K(hash_dist_exprs_), K(hash_funcs_));
  } else if (n_keys_ > hash_dist_exprs_->count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: n_keys is invalid", K(ret),
      K(n_keys_), K(hash_dist_exprs_->count()));
  } else {
    const int64_t batch_idx = eval_ctx.get_batch_idx();
    const int64_t batch_size = eval_ctx.get_batch_size();
    EvalBound bound(batch_size, batch_idx, batch_idx + 1, false);
    for (int64_t i = 0; OB_SUCC(ret) && i < n_keys_; ++i) {
      const ObExpr* dist_expr = hash_dist_exprs_->at(i);
      if (OB_FAIL(dist_expr->eval_vector(eval_ctx, *skip, bound))) {
        LOG_WARN("eval vector failed", K(ret));
      } else {
        ObIVector *vec = dist_expr->get_vector(eval_ctx);
        if (OB_FAIL(vec->murmur_hash_v3_for_one_row(*dist_expr, hash_val, batch_idx, batch_size,
                                                    hash_val))) {
          LOG_WARN("failed to cal hash");
        }
      }
    }
  }
  return ret;
}

template <bool USE_VEC>
int ObHashSliceIdCalc::get_slice_indexes_inner(const ObIArray<ObExpr*> &exprs,
                                         ObEvalCtx &eval_ctx,
                                         SliceIdxArray &slice_idx_array,
                                         ObBitVector *skip)
{
  int ret = OB_SUCCESS;
  int64_t slice_idx = 0;
  if (OB_FAIL(setup_slice_index(slice_idx_array))) {
    LOG_WARN("set slice index failed", K(ret));
  } else if (OB_FAIL(calc_slice_idx<USE_VEC>(eval_ctx, task_cnt_, slice_idx, skip))) {
    LOG_WARN("calc slice idx failed", K(ret));
  } else {
    slice_idx_array.at(0) = slice_idx;
  }
  return ret;
}

template <>
int ObHashSliceIdCalc::get_slice_idx_batch_inner<false>(const ObIArray<ObExpr*> &, ObEvalCtx &eval_ctx,
                                         ObBitVector &skip, const int64_t batch_size,
                                         int64_t *&indexes)
{
  int ret = OB_SUCCESS;
  if (n_keys_ > hash_dist_exprs_->count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: n_keys is invalid", K(ret),
      K(n_keys_), K(hash_dist_exprs_->count()));
  } else if (OB_FAIL(setup_slice_indexes(eval_ctx))) {
    LOG_WARN("setup slice indexes failed", K(ret));
  } else {
    uint64_t *hash_val = reinterpret_cast<uint64_t *>(slice_indexes_);
    uint64_t default_seed = SLICE_CALC_HASH_SEED;
    for (int64_t i = 0; OB_SUCC(ret) && i < n_keys_; i++) {
      ObExpr *e = hash_dist_exprs_->at(i);
      if (OB_FAIL(e->eval_batch(eval_ctx, skip, batch_size))) {
        LOG_WARN("eval batch failed", K(ret));
      } else {
        const bool is_batch_seed = i > 0;
        hash_funcs_->at(i).batch_hash_func_(hash_val,
                                            e->locate_batch_datums(eval_ctx),
                                            e->is_batch_result(),
                                            skip,
                                            batch_size,
                                            is_batch_seed ? hash_val : &default_seed,
                                            is_batch_seed);
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; i < batch_size; i++) {
        if (!skip.at(i)) {
          slice_indexes_[i] = hash_val[i] % task_cnt_;
        }
      }
      indexes = slice_indexes_;
    }
  }
  return ret;
}

template <>
int ObHashSliceIdCalc::get_slice_idx_batch_inner<true>(const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx,
                                            ObBitVector &skip, const int64_t batch_size,
                                            int64_t *&indexes)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(n_keys_ > hash_dist_exprs_->count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: n_keys is invalid", K(ret),
      K(n_keys_), K(hash_dist_exprs_->count()));
  } else if (OB_FAIL(setup_slice_indexes(eval_ctx))) {
    LOG_WARN("setup slice indexes failed", K(ret));
  } else {
    uint64_t *hash_val = reinterpret_cast<uint64_t *>(slice_indexes_);
    uint64_t default_seed = SLICE_CALC_HASH_SEED;
    ObIVector *vec = NULL;
    const bool all_rows_active = false;
    EvalBound eval_bound(batch_size, all_rows_active);
    for (int64_t i = 0; OB_SUCC(ret) && i < n_keys_; i++) {
      ObExpr *e = hash_dist_exprs_->at(i);
      const bool is_batch_seed = i > 0;
      if (OB_FAIL(e->eval_vector(eval_ctx, skip, eval_bound))) {
        LOG_WARN("eval batch failed", K(ret));
      } else if (FALSE_IT(vec = e->get_vector(eval_ctx))) {
      } else if (OB_FAIL(vec->murmur_hash_v3(*e, hash_val, skip, eval_bound,
                                             is_batch_seed ? hash_val : &default_seed,
                                             is_batch_seed))) {
        LOG_WARN("calc murmur hash failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; i < batch_size; i++) {
        if (!skip.at(i)) {
          slice_indexes_[i] = hash_val[i] % task_cnt_;
        }
      }
      indexes = slice_indexes_;
      LOG_TRACE("[VEC2.0] hash slice calc", K(ObArrayWrap<int64_t>(indexes, batch_size)));
    }
  }
  return ret;
}

/*******************                 ObSlaveMapPkeyRangeIdxCalc                 ********************/

ObSlaveMapPkeyRangeIdxCalc::~ObSlaveMapPkeyRangeIdxCalc()
{
  destroy();
}

int ObSlaveMapPkeyRangeIdxCalc::init(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(is_inited_));
  } else if (OB_FAIL(ObSlaveMapRepartIdxCalcBase::init(tenant_id))) {
    LOG_WARN("fail init base repart class", K(ret));
  } else if (OB_UNLIKELY(nullptr == calc_part_id_expr_ || sort_exprs_.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(calc_part_id_expr_), K(sort_exprs_.count()));
  } else if (OB_FAIL(sort_key_.reserve(sort_exprs_.count()))) {
    LOG_WARN("reserve sort key failed", K(ret), K(sort_exprs_.count()));
  } else if (OB_FAIL(build_partition_range_channel_map(part_range_map_))) {
    LOG_WARN("build partition range map failed", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObSlaveMapPkeyRangeIdxCalc::destroy()
{
  int ret = OB_SUCCESS;
  is_inited_ = false;
  if (part_range_map_.created()) {
    struct DeleteFn {
      DeleteFn(ObIAllocator &allocator) : allocator_(allocator) {}
      int operator ()(hash::HashMapPair<int64_t, PartitionRangeChannelInfo *> &it) {
        if (nullptr != it.second) {
          it.second->~PartitionRangeChannelInfo();
          allocator_.free(it.second);
          it.second = nullptr;
        }

        // The Story Behind Return Code:
        //   We change the interface for this because of supporting that iterations encounter an error
        //   to return immediately, yet for all the existing logics there, they don't care the return
        //   code and wants to continue iteration anyway. So to keep the old behavior and makes everyone
        //   else happy, we have to return OB_SUCCESS here. And we only make this return code thing
        //   affects the behavior in tenant meta manager washing tablet. If you want to change the
        //   behavior in such places, please consult the individual file owners to fully understand the
        //   needs there.
        return OB_SUCCESS;
      }
      ObIAllocator &allocator_;
    };
    DeleteFn delete_fn(exec_ctx_.get_allocator());
    if (OB_FAIL(part_range_map_.foreach_refactored(delete_fn))) {
      LOG_WARN("free item in partition range map failed", K(ret));
    } else if (OB_FAIL(part_range_map_.destroy())) {
      LOG_WARN("destroy partition range map failed", K(ret));
    }
  }
  int tmp_ret = ObSlaveMapRepartIdxCalcBase::destroy();
  ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
  return ret;
}

int ObSlaveMapPkeyRangeIdxCalc::build_partition_range_channel_map(
    common::hash::ObHashMap<int64_t, PartitionRangeChannelInfo *> &part_range_channel_map)
{
  int ret = OB_SUCCESS;
  part_range_channel_map.destroy();
  ObPxSqcHandler *handler = exec_ctx_.get_sqc_handler();
  const Ob2DArray<ObPxTabletRange> &part_ranges = handler->get_partition_ranges();
  if (OB_FAIL(part_range_channel_map.create(DEFAULT_PARTITION_COUNT, common::ObModIds::OB_SQL_PX))) {
    LOG_WARN("create part range map failed", K(ret));
  } else {
    int64_t tmp_tablet_id = OB_INVALID_INDEX_INT64;
    ObArray<int64_t> tmp_channels;
    const int64_t part_ch_count = part_ch_info_.part_ch_array_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < part_ch_count + 1; ++i) {
      const int64_t cur_tablet_id = i < part_ch_count ? part_ch_info_.part_ch_array_.at(i).first_ : OB_INVALID_INDEX_INT64;
      const int64_t cur_ch_idx = i< part_ch_count ? part_ch_info_.part_ch_array_.at(i).second_ : OB_INVALID_INDEX_INT64;
      if (cur_tablet_id != tmp_tablet_id) {
        if (tmp_channels.count() > 0) {
          // save
          void *buf = nullptr;
          PartitionRangeChannelInfo *item = nullptr;
          if (OB_ISNULL(buf = exec_ctx_.get_allocator().alloc(sizeof(PartitionRangeChannelInfo)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          } else if (FALSE_IT(item = new (buf) PartitionRangeChannelInfo(exec_ctx_.get_allocator()))) {
          } else if (FALSE_IT(item->tablet_id_ = tmp_tablet_id)) {
          } else if (OB_FAIL(item->channels_.assign(tmp_channels))) {
            LOG_WARN("assign partition channels failed", K(ret));
          } else if (OB_FAIL(part_range_channel_map.set_refactored(item->tablet_id_, item))) {
            LOG_WARN("insert partition range map failed", K(ret), K(item));
          } else {
            buf = nullptr;
          }
          if (nullptr != buf) {
            item->~PartitionRangeChannelInfo();
            item = nullptr;
            exec_ctx_.get_allocator().free(buf);
          }
        }
        // reset
        tmp_tablet_id = cur_tablet_id;
        tmp_channels.reset();
      }
      if (OB_SUCC(ret) && OB_INVALID_INDEX_INT64 != cur_ch_idx) {
        if (OB_FAIL(tmp_channels.push_back(cur_ch_idx))) {
          LOG_WARN("push back channel index failed", K(ret), K(i), K(cur_ch_idx));
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < part_ranges.count(); ++i) {
      const ObPxTabletRange &cur_part_range = part_ranges.at(i);
      PartitionRangeChannelInfo *item = nullptr;
      if (OB_FAIL(part_range_channel_map.get_refactored(cur_part_range.tablet_id_, item))) {
        LOG_WARN("get partition channel info failed", K(ret), K(i), K(cur_part_range));
      } else if (OB_FAIL(item->range_cut_.assign(cur_part_range.range_cut_))) {
        LOG_WARN("assign end key failed", K(ret), K(i), K(cur_part_range), K(*item));
      }
    }
  }
  return ret;
}

static int calc_ch_idx(const int64_t range_count, const int64_t ch_count, const int64_t range_idx, int64_t &ch_idx)
{
  int ret = OB_SUCCESS;
  ch_idx = -1;
  if (OB_UNLIKELY(ch_count <= 0 || range_idx < 0 || range_idx >= range_count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(range_count), K(ch_count), K(range_idx));
  } else if (range_count <= ch_count) {
    ch_idx = range_idx;
  } else {
    const int64_t avg = range_count / ch_count;
    const int64_t rem = range_count % ch_count;
    int64_t tmp = 0;
    if ((tmp = range_idx - rem * (avg + 1)) > 0) {
      ch_idx = rem + tmp / avg;
    } else {
      ch_idx = range_idx / (avg + 1);
    }
  }
  return ret;
}

bool ObSlaveMapPkeyRangeIdxCalc::Compare::operator()(
    const ObPxTabletRange::DatumKey &l,
    const ObPxTabletRange::DatumKey &r)
{
  bool less = false;
  int &ret = ret_;
  if (OB_FAIL(ret)) {
    // already fail
  } else if (OB_ISNULL(sort_cmp_funs_) || OB_ISNULL(sort_collations_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(l), K(r));
  } else {
    int cmp = 0;
    const int64_t cnt = sort_cmp_funs_->count();
    for (int64_t i = 0; OB_SUCC(ret) && 0 == cmp && i < cnt; i++) {
      if (OB_FAIL(sort_cmp_funs_->at(i).cmp_func_(l.at(i), r.at(i), cmp))) {
        LOG_WARN("do cmp failed", K(ret), K(i), K(l), K(r));
      } else if (cmp < 0) {
        less = sort_collations_->at(i).is_ascending_;
      } else if (cmp > 0) {
        less = !sort_collations_->at(i).is_ascending_;
      }
    }
  }
  return less;
}

int ObSlaveMapPkeyRangeIdxCalc::get_task_idx(
    const int64_t tablet_id,
    const ObPxTabletRange::DatumKey &sort_key,
    int64_t &task_idx)
{
  int ret = OB_SUCCESS;
  PartitionRangeChannelInfo *item = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(tablet_id <= 0)) {
    ret = OB_NO_PARTITION_FOR_GIVEN_VALUE;
    LOG_WARN("can't get the right partition", K(ret), K(tablet_id), K(repart_type_));
  } else if (OB_UNLIKELY(sort_key.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), K(tablet_id), K(sort_key));
  } else if (OB_FAIL(part_range_map_.get_refactored(tablet_id, item))) {
    LOG_WARN("get partition ranges failed", K(ret), K(tablet_id));
  } else if (OB_UNLIKELY(nullptr == item || item->tablet_id_ != tablet_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid range channel map", K(ret), K(tablet_id), KP(item));
  } else {
    ObPxTabletRange::RangeCut &range_cut = item->range_cut_;
    ObPxTabletRange::RangeCut::iterator found_it = std::lower_bound(
        range_cut.begin(), range_cut.end(), sort_key, sort_cmp_);
    if (OB_FAIL(sort_cmp_.ret_)) {
      LOG_WARN("sort compare failed", K(ret));
    } else {
      const int64_t range_idx = found_it - range_cut.begin();
      int64_t ch_idx = -1;
      if (OB_FAIL(calc_ch_idx(range_cut.count() + 1, item->channels_.count(), range_idx, ch_idx))) {
        LOG_WARN("get channel idx failed", K(ret), K(*item), K(range_idx));
      } else if (ch_idx < 0 || ch_idx >= item->channels_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid channel index", K(ret), K(ch_idx), K(*item));
      } else {
        task_idx = item->channels_.at(ch_idx);
      }
    }
  }
  return ret;
}

template <bool USE_VEC>
int ObSlaveMapPkeyRangeIdxCalc::get_slice_indexes_inner(const ObIArray<ObExpr*> &exprs,
                                                  ObEvalCtx &eval_ctx,
                                                  SliceIdxArray &slice_idx_array,
                                                  ObBitVector *skip)
{
  int ret = OB_SUCCESS;
  int64_t tablet_id = OB_INVALID_INDEX;
  int64_t slice_idx = 0;
  if (OB_FAIL(setup_slice_index(slice_idx_array))) {
    LOG_WARN("set slice index failed", K(ret));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(exprs.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(exprs.count()));
  } else if (OB_FAIL(ObRepartSliceIdxCalc::get_tablet_id<USE_VEC>(eval_ctx, tablet_id, skip))) {
    LOG_WARN("failed to get partition id", K(ret));
  } else {
    sort_key_.reuse();
    for (int64_t i = 0; OB_SUCC(ret) && i < sort_exprs_.count(); ++i) {
      const ObExpr *cur_expr = sort_exprs_.at(i);
      ObDatum *cur_datum = nullptr;
      if (OB_ISNULL(cur_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("current expr is null", K(ret), KP(cur_expr));
      } else if (OB_FAIL(cur_expr->eval(eval_ctx, cur_datum))) {
        LOG_WARN("eval expr to datum failed", K(ret), K(*cur_expr));
      } else if (OB_ISNULL(cur_datum)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("current datum is null", K(ret), KP(cur_datum));
      } else if (OB_FAIL(sort_key_.push_back(*cur_datum))) {
        LOG_WARN("push back datum failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(get_task_idx(tablet_id, sort_key_, slice_idx_array.at(0)))) {
        LOG_WARN("get task idx failed", K(ret), K(tablet_id), K(sort_key_));
      }
    }
  }
  return ret;
}

//TODO:shanting2.0 实现pkey range的向量化1.0和2.0接口

/*******************                 ObSlaveMapPkeyHashIdxCalc                 ********************/
int ObSlaveMapPkeyHashIdxCalc::init(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSlaveMapRepartIdxCalcBase::init(tenant_id))) {
    LOG_WARN("fail init base repart class", K(ret));
  } else if (affi_hash_map_.created()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("this map has been init twice", K(ret));
  } else if (OB_FAIL(build_affi_hash_map(affi_hash_map_))) {
    LOG_WARN("failed to build affi hash map", K(ret));
  }
  return ret;
}

int ObSlaveMapPkeyHashIdxCalc::destroy()
{
  int ret = OB_SUCCESS;
  if (affi_hash_map_.created()) {
    ret = affi_hash_map_.destroy();
  }
  int tmp_ret = ObSlaveMapRepartIdxCalcBase::destroy();
  ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
  return ret;
}

template <bool USE_VEC>
int ObSlaveMapPkeyHashIdxCalc::get_slice_indexes_inner(const ObIArray<ObExpr*> &exprs,
                                                 ObEvalCtx &eval_ctx,
                                                 SliceIdxArray &slice_idx_array,
                                                 ObBitVector *skip)
{
  int ret = OB_SUCCESS;
  // 计算过程：
  // 1. 通过row计算出对应的partition id
  // 2. 通过partition id，找到对应的task array
  // 3. hash 的方式从task array中选择task idx作为slice idx
  int64_t tablet_id = OB_INVALID_INDEX;
  if (OB_FAIL(setup_slice_index(slice_idx_array))) {
    LOG_WARN("set slice index failed", K(ret));
  } else if (part_ch_info_.part_ch_array_.size() <= 0) {
    // 表示没有 partition到task idx的映射
    ret = OB_NOT_INIT;
    LOG_WARN("the size of part task channel map is zero", K(ret));
  } else if (OB_FAIL(ObRepartSliceIdxCalc::get_tablet_id<USE_VEC>(eval_ctx, tablet_id, skip))) {
    LOG_WARN("failed to get partition id", K(ret));
  } else if (OB_FAIL(get_task_idx_by_tablet_id(eval_ctx, tablet_id, slice_idx_array.at(0)))) {
    if (OB_HASH_NOT_EXIST == ret) {
      if (tablet_id <= 0) {
        ret = OB_NO_PARTITION_FOR_GIVEN_VALUE;
      } else {
        ret = OB_SCHEMA_ERROR;
        LOG_USER_ERROR(OB_NO_PARTITION_FOR_GIVEN_VALUE);
      }
      LOG_WARN("can't get the right partition", K(ret), K(tablet_id), K(repart_type_));
    }
  }
  return ret;
}
//TODO:shanting2.0 实现pkey hash的向量化1.0和2.0接口
int ObSlaveMapPkeyHashIdxCalc::get_task_idx_by_tablet_id(ObEvalCtx &eval_ctx,
                                                         int64_t tablet_id,
                                                         int64_t &task_idx)
{
  int ret = OB_SUCCESS;
  int64_t hash_idx = 0;
  if (part_to_task_array_map_.size() <= 0) {
    ret = OB_NOT_INIT;
    LOG_WARN("part to task array is not inited", K(ret));
  } else {
    const TaskIdxArray *task_idx_array = part_to_task_array_map_.get(tablet_id);
    if (OB_ISNULL(task_idx_array)) {
      ret = OB_HASH_NOT_EXIST; // convert to hash error
      LOG_WARN("the task idx array is null", K(ret), K(tablet_id));
    } else if (task_idx_array->count() <= 0){
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the size of task idx array is zero", K(ret));
    } else if (OB_FAIL(hash_calc_.calc_slice_idx<false>(eval_ctx, task_idx_array->count(), hash_idx))) {
      LOG_WARN("fail calc hash value", K(ret));
    } else if (ObSliceIdxCalc::DEFAULT_CHANNEL_IDX_TO_DROP_ROW == hash_idx) {
      task_idx = ObSliceIdxCalc::DEFAULT_CHANNEL_IDX_TO_DROP_ROW;
    } else {
      task_idx = task_idx_array->at(hash_idx);
      LOG_TRACE("get task_idx/slice_idx by hash way", K(tablet_id), K(hash_idx), K(task_idx));
    }
  }
  return ret;
}

int ObSlaveMapPkeyHashIdxCalc::build_affi_hash_map(hash::ObHashMap<int64_t, ObPxPartChMapItem> &affi_hash_map)
{
  int ret = OB_SUCCESS;
  int64_t tablet_id = common::OB_INVALID_INDEX_INT64;
  ObPxPartChMapItem item;
  const ObPxPartChMapTMArray &part_ch_array = part_ch_info_.part_ch_array_;
  if (OB_FAIL(affi_hash_map.create(part_ch_array.count(), common::ObModIds::OB_SQL_PX))) {
    LOG_WARN("failed to create part ch map", K(ret));
  }
  ARRAY_FOREACH (part_ch_array, idx) {
    if (common::OB_INVALID_INDEX_INT64 == tablet_id) {
      tablet_id = part_ch_array.at(idx).first_;
      item.first_ = idx;
    } else if (tablet_id != part_ch_array.at(idx).first_) {
      item.second_ = idx;
      if (OB_FAIL(affi_hash_map.set_refactored(tablet_id, item))) {
        LOG_WARN("failed to set refactored", K(ret));
      }
      LOG_DEBUG("build affi hash map", K(tablet_id), K(item));
      tablet_id = part_ch_array.at(idx).first_;
      item.first_ = idx;
    }
  }
  if (OB_SUCC(ret)) {
    item.second_ = part_ch_array.count();
    if (OB_FAIL(affi_hash_map.set_refactored(tablet_id, item))) {
      LOG_WARN("failed to set refactored", K(ret));
    }
    LOG_DEBUG("build affi hash map", K(tablet_id), K(item));
  }
  return ret;
}

template <bool USE_VEC>
int ObRangeSliceIdCalc::get_slice_indexes_inner(const ObIArray<ObExpr*> &exprs,
                                          ObEvalCtx &eval_ctx,
                                          SliceIdxArray &slice_idx_array,
                                          ObBitVector *skip)
{
  int ret = OB_SUCCESS;
  if (USE_VEC) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should not call this function when use vectorize 2.0", K(ret));
  } else if (OB_FAIL(setup_slice_index(slice_idx_array))) {
    LOG_WARN("set slice index failed", K(ret));
  } else if (OB_ISNULL(dist_exprs_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected dist exprs", K(ret));
  } else if (OB_ISNULL(range_) || range_->range_cut_.empty()) {
    slice_idx_array.at(0) = 0;
  } else {
    ObPxTabletRange::DatumKey sort_key;
    ObDatum *datum = nullptr;
    for (int i = 0; i < dist_exprs_->count() && OB_SUCC(ret); ++i) {
      if (OB_ISNULL(dist_exprs_->at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr", K(ret));
      } else if (OB_FAIL(dist_exprs_->at(i)->eval(eval_ctx, datum))) {
        LOG_WARN("fail to eval expr", K(ret));
      } else if (OB_FAIL(sort_key.push_back(*datum))) {
        LOG_WARN("fail to push back sort key", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      Compare sort_cmp(&sort_cmp_funs_, &sort_collations_);
      ObPxTabletRange::RangeCut &range_cut = const_cast<ObPxTabletRange::RangeCut &>(range_->range_cut_);
      ObPxTabletRange::RangeCut::iterator found_it = std::lower_bound(
        range_cut.begin(), range_cut.end(), sort_key, sort_cmp);
      const int64_t range_idx = found_it - range_cut.begin();
      slice_idx_array.at(0) = range_idx % task_cnt_;
    }
  }
  return ret;
}

template <>
int ObRangeSliceIdCalc::get_slice_idx_batch_inner<false>(const ObIArray<ObExpr*> &,
                                              ObEvalCtx &eval_ctx,
                                              ObBitVector &skip,
                                              const int64_t batch_size,
                                              int64_t *&indexes)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dist_exprs_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected dist exprs", K(ret));
  } else if (OB_FAIL(setup_slice_indexes(eval_ctx))) {
    LOG_WARN("setup slice indexes failed", K(ret));
  } else if (OB_ISNULL(range_) || range_->range_cut_.empty()) {
    for (int64_t idx = 0; idx < batch_size; ++idx) {
      slice_indexes_[idx] = 0;
    }
    indexes = slice_indexes_;
  } else {
    Compare sort_cmp(&sort_cmp_funs_, &sort_collations_);
    ObPxTabletRange::DatumKey sort_key;
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx);
    batch_info_guard.set_batch_size(batch_size);
    for (int64_t idx = 0; OB_SUCC(ret) && idx < batch_size; ++idx) {
      if (skip.at(idx)) {
        continue;
      } else {
        batch_info_guard.set_batch_idx(idx);
        ObDatum *datum = nullptr;
        for (int64_t i = 0; i < dist_exprs_->count() && OB_SUCC(ret); ++i) {
          if (OB_ISNULL(dist_exprs_->at(i))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("null expr", K(ret));
          } else if (OB_FAIL(dist_exprs_->at(i)->eval(eval_ctx, datum))) {
            LOG_WARN("fail to eval expr", K(ret));
          } else if (OB_FAIL(sort_key.push_back(*datum))) {
            LOG_WARN("fail to push back sort key", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          ObPxTabletRange::RangeCut &range_cut = const_cast<ObPxTabletRange::RangeCut &>(range_->range_cut_);
          ObPxTabletRange::RangeCut::iterator found_it = std::lower_bound(
            range_cut.begin(), range_cut.end(), sort_key, sort_cmp);
          int64_t range_idx = found_it - range_cut.begin();
          slice_indexes_[idx] = range_idx % task_cnt_;
        }
        sort_key.reuse();
      }
    }
    indexes = slice_indexes_;
  }
  return ret;
}

template <>
int ObRangeSliceIdCalc::get_slice_idx_batch_inner<true>(const ObIArray<ObExpr*> &,
                                              ObEvalCtx &eval_ctx,
                                              ObBitVector &skip,
                                              const int64_t batch_size,
                                              int64_t *&indexes)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dist_exprs_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected dist exprs", K(ret));
  } else if (OB_FAIL(setup_slice_indexes(eval_ctx))) {
    LOG_WARN("setup slice indexes failed", K(ret));
  } else if (OB_ISNULL(range_) || range_->range_cut_.empty()) {
    for (int64_t idx = 0; idx < batch_size; ++idx) {
      slice_indexes_[idx] = 0;
    }
    indexes = slice_indexes_;
  } else {
    Compare sort_cmp(&sort_cmp_funs_, &sort_collations_);
    ObPxTabletRange::DatumKey sort_key;
    const bool all_rows_active = false;
    EvalBound eval_bound(batch_size, all_rows_active);
    for (int64_t i = 0; i < dist_exprs_->count() && OB_SUCC(ret); ++i) {
      if (OB_ISNULL(dist_exprs_->at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr", K(ret));
      } else if (OB_FAIL(dist_exprs_->at(i)->eval_vector(eval_ctx, skip, eval_bound))) {
        LOG_WARN("eval vector failed", K(ret));
      }
    }
    for (int64_t idx = 0; OB_SUCC(ret) && idx < batch_size; ++idx) {
      if (skip.at(idx)) {
        continue;
      } else {
        for (int64_t i = 0; i < dist_exprs_->count() && OB_SUCC(ret); ++i) {
          ObIVector *vec = dist_exprs_->at(i)->get_vector(eval_ctx);
          const char *payload = NULL;
          ObLength len = 0;
          vec->get_payload(idx, payload, len);
          ObDatum datum(payload, len, vec->is_null(idx));
          if (OB_FAIL(sort_key.push_back(datum))) {
            LOG_WARN("fail to push back sort key", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          ObPxTabletRange::RangeCut &range_cut = const_cast<ObPxTabletRange::RangeCut &>(range_->range_cut_);
          ObPxTabletRange::RangeCut::iterator found_it = std::lower_bound(
            range_cut.begin(), range_cut.end(), sort_key, sort_cmp);
          int64_t range_idx = found_it - range_cut.begin();
          slice_indexes_[idx] = range_idx % task_cnt_;
        }
        sort_key.reuse();
      }
    }
    indexes = slice_indexes_;
  }
  return ret;
}

bool ObRangeSliceIdCalc::Compare::operator()(
    const ObPxTabletRange::DatumKey &l,
    const ObPxTabletRange::DatumKey &r)
{
  bool less = false;
  int &ret = ret_;
  if (OB_FAIL(ret)) {
    // already fail
  } else if (OB_ISNULL(sort_cmp_funs_) || OB_ISNULL(sort_collations_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(l), K(r));
  } else {
    int cmp = 0;
    const int64_t cnt = sort_cmp_funs_->count();
    for (int64_t i = 0; OB_SUCC(ret) && 0 == cmp && i < cnt; i++) {
      if (OB_FAIL(sort_cmp_funs_->at(i).cmp_func_(l.at(i), r.at(i), cmp))) {
        LOG_WARN("do cmp failed", K(ret), K(i), K(l), K(r));
      } else if (cmp < 0) {
        less = sort_collations_->at(i).is_ascending_;
      } else if (cmp > 0) {
        less = !sort_collations_->at(i).is_ascending_;
      }
    }
  }
  return less;
}

template <bool USE_VEC>
int ObWfHybridDistSliceIdCalc::get_slice_indexes_inner(const ObIArray<ObExpr*> &exprs,
                                                 ObEvalCtx &eval_ctx,
                                                 SliceIdxArray &slice_idx_array,
                                                 ObBitVector *skip)
{
  int ret = OB_SUCCESS;

  if (slice_id_calc_type_ <= SliceIdCalcType::INVALID
      || slice_id_calc_type_ >= SliceIdCalcType::MAX) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("slice_id_calc_type_ is invalid", K(ret), K(slice_id_calc_type_));
  } else if (SliceIdCalcType::BROADCAST == slice_id_calc_type_
             && OB_FAIL(broadcast_slice_id_calc_.get_slice_indexes_inner<USE_VEC>(
                        exprs, eval_ctx, slice_idx_array, skip))) {
    LOG_WARN("get_slice_indexes_inner failed", K(ret), K(slice_id_calc_type_));
  } else if (SliceIdCalcType::RANDOM == slice_id_calc_type_
             && OB_FAIL(random_slice_id_calc_.get_slice_indexes_inner<USE_VEC>(
                        exprs, eval_ctx, slice_idx_array, skip))) {
    LOG_WARN("get_slice_indexes_inner failed", K(ret), K(slice_id_calc_type_));
  } else if (SliceIdCalcType::HASH == slice_id_calc_type_
             && OB_FAIL(hash_slice_id_calc_.get_slice_indexes_inner<USE_VEC>(exprs, eval_ctx,
                        slice_idx_array, skip))) {
    LOG_WARN("get_slice_indexes_inner failed", K(ret), K(slice_id_calc_type_));
  }

  return ret;
}
//TODO:shanting2.0 实现wf hybrid的向量化1.0和2.0接口. 否则这样直接调其他slice_calc的get_slice_indexes_inner会有问题

template <bool USE_VEC>
int ObNullAwareHashSliceIdCalc::get_slice_indexes_inner(const ObIArray<ObExpr*> &exprs,
                                                  ObEvalCtx &eval_ctx,
                                                  SliceIdxArray &slice_idx_array,
                                                  ObBitVector *skip)
{
  int ret = OB_SUCCESS;
  uint64_t hash_val = SLICE_CALC_HASH_SEED;
  UNUSED(exprs);
  bool processed = false;
  slice_idx_array.reuse();
  if (OB_ISNULL(hash_dist_exprs_) || hash_dist_exprs_->count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null aware hash join can only process 1 join key now", K(ret), K(hash_dist_exprs_));
  } else if (OB_FAIL(calc_for_null_aware<USE_VEC>(*hash_dist_exprs_->at(0), task_cnt_, eval_ctx,
                                         slice_idx_array, processed, skip))) {
    LOG_WARN("failed to calc for null aware", K(ret));
  } else if (processed) {
    // do nothing
  } else {
    if (OB_FAIL(ObHashSliceIdCalc::calc_hash_value<USE_VEC>(eval_ctx, hash_val, skip))) {
      LOG_WARN("fail calc hash value null aware",  K(ret));
    } else {
      OZ (slice_idx_array.push_back(hash_val % task_cnt_));
    }
  }
  return ret;
}

int ObNullAwareAffinitizedRepartSliceIdxCalc::init(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  OZ (ObRepartSliceIdxCalc::init(tenant_id));
  OX (support_vectorized_calc_ = false);
  return ret;
}

template <bool USE_VEC>
int ObNullAwareAffinitizedRepartSliceIdxCalc::get_slice_indexes_inner(
    const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx, SliceIdxArray &slice_idx_array,
    ObBitVector *skip)
{
  int ret = OB_SUCCESS;
  UNUSED(exprs);
  int64_t tablet_id = OB_INVALID_INDEX;
  int64_t slice_idx = OB_INVALID_INDEX;
  bool processed = false;
  slice_idx_array.reuse();
  if (task_count_ <= 0) {
    ret = OB_NOT_INIT;
    LOG_WARN("task_count not inited", K_(task_count), K(ret));
  } else if (OB_ISNULL(repartition_exprs_) || 1 != repartition_exprs_->count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected repartition exprs", KP(repartition_exprs_));
  } else if (px_repart_ch_map_.size() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid map size, affinity map should not be empty!", K_(task_count), K(ret));
  } else if (OB_FAIL(calc_for_null_aware<USE_VEC>(*repartition_exprs_->at(0), task_count_, eval_ctx,
                                         slice_idx_array, processed, skip))) {
    LOG_WARN("failed to calc for null aware", K(ret));
  } else if (processed) {
    // do nothing
  } else if (OB_FAIL(ObAffinitizedRepartSliceIdxCalc::get_slice_indexes_inner<USE_VEC>(exprs, eval_ctx, slice_idx_array, skip))) {
    LOG_WARN("failed to get slice idx", K(ret));
  }

  return ret;
}

template <bool USE_VEC>
int ObHybridHashSliceIdCalcBase::check_if_popular_value(ObEvalCtx &eval_ctx, bool &is_popular, ObBitVector *skip)
{
  int ret = OB_SUCCESS;
  uint64_t hash_val = 0;
  is_popular = false;
  if (OB_ISNULL(popular_values_hash_) || popular_values_hash_->count() <= 0) {
    // assume not popular, do nothing
  } else if (OB_UNLIKELY(hash_calc_.hash_funcs_->count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only support 1 condition for hybrid hash for now. this may change later",
             K(ret), K(hash_calc_.hash_funcs_->count()));
  } else if (OB_FAIL(hash_calc_.calc_hash_value<USE_VEC>(eval_ctx, hash_val, skip))) {
    LOG_WARN("fail get hash value", K(ret));
  } else {
    //  build a small hash table to accelerate the lookup.
    //  if popular_values_hash_->count() <= 3, we use array lookup instead
    if (use_hash_lookup_) {
      if (OB_HASH_EXIST == (ret = popular_values_map_.exist_refactored(hash_val))) {
        is_popular = true;
        ret = OB_SUCCESS; // popular value
      } else if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS; // not popular value
      } else {
        LOG_WARN("fail lookup hash map", K(ret));
      }
    } else {
      for (int64_t i = 0; i < popular_values_hash_->count(); ++i) {
        if (hash_val == popular_values_hash_->at(i)) {
          is_popular = true;
          break;
        }
      }
    }
  }
  return ret;
}

template <bool USE_VEC>
int ObHybridHashRandomSliceIdCalc::get_slice_indexes_inner(const ObIArray<ObExpr*> &exprs,
                                                     ObEvalCtx &eval_ctx,
                                                     SliceIdxArray &slice_idx_array,
                                                     ObBitVector *skip)
{
  int ret = OB_SUCCESS;
  bool is_popular = false;
  if (OB_FAIL(check_if_popular_value<USE_VEC>(eval_ctx, is_popular, skip))) {
    LOG_WARN("fail check if value popular", K(ret));
  } else if (is_popular) {
    ret = random_calc_.get_slice_indexes_inner<USE_VEC>(exprs, eval_ctx, slice_idx_array, skip);
  } else {
    ret = hash_calc_.get_slice_indexes_inner<USE_VEC>(exprs, eval_ctx, slice_idx_array, skip);
  }
  return ret;
}

template <bool USE_VEC>
int ObHybridHashRandomSliceIdCalc::get_slice_idx_batch_inner(const ObIArray<ObExpr*> &exprs,
    ObEvalCtx &eval_ctx, ObBitVector &skip,
    const int64_t batch_size, int64_t *&indexes)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(setup_slice_indexes(eval_ctx))) {
    LOG_WARN("setup slice indexes failed", K(ret));
  } else if (OB_FAIL(hash_calc_.get_slice_idx_batch_inner<USE_VEC>(exprs, eval_ctx, skip, batch_size, slice_indexes_))) {
    LOG_WARN("get hash slice idx batch failed", K(ret));
  } else {
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx);
    bool is_popular = false;
    SliceIdxArray slice_idx_array;
    if (OB_FAIL(slice_idx_array.push_back(0))) {
      LOG_WARN("push back failed", K(ret));
    }
    for (int64_t i = 0; i < batch_size && OB_SUCC(ret); i++) {
      if (skip.at(i)) {
        continue;
      }
      batch_info_guard.set_batch_idx(i);
      if (OB_FAIL(check_if_popular_value<USE_VEC>(eval_ctx, is_popular, &skip))) {
        LOG_WARN("check if popular value failed", K(ret));
      } else if (is_popular) {
        if (OB_FAIL(random_calc_.get_slice_indexes_inner<USE_VEC>(exprs, eval_ctx, slice_idx_array,
                                                                  &skip))) {
          LOG_WARN("get random slice indexes failed", K(ret));
        } else {
          slice_indexes_[i] = slice_idx_array.at(0);
        }
      }
    }
    if (OB_SUCC(ret)) {
      indexes = slice_indexes_;
    }
  }
  return ret;
}

template <bool USE_VEC>
int ObHybridHashBroadcastSliceIdCalc::get_slice_indexes_inner(
    const ObIArray<ObExpr*> &exprs, ObEvalCtx &eval_ctx, SliceIdxArray &slice_idx_array,
    ObBitVector *skip)
{
  int ret = OB_SUCCESS;
  bool is_popular = false;
  if (OB_FAIL(check_if_popular_value<USE_VEC>(eval_ctx, is_popular, skip))) {
    LOG_WARN("fail check if value popular", K(ret));
  } else if (is_popular) {
    ret = broadcast_calc_.get_slice_indexes_inner<USE_VEC>(exprs, eval_ctx, slice_idx_array, skip);
  } else {
    ret = hash_calc_.get_slice_indexes_inner<USE_VEC>(exprs, eval_ctx, slice_idx_array, skip);
  }
  return ret;
}
