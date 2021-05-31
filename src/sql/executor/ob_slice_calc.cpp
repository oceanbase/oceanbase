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
#include "sql/executor/ob_range_hash_key_getter.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_calc_partition_id.h"
#include "share/schema/ob_table_schema.h"
#include "common/row/ob_row.h"
#include "lib/ob_define.h"
#include "share/schema/ob_part_mgr_util.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

int ObSliceIdxCalc::get_slice_indexes(const common::ObNewRow& row, SliceIdxArray& slice_idx_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(slice_idx_array.count() != 1)) {
    slice_idx_array.reuse();
    if (OB_FAIL(slice_idx_array.push_back(0))) {
      LOG_WARN("array push back failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ret = get_slice_idx(row, slice_idx_array.at(0));
  }
  return ret;
}

int ObSliceIdxCalc::get_slice_indexes(
    const ObIArray<ObExpr*>& exprs, ObEvalCtx& eval_ctx, SliceIdxArray& slice_idx_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(slice_idx_array.count() != 1)) {
    slice_idx_array.reuse();
    if (OB_FAIL(slice_idx_array.push_back(0))) {
      LOG_WARN("array push back failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ret = get_slice_idx(exprs, eval_ctx, slice_idx_array.at(0));
  }
  return ret;
}

int ObSliceIdxCalc::get_previous_row_partition_id(ObObj& partition_id)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(partition_id);
  return ret;
}

int ObRepartSliceIdxCalc::get_slice_idx(const common::ObNewRow& row, int64_t& slice_idx)
{
  int ret = OB_SUCCESS;
  int64_t part_id = common::OB_INVALID_INDEX;
  int64_t subpart_id = common::OB_INVALID_INDEX;
  bool skip_row = false;

  if (OB_REPARTITION_ONE_SIDE_ONE_LEVEL == repart_type_ || OB_REPARTITION_ONE_SIDE_TWO_LEVEL == repart_type_) {
    if (OB_FAIL(shuffle_service_.get_partition_ids(exec_ctx_,
            table_schema_,
            row,
            *repart_func_,
            *repart_sub_func_,
            *repart_columns_,
            *repart_sub_columns_,
            part_id,
            subpart_id,
            skip_row))) {
      LOG_WARN("fail get part idx", K(ret), K(part_id), K(subpart_id));
    } else if (ObShuffleService::NO_MATCH_PARTITION == part_id || ObShuffleService::NO_MATCH_PARTITION == subpart_id) {
      slice_idx = ObSliceIdxCalc::DEFAULT_CHANNEL_IDX_TO_DROP_ROW;
    } else if (OB_FAIL(ObTransmit::get_slice_idx_by_partition_ids(part_id, subpart_id, table_schema_, slice_idx))) {
      LOG_WARN("fail get slice idx", K(ret), K(part_id), K(subpart_id));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected repartition type", K(ret), K(repart_type_));
  }
  return ret;
}

int ObRepartSliceIdxCalc::init_partition_cache_map()
{
  int ret = OB_SUCCESS;
  ObPartitionLevel level = table_schema_.get_part_level();
  if (PARTITION_LEVEL_ONE == level || PARTITION_LEVEL_TWO == level) {
    int64_t part_num = table_schema_.get_part_option().get_part_num();
    const ObPartition* part = NULL;
    bool check_dropped_schema = false;
    ObPartIteratorV2 iter(table_schema_, check_dropped_schema);
    if (part_num < 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the part num is not correct", K(ret), K(part_num));
    } else if (OB_FAIL(init_cache_map(part_id_to_part_array_idx_))) {
      LOG_WARN("init the part_id_to_part_array_idx map fail", K(ret), K(table_schema_));
    } else {
      int i = 0;
      while (OB_SUCC(ret) && OB_SUCC(iter.next(part))) {
        if (OB_ISNULL(part)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL ptr", K(ret), K(part));
        } else if (OB_FAIL(part_id_to_part_array_idx_.set_refactored(part->get_part_id(), i))) {
          LOG_WARN("fail to set part_id_to_part_array_idx", K(ret), K(part_num), K(i), K(*part));
        } else {
          i++;
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }

    if (OB_SUCC(ret) && table_schema_.is_hash_part()) {
      if (OB_FAIL(init_cache_map(part_idx_to_part_id_))) {
        LOG_WARN("init the part_idx_to_part_array_id map fail", K(ret), K(table_schema_));
      } else {
        iter.init(table_schema_, check_dropped_schema);
        while (OB_SUCC(ret) && OB_SUCC(iter.next(part))) {
          if (OB_ISNULL(part)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("NULL ptr", K(ret), K(part));
          } else if (OB_FAIL(part_idx_to_part_id_.set_refactored(part->get_part_idx(), part->get_part_id()))) {
            LOG_WARN("fail to set part_idx_to_part_id", K(ret), K(part_num), K(*part));
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
        if (OB_SUCC(ret)) {
          // set the part_idx_to_part_id map to shuffle_service_
          shuffle_service_.set_part_idx_to_part_id_map(&part_idx_to_part_id_);
        }
      }
    }

    if (OB_SUCC(ret) && PARTITION_LEVEL_TWO == level) {
      int64_t subpart_num = table_schema_.get_sub_part_option().get_part_num();
      ObSubPartition** subpart_array = table_schema_.get_def_subpart_array();
      if (subpart_num < 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the sub part num is not correct", K(ret), K(subpart_num));
      } else if (OB_ISNULL(subpart_array)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the sub part array is null", K(ret), K(subpart_array));
      } else if (OB_FAIL(init_cache_map(subpart_id_to_subpart_array_idx_))) {
        LOG_WARN("init the subpart_id_to_subpart_idx map fail", K(ret), K(table_schema_));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < subpart_num; i++) {
          const ObSubPartition* subpart = subpart_array[i];
          if (OB_FAIL(subpart_id_to_subpart_array_idx_.set_refactored(subpart->get_sub_part_id(), i))) {
            LOG_WARN("fail to set subpart_id_to_subpart_idx", K(ret), K(i), K(subpart_num), K(*subpart));
          }
        }
      }
      if (OB_SUCC(ret) && table_schema_.is_hash_subpart()) {
        if (OB_FAIL(init_cache_map(subpart_idx_to_subpart_id_))) {
          LOG_WARN("init the subpart_idx_to_subpart_id map fail", K(ret), K(table_schema_));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < subpart_num; i++) {
            const ObSubPartition* subpart = subpart_array[i];
            if (OB_FAIL(subpart_idx_to_subpart_id_.set_refactored(
                    subpart->get_sub_part_idx(), subpart->get_sub_part_id()))) {
              LOG_WARN("fail to set subpart_idx_to_subpart_id", K(i), K(subpart_num), K(ret), K(*subpart));
            }
          }
          // set the subpart_idx_to_subpart_id to shuffle_service_
          shuffle_service_.set_subpart_idx_to_subpart_id_map(&subpart_idx_to_subpart_id_);
        }
      }
    }
  }
  return ret;
}

int ObRepartSliceIdxCalc::init_cache_map(hash::ObHashMap<int64_t, int64_t, hash::NoPthreadDefendMode>& map)
{
  int ret = OB_SUCCESS;
  if (map.created()) {
    if (OB_FAIL(map.reuse())) {
      LOG_WARN("reuse the partition info map fail", K(ret));
    }
  } else if (OB_FAIL(map.create(DEFAULT_CACHE_MAP_BUCKET_NUM, ObModIds::OB_HASH_BUCKET_PX_TRANSIMIT_REPART))) {
    LOG_WARN("create the partition info map fail", K(ret));
  }
  return ret;
}

int ObRepartSliceIdxCalc::get_partition_id(const common::ObNewRow& row, int64_t& partition_id)
{
  int ret = OB_SUCCESS;
  int64_t part_idx = common::OB_INVALID_INDEX;
  int64_t subpart_idx = common::OB_INVALID_INDEX;
  bool skip_row = false;
  if (OB_ISNULL(repart_func_) || OB_ISNULL(repart_sub_func_) || OB_ISNULL(repart_columns_) ||
      OB_ISNULL(repart_sub_columns_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(shuffle_service_.get_partition_ids(exec_ctx_,
                 table_schema_,
                 row,
                 *repart_func_,
                 *repart_sub_func_,
                 *repart_columns_,
                 *repart_sub_columns_,
                 px_repart_ch_map_,
                 part_idx,
                 subpart_idx,
                 skip_row,
                 repart_type_))) {
    LOG_WARN("fail get part idx", K(ret), K(part_idx), K(subpart_idx));
  } else if (ObShuffleService::NO_MATCH_PARTITION == part_idx || ObShuffleService::NO_MATCH_PARTITION == subpart_idx) {
    partition_id = ObSliceIdxCalc::DEFAULT_CHANNEL_IDX_TO_DROP_ROW;
  } else {
    partition_id = generate_phy_part_id(part_idx, subpart_idx, table_schema_.get_part_level());
    partition_id_ = partition_id;
  }
  return ret;
}

int ObRepartSliceIdxCalc::get_slice_idx(const ObIArray<ObExpr*>& exprs, ObEvalCtx& eval_ctx, int64_t& slice_idx)
{
  int ret = OB_SUCCESS;
  UNUSED(exprs);
  int64_t partition_id = 0;
  int64_t part_idx = common::OB_INVALID_INDEX;
  int64_t subpart_idx = common::OB_INVALID_INDEX;
  partition_id_ = -1;
  if (OB_FAIL(get_partition_id(eval_ctx, partition_id))) {
    LOG_WARN("fail to get partition id", K(ret));
  } else if (ObSliceIdxCalc::DEFAULT_CHANNEL_IDX_TO_DROP_ROW == partition_id) {
    // do nothing
  } else if (PARTITION_LEVEL_TWO == table_schema_.get_part_level()) {
    part_idx = extract_part_idx(partition_id);
    subpart_idx = extract_subpart_idx(partition_id);
  } else {
    part_idx = partition_id;
  }
  if (OB_SUCC(ret)) {
    partition_id_ = partition_id;
    if (OB_FAIL(ObTransmit::get_slice_idx_by_partition_ids(part_idx, subpart_idx, table_schema_, slice_idx))) {
      LOG_WARN("fail to get slice idx by ids", K(part_idx), K(subpart_idx), K(table_schema_));
    }
  }

  return ret;
}

int ObRepartSliceIdxCalc::get_part_id_by_one_level_sub_ch_map(int64_t& part_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObShuffleService::get_part_id_by_ch_map(px_repart_ch_map_, part_id))) {
    LOG_WARN("fail get sub part id", K(ret));
  }
  return ret;
}

int ObRepartSliceIdxCalc::get_sub_part_id_by_one_level_first_ch_map(const int64_t part_id, int64_t& sub_part_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObShuffleService::get_sub_part_id_by_ch_map(px_repart_ch_map_, part_id, sub_part_id))) {
    LOG_WARN("fail get sub part id", K(part_id), K(ret));
  }
  return ret;
}

int ObRepartSliceIdxCalc::get_partition_id(ObEvalCtx& eval_ctx, int64_t& partition_id)
{
  int ret = OB_SUCCESS;
  ObDatum* partition_id_datum = NULL;
  if (OB_REPARTITION_ONE_SIDE_ONE_LEVEL_FIRST == repart_type_) {
    eval_ctx.exec_ctx_.set_partition_id_calc_type(CALC_IGNORE_SUB_PART);
  } else if (OB_REPARTITION_ONE_SIDE_ONE_LEVEL_SUB == repart_type_) {
    int64_t part_id = OB_INVALID_ID;
    if (OB_FAIL(get_part_id_by_one_level_sub_ch_map(part_id))) {
      LOG_WARN("fail to get part id by ch map", K(ret));
    } else {
      eval_ctx.exec_ctx_.set_partition_id_calc_type(CALC_IGNORE_FIRST_PART);
      eval_ctx.exec_ctx_.set_fixed_id(part_id);
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(calc_part_id_expr_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(calc_part_id_expr_), K(ret));
  } else if (OB_FAIL(calc_part_id_expr_->eval(eval_ctx, partition_id_datum))) {
    LOG_WARN("fail to calc part id", K(ret), K(*calc_part_id_expr_));
  } else if (ObExprCalcPartitionId::NONE_PARTITION_ID == (partition_id = partition_id_datum->get_int())) {
    partition_id = ObSliceIdxCalc::DEFAULT_CHANNEL_IDX_TO_DROP_ROW;
  } else if (OB_REPARTITION_ONE_SIDE_ONE_LEVEL_FIRST == repart_type_) {
    int64_t part_id = partition_id;
    int64_t sub_part_id = common::OB_INVALID_ID;
    if (OB_FAIL(get_sub_part_id_by_one_level_first_ch_map(part_id, sub_part_id))) {
      LOG_WARN("fail to get part id by ch map", K(ret));
    } else {
      partition_id = generate_phy_part_id(part_id, sub_part_id, PARTITION_LEVEL_TWO);
    }
  } else if (OB_REPARTITION_ONE_SIDE_ONE_LEVEL_SUB == repart_type_) {
    int64_t part_id = eval_ctx.exec_ctx_.get_fixed_id();
    int64_t sub_part_id = partition_id;
    partition_id = generate_phy_part_id(part_id, sub_part_id, PARTITION_LEVEL_TWO);
  }
  if (OB_SUCC(ret)) {
    partition_id_ = partition_id;
  }
  LOG_DEBUG("repart partition id", K(partition_id));

  return ret;
}

int ObRepartSliceIdxCalc::get_previous_row_partition_id(ObObj& partition_id)
{
  int ret = OB_SUCCESS;
  partition_id.set_int(partition_id_);
  return ret;
}

int ObSlaveMapRepartIdxCalcBase::init()
{
  int ret = OB_SUCCESS;
  // In the case of pkey random, a partition can be processed by all workers on the SQC where it is located,
  // So one partition_id may correspond to multiple task idx,
  // Form the mapping relationship between partition_id -> task_idx_list
  // For example: SQC1 has 3 workers(task1,task2,task3), 2 partitions(p0,p1);
  // SQC2 has 2 workers (task4, task5), 1 partition (p2);
  // will form:
  // p0 : [task1,task2,task3]
  // p1 : [task1,task2,task3]
  // p2 : [task4,task5]
  const ObPxPartChMapArray& part_ch_array = part_ch_info_.part_ch_array_;
  if (OB_FAIL(part_to_task_array_map_.create(max(1, part_ch_array.count()), ObModIds::OB_SQL_PX))) {
    LOG_WARN("fail create part to task array map", "count", part_ch_array.count(), K(ret));
  }

  ARRAY_FOREACH_X(part_ch_array, idx, cnt, OB_SUCC(ret))
  {
    int64_t partition_id = part_ch_array.at(idx).first_;
    int64_t task_idx = part_ch_array.at(idx).second_;
    LOG_DEBUG("get one channel relationship", K(idx), K(cnt), "key", partition_id, "val", task_idx);

    if (OB_ISNULL(part_to_task_array_map_.get(partition_id))) {
      TaskIdxArray task_idx_array;
      if (OB_FAIL(part_to_task_array_map_.set_refactored(partition_id, task_idx_array))) {
        LOG_WARN("push partition id and task idx array to map failed", K(ret), K(partition_id));
      } else {
        LOG_TRACE("add task idx array successfully", K(ret), K(partition_id));
      }
    }

    const TaskIdxArray* task_idx_array = part_to_task_array_map_.get(partition_id);
    if (OB_ISNULL(task_idx_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task idx list is null", K(ret), K(partition_id));
    } else if (OB_FAIL(const_cast<TaskIdxArray*>(task_idx_array)->push_back(task_idx))) {
      LOG_WARN("failed push back task idx to task idx array", K(ret), K(task_idx), K(partition_id));
    } else {
      LOG_TRACE("push task idx to task idx array",
          K(partition_id),
          K(task_idx),
          K(*task_idx_array),
          K(task_idx_array->count()));
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

int ObRepartRandomSliceIdxCalc::init()
{
  return ObSlaveMapRepartIdxCalcBase::init();
}

int ObRepartRandomSliceIdxCalc::destroy()
{
  return ObSlaveMapRepartIdxCalcBase::destroy();
}

int ObRepartRandomSliceIdxCalc::get_slice_idx(const common::ObNewRow& row, int64_t& slice_idx)
{
  int ret = OB_SUCCESS;
  int64_t partition_id = OB_INVALID_INDEX;
  if (part_ch_info_.part_ch_array_.size() <= 0) {
    ret = OB_NOT_INIT;
    LOG_WARN("the size of part task channel map is zero", K(ret));
  } else if (OB_FAIL(ObRepartSliceIdxCalc::get_partition_id(row, partition_id))) {
    LOG_WARN("failed to get partition id", K(ret));
  } else if (OB_FAIL(get_task_idx_by_partition_id(partition_id, slice_idx))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_NO_PARTITION_FOR_GIVEN_VALUE;
      LOG_WARN("can't get the right partition", K(ret), K(partition_id), K(slice_idx));
    }
  }
  return ret;
}

int ObRepartRandomSliceIdxCalc::get_slice_idx(const ObIArray<ObExpr*>& exprs, ObEvalCtx& eval_ctx, int64_t& slice_idx)
{
  int ret = OB_SUCCESS;
  UNUSED(exprs);
  int64_t partition_id = OB_INVALID_INDEX;
  if (part_ch_info_.part_ch_array_.size() <= 0) {
    ret = OB_NOT_INIT;
    LOG_WARN("the size of part task channel map is zero", K(ret));
  } else if (OB_FAIL(ObRepartSliceIdxCalc::get_partition_id(eval_ctx, partition_id))) {
    LOG_WARN("failed to get partition id", K(ret));
  } else if (OB_FAIL(get_task_idx_by_partition_id(partition_id, slice_idx))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_NO_PARTITION_FOR_GIVEN_VALUE;
      LOG_WARN("can't get the right partition", K(ret), K(partition_id), K(slice_idx));
    }
  }
  return ret;
}

int ObRepartRandomSliceIdxCalc::get_task_idx_by_partition_id(int64_t partition_id, int64_t& task_idx)
{
  int ret = OB_SUCCESS;
  if (part_to_task_array_map_.size() <= 0) {
    ret = OB_NOT_INIT;
    LOG_WARN("part to task array is not inited", K(ret));
  } else {
    const TaskIdxArray* task_idx_array = part_to_task_array_map_.get(partition_id);
    if (OB_ISNULL(task_idx_array)) {
      ret = OB_HASH_NOT_EXIST;  // convert to hash error
      LOG_WARN("the task idx array is null", K(ret), K(partition_id));
    } else if (task_idx_array->count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the size of task idx array is zero", K(ret));
    } else {
      static const int64_t min = 0;
      static const int64_t max = INT64_MAX;
      int64_t rand_idx = common::ObRandom::rand(min, max) % task_idx_array->count();
      task_idx = task_idx_array->at(rand_idx);
      LOG_TRACE("get task_idx/slice_idx by random way", K(partition_id), K(task_idx));
    }
  }
  return ret;
}

int ObAffinitizedRepartSliceIdxCalc::get_slice_idx(const common::ObNewRow& row, int64_t& slice_idx)
{
  int ret = OB_SUCCESS;
  int64_t partition_id = OB_INVALID_INDEX;
  if (task_count_ <= 0) {
    ret = OB_NOT_INIT;
    LOG_WARN("task_count not inited", K_(task_count), K(ret));
  } else if (px_repart_ch_map_.size() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid map size, affinity map should not be empty!", K_(task_count), K(ret));
  } else if (OB_FAIL(get_partition_id(row, partition_id))) {
    LOG_WARN("fail to get partition id", K(ret));
  } else if (OB_FAIL(px_repart_ch_map_.get_refactored(partition_id, slice_idx))) {
    if (OB_HASH_NOT_EXIST == ret && unmatch_row_dist_method_ == ObPQDistributeMethod::DROP) {
      slice_idx = ObSliceIdxCalc::DEFAULT_CHANNEL_IDX_TO_DROP_ROW;
      ret = OB_SUCCESS;
    } else if (OB_HASH_NOT_EXIST == ret && unmatch_row_dist_method_ == ObPQDistributeMethod::RANDOM) {
      slice_idx = round_robin_idx_;
      round_robin_idx_++;
      round_robin_idx_ = round_robin_idx_ % task_count_;
      ret = OB_SUCCESS;
    } else if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_NO_PARTITION_FOR_GIVEN_VALUE;
      LOG_WARN("failed get partition", K(ret), K(partition_id), K(slice_idx));
    } else {
      LOG_WARN("fail get affinitized taskid", K(ret), K(partition_id), K_(task_count), K_(unmatch_row_dist_method));
    }
  }
  return ret;
}

int ObAffinitizedRepartSliceIdxCalc::get_slice_idx(
    const ObIArray<ObExpr*>& exprs, ObEvalCtx& eval_ctx, int64_t& slice_idx)
{
  int ret = OB_SUCCESS;
  UNUSED(exprs);
  int64_t partition_id = OB_INVALID_INDEX;
  if (task_count_ <= 0) {
    ret = OB_NOT_INIT;
    LOG_WARN("task_count not inited", K_(task_count), K(ret));
  } else if (px_repart_ch_map_.size() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid map size, affinity map should not be empty!", K_(task_count), K(ret));
  } else if (OB_FAIL(ObRepartSliceIdxCalc::get_partition_id(eval_ctx, partition_id))) {
    LOG_WARN("fail to get partition id", K(ret));
  } else if (OB_FAIL(px_repart_ch_map_.get_refactored(partition_id, slice_idx))) {
    if (OB_HASH_NOT_EXIST == ret && unmatch_row_dist_method_ == ObPQDistributeMethod::DROP) {
      slice_idx = ObSliceIdxCalc::DEFAULT_CHANNEL_IDX_TO_DROP_ROW;
      ret = OB_SUCCESS;
    } else if (OB_HASH_NOT_EXIST == ret && unmatch_row_dist_method_ == ObPQDistributeMethod::RANDOM) {
      slice_idx = round_robin_idx_;
      round_robin_idx_++;
      round_robin_idx_ = round_robin_idx_ % task_count_;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail get affinitized taskid", K(ret), K(partition_id), K_(task_count), K_(unmatch_row_dist_method));
    }
  }

  return ret;
}

int ObRepartSliceIdxCalc::init()
{
  int ret = OB_SUCCESS;
  if (px_repart_ch_map_.created()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("this map has been init twice", K(ret));
  } else if (OB_FAIL(build_repart_ch_map(px_repart_ch_map_))) {
    LOG_WARN("failed to build affi hash map", K(ret));
  }
  return ret;
}

int ObRepartSliceIdxCalc::build_repart_ch_map(ObPxPartChMap& affinity_map)
{
  int ret = OB_SUCCESS;
  const ObPxPartChMapArray& part_ch_array = part_ch_info_.part_ch_array_;
  if (OB_FAIL(affinity_map.create(max(1, part_ch_array.count()), ObModIds::OB_SQL_PX))) {
    LOG_WARN("fail create hashmap", "count", part_ch_array.count(), K(ret));
  }

  ARRAY_FOREACH_X(part_ch_array, idx, cnt, OB_SUCC(ret))
  {
    LOG_DEBUG("map build", K(idx), K(cnt), "key", part_ch_array.at(idx).first_, "val", part_ch_array.at(idx).second_);
    if (OB_FAIL(affinity_map.set_refactored(part_ch_array.at(idx).first_, part_ch_array.at(idx).second_))) {
      LOG_WARN("fail add item to hash map",
          K(idx),
          K(cnt),
          "key",
          part_ch_array.at(idx).first_,
          "val",
          part_ch_array.at(idx).second_,
          K(ret));
    }
  }
  return ret;
}

int ObSlaveMapBcastIdxCalc::get_slice_indexes(const common::ObNewRow& row, SliceIdxArray& slice_idx_array)
{
  int ret = OB_SUCCESS;
  int64_t partition_id = common::OB_INVALID_INDEX;
  slice_idx_array.reuse();
  if (OB_FAIL(get_partition_id(row, partition_id))) {
    LOG_WARN("failed to get_partition_id", K(ret));
  } else if (OB_INVALID_INDEX == partition_id) {
    int64_t drop_idx = ObSliceIdxCalc::DEFAULT_CHANNEL_IDX_TO_DROP_ROW;
    if (OB_FAIL(slice_idx_array.push_back(drop_idx))) {
      LOG_WARN("failed to push back slice idx", K(ret));
    }
  } else {
    const ObPxPartChMapArray& part_ch_array = part_ch_info_.part_ch_array_;
    ARRAY_FOREACH(part_ch_array, idx)
    {
      if (partition_id == part_ch_array.at(idx).first_) {
        if (OB_FAIL(slice_idx_array.push_back(part_ch_array.at(idx).second_))) {
          LOG_WARN("failed to push back slice idx", K(ret));
        }
        LOG_DEBUG("find slice id to trans", K(partition_id), K(part_ch_array.at(idx).second_));
      }
    }
  }
  return ret;
}

int ObAllToOneSliceIdxCalc::get_slice_idx(const ObNewRow& row, int64_t& slice_idx)
{
  UNUSED(row);
  slice_idx = 0;
  return OB_SUCCESS;
}

int ObAllToOneSliceIdxCalc::get_slice_idx(const ObIArray<ObExpr*>& exprs, ObEvalCtx& eval_ctx, int64_t& slice_idx)
{
  UNUSED(exprs);
  UNUSED(eval_ctx);
  slice_idx = 0;
  return OB_SUCCESS;
}

int ObBc2HostSliceIdCalc::get_slice_indexes(const common::ObNewRow& row, SliceIdxArray& slice_idx_array)
{
  UNUSED(row);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(slice_idx_array.count() != host_idx_.count())) {
    slice_idx_array.reuse();
    FOREACH_CNT_X(it, host_idx_, OB_SUCC(ret))
    {
      UNUSED(it);
      if (OB_FAIL(slice_idx_array.push_back(0))) {
        LOG_WARN("array push back failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; i < host_idx_.count(); i++) {
      auto& hi = host_idx_.at(i);
      int64_t idx = hi.begin_ + hi.idx_ % (hi.end_ - hi.begin_);
      slice_idx_array.at(i) = channel_idx_.at(idx);
      hi.idx_++;
    }
  }
  return ret;
}

int ObBc2HostSliceIdCalc::get_slice_indexes(
    const ObIArray<ObExpr*>& exprs, ObEvalCtx& eval_ctx, SliceIdxArray& slice_idx_array)
{
  UNUSED(exprs);
  UNUSED(eval_ctx);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(slice_idx_array.count() != host_idx_.count())) {
    slice_idx_array.reuse();
    FOREACH_CNT_X(it, host_idx_, OB_SUCC(ret))
    {
      UNUSED(it);
      if (OB_FAIL(slice_idx_array.push_back(0))) {
        LOG_WARN("array push back failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; i < host_idx_.count(); i++) {
      auto& hi = host_idx_.at(i);
      int64_t idx = hi.begin_ + hi.idx_ % (hi.end_ - hi.begin_);
      slice_idx_array.at(i) = channel_idx_.at(idx);
      hi.idx_++;
    }
  }
  return ret;
}

int ObRandomSliceIdCalc::get_slice_idx(const common::ObNewRow& row, int64_t& slice_idx)
{
  UNUSED(row);
  int ret = OB_SUCCESS;
  if (slice_cnt_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid slice count", K(ret), K(slice_cnt_));
  } else {
    slice_idx = idx_ % slice_cnt_;
    idx_++;
  }
  return ret;
}

int ObRandomSliceIdCalc::get_slice_idx(const ObIArray<ObExpr*>& exprs, ObEvalCtx& eval_ctx, int64_t& slice_idx)
{
  UNUSED(exprs);
  UNUSED(eval_ctx);
  int ret = OB_SUCCESS;
  if (slice_cnt_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid slice count", K(ret), K(slice_cnt_));
  } else {
    slice_idx = idx_ % slice_cnt_;
    idx_++;
  }
  return ret;
}

int ObBroadcastSliceIdCalc::get_slice_indexes(const common::ObNewRow& row, SliceIdxArray& slice_idx_array)
{
  int ret = OB_SUCCESS;
  UNUSED(row);
  slice_idx_array.reuse();
  for (int64_t i = 0; OB_SUCC(ret) && i < slice_cnt_; ++i) {
    if (OB_FAIL(slice_idx_array.push_back(i))) {
      LOG_WARN("failed to push back i", K(ret));
    }
  }
  return ret;
}

int ObBroadcastSliceIdCalc::get_slice_indexes(
    const ObIArray<ObExpr*>& exprs, ObEvalCtx& eval_ctx, SliceIdxArray& slice_idx_array)
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

uint64_t ObHashSliceIdCalc::obj_hash_value(const ObObj& obj, const ObCollationType cs_type, uint64_t hash_val)
{
  if (ob_is_string_type(obj.get_type())) {
    hash_val = obj.varchar_hash(cs_type, hash_val);
  } else {
    hash_val = obj.hash(hash_val);
  }
  return hash_val;
}

int ObHashSliceIdCalc::calc_hash_value(
    const ObObj& obj, const ObObjType type, const ObCollationType cs_type, uint64_t& hash_val)
{
  int ret = OB_SUCCESS;
  if (obj.is_null()) {
    hash_val = obj.hash(hash_val);
  } else if (obj.get_type() == type) {
    hash_val = obj_hash_value(obj, cs_type, hash_val);
  } else {
    if (!obj_casted_) {
      obj_casted_ = true;
    }
    EXPR_DEFINE_CAST_CTX(*expr_ctx_, CM_NONE);
    ObObj buf_obj;
    const ObObj* res_obj = NULL;
    if (OB_FAIL(ObObjCaster::to_type(type, cast_ctx, obj, buf_obj, res_obj))) {
      LOG_WARN("cast object failed", K(ret), K(obj), K(type));
    } else if (OB_ISNULL(res_obj)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cast obj is NULL", K(ret));
    } else {
      hash_val = obj_hash_value(*res_obj, cs_type, hash_val);
    }
  }
  return ret;
}

int ObHashSliceIdCalc::get_multi_hash_value(const ObNewRow& row, uint64_t& hash_val)
{
  int ret = OB_SUCCESS;
  if (hash_dist_columns_->empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid task count or hash column count", K(ret), K(hash_dist_columns_->count()));
  } else {
    if (!dist_exprs_->empty() || obj_casted_) {
      expr_ctx_->calc_buf_->reset();
      expr_ctx_->row_ctx_.reset();
    }
    FOREACH_CNT_X(col, *hash_dist_columns_, OB_SUCC(ret))
    {
      if (OB_INVALID_INDEX == col->expr_idx_) {
        auto& c = row.get_cell(col->index_);
        if (OB_FAIL(calc_hash_value(c, col->cmp_type_, col->cs_type_, hash_val))) {
          LOG_WARN("calc hash value failed", K(ret), K(*col));
        }
      } else {
        auto expr = dist_exprs_->at(col->expr_idx_);
        if (NULL == expr) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null sql expression found", K(ret));
        } else {
          ObObj obj;
          if (OB_FAIL(expr->calc(*expr_ctx_, row, obj))) {
            LOG_WARN("expression evaluate failed", K(ret), K(row));
          } else {
            if (OB_FAIL(calc_hash_value(obj, col->cmp_type_, col->cs_type_, hash_val))) {
              LOG_WARN("calc hash value failed", K(ret), K(*col));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObHashSliceIdCalc::calc_hash_value(ObEvalCtx& eval_ctx, uint64_t& hash_val)
{
  int ret = OB_SUCCESS;
  ObDatum* datum = nullptr;
  if (OB_ISNULL(hash_dist_exprs_) || OB_ISNULL(hash_funcs_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("hash func and expr not init", K(ret));
  }
  for (int64_t i = 0; i < hash_dist_exprs_->count() && OB_SUCC(ret); ++i) {
    const ObExpr* dist_expr = hash_dist_exprs_->at(i);
    if (OB_FAIL(dist_expr->eval(eval_ctx, datum))) {
      LOG_WARN("failed to eval datum", K(ret));
    } else {
      hash_val = hash_funcs_->at(i).hash_func_(*datum, hash_val);
    }
  }
  return ret;
}

int ObHashSliceIdCalc::get_slice_idx(const ObNewRow& row, int64_t& slice_idx)
{
  int ret = OB_SUCCESS;
  uint64_t hash_val = 0;
  if (OB_FAIL(get_multi_hash_value(row, hash_val))) {
    LOG_WARN("failed to get hash values", K(ret));
  } else {
    slice_idx = hash_val % task_cnt_;
  }
  return ret;
}

int ObHashSliceIdCalc::get_slice_idx(const ObIArray<ObExpr*>&, ObEvalCtx& eval_ctx, int64_t& slice_idx)
{
  int ret = OB_SUCCESS;
  uint64_t hash_val = 0;
  if (OB_FAIL(calc_hash_value(eval_ctx, hash_val))) {
    LOG_WARN("fail calc hash value", K(ret));
  } else {
    slice_idx = hash_val % task_cnt_;
  }
  return ret;
}

int ObSlaveMapPkeyHashIdxCalc::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSlaveMapRepartIdxCalcBase::init())) {
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

int ObSlaveMapPkeyHashIdxCalc::get_slice_idx(const ObIArray<ObExpr*>&, ObEvalCtx& eval_ctx, int64_t& slice_idx)
{
  int ret = OB_SUCCESS;
  int64_t partition_id = OB_INVALID_INDEX;
  if (part_ch_info_.part_ch_array_.size() <= 0) {
    ret = OB_NOT_INIT;
    LOG_WARN("the size of part task channel map is zero", K(ret));
  } else if (OB_FAIL(ObRepartSliceIdxCalc::get_partition_id(eval_ctx, partition_id))) {
    LOG_WARN("failed to get partition id", K(ret));
  } else if (OB_FAIL(get_task_idx_by_partition_id(eval_ctx, partition_id, slice_idx))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_NO_PARTITION_FOR_GIVEN_VALUE;
      LOG_WARN("can't get the right partition", K(ret), K(partition_id), K(slice_idx));
    }
  }
  return ret;
}

int ObSlaveMapPkeyHashIdxCalc::get_task_idx_by_partition_id(
    ObEvalCtx& eval_ctx, int64_t partition_id, int64_t& task_idx)
{
  int ret = OB_SUCCESS;
  uint64_t hash_val = 0;
  if (part_to_task_array_map_.size() <= 0) {
    ret = OB_NOT_INIT;
    LOG_WARN("part to task array is not inited", K(ret));
  } else {
    const TaskIdxArray* task_idx_array = part_to_task_array_map_.get(partition_id);
    if (OB_ISNULL(task_idx_array)) {
      ret = OB_HASH_NOT_EXIST;  // convert to hash error
      LOG_WARN("the task idx array is null", K(ret), K(partition_id));
    } else if (task_idx_array->count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the size of task idx array is zero", K(ret));
    } else if (OB_FAIL(calc_hash_value(eval_ctx, hash_val))) {
      LOG_WARN("fail calc hash value", K(ret));
    } else {
      int64_t hash_idx = hash_val % task_idx_array->count();
      task_idx = task_idx_array->at(hash_idx);
      LOG_TRACE("get task_idx/slice_idx by hash way", K(partition_id), K(hash_val), K(task_idx));
    }
  }
  return ret;
}

int ObSlaveMapPkeyHashIdxCalc::get_slice_idx(const common::ObNewRow& row, int64_t& slice_idx)
{
  int ret = OB_SUCCESS;
  int64_t partition_id = common::OB_INVALID_INDEX;
  uint64_t hash_val = 0;
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (affi_hash_map_.size() < 1) {
    ret = OB_NOT_INIT;
    LOG_WARN("the size of slave map part task channel map is zero", K(ret));
  } else if (OB_FAIL(get_multi_hash_value(row, hash_val))) {
    LOG_WARN("failed to get hash values", K(ret));
  } else if (OB_FAIL(get_partition_id(row, partition_id))) {
    LOG_WARN("failed to get_partition_id", K(ret));
  } else if (OB_INVALID_INDEX == partition_id) {
    slice_idx = ObSliceIdxCalc::DEFAULT_CHANNEL_IDX_TO_DROP_ROW;
  } else {
    ObPxPartChMapItem item;
    const ObPxPartChMapArray& part_ch_array = part_ch_info_.part_ch_array_;
    if (OB_FAIL(affi_hash_map_.get_refactored(partition_id, item))) {
      LOG_WARN("failed to get item", K(ret));
    } else {
      int64_t offset = hash_val % (item.second_ - item.first_);
      slice_idx = part_ch_array.at(item.first_ + offset).second_;
      LOG_DEBUG("get slice indexs",
          K(offset),
          K(item.first_ + offset),
          K(part_ch_array),
          K(item),
          K(slice_idx),
          K(hash_val),
          K(partition_id),
          K(row));
    }
  }
  return ret;
}

int ObSlaveMapPkeyHashIdxCalc::build_affi_hash_map(hash::ObHashMap<int64_t, ObPxPartChMapItem>& affi_hash_map)
{
  int ret = OB_SUCCESS;
  int64_t partition_id = common::OB_INVALID_INDEX_INT64;
  ObPxPartChMapItem item;
  const ObPxPartChMapArray& part_ch_array = part_ch_info_.part_ch_array_;
  if (OB_FAIL(affi_hash_map.create(part_ch_array.count(), common::ObModIds::OB_SQL_PX))) {
    LOG_WARN("failed to create part ch map", K(ret));
  }
  ARRAY_FOREACH(part_ch_array, idx)
  {
    if (common::OB_INVALID_INDEX_INT64 == partition_id) {
      partition_id = part_ch_array.at(idx).first_;
      item.first_ = idx;
    } else if (partition_id != part_ch_array.at(idx).first_) {
      item.second_ = idx;
      if (OB_FAIL(affi_hash_map.set_refactored(partition_id, item))) {
        LOG_WARN("failed to set refactored", K(ret));
      }
      LOG_DEBUG("build affi hash map", K(partition_id), K(item));
      partition_id = part_ch_array.at(idx).first_;
      item.first_ = idx;
    }
  }
  if (OB_SUCC(ret)) {
    item.second_ = part_ch_array.count();
    if (OB_FAIL(affi_hash_map.set_refactored(partition_id, item))) {
      LOG_WARN("failed to set refactored", K(ret));
    }
    LOG_DEBUG("build affi hash map", K(partition_id), K(item));
  }
  return ret;
}

int ObSlaveMapPkeyHashIdxCalc::get_part_id_by_one_level_sub_ch_map(int64_t& part_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObShuffleService::get_part_id_by_ch_map(affi_hash_map_, part_id))) {
    LOG_WARN("fail get sub part id", K(ret));
  }
  return ret;
}

int ObSlaveMapPkeyHashIdxCalc::get_sub_part_id_by_one_level_first_ch_map(const int64_t part_id, int64_t& sub_part_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObShuffleService::get_sub_part_id_by_ch_map(affi_hash_map_, part_id, sub_part_id))) {
    LOG_WARN("fail get sub part id", K(part_id), K(ret));
  }
  return ret;
}
