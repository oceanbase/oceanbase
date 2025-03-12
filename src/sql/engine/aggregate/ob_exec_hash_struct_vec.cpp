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

#include "ob_exec_hash_struct_vec.h"
#include "sql/engine/aggregate/ob_hash_groupby_vec_op.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ShortStringAggregator::fallback_calc_hash_value_batch(const common::ObIArray<ObExpr *> &gby_exprs,
                                                          const ObBatchRows &child_brs,
                                                          ObEvalCtx &eval_ctx,
                                                          uint64_t *hash_values)
{
  int ret = OB_SUCCESS;
  uint64_t *new_hash_values = hash_values;
  uint64_t seed = ObHashGroupByVecOp::HASH_SEED;
  //firstly calc hash value for batch
  for (int64_t i = 0; OB_SUCC(ret) && i < gby_exprs.count(); ++i) {
    ObExpr *expr = gby_exprs.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid expr", K(ret), K(i));
    } else {
      const bool is_batch_seed = (i > 0);
      ObIVector *col_vec = expr->get_vector(eval_ctx);
      if (OB_FAIL(col_vec->murmur_hash_v3(*expr, new_hash_values, *child_brs.skip_,
                                          EvalBound(child_brs.size_, child_brs.all_rows_active_),
                                          is_batch_seed ? new_hash_values : &seed, is_batch_seed))) {
        LOG_WARN("failed to calc hash value", K(ret));
      }
    }
  }
  return ret;
}

int BatchAggrRowsTable::init(int64_t max_batch_size, ObIAllocator &alloc/*arena allocator*/)
{
  int ret = OB_SUCCESS;
  max_batch_size_ = max_batch_size;
  if (OB_ISNULL(aggr_rows_ = static_cast<char **> (alloc.alloc(sizeof(char *)
                                                   * MAX_REORDER_GROUPS)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_ISNULL(selectors_ = static_cast<uint16_t **> (alloc.alloc(sizeof(uint16_t *)
                                                              * MAX_REORDER_GROUPS)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_ISNULL(selectors_item_cnt_ = static_cast<uint16_t *> (alloc.alloc(sizeof(uint16_t)
                                                                  * MAX_REORDER_GROUPS)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < MAX_REORDER_GROUPS; ++i) {
      if (OB_ISNULL(selectors_[i] = static_cast<uint16_t *> (alloc.alloc(sizeof(uint16_t)
                                                             * max_batch_size_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to alloc memory for batch aggr rows table", K(ret), K(max_batch_size_));
  }
  return ret;
}

ObCompactRow &call_processor_get_groupby_stored_row_to_avoid_cycle_include(const RowMeta &row_meta,
                                                                           const share::aggregate::AggrRowPtr agg_row)
{
  ObCompactRow &srow = const_cast<ObCompactRow &> (share::aggregate::Processor::get_groupby_stored_row(row_meta, agg_row));
  return srow;
}

}//ns sql
}//ns oceanbase