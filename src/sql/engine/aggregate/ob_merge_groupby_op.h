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

#ifndef OCEANBASE_BASIC_OB_MERGE_GROUPBY_OP_H_
#define OCEANBASE_BASIC_OB_MERGE_GROUPBY_OP_H_

#include "common/row/ob_row_store.h"
#include "sql/engine/aggregate/ob_groupby_op.h"
#include "sql/engine/aggregate/ob_aggregate_function.h"

namespace oceanbase {
namespace sql {
class ObMergeGroupBySpec : public ObGroupBySpec {
  OB_UNIS_VERSION_V(1);

public:
  ObMergeGroupBySpec(common::ObIAllocator& alloc, const ObPhyOperatorType type)
      : ObGroupBySpec(alloc, type),
        group_exprs_(alloc),
        rollup_exprs_(alloc),
        is_distinct_rollup_expr_(alloc),
        has_rollup_(false)
  {}

  DECLARE_VIRTUAL_TO_STRING;
  inline void set_rollup(const bool has_rollup)
  {
    has_rollup_ = has_rollup;
  }
  inline int init_group_exprs(const int64_t count)
  {
    return group_exprs_.init(count);
  }
  inline int init_rollup_exprs(const int64_t count)
  {
    return rollup_exprs_.init(count);
  }
  inline int init_distinct_rollup_expr(const int64_t count)
  {
    return is_distinct_rollup_expr_.init(count);
  }
  int add_group_expr(ObExpr* expr);
  int add_rollup_expr(ObExpr* expr);

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObMergeGroupBySpec);

public:
  ExprFixedArray group_exprs_;   // group by column
  ExprFixedArray rollup_exprs_;  // rollup column
  common::ObFixedArray<bool, common::ObIAllocator> is_distinct_rollup_expr_;
  bool has_rollup_;
};

// The input data has been sorted according to the groupby column
class ObMergeGroupByOp : public ObGroupByOp {
public:
  ObMergeGroupByOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input)
      : ObGroupByOp(exec_ctx, spec, input),
        is_end_(false),
        cur_output_group_id_(common::OB_INVALID_INDEX),
        first_output_group_id_(0),
        last_child_output_(aggr_processor_.get_aggr_alloc()),
        curr_groupby_datums_(aggr_processor_.get_aggr_alloc())
  {}
  void reset();
  virtual int inner_open() override;
  virtual int inner_close() override;
  virtual int rescan() override;
  virtual int switch_iterator() override;
  virtual int inner_get_next_row() override;
  virtual void destroy() override;
  int init();
  int prepare_curr_groupby_datum();
  int check_same_group(int64_t& diff_pos);
  int restore_groupby_datum(const int64_t diff_pos);
  int rollup_and_calc_results(const int64_t group_id, const ObExpr* diff_expr = NULL);
  int rewrite_rollup_column(ObExpr*& diff_expr);

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObMergeGroupByOp);

private:
  bool is_end_;
  // added to support groupby with rollup
  int64_t cur_output_group_id_;
  int64_t first_output_group_id_;
  ObChunkDatumStore::LastStoredRow<> last_child_output_;
  DatumFixedArray curr_groupby_datums_;
};

}  // end namespace sql
}  // end namespace oceanbase

#endif  // OCEANBASE_BASIC_OB_MERGE_GROUPBY_OP_H_
