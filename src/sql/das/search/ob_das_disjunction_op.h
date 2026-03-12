/**
 * Copyright (c) 2024 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */


/*
 * Overview
 * - Disjunction search operator.
 *
 * Key Responsibilities
 * -
*/

#ifndef OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_DISJUNCTION_OP_H_
#define OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_DISJUNCTION_OP_H_

#include "sql/das/search/ob_i_das_search_op.h"
#include "sql/das/search/ob_daat_merge_iter.h"
#include "sql/engine/basic/ob_temp_row_store.h"
#include "sql/engine/basic/ob_temp_column_store.h"

namespace oceanbase
{
namespace sql
{

class ObIDisjunctiveDAATMerger;

class ObDASDisjunctionOpParam : public ObIDASSearchOpParam
{
public:
  ObDASDisjunctionOpParam(
      const ObIArray<ObIDASSearchOp *> &optional_ops,
      int64_t minimum_should_match,
      bool use_max_score, // false for (regular) disjunction, true for disjunction max
      ObDASSearchCost lead_cost)
    : ObIDASSearchOpParam(DAS_SEARCH_OP_DISJUNCTION),
      optional_ops_(&optional_ops),
      minimum_should_match_(minimum_should_match),
      use_max_score_(use_max_score),
      lead_cost_(lead_cost) {}
  ~ObDASDisjunctionOpParam() {}
  OB_INLINE const ObIArray<ObIDASSearchOp *> *get_optional_ops() const { return optional_ops_; }
  OB_INLINE int64_t get_minimum_should_match() const { return minimum_should_match_; }
  OB_INLINE bool get_use_max_score() const { return use_max_score_; }
  OB_INLINE ObDASSearchCost get_lead_cost() const { return lead_cost_; }
  int get_children_ops(ObIArray<ObIDASSearchOp *> &children) const override;
  INHERIT_TO_STRING_KV("ObDASDisjunctionOpParam", ObIDASSearchOpParam,
      KPC_(optional_ops),
      K_(minimum_should_match),
      K_(use_max_score),
      K(lead_cost_.cost()));
private:
  const ObIArray<ObIDASSearchOp *> *optional_ops_;
  int64_t minimum_should_match_;
  bool use_max_score_;
  ObDASSearchCost lead_cost_;
};

class ObDASDisjunctionOp : public ObIDASSearchOp
{
public:
  ObDASDisjunctionOp(ObDASSearchCtx &search_ctx);
  virtual ~ObDASDisjunctionOp() {}

private:
  int do_init(const ObIDASSearchOpParam &op_param) override;
  int do_open() override;
  int do_close() override;
  int do_rescan() override;
  int do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score) override;
  int do_next_rowid(ObDASRowID &next_id, double &score) override;
  int do_advance_shallow(
      const ObDASRowID &target,
      const bool inclusive,
      const MaxScoreTuple *&max_score_tuple) override;
  int do_set_min_competitive_score(const double &threshold) override;
  int do_calc_max_score(double &threshold) override;

private:
  int init_merge_iters(const ObDASDisjunctionOpParam &op_param, ObIAllocator &iter_allocator);
  int inner_get_next_row(ObDASRowID &next_id, double &score);
private:
  ObIDisjunctiveDAATMerger *row_merger_;
  ObFixedArray<ObDAATMergeIter *, ObIAllocator> merge_iters_;
  ObDASSearchCost lead_cost_;
  int64_t minimum_should_match_;
  MaxScoreTuple max_score_tuple_;
  bool need_score_;
  bool iter_end_;
  bool is_inited_;
};

} // namespace sql
} // namespace oceanbase

#endif // OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_DISJUNCTION_OP_H_
