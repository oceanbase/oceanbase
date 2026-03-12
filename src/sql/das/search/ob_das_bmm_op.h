/**
 * Copyright (c) 2025 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SQL_OB_DAS_BMM_OP_H_
#define OCEANBASE_SQL_OB_DAS_BMM_OP_H_

#include "sql/das/search/ob_i_das_search_op.h"
#include "sql/das/search/ob_daat_merge_iter.h"


namespace oceanbase
{
namespace sql
{
class ObIDisjunctiveDAATMerger;

class ObDASBMMOpParam : public ObIDASSearchOpParam
{
public:
  ObDASBMMOpParam(
      const ObIArray<ObIDASSearchOp *> &score_ops,
      bool query_optional,
      ObIDASSearchOp *filter_op,
      ObIAllocator &iter_allocator)
    : ObIDASSearchOpParam(DAS_SEARCH_OP_BMM),
      score_ops_(&score_ops),
      filter_op_(filter_op),
      iter_allocator_(&iter_allocator),
      query_optional_(query_optional) {}
  virtual ~ObDASBMMOpParam() {}
  INHERIT_TO_STRING_KV("ObDASBMMOpParam", ObIDASSearchOpParam,
      KPC_(score_ops),
      KP_(filter_op),
      KP_(iter_allocator),
      K_(query_optional));

  bool is_valid() const
  {
    return nullptr != score_ops_ && nullptr != iter_allocator_;
  }
  OB_INLINE const ObIArray<ObIDASSearchOp *> *get_score_ops() const { return score_ops_; }
  OB_INLINE ObIDASSearchOp *get_filter_op() const { return filter_op_; }
  OB_INLINE bool has_filter_op() const { return nullptr != filter_op_; }
  OB_INLINE ObIAllocator *get_iter_allocator() const { return iter_allocator_; }
  OB_INLINE bool query_optional() const { return query_optional_; }
  int get_children_ops(ObIArray<ObIDASSearchOp *> &children) const override;
private:
  const ObIArray<ObIDASSearchOp *> *score_ops_;
  ObIDASSearchOp *filter_op_;
  ObIAllocator *iter_allocator_;
  bool query_optional_;
};

// Block Max MaxScore top-k dynamic pruning operator
class ObDASBMMOp : public ObIDASSearchOp
{
public:
  ObDASBMMOp(ObDASSearchCtx &search_ctx);
  virtual ~ObDASBMMOp() {}

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

private:
  bool has_filter() const { return nullptr != filter_op_; }
  double get_essential_dim_threshold() const { return min_competitive_score_ - non_essential_dim_max_score_; }
  int init_merge_iters(const ObDASBMMOpParam &op_param, ObIAllocator &iter_allocator);
  int inner_get_next_row_plain(ObDASRowID &next_id, double &score);
  int inner_get_next_row_with_pruning(ObDASRowID &next_id, double &score);
  int sort_iters_by_max_score();
  int try_update_essential_dims();
  int evaluate_pivot_range(const ObDASRowID &pivot_id, double &non_essential_block_max_score, bool &is_candidate);
  int evaluate_essential_pivot(
      const ObDASRowID &pivot_id,
      const double non_essential_block_max_score,
      ObDASRowID &collected_id,
      double &essential_score,
      bool &is_candidate);
  int evaluate_pivot(const ObDASRowID &pivot_id, const double &essential_score, double &score);
  int next_filter_rowid(ObDASRowID &next_id);
  int advance_filter_to(const ObDASRowID &target, ObDASRowID &curr_id);
  int get_next_row_plain_with_filter(ObDASRowID &next_id, double &score);
  int find_next_pivot_with_filter(ObDASRowID &pivot_id);
  int evaluate_bmm_pivot(
      const ObDASRowID &pivot_id,
      const double &non_essential_bm_score,
      ObDASRowID &next_id,
      double &score,
      bool &found_row);
private:
  enum SearchStatus {
    FIND_NEXT_PIVOT,
    EVALUATE_PIVOT_RANGE,
    EVALUATE_PIVOT,
    FIND_NEXT_PIVOT_RANGE,
    FOUND_NEXT_ROW,
    FINISHED,
    MAX_STATUS,
  };
private:
  ObIAllocator *iter_allocator_;
  ObFixedArray<ObDAATMergeIter *, ObIAllocator> merge_iters_;
  ObFixedArray<int64_t, ObIAllocator> sorted_iter_idxes_;
  int64_t non_essential_dim_count_;
  double non_essential_dim_max_score_;
  double non_essential_dim_threshold_;
  ObIDisjunctiveDAATMerger *row_merger_;
  ObIDASSearchOp *filter_op_;
  ObDASRowID curr_filter_id_;
  SearchStatus status_;
  bool query_optional_;
  bool iters_sorted_;
  bool filter_end_;
  bool is_inited_;
};



} // namespace sql
} // namespace oceanbase
#endif
