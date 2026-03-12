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

#ifndef OCEANBASE_SQL_OB_DAS_BMW_OP_H_
#define OCEANBASE_SQL_OB_DAS_BMW_OP_H_

#include "sql/das/search/ob_i_das_search_op.h"
#include "sql/das/search/ob_daat_merge_iter.h"

namespace oceanbase
{
namespace sql
{
class ObIDisjunctiveDAATMerger;

class ObDASBMWOpParam : public ObIDASSearchOpParam
{
public:
  ObDASBMWOpParam(
      const ObIArray<ObIDASSearchOp *> &children,
      const int64_t minimum_should_match,
      ObIAllocator &iter_allocator)
    : ObIDASSearchOpParam(DAS_SEARCH_OP_BMW),
      children_(&children),
      iter_allocator_(&iter_allocator),
      minimum_should_match_(minimum_should_match) {}
  virtual ~ObDASBMWOpParam() {}

  bool is_valid() const
  {
    return nullptr != children_ && nullptr != iter_allocator_;
  }
  OB_INLINE const ObIArray<ObIDASSearchOp *> *get_children() const { return children_; }
  OB_INLINE int64_t get_minimum_should_match() const { return minimum_should_match_; }
  OB_INLINE ObIAllocator *get_iter_allocator() const { return iter_allocator_; }
  int get_children_ops(ObIArray<ObIDASSearchOp *> &children) const override;
  INHERIT_TO_STRING_KV("ObDASBMWOpParam", ObIDASSearchOpParam,
      KPC_(children),
      KP_(iter_allocator),
      K_(minimum_should_match));
private:
  const ObIArray<ObIDASSearchOp *> *children_;
  ObIAllocator *iter_allocator_;
  int64_t minimum_should_match_;
};

// Block Max WAND Operator
class ObDASBMWOp : public ObIDASSearchOp
{
public:
  ObDASBMWOp(ObDASSearchCtx &search_ctx);
  virtual ~ObDASBMWOp() {}

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

private:
  int init_merge_iters(const ObDASBMWOpParam &op_param, ObIAllocator &iter_allocator);
  int inner_get_next_row_plain(ObDASRowID &next_id, double &score);
  int inner_get_next_row_with_pruning(ObDASRowID &next_id, double &score);
  int find_next_pivot(ObDASRowID &pivot_id);
  int evaluate_pivot_range(const ObDASRowID &pivot_id, bool &is_candidate);
  int evaluate_pivot(const ObDASRowID &pivot_id, ObDASRowID &curr_id, double &score, int64_t &dim_cnt);
  int find_next_pivot_range();
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
  ObIAllocator *iter_allocator_; // long-lifetime allocator
  ObFixedArray<ObDAATMergeIter *, ObIAllocator> merge_iters_;
  ObIDisjunctiveDAATMerger *row_merger_;
  SearchStatus status_;
  int64_t minimum_should_match_;
  bool is_inited_;
};

} // namespace sql
} // namespace oceanbase
#endif
