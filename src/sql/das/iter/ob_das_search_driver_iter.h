/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OBDEV_SRC_SQL_DAS_ITER_OB_DAS_SEARCH_DRIVER_ITER_H_
#define OBDEV_SRC_SQL_DAS_ITER_OB_DAS_SEARCH_DRIVER_ITER_H_

#include "sql/das/iter/ob_das_iter.h"
#include "share/diagnosis/ob_runtime_profile.h"
#include "sql/das/iter/ob_das_vec_index_scan_iter.h"
#include "sql/das/iter/ob_das_vec_index_driver_iter.h"

namespace oceanbase
{
namespace sql
{

class ObIDASSearchOp;
class ObDASSearchCtx;
class ObDASRowID;

struct ObDASSearchDriverIterParam : public ObDASIterParam
{
public:
  explicit ObDASSearchDriverIterParam(
    ObDASSearchCtx &search_ctx,
    ObIDASSearchOp *op,
    const common::ObLimitParam &top_k_limit_param,
    ObExpr *score_expr = nullptr,
    ObVecFilterMode vec_filter_mode = ObVecFilterMode::VEC_FILTER_MODE_INVALID);

  ObIDASSearchOp *root_op_;
  ObDASSearchCtx &search_ctx_;
  const common::ObLimitParam top_k_limit_param_;
  ObExpr *score_expr_;
  ObVecFilterMode vec_filter_mode_;
  virtual bool is_valid() const override;
};

enum ObDASSearchType
{
  DAS_SEARCH_TYPE_INVALID = 0,
  // Top-K score retrieval, the primary scenario for hybrid search.
  DAS_SEARCH_TYPE_TOP_SCORES,
  // Retrieve Top-K docs sorted by non-score fields. Currently only supports `doc_id` ordering,
  // used for pure filter scenarios with limit.
  DAS_SEARCH_TYPE_TOP_DOCS,
  // Retrieve all docs, used for vector index pre-filtering & Analytical Processing scenarios.
  DAS_SEARCH_TYPE_COMPLETE_DOCS,
  // Retrieve docs based on input bitmap, used for vector index post-filtering scenarios.
  DAS_SEARCH_TYPE_POST_FILTER,
  DAS_SEARCH_TYPE_EXPR_FILTER,
  DAS_SEARCH_TYPE_MAX
};

class ObDASSearchDriverIter : public ObDASIter
{
public:
  ObDASSearchDriverIter()
    : ObDASIter(ObDASIterType::DAS_ITER_SEARCH_DRIVER),
      root_op_(nullptr),
      search_ctx_(nullptr),
      allocator_(common::ObMemAttr(MTL_ID(), "SearchDriverRes")),
      rowid_allocator_(common::ObMemAttr(MTL_ID(), "SearchDriverRow")),
      scores_(&allocator_),
      row_ids_(&allocator_),
      top_k_limit_param_(),
      search_type_(DAS_SEARCH_TYPE_INVALID),
      vec_filter_mode_(ObVecFilterMode::VEC_FILTER_MODE_INVALID),
      process_rows_func_(nullptr),
      sort_indices_(&allocator_),
      sort_indices_pos_(-1),
      is_top_scores_data_ready_(false),
      input_rows_cnt_(0),
      output_rows_cnt_(0),
      bitmap_(nullptr),
      bitmap_iter_(),
      score_expr_(nullptr)
  {}
  virtual ~ObDASSearchDriverIter() {}

public:
  virtual int do_table_scan() override;
  virtual int rescan() override;

  void set_bitmap(ObVecIndexBitmap *bitmap) { bitmap_ = bitmap; }
  ObDASSearchOpType get_root_op_type() const { return root_op_->get_op_type(); }
  ObIDASSearchOp *get_root_op() { return root_op_; }

protected:
  virtual int inner_init(ObDASIterParam &param) override;
  virtual int inner_reuse() override;
  virtual int inner_release() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override;

private:
  int init_bitmap_iter();

private:
  using ProcessRowsFunc = int (ObDASSearchDriverIter::*)(int64_t &count, int64_t capacity);

  /* INIT FUNCTIONS */
  int init_search_type();
  int init_top_scores_iter(int64_t max_size);
  int init_top_docs_iter(int64_t max_size);
  int init_complete_docs_iter(int64_t max_size);
  int init_post_filter_iter(int64_t max_size);
  int init_expr_filter_iter(int64_t max_size);

  /* PROCESS FUNCTIONS */
  int process_top_scores(int64_t &count, int64_t capacity);
  int process_top_docs(int64_t &count, int64_t capacity);
  int process_complete_docs(int64_t &count, int64_t capacity);
  int process_post_filter(int64_t &count, int64_t capacity);
  int process_expr_filter(int64_t &count, int64_t capacity);

  // init vectors for rowid and score expressions, always use VEC_FIXED for scores.
  int init_rowid_and_score_vectors(int64_t capacity);
  int set_rowid_and_score_evaluated_projected();

  // fetch and fill rowids and scores from root op
  int fill_rowids_and_scores(int64_t capacity, int64_t &count);
  // project rowids and scores from row_ids_/scores_ to exprs, offset and indices are optional.
  int project_rowid_and_score_to_expr(
    int64_t count,
    int64_t offset = 0,
    const common::ObIArray<int64_t> *indices = nullptr);

  // prepare and sort top scores data, used only for top scores.
  int prepare_and_sort_top_scores_data();
  int init_sort_indices(int64_t count);

private:
  // Functor class for comparing docs by scores.
  class ScoresComparator
  {
  public:
    explicit ScoresComparator(const common::ObIArray<double> &scores) : scores_(scores) {}
    bool operator()(int64_t i, int64_t j) const
    {
      return scores_.at(i) > scores_.at(j);
    }
  private:
    const common::ObIArray<double> &scores_;
  };

private:
  ObIDASSearchOp *root_op_;
  ObDASSearchCtx *search_ctx_;
  ObArenaAllocator allocator_;
  // Allocator specifically for rowid deep copies.
  // Must be reset timely to prevent memory expansion since rowid memory is not reusable.
  ObArenaAllocator rowid_allocator_;
  ObFixedArray<double, ObIAllocator> scores_;
  ObFixedArray<ObDASRowID, ObIAllocator> row_ids_;
  common::ObLimitParam top_k_limit_param_;
  ObDASSearchType search_type_;
  ObVecFilterMode vec_filter_mode_;
  ProcessRowsFunc process_rows_func_;

  /* for top scores */
  ObFixedArray<int64_t, ObIAllocator> sort_indices_;
  int64_t sort_indices_pos_;
  bool is_top_scores_data_ready_;

  /* for top docs */
  int64_t input_rows_cnt_;
  int64_t output_rows_cnt_;

  /* for vec index post filter*/
  ObVecIndexBitmap *bitmap_;
  ObVecIndexBitmapIter bitmap_iter_;

  ObExpr *score_expr_;
};

}  // namespace sql
}  // namespace oceanbase



#endif /* OBDEV_SRC_SQL_DAS_ITER_OB_DAS_SEARCH_DRIVER_ITER_H_ */
