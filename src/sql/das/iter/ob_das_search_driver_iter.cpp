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

#include "sql/engine/expr/ob_expr.h"
#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/iter/ob_das_search_driver_iter.h"
#include "sql/das/iter/ob_das_profile_iter.h"
#include "sql/das/search/ob_i_das_search_op.h"
#include "sql/das/search/ob_das_scalar_scan_op.h"
#include "sql/das/search/ob_das_search_context.h"
#include "sql/das/search/ob_das_search_utils.h"
#include "sql/ob_sql_context.h"
#include "sql/resolver/dml/ob_hint.h"
#include "lib/utility/ob_sort.h"

namespace oceanbase
{
namespace sql
{

bool ObDASSearchDriverIterParam::is_valid() const
{
  return nullptr != root_op_ && search_ctx_.is_valid() && ObDASIterParam::is_valid();
}

ObDASSearchDriverIterParam::ObDASSearchDriverIterParam(
  ObDASSearchCtx &search_ctx, ObIDASSearchOp *op, const common::ObLimitParam &top_k_limit_param, ObExpr *score_expr, ObVecFilterMode vec_filter_mode)
  : ObDASIterParam(DAS_ITER_SEARCH_DRIVER),
    root_op_(op),
    search_ctx_(search_ctx),
    top_k_limit_param_(top_k_limit_param),
    score_expr_(score_expr),
    vec_filter_mode_(vec_filter_mode)
{
  max_size_ = search_ctx.max_batch_size();
  eval_ctx_ = search_ctx.eval_ctx_;
  output_ = search_ctx.output_;
  if (OB_NOT_NULL(eval_ctx_)) {
    exec_ctx_ = &eval_ctx_->exec_ctx_;
  }
}

int ObDASSearchDriverIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;

  using InitFunc = int (ObDASSearchDriverIter::*)(int64_t);

  static const InitFunc INIT_FUNCS[] = {
    nullptr, // INVALID
    &ObDASSearchDriverIter::init_top_scores_iter,
    &ObDASSearchDriverIter::init_top_docs_iter,
    &ObDASSearchDriverIter::init_complete_docs_iter,
    &ObDASSearchDriverIter::init_post_filter_iter,
    &ObDASSearchDriverIter::init_expr_filter_iter
  };

  static const ProcessRowsFunc PROCESS_FUNCS[] = {
    nullptr, // INVALID
    &ObDASSearchDriverIter::process_top_scores,
    &ObDASSearchDriverIter::process_top_docs,
    &ObDASSearchDriverIter::process_complete_docs,
    &ObDASSearchDriverIter::process_post_filter,
    &ObDASSearchDriverIter::process_expr_filter
  };

  static_assert(ARRAYSIZEOF(INIT_FUNCS) == DAS_SEARCH_TYPE_MAX, "search type count mismatch");
  static_assert(ARRAYSIZEOF(PROCESS_FUNCS) == DAS_SEARCH_TYPE_MAX, "search type count mismatch");

  if (param.type_ != ObDASIterType::DAS_ITER_SEARCH_DRIVER) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner init das iter with bad param type", K(param), K(ret));
  } else {
    ObDASSearchDriverIterParam &driver_param = static_cast<ObDASSearchDriverIterParam &>(param);
    root_op_ = driver_param.root_op_;
    search_ctx_ = &driver_param.search_ctx_;
    top_k_limit_param_ = driver_param.top_k_limit_param_;
    vec_filter_mode_ = driver_param.vec_filter_mode_;
    score_expr_ = driver_param.score_expr_;
    if (OB_FAIL(init_search_type())) {
      LOG_WARN("failed to init search type", K(ret));
    } else if (OB_UNLIKELY(search_type_ <= DAS_SEARCH_TYPE_INVALID || search_type_ >= DAS_SEARCH_TYPE_MAX)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected search type", K(ret), K(search_type_));
    } else {
      InitFunc init_func = INIT_FUNCS[search_type_];
      process_rows_func_ = PROCESS_FUNCS[search_type_];
      if (OB_ISNULL(init_func) || OB_ISNULL(process_rows_func_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr func", K(ret), K(search_type_));
      } else if (OB_FAIL((this->*init_func)(param.max_size_))) {
        LOG_WARN("failed to init search type", K(ret), K(search_type_));
      }
    }
  }
  return ret;
}

int ObDASSearchDriverIter::inner_reuse()
{
  int ret = OB_SUCCESS;
  // no need to reuse search op, processed in rescan logic
  scores_.clear();
  row_ids_.clear();
  rowid_allocator_.reset();

  /* for top scores */
  sort_indices_.clear();
  sort_indices_pos_ = -1;
  is_top_scores_data_ready_ = false;

  /* for top docs */
  input_rows_cnt_ = 0;
  output_rows_cnt_ = 0;

  /* for vec index post filter*/
  bitmap_ = nullptr;
  bitmap_iter_.reset();
  return ret;
}

int ObDASSearchDriverIter::inner_release()
{
  int ret = OB_SUCCESS;
  scores_.reset();
  row_ids_.reset();
  allocator_.reset();
  rowid_allocator_.reset();

  /* for top scores */
  sort_indices_.reset();

  /* for vec index post filter*/
  bitmap_ = nullptr;
  bitmap_iter_.reset();
  return ret;
}

int ObDASSearchDriverIter::do_table_scan()
{
  int ret = OB_SUCCESS;
  common::ObOpProfile<common::ObMetric> *profile = nullptr;
  if (OB_ISNULL(root_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr root op", KR(ret));
  } else if (OB_FAIL(ObDASProfileIter::init_runtime_profile(
          common::ObProfileId::HYBRID_SEARCH_HYBRID_ITER, profile))) {
    LOG_WARN("failed to init runtime profile", KR(ret));
  } else {
    common::ObProfileSwitcher switcher(profile);
    if (OB_FAIL(root_op_->open())) {
      LOG_WARN("failed to open root op", KR(ret));
    }
  }
  return ret;
}

int ObDASSearchDriverIter::rescan()
{
  int ret = OB_SUCCESS;
  common::ObOpProfile<common::ObMetric> *profile = nullptr;
  if (OB_ISNULL(root_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr root op", KR(ret));
  } else if (OB_FAIL(ObDASProfileIter::init_runtime_profile(
          common::ObProfileId::HYBRID_SEARCH_HYBRID_ITER, profile))) {
    LOG_WARN("failed to init runtime profile", KR(ret));
  } else {
    common::ObProfileSwitcher switcher(profile);
    root_op_->reset_profile();
    if (OB_FAIL(root_op_->rescan())) {
      LOG_WARN("failed to rescan root op", KR(ret));
    }
  }
  return ret;
}

int ObDASSearchDriverIter::inner_get_next_row()
{
  return OB_NOT_IMPLEMENT;
}

int ObDASSearchDriverIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(process_rows_func_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("process rows func not init", K(ret));
  } else {
    ret = (this->*process_rows_func_)(count, capacity);
  }
  return ret;
}

int ObDASSearchDriverIter::init_rowid_and_score_vectors(int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(search_ctx_) ||
      OB_ISNULL(search_ctx_->eval_ctx_) ||
      OB_ISNULL(search_ctx_->rowid_exprs_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(search_ctx_));
  } else if (capacity > 0) {
    for (int64_t i = 0; OB_SUCC(ret) && i < search_ctx_->rowid_exprs_->count(); ++i) {
      ObExpr *expr = search_ctx_->rowid_exprs_->at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr expr", K(ret), K(i));
      } else if (OB_FAIL(expr->init_vector_default(*search_ctx_->eval_ctx_, capacity))) {
        LOG_WARN("failed to init vector for rowid expr", K(ret), K(i));
      }
    }
    if (OB_NOT_NULL(score_expr_)) {
      if (OB_FAIL(score_expr_->init_vector(*search_ctx_->eval_ctx_, VEC_FIXED, capacity))) {
        LOG_WARN("failed to init vector for score expr", K(ret));
      }
    }
  }
  return ret;
}

int ObDASSearchDriverIter::set_rowid_and_score_evaluated_projected()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(search_ctx_) ||
      OB_ISNULL(search_ctx_->eval_ctx_) ||
      OB_ISNULL(search_ctx_->rowid_exprs_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(search_ctx_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < search_ctx_->rowid_exprs_->count(); ++i) {
    ObExpr *expr = search_ctx_->rowid_exprs_->at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr expr", K(ret), K(i));
    } else {
      expr->set_evaluated_projected(*search_ctx_->eval_ctx_);
    }
  }
  if (OB_NOT_NULL(score_expr_)) {
    score_expr_->set_evaluated_projected(*search_ctx_->eval_ctx_);
  }
  return ret;
}

int ObDASSearchDriverIter::project_rowid_and_score_to_expr(
  int64_t count, int64_t offset, const ObIArray<int64_t> *indices)
{
  int ret = OB_SUCCESS;
  ObIVector *score_vec = nullptr;
  if ((OB_NOT_NULL(score_expr_) && row_ids_.count() != scores_.count()) ||
      (indices != nullptr && indices->count() != row_ids_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected count mismatch", K(ret), K(row_ids_.count()), K(scores_.count()));
  } else if (count <= 0 || offset < 0 || offset + count > row_ids_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected count or offset", K(ret), K(count), K(offset), K(row_ids_.count()));
  } else if (OB_ISNULL(search_ctx_) ||
             OB_ISNULL(search_ctx_->eval_ctx_) ||
             OB_ISNULL(search_ctx_->rowid_exprs_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(search_ctx_));
  } else if (OB_FAIL(init_rowid_and_score_vectors(count))) {
    LOG_WARN("failed to init rowid and score vectors", K(ret));
  } else if (OB_NOT_NULL(score_expr_) &&
             OB_ISNULL(score_vec = score_expr_->get_vector(*search_ctx_->eval_ctx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr scorevector", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      int64_t idx = indices ? indices->at(offset + i) : offset + i;
      if (OB_FAIL(search_ctx_->rowid_to_expr(row_ids_.at(idx), i))) {
        LOG_WARN("failed to convert rowid to expr", K(ret), K(i));
      } else if (OB_NOT_NULL(score_vec)) {
        score_vec->set_double(i, scores_.at(idx));
      }
    }
    if (FAILEDx(set_rowid_and_score_evaluated_projected())) {
      LOG_WARN("failed to set rowid and score evaluated projected", K(ret));
    }
  }
  return ret;
}

int ObDASSearchDriverIter::fill_rowids_and_scores(int64_t capacity, int64_t &count)
{
  int ret = OB_SUCCESS;
  count = 0;
  row_ids_.clear();
  scores_.clear();
  rowid_allocator_.reset();
  ObDASRowID row_id;
  double score = 0.0;
  while (OB_SUCC(ret) && count < capacity) {
    if (OB_FAIL(root_op_->next_rowid(row_id, score))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to get next rowid", K(ret));
    } else if (OB_FAIL(search_ctx_->deep_copy_rowid(row_id, row_id, rowid_allocator_))) {
      LOG_WARN("failed to deep copy rowid", K(ret));
    } else if (OB_FAIL(row_ids_.push_back(row_id))) {
      LOG_WARN("failed to push back rowid", K(ret));
    } else if (OB_FAIL(scores_.push_back(score))) {
      LOG_WARN("failed to push back score", K(ret));
    } else {
      count += 1;
    }
  }
  if (count > 0 && OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObDASSearchDriverIter::init_search_type()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(search_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr search ctx", K(ret));
  } else if (ObVecFilterMode::VEC_FILTER_MODE_SEARCH_DRIVER_FILTER == vec_filter_mode_) {
    search_type_ = DAS_SEARCH_TYPE_POST_FILTER;
  } else if (ObVecFilterMode::VEC_FILTER_MODE_EXPR_FILTER == vec_filter_mode_) {
    search_type_ = DAS_SEARCH_TYPE_EXPR_FILTER;
  } else if (OB_NOT_NULL(score_expr_) && top_k_limit_param_.is_valid()) {
    search_type_ = DAS_SEARCH_TYPE_TOP_SCORES;
  } else if (top_k_limit_param_.is_valid()) {
    search_type_ = DAS_SEARCH_TYPE_TOP_DOCS;
  } else {
    search_type_ = DAS_SEARCH_TYPE_COMPLETE_DOCS;
  }
  return ret;
}

int ObDASSearchDriverIter::init_top_scores_iter(int64_t max_size)
{
  UNUSED(max_size);
  int ret = OB_SUCCESS;
  if (!top_k_limit_param_.is_valid() ||
      OB_ISNULL(search_ctx_) ||
      OB_ISNULL(score_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid topk param", K(ret), K(top_k_limit_param_), K(search_ctx_));
  } else {
    const int64_t N = top_k_limit_param_.offset_ + top_k_limit_param_.limit_;
    if (OB_FAIL(scores_.reserve(N))) {
      LOG_WARN("failed to reserve scores array", K(ret));
    } else if (OB_FAIL(row_ids_.reserve(N))) {
      LOG_WARN("failed to reserve row ids array", K(ret));
    } else if (OB_FAIL(sort_indices_.reserve(N))) {
      LOG_WARN("failed to reserve sort indices array", K(ret));
    } else {
      is_top_scores_data_ready_ = false;
      sort_indices_pos_ = -1;
    }
  }
  return ret;
}

int ObDASSearchDriverIter::init_top_docs_iter(int64_t max_size)
{
  int ret = OB_SUCCESS;
  if (!top_k_limit_param_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid topk param", K(ret), K(top_k_limit_param_));
  } else if (OB_FAIL(row_ids_.reserve(max_size))) {
    LOG_WARN("failed to reserve row ids array", K(ret));
  } else if (OB_FAIL(scores_.reserve(max_size))) {
    LOG_WARN("failed to reserve scores array", K(ret));
  } else {
    input_rows_cnt_ = 0;
    output_rows_cnt_ = 0;
  }
  return ret;
}

int ObDASSearchDriverIter::init_complete_docs_iter(int64_t max_size)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(scores_.reserve(max_size))) {
    LOG_WARN("failed to reserve scores array", K(ret));
  } else if (OB_FAIL(row_ids_.reserve(max_size))) {
    LOG_WARN("failed to reserve row ids array", K(ret));
  }
  return ret;
}

int ObDASSearchDriverIter::init_post_filter_iter(int64_t max_size)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(row_ids_.reserve(max_size))) {
    LOG_WARN("failed to reserve row ids array", K(ret));
  }
  return ret;
}

int ObDASSearchDriverIter::process_top_scores(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  count = 0;
  if (!is_top_scores_data_ready_) {
    if (OB_FAIL(prepare_and_sort_top_scores_data())) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to prepare and sort top scores data", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(sort_indices_pos_ < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected negative sort indices pos", K(ret), K(sort_indices_pos_));
    } else if (sort_indices_pos_ >= sort_indices_.count()) {
      ret = OB_ITER_END;
    } else {
      capacity = std::min(capacity, sort_indices_.count() - sort_indices_pos_);
      if (OB_FAIL(project_rowid_and_score_to_expr(capacity, sort_indices_pos_, &sort_indices_))) {
        LOG_WARN("failed to project rowid and score to expr", K(ret));
      } else {
        count = capacity;
        sort_indices_pos_ += count;
      }
    }
  }
  return ret;
}

int ObDASSearchDriverIter::process_top_docs(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  count = 0;
  const int64_t offset = top_k_limit_param_.offset_;
  const int64_t limit = top_k_limit_param_.limit_;
  ObDASRowID row_id;
  double score = 0.0;

  if (output_rows_cnt_ >= limit) {
    ret = OB_ITER_END;
  } else {
    capacity = std::min(capacity, limit - output_rows_cnt_);
  }

  // skip offset rows
  while (OB_SUCC(ret) && input_rows_cnt_ < offset) {
    if (OB_FAIL(root_op_->next_rowid(row_id, score))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to get next rowid", K(ret));
    } else {
      input_rows_cnt_++;
    }
  }

  if (OB_ITER_END == ret) {
    // EOF reached before offset satisfied, keep OB_ITER_END and set count to 0
    count = 0;
  } else if (OB_FAIL(ret)) {
    LOG_WARN("failed to process top docs", K(ret));
  } else if (OB_FAIL(fill_rowids_and_scores(capacity, count))) {
    LOG_WARN("failed to fetch rowids and scores", K(ret));
  } else if (OB_FAIL(project_rowid_and_score_to_expr(count))) {
    LOG_WARN("failed to project rowid and score to expr", K(ret));
  } else {
    output_rows_cnt_ += count;
    input_rows_cnt_ += count;
  }
  return ret;
}

int ObDASSearchDriverIter::process_complete_docs(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  count = 0;
  if (OB_ISNULL(root_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr root op", K(ret));
  } else if (DAS_SEARCH_OP_SCALAR_SCAN == root_op_->get_op_type()) {
    // for vector index pre-filtering with only one filter
    ObDASScalarScanOp *scalar_scan_op = static_cast<ObDASScalarScanOp *>(root_op_);
    if (OB_FAIL(scalar_scan_op->get_next_rows(count, capacity))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to get next rows from scalar scan op", K(ret));
    }
  } else if (OB_FAIL(fill_rowids_and_scores(capacity, count))) {
    LOG_WARN("failed to fetch rowids and scores", K(ret));
  } else if (OB_FAIL(project_rowid_and_score_to_expr(count))) {
    LOG_WARN("failed to project rowid and score to expr", K(ret));
  }
  return ret;
}

int ObDASSearchDriverIter::init_bitmap_iter()
{
  int ret = OB_SUCCESS;
  if (!bitmap_iter_.is_inited()) {
    if (OB_ISNULL(bitmap_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("bitmap is null", K(ret));
    } else if (OB_FAIL(bitmap_iter_.init(bitmap_))) {
      LOG_WARN("failed to init bitmap iterator", K(ret));
    }
  }
  return ret;
}

int ObDASSearchDriverIter::process_post_filter(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  ObDASRowID row_id;
  double score = 0.0;
  count = 0;
  scores_.clear();
  row_ids_.clear();
  rowid_allocator_.reuse();

  if (OB_ISNULL(bitmap_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bitmap is null", K(ret));
  } else if (bitmap_->type_ != ObVecIndexBitmap::ROARING_BITMAP) {
    if (OB_FAIL(bitmap_->upgrade_to_roaring_bitmap())) {
      LOG_WARN("failed to upgrade to roaring bitmap", K(ret));
    }
  }

  if (FAILEDx(init_bitmap_iter())) {
    LOG_WARN("failed to init bitmap iterator", K(ret));
  } else if (OB_LIKELY(search_ctx_->use_dynamic_pruning_)) {
    ObDASRowID root_rowid;
    ObDASRowID bitmap_rowid;
    double root_score = 0.0;
    int64_t root_vid = 0;
    int64_t bitmap_vid = 0;

    if (OB_FAIL(init_rowid_and_score_vectors(capacity))) {
      LOG_WARN("failed to init rowid and score vectors", K(ret));
    } else if (OB_FAIL(bitmap_iter_.get_curr_vid(bitmap_vid))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to get current vid from bitmap iterator", K(ret), K(bitmap_vid), K(count));
    } else if (OB_FALSE_IT(bitmap_rowid.set_uint64(bitmap_vid))) {
    } else if (OB_FAIL(root_op_->advance_to(bitmap_rowid, root_rowid, root_score))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to advance root op to bitmap vid", K(ret), K(root_rowid), K(count));
    }

    while (OB_SUCC(ret) && count < capacity) {
      root_vid = root_rowid.get_uint64();
      if (root_vid == bitmap_vid) {
        if (OB_FAIL(search_ctx_->deep_copy_rowid(root_rowid, row_id, rowid_allocator_))) {
          LOG_WARN("failed to deep copy rowid", K(ret));
        } else if (OB_FAIL(row_ids_.push_back(row_id))) {
          LOG_WARN("failed to push back rowid", K(ret));
        } else if (OB_FALSE_IT(count += 1)) {
        } else if (OB_FAIL(bitmap_iter_.next_vid(bitmap_vid))) {
          LOG_WARN_IGNORE_ITER_END(ret, "failed to get next vid from bitmap iterator", K(ret), K(bitmap_vid), K(count));
        } else if (OB_FALSE_IT(bitmap_rowid.set_uint64(bitmap_vid))) {
        } else if (count < capacity && OB_FAIL(root_op_->advance_to(bitmap_rowid, root_rowid, root_score))) {
          LOG_WARN_IGNORE_ITER_END(ret, "failed to advance root op to bitmap vid", K(ret), K(root_rowid), K(count));
        }
      } else if (root_vid > bitmap_vid) {
        if (OB_FAIL(bitmap_iter_.advance_to(root_vid, bitmap_vid))) {
          LOG_WARN_IGNORE_ITER_END(ret, "failed to advance bitmap iterator to root vid", K(ret), K(root_vid), K(count));
        }
      } else {
        bitmap_rowid.set_uint64(bitmap_vid);
        if (OB_FAIL(root_op_->advance_to(bitmap_rowid, root_rowid, root_score))) {
          LOG_WARN_IGNORE_ITER_END(ret, "failed to advance root op to bitmap vid", K(ret), K(root_rowid), K(count));
        }
      }
    }

    if (count > 0 && OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }

    if (FAILEDx(project_rowid_and_score_to_expr(count))) {
      LOG_WARN("failed to set evaluated exprs", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret));
  }

  return ret;
}

int ObDASSearchDriverIter::prepare_and_sort_top_scores_data()
{
  int ret = OB_SUCCESS;
  const int64_t capacity = top_k_limit_param_.offset_ + top_k_limit_param_.limit_;
  int64_t count = 0;
  if (OB_FAIL(fill_rowids_and_scores(capacity, count))) {
    LOG_WARN("failed to fetch rowids and scores", K(ret));
  } else if (count <= top_k_limit_param_.offset_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(init_sort_indices(count))) {
    LOG_WARN("failed to init sort indices", K(ret));
  } else if (OB_UNLIKELY(scores_.count() != sort_indices_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected count mismatch", K(ret), K(scores_.count()), K(sort_indices_.count()));
  } else {
    lib::ob_sort(sort_indices_.begin(), sort_indices_.end(), ScoresComparator(scores_));
    if (OB_SUCC(ret)) {
      is_top_scores_data_ready_ = true;
      sort_indices_pos_ = top_k_limit_param_.offset_;
    }
  }
  return ret;
}

int ObDASSearchDriverIter::init_sort_indices(int64_t count)
{
  int ret = OB_SUCCESS;
  sort_indices_.clear();
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    if (OB_FAIL(sort_indices_.push_back(i))) {
      LOG_WARN("failed to push back sort index", K(ret), K(i));
    }
  }
  return ret;
}

int ObDASSearchDriverIter::init_expr_filter_iter(int64_t max_size)
{
  UNUSED(max_size);
  return OB_SUCCESS;
}

int ObDASSearchDriverIter::process_expr_filter(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  count = 0;
  if (OB_ISNULL(root_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr root op", K(ret));
  } else if (DAS_SEARCH_OP_SCALAR_SCAN == root_op_->get_op_type()) {
    ObDASScalarScanOp *scalar_scan_op = static_cast<ObDASScalarScanOp *>(root_op_);
    if (OB_FAIL(scalar_scan_op->get_next_rows(count, capacity))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to get next rows from scalar scan op", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected op type", K(ret), K(root_op_->get_op_type()));
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
