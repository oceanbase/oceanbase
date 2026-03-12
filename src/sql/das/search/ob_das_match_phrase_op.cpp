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

#define USING_LOG_PREFIX SQL_DAS

#include "plugin/sys/ob_plugin_helper.h"
#include "ob_das_match_phrase_op.h"
#include "ob_das_token_op.h"
#include "storage/retrieval/ob_phrase_match_counter.h"
#include "share/ob_fts_index_builder_util.h"
#include "share/ob_fts_pos_list_codec.h"

namespace oceanbase
{
namespace sql
{

ObDASMatchPhraseOp::ObDASMatchPhraseOp(ObDASSearchCtx &search_ctx)
  : ObIDASSearchOp(search_ctx),
    allocator_(nullptr),
    ir_ctdef_(nullptr),
    ir_rtdef_(nullptr),
    token_ids_(),
    token_helpers_(),
    token_iters_(),
    estimator_(),
    bm25_param_estimated_(false),
    total_token_weight_(-1.0),
    decoder_(nullptr),
    counter_(nullptr),
    boost_(0.0),
    use_rich_format_(false),
    curr_id_(),
    is_inited_(false)
{}

int ObDASMatchPhraseOp::do_init(const ObIDASSearchOpParam &op_param)
{
  int ret = OB_SUCCESS;
  const ObDASMatchPhraseOpParam &param = static_cast<const ObDASMatchPhraseOpParam &>(op_param);
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double initialization", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param", K(ret));
  } else {
    allocator_ = param.allocator_;
    ir_ctdef_ = param.ir_ctdef_;
    ir_rtdef_ = param.ir_rtdef_;
    counter_ = param.counter_;
    boost_ = param.boost_;
    use_rich_format_ = param.use_rich_format_;
    token_ids_.set_allocator(allocator_);
    token_helpers_.set_allocator(allocator_);
    token_iters_.set_allocator(allocator_);
    if (OB_ISNULL(decoder_ = OB_NEWx(ObFTSPositionListStore, allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to create pos list store", K(ret));
    } else if (OB_FAIL(token_ids_.assign(param.token_ids_))) {
      LOG_WARN("failed to assign token id array", K(ret));
    } else if (OB_FAIL(token_helpers_.init(param.query_tokens_.count()))) {
      LOG_WARN("failed to init token helper array", K(ret));
    } else if (OB_FAIL(token_helpers_.prepare_allocate(param.query_tokens_.count()))) {
      LOG_WARN("failed to prepare allocate token helper array", K(ret));
    } else if (OB_FAIL(token_iters_.init(param.query_tokens_.count()))) {
      LOG_WARN("failed to init token iter array", K(ret));
    } else if (OB_FAIL(token_iters_.prepare_allocate(param.query_tokens_.count()))) {
      LOG_WARN("failed to prepare allocate token iter array", K(ret));
    } else {
      ObDASTokenOpParam token_param;
      token_param.ir_ctdef_ = ir_ctdef_;
      token_param.ir_rtdef_ = ir_rtdef_;
      token_param.block_max_param_ = param.block_max_param_;
      token_param.token_boost_ = 1.0;
      token_param.use_rich_format_ = use_rich_format_;
      for (int64_t i = 0; OB_SUCC(ret) && i < param.query_tokens_.count(); ++i) {
        token_param.query_token_ = param.query_tokens_.at(i);
        if (OB_ISNULL(token_helpers_[i] = OB_NEWx(ObDASTokenOpHelper, allocator_, search_ctx_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to create token helper", K(ret));
        } else if (OB_FAIL(token_helpers_[i]->init(token_param))) {
          LOG_WARN("failed to init token helper", K(ret));
        }
      }
    }
    if (FAILEDx(token_helpers_[0]->init_bm25_param_estimator(estimator_))) {
      LOG_WARN("failed to init bm25 param estimator", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

int ObDASMatchPhraseOp::do_open()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < token_helpers_.count(); ++i) {
    if (OB_ISNULL(token_iters_[i] = OB_NEWx(ObTextRetrievalBlockMaxIter, allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to create token iter", K(ret));
    } else if (OB_FAIL(token_helpers_[i]->init_text_retrieval_iter(*token_iters_[i]))) {
      LOG_WARN("failed to open token iter", K(ret));
    }
  }
  return ret;
}

int ObDASMatchPhraseOp::do_rescan()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < token_helpers_.count(); ++i) {
    if (OB_FAIL(token_helpers_[i]->rescan())) {
      LOG_WARN("failed to rescan token iter", K(ret));
    } else {
      token_iters_[i]->reuse();
    }
  }
  bm25_param_estimated_ = false;
  total_token_weight_ = -1.0;
  return ret;
}

int ObDASMatchPhraseOp::do_close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < token_helpers_.count(); ++i) {
    if (OB_NOT_NULL(token_helpers_[i]) && OB_FAIL(token_helpers_[i]->close())) {
      LOG_WARN("failed to close token iter", K(ret));
    }
    token_helpers_[i] = nullptr;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < token_iters_.count(); ++i) {
    if (OB_NOT_NULL(token_iters_[i])) {
      token_iters_[i]->reset();
    }
    token_iters_[i] = nullptr;
  }
  token_ids_.reset();
  token_helpers_.reset();
  token_iters_.reset();
  estimator_.reset();
  if (OB_NOT_NULL(counter_)) {
    counter_->reset();
    counter_ = nullptr;
  }
  if (OB_NOT_NULL(allocator_)) {
    allocator_->reset();
    allocator_ = nullptr;
  }
  is_inited_ = false;
  return ret;
}

int ObDASMatchPhraseOp::find_intersection(ObDASRowID &rowid, double &score)
{
  int ret = OB_SUCCESS;
  const ObDatum *curr_id = nullptr;
  bool got_result = false;
  while (OB_SUCC(ret) && !got_result) {
    if (OB_FAIL(token_iters_[0]->get_curr_id(curr_id))) {
      LOG_WARN("failed to get curr id", K(ret));
    }
    got_result = true;
    for (int64_t i = 1; OB_SUCC(ret) && got_result && i < token_iters_.count(); ++i) {
      const ObDatum *temp_id = nullptr;
      if (OB_FAIL(token_iters_[i]->advance_to(*curr_id))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to advance secondary token iter", K(ret));
        }
      } else if (OB_FAIL(token_iters_[i]->get_curr_id(temp_id))) {
        LOG_WARN("failed to get curr id", K(ret));
      } else if (OB_UNLIKELY(!ObDatum::binary_equal(*curr_id, *temp_id))) {
        got_result = false;
        if (OB_FAIL(token_iters_[0]->advance_to(*temp_id))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("failed to advance primary token iter", K(ret));
          }
        }
      }
    }
    if (OB_FAIL(ret) || !got_result) {
    } else if (OB_FAIL(evaluate(score))) {
      LOG_WARN("failed to evaluate", K(ret));
    } else if (0 == score) {
      got_result = false;
      if (OB_FAIL(token_iters_[0]->get_next_row())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to get next row", K(ret));
        }
      }
    }
  }
  if (FAILEDx(write_datum_to_rowid(*curr_id, curr_id_, *allocator_))) {
    LOG_WARN("failed to write curr id to rowid", K(ret));
  } else {
    rowid = curr_id_;
  }
  return ret;
}

int ObDASMatchPhraseOp::evaluate(double &score)
{
  int ret = OB_SUCCESS;
  ObArray<ObArray<int64_t>> abs_pos_lists;
  double total_match_count = 0.0;
  if (OB_FAIL(abs_pos_lists.prepare_allocate(token_iters_.count()))) {
    LOG_WARN("failed to prepare allocate abs pos list array", K(ret));
  } else {
    ObString pos_list;
    for (int64_t i = 0; OB_SUCC(ret) && i < token_iters_.count(); ++i) {
      if (OB_FAIL(token_iters_[i]->get_curr_pos_list(pos_list))) {
        LOG_WARN("failed to get curr pos list", K(ret));
      } else if (OB_FAIL(decoder_->deserialize_and_decode(pos_list.ptr(), pos_list.length(), abs_pos_lists.at(i)))) {
        LOG_WARN("failed to decode pos list string", K(ret), K(pos_list));
      }
    }
  }

  if (FAILEDx(counter_->count_matches(abs_pos_lists, total_match_count))) {
    LOG_WARN("failed to count matches", K(ret));
  } else if (OB_UNLIKELY(total_match_count < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected negative total match count", K(ret), K(total_match_count));
  } else if (0 == total_match_count) {
    score = 0.0;
  } else {
    int64_t doc_length = 0;
    uint64_t total_doc_cnt = 0;
    double avg_doc_length = 0.0;
    if (OB_FAIL(calculate_total_token_weight_on_demand())) {
      LOG_WARN("failed to calculate total token weight", K(ret));
    } else if (OB_FAIL(token_iters_[0]->get_curr_doc_length(doc_length))) {
      LOG_WARN("failed to get curr doc length", K(ret));
    } else if (OB_FAIL(estimator_.get_bm25_param(total_doc_cnt, avg_doc_length))) {
      LOG_WARN("failed to get bm25 param", K(ret));
    } else {
      const double norm_len = doc_length / avg_doc_length;
      const double doc_weight = ObExprBM25::doc_phrase_weight(total_match_count, norm_len);
      score = total_token_weight_ * doc_weight * boost_;
    }
  }
  return ret;
}

int ObDASMatchPhraseOp::calculate_total_token_weight_on_demand()
{
  int ret = OB_SUCCESS;
  if (total_token_weight_ < 0.0) {
    total_token_weight_ = 0.0;
    for (int64_t i = 0; OB_SUCC(ret) && i < token_ids_.count(); ++i) {
      double token_weight = 0.0;
      if (OB_FAIL(token_iters_[token_ids_[i]]->get_max_token_relevance(token_weight))) {
        LOG_WARN("failed to get token weight", K(ret));
      } else {
        total_token_weight_ += token_weight;
      }
    }
  }
  return ret;
}

int ObDASMatchPhraseOp::do_next_rowid(ObDASRowID &next_id, double &score)
{
  int ret = OB_SUCCESS;
  bool got_result = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (OB_FAIL(estimate_bm25_param_on_demand())) {
    LOG_WARN("failed to estimate bm25 param", K(ret));
  } else if (OB_FAIL(token_iters_[0]->get_next_row())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get next row", K(ret));
    }
  } else if (OB_FAIL(find_intersection(next_id, score))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to intersect", K(ret));
    }
  }
  return ret;
}

int ObDASMatchPhraseOp::do_advance_to(
    const ObDASRowID &target,
    ObDASRowID &curr_id,
    double &score)
{
  int ret = OB_SUCCESS;
  ObDatum target_datum;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (OB_FAIL(estimate_bm25_param_on_demand())) {
    LOG_WARN("failed to estimate bm25 param", K(ret));
  } else if (OB_FAIL(get_datum_from_rowid(target, target_datum))) {
    LOG_WARN("failed to get datum from target", K(ret));
  } else if (OB_FAIL(token_iters_[0]->advance_to(target_datum))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get next row", K(ret));
    }
  } else if (OB_FAIL(find_intersection(curr_id, score))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to intersect", K(ret));
    }
  }
  return ret;
}

int ObDASMatchPhraseOp::do_advance_shallow(
    const ObDASRowID &target,
    const bool inclusive,
    const MaxScoreTuple *&max_score_tuple)
{
  // TODO: fix inclusive min id
  int ret = OB_SUCCESS;
  ObDASRowID tmp_id;
  double tmp_score = 0.0;
  int cmp_ret = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (OB_UNLIKELY(!target.is_normal())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid target rowid", K(ret));
  } else if (OB_FAIL(calculate_total_token_weight_on_demand())) {
    LOG_WARN("failed to calculate total token weight", K(ret));
  } else if (OB_FAIL(do_advance_to(target, tmp_id, tmp_score))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to advance to", K(ret));
  } else if (!inclusive && OB_FAIL(compare_rowid(tmp_id, target, cmp_ret))) {
    LOG_WARN("failed to compare rowids", K(ret));
  } else if (!inclusive && 0 == cmp_ret && OB_FAIL(do_next_rowid(tmp_id, tmp_score))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to get next rowid", K(ret));
  }
  if (OB_SUCC(ret)) {
    ObDASRowID max_id;
    max_id.set_max();
    max_score_tuple_.set(tmp_id, max_id, total_token_weight_);
    max_score_tuple = &max_score_tuple_;
  }
  return ret;
}

int ObDASMatchPhraseOp::do_calc_max_score(double &threshold)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (OB_FAIL(calculate_total_token_weight_on_demand())) {
    LOG_WARN("failed to calculate total token weight", K(ret));
  } else {
    threshold = total_token_weight_;
  }
  return ret;
}

int ObDASMatchPhraseOp::estimate_bm25_param_on_demand()
{
  int ret = OB_SUCCESS;
  uint64_t total_doc_cnt = 0;
  double avg_doc_token_cnt = 0.0;
  if (bm25_param_estimated_) {
    // skip
  } else if (OB_FAIL(estimator_.do_estimation(search_ctx_))) {
    LOG_WARN("failed to do inv idx bm25 param estimation on demand", K(ret));
  } else if (OB_FAIL(estimator_.get_bm25_param(total_doc_cnt, avg_doc_token_cnt))) {
    LOG_WARN("failed to get bm25 param", K(ret));
  } else {
    // project bm25 param to relevance exprs
    ObExpr *total_doc_cnt_expr = ir_ctdef_->relevance_expr_
        ->args_[ObExprBM25::TOTAL_DOC_CNT_PARAM_IDX];
    ObExpr *avg_doc_token_cnt_expr = ir_ctdef_->avg_doc_token_cnt_expr_;
    ObEvalCtx &eval_ctx = *ir_rtdef_->eval_ctx_;
    if (use_rich_format_) {
      if (OB_FAIL(total_doc_cnt_expr->init_vector_for_write(
          eval_ctx, total_doc_cnt_expr->get_default_res_format(), 1))) {
        LOG_WARN("failed to init vector for write", K(ret));
      } else if (OB_FAIL(avg_doc_token_cnt_expr->init_vector_for_write(
          eval_ctx, avg_doc_token_cnt_expr->get_default_res_format(), 1))) {
        LOG_WARN("failed to init vector for write", K(ret));
      } else {
        total_doc_cnt_expr->get_vector(eval_ctx)->set_int(0, total_doc_cnt);
        avg_doc_token_cnt_expr->get_vector(eval_ctx)->set_double(0, avg_doc_token_cnt);
      }
    } else {
      total_doc_cnt_expr->locate_datum_for_write(eval_ctx).set_int(total_doc_cnt);
      avg_doc_token_cnt_expr->locate_datum_for_write(eval_ctx).set_double(avg_doc_token_cnt);
    }
    bm25_param_estimated_ = true;
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
