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
#include "ob_das_token_op.h"

namespace oceanbase
{
namespace sql
{

ObDASTokenOpParam::ObDASTokenOpParam()
  : ObIDASSearchOpParam(DAS_SEARCH_OP_TOKEN),
    ir_ctdef_(nullptr),
    ir_rtdef_(nullptr),
    block_max_param_(nullptr),
    token_boost_(0.0),
    query_token_()
{}

bool ObDASTokenOpParam::is_valid() const
{
  return nullptr != ir_ctdef_
      && nullptr != ir_rtdef_
      && nullptr != block_max_param_
      && token_boost_ > 0.0
      && !query_token_.empty();
}

ObDASTokenOp::ObDASTokenOp(ObDASSearchCtx &search_ctx)
  : ObIDASSearchOp(search_ctx),
    arena_allocator_(common::ObMemAttr(MTL_ID(), "DASTokenOp")),
    inv_idx_tablet_id_(),
    inv_idx_scan_param_(),
    inv_idx_agg_param_(),
    inv_idx_scan_iter_(),
    inv_idx_agg_iter_(),
    block_max_param_(nullptr),
    text_retrieval_param_(),
    text_retrieval_iter_(),
    bm25_param_estimator_(),
    inv_idx_scan_range_(),
    token_boost_(0.0),
    eval_ctx_(nullptr),
    obj_buf_(),
    curr_id_(nullptr),
    min_id_(nullptr),
    max_id_(nullptr),
    is_simple_doc_id_(false),
    use_rich_format_(false),
    bm25_param_estimated_(false),
    block_max_inited_(false),
    is_inited_(false)
{}

int ObDASTokenOp::do_init(const ObIDASSearchOpParam &op_param)
{
  int ret = OB_SUCCESS;
  const ObDASTokenOpParam &param = static_cast<const ObDASTokenOpParam &>(op_param);
  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(param));
  } else if (OB_FAIL(init_scan_param(param))) {
    LOG_WARN("failed to init text retrieval param", K(ret));
  } else if (OB_FAIL(bm25_param_estimator_.init(
      *param.ir_ctdef_,
      inv_idx_tablet_id_,
      *param.ir_rtdef_,
      inv_idx_scan_param_,
      search_ctx_.get_bm25_param_cache()))) {
    LOG_WARN("failed to init bm25 param estimator", K(ret));
  } else {
    ir_ctdef_ = param.ir_ctdef_;
    ir_rtdef_ = param.ir_rtdef_;
    block_max_param_ = param.block_max_param_;
    token_boost_ = param.token_boost_;
    eval_ctx_ = ir_rtdef_->eval_ctx_;
    use_rich_format_ = param.use_rich_format_;
    is_simple_doc_id_ = search_ctx_.get_rowid_type() == DAS_ROWID_TYPE_UINT64;
    bm25_param_estimated_ = false;
    block_max_inited_ = false;
    is_inited_ = true;
  }
  return ret;
}

int ObDASTokenOp::do_open()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else {
    ObDASScanIterParam inv_idx_scan_iter_param;
    init_scan_iter_param(
        *ir_ctdef_->get_inv_idx_scan_scalar_ctdef(),
        *ir_rtdef_->get_inv_idx_scan_scalar_rtdef(),
        inv_idx_scan_iter_param);
    if (OB_FAIL(inv_idx_scan_iter_.init(inv_idx_scan_iter_param))) {
      LOG_WARN("failed to init scan iter", K(ret));
    } else if (FALSE_IT(inv_idx_scan_iter_.set_scan_param(inv_idx_scan_param_))) {
      LOG_WARN("failed to set scan param", K(ret));
    } else if (OB_FAIL(inv_idx_scan_iter_.do_table_scan())) {
      LOG_WARN("failed to open scan iter", K(ret));
    } else if (ir_ctdef_->need_calc_relevance()) {
      ObDASScanIterParam inv_idx_agg_iter_param;
      init_scan_iter_param(
          *ir_ctdef_->get_inv_idx_agg_scalar_ctdef(),
          *ir_rtdef_->get_inv_idx_agg_scalar_rtdef(),
          inv_idx_agg_iter_param);
      if (OB_FAIL(inv_idx_agg_iter_.init(inv_idx_agg_iter_param))) {
        LOG_WARN("failed to init agg iter", K(ret));
      } else if (FALSE_IT(inv_idx_agg_iter_.set_scan_param(inv_idx_agg_param_))) {
        LOG_WARN("failed to set agg param", K(ret));
      } else if (OB_FAIL(inv_idx_agg_iter_.do_table_scan())) {
        LOG_WARN("failed to open agg iter", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (FALSE_IT(init_text_retrieval_param())) {
      LOG_WARN("failed to init text retrieval param", K(ret));
    } else if (OB_FAIL(text_retrieval_iter_.init(
        text_retrieval_param_, *block_max_param_, inv_idx_scan_param_))) {
      LOG_WARN("failed to init text retrieval iter", K(ret));
    } else if (is_simple_doc_id_) {
      // skip
    } else if (OB_FAIL(init_compact_id_rows())) {
      LOG_WARN("failed to init compact id rows", K(ret));
    }
  }
  return ret;
}

int ObDASTokenOp::do_rescan()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (OB_FAIL(get_related_tablet_id(ir_ctdef_->get_inv_idx_scan_scalar_ctdef(), inv_idx_tablet_id_))) {
    LOG_WARN("failed to get related tablet id", K(ret));
  } else if (OB_FAIL(reset_inv_scan_range())) {
    LOG_WARN("failed to reset inv scan range", K(ret));
  } else {
    const bool need_relevance = ir_ctdef_->need_calc_relevance();
    ObIDASSearchOp::switch_tablet_id(inv_idx_tablet_id_, inv_idx_scan_param_);
    ObIDASSearchOp::switch_tablet_id(inv_idx_tablet_id_, inv_idx_agg_param_);
    if (OB_FAIL(inv_idx_scan_iter_.reuse())) {
      LOG_WARN("failed to reuse inv idx scan iter", K(ret));
    } else if (need_relevance && OB_FAIL(inv_idx_agg_iter_.reuse())) {
      LOG_WARN("failed to reuse inv idx agg iter", K(ret));
    }
    text_retrieval_iter_.reuse();
    block_max_inited_ = false;
    bm25_param_estimated_ = false;
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(!inv_idx_scan_param_.key_ranges_.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected non-empty scan range", K(ret), K(inv_idx_scan_param_.key_ranges_));
    } else if (OB_FAIL(inv_idx_scan_param_.key_ranges_.push_back(inv_idx_scan_range_))) {
      LOG_WARN("failed to push back scan range", K(ret));
    } else if (OB_FAIL(inv_idx_scan_iter_.rescan())) {
      LOG_WARN("failed to rescan inv idx scan iter", K(ret));
    } else if (!need_relevance) {
      // skip
    } else if (OB_UNLIKELY(!inv_idx_agg_param_.key_ranges_.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected non-empty scan range", K(ret), K(inv_idx_agg_param_.key_ranges_));
    } else if (OB_FAIL(inv_idx_agg_param_.key_ranges_.push_back(inv_idx_scan_range_))) {
      LOG_WARN("failed to push back scan range", K(ret));
    } else if (OB_FAIL(inv_idx_agg_iter_.rescan())) {
      LOG_WARN("failed to rescan inv idx agg iter", K(ret));
    }
  }
  return ret;
}

int ObDASTokenOp::do_close()
{
  int ret = OB_SUCCESS;
  inv_idx_scan_iter_.release();
  inv_idx_agg_iter_.release();
  inv_idx_scan_range_.reset();
  text_retrieval_iter_.reset();
  bm25_param_estimator_.reset();
  inv_idx_scan_param_.destroy_schema_guard();
  inv_idx_scan_param_.snapshot_.reset();
  inv_idx_scan_param_.destroy();
  inv_idx_agg_param_.destroy_schema_guard();
  inv_idx_agg_param_.snapshot_.reset();
  inv_idx_agg_param_.destroy();
  arena_allocator_.reset();
  is_inited_ = false;
  return ret;
}

int ObDASTokenOp::do_next_rowid(ObDASRowID &next_id, double &score)
{
  int ret = OB_SUCCESS;
  const ObDatum *id_datum = nullptr;
  if (OB_FAIL(do_inv_idx_bm25_param_estimation_on_demand())) {
    LOG_WARN("failed to do inv idx bm25 param estimation on demand", K(ret));
  } else if (OB_FAIL(text_retrieval_iter_.get_next_row())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get next row from text retrieval iter", K(ret));
    } else {
      ret = OB_ITER_END;
    }
  } else if (OB_FAIL(text_retrieval_iter_.get_curr_id(id_datum))) {
    LOG_WARN("failed to get curr id from text retrieval iter", K(ret));
  } else if (OB_FAIL(text_retrieval_iter_.get_curr_score(score))) {
    LOG_WARN("failed to get curr score from text retrieval iter", K(ret));
  } else if (FALSE_IT(score *= token_boost_)) {
  } else if (is_simple_doc_id_) {
    next_id.set_uint64(id_datum->get_uint64());
  } else {
    datum_to_compact_row(*id_datum, *curr_id_);
    next_id.set_compact_row(curr_id_);
  }
  return ret;
}

int ObDASTokenOp::do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score)
{
  int ret = OB_SUCCESS;
  const ObDatum *id_datum = nullptr;
  ObDatum target_datum;
  if (OB_FAIL(search_ctx_.get_datum_from_rowid(target, target_datum))) {
    LOG_WARN("failed to get datum from rowid", K(ret), K(target));
  } else if (OB_FAIL(do_inv_idx_bm25_param_estimation_on_demand())) {
    LOG_WARN("failed to do inv idx bm25 param estimation on demand", K(ret));
  } else if (OB_FAIL(text_retrieval_iter_.advance_to(target_datum))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to advance to target row", K(ret), K(target));
    }
  } else if (OB_FAIL(text_retrieval_iter_.get_curr_id(id_datum))) {
    LOG_WARN("failed to get curr id from text retrieval iter", K(ret));
  } else if (OB_FAIL(text_retrieval_iter_.get_curr_score(score))) {
    LOG_WARN("failed to get curr score from text retrieval iter", K(ret));
  } else if (FALSE_IT(score *= token_boost_)) {
  } else if (is_simple_doc_id_) {
    curr_id.set_uint64(id_datum->get_uint64());
  } else {
    datum_to_compact_row(*id_datum, *curr_id_);
    curr_id.set_compact_row(curr_id_);
  }
  return ret;
}

int ObDASTokenOp::do_advance_shallow(
    const ObDASRowID &target,
    const bool inclusive,
    const MaxScoreTuple *&max_score_tuple)
{
  int ret = OB_SUCCESS;
  const storage::ObMaxScoreTuple *storage_max_score_tuple = nullptr;
  ObDatum target_datum;
  ObDASRowID min_id;
  ObDASRowID max_id;
  if (OB_FAIL(search_ctx_.get_datum_from_rowid(target, target_datum))) {
    LOG_WARN("failed to get datum from rowid", K(ret), K(target));
  } else if (OB_FAIL(init_block_max_iter_on_demand())) {
    LOG_WARN("failed to init block max iter on demand", K(ret));
  } else if (OB_FAIL(text_retrieval_iter_.advance_shallow(target_datum, inclusive))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to advance shallow", K(ret), K(target));
    }
  } else if (OB_FAIL(text_retrieval_iter_.get_curr_block_max_info(storage_max_score_tuple))) {
    LOG_WARN("failed to get curr block max info", K(ret));
  } else {
    if (storage_max_score_tuple->min_domain_id_->is_min()) {
      min_id.set_min();
    } else if (storage_max_score_tuple->min_domain_id_->is_max()) {
      min_id.set_max();
    } else if (is_simple_doc_id_) {
      min_id.set_uint64(storage_max_score_tuple->min_domain_id_->get_uint64());
    } else {
      datum_to_compact_row(*storage_max_score_tuple->min_domain_id_, *min_id_);
    }

    if (storage_max_score_tuple->max_domain_id_->is_max()) {
      max_id.set_max();
    } else if (storage_max_score_tuple->max_domain_id_->is_min()) {
      max_id.set_min();
    } else if (is_simple_doc_id_) {
      max_id.set_uint64(storage_max_score_tuple->max_domain_id_->get_uint64());
    } else {
      datum_to_compact_row(*storage_max_score_tuple->max_domain_id_, *max_id_);
    }

    max_score_tuple_.set(min_id, max_id, storage_max_score_tuple->max_score_ * token_boost_);
    max_score_tuple = &max_score_tuple_;
  }
  return ret;
}

int ObDASTokenOp::do_calc_max_score(double &threshold)
{
  int ret = OB_SUCCESS;
  double token_max_score = 0.0;
  if (OB_FAIL(init_block_max_iter_on_demand())) {
    LOG_WARN("failed to init block max iter on demand", K(ret));
  } else if (OB_FAIL(text_retrieval_iter_.get_dim_max_score(token_max_score))) {
    LOG_WARN("failed to get dim max score", K(ret));
  } else {
    threshold += token_max_score * token_boost_;
  }
  return ret;
}

int ObDASTokenOp::init_scan_param(const ObDASTokenOpParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_scan_range(*param.ir_ctdef_, param.query_token_, inv_idx_scan_range_))) {
    LOG_WARN("failed to init scan range", K(ret), K(param.query_token_));
  } else {
    const bool need_relevance = param.ir_ctdef_->need_calc_relevance();
    const ObDASScalarScanCtDef *inv_idx_scan_ctdef = param.ir_ctdef_->get_inv_idx_scan_scalar_ctdef();
    ObDASScalarScanRtDef *inv_idx_scan_rtdef = param.ir_rtdef_->get_inv_idx_scan_scalar_rtdef();
    const ObDASScalarScanCtDef *inv_idx_agg_ctdef = param.ir_ctdef_->get_inv_idx_agg_scalar_ctdef();
    ObDASScalarScanRtDef *inv_idx_agg_rtdef = param.ir_rtdef_->get_inv_idx_agg_scalar_rtdef();
    if (OB_ISNULL(inv_idx_scan_ctdef) || OB_ISNULL(inv_idx_scan_rtdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr", K(ret), KP(inv_idx_scan_ctdef), KP(inv_idx_scan_rtdef), K(param));
    } else if (OB_UNLIKELY(need_relevance && (nullptr == inv_idx_agg_ctdef || nullptr == inv_idx_agg_rtdef))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr to inv idx agg def", K(ret), K(param));
    } else if (OB_FAIL(search_ctx_.get_related_tablet_id(inv_idx_scan_ctdef, inv_idx_tablet_id_))) {
      LOG_WARN("failed to get related tablet id", K(ret), K(inv_idx_scan_ctdef));
    } else if (OB_UNLIKELY(!inv_idx_scan_rtdef->key_ranges_.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected non-empty key ranges", K(ret), K(inv_idx_scan_rtdef->key_ranges_));
    } else if (OB_FAIL(search_ctx_.init_scan_param(inv_idx_tablet_id_, inv_idx_scan_ctdef, inv_idx_scan_rtdef, inv_idx_scan_param_))) {
      LOG_WARN("failed to init scan param", K(ret), K(inv_idx_scan_ctdef), K(inv_idx_scan_rtdef));
    } else if (OB_FAIL(inv_idx_scan_param_.key_ranges_.push_back(inv_idx_scan_range_))) {
      LOG_WARN("failed to push back scan range", K(ret));
    } else if (!need_relevance) {
      // skip
    } else if (OB_UNLIKELY(!inv_idx_agg_rtdef->key_ranges_.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected non-empty key ranges", K(ret), K(inv_idx_agg_rtdef->key_ranges_));
    } else if (OB_FAIL(search_ctx_.init_scan_param(inv_idx_tablet_id_, inv_idx_agg_ctdef, inv_idx_agg_rtdef, inv_idx_agg_param_))) {
      LOG_WARN("failed to init agg param", K(ret), K(inv_idx_agg_ctdef), K(inv_idx_agg_rtdef));
    } else if (OB_FAIL(inv_idx_agg_param_.key_ranges_.push_back(inv_idx_scan_range_))) {
      LOG_WARN("failed to push back scan range", K(ret));
    }
  }
  return ret;
}

int ObDASTokenOp::init_scan_range(
    const ObDASIRScanCtDef &ir_ctdef,
    const ObString &query_token,
    ObNewRange &scan_range)
{
  int ret = OB_SUCCESS;
  // Do we need deep copy query token here?
  obj_buf_[0].set_common_value(query_token);
  obj_buf_[0].set_meta_type(ir_ctdef.search_text_->obj_meta_);
  obj_buf_[1].set_min_value();
  obj_buf_[2].set_common_value(query_token);
  obj_buf_[2].set_meta_type(ir_ctdef.search_text_->obj_meta_);
  obj_buf_[3].set_max_value();
  ObRowkey start_key(obj_buf_, 2);
  ObRowkey end_key(obj_buf_ + 2, 2);
  scan_range.table_id_ = ir_ctdef.get_inv_idx_scan_scalar_ctdef()->ref_table_id_;
  scan_range.start_key_.assign(obj_buf_, 2);
  scan_range.end_key_.assign(obj_buf_ + 2, 2);
  scan_range.border_flag_.set_inclusive_start();
  scan_range.border_flag_.set_inclusive_end();
  return ret;
}

void ObDASTokenOp::init_scan_iter_param(
    const ObDASScalarScanCtDef &scan_ctdef,
    ObDASScalarScanRtDef &scan_rtdef,
    ObDASScanIterParam &iter_param)
{
  iter_param.scan_ctdef_ = nullptr;
  iter_param.scalar_scan_ctdef_ = &scan_ctdef;
  iter_param.use_scalar_ctdef_ = true;
  iter_param.max_size_ = scan_rtdef.eval_ctx_->is_vectorized() ? scan_rtdef.eval_ctx_->max_batch_size_ : 1;
  iter_param.eval_ctx_ = scan_rtdef.eval_ctx_;
  iter_param.exec_ctx_ = &scan_rtdef.eval_ctx_->exec_ctx_;
  iter_param.output_ = &scan_ctdef.result_output_;
}

void ObDASTokenOp::init_text_retrieval_param()
{
  text_retrieval_param_.allocator_ = &arena_allocator_;
  text_retrieval_param_.inv_idx_scan_param_ = &inv_idx_scan_param_;
  text_retrieval_param_.inv_idx_scan_iter_ = &inv_idx_scan_iter_;
  if (ir_ctdef_->need_calc_relevance()) {
    text_retrieval_param_.inv_idx_agg_param_ = &inv_idx_agg_param_;
    text_retrieval_param_.inv_idx_agg_iter_ = &inv_idx_agg_iter_;
    text_retrieval_param_.inv_idx_agg_expr_ = ir_ctdef_->get_inv_idx_agg_scalar_ctdef()->pd_expr_spec_.pd_storage_aggregate_output_.at(0);
  } else {
    text_retrieval_param_.inv_idx_agg_param_ = nullptr;
    text_retrieval_param_.inv_idx_agg_iter_ = nullptr;
    text_retrieval_param_.inv_idx_agg_expr_ = nullptr;
  }
  text_retrieval_param_.fwd_idx_scan_param_ = nullptr;
  text_retrieval_param_.fwd_idx_agg_iter_ = nullptr;
  text_retrieval_param_.fwd_idx_agg_expr_ = nullptr;
  text_retrieval_param_.eval_ctx_ = eval_ctx_;
  text_retrieval_param_.relevance_expr_ = ir_ctdef_->relevance_expr_;
  text_retrieval_param_.inv_scan_doc_length_col_ = ir_ctdef_->inv_scan_doc_length_col_;
  text_retrieval_param_.inv_scan_domain_id_col_ = ir_ctdef_->inv_scan_domain_id_col_;
  text_retrieval_param_.reuse_inv_idx_agg_res_ = true;
  text_retrieval_param_.use_rich_format_ = use_rich_format_;
}

int ObDASTokenOp::init_compact_id_rows()
{
  int ret = OB_SUCCESS;
  const int64_t single_id_row_size = ObCompactRow::calc_max_row_size(get_rowid_exprs(), 0);
  void *id_row_buf = nullptr;
  if (OB_ISNULL(id_row_buf = arena_allocator_.alloc(3 * single_id_row_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc id row buf", K(ret), K(single_id_row_size));
  } else {
    ObCompactRow *row_arr = new (id_row_buf) ObCompactRow[3];
    curr_id_ = &row_arr[0];
    min_id_ = &row_arr[1];
    max_id_ = &row_arr[2];
    curr_id_->init(get_rowid_meta());
    min_id_->init(get_rowid_meta());
    max_id_->init(get_rowid_meta());
  }
  return ret;
}

int ObDASTokenOp::do_inv_idx_bm25_param_estimation_on_demand()
{
  int ret = OB_SUCCESS;
  uint64_t total_doc_cnt = 0;
  double avg_doc_token_cnt = 0.0;
  if (bm25_param_estimated_) {
    // skip
  } else if (OB_FAIL(bm25_param_estimator_.do_estimation(search_ctx_))) {
    LOG_WARN("failed to do inv idx bm25 param estimation on demand", K(ret));
  } else if (OB_FAIL(bm25_param_estimator_.get_bm25_param(total_doc_cnt, avg_doc_token_cnt))) {
    LOG_WARN("failed to get bm25 param", K(ret));
  } else {
    // project bm25 param to relevance exprs
    ObExpr *total_doc_cnt_expr = ir_ctdef_->relevance_expr_->args_[ObExprBM25::TOTAL_DOC_CNT_PARAM_IDX];
    ObExpr *avg_doc_token_cnt_expr = ir_ctdef_->avg_doc_token_cnt_expr_;
    if (use_rich_format_) {
      if (OB_FAIL(total_doc_cnt_expr->init_vector_for_write(
          *eval_ctx_, total_doc_cnt_expr->get_default_res_format(), 1))) {
        LOG_WARN("failed to init vector for write", K(ret));
      } else if (OB_FAIL(avg_doc_token_cnt_expr->init_vector_for_write(
          *eval_ctx_, avg_doc_token_cnt_expr->get_default_res_format(), 1))) {
        LOG_WARN("failed to init vector for write", K(ret));
      } else {
        total_doc_cnt_expr->get_vector(*eval_ctx_)->set_int(0, total_doc_cnt);
        avg_doc_token_cnt_expr->get_vector(*eval_ctx_)->set_double(0, avg_doc_token_cnt);
      }
    } else {
      total_doc_cnt_expr->locate_datum_for_write(*eval_ctx_).set_int(total_doc_cnt);
      avg_doc_token_cnt_expr->locate_datum_for_write(*eval_ctx_).set_double(avg_doc_token_cnt);
    }
    bm25_param_estimated_ = true;
  }
  return ret;
}

void ObDASTokenOp::datum_to_compact_row(const ObDatum &id_datum, ObCompactRow &row)
{
  row.set_cell_payload(get_rowid_meta(), 0, id_datum.ptr_, id_datum.len_);
}

int ObDASTokenOp::init_block_max_iter_on_demand()
{
  int ret = OB_SUCCESS;
  uint64_t total_doc_cnt = 0;
  double avg_doc_token_cnt = 0.0;
  if (block_max_inited_) {
    // skip
  } else if (OB_FAIL(do_inv_idx_bm25_param_estimation_on_demand())) {
    LOG_WARN("failed to do inv idx bm25 param estimation on demand", K(ret));
  } else if (OB_FAIL(bm25_param_estimator_.get_bm25_param(total_doc_cnt, avg_doc_token_cnt))) {
    LOG_WARN("failed to get bm25 param", K(ret));
  } else if (OB_FAIL(text_retrieval_iter_.init_block_max_iter(total_doc_cnt, avg_doc_token_cnt))) {
    LOG_WARN("failed to init block max iter", K(ret));
  } else {
    block_max_inited_ = true;
  }
  return ret;
}

int ObDASTokenOp::reset_inv_scan_range()
{
  int ret = OB_SUCCESS;
  // reset inv idx scan range since doc id might be pushed forward on advance_to
  obj_buf_[1].set_min_value();
  obj_buf_[3].set_max_value();
  inv_idx_scan_range_.start_key_.assign(obj_buf_, 2);
  inv_idx_scan_range_.end_key_.assign(obj_buf_ + 2, 2);
  inv_idx_scan_range_.border_flag_.set_inclusive_start();
  inv_idx_scan_range_.border_flag_.set_inclusive_end();
  return ret;
}

}
}