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
#include "ob_das_token_utils.h"
#include "ob_das_token_op.h"

namespace oceanbase
{
namespace sql
{

ObDASTokenOpHelper::ObDASTokenOpHelper(ObDASSearchCtx &search_ctx)
  : search_ctx_(search_ctx),
    arena_allocator_(common::ObMemAttr(MTL_ID(), "DASTokenOpH")),
    ir_ctdef_(nullptr),
    ir_rtdef_(nullptr),
    inv_idx_tablet_id_(),
    inv_idx_scan_param_(),
    inv_idx_agg_param_(),
    inv_idx_scan_iter_(),
    inv_idx_agg_iter_(),
    block_max_param_(nullptr),
    inv_idx_scan_range_(),
    eval_ctx_(nullptr),
    obj_buf_(),
    use_rich_format_(false),
    is_inited_(false)
{}

int ObDASTokenOpHelper::init(const ObDASTokenOpParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(param));
  } else if (OB_FAIL(init_scan_param(param))) {
    LOG_WARN("failed to init text retrieval param", K(ret));
  } else {
    ir_ctdef_ = param.ir_ctdef_;
    ir_rtdef_ = param.ir_rtdef_;
    block_max_param_ = param.block_max_param_;
    eval_ctx_ = ir_rtdef_->eval_ctx_;
    use_rich_format_ = param.use_rich_format_;
    is_inited_ = true;
  }
  return ret;
}

int ObDASTokenOpHelper::init_scan_param(const ObDASTokenOpParam &param)
{
  int ret = OB_SUCCESS;
  const bool need_relevance = param.ir_ctdef_->need_calc_relevance();
  const ObDASScalarScanCtDef *inv_idx_scan_ctdef = param.ir_ctdef_->get_inv_idx_scan_scalar_ctdef();
  ObDASScalarScanRtDef *inv_idx_scan_rtdef = param.ir_rtdef_->get_inv_idx_scan_scalar_rtdef();
  const ObDASScalarScanCtDef *inv_idx_agg_ctdef = param.ir_ctdef_->get_inv_idx_agg_scalar_ctdef();
  ObDASScalarScanRtDef *inv_idx_agg_rtdef = param.ir_rtdef_->get_inv_idx_agg_scalar_rtdef();
  init_scan_range(*param.ir_ctdef_, param.query_token_, inv_idx_scan_range_);
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
  return ret;
}

void ObDASTokenOpHelper::init_scan_range(
    const ObDASIRScanCtDef &ir_ctdef,
    const ObString &query_token,
    ObNewRange &scan_range)
{
  obj_buf_[0].set_common_value(query_token);  // query token of start key
  obj_buf_[0].set_meta_type(ir_ctdef.search_text_->obj_meta_);
  obj_buf_[1].set_min_value();                // doc id of start key
  obj_buf_[2].set_common_value(query_token);  // query token of end key
  obj_buf_[2].set_meta_type(ir_ctdef.search_text_->obj_meta_);
  obj_buf_[3].set_max_value();                // doc id of end key
  scan_range.table_id_ = ir_ctdef.get_inv_idx_scan_scalar_ctdef()->ref_table_id_;
  scan_range.start_key_.assign(obj_buf_, 2);
  scan_range.end_key_.assign(obj_buf_ + 2, 2);
  scan_range.border_flag_.set_inclusive_start();
  scan_range.border_flag_.set_inclusive_end();
}

int ObDASTokenOpHelper::init_bm25_param_estimator(ObBM25IndexParamEstimator &estimator)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (OB_FAIL(estimator.init(
      *ir_ctdef_,
      inv_idx_tablet_id_,
      *ir_rtdef_,
      inv_idx_scan_param_,
      search_ctx_.get_bm25_param_cache()))) {
    LOG_WARN("failed to init bm25 param estimator", K(ret));
  }
  return ret;
}

int ObDASTokenOpHelper::init_text_retrieval_iter(ObTextRetrievalBlockMaxIter &iter)
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

    if (OB_SUCC(ret)) {
      ObTextRetrievalScanIterParam text_retrieval_param;
      if (FALSE_IT(init_text_retrieval_param(text_retrieval_param))) {
        LOG_WARN("failed to init text retrieval param", K(ret));
      } else if (OB_FAIL(iter.init(
          text_retrieval_param, *block_max_param_, inv_idx_scan_param_))) {
        LOG_WARN("failed to init text retrieval iter", K(ret));
      }
    }
  }
  return ret;
}

void ObDASTokenOpHelper::init_scan_iter_param(
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

void ObDASTokenOpHelper::init_text_retrieval_param(
    ObTextRetrievalScanIterParam &text_retrieval_param)
{
  text_retrieval_param.allocator_ = &arena_allocator_;
  text_retrieval_param.inv_idx_scan_param_ = &inv_idx_scan_param_;
  text_retrieval_param.inv_idx_scan_iter_ = &inv_idx_scan_iter_;
  if (ir_ctdef_->need_calc_relevance()) {
    text_retrieval_param.inv_idx_agg_param_ = &inv_idx_agg_param_;
    text_retrieval_param.inv_idx_agg_iter_ = &inv_idx_agg_iter_;
    text_retrieval_param.inv_idx_agg_expr_ = ir_ctdef_->get_inv_idx_agg_scalar_ctdef()
        ->pd_expr_spec_.pd_storage_aggregate_output_.at(0);
  } else {
    text_retrieval_param.inv_idx_agg_param_ = nullptr;
    text_retrieval_param.inv_idx_agg_iter_ = nullptr;
    text_retrieval_param.inv_idx_agg_expr_ = nullptr;
  }
  text_retrieval_param.fwd_idx_scan_param_ = nullptr;
  text_retrieval_param.fwd_idx_agg_iter_ = nullptr;
  text_retrieval_param.fwd_idx_agg_expr_ = nullptr;
  text_retrieval_param.eval_ctx_ = eval_ctx_;
  text_retrieval_param.relevance_expr_ = ir_ctdef_->relevance_expr_;
  text_retrieval_param.inv_scan_doc_length_col_ = ir_ctdef_->inv_scan_doc_length_col_;
  text_retrieval_param.inv_scan_domain_id_col_ = ir_ctdef_->inv_scan_domain_id_col_;
  text_retrieval_param.inv_scan_pos_list_col_ = ir_ctdef_->inv_scan_pos_list_col_;
  text_retrieval_param.reuse_inv_idx_agg_res_ = true;
  text_retrieval_param.use_rich_format_ = use_rich_format_;
}

int ObDASTokenOpHelper::rescan()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (OB_FAIL(search_ctx_.get_related_tablet_id(
      ir_ctdef_->get_inv_idx_scan_scalar_ctdef(), inv_idx_tablet_id_))) {
    LOG_WARN("failed to get related tablet id", K(ret));
  } else {
    const bool need_relevance = ir_ctdef_->need_calc_relevance();
    ObIDASSearchOp::switch_tablet_id(inv_idx_tablet_id_, inv_idx_scan_param_);
    ObIDASSearchOp::switch_tablet_id(inv_idx_tablet_id_, inv_idx_agg_param_);
    if (OB_FAIL(inv_idx_scan_iter_.reuse())) {
      LOG_WARN("failed to reuse inv idx scan iter", K(ret));
    } else if (need_relevance && OB_FAIL(inv_idx_agg_iter_.reuse())) {
      LOG_WARN("failed to reuse inv idx agg iter", K(ret));
    }

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

int ObDASTokenOpHelper::close()
{
  int ret = OB_SUCCESS;
  inv_idx_scan_iter_.release();
  inv_idx_agg_iter_.release();
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

} // namespace sql
} // namesapce oceanbase
