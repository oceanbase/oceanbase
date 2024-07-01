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

#define USING_LOG_PREFIX SQL_DAS
#include "ob_das_text_retrieval_iter.h"
#include "ob_das_scan_iter.h"
#include "sql/das/ob_das_ir_define.h"
#include "sql/engine/expr/ob_expr_bm25.h"

namespace oceanbase
{
namespace sql
{

ObDASTextRetrievalIter::ObDASTextRetrievalIter()
  : ObDASIter(ObDASIterType::DAS_ITER_TEXT_RETRIEVAL),
    mem_context_(nullptr),
    ir_ctdef_(nullptr),
    ir_rtdef_(nullptr),
    tx_desc_(nullptr),
    snapshot_(nullptr),
    ls_id_(),
    inv_idx_tablet_id_(),
    fwd_idx_tablet_id_(),
    inv_idx_scan_param_(),
    inv_idx_agg_param_(),
    fwd_idx_scan_param_(),
    calc_exprs_(),
    inverted_idx_scan_iter_(nullptr),
    inverted_idx_agg_iter_(nullptr),
    forward_idx_iter_(nullptr),
    fwd_range_objs_(nullptr),
    doc_token_cnt_expr_(nullptr),
    token_doc_cnt_(0),
    need_fwd_idx_agg_(false),
    need_inv_idx_agg_(false),
    inv_idx_agg_evaluated_(false),
    not_first_fwd_agg_(false),
    is_inited_(false)
{
}

int ObDASTextRetrievalIter::set_query_token(const ObString &query_token)
{
  int ret = OB_SUCCESS;
  ObNewRange inv_idx_scan_range;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("text retrieval iter not inited", K(ret));
  } else if (OB_UNLIKELY(!inv_idx_scan_param_.key_ranges_.empty() ||
      (need_inv_idx_agg_ && !inv_idx_agg_param_.key_ranges_.empty()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected set query token with not null query range", K(ret), K(query_token),
        K(inv_idx_scan_param_.key_ranges_), K_(need_inv_idx_agg), K(inv_idx_agg_param_.key_ranges_));
  } else  if (OB_FAIL(gen_inv_idx_scan_range(query_token, inv_idx_scan_range))) {
    LOG_WARN("failed to generate inverted index scan range", K(ret), K(query_token));
  } else if (OB_FAIL(inv_idx_scan_param_.key_ranges_.push_back(inv_idx_scan_range))) {
    LOG_WARN("failed to add scan range for inv idx scan", K(ret));
  } else if (need_inv_idx_agg_ && OB_FAIL(inv_idx_agg_param_.key_ranges_.push_back(inv_idx_scan_range))) {
    LOG_WARN("failed to add scan range for inv idx agg", K(ret));
  }
  return ret;
}

int ObDASTextRetrievalIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double initialization", K(ret));
  } else if (OB_UNLIKELY(ObDASIterType::DAS_ITER_TEXT_RETRIEVAL != param.type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid das iter param type for text retrieval iter", K(ret), K(param));
  } else {
    ObDASTextRetrievalIterParam &retrieval_param = static_cast<ObDASTextRetrievalIterParam &>(param);
    inverted_idx_scan_iter_ = static_cast<ObDASScanIter *>(retrieval_param.inv_idx_scan_iter_);
    ir_ctdef_ = retrieval_param.ir_ctdef_;
    ir_rtdef_ = retrieval_param.ir_rtdef_;
    tx_desc_ = retrieval_param.tx_desc_;
    snapshot_ = retrieval_param.snapshot_;
    need_fwd_idx_agg_ = ir_ctdef_->need_fwd_idx_agg();
    need_inv_idx_agg_ = ir_ctdef_->need_inv_idx_agg();

    if (need_inv_idx_agg_) {
      inverted_idx_agg_iter_ = static_cast<ObDASScanIter *>(retrieval_param.inv_idx_agg_iter_);
    }

    if (need_fwd_idx_agg_) {
      forward_idx_iter_ = static_cast<ObDASScanIter *>(retrieval_param.fwd_idx_iter_);
    }

    if (OB_ISNULL(mem_context_)) {
      lib::ContextParam param;
      param.set_mem_attr(MTL_ID(), "TextIRIter", ObCtxIds::DEFAULT_CTX_ID);
      if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
        LOG_WARN("failed to create text retrieval iterator memory context", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(init_calc_exprs())) {
      LOG_WARN("failed to init row-wise calc exprs", K(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDASTextRetrievalIter::inner_reuse()
{
  int ret = OB_SUCCESS;
  if (nullptr != mem_context_) {
    mem_context_->reset_remain_one_page();
  }
  inv_idx_agg_evaluated_ = false;
  const ObTabletID &old_inv_scan_id = inv_idx_scan_param_.tablet_id_;
  inverted_idx_scan_iter_->set_scan_param(inv_idx_scan_param_);
  inv_idx_scan_param_.need_switch_param_ = inv_idx_scan_param_.need_switch_param_ ||
    ((old_inv_scan_id.is_valid() && old_inv_scan_id != inv_idx_tablet_id_) ? true : false);
  if (OB_FAIL(inverted_idx_scan_iter_->reuse())) {
    LOG_WARN("failed to reuse inverted index iter", K(ret));
  } else {
    if (need_inv_idx_agg_) {
      const ObTabletID &old_inv_agg_id = inv_idx_agg_param_.tablet_id_;
      inverted_idx_agg_iter_->set_scan_param(inv_idx_agg_param_);
      inv_idx_agg_param_.need_switch_param_ = inv_idx_agg_param_.need_switch_param_ ||
        ((old_inv_agg_id.is_valid() && old_inv_agg_id != inv_idx_tablet_id_) ? true : false);
      if (OB_FAIL(inverted_idx_agg_iter_->reuse())) {
        LOG_WARN("failed to reuse inverted index agg iter", K(ret));
      }
    }

    if (OB_SUCC(ret) && need_fwd_idx_agg_) {
      const ObTabletID &old_fwd_agg_id = fwd_idx_scan_param_.tablet_id_;
      forward_idx_iter_->set_scan_param(fwd_idx_scan_param_);
      fwd_idx_scan_param_.need_switch_param_ = fwd_idx_scan_param_.need_switch_param_ ||
        ((old_fwd_agg_id.is_valid() && old_fwd_agg_id != fwd_idx_tablet_id_) ? true : false);
      if (OB_FAIL(forward_idx_iter_->reuse())) {
        LOG_WARN("failed to reuse forward index iter", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    inv_idx_agg_evaluated_ = false;
  }

  return ret;
}

int ObDASTextRetrievalIter::inner_release()
{
  int ret = OB_SUCCESS;

  inv_idx_scan_param_.destroy_schema_guard();
  inv_idx_scan_param_.snapshot_.reset();
  inv_idx_scan_param_.destroy();
  inv_idx_agg_param_.destroy_schema_guard();
  inv_idx_agg_param_.snapshot_.reset();
  inv_idx_agg_param_.destroy();
  fwd_idx_scan_param_.destroy_schema_guard();
  fwd_idx_scan_param_.snapshot_.reset();
  fwd_idx_scan_param_.destroy();
  calc_exprs_.reset();
  if (nullptr != mem_context_)  {
    mem_context_->reset_remain_one_page();
    DESTROY_CONTEXT(mem_context_);
    mem_context_ = nullptr;
  }
  ir_ctdef_ = nullptr;
  ir_rtdef_ = nullptr;
  inverted_idx_scan_iter_ = nullptr;
  inverted_idx_agg_iter_ = nullptr;
  forward_idx_iter_ = nullptr;
  fwd_range_objs_ = nullptr;
  doc_token_cnt_expr_ = nullptr;
  tx_desc_ = nullptr;
  snapshot_ = nullptr;
  token_doc_cnt_ = 0;
  need_fwd_idx_agg_ = false;
  need_inv_idx_agg_ = false;
  inv_idx_agg_evaluated_ = false;
  not_first_fwd_agg_ = false;
  is_inited_ = false;

  return ret;
}

int ObDASTextRetrievalIter::do_table_scan()
{
  int ret = OB_SUCCESS;
  inverted_idx_scan_iter_->set_scan_param(inv_idx_scan_param_);
  if (need_inv_idx_agg_) {
    inverted_idx_agg_iter_->set_scan_param(inv_idx_agg_param_);
  }

  if (OB_FAIL(init_inv_idx_scan_param())) {
    LOG_WARN("failed to init inv idx scan param", K(ret));
  } else if (OB_FAIL(inverted_idx_scan_iter_->do_table_scan())) {
    LOG_WARN("failed to do inverted index table scan", K(ret));
  } else if (need_inv_idx_agg_ && OB_FAIL(inverted_idx_agg_iter_->do_table_scan())) {
    LOG_WARN("failed to do inverted index agg", K(ret));
  }
  return ret;
}

int ObDASTextRetrievalIter::rescan()
{
  int ret = OB_SUCCESS;

  inv_idx_scan_param_.tablet_id_ = inv_idx_tablet_id_;
  inv_idx_scan_param_.ls_id_ = ls_id_;
  if (need_inv_idx_agg_) {
    inv_idx_agg_param_.tablet_id_ = inv_idx_tablet_id_;
    inv_idx_agg_param_.ls_id_ = ls_id_;
  }
  if (OB_FAIL(inverted_idx_scan_iter_->rescan())) {
    LOG_WARN("failed to rescan inverted scan iter", K(ret));
  } else if (need_inv_idx_agg_ && OB_FAIL(inverted_idx_agg_iter_->rescan())) {
    LOG_WARN("failed to  rescan inverted index agg iter", K(ret));
  }
  return ret;
}

int ObDASTextRetrievalIter::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("retrieval iterator not inited", K(ret));
  } else if (!inv_idx_agg_evaluated_ && need_inv_idx_agg_) {
    if (OB_FAIL(do_doc_cnt_agg())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("Fail to do document count aggregation", K(ret), K_(inv_idx_agg_param));
      }
    } else {
      inv_idx_agg_evaluated_ = true;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_next_single_row(inv_idx_scan_param_.op_->is_vectorized(), inverted_idx_scan_iter_))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get next row from inverted index", K(ret), K_(inv_idx_scan_param), KPC_(inverted_idx_scan_iter));
    }
  } else {
    LOG_DEBUG("get one invert index scan row", "row",
        ROWEXPR2STR(*ir_rtdef_->get_inv_idx_scan_rtdef()->eval_ctx_,
        *inv_idx_scan_param_.output_exprs_));
    if (ir_ctdef_->need_calc_relevance()) {
      clear_row_wise_evaluated_flag();
      if (OB_FAIL(get_next_doc_token_cnt(need_fwd_idx_agg_))) {
        LOG_WARN("failed to get next doc token count", K(ret));
      } else if (OB_FAIL(fill_token_doc_cnt())) {
        LOG_WARN("failed to get token doc cnt", K(ret));
      } else if (OB_FAIL(project_relevance_expr())) {
        LOG_WARN("failed to evaluate simarity expr", K(ret));
      }
    }
  }
  return ret;
}

int ObDASTextRetrievalIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  UNUSEDx(count, capacity);
  return OB_NOT_IMPLEMENT;
}

int ObDASTextRetrievalIter::init_inv_idx_scan_param()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_base_idx_scan_param(
      ls_id_,
      inv_idx_tablet_id_,
      ir_ctdef_->get_inv_idx_scan_ctdef(),
      ir_rtdef_->get_inv_idx_scan_rtdef(),
      tx_desc_,
      snapshot_,
      inv_idx_scan_param_))) {
    LOG_WARN("fail to init inverted index scan param", K(ret), KPC_(ir_ctdef));
  } else if (need_inv_idx_agg_) {
    if (OB_FAIL(init_base_idx_scan_param(
        ls_id_,
        inv_idx_tablet_id_,
        ir_ctdef_->get_inv_idx_agg_ctdef(),
        ir_rtdef_->get_inv_idx_agg_rtdef(),
        tx_desc_,
        snapshot_,
        inv_idx_agg_param_))) {
      LOG_WARN("fail to init inverted index count aggregate param", K(ret), KPC_(ir_ctdef));
    } else {
      if (OB_UNLIKELY(!static_cast<sql::ObStoragePushdownFlag>(
          ir_ctdef_->get_inv_idx_agg_ctdef()->pd_expr_spec_.pd_storage_flag_).is_aggregate_pushdown())) {
        ret = OB_NOT_IMPLEMENT;
        LOG_ERROR("not pushdown aggregate not supported", K(ret), KPC(ir_ctdef_->get_inv_idx_agg_ctdef()));
      }
    }
  }

  return ret;
}

int ObDASTextRetrievalIter::init_fwd_idx_scan_param()
{
  int ret = OB_SUCCESS;

  if (!ir_ctdef_->need_calc_relevance()) {
  } else if (OB_FAIL(init_base_idx_scan_param(
      ls_id_,
      fwd_idx_tablet_id_,
      ir_ctdef_->get_fwd_idx_agg_ctdef(),
      ir_rtdef_->get_fwd_idx_agg_rtdef(),
      tx_desc_,
      snapshot_,
      fwd_idx_scan_param_))) {
    LOG_WARN("Fail to init foward index scan param", K(ret), KPC_(ir_ctdef));
  }
  return ret;
}

int ObDASTextRetrievalIter::init_base_idx_scan_param(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const sql::ObDASScanCtDef *ctdef,
    sql::ObDASScanRtDef *rtdef,
    transaction::ObTxDesc *tx_desc,
    transaction::ObTxReadSnapshot *snapshot,
    ObTableScanParam &scan_param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctdef) || OB_ISNULL(rtdef)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(ctdef), KPC(rtdef), K(ls_id), K(tablet_id));
  } else {
    uint64_t tenant_id = MTL_ID();
    scan_param.tenant_id_ = tenant_id;
    scan_param.key_ranges_.set_attr(ObMemAttr(tenant_id, "ScanParamKR"));
    scan_param.ss_key_ranges_.set_attr(ObMemAttr(tenant_id, "ScanParamSSKR"));
    scan_param.tx_lock_timeout_ = rtdef->tx_lock_timeout_;
    scan_param.index_id_ = ctdef->ref_table_id_;
    scan_param.is_get_ = false; // scan
    scan_param.is_for_foreign_check_ = false;
    scan_param.timeout_ = rtdef->timeout_ts_;
    scan_param.scan_flag_ = rtdef->scan_flag_;
    scan_param.reserved_cell_count_ = ctdef->access_column_ids_.count();
    scan_param.allocator_ = &rtdef->stmt_allocator_;
    scan_param.scan_allocator_ = &rtdef->scan_allocator_;
    scan_param.sql_mode_ = rtdef->sql_mode_;
    scan_param.frozen_version_ = rtdef->frozen_version_;
    scan_param.force_refresh_lc_ = rtdef->force_refresh_lc_;
    scan_param.output_exprs_ = &(ctdef->pd_expr_spec_.access_exprs_);
    scan_param.calc_exprs_ = &(ctdef->pd_expr_spec_.calc_exprs_);
    scan_param.aggregate_exprs_ = &(ctdef->pd_expr_spec_.pd_storage_aggregate_output_);
    scan_param.table_param_ = &(ctdef->table_param_);
    scan_param.op_ = rtdef->p_pd_expr_op_;
    scan_param.row2exprs_projector_ = rtdef->p_row2exprs_projector_;
    scan_param.schema_version_ = ctdef->schema_version_;
    scan_param.tenant_schema_version_ = rtdef->tenant_schema_version_;
    scan_param.limit_param_ = rtdef->limit_param_;
    scan_param.need_scn_ = rtdef->need_scn_;
    scan_param.pd_storage_flag_ = ctdef->pd_expr_spec_.pd_storage_flag_.pd_flag_;
    scan_param.fb_snapshot_ = rtdef->fb_snapshot_;
    scan_param.fb_read_tx_uncommitted_ = rtdef->fb_read_tx_uncommitted_;
    scan_param.ls_id_ = ls_id;
    scan_param.tablet_id_ = tablet_id;
    if (!ctdef->pd_expr_spec_.pushdown_filters_.empty()) {
      scan_param.op_filters_ = &ctdef->pd_expr_spec_.pushdown_filters_;
    }
    scan_param.pd_storage_filters_ = rtdef->p_pd_expr_op_->pd_storage_filters_;
    if (OB_NOT_NULL(tx_desc)) {
      scan_param.tx_id_ = tx_desc->get_tx_id();
    } else {
      scan_param.tx_id_.reset();
    }

    if (OB_NOT_NULL(snapshot)) {
      scan_param.snapshot_ = *snapshot;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("null snapshot", K(ret), KP(snapshot));
    }

    if (FAILEDx(scan_param.column_ids_.assign(ctdef->access_column_ids_))) {
      LOG_WARN("failed to init column ids", K(ret));
    }
  }
  return ret;
}

int ObDASTextRetrievalIter::do_doc_cnt_agg()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!static_cast<sql::ObStoragePushdownFlag>(inv_idx_agg_param_.pd_storage_flag_).is_aggregate_pushdown())) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("aggregate without pushdown not supported", K(ret));
  } else if (OB_FAIL(get_next_single_row(inv_idx_agg_param_.op_->is_vectorized(), inverted_idx_agg_iter_))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get aggregated row from iter", K(ret));
    }
  } else {
    const sql::ObExpr *inv_idx_agg_expr = inv_idx_agg_param_.aggregate_exprs_->at(0);
    sql::ObEvalCtx *eval_ctx = ir_rtdef_->get_inv_idx_agg_rtdef()->eval_ctx_;
    ObDatum *doc_cnt_datum = nullptr;
    if (OB_FAIL(inv_idx_agg_expr->eval(*eval_ctx, doc_cnt_datum))) {
      LOG_WARN("failed to evaluate aggregated expr", K(ret));
    } else {
      token_doc_cnt_ = doc_cnt_datum->get_int();
    }
  }
  return ret;
}

int ObDASTextRetrievalIter::get_next_doc_token_cnt(const bool use_fwd_idx_agg)
{
  int ret = OB_SUCCESS;
  if (use_fwd_idx_agg) {
    common::ObDocId cur_doc_id;
    int64_t token_cnt = 0;
    if (OB_FAIL(get_inv_idx_scan_doc_id(cur_doc_id))) {
      LOG_WARN("failed to get current doc id", K(ret));
    } else if (OB_FAIL(do_token_cnt_agg(cur_doc_id, token_cnt))) {
      LOG_WARN("failed to do token count agg on fwd index", K(ret));
    }
  } else {
    if (OB_FAIL(fill_token_cnt_with_doc_len())) {
      LOG_WARN("failed to fill token cnt with document length", K(ret));
    }
  }
  return ret;
}

int ObDASTextRetrievalIter::get_inv_idx_scan_doc_id(ObDocId &doc_id)
{
  int ret = OB_SUCCESS;
  sql::ObExpr *doc_id_expr = ir_ctdef_->inv_scan_doc_id_col_;
  sql::ObEvalCtx *eval_ctx = ir_rtdef_->get_inv_idx_scan_rtdef()->eval_ctx_;
  ObDatum &doc_id_datum = doc_id_expr->locate_expr_datum(*eval_ctx);
  if (OB_FAIL(doc_id.from_string(doc_id_datum.get_string()))) {
    LOG_WARN("failed to get ObDocId from datum", K(ret));
  }

  return ret;
}

int ObDASTextRetrievalIter::do_token_cnt_agg(const ObDocId &doc_id, int64_t &token_count)
{
  int ret = OB_SUCCESS;

  token_count = 0;
  ObNewRange scan_range;
  if (OB_FAIL(gen_fwd_idx_scan_range(doc_id, scan_range))) {
    LOG_WARN("failed to generate forward index scan range", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (not_first_fwd_agg_) {
      fwd_idx_scan_param_.tablet_id_ = fwd_idx_tablet_id_;
      fwd_idx_scan_param_.ls_id_ = ls_id_;
      if (OB_FAIL(reuse_fwd_idx_iter())) {
        LOG_WARN("failed to reuse forward index iterator", K(ret));
      } else if (OB_FAIL(fwd_idx_scan_param_.key_ranges_.push_back(scan_range))) {
        LOG_WARN("failed to add forward index scan range", K(ret), K(scan_range));
      } else if (OB_FAIL(forward_idx_iter_->rescan())) {
        LOG_WARN("failed to rescan forward index", K(ret));
      }
    } else {
      if (OB_FAIL(init_fwd_idx_scan_param())) {
        LOG_WARN("failed to init forward index scan param", K(ret));
      } else if (OB_FAIL(fwd_idx_scan_param_.key_ranges_.push_back(scan_range))) {
        LOG_WARN("failed to add forward index scan range", K(ret), K(scan_range));
      } else if (FALSE_IT(forward_idx_iter_->set_scan_param(fwd_idx_scan_param_))) {
      } else if (OB_FAIL(forward_idx_iter_->do_table_scan())) {
        LOG_WARN("failed to do forward index scan", K(ret));
      } else {
        not_first_fwd_agg_ = true;
      }
    }

    if (OB_SUCC(ret)) {
      if (!static_cast<sql::ObStoragePushdownFlag>(
          ir_ctdef_->get_fwd_idx_agg_ctdef()->pd_expr_spec_.pd_storage_flag_).is_aggregate_pushdown()) {
        ret = OB_NOT_IMPLEMENT;
        LOG_ERROR("aggregate without pushdown not implemented", K(ret));
      } else {
        if (OB_FAIL(forward_idx_iter_->get_next_row())) {
          LOG_WARN("failed to get next row from forward index iterator", K(ret));
        } else {
          const sql::ObExpr *agg_expr = ir_ctdef_->get_fwd_idx_agg_ctdef()->pd_expr_spec_.pd_storage_aggregate_output_.at(0);
          sql::ObEvalCtx *eval_ctx = ir_rtdef_->get_fwd_idx_agg_rtdef()->eval_ctx_;
          const ObDatum &word_cnt_datum = agg_expr->locate_expr_datum(*eval_ctx);
          token_count = word_cnt_datum.get_int();
          LOG_DEBUG("retrieval iterator get token cnt for doc", K(ret), K(doc_id), K(token_count));
        }
      }
    }
  }

  return ret;
}

int ObDASTextRetrievalIter::fill_token_cnt_with_doc_len()
{
  int ret = OB_SUCCESS;
  const sql::ObExpr *agg_expr = doc_token_cnt_expr_;
  const sql::ObExpr *doc_length_expr = ir_ctdef_->inv_scan_doc_length_col_;
  sql::ObEvalCtx *eval_ctx = ir_rtdef_->eval_ctx_;
  ObDatum *doc_length_datum = nullptr;
  if (OB_ISNULL(agg_expr) || OB_ISNULL(doc_length_expr) || OB_ISNULL(eval_ctx)
      || OB_UNLIKELY(agg_expr->datum_meta_.get_type() != ObDecimalIntType)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr", K(ret), KPC(agg_expr), KP(doc_length_expr), KP(eval_ctx));
  } else if (OB_FAIL(doc_length_expr->eval(*eval_ctx, doc_length_datum))) {
    LOG_WARN("failed to evaluate document length expr", K(ret));
  } else {
    ObDatum &agg_datum = agg_expr->locate_datum_for_write(*eval_ctx);
    agg_datum.set_decimal_int(doc_length_datum->get_uint());
  }
  return ret;
}

int ObDASTextRetrievalIter::fill_token_doc_cnt()
{
  int ret = OB_SUCCESS;
  const sql::ObExpr *inv_idx_agg_expr = inv_idx_agg_param_.aggregate_exprs_->at(0);
  sql::ObEvalCtx *eval_ctx = ir_rtdef_->get_inv_idx_agg_rtdef()->eval_ctx_;
  if (OB_ISNULL(inv_idx_agg_expr) || OB_ISNULL(eval_ctx)
      || OB_UNLIKELY(inv_idx_agg_expr->datum_meta_.get_type() != ObIntType)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr", K(ret), KP(inv_idx_agg_expr), KP(eval_ctx));
  } else {
    ObDatum &doc_cnt_datum = inv_idx_agg_expr->locate_datum_for_write(*eval_ctx);
    doc_cnt_datum.set_int(token_doc_cnt_);
  }
  return ret;
}

int ObDASTextRetrievalIter::project_relevance_expr()
{
  int ret = OB_SUCCESS;
  sql::ObExpr *relevance_expr = ir_ctdef_->relevance_expr_;
  ObDatum *relevance_datum = nullptr;
  if (OB_ISNULL(relevance_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid relevance expr", K(ret));
  } else if (OB_FAIL(relevance_expr->eval(*ir_rtdef_->eval_ctx_, relevance_datum))) {
    LOG_WARN("failed to evaluate relevance", K(ret));
  }
  return ret;
}

int ObDASTextRetrievalIter::reuse_fwd_idx_iter()
{
  int ret = OB_SUCCESS;
  if (nullptr != forward_idx_iter_) {
    if (OB_FAIL(forward_idx_iter_->reuse())) {
      LOG_WARN("failed to reuse forward index iter", K(ret));
    }
  }
  return ret;
}

int ObDASTextRetrievalIter::gen_inv_idx_scan_range(const ObString &query_token, ObNewRange &scan_range)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  ObObj *obj_ptr = nullptr;
  common::ObArenaAllocator &ctx_alloc = mem_context_->get_arena_allocator();
  constexpr int64_t obj_cnt = INV_IDX_ROWKEY_COL_CNT * 2;
  ObObj tmp_obj;
  tmp_obj.set_string(ObVarcharType, query_token);
  // We need to ensure collation type / level between query text and token column is compatible
  tmp_obj.set_meta_type(ir_ctdef_->search_text_->obj_meta_);

  if (OB_ISNULL(buf = ctx_alloc.alloc(sizeof(ObObj) * obj_cnt))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for rowkey obj", K(ret));
  } else if (OB_ISNULL(obj_ptr = new (buf) ObObj[obj_cnt])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret));
  } else if (OB_FAIL(ob_write_obj(ctx_alloc, tmp_obj, obj_ptr[0]))) {
    LOG_WARN("failed to write obj", K(ret));
  } else if (OB_FAIL(ob_write_obj(ctx_alloc, tmp_obj, obj_ptr[2]))) {
    LOG_WARN("failed to write obj", K(ret));
  } else {
    obj_ptr[1].set_min_value();
    obj_ptr[3].set_max_value();
    ObRowkey start_key(obj_ptr, INV_IDX_ROWKEY_COL_CNT);
    ObRowkey end_key(&obj_ptr[2], INV_IDX_ROWKEY_COL_CNT);
    common::ObTableID inv_table_id = ir_ctdef_->get_inv_idx_scan_ctdef()->ref_table_id_;
    scan_range.table_id_ = inv_table_id;
    scan_range.start_key_.assign(obj_ptr, INV_IDX_ROWKEY_COL_CNT);
    scan_range.end_key_.assign(&obj_ptr[2], INV_IDX_ROWKEY_COL_CNT);
    scan_range.border_flag_.set_inclusive_start();
    scan_range.border_flag_.set_inclusive_end();
  }
  return ret;
}

int ObDASTextRetrievalIter::gen_fwd_idx_scan_range(const ObDocId &doc_id, ObNewRange &scan_range)
{
  int ret = OB_SUCCESS;
  if (nullptr == fwd_range_objs_) {
    void *buf = nullptr;
    common::ObArenaAllocator &ctx_alloc = mem_context_->get_arena_allocator();
    constexpr int64_t obj_cnt = FWD_IDX_ROWKEY_COL_CNT * 2;
    if (OB_ISNULL(buf = ctx_alloc.alloc(sizeof(ObObj) * obj_cnt))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for rowkey obj", K(ret));
    } else if (OB_ISNULL(fwd_range_objs_ = new (buf) ObObj[obj_cnt])) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    fwd_range_objs_[0].set_varbinary(doc_id.get_string());
    fwd_range_objs_[1].set_min_value();
    fwd_range_objs_[2].set_varbinary(doc_id.get_string());
    fwd_range_objs_[3].set_max_value();
    scan_range.table_id_ = ir_ctdef_->get_fwd_idx_agg_ctdef()->ref_table_id_;
    scan_range.start_key_.assign(fwd_range_objs_, FWD_IDX_ROWKEY_COL_CNT);
    scan_range.end_key_.assign(&fwd_range_objs_[2], FWD_IDX_ROWKEY_COL_CNT);
    scan_range.border_flag_.set_inclusive_start();
    scan_range.border_flag_.set_inclusive_end();
  }
  return ret;
}

int ObDASTextRetrievalIter::init_calc_exprs()
{
  int ret = OB_SUCCESS;
  if (ir_ctdef_->need_calc_relevance()) {
    sql::ObExpr *relevance_expr = ir_ctdef_->relevance_expr_;
    sql::ObEvalCtx *eval_ctx = ir_rtdef_->eval_ctx_;
    if (OB_ISNULL(relevance_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null relevance expr", K(ret));
    } else if (OB_FAIL(calc_exprs_.push_back(relevance_expr))) {
      LOG_WARN("failed to append relevance expr", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < relevance_expr->arg_cnt_; ++i) {
      sql::ObExpr *arg_expr = relevance_expr->args_[i];
      if (OB_ISNULL(arg_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null arg expr", K(ret));
      } else if (T_FUN_SYS_CAST == arg_expr->type_) {
        // cast expr is evaluated with relevance expr
        if (OB_FAIL(calc_exprs_.push_back(arg_expr))) {
          LOG_WARN("failed to append cast expr", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      sql::ObExpr *doc_token_cnt_param_expr = relevance_expr->args_[sql::ObExprBM25::DOC_TOKEN_CNT_PARAM_IDX];
      if (T_FUN_SYS_CAST == doc_token_cnt_param_expr->type_) {
        doc_token_cnt_param_expr = doc_token_cnt_param_expr->args_[0];
      }
      if (OB_UNLIKELY(doc_token_cnt_param_expr->type_ != T_FUN_SUM)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected doc token cnt expr type", K(ret), KPC(doc_token_cnt_param_expr));
      } else {
        doc_token_cnt_expr_ = doc_token_cnt_param_expr;
      }
    }
  }
  return ret;
}

void ObDASTextRetrievalIter::clear_row_wise_evaluated_flag()
{
  sql::ObEvalCtx *eval_ctx = ir_rtdef_->eval_ctx_;
  for (int64_t i = 0; i < calc_exprs_.count(); ++i) {
    sql::ObExpr *expr = calc_exprs_.at(i);
    if (expr->is_batch_result()) {
      expr->get_evaluated_flags(*eval_ctx).unset(eval_ctx->get_batch_idx());
    } else {
      expr->get_eval_info(*eval_ctx).clear_evaluated_flag();
    }
  }
}

} // namespace sql
} // namespace oceanbase
