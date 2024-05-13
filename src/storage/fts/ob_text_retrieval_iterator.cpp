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

#define USING_LOG_PREFIX STORAGE_FTS

#include "sql/engine/expr/ob_expr_bm25.h"
#include "sql/das/ob_text_retrieval_op.h"
#include "storage/fts/ob_text_retrieval_iterator.h"
#include "storage/tx_storage/ob_access_service.h"

namespace oceanbase
{
namespace storage
{

bool ObTokenRetrievalParam::need_relevance() const
{
  OB_ASSERT(nullptr != ir_ctdef_);
  return ir_ctdef_->need_calc_relevance();
}
const share::ObLSID &ObTokenRetrievalParam::get_ls_id() const
{
  return ls_id_;
}

const sql::ObDASIRScanCtDef *ObTokenRetrievalParam::get_ir_ctdef() const
{
  return ir_ctdef_;
}

sql::ObDASIRScanRtDef *ObTokenRetrievalParam::get_ir_rtdef()
{
  return ir_rtdef_;
}
const sql::ObDASScanCtDef *ObTokenRetrievalParam::get_inv_idx_scan_ctdef() const
{
  OB_ASSERT(nullptr != ir_ctdef_);
  return ir_ctdef_->get_inv_idx_scan_ctdef();
}

const sql::ObDASScanCtDef *ObTokenRetrievalParam::get_inv_idx_agg_ctdef() const
{
  OB_ASSERT(nullptr != ir_ctdef_);
  return ir_ctdef_->get_inv_idx_agg_ctdef();
}

const sql::ObDASScanCtDef *ObTokenRetrievalParam::get_fwd_idx_agg_ctdef() const
{
  OB_ASSERT(nullptr != ir_ctdef_);
  return ir_ctdef_->get_fwd_idx_agg_ctdef();
}

const sql::ObDASScanCtDef *ObTokenRetrievalParam::get_doc_id_idx_agg_ctdef() const
{
  OB_ASSERT(nullptr != ir_ctdef_);
  return ir_ctdef_->get_doc_id_idx_agg_ctdef();
}

const common::ObTabletID &ObTokenRetrievalParam::get_inv_idx_tablet_id() const
{
  return inv_idx_tablet_id_;
}
const common::ObTabletID &ObTokenRetrievalParam::get_fwd_idx_tablet_id() const
{
  return fwd_idx_tablet_id_;
}

const common::ObTabletID &ObTokenRetrievalParam::get_doc_id_idx_tablet_id() const
{
  return doc_id_idx_tablet_id_;
}

ObTextRetrievalIterator::ObTextRetrievalIterator()
  : ObNewRowIterator(),
    mem_context_(nullptr),
    retrieval_param_(nullptr),
    tx_desc_(nullptr),
    snapshot_(nullptr),
    inv_idx_scan_param_(),
    inv_idx_agg_param_(),
    fwd_idx_scan_param_(),
    calc_exprs_(),
    inverted_idx_iter_(nullptr),
    forward_idx_iter_(nullptr),
    fwd_range_objs_(nullptr),
    doc_token_cnt_expr_(nullptr),
    token_doc_cnt_(0),
    need_fwd_idx_agg_(false),
    need_inv_idx_agg_(false),
    inv_idx_agg_evaluated_(false),
    is_inited_(false)
{
}

ObTextRetrievalIterator::~ObTextRetrievalIterator()
{
  reset();
}

int ObTextRetrievalIterator::init(
    ObTokenRetrievalParam &retrieval_param,
    const ObString &query_token,
    transaction::ObTxDesc *tx_desc,
    transaction::ObTxReadSnapshot *snapshot)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double initialization", K(ret), KPC(this));
  } else if (OB_UNLIKELY(nullptr == tx_desc || nullptr == snapshot || !tx_desc->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(tx_desc), KPC(snapshot));
  } else {
    retrieval_param_ = &retrieval_param;
    tx_desc_ = tx_desc;
    snapshot_ = snapshot;
    need_fwd_idx_agg_ = retrieval_param.get_ir_ctdef()->has_fwd_agg_;
    need_inv_idx_agg_ = retrieval_param.need_relevance();

    if (OB_ISNULL(mem_context_)) {
      lib::ContextParam param;
      param.set_mem_attr(MTL_ID(), "TextIRIter", ObCtxIds::DEFAULT_CTX_ID);
      if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
        LOG_WARN("failed to create text retrieval iterator memory context", K(ret));
      }
    }

    if (FAILEDx(init_inv_idx_scan_param(query_token))) {
      LOG_WARN("failed to init inverted index scan param", K(ret), K_(inv_idx_scan_param), K_(inv_idx_agg_param));
    } else if (need_fwd_idx_agg_ && OB_FAIL(init_fwd_idx_scan_param())) {
      LOG_WARN("failed to init forward index scan param", K(ret), K_(fwd_idx_scan_param));
    } else if (OB_FAIL(init_calc_exprs())) {
      LOG_WARN("failed to init row-wise calc exprs", K(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

void ObTextRetrievalIterator::reset()
{
  int ret = OB_SUCCESS;
  ObAccessService *tsc_service = MTL(ObAccessService *);
  if (nullptr == tsc_service) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("failed to get access service when reset text retrieval iterator", K(ret));
  } else {
    if (nullptr != inverted_idx_iter_) {
      if (OB_FAIL(tsc_service->revert_scan_iter(inverted_idx_iter_))) {
        LOG_ERROR("failed to revert inverted index iter", K(ret));
      }
      inverted_idx_iter_ = nullptr;
    }
    if (nullptr != forward_idx_iter_) {
      if (OB_FAIL(tsc_service->revert_scan_iter(forward_idx_iter_))) {
        LOG_ERROR("failed to revert forward index iter", K(ret));
      }
      forward_idx_iter_ = nullptr;
    }
  }
  inv_idx_scan_param_.need_switch_param_ = false;
  inv_idx_scan_param_.destroy_schema_guard();
  inv_idx_agg_param_.need_switch_param_ = false;
  inv_idx_agg_param_.destroy_schema_guard();
  fwd_idx_scan_param_.need_switch_param_ = false;
  fwd_idx_scan_param_.destroy_schema_guard();
  calc_exprs_.reset();

  if (nullptr != mem_context_)  {
    mem_context_->reset_remain_one_page();
    DESTROY_CONTEXT(mem_context_);
    mem_context_ = nullptr;
  }
  fwd_range_objs_ = nullptr;
  doc_token_cnt_expr_ = nullptr;
  retrieval_param_ = nullptr;
  tx_desc_ = nullptr;
  snapshot_ = nullptr;
  token_doc_cnt_ = 0;
  need_fwd_idx_agg_ = false;
  need_inv_idx_agg_ = false;
  inv_idx_agg_evaluated_ = false;
  is_inited_ = false;
}

int ObTextRetrievalIterator::get_next_row(ObNewRow *&row)
{
  UNUSED(row);
  return OB_NOT_IMPLEMENT;
}

int ObTextRetrievalIterator::get_next_row()
{
  int ret = OB_SUCCESS;
  ObAccessService *tsc_service = MTL(ObAccessService *);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("retrieval iterator not inited", K(ret));
  } else if (!inv_idx_agg_evaluated_) {
    if (OB_FAIL(do_doc_cnt_agg())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("Fail to do document count aggregation", K(ret), K_(inv_idx_agg_param));
      }
    } else if (OB_FAIL(tsc_service->revert_scan_iter(inverted_idx_iter_))) {
      LOG_WARN("Fail to revert inverted index scan iterator after count aggregation", K(ret));
    } else if (FALSE_IT(inverted_idx_iter_ = nullptr)) {
    } else if (OB_FAIL(tsc_service->table_scan(inv_idx_scan_param_, inverted_idx_iter_))) {
      LOG_WARN("failed to init inverted index scan iterator", K(ret));
    } else {
      inv_idx_agg_evaluated_ = true;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_next_single_row(inv_idx_scan_param_.op_->is_vectorized(), inverted_idx_iter_))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get next row from inverted index", K(ret), K_(inv_idx_scan_param), KPC_(inverted_idx_iter));
    }
  } else {
    LOG_DEBUG("get one invert index scan row", "row",
        ROWEXPR2STR(*retrieval_param_->get_ir_rtdef()->get_inv_idx_scan_rtdef()->eval_ctx_,
        *inv_idx_scan_param_.output_exprs_));
    clear_row_wise_evaluated_flag();
    if (OB_FAIL(get_next_doc_token_cnt(need_fwd_idx_agg_))) {
      LOG_WARN("failed to get next doc token count", K(ret));
    } else if (OB_FAIL(fill_token_doc_cnt())) {
      LOG_WARN("failed to get token doc cnt", K(ret));
    } else if (OB_FAIL(project_relevance_expr())) {
      LOG_WARN("failed to evaluate simarity expr", K(ret));
    }
  }
  return ret;
}

int ObTextRetrievalIterator::get_next_rows(int64_t &count, int64_t capacity)
{
  UNUSEDx(count, capacity);
  return OB_NOT_IMPLEMENT;
}

int ObTextRetrievalIterator::get_curr_iter_row(
    const sql::ExprFixedArray *&curr_row,
    sql::ObEvalCtx *&curr_eval_ctx)
{
  UNUSEDx(curr_row, curr_eval_ctx);
  return OB_NOT_IMPLEMENT;
}

int ObTextRetrievalIterator::get_curr_doc_id()
{
  return OB_NOT_IMPLEMENT;
}

int ObTextRetrievalIterator::forward_to_doc(const ObDocId &doc_id)
{
  UNUSED(doc_id);
  return OB_NOT_IMPLEMENT;
}

int ObTextRetrievalIterator::init_inv_idx_scan_param(const ObString &query_token)
{
  int ret = OB_SUCCESS;
  ObNewRange inv_idx_scan_range;
  if (OB_FAIL(gen_inv_idx_scan_range(query_token, inv_idx_scan_range))) {
    LOG_WARN("failed to generate inverted index scan range", K(ret), K(query_token));
  } else if (OB_FAIL(init_base_idx_scan_param(
      retrieval_param_->get_ls_id(),
      retrieval_param_->get_inv_idx_tablet_id(),
      retrieval_param_->get_inv_idx_scan_ctdef(),
      retrieval_param_->get_ir_rtdef()->get_inv_idx_scan_rtdef(),
      tx_desc_,
      snapshot_,
      inv_idx_scan_param_))) {
    LOG_WARN("fail to init inverted index scan param", K(ret), KPC_(retrieval_param));
  } else if (OB_FAIL(inv_idx_scan_param_.key_ranges_.push_back(inv_idx_scan_range))) {
    LOG_WARN("failed to append scan range", K(ret));
  }

  if (OB_SUCC(ret) && need_inv_idx_agg_) {
    if (OB_FAIL(init_base_idx_scan_param(
        retrieval_param_->get_ls_id(),
        retrieval_param_->get_inv_idx_tablet_id(),
        retrieval_param_->get_inv_idx_agg_ctdef(),
        retrieval_param_->get_ir_rtdef()->get_inv_idx_agg_rtdef(),
        tx_desc_,
        snapshot_,
        inv_idx_agg_param_))) {
      LOG_WARN("fail to init inverted index count aggregate param", K(ret), KPC_(retrieval_param));
    } else if (OB_FAIL(inv_idx_agg_param_.key_ranges_.push_back(inv_idx_scan_range))) {
      LOG_WARN("failed to append scan range", K(ret));
    } else {
      if (OB_UNLIKELY(!static_cast<sql::ObStoragePushdownFlag>(
          retrieval_param_->get_inv_idx_agg_ctdef()->pd_expr_spec_.pd_storage_flag_).is_aggregate_pushdown())) {
        ret = OB_NOT_IMPLEMENT;
        LOG_ERROR("not pushdown aggregate not supported", K(ret), K_(retrieval_param));
      }
    }
  }

  return ret;
}

int ObTextRetrievalIterator::init_fwd_idx_scan_param()
{
  int ret = OB_SUCCESS;

  if (!retrieval_param_->need_relevance()) {
  } else if (OB_FAIL(init_base_idx_scan_param(
      retrieval_param_->get_ls_id(),
      retrieval_param_->get_fwd_idx_tablet_id(),
      retrieval_param_->get_fwd_idx_agg_ctdef(),
      retrieval_param_->get_ir_rtdef()->get_fwd_idx_agg_rtdef(),
      tx_desc_,
      snapshot_,
      fwd_idx_scan_param_))) {
    LOG_WARN("Fail to init foward index scan param", K(ret), KPC_(retrieval_param));
  }
  return ret;
}

int ObTextRetrievalIterator::init_base_idx_scan_param(
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

int ObTextRetrievalIterator::do_doc_cnt_agg()
{
  int ret = OB_SUCCESS;
  ObAccessService *tsc_service = MTL(ObAccessService *);
  if (OB_ISNULL(tsc_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get table access service", K(ret));
  } else if (OB_FAIL(tsc_service->table_scan(inv_idx_agg_param_, inverted_idx_iter_))) {
    if (OB_SNAPSHOT_DISCARDED == ret && inv_idx_agg_param_.fb_snapshot_.is_valid()) {
      ret = OB_INVALID_QUERY_TIMESTAMP;
    } else if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("failed to do table scan for document count aggregation", K(ret));
    }
  } else {
    if (OB_UNLIKELY(!static_cast<sql::ObStoragePushdownFlag>(inv_idx_agg_param_.pd_storage_flag_).is_aggregate_pushdown())) {
      ret = OB_NOT_SUPPORTED;
      LOG_ERROR("aggregate without pushdown not supported", K(ret));
    } else if (OB_FAIL(get_next_single_row(inv_idx_agg_param_.op_->is_vectorized(), inverted_idx_iter_))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to get aggregated row from iter", K(ret));
      }
    } else {
      const sql::ObExpr *inv_idx_agg_expr = inv_idx_agg_param_.aggregate_exprs_->at(0);
      sql::ObEvalCtx *eval_ctx = retrieval_param_->get_ir_rtdef()->get_inv_idx_agg_rtdef()->eval_ctx_;
      ObDatum *doc_cnt_datum = nullptr;
      if (OB_FAIL(inv_idx_agg_expr->eval(*eval_ctx, doc_cnt_datum))) {
        LOG_WARN("failed to evaluate aggregated expr", K(ret));
      } else {
        token_doc_cnt_ = doc_cnt_datum->get_int();
      }
    }
  }
  return ret;
}

int ObTextRetrievalIterator::get_next_doc_token_cnt(const bool use_fwd_idx_agg)
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

int ObTextRetrievalIterator::get_inv_idx_scan_doc_id(ObDocId &doc_id)
{
  int ret = OB_SUCCESS;
  sql::ObExpr *doc_id_expr = retrieval_param_->get_ir_ctdef()->inv_scan_doc_id_col_;
  sql::ObEvalCtx *eval_ctx = retrieval_param_->get_ir_rtdef()->get_inv_idx_scan_rtdef()->eval_ctx_;
  ObDatum &doc_id_datum = doc_id_expr->locate_expr_datum(*eval_ctx);
  if (OB_FAIL(doc_id.from_string(doc_id_datum.get_string()))) {
    LOG_WARN("failed to get ObDocId from datum", K(ret));
  }

  return ret;
}

int ObTextRetrievalIterator::do_token_cnt_agg(const ObDocId &doc_id, int64_t &token_count)
{
  int ret = OB_SUCCESS;

  token_count = 0;
  ObNewRange scan_range;
  if (OB_FAIL(reuse_fwd_idx_iter())) {
    LOG_WARN("failed to reuse forward index iterator", K(ret));
  } else if (OB_UNLIKELY(!fwd_idx_scan_param_.key_ranges_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected non empty forward index scan range", K(ret));
  } else if (OB_FAIL(gen_fwd_idx_scan_range(doc_id, scan_range))) {
    LOG_WARN("failed to generate forward index scan range", K(ret));
  } else if (OB_FAIL(fwd_idx_scan_param_.key_ranges_.push_back(scan_range))) {
    LOG_WARN("failed to add forward index scan range", K(ret), K(scan_range));
  }

  if (OB_SUCC(ret)) {
    ObAccessService *tsc_service = MTL(ObAccessService *);
    if (OB_ISNULL(tsc_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get table access service", K(ret));
    } else if (nullptr == forward_idx_iter_) {
      if (OB_FAIL(tsc_service->table_scan(fwd_idx_scan_param_, forward_idx_iter_))) {
        if (OB_SNAPSHOT_DISCARDED == ret && fwd_idx_scan_param_.fb_snapshot_.is_valid()) {
          ret = OB_INVALID_QUERY_TIMESTAMP;
        } else if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
          LOG_WARN("failed to init forward index scan iterator", K(ret), K_(fwd_idx_scan_param));
        }
      }
    } else {
      const ObTabletID &storage_tablet_id = fwd_idx_scan_param_.tablet_id_;
      const bool need_switch_param =
          storage_tablet_id.is_valid() && storage_tablet_id != retrieval_param_->get_fwd_idx_tablet_id();
      fwd_idx_scan_param_.need_switch_param_ = need_switch_param;
      if (OB_FAIL(tsc_service->reuse_scan_iter(need_switch_param, forward_idx_iter_))) {
        LOG_WARN("failed to reuse scan iter", K(ret));
      } else if (OB_FAIL(tsc_service->table_rescan(fwd_idx_scan_param_, forward_idx_iter_))) {
        LOG_WARN("failed to rescan forward index table", K(ret), K_(fwd_idx_scan_param));
      }
    }

    if (OB_SUCC(ret)) {
      if (!static_cast<sql::ObStoragePushdownFlag>(
          retrieval_param_->get_fwd_idx_agg_ctdef()->pd_expr_spec_.pd_storage_flag_).is_aggregate_pushdown()) {
        ret = OB_NOT_IMPLEMENT;
        LOG_ERROR("aggregate without pushdown not implemented", K(ret));
      } else {
        if (OB_FAIL(forward_idx_iter_->get_next_row())) {
          LOG_WARN("failed to get next row from forward index iterator", K(ret));
        } else {
          const sql::ObExpr *agg_expr = retrieval_param_->get_fwd_idx_agg_ctdef()->pd_expr_spec_.pd_storage_aggregate_output_.at(0);
          sql::ObEvalCtx *eval_ctx = retrieval_param_->get_ir_rtdef()->get_fwd_idx_agg_rtdef()->eval_ctx_;
          const ObDatum &word_cnt_datum = agg_expr->locate_expr_datum(*eval_ctx);
          token_count = word_cnt_datum.get_int();
          LOG_DEBUG("retrieval iterator get token cnt for doc", K(ret), K(doc_id), K(token_count));
        }
      }
    }
  }

  return ret;
}

int ObTextRetrievalIterator::fill_token_cnt_with_doc_len()
{
  int ret = OB_SUCCESS;
  const sql::ObExpr *agg_expr = doc_token_cnt_expr_;
  const sql::ObExpr *doc_length_expr = retrieval_param_->get_ir_ctdef()->inv_scan_doc_length_col_;
  sql::ObEvalCtx *eval_ctx = retrieval_param_->get_ir_rtdef()->eval_ctx_;
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

int ObTextRetrievalIterator::fill_token_doc_cnt()
{
  int ret = OB_SUCCESS;
  const sql::ObExpr *inv_idx_agg_expr = inv_idx_agg_param_.aggregate_exprs_->at(0);
  sql::ObEvalCtx *eval_ctx = retrieval_param_->get_ir_rtdef()->get_inv_idx_agg_rtdef()->eval_ctx_;
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

int ObTextRetrievalIterator::project_relevance_expr()
{
  int ret = OB_SUCCESS;
  const sql::ObDASIRScanRtDef *ir_rtdef = retrieval_param_->get_ir_rtdef();
  sql::ObExpr *relevance_expr = retrieval_param_->get_ir_ctdef()->relevance_expr_;
  ObDatum *relevance_datum = nullptr;
  if (OB_ISNULL(relevance_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid relevance expr", K(ret));
  } else if (OB_FAIL(relevance_expr->eval(*ir_rtdef->eval_ctx_, relevance_datum))) {
    LOG_WARN("failed to evaluate relevance", K(ret));
  }
  return ret;
}

int ObTextRetrievalIterator::reuse_fwd_idx_iter()
{
  int ret = OB_SUCCESS;
  if (nullptr != forward_idx_iter_) {
    fwd_idx_scan_param_.key_ranges_.reuse();
  }
  return ret;
}

int ObTextRetrievalIterator::gen_inv_idx_scan_range(const ObString &query_token, ObNewRange &scan_range)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  ObObj *obj_ptr = nullptr;
  common::ObArenaAllocator &ctx_alloc = mem_context_->get_arena_allocator();
  constexpr int64_t obj_cnt = INV_IDX_ROWKEY_COL_CNT * 2;
  ObObj tmp_obj;
  tmp_obj.set_string(ObVarcharType, query_token);
  // We need to ensure collation type / level between query text and token column is compatible
  tmp_obj.set_meta_type(retrieval_param_->get_ir_ctdef()->search_text_->obj_meta_);

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
    common::ObTableID inv_table_id = retrieval_param_->get_inv_idx_scan_ctdef()->ref_table_id_;
    scan_range.table_id_ = inv_table_id;
    scan_range.start_key_.assign(obj_ptr, INV_IDX_ROWKEY_COL_CNT);
    scan_range.end_key_.assign(&obj_ptr[2], INV_IDX_ROWKEY_COL_CNT);
    scan_range.border_flag_.set_inclusive_start();
    scan_range.border_flag_.set_inclusive_end();
  }
  return ret;
}

int ObTextRetrievalIterator::gen_fwd_idx_scan_range(const ObDocId &doc_id, ObNewRange &scan_range)
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
    scan_range.table_id_ = retrieval_param_->get_fwd_idx_agg_ctdef()->ref_table_id_;
    scan_range.start_key_.assign(fwd_range_objs_, FWD_IDX_ROWKEY_COL_CNT);
    scan_range.end_key_.assign(&fwd_range_objs_[2], FWD_IDX_ROWKEY_COL_CNT);
    scan_range.border_flag_.set_inclusive_start();
    scan_range.border_flag_.set_inclusive_end();
  }
  return ret;
}

int ObTextRetrievalIterator::init_calc_exprs()
{
  int ret = OB_SUCCESS;
  if (retrieval_param_->get_ir_ctdef()->need_calc_relevance()) {
    sql::ObExpr *relevance_expr = retrieval_param_->get_ir_ctdef()->relevance_expr_;
    sql::ObEvalCtx *eval_ctx = retrieval_param_->get_ir_rtdef()->eval_ctx_;
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

void ObTextRetrievalIterator::clear_row_wise_evaluated_flag()
{
  sql::ObEvalCtx *eval_ctx = retrieval_param_->get_ir_rtdef()->eval_ctx_;
  for (int64_t i = 0; i < calc_exprs_.count(); ++i) {
    sql::ObExpr *expr = calc_exprs_.at(i);
    if (expr->is_batch_result()) {
      expr->get_evaluated_flags(*eval_ctx).unset(eval_ctx->get_batch_idx());
    } else {
      expr->get_eval_info(*eval_ctx).clear_evaluated_flag();
    }
  }
}

} // end storage
} // end oceanbase
