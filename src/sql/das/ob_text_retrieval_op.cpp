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
#include "sql/das/ob_text_retrieval_op.h"
#include "share/text_analysis/ob_text_analyzer.h"
#include "storage/fts/ob_fts_plugin_helper.h"
#include "storage/tx_storage/ob_access_service.h"

namespace oceanbase
{
namespace sql
{
OB_SERIALIZE_MEMBER((ObDASIRScanCtDef, ObDASAttachCtDef),
                    flags_,
                    search_text_,
                    inv_scan_doc_id_col_,
                    inv_scan_doc_length_col_,
                    match_filter_,
                    relevance_expr_,
                    relevance_proj_col_,
                    estimated_total_doc_cnt_);

OB_SERIALIZE_MEMBER(ObDASIRScanRtDef);

OB_SERIALIZE_MEMBER((ObDASIRAuxLookupCtDef, ObDASAttachCtDef),
                    relevance_proj_col_);

OB_SERIALIZE_MEMBER((ObDASIRAuxLookupRtDef, ObDASAttachRtDef));

ObIRIterLoserTreeItem::ObIRIterLoserTreeItem()
  : relevance_(0), doc_id_(), iter_idx_(-1)
{
}

ObIRIterLoserTreeCmp::ObIRIterLoserTreeCmp()
  : cmp_func_(), is_inited_(false)
{
}

ObIRIterLoserTreeCmp::~ObIRIterLoserTreeCmp()
{
}

int ObIRIterLoserTreeCmp::init()
{
  int ret = OB_SUCCESS;
  sql::ObExprBasicFuncs *basic_funcs = ObDatumFuncs::get_basic_func(ObVarcharType, CS_TYPE_BINARY);
  cmp_func_ = lib::is_oracle_mode() ? basic_funcs->null_last_cmp_ : basic_funcs->null_first_cmp_;
  if (OB_ISNULL(cmp_func_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to init IRIterLoserTreeCmp", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObIRIterLoserTreeCmp::cmp(
    const ObIRIterLoserTreeItem &l,
    const ObIRIterLoserTreeItem &r,
    int64_t &cmp_ret)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    ObDatum l_datum;
    ObDatum r_datum;
    l_datum.set_string(l.doc_id_.get_string());
    r_datum.set_string(r.doc_id_.get_string());
    int tmp_ret = 0;
    if (OB_FAIL(cmp_func_(l_datum, r_datum, tmp_ret))) {
      LOG_WARN("failed to compare doc id by datum", K(ret));
    } else {
      cmp_ret = tmp_ret;
    }
  }
  return ret;
}

ObTextRetrievalMerge::ObTextRetrievalMerge()
  : common::ObNewRowIterator(),
    relation_type_(MAX_RELATION_TYPE),
    processing_type_(MAX_PROC_TYPE),
    allocator_(nullptr),
    retrieval_param_(),
    query_tokens_(),
    loser_tree_cmp_(),
    iter_row_heap_(nullptr),
    next_batch_iter_idxes_(),
    next_batch_cnt_(0),
    whole_doc_cnt_iter_(nullptr),
    whole_doc_agg_param_(),
    doc_cnt_calculated_(false),
    is_inited_(false)
{
}

ObTextRetrievalMerge::~ObTextRetrievalMerge()
{
  reset();
  ObNewRowIterator::~ObNewRowIterator();
}

int ObTextRetrievalMerge::init(
    const share::ObLSID &ls_id,
    const ObTabletID &inv_idx_tablet_id,
    const ObTabletID &fwd_idx_tablet_id,
    const ObTabletID &doc_id_idx_tablet_id,
    const ObDASIRScanCtDef *ir_ctdef,
    ObDASIRScanRtDef *ir_rtdef,
    transaction::ObTxDesc *tx_desc,
    transaction::ObTxReadSnapshot *snapshot,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double initialization", K(ret));
  } else {
    relation_type_ = TokenRelationType::DISJUNCTIVE;
    processing_type_ = RetrievalProcType::DAAT;
    allocator_ = &allocator;

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(loser_tree_cmp_.init())) {
      LOG_WARN("failed to init loser tree comparator", K(ret));
    } else if (OB_ISNULL(iter_row_heap_ = OB_NEWx(ObIRIterLoserTree, allocator_, loser_tree_cmp_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate loser tree", K(ret));
    } else if (OB_FAIL(init_iter_params(ls_id, inv_idx_tablet_id, fwd_idx_tablet_id, doc_id_idx_tablet_id, ir_ctdef, ir_rtdef))) {
      LOG_WARN("failed to init iter params", K(ret));
    } else if (0 == query_tokens_.count()) {
      // empty token set
      LOG_DEBUG("empty query token set after tokenization", K(ret), KPC(ir_ctdef));
      is_inited_ = true;
    } else if (OB_UNLIKELY(query_tokens_.count() > OB_MAX_TEXT_RETRIEVAL_TOKEN_CNT)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("too many query tokens in a single query not supported", K(ret), K_(query_tokens));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "text retrieval query with token count exceed limit");
    } else if (OB_FAIL(iter_row_heap_->init(query_tokens_.count(), *allocator_))) {
      LOG_WARN("failed to init iter loser tree", K(ret));
    } else if (OB_FAIL(init_iters(tx_desc, snapshot, query_tokens_))) {
      LOG_WARN("failed to init iterators", K(ret), K_(query_tokens));
    } else if (OB_FAIL(init_total_doc_cnt_param(tx_desc, snapshot))) {
      LOG_WARN("failed to do total doc cnt", K(ret));
    } else {
      is_inited_ = true;
    }
    LOG_DEBUG("init text retrieval op", K(ret), K_(retrieval_param));
  }

  return ret;
}

int ObTextRetrievalMerge::rescan(
    const share::ObLSID &ls_id,
    const ObTabletID &inv_idx_tablet_id,
    const ObTabletID &fwd_idx_tablet_id,
    const ObTabletID &doc_id_idx_tablet_id,
    const ObDASIRScanCtDef *ir_ctdef,
    ObDASIRScanRtDef *ir_rtdef,
    transaction::ObTxDesc *tx_desc,
    transaction::ObTxReadSnapshot *snapshot,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  // TODO: opt rescan
  reset();
  if (OB_FAIL(init(ls_id,
                   inv_idx_tablet_id,
                   fwd_idx_tablet_id,
                   doc_id_idx_tablet_id,
                   ir_ctdef,
                   ir_rtdef,
                   tx_desc,
                   snapshot,
                   allocator))) {
    LOG_WARN("failed to re init", K(ret));
  }
  return ret;
}

void ObTextRetrievalMerge::reset()
{
  query_tokens_.reset();
  if (nullptr != iter_row_heap_) {
    iter_row_heap_->~ObIRIterLoserTree();
    iter_row_heap_ = nullptr;
  }
  release_iters();
  next_batch_iter_idxes_.reset();
  next_batch_cnt_ = 0;
  allocator_ = nullptr;
  doc_cnt_calculated_ = false;
  is_inited_ = false;
}

int ObTextRetrievalMerge::get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (0 == query_tokens_.count()) {
    ret = OB_ITER_END;
  } else if (!doc_cnt_calculated_) {
    if (OB_FAIL(do_total_doc_cnt())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to do total document count", K(ret), K_(retrieval_param));
      }
    } else {
      doc_cnt_calculated_ = true;
    }
  }

  bool got_valid_document = false;
  ObExpr *match_filter = retrieval_param_.need_relevance()
      ? retrieval_param_.get_ir_ctdef()->match_filter_ : nullptr;
  ObDatum *filter_res = nullptr;
  while (OB_SUCC(ret) && !got_valid_document) {
    clear_evaluated_infos();
    if (OB_FAIL(pull_next_batch_rows())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to pull next batch rows from iterator", K(ret));
      }
    } else if (OB_FAIL(next_disjunctive_document())) {
      LOG_WARN("failed to get next document with disjunctive tokens", K(ret));
    } else if (OB_ISNULL(match_filter)) {
      got_valid_document = true;
    } else if (OB_FAIL(match_filter->eval(*retrieval_param_.get_ir_rtdef()->eval_ctx_, filter_res))) {
      LOG_WARN("failed to evaluate match filter", K(ret));
    } else {
      got_valid_document = !(filter_res->is_null() || 0 == filter_res->get_int());
    }
  }

  return ret;
}

int ObTextRetrievalMerge::get_next_rows(int64_t &count, int64_t capacity)
{
  // only one row at a time
  // TODO: support batch vectorized execution later
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_next_row())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get next row", K(ret));
    }
  } else {
    count += 1;
  }
  return ret;
}

int ObTextRetrievalMerge::init_iter_params(
    const share::ObLSID &ls_id,
    const ObTabletID &inv_idx_tablet_id,
    const ObTabletID &fwd_idx_tablet_id,
    const ObTabletID &doc_id_idx_tablet_id,
    const ObDASIRScanCtDef *ir_ctdef,
    ObDASIRScanRtDef *ir_rtdef)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_query_tokens(ir_ctdef, ir_rtdef))) {
    LOG_WARN("failed to init query tokens", K(ret));
  } else {
    retrieval_param_.set_param(
        ls_id, inv_idx_tablet_id, fwd_idx_tablet_id, doc_id_idx_tablet_id, ir_ctdef, ir_rtdef);
  }
  return ret;
}

int ObTextRetrievalMerge::init_query_tokens(const ObDASIRScanCtDef *ir_ctdef, ObDASIRScanRtDef *ir_rtdef)
{
  int ret = OB_SUCCESS;
  ObExpr *search_text = ir_ctdef->search_text_;
  ObEvalCtx *eval_ctx = ir_rtdef->eval_ctx_;
  ObDatum *search_text_datum = nullptr;
  if (OB_ISNULL(search_text) || OB_ISNULL(eval_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(search_text), KP(eval_ctx));
  } else if (OB_FAIL(search_text->eval(*eval_ctx, search_text_datum))) {
    LOG_WARN("expr evaluation failed", K(ret));
  } else if (0 == search_text_datum->len_) {
    // empty query text
  } else {
    // TODO: FTParseHelper currently does not support deduplicate tokens
    //       We should abstract such universal analyse functors into utility structs
    const ObString &search_text_string = search_text_datum->get_string();
    const ObString &parser_name = ir_ctdef->get_inv_idx_scan_ctdef()->table_param_.get_parser_name();
    const ObCollationType &cs_type = search_text->datum_meta_.cs_type_;
    int64_t doc_length = 0;
    storage::ObFTParseHelper tokenize_helper;
    common::ObSEArray<ObFTWord, 16> tokens;
    hash::ObHashMap<ObFTWord, int64_t> token_map;
    const int64_t ft_word_bkt_cnt = MAX(search_text_string.length() / 10, 2);
    if (OB_FAIL(tokenize_helper.init(allocator_, parser_name))) {
      LOG_WARN("failed to init tokenize helper", K(ret));
    } else if (OB_FAIL(token_map.create(ft_word_bkt_cnt, common::ObMemAttr(MTL_ID(), "FTWordMap")))) {
      LOG_WARN("failed to create token map", K(ret));
    } else if (OB_FAIL(tokenize_helper.segment(
        cs_type, search_text_string.ptr(), search_text_string.length(), doc_length, token_map))) {
      LOG_WARN("failed to segment");
    } else {
      for (hash::ObHashMap<ObFTWord, int64_t>::const_iterator iter = token_map.begin();
          OB_SUCC(ret) && iter != token_map.end();
          ++iter) {
        const ObFTWord &token = iter->first;
        ObString token_string;
        if (OB_FAIL(ob_write_string(*allocator_, token.get_word(), token_string))) {
          LOG_WARN("failed to deep copy query token", K(ret));
        } else if (OB_FAIL(query_tokens_.push_back(token_string))) {
          LOG_WARN("failed to append query token", K(ret));
        }
      }
    }

// TODO: try use this interface instead
/*
    share::ObITokenStream *token_stream = nullptr;
    share::ObTextAnalysisCtx query_analysis_ctx;
    query_analysis_ctx.need_grouping_ = true;
    query_analysis_ctx.filter_stopword_ = true;
    query_analysis_ctx.cs_ = common::ObCharset::get_charset(search_text->obj_meta_.get_collation_type());
    share::ObEnglishTextAnalyzer query_analyzer;
    if (OB_FAIL(query_analyzer.init(query_analysis_ctx, token_analyze_alloc))) {
      LOG_WARN("failed to init query text analyzer", K(ret));
    } else if (OB_FAIL(query_analyzer.analyze(*search_text_datum, token_stream))) {
      LOG_WARN("failed to analyze search text", K(ret), K(query_analysis_ctx), KPC(search_text_datum));
    }
    while (OB_SUCC(ret)) {
      ObDatum token;
      ObString token_string;
      if (OB_FAIL(token_stream->get_next(token))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to get next query token", K(ret));
        }
      } else if (OB_FAIL(ob_write_string(token_analyze_alloc, token.get_string(), token_string))) {
        LOG_WARN("failed to deep copy query token", K(ret));
      } else if (OB_FAIL(query_tokens_.push_back(token_string))) {
        LOG_WARN("failed to append query token", K(ret));
      }
    }

    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to init query tokens", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
*/

    LOG_DEBUG("tokenized text query:", K(ret), KPC(search_text_datum), K_(query_tokens));
  }
  return ret;
}

int ObTextRetrievalMerge::init_iters(
    transaction::ObTxDesc *tx_desc,
    transaction::ObTxReadSnapshot *snapshot,
    const ObIArray<ObString> &query_tokens)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 == query_tokens.count() || query_tokens.count() > OB_MAX_TEXT_RETRIEVAL_TOKEN_CNT)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid empty query tokens", K(ret), K(query_tokens.count()), K(query_tokens));
  } else if (FALSE_IT(next_batch_iter_idxes_.set_allocator(allocator_))) {
  } else if (OB_FAIL(next_batch_iter_idxes_.init(query_tokens_.count()))) {
    LOG_WARN("failed to init next batch iter idxes array", K(ret));
  } else if (OB_FAIL(next_batch_iter_idxes_.prepare_allocate(query_tokens_.count()))) {
    LOG_WARN("failed to prepare allocate next batch iter idxes array", K(ret));
  } else {
    next_batch_cnt_ = query_tokens.count();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < query_tokens.count(); ++i) {
    const ObString &query_token = query_tokens.at(i);
    storage::ObTextRetrievalIterator *iter = nullptr;
    if (OB_ISNULL(iter = OB_NEWx(storage::ObTextRetrievalIterator, allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for text retrieval iterator", K(ret));
    } else if (OB_FAIL(iter->init(retrieval_param_, query_token, tx_desc, snapshot))) {
      LOG_WARN("failed to init one single text ir iterator", K(ret), K(i), K(query_token));
    } else if (OB_FAIL(token_iters_.push_back(iter))) {
      LOG_WARN("failed to append token iter to array", K(ret));
    } else {
      next_batch_iter_idxes_[i] = i;
    }

    if (OB_FAIL(ret)) {
      if (nullptr != iter) {
        iter->~ObTextRetrievalIterator();
        allocator_->free(iter);
      }
    }
  }

  if (OB_FAIL(ret)) {
    release_iters();
  }
  return ret;
}

void ObTextRetrievalMerge::release_iters()
{
  int ret = OB_SUCCESS;
  if (nullptr != allocator_) {
    for (int64_t i = 0; i < token_iters_.count(); ++i) {
      storage::ObTextRetrievalIterator *iter = token_iters_.at(i);
      if (nullptr != iter) {
        iter->reset();
        iter->~ObTextRetrievalIterator();
        allocator_->free(iter);
      }
    }
    token_iters_.reset();
    if (nullptr != whole_doc_cnt_iter_) {
      ObITabletScan *tsc_service = MTL(ObAccessService *);
      if (nullptr != tsc_service) {
        if (OB_FAIL(tsc_service->revert_scan_iter(whole_doc_cnt_iter_))) {
          LOG_WARN("failed to revert scan iter", K(ret));
        }
        whole_doc_cnt_iter_ = nullptr;
      }
    }
    whole_doc_agg_param_.need_switch_param_ = false;
    whole_doc_agg_param_.destroy();
  }
}

int ObTextRetrievalMerge::pull_next_batch_rows()
{
  int ret = OB_SUCCESS;
  ObIRIterLoserTreeItem item;
  for (int64_t i = 0; OB_SUCC(ret) && i < next_batch_cnt_; ++i) {
    const int64_t iter_idx = next_batch_iter_idxes_[i];
    storage::ObTextRetrievalIterator *iter = nullptr;
    if (OB_ISNULL(iter = token_iters_.at(iter_idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null token iter ptr", K(ret), K(iter_idx), K(token_iters_.count()));
    } else if (OB_FAIL(iter->get_next_row())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to get pull next batch rows from iterator", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (OB_FAIL(fill_loser_tree_item(*iter, iter_idx, item))) {
      LOG_WARN("fail to fill loser tree item", K(ret));
    } else if (OB_FAIL(iter_row_heap_->push(item))) {
      LOG_WARN("fail to push item to loser tree", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (iter_row_heap_->empty()) {
      ret = OB_ITER_END;
    } else if (OB_FAIL(iter_row_heap_->rebuild())) {
      LOG_WARN("fail to rebuild loser tree", K(ret), K_(next_batch_cnt));
    } else {
      next_batch_cnt_ = 0;
    }
  }
  return ret;
}

int ObTextRetrievalMerge::next_disjunctive_document()
{
  int ret = OB_SUCCESS;
  int64_t doc_cnt = 0;
  bool curr_doc_end = false;
  const ObIRIterLoserTreeItem *top_item = nullptr;
  // Do we need to use ObExpr to collect relevance?
  double cur_doc_relevance = 0.0;
  while (OB_SUCC(ret) && !iter_row_heap_->empty() && !curr_doc_end) {
    if (iter_row_heap_->is_unique_champion()) {
      curr_doc_end = true;
    }
    if (OB_FAIL(iter_row_heap_->top(top_item))) {
      LOG_WARN("failed to get top item from heap", K(ret));
    } else {
      // consider to add an expr for collectiong conjunction result between query tokens here?
      cur_doc_relevance += top_item->relevance_;
      next_batch_iter_idxes_[next_batch_cnt_++] = top_item->iter_idx_;
      if (OB_FAIL(iter_row_heap_->pop())) {
        LOG_WARN("failed to pop top item in heap", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    const double project_relevance = retrieval_param_.need_relevance() ? cur_doc_relevance : 1;
    if (OB_FAIL(project_result(*top_item, project_relevance))) {
      LOG_WARN("failed to project relevance", K(ret));
    }
  }

  return ret;
}

int ObTextRetrievalMerge::project_result(const ObIRIterLoserTreeItem &item, const double relevance)
{
  int ret = OB_SUCCESS;
  // TODO: usage of doc id column is somehow weird here, since in single token retrieval iterators,
  //       we use doc id expr to scan doc_id column for scan document. But here after DaaT processing, we use this expr
  //       to record current disjunctive documents. Though current implementation can make sure lifetime is
  //       safe, but it's tricky and indirect to read.
  // P.S we cannot allocate multiple doc id expr at cg for every query token since tokenization now is an runtime operation
  ObExpr *doc_id_col = retrieval_param_.get_ir_ctdef()->inv_scan_doc_id_col_;
  ObEvalCtx *eval_ctx = retrieval_param_.get_ir_rtdef()->eval_ctx_;
  if (OB_ISNULL(doc_id_col) || OB_ISNULL(eval_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr to relevance proejction column",
        K(ret), KP(doc_id_col), KP(eval_ctx));
  } else {
    ObDatum &doc_id_proj_datum = doc_id_col->locate_datum_for_write(*eval_ctx);
    doc_id_proj_datum.set_string(item.doc_id_.get_string());
    if (retrieval_param_.get_ir_ctdef()->need_proj_relevance_score()) {
      ObExpr *relevance_proj_col = retrieval_param_.get_ir_ctdef()->relevance_proj_col_;
      if (OB_ISNULL(relevance_proj_col)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null relevance proj col", K(ret));
      } else {
        ObDatum &relevance_proj_datum = relevance_proj_col->locate_datum_for_write(*eval_ctx);
        relevance_proj_datum.set_double(relevance);
      }
    }
    LOG_DEBUG("project one fulltext search result", K(ret), K(item));
  }
  return ret;
}

int ObTextRetrievalMerge::fill_loser_tree_item(
    storage::ObTextRetrievalIterator &iter,
    const int64_t iter_idx,
    ObIRIterLoserTreeItem &item)
{
  int ret = OB_SUCCESS;
  item.iter_idx_ = iter_idx;
  ObExpr *doc_id_expr = retrieval_param_.get_ir_ctdef()->inv_scan_doc_id_col_;
  const ObDatum &doc_id_datum = doc_id_expr->locate_expr_datum(*retrieval_param_.get_ir_rtdef()->eval_ctx_);
  if (OB_FAIL(item.doc_id_.from_string(doc_id_datum.get_string()))) {
    LOG_WARN("failed to get ObDocId from string", K(ret), K(doc_id_datum), KPC(doc_id_expr));
  } else if (retrieval_param_.need_relevance()) {
    ObExpr *relevance_expr = retrieval_param_.get_ir_ctdef()->relevance_expr_;
    const ObDatum &relevance_datum = relevance_expr->locate_expr_datum(*retrieval_param_.get_ir_rtdef()->eval_ctx_);
    item.relevance_ = relevance_datum.get_double();
  }
  return ret;
}

int ObTextRetrievalMerge::init_total_doc_cnt_param(
    transaction::ObTxDesc *tx_desc,
    transaction::ObTxReadSnapshot *snapshot)
{
  int ret = OB_SUCCESS;
  const ObDASScanCtDef *ctdef = retrieval_param_.get_doc_id_idx_agg_ctdef();
  ObDASScanRtDef *rtdef = retrieval_param_.get_ir_rtdef()->get_doc_id_idx_agg_rtdef();
  if (!retrieval_param_.need_relevance()) {
    // no need to do total doc cnt
  } else if (OB_ISNULL(ctdef) || OB_ISNULL(rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected scan descriptor", K(ret));
  } else {
    ObTableScanParam &scan_param = whole_doc_agg_param_;
    scan_param.tenant_id_ = MTL_ID();
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
    scan_param.ls_id_ = retrieval_param_.get_ls_id();
    scan_param.tablet_id_ = retrieval_param_.get_doc_id_idx_tablet_id();
    if (ctdef->pd_expr_spec_.pushdown_filters_.empty()) {
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

int ObTextRetrievalMerge::do_total_doc_cnt()
{
  int ret = OB_SUCCESS;

  if (!retrieval_param_.need_relevance()) {
    // skip
  } else if (retrieval_param_.get_ir_ctdef()->need_do_total_doc_cnt()) {
    // When estimation info not exist, or we found estimation info not accurate, calculate document count by scan
    ObITabletScan *tsc_service = MTL(ObAccessService *);
    if (OB_ISNULL(tsc_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get table access service", K(ret));
    } else if (OB_FAIL(tsc_service->table_scan(whole_doc_agg_param_, whole_doc_cnt_iter_))) {
      if (OB_SNAPSHOT_DISCARDED == ret && whole_doc_agg_param_.fb_snapshot_.is_valid()) {
        ret = OB_INVALID_QUERY_TIMESTAMP;
      } else if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("failed to do table scan for document count aggregation", K(ret));
      }
    } else {
      if (OB_UNLIKELY(!static_cast<sql::ObStoragePushdownFlag>(whole_doc_agg_param_.pd_storage_flag_).is_aggregate_pushdown())) {
        ret = OB_NOT_IMPLEMENT;
        LOG_ERROR("aggregate without pushdown not implemented", K(ret));
      } else if (OB_FAIL(whole_doc_cnt_iter_->get_next_row())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to get aggregated row from iter", K(ret));
        }
      }
    }
  } else {
    // use estimated document count for relevance estimation
    // Need to note that when total doc count is under estimated too much, the IDF component in BM25
    // would be invalidate and result to token frequence have major influence on final relevance score
    ObExpr *total_doc_cnt_expr = whole_doc_agg_param_.aggregate_exprs_->at(0);
    if (OB_ISNULL(total_doc_cnt_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null total doc cnt expr", K(ret));
    } else {
      ObDatum &total_doc_cnt = total_doc_cnt_expr->locate_datum_for_write(*retrieval_param_.get_ir_rtdef()->eval_ctx_);
      total_doc_cnt.set_int(retrieval_param_.get_ir_ctdef()->estimated_total_doc_cnt_);
      FLOG_INFO("[Salton] use estimated row count as partition document count", K(ret), K(total_doc_cnt));
    }
  }

  return ret;
}

void ObTextRetrievalMerge::clear_evaluated_infos()
{
  ObExpr *match_filter = retrieval_param_.get_ir_ctdef()->match_filter_;
  ObEvalCtx *eval_ctx = retrieval_param_.get_ir_rtdef()->eval_ctx_;
  if (nullptr != match_filter) {
    if (match_filter->is_batch_result()) {
      match_filter->get_evaluated_flags(*eval_ctx).unset(eval_ctx->get_batch_idx());
    } else {
      match_filter->get_eval_info(*eval_ctx).clear_evaluated_flag();
    }
  }
}



ObTextRetrievalOp::ObTextRetrievalOp()
  : common::ObNewRowIterator(ObNewRowIterator::IterType::ObTextRetrievalOp),
    mem_context_(),
    token_merge_(),
    limit_param_(),
    input_row_cnt_(0),
    output_row_cnt_(0),
    sort_impl_(nullptr),
    sort_row_(),
    sort_finished_(false),
    is_inited_(false)
{
}

ObTextRetrievalOp::~ObTextRetrievalOp()
{
  reset();
  ObNewRowIterator::~ObNewRowIterator();
}

int ObTextRetrievalOp::init(
    const share::ObLSID &ls_id,
    const ObTabletID &inv_idx_tablet_id,
    const ObTabletID &fwd_idx_tablet_id,
    const ObTabletID &doc_id_idx_tablet_id,
    const ObDASIRScanCtDef *ir_ctdef,
    ObDASIRScanRtDef *ir_rtdef,
    const ObDASSortCtDef *sort_ctdef,
    ObDASSortRtDef *sort_rtdef,
    transaction::ObTxDesc *tx_desc,
    transaction::ObTxReadSnapshot *snapshot)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double initialization", K(ret));
  } else {
    if (OB_ISNULL(mem_context_)) {
      lib::ContextParam param;
      param.set_mem_attr(MTL_ID(), "TextIROp", ObCtxIds::DEFAULT_CTX_ID);
      if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
        LOG_WARN("failed to create text retrieval operator memory context", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(token_merge_.init(
        ls_id,
        inv_idx_tablet_id,
        fwd_idx_tablet_id,
        doc_id_idx_tablet_id,
        ir_ctdef,
        ir_rtdef,
        tx_desc,
        snapshot,
        mem_context_->get_arena_allocator()))) {
      LOG_WARN("failed to init token merge", K(ret));
    } else if (OB_FAIL(init_limit(ir_ctdef, ir_rtdef, sort_ctdef, sort_rtdef))) {
      LOG_WARN("failed to init limit", K(ret), KPC(ir_ctdef), KPC(ir_rtdef));
    } else if (nullptr != sort_ctdef && OB_FAIL(init_sort(ir_ctdef, sort_ctdef, sort_rtdef))) {
      LOG_WARN("failed to init sort", K(ret), KPC(ir_ctdef), KPC(ir_rtdef));
    } else {
      is_inited_ = true;
    }
    LOG_DEBUG("init text retrieval op", K(ret));
  }

  return ret;
}

int ObTextRetrievalOp::rescan(
    const share::ObLSID &ls_id,
    const ObTabletID &inv_idx_tablet_id,
    const ObTabletID &fwd_idx_tablet_id,
    const ObTabletID &doc_id_idx_tablet_id,
    const ObDASIRScanCtDef *ir_ctdef,
    ObDASIRScanRtDef *ir_rtdef,
    const ObDASSortCtDef *sort_ctdef,
    ObDASSortRtDef *sort_rtdef,
    transaction::ObTxDesc *tx_desc,
    transaction::ObTxReadSnapshot *snapshot)
{
  int ret = OB_SUCCESS;
  // TODO: opt rescan
  reset();
  if (OB_FAIL(init(ls_id,
                   inv_idx_tablet_id,
                   fwd_idx_tablet_id,
                   doc_id_idx_tablet_id,
                   ir_ctdef,
                   ir_rtdef,
                   sort_ctdef,
                   sort_rtdef,
                   tx_desc,
                   snapshot))) {
    LOG_WARN("failed to re init", K(ret));
  }
  return ret;
}

void ObTextRetrievalOp::reset()
{
  token_merge_.reset();
  if (nullptr != sort_impl_) {
    sort_impl_->reset();
    sort_impl_->~ObSortOpImpl();
    sort_impl_ = nullptr;
  }
  if (nullptr != mem_context_)  {
    mem_context_->reset_remain_one_page();
    DESTROY_CONTEXT(mem_context_);
    mem_context_ = nullptr;
  }
  sort_row_.reset();
  input_row_cnt_ = 0;
  output_row_cnt_ = 0;
  limit_param_.offset_ = 0;
  limit_param_.limit_ = -1;
  sort_finished_ = false;
  is_inited_ = false;
}

int ObTextRetrievalOp::get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    if (limit_param_.limit_ > 0 && output_row_cnt_ >= limit_param_.limit_) {
      ret = OB_ITER_END;
      LOG_DEBUG("get row with limit finished",
          K(ret), K_(limit_param), K_(output_row_cnt), K_(input_row_cnt));
    }

    bool got_valid_document = false;
    while (OB_SUCC(ret) && !got_valid_document) {
      if (OB_FAIL(inner_get_next_row_for_output())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to get next row from token merge", K(ret));
        }
      } else {
        ++input_row_cnt_;
        if (input_row_cnt_ > limit_param_.offset_) {
          got_valid_document = true;
          ++output_row_cnt_;
        }
      }
    }
  }

  return ret;
}

int ObTextRetrievalOp::inner_get_next_row_for_output()
{
  int ret = OB_SUCCESS;
  if (nullptr != sort_impl_) {
    if (!sort_finished_ && OB_FAIL(do_sort())) {
      LOG_WARN("failed to do sort", K(ret));
    } else if (OB_FAIL(sort_impl_->get_next_row(sort_row_))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to get next row from sort op", K(ret));
      }
    }
  } else if (OB_FAIL(token_merge_.get_next_row())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get next row from token merge", K(ret));
    }
  }
  return ret;
}

int ObTextRetrievalOp::get_next_rows(int64_t &count, int64_t capacity)
{
  // only one row at a time
  // TODO: support batch vectorized execution later
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_next_row())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get next row", K(ret));
    }
  } else {
    count += 1;
  }
  return ret;
}

int ObTextRetrievalOp::init_sort(
    const ObDASIRScanCtDef *ir_ctdef,
    const ObDASSortCtDef *sort_ctdef,
    ObDASSortRtDef *sort_rtdef)
{
  int ret = OB_SUCCESS;
  const int64_t top_k_cnt = limit_param_.is_valid() ? (limit_param_.limit_ + limit_param_.offset_) : INT64_MAX;
  if (OB_ISNULL(sort_ctdef) || OB_ISNULL(sort_rtdef) || OB_ISNULL(ir_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null sort def", K(ret), KPC(sort_rtdef), KPC(sort_ctdef), KPC(ir_ctdef));
  } else if (OB_ISNULL(sort_impl_ = OB_NEWx(ObSortOpImpl, &mem_context_->get_arena_allocator()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate sort op", K(ret));
  } else if (OB_FAIL(sort_impl_->init(
      MTL_ID(),
      &sort_ctdef->sort_collations_,
      &sort_ctdef->sort_cmp_funcs_,
      sort_rtdef->eval_ctx_,
      &sort_rtdef->eval_ctx_->exec_ctx_,
      false, /* enable encode sort key */
      false, /* local order */
      false, /* need rewind */
      0, /* part_cnt */
      top_k_cnt,
      sort_ctdef->fetch_with_ties_))) {
    LOG_WARN("failed to init inner sort op", K(ret));
  } else if (OB_FAIL(append(sort_row_, sort_ctdef->sort_exprs_))) {
    LOG_WARN("failed to append sort exprs", K(ret));
  } else {
    for (int64_t i = 0; i < ir_ctdef->result_output_.count() && OB_SUCC(ret); ++i) {
      ObExpr *expr = ir_ctdef->result_output_.at(i);
      if (is_contain(sort_row_, expr)) {
        // skip
      } else if (OB_FAIL(sort_row_.push_back(expr))) {
        LOG_WARN("failed to append sort rows", K(ret));
      }
    }
  }
  return ret;
}

int ObTextRetrievalOp::init_limit(
    const ObDASIRScanCtDef *ir_ctdef,
    ObDASIRScanRtDef *ir_rtdef,
    const ObDASSortCtDef *sort_ctdef,
    ObDASSortRtDef *sort_rtdef)
{
  int ret = OB_SUCCESS;
  if (nullptr != sort_ctdef) {
    // try init top-k limits
    bool is_null = false;
    if (OB_UNLIKELY((nullptr != sort_ctdef->limit_expr_ || nullptr != sort_ctdef->offset_expr_)
        && ir_rtdef->get_inv_idx_scan_rtdef()->limit_param_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected top k limit with table scan limit pushdown", K(ret), KPC(ir_ctdef), KPC(ir_rtdef));
    } else if (nullptr != sort_ctdef->limit_expr_) {
      ObDatum *limit_datum = nullptr;
      if (OB_FAIL(sort_ctdef->limit_expr_->eval(*sort_rtdef->eval_ctx_, limit_datum))) {
        LOG_WARN("failed to eval limit expr", K(ret));
      } else if (limit_datum->is_null()) {
        is_null = true;
        limit_param_.limit_ = 0;
      } else {
        limit_param_.limit_ = limit_datum->get_int() < 0 ? 0 : limit_datum->get_int();
      }
    }

    if (OB_SUCC(ret) && !is_null && nullptr != sort_ctdef->offset_expr_) {
      ObDatum *offset_datum = nullptr;
      if (OB_FAIL(sort_ctdef->offset_expr_->eval(*sort_rtdef->eval_ctx_, offset_datum))) {
        LOG_WARN("failed to eval offset expr", K(ret));
      } else if (offset_datum->is_null()) {
        limit_param_.offset_ = 0;
        limit_param_.limit_ = 0;
      } else {
        limit_param_.offset_ = offset_datum->get_int() < 0 ? 0 : offset_datum->get_int();
      }
    }
  } else {
    // init with table scan pushdown limit
    limit_param_ = ir_rtdef->get_inv_idx_scan_rtdef()->limit_param_;
  }
  return ret;
}

int ObTextRetrievalOp::do_sort()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(sort_finished_) || OB_ISNULL(sort_impl_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected sort status", K(ret), K_(sort_finished), K_(token_merge), KP_(sort_impl));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(token_merge_.get_next_row())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to get next index row", K(ret));
        }
      } else if (OB_FAIL(sort_impl_->add_row(sort_row_))) {
        LOG_WARN("failed to add to to top k processor", K(ret));
      }
    }

    if (OB_LIKELY(OB_ITER_END == ret)) {
      ret = OB_SUCCESS;
      if (OB_FAIL(sort_impl_->sort())) {
        LOG_WARN("failed to do top-k sort", K(ret));
      } else {
        sort_finished_ = true;
      }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
