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
#include "ob_das_scan_iter.h"
#include "ob_das_text_retrieval_merge_iter.h"
#include "ob_das_text_retrieval_iter.h"
#include "sql/das/ob_das_ir_define.h"
#include "share/text_analysis/ob_text_analyzer.h"
#include "storage/fts/ob_fts_plugin_helper.h"

namespace oceanbase
{
namespace sql
{

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

ObDASTextRetrievalMergeIter::ObDASTextRetrievalMergeIter()
  : ObDASIter(ObDASIterType::DAS_ITER_TEXT_RETRIEVAL_MERGE),
    mem_context_(nullptr),
    relation_type_(MAX_RELATION_TYPE),
    processing_type_(MAX_PROC_TYPE),
    ir_ctdef_(nullptr),
    ir_rtdef_(nullptr),
    tx_desc_(nullptr),
    snapshot_(nullptr),
    ls_id_(),
    doc_id_idx_tablet_id_(),
    query_tokens_(),
    loser_tree_cmp_(),
    iter_row_heap_(nullptr),
    next_batch_iter_idxes_(),
    next_batch_cnt_(0),
    whole_doc_cnt_iter_(nullptr),
    whole_doc_agg_param_(),
    limit_param_(),
    input_row_cnt_(0),
    output_row_cnt_(0),
    doc_cnt_calculated_(false),
    is_inited_(false)
{
}

int ObDASTextRetrievalMergeIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double initialization", K(ret));
  } else if (OB_UNLIKELY(ObDASIterType::DAS_ITER_TEXT_RETRIEVAL_MERGE != param.type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid das iter param type for text retrieval iter", K(ret), K(param));
  } else {
    ObDASTextRetrievalMergeIterParam &retrieval_param = static_cast<ObDASTextRetrievalMergeIterParam &>(param);
    ir_ctdef_ = retrieval_param.ir_ctdef_;
    ir_rtdef_ = retrieval_param.ir_rtdef_;
    whole_doc_cnt_iter_ = static_cast<ObDASScanIter *>(retrieval_param.doc_cnt_iter_);
    tx_desc_ = retrieval_param.tx_desc_;
    snapshot_ = retrieval_param.snapshot_;

    relation_type_ = TokenRelationType::DISJUNCTIVE;
    processing_type_ = RetrievalProcType::DAAT;
    if (OB_ISNULL(mem_context_)) {
      lib::ContextParam param;
      param.set_mem_attr(MTL_ID(), "TextIRIter", ObCtxIds::DEFAULT_CTX_ID);
      if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
        LOG_WARN("failed to create text retrieval iterator memory context", K(ret));
      }
    }

    if (FAILEDx(loser_tree_cmp_.init())) {
      LOG_WARN("failed to init loser tree comparator", K(ret));
    } else if (OB_ISNULL(iter_row_heap_ = OB_NEWx(ObIRIterLoserTree, &mem_context_->get_arena_allocator(), loser_tree_cmp_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate loser tree", K(ret));
    } else if (OB_FAIL(init_query_tokens(ir_ctdef_, ir_rtdef_))) {
      LOG_WARN("failed to init query tokens", K(ret));
    } else if (0 == query_tokens_.count()) {
      // empty token set
      LOG_DEBUG("empty query token set after tokenization", K(ret), KPC_(ir_ctdef));
      is_inited_ = true;
    } else if (OB_UNLIKELY(query_tokens_.count() > OB_MAX_TEXT_RETRIEVAL_TOKEN_CNT)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("too many query tokens in a single query not supported", K(ret), K_(query_tokens));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "text retrieval query with token count exceed limit");
    } else if (OB_FAIL(iter_row_heap_->init(query_tokens_.count(), mem_context_->get_arena_allocator()))) {
      LOG_WARN("failed to init iter loser tree", K(ret));
    } else {
      limit_param_ = ir_rtdef_->get_inv_idx_scan_rtdef()->limit_param_;
      is_inited_ = true;
    }
    LOG_DEBUG("init text retrieval op", K(ret), KPC_(ir_ctdef), KPC_(ir_rtdef));

  }
  return ret;
}

int ObDASTextRetrievalMergeIter::rescan()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(query_tokens_.count() != token_iters_.count())) {
    ret= OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected iter count mismatch with query tokens",
        K(ret), K_(query_tokens), K_(token_iters));
  } else if (0 == query_tokens_.count()) {
  } else if (nullptr != whole_doc_cnt_iter_ && OB_FAIL(whole_doc_cnt_iter_->rescan())) {
    LOG_WARN("failed to rescan doc count iter", K(ret));
  } else if (OB_FAIL(next_batch_iter_idxes_.init(query_tokens_.count()))) {
    LOG_WARN("failed to init next batch iter idxes array", K(ret));
  } else if (OB_FAIL(next_batch_iter_idxes_.prepare_allocate(query_tokens_.count()))) {
    LOG_WARN("failed to prepare allocate next batch iter idxes array", K(ret));
  } else if (OB_FAIL(iter_row_heap_->open(query_tokens_.count()))) {
    LOG_WARN("failed to open iter row heap", K(ret), K_(query_tokens));
  } else {
    limit_param_ = ir_rtdef_->get_inv_idx_scan_rtdef()->limit_param_;
    next_batch_cnt_ = token_iters_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < token_iters_.count(); ++i) {
      ObDASTextRetrievalIter *iter = token_iters_.at(i);
      if (OB_FAIL(token_iters_.at(i)->set_query_token(query_tokens_.at(i)))) {
        LOG_WARN("failed to set query token before rescan", K(ret), K(query_tokens_.at(i)));
      } else if (OB_FAIL(iter->rescan())) {
        LOG_WARN("failed to append token iter to array", K(ret));
      } else {
        next_batch_iter_idxes_[i] = i;
      }
    }
  }
  return ret;
}

int ObDASTextRetrievalMergeIter::do_table_scan()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < token_iters_.count(); ++i) {
    if (OB_FAIL(token_iters_.at(i)->do_table_scan())) {
      LOG_WARN("failed to do table scan for token iter", K(ret));
    }
  }
  return ret;
}

int ObDASTextRetrievalMergeIter::set_related_tablet_ids(
    const ObLSID &ls_id,
    const ObDASRelatedTabletID &related_tablet_ids)
{
  int ret = OB_SUCCESS;
  ls_id_ = ls_id;
  doc_id_idx_tablet_id_ = related_tablet_ids.doc_id_idx_tablet_id_;
  for (int64_t i = 0; i < token_iters_.count(); ++i) {
    token_iters_.at(i)->set_ls_tablet_ids(
        ls_id,
        related_tablet_ids.inv_idx_tablet_id_,
        related_tablet_ids.fwd_idx_tablet_id_);
  }
  return ret;
}

int ObDASTextRetrievalMergeIter::inner_reuse()
{
  int ret = OB_SUCCESS;
  next_batch_iter_idxes_.reuse();
  iter_row_heap_->reuse();
  next_batch_cnt_ = 0;
  doc_cnt_calculated_ = false;
  input_row_cnt_ = 0;
  output_row_cnt_ = 0;
  const ObTabletID &old_doc_id_tablet_id = whole_doc_agg_param_.tablet_id_;
  whole_doc_agg_param_.need_switch_param_ = whole_doc_agg_param_.need_switch_param_ ||
    ((old_doc_id_tablet_id.is_valid() && old_doc_id_tablet_id != doc_id_idx_tablet_id_) ? true : false);
  if (nullptr != whole_doc_cnt_iter_) {
    whole_doc_cnt_iter_->set_scan_param(whole_doc_agg_param_);
    if (OB_FAIL(whole_doc_cnt_iter_->reuse())) {
      LOG_WARN("failed to reuse whole doc cnt iter", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < token_iters_.count(); ++i) {
    if (OB_FAIL(token_iters_.at(i)->reuse())) {
      LOG_WARN("failed to reuse token iters", K(ret));
    }
  }
  return ret;
}

int ObDASTextRetrievalMergeIter::inner_release()
{
  int ret = OB_SUCCESS;
  query_tokens_.reset();
  if (nullptr != iter_row_heap_) {
    iter_row_heap_->~ObIRIterLoserTree();
    iter_row_heap_ = nullptr;
  }
  whole_doc_agg_param_.destroy_schema_guard();
  whole_doc_agg_param_.snapshot_.reset();
  whole_doc_agg_param_.destroy();
  whole_doc_cnt_iter_ = nullptr;
  token_iters_.reset();
  next_batch_iter_idxes_.reset();
  if (nullptr != mem_context_)  {
    mem_context_->reset_remain_one_page();
    DESTROY_CONTEXT(mem_context_);
    mem_context_ = nullptr;
  }
  next_batch_cnt_ = 0;
  input_row_cnt_ = 0;
  output_row_cnt_ = 0;
  limit_param_.offset_ = 0;
  limit_param_.limit_ = -1;
  doc_cnt_calculated_ = false;
  is_inited_ = false;
  return ret;
}

int ObDASTextRetrievalMergeIter::inner_get_next_row()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (0 == query_tokens_.count()) {
    ret = OB_ITER_END;
  } else if (limit_param_.limit_ > 0 && output_row_cnt_ >= limit_param_.limit_) {
    ret = OB_ITER_END;
    LOG_DEBUG("get row with limit finished",
        K(ret), K_(limit_param), K_(output_row_cnt), K_(input_row_cnt));
  } else if (!doc_cnt_calculated_) {
    if (OB_FAIL(do_total_doc_cnt())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to do total document count", K(ret), KPC_(ir_ctdef));
      }
    } else {
      doc_cnt_calculated_ = true;
    }
  }

  bool filter_valid = false;
  bool got_valid_document = false;
  ObExpr *match_filter = ir_ctdef_->need_calc_relevance() ? ir_ctdef_->match_filter_ : nullptr;
  ObDatum *filter_res = nullptr;
  while (OB_SUCC(ret) && !got_valid_document) {
    clear_evaluated_infos();
    filter_valid = false;
    if (OB_FAIL(pull_next_batch_rows())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to pull next batch rows from iterator", K(ret));
      }
    } else if (OB_FAIL(next_disjunctive_document())) {
      LOG_WARN("failed to get next document with disjunctive tokens", K(ret));
    } else if (OB_ISNULL(match_filter)) {
      filter_valid = true;
    } else if (OB_FAIL(match_filter->eval(*ir_rtdef_->eval_ctx_, filter_res))) {
      LOG_WARN("failed to evaluate match filter", K(ret));
    } else {
      filter_valid = !(filter_res->is_null() || 0 == filter_res->get_int());
    }

    if (OB_SUCC(ret)) {
      if (filter_valid) {
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

int ObDASTextRetrievalMergeIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  // only one row at a time
  // TODO: support batch vectorized execution later
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_get_next_row())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get next row", K(ret));
    }
  } else {
    count += 1;
  }
  return ret;
}

int ObDASTextRetrievalMergeIter::init_query_tokens(const ObDASIRScanCtDef *ir_ctdef, ObDASIRScanRtDef *ir_rtdef)
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
    if (OB_FAIL(tokenize_helper.init(&mem_context_->get_arena_allocator(), parser_name))) {
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
        if (OB_FAIL(ob_write_string(mem_context_->get_arena_allocator(), token.get_word(), token_string))) {
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

int ObDASTextRetrievalMergeIter::set_merge_iters(const ObIArray<ObDASIter *> &retrieval_iters)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(retrieval_iters.count() != query_tokens_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid retrieval iter count mismatch with query tokens", K(ret), K_(query_tokens), K(retrieval_iters));
  } else if (token_iters_.count() != 0) {
    ret = OB_INIT_TWICE;
    LOG_WARN("set retrieval iters to a non-empty merge iter", K(ret));
  } else if (0 == query_tokens_.count()) {
    // no valid tokens
  } else if (FALSE_IT(next_batch_iter_idxes_.set_allocator(&mem_context_->get_arena_allocator()))) {
  } else if (OB_FAIL(next_batch_iter_idxes_.init(query_tokens_.count()))) {
    LOG_WARN("failed to init next batch iter idxes array", K(ret));
  } else if (OB_FAIL(next_batch_iter_idxes_.prepare_allocate(query_tokens_.count()))) {
    LOG_WARN("failed to prepare allocate next batch iter idxes array", K(ret));
  } else {
    next_batch_cnt_ = query_tokens_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < query_tokens_.count(); ++i) {
      const ObString &query_token = query_tokens_.at(i);
      ObDASTextRetrievalIter *iter = static_cast<ObDASTextRetrievalIter *>(retrieval_iters.at(i));
      if (OB_FAIL(token_iters_.push_back(iter))) {
        LOG_WARN("failed to append token iter to array", K(ret));
      } else {
        next_batch_iter_idxes_[i] = i;
      }
    }
  }

  return ret;
}

int ObDASTextRetrievalMergeIter::pull_next_batch_rows()
{
  int ret = OB_SUCCESS;
  ObIRIterLoserTreeItem item;
  for (int64_t i = 0; OB_SUCC(ret) && i < next_batch_cnt_; ++i) {
    const int64_t iter_idx = next_batch_iter_idxes_[i];
    ObDASTextRetrievalIter *iter = nullptr;
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

int ObDASTextRetrievalMergeIter::next_disjunctive_document()
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
    const double project_relevance = ir_ctdef_->need_calc_relevance() ? cur_doc_relevance : 1;
    if (OB_FAIL(project_result(*top_item, project_relevance))) {
      LOG_WARN("failed to project relevance", K(ret));
    }
  }

  return ret;
}

int ObDASTextRetrievalMergeIter::project_result(const ObIRIterLoserTreeItem &item, const double relevance)
{
  int ret = OB_SUCCESS;
  // TODO: usage of doc id column is somehow weird here, since in single token retrieval iterators,
  //       we use doc id expr to scan doc_id column for scan document. But here after DaaT processing, we use this expr
  //       to record current disjunctive documents. Though current implementation can make sure lifetime is
  //       safe, but it's tricky and indirect to read.
  // P.S we cannot allocate multiple doc id expr at cg for every query token since tokenization now is an runtime operation
  ObExpr *doc_id_col = ir_ctdef_->inv_scan_doc_id_col_;
  ObEvalCtx *eval_ctx = ir_rtdef_->eval_ctx_;
  if (OB_ISNULL(doc_id_col) || OB_ISNULL(eval_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr to relevance proejction column",
        K(ret), KP(doc_id_col), KP(eval_ctx));
  } else {
    ObDatum &doc_id_proj_datum = doc_id_col->locate_datum_for_write(*eval_ctx);
    doc_id_proj_datum.set_string(item.doc_id_.get_string());
    if (ir_ctdef_->need_proj_relevance_score()) {
      ObExpr *relevance_proj_col = ir_ctdef_->relevance_proj_col_;
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

int ObDASTextRetrievalMergeIter::fill_loser_tree_item(
    ObDASTextRetrievalIter &iter,
    const int64_t iter_idx,
    ObIRIterLoserTreeItem &item)
{
  int ret = OB_SUCCESS;
  item.iter_idx_ = iter_idx;
  ObExpr *doc_id_expr = ir_ctdef_->inv_scan_doc_id_col_;
  const ObDatum &doc_id_datum = doc_id_expr->locate_expr_datum(*ir_rtdef_->eval_ctx_);
  if (OB_FAIL(item.doc_id_.from_string(doc_id_datum.get_string()))) {
    LOG_WARN("failed to get ObDocId from string", K(ret), K(doc_id_datum), KPC(doc_id_expr));
  } else if (ir_ctdef_->need_calc_relevance()) {
    ObExpr *relevance_expr = ir_ctdef_->relevance_expr_;
    const ObDatum &relevance_datum = relevance_expr->locate_expr_datum(*ir_rtdef_->eval_ctx_);
    item.relevance_ = relevance_datum.get_double();
  }
  return ret;
}

int ObDASTextRetrievalMergeIter::init_total_doc_cnt_param(
    transaction::ObTxDesc *tx_desc,
    transaction::ObTxReadSnapshot *snapshot)
{
  int ret = OB_SUCCESS;
  const ObDASScanCtDef *ctdef = ir_ctdef_->get_doc_id_idx_agg_ctdef();
  ObDASScanRtDef *rtdef = ir_rtdef_->get_doc_id_idx_agg_rtdef();
  if (!ir_ctdef_->need_calc_relevance()) {
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
    scan_param.ls_id_ = ls_id_;
    scan_param.tablet_id_ = doc_id_idx_tablet_id_;
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

int ObDASTextRetrievalMergeIter::do_total_doc_cnt()
{
  int ret = OB_SUCCESS;

  if (!ir_ctdef_->need_calc_relevance()) {
    // skip
  } else if (ir_ctdef_->need_do_total_doc_cnt()) {
    // When estimation info not exist, or we found estimation info not accurate, calculate document count by scan
    if (OB_FAIL(init_total_doc_cnt_param(tx_desc_, snapshot_))) {
      LOG_WARN("failed to do total doc cnt", K(ret));
    } else if (FALSE_IT(whole_doc_cnt_iter_->set_scan_param(whole_doc_agg_param_))) {
    } else if (OB_FAIL(whole_doc_cnt_iter_->do_table_scan())) {
      LOG_WARN("failed to do table scan for document count aggregation", K(ret));
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
    ObExpr *total_doc_cnt_expr = ir_ctdef_->get_doc_id_idx_agg_ctdef()->pd_expr_spec_.pd_storage_aggregate_output_.at(0);
    if (OB_ISNULL(total_doc_cnt_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null total doc cnt expr", K(ret));
    } else {
      ObDatum &total_doc_cnt = total_doc_cnt_expr->locate_datum_for_write(*ir_rtdef_->eval_ctx_);
      total_doc_cnt.set_int(ir_ctdef_->estimated_total_doc_cnt_);
      LOG_TRACE("use estimated row count as partition document count", K(ret), K(total_doc_cnt));
    }
  }

  return ret;
}

void ObDASTextRetrievalMergeIter::clear_evaluated_infos()
{
  ObExpr *match_filter = ir_ctdef_->match_filter_;
  ObEvalCtx *eval_ctx = ir_rtdef_->eval_ctx_;
  if (nullptr != match_filter) {
    if (match_filter->is_batch_result()) {
      match_filter->get_evaluated_flags(*eval_ctx).unset(eval_ctx->get_batch_idx());
    } else {
      match_filter->get_eval_info(*eval_ctx).clear_evaluated_flag();
    }
  }
}

} // namespace sql
} // namespace oceanbase
