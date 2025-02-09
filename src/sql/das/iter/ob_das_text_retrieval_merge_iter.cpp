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
#include "ob_das_text_retrieval_eval_node.h"

namespace oceanbase
{
using namespace share;
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
    cache_doc_ids_(),
    hints_(),
    cache_relevances_(),
    reverse_hints_(),
    boolean_relevances_(),
    root_node_(nullptr),
    rangekey_size_(0),
    next_written_idx_(0),
    whole_doc_cnt_iter_(nullptr),
    whole_doc_agg_param_(),
    limit_param_(),
    input_row_cnt_(0),
    output_row_cnt_(0),
    force_return_docid_(false),
    doc_cnt_calculated_(false),
    doc_cnt_iter_acquired_(false),
    is_inited_(false)
{
}

int ObDASTextRetrievalMergeIter::do_table_scan()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < token_iters_.count(); ++i) {
    if (OB_FAIL(token_iters_.at(i)->do_table_scan())) {
      LOG_WARN("failed to do table scan for token iter", K(ret));
    }
  }
  return ret;
}

int ObDASTextRetrievalMergeIter::rescan()
{
  int ret = OB_SUCCESS;
  if (0 == query_tokens_.count()) {
  } else if (nullptr != whole_doc_cnt_iter_ &&
            (!force_return_docid_ || whole_doc_agg_param_.need_switch_param_) &&
            OB_FAIL(whole_doc_cnt_iter_->rescan())) { // for force_return_docid_ mdoe, we just read the cnt once.
    LOG_WARN("failed to rescan doc count iter", K(ret));
  } else {
    next_written_idx_ = 0;
    limit_param_ = ir_rtdef_->get_inv_idx_scan_rtdef()->limit_param_;
  }
  return ret;
}

int ObDASTextRetrievalMergeIter::set_related_tablet_ids(
    const ObLSID &ls_id,
    const ObDASFTSTabletID &related_tablet_ids)
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

int ObDASTextRetrievalMergeIter::set_merge_iters(const ObIArray<ObDASIter *> &retrieval_iters)
{
  int ret = OB_ERR_UNEXPECTED;
  LOG_WARN("not implemented", K(ret));
  return ret;
}

int ObDASTextRetrievalMergeIter::build_query_tokens(const ObDASIRScanCtDef *ir_ctdef,
                                                   ObDASIRScanRtDef *ir_rtdef,
                                                   common::ObIAllocator &alloc,
                                                   ObArray<ObString> &query_tokens,
                                                   ObFtsEvalNode *&root_node)
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
  } else if (BOOLEAN_MODE == ir_ctdef->mode_flag_) {
    const ObString &search_text_string = search_text_datum->get_string();
    const ObCollationType &cs_type = search_text->datum_meta_.cs_type_;
    ObString str_dest;
    ObCharset::tolower(cs_type, search_text_string, str_dest, alloc);

    void *buf = nullptr;
    FtsParserResult *fts_parser;
    if (OB_ISNULL(buf = (&alloc)->alloc(sizeof(FtsParserResult)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate enough memory", K(sizeof(FtsParserResult)), K(ret));
    } else {
      fts_parser = static_cast<FtsParserResult *>(buf);
    }

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(buf = (&alloc)->alloc(str_dest.length() + 1))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate enough memory", K(sizeof(FtsParserResult)), K(ret));
    } else {
      MEMSET(buf, 0, str_dest.length() + 1);
      MEMCPY(buf, str_dest.ptr(), str_dest.length());
    }

    if (OB_FAIL(ret)) {
    } else if (FALSE_IT(fts_parse_docment(static_cast<char *>(buf), &alloc, fts_parser))) {
    } else if (FTS_OK != fts_parser->ret_) {
      if (FTS_ERROR_MEMORY == fts_parser->ret_) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (FTS_ERROR_SYNTAX == fts_parser->ret_) {
        ret = OB_ERR_PARSER_SYNTAX;
      } else if (FTS_ERROR_OTHER == fts_parser->ret_) {
        ret = OB_ERR_UNEXPECTED;
      }
      LOG_WARN("failed to parse query text", K(ret), K(fts_parser->err_info_.str_));
    } else if (OB_ISNULL(fts_parser->root_)) {
      // do nothing
    } else {
      FtsNode *node = fts_parser->root_;
      ObFtsEvalNode *parant_node =nullptr;
      hash::ObHashMap<ObString, int32_t> tokens_map;
      const int64_t ft_word_bkt_cnt = MAX(search_text_string.length() / 10, 2);
      if (OB_FAIL(tokens_map.create(ft_word_bkt_cnt, common::ObMemAttr(MTL_ID(), "FTWordMap")))) {
        LOG_WARN("failed to create token map", K(ret));
      } else if (OB_FAIL(ObFtsEvalNode::fts_boolean_node_create(parant_node, node, alloc, query_tokens, tokens_map))) {
        LOG_WARN("failed to get query tokens", K(ret));
      } else {
        root_node = parant_node;
      }
    }
  } else {
    // TODO: FTParseHelper currently does not support deduplicate tokens
    //       We should abstract such universal analyse functors into utility structs
    const ObString &search_text_string = search_text_datum->get_string();
    const ObString &parser_name = ir_ctdef->get_inv_idx_scan_ctdef()->table_param_.get_parser_name();
    const ObString &parser_properties = ir_ctdef->get_inv_idx_scan_ctdef()->table_param_.get_parser_property();
    const ObCollationType &cs_type = search_text->datum_meta_.cs_type_;
    int64_t doc_length = 0;
    storage::ObFTParseHelper tokenize_helper;
    common::ObSEArray<ObFTWord, 16> tokens;
    hash::ObHashMap<ObFTWord, int64_t> token_map;
    const int64_t ft_word_bkt_cnt = MAX(search_text_string.length() / 10, 2);
    if (OB_FAIL(tokenize_helper.init(&alloc, parser_name, parser_properties))) {
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
        if (OB_FAIL(ob_write_string(alloc, token.get_word(), token_string))) {
          LOG_WARN("failed to deep copy query token", K(ret));
        } else if (OB_FAIL(query_tokens.push_back(token_string))) {
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
    LOG_DEBUG("tokenized text query:", K(ret), KPC(search_text_datum), K(query_tokens));
  }
  return ret;
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

    relation_type_ = ir_ctdef_->mode_flag_ == BOOLEAN_MODE ? BOOLEAN : DISJUNCTIVE;
    force_return_docid_ = retrieval_param.force_return_docid_; // from param

    if (OB_ISNULL(mem_context_)) {
      lib::ContextParam param;
      param.set_mem_attr(MTL_ID(), "TextIRIter", ObCtxIds::DEFAULT_CTX_ID);
      if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
        LOG_WARN("failed to create text retrieval iterator memory context", K(ret));
      }
    }

    if (FAILEDx(init_query_tokens(retrieval_param.query_tokens_))) {
      LOG_WARN("failed to init query tokens", K(ret));
    } else if (0 == query_tokens_.count()) {
      // empty token set
      LOG_DEBUG("empty query token set after tokenization", K(ret), KPC_(ir_ctdef));
    } else {
      int64_t size = ir_ctdef_->inv_scan_doc_id_col_->is_batch_result() ? retrieval_param.max_size_ : 1;
      if (FALSE_IT(cache_doc_ids_.set_allocator(&mem_context_->get_arena_allocator()))) {
      } else if (OB_FAIL(cache_doc_ids_.init(size))) {
        LOG_WARN("failed to init next batch iter idxes array", K(ret));
      } else if (OB_FAIL(cache_doc_ids_.prepare_allocate(size))) {
        LOG_WARN("failed to prepare allocate next batch iter idxes array", K(ret));
      } else {
        limit_param_ = ir_rtdef_->get_inv_idx_scan_rtdef()->limit_param_;
      }
      if (OB_FAIL(ret)) {
      } else if (force_return_docid_) {
        if (FALSE_IT(hints_.set_allocator(&mem_context_->get_arena_allocator()))) {
        } else if (FALSE_IT(cache_relevances_.set_allocator(&mem_context_->get_arena_allocator()))) {
        } else if (FALSE_IT(reverse_hints_.set_allocator(&mem_context_->get_arena_allocator()))) {
        } else if (OB_FAIL(hints_.init(size))) {
          LOG_WARN("failed to init hints array", K(ret));
        } else if (OB_FAIL(hints_.prepare_allocate(size))) {
          LOG_WARN("failed to prepare allocate hints array", K(ret));
        } else if (OB_FAIL(cache_relevances_.init(size))) {
          LOG_WARN("failed to init relevances array", K(ret));
        } else if (OB_FAIL(cache_relevances_.prepare_allocate(size))) {
          LOG_WARN("failed to prepare allocate relevances array", K(ret));
        } else if (OB_FAIL(reverse_hints_.init(size))) {
          LOG_WARN("failed to init hints array", K(ret));
        } else if (OB_FAIL(reverse_hints_.prepare_allocate(size))) {
          LOG_WARN("failed to prepare allocate hints array", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (BOOLEAN == relation_type_) {
        if (FALSE_IT(boolean_relevances_.set_allocator(&mem_context_->get_arena_allocator()))) {
        } else if (OB_FAIL(boolean_relevances_.init(query_tokens_.count()))) {
          LOG_WARN("failed to init relevances array", K(ret));
        } else if (OB_FAIL(boolean_relevances_.prepare_allocate(query_tokens_.count()))) {
          LOG_WARN("failed to prepare allocate relevances array", K(ret));
        }
      }
    }
    LOG_DEBUG("init text retrieval op", K(ret), KPC_(ir_ctdef), KPC_(ir_rtdef));

  }
  return ret;
}

int ObDASTextRetrievalMergeIter::inner_reuse()
{
  int ret = OB_SUCCESS;
  int64_t size = ir_ctdef_->inv_scan_doc_id_col_->is_batch_result() ? ir_rtdef_->eval_ctx_->max_batch_size_ : 1;
  if (0 == token_iters_.count()) {
    // do nothing
  } else if (size <= cache_doc_ids_.count()) {
    // do nothing
  } else {
    cache_doc_ids_.reuse();
    hints_.reuse();
    cache_relevances_.reuse();
    reverse_hints_.reuse();
    if (OB_FAIL(cache_doc_ids_.init(size))) {
      LOG_WARN("failed to init cache_doc_ids_ array", K(ret));
    } else if (OB_FAIL(cache_doc_ids_.prepare_allocate(size))) {
      LOG_WARN("failed to prepare allocate cache_doc_ids_ array", K(ret));
    } else if (force_return_docid_) {
      if (OB_FAIL(hints_.init(size))) {
        LOG_WARN("failed to init hints array", K(ret));
      } else if (OB_FAIL(hints_.prepare_allocate(size))) {
        LOG_WARN("failed to prepare allocate hints array", K(ret));
      } else if (OB_FAIL(cache_relevances_.init(size))) {
        LOG_WARN("failed to init relevances array", K(ret));
      } else if (OB_FAIL(cache_relevances_.prepare_allocate(size))) {
        LOG_WARN("failed to prepare allocate relevances array", K(ret));
      } else if (OB_FAIL(reverse_hints_.init(size))) {
        LOG_WARN("failed to init relevances array", K(ret));
      } else if (OB_FAIL(reverse_hints_.prepare_allocate(size))) {
        LOG_WARN("failed to prepare allocate relevances array", K(ret));
      }
    }
  }
  next_written_idx_ = 0;
  if (!force_return_docid_) {
    doc_cnt_calculated_ = false;
  }
  input_row_cnt_ = 0;
  output_row_cnt_ = 0;
  const ObTabletID &old_doc_id_tablet_id = whole_doc_agg_param_.tablet_id_;
  whole_doc_agg_param_.need_switch_param_ = whole_doc_agg_param_.need_switch_param_ ||
    ((old_doc_id_tablet_id.is_valid() && old_doc_id_tablet_id != doc_id_idx_tablet_id_) ? true : false);
  if (!force_return_docid_ || whole_doc_agg_param_.need_switch_param_) {
    if (nullptr != whole_doc_cnt_iter_) {
      whole_doc_cnt_iter_->set_scan_param(whole_doc_agg_param_);
      if (OB_FAIL(whole_doc_cnt_iter_->reuse())) {
        LOG_WARN("failed to reuse whole doc cnt iter", K(ret));
      }
    }
    doc_cnt_calculated_ = false;
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
  whole_doc_agg_param_.destroy_schema_guard();
  whole_doc_agg_param_.snapshot_.reset();
  whole_doc_agg_param_.destroy();
  whole_doc_cnt_iter_ = nullptr;
  token_iters_.reset();
  cache_doc_ids_.reset();
  hints_.reset();
  cache_relevances_.reset();
  reverse_hints_.reset();
  if (BOOLEAN == relation_type_) {
    boolean_relevances_.reset();
    if (OB_NOT_NULL(root_node_)) {
      root_node_->release();
      root_node_ = nullptr;
    }
  }
  if (nullptr != mem_context_)  {
    mem_context_->reset_remain_one_page();
    DESTROY_CONTEXT(mem_context_);
    mem_context_ = nullptr;
  }
  next_written_idx_ = 0;
  input_row_cnt_ = 0;
  output_row_cnt_ = 0;
  limit_param_.offset_ = 0;
  limit_param_.limit_ = -1;
  force_return_docid_ = false;
  doc_cnt_calculated_ = false;
  doc_cnt_iter_acquired_ = false;
  is_inited_ = false;
  return ret;
}

int ObDASTextRetrievalMergeIter::inner_get_next_row()
{
  int ret = OB_NOT_SUPPORTED;
  LOG_WARN("not unsupported", K(ret));
  return ret;
}

int ObDASTextRetrievalMergeIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_NOT_SUPPORTED;
  LOG_WARN("not unsupported", K(ret));
  return ret;
}

int ObDASTextRetrievalMergeIter::set_rangkey_and_selector(const common::ObIArray<std::pair<ObDocId, int>> &virtual_rangkeys)
{
  int ret = OB_SUCCESS;
  rangekey_size_ = virtual_rangkeys.count();
  if (OB_UNLIKELY(!force_return_docid_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected mode", K(ret));
  } else if (rangekey_size_ > OB_MAX(ir_rtdef_->eval_ctx_->max_batch_size_, 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected rangekey size", K(ret), K_(rangekey_size));
  } else if (0 != token_iters_.count()) {
    int64_t max_size = ir_ctdef_->inv_scan_doc_id_col_->is_batch_result() ? ir_rtdef_->eval_ctx_->max_batch_size_ : 1;
    if (rangekey_size_ > cache_doc_ids_.count() || rangekey_size_ > max_size) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected size", K(ret), K(rangekey_size_), K(cache_doc_ids_.count()));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < virtual_rangkeys.count(); ++i) {
      cache_doc_ids_[i].from_string(virtual_rangkeys.at(i).first.get_string());
      hints_[i] = virtual_rangkeys.at(i).second;
      cache_relevances_[i] = 0.0;
      if (virtual_rangkeys.at(i).second >= max_size) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected size", K(ret), K(virtual_rangkeys.at(i).second), K(max_size));
      } else {
        reverse_hints_[virtual_rangkeys.at(i).second] = i;
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < token_iters_.count(); ++i) {
      if (OB_FAIL(token_iters_.at(i)->set_query_token_and_rangekey(query_tokens_.at(i), cache_doc_ids_, rangekey_size_))) {
        LOG_WARN("failed to set token and rangekey", K(ret), K_(rangekey_size));
      }
    }
  }
  return ret;
}

int ObDASTextRetrievalMergeIter::check_and_prepare()
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
    if (!ir_ctdef_->need_calc_relevance()) {
      doc_cnt_calculated_ = true;
    } else if (OB_FAIL(do_total_doc_cnt())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to do total document count", K(ret), KPC_(ir_ctdef));
      } else {
        doc_cnt_calculated_ = true;
      }
    } else {
      doc_cnt_calculated_ = true;
    }
  }
  return ret;
}

int ObDASTextRetrievalMergeIter::project_result(const ObDocId &docid, const double relevance)
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
    doc_id_proj_datum.set_string(docid.get_string());
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
    LOG_DEBUG("project one fulltext search result", K(ret), K(docid), K(relevance));
  }
  return ret;
}

int ObDASTextRetrievalMergeIter::project_relevance(const ObDocId &docid, const double relevance)
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
    cache_doc_ids_[next_written_idx_] = docid;
    if (ir_ctdef_->need_proj_relevance_score()) {
      ObExpr *relevance_proj_col = ir_ctdef_->relevance_proj_col_;
      if (OB_ISNULL(relevance_proj_col)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null relevance proj col", K(ret));
      } else {
        ObEvalCtx::BatchInfoScopeGuard guard(*eval_ctx);
        guard.set_batch_idx(next_written_idx_);
        ObDatum &relevance_proj_datum = relevance_proj_col->locate_datum_for_write(*eval_ctx);
        relevance_proj_datum.set_double(relevance);
        relevance_proj_col->get_evaluated_flags(*eval_ctx).set(next_written_idx_);
        relevance_proj_col->set_evaluated_projected(*eval_ctx);
      }
    }
  }
  return ret;
}

int ObDASTextRetrievalMergeIter::project_docid()
{
  int ret = OB_SUCCESS;
  ObExpr *doc_id_col = ir_ctdef_->inv_scan_doc_id_col_;
  ObEvalCtx *eval_ctx = ir_rtdef_->eval_ctx_;
  ObDatum *doc_id_proj_datum = doc_id_col->locate_batch_datums(*eval_ctx);
  for (int64_t i = 0; OB_SUCC(ret) && i < next_written_idx_; ++i) {
    doc_id_proj_datum[i].set_string(cache_doc_ids_[i].get_string());
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

int ObDASTextRetrievalMergeIter::init_query_tokens(const ObArray<ObString> &query_tokens)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < query_tokens.count(); ++i) {
    ObString token_string;
    if (OB_FAIL(ob_write_string(mem_context_->get_arena_allocator(), query_tokens[i], token_string))) {
      LOG_WARN("failed to deep copy query token", K(ret));
    } else if (OB_FAIL(query_tokens_.push_back(token_string))) {
      LOG_WARN("failed to append query token", K(ret));
    }
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
      if (OB_FAIL(scan_param.snapshot_.assign(*snapshot))) {
        LOG_WARN("assign snapshot fail", K(ret));
      }
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
  ObEvalCtx::BatchInfoScopeGuard guard(*ir_rtdef_->eval_ctx_);
  guard.set_batch_idx(0);
  if (!ir_ctdef_->need_estimate_total_doc_cnt()) {
    bool get_next = false;
    // When estimation info not exist, or we found estimation info not accurate, calculate document count by scan
    if (!doc_cnt_iter_acquired_) {
      if (OB_FAIL(init_total_doc_cnt_param(tx_desc_, snapshot_))) {
        LOG_WARN("failed to do total doc cnt", K(ret));
      } else if (FALSE_IT(whole_doc_cnt_iter_->set_scan_param(whole_doc_agg_param_))) {
      } else if (OB_FAIL(whole_doc_cnt_iter_->do_table_scan())) {
        LOG_WARN("failed to do table scan for document count aggregation", K(ret));
      } else {
        doc_cnt_iter_acquired_ = true;
        get_next = true;
      }
    } else {
      const ObTabletID old_tablet_id = whole_doc_agg_param_.tablet_id_;
      whole_doc_agg_param_.need_switch_param_ = whole_doc_agg_param_.need_switch_param_
          || ((old_tablet_id.is_valid() && old_tablet_id != doc_id_idx_tablet_id_ ) ? true : false);
      whole_doc_agg_param_.tablet_id_ = doc_id_idx_tablet_id_;
      whole_doc_agg_param_.ls_id_ = ls_id_;
      if (!force_return_docid_ || whole_doc_agg_param_.need_switch_param_) {
        if (OB_FAIL(whole_doc_cnt_iter_->reuse())) {
          LOG_WARN("failed to reuse whole doc cnt iter", K(ret));
        } else if (OB_FAIL(whole_doc_cnt_iter_->rescan())) {
          LOG_WARN("failed to rescan whole doc cnt iter", K(ret));
        } else {
          get_next = true;
        }
      }
    }
    if (OB_SUCC(ret) && get_next) {
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

ObDASTRTaatIter::ObDASTRTaatIter()
  : ObDASTextRetrievalMergeIter(),
    hash_maps_(),
    datum_stores_(),
    datum_store_iters_(),
    hash_map_size_(0),
    cur_map_iter_(nullptr),
    total_doc_cnt_(-1),
    next_clear_map_idx_(0),
    cur_map_idx_(-1),
    cache_first_docid_(),
    is_chunk_store_inited_(false),
    is_hashmap_inited_(false)
{
}

int ObDASTRTaatIter::rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDASTextRetrievalMergeIter::rescan())) {
    LOG_WARN("failed to rescan iter", K(ret));
  } else if (OB_UNLIKELY(token_iters_.count() > 1)) {
    ret= OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected iter count mismatch with query tokens",
        K(ret), K_(query_tokens), K_(token_iters));
  } else if (0 != query_tokens_.count()) {
    ObDASTextRetrievalIter *iter = token_iters_.at(0);
    if (OB_FAIL(iter->set_query_token(query_tokens_.at(0)))) {
      LOG_WARN("failed to set query token before rescan", K(ret), K(query_tokens_.at(0)));
    } else if (OB_FAIL(iter->rescan())) {
      LOG_WARN("failed to append token iter to array", K(ret));
    }
    is_hashmap_inited_ = false;
    cur_map_idx_= -1;
    next_clear_map_idx_ = 0;
  }
  return ret;
}

int ObDASTRTaatIter::set_merge_iters(const ObIArray<ObDASIter *> &retrieval_iters)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (0 == query_tokens_.count()) {
    // no valid tokens
  } else if (OB_UNLIKELY(retrieval_iters.count() != 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid retrieval iter count mismatch with query tokens", K(ret), K_(query_tokens), K(retrieval_iters));
  } else if (token_iters_.count() != 0) {
    ret = OB_INIT_TWICE;
    LOG_WARN("set retrieval iters to a non-empty merge iter", K(ret));
  } else if (processing_type_ != RetrievalProcType::TAAT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("processing type unexpected", K(ret));
  } else {
    const ObString &query_token = query_tokens_.at(0);
    ObDASTextRetrievalIter *iter = static_cast<ObDASTextRetrievalIter *>(retrieval_iters.at(0));
    if (OB_FAIL(token_iters_.push_back(iter))) {
      LOG_WARN("failed to append token iter to array", K(ret));
    }
  }
  return ret;
}

int ObDASTRTaatIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDASTextRetrievalMergeIter::inner_init(param))) {
    LOG_WARN("failed to init text retrieval", K(ret));
  } else {
    processing_type_ = RetrievalProcType::TAAT;
    is_inited_ = true;
  }
  return ret;
}

int ObDASTRTaatIter::inner_reuse()
{
  int ret = OB_SUCCESS;
  if (hash_maps_) {
    for (int64_t i = 0; i < hash_map_size_; ++i) {
      hash_maps_[i]->destroy();
    }
    hash_maps_ = nullptr;
  }
  if (datum_stores_) {
    for (int64_t i = 0; i < hash_map_size_; ++i) {
      datum_stores_[i]->reset();
    }
    datum_stores_ = nullptr;
  }
  if (datum_store_iters_) {
    for (int64_t i = 0; i < hash_map_size_; ++i) {
      datum_store_iters_[i]->reset();
    }
    datum_store_iters_ = nullptr;
  }
  hash_map_size_ = 0;
  cur_map_iter_ = nullptr;
  next_clear_map_idx_ = 0;
  if (!force_return_docid_) {
    total_doc_cnt_ = -1;
  }
  cur_map_idx_= -1;
  cache_first_docid_.reset();
  is_chunk_store_inited_ = false;
  is_hashmap_inited_ = false;
  if (OB_FAIL(ObDASTextRetrievalMergeIter::inner_reuse())) {
    LOG_WARN("failed to reuse iter", K(ret));
  }
  return ret;
}

int ObDASTRTaatIter::inner_release()
{
  int ret = OB_SUCCESS;
  if (hash_maps_) {
    for (int64_t i = 0; i < hash_map_size_; ++i) {
      hash_maps_[i]->destroy();
    }
    hash_maps_ = nullptr;
  }
  if (datum_stores_) {
    for (int64_t i = 0; i < hash_map_size_; ++i) {
      datum_stores_[i]->reset();
    }
    datum_stores_ = nullptr;
  }
  if (datum_store_iters_) {
    for (int64_t i = 0; i < hash_map_size_; ++i) {
      datum_store_iters_[i]->reset();
    }
    datum_store_iters_ = nullptr;
  }
  hash_map_size_ = 0;
  cur_map_iter_ = nullptr;
  total_doc_cnt_ = -1;
  next_clear_map_idx_ = 0;
  cur_map_idx_= -1;
  cache_first_docid_.reset();
  is_chunk_store_inited_ = false;
  is_hashmap_inited_ = false;
  if (OB_FAIL(ObDASTextRetrievalMergeIter::inner_release())) {
    LOG_WARN("failed to inner release", K(ret));
  }
  return ret;
}

int ObDASTRTaatIter::check_and_prepare()
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
      } else {
        doc_cnt_calculated_ = true;
      }
    } else {
      doc_cnt_calculated_ = true;
    }
  }
  return ret;
}

int ObDASTRTaatIter::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  bool need_fill_doc_cnt = !doc_cnt_calculated_;
  if (OB_FAIL(check_and_prepare())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to prepare to get next row", K(ret));
    }
  } else if (need_fill_doc_cnt && OB_FAIL(fill_total_doc_cnt())) {
    LOG_WARN("failed to fill total document count", K(ret), K(total_doc_cnt_));
  } else if (0 == total_doc_cnt_) {
    ret = OB_ITER_END;
  } else {
    int64_t count = 0;
    const int64_t cap = 1;
    if (OB_FAIL(get_next_batch_rows(count, cap))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to get next row with taat", K(ret));
      } else if (OB_UNLIKELY(count != 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected row count", K(ret), K(count));
      }
    } else if (OB_UNLIKELY(count != 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected row count", K(ret), K(count));
    }
  }
  return ret;
}

int ObDASTRTaatIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  bool need_fill_doc_cnt = !doc_cnt_calculated_;
  if (OB_FAIL(check_and_prepare())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to prepare to get next row", K(ret));
    }
  } else if (need_fill_doc_cnt && OB_FAIL(fill_total_doc_cnt())) {
    LOG_WARN("failed to fill total document count", K(ret), K(total_doc_cnt_));
  } else if (0 == total_doc_cnt_) {
    ret = OB_ITER_END;
  } else if (0 == capacity) {
    count = 0;
  } else if (OB_FAIL(get_next_batch_rows(count, capacity))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get next rows with taat", K(ret));
    }
  }
  return ret;
}

int ObDASTRTaatIter::get_next_batch_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  const bool need_relevance = ir_ctdef_->need_proj_relevance_score();
  int64_t real_capacity = ir_rtdef_->eval_ctx_->max_batch_size_ > 0 ? min(capacity, ir_rtdef_->eval_ctx_->max_batch_size_) : 1;
  if (capacity > real_capacity || count != 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected capacity or count", K(ret), K(capacity), K(ir_rtdef_->eval_ctx_->max_batch_size_), K(count));
  } else if (!is_hashmap_inited_ && OB_FAIL(init_stores_by_partition())) {
    LOG_WARN("failed to init stores by partition", K(ret));
  } else if (!is_chunk_store_inited_ && OB_FAIL(fill_chunk_store_by_tr_iter())) {
    LOG_WARN("failed to fill chunk store by tr iter", K(ret));
  } else if (cur_map_idx_ >= hash_map_size_) {
    ret = OB_ITER_END;
  }
  bool clear_tag = OB_SUCC(ret) || ret == OB_ITER_END;
  // clear the last batch hashmaps
  while (clear_tag && next_clear_map_idx_ < cur_map_idx_) {
    if (OB_UNLIKELY(next_clear_map_idx_ >= hash_map_size_)) {
      clear_tag = false;
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected next_clear_map_idx_", K(ret), K(next_clear_map_idx_), K(cur_map_idx_));
    } else {
      hash_maps_[next_clear_map_idx_++]->clear();
    }
  }
  next_written_idx_ = 0;
  count = 0;
  while (OB_SUCC(ret) && next_written_idx_ != real_capacity) {
    if (cur_map_idx_!= -1 &&
      (force_return_docid_ || (cur_map_iter_ != nullptr && (*cur_map_iter_) != hash_maps_[cur_map_idx_]->end()))) {
      if (OB_UNLIKELY(next_written_idx_ > real_capacity)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected next_written_idx", K(ret), K(next_written_idx_), K(real_capacity));
      } else if (OB_FAIL(fill_output_exprs(count, real_capacity))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to fill output exprs with taat", K(ret));
        }
      }
    } else if (OB_FAIL(load_next_hashmap())) { // cache data
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to load next hashmap", K(ret), K_(cur_map_idx), K_(next_written_idx), K(count), K(real_capacity));
      }
    }
  }

  if (OB_SUCC(ret) || OB_ITER_END == ret) {
    if (next_written_idx_ > 0 && !force_return_docid_) {
      ObExpr *doc_id_col = ir_ctdef_->inv_scan_doc_id_col_;
      ObEvalCtx *eval_ctx = ir_rtdef_->eval_ctx_;
      ObDatum *doc_id_proj_datum = doc_id_col->locate_batch_datums(*eval_ctx);
      doc_id_proj_datum[0].set_string(cache_first_docid_.get_string());
    }
  }

  return ret;
}

int ObDASTRTaatIter::fill_output_exprs(int64_t &count, int64_t safe_capacity)
{
  int ret = OB_SUCCESS;
  const bool need_relevance = ir_ctdef_->need_proj_relevance_score();
  ObDatum *filter_res = nullptr;
  ObExpr *match_filter = need_relevance ? ir_ctdef_->match_filter_ : nullptr;
  hash::ObHashMap<ObDocId, double> *map = hash_maps_[cur_map_idx_];
  ObEvalCtx *eval_ctx = ir_rtdef_->eval_ctx_;
  ObExpr *relevance_proj_col = ir_ctdef_->relevance_proj_col_;
  ObDatum *relevance_proj_datum = nullptr;
  ObExpr *doc_id_col = ir_ctdef_->inv_scan_doc_id_col_;
  ObDatum *doc_id_proj_datum = doc_id_col->locate_batch_datums(*eval_ctx);
  bool filter_valid = false;

  if (need_relevance && count == 0) {
    relevance_proj_datum = relevance_proj_col->locate_datums_for_update(*eval_ctx, safe_capacity);
  } else if (need_relevance) {
    relevance_proj_datum = relevance_proj_col->locate_batch_datums(*eval_ctx);
  }
  for (;OB_SUCC(ret) && next_written_idx_ < safe_capacity && (*cur_map_iter_) != map->end(); (*cur_map_iter_)++) {
      ObEvalCtx *ctx = ir_rtdef_->eval_ctx_;
      ObEvalCtx::BatchInfoScopeGuard guard(*ctx);
      guard.set_batch_idx(next_written_idx_);
      doc_id_proj_datum[next_written_idx_].set_string((*cur_map_iter_)->first.get_string());
      if (need_relevance) {
        relevance_proj_datum[next_written_idx_].set_double((*cur_map_iter_)->second);
        relevance_proj_col->set_evaluated_flag(*eval_ctx);
      }
      if (0 == next_written_idx_) {
        cache_first_docid_.from_string((*cur_map_iter_)->first.get_string());
      }
      clear_evaluated_infos();
      if (nullptr == match_filter) {
        filter_valid = true;
      } else if (OB_FAIL(match_filter->eval(*ir_rtdef_->eval_ctx_, filter_res))) {
        LOG_WARN("failed to evaluate match filter", K(ret));
      } else {
        filter_valid = !(filter_res->is_null() || 0 == filter_res->get_int());
      }
      if (OB_SUCC(ret) && filter_valid) {
        ++input_row_cnt_;
        if (limit_param_.limit_ > 0 && input_row_cnt_ <= limit_param_.offset_) {
        } else {
          next_written_idx_++;
          output_row_cnt_ ++;
          count ++;
          if (limit_param_.limit_ > 0 && output_row_cnt_ >= limit_param_.limit_) {
            ret = OB_ITER_END;
          }
        }
      }
  }
  if (OB_SUCC(ret) && need_relevance) {
    relevance_proj_col->set_evaluated_projected(*eval_ctx);
  }
  return ret;
}

int ObDASTRTaatIter::fill_total_doc_cnt()
{
  int ret = OB_SUCCESS;
  ObEvalCtx::BatchInfoScopeGuard guard(*ir_rtdef_->eval_ctx_);
  guard.set_batch_idx(0);
  ObExpr *total_doc_cnt_expr = ir_ctdef_->get_doc_id_idx_agg_ctdef()->pd_expr_spec_.pd_storage_aggregate_output_.at(0);
  if (OB_ISNULL(total_doc_cnt_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null total doc cnt expr", K(ret));
  } else {
    ObDatum &total_doc_cnt = total_doc_cnt_expr->locate_expr_datum(*ir_rtdef_->eval_ctx_);
    total_doc_cnt_ = total_doc_cnt.get_int();
    if (-1 == total_doc_cnt_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected total doc cnt", K(ret), K(total_doc_cnt));
    }
    LOG_TRACE("use estimated row count as partition document count", K(ret), K(total_doc_cnt));
  }
  return ret;
}

int ObDASTRTaatIter::init_stores_by_partition()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(total_doc_cnt_ <= 0 || is_hashmap_inited_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("total doc cnt is not set", K(ret), K_(is_hashmap_inited));
  } else {
    int64_t partition_cnt = force_return_docid_ ? 1 : OB_MIN((total_doc_cnt_- 1) / OB_HASHMAP_DEFAULT_SIZE + 1, OB_MAX_HASHMAP_COUNT);
    hash_map_size_ = partition_cnt;
    void *buf = nullptr;
    if (nullptr == hash_maps_ && OB_SUCC(ret)) {
      if (OB_ISNULL(buf = mem_context_->get_arena_allocator().alloc(sizeof(hash::ObHashMap<ObDocId, double> *) * partition_cnt))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate enough memory", K(sizeof(hash::ObHashMap<ObDocId, double> *) * partition_cnt), K(ret));
      } else {
        hash_maps_ = static_cast<hash::ObHashMap<ObDocId, double> **>(buf);
      }
    }
    if (nullptr == datum_stores_ && OB_SUCC(ret)) {
      if (OB_ISNULL(buf = mem_context_->get_arena_allocator().alloc(sizeof(sql::ObChunkDatumStore *) * partition_cnt))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate enough memory", K(sizeof(sql::ObChunkDatumStore *) * partition_cnt), K(ret));
      } else {
        datum_stores_ = static_cast<sql::ObChunkDatumStore **>(buf);
      }
    }
    if (nullptr == datum_store_iters_ && OB_SUCC(ret)) {
      if (OB_ISNULL(buf = mem_context_->get_arena_allocator().alloc(sizeof(sql::ObChunkDatumStore::Iterator *) * partition_cnt))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate enough memory", K(sizeof(sql::ObChunkDatumStore::Iterator *) * partition_cnt), K(ret));
      } else {
        datum_store_iters_ = static_cast<sql::ObChunkDatumStore::Iterator **>(buf);
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < partition_cnt; ++i) {
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(buf = mem_context_->get_arena_allocator().alloc(sizeof(hash::ObHashMap<ObDocId, double>)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate enough memory", K(sizeof(hash::ObHashMap<ObDocId, double>)), K(ret));
      } else {
        hash::ObHashMap<ObDocId, double> *hash_map = new(buf) hash::ObHashMap<ObDocId, double>();
        if (OB_FAIL(hash_map->create(10, common::ObMemAttr(MTL_ID(), "FTTaatMap")))) {
          LOG_WARN("failed to create token map", K(ret));
        } else {
          hash_maps_[i] = hash_map;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(buf = mem_context_->get_arena_allocator().alloc(sizeof(sql::ObChunkDatumStore)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate enough memory", K(sizeof(sql::ObChunkDatumStore)), K(ret));
      } else {
        sql::ObChunkDatumStore *store = new(buf) sql::ObChunkDatumStore(common::ObModIds::OB_SQL_CHUNK_ROW_STORE);
        if(OB_FAIL(store->init(
          1024 * 8 /* mem limit */,
          MTL_ID(), common::ObCtxIds::DEFAULT_CTX_ID, common::ObModIds::OB_SQL_CHUNK_ROW_STORE,
          true /* enable dump */,
          0, /* row_extra_size */
          ObChunkDatumStore::BLOCK_SIZE))) {
            LOG_WARN("init chunk datum store failed", K(ret));
        } else {
          // datum_store_.set_dir_id(sql_mem_processor_.get_dir_id());
          store->alloc_dir_id();
          store->set_allocator(mem_context_->get_malloc_allocator());
          // datum_store_.set_io_event_observer(io_event_observer_);
          // datum_store_.set_callback(&sql_mem_processor_);
          datum_stores_[i] = store;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(buf = mem_context_->get_arena_allocator().alloc(sizeof(sql::ObChunkDatumStore::Iterator)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate enough memory", K(sizeof(sql::ObChunkDatumStore::Iterator)), K(ret));
      } else {
        sql::ObChunkDatumStore::Iterator *iter = new(buf) sql::ObChunkDatumStore::Iterator();
        datum_store_iters_[i] = iter;
      }
    }
    if (OB_SUCC(ret)) {
      is_hashmap_inited_ = true;
    }
  }
  return ret;
}

int ObDASTRTaatIter::fill_chunk_store_by_tr_iter()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(token_iters_.count() != 1
    || typeid(*token_iters_.at(0)) != typeid(ObDASTextRetrievalIter)
    || ir_rtdef_->get_inv_idx_scan_rtdef()->eval_ctx_ != ir_rtdef_->eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("token iters count unexpected", K(token_iters_.count()), K(ret));
  } else if (OB_UNLIKELY(is_chunk_store_inited_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("chunk store is already inited", K(ret));
  } else {
    ObSEArray<ObExpr *, 2> exprs;
    if (OB_FAIL(exprs.push_back(ir_ctdef_->inv_scan_doc_id_col_))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else if (ir_ctdef_->need_proj_relevance_score()) {
      if (OB_FAIL(exprs.push_back(ir_ctdef_->relevance_expr_))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    }

    int64_t capacity = ir_rtdef_->eval_ctx_->max_batch_size_;
    sql::ObBitVector **skips = nullptr;
    void *buf = nullptr;
    if (OB_SUCC(ret) && ir_ctdef_->inv_scan_doc_id_col_->is_batch_result()) {
      if (nullptr == skips && OB_SUCC(ret)) {
        if (OB_ISNULL(buf = mem_context_->get_arena_allocator().alloc(sizeof(sql::ObBitVector *) * hash_map_size_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate enough memory", K(sizeof(sql::ObBitVector *) * hash_map_size_), K(ret));
        } else {
          skips = static_cast<sql::ObBitVector **>(buf);
        }
      }
      if (OB_SUCC(ret) && ir_ctdef_->inv_scan_doc_id_col_->is_batch_result()) {
        for (int64_t i = 0; OB_SUCC(ret) && i < hash_map_size_; ++i) {
          sql::ObBitVector *skip = nullptr;
          if (OB_ISNULL(skip = to_bit_vector(mem_context_->get_arena_allocator().alloc(ObBitVector::memory_size(capacity))))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate enough memory", K(capacity), K(ret));
          } else {
            skip->init(capacity);
            skips[i] = skip;
          }

        }
      }
    }

    int64_t all_token_doc_cnt = 0;
    for (int64_t token_idx = 0; OB_SUCC(ret) && token_idx < query_tokens_.count(); ++token_idx) {
      int64_t token_doc_cnt = 0;
      ObDocId doc_id;
      if (ir_ctdef_->inv_scan_doc_id_col_->is_batch_result()) {
        while (OB_SUCC(ret)) {
          int64_t count = 0;
          // get batch rows from storage
          if (OB_FAIL(token_iters_.at(0)->get_next_rows(count, capacity))) {
            if (OB_UNLIKELY(OB_ITER_END != ret)) {
              LOG_WARN("failed to get batch rows from inverted index", K(ret));
            } else if (OB_LIKELY(count > 0)) {
              ret = OB_SUCCESS;
            }
          }
          if (OB_SUCC(ret)) {
            // handle skips // TODO: use batch skip like hashjoin
            for (int64_t i = 0; i < hash_map_size_; ++i) {
              skips[i]->set_all(capacity);
            }
            for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
              ObExpr *doc_id_expr = ir_ctdef_->inv_scan_doc_id_col_;
              const ObDatum &doc_id_datum = doc_id_expr->locate_expr_datum(*ir_rtdef_->get_inv_idx_scan_rtdef()->eval_ctx_, i);
              if (OB_FAIL(doc_id.from_string(doc_id_datum.get_string()))) {
                LOG_WARN("failed to get doc id", K(ret));
              } else {
                int64_t partition = doc_id.seq_id_ % hash_map_size_;
                skips[partition]->unset(i);
              }
            }
            int64_t check_count = 0;
            // fill the datum_stores_
            for (int64_t i = 0; OB_SUCC(ret) && i < hash_map_size_; ++i) {
              int64_t stored_rows_count = 0;
              if (OB_FAIL(datum_stores_[i]->add_batch(exprs,
                                                 *(ir_rtdef_->get_inv_idx_scan_rtdef()->eval_ctx_),
                                                 *skips[i],
                                                 count,
                                                 stored_rows_count))) {
                LOG_WARN("failed to add datum", K(ret));
              } else {
                check_count += stored_rows_count;
                token_doc_cnt += stored_rows_count;
              }
            }
            if (OB_SUCC(ret) && check_count != count) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("failed to add all datums", K(ret));
            }
          }
        }
      } else {
        while (OB_SUCC(ret)) {
          if (OB_FAIL(token_iters_.at(0)->get_next_row())) {
            if (OB_UNLIKELY(OB_ITER_END != ret)) {
              LOG_WARN("failed to get batch rows from inverted index", K(ret));
            }
          } else {
            ObExpr *doc_id_expr = ir_ctdef_->inv_scan_doc_id_col_;
            const ObDatum &doc_id_datum = doc_id_expr->locate_expr_datum(*ir_rtdef_->get_inv_idx_scan_rtdef()->eval_ctx_);
            if (OB_FAIL(doc_id.from_string(doc_id_datum.get_string()))) {
              LOG_WARN("failed to get doc id", K(ret));
            } else {
              int64_t partition = doc_id.seq_id_ % hash_map_size_;
              ObChunkDatumStore::StoredRow **sr = NULL;
              if (OB_FAIL(datum_stores_[partition]->add_row(exprs,
                                                            ir_rtdef_->get_inv_idx_scan_rtdef()->eval_ctx_,
                                                            sr))) {
                LOG_WARN("failed to add datum", K(ret));
              } else {
                token_doc_cnt ++;
              }
            }
          }
        }
      }
      ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
      if (OB_FAIL(ret)) {
      } else {
        all_token_doc_cnt += token_doc_cnt;
        sql::ObEvalCtx *eval_ctx = ir_rtdef_->get_inv_idx_scan_rtdef()->eval_ctx_;
        eval_ctx->reuse(eval_ctx->get_batch_size());
        // the last token don't reuse and rescan
        if (token_idx + 1 < query_tokens_.count()) {
          if (OB_FAIL(token_iters_.at(0)->reuse())) {
            LOG_WARN("failed to reuse tr iter", K(ret));
          } else if (OB_FAIL(reset_query_token(query_tokens_.at(token_idx + 1)))) {
            LOG_WARN("failed to set query token", K(ret));
          } else if (OB_FAIL(token_iters_.at(0)->rescan())) {
            LOG_WARN("failed to rescan tr iter", K(ret));
          }
       }
      }
    }

    if (OB_FAIL(ret)) {
    } else {
      int64_t check_cnt = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < hash_map_size_; ++i) {
        if (OB_FAIL(datum_store_iters_[i]->init(datum_stores_[i]))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to init datum iter", K(ret));
        } else {
          check_cnt += datum_stores_[i]->get_row_cnt();
        }
      }
      if (OB_SUCC(ret) && OB_UNLIKELY(check_cnt != all_token_doc_cnt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get the wrong rows cnt", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    is_chunk_store_inited_ = true;
  }
  return ret;
}

int ObDASTRTaatIter::load_next_hashmap()
{
  int ret = OB_SUCCESS;
  if (cur_map_idx_ >= hash_map_size_ - 1) {
    cur_map_idx_= hash_map_size_;
    ret = OB_ITER_END;
  } else if (FALSE_IT(++cur_map_idx_)) {
  } else if (OB_FAIL(inner_load_next_hashmap())) {
    LOG_WARN("failed to load next hashmap", K(ret));
  } else if (force_return_docid_) {
    // do nothing
  } else {
    hash::ObHashMap<ObDocId, double>::iterator iter = hash_maps_[cur_map_idx_]->begin();
    void *buf = nullptr;
    if (nullptr == cur_map_iter_ && OB_ISNULL(buf = mem_context_->get_arena_allocator().alloc(sizeof(hash::ObHashMap<ObDocId, double>::iterator)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate enough memory", K(sizeof(hash::ObHashMap<ObDocId, double>::iterator)), K(ret));
    } else if (nullptr == buf) {
      cur_map_iter_ = new(cur_map_iter_) hash::ObHashMap<ObDocId, double>::iterator(iter);
    } else {
      cur_map_iter_ = new(buf) hash::ObHashMap<ObDocId, double>::iterator(iter);
    }
  }
  return ret;
}

int ObDASTRTaatIter::inner_load_next_hashmap()
{
  int ret = OB_SUCCESS;
  const bool need_relevance = ir_ctdef_->need_proj_relevance_score();
  if (OB_UNLIKELY(hash_maps_[cur_map_idx_]->size() != 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected to get one empty hashmap", K(ret), K_(cur_map_idx));
  }
  hash::ObHashMap<ObDocId, double> *map = hash_maps_[cur_map_idx_];
  sql::ObChunkDatumStore::Iterator *store_iter = datum_store_iters_[cur_map_idx_];

  sql::ObExpr *relevance_expr = ir_ctdef_->relevance_expr_;
  ObExpr *doc_id_expr = ir_ctdef_->inv_scan_doc_id_col_;
  if (OB_FAIL(ret)) {
  } else if (need_relevance && (OB_ISNULL(relevance_expr) || OB_ISNULL(doc_id_expr))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid relevance or doc id expr", K(ret));
  } else if (!need_relevance && OB_ISNULL(doc_id_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid relevance or doc id expr", K(ret));
  }

  ObSEArray<ObExpr *, 2> exprs;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(exprs.push_back(doc_id_expr))) {
    LOG_WARN("failed to push back expr", K(ret));
  } else if (need_relevance) {
    if (OB_FAIL(exprs.push_back(relevance_expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  }

  sql::ObEvalCtx *ctx = ir_rtdef_->eval_ctx_;
  ObEvalCtx::BatchInfoScopeGuard guard(*ctx);
  guard.set_batch_idx(0);

  ObDocId doc_id;
  double cur_relevance = 0;
  double last_relevance = 0;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(store_iter->get_next_row(*ctx, exprs))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to get next row from datum store", K(ret));
      }
    } else {
      ObDatum &doc_id_datum = doc_id_expr->locate_expr_datum(*ir_rtdef_->eval_ctx_);
      if (OB_FAIL(doc_id.from_string(doc_id_datum.get_string()))) {
        LOG_WARN("failed to get doc id", K(ret));
      } else if (need_relevance) {
        ObDatum &relevance_datum = relevance_expr->locate_expr_datum(*ir_rtdef_->eval_ctx_);
        cur_relevance = relevance_datum.get_double();
        if (OB_FAIL(map->get_refactored(doc_id, last_relevance))) {
          if (OB_HASH_NOT_EXIST != ret) {
            LOG_WARN("fail to get relevance", K(ret), K(last_relevance));
          } else if (OB_FAIL(map->set_refactored(doc_id, cur_relevance, 1/*overwrite*/))) {
            LOG_WARN("failed to push data", K(ret));
          }
        } else if (OB_FAIL(map->set_refactored(doc_id, cur_relevance + last_relevance, 1/*overwrite*/))) {
          LOG_WARN("failed to push data", K(ret));
        }
      } else {
        if (OB_FAIL(map->set_refactored(doc_id, 1, 0/*overwrite*/))) {
          LOG_WARN("failed to push data", K(ret));
        }
      }
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObDASTRTaatIter::reset_query_token(const ObString &query_token)
{
  int ret = OB_SUCCESS;
  if (!force_return_docid_) {
    if (OB_FAIL(token_iters_.at(0)->set_query_token(query_token))) {
      LOG_WARN("failed to set query token", K(ret));
    }
  } else {
    if (OB_FAIL(token_iters_.at(0)->set_query_token_and_rangekey(query_token, cache_doc_ids_, rangekey_size_))) {
      LOG_WARN("failed to set token and rangekey", K(ret), K_(rangekey_size));
    }
  }
  return ret;
}

ObDASTRTaatLookupIter::ObDASTRTaatLookupIter()
  : ObDASTRTaatIter()
{
}

int ObDASTRTaatLookupIter::rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDASTextRetrievalMergeIter::rescan())) {
    LOG_WARN("failed to rescan iter", K(ret));
  } else if (OB_UNLIKELY(token_iters_.count() > 1)) {
    ret= OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected iter count mismatch with query tokens",
        K(ret), K_(query_tokens), K_(token_iters));
  } else if (0 != query_tokens_.count()) {
    ObDASTextRetrievalIter *iter = token_iters_.at(0);
    if (OB_FAIL(iter->rescan())) {
      LOG_WARN("failed to append token iter to array", K(ret));
    }
    is_hashmap_inited_ = false;
    cur_map_idx_= -1;
    next_clear_map_idx_ = 0;
  }
  return ret;
}

int ObDASTRTaatLookupIter::fill_output_exprs(int64_t &count, int64_t safe_capacity)
{
  int ret = OB_SUCCESS;
  const bool need_relevance = ir_ctdef_->need_proj_relevance_score();
  ObDatum *filter_res = nullptr;
  ObExpr *match_filter = need_relevance ? ir_ctdef_->match_filter_ : nullptr;
  hash::ObHashMap<ObDocId, double> *map = hash_maps_[cur_map_idx_];
  ObEvalCtx *eval_ctx = ir_rtdef_->eval_ctx_;
  ObExpr *relevance_proj_col = ir_ctdef_->relevance_proj_col_;
  ObDatum *relevance_proj_datum = nullptr;
  ObExpr *doc_id_col = ir_ctdef_->inv_scan_doc_id_col_;
  ObDatum *doc_id_proj_datum = doc_id_col->locate_batch_datums(*eval_ctx);
  bool filter_valid = false;

  if (need_relevance) {
    relevance_proj_datum = relevance_proj_col->locate_datums_for_update(*eval_ctx, safe_capacity);
  }

  if (OB_UNLIKELY(count != 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected size", K(ret), K_(rangekey_size), K(safe_capacity), K(count));
  } else if (OB_UNLIKELY(hash_map_size_ != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected size", K(ret), K_(hash_map_size));
  } else if (OB_UNLIKELY(nullptr != match_filter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected match filter", K(ret));
  }

  hash::ObHashMap<ObDocId, double> *first_map = hash_maps_[0];
  ObEvalCtx *ctx = ir_rtdef_->eval_ctx_;
  for (int64_t i = 0; OB_SUCC(ret) && i < rangekey_size_; ++i) {
    double cur_relevance = 0;
    if (OB_FAIL(first_map->get_refactored(cache_doc_ids_[i], cur_relevance))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("fail to get relevance", K(ret), K(cur_relevance));
      } else {
        ret = OB_SUCCESS;
      }
    }
    if (OB_SUCC(ret)) {
      int64_t pos = hints_[i];
      if (pos < safe_capacity) {
        ObEvalCtx::BatchInfoScopeGuard guard(*ctx);
        guard.set_batch_idx(pos);
        doc_id_proj_datum[pos].set_string(cache_doc_ids_[i].get_string());
        if (need_relevance) {
          relevance_proj_datum[pos].set_double(cur_relevance);
          relevance_proj_col->set_evaluated_flag(*eval_ctx);
        }
        output_row_cnt_ ++;
        input_row_cnt_ ++;
        count ++;
      } else {
        cache_relevances_[pos] = cur_relevance;
      }
      next_written_idx_++;
    }
  }
  if (OB_SUCC(ret)) {
    next_written_idx_ = count;
    if (need_relevance) {
      relevance_proj_col->set_evaluated_projected(*eval_ctx);
    }
    if (count != OB_MIN(safe_capacity, rangekey_size_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected size", K(ret), K(count), K_(rangekey_size), K(safe_capacity));
    } else if (count == rangekey_size_) {
      ret = OB_ITER_END;
    }
  }
  return ret;
}

int ObDASTRTaatLookupIter::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  bool need_fill_doc_cnt = !doc_cnt_calculated_;
  if (OB_UNLIKELY(1 != rangekey_size_)) { // if rangekey_size_ > 1, UNSUPPORTED
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected rangekey size", K(ret), K_(rangekey_size));
  } else if (next_written_idx_ >= rangekey_size_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(check_and_prepare())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to prepare to get next row", K(ret));
    } else {
      ObDocId default_docid;
      ObEvalCtx *ctx = ir_rtdef_->eval_ctx_;
      ObEvalCtx::BatchInfoScopeGuard guard(*ctx);
      guard.set_batch_idx(0);
      if (OB_FAIL(project_result(default_docid,0))) {
        LOG_WARN("failed to project result", K(ret));
      } else {
        next_written_idx_++;
      }
    }
  } else if (need_fill_doc_cnt && OB_FAIL(fill_total_doc_cnt())) {
    LOG_WARN("failed to fill total document count", K(ret), K(total_doc_cnt_));
  } else if (0 == total_doc_cnt_) {
    ret = OB_ITER_END;
  } else if (total_doc_cnt_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected total doc cnt", K(ret), K(total_doc_cnt_));
  } else {
    int64_t count = 0;
    const int64_t cap = 1;
    if (OB_FAIL(get_next_batch_rows(count, cap))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to get next row with taat", K(ret));
      } else if (0 != count) {
        ret = OB_SUCCESS;
      }
    } else if (OB_UNLIKELY(count != 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected row count", K(ret), K(count));
    }
  }
  return ret;
}

int ObDASTRTaatLookupIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  count = 0;
  bool need_fill_doc_cnt = !doc_cnt_calculated_;
  if (OB_UNLIKELY(capacity == 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected capacity size", K(ret), K(capacity));
  } else if (OB_FAIL(check_and_prepare())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to prepare to get next row", K(ret));
    } else if (next_written_idx_ == rangekey_size_) {
      // do nothing
    } else if (next_written_idx_ != 0 ) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected next_written_idx", K(ret), K_(next_written_idx));
    } else {
      ret = OB_SUCCESS;
      ObEvalCtx *ctx = ir_rtdef_->eval_ctx_;
      ObExpr *relevance_proj_col = ir_ctdef_->relevance_proj_col_;
      while (OB_SUCC(ret) && next_written_idx_ < rangekey_size_) {
        // fill the remaining results with the relevance value of '0'
        // Note: if we need calculate the docid expr, fix the code and cache the hints.
        ObDocId default_docid;
        ObEvalCtx::BatchInfoScopeGuard guard(*ctx);
        guard.set_batch_idx(next_written_idx_);
        if (OB_FAIL(project_result(default_docid, 0))) {
          LOG_WARN("failed to project result", K(ret));
        }
        next_written_idx_ ++;
      }
      if (OB_SUCC(ret)) {
        for (int i = 0; i < rangekey_size_; i++) {
          relevance_proj_col->get_evaluated_flags(*ctx).set(i);
        }
        ret = OB_ITER_END;
      }
      relevance_proj_col->set_evaluated_projected(*ctx);
      count = OB_MIN(rangekey_size_, capacity);
      next_written_idx_ = OB_MIN(rangekey_size_, capacity);
    }
  } else if (need_fill_doc_cnt && OB_FAIL(fill_total_doc_cnt())) {
    LOG_WARN("failed to fill total document count", K(ret), K(total_doc_cnt_));
  } else if (0 == total_doc_cnt_) {
    ret = OB_ITER_END;
  } else if (total_doc_cnt_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected total doc cnt", K(ret), K(total_doc_cnt_));
  } else if (next_written_idx_ == rangekey_size_) {
    ret = OB_ITER_END;
  } else if (next_written_idx_ > rangekey_size_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected capacity size", K(ret), K(capacity));
  } else if (next_written_idx_ == 0 && OB_FAIL(get_next_batch_rows(count, capacity))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get next rows with taat", K(ret));
    }
  } else if (next_written_idx_ < rangekey_size_) {
    int remain_size = rangekey_size_ - next_written_idx_;
    int return_size = OB_MIN(remain_size, capacity);
    // next_written_idx_ is the output idx
    ObEvalCtx *ctx = ir_rtdef_->eval_ctx_;
    while (count < return_size && OB_SUCC(ret)) {
      int64_t pos = reverse_hints_[next_written_idx_];
      ObEvalCtx::BatchInfoScopeGuard guard(*ctx);
      guard.set_batch_idx(count);
      if (OB_FAIL(project_result(cache_doc_ids_[pos], cache_relevances_[next_written_idx_]))) {
        LOG_WARN("failed to project result", K(ret));
      }
      next_written_idx_++;
      count ++;
    }
    if (OB_SUCC(ret) && next_written_idx_ == rangekey_size_) {
      ret = OB_ITER_END;
    }
  }
  return ret;
}

ObDASTRDaatIter::ObDASTRDaatIter()
  : ObDASTextRetrievalMergeIter(),
    loser_tree_cmp_(),
    iter_row_heap_(nullptr),
    next_batch_iter_idxes_(),
    next_batch_cnt_(0)
{
}

int ObDASTRDaatIter::rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDASTextRetrievalMergeIter::rescan())) {
    LOG_WARN("failed to rescan iter", K(ret));
  } else if (OB_UNLIKELY(query_tokens_.count() != token_iters_.count())) {
    ret= OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected iter count mismatch with query tokens",
        K(ret), K_(query_tokens), K_(token_iters));
  } else if (0 != query_tokens_.count()) {
    if (OB_FAIL(next_batch_iter_idxes_.init(query_tokens_.count()))) {
      LOG_WARN("failed to init next batch iter idxes array", K(ret));
    } else if (OB_FAIL(next_batch_iter_idxes_.prepare_allocate(query_tokens_.count()))) {
      LOG_WARN("failed to prepare allocate next batch iter idxes array", K(ret));
    } else if (OB_FAIL(iter_row_heap_->open(query_tokens_.count()))) {
      LOG_WARN("failed to open iter row heap", K(ret), K_(query_tokens));
    } else {
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
  }
  return ret;
}

int ObDASTRDaatIter::set_merge_iters(const ObIArray<ObDASIter *> &retrieval_iters)
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
  } else if (processing_type_ != RetrievalProcType::DAAT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("processing type unexpected", K(ret));
  } else {
    next_batch_cnt_ = query_tokens_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < query_tokens_.count(); ++i) {
      const ObString &query_token = query_tokens_.at(i);
      ObDASTextRetrievalIter *iter = static_cast<ObDASTextRetrievalIter *>(retrieval_iters.at(i));
      if (OB_FAIL(token_iters_.push_back(iter))) {
        LOG_WARN("failed to append token iter to array", K(ret));
      }
    }
  }
  return ret;
}

int ObDASTRDaatIter::do_table_scan()
{
  int ret = OB_SUCCESS;
  if (FALSE_IT(next_batch_iter_idxes_.set_allocator(&mem_context_->get_arena_allocator()))) {
  } else if (OB_FAIL(next_batch_iter_idxes_.init(query_tokens_.count()))) {
    LOG_WARN("failed to init next batch iter idxes array", K(ret));
  } else if (OB_FAIL(next_batch_iter_idxes_.prepare_allocate(query_tokens_.count()))) {
    LOG_WARN("failed to prepare allocate next batch iter idxes array", K(ret));
  } else if (query_tokens_.count()!= 0 && OB_FAIL(iter_row_heap_->open(query_tokens_.count()))) {
      LOG_WARN("failed to open iter row heap", K(ret), K_(query_tokens));
  } else {
    next_batch_cnt_ = query_tokens_.count();
    for (int64_t i = 0; i < next_batch_cnt_; ++i) {
      next_batch_iter_idxes_[i] = i;
    }
  }
  if (OB_FAIL(ret)) {
  } else if(OB_FAIL(ObDASTextRetrievalMergeIter::do_table_scan())) {
    LOG_WARN("failed to do table scan", K(ret));
  }
  return ret;
}

int ObDASTRDaatIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDASTextRetrievalMergeIter::inner_init(param))) {
    LOG_WARN("failed to init text retrieval", K(ret));
  } else if (OB_FAIL(loser_tree_cmp_.init())) {
    LOG_WARN("failed to init loser tree comparator", K(ret));
  } else if (0 == query_tokens_.count()) {
    processing_type_ = RetrievalProcType::DAAT;
    is_inited_ = true;
  } else if (OB_UNLIKELY(query_tokens_.count() > OB_MAX_TEXT_RETRIEVAL_TOKEN_CNT)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query tokens count unexpected", K(ret), K(query_tokens_.count()));
  } else if (OB_ISNULL(iter_row_heap_ = OB_NEWx(ObIRIterLoserTree, &mem_context_->get_arena_allocator(), loser_tree_cmp_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate loser tree", K(ret));
  } else if (OB_FAIL(iter_row_heap_->init(query_tokens_.count(), mem_context_->get_arena_allocator()))) {
    LOG_WARN("failed to init iter loser tree", K(ret));
  } else {
    processing_type_ = RetrievalProcType::DAAT;
    is_inited_ = true;
  }
  return ret;
}

int ObDASTRDaatIter::inner_reuse()
{
  int ret = OB_SUCCESS;
  next_batch_cnt_ = 0;
  next_batch_iter_idxes_.reuse();
  if (iter_row_heap_) {
    iter_row_heap_->reuse();
  }
  if (OB_FAIL(ObDASTextRetrievalMergeIter::inner_reuse())) {
    LOG_WARN("failed to reuse iter", K(ret));
  }
  return ret;
}

int ObDASTRDaatIter::inner_release()
{
  int ret = OB_SUCCESS;
  if (nullptr != iter_row_heap_) {
    iter_row_heap_->~ObIRIterLoserTree();
    iter_row_heap_ = nullptr;
  }
  next_batch_iter_idxes_.reset();
  next_batch_cnt_ = 0;
  if (OB_FAIL(ObDASTextRetrievalMergeIter::inner_release())) {
    LOG_WARN("failed to inner release", K(ret));
  }
  return ret;
}

int ObDASTRDaatIter::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_and_prepare())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to prepare to get next row", K(ret));
    }
  } else {
    bool filter_valid = false;
    bool got_valid_document = false;
    bool doc_valid = false;
    ObExpr *match_filter = ir_ctdef_->need_calc_relevance() ? ir_ctdef_->match_filter_ : nullptr;
    ObDatum *filter_res = nullptr;
    const bool is_batch = false;
    while (OB_SUCC(ret) && !got_valid_document) {
      clear_evaluated_infos();
      filter_valid = false;
      doc_valid = true;
      if (OB_FAIL(pull_next_batch_rows())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to pull next batch rows from iterator", K(ret));
        }
      } else if (OB_FAIL(next_disjunctive_document(is_batch, doc_valid))) {
        LOG_WARN("failed to get next document with disjunctive tokens", K(ret));
      } else if (!doc_valid) {
        // do nothing
      } else if (nullptr == match_filter) {
        filter_valid = true;
      } else if (OB_FAIL(match_filter->eval(*ir_rtdef_->eval_ctx_, filter_res))) {
        LOG_WARN("failed to evaluate match filter", K(ret));
      } else {
        filter_valid = !(filter_res->is_null() || 0 == filter_res->get_int());
      }
      if (OB_SUCC(ret)) {
        if (doc_valid && filter_valid) {
          ++input_row_cnt_;
          if (input_row_cnt_ > limit_param_.offset_) {
            got_valid_document = true;
            ++output_row_cnt_;
          }
        }
      }
    }
  }
  return ret;
}

int ObDASTRDaatIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_and_prepare())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to prepare to get next rows", K(ret));
    }
  } else if (0 == capacity) {
    count = 0;
  } else {
    ObExpr *match_filter = ir_ctdef_->need_calc_relevance() ? ir_ctdef_->match_filter_ : nullptr;
    int64_t real_capacity = min(capacity, ir_rtdef_->eval_ctx_->max_batch_size_);
    ObDatum *filter_res = nullptr;
    const bool is_batch = true;
    next_written_idx_ = 0;
    count = 0;
    bool filter_valid = false;
    bool doc_valid = true;
    while (OB_SUCC(ret) && next_written_idx_ < real_capacity) {
      if (OB_FAIL(pull_next_batch_rows_with_batch_mode())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to pull next batch rows from iterator", K(ret));
        }
      } else if (OB_FAIL(next_disjunctive_document(is_batch, doc_valid))) {
        LOG_WARN("failed to get next document with disjunctive tokens", K(ret));
      } else if (!doc_valid) {
        // do nothing
      } else {
        ObEvalCtx *ctx = ir_rtdef_->eval_ctx_;
        ObEvalCtx::BatchInfoScopeGuard guard(*ctx);
        guard.set_batch_idx(next_written_idx_);
        clear_evaluated_infos();
        // Although we use the batch interface, we need still use the non-batch interface of the match_filter.
        // This is because the batch interface of the match_filter might lead to generate some invalid data position in the ctx of expr.
        // Passing this invalid data position to the upper level is Costly.
        if (nullptr == match_filter) {
          filter_valid = true;
        } else if (OB_FAIL(match_filter->eval(*ir_rtdef_->eval_ctx_, filter_res))) {
          LOG_WARN("failed to evaluate match filter", K(ret));
        } else {
          filter_valid = !(filter_res->is_null() || 0 == filter_res->get_int());
        }
        if (OB_SUCC(ret) && filter_valid) {
          ++input_row_cnt_;
          if (limit_param_.limit_ > 0 && input_row_cnt_ <= limit_param_.offset_) {
          } else {
            next_written_idx_++;
            count ++;
            output_row_cnt_ ++;
            if (limit_param_.limit_ > 0 && output_row_cnt_ >= limit_param_.limit_) {
              ret = OB_ITER_END;
            }
          }
        }
      }
    }
    if ((OB_SUCC(ret) || OB_ITER_END == ret) && count > 0 && OB_FAIL(project_docid())) {
      LOG_WARN("failed to project docids", K(ret));
    }
  }
  return ret;
}

int ObDASTRDaatIter::pull_next_batch_rows()
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
    } else if (0 != next_batch_cnt_ && OB_FAIL(iter_row_heap_->rebuild())) {
      LOG_WARN("fail to rebuild loser tree", K(ret), K_(next_batch_cnt));
    } else {
      next_batch_cnt_ = 0;
    }
  }
  return ret;
}

int ObDASTRDaatIter::pull_next_batch_rows_with_batch_mode()
{
  int ret = OB_SUCCESS;
  ObIRIterLoserTreeItem item;
  int64_t count = 0;
  int64_t capacity = 1;
  for (int64_t i = 0; OB_SUCC(ret) && i < next_batch_cnt_; ++i) {
    const int64_t iter_idx = next_batch_iter_idxes_[i];
    ObDASTRCacheIter *iter = nullptr;
    count = 0;
    if (OB_ISNULL(iter = static_cast<ObDASTRCacheIter *>(token_iters_.at(iter_idx)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null token iter ptr", K(ret), K(iter_idx), K(token_iters_.count()));
    } else if (OB_FAIL(iter->get_next_rows(count, capacity))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to get pull next batch rows from iterator", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    } else {
      item.iter_idx_ = iter_idx;
      if (OB_FAIL(iter->get_cur_row(item.relevance_, item.doc_id_))) {
        LOG_WARN("fail to fill loser tree item", K(ret));
      } else if (OB_FAIL(iter_row_heap_->push(item))) {
        LOG_WARN("fail to push item to loser tree", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (iter_row_heap_->empty()) {
      ret = OB_ITER_END;
    } else if (0 != next_batch_cnt_ && OB_FAIL(iter_row_heap_->rebuild())) {
      LOG_WARN("fail to rebuild loser tree", K(ret), K_(next_batch_cnt));
    } else {
      next_batch_cnt_ = 0;
    }
  }
  return ret;
}

int ObDASTRDaatIter::fill_loser_tree_item(
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

int ObDASTRDaatIter::next_disjunctive_document(const bool is_batch, bool &doc_valid)
{
  int ret = OB_SUCCESS;
  int64_t doc_cnt = 0;
  bool curr_doc_end = false;
  const ObIRIterLoserTreeItem *top_item = nullptr;
  // Do we need to use ObExpr to collect relevance?
  double cur_doc_relevance = 0.0;
  doc_valid = true;
  if (BOOLEAN == relation_type_) {
    for (int i = 0; OB_SUCC(ret) && i < query_tokens_.count(); ++i) {
      boolean_relevances_[i] = 0;
    }
  }
  while (OB_SUCC(ret) && !iter_row_heap_->empty() && !curr_doc_end) {
    if (iter_row_heap_->is_unique_champion()) {
      curr_doc_end = true;
    }
    if (OB_FAIL(iter_row_heap_->top(top_item))) {
      LOG_WARN("failed to get top item from heap", K(ret));
    } else {
      next_batch_iter_idxes_[next_batch_cnt_++] = top_item->iter_idx_;
      if (BOOLEAN == relation_type_) {
        boolean_relevances_[top_item->iter_idx_] = top_item->relevance_;
      } else {
        // consider to add an expr for collectiong conjunction result between query tokens here?
        cur_doc_relevance += top_item->relevance_;
      }
      if (OB_FAIL(iter_row_heap_->pop())) {
        LOG_WARN("failed to pop top item in heap", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
    //do nothing
  } else if (BOOLEAN != relation_type_) {
    //do nothing
  } else if (OB_FAIL(ObFtsEvalNode::fts_boolean_eval(root_node_, boolean_relevances_, cur_doc_relevance))) {
    LOG_WARN("failed to evaluate boolean relevance", K(ret));
  } else {
    doc_valid = cur_doc_relevance > 0;
  }

  if (OB_SUCC(ret) && doc_valid) {
    const double relevance_score = ir_ctdef_->need_calc_relevance() ? cur_doc_relevance : 1;
    if (!is_batch && OB_FAIL(project_result(top_item->doc_id_, relevance_score))) {
      LOG_WARN("failed to project result", K(ret));
    } else if (is_batch && OB_FAIL(project_relevance(top_item->doc_id_, relevance_score))) {
      LOG_WARN("failed to project relevance", K(ret));
    }
  }
  return ret;
}

ObDASTRDaatLookupIter::ObDASTRDaatLookupIter()
  : ObDASTRDaatIter()
{
}

int ObDASTRDaatLookupIter::rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDASTextRetrievalMergeIter::rescan())) {
    LOG_WARN("failed to rescan iter", K(ret));
  } else if (OB_UNLIKELY(query_tokens_.count() != token_iters_.count())) {
    ret= OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected iter count mismatch with query tokens",
        K(ret), K_(query_tokens), K_(token_iters));
  } else if (0 != query_tokens_.count()) {
    if (OB_FAIL(next_batch_iter_idxes_.init(query_tokens_.count()))) {
      LOG_WARN("failed to init next batch iter idxes array", K(ret));
    } else if (OB_FAIL(next_batch_iter_idxes_.prepare_allocate(query_tokens_.count()))) {
      LOG_WARN("failed to prepare allocate next batch iter idxes array", K(ret));
    } else if (OB_FAIL(iter_row_heap_->open(query_tokens_.count()))) {
      LOG_WARN("failed to open iter row heap", K(ret), K_(query_tokens));
    } else {
      next_batch_cnt_ = token_iters_.count();
      for (int64_t i = 0; OB_SUCC(ret) && i < token_iters_.count(); ++i) {
        ObDASTextRetrievalIter *iter = token_iters_.at(i);
        if (OB_FAIL(iter->rescan())) {
          LOG_WARN("failed to append token iter to array", K(ret));
        } else {
          next_batch_iter_idxes_[i] = i;
        }
      }
    }
  }
  return ret;
}

int ObDASTRDaatLookupIter::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(1 != rangekey_size_)) { // if rangekey_size_ > 1, UNSUPPORTED
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected rangekey size", K(ret), K_(rangekey_size));
  } else if (next_written_idx_ >= rangekey_size_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(check_and_prepare())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to prepare to get next rows", K(ret));
    } else {
      ObEvalCtx *ctx = ir_rtdef_->eval_ctx_;
      ObEvalCtx::BatchInfoScopeGuard guard(*ctx);
      guard.set_batch_idx(0);
      ObDocId default_doc_id;
      if (OB_FAIL(project_result(default_doc_id, 0))) {
        LOG_WARN("failed to project result", K(ret));
      } else {
        next_written_idx_++;
      }
    }
  } else if (next_written_idx_ == rangekey_size_) {
    ret = OB_ITER_END;
  } else if (next_written_idx_ > rangekey_size_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected capacity size", K(ret));
  } else if (next_written_idx_ == 0) {
    clear_evaluated_infos();
    int capacity = 1;
    if (OB_FAIL(pull_next_batch_rows())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to pull next batch rows from iterator", K(ret));
      } else if (OB_FAIL(project_result(cache_doc_ids_[0], 0))) {
        LOG_WARN("failed to project result", K(ret));
      } else {
        next_written_idx_ ++;
      }
    } else if (OB_FAIL(next_disjunctive_document(capacity))) {
      LOG_WARN("failed to get next document with disjunctive tokens", K(ret));
    } else {
      next_written_idx_ ++;
    }
  }
  return ret;
}

int ObDASTRDaatLookupIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  count = 0;
  if (OB_UNLIKELY(capacity == 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected capacity size", K(ret), K(capacity));
  } else if (OB_FAIL(check_and_prepare())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to prepare to get next rows", K(ret));
    } else if (next_written_idx_ == rangekey_size_) {
      // do nothing
    } else if (next_written_idx_ != 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected next_written_idx", K(ret), K_(next_written_idx));
    } else {
      ObEvalCtx *ctx = ir_rtdef_->eval_ctx_;
      ObExpr *relevance_proj_col = ir_ctdef_->relevance_proj_col_;
      ret = OB_SUCCESS;
      while (OB_SUCC(ret) && next_written_idx_ < rangekey_size_) {
        // fill the remaining results with the relevance value of '0'
        // Note: if we need calculate the docid expr, fix the code and cache the hints.
        ObDocId default_docid;
        ObEvalCtx::BatchInfoScopeGuard guard(*ctx);
        guard.set_batch_idx(next_written_idx_);
        if (OB_FAIL(project_result(default_docid, 0))) {
          LOG_WARN("failed to project result", K(ret));
        }
        next_written_idx_ ++;
      }
      if (OB_SUCC(ret)) {
        for (int i = 0; i < rangekey_size_; i++) {
          relevance_proj_col->get_evaluated_flags(*ctx).set(i);
        }
        ret = OB_ITER_END;
      }
      relevance_proj_col->set_evaluated_projected(*ctx);
      count = OB_MIN(rangekey_size_, capacity);
      next_written_idx_ = OB_MIN(rangekey_size_, capacity);
    }
  } else if (next_written_idx_ == rangekey_size_) {
    ret = OB_ITER_END;
  } else if (next_written_idx_ > rangekey_size_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected capacity size", K(ret), K(capacity));
  } else if (next_written_idx_ == 0) {
    // for normal case
    ObExpr *match_filter = ir_ctdef_->need_calc_relevance() ? ir_ctdef_->match_filter_ : nullptr;
    const bool is_batch = true;
    next_written_idx_ = 0;
    bool filter_valid = false;
    // fill the all result in the range of rangkey_size_
    while (OB_SUCC(ret) && next_written_idx_ < rangekey_size_) {
      if (OB_FAIL(pull_next_batch_rows_with_batch_mode())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to pull next batch rows from iterator", K(ret));
        }
      } else if (OB_FAIL(next_disjunctive_document(capacity))) {
        LOG_WARN("failed to get next document with disjunctive tokens", K(ret));
      } else {
        next_written_idx_ ++;
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    // fill the remaining results with the relevance value of '0'
    if (OB_LIKELY(next_written_idx_ < rangekey_size_)) {
      ObEvalCtx *ctx = ir_rtdef_->eval_ctx_;
      while (next_written_idx_ < rangekey_size_ && OB_SUCC(ret)) {
        int64_t pos = hints_[next_written_idx_];
        if (pos < capacity) {
          ObEvalCtx::BatchInfoScopeGuard guard(*ctx);
          guard.set_batch_idx(pos);
          if (OB_FAIL(project_result(cache_doc_ids_[next_written_idx_], 0))) {
            LOG_WARN("failed to project result", K(ret));
          }
        }
        next_written_idx_++;
      }
    }
    // output the part result in the range of capacity
    if (OB_SUCC(ret)) {
      ObEvalCtx *ctx = ir_rtdef_->eval_ctx_;
      ObExpr *relevance_proj_col = ir_ctdef_->relevance_proj_col_;
      for (int i = 0; i < OB_MIN(rangekey_size_, capacity); i++) {
        relevance_proj_col->get_evaluated_flags(*ctx).set(i);
      }
      relevance_proj_col->set_evaluated_projected(*ctx);
      next_written_idx_ = OB_MIN(rangekey_size_, capacity);
      count = OB_MIN(rangekey_size_, capacity);
    }
  } else if (next_written_idx_ < rangekey_size_) {
    int remain_size = rangekey_size_ - next_written_idx_;
    int return_size = OB_MIN(remain_size, capacity);
    // next_written_idx_ is the output idx
    ObEvalCtx *ctx = ir_rtdef_->eval_ctx_;
    while (count < return_size && OB_SUCC(ret)) {
      int64_t pos = reverse_hints_[next_written_idx_];
      ObEvalCtx::BatchInfoScopeGuard guard(*ctx);
      guard.set_batch_idx(count);
      if (OB_FAIL(project_result(cache_doc_ids_[pos], cache_relevances_[next_written_idx_]))) {
        LOG_WARN("failed to project result", K(ret));
      }
      next_written_idx_++;
      count ++;
    }
    if (OB_SUCC(ret) && next_written_idx_ == rangekey_size_) {
      ret = OB_ITER_END;
    }
  }

  return ret;
}

int ObDASTRDaatLookupIter::next_disjunctive_document(const int capacity)
{
  int ret = OB_SUCCESS;
  const ObIRIterLoserTreeItem *top_item = nullptr;
  ObEvalCtx *ctx = ir_rtdef_->eval_ctx_;
  if (!iter_row_heap_->empty()) {
    if (OB_FAIL(iter_row_heap_->top(top_item))) {
      LOG_WARN("failed to get top item from heap", K(ret));
    } else if (cache_doc_ids_[next_written_idx_] != top_item->doc_id_) {
      // fill some unexit results with the relevance value of '0'
      // when rangekey_size_ is 1, we shouldn't use the path
      int64_t pos = hints_[next_written_idx_];
      if (OB_UNLIKELY(next_written_idx_ == 0 && rangekey_size_ == 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected case", K(ret));
      } else if (pos < capacity) {
        ObEvalCtx::BatchInfoScopeGuard guard(*ctx);
        guard.set_batch_idx(pos);
        if (OB_FAIL(project_result(cache_doc_ids_[next_written_idx_], 0))) {
          LOG_WARN("failed to project result", K(ret));
        }
      } else {
        // cache it
        cache_relevances_[pos] = 0;
      }
    } else {
      int64_t doc_cnt = 0;
      bool curr_doc_end = false;
      // Do we need to use ObExpr to collect relevance?
      double cur_doc_relevance = 0.0;
      if (BOOLEAN == relation_type_) {
        for (int i = 0; OB_SUCC(ret) && i < query_tokens_.count(); ++i) {
          boolean_relevances_[i] = 0;
        }
      }
      while (OB_SUCC(ret) && !iter_row_heap_->empty() && !curr_doc_end) {
        if (iter_row_heap_->is_unique_champion()) {
          curr_doc_end = true;
        }
        if (OB_FAIL(iter_row_heap_->top(top_item))) {
          LOG_WARN("failed to get top item from heap", K(ret));
        } else {
          next_batch_iter_idxes_[next_batch_cnt_++] = top_item->iter_idx_;
          if (BOOLEAN == relation_type_) {
            boolean_relevances_[top_item->iter_idx_] = top_item->relevance_;
          } else {
            // consider to add an expr for collectiong conjunction result between query tokens here?
            cur_doc_relevance += top_item->relevance_;
          }
          if (OB_FAIL(iter_row_heap_->pop())) {
            LOG_WARN("failed to pop top item in heap", K(ret));
          }
        }
      }

      if (OB_FAIL(ret)) {
        //do nothing
      } else if (BOOLEAN != relation_type_) {
        //do nothing
      } else if (OB_FAIL(ObFtsEvalNode::fts_boolean_eval(root_node_, boolean_relevances_, cur_doc_relevance))) {
        LOG_WARN("failed to evaluate boolean relevance", K(ret));
      } else {
        cur_doc_relevance = OB_MAX(cur_doc_relevance, 0);
      }

      if (OB_SUCC(ret)) {
        const double relevance_score = ir_ctdef_->need_calc_relevance() ? cur_doc_relevance : 0;
        int64_t pos = hints_[next_written_idx_];
        if (pos < capacity) {
          ObEvalCtx::BatchInfoScopeGuard guard(*ctx);
          guard.set_batch_idx(pos);
          if (OB_FAIL(project_result(top_item->doc_id_, relevance_score))) {
            LOG_WARN("failed to project result", K(ret));
          }
        } else {
          // cache it
          cache_relevances_[pos] = relevance_score;
        }
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter_row_heap_ is empty", K(ret));
  }
  return ret;
}
} // namespace sql
} // namespace oceanbase
