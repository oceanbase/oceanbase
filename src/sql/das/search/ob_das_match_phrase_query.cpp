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
#include "ob_das_match_phrase_query.h"
#include "ob_das_match_phrase_op.h"
#include "ob_das_dummy_op.h"
#include "ob_das_token_op.h"
#include "storage/retrieval/ob_phrase_match_counter.h"
#include "share/ob_fts_index_builder_util.h"
#include "storage/fts/ob_fts_tokenizer.h"

namespace oceanbase
{
namespace sql
{

OB_SERIALIZE_MEMBER((ObDASMatchPhraseCtDef, ObIDASSearchCtDef),
                    query_text_,
                    boost_,
                    slop_,
                    ir_ctdef_idx_);

OB_SERIALIZE_MEMBER((ObDASMatchPhraseRtDef, ObIDASSearchRtDef));

int ObDASMatchPhraseRtDef::generate_op(
    ObDASSearchCost lead_cost,
    ObDASSearchCtx &search_ctx,
    ObIDASSearchOp *&op)
{
  int ret = OB_SUCCESS;
  const ObDASMatchPhraseCtDef *ctdef = static_cast<const ObDASMatchPhraseCtDef *>(ctdef_);
  ObDASMatchPhraseOpParam param;
  ObDatum *query_text_datum = nullptr;
  ObDatum *boost_datum = nullptr;
  ObDatum *slop_datum = nullptr;
  ObSEArray<int64_t, 16> token_offsets;
  bool has_duplicate_tokens = false;
  if (OB_ISNULL(ctdef) || OB_ISNULL(eval_ctx_) || OB_ISNULL(ctdef->get_ir_ctdef())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(ctdef), KP_(eval_ctx), KP(ctdef->get_ir_ctdef()));
  } else if (OB_FAIL(ctdef->query_text_->eval(*eval_ctx_, query_text_datum))) {
    LOG_WARN("failed to eval query text", K(ret));
  } else if (OB_UNLIKELY(query_text_datum->is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null datum", K(ret), KPC(query_text_datum));
  } else if (OB_FAIL(ObFTSTokenizer::tokenize(
      allocator_,
      query_text_datum->get_string(),
      ctdef->get_ir_ctdef()->get_inv_idx_scan_scalar_ctdef()->table_param_.get_parser_name(),
      ctdef->get_ir_ctdef()->get_inv_idx_scan_scalar_ctdef()->table_param_.get_parser_property(),
      ctdef->query_text_->obj_meta_,
      param.query_tokens_, param.token_ids_, token_offsets, has_duplicate_tokens))) {
    LOG_WARN("failed to tokenize query text", K(ret));
  } else if (param.query_tokens_.empty()) {
    ObDASDummyOp *dummy_op = nullptr;
    ObDASDummyOpParam dummy_op_param;
    if (OB_FAIL(search_ctx.create_op(dummy_op_param, dummy_op))) {
      LOG_WARN("failed to create dummy op", K(ret));
    } else {
      op = dummy_op;
    }
  } else if (OB_ISNULL(ctdef->get_ir_ctdef()->inv_scan_pos_list_col_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("missing pos list column in index", K(ret));
  } else if (OB_FAIL(ctdef->boost_->eval(*eval_ctx_, boost_datum))) {
    LOG_WARN("failed to eval boost", K(ret));
  } else if (OB_FAIL(ctdef->slop_->eval(*eval_ctx_, slop_datum))) {
    LOG_WARN("failed to eval slop", K(ret));
  } else if (OB_UNLIKELY(boost_datum->is_null() || slop_datum->is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null datum", K(ret), KPC(boost_datum), KPC(slop_datum));
  } else if (block_max_param_initialized_) {
    // skip
  } else if (OB_FAIL(block_max_param_.init(*ctdef->get_ir_ctdef(), allocator_))) {
    LOG_WARN("failed to init block max param", KR(ret));
  } else {
    block_max_param_initialized_ = true;
  }

  if (OB_FAIL(ret) || param.query_tokens_.empty()) {
  } else if (1 == param.query_tokens_.count() && !has_duplicate_tokens) {
    ObDASTokenOp *token_op = nullptr;
    ObDASTokenOpParam token_op_param;
    token_op_param.ir_ctdef_ = ctdef->get_ir_ctdef();
    token_op_param.ir_rtdef_ = get_ir_rtdef();
    token_op_param.block_max_param_ = &block_max_param_;
    token_op_param.token_boost_ = boost_datum->get_double();
    token_op_param.query_token_ = param.query_tokens_.at(0);
    token_op_param.use_rich_format_ = true;
    if (OB_FAIL(search_ctx.create_op(token_op_param, token_op))) {
      LOG_WARN("failed to create token op", KR(ret));
    } else {
      op = token_op;
    }
  } else if (OB_FAIL(ObPhraseMatchCounter::create(
      allocator_, param.token_ids_, token_offsets,
      slop_datum->get_int(), has_duplicate_tokens, param.counter_))) {
    LOG_WARN("failed to create phrase match counter", K(ret));
  } else if (OB_UNLIKELY((param.boost_ = boost_datum->get_double()) <= 0.0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected non-positive boost", K(ret), K_(param.boost));
  } else {
    param.allocator_ = &allocator_;
    param.ir_ctdef_ = ctdef->get_ir_ctdef();
    param.ir_rtdef_ = get_ir_rtdef();
    param.block_max_param_ = &block_max_param_;
    param.use_rich_format_ = true;
    ObDASMatchPhraseOp *match_phrase_op = nullptr;
    if (OB_FAIL(search_ctx.create_op(param, match_phrase_op))) {
      LOG_WARN("failed to create match phrase op", K(ret));
    } else {
      op = match_phrase_op;
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
