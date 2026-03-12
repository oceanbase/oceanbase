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
#include "ob_das_match_query.h"
#include "ob_das_bmw_op.h"
#include "ob_das_bmm_op.h"
#include "ob_das_dummy_op.h"
#include "ob_das_token_op.h"
#include "ob_das_disjunction_op.h"
#include "ob_das_conjunction_op.h"
#include "ob_das_scalar_define.h"
#include "storage/fts/ob_fts_tokenizer.h"


namespace oceanbase
{

namespace sql
{

OB_SERIALIZE_MEMBER((ObDASMatchCtDef, ObIDASSearchCtDef),
                    query_text_,
                    boost_,
                    match_operator_,
                    minimum_should_match_,
                    ir_ctdef_idx_,
                    pushdown_must_filter_op_ctdef_idx_);

OB_SERIALIZE_MEMBER((ObDASMatchRtDef, ObIDASSearchRtDef));

int ObDASMatchRtDef::can_pushdown_filter_to_bmm(bool &can_pushdown)
{
  int ret = OB_SUCCESS;
  can_pushdown = false;
  const ObDASMatchCtDef *ctdef = static_cast<const ObDASMatchCtDef *>(ctdef_);
  ObDatum *minimum_should_match_datum = nullptr;
  ObDatum *match_operator_datum = nullptr;
  if (OB_ISNULL(ctdef) || OB_ISNULL(eval_ctx_) || OB_ISNULL(ctdef->get_ir_ctdef())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", KR(ret), KP(ctdef), KP_(eval_ctx));
  } else if (OB_NOT_NULL(ctdef->get_pushdown_must_filter_op_ctdef())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("pushdown must filter op ctdef is not supported", KR(ret));
  } else if (OB_FAIL(ctdef->minimum_should_match_->eval(*eval_ctx_, minimum_should_match_datum))) {
    LOG_WARN("expr evaluation failed", KR(ret));
  } else if (OB_FAIL(ctdef->match_operator_->eval(*eval_ctx_, match_operator_datum))) {
    LOG_WARN("expr evaluation failed", KR(ret));
  } else if (OB_UNLIKELY(minimum_should_match_datum->is_null() || match_operator_datum->is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null datum", KR(ret),
        KPC(minimum_should_match_datum), KPC(match_operator_datum));
  } else {
    can_pushdown = ctdef->is_top_level_scoring()
        && ObMatchOperator::MATCH_OPERATOR_AND != match_operator_datum->get_int()
        && minimum_should_match_datum->get_int() <= 1;
  }
  return ret;
}

int ObDASMatchRtDef::generate_op(ObDASSearchCost lead_cost, ObDASSearchCtx &search_ctx, ObIDASSearchOp *&op)
{
  int ret = OB_SUCCESS;
  const ObDASMatchCtDef *ctdef = static_cast<const ObDASMatchCtDef *>(ctdef_);
  ObDatum *search_text_datum = nullptr;
  ObDatum *minimum_should_match_datum = nullptr;
  ObSEArray<ObString, 16> query_tokens;
  if (OB_ISNULL(ctdef) || OB_ISNULL(eval_ctx_) || OB_ISNULL(ctdef->get_ir_ctdef())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", KR(ret), KP(ctdef), KP_(eval_ctx));
  } else if (OB_NOT_NULL(ctdef->get_pushdown_must_filter_op_ctdef())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("pushdown must filter op ctdef is not supported", KR(ret));
  } else if (OB_FAIL(ctdef->query_text_->eval(*eval_ctx_, search_text_datum))) {
    LOG_WARN("expr evaluation failed", KR(ret));
  } else if (OB_FAIL(ctdef->minimum_should_match_->eval(*eval_ctx_, minimum_should_match_datum))) {
    LOG_WARN("expr evaluation failed", KR(ret));
  } else if (OB_UNLIKELY(search_text_datum->is_null() || minimum_should_match_datum->is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null datum", KR(ret), KPC(search_text_datum), KPC(minimum_should_match_datum));
  } else if (OB_FAIL(ObFTSTokenizer::tokenize(
      allocator_,
      search_text_datum->get_string(),
      ctdef->get_ir_ctdef()->get_inv_idx_scan_scalar_ctdef()->table_param_.get_parser_name(),
      ctdef->get_ir_ctdef()->get_inv_idx_scan_scalar_ctdef()->table_param_.get_parser_property(),
      ctdef->query_text_->obj_meta_,
      query_tokens))) {
    LOG_WARN("failed to tokenize query text", KR(ret), K(search_text_datum));
  }

  if (OB_FAIL(ret)) {
    // skip
  } else if (query_tokens.count() == 0) {
    // empty query tokens
    ObDASDummyOp *dummy_op = nullptr;
    ObDASDummyOpParam dummy_op_param;
    if (OB_FAIL(search_ctx.create_op(dummy_op_param, dummy_op))) {
      LOG_WARN("failed to create dummy op", KR(ret));
    } else {
      op = dummy_op;
    }
  } else {
    ObDatum *boost_datum = nullptr;
    ObDatum *match_operator_datum = nullptr;
    ObSEArray<ObIDASSearchOp *, 16> token_ops;
    if (OB_FAIL(ctdef->boost_->eval(*eval_ctx_, boost_datum))) {
      LOG_WARN("expr evaluation failed", KR(ret));
    } else if (OB_FAIL(ctdef->match_operator_->eval(*eval_ctx_, match_operator_datum))) {
      LOG_WARN("expr evaluation failed", KR(ret));
    } else if (OB_UNLIKELY(boost_datum->is_null() || match_operator_datum->is_null())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null param datum", KR(ret), KPC(boost_datum), KPC(match_operator_datum));
    } else if (OB_FAIL(token_ops.reserve(query_tokens.count()))) {
      LOG_WARN("failed to reserve token ops", KR(ret), K(query_tokens.count()));
    } else if (block_max_param_initialized_) {
    } else if (OB_FAIL(block_max_param_.init(*ctdef->get_ir_ctdef(), allocator_))) {
      LOG_WARN("failed to init block max param", KR(ret));
    } else {
      block_max_param_initialized_ = true;
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < query_tokens.count(); ++i) {
      const ObString &query_token = query_tokens.at(i);
      ObDASTokenOp *token_op = nullptr;
      ObDASTokenOpParam token_op_param;
      token_op_param.ir_ctdef_ = ctdef->get_ir_ctdef();
      token_op_param.ir_rtdef_ = get_ir_rtdef();
      token_op_param.block_max_param_ = &block_max_param_;
      token_op_param.token_boost_ = boost_datum->get_double();
      token_op_param.query_token_ = query_token;
      token_op_param.use_rich_format_ = true;
      if (OB_FAIL(search_ctx.create_op(token_op_param, token_op))) {
        LOG_WARN("failed to create token op", KR(ret));
      } else if (OB_FAIL(token_ops.push_back(token_op))) {
        LOG_WARN("failed to push back token op", KR(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (ObMatchOperator::MATCH_OPERATOR_AND == match_operator_datum->get_int()) {
      ObDASConjunctionOp *conjunction_op = nullptr;
      ObDASConjunctionOpParam conjunction_op_param(token_ops);
      if (OB_FAIL(search_ctx.create_op(conjunction_op_param, conjunction_op))) {
        LOG_WARN("failed to create conjunction op", KR(ret));
      } else {
        op = conjunction_op;
      }
    } else {
      int64_t minimum_should_match = minimum_should_match_datum->get_int() > query_tokens.count()
          ? query_tokens.count() : minimum_should_match_datum->get_int();
      if (get_ctdef()->is_top_level_scoring()) {
        if (minimum_should_match > 1) {
          ObDASBMWOp *bmw_op = nullptr;
          ObDASBMWOpParam bmw_op_param(token_ops, minimum_should_match, allocator_);
          if (OB_FAIL(search_ctx.create_op(bmw_op_param, bmw_op))) {
            LOG_WARN("failed to create bmw op", KR(ret));
          } else {
            op = bmw_op;
          }
        } else {
          ObDASBMMOp *bmm_op = nullptr;
          ObDASBMMOpParam bmm_op_param(
              token_ops, query_optional_, pushdown_filter_op_, allocator_);
          if (OB_FAIL(search_ctx.create_op(bmm_op_param, bmm_op))) {
            LOG_WARN("failed to create bmm op", KR(ret));
          } else {
            op = bmm_op;
          }
        }
      } else {
        ObDASDisjunctionOp *disjunction_op = nullptr;
        ObDASDisjunctionOpParam disjunction_op_param(
            token_ops,
            minimum_should_match,
            false,
            lead_cost);
        if (OB_FAIL(search_ctx.create_op(disjunction_op_param, disjunction_op))) {
          LOG_WARN("failed to create disjunction op", KR(ret));
        } else {
          op = disjunction_op;
        }
      }
    }
  }
  return ret;
}

}
}
