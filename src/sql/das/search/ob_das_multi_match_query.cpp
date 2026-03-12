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

#include "ob_das_multi_match_query.h"
#include "ob_das_token_op.h"
#include "ob_das_disjunction_op.h"
#include "ob_das_conjunction_op.h"
#include "ob_das_bmw_op.h"
#include "ob_das_bmm_op.h"
#include "ob_das_dummy_op.h"
#include "storage/fts/ob_fts_tokenizer.h"

namespace oceanbase {

namespace sql {

OB_SERIALIZE_MEMBER((ObDASMultiMatchCtDef, ObIDASSearchCtDef),
                    query_text_,
                    boost_,
                    match_operator_,
                    minimum_should_match_,
                    type_,
                    field_boosts_,
                    ir_ctdef_indices_);

OB_SERIALIZE_MEMBER((ObDASMultiMatchRtDef, ObIDASSearchRtDef));

int ObDASMultiMatchRtDef::init_block_max_params(
    const ObDASMultiMatchCtDef &ctdef,
    const int64_t field_cnt)
{
  int ret = OB_SUCCESS;
  if (block_max_params_initialized_) {
    // skip
  } else if (OB_FAIL(block_max_params_.init(field_cnt))) {
    LOG_WARN("failed to init array of block max params", K(ret), K(field_cnt));
  } else if (OB_FAIL(block_max_params_.prepare_allocate(field_cnt))) {
    LOG_WARN("failed to prepare allocate array of block max params", K(ret), K(field_cnt));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < field_cnt; ++i) {
      ObDASIRScanCtDef *ir_ctdef = ctdef.get_ir_ctdef(i);
      if (OB_ISNULL(ir_ctdef)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", K(ret), K(i));
      } else if (OB_FAIL(block_max_params_[i].init(*ir_ctdef, allocator_))) {
        LOG_WARN("failed to init block max param", K(ret), K(i));
      }
    }
  }
  if (OB_SUCC(ret)) {
    block_max_params_initialized_ = true;
  } else {
    block_max_params_.reset();
  }
  return ret;
}

int ObDASMultiMatchRtDef::can_pushdown_filter_to_bmm(bool &can_pushdown)
{
  int ret = OB_SUCCESS;
  can_pushdown = false;
  const ObDASMultiMatchCtDef *ctdef = static_cast<const ObDASMultiMatchCtDef *>(ctdef_);
  ObDatum *match_operator_datum = nullptr;
  ObDatum *min_should_match_datum = nullptr;
  ObDatum *type_datum = nullptr;
  if (OB_ISNULL(ctdef) || OB_ISNULL(eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", KR(ret), KP(ctdef), KP_(eval_ctx));
  } else if (ctdef->ir_ctdef_indices_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected no ir ctdef", KR(ret));
  } else if (OB_FAIL(ctdef->match_operator_->eval(*eval_ctx_, match_operator_datum))) {
    LOG_WARN("failed to eval match_operator", KR(ret));
  } else if (OB_FAIL(ctdef->minimum_should_match_->eval(*eval_ctx_, min_should_match_datum))) {
    LOG_WARN("failed to eval minimum_should_match", KR(ret));
  } else if (OB_FAIL(ctdef->type_->eval(*eval_ctx_, type_datum))) {
    LOG_WARN("failed to eval type", KR(ret));
  } else if (OB_UNLIKELY(match_operator_datum->is_null()
      || min_should_match_datum->is_null()
      || type_datum->is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null datum", KR(ret),
        KPC(match_operator_datum), KPC(min_should_match_datum), KPC(type_datum));
  } else {
    const int64_t field_cnt = ctdef->ir_ctdef_indices_.count();
    const int64_t type_val = type_datum->get_int();
    const bool is_supported_type =
        (ObMatchFieldsType::MATCH_BEST_FIELDS == type_val
        || ObMatchFieldsType::MATCH_MOST_FIELDS == type_val) && 1 == field_cnt;
    can_pushdown = ObMatchOperator::MATCH_OPERATOR_OR == match_operator_datum->get_int()
        && min_should_match_datum->get_int() <= 1
        && ctdef->is_top_level_scoring()
        && is_supported_type;
  }
  return ret;
}

/*
                NOT top-level scoring      top-level scoring

  BEST_FIELDS:      disj max op               disj max op
                     /   |   \                 /   |   \
                 disj op / conj op     bmm op / bmw op / conj op

  MOST_FIELDS:        disj op                   bmm op
                     /   |   \                 /   |   \
                 disj op / conj op         disj op / conj op
 */

int ObDASMultiMatchRtDef::generate_op(
    ObDASSearchCost lead_cost,
    ObDASSearchCtx &search_ctx,
    ObIDASSearchOp *&op)
{
  int ret = OB_SUCCESS;
  const ObDASMultiMatchCtDef *ctdef = static_cast<const ObDASMultiMatchCtDef *>(ctdef_);
  ObDatum *query_text_datum = nullptr;
  ObSEArray<ObString, 16> query_tokens;
  const bool has_pushdown_filter = nullptr != pushdown_filter_op_;
  if (OB_ISNULL(ctdef) || OB_ISNULL(eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", KR(ret), KP(ctdef), KP_(eval_ctx));
  } else if (ctdef->ir_ctdef_indices_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected no ir ctdef", KR(ret));
  } else if (OB_FAIL(ctdef->query_text_->eval(*eval_ctx_, query_text_datum))) {
    LOG_WARN("failed to eval query text", KR(ret));
  } else if (OB_UNLIKELY(query_text_datum->is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null param", KR(ret), KPC(query_text_datum));
  } else if (OB_ISNULL(ctdef->get_ir_ctdef(0))
      || OB_ISNULL(ctdef->get_ir_ctdef(0)->get_inv_idx_scan_scalar_ctdef())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret));
  } else if (OB_FAIL(ObFTSTokenizer::tokenize(
      allocator_,
      query_text_datum->get_string(),
      ctdef->get_ir_ctdef(0)->get_inv_idx_scan_scalar_ctdef()->table_param_.get_parser_name(),
      ctdef->get_ir_ctdef(0)->get_inv_idx_scan_scalar_ctdef()->table_param_.get_parser_property(),
      ctdef->query_text_->obj_meta_,
      query_tokens))) {
    LOG_WARN("failed to tokenize query text", K(ret));
  }

  ObDatum *boost_datum = nullptr;
  ObDatum *match_operator_datum = nullptr;
  ObDatum *min_should_match_datum = nullptr;
  ObDatum *type_datum = nullptr;

  if (OB_FAIL(ret)) {
  } else if (query_tokens.empty()) {
    ObDASDummyOp *dummy_op = nullptr;
    ObDASDummyOpParam dummy_op_param;
    if (OB_FAIL(search_ctx.create_op(dummy_op_param, dummy_op))) {
      LOG_WARN("failed to create dummy op", KR(ret));
    } else {
      op = dummy_op;
    }
  } else {
    const int64_t field_cnt = ctdef->ir_ctdef_indices_.count();
    const int64_t token_cnt = query_tokens.count();
    int64_t min_should_match = 0;
    if (OB_FAIL(ctdef->boost_->eval(*eval_ctx_, boost_datum))) {
      LOG_WARN("failed to eval boost", KR(ret));
    } else if (OB_FAIL(ctdef->match_operator_->eval(*eval_ctx_, match_operator_datum))) {
      LOG_WARN("failed to eval match_operator", KR(ret));
    } else if (OB_FAIL(ctdef->minimum_should_match_->eval(*eval_ctx_, min_should_match_datum))) {
      LOG_WARN("failed to eval minimum_should_match", KR(ret));
    } else if (OB_FAIL(ctdef->type_->eval(*eval_ctx_, type_datum))) {
      LOG_WARN("failed to eval type", KR(ret));
    } else if (OB_UNLIKELY(match_operator_datum->is_null()
        || min_should_match_datum->is_null() || type_datum->is_null())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null param", KR(ret), KPC(query_text_datum), KPC(match_operator_datum),
              KPC(min_should_match_datum), KPC(type_datum));
    } else {
      min_should_match = MIN(MAX(min_should_match_datum->get_int(), 1), token_cnt);
    }

    ObSEArray<ObIDASSearchOp *, 8> field_ops;
    ObSEArray<ObIDASSearchOp *, 8> token_ops;
    if (FAILEDx(field_ops.reserve(field_cnt))) {
      LOG_WARN("failed to reserve for field ops", K(ret));
    } else if (OB_FAIL(token_ops.reserve(token_cnt))) {
      LOG_WARN("failed to reserve for token ops", K(ret), K(token_cnt));
    } else if (OB_FAIL(init_block_max_params(*ctdef, field_cnt))) {
      LOG_WARN("failed to init block max params", K(ret), KPC(ctdef), K(field_cnt));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < field_cnt; ++i) {
      ObDASIRScanCtDef *ir_ctdef = ctdef->get_ir_ctdef(i);
      ObDASIRScanRtDef *ir_rtdef = get_ir_rtdef(i);
      ObDatum *field_boost_datum = nullptr;
      if (OB_ISNULL(ir_ctdef) || OB_ISNULL(ir_rtdef) || OB_ISNULL(ir_rtdef->eval_ctx_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", K(ret), K(i));
      } else if (OB_FAIL(ctdef->field_boosts_.at(i)->eval(*eval_ctx_, field_boost_datum))) {
        LOG_WARN("failed to eval field boost", K(ret), K(i));
      } else if (field_boost_datum->is_null()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", KR(ret), K(i));
      }
      token_ops.reuse();
      for (int64_t j = 0; OB_SUCC(ret) && j < token_cnt; ++j) {
        ObDASTokenOp *token_op = nullptr;
        ObDASTokenOpParam token_op_param;
        token_op_param.ir_ctdef_ = ir_ctdef;
        token_op_param.ir_rtdef_ = ir_rtdef;
        token_op_param.block_max_param_ = &block_max_params_[i];
        token_op_param.token_boost_ = field_boost_datum->get_double() * boost_datum->get_double();
        token_op_param.query_token_ = query_tokens.at(j);
        token_op_param.use_rich_format_ = true;
        if (OB_FAIL(search_ctx.create_op(token_op_param, token_op))) {
          LOG_WARN("failed to create token op", KR(ret));
        } else {
          if (OB_FAIL(token_ops.push_back(token_op))) {
            LOG_WARN("failed to append token op", KR(ret));
          }
        }
      }

      if (OB_FAIL(ret)) {
      } else if (1 == token_cnt && !has_pushdown_filter) {
        if (OB_FAIL(field_ops.push_back(token_ops.at(0)))) {
          LOG_WARN("failed to append the only token op to field ops", K(ret));
        }
      } else if (ObMatchOperator::MATCH_OPERATOR_AND == match_operator_datum->get_int()
          || (ObMatchOperator::MATCH_OPERATOR_OR == match_operator_datum->get_int()
            && min_should_match >= token_cnt
            && !has_pushdown_filter)) {
        ObDASConjunctionOp *conj_op = nullptr;
        ObDASConjunctionOpParam conj_op_param(token_ops);
        if (OB_FAIL(search_ctx.create_op(conj_op_param, conj_op))) {
          LOG_WARN("failed to create conjunction op", KR(ret));
        } else if (OB_FAIL(field_ops.push_back(conj_op))) {
          LOG_WARN("failed to append conjunction op to field ops", K(ret));
        }
      } else if (ObMatchOperator::MATCH_OPERATOR_OR == match_operator_datum->get_int()) {
        if (!ctdef->is_top_level_scoring()
            || (field_cnt > 1 && ObMatchFieldsType::MATCH_MOST_FIELDS == type_datum->get_int())) {
          ObDASDisjunctionOp *disj_op = nullptr;
          ObDASDisjunctionOpParam disj_op_param(token_ops, min_should_match, false, lead_cost);
          if (OB_FAIL(search_ctx.create_op(disj_op_param, disj_op))) {
            LOG_WARN("failed to create disjunction op", KR(ret));
          } else if (OB_FAIL(field_ops.push_back(disj_op))) {
            LOG_WARN("failed to append disjunction op to field ops", K(ret));
          }
        } else if (min_should_match > 1) {
          ObDASBMWOp *bmw_op = nullptr;
          ObDASBMWOpParam bmw_op_param(token_ops, min_should_match, allocator_);
          if (OB_FAIL(search_ctx.create_op(bmw_op_param, bmw_op))) {
            LOG_WARN("failed to create bmw op", KR(ret));
          } else if (OB_FAIL(field_ops.push_back(bmw_op))) {
            LOG_WARN("failed to append bmw op to field ops", K(ret));
          }
        } else {
          ObDASBMMOp *bmm_op = nullptr;
          ObDASBMMOpParam bmm_op_param(
              token_ops, query_optional_, pushdown_filter_op_, allocator_);
          if (OB_FAIL(search_ctx.create_op(bmm_op_param, bmm_op))) {
            LOG_WARN("failed to create bmm op", KR(ret));
          } else if (OB_FAIL(field_ops.push_back(bmm_op))) {
            LOG_WARN("failed to append bmm op to field ops", K(ret));
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected match operator", K(ret), K(match_operator_datum->get_int()));
      }
    }

    if (OB_SUCC(ret)) {
      if (1 == field_ops.count()) {
        op = field_ops.at(0);
      } else if (ObMatchFieldsType::MATCH_BEST_FIELDS == type_datum->get_int()) {
        ObDASDisjunctionOp *disj_op = nullptr;
        ObDASDisjunctionOpParam disj_op_param(field_ops, 1, true, lead_cost);
        if (OB_FAIL(search_ctx.create_op(disj_op_param, disj_op))) {
          LOG_WARN("failed to create disjunction op", KR(ret));
        } else {
          op = disj_op;
        }
      } else if (ObMatchFieldsType::MATCH_MOST_FIELDS == type_datum->get_int()) {
        if (!ctdef->is_top_level_scoring()) {
          ObDASDisjunctionOp *disj_op = nullptr;
          ObDASDisjunctionOpParam disj_op_param(field_ops, 1, false, lead_cost);
          if (OB_FAIL(search_ctx.create_op(disj_op_param, disj_op))) {
            LOG_WARN("failed to create disjunction op", KR(ret));
          } else {
            op = disj_op;
          }
        } else {
          ObDASBMMOp *bmm_op = nullptr;
          ObDASBMMOpParam bmm_op_param(
              field_ops, query_optional_, pushdown_filter_op_, allocator_);
          if (OB_FAIL(search_ctx.create_op(bmm_op_param, bmm_op))) {
            LOG_WARN("failed to create bmm op", KR(ret));
          } else {
            op = bmm_op;
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected match fields type", K(ret), K(type_datum->get_int()));
      }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
