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

#include "ob_das_query_string_query.h"
#include "ob_das_token_op.h"
#include "ob_das_disjunction_op.h"
#include "ob_das_conjunction_op.h"
#include "ob_das_bmw_op.h"
#include "ob_das_bmm_op.h"
#include "ob_das_dummy_op.h"
#include "storage/fts/ob_fts_tokenizer.h"

namespace oceanbase {

namespace sql {

OB_SERIALIZE_MEMBER((ObDASQueryStringCtDef, ObIDASSearchCtDef),
                    query_text_,
                    boost_,
                    default_operator_,
                    minimum_should_match_,
                    type_,
                    field_boosts_,
                    ir_ctdef_indices_);

OB_SERIALIZE_MEMBER((ObDASQueryStringRtDef, ObIDASSearchRtDef));

int ObDASQueryStringRtDef::init_block_max_params(
    const ObDASQueryStringCtDef &ctdef,
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

int ObDASQueryStringRtDef::parse_query_string(
    ObIArray<ObArray<ObString>> &token_groups,
    ObIArray<double> &token_boosts)
{
  int ret = OB_SUCCESS;
  const ObDASQueryStringCtDef *ctdef = static_cast<const ObDASQueryStringCtDef *>(ctdef_);
  ObDatum *query_text_datum = nullptr;
  if (OB_FAIL(ctdef->query_text_->eval(*eval_ctx_, query_text_datum))) {
    LOG_WARN("failed to eval query text", K(ret));
  } else if (OB_UNLIKELY(query_text_datum->is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null param", K(ret), KPC(query_text_datum));
  } else {
    const ObString &original_string = query_text_datum->get_string();
    const ObString &parser_name = ctdef->get_ir_ctdef(0)->get_inv_idx_scan_scalar_ctdef()
        ->table_param_.get_parser_name();
    const ObString &parser_property = ctdef->get_ir_ctdef(0)->get_inv_idx_scan_scalar_ctdef()
        ->table_param_.get_parser_property();
    const ObObjMeta &meta = ctdef->query_text_->obj_meta_;
    const ObCollationType src_coll = ctdef->query_text_->datum_meta_.cs_type_;
    const ObCollationType dst_coll = ObCollationType::CS_TYPE_UTF8MB4_GENERAL_CI;
    ObString query_string;
    if (src_coll != dst_coll) {
      ObString tmp_string;
      if (OB_FAIL(ObCharset::tolower(src_coll, original_string, tmp_string, allocator_))) {
        LOG_WARN("failed to casedown string", K(ret), K(src_coll), K(original_string));
      } else if (OB_FAIL(ObCharset::charset_convert(
          allocator_, tmp_string, src_coll, dst_coll, query_string))) {
        LOG_WARN("failed to convert string", K(ret), K(src_coll), K(original_string));
      }
    } else if (OB_FAIL(ObCharset::tolower(src_coll, original_string, query_string, allocator_))) {
      LOG_WARN("failed to casedown string", K(ret), K(src_coll), K(original_string));
    }
    if (FAILEDx(check_reserved_operators(query_string))) {
      LOG_WARN("query has reserved operators", K(ret));
    }
    const char separator = ' ';
    const char boost_op = '^';
    ObArray<ObString> empty;
    while (OB_SUCC(ret) && !query_string.empty()) {
      ObString boosted_text = query_string.split_on(boost_op);
      if (boosted_text.empty()) {
        ObString tmp_string;
        if (!token_groups.empty() && token_groups.at(token_groups.count() - 1).empty()) {
          // skip adding a group
          token_boosts.at(token_boosts.count() - 1) = 1.0;
        } else if (OB_FAIL(token_groups.push_back(empty))) {
          LOG_WARN("failed to append token groups", K(ret));
        } else if (OB_FAIL(token_boosts.push_back(1.0))) {
          LOG_WARN("failed to append token boosts", K(ret));
        }
        if (OB_FAIL(ret)) {
        } else if (src_coll == dst_coll) {
          tmp_string = query_string;
        } else if (OB_FAIL(ObCharset::charset_convert(
            allocator_, query_string, dst_coll, src_coll, tmp_string))) {
          LOG_WARN("failed to convert string", K(ret), K(dst_coll), K(query_string));
        }
        if (FAILEDx(ObFTSTokenizer::tokenize(
            allocator_, tmp_string, parser_name, parser_property, meta,
            token_groups.at(token_groups.count() - 1)))) {
          LOG_WARN("failed to tokenize query text", K(ret));
        } else {
          query_string.reset();
        }
      } else {
        ObString unboosted_text = boosted_text.split_on(boosted_text.reverse_find(separator));
        ObString boost_string = query_string.split_on(separator);
        if (boost_string.empty()) {
          boost_string.assign(query_string.ptr(), query_string.length());
          query_string.reset();
        }
        if (!unboosted_text.empty()) {
          ObString tmp_string;
          if (!token_groups.empty() && token_groups.at(token_groups.count() - 1).empty()) {
            // skip adding a group
            token_boosts.at(token_boosts.count() - 1) = 1.0;
          } else if (OB_FAIL(token_groups.push_back(empty))) {
            LOG_WARN("failed to append token groups", K(ret));
          } else if (OB_FAIL(token_boosts.push_back(1.0))) {
            LOG_WARN("failed to append token boosts", K(ret));
          }
          if (OB_FAIL(ret)) {
          } else if (src_coll == dst_coll) {
            tmp_string = unboosted_text;
          } else if (OB_FAIL(ObCharset::charset_convert(
              allocator_, unboosted_text, dst_coll, src_coll, tmp_string))) {
            LOG_WARN("failed to convert string", K(ret), K(dst_coll), K(unboosted_text));
          }
          if (FAILEDx(ObFTSTokenizer::tokenize(
              allocator_, tmp_string, parser_name, parser_property, meta,
              token_groups.at(token_groups.count() - 1)))) {
            LOG_WARN("failed to tokenize unboosted text", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_UNLIKELY(boost_string.empty())) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "query_string query: malformed query");
          LOG_WARN("malformed query in query string query", K(ret));
        } else {
          char *boost_str = static_cast<char *>(allocator_.alloc(boost_string.length() + 1));
          if (OB_ISNULL(boost_str)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to alloc memory", K(ret));
          } else {
            memcpy(boost_str, boost_string.ptr(), boost_string.length());
            boost_str[boost_string.length()] = '\0';
            char *end_ptr = nullptr;
            double boost = strtod(boost_str, &end_ptr);
            if (OB_UNLIKELY(end_ptr != boost_str + boost_string.length())) {
              ret = OB_INVALID_ARGUMENT;
              LOG_USER_ERROR(OB_INVALID_ARGUMENT, "query_string query: malformed query");
              LOG_WARN("failed to parse boost", K(ret));
            } else if (OB_UNLIKELY(boost <= 0.0)) {
              ret = OB_INVALID_ARGUMENT;
              LOG_USER_ERROR(OB_INVALID_ARGUMENT,
                             "query_string query: token boost must be positive");
              LOG_WARN("unexpected non-positive boost", K(ret));
            }
            ObString tmp_string;
            if (OB_FAIL(ret)) {
            } else if (!token_groups.empty() && token_groups.at(token_groups.count() - 1).empty()) {
              // skip adding a group
              token_boosts.at(token_boosts.count() - 1) = boost;
            } else if (OB_FAIL(token_groups.push_back(empty))) {
              LOG_WARN("failed to append token groups", K(ret));
            } else if (OB_FAIL(token_boosts.push_back(boost))) {
              LOG_WARN("failed to append token boosts", K(ret));
            }
            if (OB_FAIL(ret)) {
            } else if (src_coll == dst_coll) {
              tmp_string = boosted_text;
            } else if (OB_FAIL(ObCharset::charset_convert(
                allocator_, boosted_text, dst_coll, src_coll, tmp_string))) {
              LOG_WARN("failed to convert string", K(ret), K(dst_coll), K(boosted_text));
            }
            if (FAILEDx(ObFTSTokenizer::tokenize(
                allocator_, tmp_string, parser_name, parser_property, meta,
                token_groups.at(token_groups.count() - 1)))) {
              LOG_WARN("failed to tokenize boosted text", K(ret));
            }
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!token_groups.empty() && token_groups.at(token_groups.count() - 1).empty()) {
      token_groups.pop_back();
      token_boosts.pop_back();
    }
  }
  return ret;
}

int ObDASQueryStringRtDef::check_reserved_operators(const ObString &query)
{
  int ret = OB_SUCCESS;
  if (!query.empty()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(RESERVED_CHARS); ++i) {
      if (NULL != query.find(RESERVED_CHARS[i])) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "query_string query: query contains reserved character");
        LOG_WARN("query has reserved operator", K(ret), K(query), K(RESERVED_CHARS[i]));
      }
    }

    ObString tmp = query;
    while (OB_SUCC(ret) && !tmp.empty()) {
      const int64_t old_len = tmp.length();
      ObString token = tmp.split_on(' ');
      if (token.empty() && tmp.length() == old_len) {
        token = tmp;
        tmp.reset();
      }
      if (token.empty()) {
        // skip empty token caused by consecutive spaces
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(RESERVED_KEYWORDS); ++i) {
          if (token.compare_equal(RESERVED_KEYWORDS[i])) {
            ret = OB_INVALID_ARGUMENT;
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, "query_string query: query contains reserved keyword");
            LOG_WARN("query has reserved operator", K(ret), K(query), K(token));
          }
        }
      }
    }
  }
  return ret;
}

int ObDASQueryStringRtDef::can_pushdown_filter_to_bmm(bool &can_pushdown)
{
  int ret = OB_SUCCESS;
  can_pushdown = false;
  const ObDASQueryStringCtDef *ctdef = static_cast<const ObDASQueryStringCtDef *>(ctdef_);
  ObDatum *default_operator_datum = nullptr;
  ObDatum *min_should_match_datum = nullptr;
  ObDatum *type_datum = nullptr;
  if (OB_ISNULL(ctdef) || OB_ISNULL(eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", KR(ret), KP(ctdef), KP_(eval_ctx));
  } else if (OB_FAIL(ctdef->default_operator_->eval(*eval_ctx_, default_operator_datum))) {
    LOG_WARN("failed to eval default_operator", KR(ret));
  } else if (OB_FAIL(ctdef->minimum_should_match_->eval(*eval_ctx_, min_should_match_datum))) {
    LOG_WARN("failed to eval minimum_should_match", KR(ret));
  } else if (OB_FAIL(ctdef->type_->eval(*eval_ctx_, type_datum))) {
    LOG_WARN("failed to eval type", KR(ret));
  } else if (OB_UNLIKELY(default_operator_datum->is_null()
      || min_should_match_datum->is_null()
      || type_datum->is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null datum", KR(ret),
        KPC(default_operator_datum), KPC(min_should_match_datum), KPC(type_datum));
  } else {
    const int64_t field_cnt = ctdef->ir_ctdef_indices_.count();
    const int64_t type_val = type_datum->get_int();
    const bool is_supported_type =
        (ObMatchFieldsType::MATCH_BEST_FIELDS == type_val && 1 == field_cnt)
        || ObMatchFieldsType::MATCH_MOST_FIELDS == type_val;
    can_pushdown = ctdef->is_top_level_scoring()
        && ObMatchOperator::MATCH_OPERATOR_OR == default_operator_datum->get_int()
        && min_should_match_datum->get_int() <= 1
        && is_supported_type;
  }
  return ret;
}

int ObDASQueryStringRtDef::generate_op(
    ObDASSearchCost lead_cost,
    ObDASSearchCtx &search_ctx,
    ObIDASSearchOp *&op)
{
  int ret = OB_SUCCESS;
  const ObDASQueryStringCtDef *ctdef = static_cast<const ObDASQueryStringCtDef *>(ctdef_);
  const bool has_pushdown_filter = nullptr != pushdown_filter_op_;
  ObDatum *query_text_datum = nullptr;
  ObArray<ObArray<ObString>> token_groups;
  ObArray<double> token_boosts;
  int64_t token_cnt = 0;
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
  } else if (OB_FAIL(parse_query_string(token_groups, token_boosts))) {
    LOG_WARN("failed to parse query string", K(ret));
  } else {
    for (int64_t i = 0; i < token_groups.count(); ++i) {
      token_cnt += token_groups.at(i).count();
    }
  }

  ObDatum *boost_datum = nullptr;
  ObDatum *default_operator_datum = nullptr;
  ObDatum *min_should_match_datum = nullptr;
  ObDatum *type_datum = nullptr;

  if (OB_FAIL(ret)) {
  } else if (0 == token_cnt) {
    ObDASDummyOp *dummy_op = nullptr;
    ObDASDummyOpParam dummy_op_param;
    if (OB_FAIL(search_ctx.create_op(dummy_op_param, dummy_op))) {
      LOG_WARN("failed to create dummy op", KR(ret));
    } else {
      op = dummy_op;
    }
  } else {
    const int64_t group_cnt = token_groups.count();
    const int64_t field_cnt = ctdef->ir_ctdef_indices_.count();
    int64_t min_should_match = 0;
    if (OB_FAIL(ctdef->boost_->eval(*eval_ctx_, boost_datum))) {
      LOG_WARN("failed to eval boost", KR(ret));
    } else if (OB_FAIL(ctdef->default_operator_->eval(*eval_ctx_, default_operator_datum))) {
      LOG_WARN("failed to eval default_operator", KR(ret));
    } else if (OB_FAIL(ctdef->minimum_should_match_->eval(*eval_ctx_, min_should_match_datum))) {
      LOG_WARN("failed to eval minimum_should_match", KR(ret));
    } else if (OB_FAIL(ctdef->type_->eval(*eval_ctx_, type_datum))) {
      LOG_WARN("failed to eval type", KR(ret));
    } else if (OB_UNLIKELY(default_operator_datum->is_null()
        || min_should_match_datum->is_null() || type_datum->is_null())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null param", KR(ret), KPC(query_text_datum), KPC(default_operator_datum),
              KPC(min_should_match_datum), KPC(type_datum));
    } else {
      min_should_match = MIN(MAX(min_should_match_datum->get_int(), 1), group_cnt);
    }

    ObSEArray<double, 8> field_boosts;
    if (FAILEDx(field_boosts.reserve(field_cnt))) {
      LOG_WARN("failed to reserve array of field boosts", K(ret));
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
      } else if (OB_FAIL(field_boosts.push_back(field_boost_datum->get_double()))) {
        LOG_WARN("failed to append field boost", K(ret));
      }
    }

    ObSEArray<ObIDASSearchOp *, 8> group_ops;
    ObSEArray<ObIDASSearchOp *, 8> field_ops;
    ObSEArray<ObIDASSearchOp *, 8> token_ops;
    if (FAILEDx(group_ops.reserve(group_cnt))) {
      LOG_WARN("failed to reserve array of group ops", K(ret));
    } else if (OB_FAIL(field_ops.reserve(field_cnt))) {
      LOG_WARN("failed to reserve array of group ops", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < group_cnt; ++i) {
      field_ops.reuse();
      const ObArray<ObString> &tokens = token_groups.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < field_cnt; ++j) {
        ObDASIRScanCtDef *ir_ctdef = ctdef->get_ir_ctdef(j);
        ObDASIRScanRtDef *ir_rtdef = get_ir_rtdef(j);
        token_ops.reuse();
        if (OB_FAIL(token_ops.reserve(tokens.count()))) {
          LOG_WARN("failed to reserve array of token ops", K(ret));
        }
        for (int64_t k = 0; OB_SUCC(ret) && k < tokens.count(); ++k) {
          ObDASTokenOp *token_op = nullptr;
          ObDASTokenOpParam token_op_param;
          token_op_param.ir_ctdef_ = ir_ctdef;
          token_op_param.ir_rtdef_ = ir_rtdef;
          token_op_param.block_max_param_ = &block_max_params_[j];
          token_op_param.token_boost_ = boost_datum->get_double()
                                        * field_boosts.at(j) * token_boosts.at(i);
          token_op_param.query_token_ = tokens.at(k);
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
        } else if (1 == token_ops.count() && !has_pushdown_filter) {
          if (OB_FAIL(field_ops.push_back(token_ops.at(0)))) {
            LOG_WARN("failed to append the only token op to field ops", K(ret));
          }
        } else if (ObMatchOperator::MATCH_OPERATOR_OR == default_operator_datum->get_int()) {
          if (ctdef->is_top_level_scoring() && 1 == group_cnt && (1 == field_cnt
                || ObMatchFieldsType::MATCH_BEST_FIELDS == type_datum->get_int())) {
            ObDASBMMOp *bmm_op = nullptr;
            ObDASBMMOpParam bmm_op_param(
                token_ops, query_optional_, pushdown_filter_op_, allocator_);
            if (OB_FAIL(search_ctx.create_op(bmm_op_param, bmm_op))) {
              LOG_WARN("failed to create bmm op", KR(ret));
            } else if (OB_FAIL(field_ops.push_back(bmm_op))) {
              LOG_WARN("failed to append bmm op to field ops", K(ret));
            }
          } else {
            ObDASDisjunctionOp *disj_op = nullptr;
            ObDASDisjunctionOpParam disj_op_param(token_ops, 1, false, lead_cost);
            if (OB_FAIL(search_ctx.create_op(disj_op_param, disj_op))) {
              LOG_WARN("failed to create disjunction op", KR(ret));
            } else if (OB_FAIL(field_ops.push_back(disj_op))) {
              LOG_WARN("failed to append disjunction op to field ops", K(ret));
            }
          }
        } else if (ObMatchOperator::MATCH_OPERATOR_AND == default_operator_datum->get_int()) {
          ObDASConjunctionOp *conj_op = nullptr;
          ObDASConjunctionOpParam conj_op_param(token_ops);
          if (OB_FAIL(search_ctx.create_op(conj_op_param, conj_op))) {
            LOG_WARN("failed to create conjunction op", KR(ret));
          } else if (OB_FAIL(field_ops.push_back(conj_op))) {
            LOG_WARN("failed to append conjunction op to field ops", K(ret));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected match operator", K(ret), K(default_operator_datum->get_int()));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (1 == field_ops.count()) {
        if (OB_FAIL(group_ops.push_back(field_ops.at(0)))) {
          LOG_WARN("failed to append the only field op to group ops", K(ret));
        }
      } else if (ObMatchFieldsType::MATCH_BEST_FIELDS == type_datum->get_int()) {
        ObDASDisjunctionOp *disj_op = nullptr;
        ObDASDisjunctionOpParam disj_op_param(field_ops, 1, true, lead_cost);
        if (OB_FAIL(search_ctx.create_op(disj_op_param, disj_op))) {
          LOG_WARN("failed to create disjunction op", KR(ret));
        } else if (OB_FAIL(group_ops.push_back(disj_op))) {
          LOG_WARN("failed to append disjunction op to group ops", K(ret));
        }
      } else if (ObMatchFieldsType::MATCH_MOST_FIELDS == type_datum->get_int()) {
        if (ctdef->is_top_level_scoring() && 1 == group_cnt) {
          ObDASBMMOp *bmm_op = nullptr;
          ObDASBMMOpParam bmm_op_param(
              field_ops, query_optional_, pushdown_filter_op_, allocator_);
          if (OB_FAIL(search_ctx.create_op(bmm_op_param, bmm_op))) {
            LOG_WARN("failed to create bmm op", KR(ret));
          } else if (OB_FAIL(group_ops.push_back(bmm_op))) {
            LOG_WARN("failed to append bmm op to group ops", K(ret));
          }
        } else {
          ObDASDisjunctionOp *disj_op = nullptr;
          ObDASDisjunctionOpParam disj_op_param(field_ops, 1, false, lead_cost);
          if (OB_FAIL(search_ctx.create_op(disj_op_param, disj_op))) {
            LOG_WARN("failed to create disjunction op", KR(ret));
          } else if (OB_FAIL(group_ops.push_back(disj_op))) {
            LOG_WARN("failed to append disjunction op to group ops", K(ret));
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected match fields type", K(ret), K(type_datum->get_int()));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (1 == group_ops.count()) {
      op = group_ops.at(0);
    } else if ((MATCH_OPERATOR_AND == default_operator_datum->get_int()
        || min_should_match >= group_cnt) && !has_pushdown_filter) {
      ObDASConjunctionOp *conj_op = nullptr;
      ObDASConjunctionOpParam conj_op_param(group_ops);
      if (OB_FAIL(search_ctx.create_op(conj_op_param, conj_op))) {
        LOG_WARN("failed to create conjunction op", KR(ret));
      } else {
        op = conj_op;
      }
    } else if (!ctdef->is_top_level_scoring()) {
      ObDASDisjunctionOp *disj_op = nullptr;
      ObDASDisjunctionOpParam disj_op_param(group_ops, min_should_match, false, lead_cost);
      if (OB_FAIL(search_ctx.create_op(disj_op_param, disj_op))) {
        LOG_WARN("failed to create disjunction op", KR(ret));
      } else {
        op = disj_op;
      }
    } else if (min_should_match > 1) {
      ObDASBMWOp *bmw_op = nullptr;
      ObDASBMWOpParam bmw_op_param(group_ops, min_should_match, allocator_);
      if (OB_FAIL(search_ctx.create_op(bmw_op_param, bmw_op))) {
        LOG_WARN("failed to create bmw op", KR(ret));
      } else {
        op = bmw_op;
      }
    } else {
      ObDASBMMOp *bmm_op = nullptr;
      ObDASBMMOpParam bmm_op_param(
          group_ops, query_optional_, pushdown_filter_op_, allocator_);
      if (OB_FAIL(search_ctx.create_op(bmm_op_param, bmm_op))) {
        LOG_WARN("failed to create bmm op", KR(ret));
      } else {
        op = bmm_op;
      }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
