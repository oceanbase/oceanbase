/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_CG
#include "sql/code_generator/ob_hybrid_search_cg_service.h"
#include "sql/code_generator/ob_static_engine_cg.h"
#include "sql/das/search/ob_das_match_query.h"
#include "sql/das/search/ob_das_multi_match_query.h"
#include "sql/das/search/ob_das_match_phrase_query.h"
#include "sql/das/search/ob_das_query_string_query.h"
#include "sql/das/search/ob_das_topk_collect_op.h"
#include "sql/hybrid_search/ob_fulltext_search_node.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/engine/table/ob_table_scan_op.h"

namespace oceanbase
{
namespace sql
{
int ObHybridSearchCgService::generate_ctdef(ObLogTableScan &op,
                                            const ObFullTextQueryNode *fulltext_query_node,
                                            ObDASBaseCtDef *&fulltext_query_ctdef)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cg_.phy_plan_) || OB_ISNULL(fulltext_query_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", KR(ret), K(cg_.phy_plan_), K(fulltext_query_node));
  } else {
    switch (fulltext_query_node->get_query_type()) {
      case ObFullTextQueryNode::QueryType::OB_FULLTEXT_QUERY_TYPE_MATCH: {
        ObDASMatchCtDef *match_query_ctdef = nullptr;
        if (OB_FAIL(generate_fulltext_query_ctdef(
            op, static_cast<const ObMatchQueryNode*>(fulltext_query_node), match_query_ctdef))) {
          LOG_WARN("failed to generate fulltext query ctdef", K(ret));
        } else {
          fulltext_query_ctdef = static_cast<ObDASBaseCtDef*>(match_query_ctdef);
        }
        break;
      }
      case ObFullTextQueryNode::QueryType::OB_FULLTEXT_QUERY_TYPE_MATCH_PHRASE: {
        ObDASMatchPhraseCtDef *match_phrase_query_ctdef = nullptr;
        if (OB_FAIL(generate_match_phrase_query_ctdef(
            op,
            static_cast<const ObMatchPhraseQueryNode *>(fulltext_query_node),
            match_phrase_query_ctdef))) {
          LOG_WARN("failed to generate match phrase query ctdef", K(ret));
        } else {
          fulltext_query_ctdef = match_phrase_query_ctdef;
        }
        break;
      }
      case ObFullTextQueryNode::QueryType::OB_FULLTEXT_QUERY_TYPE_MULTI_MATCH: {
        ObDASMultiMatchCtDef *multi_match_query_ctdef = nullptr;
        if (OB_FAIL(generate_multi_match_query_ctdef(
            op,
            static_cast<const ObMultiMatchQueryNode *>(fulltext_query_node),
            multi_match_query_ctdef))) {
          LOG_WARN("failed to generate multi match query ctdef", K(ret));
        } else {
          fulltext_query_ctdef = multi_match_query_ctdef;
        }
        break;
      }
      case ObFullTextQueryNode::QueryType::OB_FULLTEXT_QUERY_TYPE_QUERY_STRING: {
        ObDASQueryStringCtDef *query_string_query_ctdef = nullptr;
        if (OB_FAIL(generate_query_string_query_ctdef(
            op,
            static_cast<const ObQueryStringQueryNode *>(fulltext_query_node),
            query_string_query_ctdef))) {
          LOG_WARN("failed to generate query string query ctdef", K(ret));
        } else {
          fulltext_query_ctdef = query_string_query_ctdef;
        }
        break;
      }
      default:
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not supported fulltext query type", K(ret), K(fulltext_query_node->get_query_type()));
    }
  }

  if (OB_SUCC(ret)) {
    static_cast<ObIDASSearchCtDef*>(fulltext_query_ctdef)->set_is_top_level_scoring(fulltext_query_node->is_top_level_scoring_);
    static_cast<ObIDASSearchCtDef*>(fulltext_query_ctdef)->set_is_scoring(fulltext_query_node->is_scoring_);
  }
  return ret;
}

int ObHybridSearchCgService::generate_fulltext_query_ctdef(ObLogTableScan &op,
                                                           const ObMatchQueryNode *match_node,
                                                           ObDASMatchCtDef *&match_query_ctdef)
{
  int ret = OB_SUCCESS;
  ObIAllocator &allocator = cg_.phy_plan_->get_allocator();
  match_query_ctdef = nullptr;
  ObDASIRScanCtDef *ir_scan_ctdef = nullptr;
  uint64_t doc_id_idx_tid = OB_INVALID_ID;
  if (OB_ISNULL(cg_.phy_plan_) || OB_ISNULL(match_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", KR(ret), K(cg_.phy_plan_), K(match_node));
  } else if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_MATCH_QUERY, cg_.phy_plan_->get_allocator(), match_query_ctdef))) {
    LOG_WARN("failed to allocate match query ctdef", K(ret));
  } else if (OB_ISNULL(match_query_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("match query ctdef is null", K(ret));
  } else if (FALSE_IT(doc_id_idx_tid = OB_INVALID_ID == match_node->doc_id_idx_tid_
      ? op.get_ref_table_id() : match_node->doc_id_idx_tid_)) {
  } else if (OB_FAIL(generate_text_ir_ctdef(
      op,
      match_node->index_info_,
      *match_node,
      true, /* is topk query, define later */
      match_node->inv_idx_tid_,
      doc_id_idx_tid,
      ir_scan_ctdef))) {
    LOG_WARN("failed to generate text ir ctdef", K(ret));
  } else if (OB_ISNULL(match_query_ctdef->children_ = OB_NEW_ARRAY(ObDASBaseCtDef*, &allocator, 1))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate match query ctdef children failed", K(ret));
  } else {
    match_query_ctdef->children_cnt_ = 1;
    match_query_ctdef->children_[0] = ir_scan_ctdef;
    match_query_ctdef->ir_ctdef_idx_ = 0;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(cg_.generate_rt_expr(*match_node->query_text_, match_query_ctdef->query_text_))) {
    LOG_WARN("failed to generate rt expr for query text", K(ret));
  } else if (OB_FAIL(cg_.generate_rt_expr(*match_node->boost_, match_query_ctdef->boost_))) {
    LOG_WARN("failed to generate rt expr for boost", K(ret));
  } else if (OB_FAIL(cg_.generate_rt_expr(*match_node->minimum_should_match_, match_query_ctdef->minimum_should_match_))) {
    LOG_WARN("failed to generate rt expr for minimum should match", K(ret));
  } else if (OB_FAIL(cg_.generate_rt_expr(*match_node->operator_, match_query_ctdef->match_operator_))) {
    LOG_WARN("failed to generate rt expr for operator", K(ret));
  } else {
    ir_scan_ctdef->search_text_ = match_query_ctdef->query_text_;
  }
  return ret;
}

int ObHybridSearchCgService::generate_match_phrase_query_ctdef(
    ObLogTableScan &op,
    const ObMatchPhraseQueryNode *match_phrase_node,
    ObDASMatchPhraseCtDef *&match_phrase_query_ctdef)
{
  int ret = OB_SUCCESS;
  ObIAllocator &allocator = cg_.phy_plan_->get_allocator();
  match_phrase_query_ctdef = nullptr;
  ObDASIRScanCtDef *ir_scan_ctdef = nullptr;
  uint64_t doc_id_idx_tid = OB_INVALID_ID;
  if (OB_ISNULL(cg_.phy_plan_) || OB_ISNULL(match_phrase_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", KR(ret), K(cg_.phy_plan_), K(match_phrase_node));
  } else if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(
      DAS_OP_MATCH_PHRASE_QUERY, cg_.phy_plan_->get_allocator(), match_phrase_query_ctdef))) {
    LOG_WARN("failed to allocate match phrase query ctdef", K(ret));
  } else if (OB_ISNULL(match_phrase_query_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("match phrase query ctdef is null", K(ret));
  } else if (FALSE_IT(doc_id_idx_tid = OB_INVALID_ID == match_phrase_node->doc_id_idx_tid_
      ? op.get_ref_table_id() : match_phrase_node->doc_id_idx_tid_)) {
  } else if (OB_FAIL(generate_text_ir_ctdef(
      op,
      match_phrase_node->index_info_,
      *match_phrase_node,
      true, /* is topk query, define later */
      match_phrase_node->inv_idx_tid_,
      doc_id_idx_tid,
      ir_scan_ctdef))) {
    LOG_WARN("failed to generate text ir ctdef", K(ret));
  } else if (OB_ISNULL(match_phrase_query_ctdef->children_ = OB_NEW_ARRAY(ObDASBaseCtDef*, &allocator, 1))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate match phrase query ctdef children failed", K(ret));
  } else {
    match_phrase_query_ctdef->children_cnt_ = 1;
    match_phrase_query_ctdef->children_[0] = ir_scan_ctdef;
    match_phrase_query_ctdef->ir_ctdef_idx_ = 0;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(cg_.generate_rt_expr(*match_phrase_node->query_text_, match_phrase_query_ctdef->query_text_))) {
    LOG_WARN("failed to generate rt expr for query text", K(ret));
  } else if (OB_FAIL(cg_.generate_rt_expr(*match_phrase_node->boost_, match_phrase_query_ctdef->boost_))) {
    LOG_WARN("failed to generate rt expr for boost", K(ret));
  } else if (OB_FAIL(cg_.generate_rt_expr(*match_phrase_node->slop_, match_phrase_query_ctdef->slop_))) {
    LOG_WARN("failed to generate rt expr for slop", K(ret));
  } else {
    ir_scan_ctdef->search_text_ = match_phrase_query_ctdef->query_text_;
  }
  return ret;
}

int ObHybridSearchCgService::generate_multi_match_query_ctdef(
    ObLogTableScan &op,
    const ObMultiMatchQueryNode *multi_match_node,
    ObDASMultiMatchCtDef *&multi_match_query_ctdef)
{
  int ret = OB_SUCCESS;
  ObIAllocator &allocator = cg_.phy_plan_->get_allocator();
  multi_match_query_ctdef = nullptr;
  ObDASIRScanCtDef *ir_scan_ctdef = nullptr;
  uint64_t doc_id_idx_tid = OB_INVALID_ID;
  int64_t field_cnt = 0;
  if (OB_ISNULL(cg_.phy_plan_) || OB_ISNULL(multi_match_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", KR(ret), K(cg_.phy_plan_), K(multi_match_node));
  } else if (FALSE_IT(doc_id_idx_tid = OB_INVALID_ID == multi_match_node->doc_id_idx_tid_
      ? op.get_ref_table_id() : multi_match_node->doc_id_idx_tid_)) {
  } else if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(
      DAS_OP_MULTI_MATCH_QUERY, cg_.phy_plan_->get_allocator(), multi_match_query_ctdef))) {
    LOG_WARN("failed to allocate multi match query ctdef", K(ret));
  } else if (OB_ISNULL(multi_match_query_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("multi match query ctdef is null", K(ret));
  } else if (FALSE_IT(field_cnt = multi_match_node->fields_.count())) {
  } else if (OB_UNLIKELY(field_cnt != multi_match_node->field_boosts_.count()
      || field_cnt != multi_match_node->inv_idx_tids_.count()
      || field_cnt != multi_match_node->index_infos_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inconsistent field count", K(ret));
  } else if (OB_ISNULL(multi_match_query_ctdef->children_
      = OB_NEW_ARRAY(ObDASBaseCtDef *, &allocator, field_cnt))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate children of multi match query ctdef", K(ret));
  } else if (OB_FAIL(multi_match_query_ctdef->ir_ctdef_indices_.init(field_cnt))) {
    LOG_WARN("failed to init ir ctdef indices", K(ret));
  } else if (OB_FAIL(multi_match_query_ctdef->ir_ctdef_indices_.prepare_allocate(field_cnt))) {
    LOG_WARN("failed to prepare allocate ir ctdef indices", K(ret));
  } else if (OB_FAIL(multi_match_query_ctdef->field_boosts_.init(field_cnt))) {
    LOG_WARN("failed to init field boosts", K(ret));
  } else if (OB_FAIL(multi_match_query_ctdef->field_boosts_.prepare_allocate(field_cnt))) {
    LOG_WARN("failed to prepare allocate field boosts", K(ret));
  } else {
    multi_match_query_ctdef->children_cnt_ = field_cnt;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < field_cnt; ++i) {
    if (OB_FAIL(generate_text_ir_ctdef(
        op,
        multi_match_node->index_infos_[i],
        *multi_match_node,
        true, /* is topk query, define later */
        multi_match_node->inv_idx_tids_[i],
        doc_id_idx_tid,
        ir_scan_ctdef))) {
      LOG_WARN("failed to generate text ir ctdef", K(ret));
    } else if (OB_FAIL(cg_.generate_rt_expr(
        *multi_match_node->field_boosts_[i], multi_match_query_ctdef->field_boosts_[i]))) {
      LOG_WARN("failed to generate rt expr for field boost", K(ret));
    } else {
      multi_match_query_ctdef->children_[i] = ir_scan_ctdef;
      multi_match_query_ctdef->ir_ctdef_indices_[i] = i;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(cg_.generate_rt_expr(
      *multi_match_node->query_text_, multi_match_query_ctdef->query_text_))) {
    LOG_WARN("failed to generate rt expr for query text", K(ret));
  } else if (OB_FAIL(cg_.generate_rt_expr(
      *multi_match_node->boost_, multi_match_query_ctdef->boost_))) {
    LOG_WARN("failed to generate rt expr for boost", K(ret));
  } else if (OB_FAIL(cg_.generate_rt_expr(
      *multi_match_node->minimum_should_match_, multi_match_query_ctdef->minimum_should_match_))) {
    LOG_WARN("failed to generate rt expr for minimum should match", K(ret));
  } else if (OB_FAIL(cg_.generate_rt_expr(
      *multi_match_node->operator_, multi_match_query_ctdef->match_operator_))) {
    LOG_WARN("failed to generate rt expr for operator", K(ret));
  } else if (OB_FAIL(cg_.generate_rt_expr(
      *multi_match_node->field_type_, multi_match_query_ctdef->type_))) {
    LOG_WARN("failed to generate rt expr for field type", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < field_cnt; ++i) {
    static_cast<ObDASIRScanCtDef *>(multi_match_query_ctdef->children_[i])->search_text_
        = multi_match_query_ctdef->query_text_;
  }
  return ret;
}

int ObHybridSearchCgService::generate_query_string_query_ctdef(
    ObLogTableScan &op,
    const ObQueryStringQueryNode *query_string_node,
    ObDASQueryStringCtDef *&query_string_query_ctdef)
{
  int ret = OB_SUCCESS;
  ObIAllocator &allocator = cg_.phy_plan_->get_allocator();
  query_string_query_ctdef = nullptr;
  ObDASIRScanCtDef *ir_scan_ctdef = nullptr;
  uint64_t doc_id_idx_tid = OB_INVALID_ID;
  int64_t field_cnt = 0;
  if (OB_ISNULL(cg_.phy_plan_) || OB_ISNULL(query_string_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", KR(ret), K(cg_.phy_plan_), K(query_string_node));
  } else if (FALSE_IT(doc_id_idx_tid = OB_INVALID_ID == query_string_node->doc_id_idx_tid_
      ? op.get_ref_table_id() : query_string_node->doc_id_idx_tid_)) {
  } else if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(
      DAS_OP_QUERY_STRING_QUERY, cg_.phy_plan_->get_allocator(), query_string_query_ctdef))) {
    LOG_WARN("failed to allocate query string query ctdef", K(ret));
  } else if (OB_ISNULL(query_string_query_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query string query ctdef is null", K(ret));
  } else if (FALSE_IT(field_cnt = query_string_node->fields_.count())) {
  } else if (OB_UNLIKELY(field_cnt != query_string_node->field_boosts_.count()
      || field_cnt != query_string_node->inv_idx_tids_.count()
      || field_cnt != query_string_node->index_infos_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inconsistent field count", K(ret));
  } else if (OB_ISNULL(query_string_query_ctdef->children_
      = OB_NEW_ARRAY(ObDASBaseCtDef *, &allocator, field_cnt))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate children of query string query ctdef", K(ret));
  } else if (OB_FAIL(query_string_query_ctdef->ir_ctdef_indices_.init(field_cnt))) {
    LOG_WARN("failed to init ir ctdef indices", K(ret));
  } else if (OB_FAIL(query_string_query_ctdef->ir_ctdef_indices_.prepare_allocate(field_cnt))) {
    LOG_WARN("failed to prepare allocate ir ctdef indices", K(ret));
  } else if (OB_FAIL(query_string_query_ctdef->field_boosts_.init(field_cnt))) {
    LOG_WARN("failed to init field boosts", K(ret));
  } else if (OB_FAIL(query_string_query_ctdef->field_boosts_.prepare_allocate(field_cnt))) {
    LOG_WARN("failed to prepare allocate field boosts", K(ret));
  } else {
    query_string_query_ctdef->children_cnt_ = field_cnt;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < field_cnt; ++i) {
    if (OB_FAIL(generate_text_ir_ctdef(
        op,
        query_string_node->index_infos_[i],
        *query_string_node,
        true, /* is topk query, define later */
        query_string_node->inv_idx_tids_[i],
        doc_id_idx_tid,
        ir_scan_ctdef))) {
      LOG_WARN("failed to generate text ir ctdef", K(ret));
    } else if (OB_FAIL(cg_.generate_rt_expr(
        *query_string_node->field_boosts_[i], query_string_query_ctdef->field_boosts_[i]))) {
      LOG_WARN("failed to generate rt expr for field boost", K(ret));
    } else {
      query_string_query_ctdef->children_[i] = ir_scan_ctdef;
      query_string_query_ctdef->ir_ctdef_indices_[i] = i;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(cg_.generate_rt_expr(
      *query_string_node->query_text_, query_string_query_ctdef->query_text_))) {
    LOG_WARN("failed to generate rt expr for query text", K(ret));
  } else if (OB_FAIL(cg_.generate_rt_expr(
      *query_string_node->boost_, query_string_query_ctdef->boost_))) {
    LOG_WARN("failed to generate rt expr for boost", K(ret));
  } else if (OB_FAIL(cg_.generate_rt_expr(
      *query_string_node->minimum_should_match_, query_string_query_ctdef->minimum_should_match_))) {
    LOG_WARN("failed to generate rt expr for minimum should match", K(ret));
  } else if (OB_FAIL(cg_.generate_rt_expr(
      *query_string_node->default_operator_, query_string_query_ctdef->default_operator_))) {
    LOG_WARN("failed to generate rt expr for operator", K(ret));
  } else if (OB_FAIL(cg_.generate_rt_expr(
      *query_string_node->field_type_, query_string_query_ctdef->type_))) {
    LOG_WARN("failed to generate rt expr for field type", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < field_cnt; ++i) {
    static_cast<ObDASIRScanCtDef *>(query_string_query_ctdef->children_[i])->search_text_
        = query_string_query_ctdef->query_text_;
  }
  return ret;
}

int ObHybridSearchCgService::generate_text_ir_ctdef(ObLogTableScan &op,
                                                    const ObTextRetrievalIndexInfo &index_info,
                                                    const ObFullTextQueryNode &fulltext_node,
                                                    const bool is_topk_query,
                                                    const uint64_t inv_idx_tid,
                                                    const uint64_t doc_id_idx_tid,
                                                    ObDASIRScanCtDef *&ir_scan_ctdef)
{
  int ret = OB_SUCCESS;
  ObIAllocator &allocator = cg_.phy_plan_->get_allocator();
  ir_scan_ctdef = nullptr;
  ObDASScalarScanCtDef *inv_idx_scan_ctdef = nullptr;
  const bool need_score = fulltext_node.need_score();
  const bool need_pos_list = (nullptr != index_info.pos_list_column_);
  if (OB_UNLIKELY(!index_info.is_valid() || OB_INVALID_ID == inv_idx_tid)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(inv_idx_tid), K(doc_id_idx_tid), K(index_info));
  } else if (OB_ISNULL(cg_.phy_plan_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy plan is null", KR(ret), KP(cg_.phy_plan_));
  } else if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_IR_SCAN, cg_.phy_plan_->get_allocator(), ir_scan_ctdef))) {
    LOG_WARN("failed to allocate ir scan ctdef", K(ret));
  } else if (OB_ISNULL(ir_scan_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ir scan ctdef is null", K(ret));
  } else if (OB_FAIL(generate_text_ir_sub_scan_ctdef(
      op, index_info, ObTSCIRScanType::OB_IR_INV_IDX_SCAN, need_score, need_pos_list,
      inv_idx_tid, doc_id_idx_tid, inv_idx_scan_ctdef))) {
    LOG_WARN("failed to generate inv idx scan ctdef", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (need_score) {
    const bool need_block_max_scan = is_topk_query;
    const int64_t children_cnt = need_block_max_scan ? 4 : 3;
    ObDASScalarScanCtDef *inv_idx_agg_ctdef = nullptr;
    ObDASScalarScanCtDef *doc_id_idx_agg_ctdef = nullptr;
    ObDASScalarScanCtDef *inv_block_max_scan_ctdef = nullptr;
    if (OB_FAIL(generate_text_ir_sub_scan_ctdef(
        op, index_info, ObTSCIRScanType::OB_IR_INV_IDX_AGG, need_score, need_pos_list,
        inv_idx_tid, doc_id_idx_tid, inv_idx_agg_ctdef))) {
      LOG_WARN("failed to generate inv idx agg ctdef", K(ret));
    } else if (OB_FAIL(generate_text_ir_sub_scan_ctdef(
        op, index_info, ObTSCIRScanType::OB_IR_DOC_ID_IDX_AGG, need_score, need_pos_list,
        inv_idx_tid, doc_id_idx_tid, doc_id_idx_agg_ctdef))) {
      LOG_WARN("failed to generate doc id idx agg ctdef", K(ret));
    } else if (need_block_max_scan && OB_FAIL(generate_text_ir_sub_scan_ctdef(
        op, index_info, ObTSCIRScanType::OB_IR_BLOCK_MAX_SCAN, need_score, need_pos_list,
        inv_idx_tid, doc_id_idx_tid, inv_block_max_scan_ctdef))) {
      LOG_WARN("failed to generate block max scan ctdef", K(ret));
    } else if (OB_ISNULL(ir_scan_ctdef->children_ = OB_NEW_ARRAY(ObDASBaseCtDef*, &allocator, children_cnt))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate ir scan ctdef children failed", K(ret));
    } else {
      ir_scan_ctdef->children_cnt_ = children_cnt;
      ir_scan_ctdef->children_[0] = inv_idx_scan_ctdef;
      ir_scan_ctdef->children_[1] = inv_idx_agg_ctdef;
      ir_scan_ctdef->children_[2] = doc_id_idx_agg_ctdef;
      if (need_block_max_scan) {
        ir_scan_ctdef->children_[3] = inv_block_max_scan_ctdef;
      }
      ir_scan_ctdef->has_inv_agg_ = true;
      ir_scan_ctdef->has_doc_id_agg_ = true;
      ir_scan_ctdef->has_block_max_scan_ = need_block_max_scan;
    }
  } else {
    if (OB_ISNULL(ir_scan_ctdef->children_ = OB_NEW_ARRAY(ObDASBaseCtDef*, &allocator, 1))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate ir scan ctdef children failed", K(ret));
    } else {
      ir_scan_ctdef->children_cnt_ = 1;
      ir_scan_ctdef->children_[0] = inv_idx_scan_ctdef;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (FALSE_IT(ir_scan_ctdef->use_scalar_scan_def_ = true)) {
  } else if (OB_FAIL(generate_text_ir_spec(index_info, fulltext_node, is_topk_query, inv_idx_tid, *ir_scan_ctdef))) {
    LOG_WARN("failed to generate text ir spec", K(ret));
  } else {
    const ObCostTableScanInfo *est_cost_info = op.get_est_cost_info();
    int partition_row_cnt = 0;
    if (nullptr == est_cost_info
        || nullptr == est_cost_info->table_meta_info_
        || 0 == est_cost_info->table_meta_info_->part_count_) {
      // No estimated info or approx agg not allowed, do total document count on execution;
    } else {
      partition_row_cnt = est_cost_info->table_meta_info_->table_row_count_ / est_cost_info->table_meta_info_->part_count_;
    }
    ir_scan_ctdef->estimated_total_doc_cnt_ = partition_row_cnt;
  }

  return ret;
}

int ObHybridSearchCgService::generate_text_ir_sub_scan_ctdef(ObLogTableScan &op,
                                                             const ObTextRetrievalIndexInfo &index_info,
                                                             const ObTSCIRScanType ir_scan_type,
                                                             const bool need_score,
                                                             const bool need_pos_list,
                                                             const uint64_t inv_idx_tid,
                                                             const uint64_t doc_id_idx_tid,
                                                             ObDASScalarScanCtDef *&scalar_scan_ctdef)
{
  int ret = OB_SUCCESS;
  scalar_scan_ctdef = nullptr;
  ObSEArray<ObRawExpr *, 4> access_exprs;
  ObSEArray<uint64_t, 4> output_cids;
  ObSEArray<ObRawExpr *, 1> agg_exprs;
  if (OB_FAIL(extract_text_ir_access_columns(
      index_info, ir_scan_type, need_score, need_pos_list, access_exprs, output_cids, agg_exprs))) {
    LOG_WARN("failed to extract text ir access columns", K(ret));
  } else {
    // TOOD: generate_scalar_scan_ctdef doesn't support aggregate pushdown for now
    const uint64_t idx_tid = ir_scan_type == ObTSCIRScanType::OB_IR_DOC_ID_IDX_AGG ? doc_id_idx_tid : inv_idx_tid;
    DASScalarCGParams scalar_cg_params(
        op.get_ref_table_id(),
        idx_tid,
        false, /* is new query range */
        true, /* is rowkey order scan */
        false, /* is primary table scan */
        op.get_table_type(),
        &access_exprs,
        &output_cids,
        &op.get_rowkey_exprs(), /* rowkey exprs */
        nullptr, /* pre range graph */
        nullptr, /* pre query range */
        false, /* has pre query range */
        false, /* is get */
        true, /* need rowkey order */
        false, /* is use column store */
        nullptr, /* domain id expr */
        nullptr, /* domain id col ids */
        nullptr, /* pushdown filters */
        &agg_exprs, /* pushdown aggr exprs */
        nullptr /* filter monotonicity */
    );
    if (OB_FAIL(generate_scalar_scan_ctdef(scalar_cg_params, scalar_scan_ctdef))) {
      LOG_WARN("failed to generate scalar scan ctdef", K(ret), K(ir_scan_type), K(idx_tid),
          K(access_exprs), K(output_cids), K(agg_exprs));
    }
  }
  return ret;
}

int ObHybridSearchCgService::extract_text_ir_access_columns(const ObTextRetrievalIndexInfo &index_info,
                                                            const ObTSCIRScanType ir_scan_type,
                                                            const bool need_score,
                                                            const bool need_pos_list,
                                                            ObIArray<ObRawExpr *> &access_exprs,
                                                            ObIArray<uint64_t> &output_cids,
                                                            ObIArray<ObRawExpr *> &agg_exprs)
{
  int ret = OB_SUCCESS;
  switch (ir_scan_type) {
  case ObTSCIRScanType::OB_IR_INV_IDX_SCAN: {
    if (OB_FAIL(add_var_to_array_no_dup(access_exprs, static_cast<ObRawExpr*>(index_info.domain_id_column_)))) {
      LOG_WARN("failed to add document id column to access exprs", K(ret));
    } else if (OB_FAIL(output_cids.push_back(index_info.domain_id_column_->get_column_id()))) {
      LOG_WARN("failed to append document id column id to output cids", K(ret));
    } else if (!need_score) {
      // skip
    } else if (OB_FAIL(add_var_to_array_no_dup(access_exprs, static_cast<ObRawExpr*>(index_info.token_cnt_column_)))) {
      LOG_WARN("failed to add token cnt column to access exprs", K(ret));
    } else if (OB_FAIL(output_cids.push_back(index_info.token_cnt_column_->get_column_id()))) {
      LOG_WARN("failed to append token cnt column id to output cids", K(ret));
    } else if (OB_FAIL(add_var_to_array_no_dup(access_exprs, static_cast<ObRawExpr*>(index_info.doc_length_column_)))) {
      LOG_WARN("failed to add document length column to access exprs", K(ret));
    } else if (OB_FAIL(output_cids.push_back(index_info.doc_length_column_->get_column_id()))) {
      LOG_WARN("failed to append document length column id to output cids", K(ret));
    }
    if (OB_FAIL(ret) || !need_pos_list) {
      // skip
    } else if (OB_FAIL(add_var_to_array_no_dup(access_exprs, static_cast<ObRawExpr*>(index_info.pos_list_column_)))) {
      LOG_WARN("failed to add pos list column to access exprs", K(ret));
    } else if (OB_FAIL(output_cids.push_back(index_info.pos_list_column_->get_column_id()))) {
      LOG_WARN("failed to append pos list column id to output cids", K(ret));
    }
    break;
  }
  case ObTSCIRScanType::OB_IR_DOC_ID_IDX_AGG: {
    if (OB_FAIL(add_var_to_array_no_dup(access_exprs, static_cast<ObRawExpr*>(index_info.total_doc_cnt_->get_param_expr((0)))))) {
      LOG_WARN("failed to add document id column to access exprs", K(ret));
    } else if (OB_FAIL(add_var_to_array_no_dup(agg_exprs, static_cast<ObRawExpr*>(index_info.total_doc_cnt_)))) {
      LOG_WARN("failed to add document id column to agg exprs", K(ret));
    }
    break;
  }
  case ObTSCIRScanType::OB_IR_INV_IDX_AGG: {
    if (OB_FAIL(add_var_to_array_no_dup(access_exprs, static_cast<ObRawExpr*>(index_info.related_doc_cnt_->get_param_expr(0))))) {
      LOG_WARN("failed to add token cnt column to access exprs", K(ret));
    } else if (OB_FAIL(add_var_to_array_no_dup(agg_exprs, static_cast<ObRawExpr*>(index_info.related_doc_cnt_)))) {
      LOG_WARN("failed to add token cnt column to agg exprs", K(ret));
    }
    break;
  }
  case ObTSCIRScanType::OB_IR_BLOCK_MAX_SCAN: {
    if (OB_FAIL(add_var_to_array_no_dup(access_exprs, static_cast<ObRawExpr*>(index_info.token_column_)))) {
      LOG_WARN("failed to add token column to access exprs", K(ret));
    } else if (OB_FAIL(add_var_to_array_no_dup(access_exprs, static_cast<ObRawExpr*>(index_info.domain_id_column_)))) {
      LOG_WARN("failed to add document id column to access exprs", K(ret));
    } else if (OB_FAIL(add_var_to_array_no_dup(access_exprs, static_cast<ObRawExpr*>(index_info.token_cnt_column_)))) {
      LOG_WARN("failed to add token cnt column to access exprs", K(ret));
    } else if (OB_FAIL(add_var_to_array_no_dup(access_exprs, static_cast<ObRawExpr*>(index_info.doc_length_column_)))) {
      LOG_WARN("failed to add document length column to access exprs", K(ret));
    }
    break;
  }
  default: {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected text ir scan type", K(ret), K(ir_scan_type));
  }
  }
  return ret;
}

int ObHybridSearchCgService::generate_text_ir_spec(const ObTextRetrievalIndexInfo &index_info,
                                                   const ObFullTextQueryNode &fulltext_node,
                                                   const bool is_topk_query,
                                                   const uint64_t inv_idx_tid,
                                                   ObDASIRScanCtDef &ir_scan_ctdef)
{
  int ret = OB_SUCCESS;
  const bool need_score = fulltext_node.need_score();
  ObSEArray<ObExpr *, 4> result_output;
  const UIntFixedArray &inv_scan_col_ids = ir_scan_ctdef.get_inv_idx_scan_scalar_ctdef()->access_column_ids_;
  const ObColumnRefRawExpr *doc_id_column = static_cast<ObColumnRefRawExpr *>(index_info.domain_id_column_);
  const ObColumnRefRawExpr *doc_length_column = static_cast<ObColumnRefRawExpr *>(index_info.doc_length_column_);
  const ObColumnRefRawExpr *pos_list_column = static_cast<ObColumnRefRawExpr *>(index_info.pos_list_column_);
  if (OB_UNLIKELY(OB_ISNULL(doc_id_column)
      || (need_score && OB_ISNULL(doc_length_column))
      || (is_topk_query && !need_score))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status", K(ret), K(need_score), K(is_topk_query), K(doc_id_column), K(doc_length_column));
  }
  int64_t doc_id_col_idx = -1;
  int64_t doc_length_col_idx = -1;
  int64_t pos_list_col_idx = -1;
  for (int64_t i = 0; OB_SUCC(ret) && i < inv_scan_col_ids.count(); ++i) {
    if (inv_scan_col_ids.at(i) == doc_id_column->get_column_id()) {
      doc_id_col_idx = i;
    } else if (need_score && inv_scan_col_ids.at(i) == doc_length_column->get_column_id()) {
      doc_length_col_idx = i;
    } else if (nullptr != pos_list_column
        && inv_scan_col_ids.at(i) == pos_list_column->get_column_id()) {
      pos_list_col_idx = i;
    }
  }
  if (OB_UNLIKELY(-1 == doc_id_col_idx || (need_score && -1 == doc_length_col_idx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected column not found in inverted index scan access columns",
        K(ret), K(inv_scan_col_ids), KPC(doc_id_column), KPC(doc_length_column));
  } else {
    ir_scan_ctdef.inv_scan_domain_id_col_ = ir_scan_ctdef.get_inv_idx_scan_scalar_ctdef()->pd_expr_spec_.access_exprs_.at(doc_id_col_idx);
    if (need_score) {
      ir_scan_ctdef.inv_scan_doc_length_col_ = ir_scan_ctdef.get_inv_idx_scan_scalar_ctdef()->pd_expr_spec_.access_exprs_.at(doc_length_col_idx);
    } else {
      ir_scan_ctdef.inv_scan_doc_length_col_ = nullptr;
    }
    if (-1 != pos_list_col_idx) {
      ir_scan_ctdef.inv_scan_pos_list_col_ = ir_scan_ctdef.get_inv_idx_scan_scalar_ctdef()->pd_expr_spec_.access_exprs_.at(pos_list_col_idx);
    } else {
      ir_scan_ctdef.inv_scan_pos_list_col_ = nullptr;
    }
    if (OB_FAIL(result_output.push_back(ir_scan_ctdef.inv_scan_domain_id_col_))) {
      LOG_WARN("failed to append result output expr", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!need_score) {
    // skip
  } else if (OB_FAIL(cg_.generate_rt_expr(*index_info.relevance_expr_, ir_scan_ctdef.relevance_expr_))) {
    LOG_WARN("failed to generate rt expr for relevance expr", K(ret));
  } else if (OB_FAIL(cg_.generate_rt_expr(*fulltext_node.score_project_expr_, ir_scan_ctdef.relevance_proj_col_))) {
    LOG_WARN("failed to generate rt expr for score project expr", K(ret));
  } else if (OB_FAIL(result_output.push_back(ir_scan_ctdef.relevance_proj_col_))) {
    LOG_WARN("failed to append result output expr", K(ret));
  } else if (nullptr == index_info.avg_doc_token_cnt_) {
    ir_scan_ctdef.has_avg_doc_len_est_ = false;
  } else if (OB_FAIL(generate_avg_doc_len_est_spec(index_info, inv_idx_tid, ir_scan_ctdef))) {
    LOG_WARN("failed to generate avg doc len est spec", K(ret));
  } else {
    ir_scan_ctdef.has_avg_doc_len_est_ = true;
  }

  bool is_skip_index_valid_for_block_max_scan = false;
  if (OB_FAIL(ret) || !is_topk_query) {
  } else if (OB_FAIL(check_skip_index_validity(index_info, inv_idx_tid, is_skip_index_valid_for_block_max_scan))) {
    LOG_WARN("failed to check skip index validity for block max scan", K(ret));
  } else if (!is_skip_index_valid_for_block_max_scan) {
    ir_scan_ctdef.has_block_max_scan_ = false;
  } else if (OB_FAIL(generate_block_max_scan_spec(index_info, inv_idx_tid, ir_scan_ctdef))) {
    LOG_WARN("failed to generate block max scan spec", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ir_scan_ctdef.result_output_.assign(result_output))) {
    LOG_WARN("failed to assign result output", K(ret), K(result_output));
  }

  return ret;
}

int ObHybridSearchCgService::generate_block_max_scan_spec(const ObTextRetrievalIndexInfo &index_info,
                                                          const uint64_t inv_idx_tid,
                                                          ObDASIRScanCtDef &ir_scan_ctdef)
{
  int ret = OB_SUCCESS;
  // generate block max scan agg columns
  // fulltext index need to scan min(domain_id), max(domain_id), max(token_cnt), min(doc_length) for max score estimation
  const int64_t block_max_scan_col_cnt = 4;
  ObSqlSchemaGuard *schema_guard = cg_.opt_ctx_->get_sql_schema_guard();
  const ObTableSchema *inv_idx_schema = nullptr;
  ObSEArray<ObColDesc, 8> inv_idx_col_ids;
  ObTextBlockMaxSpec &block_max_spec = ir_scan_ctdef.block_max_spec_;
  const ObDASScalarScanCtDef *block_max_scan_ctdef = ir_scan_ctdef.get_block_max_scan_scalar_ctdef();
  if (OB_ISNULL(index_info.token_column_)
      || OB_ISNULL(schema_guard)
      || OB_ISNULL(block_max_scan_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status", K(ret), KP(schema_guard), KP(block_max_scan_ctdef), K(index_info));
  } else if (OB_FAIL(cg_.generate_rt_expr(*index_info.token_column_, ir_scan_ctdef.token_col_))) {
    LOG_WARN("cg rt expr for token column failed", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(schema_guard->get_table_schema(inv_idx_tid, inv_idx_schema))) {
    LOG_WARN("get inv idx schema failed", K(ret), K(inv_idx_tid));
  } else if (OB_ISNULL(inv_idx_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null inv idx schema", K(ret), K(inv_idx_tid));
  } else if (OB_FAIL(inv_idx_schema->get_multi_version_column_descs(inv_idx_col_ids))) {
    LOG_WARN("get inv idx col ids failed", K(ret), KPC(inv_idx_schema));
  } else if (OB_FAIL(block_max_spec.col_types_.init(block_max_scan_col_cnt))) {
    LOG_WARN("failed to init block max scan col store idxes", K(ret));
  } else if (OB_FAIL(block_max_spec.col_store_idxes_.init(block_max_scan_col_cnt))) {
    LOG_WARN("failed to init block max scan col types", K(ret));
  } else if (OB_FAIL(block_max_spec.scan_col_proj_.init(block_max_scan_col_cnt))) {
    LOG_WARN("failed to init block max scan col proj", K(ret));
  } else if (OB_FAIL(append_block_max_scan_agg_column(index_info.domain_id_column_->get_column_id(),
                                                      *inv_idx_schema,
                                                      ObSkipIndexColType::SK_IDX_MIN,
                                                      inv_idx_col_ids,
                                                      block_max_scan_ctdef->access_column_ids_,
                                                      block_max_spec.col_store_idxes_,
                                                      block_max_spec.col_types_,
                                                      block_max_spec.scan_col_proj_))) {
    LOG_WARN("failed to append block max scan agg column", K(ret));
  } else if (OB_FAIL(append_block_max_scan_agg_column(index_info.domain_id_column_->get_column_id(),
                                                      *inv_idx_schema,
                                                      ObSkipIndexColType::SK_IDX_MAX,
                                                      inv_idx_col_ids,
                                                      block_max_scan_ctdef->access_column_ids_,
                                                      block_max_spec.col_store_idxes_,
                                                      block_max_spec.col_types_,
                                                      block_max_spec.scan_col_proj_))) {
    LOG_WARN("failed to append block max scan agg column", K(ret));
  } else if (OB_FAIL(append_block_max_scan_agg_column(index_info.token_cnt_column_->get_column_id(),
                                                      *inv_idx_schema,
                                                      ObSkipIndexColType::SK_IDX_BM25_MAX_SCORE_TOKEN_FREQ,
                                                      inv_idx_col_ids,
                                                      block_max_scan_ctdef->access_column_ids_,
                                                      block_max_spec.col_store_idxes_,
                                                      block_max_spec.col_types_,
                                                      block_max_spec.scan_col_proj_))) {
    LOG_WARN("failed to append block max scan agg column", K(ret));
  } else if (OB_FAIL(append_block_max_scan_agg_column(index_info.doc_length_column_->get_column_id(),
                                                      *inv_idx_schema,
                                                      ObSkipIndexColType::SK_IDX_BM25_MAX_SCORE_DOC_LEN,
                                                      inv_idx_col_ids,
                                                      block_max_scan_ctdef->access_column_ids_,
                                                      block_max_spec.col_store_idxes_,
                                                      block_max_spec.col_types_,
                                                      block_max_spec.scan_col_proj_))) {
    LOG_WARN("failed to append block max scan agg column", K(ret));
  } else {
    block_max_spec.min_id_idx_ = 0;
    block_max_spec.max_id_idx_ = 1;
    block_max_spec.token_freq_idx_ = 2;
    block_max_spec.doc_length_idx_ = 3;
  }
  return ret;
}

int ObHybridSearchCgService::generate_avg_doc_len_est_spec(const ObTextRetrievalIndexInfo &index_info,
                                                           const uint64_t inv_idx_tid,
                                                           ObDASIRScanCtDef &ir_scan_ctdef)
{
  int ret = OB_SUCCESS;
  ObTextAvgDocLenEstSpec &avg_doc_len_est_spec = ir_scan_ctdef.avg_doc_len_est_spec_;
  const ObTableSchema *inv_idx_schema = nullptr;
  ObSEArray<ObColDesc, 8> inv_idx_col_ids;
  const ObColumnSchemaV2 *col_schema = nullptr;
  ObSqlSchemaGuard *schema_guard = cg_.opt_ctx_->get_sql_schema_guard();
  uint64_t column_id = OB_INVALID_ID;
  // reuse inv idx scan ctdef here, since we only need to access skip index of sum(token_cnt) on basline major sstable
  const ObDASScalarScanCtDef *inv_idx_scan_ctdef = ir_scan_ctdef.get_inv_idx_scan_scalar_ctdef();
  if (OB_ISNULL(index_info.avg_doc_token_cnt_) || OB_ISNULL(index_info.token_cnt_column_) || OB_ISNULL(inv_idx_scan_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(index_info), KP(inv_idx_scan_ctdef));
  } else if (OB_FAIL(cg_.generate_rt_expr(*index_info.avg_doc_token_cnt_, ir_scan_ctdef.avg_doc_token_cnt_expr_))) {
    LOG_WARN("cg rt expr for avg doc token count expr failed", K(ret));
  } else if (OB_FAIL(avg_doc_len_est_spec.col_types_.init(1))) {
    LOG_WARN("failed to init avg doc len est col types", K(ret));
  } else if (OB_FAIL(avg_doc_len_est_spec.col_store_idxes_.init(1))) {
    LOG_WARN("failed to init avg doc len est col store idxes", K(ret));
  } else if (OB_FAIL(avg_doc_len_est_spec.scan_col_proj_.init(1))) {
    LOG_WARN("failed to init avg doc len est scan col proj", K(ret));
  } else if (FALSE_IT(column_id = index_info.token_cnt_column_->get_column_id())) {
  } else if (OB_FAIL(schema_guard->get_table_schema(inv_idx_tid, inv_idx_schema))) {
    LOG_WARN("get inv idx schema failed", K(ret), K(inv_idx_tid));
  } else if (OB_ISNULL(inv_idx_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null inv idx schema", K(ret), K(inv_idx_tid));
  } else if (OB_FAIL(inv_idx_schema->get_multi_version_column_descs(inv_idx_col_ids))) {
    LOG_WARN("get inv idx col ids failed", K(ret), KPC(inv_idx_schema));
  } else if (OB_ISNULL(col_schema = inv_idx_schema->get_column_schema(column_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get column schema", K(ret), K(column_id));
  } else if (OB_UNLIKELY(!col_schema->get_skip_index_attr().has_sum())) {
    ir_scan_ctdef.avg_doc_len_est_spec_.can_est_by_sum_skip_index_ = false;
  } else {
    int64_t store_idx = -1;
    int64_t column_proj = -1;
    for (int64_t i = 0; i < inv_idx_col_ids.count(); ++i) {
      if (inv_idx_col_ids.at(i).col_id_ == column_id) {
        store_idx = i;
        break;
      }
    }
    for (int64_t i = 0; i < inv_idx_scan_ctdef->access_column_ids_.count(); ++i) {
      if (inv_idx_scan_ctdef->access_column_ids_.at(i) == column_id) {
        column_proj = i;
        break;
      }
    }

    if (OB_UNLIKELY(-1 == store_idx || -1 == column_proj)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected column idx not found", K(ret), K(column_id), K(store_idx), K(column_proj),
          K(inv_idx_col_ids), KPC(inv_idx_scan_ctdef));
    } else if (OB_FAIL(avg_doc_len_est_spec.col_types_.push_back(ObSkipIndexColType::SK_IDX_SUM))) {
      LOG_WARN("failed to push back avg doc len est col type", K(ret));
    } else if (OB_FAIL(avg_doc_len_est_spec.col_store_idxes_.push_back(store_idx))) {
      LOG_WARN("failed to push back avg doc len est col store idx", K(ret));
    } else if (OB_FAIL(avg_doc_len_est_spec.scan_col_proj_.push_back(column_proj))) {
      LOG_WARN("failed to push back avg doc len est scan col proj", K(ret));
    } else {
      ir_scan_ctdef.avg_doc_len_est_spec_.can_est_by_sum_skip_index_ = true;
    }
  }
  return ret;
}

int ObHybridSearchCgService::append_block_max_scan_agg_column(const int64_t column_id,
                                                              const ObTableSchema &table_schema,
                                                              const ObSkipIndexColType skip_index_type,
                                                              const ObIArray<ObColDesc> &col_descs,
                                                              const ObIArray<uint64_t> &access_column_ids,
                                                              ObIArray<int32_t> &block_max_scan_col_store_idxes,
                                                              ObIArray<ObSkipIndexColType> &block_max_scan_col_types,
                                                              ObIArray<int32_t> &block_max_scan_col_proj)
{
  int ret = OB_SUCCESS;
  const ObColumnSchemaV2 *col_schema = nullptr;
  if (OB_UNLIKELY(OB_INVALID_ID == column_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid column id", K(ret), K(column_id));
  } else if (OB_ISNULL(col_schema = table_schema.get_column_schema(column_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get column schema", K(ret), K(column_id));
  } else {
    const ObSkipIndexColumnAttr &skip_index_attr = col_schema->get_skip_index_attr();
    switch (skip_index_type) {
      case ObSkipIndexColType::SK_IDX_MIN:
      case ObSkipIndexColType::SK_IDX_MAX:
        if (OB_UNLIKELY(!skip_index_attr.has_loose_min_max())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected non-loose min/max skip index attr on inverted index", K(ret), K(skip_index_attr));
        }
        break;
      case ObSkipIndexColType::SK_IDX_BM25_MAX_SCORE_TOKEN_FREQ:
        if (OB_UNLIKELY(!skip_index_attr.has_bm25_token_freq_param())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected non-loose min/max skip index attr on inverted index", K(ret), K(skip_index_attr));
        }
        break;
      case ObSkipIndexColType::SK_IDX_BM25_MAX_SCORE_DOC_LEN:
        if (OB_UNLIKELY(!skip_index_attr.has_bm25_doc_len_param())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected non-loose min/max skip index attr on inverted index", K(ret), K(skip_index_attr));
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected skip index type", K(ret), K(skip_index_type));
    }
  }

  if (OB_SUCC(ret)) {
    int64_t store_idx = -1;
    int64_t column_proj = -1;
    for (int64_t i = 0; i < col_descs.count(); ++i) {
      if (col_descs.at(i).col_id_ == column_id) {
        store_idx = i;
        break;
      }
    }
    for (int64_t i = 0; i < access_column_ids.count(); ++i) {
      if (access_column_ids.at(i) == column_id) {
        column_proj = i;
        break;
      }
    }
    if (OB_UNLIKELY(-1 == store_idx || -1 == column_proj)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected column idx not found", K(ret), K(column_id), K(store_idx),
          K(column_proj), K(col_descs), K(access_column_ids));
    } else {
      if (OB_FAIL(block_max_scan_col_store_idxes.push_back(store_idx))) {
        LOG_WARN("failed to push back block max scan col store idx", K(ret));
      } else if (OB_FAIL(block_max_scan_col_types.push_back(skip_index_type))) {
        LOG_WARN("failed to push back block max scan col type", K(ret));
      } else if (OB_FAIL(block_max_scan_col_proj.push_back(column_proj))) {
        LOG_WARN("failed to push back block max scan col proj", K(ret));
      }
    }
  }
  return ret;
}

int ObHybridSearchCgService::check_skip_index_validity(const ObTextRetrievalIndexInfo &index_info,
                                                       const uint64_t inv_idx_tid,
                                                       bool &is_valid) const
{
  int ret = OB_SUCCESS;
  is_valid = true;
  ObSqlSchemaGuard *schema_guard = cg_.opt_ctx_->get_sql_schema_guard();
  const ObTableSchema *inv_idx_schema = nullptr;
  const ObColumnSchemaV2 *col_schema = nullptr;
  int64_t column_id = 0;
  if (OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null schema guard", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(inv_idx_tid, inv_idx_schema))) {
    LOG_WARN("get inv idx schema failed", K(ret), K(inv_idx_tid));
  } else if (OB_ISNULL(inv_idx_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null inv idx schema", K(ret), K(inv_idx_tid));
  } else if (OB_UNLIKELY(OB_INVALID_ID == (column_id = index_info.domain_id_column_->get_column_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid column id", K(ret), K(column_id));
  } else if (OB_ISNULL(col_schema = inv_idx_schema->get_column_schema(column_id))){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get column schema", K(ret), K(column_id));
  } else if (OB_UNLIKELY(!col_schema->get_skip_index_attr().has_loose_min_max())) {
    is_valid = false;
  } else if (OB_UNLIKELY(OB_INVALID_ID == (column_id = index_info.token_cnt_column_->get_column_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid column id", K(ret), K(column_id));
  } else if (OB_ISNULL(col_schema = inv_idx_schema->get_column_schema(column_id))){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get column schema", K(ret), K(column_id));
  } else if (OB_UNLIKELY(!col_schema->get_skip_index_attr().has_bm25_token_freq_param())) {
    is_valid = false;
  } else if (OB_UNLIKELY(OB_INVALID_ID == (column_id = index_info.doc_length_column_->get_column_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid column id", K(ret), K(column_id));
  } else if (OB_ISNULL(col_schema = inv_idx_schema->get_column_schema(column_id))){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get column schema", K(ret), K(column_id));
  } else if (OB_UNLIKELY(!col_schema->get_skip_index_attr().has_bm25_doc_len_param())) {
    is_valid = false;
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
