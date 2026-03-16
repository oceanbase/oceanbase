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

#define USING_LOG_PREFIX SQL_OPT
#include "ob_hybrid_search_node.h"
#include "ob_fulltext_search_node.h"
#include "lib/json_type/ob_json_bin.h"
#include "lib/json_type/ob_json_tree.h"
#include "share/ob_json_access_utils.h"
#include "lib/udt/ob_array_type.h"
#include "share/vector_index/ob_vector_index_util.h"
#include "sql/engine/expr/ob_array_expr_utils.h"
#include "sql/engine/expr/ob_expr_json_func_helper.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/ob_sql_utils.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"

using namespace oceanbase;
using namespace sql;

const double ObHybridSearchGenerator::MAX_INTERSECTION_MERGE_SEL = 0.20;
const double ObHybridSearchGenerator::MAX_SEL_GAP_RATIO = 2.0;

bool ObHybridSearchGenerator::in_merge_expr_whitelist(const ObRawExpr *expr)
{
  bool is_match = false;
  if (OB_NOT_NULL(expr) && IS_STRICT_OP(expr->get_expr_type())) {
    for (int64_t i = 0; !is_match && i < expr->get_param_count(); ++i) {
      const ObRawExpr *param_expr = expr->get_param_expr(i);
      if (OB_NOT_NULL(param_expr) &&
          ObDSLResolver::in_merge_node_whitelist(ObRawExprUtils::skip_inner_added_expr(param_expr))) {
        is_match = true;
      }
    }
  }
  return is_match;
}

int ObHybridSearchGenerator::deal_table_scan_filters(ObIndexMergeNode *hybrid_search_tree,
                                                     ObIArray<ObRawExpr*> &table_filters,
                                                     bool &ingore_normal_access_path)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(hybrid_search_tree)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(hybrid_search_tree));
  } else if (INDEX_MERGE_HYBRID_FUSION_SEARCH != hybrid_search_tree->node_type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected node type", K(ret), K(hybrid_search_tree->node_type_));
  } else {
    ObFusionNode *fusion_node = static_cast<ObFusionNode *>(hybrid_search_tree);
    int64_t index_scan_count = 0;
    ingore_normal_access_path = false; // set to false in sql query.
    if (fusion_node->children_.count() != 1 || OB_ISNULL(fusion_node->children_.at(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected children count", K(ret), K(fusion_node->children_.count()));
    } else if (fusion_node->children_.at(0)->is_hybrid_scalar_node()) {
      // if single scan node is not search index path, normal access path is better.
      ingore_normal_access_path = false;
      if (OB_FAIL(recurse_count_index_nodes(fusion_node->children_.at(0), index_scan_count,
                                            ingore_normal_access_path))) {
        LOG_WARN("failed to recurse count index nodes", K(ret));
      }
    } else if (fusion_node->children_.at(0)->node_type_ != INDEX_MERGE_HYBRID_BOOLEAN_QUERY) {
      // do nothing
    } else {
      ObBooleanQueryNode *bool_node = static_cast<ObBooleanQueryNode *>(fusion_node->children_.at(0));
      double min_selectivity = 1.0;
      ObSEArray<double, 8> children_selectivity;
      double invalid_sel = 100;
      int idx = -1;
      for (int i = 0; i < bool_node->filter_nodes_.count() && OB_SUCC(ret); i++) {
        ObIndexMergeNode *child_node = bool_node->filter_nodes_.at(i);
        double child_sel = 0.0;
        bool child_prune_happened = false;
        if (OB_FAIL(deal_child_node(child_node, child_prune_happened, child_sel))) {
          LOG_WARN("failed to deal scalar node", K(ret), K(i), KPC(child_node));
        } else if (child_prune_happened) {
          child_sel = invalid_sel;
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(children_selectivity.push_back(child_sel))) {
          LOG_WARN("failed to push back child node selectivity", K(ret));
        } else if (child_sel > 0.0 && child_sel < min_selectivity) {
          min_selectivity = child_sel;
          idx = i;
        }
      }
      common::ObSEArray<ObIndexMergeNode*, 4> filter_nodes_tmp;
      if (OB_FAIL(ret) || children_selectivity.empty()) {
      } else if (OB_FAIL(filter_nodes_tmp.assign(bool_node->filter_nodes_))) {
        LOG_WARN("failed to assign must nodes", K(ret));
      } else if (min_selectivity > MAX_INTERSECTION_MERGE_SEL) {
        bool_node->filter_nodes_.reuse();
        for (int i = 0; i < filter_nodes_tmp.count() && OB_SUCC(ret); i++) {
          ObIndexMergeNode *child_node = filter_nodes_tmp.at(i);
          if (i == idx || children_selectivity.at(i) <= 0.0) {
            if (OB_FAIL(bool_node->filter_nodes_.push_back(child_node))) {
              LOG_WARN("failed to push back child node", K(ret));
            }
          } else if (child_node->is_hybrid_scalar_node()) {
            ObScalarQueryNode *scalar_node = static_cast<ObScalarQueryNode *>(child_node);
            if (OB_FAIL(append_array_no_dup(table_filters, scalar_node->pri_table_query_params_.pushdown_filters_))) {
              LOG_WARN("failed to append expr", K(ret));
            }
          } else if (child_node->node_type_ == INDEX_MERGE_HYBRID_BOOLEAN_QUERY) {
            if (OB_FAIL(add_var_to_array_no_dup(table_filters, static_cast<ObBooleanQueryNode *>(child_node)->origin_expr_))) {
              LOG_WARN("failed to append expr", K(ret));
            }
          }
        }
      } else {
        bool_node->filter_nodes_.reuse();
        const double max_sel = std::min(min_selectivity * MAX_SEL_GAP_RATIO, MAX_INTERSECTION_MERGE_SEL);
        for (int64_t i = 0; OB_SUCC(ret) && i < children_selectivity.count(); ++i) {
          ObIndexMergeNode *child_node = filter_nodes_tmp.at(i);
          if (children_selectivity.at(i) <= max_sel) {
            if (OB_FAIL(bool_node->filter_nodes_.push_back(child_node))) {
              LOG_WARN("failed to push back child node", K(ret));
            }
          } else {
            if (child_node->is_hybrid_scalar_node()) {
              ObScalarQueryNode *scalar_node = static_cast<ObScalarQueryNode *>(child_node);
              if (OB_FAIL(append_array_no_dup(table_filters, scalar_node->pri_table_query_params_.pushdown_filters_))) {
                LOG_WARN("failed to append expr", K(ret));
              }
            } else if (child_node->node_type_ == INDEX_MERGE_HYBRID_BOOLEAN_QUERY) {
              if (OB_FAIL(add_var_to_array_no_dup(table_filters, static_cast<ObBooleanQueryNode *>(child_node)->origin_expr_))) {
                LOG_WARN("failed to append expr", K(ret));
              }
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        ingore_normal_access_path = false;
        if (OB_FAIL(recurse_count_index_nodes(bool_node, index_scan_count,
                                              ingore_normal_access_path))) {
          LOG_WARN("failed to recurse count index nodes", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObHybridSearchGenerator::deal_child_node(ObIndexMergeNode *node, bool &prune_happened, double &sel)
{
  int ret = OB_SUCCESS;
  if (node->is_hybrid_scalar_node()) {
    if (OB_ISNULL(node->ap_)) {
      prune_happened = true;
      LOG_TRACE("prune node because no available index", KPC(node));
    } else if (!node->ap_->is_new_query_range_with_precise_expr()) {
      prune_happened = true;
      LOG_TRACE("prune node because no precise query range", KPC(node));
    } else {
      if (node->filter_.count() == 1 && (node->filter_.at(0)->is_json_domain_expr() ||
                                       node->filter_.at(0)->is_domain_array_expr())) {
        // Domain expressions (json_contains/array_contains) have high computation cost and their
        // selectivity may not be estimated accurately. When valid index exists, force using index
        // by setting negative selectivity to prevent this node from being pruned.
        sel = -0.0;
      } else {
        sel = node->ap_->est_cost_info_.prefix_filter_sel_;
      }
    }
  } else if (node->node_type_ == INDEX_MERGE_HYBRID_BOOLEAN_QUERY) {
    ObBooleanQueryNode *bool_node = static_cast<ObBooleanQueryNode*>(node);
    if (bool_node->filter_nodes_.count() > 0) {
      // and condition
      double min_selectivity = 1.0;
      ObSEArray<double, 8> children_selectivity;
      for (int i = 0; i < bool_node->filter_nodes_.count() && OB_SUCC(ret); i++) {
        ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(bool_node->filter_nodes_.at(i));
        double child_sel = 0.0;
        bool child_prune_happened = false;
        if (OB_FAIL(deal_child_node(child_node, child_prune_happened, child_sel))) {
          LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
        } else if (child_prune_happened) {
          prune_happened = true;
        } else if (OB_FAIL(children_selectivity.push_back(child_sel))) {
          LOG_WARN("failed to append child selectivity", K(ret), K(i), K(child_sel));
        } else if (child_sel > 0.0 && child_sel < min_selectivity) {
          min_selectivity = child_sel;
        }
      }
      if (OB_SUCC(ret) && !prune_happened) {
        if (min_selectivity < 1.0 && min_selectivity > MAX_INTERSECTION_MERGE_SEL) {
          prune_happened = true;
        } else {
          sel = 0.0;
          const double max_sel = std::min(min_selectivity * MAX_SEL_GAP_RATIO, MAX_INTERSECTION_MERGE_SEL);
          for (int i = 0; i < children_selectivity.count() && OB_SUCC(ret) && !prune_happened; i++) {
            if (children_selectivity.at(i) > max_sel) {
              // if prune_happened is true, sel is invalid
              prune_happened = true;
            } else {
              sel += children_selectivity.at(i);
            }
          }
        }
      }
    } else if (bool_node->should_nodes_.count() > 0) {
      // or condition
      for (int i = 0; i < bool_node->should_nodes_.count() && OB_SUCC(ret); i++) {
        ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(bool_node->should_nodes_.at(i));
        double child_sel = 0.0;
        bool child_prune_happened = false;
        if (OB_FAIL(deal_child_node(child_node, child_prune_happened, child_sel))) {
          LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
        } else if (child_prune_happened) {
          prune_happened = true;
        } else {
          sel += child_sel;
        }
      }
    }
  }
  return ret;
}

int ObHybridSearchGenerator::recurse_count_index_nodes(const ObIndexMergeNode *node,
                                                       int64_t &index_scan_count,
                                                       bool &ingore_normal_access_path) const
{
  int ret = OB_SUCCESS;
  if (ingore_normal_access_path) {
    // no need to count index nodes
  } else if (node->is_hybrid_scalar_node()) {
    if (OB_NOT_NULL(node->ap_)) {
      index_scan_count++;
      if (node->ap_->is_search_index_path() || index_scan_count > 1) {
        // if scan node is search index path or has multiple index scan nodes,
        // always use hybrid index merge path.
        ingore_normal_access_path = true;
      }
    }
  } else if (node->node_type_ == INDEX_MERGE_HYBRID_BOOLEAN_QUERY) {
    const ObBooleanQueryNode *bool_node = static_cast<const ObBooleanQueryNode*>(node);
    // in sql query, boolean query only has filter(AND) and should(OR) nodes.
    CK (bool_node->must_nodes_.empty());
    CK (bool_node->must_not_nodes_.empty());
    for (int i = 0; i < bool_node->filter_nodes_.count() && OB_SUCC(ret); i++) {
      ObIndexMergeNode *child_node = static_cast<ObIndexMergeNode *>(bool_node->filter_nodes_.at(i));
      if (OB_FAIL(recurse_count_index_nodes(child_node, index_scan_count, ingore_normal_access_path))) {
        LOG_WARN("failed to recurse count index nodes", K(ret), K(i), KPC(child_node));
      }
    }
    for (int i = 0; i < bool_node->should_nodes_.count() && OB_SUCC(ret); i++) {
      ObIndexMergeNode *child_node = static_cast<ObIndexMergeNode *>(bool_node->should_nodes_.at(i));
      if (OB_FAIL(recurse_count_index_nodes(child_node, index_scan_count, ingore_normal_access_path))) {
        LOG_WARN("failed to recurse count index nodes", K(ret), K(i), KPC(child_node));
      }
    }
  } else {
    // other node type, set ingore_normal_access_path to true
    ingore_normal_access_path = true;
  }
  return ret;
}

int ObHybridSearchGenerator::generate(const ObDSLQueryInfo *dsl_query,
                                      ObIndexMergeNode *&hybrid_search_tree)
{
  int ret = OB_SUCCESS;
  const ObDSLQueryInfo *dsl_info = NULL;
  ObSQLSessionInfo *session = nullptr;
  ObRawExprFactory *expr_factory = nullptr;
  if (OB_ISNULL(plan_)|| OB_ISNULL(allocator_)
      || OB_ISNULL(expr_factory = &plan_->get_optimizer_context().get_expr_factory())
      || OB_ISNULL(session = plan_->get_optimizer_context().get_session_info())
      || OB_ISNULL(dsl_info = dsl_query)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(allocator_), K(plan_));
  } else if (OB_ISNULL(hybrid_search_tree = OB_NEWx(ObFusionNode, allocator_, *allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate hybrid search node", K(ret));
  } else {
    ObFusionNode *fusion_node = static_cast<ObFusionNode *>(hybrid_search_tree);
    if (OB_FAIL(init_fusion_node(dsl_info, fusion_node))) {
      LOG_WARN("failed to init fusion node", K(ret));
    }
    for (int i = 0; i < dsl_info->queries_.count() && OB_SUCC(ret); i++) {
      ObDSLQuery *query =  dsl_info->queries_.at(i);
      ObIndexMergeNode *sub_node = NULL;
      ObConstRawExpr *one_boost_expr = static_cast<ObConstRawExpr*>(dsl_query->one_const_expr_);
      if (OB_FAIL(generate_node(query, sub_node))) {
        LOG_WARN("failed to generate hybrid search node", K(ret));
      } else if (OB_FAIL(fusion_node->children_.push_back(sub_node))) {
        LOG_WARN("failed to append hybrid search node", K(ret));
      } else if (OB_FAIL(collect_child_boost_and_update_flags(fusion_node, sub_node, one_boost_expr, i))) {
        LOG_WARN("failed to collect child boost and update flags", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObHybridSearchGenerator::init_fusion_node(const ObDSLQueryInfo *query_info, ObFusionNode *fusion_node)
{
  int ret = OB_SUCCESS;
  fusion_node->from_ = query_info->from_;
  fusion_node->size_ = query_info->size_;
  fusion_node->min_score_ = query_info->min_score_;
  fusion_node->method_ = query_info->rank_info_.method_;
  fusion_node->window_size_ = query_info->rank_info_.window_size_;
  fusion_node->rank_const_ = query_info->rank_info_.rank_const_;
  fusion_node->is_top_k_query_ = query_info->is_top_k_query_;
  for (int i = 0; i < query_info->rowkey_cols_.count() && OB_SUCC(ret); i++) {
    if (OB_FAIL(fusion_node->rowkey_cols_.push_back(query_info->rowkey_cols_.at(i)))) {
      LOG_WARN("failed to append rowkey expr", K(ret));
    }
  }
  for (int i = 0; i < query_info->score_cols_.count() && OB_SUCC(ret); i++) {
    if (OB_FAIL(fusion_node->score_cols_.push_back(query_info->score_cols_.at(i)))) {
      LOG_WARN("failed to append score expr", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    fusion_node_ = fusion_node;
  }
  return ret;
}

int ObHybridSearchGenerator::generate_node(const ObDSLQuery *query,
                                           ObIndexMergeNode *&node)
{
  int ret = OB_SUCCESS;
  switch (query->query_type_) {
    case ObEsQueryItem::QUERY_ITEM_MATCH :
    case ObEsQueryItem::QUERY_ITEM_MULTI_MATCH :
    case ObEsQueryItem::QUERY_ITEM_MATCH_PHRASE :
    case ObEsQueryItem::QUERY_ITEM_QUERY_STRING : {
      ObFullTextQueryNode *fulltext_node = nullptr;
      const ObDSLFullTextQuery *fulltext_query = static_cast<const ObDSLFullTextQuery *>(query);
      if (OB_FAIL(generate_fulltext_node(fulltext_query, fulltext_node))) {
        LOG_WARN("failed to allocate hybrid search node", K(ret));
      } else {
        node = fulltext_node;
      }
      break;
    }
    case ObEsQueryItem::QUERY_ITEM_RANGE :
    case ObEsQueryItem::QUERY_ITEM_TERM :
    case ObEsQueryItem::QUERY_ITEM_TERMS :
    case ObEsQueryItem::QUERY_ITEM_JSON_CONTAINS :
    case ObEsQueryItem::QUERY_ITEM_JSON_MEMBER_OF :
    case ObEsQueryItem::QUERY_ITEM_JSON_OVERLAPS :
    case ObEsQueryItem::QUERY_ITEM_ARRAY_CONTAINS :
    case ObEsQueryItem::QUERY_ITEM_ARRAY_CONTAINS_ALL :
    case ObEsQueryItem::QUERY_ITEM_ARRAY_OVERLAPS : {
      ObScalarQueryNode *term_node = NULL;
      // json_contains/array_contains need split multi sub exprs to use index merge.
      ObIndexMergeNode *split_node = NULL;
      const ObDSLScalarQuery *scalar_query = static_cast<const ObDSLScalarQuery *>(query);
      if (OB_FAIL(split_domain_contains_node(scalar_query, split_node))) {
        LOG_WARN("failed to split domain contains node", K(ret));
      } else if (split_node != NULL) {
        // split happened, use split node
        node = split_node;
      } else {
        if (OB_FAIL(generate_scalar_node(scalar_query->scalar_expr_, term_node))) {
          LOG_WARN("failed to allocate hybrid search node", K(ret));
        } else {
          node = term_node;
        }
      }
      break;
    }
    case ObEsQueryItem::QUERY_ITEM_BOOL : {
      ObBooleanQueryNode *boolean_node = NULL;
      const ObDSLBoolQuery *bool_query = static_cast<const ObDSLBoolQuery *>(query);
      if (OB_FAIL(generate_boolean_node(bool_query, boolean_node))) {
        LOG_WARN("failed to append hybrid search node", K(ret));
      } else {
        node = boolean_node;
      }
      break;
    }
    case ObEsQueryItem::QUERY_ITEM_KNN : {
      ObVecSearchNode *vec_node = NULL;
      const ObDSLKnnQuery *knn_query = static_cast<const ObDSLKnnQuery *>(query);
      if (OB_FAIL(generate_knn_node(knn_query, vec_node))) {
        LOG_WARN("failed to append hybrid search node", K(ret));
      } else {
        node = vec_node;
      }
      break;
    }
    default :
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected query type", K(ret), K(query->query_type_));
  }

  if (OB_SUCC(ret)) {
    ObHybridSearchNodeBase *new_node = static_cast<ObHybridSearchNodeBase *>(node);
    new_node->is_scoring_ = query->need_cal_score_;
    new_node->is_top_level_scoring_ = query->is_top_level_score_;
    new_node->boost_ = query->boost_;
  }
  return ret;
}

int ObHybridSearchGenerator::generate_knn_node(const ObDSLKnnQuery *knn_query, ObVecSearchNode *&knn_node)
{
  int ret = OB_SUCCESS;
  bool has_index = false;
  uint64_t index_id = OB_INVALID_ID;
  if (OB_ISNULL(knn_node = OB_NEWx(ObVecSearchNode, allocator_, *allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate hybrid search node", K(ret));
  } else if (OB_FAIL(get_vector_index_tid_from_expr(knn_query->field_, index_id))) {
    LOG_WARN("failed to allocate hybrid search node", K(ret));
  } else if (index_id == OB_INVALID_ID) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "knn search without vector index");
  } else {
    knn_node->vec_info_.sort_key_.expr_ = knn_query->distance_;
    knn_node->vec_info_.topk_limit_expr_ = knn_query->k_;
    // knn search does not support offset
    knn_node->vec_info_.topk_offset_expr_ = nullptr;
    knn_node->field_ = knn_query->field_;
    knn_node->table_schema_ = table_schema_;
    knn_node->search_option_ = knn_query->search_option_;
    knn_node->main_index_table_id_ = index_id;
    fusion_node_->contains_vec_node_ = true;

    if (OB_FAIL(ret)) {
    } else if (knn_query->filter_.count() == 0) {
      // do nothing
    } else if (knn_query->filter_.count() == 1) {
      ObIndexMergeNode *sub_node = NULL;
      if (OB_FAIL(generate_node(knn_query->filter_.at(0), sub_node))) {
        LOG_WARN("failed to generate child node", K(ret), K(0));
      } else if (OB_FAIL(knn_node->filter_nodes_.push_back(sub_node))) {
        LOG_WARN("failed to append child node", K(ret));
      }
    } else {
      ObBooleanQueryNode *bool_node = NULL;
      if (OB_ISNULL(bool_node = OB_NEWx(ObBooleanQueryNode, allocator_, *allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate hybrid search node", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < knn_query->filter_.count(); i++) {
          ObIndexMergeNode *sub_node = NULL;
          if (OB_FAIL(generate_node(knn_query->filter_.at(i), sub_node))) {
            LOG_WARN("failed to generate child node", K(ret), K(i));
          } else if (OB_FAIL(bool_node->filter_nodes_.push_back(sub_node))) {
            LOG_WARN("failed to append child node", K(ret), K(i));
          }
        }

        if (FAILEDx(bool_node->children_.reserve(bool_node->filter_nodes_.count()))) {
          LOG_WARN("failed to reserve children", K(ret));
        } else if (OB_FAIL(common::append(bool_node->children_, bool_node->filter_nodes_))) {
          LOG_WARN("failed to append must nodes", K(ret));
        } else if (OB_FALSE_IT(bool_node->boost_ = knn_query->boost_)) {
        } else if (OB_FAIL(knn_node->filter_nodes_.push_back(bool_node))) {
          LOG_WARN("failed to append child node", K(ret));
        }
      }
    }

    if (OB_SUCC(ret) && knn_node->filter_nodes_.count() == 1 && knn_node->need_extract_filter()) {
      ObRawExpr *extracted_filter = nullptr;
      int extracted_ret = OB_SUCCESS;
      ObHybridSearchNodeBase *filter_node = static_cast<ObHybridSearchNodeBase*>(knn_node->filter_nodes_.at(0));
      ObRawExprFactory *expr_factory = &plan_->get_optimizer_context().get_expr_factory();

      if (OB_ISNULL(filter_node) || OB_ISNULL(expr_factory)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(filter_node), K(expr_factory));
      } else if (OB_FALSE_IT(extracted_ret = filter_node->extract_filter(*expr_factory, extracted_filter))){
      } else if (OB_UNLIKELY(OB_SUCCESS != extracted_ret)) {
        if (OB_NOT_SUPPORTED != extracted_ret) {
          ret = extracted_ret;
          LOG_WARN("failed to extract filter from filter node", K(ret));
        }
      } else if (OB_NOT_NULL(extracted_filter)) {
        ObScalarQueryNode *scalar_node = nullptr;
        if (OB_FAIL(extracted_filter->formalize(plan_->get_optimizer_context().get_session_info()))) {
          LOG_WARN("failed to formalize extracted filter", K(ret));
        } else if (OB_FAIL(generate_scalar_node(extracted_filter, scalar_node))) {
          LOG_WARN("failed to generate scalar node from extracted filter", K(ret));
        } else {
          scalar_node->has_valid_index_ = false;
          scalar_node->boost_ = knn_query->boost_;
          scalar_node->is_extracted_ = true;
          scalar_node->need_rowkey_order_ = false;
          knn_node->extracted_scalar_node_ = scalar_node;
        }
      }
    }

    if (OB_SUCC(ret)) {
      const ObTableSchema *index_schema = NULL;
      if (knn_node->filter_nodes_.count() > 0 &&
          OB_FAIL(knn_node->children_.assign(knn_node->filter_nodes_))) {
        LOG_WARN("failed to assign children", K(ret));
      } else  if (OB_FAIL(schema_guard_->get_table_schema(index_id, index_schema))) {
        LOG_WARN("failed to get table schema", K(ret));
      } else if (OB_FAIL(index_schema->get_index_name(knn_node->index_name_))) {
        LOG_WARN("fail to get index name", KR(ret));
      }
    }
  }

  return ret;
}

int ObHybridSearchGenerator::get_vector_index_tid_from_expr(ObColumnRefRawExpr *field, uint64_t& vec_index_tid)
{
  int ret = OB_SUCCESS;
  bool vector_index_match = false;
  ObIndexType index_type;
  if (OB_FAIL(ObVectorIndexUtil::check_column_has_vector_index(*table_schema_,
                                                                *(schema_guard_->get_schema_guard()),
                                                                field->get_column_id(),
                                                                vector_index_match,
                                                                index_type))) {
    LOG_WARN("failed to check column has vector index", K(ret), K(field->get_column_id()), K(vector_index_match));
  } else if (vector_index_match) {
    if (OB_FAIL(ObVectorIndexUtil::get_vector_index_tid(schema_guard_->get_schema_guard(),
                                                        *table_schema_,
                                                        index_type,
                                                        field->get_column_id(),
                                                        true,
                                                        vec_index_tid))) {
      LOG_WARN("fail to get spec vector delta buffer table id", K(ret), K(field->get_column_id()), KPC(table_schema_));
    }
  }
  return ret;
}

int ObHybridSearchGenerator::generate_scalar_node(ObRawExpr *filter, ObScalarQueryNode *&scalar_node)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr *> rowkey_exprs;
  ObArray<ObRawExpr *> access_exprs;
  uint64_t index_id = OB_INVALID_ID;
  if (OB_ISNULL(plan_) || OB_ISNULL(table_item_) || OB_ISNULL(table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(plan_), K(table_item_), K(table_schema_));
  } else if (OB_ISNULL(scalar_node = OB_NEWx(ObScalarQueryNode, allocator_, *allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate hybrid search node", K(ret));
  } else if (OB_FAIL(scalar_node->filter_.push_back(filter))) {
    LOG_WARN("failed to append scalar filter", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(filter, access_exprs))) {
    LOG_WARN("extract column exprs failed", K(ret));
  } else if (OB_FAIL(plan_->get_rowkey_exprs(table_item_->table_id_, *table_schema_, rowkey_exprs))) {
    LOG_WARN("failed to get rowkey exprs", K(ret));
  } else if (OB_FAIL(append_array_no_dup(access_exprs, rowkey_exprs))) {
    LOG_WARN("append rowkey expr to access exprs failed", K(ret));
  } else if (OB_FAIL(scalar_node->index_table_rowkey_exprs_.assign(rowkey_exprs))) {
    LOG_WARN("assign rowkey exprs failed", K(ret));
  } else if (OB_FAIL(scalar_node->index_table_query_params_.access_exprs_.assign(access_exprs))) {
    LOG_WARN("assign access exprs failed", K(ret));
  } else if (OB_FAIL(scalar_node->pri_table_query_params_.access_exprs_.assign(access_exprs))) {
    LOG_WARN("assign access exprs failed", K(ret));
  } else if (OB_FAIL(table_schema_->get_rowkey_column_ids(scalar_node->pri_table_query_params_.output_col_ids_))) {
    LOG_WARN("fail to get rowkey column ids", K(ret));
  } else if (OB_FAIL(table_schema_->get_rowkey_column_ids(scalar_node->index_table_query_params_.output_col_ids_))) {
    LOG_WARN("fail to get rowkey column ids", K(ret));
  } else if (OB_FAIL(check_filter_has_index(filter, scalar_node->has_valid_index_, index_id))) {
    LOG_WARN("fail to check node has index", K(ret));
  } else if (OB_FAIL(scalar_node->pri_table_query_params_.pushdown_filters_.push_back(filter))) { // generate pushdown filters for primary table
    LOG_WARN("failed to push down filter", K(ret));
  }
  return ret;
}


int ObHybridSearchGenerator::generate_fulltext_node(
    const ObDSLFullTextQuery *fulltext_query,
    ObFullTextQueryNode *&fulltext_node)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(fulltext_query)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(fulltext_query));
  } else if (OB_FAIL(fulltext_query->produce_query_node(*table_schema_, *schema_guard_, *allocator_, fulltext_node))) {
    LOG_WARN("failed to produce fulltext query node", K(ret));
  }
  return ret;
}

int ObHybridSearchGenerator::check_filter_has_index(ObRawExpr *filter, bool &has_index, uint64_t &target_index_id)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 4> column_ids;
  has_index = false;
  target_index_id = OB_INVALID_ID;
  if (OB_FAIL(ObRawExprUtils::extract_column_ids(filter, column_ids))) {
    LOG_WARN("failed to extract column ids", K(filter), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < valid_index_ids_.count() && !has_index; ++i) {
      const uint64_t index_id = valid_index_ids_.at(i);
      const ObIArray<uint64_t> &index_column_ids = valid_index_cols_.at(i);
      const bool can_ignore_prefix = index_can_ignore_prefix_.at(i);
      if (ObOptimizerUtil::check_index_match_prefix(column_ids, index_column_ids, can_ignore_prefix)) {
        has_index = true;
        target_index_id = index_id;
      }
    }
  }
  return ret;
}

int ObHybridSearchGenerator::check_filter_has_search_index(ObRawExpr *filter, bool &has_search_index)
{
  int ret = OB_SUCCESS;
  has_search_index = false;
  ObSEArray<uint64_t, 4> column_ids;
  has_search_index = false;
  if (OB_FAIL(ObRawExprUtils::extract_column_ids(filter, column_ids))) {
    LOG_WARN("failed to extract column ids", K(filter), K(ret));
  } else if (column_ids.count() != 1) {
    // empty or multiple columns, can not use search index
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < valid_index_ids_.count() && !has_search_index; ++i) {
      const uint64_t index_id = valid_index_ids_.at(i);
      const ObIArray<uint64_t> &index_column_ids = valid_index_cols_.at(i);
      // search index can ignore prefix
      const bool can_ignore_prefix = index_can_ignore_prefix_.at(i);
      if (can_ignore_prefix && ObOptimizerUtil::check_index_match_prefix(
          column_ids, index_column_ids, true /* ignore prefix */)) {
        has_search_index = true;
      }
    }
  }
  return ret;
}

int ObHybridSearchGenerator::move_bool_nodes_to_children(ObIndexMergeNode* node)
{
  int ret = OB_SUCCESS;
  if (node->node_type_ == INDEX_MERGE_HYBRID_FUSION_SEARCH ||
      node->node_type_ == INDEX_MERGE_HYBRID_KNN) {
    for (int i = 0; i < node->children_.count() && OB_SUCC(ret); i++) {
      if (OB_FAIL(SMART_CALL(move_bool_nodes_to_children(node->children_.at(i))))) {
        LOG_WARN("failed to append node for child", K(ret), K(i));
      }
    }
  } else if (node->node_type_ == INDEX_MERGE_HYBRID_BOOLEAN_QUERY) {
    ObBooleanQueryNode *bool_node = static_cast<ObBooleanQueryNode*>(node);
    for (int i = 0; i < bool_node->must_nodes_.count() && OB_SUCC(ret); i++) {
      if (OB_FAIL(SMART_CALL(move_bool_nodes_to_children(bool_node->must_nodes_.at(i))))) {
        LOG_WARN("failed to append node for child", K(ret), K(i));
      }
    }
    for (int i = 0; i < bool_node->should_nodes_.count() && OB_SUCC(ret); i++) {
      if (OB_FAIL(SMART_CALL(move_bool_nodes_to_children(bool_node->should_nodes_.at(i))))) {
        LOG_WARN("failed to append node for child", K(ret), K(i));
      }
    }
    for (int i = 0; i < bool_node->filter_nodes_.count() && OB_SUCC(ret); i++) {
      if (OB_FAIL(SMART_CALL(move_bool_nodes_to_children(bool_node->filter_nodes_.at(i))))) {
        LOG_WARN("failed to append node for child", K(ret), K(i));
      }
    }
    for (int i = 0; i < bool_node->must_not_nodes_.count() && OB_SUCC(ret); i++) {
      if (OB_FAIL(SMART_CALL(move_bool_nodes_to_children(bool_node->must_not_nodes_.at(i))))) {
        LOG_WARN("failed to append node for child", K(ret), K(i));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(bool_node->children_.reserve(bool_node->must_nodes_.count() + bool_node->should_nodes_.count() +
                                                  bool_node->filter_nodes_.count() + bool_node->must_not_nodes_.count()))) {
      LOG_WARN("failed to reserve children", K(ret));
    } else if (OB_FAIL(common::append(bool_node->children_, bool_node->must_nodes_))) {
      LOG_WARN("failed to append must nodes", K(ret));
    } else if (OB_FAIL(common::append(bool_node->children_, bool_node->should_nodes_))) {
      LOG_WARN("failed to append should nodes", K(ret));
    } else if (OB_FAIL(common::append(bool_node->children_, bool_node->filter_nodes_))) {
      LOG_WARN("failed to append filter nodes", K(ret));
    } else if (OB_FAIL(common::append(bool_node->children_, bool_node->must_not_nodes_))) {
      LOG_WARN("failed to append must not nodes", K(ret));
    }
  }
  return ret;
}

/**
 * @brief ObHybridSearchGenerator::try_merge_nodes
 *
 * For simple condition and range condition, if there is another child node which
 * contains the same column, we can merge the new filter into the child directly.
 */
int ObHybridSearchGenerator::try_merge_nodes(const ObDSLQuery *query, ObIArray<ObIndexMergeNode*> &nodes, bool &merge_happened)
{
  int ret = OB_SUCCESS;
  const ObDSLScalarQuery *scalar_query = static_cast<const ObDSLScalarQuery *>(query);
  ObSEArray<uint64_t, 4> filter_column_ids;
  merge_happened = false;
  // first check if the query is allowed to be merged
  if (query->need_cal_score_ ||
      (query->query_type_ != QUERY_ITEM_TERM && query->query_type_ != QUERY_ITEM_RANGE)) {
    // do nothing
  } else if (!scalar_query->scalar_expr_->has_flag(IS_SIMPLE_COND) &&
             !scalar_query->scalar_expr_->has_flag(IS_RANGE_COND) &&
             !in_merge_expr_whitelist(scalar_query->scalar_expr_)) {
    // only simple condition and range condition can be merged
    LOG_TRACE("only simple condition and range condition can be merged", KPC(scalar_query->scalar_expr_));
  } else if (OB_FAIL(ObRawExprUtils::extract_column_ids(scalar_query->scalar_expr_, filter_column_ids))) {
    LOG_WARN("failed to extract column ids", K(scalar_query->scalar_expr_), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < nodes.count(); i++) {
      ObScalarQueryNode *scalar_node = nullptr;
      const ObRawExpr *child_filter = nullptr;
      const ObRawExpr *sub_filter = nullptr;
      ObSEArray<uint64_t, 4> child_column_ids;
      if (!nodes.at(i)->is_hybrid_scalar_node()) {
      } else if ((OB_ISNULL(scalar_node = static_cast<ObScalarQueryNode *>(nodes.at(i))))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null scalar node", K(ret));
      } else if (OB_UNLIKELY(scalar_node->filter_.empty()) || OB_ISNULL(child_filter = scalar_node->filter_.at(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected child node filter", K(ret));
      } else if (child_filter->get_expr_type() != T_OP_AND) {
        sub_filter = child_filter;
      } else if (child_filter->get_param_count() < 2) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected child node filter", K(ret));
      } else if (OB_ISNULL(sub_filter = child_filter->get_param_expr(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null sub filter", K(ret));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(sub_filter)) {
      } else if (!IS_RANGE_CMP_OP(sub_filter->get_expr_type()) && sub_filter->get_expr_type() != T_OP_EQ) {
        // do nothing, only merge simple condition and range condition
      } else if (OB_FAIL(ObRawExprUtils::extract_column_ids(sub_filter, child_column_ids))) {
        LOG_WARN("failed to extract column ids", K(ret), KPC(sub_filter));
      } else if (is_array_equal(filter_column_ids, child_column_ids)) {
        // merge if any one of the conditions is satisfied:
        // (1) both filters have no whitelist expression, meaning they have the same simple column reference
        // (2) both filters have the same whitelist expression
        const ObRawExpr *whitelist_expr1 = nullptr;
        const ObRawExpr *whitelist_expr2 = nullptr;
        if (ObDSLResolver::in_merge_node_whitelist(scalar_query->field_)) {
          whitelist_expr1 = scalar_query->field_;
        }
        for (int64_t i = 0 ; OB_SUCC(ret) && i < sub_filter->get_param_count(); ++i) {
          const ObRawExpr *param_expr = sub_filter->get_param_expr(i);
          if (OB_ISNULL(param_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null param expr", K(ret));
          } else if (ObDSLResolver::in_merge_node_whitelist(param_expr)) {
            whitelist_expr2 = param_expr;
            break;
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_ISNULL(whitelist_expr1) && OB_ISNULL(whitelist_expr2)) {
            merge_happened = true;
          } else if (OB_NOT_NULL(whitelist_expr1) && OB_NOT_NULL(whitelist_expr2) &&
                     whitelist_expr1->same_as(*whitelist_expr2)) {
            merge_happened = true;
          }
          if (!merge_happened) {
          } else if (OB_FAIL(nodes.at(i)->filter_.push_back(scalar_query->scalar_expr_))) {
            LOG_WARN("failed to push back filter", K(ret));
          // after merging into an existing scalar node, we need to push the new filter condition down to the primary table.
          // this ensures that the condition is properly applied during the scalar query execution.
          } else if (OB_FAIL(scalar_node->pri_table_query_params_.pushdown_filters_.push_back(scalar_query->scalar_expr_))) {
            LOG_WARN("failed to push back pushdown filter", K(ret));
          }
        }
      }
    }
  }
  return ret;
}


int ObHybridSearchGenerator::generate_boolean_node(const ObDSLBoolQuery *dsl_query, ObBooleanQueryNode *&bool_node)
{
  int ret = OB_SUCCESS;
  bool merge_happened = false;
  if (OB_ISNULL(dsl_query)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr");
  } else if (OB_ISNULL(bool_node = OB_NEWx(ObBooleanQueryNode, allocator_, *allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate hybrid search node", K(ret));
  }
  for (int i = 0; i < dsl_query->must_.count() && OB_SUCC(ret); i++) {
    ObIndexMergeNode *sub_node = NULL;
    bool merge_happened = false;
    if (OB_FAIL(try_merge_nodes(dsl_query->must_.at(i), bool_node->must_nodes_, merge_happened))) {
      LOG_WARN("failed to merge bool node filters", K(ret));
    } else if (merge_happened) {
      // do nothing
    } else if (OB_FAIL(generate_node(dsl_query->must_.at(i), sub_node))) {
      LOG_WARN("failed to generate hybrid search node", K(ret));
    } else if (OB_FAIL(bool_node->must_nodes_.push_back(sub_node))) {
      LOG_WARN("failed to generate hybrid search node", K(ret));
    }
  }
  for (int i = 0; i < dsl_query->should_.count() && OB_SUCC(ret); i++) {
    ObIndexMergeNode *sub_node = NULL;
    if (OB_FAIL(generate_node(dsl_query->should_.at(i), sub_node))) {
      LOG_WARN("failed to generate hybrid search node", K(ret));
    } else if (OB_FAIL(bool_node->should_nodes_.push_back(sub_node))) {
      LOG_WARN("failed to generate hybrid search node", K(ret));
    }
  }
  for (int i = 0; i < dsl_query->filter_.count() && OB_SUCC(ret); i++) {
    ObIndexMergeNode *sub_node = NULL;
    bool merge_happened = false;
    if (OB_FAIL(try_merge_nodes(dsl_query->filter_.at(i), bool_node->filter_nodes_, merge_happened))) {
      LOG_WARN("failed to merge bool node filters", K(ret));
    } else if (merge_happened) {
      // do nothing
    } else if (OB_FAIL(generate_node(dsl_query->filter_.at(i), sub_node))) {
      LOG_WARN("failed to generate hybrid search node", K(ret));
    } else if (OB_FAIL(bool_node->filter_nodes_.push_back(sub_node))) {
      LOG_WARN("failed to generate hybrid search node", K(ret));
    }
  }
  for (int i = 0; i < dsl_query->must_not_.count() && OB_SUCC(ret); i++) {
    ObIndexMergeNode *sub_node = NULL;
    if (OB_FAIL(generate_node(dsl_query->must_not_.at(i), sub_node))) {
      LOG_WARN("failed to generate hybrid search node", K(ret));
    } else if (OB_FAIL(bool_node->must_not_nodes_.push_back(sub_node))) {
      LOG_WARN("failed to generate hybrid search node", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    bool_node->min_should_match_ = dsl_query->msm_;
    bool_node->origin_expr_ = dsl_query->origin_expr_;
  }

  return ret;
}

int ObHybridSearchNodeBase::get_index_name(ObIArray<ObString> &index_names)
{
  int ret = OB_SUCCESS;
  if (node_type_ == INDEX_MERGE_HYBRID_SCALAR_QUERY) {
    ObScalarQueryNode *scalar_node = static_cast<ObScalarQueryNode *>(this);
    if (scalar_node->is_hybrid_scalar_node_with_index()) {
       ObIndexMergeNode* node = static_cast<ObIndexMergeNode *>(this);
      if (!index_name_.empty() && OB_FAIL(index_names.push_back(index_name_))) {
        LOG_WARN("failed to append node", K(ret));
      }
    }
  } else if (node_type_ == INDEX_MERGE_HYBRID_FTS_QUERY) {
    ObFullTextQueryNode *fulltext_node = static_cast<ObFullTextQueryNode *>(this);
    switch (fulltext_node->query_type_) {
      case ObFullTextQueryNode::QueryType::OB_FULLTEXT_QUERY_TYPE_MATCH:
      case ObFullTextQueryNode::QueryType::OB_FULLTEXT_QUERY_TYPE_MATCH_PHRASE: {
        if (!index_name_.empty()) {
          bool exists = false;
          for (int64_t i = 0; !exists && i < index_names.count(); ++i) {
            if (index_names.at(i) == fulltext_node->index_name_) {
              exists = true;
            }
          }
          if (!exists && OB_FAIL(index_names.push_back(fulltext_node->index_name_))) {
            LOG_WARN("failed to append fts index name", K(ret));
          }
        }
        break;
      }
      case ObFullTextQueryNode::QueryType::OB_FULLTEXT_QUERY_TYPE_MULTI_MATCH: {
        ObMultiMatchQueryNode *multi_match_node = static_cast<ObMultiMatchQueryNode *>(this);
        for (int64_t i = 0; OB_SUCC(ret) && i < multi_match_node->index_names_.count(); ++i) {
          if (!multi_match_node->index_names_.at(i).empty()) {
            bool exists = false;
            for (int64_t j = 0; !exists && j < index_names.count(); ++j) {
              if (multi_match_node->index_names_.at(i) == index_names.at(j)) {
                exists = true;
              }
            }
            if (!exists && OB_FAIL(index_names.push_back(multi_match_node->index_names_.at(i)))) {
              LOG_WARN("failed to append fts index name", K(ret));
            }
          }
        }
        break;
      }
      case ObFullTextQueryNode::QueryType::OB_FULLTEXT_QUERY_TYPE_QUERY_STRING: {
        ObQueryStringQueryNode *query_string_node = static_cast<ObQueryStringQueryNode *>(this);
        for (int64_t i = 0; OB_SUCC(ret) && i < query_string_node->index_names_.count(); ++i) {
          if (!query_string_node->index_names_.at(i).empty()) {
            bool exists = false;
            for (int64_t j = 0; !exists && j < index_names.count(); ++j) {
              if (query_string_node->index_names_.at(i) == index_names.at(j)) {
                exists = true;
              }
            }
            if (!exists && OB_FAIL(index_names.push_back(query_string_node->index_names_.at(i)))) {
              LOG_WARN("failed to append fts index name", K(ret));
            }
          }
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected fulltext query type", K(ret), K_(fulltext_node->query_type));
        break;
      }
    }
  } else if (node_type_ == INDEX_MERGE_HYBRID_KNN) {
    if (!index_name_.empty() && OB_FAIL(index_names.push_back(index_name_))) {
      LOG_WARN("failed to append node", K(ret));
    }
    ObVecSearchNode *vec_node = static_cast<ObVecSearchNode *>(this);
    for (int i = 0; i < vec_node->filter_nodes_.count() && OB_SUCC(ret); i++) {
      ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(vec_node->filter_nodes_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->get_index_name(index_names)))) {
        LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
      }
    }
  } else if (node_type_ == INDEX_MERGE_HYBRID_FUSION_SEARCH) {
    for (int i = 0; i < children_.count() && OB_SUCC(ret); i++) {
      ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(children_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->get_index_name(index_names)))) {
        LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
      }
    }
  } else if (node_type_ == INDEX_MERGE_HYBRID_BOOLEAN_QUERY) {
    ObBooleanQueryNode *bool_node = static_cast<ObBooleanQueryNode*>(this);
    for (int i = 0; i < bool_node->must_nodes_.count() && OB_SUCC(ret); i++) {
      ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(bool_node->must_nodes_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->get_index_name(index_names)))) {
        LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
      }
    }
    for (int i = 0; i < bool_node->should_nodes_.count() && OB_SUCC(ret); i++) {
      ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(bool_node->should_nodes_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->get_index_name(index_names)))) {
        LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
      }
    }
    for (int i = 0; i < bool_node->filter_nodes_.count() && OB_SUCC(ret); i++) {
      ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(bool_node->filter_nodes_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->get_index_name(index_names)))) {
        LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
      }
    }
    for (int i = 0; i < bool_node->must_not_nodes_.count() && OB_SUCC(ret); i++) {
      ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(bool_node->must_not_nodes_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->get_index_name(index_names)))) {
        LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
      }
    }
  }
  return ret;
}


int ObHybridSearchNodeBase::get_all_scan_node(ObIArray<ObIndexMergeNode*> &scan_nodes)
{
  int ret = OB_SUCCESS;
  if (node_type_ == INDEX_MERGE_HYBRID_SCALAR_QUERY) {
    ObScalarQueryNode *scalar_node = static_cast<ObScalarQueryNode *>(this);
    if (scalar_node->is_hybrid_scalar_node_with_index()) {
       ObIndexMergeNode* node = static_cast<ObIndexMergeNode *>(this);
      if (OB_FAIL(scan_nodes.push_back(node))) {
        LOG_WARN("failed to append node", K(ret));
      }
    }
  } else if (node_type_ == INDEX_MERGE_HYBRID_FUSION_SEARCH) {
    for (int i = 0; i < children_.count() && OB_SUCC(ret); i++) {
      ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(children_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->get_all_scan_node(scan_nodes)))) {
        LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
      }
    }
  } else if (node_type_ == INDEX_MERGE_HYBRID_BOOLEAN_QUERY) {
    ObBooleanQueryNode *bool_node = static_cast<ObBooleanQueryNode*>(this);
    for (int i = 0; i < bool_node->must_nodes_.count() && OB_SUCC(ret); i++) {
      ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(bool_node->must_nodes_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->get_all_scan_node(scan_nodes)))) {
        LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
      }
    }
    for (int i = 0; i < bool_node->should_nodes_.count() && OB_SUCC(ret); i++) {
      ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(bool_node->should_nodes_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->get_all_scan_node(scan_nodes)))) {
        LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
      }
    }
    for (int i = 0; i < bool_node->filter_nodes_.count() && OB_SUCC(ret); i++) {
      ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(bool_node->filter_nodes_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->get_all_scan_node(scan_nodes)))) {
        LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
      }
    }
    for (int i = 0; i < bool_node->must_not_nodes_.count() && OB_SUCC(ret); i++) {
      ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(bool_node->must_not_nodes_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->get_all_scan_node(scan_nodes)))) {
        LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
      }
    }
  } else if (node_type_ == INDEX_MERGE_HYBRID_KNN) {
    ObVecSearchNode *vec_node = static_cast<ObVecSearchNode *>(this);
    for (int i = 0; i < vec_node->filter_nodes_.count() && OB_SUCC(ret); i++) {
      ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(vec_node->filter_nodes_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->get_all_scan_node(scan_nodes)))) {
        LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
      }
    }
  }
  return ret;
}

int ObHybridSearchNodeBase::collect_hybrid_search_exprs(ObIArray<ObRawExpr*> &query_exprs, ObIArray<ObRawExpr*> &score_exprs) const
{
  int ret = OB_SUCCESS;
  if (node_type_ == INDEX_MERGE_HYBRID_SCALAR_QUERY) {
    const ObScalarQueryNode *scalar_node = static_cast<const ObScalarQueryNode *>(this);
    if (OB_FAIL(append_array_no_dup(query_exprs, scalar_node->filter_))) {
      LOG_WARN("failed to append filter", K(ret));
    } else if (OB_FAIL(append_array_no_dup(query_exprs, scalar_node->index_table_query_params_.access_exprs_))) {
      LOG_WARN("append rowkey expr to access exprs failed", K(ret));
    } else if (OB_FAIL(append_array_no_dup(query_exprs, scalar_node->pri_table_query_params_.access_exprs_))) {
      LOG_WARN("append rowkey expr to access exprs failed", K(ret));
    } else if (OB_FAIL(append_array_no_dup(query_exprs, scalar_node->pri_table_query_params_.pushdown_filters_))) {
      LOG_WARN("append pushdown filters to access exprs failed", K(ret));
    }
  } else if (node_type_ == INDEX_MERGE_HYBRID_FTS_QUERY) {
    const ObFullTextQueryNode *fulltext_node = static_cast<const ObFullTextQueryNode *>(this);
    if (OB_FAIL(fulltext_node->collect_exprs(query_exprs, score_exprs))) {
      LOG_WARN("failed to collect exprs", K(ret));
    }
  } else if (node_type_ == INDEX_MERGE_HYBRID_KNN) {
    const ObVecSearchNode *vec_node = static_cast<const ObVecSearchNode *>(this);
    ObRawExpr *col_expr = static_cast<ObRawExpr *>(vec_node->field_);
    if (OB_FAIL(add_var_to_array_no_dup(query_exprs, col_expr))) {
      LOG_WARN("failed to append expr", K(ret));
    } else {
      for (int i = 0; i < vec_node->filter_nodes_.count() && OB_SUCC(ret); i++) {
        const ObHybridSearchNodeBase *child_node = static_cast<const ObHybridSearchNodeBase *>(vec_node->filter_nodes_.at(i));
        if (OB_FAIL(SMART_CALL(child_node->collect_hybrid_search_exprs(query_exprs, score_exprs)))) {
          LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
        }
      }
      if (OB_SUCC(ret) && OB_NOT_NULL(vec_node->extracted_scalar_node_)) {
        if (OB_FAIL(SMART_CALL(vec_node->extracted_scalar_node_->collect_hybrid_search_exprs(query_exprs, score_exprs)))) {
          LOG_WARN("failed to collect exprs from extracted scalar node", K(ret));
        }
      }
    }
  } else if (node_type_ == INDEX_MERGE_HYBRID_FUSION_SEARCH) {
    const ObFusionNode *fusion_node = static_cast<const ObFusionNode *>(this);
    if (OB_FAIL(add_var_to_array_no_dup(query_exprs, fusion_node->from_))) {
      LOG_WARN("failed to append expr", K(ret));
    } else if (OB_FAIL(add_var_to_array_no_dup(query_exprs, fusion_node->size_))) {
      LOG_WARN("failed to append expr", K(ret));
    } else if (OB_FAIL(add_var_to_array_no_dup(query_exprs, fusion_node->min_score_))) {
      LOG_WARN("failed to append expr", K(ret));
    } else if (OB_FAIL(add_var_to_array_no_dup(query_exprs, fusion_node->window_size_))) {
      LOG_WARN("failed to append expr", K(ret));
    } else if (OB_NOT_NULL(fusion_node->rank_const_) &&
               OB_FAIL(add_var_to_array_no_dup(query_exprs, fusion_node->rank_const_))) {
      LOG_WARN("failed to append expr", K(ret));
    } else if (OB_FAIL(append_array_no_dup(query_exprs, fusion_node->rowkey_cols_))) {
      LOG_WARN("failed to append expr", K(ret));
    } else if (OB_FAIL(append_array_no_dup(score_exprs, fusion_node->score_cols_))) {
      LOG_WARN("failed to append expr", K(ret));
    } else if (OB_FAIL(append_array_no_dup(query_exprs, fusion_node->weight_cols_))) {
      LOG_WARN("failed to append weight_cols_", K(ret));
    } else if (fusion_node->is_top_k_query_ && OB_FAIL(append_array_no_dup(query_exprs, fusion_node->path_top_k_limit_))) {
      LOG_WARN("failed to append path_top_k_limit_", K(ret));
    }
    for (int i = 0; i < children_.count() && OB_SUCC(ret); i++) {
      const ObHybridSearchNodeBase *child_node = static_cast<const ObHybridSearchNodeBase *>(children_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->collect_hybrid_search_exprs(query_exprs, score_exprs)))) {
        LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
      }
    }
  } else if (node_type_ == INDEX_MERGE_HYBRID_BOOLEAN_QUERY) {
    const ObBooleanQueryNode *bool_node = static_cast<const ObBooleanQueryNode*>(this);
    for (int i = 0; i < bool_node->must_nodes_.count() && OB_SUCC(ret); i++) {
      const ObHybridSearchNodeBase *child_node = static_cast<const ObHybridSearchNodeBase *>(bool_node->must_nodes_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->collect_hybrid_search_exprs(query_exprs, score_exprs)))) {
        LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
      }
    }
    for (int i = 0; i < bool_node->should_nodes_.count() && OB_SUCC(ret); i++) {
      const ObHybridSearchNodeBase *child_node = static_cast<const ObHybridSearchNodeBase *>(bool_node->should_nodes_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->collect_hybrid_search_exprs(query_exprs, score_exprs)))) {
        LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
      }
    }
    for (int i = 0; i < bool_node->filter_nodes_.count() && OB_SUCC(ret); i++) {
      const ObHybridSearchNodeBase *child_node = static_cast<const ObHybridSearchNodeBase *>(bool_node->filter_nodes_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->collect_hybrid_search_exprs(query_exprs, score_exprs)))) {
        LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
      }
    }
    for (int i = 0; i < bool_node->must_not_nodes_.count() && OB_SUCC(ret); i++) {
      const ObHybridSearchNodeBase *child_node = static_cast<const ObHybridSearchNodeBase *>(bool_node->must_not_nodes_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->collect_hybrid_search_exprs(query_exprs, score_exprs)))) {
        LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
      }
    }
  }

  if (OB_SUCC(ret) && node_type_ != INDEX_MERGE_HYBRID_FUSION_SEARCH) {
    if (OB_ISNULL(boost_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("boost_ is null", K(ret));
    } else if (OB_FAIL(add_var_to_array_no_dup(query_exprs, static_cast<ObRawExpr *>(boost_)))) {
      LOG_WARN("failed to append boost expr", K(ret));
    }
  }

  return ret;
}

int ObHybridSearchNodeBase::generate_access_exprs(const TableItem &table_item, ObOptimizerContext &opt_ctx)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < children_.count() && OB_SUCC(ret); i++) {
    ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(children_.at(i));
    if (OB_ISNULL(child_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null child node", K(ret));
    } else if (OB_FAIL(SMART_CALL(child_node->generate_access_exprs(table_item, opt_ctx)))) {
      LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
    }
  }
  return ret;
}

int ObHybridSearchNodeBase::collect_exprs(ObIArray<ObRawExpr*> &query_exprs, ObIArray<ObRawExpr*> &score_exprs) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < children_.count() && OB_SUCC(ret); i++) {
    const ObHybridSearchNodeBase *child_node = static_cast<const ObHybridSearchNodeBase *>(children_.at(i));
    if (OB_ISNULL(child_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null child node", K(ret));
    } else if (OB_FAIL(SMART_CALL(child_node->collect_exprs(query_exprs, score_exprs)))) {
      LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
    }
  }
  return ret;
}

int ObHybridSearchNodeBase::get_all_index_ids(ObIArray<uint64_t> &index_ids) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < children_.count() && OB_SUCC(ret); i++) {
    const ObHybridSearchNodeBase *child_node = static_cast<const ObHybridSearchNodeBase *>(children_.at(i));
    if (OB_ISNULL(child_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null child node", K(ret));
    } else if (OB_FAIL(SMART_CALL(child_node->get_all_index_ids(index_ids)))) {
      LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
    }
  }
  return ret;
}

int ObHybridSearchNodeBase::collect_vec_infos(ObIArray<ObVecIndexInfo *> &vec_infos)
{
  int ret = OB_SUCCESS;
  if (node_type_ == INDEX_MERGE_HYBRID_KNN) {
    ObVecSearchNode *vec_node = static_cast<ObVecSearchNode *>(this);
    if (OB_FAIL(add_var_to_array_no_dup(vec_infos, &vec_node->vec_info_))) {
      LOG_WARN("failed to append vec info", K(ret));
    }
    for (int i = 0; i < vec_node->filter_nodes_.count() && OB_SUCC(ret); i++) {
      ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(vec_node->filter_nodes_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->collect_vec_infos(vec_infos)))) {
        LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
      }
    }
  } else if (node_type_ == INDEX_MERGE_HYBRID_FUSION_SEARCH) {
    for (int i = 0; i < children_.count() && OB_SUCC(ret); i++) {
      ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(children_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->collect_vec_infos(vec_infos)))) {
        LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
      }
    }
  } else if (node_type_ == INDEX_MERGE_HYBRID_BOOLEAN_QUERY) {
    ObBooleanQueryNode *bool_node = static_cast<ObBooleanQueryNode*>(this);
    for (int i = 0; i < bool_node->must_nodes_.count() && OB_SUCC(ret); i++) {
      ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(bool_node->must_nodes_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->collect_vec_infos(vec_infos)))) {
        LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
      }
    }
    for (int i = 0; i < bool_node->should_nodes_.count() && OB_SUCC(ret); i++) {
      ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(bool_node->should_nodes_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->collect_vec_infos(vec_infos)))) {
        LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
      }
    }
    for (int i = 0; i < bool_node->filter_nodes_.count() && OB_SUCC(ret); i++) {
      ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(bool_node->filter_nodes_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->collect_vec_infos(vec_infos)))) {
        LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
      }
    }
    for (int i = 0; i < bool_node->must_not_nodes_.count() && OB_SUCC(ret); i++) {
      ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(bool_node->must_not_nodes_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->collect_vec_infos(vec_infos)))) {
        LOG_WARN("failed to append node for child", K(ret), K(i), KPC(child_node));
      }
    }
  }

  return ret;
}

int ObVecSearchNode::init_vec_info(ObSqlSchemaGuard *schema_guard)
{
  int ret = OB_SUCCESS;
  if (ap_ != NULL) {
    vec_info_.main_table_tid_ = table_schema_->get_table_id();
    vec_info_.vec_type_ = ObVecIndexType::VEC_INDEX_INVALID;
    vec_info_.selectivity_ = ap_->vec_idx_info_.vec_extra_info_.get_selectivity();
    vec_info_.row_count_ = ap_->vec_idx_info_.vec_extra_info_.get_row_count();
    vec_info_.set_can_use_vec_pri_opt(ap_->vec_idx_info_.vec_extra_info_.can_use_vec_pri_opt());
    vec_info_.vector_index_param_ = ap_->vec_idx_info_.vec_extra_info_.get_vector_index_param();
    vec_info_.adaptive_try_path_ = ap_->vec_idx_info_.vec_extra_info_.adaptive_try_path_;
    vec_info_.can_extract_range_ = ap_->vec_idx_info_.vec_extra_info_.can_extract_range_;
    vec_info_.is_spatial_index_ =  ap_->vec_idx_info_.vec_extra_info_.is_spatial_index_;
    vec_info_.is_multi_value_index_ = ap_->vec_idx_info_.vec_extra_info_.is_multi_value_index_;
    bool is_hybrid = false;
    if (search_option_ != nullptr) {
      vec_info_.query_param_ = search_option_->param_;
      vec_info_.knn_filter_mode_ = search_option_->filter_mode_;
      if (search_option_->filter_mode_ == ObKnnFilterMode::PRE_ADAPTIVE ||
          search_option_->filter_mode_ == ObKnnFilterMode::PRE_KNN ||
          search_option_->filter_mode_ == ObKnnFilterMode::PRE_BRUTE_FORCE) {
        vec_info_.vec_type_ = VEC_INDEX_PRE;
      } else if (search_option_->filter_mode_ == ObKnnFilterMode::POST_FILTER ||
                 search_option_->filter_mode_ == ObKnnFilterMode::POST_INDEX_MERGE) {
        vec_info_.vec_type_ = VEC_INDEX_POST_ITERATIVE_FILTER;
      }
    }
    if (vec_info_.is_hnsw_vec_scan()) {
      uint64_t vid_rowkey_tid = OB_INVALID_ID;
      uint64_t rowkey_vid_tid = OB_INVALID_ID;
      uint64_t delta_buffer_tid = OB_INVALID_ID; // hybrid index log table when is_hybrid is true
      uint64_t index_id_tid = OB_INVALID_ID;
      uint64_t index_snapshot_data_tid = OB_INVALID_ID;
      uint64_t hybrid_index_embedded_tid = OB_INVALID_ID;
      uint64_t vec_idx_tid = main_index_table_id_;
      if (FAILEDx(ObVectorIndexUtil::get_index_tids_for_hnsw_by_prefix(schema_guard->get_schema_guard(),
                                                                                   *table_schema_, // data table schema
                                                                                   vec_idx_tid,
                                                                                   delta_buffer_tid,
                                                                                   index_id_tid,
                                                                                   index_snapshot_data_tid,
                                                                                   hybrid_index_embedded_tid,
                                                                                   is_hybrid))) {
        LOG_WARN("fail to get index tids for hnsw by prefix", K(ret), K(vec_idx_tid), K(*table_schema_));
      } else if (delta_buffer_tid == OB_INVALID_ID || index_id_tid == OB_INVALID_ID || index_snapshot_data_tid == OB_INVALID_ID) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to init aux table id", K(delta_buffer_tid), K(index_id_tid), K(index_snapshot_data_tid), K(ret));
      /* do not change push order, should be same as ObVectorAuxTableIdx */
      } else if (OB_FAIL(vec_info_.aux_table_id_.push_back(delta_buffer_tid))) {
        LOG_WARN("fail to push back aux table id", K(ret), K(delta_buffer_tid), K(vec_info_.aux_table_id_.count()));
      } else if (OB_FAIL(vec_info_.aux_table_id_.push_back(index_id_tid))) {
        LOG_WARN("fail to push back aux table id", K(ret), K(index_id_tid), K(vec_info_.aux_table_id_.count()));
      } else if (OB_FAIL(vec_info_.aux_table_id_.push_back(index_snapshot_data_tid))) {
        LOG_WARN("fail to push back aux table id", K(ret), K(index_snapshot_data_tid), K(vec_info_.aux_table_id_.count()));
      } else if (OB_INVALID_ID != rowkey_vid_tid && OB_FAIL(vec_info_.aux_table_id_.push_back(rowkey_vid_tid))) {
        LOG_WARN("fail to push back aux table id", K(ret), K(rowkey_vid_tid), K(vec_info_.aux_table_id_.count()));
      } else if (OB_INVALID_ID != vid_rowkey_tid && OB_FAIL(vec_info_.aux_table_id_.push_back(vid_rowkey_tid))) {
        LOG_WARN("fail to push back aux table id", K(ret), K(vid_rowkey_tid), K(vec_info_.aux_table_id_.count()));
      } else if (is_hybrid && OB_FAIL(vec_info_.aux_table_id_.push_back(hybrid_index_embedded_tid))) {
        LOG_WARN("fail to push back aux table id", K(ret), K(hybrid_index_embedded_tid), K(vec_info_.aux_table_id_.count()));
      }
    }
  }
  return ret;
}

int ObVecSearchNode::get_delta_buf_table_params(VecIndexScanParams &params) const
{
  int ret = OB_SUCCESS;
  if (!vec_info_.is_hnsw_vec_scan()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("delta buf table params only for HNSW", K(ret));
  } else {
    params.table_id_ = vec_info_.get_aux_table_id(ObVectorAuxTableIdx::VEC_FIRST_AUX_TBL_IDX);
    params.scan_type_ = ObTSCIRScanType::OB_VEC_DELTA_BUF_SCAN;
    params.access_exprs_.reset();
    if (OB_FAIL(params.access_exprs_.push_back(vec_info_.get_aux_table_column(ObVectorHNSWColumnIdx::HNSW_DELTA_VID_COL)))) {
      LOG_WARN("failed to add delta vid column", K(ret));
    } else if (OB_FAIL(params.access_exprs_.push_back(vec_info_.get_aux_table_column(ObVectorHNSWColumnIdx::HNSW_DELTA_TYPE_COL)))) {
      LOG_WARN("failed to add delta type column", K(ret));
    }
  }
  return ret;
}

int ObVecSearchNode::get_index_id_table_params(VecIndexScanParams &params) const
{
  int ret = OB_SUCCESS;
  if (!vec_info_.is_hnsw_vec_scan()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index id table params only for HNSW", K(ret));
  } else {
    params.table_id_ = vec_info_.get_aux_table_id(ObVectorAuxTableIdx::VEC_SECOND_AUX_TBL_IDX);
    params.scan_type_ = ObTSCIRScanType::OB_VEC_IDX_ID_SCAN;

    params.access_exprs_.reset();
    if (OB_FAIL(params.access_exprs_.push_back(vec_info_.get_aux_table_column(ObVectorHNSWColumnIdx::HNSW_INDEX_ID_SCN_COL)))) {
      LOG_WARN("failed to add index id scn column", K(ret));
    } else if (OB_FAIL(params.access_exprs_.push_back(vec_info_.get_aux_table_column(ObVectorHNSWColumnIdx::HNSW_INDEX_ID_VID_COL)))) {
      LOG_WARN("failed to add index id vid column", K(ret));
    } else if (OB_FAIL(params.access_exprs_.push_back(vec_info_.get_aux_table_column(ObVectorHNSWColumnIdx::HNSW_INDEX_ID_TYPE_COL)))) {
      LOG_WARN("failed to add index id type column", K(ret));
    }
  }
  return ret;
}

int ObVecSearchNode::get_snapshot_table_params(VecIndexScanParams &params) const
{
  int ret = OB_SUCCESS;
  if (!vec_info_.is_hnsw_vec_scan()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("snapshot table params only for HNSW", K(ret));
  } else {
    params.table_id_ = vec_info_.get_aux_table_id(ObVectorAuxTableIdx::VEC_THIRD_AUX_TBL_IDX);
    params.scan_type_ = ObTSCIRScanType::OB_VEC_SNAPSHOT_SCAN;
    params.access_exprs_.reset();
    if (OB_FAIL(params.access_exprs_.push_back(vec_info_.get_aux_table_column(ObVectorHNSWColumnIdx::HNSW_SNAPSHOT_KEY_COL)))) {
      LOG_WARN("failed to add snapshot key column", K(ret));
    } else if (OB_FAIL(params.access_exprs_.push_back(vec_info_.get_aux_table_column(ObVectorHNSWColumnIdx::HNSW_SNAPSHOT_DATA_COL)))) {
      LOG_WARN("failed to add snapshot data column", K(ret));
    } else if (vec_info_.has_get_visible_column() &&
               OB_FAIL(params.access_exprs_.push_back(vec_info_.get_aux_table_column(ObVectorHNSWColumnIdx::HNSW_SNAPSHOT_VISIBLE_COL)))) {
      LOG_WARN("failed to add snapshot visible column", K(ret));
    }
  }
  return ret;
}

int ObVecSearchNode::get_com_aux_vec_table_params(VecIndexScanParams &params) const
{
  int ret = OB_SUCCESS;
  if (!vec_info_.is_hnsw_vec_scan() && !vec_info_.is_ivf_vec_scan()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("com aux vec table params only for HNSW or IVF", K(ret));
  } else {
    params.table_id_ = vec_info_.main_table_tid_;
    params.scan_type_ = ObTSCIRScanType::OB_VEC_COM_AUX_SCAN;
    params.access_exprs_.reset();
    if (OB_ISNULL(vec_info_.target_vec_column_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("target vec column is null", K(ret));
    } else if (OB_FAIL(params.access_exprs_.push_back(vec_info_.target_vec_column_))) {
      LOG_WARN("failed to add target vec column", K(ret));
    }
  }
  return ret;
}

int ObHybridSearchNodeBase::print_blank_space(char *buf, int64_t buf_len, int64_t &pos, int n)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(BUF_PRINTF("\n"))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  }
  for (int i = 0; i < n && OB_SUCC(ret); i++) {
    if (OB_FAIL(BUF_PRINTF(" "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    }
  }
  return ret;
}

int ObFusionNode::explain_info(char *buf, int64_t buf_len, int64_t &pos, ExplainType type, int blank_space_count)
{
  int ret = OB_SUCCESS;
  if (is_top_k_query_) {
    ObRawExpr *limit = size_;
    ObRawExpr *window_size = window_size_;
    ObRawExpr *min_score = min_score_;
    ObRawExpr *rank_const = rank_const_;
    const char *method_str = nullptr;
    if (method_ == ObFusionMethod::WEIGHT_SUM) {
      method_str = "WEIGHT_SUM";
    } else if (method_ == ObFusionMethod::RRF) {
      method_str = "RRF";
    } else if (method_ == ObFusionMethod::MINMAX_NORMALIZER) {
      method_str = "MINMAX_NORMALIZER";
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected fusion method", K(ret), K(method_));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(BUF_PRINTF("fusion node:"))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF(" method=%s", method_str))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF(", "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (FALSE_IT(EXPLAIN_PRINT_EXPR(limit, type))) {
    } else if (OB_FAIL(BUF_PRINTF(", "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (FALSE_IT(EXPLAIN_PRINT_EXPR(window_size, type))) {
    } else if (OB_NOT_NULL(rank_const) && OB_FAIL(BUF_PRINTF(", "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_NOT_NULL(rank_const) && FALSE_IT(EXPLAIN_PRINT_EXPR(rank_const, type))) {
    } else if (OB_FAIL(BUF_PRINTF(", "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (FALSE_IT(EXPLAIN_PRINT_EXPR(min_score, type))) {
    }
  } else {
    if (OB_FAIL(BUF_PRINTF("search index:"))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    }
  }
  for (int i = 0; i < children_.count() && OB_SUCC(ret); i++) {
    ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(children_.at(i));
    if (OB_FAIL(SMART_CALL(child_node->explain_info(buf, buf_len, pos, type, blank_space_count + 2)))) {
      LOG_WARN("failed to explain info for child", K(ret), K(i), KPC(child_node));
    }
  }
  return ret;
}

int ObBooleanQueryNode::explain_info(char *buf, int64_t buf_len, int64_t &pos, ExplainType type, int blank_space_count)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(print_blank_space(buf, buf_len, pos, blank_space_count))) {
    LOG_WARN("failed to print blank space", K(ret), K(blank_space_count));
  } else if (OB_FAIL(BUF_PRINTF("boolean node:"))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else if (must_nodes_.count() > 0) {
    if (OB_FAIL(print_blank_space(buf, buf_len, pos, blank_space_count + 2))) {
      LOG_WARN("failed to print blank space", K(ret), K(blank_space_count));
    } else if (OB_FAIL(BUF_PRINTF("must:"))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    }
    for (int i = 0; i < must_nodes_.count() && OB_SUCC(ret); i++) {
      ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(must_nodes_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->explain_info(buf, buf_len, pos, type, blank_space_count + 4)))) {
        LOG_WARN("failed to explain info for child", K(ret), K(i), KPC(child_node));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (should_nodes_.count() > 0) {
    if (OB_FAIL(print_blank_space(buf, buf_len, pos, blank_space_count + 2))) {
      LOG_WARN("failed to print blank space", K(ret), K(blank_space_count));
    } else if (OB_FAIL(BUF_PRINTF("should:"))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    }
    for (int i = 0; i < should_nodes_.count() && OB_SUCC(ret); i++) {
      ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(should_nodes_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->explain_info(buf, buf_len, pos, type, blank_space_count + 4)))) {
        LOG_WARN("failed to explain info for child", K(ret), K(i), KPC(child_node));
        }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (filter_nodes_.count() > 0) {
    if (OB_FAIL(print_blank_space(buf, buf_len, pos, blank_space_count + 2))) {
      LOG_WARN("failed to print blank space", K(ret), K(blank_space_count));
    } else if (OB_FAIL(BUF_PRINTF("filter:"))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    }
    for (int i = 0; i < filter_nodes_.count() && OB_SUCC(ret); i++) {
      ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(filter_nodes_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->explain_info(buf, buf_len, pos, type, blank_space_count + 4)))) {
        LOG_WARN("failed to explain info for child", K(ret), K(i), KPC(child_node));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (must_not_nodes_.count() > 0) {
    if (OB_FAIL(print_blank_space(buf, buf_len, pos, blank_space_count))) {
      LOG_WARN("failed to print blank space", K(ret), K(blank_space_count));
    } else if (OB_FAIL(BUF_PRINTF("must not:"))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    }
    for (int i = 0; i < must_not_nodes_.count() && OB_SUCC(ret); i++) {
      ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(must_not_nodes_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->explain_info(buf, buf_len, pos, type, blank_space_count + 4)))) {
        LOG_WARN("failed to explain info for child", K(ret), K(i), KPC(child_node));
      }
    }
  }
  return ret;
}

int ObBooleanQueryNode::extract_filter(ObRawExprFactory &expr_factory, ObRawExpr *&extracted_expr)
{
  int ret = OB_SUCCESS;
  extracted_expr = nullptr;
  ObSEArray<ObRawExpr*, 8> all_filters;

  if (!must_not_nodes_.empty()) {
    ret = OB_NOT_SUPPORTED;
    LOG_INFO("bool node with must_not is not supported for filter extraction", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < must_nodes_.count(); ++i) {
    ObHybridSearchNodeBase *child = static_cast<ObHybridSearchNodeBase*>(must_nodes_.at(i));
    if (OB_ISNULL(child)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child is null", K(ret), K(i));
    } else if (OB_UNLIKELY(child->node_type_ == INDEX_MERGE_HYBRID_BOOLEAN_QUERY)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("nested boolean query node is not supported for filter extraction", K(ret));
    } else {
      ObRawExpr *child_filter = nullptr;
      if (OB_FAIL(child->extract_filter(expr_factory, child_filter))) {
        if (OB_NOT_SUPPORTED != ret) {
          LOG_WARN("failed to extract filter from must child node", K(ret), K(i));
        }
      } else if (OB_NOT_NULL(child_filter) && OB_FAIL(all_filters.push_back(child_filter))) {
        LOG_WARN("failed to push back must child filter", K(ret));
      }
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < filter_nodes_.count(); ++i) {
    ObHybridSearchNodeBase *child = static_cast<ObHybridSearchNodeBase*>(filter_nodes_.at(i));
    if (OB_ISNULL(child)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child is null", K(ret), K(i));
    } else if (OB_UNLIKELY(child->node_type_ == INDEX_MERGE_HYBRID_BOOLEAN_QUERY)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("nested boolean query node is not supported for filter extraction", K(ret));
    } else {
      ObRawExpr *child_filter = nullptr;
      if (OB_FAIL(child->extract_filter(expr_factory, child_filter))) {
        if (OB_NOT_SUPPORTED != ret) {
          LOG_WARN("failed to extract filter from must child node", K(ret), K(i));
        }
      } else if (OB_NOT_NULL(child_filter) && OB_FAIL(all_filters.push_back(child_filter))) {
        LOG_WARN("failed to push back filter child filter", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && !should_nodes_.empty()) {
    ObSEArray<ObRawExpr*, 4> should_filters;

    if (min_should_match_ == 0) {
      // do nothing
    } else if (min_should_match_ == 1) {
      for (int64_t i = 0; OB_SUCC(ret) && i < should_nodes_.count(); ++i) {
        ObHybridSearchNodeBase *child = static_cast<ObHybridSearchNodeBase*>(should_nodes_.at(i));
        if (OB_ISNULL(child)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("child is null", K(ret), K(i));
        } else if (OB_UNLIKELY(child->node_type_ == INDEX_MERGE_HYBRID_BOOLEAN_QUERY)) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("nested boolean query node is not supported for filter extraction", K(ret));
        } else {
          ObRawExpr *child_filter = nullptr;
          if (OB_FAIL(child->extract_filter(expr_factory, child_filter))) {
            if (OB_NOT_SUPPORTED != ret) {
              LOG_WARN("failed to extract filter from must child node", K(ret), K(i));
            }
          } else if (OB_NOT_NULL(child_filter) && OB_FAIL(should_filters.push_back(child_filter))) {
            LOG_WARN("failed to push back should child filter", K(ret));
          }
        }
      }

      if (OB_SUCC(ret) && !should_filters.empty()) {
        ObRawExpr *or_expr = nullptr;
        if (OB_FAIL(ObRawExprUtils::build_or_exprs(expr_factory, should_filters, or_expr))) {
          LOG_WARN("failed to build or expr from should filters", K(ret));
        } else if (OB_NOT_NULL(or_expr) && OB_FAIL(all_filters.push_back(or_expr))) {
          LOG_WARN("failed to push back should or expr", K(ret));
        }
      }
    } else if (min_should_match_ >= should_nodes_.count()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < should_nodes_.count(); ++i) {
        ObHybridSearchNodeBase *child = static_cast<ObHybridSearchNodeBase*>(should_nodes_.at(i));
        if (OB_ISNULL(child)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("child is null", K(ret), K(i));
        } else if (OB_UNLIKELY(child->node_type_ == INDEX_MERGE_HYBRID_BOOLEAN_QUERY)) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("nested boolean query node is not supported for filter extraction", K(ret));
        } else {
          ObRawExpr *child_filter = nullptr;
          if (OB_FAIL(child->extract_filter(expr_factory, child_filter))) {
            if (OB_NOT_SUPPORTED != ret) {
              LOG_WARN("failed to extract filter from must child node", K(ret), K(i));
            }
          } else if (OB_NOT_NULL(child_filter) && OB_FAIL(all_filters.push_back(child_filter))) {
            LOG_WARN("failed to push back should child filter", K(ret));
          }
        }
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_INFO("minimum_should_match between 1 and count is not supported",
               K(ret), K(min_should_match_), K(should_nodes_.count()));
    }
  }

  if (FAILEDx(ObRawExprUtils::build_and_expr(expr_factory, all_filters, extracted_expr))) {
    LOG_WARN("failed to build final and expr", K(ret), K(all_filters.count()));
  }

  return ret;
}

int ObScalarQueryNode::explain_info(char *buf, int64_t buf_len, int64_t &pos, ExplainType type, int blank_space_count)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(print_blank_space(buf, buf_len, pos, blank_space_count))) {
    LOG_WARN("failed to print blank space", K(ret), K(blank_space_count));
  } else if (OB_FAIL(BUF_PRINTF("scalar node: "))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else if (has_valid_index_) {
    if (OB_FAIL(BUF_PRINTF("index name=%.*s", index_name_.length(), index_name_.ptr()))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF(", "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (has_valid_index_) {
    ObIArray<ObRawExpr*> &range_cond = filter_;
    EXPLAIN_PRINT_EXPRS(range_cond, type);
  } else {
    ObIArray<ObRawExpr*> &filter = filter_;
    EXPLAIN_PRINT_EXPRS(filter, type);
  }
  return ret;
}

int ObScalarQueryNode::extract_filter(ObRawExprFactory &expr_factory, ObRawExpr *&extracted_expr)
{
  int ret = OB_SUCCESS;
  extracted_expr = nullptr;

  if (OB_FAIL(ObRawExprUtils::build_and_expr(expr_factory, filter_, extracted_expr))) {
    LOG_WARN("failed to build and expr from filters", K(ret), K(filter_.count()));
  }

  return ret;
}

int ObVecSearchNode::explain_info(char *buf, int64_t buf_len, int64_t &pos, ExplainType type, int blank_space_count)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(print_blank_space(buf, buf_len, pos, blank_space_count))) {
    LOG_WARN("failed to print blank space", K(ret), K(blank_space_count));
  } else if (OB_FAIL(BUF_PRINTF("vector node:"))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else if (OB_FAIL(BUF_PRINTF(" index name=%.*s", index_name_.length(), index_name_.ptr()))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else if (search_option_ != nullptr) {
    if (OB_FAIL(BUF_PRINTF(", filter_mode=%d", search_option_->filter_mode_))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (search_option_->param_.is_set_ef_search_ &&
      OB_FAIL(BUF_PRINTF(", ef_search=%d", search_option_->param_.ef_search_))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (search_option_->param_.is_set_refine_k_ &&
      OB_FAIL(BUF_PRINTF(", refine_k=%f", search_option_->param_.refine_k_))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (filter_nodes_.count() > 0) {
   if (OB_FAIL(print_blank_space(buf, buf_len, pos, blank_space_count + 2))) {
      LOG_WARN("failed to print blank space", K(ret), K(blank_space_count + 2));
    } else if (OB_FAIL(BUF_PRINTF("filter:"))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    }
    for (int i = 0; i < filter_nodes_.count() && OB_SUCC(ret); i++) {
      ObHybridSearchNodeBase *child_node = static_cast<ObHybridSearchNodeBase *>(filter_nodes_.at(i));
      if (OB_FAIL(SMART_CALL(child_node->explain_info(buf, buf_len, pos, type, blank_space_count + 4)))) {
        LOG_WARN("failed to explain info for child", K(ret), K(i), KPC(child_node));
      }
    }
  }
  return ret;
}

bool ObHybridSearchGenerator::is_search_subquery(ObIndexMergeNode *node) const
{
  return node->node_type_ == INDEX_MERGE_HYBRID_SCALAR_QUERY ||
         node->node_type_ == INDEX_MERGE_HYBRID_BOOLEAN_QUERY ||
         node->node_type_ == INDEX_MERGE_HYBRID_FTS_QUERY;
}

bool ObHybridSearchGenerator::is_knn_subquery(ObIndexMergeNode *node) const
{
  return node->node_type_ == INDEX_MERGE_HYBRID_KNN;
}

int ObHybridSearchGenerator::collect_child_boost_and_update_flags(
    ObFusionNode *fusion_node,
    ObIndexMergeNode *sub_node,
    ObConstRawExpr *one_boost_expr,
    int64_t child_idx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(fusion_node) || OB_ISNULL(sub_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fusion_node or sub_node is null", K(ret), KP(fusion_node), KP(sub_node));
  } else {
    ObHybridSearchNodeBase *top_score_node = static_cast<ObHybridSearchNodeBase *>(sub_node);
    ObRawExpr *path_top_k_limit = nullptr;
    if (OB_FAIL(fusion_node->weight_cols_.push_back(top_score_node->boost_))) {
      LOG_WARN("failed to push back weight expr", K(ret), K(child_idx));
    } else if (OB_FALSE_IT(top_score_node->boost_ = one_boost_expr)) {
      LOG_WARN("failed to set boost expr", K(ret));
    } else if (is_search_subquery(sub_node)) {
      if (fusion_node->has_search_subquery_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("multiple search subquery", K(ret));
      } else {
        fusion_node->has_search_subquery_ = true;
        fusion_node->search_index_ = child_idx;
        path_top_k_limit = fusion_node->window_size_;
      }
    } else if (is_knn_subquery(sub_node)) {
      fusion_node->has_vector_subquery_ = true;
      path_top_k_limit = static_cast<ObVecSearchNode *>(top_score_node)->get_topk_limit_expr();
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(path_top_k_limit)) {
      fusion_node->path_top_k_limit_.push_back(path_top_k_limit);
    }
  }
  return ret;
}

int ObHybridSearchGenerator::split_domain_contains_node(const ObDSLScalarQuery *scalar_query,
                                                        ObIndexMergeNode *&split_node)
{
  int ret = OB_SUCCESS;
  split_node = nullptr;
  ObRawExpr *filter_expr = nullptr;
  if (OB_ISNULL(scalar_query)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null scalar query", K(ret), KP(scalar_query));
  } else if (OB_ISNULL(filter_expr = scalar_query->scalar_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null filter expr", K(ret), KPC(scalar_query));
  } else if (OB_ISNULL(filter_expr = ObRawExprUtils::skip_inner_added_expr(filter_expr))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null filter expr", K(ret), KPC(filter_expr));
  } else if (filter_expr->get_expr_type() == T_FUN_SYS_JSON_CONTAINS ||
             filter_expr->get_expr_type() == T_FUNC_SYS_ARRAY_CONTAINS_ALL) {
    bool has_search_index = false;
    if (OB_FAIL(check_filter_has_search_index(filter_expr, has_search_index))) {
      LOG_WARN("failed to check filter has search index", K(ret));
    } else if (!has_search_index) {
      // no search index, no need to split
    } else if (filter_expr->get_expr_type() == T_FUN_SYS_JSON_CONTAINS) {
      if (OB_FAIL(split_json_contains(scalar_query, filter_expr, split_node))) {
        LOG_WARN("failed to split json contains node", K(ret));
      }
    } else if (filter_expr->get_expr_type() == T_FUNC_SYS_ARRAY_CONTAINS_ALL) {
      if (OB_FAIL(split_array_contains_all(scalar_query, filter_expr, split_node))) {
        LOG_WARN("failed to split array contains all node", K(ret));
      }
    }
  } else {
    // no need to split
  }
  return ret;
}

int ObHybridSearchGenerator::split_json_contains(const ObDSLScalarQuery *scalar_query,
                                                 ObRawExpr *filter_expr,
                                                 ObIndexMergeNode *&split_node)
{
  int ret = OB_SUCCESS;
  split_node = nullptr;
  ObRawExprFactory *expr_factory = nullptr;
  ObExecContext *exec_ctx = nullptr;
  if (OB_ISNULL(scalar_query) || OB_ISNULL(plan_) || OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), KP(scalar_query), KP(plan_), KP(allocator_));
  } else if (OB_ISNULL(exec_ctx = plan_->get_optimizer_context().get_exec_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec ctx is null", K(ret), KP(exec_ctx));
  } else if (OB_ISNULL(expr_factory = &plan_->get_optimizer_context().get_expr_factory())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr factory", K(ret));
  } else {
    const ObRawExpr *candidate_expr = nullptr;
    if (filter_expr->get_param_count() == 2) {
      candidate_expr = filter_expr->get_param_expr(1);
    }
    if (OB_ISNULL(candidate_expr) || !candidate_expr->is_const_expr() ||
       !ObJsonExprHelper::is_convertible_to_json(candidate_expr->get_result_type().get_type())) {
      // do nothing
    } else {
      bool got_value = false;
      ObObj result;
      ObArenaAllocator tmp_allocator(ObModIds::OB_SQL_EXPR_CALC);
      ObIJsonBase *j_base = nullptr;
      bool can_extract = false;
      if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(exec_ctx,
                                                           candidate_expr,
                                                           result,
                                                           got_value,
                                                           *allocator_))) {
        LOG_WARN("failed to calc const expr", K(ret), KPC(candidate_expr));
      } else if (!got_value || result.is_null()) {
        // no need to split
      } else if (OB_FAIL(ObJsonExprHelper::refine_range_json_value_const(result,
                                                                         exec_ctx,
                                                                         false,
                                                                         allocator_,
                                                                         j_base))) {
        LOG_WARN("failed to refine range json value const", K(ret));
      } else if (OB_ISNULL(j_base)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get json base", K(ret));
      } else if (j_base->is_json_scalar(j_base->json_type())) {
        // scalar, no need to split, need to add scalar constraint during query range extraction
      } else if (j_base->json_type() != common::ObJsonNodeType::J_ARRAY) {
        // not an array, no need to split
      } else if (OB_FAIL(ObSearchIndexConstraint::is_json_scalar_or_array_match(j_base, 0,
                                                                                can_extract))) {
        LOG_WARN("failed to check json array match", K(ret));
      } else if (!can_extract) {
        // not all member json scalar or json string length can safety to compare after search
        // index value encoding, no need to split.
      } else {
        // array, need to split
        // e.g. json_contains(doc, '[1,2,3]') ->
        // json_contains(doc, json_extract('[1,2,3]', '$[0]')) AND
        // json_contains(doc, json_extract('[1,2,3]', '$[1]')) AND
        // json_contains(doc, json_extract('[1,2,3]', '$[2]'))
        const int64_t element_count = j_base->element_count();
        ObBooleanQueryNode *bool_node = nullptr;
        if (OB_ISNULL(bool_node = OB_NEWx(ObBooleanQueryNode, allocator_, *allocator_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate boolean node", K(ret));
        } else {
          bool_node->origin_expr_ = filter_expr;
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < element_count; ++i) {
          ObRawExpr *json_extract_expr = nullptr;
          ObSysFunRawExpr *new_expr = nullptr;
          ObScalarQueryNode *child_node = nullptr;
          char *json_path = nullptr;
          const static int64_t MAX_JSON_PATH_SIZE = 32;
          if (OB_ISNULL(json_path = static_cast<char *>(allocator_->alloc(MAX_JSON_PATH_SIZE)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate json path", K(ret));
          } else if (OB_FALSE_IT(snprintf(json_path, MAX_JSON_PATH_SIZE, "$[%ld]", i))) {
          } else if (OB_FAIL(ObRawExprUtils::build_json_extract_expr(*expr_factory,
                                                                      *plan_->get_optimizer_context().get_session_info(),
                                                                      const_cast<ObRawExpr*>(candidate_expr),
                                                                      ObString::make_string(json_path),
                                                                      json_extract_expr))) {
            LOG_WARN("failed to build json extract expr", K(ret));
          } else if (OB_ISNULL(json_extract_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("json extract expr is null", K(ret));
          } else if (OB_FAIL(expr_factory->create_raw_expr(T_FUN_SYS_JSON_CONTAINS, new_expr))) {
            LOG_WARN("failed to create json contains expr", K(ret));
          } else if (OB_FALSE_IT(new_expr->set_func_name(N_JSON_CONTAINS))) {
          } else if (OB_FAIL(new_expr->set_param_exprs(filter_expr->get_param_expr(0), json_extract_expr))) {
            LOG_WARN("failed to set json contains params", K(ret));
          } else if (OB_FAIL(new_expr->formalize(plan_->get_optimizer_context().get_session_info()))) {
            LOG_WARN("failed to formalize json contains expr", K(ret));
          } else if (OB_FALSE_IT(new_expr->set_is_inner_split_contains_expr(true))) {
          } else if (OB_FAIL(generate_scalar_node(new_expr, child_node))) {
            LOG_WARN("failed to generate scalar node", K(ret));
          } else if (OB_UNLIKELY(1 != child_node->pri_table_query_params_.pushdown_filters_.count())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected pushdown filter count", K(ret),
              K(child_node->pri_table_query_params_.pushdown_filters_.count()));
          } else if (FALSE_IT(child_node->pri_table_query_params_.pushdown_filters_.at(0) = filter_expr)) {
            // replace the pushdown filter with the original filter
          } else if (OB_FAIL(bool_node->filter_nodes_.push_back(child_node))) {
            LOG_WARN("failed to push back child node", K(ret));
          } else {
            ObHybridSearchNodeBase *new_node = static_cast<ObHybridSearchNodeBase *>(child_node);
            new_node->is_scoring_ = scalar_query->need_cal_score_;
            new_node->is_top_level_scoring_ = scalar_query->is_top_level_score_;
            new_node->boost_ = scalar_query->boost_;
          }
        }
        if (OB_SUCC(ret)) {
          // add json array count constraint
          PreCalcExprExpectResult expect_result = PRE_CALC_SEARCH_INDEX_CONSTRAINT;
          int64_t extra = ObSearchIndexConstraint::make_extra(
              ObSearchIndexConstraint::JSON_ARRAY_COUNT, 0, element_count);
          ObConstraintExtra cons_extra;
          cons_extra.extra_ = extra;
          if (OB_FAIL(bool_node->all_expr_constraints_.push_back(
              ObExprConstraint(const_cast<ObRawExpr*>(candidate_expr), expect_result, cons_extra)))) {
            LOG_WARN("failed to push back expr constraint", K(ret));
          } else {
            split_node = bool_node;
          }
        }
      }
    }
  }
  return ret;
}

int ObHybridSearchGenerator::split_array_contains_all(const ObDSLScalarQuery *scalar_query,
                                                      ObRawExpr *filter_expr,
                                                      ObIndexMergeNode *&split_node)
{
  int ret = OB_SUCCESS;
  split_node = nullptr;
  ObRawExprFactory *expr_factory = nullptr;
  ObExecContext *exec_ctx = nullptr;
  if (OB_ISNULL(scalar_query) || OB_ISNULL(plan_) || OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), KP(scalar_query), KP(plan_), KP(allocator_));
  } else if (OB_ISNULL(exec_ctx = plan_->get_optimizer_context().get_exec_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec ctx is null", K(ret), KP(exec_ctx));
  } else if (OB_ISNULL(expr_factory = &plan_->get_optimizer_context().get_expr_factory())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr factory", K(ret));
  } else {
    const ObRawExpr *array_expr = nullptr;
    const ObRawExpr *candidate_expr = nullptr;
    if (filter_expr->get_param_count() == 2) {
      array_expr = filter_expr->get_param_expr(0);
      candidate_expr = filter_expr->get_param_expr(1);
    }
    if (OB_ISNULL(array_expr) || !array_expr->has_flag(IS_COLUMN)) {
      // do nothing
    } else if (OB_ISNULL(candidate_expr) || !candidate_expr->is_const_expr()) {
      // do nothing
    } else {
      bool got_value = false;
      ObObj result;
      ObArenaAllocator tmp_allocator(ObModIds::OB_SQL_EXPR_CALC);
      const common::ObSqlCollectionInfo *coll_info = nullptr;
      const common::ObSqlCollectionInfo *col_coll_info = nullptr;
      if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(exec_ctx,
                                                           candidate_expr,
                                                           result,
                                                           got_value,
                                                           *allocator_))) {
        LOG_WARN("failed to calc const expr", K(ret), KPC(candidate_expr));
      } else if (!got_value || result.is_null() || !result.is_collection_sql_type()) {
        // no need to split
      } else if (OB_FAIL(ObRawExprUtils::get_expr_collection_info(candidate_expr, exec_ctx, coll_info))) {
        LOG_WARN("failed to get collection info", K(ret));
      } else if (OB_ISNULL(coll_info) || OB_ISNULL(coll_info->collection_meta_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("collection info is null", K(ret), KPC(coll_info));
      } else if (OB_FAIL(ObRawExprUtils::get_expr_collection_info(array_expr, exec_ctx, col_coll_info))) {
        LOG_WARN("failed to get column collection info", K(ret));
      } else if (OB_ISNULL(col_coll_info) || OB_ISNULL(col_coll_info->collection_meta_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column collection info is null", K(ret));
      } else {
        ObString data_str = result.get_string();
        common::ObIArrayType *arr_obj = nullptr;
        bool can_extract = true;
        const bool need_array_string_constraint =
          ObSearchIndexConstraint::need_array_string_constraint(*col_coll_info);
        if (OB_FAIL(ObTextStringHelper::read_real_string_data(&tmp_allocator, result, data_str))) {
          LOG_WARN("failed to read array data", K(ret));
        } else if (OB_FAIL(common::ObArrayTypeObjFactory::construct(tmp_allocator,
                                                                    *coll_info->collection_meta_,
                                                                    arr_obj,
                                                                    true))) {
          LOG_WARN("failed to construct array obj", K(ret));
        } else if (OB_FAIL(arr_obj->init(data_str))) {
          LOG_WARN("failed to init array obj", K(ret));
        } else if (need_array_string_constraint &&
            OB_FAIL(ObSearchIndexConstraint::is_array_string_length_match(arr_obj, can_extract))) {
          LOG_WARN("failed to check array string length", K(ret));
        } else if (!can_extract) {
          // do nothing
        } else {
          ObBooleanQueryNode *bool_node = nullptr;
          if (OB_ISNULL(bool_node = OB_NEWx(ObBooleanQueryNode, allocator_, *allocator_))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate boolean node", K(ret));
          } else {
            bool_node->origin_expr_ = filter_expr;
          }
          const int64_t element_count = arr_obj->size();
          for (int64_t i = 0; OB_SUCC(ret) && i < element_count; ++i) {
            ObConstRawExpr *idx_expr = nullptr;
            ObSysFunRawExpr *element_at_expr = nullptr;
            ObSysFunRawExpr *new_expr = nullptr;
            ObScalarQueryNode *child_node = nullptr;
            // Build element_at(array, index) expression, note: element_at uses 1-based index
            if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*expr_factory, ObIntType, i + 1, idx_expr))) {
              LOG_WARN("failed to build const int expr", K(ret), K(i));
            } else if (OB_FAIL(expr_factory->create_raw_expr(T_FUNC_SYS_ELEMENT_AT, element_at_expr))) {
              LOG_WARN("failed to create element_at expr", K(ret));
            } else if (OB_FALSE_IT(element_at_expr->set_func_name(N_ELEMENT_AT))) {
            } else if (OB_FAIL(element_at_expr->set_param_exprs(
                const_cast<ObRawExpr*>(candidate_expr), idx_expr))) {
              LOG_WARN("failed to set element_at params", K(ret));
            } else if (OB_FAIL(element_at_expr->formalize(plan_->get_optimizer_context().get_session_info()))) {
              LOG_WARN("failed to formalize element_at expr", K(ret));
            } else if (OB_FAIL(expr_factory->create_raw_expr(T_FUNC_SYS_ARRAY_CONTAINS, new_expr))) {
              LOG_WARN("failed to create array contains expr", K(ret));
            } else if (OB_FALSE_IT(new_expr->set_func_name(N_ARRAY_CONTAINS))) {
            } else if (OB_FAIL(new_expr->set_param_exprs(filter_expr->get_param_expr(0), element_at_expr))) {
              LOG_WARN("failed to set array contains params", K(ret));
            } else if (OB_FAIL(new_expr->formalize(plan_->get_optimizer_context().get_session_info()))) {
              LOG_WARN("failed to formalize array contains expr", K(ret));
            } else if (OB_FALSE_IT(new_expr->set_is_inner_split_contains_expr(true))) {
            } else if (OB_FAIL(generate_scalar_node(new_expr, child_node))) {
              LOG_WARN("failed to generate scalar node", K(ret));
            } else if (OB_FAIL(bool_node->filter_nodes_.push_back(child_node))) {
              LOG_WARN("failed to push back child node", K(ret));
            } else {
              ObHybridSearchNodeBase *new_node = static_cast<ObHybridSearchNodeBase *>(child_node);
              new_node->is_scoring_ = scalar_query->need_cal_score_;
              new_node->is_top_level_scoring_ = scalar_query->is_top_level_score_;
              new_node->boost_ = scalar_query->boost_;
            }
          }
          if (OB_SUCC(ret)) {
            PreCalcExprExpectResult expect_result = PRE_CALC_SEARCH_INDEX_CONSTRAINT;
            // add array string length constraint
            if (need_array_string_constraint) {
              int64_t extra = ObSearchIndexConstraint::make_extra(
                ObSearchIndexConstraint::ARRAY_STRING_LENGTH, 0, 0);
              ObConstraintExtra cons_extra;
              cons_extra.extra_ = extra;
              if (OB_FAIL(bool_node->all_expr_constraints_.push_back(
                  ObExprConstraint(const_cast<ObRawExpr*>(candidate_expr), expect_result, cons_extra)))) {
                LOG_WARN("failed to push back expr constraint", K(ret));
              }
            }
            if (OB_SUCC(ret)) {
              // add array count constraint
              int64_t extra = ObSearchIndexConstraint::make_extra(
                ObSearchIndexConstraint::ARRAY_ELEMENT_COUNT, 0, element_count);
              ObConstraintExtra cons_extra;
              cons_extra.extra_ = extra;
              if (OB_FAIL(bool_node->all_expr_constraints_.push_back(
                  ObExprConstraint(const_cast<ObRawExpr*>(candidate_expr), expect_result, cons_extra)))) {
                LOG_WARN("failed to push back expr constraint", K(ret));
              } else {
                split_node = bool_node;
              }
            }
          }
        }
      }
    }
  }
  return ret;
}
